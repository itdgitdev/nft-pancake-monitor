import mysql.connector
from flask import jsonify
from datetime import timedelta
from services.pancake_api import get_data_pool_bsc
from services.helper import to_datetime_safe
from web3 import Web3
from services.db_connect import get_connection, get_db_config

DB_CONFIG = get_db_config()

def _fetchall(query, params=None, dict_cursor=True):
    """Helper to fetch all rows from a query."""
    with get_connection() as conn:
        with conn.cursor(dictionary=dict_cursor) as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

def _fetchone(query, params=None, dict_cursor=True):
    """Helper to fetch one row from a query."""
    with get_connection() as conn:
        with conn.cursor(dictionary=dict_cursor) as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchone()

def _execute(query, params=None):
    """Helper to execute a query (insert/update/delete)."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.rowcount

def fetch_latest_nft_id():
    """Fetch the latest NFT positions that are not closed or blacklisted."""
    query = """
        SELECT t1.*,
            CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted
        FROM wallet_nft_position t1
        INNER JOIN (
            SELECT nft_id, MAX(created_at) AS max_created_at
            FROM wallet_nft_position
            GROUP BY nft_id
        ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
        LEFT JOIN nft_blacklist b 
            ON t1.wallet_address = b.wallet_address AND t1.chain = b.chain AND t1.nft_id = b.nft_id
        WHERE t1.status != 'Closed' AND b.id IS NULL
        ORDER BY t1.created_at DESC;
    """
    try:
        return _fetchall(query)
    except mysql.connector.Error as e:
        print(f"Error fetching full NFT data: {e}")
        return []

def fetch_latest_nft_by_wallet(wallet_address):
    """Fetch the latest NFT positions for a specific wallet."""
    query = """
        SELECT t1.*,
            CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted
        FROM wallet_nft_position t1
        INNER JOIN (
            SELECT nft_id, MAX(created_at) AS max_created_at
            FROM wallet_nft_position
            GROUP BY nft_id
        ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
        LEFT JOIN nft_blacklist b 
            ON t1.wallet_address = b.wallet_address AND t1.chain = b.chain AND t1.nft_id = b.nft_id
        WHERE LOWER(t1.wallet_address) = LOWER(%s)
          AND t1.status != 'Closed'
          AND b.id IS NULL
        ORDER BY t1.created_at DESC
    """
    try:
        return _fetchall(query, (wallet_address,))
    except mysql.connector.Error as e:
        print(f"Error fetching latest NFT data by wallet: {e}")
        return []
            
def fetch_latest_nft_by_wallet_and_chain(wallet_address, chain):
    """Fetch the latest NFT positions for a specific wallet and chain."""
    query = """
        SELECT t1.*
        FROM wallet_nft_position t1
        INNER JOIN (
            SELECT nft_id, MAX(created_at) AS max_created_at
            FROM wallet_nft_position
            GROUP BY nft_id
        ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
        LEFT JOIN nft_blacklist b 
            ON t1.wallet_address = b.wallet_address 
            AND t1.chain = b.chain 
            AND t1.nft_id = b.nft_id
        WHERE t1.chain = %s 
          AND LOWER(t1.wallet_address) = LOWER(%s) 
          AND t1.status != 'Closed'
          AND b.id IS NULL
        ORDER BY t1.created_at DESC
    """
    try:
        return _fetchall(query, (chain, wallet_address))
    except mysql.connector.Error as e:
        print(f"Error fetching latest NFT data by wallet and chain: {e}")
        return []

def fetch_nft_by_token(token):
    """Fetch NFT positions filtered by token, not closed or blacklisted."""
    query = """
        SELECT t1.*, 
               CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted
        FROM wallet_nft_position t1
        INNER JOIN (
            SELECT nft_id, MAX(created_at) AS max_created_at
            FROM wallet_nft_position
            GROUP BY nft_id
        ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
        LEFT JOIN nft_blacklist b 
               ON t1.wallet_address = b.wallet_address 
               AND t1.chain = b.chain 
               AND t1.nft_id = b.nft_id
        LEFT JOIN pool_info p
               ON t1.chain = p.chain AND (
                    (t1.token0_symbol = p.token0_symbol AND t1.token1_symbol = p.token1_symbol)
                 OR (t1.token0_symbol = p.token1_symbol AND t1.token1_symbol = p.token0_symbol)
               )
        WHERE t1.status != 'Closed'
          AND b.id IS NULL
    """
    params = []
    if token:
        query += """
            AND (
                LOWER(p.token0_symbol) LIKE %s OR LOWER(p.token1_symbol) LIKE %s OR
                LOWER(p.token0_address) = %s OR LOWER(p.token1_address) = %s OR
                LOWER(p.pool_address) = %s
            )
        """
        token_like = f"%{token.lower()}%"
        token_exact = token.lower()
        params.extend([token_like, token_like, token_exact, token_exact, token_exact])
    query += " ORDER BY t1.created_at DESC"
    try:
        return _fetchall(query, params)
    except mysql.connector.Error as e:
        print(f"[DB Error] filter_nft_by_token_only: {e}")
        return []

def fetch_latest_summary_by_token(wallet_address, start_date=None, end_date=None):
    """Fetch summary by token for a wallet, with optional date filtering."""
    start_date = to_datetime_safe(start_date)
    end_date = to_datetime_safe(end_date)
    if end_date:
        end_date += timedelta(days=1)
    date_condition = ""
    if start_date and end_date:
        date_condition = "AND created_at >= %s AND created_at < %s"
    elif start_date:
        date_condition = "AND created_at >= %s"
    elif end_date:
        date_condition = "AND created_at < %s"
    subquery_condition = f"""
        AND (wallet_address, nft_id, created_at) IN (
            SELECT p.wallet_address, p.nft_id, MAX(p.created_at)
            FROM wallet_nft_position p
            LEFT JOIN nft_blacklist b 
                ON p.wallet_address = b.wallet_address 
                AND p.chain = b.chain 
                AND p.nft_id = b.nft_id
            WHERE p.wallet_address = %s 
                AND p.status != 'Closed' 
                AND b.id IS NULL
                {date_condition}
            GROUP BY p.wallet_address, p.nft_id
        )
    """
    query = f"""
        SELECT 
            combined.token, 
            SUM(combined.initial_amount) AS initial, 
            SUM(combined.current_amount) AS current, 
            SUM(combined.delta_amount) AS delta, 
            SUM(combined.delta_amount_usd) AS delta_usd,
            reward_data.total_pending_cake AS reward
        FROM (
            SELECT 
                token0_symbol AS token,
                initial_token0_amount AS initial_amount,
                current_token0_amount AS current_amount,
                (current_token0_amount - initial_token0_amount) AS delta_amount,
                (current_token0_amount - initial_token0_amount) * price_token0 AS delta_amount_usd
            FROM wallet_nft_position
            WHERE wallet_address = %s AND status != 'Closed'
            {subquery_condition}

            UNION ALL

            SELECT 
                token1_symbol AS token,
                initial_token1_amount AS initial_amount,
                current_token1_amount AS current_amount,
                (current_token1_amount - initial_token1_amount) AS delta_amount,
                (current_token1_amount - initial_token1_amount) * price_token1 AS delta_amount_usd
            FROM wallet_nft_position
            WHERE wallet_address = %s AND status != 'Closed'
            {subquery_condition}
        ) AS combined

        CROSS JOIN (
            SELECT SUM(pending_cake) AS total_pending_cake
            FROM wallet_nft_position
            WHERE wallet_address = %s AND status != 'Closed'
            {subquery_condition}
        ) AS reward_data

        GROUP BY combined.token, reward_data.total_pending_cake
        ORDER BY combined.token
    """
    def get_date_params():
        if start_date and end_date:
            return [wallet_address, start_date, end_date]
        elif start_date:
            return [wallet_address, start_date]
        elif end_date:
            return [wallet_address, end_date]
        else:
            return [wallet_address]
    params = (
        [wallet_address] + get_date_params() +
        [wallet_address] + get_date_params() +
        [wallet_address] + get_date_params()
    )
    try:
        return _fetchall(query, tuple(params))
    except mysql.connector.Error as e:
        print(f"Lỗi truy vấn: {e}")
        return []

def fetch_latest_summary_by_wallet_and_chain(wallet_address, chain, start_date=None, end_date=None):
    """Fetch summary by token for a wallet and chain, with optional date filtering."""
    start_date = to_datetime_safe(start_date)
    end_date = to_datetime_safe(end_date)
    if end_date:
        end_date += timedelta(days=1)
    date_condition = ""
    if start_date and end_date:
        date_condition = "AND p.created_at >= %s AND p.created_at < %s"
    elif start_date:
        date_condition = "AND p.created_at >= %s"
    elif end_date:
        date_condition = "AND p.created_at < %s"
    subquery_condition = f"""
        AND (wallet_address, nft_id, created_at) IN (
            SELECT p.wallet_address, p.nft_id, MAX(p.created_at)
            FROM wallet_nft_position p
            LEFT JOIN nft_blacklist b 
                ON p.wallet_address = b.wallet_address 
                AND p.chain = b.chain 
                AND p.nft_id = b.nft_id
            WHERE p.wallet_address = %s 
                AND p.chain = %s 
                AND p.status != 'Closed' 
                AND b.id IS NULL
                {date_condition}
            GROUP BY p.wallet_address, p.nft_id
        )
    """
    query = f"""
        SELECT 
            combined.token, 
            SUM(combined.initial_amount) AS initial, 
            SUM(combined.current_amount) AS current, 
            SUM(combined.delta_amount) AS delta, 
            SUM(combined.delta_amount_usd) AS delta_usd,
            reward_data.total_pending_cake AS reward
        FROM (
            SELECT 
                token0_symbol AS token,
                initial_token0_amount AS initial_amount,
                current_token0_amount AS current_amount,
                (current_token0_amount - initial_token0_amount) AS delta_amount,
                (current_token0_amount - initial_token0_amount) * price_token0 AS delta_amount_usd
            FROM wallet_nft_position
            WHERE wallet_address = %s AND chain = %s AND status != 'Closed'
            {subquery_condition}

            UNION ALL

            SELECT 
                token1_symbol AS token,
                initial_token1_amount AS initial_amount,
                current_token1_amount AS current_amount,
                (current_token1_amount - initial_token1_amount) AS delta_amount,
                (current_token1_amount - initial_token1_amount) * price_token1 AS delta_amount_usd
            FROM wallet_nft_position
            WHERE wallet_address = %s AND chain = %s AND status != 'Closed'
            {subquery_condition}
        ) AS combined

        CROSS JOIN (
            SELECT SUM(pending_cake) AS total_pending_cake
            FROM wallet_nft_position
            WHERE wallet_address = %s AND chain = %s AND status != 'Closed'
            {subquery_condition}
        ) AS reward_data

        GROUP BY combined.token, reward_data.total_pending_cake
        ORDER BY combined.token
    """
    def get_date_params():
        if start_date and end_date:
            return [wallet_address, chain, start_date, end_date]
        elif start_date:
            return [wallet_address, chain, start_date]
        elif end_date:
            return [wallet_address, chain, end_date]
        else:
            return [wallet_address, chain]
    params = (
        [wallet_address, chain] + get_date_params() +
        [wallet_address, chain] + get_date_params() +
        [wallet_address, chain] + get_date_params()
    )
    try:
        return _fetchall(query, tuple(params))
    except mysql.connector.Error as e:
        print(f"Lỗi truy vấn: {e}")
        return []

def get_latest_total_pending_cake_by_wallet(wallet_address, start_date=None, end_date=None):
    """Get the latest total pending cake for a wallet, with optional date filtering."""
    start_date = to_datetime_safe(start_date)
    end_date = to_datetime_safe(end_date)
    if end_date:
        end_date += timedelta(days=1)
    date_condition = ""
    if start_date and end_date:
        date_condition = "AND p.created_at >= %s AND p.created_at < %s"
    elif start_date:
        date_condition = "AND p.created_at >= %s"
    elif end_date:
        date_condition = "AND p.created_at < %s"
    subquery = f"""
        SELECT p.wallet_address, p.nft_id, MAX(p.created_at) AS max_created
        FROM wallet_nft_position p
        LEFT JOIN nft_blacklist b 
            ON p.wallet_address = b.wallet_address 
            AND p.chain = b.chain 
            AND p.nft_id = b.nft_id
        WHERE p.wallet_address = %s 
            AND p.status != 'Closed'
            AND b.id IS NULL
            {date_condition}
        GROUP BY p.wallet_address, p.nft_id
    """
    main_query = f"""
        SELECT SUM(wp.pending_cake)
        FROM wallet_nft_position wp
        INNER JOIN (
            {subquery}
        ) latest
            ON wp.wallet_address = latest.wallet_address
            AND wp.nft_id = latest.nft_id
            AND wp.created_at = latest.max_created
    """
    params = [wallet_address]
    if start_date and end_date:
        params += [start_date, end_date]
    elif start_date:
        params.append(start_date)
    elif end_date:
        params.append(end_date)
    try:
        result = _fetchone(main_query, tuple(params), dict_cursor=False)
        return result[0] if result and result[0] is not None else 0
    except mysql.connector.Error as e:
        print(f"Lỗi truy vấn: {e}")
        return 0

def get_latest_total_pending_cake_by_wallet_and_chain(wallet_address, chain, start_date=None, end_date=None):
    """Get the latest total pending cake for a wallet and chain, with optional date filtering."""
    start_date = to_datetime_safe(start_date)
    end_date = to_datetime_safe(end_date)
    if end_date:
        end_date += timedelta(days=1)
    date_condition = ""
    if start_date and end_date:
        date_condition = "AND p.created_at >= %s AND p.created_at < %s"
    elif start_date:
        date_condition = "AND p.created_at >= %s"
    elif end_date:
        date_condition = "AND p.created_at < %s"
    subquery = f"""
        SELECT p.wallet_address, p.chain, p.nft_id, MAX(p.created_at) AS max_created
        FROM wallet_nft_position p
        LEFT JOIN nft_blacklist b 
            ON p.wallet_address = b.wallet_address 
            AND p.chain = b.chain 
            AND p.nft_id = b.nft_id
        WHERE p.wallet_address = %s 
            AND p.chain = %s
            AND p.status != 'Closed'
            AND b.id IS NULL
            {date_condition}
        GROUP BY p.wallet_address, p.nft_id
    """
    main_query = f"""
        SELECT SUM(wp.pending_cake)
        FROM wallet_nft_position wp
        INNER JOIN (
            {subquery}
        ) latest
            ON wp.wallet_address = latest.wallet_address
            AND wp.chain = latest.chain
            AND wp.nft_id = latest.nft_id
            AND wp.created_at = latest.max_created
    """
    params = [wallet_address, chain]
    if start_date and end_date:
        params += [start_date, end_date]
    elif start_date:
        params.append(start_date)
    elif end_date:
        params.append(end_date)
    try:
        result = _fetchone(main_query, tuple(params), dict_cursor=False)
        return result[0] if result and result[0] is not None else 0
    except mysql.connector.Error as e:
        print(f"Lỗi truy vấn: {e}")
        return 0

def fetch_nft_history_by_id(nft_id, limit=30, offset=0):
    """Fetch NFT history by ID with pagination, using context manager helpers."""
    try:
        nft_id = int(nft_id)
        limit = int(limit)
        offset = int(offset)
        if nft_id < 0 or limit < 0 or offset < 0:
            raise ValueError("Parameters must be non-negative integers.")
        query = """
            SELECT *
            FROM wallet_nft_position
            WHERE nft_id = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        return _fetchall(query, (nft_id, limit, offset))
    except mysql.connector.Error as e:
        print(f"Error fetching history for NFT ID {nft_id}: {e}")
        return []
    except ValueError as ve:
        print(f"Invalid parameter: {ve}")
        return []

def count_nft_history_records_by_id(nft_id):
    """Count the number of history records for a given NFT ID."""
    try:
        nft_id = int(nft_id)
        if nft_id < 0:
            raise ValueError("NFT ID must be non-negative integer.")
        query = "SELECT COUNT(*) AS count FROM wallet_nft_position WHERE nft_id = %s"
        result = _fetchone(query, (nft_id,), dict_cursor=True)
        return result['count'] if result and 'count' in result else 0
    except mysql.connector.Error as e:
        print(f"Error counting history records for NFT ID {nft_id}: {e}")
        return 0
    except ValueError as ve:
        print(f"Invalid parameter: {ve}")
        return 0

def toggle_blacklist(wallet_address, chain, nft_id):
    """Toggle blacklist status for a given NFT."""
    try:
        # Check if NFT is already blacklisted
        select_query = """
            SELECT 1 FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s AND nft_id = %s
        """
        exists = _fetchone(select_query, (wallet_address, chain, nft_id), dict_cursor=False)
        if exists:
            delete_query = """
                DELETE FROM nft_blacklist 
                WHERE wallet_address = %s AND chain = %s AND nft_id = %s
            """
            _execute(delete_query, (wallet_address, chain, nft_id))
            message = f'Removed NFT ID: {nft_id} from Blacklist'
        else:
            insert_query = """
                INSERT INTO nft_blacklist (wallet_address, chain, nft_id)
                VALUES (%s, %s, %s)
            """
            _execute(insert_query, (wallet_address, chain, nft_id))
            message = f'Added NFT ID: {nft_id} to Blacklist'
        return {'status': 'success', 'message': message}
    except mysql.connector.Error as e:
        print(f"Error toggling blacklist: {e}")
        return {'status': 'error', 'message': 'Failed to toggle blacklist'}
            
def get_blacklist_nft_ids(wallet_address, chain_name):
    """Get all blacklisted NFT IDs for a wallet and chain."""
    try:
        query = """
            SELECT nft_id 
            FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s
        """
        results = _fetchall(query, (wallet_address, chain_name), dict_cursor=False)
        return [row[0] for row in results] if results else []
    except mysql.connector.Error as e:
        print(f"Error fetching blacklist NFT IDs: {e}")
        return []
            
def fetch_blacklist_nft_ids():
    """Fetch all NFT blacklist records."""
    try:
        query = "SELECT * FROM nft_blacklist"
        return _fetchall(query)
    except mysql.connector.Error as e:
        print(f"Error fetching all NFT data: {e}")
        return []
            
def get_pool_info_with_fallback(factory_contract, chain_name, chain_api, token0, token1, fee):
    """Get pool info from DB or fallback to contract/API, using context manager helpers."""
    t0, t1 = sorted([token0.lower(), token1.lower()])
    try:
        # 1. Check in DB
        select_query = """
            SELECT * FROM pool_info 
            WHERE token0_address = %s AND token1_address = %s AND fee = %s
        """
        result = _fetchone(select_query, (t0, t1, fee))
        if result:
            print("✅ Pool data found in DB")
            pool_address = Web3.to_checksum_address(result["pool_address"])
            return {
                "chain": result["chain"],
                "pool_address": pool_address,
                "token0_symbol": result["token0_symbol"],
                "token1_symbol": result["token1_symbol"],
                "token0_decimals": result["token0_decimals"],
                "token1_decimals": result["token1_decimals"],
                "fee": result["fee"],
                "alloc_point": result.get("alloc_point", 0),
                "source": "db"
            }
        # 2. Not found → call contract and API
        print("❌ Pool data not found in DB, calling contract and API")
        pool_address = factory_contract.functions.getPool(token0, token1, fee).call()
        pool_data = get_data_pool_bsc(chain_api, pool_address)
        token0_symbol = pool_data["token0"]["symbol"]
        token1_symbol = pool_data["token1"]["symbol"]
        token0_decimals = pool_data["token0"]["decimals"]
        token1_decimals = pool_data["token1"]["decimals"]
        alloc_point = 0
        # Insert into DB
        insert_query = """
            INSERT INTO pool_info (
                chain, pool_address, token0_address, token1_address,
                token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee, alloc_point
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        _execute(insert_query, (
            chain_name, pool_address, t0, t1,
            token0_symbol, token1_symbol,
            int(token0_decimals), int(token1_decimals),
            fee, alloc_point
        ))
        return {
            "chain": chain_name,
            "pool_address": pool_address,
            "token0_symbol": token0_symbol, 
            "token1_symbol": token1_symbol,
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "fee": fee,
            'alloc_point': alloc_point,
            "source": "api"
        }
    except Exception as e:
        print(f"❌ Error in get_pool_info_with_fallback: {e}")
        return None

def get_total_alloc_point_each_chain(chain):
    """Get total allocation point for each chain using context manager helpers."""
    try:
        query = """
            SELECT SUM(alloc_point) AS total_alloc_point
            FROM pool_info
            WHERE chain = %s
        """
        result = _fetchone(query, (chain,))
        if result and result['total_alloc_point'] is not None:
            return result['total_alloc_point']
        else:
            return 0
    except mysql.connector.Error as e:
        print(f"Error fetching total alloc point for chain {chain}: {e}")
        return 0
            
def get_nft_status_data(wallet_address, chain_name):
    """Get NFT status data for a wallet and chain using context manager helpers."""
    try:
        # 1. NFT Active + Inactive (latest)
        query_active_inactive = """
            SELECT t1.nft_id, t1.status
            FROM wallet_nft_position t1
            JOIN (
                SELECT nft_id, MAX(created_at) AS latest
                FROM wallet_nft_position
                WHERE wallet_address = %s AND chain = %s
                GROUP BY nft_id
            ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.latest
            WHERE t1.status IN ('Active', 'Inactive')
        """
        # 2. Closed from cache
        query_closed_cache = """
            SELECT nft_id
            FROM nft_closed_cache
            WHERE wallet_address = %s AND chain_name = %s AND status = 'Closed';
        """
        # 3. Blacklist
        query_blacklist = """
            SELECT nft_id 
            FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s
        """
        active_inactive_rows = _fetchall(query_active_inactive, (wallet_address, chain_name))
        closed_rows = _fetchall(query_closed_cache, (wallet_address, chain_name))
        blacklist_rows = _fetchall(query_blacklist, (wallet_address, chain_name))
        active_inactive_map = {row['nft_id']: row['status'] for row in active_inactive_rows}
        closed_ids = [row['nft_id'] for row in closed_rows]
        blacklist_ids = [row['nft_id'] for row in blacklist_rows]
        return {
            "active_inactive_map": active_inactive_map,  # {nft_id: status}
            "closed_ids": closed_ids,  # list
            "blacklist_ids": blacklist_ids,  # list
        }
    except mysql.connector.Error as e:
        print(f"Error fetching NFT status data: {e}")
        return None