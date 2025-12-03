import mysql.connector
from flask import jsonify
from datetime import datetime, timedelta
from services.pancake_api import get_data_pool_bsc
from services.helper import to_datetime_safe
from web3 import Web3
from services.db_connect import get_connection, get_db_config
from config import RPC_BACKUP_LIST

DB_CONFIG = get_db_config()

def fetch_latest_nft_id(status):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.*,
                CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted,
                COALESCE(p_sol.total_valid_liquidity) AS total_valid_liquidity,
                COALESCE(p_sol.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity_sol,
                COALESCE(p_evm.total_value_lock) AS total_value_lock,
                COALESCE(p_evm.total_staked_liquidity) AS total_staked_liquidity,
                COALESCE(p_evm.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity,
                COALESCE(p_evm.alloc_point, 0) AS alloc_point_evm,
                COALESCE(p_evm.fee, 0) AS fee_evm,
                COALESCE(p_evm.cake_per_day, 0) AS cake_per_day_evm,
                COALESCE(p_sol.fee, 0) AS fee_sol,
                COALESCE(p_sol.weekly_rewards, 0) AS weekly_rewards,
                COALESCE(p_sol.token0_decimals, 0) AS token0_decimals_sol,
                COALESCE(p_sol.token1_decimals, 0) AS token1_decimals_sol,
                COALESCE(p_evm.token0_decimals, 0) AS token0_decimals_evm,
                COALESCE(p_evm.token1_decimals, 0) AS token1_decimals_evm
            FROM wallet_nft_position t1
            INNER JOIN (
                SELECT nft_id, MAX(created_at) AS max_created_at
                FROM wallet_nft_position
                GROUP BY nft_id
            ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
            LEFT JOIN nft_blacklist b 
                ON t1.wallet_address = b.wallet_address AND t1.chain = b.chain AND t1.nft_id = b.nft_id
            LEFT JOIN pool_sol_info p_sol
                ON t1.chain = 'SOL' AND t1.pool_address = p_sol.pool_account
            LEFT JOIN pool_info p_evm
                ON t1.chain != 'SOL' AND t1.pool_address = p_evm.pool_address AND t1.chain = p_evm.chain
            WHERE b.id IS NULL {status_condition}
            ORDER BY t1.created_at DESC;
        """
        
        if status is None:
            query = query.format(status_condition="")
            cursor.execute(query)
        else:
            query = query.format(status_condition="AND t1.status != %s")
            cursor.execute(query, (status,))
        
        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching full NFT data: {e}")
        return []

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def fetch_latest_nft_by_wallet(wallet_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.*,
                CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted,
                COALESCE(p_sol.total_valid_liquidity) AS total_valid_liquidity,
                COALESCE(p_sol.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity_sol,
                COALESCE(p_evm.total_value_lock) AS total_value_lock,
                COALESCE(p_evm.total_staked_liquidity) AS total_staked_liquidity,
                COALESCE(p_evm.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity,
                COALESCE(p_evm.alloc_point, 0) AS alloc_point_evm,
                COALESCE(p_evm.fee, 0) AS fee_evm,
                COALESCE(p_evm.cake_per_day, 0) AS cake_per_day_evm,
                COALESCE(p_sol.fee, 0) AS fee_sol,
                COALESCE(p_sol.weekly_rewards, 0) AS weekly_rewards,
                COALESCE(p_sol.token0_decimals, 0) AS token0_decimals_sol,
                COALESCE(p_sol.token1_decimals, 0) AS token1_decimals_sol,
                COALESCE(p_evm.token0_decimals, 0) AS token0_decimals_evm,
                COALESCE(p_evm.token1_decimals, 0) AS token1_decimals_evm
            FROM wallet_nft_position t1
            INNER JOIN (
                SELECT nft_id, MAX(created_at) AS max_created_at
                FROM wallet_nft_position
                GROUP BY nft_id
            ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
            LEFT JOIN nft_blacklist b 
                ON t1.wallet_address = b.wallet_address AND t1.chain = b.chain AND t1.nft_id = b.nft_id
            LEFT JOIN pool_sol_info p_sol
                ON t1.chain = 'SOL' AND t1.pool_address = p_sol.pool_account
            LEFT JOIN pool_info p_evm
                ON t1.chain != 'SOL' AND t1.pool_address = p_evm.pool_address AND t1.chain = p_evm.chain
            WHERE LOWER(t1.wallet_address) = LOWER(%s)
              AND t1.status != 'Burned'
              AND b.id IS NULL
            ORDER BY t1.created_at DESC
        """

        cursor.execute(query, (wallet_address,))
        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching latest NFT data by wallet: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def fetch_latest_nft_by_wallet_and_chain(wallet_address, chain):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.*,
                CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted,
                COALESCE(p_sol.total_valid_liquidity) AS total_valid_liquidity,
                COALESCE(p_sol.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity_sol,
                COALESCE(p_evm.total_value_lock) AS total_value_lock,
                COALESCE(p_evm.total_staked_liquidity) AS total_staked_liquidity,
                COALESCE(p_evm.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity,
                COALESCE(p_evm.alloc_point, 0) AS alloc_point_evm,
                COALESCE(p_evm.fee, 0) AS fee_evm,
                COALESCE(p_evm.cake_per_day, 0) AS cake_per_day_evm,
                COALESCE(p_sol.fee, 0) AS fee_sol,
                COALESCE(p_sol.weekly_rewards, 0) AS weekly_rewards,
                COALESCE(p_sol.token0_decimals, 0) AS token0_decimals_sol,
                COALESCE(p_sol.token1_decimals, 0) AS token1_decimals_sol,
                COALESCE(p_evm.token0_decimals, 0) AS token0_decimals_evm,
                COALESCE(p_evm.token1_decimals, 0) AS token1_decimals_evm
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
            LEFT JOIN pool_sol_info p_sol
                ON t1.chain = 'SOL' AND t1.pool_address = p_sol.pool_account
            LEFT JOIN pool_info p_evm
                ON t1.chain != 'SOL' AND t1.pool_address = p_evm.pool_address AND t1.chain = p_evm.chain
            WHERE t1.chain = %s 
              AND LOWER(t1.wallet_address) = LOWER(%s) 
              AND t1.status != 'Burned'
              AND b.id IS NULL
            ORDER BY t1.created_at DESC
        """

        cursor.execute(query, (chain, wallet_address))
        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching latest NFT data by wallet and chain: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# def fetch_nft_by_token(token):
#     try:
#         conn = get_connection()
#         cursor = conn.cursor(dictionary=True)

#         query = """
#             SELECT t1.*, 
#                    CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS is_blacklisted
#             FROM wallet_nft_position t1
#             INNER JOIN (
#                 SELECT nft_id, MAX(created_at) AS max_created_at
#                 FROM wallet_nft_position
#                 GROUP BY nft_id
#             ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
#             LEFT JOIN nft_blacklist b 
#                    ON t1.wallet_address = b.wallet_address 
#                    AND t1.chain = b.chain 
#                    AND t1.nft_id = b.nft_id
#             LEFT JOIN pool_info p
#                    ON t1.chain = p.chain AND (
#                         (t1.token0_symbol = p.token0_symbol AND t1.token1_symbol = p.token1_symbol)
#                      OR (t1.token0_symbol = p.token1_symbol AND t1.token1_symbol = p.token0_symbol)
#                    )
#             WHERE t1.status != 'Closed'
#               AND b.id IS NULL
#         """

#         params = []

#         if token:
#             query += """
#                 AND (
#                     LOWER(p.token0_symbol) LIKE %s OR LOWER(p.token1_symbol) LIKE %s OR
#                     LOWER(p.token0_address) = %s OR LOWER(p.token1_address) = %s OR
#                     LOWER(p.pool_address) = %s
#                 )
#             """
#             token_like = f"%{token.lower()}%"
#             token_exact = token.lower()
#             params.extend([token_like, token_like, token_exact, token_exact, token_exact])

#         query += " ORDER BY t1.created_at DESC"

#         cursor.execute(query, params)
#         return cursor.fetchall()

#     except mysql.connector.Error as e:
#         print(f"[DB Error] filter_nft_by_token_only: {e}")
#         return []

#     finally:
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals():
#             conn.close()

def filter_by_token(nfts, token):
    if not token:
        return nfts

    token = str(token).lower()
    filtered = []

    for nft in nfts:
        token0_symbol = str(nft.get('token0_symbol') or '').lower()
        token1_symbol = str(nft.get('token1_symbol') or '').lower()
        token0_address = str(nft.get('token0_address') or '').lower()
        token1_address = str(nft.get('token1_address') or '').lower()
        pool_address = str(nft.get('pool_address') or '').lower()

        if (
            token in token0_symbol
            or token in token1_symbol
            or token == token0_address
            or token == token1_address
            or token == pool_address
        ):
            filtered.append(nft)

    return filtered

def enrich_with_pool_info(nfts):
    """
    Tối ưu: query tất cả pool_info chỉ 1 lần thay vì loop query từng NFT.
    """
    if not nfts:
        return nfts

    # Chuẩn bị tập hợp các cặp token để query
    token_pairs = set()
    for nft in nfts:
        chain = nft.get('chain') or ''
        t0 = nft.get('token0_symbol') or ''
        t1 = nft.get('token1_symbol') or ''
        token_pairs.add((chain, t0, t1))
        token_pairs.add((chain, t1, t0))  # để cover swap order

    # Tạo list params và placeholders
    params = []
    placeholders = []
    for chain, t0, t1 in token_pairs:
        placeholders.append("(chain=%s AND token0_symbol=%s AND token1_symbol=%s)")
        params.extend([chain, t0, t1])

    query = f"""
        SELECT *
        FROM pool_info
        WHERE {" OR ".join(placeholders)}
    """

    # Query 1 lần
    conn = get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        pool_rows = cursor.fetchall()

        # Map key = (chain, token0_symbol, token1_symbol)
        pool_map = {}
        for pool in pool_rows:
            key1 = (pool['chain'], pool['token0_symbol'], pool['token1_symbol'])
            key2 = (pool['chain'], pool['token1_symbol'], pool['token0_symbol'])
            pool_map[key1] = pool
            pool_map[key2] = pool  # để cover swap order

        # Update nfts
        for nft in nfts:
            chain = nft.get('chain') or ''
            t0 = nft.get('token0_symbol') or ''
            t1 = nft.get('token1_symbol') or ''
            pool = pool_map.get((chain, t0, t1))
            if pool:
                nft.update({
                    'pool_address': pool['pool_address'],
                    'token0_address': pool['token0_address'],
                    'token1_address': pool['token1_address'],
                    'fee': pool.get('fee'),
                    'pool_info': pool
                })

        return nfts
    finally:
        cursor.close()
        conn.close()

def fetch_latest_summary_by_token(wallet_address, include_closed=True, start_date=None, end_date=None):
    # chuẩn hóa ngày
    start_date = to_datetime_safe(start_date)
    end_date = to_datetime_safe(end_date)
    if end_date:
        end_date += timedelta(days=1)  # make end_date inclusive

    # Lấy raw latest NFT per nft_id (function bạn đã có)
    nfts = fetch_latest_nft_by_wallet(wallet_address) or []

    # Lọc theo ngày (nếu user truyền)
    if start_date or end_date:
        filtered = []
        for nft in nfts:
            ca = nft.get("created_at")
            if ca is None:
                # Nếu có filter ngày mà record không có created_at -> bỏ
                continue
            ca_dt = to_datetime_safe(ca)
            if start_date and ca_dt < start_date:
                continue
            if end_date and ca_dt >= end_date:
                continue
            filtered.append(nft)
        nfts = filtered

    summary = {}
    total_reward = 0.0

    for nft in nfts:
        # bỏ closed nếu người gọi yêu cầu
        if not include_closed and nft.get("status") == "Closed":
            continue

        # token0
        token0 = nft.get("token0_symbol")
        if token0:
            initial = float(nft.get("initial_token0_amount") or 0)
            current = float(nft.get("current_token0_amount") or 0)
            delta = current - initial
            price0 = float(nft.get("price_token0") or 0)
            delta_usd = delta * price0
            fee = float(nft.get("unclaimed_fee_token0") or 0.0)

            s = summary.setdefault(token0, {"token": token0, "initial": 0.0, "current": 0.0, "delta": 0.0, "delta_usd": 0.0, "fee": 0.0})
            s["initial"] += initial
            s["current"] += current
            s["delta"] += delta
            s["delta_usd"] += delta_usd
            s["fee"] += fee

        # token1
        token1 = nft.get("token1_symbol")
        if token1:
            initial = float(nft.get("initial_token1_amount") or 0)
            current = float(nft.get("current_token1_amount") or 0)
            delta = current - initial
            price1 = float(nft.get("price_token1") or 0)
            delta_usd = delta * price1
            fee = float(nft.get("unclaimed_fee_token1") or 0)

            s = summary.setdefault(token1, {"token": token1, "initial": 0.0, "current": 0.0, "delta": 0.0, "delta_usd": 0.0, "fee": 0.0})
            s["initial"] += initial
            s["current"] += current
            s["delta"] += delta
            s["delta_usd"] += delta_usd
            s["fee"] += fee

        # reward (tổng ví)
        total_reward += float(nft.get("pending_cake") or 0)

    # Gắn reward tổng vào từng token (hoặc bạn có thể trả riêng)
    results = []
    for token, data in summary.items():
        data["reward"] = total_reward
        results.append(data)

    return results

def fetch_latest_summary_by_wallet_and_chain(wallet_address, chain, include_closed=True, start_date=None, end_date=None):
    # Lấy raw NFT mới nhất
    nfts = fetch_latest_nft_by_wallet_and_chain(wallet_address, chain)

    # Filter theo date
    if start_date:
        start_date = to_datetime_safe(start_date)
        nfts = [n for n in nfts if n.get("created_at") and n["created_at"] >= start_date]
    if end_date:
        end_date = to_datetime_safe(end_date) + timedelta(days=1)
        nfts = [n for n in nfts if n.get("created_at") and n["created_at"] < end_date]

    # Loại Closed nếu cần
    if not include_closed:
        nfts = [n for n in nfts if n.get("status") != "Closed"]

    summary = {}
    total_reward = 0

    for nft in nfts:
        # token0
        token0 = nft.get("token0_symbol")
        if token0:
            initial = nft.get("initial_token0_amount", 0) or 0
            current = nft.get("current_token0_amount", 0) or 0
            delta = current - initial
            delta_usd = delta * (nft.get("price_token0", 0) or 0)
            fee = float(nft.get("unclaimed_fee_token0") or 0)

            s = summary.setdefault(token0, {"token": token0, "initial": 0, "current": 0, "delta": 0, "delta_usd": 0, "fee": 0})
            s["initial"] += initial
            s["current"] += current
            s["delta"] += delta
            s["delta_usd"] += delta_usd
            s["fee"] += fee

        # token1
        token1 = nft.get("token1_symbol")
        if token1:
            initial = nft.get("initial_token1_amount", 0) or 0
            current = nft.get("current_token1_amount", 0) or 0
            delta = current - initial
            delta_usd = delta * (nft.get("price_token1", 0) or 0)
            fee = float(nft.get("unclaimed_fee_token1") or 0)

            s = summary.setdefault(token1, {"token": token1, "initial": 0, "current": 0, "delta": 0, "delta_usd": 0, "fee": 0})
            s["initial"] += initial
            s["current"] += current
            s["delta"] += delta
            s["delta_usd"] += delta_usd
            s["fee"] += fee

        # reward
        total_reward += nft.get("pending_cake", 0) or 0

    # Gắn reward vào từng token
    results = []
    for token, data in summary.items():
        data["reward"] = total_reward
        results.append(data)

    return results

def get_futures_positions_binance_data_by_wallet(waller_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT p1.*
            FROM futures_positions_binance p1
            INNER JOIN (
                SELECT wallet_id, MAX(created_at) AS max_created_at
                FROM futures_positions_binance
                GROUP BY wallet_id
            ) p2 ON p1.wallet_id = p2.wallet_id AND p1.created_at = p2.max_created_at
            WHERE p1.wallet_id = %s
        """
        cursor.execute(query, (waller_address,))

        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching all binance data: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_futures_orders_binance_data_by_wallet(waller_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT o1.* 
            FROM futures_orders_binance o1
             INNER JOIN (
                SELECT wallet_id, MAX(created_at) AS max_created_at 
                FROM futures_positions_binance
                GROUP BY wallet_id
            ) o2 ON o1.wallet_id = o2.wallet_id AND o1.created_at = o2.max_created_at
            WHERE 01.wallet_id = %s
        """
        cursor.execute(query, (waller_address,))

        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching all binance data: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_latest_total_pending_cake_by_wallet(wallet_address, start_date=None, end_date=None):
    """
    Tổng pending_cake từ tất cả NFT mới nhất trong ví (không Burned, không blacklist).
    Lọc theo start_date / end_date nếu có.
    """
    nfts = fetch_latest_nft_by_wallet(wallet_address)
    total_reward = 0

    for nft in nfts:
        created_at = nft.get("created_at")

        # parse created_at nếu là string
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except Exception:
                created_at = None

        # filter theo date
        if start_date and created_at and created_at < start_date:
            continue
        if end_date and created_at and created_at >= (end_date + timedelta(days=1)):
            continue

        total_reward += nft.get("pending_cake", 0) or 0

    return total_reward

def get_latest_total_pending_cake_by_wallet_and_chain(wallet_address, chain, start_date=None, end_date=None):
    """
    Tổng pending_cake từ tất cả NFT mới nhất trong ví theo chain (không Burned, không blacklist).
    Lọc theo start_date / end_date nếu có.
    """
    nfts = fetch_latest_nft_by_wallet_and_chain(wallet_address, chain)
    total_reward = 0

    for nft in nfts:
        created_at = nft.get("created_at")

        # parse created_at nếu là string
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except Exception:
                created_at = None

        # filter theo date
        if start_date and created_at and created_at < start_date:
            continue
        if end_date and created_at and created_at >= (end_date + timedelta(days=1)):
            continue

        total_reward += nft.get("pending_cake", 0) or 0

    return total_reward

def fetch_nft_history_by_id(nft_id, limit=30, offset=0):
    try:
        # Validate parameters
        nft_id = nft_id.strip()
        limit = int(limit)
        offset = int(offset)
        if limit < 0 or offset < 0:
            raise ValueError("Parameters must be non-negative integers.")

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.*,
                COALESCE(p_sol.total_valid_liquidity) AS total_valid_liquidity,
                COALESCE(p_evm.alloc_point, 0) AS alloc_point_evm,
                COALESCE(p_evm.fee, 0) AS fee_evm,
                COALESCE(p_evm.cake_per_day, 0) AS cake_per_day_evm,
                COALESCE(p_sol.fee, 0) AS fee_sol,
                COALESCE(p_sol.weekly_rewards, 0) AS weekly_rewards,
                COALESCE(p_sol.total_valid_liquidity) AS total_valid_liquidity,
                COALESCE(p_sol.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity_sol,
                COALESCE(p_evm.total_value_lock) AS total_value_lock,
                COALESCE(p_evm.total_staked_liquidity) AS total_staked_liquidity,
                COALESCE(p_evm.total_inactive_staked_liquidity) AS total_inactive_staked_liquidity,
                COALESCE(p_sol.token0_decimals, 0) AS token0_decimals_sol,
                COALESCE(p_sol.token1_decimals, 0) AS token1_decimals_sol,
                COALESCE(p_evm.token0_decimals, 0) AS token0_decimals_evm,
                COALESCE(p_evm.token1_decimals, 0) AS token1_decimals_evm
            FROM wallet_nft_position AS t1
            LEFT JOIN pool_sol_info AS p_sol
                ON t1.chain = 'SOL' AND t1.pool_address = p_sol.pool_account
            LEFT JOIN pool_info p_evm
                ON t1.chain != 'SOL' AND t1.pool_address = p_evm.pool_address AND t1.chain = p_evm.chain
            WHERE t1.nft_id = %s
            ORDER BY t1.created_at DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (nft_id, limit, offset))
        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching history for NFT ID {nft_id}: {e}")
        return []

    except ValueError as ve:
        print(f"Invalid parameter: {ve}")
        return []

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def count_nft_history_records_by_id(nft_id):
    try:
        nft_id = nft_id.strip()
        if not nft_id:
            raise ValueError("NFT ID must be a non-empty string.")

        conn = get_connection()
        cursor = conn.cursor()

        query = "SELECT COUNT(*) FROM wallet_nft_position WHERE nft_id = %s"
        cursor.execute(query, (nft_id,))
        count = cursor.fetchone()[0]
        return count

    except mysql.connector.Error as e:
        print(f"Error counting history records for NFT ID {nft_id}: {e}")
        return 0

    except ValueError as ve:
        print(f"Invalid parameter: {ve}")
        return 0

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def toggle_blacklist(wallet_address, chain, nft_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 1 FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s AND nft_id = %s
        """
        
        cursor.execute(query, (wallet_address, chain, nft_id))
        exists = cursor.fetchone()
        
        if exists:
            query = """
                DELETE FROM nft_blacklist 
                WHERE wallet_address = %s AND chain = %s AND nft_id = %s
            """
            cursor.execute(query, (wallet_address, chain, nft_id))
            message = 'Removed NFT ID: ' + nft_id + ' from Blacklist'
        else:
            query = """
                INSERT INTO nft_blacklist (wallet_address, chain, nft_id)
                VALUES (%s, %s, %s)
            """
            cursor.execute(query, (wallet_address, chain, nft_id))
            message = f'Added NFT ID: {nft_id} to Blacklist'
            
        conn.commit()
        return {'status': 'success', 'message': message}  # ✅ Trả về dict

    except mysql.connector.Error as e:
        print(f"Error toggling blacklist: {e}")
        return {'status': 'error', 'message': 'Failed to toggle blacklist'}  # ✅ dict thuần

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def get_blacklist_nft_ids(wallet_address, chain_name):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT nft_id 
            FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s
        """
        
        cursor.execute(query, (wallet_address, chain_name))
        results = cursor.fetchall()
        
        return [row[0] for row in results] if results else []

    except mysql.connector.Error as e:
        print(f"Error fetching blacklist NFT IDs: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def fetch_blacklist_nft_ids():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT * FROM nft_blacklist
        """
        cursor.execute(query, )

        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching all NFT data: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def get_pool_info_with_fallback(factory_contract, chain_name, chain_api, token0, token1, fee, rpc_list=None):
    t0, t1 = sorted([token0.lower(), token1.lower()])
    
    if rpc_list is None:
        rpc_list = RPC_BACKUP_LIST.get(chain_name, [])

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ 1. Check trong DB
        cursor.execute("""
            SELECT * FROM pool_info 
            WHERE token0_address = %s AND token1_address = %s AND fee = %s
        """, (t0, t1, fee))
        result = cursor.fetchone()
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

        # ❌ 2. Nếu chưa có → thử gọi contract với RPC chính trước
        print("❌ Pool data not found in DB, calling contract and API")

        pool_address = None
        try:
            pool_address = factory_contract.functions.getPool(token0, token1, fee).call()
        except Exception as e:
            print(f"⚠️ Primary RPC failed getPool({token0}, {token1}, {fee}): {e}")

        # Nếu RPC chính fail → thử fallback RPC
        if not pool_address:
            for rpc in rpc_list:
                try:
                    w3_backup = Web3(Web3.HTTPProvider(rpc))
                    factory_backup = w3_backup.eth.contract(address=factory_contract.address, abi=factory_contract.abi)
                    pool_address = factory_backup.functions.getPool(token0, token1, fee).call()
                    print(f"✅ Success with backup RPC {rpc}")
                    break
                except Exception as e:
                    print(f"⚠️ Retry getPool failed with {rpc}: {e}")
                    continue

        if not pool_address:
            print(f"❌ All RPC failed for getPool({token0}, {token1}, {fee})")
            return None

        # Lấy thêm data từ API
        pool_data = get_data_pool_bsc(chain_api, pool_address)
        token0_symbol = pool_data["token0"]["symbol"]
        token1_symbol = pool_data["token1"]["symbol"]
        token0_decimals = pool_data["token0"]["decimals"]
        token1_decimals = pool_data["token1"]["decimals"]
        alloc_point = 0

        # ✅ Insert vào DB
        insert_query = """
            INSERT INTO pool_info (
                chain, pool_address, token0_address, token1_address,
                token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee, alloc_point
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            chain_name, pool_address, t0, t1,
            token0_symbol, token1_symbol,
            int(token0_decimals), int(token1_decimals),
            fee, alloc_point
        ))
        conn.commit()

        return {
            "chain": chain_name,
            "pool_address": pool_address,
            "token0_symbol": token0_symbol, 
            "token1_symbol": token1_symbol,
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "fee": fee,
            "alloc_point": alloc_point,
            "source": "api"
        }

    except Exception as e:
        print(f"❌ Error in get_pool_info_with_fallback: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_total_alloc_point_each_chain(chain):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT SUM(alloc_point) AS total_alloc_point
            FROM pool_info
            WHERE chain = %s
        """
        cursor.execute(query, (chain,))
        result = cursor.fetchone()

        if result and result['total_alloc_point'] is not None:
            return result['total_alloc_point']
        else:
            return 0

    except mysql.connector.Error as e:
        print(f"Error fetching total alloc point for chain {chain}: {e}")
        return 0
            
def get_nft_status_data(wallet_address, chain_name):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

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

        # 2. Closed từ cache
        query_closed_cache = """
            SELECT nft_id
            FROM nft_closed_cache
            WHERE wallet_address = %s AND chain_name = %s AND status = 'Burned';
        """

        # 3. Blacklist
        query_blacklist = """
            SELECT nft_id 
            FROM nft_blacklist
            WHERE wallet_address = %s AND chain = %s
        """

        # Execute all 3
        cursor.execute(query_active_inactive, (wallet_address, chain_name))
        active_inactive_rows = cursor.fetchall()

        cursor.execute(query_closed_cache, (wallet_address, chain_name))
        closed_rows = cursor.fetchall()

        cursor.execute(query_blacklist, (wallet_address, chain_name))
        blacklist_rows = cursor.fetchall()

        # Process
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

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Fetch all pool info from the database
def fetch_all_pool_info(chain_name=None):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        if chain_name is not None:
            query = """
                SELECT * FROM pool_info WHERE chain = %s AND alloc_point > 0
                ORDER BY chain, pid DESC
            """
            cursor.execute(query, (chain_name,))
            results = cursor.fetchall()
            return results
        else:
            query = """
                SELECT * FROM pool_info WHERE alloc_point > 0
                ORDER BY chain, pid DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()

            return results

    except mysql.connector.Error as e:
        print(f"Error fetching all pool info: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Fetch all pool sol info from the database
def fetch_all_pool_sol_info(chain_name=None):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        if chain_name is not None:
            query = """
                SELECT * FROM pool_sol_info
                ORDER BY reward_state DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        else:
            query = """
                SELECT * FROM pool_sol_info
                ORDER BY reward_state DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()

            return results

    except mysql.connector.Error as e:
        print(f"Error fetching all pool info: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_pool_sol_info(pool_account):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT *
            FROM pool_sol_info
            WHERE pool_account = %s
        """
        cursor.execute(query, (pool_account,))
        result = cursor.fetchone()
        return result  # dict hoặc None nếu không có

    except mysql.connector.Error as e:
        print(f"Error fetching pool info: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_total_cake_per_day_each_chain():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT chain, SUM(cake_per_day) AS total_cake_per_day
            FROM pool_info
            GROUP BY chain
        """
        cursor.execute(query)
        results = cursor.fetchall()
        return results

    except mysql.connector.Error as e:
        print(f"Error fetching pool info: {e}")
        return None

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_total_cake_per_day_on_chain(chain):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT SUM(cake_per_day) AS total_cake_per_day
            FROM pool_info
            WHERE chain = %s
        """
        cursor.execute(query, (chain,))
        result = cursor.fetchone()
        
        return result["total_cake_per_day"] if result and result["total_cake_per_day"] else 0

    except mysql.connector.Error as e:
        print(f"Error fetching pool info: {e}")
        return None

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_total_weekly_rewards_sol():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT SUM(weekly_rewards) AS total_weekly_rewards
            FROM pool_sol_info
        """
        cursor.execute(query)
        result = cursor.fetchone()  # chỉ 1 row
        return result["total_weekly_rewards"] if result else 0

    except mysql.connector.Error as e:
        print(f"Error fetching pool info: {e}")
        return 0

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_weekly_reward_per_pool(chain, pool_account):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT weekly_rewards
            FROM pool_sol_info
            WHERE chain = %s AND pool_account = %s
        """
        cursor.execute(query, (chain, pool_account))
        result = cursor.fetchone()
        if result:
            return result["weekly_rewards"]
        return None

    except mysql.connector.Error as e:
        print(f"Error fetching pool info: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def get_latest_nft_id_sol_from_db(chain: str, wallet_address: str):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.nft_id
            FROM wallet_nft_position t1
            INNER JOIN (
                SELECT nft_id, MAX(created_at) AS max_created_at
                FROM wallet_nft_position
                WHERE chain = %s
                GROUP BY nft_id
            ) t2 
                ON t1.nft_id = t2.nft_id 
                AND t1.created_at = t2.max_created_at
            LEFT JOIN nft_blacklist b 
                ON t1.wallet_address = b.wallet_address 
                AND t1.chain = b.chain 
                AND t1.nft_id = b.nft_id
            WHERE t1.chain = %s 
              AND t1.wallet_address = %s
              AND b.nft_id IS NULL
        """
        cursor.execute(query, (chain, chain, wallet_address))
        results = cursor.fetchall()
        
        return [row["nft_id"] for row in results]

    except mysql.connector.Error as e:
        print(f"❌ Error fetching NFT IDs: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def get_all_burned_nfts_sol(wallet_address, chain_name):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT nft_id
            FROM nft_closed_cache
            WHERE chain_name = %s AND wallet_address = %s AND status = 'Burned'
        """
        cursor.execute(query, (chain_name, wallet_address))
        results = cursor.fetchall()
        
        burned_nft_ids = [row["nft_id"] for row in results]
        return burned_nft_ids

    except mysql.connector.Error as e:
        print(f"❌ Error fetching burned NFT data: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_latest_closed_nft_ids(wallet_address: str, chain: str):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT t1.nft_id
            FROM wallet_nft_position t1
            INNER JOIN (
                SELECT nft_id, MAX(created_at) AS max_created_at
                FROM wallet_nft_position
                WHERE chain = %s AND status = 'Closed'
                GROUP BY nft_id
            ) t2
                ON t1.nft_id = t2.nft_id
                AND t1.created_at = t2.max_created_at
            LEFT JOIN nft_blacklist b
                ON t1.wallet_address = b.wallet_address
                AND t1.chain = b.chain
                AND t1.nft_id = b.nft_id
            WHERE t1.chain = %s
              AND t1.wallet_address = %s
              AND t1.status = 'Closed'
              AND b.nft_id IS NULL
        """
        cursor.execute(query, (chain, chain, wallet_address))
        results = cursor.fetchall()

        return [row["nft_id"] for row in results]

    except mysql.connector.Error as e:
        print(f"❌ Error fetching Closed NFT IDs: {e}")
        return []

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_nft_initial_amount_from_db(nft_id, chain, wallet_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT initial_token0_amount, initial_token1_amount
            FROM wallet_nft_position
            WHERE nft_id = %s
              AND chain = %s
              AND wallet_address = %s
              AND initial_token0_amount > 0
              AND initial_token1_amount > 0
            ORDER BY created_at DESC
            LIMIT 1
        """
        cursor.execute(query, (nft_id, chain, wallet_address))
        result = cursor.fetchone()

        if result:
            return (
                result["initial_token0_amount"],
                result["initial_token1_amount"]
            )
        return None

    except mysql.connector.Error as e:
        print(f"❌ Error fetching initial amount for NFT {nft_id}: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def toggle_stake_track_api(chain, pool_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT is_stake_tracked 
            FROM pool_info 
            WHERE chain = %s AND pool_address = %s
        """, (chain, pool_address))
        pool = cursor.fetchone()

        if not pool:
            return jsonify({'success': False, 'message': 'Pool not found'}), 404

        new_state = not bool(pool['is_stake_tracked'])

        cursor.execute("""
            UPDATE pool_info 
            SET is_stake_tracked = %s
            WHERE chain = %s AND pool_address = %s
        """, (new_state, chain, pool_address))
        conn.commit()

        print(f"✅ Updated is_stake_tracked for {chain}:{pool_address} → {new_state}")

        return jsonify({'success': True, 'is_stake_tracked': new_state})
    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass