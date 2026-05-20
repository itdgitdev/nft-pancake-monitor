import json
import requests
from web3 import Web3
from config import MASTERCHEF_V3_ADDRESSES, RPC_URLS, CHAIN_SCAN_URLS, PANCAKE_CHAIN_MAP, CHAIN_ID_MAP, BLACKLIST_POOLS

import time
from datetime import datetime
from create_db import get_connection, create_database_and_table
from helper import get_list_pool_actived_farm_by_api_pancake, notify_discord

ABI_FILE = '/home/dev/nft_pancake_app/latest_farms/abi_config.json'
# ABI_FILE = 'abi_config.json'
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1386555618751549520/i6GTfThX2VckPF4isp9ktn7ds1B0Ik7YWGGPR016nCO79uPIqm4ukYXPK-PR21_YvYyT"

ERC20_ABI = [
    {
        "name": "decimals", 
        "outputs": [{"type": "uint8"}], 
        "inputs": [], 
        "stateMutability": "view", 
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]

V3POOL_ABI = [
    {
        "name": "slot0",
        "outputs": [
            {"type": "uint160", "name": "sqrtPriceX96"},
            {"type": "int24", "name": "tick"},
            {"type": "uint16", "name": "observationIndex"},
            {"type": "uint16", "name": "observationCardinality"},
            {"type": "uint16", "name": "observationCardinalityNext"},
            {"type": "uint32", "name": "feeProtocol"}, 
            {"type": "bool", "name": "unlocked"}
        ],
        "inputs": [],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "name": "liquidity",
        "outputs": [{"type": "uint128"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "name": "token0",
        "outputs": [{"type": "address"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "name": "token1",
        "outputs": [{"type": "address"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function"
    },
]

create_database_and_table()

def update_pool_info(chain_key, pool_address, token0, token1, token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee, alloc_point, pid):
    try:
        # Kết nối DB
        conn = get_connection()
        cursor = conn.cursor()

        sql = """
        INSERT INTO pool_info (
                    chain, pool_address,
                    token0_address, token1_address,
                    token0_symbol, token1_symbol,
                    token0_decimals, token1_decimals,
                    fee, alloc_point, pid
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            token0_symbol = VALUES(token0_symbol),
            token1_symbol = VALUES(token1_symbol),
            fee = VALUES(fee),
            alloc_point = VALUES(alloc_point)
        """

        values = (
            chain_key,
            pool_address,
            token0,
            token1,
            token0_symbol,
            token1_symbol,
            token0_decimals,
            token1_decimals,
            fee,
            alloc_point,
            pid
        )

        cursor.execute(sql, values)
        conn.commit()
        print(f"✅ Updated pool_info for {chain_key} PID {pid} ({token0_symbol}-{token1_symbol})")

    except Exception as e:
        print(f"❌ Error updating pool_info for {chain_key} PID {pid}: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_abi():
    with open(ABI_FILE, 'r') as f:
        return json.load(f)

def load_state(connection, limit_latest_pids=30):
    state = {}
    first_run = False

    with connection.cursor() as cursor:
        for chain_key in MASTERCHEF_V3_ADDRESSES.keys():
            cursor.execute("""
                           SELECT chain, pid, v3Pool, fee, alloc_point, timestamp
                           FROM farm_state
                           WHERE chain = %s
                           ORDER BY pid DESC
                               LIMIT %s
                           """, (chain_key, limit_latest_pids))

            rows = cursor.fetchall()

            if rows:
                state[chain_key] = {}
                for row in rows:
                    pid = str(row[1])
                    v3Pool = row[2]
                    fee = row[3]
                    alloc_point = row[4]
                    activated_at = row[5]

                    state[chain_key][pid] = {
                        "pid": int(pid),
                        "v3Pool": v3Pool,
                        "fee": fee,
                        "allocPoint": alloc_point,
                        "activated_at": int(activated_at.timestamp()) if activated_at else None
                    }
            else:
                first_run = True

    return state, first_run

def save_state(state, connection):
    with connection.cursor() as cursor:
        for chain, pools in state.items():
            for pid, pool_data in pools.items():
                alloc_point = pool_data['allocPoint']
                v3Pool = pool_data['v3Pool']
                fee = pool_data['fee']
                activated_at = datetime.now()

                cursor.execute(
                    "SELECT alloc_point, fee FROM farm_state WHERE chain = %s AND pid = %s",
                    (chain, pid)
                )
                result = cursor.fetchone()

                if result:
                    db_alloc_point, db_fee = result
                    if db_alloc_point != alloc_point:
                        cursor.execute(
                            """
                            UPDATE farm_state SET alloc_point = %s, timestamp = %s
                            WHERE chain = %s AND pid = %s
                            """,
                            (alloc_point, activated_at, chain, pid)
                        )
                else:
                    cursor.execute(
                        """
                        INSERT INTO farm_state (chain, pid, alloc_point, v3Pool, fee, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (chain, pid, alloc_point, v3Pool, fee, activated_at)
                    )
        connection.commit()

# def notify_discord(message):
#     data = {"content": message}
#     response = requests.post(DISCORD_WEBHOOK_URL, json=data)
#     if response.status_code != 204:
#         print("❌ Discord notify failed:", response.text)

def get_token_symbol(w3, token_address):    
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    
    symbol = "Unknown"  # default fallback
    decimals = 18  # default fallback
    
    try:
        symbol = token_contract.functions.symbol().call()
    except Exception as e:
        print(f"⚠️ Error getting symbol for token {token_address}: {e}")
    
    try:
        decimals = token_contract.functions.decimals().call()
    except Exception as e:
        print(f"⚠️ Error getting decimals for token {token_address}: {e}")
    
    return symbol, decimals

def check_chain(chain_key, abi, state, first_run):
    rpc_url = RPC_URLS.get(chain_key)
    contract_address = MASTERCHEF_V3_ADDRESSES.get(chain_key)
    chain_id = CHAIN_ID_MAP.get(chain_key)
    
    if not rpc_url or not contract_address:
        print(f"❌ Missing RPC or Address for {chain_key}")
        return state

    print(f"🔍 Checking {chain_key}...")

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            print(f"❌ Cannot connect to {chain_key}")
            return state
        
        contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)

        pool_length = contract.functions.poolLength().call()
        last_pool_data = state.get(chain_key, {})
        current_pool_data = {}

        MAX_CHECK = 30  # Chỉ kiểm tra 30 pool mới nhất mỗi lần
        blacklist = set(BLACKLIST_POOLS.get(chain_key, []))

        # === Lần chạy đầu tiên: lưu tất cả pool hiện tại nhưng không notify ===
        if first_run:
            print(f"⏳ First run for {chain_key}. Initializing state...")
            for pid in range(1, pool_length + 1):
                if pid in blacklist:
                    print(f"🚫 Skipping blacklisted pool {pid} on {chain_key}")
                    continue
                try:
                    pool_info = contract.functions.poolInfo(pid).call()
                    allocPoint = pool_info[0]
                    current_pool_data[str(pid)] = {
                        "pid": str(pid),
                        "v3Pool": pool_info[1],
                        "fee": pool_info[4],
                        "allocPoint": allocPoint,
                        "activated_at": int(time.time()) if allocPoint > 0 else None
                    }
                    time.sleep(0.25)
                except Exception as e:
                    print(f"⚠️ Error on pool {pid}: {e}")
            state[chain_key] = current_pool_data
            return state

        # === Xác định các pool cần kiểm tra ===
        max_known_pid = max(map(int, last_pool_data.keys()), default=-1)
        new_pids = list(range(max_known_pid + 1, pool_length + 1))

        # Pool allocPoint == 0 → cần kiểm tra lại (chỉ PID cao nhất)
        inactive_pids_all = sorted(
            [int(pid) for pid, data in last_pool_data.items()],
            reverse=True
        )
        inactive_pids = inactive_pids_all[:MAX_CHECK]

        # Gộp các PID cần kiểm tra
        pids_to_check = sorted(set(new_pids + inactive_pids), reverse=True)
        print(f"🧪 Checking {len(pids_to_check)} pool(s): {pids_to_check}")

        # Pool active farm from api pancake
        active_pools = get_list_pool_actived_farm_by_api_pancake(chain_key)
        
        for pid in pids_to_check:
            if pid in blacklist:
                print(f"🚫 Skipping blacklisted pool {pid} on {chain_key}")
                continue

            time.sleep(0.25)
            pid_str = str(pid)
            try:
                pool_info = contract.functions.poolInfo(pid).call()
                allocPoint, v3Pool, token0, token1, fee = pool_info[0:5]
            except Exception as e:
                print(f"⚠️ Error fetching poolInfo({pid}) on {chain_key}: {e}")
                continue
            
            symbol0, decimals0 = get_token_symbol(w3, token0)
            symbol1, decimals1 = get_token_symbol(w3, token1)
            
            prev_entry = last_pool_data.get(pid_str)
            prev_alloc = prev_entry["allocPoint"] if prev_entry else None
            activated_at = prev_entry["activated_at"] if prev_entry else None

            use_api_filter = bool(active_pools)
            if use_api_filter and v3Pool.lower() not in active_pools.values():
                print(f"🚫 Skipping inactive pool {pid} ({v3Pool}) on {chain_key} per Pancake API")
                continue
            
            current_pool_data[pid_str] = {
                "allocPoint": allocPoint,
                "pid": pid_str,
                "v3Pool": v3Pool,
                "fee": fee,
                "activated_at": activated_at
            }
            
            print(f"ℹ️ Pool from pancake api: {v3Pool} - on chain {chain_key} - pid {pid} - allocPoint {allocPoint}.")
            print(f"prev_alloc: {prev_alloc}, allocPoint: {allocPoint}, activated_at: {activated_at}")
                
            is_new = prev_alloc is None
            is_activated = prev_alloc == 0 and allocPoint > 0
            is_deactivated = prev_alloc and prev_alloc > 0 and allocPoint == 0
            is_alloc_point_changed = prev_alloc and prev_alloc != allocPoint and allocPoint > 0

            if (is_new or is_activated) and allocPoint > 0:
                print(f"⚠️ Farm PIP {pid} on {chain_key} add new active.")
                activated_at = int(time.time())
                current_pool_data[pid_str]["activated_at"] = activated_at
                
                pool_msg = (
                    f"🎉 [NEW FARM ACTIVE] on {chain_key.upper()}\n"
                    f"🔹 Pool ID: {pid}\n"
                    f"🔸 Pair: {symbol0} - {symbol1}\n"
                    f"💰 Fee: {fee / 10000:.2f}%\n"
                    f"🔥 AllocPoint: {allocPoint}\n"
                    f"🕒 Activated At: <t:{activated_at}:R>\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain_key]}{v3Pool}\n\n"
                    f"{CHAIN_SCAN_URLS[chain_key]}{v3Pool}\n"
                    f"https://pancakeswap.finance/farms?chain={PANCAKE_CHAIN_MAP[chain_key]}\n"
                )
                
                # Insert new pool
                print(f"⏳ Inserting new pool {pid} on {chain_key}...")
                update_pool_info(chain_key, v3Pool, token0, token1, symbol0, symbol1, decimals0, decimals1, fee, allocPoint, pid)
                
                notify_discord(pool_msg, DISCORD_WEBHOOK_URL)

            elif is_deactivated:
                print(f"⚠️ Farm PID {pid} on {chain_key} was deactivated (allocPoint now 0).")
                
                deact_msg = (
                    f"⚠️ [FARM DISABLED] on {chain_key.upper()}\n"
                    f"🔹 Pool ID: {pid}\n"
                    f"🔸 Pair: {symbol0} - {symbol1}\n"
                    f"💰 Fee: {fee / 10000:.2f}%\n"
                    f"🔸 AllocPoint: {prev_alloc} ➜ 0\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain_key]}{v3Pool}\n\n"
                    f"{CHAIN_SCAN_URLS[chain_key]}{contract_address}\n"
                    f"https://pancakeswap.finance/farms?chain={PANCAKE_CHAIN_MAP[chain_key]}\n"
                )
                
                # Update pool_info
                print(f"⚠️ Updating pool_info for {pid} on {chain_key} with allocPoint 0.")
                update_pool_info(chain_key, v3Pool, token0, token1, symbol0, symbol1, decimals0, decimals1, fee, allocPoint, pid)
                
                notify_discord(deact_msg, DISCORD_WEBHOOK_URL)

            elif is_alloc_point_changed:
                print(f"⚠️ Farm PID {pid} on {chain_key} updated allocPoint.")
                update_msg = (
                    f"⚠️ [FARM ALLOC POINT CHANGED] on {chain_key.upper()}\n"
                    f"🔹 Pool ID: {pid}\n"
                    f"🔸 Pair: {symbol0} - {symbol1}\n"
                    f"💰 Fee: {fee / 10000:.2f}%\n"
                    f"🔸 AllocPoint changed: {prev_alloc} ➜ {allocPoint}\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain_key]}{v3Pool}\n\n"
                    f"{CHAIN_SCAN_URLS[chain_key]}{contract_address}\n"
                    f"https://pancakeswap.finance/farms?chain={PANCAKE_CHAIN_MAP[chain_key]}\n"
                )
                
                # Update pool_info
                print(f"⚠️ Updating pool_info for {pid} on {chain_key} from allocPoint = {prev_alloc} to new allocPoint = {allocPoint}.")
                update_pool_info(chain_key, v3Pool, token0, token1, symbol0, symbol1, decimals0, decimals1, fee, allocPoint, pid)
                
                notify_discord(update_msg, DISCORD_WEBHOOK_URL)

        state[chain_key] = current_pool_data

    except Exception as e:
        print(f"⚠️ Error on {chain_key}: {e}")

    return state

def main():
    abi = load_abi()
    connection = get_connection()
    state, first_run = load_state(connection)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"🔄 Starting farm check at {current_time}...")

    for chain_key in MASTERCHEF_V3_ADDRESSES.keys():
        state = check_chain(chain_key, abi, state, first_run)

    save_state(state, connection)

if __name__ == "__main__":
    main()
