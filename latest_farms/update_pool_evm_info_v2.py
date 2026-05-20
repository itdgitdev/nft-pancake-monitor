import json, os, requests
from web3 import Web3
from w3multicall.multicall import W3Multicall
from config import MASTERCHEF_V3_ADDRESSES, RPC_URLS_2, RPC_BACKUP_LIST
from create_db import get_connection
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import math
from helper import *
from config import CHAIN_SCAN_URLS, API_URLS, API_KEYS, MASTERCHEF_V3_ADDRESSES, FACTORY_ADDRESSES, FACTORY_DEPLOYED_BLOCK, MASTERCHEF_DEPLOYED_BLOCK
from logging_setup import pool_evm_info_logger as log

ABI_FILE = 'latest_farms/abi_config.json'
# ABI_FILE = 'abi_config.json'

DISCORD_WEBHOOK_URL_PERCENT = "https://discordapp.com/api/webhooks/1385563895850078340/HjXe2bFPkBgdGBMalvRIUMDNgl4mazFvyaJIXs7LRHb66Z2xtOsPMoJVUGCuZLyqF6_T"
DISCORD_WEBHOOK_URL_LATEST = "https://discordapp.com/api/webhooks/1386555618751549520/i6GTfThX2VckPF4isp9ktn7ds1B0Ik7YWGGPR016nCO79uPIqm4ukYXPK-PR21_YvYyT"

# DISCORD_WEBHOOK_URL_PERCENT = "https://discordapp.com/api/webhooks/1376414684294549564/yxz1viXwF5f4b3EjakEp809E0Bqx62rDvqfc2y3aT8vfuA_1Wp9yIR_ZX05CuT5ayNHN"
# DISCORD_WEBHOOK_URL_LATEST = "https://discordapp.com/api/webhooks/1377961748925124681/4L4i0oxq6PD1jLlBUV2IxH-G2vobb-ESm2VhKWL30dQztF4sRVg8IkgOoWe4W2EB0IFS"

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
            "internalType": "uint8",
            "name": "",
            "type": "uint8"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
            "internalType": "string",
            "name": "",
            "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
            "internalType": "address",
            "name": "account",
            "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
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

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

def load_abi():
    with open(ABI_FILE, 'r') as f:
        return json.load(f)
        
# Get Web3 with backup RPCs
def web3_connection(chain_name: str, timeout: int = 30) -> Web3:
    """
    Trả về Web3 provider hoạt động được (ưu tiên RPC chính, sau đó backup).
    """
    urls = [RPC_URLS_2.get(chain_name)] + RPC_BACKUP_LIST.get(chain_name, [])
    urls = [u for u in urls if u]

    for rpc_url in urls:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': timeout}))
            if w3.is_connected():
                log.info(f"[OK] Connected to {chain_name} RPC: {rpc_url}")
                return w3
            else:
                log.warning(f"[WARN] {chain_name} RPC not responding: {rpc_url}")
        except Exception as e:
            log.error(f"[ERROR] {chain_name} RPC failed: {rpc_url} -> {e}")
        time.sleep(0.5)

    raise Exception(f"[FATAL] No working RPC found for {chain_name}")

def get_token_symbol(w3, token_address):    
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    
    symbol = "Unknown"  # default fallback
    decimals = 18  # default fallback
    
    try:
        symbol = token_contract.functions.symbol().call()
    except Exception as e:
        log.warning(f"⚠️ Error getting symbol for token {token_address}: {e}")
    
    try:
        decimals = token_contract.functions.decimals().call()
    except Exception as e:
        log.warning(f"⚠️ Error getting decimals for token {token_address}: {e}")
    
    return symbol, decimals

def get_cake_per_second(chain: str, abi: list) -> float | None:
    # Lấy RPC và contract address
    rpc_url = RPC_URLS_2.get(chain)
    contract_address = MASTERCHEF_V3_ADDRESSES.get(chain)

    if not rpc_url or not contract_address:
        raise ValueError(f"⚠️ Invalid configuration for chain: {chain}")

    # Kết nối Web3
    w3 = web3_connection(chain)
    if not w3:
        return
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(contract_address),
        abi=abi
    )

    # Lấy pool có alloc_point lớn nhất
    max_alloc_point_pool = get_max_alloc_point_per_chain(chain)
    if not max_alloc_point_pool:
        log.warning(f"⚠️ No active pool found on {chain}, skip CAKE per second calc")
        return None

    total_alloc_point = get_total_alloc_point_each_chain(chain) or 0
    max_alloc_point = max_alloc_point_pool["alloc_point"]
    pid = max_alloc_point_pool["pid"]

    log.info(f"🔍 Chain: {chain} | Total alloc_point: {total_alloc_point} | Max alloc_point: {max_alloc_point} | PID: {pid}")

    if pid <= 0 or max_alloc_point <= 0 or total_alloc_point <= 0:
        log.warning(f"⚠️ Skipping invalid data for chain {chain}")
        return None

    try:
        # Gọi hàm contract để lấy reward rate
        cake_per_second_raw, end_time = contract.functions.getLatestPeriodInfoByPid(pid).call()

        # Chuẩn hóa giá trị
        cake_per_second = cake_per_second_raw / (10 ** 30)

        # Tính cake per second cho toàn chain
        cake_per_second_chain = float(cake_per_second) * float(total_alloc_point) / float(max_alloc_point)

        log.info(f"✅ CAKE per second for {chain}: {cake_per_second_chain}")
        return cake_per_second_chain

    except Exception as e:
        log.error(f"❌ Error fetching CAKE per second for {chain}: {e}")
        return None

def calc_cake_per_day_for_pool(cake_per_second_chain, total_alloc_point_per_chain, alloc_point_pool):
    cake_per_second_pool = float(cake_per_second_chain) * float(alloc_point_pool) / float(total_alloc_point_per_chain)
    cake_per_day_pool = cake_per_second_pool * (24*60*60)
    return cake_per_day_pool

def safe_write_json(file_path, data):
    """Ghi file an toàn, tránh corrupt khi dừng giữa chừng"""
    tmp_path = file_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, file_path)

def get_masterchef_events(api_urls, api_keys, chain, masterchef_addresses,
                          start_block, end_block, pid=None,
                          event_name="Deposit", step=10_000_000,
                          save_dir="test", max_retry=3):
    """
    Lấy toàn bộ event Deposit/Withdraw(pid, tokenId, to) từ MasterChefV3
    (Tự chia nhỏ nếu vượt giới hạn 1000 logs mỗi lần gọi API)
    """
    topic_map = {
        "Deposit":  "0xb19157bff94fdd40c58c7d4a5d52e8eb8c2d570ca17b322b49a2bbbeedc82fbf",
        "Withdraw": "0xf341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567"
    }
    topic0 = topic_map.get(event_name)
    if not topic0:
        raise ValueError("event_name phải là 'Deposit' hoặc 'Withdraw'")

    topic2_pid = None
    if pid is not None:
        topic2_pid = "0x" + hex(pid)[2:].zfill(64)
        log.info(f"🎯 Filtering only PID={pid}")

    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"{event_name.lower()}_{chain}_pid_{pid}.json")

    # --- Resume logic ---
    all_events = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                state = json.load(f)
            all_events = state.get("events", [])
            last_block = state.get("last_block", start_block)
            # Sort + dedupe khi resume
            all_events.sort(key=lambda x: (x["blockNumber"], x["tokenId"]))
            seen = set()
            deduped = []
            for e in all_events:
                key = (e["blockNumber"], e["tokenId"])
                if key not in seen:
                    seen.add(key)
                    deduped.append(e)
            all_events = deduped
            start_block = max(start_block, last_block + 1)
            log.info(f"🔁 Resume PID={pid} from block {start_block} (loaded {len(all_events)} events)")
        except Exception:
            log.warning(f"⚠️ File {file_path} bị hỏng, khôi phục bản rỗng.")
            all_events, start_block = [], start_block
    else:
        log.info(f"🆕 Start PID={pid} from block {start_block}")

    url = api_urls[chain]
    api_key = api_keys[chain]
    masterchef_address = masterchef_addresses[chain]

    # --- Fetch logs ---
    def fetch_logs(from_block, to_block, depth=0):
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": masterchef_address,
            "topic0": topic0,
            "topic2": topic2_pid,
            "apikey": api_key
        }
        for attempt in range(max_retry):
            try:
                r = requests.get(url, params=params, timeout=10)
                data = r.json()
                if "result" not in data:
                    log.warning(f"⚠️ API error {from_block}-{to_block}: {data}")
                    time.sleep(1)
                    continue

                logs = data["result"]
                if logs is None:
                    logs = []

                # Nếu kết quả đạt 1000 => chia nhỏ range
                if len(logs) >= 1000 and (to_block - from_block) > 10:
                    mid = (from_block + to_block) // 2
                    log.warning(f"⚠️ >1000 logs ({len(logs)}) => chia nhỏ [{from_block}-{mid}] + [{mid+1}-{to_block}]")
                    left = fetch_logs(from_block, mid, depth+1)
                    right = fetch_logs(mid+1, to_block, depth+1)
                    if left is None or right is None:
                        return None
                    return left + right

                results = []
                for log_item in logs:
                    pid_val = int(log_item["topics"][2], 16)
                    token_id = int(log_item["topics"][3], 16)
                    from_addr = "0x" + log_item["topics"][1][-40:]
                    results.append({
                        "blockNumber": int(log_item["blockNumber"], 16),
                        "pid": pid_val,
                        "tokenId": token_id,
                        "from": from_addr
                    })
                return results
            except Exception as e:
                log.error(f"❌ Error {from_block}-{to_block}: {e}")
                time.sleep(3)
        log.warning(f"🚫 Failed to fetch range {from_block}-{to_block} after {max_retry} retries")
        return None

    # --- Main loop ---
    for block in range(start_block, end_block, step):
        to_block = min(block + step - 1, end_block)

        try:
            new_logs = fetch_logs(block, to_block)
        except Exception as e:
            log.error(f"❌ Range {block}-{to_block} failed: {e}, stopping...")
            break

        if new_logs is None:
            log.warning(f"🚫 Range {block}-{to_block} exhausted retries, stopping to preserve state.")
            break
        elif len(new_logs) == 0:
            log.info(f"✅ {event_name} | {block}-{to_block} | +0 (total={len(all_events)})")
        else:
            log.info(f"✅ {event_name} | {block}-{to_block} | +{len(new_logs)} (total={len(all_events)})")
            all_events.extend(new_logs)
        all_events.sort(key=lambda x: (x["blockNumber"], x["tokenId"]))

        # Dedupe theo tokenId, giữ event cuối cùng
        latest_event_per_token = {}
        for e in all_events:
            latest_event_per_token[e["tokenId"]] = e
        all_events = list(latest_event_per_token.values())

        log.info(f"✅ {event_name} | {block}-{to_block} | +{len(new_logs)} (total={len(all_events)})")

        # Ghi incremental
        safe_write_json(file_path, {
            "contract": masterchef_address,
            "pid": pid,
            "event": event_name,
            "last_block": to_block,
            "events": all_events
        })
        time.sleep(0.25)

    return all_events

def get_current_amounts(liquidity, sqrt_price_x96, tick_lower, tick_upper):
    sqrt_price = float(sqrt_price_x96) / 2**96
    sqrt_price_lower = math.sqrt(1.0001 ** tick_lower)
    sqrt_price_upper = math.sqrt(1.0001 ** tick_upper)
    
    if sqrt_price <= sqrt_price_lower:
        amount0 = liquidity * (sqrt_price_upper - sqrt_price_lower) / (sqrt_price_lower * sqrt_price_upper)
        amount1 = 0
    elif sqrt_price < sqrt_price_upper:
        amount0 = liquidity * (sqrt_price_upper - sqrt_price) / (sqrt_price * sqrt_price_upper)
        amount1 = liquidity * (sqrt_price - sqrt_price_lower)
    else:
        amount0 = 0
        amount1 = liquidity * (sqrt_price_upper - sqrt_price_lower)

    return amount0, amount1

def get_position_status(liquidity, tick_lower, tick_upper, current_tick):
    if liquidity > 0:
        if tick_lower <= current_tick <= tick_upper:
            return "Active"
        else:
            return "Inactive"
    else:
        return "Burned"

def batch_iterable(iterable, batch_size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch

def get_position_info_with_multicall(w3, chain, masterchef_addresses, token_ids, pid, batch_size=100):
    contract_address = masterchef_addresses[chain]
    position_infos = {}

    for batch_num, token_batch in enumerate(batch_iterable(token_ids, batch_size), start=1):
        mc = W3Multicall(w3)
        for token_id in token_batch:
            mc.add(
                W3Multicall.Call(
                    contract_address,
                    "userPositionInfos(uint256)(uint128,uint128,int24,int24,uint256,uint256,address,uint256,uint256)",
                    token_id
                )
            )

        try:
            position_results = mc.call()
        except Exception as e:
            log.warning(f"⚠️ Multicall batch {batch_num} lỗi: {e}")
            continue

        for idx, data in enumerate(position_results):
            token_id = token_batch[idx]
            if not data:
                continue

            try:
                liquidity, boost_liquidity, tick_lower, tick_upper, reward_growth_inside, reward, user, position_pid, boost_multiplier = data
                if pid != position_pid:
                    continue
                position_infos[token_id] = {
                    "liquidity": liquidity,
                    "boost_liquidity": boost_liquidity,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "reward_growth_inside": reward_growth_inside,
                    "reward": reward,
                    "user": user,
                    "position_pid": position_pid,
                    "boost_multiplier": boost_multiplier
                }
            except Exception as e:
                log.warning(f"⚠️ Error unpacking token_id={token_id}: {e}")
                continue

        log.info(f"✅ Done batch {batch_num}: {len(token_batch)} token_ids processed")

    return position_infos

def update_pool_info(chain_key, pool_address, token0, token1, token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee, alloc_point, pid):
    try:
        # Kết nối DB
        conn = get_connection()
        cursor = conn.cursor()
        
        # Vietname time current
        vietnam_timezone = timezone(timedelta(hours=7))
        current_time = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")
        
        sql = """
        INSERT INTO pool_info (
                    chain, pool_address,
                    token0_address, token1_address,
                    token0_symbol, token1_symbol,
                    token0_decimals, token1_decimals,
                    fee, alloc_point, pid, timestamp
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            token0_symbol = VALUES(token0_symbol),
            token1_symbol = VALUES(token1_symbol),
            fee = VALUES(fee),
            alloc_point = VALUES(alloc_point),
            timestamp = VALUES(timestamp)
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
            pid,
            current_time
        )

        cursor.execute(sql, values)
        conn.commit()
        log.info(f"✅ Updated pool_info for {chain_key} PID {pid} ({token0_symbol}-{token1_symbol})")

    except Exception as e:
        log.error(f"❌ Error updating pool_info for {chain_key} PID {pid}: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_pool_infos(w3, pool_address, pool_abi):
    pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)
    
    slot0 = pool_contract.functions.slot0().call()
    sqrt_price_x96 = slot0[0]
    tick_current = slot0[1]
    
    return {
        "sqrt_price_x96": sqrt_price_x96,
        "tick_current": tick_current
    }

def get_pool_infos_with_multicall(w3, contract_address, pool_length):
    # --------------------------
    # 1️⃣ Multicall lấy toàn bộ poolInfo
    # --------------------------
    mc = W3Multicall(w3)
    for pid in range(pool_length+1):
        mc.add(
            W3Multicall.Call(
                contract_address,
                "poolInfo(uint256)(uint256,address,address,address,uint24,uint256,uint256)",
                pid
            )
        )
    pool_results = mc.call()

    pool_infos = {}
    token_set = set()
    for pid, data in enumerate(pool_results):
        alloc_point, pool_address, token0, token1, fee, total_liquidity, total_boost_liquidity = data
        
        if int(pool_address, 16) == 0:
            log.warning(f"⚠️ Skip dummy pool pid={pid}")
            continue
        
        pool_infos[pid] = {
            "alloc_point": alloc_point,
            "pool_address": Web3.to_checksum_address(pool_address),
            "token0": Web3.to_checksum_address(token0),
            "token1": Web3.to_checksum_address(token1),
            "fee": fee,
            "total_liquidity": total_liquidity,
            "total_boost_liquidity": total_boost_liquidity
        }
        token_set.add(pool_infos[pid]["token0"])
        token_set.add(pool_infos[pid]["token1"])

    # --------------------------
    # 2️⃣ Multicall lấy decimals + symbol cho mỗi token
    # --------------------------
    mc2 = W3Multicall(w3)
    for token in token_set:
        mc2.add(W3Multicall.Call(token, "decimals()(uint8)"))
        mc2.add(W3Multicall.Call(token, "symbol()(string)"))
    token_meta_results = mc2.call()

    token_meta = {}
    i = 0
    for token in token_set:
        decimals = token_meta_results[i]; i += 1
        symbol = token_meta_results[i]; i += 1
        if decimals is None:  # fallback
            decimals = 18
        token_meta[token] = {
            "decimals": decimals,
            "symbol": symbol
        }

    # --------------------------
    # 3️⃣ Multicall lấy balanceOf(poolAddress) cho mỗi token/pool
    # --------------------------
    mc3 = W3Multicall(w3)
    balance_map = []  # list các tuple (pid, token)

    for pid, info in pool_infos.items():
        pool_address = info["pool_address"]
        for token in (info["token0"], info["token1"]):
            mc3.add(W3Multicall.Call(token, "balanceOf(address)(uint256)", pool_address))
            balance_map.append((pid, token))  # lưu mapping

    balance_results = mc3.call()

    # Parse balance results với mapping
    for idx, (pid, token) in enumerate(balance_map):
        balance_raw = balance_results[idx]
        if balance_raw is None:
            balance = 0
        else:
            decimals = token_meta.get(token, {}).get("decimals", 18)  # fallback 18
            balance = balance_raw / (10 ** decimals)

        # Gán vào pool_infos
        info = pool_infos[pid]
        info[f"balance_{token}"] = balance
        info[f"decimals_{token}"] = token_meta.get(token, {}).get("decimals", 18)
        info[f"symbol_{token}"] = token_meta.get(token, {}).get("symbol", "UNKNOWN")
        
    return pool_infos

def get_existing_pool_address_from_db(chain, cursor):
    # Get list of existing pool addresses from DB
    existing_pool_addresses = set()
    cursor.execute("SELECT pool_address FROM pool_info WHERE chain = %s", (chain,))
    for row in cursor.fetchall():
        existing_pool_addresses.add(row[0].lower())
    
    return existing_pool_addresses

def check_cake_price():
    # Get cake price
    cake_price = get_cake_price_usd()
    if not cake_price:
        cake_price = 2.5
    
    return cake_price

def load_pool_info_state(connection, chain_key: str):
    state = {}
    first_run = False

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT chain, pool_address, 
                   token0_address, token1_address,
                   token0_symbol, token1_symbol,
                   token0_decimals, token1_decimals,
                   fee, alloc_point, pid, cake_per_day, 
                   total_value_lock, cake_reward_1h,
                   total_current_liquidity, total_staked_liquidity,
                   is_stake_tracked, timestamp, is_bot_managed
            FROM pool_info
            WHERE chain = %s
        """, (chain_key,))
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                pid = str(row[10])
                state[pid] = {
                    "chain": row[0],
                    "pool_address": row[1],
                    "token0_address": row[2],
                    "token1_address": row[3],
                    "token0_symbol": row[4],
                    "token1_symbol": row[5],
                    "token0_decimals": row[6],
                    "token1_decimals": row[7],
                    "fee": row[8],
                    "allocPoint": row[9],
                    "pid": row[10],
                    "cake_per_day": row[11],
                    "total_value_lock": row[12],
                    "cake_reward_1h": row[13],
                    "total_current_liquidity": row[14],
                    "total_staked_liquidity": row[15],
                    "is_stake_tracked": row[16],
                    "timestamp": int(row[17].timestamp()) if row[17] else None,
                    "is_bot_managed": row[18] if len(row) > 18 and row[18] else False
                }
        else:
            first_run = True

    return state, first_run

def save_pool_info_state(connection, chain_key: str, pool_data: dict):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pid, alloc_point FROM pool_info WHERE chain = %s AND pool_address = %s",
            (chain_key, pool_data["pool_address"])
        )
        result = cursor.fetchone()
        
        # Vietnam timezone current time
        vietnam_timezone = timezone(timedelta(hours=7))
        now = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")

        if result:
            db_pid, db_alloc_point = result
            # Nếu alloc_point hoặc pid thay đổi → update
            if db_alloc_point != pool_data["alloc_point"] or db_pid != pool_data["pid"]:
                cursor.execute(
                    """
                    UPDATE pool_info
                    SET pid = %s, alloc_point = %s, fee = %s,
                        cake_per_day = %s, total_value_lock = %s,
                        cake_reward_1h = %s, total_current_liquidity = %s,
                        total_staked_liquidity = %s, total_inactive_staked_liquidity = %s,
                        farm_apr = %s, token0_symbol = %s, token1_symbol = %s,
                        token0_decimals = %s, token1_decimals = %s, is_bot_managed = %s, timestamp = %s
                    WHERE chain = %s AND pool_address = %s
                    """,
                    (
                        pool_data["pid"],
                        pool_data["alloc_point"],
                        pool_data["fee"],
                        pool_data["cake_per_day"],
                        pool_data["total_value_lock"],
                        pool_data["cake_reward_1h"],
                        pool_data["total_current_liquidity"],
                        pool_data["total_staked_liquidity"],
                        pool_data["total_inactive_staked_liquidity"],
                        pool_data["farm_apr"],
                        pool_data["token0_symbol"],
                        pool_data["token1_symbol"],
                        pool_data["token0_decimals"],
                        pool_data["token1_decimals"],
                        pool_data.get("is_bot_managed", False),
                        now,
                        chain_key,
                        pool_data["pool_address"]
                    )
                )
        else:
            # Insert pool mới
            cursor.execute(
                """
                INSERT INTO pool_info (
                    chain, pool_address,
                    token0_address, token1_address,
                    token0_symbol, token1_symbol,
                    token0_decimals, token1_decimals,
                    fee, alloc_point, pid,
                    cake_per_day, total_value_lock,
                    cake_reward_1h, total_current_liquidity,
                    total_staked_liquidity, total_inactive_staked_liquidity, 
                    farm_apr, is_bot_managed, timestamp
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    chain_key,
                    pool_data["pool_address"],
                    pool_data["token0_address"],
                    pool_data["token1_address"],
                    pool_data["token0_symbol"],
                    pool_data["token1_symbol"],
                    pool_data["token0_decimals"],
                    pool_data["token1_decimals"],
                    pool_data["fee"],
                    pool_data["alloc_point"],
                    pool_data["pid"],
                    pool_data["cake_per_day"],
                    pool_data["total_value_lock"],
                    pool_data["cake_reward_1h"],
                    pool_data["total_current_liquidity"],
                    pool_data["total_staked_liquidity"],
                    pool_data["total_inactive_staked_liquidity"],
                    pool_data["farm_apr"],
                    pool_data.get("is_bot_managed", False),
                    now
                )
            )
    connection.commit()

def get_block_created_pool(api_urls, api_keys, chain, factory_addresses, token0, token1, fee, start_block, end_block, max_retries=3, delay=2, timeout=10):
    
    fee_hex = hex(fee)[2:].rjust(64, "0")
    
    topic0 = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
    topic1 = f"0x000000000000000000000000{token0.lower()[2:]}"
    topic2 = f"0x000000000000000000000000{token1.lower()[2:]}"
    topic3 = f"0x{fee_hex}"
    
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start_block,
        "toBlock": end_block,
        "address": factory_addresses[chain],
        "topic0": topic0,
        "topic1": topic1,
        "topic2": topic2,
        "topic3": topic3,
        "apikey": api_keys[chain]
    }

    url = api_urls[chain]
    
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code != 200:
                log.error(f"⚠️ [{chain}] HTTP {resp.status_code} (Attempt {attempt}/{max_retries})")
                time.sleep(delay)
                continue

            data = resp.json()
            if data.get("status") == "1" and data.get("result"):
                result = data["result"][0]
                block_number = int(result["blockNumber"], 16)
                tx_hash = result["transactionHash"]
                log.info(f"✅ [{chain}] Pool created at block {block_number} (tx: {tx_hash})")
                return block_number
            else:
                msg = data.get("message", "Unknown error")
                log.warning(f"⚠️ [{chain}] API returned empty or invalid result ({msg}) (Attempt {attempt}/{max_retries})")
                time.sleep(delay)

        except requests.exceptions.Timeout:
            log.warning(f"⏳ [{chain}] Timeout when calling API (Attempt {attempt}/{max_retries})")
            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            log.error(f"❌ [{chain}] Request failed: {e} (Attempt {attempt}/{max_retries})")
            time.sleep(delay)

        except Exception as e:
            log.error(f"❌ [{chain}] Unexpected error: {e}")
            break

    log.warning(f"🚫 [{chain}] Failed to fetch PoolCreated event after {max_retries} retries.")
    return None

def calculate_total_staked_liquidity(
    chain,
    w3,
    pid,
    pool_address,
    prev_entry,
    token0,
    token1,
    price_token0,
    price_token1,
    API_URLS,
    API_KEYS,
    FACTORY_ADDRESSES,
    FACTORY_DEPLOYED_BLOCK,
    MASTERCHEF_DEPLOYED_BLOCK,
    MASTERCHEF_V3_ADDRESSES,
    V3POOL_ABI
):
    """
    Tính tổng giá trị thanh khoản (USD) đang stake trong MasterChefV3 của 1 pool.
    Nếu pool chưa được stake-tracked, trả về 0.
    """

    # ==== 1️⃣ Kiểm tra điều kiện ====
    prev_is_stake_tracked = prev_entry.get("is_stake_tracked", False) if prev_entry else False
    if not prev_is_stake_tracked:
        log.warning(f"⏭️ Pool {pool_address} not tracked for stake.")
        return False

    token0_decimals = prev_entry.get("token0_decimals", 18)
    token1_decimals = prev_entry.get("token1_decimals", 18)
    fee = prev_entry.get("fee", 0)
    token0 = prev_entry.get("token0_address", token0)
    token1 = prev_entry.get("token1_address", token1)

    # ==== 2️⃣ Xác định block bắt đầu ====
    start_block = FACTORY_DEPLOYED_BLOCK.get(chain, 0)
    initial_block = get_block_created_pool(
        API_URLS, API_KEYS, chain, FACTORY_ADDRESSES,
        token0, token1, fee,
        start_block=start_block,
        end_block=w3.eth.block_number
    )

    if initial_block is None:
        initial_block = MASTERCHEF_DEPLOYED_BLOCK.get(chain, 0)

    log.info(f"📦 Start block: {initial_block}, End block: {w3.eth.block_number}")
    log.info(f"Token0 decimals: {token0_decimals}, Token1 decimals: {token1_decimals}")
    log.info(f"Token0 price: {price_token0}, Token1 price: {price_token1}")

    folder_path = "latest_farms/" # server
    # folder_path = "" # local
    # ==== 3️⃣ Lấy danh sách event Deposit/Withdraw ====
    log.info(f"🔍 Fetching deposit & withdraw events for PID {pid}...")
    deposits = get_masterchef_events(
        API_URLS, API_KEYS, chain, MASTERCHEF_V3_ADDRESSES,
        initial_block, w3.eth.block_number,
        pid=pid, event_name="Deposit", save_dir=f"{folder_path}stake_event_json/deposit_json"
    )
    withdraws = get_masterchef_events(
        API_URLS, API_KEYS, chain, MASTERCHEF_V3_ADDRESSES,
        initial_block, w3.eth.block_number,
        pid=pid, event_name="Withdraw", save_dir=f"{folder_path}stake_event_json/withdraw_json"
    )

    last_deposit = {}
    last_withdraw = {}
    
    for d in deposits:
        tid = d["tokenId"]
        blk = d["blockNumber"]
        if tid not in last_deposit or blk > last_deposit[tid]:
            last_deposit[tid] = blk
    
    for w in withdraws:
        tid = w["tokenId"]
        blk = w["blockNumber"]
        if tid not in last_withdraw or blk > last_withdraw[tid]:
            last_withdraw[tid] = blk
    
    staked = []
    all_token_ids = set(last_deposit.keys()) | set(last_withdraw.keys())
    for tid in all_token_ids:
        dep_block = last_deposit.get(tid, -1)
        wd_block = last_withdraw.get(tid, -1)

        if dep_block >= wd_block and dep_block > 0:
            staked.append(tid)
    
    # token_ids_deposit = {d["tokenId"] for d in deposits}
    # token_ids_withdraw = {w["tokenId"] for w in withdraws}
    # token_ids_staked = sorted(list(token_ids_deposit - token_ids_withdraw))
    
    is_bot_managed = False
    recent_withdraws = sorted(withdraws, key=lambda x: x["blockNumber"], reverse=True)[:100]
    lifespans = []
    
    for w in recent_withdraws:
        tid = w["tokenId"]
        wd_block = w["blockNumber"]
        dep_block = last_deposit.get(tid, -1)
        if dep_block > 0 and wd_block >= dep_block:
            lifespans.append(wd_block - dep_block)

    if len(lifespans) >= 10:
        avg_lifespan = sum(lifespans) / len(lifespans)
        blocks_2h_map = {
            "BNB": 2400, "BSC": 2400,
            "ETH": 600,
            "ARB": 28000,
            "BAS": 3600, "BASE": 3600,
            "LIN": 3600, "LINEA": 3600,
            "POL": 3600, "POLYGON": 3600,
        }
        threshold_2h = float(blocks_2h_map.get(chain.upper(), 2400))
        
        log.info(f"🤖 [Bot Detect] PID={pid}: Average lifespan of recent {len(lifespans)} withdraws is {avg_lifespan:.1f} blocks (Threshold 2h = {threshold_2h} blocks)")
        
        if avg_lifespan <= threshold_2h:
            is_bot_managed = True
            log.warning(f"⚠️ [Bot Detect] PID={pid} is managed by bot! Avg Lifespan: {avg_lifespan:.1f} blocks <= {threshold_2h}")
            
    token_ids_staked = sorted(staked)

    log.info(f"🎯 Token IDs còn stake: {token_ids_staked}")
    if not token_ids_staked:
        log.warning("⚠️ Không có position nào đang stake.")
        
    return is_bot_managed

def get_pool_info_and_save(chain, abi):
    rpc_url = RPC_URLS_2.get(chain)
    contract_address = MASTERCHEF_V3_ADDRESSES.get(chain)

    if not rpc_url or not contract_address:
        raise ValueError(f"Invalid configuration for chain: {chain}")

    # Connect to web3 rpc
    w3 = web3_connection(chain)
    if not w3:
        return
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)

    pool_length = contract.functions.poolLength().call()
    log.info(f"\n🔍 Found {pool_length} pools on {chain} chain at {(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}")

    cake_per_second_chain = get_cake_per_second(chain, abi)
    total_alloc_point_chain = get_total_alloc_point_each_chain(chain)

    conn = get_connection()
    cursor = conn.cursor()

    # Load DB state
    db_state, first_run = load_pool_info_state(conn, chain)

    # Multicall lấy pool info
    pool_infos = get_pool_infos_with_multicall(w3, contract_address, pool_length)

    # Giá CAKE
    cake_price = check_cake_price()
    log.info(f"🔍 Cake price: {cake_price}")

    # Filter active pool từ Pancake API
    active_pools = get_list_pool_actived_farm_by_api_pancake(chain)
    use_api_filter = bool(active_pools)

    update_rows = []
    for pid, info in pool_infos.items():
        alloc_point = info["alloc_point"]
        pool_address = info["pool_address"].lower()
        token0, token1 = info["token0"], info["token1"]
        fee = info["fee"]
        pid_str = str(pid)

        # if use_api_filter and pool_address.lower() not in active_pools.values():
        #     log.warning(f"🚫 Skipping inactive pool {pool_address} on {chain} per Pancake API")
        #     continue

        # Trạng thái cũ từ DB
        prev_entry = db_state.get(pid_str)
        prev_alloc = prev_entry["allocPoint"] if prev_entry else None
        log.info(f"🔍 Checking {pid_str}... with pre_alloc = {prev_alloc} and alloc = {alloc_point}")

        # ==== LOGIC old vs new ====
        is_new = prev_alloc is None
        is_activated = prev_alloc == 0 and alloc_point > 0
        is_deactivated = prev_alloc and prev_alloc > 0 and alloc_point == 0
        is_alloc_changed = prev_alloc and prev_alloc != alloc_point and alloc_point > 0

        symbol0, symbol1 = info[f"symbol_{token0}"], info[f"symbol_{token1}"]
        decimals0, decimals1 = info[f"decimals_{token0}"], info[f"decimals_{token1}"]

        # ==== Notify farm events (skip nếu first_run) ====
        if not first_run:
            if (is_new or is_activated) and alloc_point > 0:
                msg = (
                    f"🎉 [NEW FARM ACTIVE] on {chain.upper()}\n"
                    f"🔹 PID: {pid}\n"
                    f"🔸 Pair: {symbol0}-{symbol1}\n"
                    f"💰 Fee: {fee/10000:.2f}%\n"
                    f"🔥 AllocPoint: {alloc_point}\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain]}{pool_address}\n"
                )
                
                log.info(msg)
                notify_discord(msg, DISCORD_WEBHOOK_URL_LATEST)

            elif is_deactivated:
                msg = (
                    f"⚠️ [FARM DISABLED] on {chain.upper()}\n"
                    f"🔹 PID: {pid}\n"
                    f"🔸 Pair: {symbol0}-{symbol1}\n"
                    f"💰 Fee: {fee/10000:.2f}%\n"
                    f"🔸 AllocPoint: {prev_alloc} ➜ 0\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain]}{pool_address}\n"
                )
                
                log.info(msg)
                notify_discord(msg, DISCORD_WEBHOOK_URL_LATEST)

            elif is_alloc_changed:
                msg = (
                    f"⚠️ [FARM ALLOC POINT CHANGED] on {chain.upper()}\n"
                    f"🔹 PID: {pid}\n"
                    f"🔸 Pair: {symbol0}-{symbol1}\n"
                    f"💰 Fee: {fee/10000:.2f}%\n"
                    f"🔸 AllocPoint: {prev_alloc} ➜ {alloc_point}\n"
                    f"🔗 Pool: {CHAIN_SCAN_URLS[chain]}{pool_address}\n"
                )

                log.info(msg)
                notify_discord(msg, DISCORD_WEBHOOK_URL_LATEST)

        # ==== Update metadata vào DB ====
        update_pool_info(
            chain, pool_address, token0, token1,
            symbol0, symbol1,
            decimals0, decimals1,
            fee, alloc_point, pid
        )

        # ==== Nếu active → tính APR & reward ====
        if alloc_point > 0:
            amount_token0 = info[f"balance_{token0}"]
            amount_token1 = info[f"balance_{token1}"]

            total_cake_reward_1h, total_nft, total_pending_increase_1h = get_total_cake_reward_1h_pool(chain, pool_address)
            total_current_liquidity, _ = get_total_current_liquidity_on_pool(chain, pool_address)            
            log.info(f"🔍 Total CAKE reward 1h: {total_cake_reward_1h}, total NFT: {total_nft}, total pending increase 1h: {total_pending_increase_1h}, total liquidity: {total_current_liquidity}")

            price_token0 = get_price_tokens(chain, token0)
            price_token1 = get_price_tokens(chain, token1)
            
            if price_token0 is None or price_token1 is None:
                log.warning(f"⚠️ Can't fetch price for token0 or token1. Skipping TVL and APR calculation.")
                pool_info = get_pool_infos(w3, pool_address=Web3.to_checksum_address(pool_address), pool_abi=V3POOL_ABI)
                tick_current = pool_info["tick_current"]
                log.info(f"🎯 Current tick: {tick_current}")
                
                price_token0 = get_price_tokens(chain, token0, tick_current, token0, token1, decimals0, decimals1)
                price_token1 = get_price_tokens(chain, token1, tick_current, token0, token1, decimals0, decimals1)

            # Token price
            if price_token0 and price_token1:
                log.info(f"🔍 Price {symbol0}: {price_token0}")
                log.info(f"🔍 Price {symbol1}: {price_token1}")
            
            tvl = amount_token0 * price_token0 + amount_token1 * price_token1

            calc_cake_per_day = 0
            if cake_per_second_chain and total_alloc_point_chain and alloc_point > 0:
                calc_cake_per_day = calc_cake_per_day_for_pool(
                    cake_per_second_chain, total_alloc_point_chain, alloc_point
                )
            log.info(f"🔍 CAKE per day: {calc_cake_per_day}")

            # Check bot
            is_bot_managed = calculate_total_staked_liquidity(
                chain, w3, pid, pool_address, prev_entry,
                token0, token1, price_token0, price_token1,
                API_URLS, API_KEYS,
                FACTORY_ADDRESSES, FACTORY_DEPLOYED_BLOCK,
                MASTERCHEF_DEPLOYED_BLOCK,
                MASTERCHEF_V3_ADDRESSES, V3POOL_ABI
            )
            
            # Retrieve real-time total_staked_liquidity from DB
            cursor_temp = conn.cursor(dictionary=True)
            cursor_temp.execute("SELECT total_staked_liquidity FROM pool_info WHERE chain=%s AND pool_address=%s", (chain, pool_address))
            row_temp = cursor_temp.fetchone()
            total_staked_liquidity = float(row_temp["total_staked_liquidity"]) if row_temp and row_temp.get("total_staked_liquidity") else 0
            cursor_temp.close()
            
            used_tvl = total_staked_liquidity if total_staked_liquidity > 0 else tvl
            percent = (calc_cake_per_day * cake_price) / used_tvl * 100 if used_tvl > 0 else 0
            
            farming_pos_cake = (float(total_pending_increase_1h*12) / calc_cake_per_day) * 100 if calc_cake_per_day > 0 else 0
            farming_pos_lp = (total_current_liquidity / used_tvl) * 100 if used_tvl > 0 else 0

            pool_url = CHAIN_SCAN_URLS[chain] + f'/{pool_address}'
            msg = f'✅ Pool({chain}) [{pool_address[:6]}...{pool_address[-6:]}]({pool_url}) ({symbol0}-{symbol1}) | Fee:({fee/10000:.2f}%) | Reward:'
            msg += f' {round(calc_cake_per_day)} cake/day = {round(calc_cake_per_day * cake_price)} / {round(used_tvl)} = {percent:.2f}% - Farming {total_nft} pos:'
            msg += f' cake {round(float(total_pending_increase_1h*12), 3)} / {round(calc_cake_per_day)} = {(farming_pos_cake):.0f}%,'
            msg += f' LP {round(total_current_liquidity)} / {round(used_tvl)} = {(farming_pos_lp):.0f}%'
            
            if percent > 1 and (0 < total_cake_reward_1h <= 50):
                notify_discord(msg, DISCORD_WEBHOOK_URL_PERCENT)
            
            # Update DB basic pool info
            log.info(f"🔸 Updating pool info chain {chain} pid {pid} and allocPoint({alloc_point}) > 0.")
            # cursor.execute("""
            #     UPDATE pool_info
            #     SET cake_per_day=%s, total_value_lock=%s, cake_reward_1h=%s, total_current_liquidity=%s, total_staked_liquidity=%s, total_inactive_staked_liquidity=%s, farm_apr=%s, timestamp=NOW()
            #     WHERE chain=%s AND pool_address=%s
            # """, (calc_cake_per_day, tvl, total_cake_reward_1h, total_current_liquidity, total_staked_liquidity, total_inactive_staked_liquidity, farm_apr, chain, pool_address))
            
            # Vietnam timezone current time
            vietnam_timezone = timezone(timedelta(hours=7))
            current_time = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")
            
            update_rows.append((
                calc_cake_per_day, tvl, total_cake_reward_1h,
                total_current_liquidity, is_bot_managed, current_time,
                chain, pool_address
            ))
    
    if update_rows:
        log.info(f"🔸 Updating {len(update_rows)} pool info")        
        cursor.executemany("""
            UPDATE pool_info SET 
                cake_per_day=%s, total_value_lock=%s, cake_reward_1h=%s,
                total_current_liquidity=%s, is_bot_managed=%s, timestamp=%s
            WHERE chain=%s AND pool_address=%s
        """, update_rows)
        
        conn.commit()
    else:
        log.info(f"🔸 No pool farm info to update")
    
    cursor.close()
    conn.close()
    
def main():
    abi = load_abi()
    
    # Vietnam timezone current time
    vietnam_timezone = timezone(timedelta(hours=7))
    current_time = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"🔄 Starting farm check at {current_time}...")

    chains = list(MASTERCHEF_V3_ADDRESSES.keys())
    
    # Sử dụng ThreadPoolExecutor để chạy song song các chain
    with ThreadPoolExecutor(max_workers=len(chains)) as executor:
        future_to_chain = {executor.submit(get_pool_info_and_save, chain, abi): chain for chain in chains}
        
        for future in as_completed(future_to_chain):
            chain = future_to_chain[future]
            try:
                future.result()
            except Exception as e:
                log.error(f"❌ [{chain}] Error in thread: {e}")

if __name__ == "__main__": 
    main()
