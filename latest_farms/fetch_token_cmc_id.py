import requests
from datetime import datetime
import time
from create_db import *
from web3 import Web3
from solders.pubkey import Pubkey
from logging_setup import token_cmc_map_logger as log

# ==== CONFIG ====
API_KEY = "431ffea7-d90a-47db-843a-90e08887b28d"
HEADERS = {"X-CMC_PRO_API_KEY": API_KEY}
NOW = int(time.time())

# ==== GET UNIQUE TOKEN ADDRESSES FROM DB ====
def get_unique_token_addresses(cursor):
    # Get EVM tokens (alloc_point > 0)
    cursor.execute("""
        SELECT DISTINCT chain, token0_address AS token_address 
        FROM pool_info 
        WHERE token0_address IS NOT NULL AND alloc_point > 0
        UNION
        SELECT DISTINCT chain, token1_address AS token_address 
        FROM pool_info 
        WHERE token1_address IS NOT NULL AND alloc_point > 0
    """)
    evm_tokens = cursor.fetchall()
    
    # Get Solana tokens (weekly_rewards > 0)
    cursor.execute("""
        SELECT DISTINCT chain, token0_mint AS token_address 
        FROM pool_sol_info
        WHERE token0_mint IS NOT NULL AND weekly_rewards > 0
        UNION
        SELECT DISTINCT chain, token1_mint AS token_address 
        FROM pool_sol_info
        WHERE token1_mint IS NOT NULL AND weekly_rewards > 0
        UNION
        SELECT DISTINCT chain, reward_account AS token_address 
        FROM pool_sol_info
        WHERE reward_account IS NOT NULL AND weekly_rewards > 0
    """)
    sol_tokens = cursor.fetchall()
    
    # Get Aerodrome Tokens
    cursor.execute("""
        SELECT DISTINCT p1.chain, p1.token0_address AS token_address 
        FROM aerodrome_pool_info AS p1
        INNER JOIN aerodrome_pool_epoch_state AS p2
        ON p1.chain = p2.chain AND p1.pool_address = p2.pool_address
        WHERE p1.token0_address IS NOT NULL AND p2.farm_active = 1
        UNION
        SELECT DISTINCT p1.chain, p1.token1_address AS token_address 
        FROM aerodrome_pool_info AS p1
        INNER JOIN aerodrome_pool_epoch_state AS p2
        ON p1.chain = p2.chain AND p1.pool_address = p2.pool_address
        WHERE p1.token1_address IS NOT NULL AND p2.farm_active = 1
    """)
    aerodrome_evm_tokens = cursor.fetchall()
    
    # Combine all and remove duplicates
    unique_tokens = {}
    for row in evm_tokens + sol_tokens + aerodrome_evm_tokens:
        # --- hỗ trợ cả DictCursor lẫn Tuple ---
        if isinstance(row, dict):
            token_address = row.get("token_address")
            chain = row.get("chain")
        else:
            chain, token_address = row  # khi fetchall() trả tuple (chain, token_address)

        if not token_address:
            continue

        addr = token_address.strip()
        chain = (chain or "")

        if addr and addr not in unique_tokens:
            unique_tokens[addr] = chain

    return [{"chain": c, "token_address": t} for t, c in unique_tokens.items()]

# ==== GET TOKEN INFO FROM CMC ====
def fetch_cmc_info(token_address: str):
    try:
        url = f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?address={token_address}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        data = resp.json().get("data", {})

        if not data:
            log.info(f"[SKIP] Token {token_address} chưa được list trên CMC.")
            return None

        cmc_id = list(data.keys())[0]
        info = data[cmc_id]
        return {
            "cmc_id": cmc_id,
            "symbol": info.get("symbol"),
            "name": info.get("name")
        }

    except Exception as e:
        log.error(f"[ERROR] Khi lấy thông tin token {token_address}: {e}")
        return None

# ==== MAIN PROCESS ====
def update_token_cmc_map():
    conn = get_connection()
    cursor = conn.cursor()
    
    create_database_and_table()
    
    # 1️⃣ Lấy toàn bộ token address duy nhất
    token_addresses = get_unique_token_addresses(cursor)
    log.info(f"📦 Tìm thấy {len(token_addresses)} token address trong pool_info / pool_sol_info")

    # 2️⃣ Lấy danh sách token đã có trong token_cmc_map
    cursor.execute("SELECT token_address FROM token_cmc_map")
    existing = {r[0].lower() for r in cursor.fetchall()}

    # ⚙️ Chỉ lấy token chưa có trong DB
    new_tokens = [t for t in token_addresses if t["token_address"].lower() not in existing]
    log.info(f"🆕 Có {len(new_tokens)} token mới cần cập nhật")

    # 3️⃣ Gọi API và lưu vào DB
    for i, addr in enumerate(new_tokens, start=1):
        token_address = addr["token_address"]
        chain = addr["chain"]
        
        if chain != "SOL":
            token_address = Web3.to_checksum_address(token_address)
        
        info = fetch_cmc_info(str(token_address))
        
        if info:
            cursor.execute("""
                INSERT INTO token_cmc_map (token_address, chain, cmc_id, symbol, name, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    chain = VALUES(chain),
                    cmc_id = VALUES(cmc_id),
                    symbol = VALUES(symbol),
                    name = VALUES(name),
                    last_updated = VALUES(last_updated)
            """, (
                token_address, chain, info["cmc_id"], info["symbol"], info["name"], datetime.now()
            ))
            conn.commit()
            log.info(f"[{i}/{len(new_tokens)}] ✅ {chain} {info['symbol']} ({token_address}) → ID {info['cmc_id']}")
        else:
            log.warning(f"[{i}/{len(new_tokens)}] ⚠️ Không lấy được dữ liệu cho {token_address}")

        # Giới hạn tần suất gọi API (CMC rate limit: 30 req/min với free tier)
        time.sleep(2)

    conn.close()
    log.info("🎯 Hoàn tất cập nhật token_cmc_map!")

def get_token_prices_by_address(convert: str = "USD"):
    """
    Lấy giá token hiện tại từ CoinMarketCap thông qua CMC ID đã lưu trong DB.
    Trả về dict dạng {token_address: price}.
    """
    CMC_PRICE_URL = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT token_address, cmc_id FROM token_cmc_map WHERE cmc_id IS NOT NULL")
    rows = cursor.fetchall()

    # map token_address -> cmc_id
    addr_to_id = {row["token_address"]: str(row["cmc_id"]) for row in rows if row["cmc_id"]}
    log.info(f"📦 Tìm thấy {len(addr_to_id)} token trong token_cmc_map")

    # tạo list cmc_id (không loại trùng)
    cmc_id_list = list(addr_to_id.values())
    log.info(f"📦 Tổng cộng {len(cmc_id_list)} CMC ID (bao gồm trùng)")
    
    all_prices = {}
    BATCH_SIZE = 100

    for i in range(0, len(cmc_id_list), BATCH_SIZE):
        batch = cmc_id_list[i:i + BATCH_SIZE]
        ids_str = ",".join(batch)  # không loại trùng

        attempt = 0
        while attempt < 3:
            try:
                resp = requests.get(CMC_PRICE_URL, headers=HEADERS, params={"id": ids_str, "convert": convert}, timeout=10)
                resp.raise_for_status()
                data = resp.json().get("data", {})

                for cid, info in data.items():
                    quote = info.get("quote", {}).get(convert, {})
                    all_prices[cid] = quote.get("price")

                time.sleep(1.2)  # tránh rate limit
                break  # thành công, thoát retry

            except Exception as e:
                attempt += 1
                log.error(f"[ERROR] Khi lấy giá batch {ids_str}, attempt {attempt}: {e}")
                time.sleep(3)
                if attempt == 3:
                    log.warning(f"[WARN] Bỏ batch này sau 3 lần thất bại: {ids_str}")

    # mapping ngược token_address -> price
    result = {token_addr: all_prices.get(cmc_id) for token_addr, cmc_id in addr_to_id.items()}

    log.info(f"✅ Lấy giá xong, có {len(result)} token có giá")
    return result

# ==== RUN ====
if __name__ == "__main__":
    update_token_cmc_map()
