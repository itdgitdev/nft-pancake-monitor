from create_db import get_connection
import mysql.connector
import requests
import time
import math
import threading
from config import CHAIN_ID_MAP

DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1385563895850078340/HjXe2bFPkBgdGBMalvRIUMDNgl4mazFvyaJIXs7LRHb66Z2xtOsPMoJVUGCuZLyqF6_T"
# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1377961748925124681/4L4i0oxq6PD1jLlBUV2IxH-G2vobb-ESm2VhKWL30dQztF4sRVg8IkgOoWe4W2EB0IFS"

def get_max_alloc_point_per_chain(chain):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT p.*
            FROM pool_info p
            JOIN (
                SELECT chain, MAX(alloc_point) AS max_alloc
                FROM pool_info
                GROUP BY chain
            ) m ON p.chain = m.chain AND p.alloc_point = m.max_alloc
            WHERE p.alloc_point > 0 AND p.chain = %s;
        """
        cursor.execute(query, (chain,))
        result = cursor.fetchone()
        return result if result else None
    
    except mysql.connector.Error as e:
        print(f"Error fetching max alloc point: {e}")
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
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
def get_total_cake_reward_1h_pool(chain, pool_address):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            WITH ranked AS (
                SELECT
                    t1.nft_id,
                    t1.wallet_address,
                    t1.pool_address,
                    t1.chain,
                    t1.status,
                    CAST(t1.cake_reward_1h AS DECIMAL(30,18)) AS cake_reward_1h,
                    CAST(t1.pending_cake AS DECIMAL(30,18)) AS pending_cake,
                    t1.created_at,
                    LAG(CAST(t1.pending_cake AS DECIMAL(30,18))) 
                        OVER (PARTITION BY t1.nft_id ORDER BY t1.created_at ASC) AS prev_pending_cake,
                    ROW_NUMBER() OVER (PARTITION BY t1.nft_id ORDER BY t1.created_at DESC) AS rn
                FROM wallet_nft_position t1
                LEFT JOIN nft_blacklist b
                    ON t1.wallet_address = b.wallet_address
                AND t1.chain = b.chain
                AND t1.nft_id = b.nft_id
                WHERE b.id IS NULL
                AND t1.chain = %s
                AND t1.pool_address = %s
            )
            SELECT
                COUNT(*) AS total_nft_latest,
                SUM(
                    CASE
                        WHEN prev_pending_cake IS NOT NULL 
                            AND (pending_cake - prev_pending_cake) > 0
                        THEN (pending_cake - prev_pending_cake)
                        ELSE 0
                    END
                ) AS total_delta_pending_positive,
                SUM(cake_reward_1h) AS total_cake_reward_latest
            FROM ranked
            WHERE rn = 1
                AND status != 'Burned';
        """
        cursor.execute(query, (chain, pool_address))
        result = cursor.fetchone()

        if result and result['total_cake_reward_latest'] is not None and result['total_delta_pending_positive'] is not None:
            return result['total_cake_reward_latest'], result['total_nft_latest'], result['total_delta_pending_positive']
        else:
            return 0.0, 0, 0.0

    except mysql.connector.Error as e:
        print(f"❌ Error fetching total cake reward for pool {pool_address}: {e}")
        return 0.0, 0

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

def get_total_current_liquidity_on_pool(chain, pool_address):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            WITH latest_nft AS (
                SELECT t1.nft_id,
                    t1.status,
                    t1.current_total_value,
                    t1.pool_address,
                    t1.chain
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
                WHERE b.id IS NULL AND t1.status != 'Burned'
            )
            SELECT 
                SUM(latest_nft.current_total_value) AS total_current_liquidity,
                COUNT(*) AS total_nft
            FROM latest_nft
            WHERE latest_nft.status != 'Burned'
            AND latest_nft.chain = %s
            AND latest_nft.pool_address = %s;
        """
        cursor.execute(query, (chain, pool_address))
        result = cursor.fetchone()

        if result and result['total_current_liquidity'] is not None:
            return result['total_current_liquidity'], result['total_nft']
        else:
            return 0.0, 0

    except mysql.connector.Error as e:
        print(f"❌ Error fetching total cake reward for pool {pool_address}: {e}")
        return 0.0, 0

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def _normalize_discord_webhook_url(discord_webhook_url: str) -> str:
    url = (discord_webhook_url or "").strip()
    if url.startswith("https://discordapp.com/"):
        url = url.replace("https://discordapp.com/", "https://discord.com/", 1)
    return url


def notify_discord(message, discord_webhook_url, retries=3):
    webhook_url = _normalize_discord_webhook_url(discord_webhook_url)
    if not webhook_url:
        print("❌ Discord notify skipped: empty webhook URL")
        return False

    data = {"content": message}
    for attempt in range(retries):
        try:
            response = requests.post(webhook_url, json=data, timeout=15)
            
            # ✅ Thành công
            if response.status_code == 204:
                return True

            # ⚠️ Bị rate limit
            if response.status_code == 429:
                error_data = response.json()
                retry_after = error_data.get("retry_after", 1)
                print(f"⏳ Discord rate limited. Retry after {retry_after}s")
                time.sleep(retry_after)
                continue  # thử lại sau sleep

            # 🔁 Lỗi upstream / tạm thời từ Discord hoặc mạng trung gian
            if response.status_code >= 500:
                print(f"⚠️ Discord upstream error ({response.status_code}), retry {attempt + 1}/{retries}: {response.text}")
                time.sleep(min(2 ** attempt, 5))
                continue

            # ❌ Lỗi khác
            print(f"❌ Discord notify failed ({response.status_code}): {response.text}")
            return False

        except requests.RequestException as e:
            print(f"⚠️ Discord exception retry {attempt + 1}/{retries}: {e}")
            time.sleep(min(2 ** attempt, 5))

    return False

# Get CAKE price USD
def get_cake_price_usd():
    try:
        response = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=CAKEUSDT", timeout=10)
        response.raise_for_status()
        return float(response.json().get("price", 0))
    except Exception as e:
        print(f"❌ Error getting CAKE price: {e}")
        return 0

def get_price_tokens_coingecko(chain_id, token_address):
    PLATFORM_MAP = {
        "BNB": "binance-smart-chain",
        "ETH": "ethereum",
        "POL": "polygon-pos",   
        "ARB": "arbitrum-one",
        "LIN": "linea",
        "BAS": "base",
        "SOL": "solana"
    }

    platform = PLATFORM_MAP.get(chain_id)
    if not platform:
        print(f"❌ Unsupported chain_id for CoinGecko: {chain_id}")
        return 0

    url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}?contract_addresses={token_address}&vs_currencies=usd"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if chain_id == 7565164:
            price = data.get(token_address, {}).get("usd", 0)
        else:
            price = data.get(token_address.lower(), {}).get("usd", 0)
            
        print(f"💰 CoinGecko price for {token_address}: {price}")
        return price
    except Exception as e:
        print(f"❌ CoinGecko Error: {e}")
        return 0
    
def get_token_price_solana_pancake(token_address):
    API_URL = f"https://sol-explorer.pancakeswap.com/api/cached/v1/tokens/price?ids={token_address}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    response = requests.get(API_URL, headers=headers)
    response.raise_for_status()  # Check error HTTP
    response_js = response.json()
    data = response_js.get("data")
    
    if token_address in data:
        price = data[token_address]
        print(f"💰 PancakeSwap price for {token_address}: {price}")
        return float(price)
    else:
        print("❌ Token price data not found")
        return 0
    
def get_token_price_evm_pancake(token_address, chain):
    API_URL = f"https://wallet-api.pancakeswap.com/v1/prices/list/{CHAIN_ID_MAP[chain]}%3A{token_address}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        response_js = response.json()
        
        key = f"{CHAIN_ID_MAP[chain]}:{token_address.lower()}"
        price = response_js.get(key)
        
        if price is not None:
            print(f"💰 PancakeSwap price for {token_address} on {chain}: {price}")
            return float(price)
        else:
            print(f"❌ Token {token_address} not found in PancakeSwap EVM API response")
            return 0
    
    except Exception as e:
        print(f"❌ PancakeSwap API error for {token_address} on {chain}: {e}")
        return 0
    
def get_token_price_from_dexscreener(token_address, min_liquidity_usd=1000):
    """
    Lấy giá token từ DexScreener API.
    Ưu tiên dexId uy tín và pool có liquidity lớn nhất.
    """

    API_URL = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(API_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ DexScreener API error: {e}")
        return 0

    pairs = data.get("pairs", [])
    if not pairs:
        print("❌ No pairs found on DexScreener")
        return 0

    # ✅ Danh sách dex uy tín (có thể mở rộng thêm)
    trusted_dex = {"pancakeswap", "aerodrome", "uniswap", "sushiswap"}

    # Lọc theo dex uy tín + liquidity đủ lớn
    filtered = [
        p for p in pairs
        if p.get("dexId") in trusted_dex and p.get("liquidity", {}).get("usd", 0) > min_liquidity_usd
    ]

    if not filtered:
        # Nếu không có dex uy tín thì fallback: chọn pool liquidity cao nhất bất kỳ
        filtered = sorted(pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0), reverse=True)[:1]

    # Chọn pool có liquidity lớn nhất trong danh sách còn lại
    best_pair = max(filtered, key=lambda x: x.get("liquidity", {}).get("usd", 0))
    price = best_pair.get("priceUsd")

    if price:
        price = float(price)
        dex_id = best_pair.get("dexId")
        liq = best_pair.get("liquidity", {}).get("usd", 0)
        print(f"💰 DexScreener price for {token_address}: {price} USD (DEX={dex_id}, Liquidity=${liq:,.0f})")
        return price
    else:
        print("❌ Price not found in selected pair")
        return 0
    
# API_CMC_KEY = "431ffea7-d90a-47db-843a-90e08887b28d"
API_CMC_KEY = "6db1422fd02046ae915c39c0660b0997"
HEADERS = {"X-CMC_PRO_API_KEY": API_CMC_KEY}

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
    print(f"📦 Tìm thấy {len(addr_to_id)} token trong token_cmc_map")

    # tạo list cmc_id (không loại trùng)
    cmc_id_list = list(addr_to_id.values())
    print(f"📦 Tổng cộng {len(cmc_id_list)} CMC ID (bao gồm trùng)")
    
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
                print(f"[ERROR] Khi lấy giá batch {ids_str}, attempt {attempt}: {e}")
                time.sleep(3)
                if attempt == 3:
                    print(f"[WARN] Bỏ batch này sau 3 lần thất bại: {ids_str}")

    # mapping ngược token_address (lower) -> price
    result = {token_addr.lower(): all_prices.get(cmc_id) for token_addr, cmc_id in addr_to_id.items()}

    print(f"✅ Lấy giá xong, có {len(result)} token có giá")
    return result

# cache lưu: {(chain_id, token_address): (price, timestamp)}
_price_cache = {}
CACHE_TTL = 600  # giây (10 phút)

# Lock cho phép truy cập thread-safe vào cache
_cache_lock = threading.Lock()

# global dict lưu giá từ CMC, update định kỳ khi cần
_cmc_prices = {}

def update_cmc_prices(convert="USD"):
    """
    Cập nhật giá tất cả token từ CoinMarketCap.
    Lưu vào _cmc_prices: {token_address: price}
    """
    global _cmc_prices
    _cmc_prices = get_token_prices_by_address(convert=convert)
    print(f"✅ Updated CMC prices ({len(_cmc_prices)} token)")

def calc_price_from_tick(tick_current, dec0, dec1, stable_price=0.999, mode="token1_is_stable"):
    ratio = math.pow(1.0001, tick_current) / math.pow(10, dec1 - dec0)
    if mode == "token1_is_stable":
        return ratio * stable_price
    else:
        return ratio / stable_price

def get_price_tokens(chain_name, token_address, tick_current=None, token0_address=None, token1_address=None, dec0=None, dec1=None, convert="USD"):
    """
    Lấy giá token ưu tiên từ CMC, fallback sang Coingecko nếu không có.
    Tự động cập nhật _cmc_prices nếu lần đầu chưa có dữ liệu.
    """
    
    now = time.time()
    token_address_lower = token_address.lower()
    cache_key = (chain_name, token_address_lower)

    # 1️⃣ Check cache
    with _cache_lock:
        if cache_key in _price_cache:
            cached_price, ts = _price_cache[cache_key]
            if now - ts < CACHE_TTL:
                return cached_price

    # 2️⃣ Check CMC giá
    global _cmc_prices
    if not _cmc_prices:
        with _cache_lock:
            if not _cmc_prices: # Double check pattern
                print("ℹ️ _cmc_prices rỗng, tự động cập nhật từ CMC...")
                update_cmc_prices(convert=convert)

    price = _cmc_prices.get(token_address_lower)

    # 3️⃣ Fallback DexScreener nếu CMC không có giá
    if price is None or price == 0 and token_address_lower != "0x55d398326f99059ff775485246999027b3197955":
        print(f"🔁 Fallback to DexScreener for {token_address_lower}")
        price = get_token_price_from_dexscreener(token_address_lower)

    # 4️⃣ Fallback Coingecko nếu DexScreener không có
    if price is None or price == 0:
        print(f"🔁 Fallback to Coingecko for {token_address_lower}")
        price = get_price_tokens_coingecko(chain_name, token_address_lower)

    # 5️⃣ Last Fallback: On-chain Tick Calculation
    if price == 0 and tick_current and dec0 and dec1:
        print(f"🚨 API Fail. Calculating price from Tick for {token_address_lower} on {chain_name}")
        
        # Lấy giá của token đối ứng trong pool để làm mốc (Anchor)
        t0_low = token0_address.lower() if token0_address else None
        t1_low = token1_address.lower() if token1_address else None
        
        # Xác định giá mốc từ CMC hoặc Coingecko của token đối ứng
        p0_anchor = _cmc_prices.get(t0_low) if t0_low else None
        p1_anchor = _cmc_prices.get(t1_low) if t1_low else None

        # TRƯỜNG HỢP 1: Token1 là Anchor (Ví dụ cặp SOL/USDC hoặc SOL/ZORA mà đã biết giá SOL)
        if p1_anchor is not None:
            price = calc_price_from_tick(tick_current, dec0, dec1, stable_price=p1_anchor, mode="token1_is_stable")
            print(f"💡 Calculated via Token1 anchor ({t1_low}): {price}")
            
        # TRƯỜNG HỢP 2: Token0 là Anchor (Ví dụ cặp USDC/ZORA)
        elif p0_anchor is not None:
            ratio = math.pow(1.0001, tick_current) / math.pow(10, dec1 - dec0)
            price = p0_anchor / ratio
            print(f"💡 Calculated via Token0 anchor ({t0_low}): {price}")
            
        else:
            print(f"❌ Cannot calculate price from tick because no anchor price available for {token_address_lower}")
            price = None

    if price is not None:
        with _cache_lock:
            _price_cache[cache_key] = (price, now)

    return price or 0

    
# cache dict
# _price_cache = {}
# CACHE_TTL = 1800  # 5 phút

# Get market token price from apebondapi
def get_token_price_by_apebond_api(token_address, chain):
    now = time.time()
    
    # 🔹 Check cache trước
    if token_address in _price_cache:
        cached_price, ts = _price_cache[token_address]
        print(f"💰 Price from cache for {token_address}: {cached_price}")
        if now - ts < CACHE_TTL:
            return cached_price 
    
    url = "https://price-api.ape.bond/prices"
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.ape.bond",
        "Referer": "https://www.ape.bond/",
        "User-Agent": "Mozilla/5.0"
    }

    payload = {
        "rpcUrl": "string",
        "tokens": [
            f"{token_address}"
        ],
        "chain": int(CHAIN_ID_MAP[chain])
    }

    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()
    print(result)
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        
        if result and isinstance(result, list) and "price" in result[0]:
            token_price = float(result[0]['price'])
            print(f"💰 Price from ApeBondAPI for {token_address}: {token_price}")
        else:
            token_price = None
    except Exception:
        token_price = None

    # 🔹 Coingecko fail → fallback sang solana pancake api
    if not token_price or token_price == 0:
        if not token_address.startswith("0x"):
            try:
                token_price = get_token_price_solana_pancake(token_address)
            except Exception:
                token_price = None
        else:
            try:
                token_price = get_token_price_evm_pancake(token_address, chain)
            except Exception:
                token_price = None

    # 🔹 Nếu ApeBond fail → fallback sang CoinGecko
    if not token_price or token_price == 0:
        try:
            token_price = get_price_tokens_coingecko(int(CHAIN_ID_MAP[chain]), token_address)
        except Exception:
            token_price = None
    
    # 🔹 CoinGecko fail → fallback sang dexscreener
    if not token_price or token_price == 0:
        try:
            token_price = get_token_price_from_dexscreener(token_address)
        except Exception:
            token_price = None

    # 🔹 Lưu cache
    if token_price:
        _price_cache[token_address] = (token_price, now)
    
    return token_price

def get_list_pool_actived_farm_by_api_pancake(chain):
    if chain not in CHAIN_ID_MAP:
        print(f"❌ Chain {chain} not supported in CHAIN_ID_MAP")
        return None
    
    API_URL = f"https://configs.pancakeswap.com/api/data/cached/farms?chainId={CHAIN_ID_MAP[chain]}&protocol=v3"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        response_js = response.json()
        
        if not isinstance(response_js, list) or len(response_js) == 0:
            print("❌ Unexpected PancakeSwap API response format")
            return None
        
        pools = {
            farm.get("pid"): farm.get("lpAddress", "").lower()
            for farm in sorted(response_js, key=lambda x: x.get("pid", 0))
            if farm.get("pid") is not None and farm.get("lpAddress")
        }
        return pools
    
    except Exception as e:
        print(f"❌ PancakeSwap API error fetching farms on {chain}: {e}")
        return None

    