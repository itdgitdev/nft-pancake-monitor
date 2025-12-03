import requests
import time
from services.db_connect import get_connection
from config import CHAIN_ID_MAP
from logging_config import api_logger as log

def get_list_farms_data(chain_id, retries=6, delay=3):
    API_URL = f"https://configs.pancakeswap.com/api/data/cached/farms?chainId={chain_id}&protocol=v3"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(API_URL, headers=headers)
            response.raise_for_status()  # Check error HTTP
            data = response.json()

            if any("pid" in item for item in data):
                return data
            else:
                log.error("❌ Farms data not found")
                return None
        except requests.exceptions.RequestException as e:
            log.warning(f"⚠️ Attempt {attempt+1}: {e}")
            time.sleep(delay)

    log.error("❌ All retry attempts failed.")
    return None

def get_price_tokens_pancake(chain_id, token_address, retries=6, delay=3):
    API_URL = f"https://wallet-api.pancakeswap.com/v1/prices/list/{chain_id}%3A{token_address}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    for attempt in range(retries):
        try:
            response  = requests.get(API_URL, headers=headers)
            response.raise_for_status()  # Check error HTTP
            data = response.json()
            
            if f"{chain_id}:{token_address}" in data:
                price = data[f"{chain_id}:{token_address}"]
                log.info(f"💰 PancakeSwap price for {token_address}: {price}")
                return price
            else:
                log.error("❌ Token price data not found")
                return 0
        except requests.exceptions.RequestException as e:   
            log.warning(f"⚠️ Attempt {attempt+1}: {e}")
            time.sleep(delay)

    log.error("❌ All retry attempts failed.")
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
        log.info(f"💰 PancakeSwap price for {token_address}: {price}")
        return float(price)
    else:
        log.error("❌ Token price data not found for Solana")
        return 0
    
def get_price_tokens_coingecko(chain_name, token_address):
    PLATFORM_MAP = {
        "BNB": "binance-smart-chain",
        "ETH": "ethereum",
        "POL": "polygon-pos",
        "ARB": "arbitrum-one",
        "LIN": "linea",
        "BAS": "base",
        "SOL": "solana"
    }

    platform = PLATFORM_MAP.get(chain_name)
    if not platform:
        log.error(f"❌ Unsupported chain_id for CoinGecko: {chain_name}")
        return 0

    url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}?contract_addresses={token_address}&vs_currencies=usd"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if chain_name == "SOL":
            price = data.get(token_address, {}).get("usd", 0)
        else:
            price = data.get(token_address.lower(), {}).get("usd", 0)
            
        log.info(f"💰 CoinGecko price for {token_address}: {price}")
        return price
    except Exception as e:
        log.error(f"❌ CoinGecko Error: {e}")
        return 0

def get_token_price_token_by_cmc(chain, token_address, convert: str = "USD"):
    """
    Lấy giá token hiện tại từ CoinMarketCap thông qua CMC ID đã lưu trong DB.
    Trả về dict dạng {token_address: price}.
    """
    CMC_PRICE_URL = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT cmc_id FROM token_cmc_map WHERE cmc_id IS NOT NULL and chain=%s and token_address=%s", (chain, token_address))
    row = cursor.fetchone()

    if not row:
        log.warning(f"[WARN] Không tìm thấy CMC ID cho token: {token_address}")
        return None

    cmc_id = str(row["cmc_id"])

    for attempt in range(3):
        try:
            resp = requests.get(
                CMC_PRICE_URL,
                headers=HEADERS,
                params={"id": cmc_id, "convert": convert},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            if cmc_id in data:
                quote = data[cmc_id].get("quote", {}).get(convert, {})
                price = quote.get("price")
                log.info(f"✅ {token_address} (CMC {cmc_id}) = {price:.6f} {convert}")
                return price
            else:
                log.warning(f"[WARN] Không có dữ liệu cho CMC ID {cmc_id}")
                return None

        except Exception as e:
            log.error(f"[ERROR] Khi lấy giá {token_address}, attempt {attempt+1}: {e}")
            time.sleep(2)

    log.error(f"[FAIL] Bỏ qua token {token_address} sau 3 lần retry")
    return None

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
        log.error(f"❌ DexScreener API error: {e}")
        return 0

    pairs = data.get("pairs", [])
    if not pairs:
        log.error("❌ No pairs found on DexScreener")
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
        log.info(f"💰 DexScreener price for {token_address}: {price} USD (DEX={dex_id}, Liquidity=${liq:,.0f})")
        return price
    else:
        log.error("❌ Price not found in selected pair")
        return 0

API_CMC_KEY = "431ffea7-d90a-47db-843a-90e08887b28d"
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
                log.warning(f"[WARN] Khi lấy giá batch {ids_str}, attempt {attempt}: {e}")
                time.sleep(3)
                if attempt == 3:
                    log.error(f"[ERROR] Bỏ batch này sau 3 lần thất bại: {ids_str}")

    # mapping ngược token_address -> price
    result = {token_addr: all_prices.get(cmc_id) for token_addr, cmc_id in addr_to_id.items()}

    log.info(f"✅ Lấy giá xong, có {len(result)} token có giá")
    return result

# cache lưu: {(chain_id, token_address): (price, timestamp)}
_price_cache = {}
CACHE_TTL = 600  # giây (10 phút)

# global dict lưu giá từ CMC, update định kỳ khi cần
_cmc_prices = {}

def update_cmc_prices(convert="USD"):
    """
    Cập nhật giá tất cả token từ CoinMarketCap.
    Lưu vào _cmc_prices: {token_address: price}
    """
    global _cmc_prices
    _cmc_prices = get_token_prices_by_address(convert=convert)
    log.info(f"✅ Updated CMC prices ({len(_cmc_prices)} token)")

def get_price_tokens(chain_name, token_address, convert="USD"):
    """
    Lấy giá token ưu tiên từ CMC, fallback sang Coingecko nếu không có.
    Tự động cập nhật _cmc_prices nếu lần đầu chưa có dữ liệu.
    """
    
    now = time.time()
    cache_key = (chain_name, token_address)

    # 1️⃣ Check cache
    if cache_key in _price_cache:
        cached_price, ts = _price_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached_price

    # 2️⃣ Check CMC giá
    global _cmc_prices
    if not _cmc_prices:
        log.warning("ℹ️ _cmc_prices rỗng, tự động cập nhật từ CMC...")
        update_cmc_prices(convert=convert)

    price = _cmc_prices.get(token_address)

    # 3️⃣ Fallback Coingecko nếu CMC không có giá
    if price is None:
        log.warning(f"🔁 Fallback to Coingecko for {token_address}")
        price = get_price_tokens_coingecko(chain_name, token_address)

    # 4️⃣ Lưu vào cache
    _price_cache[cache_key] = (price, now)

    return price

#API_URL = f"https://explorer.pancakeswap.com/api/cached/pools/{chain}/{pool_address}"
def get_data_pool_bsc(chain, pool_address, retries=6, delay=3):
    API_URL = f"https://explorer.pancakeswap.com/api/cached/pools/{chain}/{pool_address}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(retries):
        try:
            response = requests.get(API_URL, headers=headers)
            response.raise_for_status()
            data = response.json()

            if "id" in data:
                return data
            else:
                log.error("❌ Pool data not found")
                return None

        except requests.exceptions.RequestException as e:
            log.warning(f"⚠️ Attempt {attempt+1}: {e}")
            time.sleep(delay)

    log.error("❌ All retry attempts failed.")
    return None

# Get datas of apr pool
def get_data_pool_apr(chain_api, pool_address, retries=6, delay=3):
    API_URL = f"https://explorer.pancakeswap.com/api/cached/pools/apr/v3/{chain_api}/{pool_address}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(retries):
        try:
            response = requests.get(API_URL, headers=headers)
            response.raise_for_status()
            data = response.json()

            if "apr24h" in data:
                return data
            else:
                log.error("❌ APR data not found in response.")
                return None

        except requests.exceptions.HTTPError as http_err:
            log.warning(f"⚠️ HTTP error (attempt {attempt+1}): {http_err}")
        except requests.exceptions.RequestException as e:
            log.warning(f"⚠️ Request exception (attempt {attempt+1}): {e}")
        except ValueError as e:
            log.error(f"❌ Failed to parse JSON (attempt {attempt+1}): {e}")
        
        time.sleep(delay)

    log.error("❌ All retry attempts failed.")
    return None

# Get CAKE price USD
def get_cake_price_usd():
    try:
        response = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=CAKEUSDT", timeout=10)
        response.raise_for_status()
        return float(response.json().get("price", 0))
    except Exception as e:
        log.error(f"❌ Error getting CAKE price: {e}")
        return 0

# cache dict
_price_cache = {}
CACHE_TTL = 600  # 5 phút

# Get market token price from apebondapi
def get_token_price_by_apebond_api(token_address):
    now = time.time()
    
    # 🔹 Check cache trước
    if token_address in _price_cache:
        cached_price, ts = _price_cache[token_address]
        log.info(f"💰 Price from cache for {token_address}: {cached_price}")
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
        "chain": 7565164
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        
        if result and isinstance(result, list) and "price" in result[0]:
            token_price = float(result[0]['price'])
        else:
            token_price = None
    except Exception:
        token_price = None
        
    # 🔹 Coingecko fail → fallback sang solana pancake api
    if not token_price or token_price == 0:
        try:
            token_price = get_token_price_solana_pancake(token_address)
        except Exception:
            token_price = None

    # 🔹 Nếu ApeBond fail → fallback sang CoinGecko
    if not token_price or token_price == 0:
        try:
            token_price = get_price_tokens_coingecko("SOL", token_address)
        except Exception:
            token_price = None

    # 🔹 Lưu cache
    if token_price:
        _price_cache[token_address] = (token_price, now)
    
    return token_price