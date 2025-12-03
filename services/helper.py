from datetime import datetime
from web3 import Web3
import time
from services.db_connect import get_connection
import re

def to_datetime_safe(s):
    if isinstance(s, str) and s:
        return datetime.strptime(s, "%Y-%m-%d")

def safe_api_call(func, *args, default=None, **kwargs):
    """Wrapper an toàn cho các API call, tránh crash nếu lỗi xảy ra"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"⚠️ Lỗi API khi gọi {func.__name__}: {e}")
        return default

def safe_to_timestamp(time_str: str, fmt: str = "%m-%d-%Y %H:%M:%S") -> int:
    """Chuyển chuỗi thời gian sang timestamp, nếu lỗi trả về 0"""
    if not time_str:
        return 0
    try:
        return int(datetime.strptime(time_str, fmt).timestamp())
    except Exception as e:
        print(f"⚠️ Lỗi convert time '{time_str}': {e}")
        return 0

def safe_to_timestamp_with_fallback(
    time_str: str,
    nft_id: int,
    chain_name: str,
    wallet_address: str,
    column_name: str,
    fmt: str = "%m-%d-%Y %H:%M:%S",
    max_age_hours: int = 48
) -> int:
    """Convert time_str -> timestamp, fallback sang snapshot DB gần nhất nếu lỗi"""
    ts = safe_to_timestamp(time_str, fmt)
    if ts > 0:
        return ts

    # Nếu time_str invalid thì fallback
    try:
        db_conn = get_connection()
        cursor = db_conn.cursor(dictionary=True)
        
        sql = f"""
            SELECT {column_name}
            FROM wallet_nft_position
            WHERE nft_id = %s
              AND chain = %s
              AND wallet_address = %s
              AND {column_name} > '1970-01-01 08:00:00'
              AND {column_name} IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """
        cursor.execute(sql, (nft_id, chain_name, wallet_address))
        result = cursor.fetchone()
        cursor.close()
        
        print(f"⚠️ Fallback {column_name} từ DB: {result}")
        
        if result:
            value = result.get(column_name)
            print(f"DEBUG value from DB: {value}, type={type(value)}")
            if not value:
                print(f"⚠️ {column_name} trong snapshot DB = None, bỏ qua fallback")
                return 0  # Không có dữ liệu hợp lệ để fallback

            if isinstance(value, datetime):
                ts_snap = int(value.timestamp())
                print(f"DEBUG ts_snap from datetime: {ts_snap}, type={type(ts_snap)}")
            else:
                ts_snap = safe_to_timestamp(str(value), "%Y-%m-%d %H:%M:%S")
                print(f"DEBUG ts_snap from string: {ts_snap}, type={type(ts_snap)}")

            if ts_snap > 0:
                print(f"✅ Fallback {column_name} từ snapshot gần nhất ({value})")
                return ts_snap
            else:
                print(f"⚠️ Không có snapshot DB hợp lệ, bỏ qua fallback")
                return 0
        else:
            print(f"⚠️ Không có snapshot DB, bỏ qua fallback")
            return 0

    except Exception as e:
        print(f"⚠️ Lỗi DB fallback {column_name}: {e}")
        return 0

def call_with_fallback(contract_function, rpc_list, contract_abi, max_retries=3, delay=1, w3_main=None):
    """
    Thực hiện contract_function.call() với fallback RPC và retry khi lỗi.
    Nếu tất cả RPC fail, trả về None để tiếp tục xử lý các NFT khác.
    w3_main: Web3 instance của RPC chính (nếu contract_function không có .web3)
    """
    # danh sách RPC: RPC chính (None) + backup
    rpc_candidates = [None] + rpc_list

    for rpc in rpc_candidates:
        # xác định Web3 instance
        if rpc:
            w3 = Web3(Web3.HTTPProvider(rpc))
            rpc_name = f"backup {rpc}"
        else:
            if w3_main is None:
                print("⚠️ No Web3 instance provided for main RPC, skip main RPC")
                continue  # bỏ qua RPC chính
            w3 = w3_main
            rpc_name = "main"

        # rebuild contract với ABI đầy đủ
        contract = w3.eth.contract(
            address=contract_function.address,
            abi=contract_abi
        )
        fn_name = contract_function.fn_name
        args = contract_function.args
        fn = getattr(contract.functions, fn_name)(*args)

        # retry call
        for attempt in range(1, max_retries + 1):
            try:
                result = fn.call()
                return result
            except Exception as e:
                print(f"⚠️ RPC {rpc_name} attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    print(f"❌ RPC {rpc_name} all retries failed, moving to next RPC.")
                    break

    print("❌ All RPC endpoints failed for this call. Returning None.")
    return None

def normalize_symbol(symbol: str) -> str:
    """
    Chuẩn hóa symbol Binance Futures:
    - Loại bỏ hậu tố stablecoin USDT, USD, BUSD, FDUSD
    - Giữ nguyên _PERP để phân biệt COIN-M
    """
    s = symbol.upper()
    s = re.sub(r"(USDT|FDUSD|BUSD|USD)$", "", s)
    return s.strip()

def merge_summary(onchain_data, binance_positions):
    summary = []

    # Map on-chain theo token
    onchain_map = {d["token"].upper(): d for d in onchain_data}

    # Map Binance positions theo token → list giữ tất cả futures_type
    binance_map = {}
    for p in binance_positions:
        token = normalize_symbol(p["symbol"])
        binance_map.setdefault(token, []).append(p)

    # 1️⃣ Duyệt tất cả Binance positions trước
    used_tokens = set()
    for token, positions in binance_map.items():
        for pos in positions:
            onchain = onchain_map.get(token, {})
            summary.append({
                "token": token,
                "initial": onchain.get("initial", 0),
                "current": onchain.get("current", 0),
                "delta": onchain.get("delta", 0),
                "delta_usd": onchain.get("delta_usd", 0),
                "fee": onchain.get("fee", 0),
                "binance_side": pos.get("position_side"),
                "binance_amount": pos.get("position_amt"),
                "entry_price": pos.get("entry_price"),
                "mark_price": pos.get("mark_price"),
                "leverage": pos.get("leverage"),
                "unrealized_pnl": pos.get("unrealized_pnl"),
                "margin_type": pos.get("margin_type"),
                "futures_type": pos.get("futures_type")
            })
        used_tokens.add(token)

    # 2️⃣ Thêm các token on-chain chưa xuất hiện trong Binance positions
    for token, onchain in onchain_map.items():
        if token in used_tokens:
            continue
        summary.append({
            "token": token,
            "initial": onchain.get("initial", 0),
            "current": onchain.get("current", 0),
            "delta": onchain.get("delta", 0),
            "delta_usd": onchain.get("delta_usd", 0),
            "fee": onchain.get("fee", 0),
            "binance_side": None,
            "binance_amount": 0,
            "entry_price": 0,
            "mark_price": 0,
            "leverage": 0,
            "unrealized_pnl": 0,
            "margin_type": None,
            "futures_type": None
        })

    return summary