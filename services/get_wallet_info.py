# import sys
# import os
# # Lấy path tới root của project
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
# sys.path.append(PROJECT_ROOT)

import struct
from solders.pubkey import Pubkey
from solana.rpc.api import Client
from solana.rpc.types import TokenAccountOpts
from datetime import datetime, timezone
import json
import math
import requests
from datetime import datetime, timedelta
import time
from services.execute_data import *
from services.update_query import *
from services.pancake_api import *
from services.solana.decode_account import *
from config import *
from services.update_query import get_pool_sol_info

RPC_ENDPOINTS = [
    "https://mainnet.helius-rpc.com/?api-key=bb4fcdca-d41d-4930-ada1-6490968dabe4",
    "https://api.mainnet-beta.solana.com"
]

def safe_call(func, *args, retries=4, delay=1, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ RPC call failed ({i+1}/{retries}): {e}")
            time.sleep(delay)
    return None

from solana.rpc.api import Client

RPC_ENDPOINTS = [
    "https://mainnet.helius-rpc.com/?api-key=bb4fcdca-d41d-4930-ada1-6490968dabe4",
    "https://api.mainnet-beta.solana.com"
]

def get_client_with_fallback():
    for url in RPC_ENDPOINTS:
        try:
            client = Client(url, timeout=10)
            resp = client.get_version()
            
            if resp and getattr(resp, "value", None):
                print(f"✅ Using RPC: {url}")
                return client
            else:
                print(f"⚠️ RPC {url} returned no value")
        except Exception as e:
            print(f"❌ RPC {url} failed: {e}")
            continue
    
    raise Exception("❌ No working RPC found")

def get_token_account_nft(client, mint_address):
    try:
        resp = safe_call(client.get_token_largest_accounts, mint_address)
        resp = getattr(resp, "value", [])
    except Exception as e:
        print(f"⚠️ Primary client failed: {e}")
        resp = []

    if not resp:  # fallback nếu lỗi hoặc empty
        alt_client = get_client_with_fallback()
        resp = safe_call(alt_client.get_token_largest_accounts, mint_address)
        resp = getattr(resp, "value", [])

    if not resp:
        raise Exception(f"❌ No token accounts found for mint {mint_address}")

    return resp[0].address

def get_all_signatures(client, address: str, max_limit: int = 1000):
    signatures = []
    before = None

    while True:
        # thử call với client chính
        try:
            resp = safe_call(client.get_signatures_for_address, address, limit=1000, before=before)
            resp = resp.value
        except Exception:
            # fallback sang client khác
            alt_client = get_client_with_fallback()
            resp = safe_call(alt_client.get_signatures_for_address, address, limit=1000, before=before)
            resp = resp.value

        if not resp:
            break

        signatures.extend(resp)
        before = resp[-1].signature

        if len(resp) < 1000 or len(signatures) >= max_limit:
            break

    return [s.signature for s in signatures]

def get_tx_logs(client, signature: str):
    try:
        # thử với client chính
        resp = safe_call(
            client.get_transaction,
            signature,
            max_supported_transaction_version=0,
            encoding="jsonParsed"
        )
    except Exception as e:
        print(f"⚠️ Primary client failed for tx {signature}, fallback...: {e}")
        alt_client = get_client_with_fallback()
        resp = safe_call(
            alt_client.get_transaction,
            signature,
            max_supported_transaction_version=0,
            encoding="jsonParsed"
        )

    # validate response
    if not resp or not hasattr(resp, "value"):
        return None

    if resp.value is None:
        return None  # Transaction không tồn tại hoặc chưa finalised

    return json.loads(resp.value.to_json())

def get_initial_timestamp(client, mint_address):
    try:
        # thử với client chính
        resp = safe_call(client.get_signatures_for_address, mint_address, limit=1)
    except Exception as e:
        print(f"⚠️ Primary client failed for {mint_address}, fallback...: {e}")
        alt_client = get_client_with_fallback()
        resp = safe_call(alt_client.get_signatures_for_address, mint_address, limit=1)

    # validate kết quả
    if resp.value is None or len(resp.value) == 0:
        return None  # không có tx nào

    sig_info = resp.value[0]  # giao dịch mới nhất
    block_time = sig_info.block_time  # Optional[int]

    if block_time is None:
        return None

    dt_utc = datetime.fromtimestamp(block_time, tz=timezone.utc)
    return dt_utc

def find_last_actions(client: Client, address: str, actions=("DecreaseLiquidityV2", "IncreaseLiquidityV2")):
    signatures = get_all_signatures(client, address)
    last_actions = {action: None for action in actions}
    
    for sig in signatures:
        tx_json = get_tx_logs(client, sig)
        if not tx_json:
            continue

        block_time = tx_json.get("blockTime")
        if block_time is None:  # 🚨 bỏ qua nếu không có block time
            continue

        tx_logs = tx_json.get("meta", {}).get("logMessages", [])

        for action in actions:
            if any(action in log for log in tx_logs):
                if last_actions[action] is None or block_time > last_actions[action]:
                    last_actions[action] = block_time

    return last_actions

def detect_liquidity_action(tx_json: dict) -> str | None:
    meta = tx_json.get("meta", {})
    log_messages = meta.get("logMessages", []) or []

    for log in log_messages:
        if "IncreaseLiquidityV2" in log:
            return "increase"
        elif "OpenPositionWithToken22Nft" in log:
            return "increase"
        elif "DecreaseLiquidityV2" in log:
            return "decrease"

    # fallback: check legacy naming (if exists)
    for log in log_messages:
        if "IncreaseLiquidity" in log:
            return "increase"
        elif "OpenPositionWithToken" in log:
            return "increase"
        elif "DecreaseLiquidity" in log:
            return "decrease"

    return None

def parse_amounts_from_logs(tx_json, token0_mint, token1_mint):
    action = detect_liquidity_action(tx_json)
    sign = 1 if action == "increase" else -1 if action == "decrease" else 1
    
    amount0, amount1 = 0, 0
    meta = tx_json.get("meta")
    if not meta:
        return 0, 0

    inner_ixs = meta.get("innerInstructions", [])
    for ix in inner_ixs:
        for inner in ix.get("instructions", []):
            parsed = inner.get("parsed")
            if not parsed:
                continue
            if parsed.get("type") == "transferChecked":
                info = parsed.get("info", {})
                mint = info.get("mint")
                amount = info.get("tokenAmount", {}).get("uiAmount", 0)
                if mint == token0_mint:
                    amount0 += sign * amount
                elif mint == token1_mint:
                    amount1 += sign * amount

    return amount0, amount1

def get_total_increase_decrease(client, token_account, token0_mint, token1_mint, limit=1000):
    total0, total1 = 0, 0
    signatures = get_all_signatures(client, token_account, limit)

    for sig in signatures:
        tx_json = get_tx_logs(client, sig)
        if not tx_json:
            continue
        amt0, amt1 = parse_amounts_from_logs(tx_json, token0_mint, token1_mint)
        # print(f"Token account: {token_account}, token0_mint: {token0_mint}, token1_mint: {token1_mint}, amt0: {amt0}, amt1: {amt1}")
        total0 += amt0
        total1 += amt1

    return total0, total1

def get_position_status(liquidity, tick_lower, tick_upper, current_tick, tokens_owed0, tokens_owed1):
    if liquidity > 0:
        if tick_lower <= current_tick <= tick_upper:
            return "Active"
        else:
            return "Inactive"
    elif tokens_owed0 > 0 or tokens_owed1 > 0:
        return "Unclaimed"
    else:
        return "Closed"

def get_current_amounts(liquidity, sqrt_price_x96, tick_lower, tick_upper):
    sqrt_price = float(sqrt_price_x96) / 2**64
    sqrt_price_lower = math.sqrt(1.0001 ** tick_lower)
    sqrt_price_upper = math.sqrt(1.0001 ** tick_upper)
    print("sqrt_price: ",sqrt_price, "sqrt_price_lower: ",sqrt_price_lower, "sqrt_price_upper: ")
    
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

TICK_ARRAY_SIZE = 60

def get_start_tick_index(tick_index: int, tick_spacing: int) -> int:
    # tick_index phải thuộc về một TickArray range
    return (tick_index // (TICK_ARRAY_SIZE * tick_spacing)) * (TICK_ARRAY_SIZE * tick_spacing)
    
def compute_fee_growth_inside(
    fee_growth_global, 
    fee_growth_outside_lower, 
    fee_growth_outside_upper, 
    tick_current, 
    tick_lower, 
    tick_upper
):
    if tick_current < tick_lower:
        return fee_growth_outside_lower - fee_growth_outside_upper
    elif tick_current >= tick_upper:
        return fee_growth_outside_upper - fee_growth_outside_lower
    else:
        return fee_growth_global - fee_growth_outside_lower - fee_growth_outside_upper

# Get position status
POOL_CACHE = {}

def get_all_status_nft_ids_sol(client, chain_name, owner_nft_ids):
    active_nft_ids, inactive_nft_ids = [], []
    status_map, position_map, pool_map = {}, {}, {}

    for nft_id in owner_nft_ids:
        try:
            nft_id_pubkey = Pubkey.from_string(nft_id)

            # --- Fetch position (retry + fallback) ---
            position_account = safe_call(get_position_account_by_mint, client, str(nft_id_pubkey))
            used_client = client
            if not position_account:
                alt_client = get_client_with_fallback()
                position_account = safe_call(get_position_account_by_mint, alt_client, str(nft_id_pubkey))
                used_client = alt_client
            if not position_account:
                print(f"⚠️ Skip NFT {nft_id}, cannot fetch position_account")
                continue

            position_info = safe_call(decode_personal_position_state, used_client, str(position_account))
            if not position_info:
                print(f"⚠️ Skip NFT {nft_id}, decode position_info failed")
                continue
            position_map[nft_id] = position_info

            # --- Fetch pool with cache ---
            pool_account = position_info.get("pool_id")
            if pool_account in POOL_CACHE:
                pool_info = POOL_CACHE[pool_account]
            else:
                pool_info = safe_call(decode_pool_state, used_client, str(pool_account))
                if not pool_info:
                    alt_client = get_client_with_fallback()
                    pool_info = safe_call(decode_pool_state, alt_client, str(pool_account))
                if not pool_info:
                    print(f"⚠️ Skip NFT {nft_id}, pool_info not available")
                    continue
                POOL_CACHE[pool_account] = pool_info
            pool_map[nft_id] = pool_info

            # --- Extract values ---
            position_status = get_position_status(
                position_info.get("liquidity", 0),
                position_info.get("tick_lower_index", 0),
                position_info.get("tick_upper_index", 0),
                pool_info.get("tick_current", 0),
                position_info.get("token_fees_owed_0", 0),
                position_info.get("token_fees_owed_1", 0)
            )

            status_map[nft_id] = position_status
            if position_status == "Active":
                active_nft_ids.append(nft_id)
            else:
                inactive_nft_ids.append(nft_id)

        except Exception as e:
            import traceback
            print(f"❌ Error processing NFT {nft_id}: {e}\n{traceback.format_exc()}")
            continue

    return active_nft_ids, inactive_nft_ids, status_map, position_map, pool_map


# Send Discord webhook
def send_discord_webhook_message(message: str, webhook_url: str = DISCORD_WEBHOOK_URL):
    data = {"content": message}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(webhook_url, json=data, headers=headers, timeout=5)
        print(f"✅ Discord webhook sent: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send Discord webhook: {e}")

def notify_inactive_nft(nft_id, chain_name, wallet_address, token0_name, token1_name, current_token0_amount, current_token1_amount, current_amount, farm_apr):
    """
    Notify khi NFT position bị chuyển sang trạng thái Inactive.
    """
    # Format số cho đẹp
    current_token0_amount_fmt = f"{current_token0_amount:,.3f}"
    current_token1_amount_fmt = f"{current_token1_amount:,.3f}"
    current_amount_fmt = f"${current_amount:,.2f}"
    farm_apr_fmt = f"{farm_apr:.2f}"

    nft_url = f"https://solscan.io/token/{nft_id}"
    wallet_url = f"https://solscan.io/account/{wallet_address}"
    
    send_discord_webhook_message(
        f'ID [{nft_id[:6]}...{nft_id[-6:]}]({nft_url}) {chain_name}, '
        f'(({token0_name} {current_token0_amount_fmt})-({token1_name} {current_token1_amount_fmt}), {current_amount_fmt}, {farm_apr_fmt})% '
        f'[Wallet {wallet_address[:6]}...{wallet_address[-4:]}]({wallet_url}) ✅ Active ➜ ❌ Inactive.'
    )

def sync_nft_status_sol(
    client,
    wallet_address,
    chain_name,
    owned_token_ids,
    get_all_status_nft_ids_sol,
):

    # 2. Lấy dữ liệu status đã lưu trong DB
    nft_status_data = get_nft_status_data(wallet_address, chain_name)
    db_active_inactive_map = nft_status_data.get("active_inactive_map", {})
    db_closed_nft_ids = nft_status_data.get("closed_ids", [])
    db_blacklist_nft_ids = nft_status_data.get("blacklist_ids", [])

    db_active_nft_ids = [
        nft_id for nft_id, status in db_active_inactive_map.items() if status == "Active"
    ]
    db_inactive_nft_ids = [
        nft_id for nft_id, status in db_active_inactive_map.items() if status == "Inactive"
    ]

    # 3. Lọc bỏ NFT đã Closed trong DB
    cached_closed_set = set(map(str, db_closed_nft_ids))
    filtered_owned_token_ids = [
        nft_id for nft_id in owned_token_ids if str(nft_id) not in cached_closed_set
    ]

    # 4. Lấy trạng thái Active / Inactive on-chain (dùng hàm custom cho Solana)
    active_nft_ids, inactive_nft_ids, status_map, position_map, pool_map = get_all_status_nft_ids_sol(
        client, chain_name, filtered_owned_token_ids
    )

    # 5. Lọc blacklist
    blacklist_set = set(map(str, db_blacklist_nft_ids))
    filtered_active_nft_ids = [nft_id for nft_id in active_nft_ids if str(nft_id) not in blacklist_set]
    filtered_inactive_nft_ids = [nft_id for nft_id in inactive_nft_ids if str(nft_id) not in blacklist_set]

    # 6. Xác định Closed NFT (trên chain không còn Active/Inactive)
    closed_nft_ids = sorted(set(owned_token_ids) - set(active_nft_ids + inactive_nft_ids))
    if closed_nft_ids:
        for nft_id in closed_nft_ids:
            insert_nft_closed_cache(wallet_address, chain_name, nft_id, "Closed")

    # 7. So sánh DB vs on-chain để update Closed
    current_nft_set = set(active_nft_ids + inactive_nft_ids)
    db_active_inactive_set = set(db_active_nft_ids + db_inactive_nft_ids)
    nft_ids_to_closed = db_active_inactive_set - current_nft_set
    for nft_id in nft_ids_to_closed:
        update_nft_status_to_closed(wallet_address, chain_name, nft_id)
        insert_nft_closed_cache(wallet_address, chain_name, nft_id, "Closed")

    # 8. So sánh Active -> Inactive để notify
    filtered_db_active_nft_ids = [nft_id for nft_id in db_active_nft_ids if str(nft_id) not in closed_nft_ids]
    sorted_active_nft_ids = [str(nft_id) for nft_id in filtered_active_nft_ids]

    inactived_nft_ids = [
        nft_id for nft_id in filtered_db_active_nft_ids if str(nft_id) not in sorted_active_nft_ids
    ]

    # for nft_id in inactived_nft_ids:
    #     inactive_data = get_data_inactive_nft_id(nft_id)
    #     if not inactive_data:
    #         print(f"[❌] NFT ID {nft_id} không có dữ liệu.")
    #         continue

    #     token0_name, token1_name, token0_amount, token1_amount, current_amount, farm_apr = inactive_data
    #     nft_url = f"https://solscan.io/token/{nft_id}"
    #     send_discord_webhook_message(
    #         f"ID [{nft_id[:6]}...{nft_id[-6:]}]({nft_url}) {chain_name} (({token0_name} {token0_amount})-({token1_name} {token1_amount}), {current_amount}, {farm_apr})% "
    #         f"✅ Active ➜ ❌ Inactive"
    #     )
        
    return status_map, position_map, pool_map, inactived_nft_ids

def validate_position_and_pool(position_info, pool_info):
    required_position_fields = [
        "liquidity",
        "tick_lower_index",
        "tick_upper_index",
        "token_fees_owed_0",
        "token_fees_owed_1",
        "fee_growth_inside_0_last_x64",
        "fee_growth_inside_1_last_x64",
        "reward_infos",
        "pool_id",
    ]
    required_pool_fields = [
        "tick_current",
        "token_mint_0",
        "token_mint_1",
        "sqrt_price_x64",
        "mint_decimals_0",
        "mint_decimals_1",
        "tick_spacing",
        "fee_growth_global_0_x64",
        "fee_growth_global_1_x64",
        "reward_infos",
    ]

    # check position
    if not position_info or not all(k in position_info for k in required_position_fields):
        return False

    # check pool
    if not pool_info or not all(k in pool_info for k in required_pool_fields):
        return False

    return True

def get_nft_solana_data(wallet_address, token_account_opts, chain_name):
    results = []

    try:
        resp = safe_call(CLIENT.get_token_accounts_by_owner, wallet_address, token_account_opts)
    except Exception as e:
        print(f"❌ Primary CLIENT RPC error get_token_accounts_by_owner for {wallet_address}: {e}")
        # fallback sang RPC khác
        alt_client = get_client_with_fallback()
        resp = safe_call(alt_client.get_token_accounts_by_owner, wallet_address, token_account_opts)

    if not resp or not getattr(resp, "value", None):
        print(f"⚠️ No token accounts found for wallet {wallet_address}")
        return results

    owner_nft_id = []
    for acc in resp.value:
        try:
            mint, owner, amount = parse_token_account(bytes(acc.account.data))
            if amount == 1:
                owner_nft_id.append(str(mint))
        except Exception as e:
            print(f"⚠️ Failed to parse token account {acc}: {e}")
            continue

    print(f"✅ NFT ID: {owner_nft_id}")
    
    latest_nft_id_from_db = get_latest_nft_id_sol_from_db("SOL")
    print(f"✅ Latest NFT ID from DB: {latest_nft_id_from_db}")
    
    burned_nft_ids = [nft_id for nft_id in latest_nft_id_from_db if nft_id not in owner_nft_id]
    print(f"🔥 Burned NFT IDs: {burned_nft_ids}")

    try:
        status_map, position_map, pool_map, inactived_nft_ids = sync_nft_status_sol(
            CLIENT, str(wallet_address), chain_name, owner_nft_id, get_all_status_nft_ids_sol
        )
    except Exception as e:
        print(f"❌ sync_nft_status_sol failed for {wallet_address}: {e}")
        return results

    for nft_id in owner_nft_id:
        try:
            # raw_data = bytes(acc.account.data) 
            # mint, owner, amount = parse_token_account(raw_data)
            # if amount == 1:
            mint = Pubkey.from_string(nft_id)
            
            print(f"✅ Processing NFT Mint: {mint}, Chain: {chain_name}")
            nft_accounts = get_token_account_nft(CLIENT, mint)
            print(f" - Token account: {nft_accounts}")
            
            vietnam_tz = timezone(timedelta(hours=7))
            
            ### INITIAL TIMESTAMP ###            
            try:
                initial_datetime = get_initial_timestamp(CLIENT, mint)
            except Exception:
                initial_datetime = datetime.now(timezone.utc)
            
            initial_datetime = initial_datetime.astimezone(vietnam_tz)
            print(f" - Initial timestamp: {initial_datetime} (VN)")

            ### LAST ACTIONS ###
            last_actions = find_last_actions(CLIENT, nft_accounts)

            last_decrease_liquidity_ts = (
                datetime.fromtimestamp(last_actions["DecreaseLiquidityV2"], tz=timezone.utc)
                if last_actions["DecreaseLiquidityV2"] is not None else None
            )
            last_decrease_liquidity_ts_vn = (
                last_decrease_liquidity_ts.astimezone(vietnam_tz)
                if last_decrease_liquidity_ts is not None else None
            )
            print(f" - Last Decrease Liquidity: {last_decrease_liquidity_ts or 'N/A'} (VN)")
            
            ### POSITION & POOL INFO ###
            position_info = position_map.get(str(mint))
            pool_info = pool_map.get(str(mint))

            if validate_position_and_pool(position_info, pool_info):
                # cache có sẵn, chỉ in log
                pool_account = position_info.get("pool_id")
                print(f" - Pool account: {pool_account}")
            else:
                # luôn lấy client mới có fallback
                client = get_client_with_fallback()
                
                position_account = safe_call(get_position_account_by_mint, client, str(mint))
                position_info = safe_call(decode_personal_position_state, client, str(position_account)) if position_account else None
                
                pool_account = position_info.get("pool_id") if position_info else None
                print(f" - Pool account: {pool_account}")
                
                pool_info = safe_call(decode_pool_state, client, str(pool_account)) if pool_account else None
                
                if validate_position_and_pool(position_info, pool_info):
                    # cache lại khi đủ thông tin
                    position_map[str(mint)] = position_info
                    pool_map[str(mint)] = pool_info
                else:
                    print(f"⚠️ Invalid position or pool info for NFT {nft_id}")
                    continue
            
            position_liquidity = position_info.get("liquidity")
            position_tick_lower = position_info.get("tick_lower_index")
            position_tick_upper = position_info.get("tick_upper_index")
            position_current_tick = pool_info.get("tick_current")
            position_tokens_owed0 = position_info.get("token_fees_owed_0")
            position_tokens_owed1 = position_info.get("token_fees_owed_1")
            fee_growth_inside_0_last_x64 = position_info.get("fee_growth_inside_0_last_x64")
            fee_growth_inside_1_last_x64 = position_info.get("fee_growth_inside_1_last_x64")
            position_rewards = position_info.get("reward_infos") or []
            
            ### LOG ###
            print(f" - Position liquidity: {position_liquidity}")
            print(f" - Position tick lower: {position_tick_lower}")
            print(f" - Position tick upper: {position_tick_upper}")
            print(f" - Position current tick: {position_current_tick}")
            print(f" - Position tokens owed 0: {position_tokens_owed0}")
            print(f" - Position tokens owed 1: {position_tokens_owed1}")
            print(f" - Fee growth inside 0 last x64: {fee_growth_inside_0_last_x64}")
            print(f" - Fee growth inside 1 last x64: {fee_growth_inside_1_last_x64}")
            
            token_mint_0 = pool_info.get("token_mint_0")
            token_mint_1 = pool_info.get("token_mint_1")
            sqrt_price_x64 = pool_info.get("sqrt_price_x64")
            token_mint_0_decimals = pool_info.get("mint_decimals_0")
            token_mint_1_decimals = pool_info.get("mint_decimals_1")
            tick_spacing = pool_info.get("tick_spacing")
            fee_growth_global_0_x64 = pool_info.get("fee_growth_global_0_x64")
            fee_growth_global_1_x64 = pool_info.get("fee_growth_global_1_x64")
            pool_rewards = pool_info.get("reward_infos") or []
            
            ### LOG ###
            print(f" - Token mint 0: {token_mint_0}")
            print(f" - Token mint 1: {token_mint_1}")
            print(f" - Sqrt price x96: {sqrt_price_x64}")
            print(f" - Token mint 0 decimals: {token_mint_0_decimals}")
            print(f" - Token mint 1 decimals: {token_mint_1_decimals}")
            print(f" - Tick spacing: {tick_spacing}")
            print(f" - Fee growth global 0 x64: {fee_growth_global_0_x64}")
            print(f" - Fee growth global 1 x64: {fee_growth_global_1_x64}")

            if pool_account:
                pool_db_info = get_pool_sol_info(str(pool_account))
                token_mint_0_symbol = str(pool_db_info.get("token0_symbol", "Unknown"))
                token_mint_1_symbol = str(pool_db_info.get("token1_symbol", "Unknown"))
            else:
                # fallback qua on-chain
                token_mint_0_info = decode_metadata_pda(CLIENT, str(token_mint_0))
                token_mint_1_info = decode_metadata_pda(CLIENT, str(token_mint_1))
                token_mint_0_symbol = str(token_mint_0_info.get("symbol") if token_mint_0_info else "Unknown")
                token_mint_1_symbol = str(token_mint_1_info.get("symbol") if token_mint_1_info else "Unknown")

            print(f" - Token mint 0 symbol({token_mint_0}): {token_mint_0_symbol}, Token mint 1 symbol({token_mint_1}): {token_mint_1_symbol}")
            
            ### GET TOKEN PRICE FROM API ###
            token_mint_0_price = get_token_price_by_apebond_api(token_mint_0) or 0
            token_mint_1_price = get_token_price_by_apebond_api(token_mint_1) or 0
            if token_mint_0_price <= 0 or token_mint_1_price <= 0:
                print(f"⚠️ Invalid token price for {mint}, skip.")
                continue
            print(f" - Token mint 0 price: {token_mint_0_price}, Token mint 1 price: {token_mint_1_price}")
            
            ### CALCULATE INITIAL AMOUNT TOKENS ###
            amount0, amount1 = get_total_increase_decrease(CLIENT, nft_accounts, token_mint_0, token_mint_1)
            total_initial_price = (token_mint_0_price * amount0 + token_mint_1_price * amount1)
            print(f" - Initial Amount0({token_mint_0_symbol}): {amount0}, Initial Amount1({token_mint_1_symbol}): {amount1}, Total initial price: {total_initial_price}")
            
            ### CALCULATE CURRENT AMOUNT TOKENS ###
            current_amount0, current_amount1 = get_current_amounts(position_liquidity, sqrt_price_x64, position_tick_lower, position_tick_upper)
            current_amount0_decimals = current_amount0 / (10 ** token_mint_0_decimals)
            current_amount1_decimals = current_amount1 / (10 ** token_mint_1_decimals)
            total_current_price = (token_mint_0_price * current_amount0_decimals + token_mint_1_price * current_amount1_decimals)
            print(f" - Current Amount0({token_mint_0_symbol}): {current_amount0_decimals}, Current Amount1({token_mint_1_symbol}): {current_amount1_decimals}, Total current price: {total_current_price}")
            
            ### CALCULATE DELTA AMOUNT TOKEN ###
            delta_amount0 = current_amount0_decimals - amount0
            delta_amount1 = current_amount1_decimals - amount1
            total_delta_price = (token_mint_0_price * delta_amount0 + token_mint_1_price * delta_amount1)
            print(f" - Delta Amount0({token_mint_0_symbol}): {delta_amount0}, Delta Amount1({token_mint_1_symbol}): {delta_amount1}, Total delta price: {total_delta_price}")
            
            ### CALCULATE DELTA PERCENT ###
            denominator = total_current_price - total_delta_price
            if denominator and abs(denominator) > 1e-6:
                percent_delta = (total_delta_price / denominator) * 100
            else:
                percent_delta = 0
            print(f" - Delta percent: {percent_delta}")
            
            ### DETERMINE POSITION STATUS ###
            position_status = status_map.get(str(mint), "Unknown")
            if position_status == 'Unknown':
                position_status = get_position_status(position_liquidity, position_tick_lower, position_tick_upper, position_current_tick, position_tokens_owed0, position_tokens_owed1)
            print(f" - Position Status: {position_status}")
            is_active = 1 if position_status == 'Active' else 0
            
            ### CALCULATE FEES ### 
            tick_start_index_lower = get_start_tick_index(position_tick_lower, tick_spacing)
            tick_start_index_upper = get_start_tick_index(position_tick_upper, tick_spacing)
            print(f" - Tick start index lower: {tick_start_index_lower}, Tick start index upper: {tick_start_index_upper}, Tick current index: {position_current_tick}") 
            
            pool_pubkey = Pubkey.from_string(pool_account)
            tick_array_lower_account = derive_tick_array_pda(pool_pubkey, tick_start_index_lower)
            tick_array_upper_account = derive_tick_array_pda(pool_pubkey, tick_start_index_upper)
            print(f" - Tick array lower account: {tick_array_lower_account}, Tick array upper account: {tick_array_upper_account}")
            
            tick_array_state_lower = decode_tick_array_state(CLIENT, str(tick_array_lower_account), position_tick_lower)
            tick_array_state_upper = decode_tick_array_state(CLIENT, str(tick_array_upper_account), position_tick_upper)
            print(f" Tick array state lower: {tick_array_state_lower}.")
            print(f" Tick array state upper: {tick_array_state_upper}.")
            
            ticks_lower_list = tick_array_state_lower.get("ticks", [])
            ticks_upper_list = tick_array_state_upper.get("ticks", [])

            ticks_lower = ticks_lower_list[0] if ticks_lower_list else {"fee_growth_outside_0_x64": 0, "fee_growth_outside_1_x64": 0}
            ticks_upper = ticks_upper_list[0] if ticks_upper_list else {"fee_growth_outside_0_x64": 0, "fee_growth_outside_1_x64": 0}
            
            fee_growth_outside_0_x64_lower = ticks_lower.get("fee_growth_outside_0_x64", 0)
            fee_growth_outside_1_x64_lower = ticks_lower.get("fee_growth_outside_1_x64", 0)
            fee_growth_outside_0_x64_upper = ticks_upper.get("fee_growth_outside_0_x64", 0)
            fee_growth_outside_1_x64_upper = ticks_upper.get("fee_growth_outside_1_x64", 0)
            
            ### LOG ###
            print(f"fee_growth_outside_0_x64_lower: {fee_growth_outside_0_x64_lower}, fee_growth_outside_1_x64_lower: {fee_growth_outside_1_x64_lower}")
            print(f"fee_growth_outside_0_x64_upper: {fee_growth_outside_0_x64_upper}, fee_growth_outside_1_x64_upper: {fee_growth_outside_1_x64_upper}")
            
            MODULO = 1 << 128
            fee_growth_inside_0_current_x64 = compute_fee_growth_inside(fee_growth_global_0_x64, fee_growth_outside_0_x64_lower, fee_growth_outside_0_x64_upper, position_current_tick, position_tick_lower, position_tick_upper)
            fee_growth_inside_1_current_x64 = compute_fee_growth_inside(fee_growth_global_1_x64, fee_growth_outside_1_x64_lower, fee_growth_outside_1_x64_upper, position_current_tick, position_tick_lower, position_tick_upper)
            print(f"fee_growth_inside_0_current_x64: {fee_growth_inside_0_current_x64}, fee_growth_inside_1_current_x64: {fee_growth_inside_1_current_x64}")
            
            fees_owed0 = position_tokens_owed0 + position_liquidity * ((fee_growth_inside_0_current_x64 - fee_growth_inside_0_last_x64) % MODULO) // (2**64)
            fees_owed1 = position_tokens_owed1 + position_liquidity * ((fee_growth_inside_1_current_x64 - fee_growth_inside_1_last_x64) % MODULO) // (2**64)
            print(f"fees_owed0: {fees_owed0}, fees_owed1: {fees_owed1}")
            
            fees_owed0_decimals = fees_owed0 / (10 ** token_mint_0_decimals)
            fees_owed1_decimals = fees_owed1 / (10 ** token_mint_1_decimals)
            
            if fees_owed0_decimals < 0 or fees_owed1_decimals < 0:
                print("⚠️ Warning: negative fees detected, forcing to 0")
                fees_owed0_decimals = max(fees_owed0_decimals, 0)
                fees_owed1_decimals = max(fees_owed1_decimals, 0)
            
            total_fees_price = (fees_owed0_decimals * token_mint_0_price + fees_owed1_decimals * token_mint_1_price)
            print(f" - Fees owed0({token_mint_0_symbol}): {fees_owed0_decimals}, Fees owed1({token_mint_1_symbol}): {fees_owed1_decimals}, Total fees: {total_fees_price}")
            
            ### CALCULATE CAKE REWARDS ###
            position_first_rewards = position_rewards[0]
            pool_first_rewards = pool_rewards[0]
            print(f"position_first_rewards: {position_first_rewards}, \npool_first_rewards: {pool_first_rewards}")
            
            reward_growth_inside_last_x64 = position_first_rewards.get("growth_inside_last_x64", 0)
            reward_amount_owed = position_first_rewards.get("reward_amount_owed", 0)
            reward_growth_global_x64 = pool_first_rewards.get("reward_growth_global_x64", 0) 
            
            reward_growths_outside_x64_lower_list = ticks_lower.get("reward_growths_outside_x64") or [0]
            reward_growths_outside_x64_upper_list = ticks_upper.get("reward_growths_outside_x64") or [0]
            reward_growths_outside_x64_lower = reward_growths_outside_x64_lower_list[0]
            reward_growths_outside_x64_upper = reward_growths_outside_x64_upper_list[0]
            
            token_reward_mint_account = pool_first_rewards.get("token_mint", "")
            print(f"reward_growth_inside_last_x64: {reward_growth_inside_last_x64}, reward_growth_global_x64: {reward_growth_global_x64}, reward_amount_owed: {reward_amount_owed}")
            print(f"reward_growths_outside_x64_lower: {reward_growths_outside_x64_lower}, reward_growths_outside_x64_upper: {reward_growths_outside_x64_upper}")
            
            reward_growth_inside_current_x64 = compute_fee_growth_inside(reward_growth_global_x64, reward_growths_outside_x64_lower, reward_growths_outside_x64_upper, position_current_tick, position_tick_lower, position_tick_upper)
            print(f"reward_growth_inside_current_x64: {reward_growth_inside_current_x64}")
            
            cake_rewards_owed = reward_amount_owed + position_liquidity * ((reward_growth_inside_current_x64 - reward_growth_inside_last_x64) % MODULO) // (2**64)
            cake_rewards_owed_decimals = cake_rewards_owed / (10 ** 9)
            if cake_rewards_owed_decimals < 0:
                print("⚠️ Warning: negative CAKE rewards detected, forcing to 0")
                cake_rewards_owed_decimals = max(cake_rewards_owed_decimals, 0)
                
            print(f" - Cake reward: {cake_rewards_owed_decimals}")
            
            ### LP FEE APR ###
            if last_decrease_liquidity_ts is None:
                max_latest_time_add_collect_ts = initial_datetime
            else:
                max_latest_time_add_collect_ts = max(last_decrease_liquidity_ts_vn, initial_datetime)
            time_current = int(datetime.now().timestamp())
            delta_time = time_current - int((max_latest_time_add_collect_ts).timestamp())
            delta_time_in_day = delta_time / 60  # minutes
            safe_minutes = delta_time_in_day if delta_time_in_day >= 1 else 1 # at least 1 minute
            
            if total_current_price and abs(total_current_price) > 1e-6:
                lp_fee_apr = (total_fees_price / safe_minutes * 60 * 24 * 365 / total_current_price) * 100
            else:
                lp_fee_apr = 0
            
            ### LP FEE APR 1H ###
            fee_data = get_last_unclaimed_fee_token(str(mint))
            if fee_data:
                try:
                    unclaimed_fee_token0_ago = float(fee_data.get("unclaimed_fee_token0", 0))
                except (ValueError, TypeError):
                    unclaimed_fee_token0_ago = 0.0

                try:
                    unclaimed_fee_token1_ago = float(fee_data.get("unclaimed_fee_token1", 0))
                except (ValueError, TypeError):
                    unclaimed_fee_token1_ago = 0.0

                created_at = fee_data.get("created_at", datetime.now())
                if not isinstance(created_at, datetime):
                    try:
                        created_at = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        created_at = datetime.now()
                
                delta_unclaimed_fee_token0 = fees_owed0_decimals - unclaimed_fee_token0_ago
                delta_unclaimed_fee_token1 = fees_owed1_decimals - unclaimed_fee_token1_ago
                total_delta_fee_usd = delta_unclaimed_fee_token0 * token_mint_0_price + delta_unclaimed_fee_token1 * token_mint_1_price
                
                delta_time = (time_current - int(created_at.timestamp())) / 60 # minutes
                delta_time = max(delta_time, 1)
                
                if denominator and abs(denominator) > 1e-6:
                    lp_fee_apr_1h = (total_delta_fee_usd / delta_time * 60 * 24 * 365) / denominator * 100
                else:
                    lp_fee_apr_1h = 0
            else:
                lp_fee_apr_1h = lp_fee_apr

            ### BOOST ###
            boost = 0
            
            # Time latest stake liquidity            
            time_elapsed_stake_days = (time_current - int(initial_datetime.timestamp())) / (3600 * 24)
            cake_price = get_token_price_by_apebond_api(str(token_reward_mint_account))
            
            ### FARM APR ALL ###
            cake_reward_price = cake_rewards_owed_decimals * cake_price
            if denominator and abs(denominator) > 1e-6 and time_elapsed_stake_days > 0:
                apr_all = (((cake_reward_price / time_elapsed_stake_days) * 365) / denominator * 100)
            else:
                apr_all = 0
            
            ### CAKE APR 1H ###
            pending_cake_info = get_last_pending_cake_info(str(mint))
            last_pending_cake_timestamp = None
            pending_cake_ago = 0.0
            
            if pending_cake_info:
                pending_cake_ago = pending_cake_info.get("pending_cake", 0.0)
                last_pending_cake_timestamp = pending_cake_info.get("created_at", datetime.now())
                print(f"⏳ Time Elapsed pending CAKE ago: {last_pending_cake_timestamp}")
                
                if last_pending_cake_timestamp:
                    if isinstance(last_pending_cake_timestamp, datetime):
                        last_pending_cake_timestamp = int(last_pending_cake_timestamp.timestamp())

                    delta_time_hour = max((time_current - last_pending_cake_timestamp) / 60, 1)
                else:
                    delta_time_hour = 1
                    
                print(f"⏳ Time Elapsed pending CAKE: {delta_time_hour} minutes")
                    
                delta_pending_cake_amount = cake_rewards_owed_decimals - pending_cake_ago
                print(f"📉 Pending CAKE Reward: ${cake_rewards_owed_decimals}")
                print(f"📉 Pending CAKE Reward ago: ${pending_cake_ago}")
                print(f"📉 Delta pending cake amount: {delta_pending_cake_amount} %")
                
                if denominator and abs(denominator) > 1e-6:
                    apr_1h = (delta_pending_cake_amount * cake_price / delta_time_hour * 60 * 24 * 365) / denominator * 100
                else:
                    apr_1h = 0
            else:
                apr_1h = apr_all
            
            ### CAKE REWARD 1H ###
            weekly_rewards = get_weekly_reward_per_pool(chain_name, pool_account)
            print(f"📉 Weekly CAKE Reward: {weekly_rewards}")
            
            if pending_cake_info is None:
                delta_pending_cake_amount = cake_rewards_owed_decimals
                print(f"📉 Pending CAKE Reward: ${cake_rewards_owed_decimals}")
                print(f"📉 Pending CAKE Reward ago(first time): ${pending_cake_ago}")
                print(f"📉 Delta pending cake amount: {delta_pending_cake_amount} %")
                
                delta_time_hour = (time_current - int(initial_datetime.timestamp())) / 60
                print(f"⏳ Time Elapsed(cake reward 1h): {delta_time_hour} minutes")
                
            if weekly_rewards:
                cake_per_second_pool = float(weekly_rewards) / (7*24*60*60) 
                cake_reward_1h = float(delta_pending_cake_amount) / (float(cake_per_second_pool) * (delta_time_hour * 60)) * 100
                print(f"📉 CAKE Reward 1H: {cake_reward_1h}")
            else:
                cake_reward_1h = 0
            
            wallet_url_db = f"https://solscan.io/account/{str(wallet_address)}"
            nft_url_db = f"https://solscan.io/token/{str(mint)}"
            
            vietnam_timezone = timezone(timedelta(hours=7))
            vietnam_time_current_formatted = datetime.now(vietnam_timezone)
            print(f"📅 Time Current: {vietnam_time_current_formatted}")
            
            has_invalid_price = (not token_mint_0_price or token_mint_0_price <= 0 or
                    not token_mint_1_price or token_mint_1_price <= 0)
            
            if inactived_nft_ids:
                if nft_id in inactived_nft_ids:
                    notify_inactive_nft(nft_id, chain_name, str(wallet_address), token_mint_0_symbol.rstrip("\x00"), token_mint_1_symbol.rstrip("\x00"), current_amount0_decimals, current_amount1_decimals, total_current_price, apr_all)
            
            data_nft = (
                str(wallet_address),
                chain_name,
                str(mint),
                token_mint_0_symbol.rstrip("\x00"),
                token_mint_1_symbol.rstrip("\x00"),
                str(pool_account),
                token_mint_0_price,
                token_mint_1_price,
                position_status,
                initial_datetime,
                amount0,
                amount1,
                round(total_initial_price, 2),
                round(current_amount0_decimals, 6),
                round(current_amount1_decimals, 6),
                round(total_current_price, 2),
                round(total_delta_price, 2),
                round(percent_delta, 2),
                round(fees_owed0_decimals, 6),
                round(fees_owed1_decimals, 6),
                round(total_fees_price, 2),
                round(lp_fee_apr, 2),
                round(lp_fee_apr_1h, 2),
                cake_rewards_owed_decimals,
                cake_reward_1h,
                boost,
                round(apr_1h, 2),
                round(apr_all, 2),
                is_active,
                wallet_url_db,
                nft_url_db,
                vietnam_time_current_formatted,
                has_invalid_price
            )
            results.append(data_nft)
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Error processing NFT in wallet {wallet_address}: {e}")
            continue
            
    return results

# if __name__ == "__main__":
#     WALLET_ADDRESS = Pubkey.from_string("4rDyyA4vydw4T5uekxY5La4Ywv43nSZ2PgG7rfBfvQAJ")

#     TOKEN_ACCOUNT_OPTS = TokenAccountOpts(
#         program_id=Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
#     )
    
#     results = get_nft_solana_data(WALLET_ADDRESS, TOKEN_ACCOUNT_OPTS, 'SOL')
#     print("Results:", results)

    
