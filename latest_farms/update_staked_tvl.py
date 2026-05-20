"""
update_staked_tvl.py  (v2 — Correct approach)
==============================================
Script nhẹ — chạy cronjob mỗi 15 phút.

Mục đích:
  Tính lại chính xác `total_staked_liquidity` (active, in-range, USD)
  của từng pool farm mà KHÔNG cần quét API event.

Cách hoạt động:
  1. Đọc JSON cache deposit/withdraw có sẵn (do script 4h tạo ra)
     → xác định danh sách tokenId đang stake (dep_block > wd_block)
  2. Multicall MasterChef.userPositionInfos(tokenId)
     → lấy (liquidity, tickLower, tickUpper, pid)
  3. Multicall Pool.slot0()
     → lấy (sqrtPriceX96, currentTick)
  4. Chỉ tính các position: liquidity > 0 VÀ tickLower <= currentTick <= tickUpper
     → position thực sự đang in-range và active
  5. Đổi (liquidity, sqrtPriceX96, ticks) → amount0, amount1 → USD
  6. Cập nhật cột total_staked_liquidity, total_inactive_staked_liquidity vào DB

Chi phí RPC: chỉ dùng Multicall (2–3 request/chain), không gọi API event.
"""

import json
import os
import math
import time
from datetime import datetime, timezone, timedelta
from itertools import islice
from concurrent.futures import ThreadPoolExecutor, as_completed

from web3 import Web3
from w3multicall.multicall import W3Multicall

from config import MASTERCHEF_V3_ADDRESSES, RPC_URLS_2, RPC_BACKUP_LIST, NPM_ADDRESSES
from create_db import get_connection
from helper import get_price_tokens, get_cake_price_usd, get_total_current_liquidity_on_pool
from logging_setup import pool_evm_info_logger as log
from parasite_bot.position_math import is_narrow_range_position
from parasite_bot.bot_config import (
    TICK_SPACING_MAP,
    NARROW_RANGE_N, NARROW_RANGE_N_MAP, NARROW_RANGE_SYMMETRY_TOL,
    NARROW_RANGE_MIN_USD, NARROW_RANGE_FLAG_TVL_PCT,
    NARROW_RANGE_ULTRA_N,
    DISCORD_WEBHOOK_BOT_ALERT
)

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ABI_FILE = os.path.join(BASE_DIR, "latest_farms", "abi_config.json")
DEPOSIT_DIR = os.path.join(BASE_DIR, "latest_farms", "stake_event_json", "deposit_json")
WITHDRAW_DIR = os.path.join(BASE_DIR, "latest_farms", "stake_event_json", "withdraw_json")

CACHE_DIR = os.path.join(BASE_DIR, "latest_farms", "positions_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# --- Event Topics ---
TOPIC_DEPOSIT_HEX  = "0x" + Web3.keccak(text="Deposit(address,uint256,uint256,uint256,int24,int24)").hex()
TOPIC_WITHDRAW_HEX = "0x" + Web3.keccak(text="Withdraw(address,address,uint256,uint256)").hex()
TOPIC_INCREASE_HEX = "0x" + Web3.keccak(text="IncreaseLiquidity(uint256,uint128,uint256,uint256)").hex()
TOPIC_DECREASE_HEX = "0x" + Web3.keccak(text="DecreaseLiquidity(uint256,uint128,uint256,uint256)").hex()

# --- Multicall batch size (tránh timeout với pool nhiều position) ---
MULTICALL_BATCH = 200

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def load_abi():
    with open(ABI_FILE, "r") as f:
        return json.load(f)


def web3_connection(chain_name: str, timeout: int = 30) -> Web3:
    urls = [RPC_URLS_2.get(chain_name)] + RPC_BACKUP_LIST.get(chain_name, [])
    urls = [u for u in urls if u]
    for rpc_url in urls:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": timeout}))
            
            # Khắc phục lỗi extraData 280 bytes cho mạng BNB
            if chain_name == "BNB":
                try:
                    from web3.middleware import ExtraDataToPOAMiddleware
                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                except ImportError:
                    from web3.middleware import geth_poa_middleware
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            
            if w3.is_connected():
                log.info(f"[OK] Connected to {chain_name}: {rpc_url}")
                return w3
        except Exception as e:
            log.error(f"[ERROR] {chain_name} RPC {rpc_url}: {e}")
        time.sleep(0.5)
    raise Exception(f"[FATAL] No working RPC for {chain_name}")


def batch_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def get_current_amounts(liquidity: int, sqrt_price_x96: int, tick_lower: int, tick_upper: int):
    """Tính amount0, amount1 từ L và sqrtPriceX96 (V3 formula)."""
    sqrt_price = float(sqrt_price_x96) / (2 ** 96)
    sqrt_lower = math.sqrt(1.0001 ** tick_lower)
    sqrt_upper = math.sqrt(1.0001 ** tick_upper)
    L = float(liquidity)

    if sqrt_price <= sqrt_lower:
        amount0 = L * (1 / sqrt_lower - 1 / sqrt_upper)
        amount1 = 0.0
    elif sqrt_price < sqrt_upper:
        amount0 = L * (1 / sqrt_price - 1 / sqrt_upper)
        amount1 = L * (sqrt_price - sqrt_lower)
    else:
        amount0 = 0.0
        amount1 = L * (sqrt_upper - sqrt_lower)

    return amount0, amount1


# ─────────────────────────────────────────────────────────────
# Load staked tokenIds từ JSON cache
# ─────────────────────────────────────────────────────────────

def load_staked_token_ids_from_cache(chain: str, pid: int):
    """
    Đọc deposit/withdraw JSON cache → trả về list tokenId còn đang stake.
    Logic: tokenId được stake khi last_deposit_block > last_withdraw_block.
    """
    dep_file = os.path.join(DEPOSIT_DIR, f"deposit_{chain}_pid_{pid}.json")
    wd_file  = os.path.join(WITHDRAW_DIR, f"withdraw_{chain}_pid_{pid}.json")

    if not os.path.exists(dep_file):
        log.warning(f"[{chain}] PID={pid}: No deposit cache found at {dep_file}")
        return []

    with open(dep_file, "r") as f:
        dep_data = json.load(f)

    wd_events = []
    if os.path.exists(wd_file):
        with open(wd_file, "r") as f:
            wd_data = json.load(f)
        wd_events = wd_data.get("events", [])

    dep_events = dep_data.get("events", [])

    # Lấy block deposit cuối và block withdraw cuối của từng tokenId
    last_dep = {}
    for e in dep_events:
        tid, blk = e["tokenId"], e["blockNumber"]
        if tid not in last_dep or blk > last_dep[tid]:
            last_dep[tid] = blk

    last_wd = {}
    for e in wd_events:
        tid, blk = e["tokenId"], e["blockNumber"]
        if tid not in last_wd or blk > last_wd[tid]:
            last_wd[tid] = blk

    staked = [
        tid for tid in last_dep
        if last_dep[tid] >= last_wd.get(tid, -1)
    ]
    return sorted(staked)

# ─────────────────────────────────────────────────────────────
# NEW: Position Caching & Delta Sweep
# ─────────────────────────────────────────────────────────────

def load_positions_cache(chain: str) -> tuple[dict, int, set]:
    path = os.path.join(CACHE_DIR, f"positions_cache_{chain}.json")
    if not os.path.exists(path):
        return {}, 0, set()
    try:
        with open(path, "r") as f:
            data = json.load(f)
        positions = {int(k): v for k, v in data.get("positions", {}).items()}
        bootstrapped_pids = set(data.get("bootstrapped_pids", []))
        return positions, data.get("last_synced_block", 0), bootstrapped_pids
    except Exception as e:
        log.warning(f"[{chain}] Cannot load positions cache: {e}. Starting fresh.")
        return {}, 0, set()

def save_positions_cache(chain: str, positions: dict, last_synced_block: int, bootstrapped_pids: set):
    path = os.path.join(CACHE_DIR, f"positions_cache_{chain}.json")
    try:
        with open(path, "w") as f:
            json.dump({
                "last_synced_block": last_synced_block,
                "bootstrapped_pids": list(bootstrapped_pids),
                "positions": positions
            }, f)
    except Exception as e:
        log.warning(f"[{chain}] Cannot save positions cache: {e}")

def sweep_delta_logs(w3, chain: str, masterchef_addr: str, npm_addr: str, from_block: int, to_block: int):
    CHUNK_SIZE = 1000 # Giảm từ 2000 xuống 1000 để bẻ nhỏ query (tránh lỗi block range too large trên BNB)
    new_stake_ids = set()
    unstake_ids = set()
    changed_ids = set()
    
    masterchef_checksum = Web3.to_checksum_address(masterchef_addr)
    if not npm_addr:
        return set(), set(), set(), to_block
    npm_checksum = Web3.to_checksum_address(npm_addr)
    
    # Giới hạn số block tối đa quét Delta để chống Timeout (nếu bỏ quá lâu)
    if to_block - from_block > 100000:
        log.warning(f"[{chain}] Delta range too large ({to_block - from_block} blocks). Fallback required.")
        return None, None, None, None
        
    for start in range(from_block, to_block + 1, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE - 1, to_block)
        
        # --- Smart Retry Logic (3 Attempts) ---
        success = False
        for attempt in range(3):
            try:
                # 1. Quét MasterChef logs
                mc_logs = w3.eth.get_logs({
                    'fromBlock': start, 'toBlock': end,
                    'address': masterchef_checksum,
                    'topics': [[TOPIC_DEPOSIT_HEX, TOPIC_WITHDRAW_HEX]]
                })
                for log_ev in mc_logs:
                    topic0 = Web3.to_hex(log_ev['topics'][0]).lower()
                    token_id = int.from_bytes(log_ev['topics'][3], 'big')
                    if topic0 == TOPIC_DEPOSIT_HEX.lower():
                        new_stake_ids.add(token_id)
                    elif topic0 == TOPIC_WITHDRAW_HEX.lower():
                        unstake_ids.add(token_id)

                # 2. Quét NPM logs
                npm_logs = w3.eth.get_logs({
                    'fromBlock': start, 'toBlock': end,
                    'address': npm_checksum,
                    'topics': [[TOPIC_INCREASE_HEX, TOPIC_DECREASE_HEX]]
                })
                for log_ev in npm_logs:
                    token_id = int.from_bytes(log_ev['topics'][1], 'big')
                    changed_ids.add(token_id)
                
                success = True
                break # Thành công thì thoát loop retry
            except Exception as e:
                log.warning(f"[{chain}] sweep_delta_logs attempt {attempt+1} failed at {start}-{end}: {e}")
                if attempt < 2:
                    time.sleep(2) # Chờ 2 giây trước khi thử lại
        
        if not success:
            log.error(f"[{chain}] sweep_delta_logs permanently failed at {start}-{end} after 3 attempts.")
            if new_stake_ids or unstake_ids or changed_ids:
                partial_end = start - 1
                log.warning(f"[{chain}] Returning PARTIAL results up to block {partial_end} ({len(new_stake_ids)} deposits, {len(unstake_ids)} withdraws, {len(changed_ids)} tweaks). Next cycle will re-scan from block {start}.")
                return new_stake_ids, unstake_ids, changed_ids, partial_end
            return None, None, None, None
            
    if new_stake_ids or unstake_ids or changed_ids:
        log.info(f"[{chain}] RPC found {len(new_stake_ids)} new stake, {len(unstake_ids)} unstake, {len(changed_ids)} tweaks.")
        if new_stake_ids: log.info(f" -> Deposits: {sorted(list(new_stake_ids))}")
        if unstake_ids: log.info(f" -> Withdraws: {sorted(list(unstake_ids))}")
        if changed_ids: log.info(f" -> Tweaks: {sorted(list(changed_ids))}")
            
    return new_stake_ids, unstake_ids, changed_ids, to_block

def refresh_positions_multicall(w3, contract_address: str, token_ids: set) -> dict:
    updated_infos = {}
    if not token_ids:
        return updated_infos
        
    for batch in batch_iterable(sorted(list(token_ids)), MULTICALL_BATCH):
        mc = W3Multicall(w3)
        for tid in batch:
            mc.add(W3Multicall.Call(
                contract_address,
                "userPositionInfos(uint256)(uint128,uint128,int24,int24,uint256,uint256,address,uint256,uint256)",
                tid,
            ))
        success = False
        for attempt in range(3):
            try:
                results = mc.call()
                for i, tid in enumerate(batch):
                    data = results[i]
                    if not data:
                        continue
                    liquidity, _, tick_lower, tick_upper, _, _, _, pos_pid, _ = data
                    updated_infos[tid] = {
                        "liquidity": liquidity,
                        "tick_lower": tick_lower,
                        "tick_upper": tick_upper,
                        "pid": int(pos_pid)
                    }
                success = True
                break
            except Exception as e:
                log.warning(f"Multicall userPositionInfos attempt {attempt+1} failed for batch: {e}")
                if attempt < 2:
                    time.sleep(2)
        if not success:
            log.error(f"Multicall userPositionInfos permanently failed after 3 attempts. Aborting refresh.")
            return None
            
    return updated_infos


# ─────────────────────────────────────────────────────────────
# Main calculation per chain
# ─────────────────────────────────────────────────────────────

def recalculate_staked_tvl(chain: str, abi: list):
    contract_address = MASTERCHEF_V3_ADDRESSES.get(chain)
    npm_address = NPM_ADDRESSES.get(chain)
    if not contract_address or not npm_address:
        log.warning(f"⚠️ No MasterChef or NPM address for chain: {chain}")
        return

    w3 = web3_connection(chain)
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
    pool_length = contract.functions.poolLength().call()
    cake_price = get_cake_price_usd()
    
    log.info(f"\n🔄 [{chain}] Recalculating TVL for {pool_length} pools (CAKE Price: ${cake_price:.3f})...")

    # ── Step 1: Lấy thông tin pool từ DB ──────────────────────────
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT p.pid, p.pool_address, p.token0_address, p.token1_address,
               p.token0_decimals, p.token1_decimals, p.alloc_point, p.cake_per_day, p.fee,
               COALESCE(p.has_narrow_bot_flag, FALSE) AS has_narrow_bot_flag,
               (dp.selection_source = 'COPY_BOT' AND dp.status NOT IN ('REJECTED')) AS is_copy_bot_selected
        FROM pool_info p
        LEFT JOIN detected_pools dp
            ON p.chain = dp.chain AND p.pool_address = dp.pool_address
            AND dp.selection_source = 'COPY_BOT'
        WHERE p.chain = %s AND p.alloc_point > 0 AND p.is_stake_tracked = 1
        """,
        (chain,),
    )
    pool_rows = cursor.fetchall()
    cursor.close()

    if not pool_rows:
        log.info(f"[{chain}] No active pools in DB.")
        conn.close()
        return

    # Tra tick_spacing và N per pool dựa vào fee tier
    tick_spacing_map = {row[0]: TICK_SPACING_MAP.get(row[8] or 3000, 60) for row in pool_rows}
    narrow_n_map = {
        row[0]: NARROW_RANGE_N_MAP.get(tick_spacing_map[row[0]], NARROW_RANGE_N)
        for row in pool_rows
    }
    # Lưu trạng thái narrow-flag cũ để phát hiện lần đầu xuất hiện (alert)
    prev_flag_map = {row[0]: bool(row[9]) for row in pool_rows}
    # Pool đã được chọn copy-bot → không re-alert (tránh detect chính position của mình)
    is_copy_bot_map = {row[0]: bool(row[10]) for row in pool_rows}

    vietnam_tz = timezone(timedelta(hours=7))
    update_rows = []
    valid_pids = {row[0] for row in pool_rows}

    # ── Step 2: Cập Nhật Position Cache (Kỹ Thuật Delta Sweep) ─
    positions_cache, last_synced_block, bootstrapped_pids = load_positions_cache(chain)
    
    try:
        current_block = w3.eth.get_block('latest')['number']
    except Exception as e:
        log.error(f"[{chain}] Cannot get latest block: {e}")
        conn.close()
        return

    delta_stake = set()
    delta_unstake = set()
    delta_change = set()
    
    # Quyết định Nạp Mới Lại Hay Quét Bù (Vượt ngưỡng 100k block / chưa có mem thì phải Fallback mồi)
    if last_synced_block == 0 or current_block - last_synced_block > 100000:
        log.info(f"[{chain}] Building positions cache from scratch (Local JSON fallback)!")
        
        all_initial_tokens = set()
        for pid in valid_pids:
            tids = load_staked_token_ids_from_cache(chain, pid)
                
            all_initial_tokens.update(tids)
            bootstrapped_pids.add(pid)
            
        delta_stake = all_initial_tokens
        positions_cache = {}  # Đập bỏ Cache cũ nếu quá lỗi thời
    else:
        # Tự động phát hiện PID mới thêm vào DB mà chưa được load lịch sử
        new_pids = valid_pids - bootstrapped_pids
        if new_pids:
            log.info(f"[{chain}] Auto-discovering {len(new_pids)} new PIDs. Bootstrapping history...")
            for pid in new_pids:
                tids = load_staked_token_ids_from_cache(chain, pid)
                delta_stake.update(tids)
                bootstrapped_pids.add(pid)
            if delta_stake:
                log.info(f"[{chain}] Found {len(delta_stake)} combined historical tokens for new PIDs.")

        log.info(f"[{chain}] Syncing MasterChef Delta Logs [{last_synced_block + 1} -> {current_block}]...")
        delta_results = sweep_delta_logs(
            w3, chain, contract_address, npm_address, last_synced_block + 1, current_block
        )
        ds, du, dc, synced_up_to = delta_results
        if ds is None:
            log.warning(f"[{chain}] Delta sweep failed (RPC ban/timeout). Aborting to preserve last_synced_block={last_synced_block}.")
            log.warning(f"[{chain}] Data will be re-synced from block {last_synced_block + 1} next cycle.")
            conn.close()
            return
        
        # Nếu partial, synced_up_to < current_block → lần sau sẽ re-scan phần còn lại
        current_block = synced_up_to
        delta_stake.update(ds)
        delta_unstake.update(du)
        delta_change.update(dc)
        log.info(f"[{chain}] Delta sweep found: +{len(ds)} deposits, -{len(du)} withdraws, {len(dc)} liq tweaks.")

    # A) Trừ Token đã Withdraw khỏi RAM
    for tid in delta_unstake:
        if tid in positions_cache:
            del positions_cache[tid]

    # B) Kéo API Multicall để Bật/Cập nhật số liệu cho các mớ Token vừa cọ xát
    ids_to_update = delta_stake.union(delta_change)
    if ids_to_update:
        log.info(f"[{chain}] Multicalling {len(ids_to_update)} updated positions on Node...")
        updated_infos = refresh_positions_multicall(w3, contract_address, ids_to_update)
        if updated_infos is None:
            log.warning(f"[{chain}] refresh_positions_multicall failed. Aborting to preserve last_synced_block={last_synced_block}.")
            conn.close()
            return
        for tid, info in updated_infos.items():
            positions_cache[tid] = info

    # Lọc rác: Loại trừ các TokenDeposit không thuộc mảng Valid_PIDs (Phòng Deposit nhầm farm lỗi sập)
    keys_to_delete = [tid for tid, info in positions_cache.items() if info['pid'] not in valid_pids]
    for tid in keys_to_delete:
        del positions_cache[tid]
        
    save_positions_cache(chain, positions_cache, current_block, bootstrapped_pids)
    log.info(f"[{chain}] ✅ Memory cached loaded {len(positions_cache)} active positions.")

    # ── Step 3: Tổng tiến công (In-RAM Multicall + Tính Giá/TVL)
    # Lắp thẻ slot0 call cho tất cả Active Pools
    log.info(f"[{chain}] Fetching slot0() (CurrentTick) for ALL pools in single Multicall...")
    mc_slot = W3Multicall(w3)
    for row in pool_rows:
        mc_slot.add(W3Multicall.Call(
            Web3.to_checksum_address(row[1]),
            "slot0()(uint160,int24,uint16,uint16,uint16,uint32,bool)"
        ))
    try:
        slot_results = mc_slot.call()
    except Exception as e:
        log.warning(f"[{chain}] slot0 batch multicall error: {e}")
        conn.close()
        return
        
    slot_data_map = {}
    for i, row in enumerate(pool_rows):
        res = slot_results[i]
        if res:
            slot_data_map[row[0]] = {
                "sqrt_price_x96": res[0],
                "current_tick": res[1]
            }

    # Bắt đầu tính toán nhanh trên từng Pool
    log.info(f"[{chain}] In-RAM Math TVL computation running...")
    for row in pool_rows:
        pid = row[0]
        pool_address   = row[1]
        token0_address = row[2]
        token1_address = row[3]
        token0_dec     = row[4] or 18
        token1_dec     = row[5] or 18
        cake_per_day   = float(row[7] or 0)
        
        slot = slot_data_map.get(pid)
        if not slot:
            continue
            
        current_tick = slot["current_tick"]
        sqrt_price_x96 = slot["sqrt_price_x96"]
        
        price0 = get_price_tokens(chain, token0_address)
        price1 = get_price_tokens(chain, token1_address)
        if not price0 or not price1:
            continue
            
        total_active   = 0.0
        total_inactive = 0.0
        count_pos = 0
        narrow_count = 0
        narrow_tvl   = 0.0
        tick_spacing = tick_spacing_map.get(pid, 60)
        narrow_n     = narrow_n_map.get(pid, NARROW_RANGE_N)

        # Mỏ khoáng In-RAM
        for tid, info in positions_cache.items():
            if info['pid'] != pid:
                continue
                
            liq = info["liquidity"]
            if liq == 0:
                continue

            count_pos += 1
            tick_lower = info["tick_lower"]
            tick_upper = info["tick_upper"]
            is_inrange = tick_lower <= current_tick <= tick_upper
            
            amount0_raw, amount1_raw = get_current_amounts(liq, sqrt_price_x96, tick_lower, tick_upper)
            amount0 = amount0_raw / (10 ** token0_dec)
            amount1 = amount1_raw / (10 ** token1_dec)
            usd_val = amount0 * price0 + amount1 * price1

            if (pid == 492 and chain == "BSC"):
                log.info(f"[{chain}] PID={pid} Token id={tid}: amount0={amount0}, amount1={amount1}, price0={price0}, price1={price1}, usd_val={usd_val}, is_inrange={is_inrange}")

            if is_inrange:
                total_active += usd_val
                # Narrow-range bot detection (chỉ check in-range + đủ dust threshold)
                if usd_val >= NARROW_RANGE_MIN_USD and is_narrow_range_position(
                    tick_lower, tick_upper, current_tick,
                    tick_spacing, narrow_n, NARROW_RANGE_SYMMETRY_TOL,
                    NARROW_RANGE_ULTRA_N
                ):
                    narrow_count += 1
                    narrow_tvl   += usd_val
            else:
                total_inactive += usd_val

        # Kiểm tra flag và tính tỉ lệ narrow-TVL
        has_flag = (
            narrow_count > 0
            and total_active > 0
            and (narrow_tvl / total_active) >= NARROW_RANGE_FLAG_TVL_PCT
        )

        if count_pos > 0:
            log.info(
                f"✅ [{chain}] PID={pid} | Active: ${total_active:,.2f} "
                f"| Inactive: ${total_inactive:,.2f} "
                f"| tick={current_tick} | pos={count_pos}"
                + (f" | ⚠️ NARROW_BOT flag={has_flag} ({narrow_count} pos / ${narrow_tvl:,.2f})" if narrow_count > 0 else "")
            )

        # Alert Discord khi flag mới xuất hiện lần đầu
        # Bỏ qua nếu pool đã được copy-bot-selected → tránh alert chính vị trí mình đã mint
        if has_flag and not prev_flag_map.get(pid, False) and not is_copy_bot_map.get(pid, False):
            try:
                from helper import notify_discord
                notify_discord(
                    f"⚠️ **Narrow Range Bot Detected**\n"
                    f"Chain: `{chain}` | PID: `{pid}` | Pool: `{pool_address}`\n"
                    f"Narrow positions: **{narrow_count}** / ${narrow_tvl:,.2f} USD "
                    f"({narrow_tvl / total_active * 100:.1f}% of active TVL)",
                    DISCORD_WEBHOOK_BOT_ALERT
                )
            except Exception as _alert_err:
                log.warning(f"[{chain}] Discord alert failed: {_alert_err}")

        used_tvl = total_active if total_active > 0 else 1.0
        farm_apr = (cake_per_day * cake_price * 365) / used_tvl * 100 if used_tvl > 1.0 else 0
        total_current_liquidity, _ = get_total_current_liquidity_on_pool(chain, pool_address)
        
        now = datetime.now(vietnam_tz).strftime("%Y-%m-%d %H:%M:%S")
        update_rows.append((
            total_current_liquidity, total_active, total_inactive, farm_apr,
            narrow_count, narrow_tvl, has_flag,
            now, chain, pool_address.lower()
        ))

    # ── Step 4: Batch update DB ────────────────────────────────────
    if update_rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE pool_info
                SET total_current_liquidity         = %s,
                    total_staked_liquidity          = %s,
                    total_inactive_staked_liquidity = %s,
                    farm_apr                        = %s,
                    narrow_range_count              = %s,
                    narrow_range_tvl_usd            = %s,
                    has_narrow_bot_flag             = %s,
                    timestamp                       = %s
                WHERE chain = %s AND pool_address   = %s
                """,
                update_rows,
            )
        conn.commit()
        log.info(f"✅ [{chain}] Updated {len(update_rows)} pools in DB.")
    else:
        log.info(f"[{chain}] Nothing to update.")

    conn.close()


# ─────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────

def main():
    abi = load_abi()
    vietnam_tz = timezone(timedelta(hours=7))
    log.info(f"🔄 [update_staked_tvl] Start at {datetime.now(vietnam_tz).strftime('%Y-%m-%d %H:%M:%S')}")

    chains = list(MASTERCHEF_V3_ADDRESSES.keys())
    
    # Sử dụng ThreadPoolExecutor để chạy song song các chain
    # Max worker bằng số lượng chain để tối ưu hóa
    with ThreadPoolExecutor(max_workers=len(chains)) as executor:
        future_to_chain = {}
        for chain in chains:
            # --- Staggered Start (2s delay per chain) ---
            future = executor.submit(recalculate_staked_tvl, chain, abi)
            future_to_chain[future] = chain
            time.sleep(2) 
        
        for future in as_completed(future_to_chain):
            chain = future_to_chain[future]
            try:
                future.result()
            except Exception as e:
                log.error(f"❌ [{chain}] Error in thread: {e}")

    log.info("✅ [update_staked_tvl] Done.")


if __name__ == "__main__":
    main()
