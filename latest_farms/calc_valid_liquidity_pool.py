from solders.pubkey import Pubkey
from solana.rpc.api import Client
from solana.rpc.types import MemcmpOpts
import struct
import math
import requests
import time
from helper import get_price_tokens
from logging_setup import pool_sol_info_logger as log

def decode_pubkey(data: bytes, offset: int):
    return str(Pubkey(data[offset:offset + 32])), offset + 32

def decode_u128(data: bytes, offset: int):
    return int.from_bytes(data[offset:offset + 16], "little"), offset + 16

def decode_i128(data, offset):
    val = int.from_bytes(data[offset:offset+16], "little", signed=True)
    return val, offset + 16

def decode_reward_info_position(data: bytes, offset: int):
    reward_infos = []
    for _ in range(3):  # luôn có 3 reward slot
        growth_inside_last_x64, offset = decode_u128(data, offset)
        reward_amount_owed = struct.unpack_from("<Q", data, offset)[0]
        offset += 8
        reward_infos.append({
            "growth_inside_last_x64": growth_inside_last_x64,
            "reward_amount_owed": reward_amount_owed
        })
    return reward_infos, offset

def decode_personal_position_state(data):
    if len(data) < 281:
        raise ValueError(f"Invalid account data length: {len(data)}")
    
    offset = 8
    bump = struct.unpack_from("<I", data, offset)[0]; offset += 1
    nft_mint = decode_pubkey(data, offset); offset += 32
    pool_id = decode_pubkey(data, offset); offset += 32
    tick_lower_index = struct.unpack_from("<i", data, offset)[0]; offset += 4
    tick_upper_index = struct.unpack_from("<i", data, offset)[0]; offset += 4
    liquidity, offset = decode_u128(data, offset)
    fee_growth_inside_0_last_x64, offset = decode_u128(data, offset)
    fee_growth_inside_1_last_x64, offset = decode_u128(data, offset)
    token_fees_owed_0 = struct.unpack_from("<Q", data, offset)[0]; offset += 8
    token_fees_owed_1 = struct.unpack_from("<Q", data, offset)[0]; offset += 8
    reward_infos, offset = decode_reward_info_position(data, offset)
    
    return {
        "nft_mint": nft_mint[0],
        "pool_id": pool_id[0],
        "tick_lower_index": tick_lower_index,
        "tick_upper_index": tick_upper_index,
        "liquidity": liquidity,
        "fee_growth_inside_0_last_x64": fee_growth_inside_0_last_x64,
        "fee_growth_inside_1_last_x64": fee_growth_inside_1_last_x64,
        "token_fees_owed_0": token_fees_owed_0,
        "token_fees_owed_1": token_fees_owed_1,
        "reward_infos": reward_infos
    }

# --- Quét toàn bộ PersonalPositionState ---
def fetch_positions(client, pool_account: Pubkey, program_id: Pubkey):
    resp = client.get_program_accounts(
        program_id,
        filters=[
            281,
            MemcmpOpts(offset=41, bytes=str(pool_account))    
        ]
    )

    accounts = resp.value
    positions = []
    for acc in accounts:
        raw_data = acc.account.data
        position_state = decode_personal_position_state(raw_data)
        positions.append(position_state)
    
    return positions

def get_current_amounts(liquidity, sqrt_price_x96, tick_lower, tick_upper):
    sqrt_price = float(sqrt_price_x96) / 2**64
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

def get_pool_tvl(client, pool_state, pool_account: Pubkey, program_id: Pubkey):
    # Decode thông tin pool
    sqrt_price_x96 = pool_state["sqrt_price_x64"]
    token_mint_0 = pool_state["token_mint_0"]
    token_mint_1 = pool_state["token_mint_1"]
    token_mint_0_decimals = pool_state["mint_decimals_0"]
    token_mint_1_decimals = pool_state["mint_decimals_1"]
    tick_current = pool_state["tick_current"]

    # Fetch toàn bộ position thuộc pool
    positions = fetch_positions(client, pool_account, program_id)

    # Lấy giá token từ API
    token_mint_0_price = get_price_tokens("SOL", token_mint_0, int(tick_current), token_mint_0, token_mint_1, int(token_mint_0_decimals), int(token_mint_1_decimals)) or 0
    token_mint_1_price = get_price_tokens("SOL", token_mint_1, int(tick_current), token_mint_0, token_mint_1, int(token_mint_0_decimals), int(token_mint_1_decimals)) or 0

    total_liquidity_valid = 0
    total_inactive_liquidity = 0

    # Duyệt từng position để tính giá trị
    for position_info in positions:
        liquidity = position_info["liquidity"]
        tick_lower = position_info["tick_lower_index"]
        tick_upper = position_info["tick_upper_index"]
        token_fees_owed_0 = position_info["token_fees_owed_0"]  
        token_fees_owed_1 = position_info["token_fees_owed_1"]

        raw_amount0, raw_amount1 = get_current_amounts(
            liquidity, sqrt_price_x96, tick_lower, tick_upper
        )
        amount0 = raw_amount0 / 10 ** token_mint_0_decimals
        amount1 = raw_amount1 / 10 ** token_mint_1_decimals
        
        position_status = get_position_status(liquidity, tick_lower, tick_upper, tick_current, token_fees_owed_0, token_fees_owed_1)
        log.info(f"💰 Position status: {position_status}")
        position_value = amount0 * token_mint_0_price + amount1 * token_mint_1_price
        if position_status == "Active":
            total_liquidity_valid += position_value
        elif position_status == "Inactive":
            total_inactive_liquidity += position_value
        else:
            continue

    log.info(f"💰 TVL for {pool_account}: {total_liquidity_valid}, Total inactive liquidity: {total_inactive_liquidity}")
    return total_liquidity_valid, total_inactive_liquidity