from solders.pubkey import Pubkey
from solana.rpc.api import Client
import struct
from datetime import datetime, timezone, timedelta
from create_db import get_connection, create_database_and_table
from decimal import Decimal, getcontext
from calc_valid_liquidity_pool import get_pool_tvl
from helper import *
from logging_setup import pool_sol_info_logger as log

getcontext().prec = 50

TWO_64 = Decimal(2) ** 64
WEEK_SECONDS = Decimal(7 * 24 * 3600)

create_database_and_table()

PROGRAM_ID = Pubkey.from_string("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq")
RAYDIUM_PROGRAM_ID = Pubkey.from_string("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK")
METAPLEX_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1385563895850078340/HjXe2bFPkBgdGBMalvRIUMDNgl4mazFvyaJIXs7LRHb66Z2xtOsPMoJVUGCuZLyqF6_T"
# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1376414684294549564/yxz1viXwF5f4b3EjakEp809E0Bqx62rDvqfc2y3aT8vfuA_1Wp9yIR_ZX05CuT5ayNHN"

def get_token_decimals(client: Client, mint_address: str):
    mint_key = Pubkey.from_string(mint_address)
    mint_info = client.get_account_info(mint_key)

    if mint_info.value is None:
        return {"error": "Mint not found"}

    raw_data = bytes(mint_info.value.data)

    # Theo layout SPL Token Mint (82 bytes)
    supply = struct.unpack_from("<Q", raw_data, 36)[0]  # total supply
    decimals = raw_data[44]
    
    return decimals

def calc_cake_weekly_reward(client, reward_info):
    if reward_info.get("reward_state") != 2:
        return 0
    
    token_mint = reward_info.get("token_mint", "")
    if token_mint == "4qQeZ5LwSz6HuupUu8jCtgXyW1mYQcNbFAW1sWZp89HL":
        token_mint_decimals = Decimal(9)
    else:
        token_mint_decimals = Decimal(get_token_decimals(client, token_mint))
    
    duration = Decimal(reward_info.get("end_time", 0)) - Decimal(reward_info.get("open_time", 0))
    eps_x64 = Decimal(reward_info.get("emissions_per_second_x64", 0))
    eps_base = eps_x64 / TWO_64
    total_base = eps_base * duration
    total_token_reward = total_base / (Decimal(10) ** token_mint_decimals)
    
    weeks = duration / WEEK_SECONDS
    weekly_token = total_token_reward / weeks
    
    return float(weekly_token)

def decode_pubkey(data: bytes, offset: int):
    return str(Pubkey(data[offset:offset + 32])), offset + 32

def decode_u128(data: bytes, offset: int):
    return int.from_bytes(data[offset:offset + 16], "little"), offset + 16

def parse_reward_info(data: bytes, offset: int):
    rewards = []
    for _ in range(3):
        reward_state = struct.unpack_from("<B", data, offset)[0]; offset += 1
        open_time, end_time, last_update_time = struct.unpack_from("<QQQ", data, offset); offset += 24
        emissions_per_second_x64, offset = decode_u128(data, offset)
        reward_total_emissioned, reward_claimed = struct.unpack_from("<QQ", data, offset); offset += 16
        token_mint, offset = decode_pubkey(data, offset)
        token_vault, offset = decode_pubkey(data, offset)
        authority, offset = decode_pubkey(data, offset)
        reward_growth_global_x64, offset = decode_u128(data, offset)

        rewards.append({
            "reward_state": reward_state,
            "open_time": open_time,
            "end_time": end_time,
            "last_update_time": last_update_time,
            "emissions_per_second_x64": emissions_per_second_x64,
            "reward_total_emissioned": reward_total_emissioned,
            "reward_claimed": reward_claimed,
            "token_mint": token_mint,
            "token_vault": token_vault,
            "authority": authority,
            "reward_growth_global_x64": reward_growth_global_x64,
        })
    return rewards, offset

def parse_pool_state(raw_data):
    """Parse raw data from a pool account."""
    if len(raw_data) < 1536:
        raise ValueError("Invalid data length for pool account")

    offset = 8
    
    # Extracting fields based on the expected layout
    bump = int.from_bytes(raw_data[offset:offset+1], "little"); offset += 1
    
    # Pubkey parsing
    amm_config, offset = decode_pubkey(raw_data, offset)
    owner, offset = decode_pubkey(raw_data, offset)
    token_mint_0, offset = decode_pubkey(raw_data, offset)
    token_mint_1, offset = decode_pubkey(raw_data, offset)
    token_vault_0, offset = decode_pubkey(raw_data, offset)
    token_vault_1, offset = decode_pubkey(raw_data, offset)
    observation_key, offset = decode_pubkey(raw_data, offset)
    
    # Decimals parsing 
    mint_decimals_0 = int.from_bytes(raw_data[offset:offset + 1], "little"); offset += 1
    mint_decimals_1 = int.from_bytes(raw_data[offset:offset + 1], "little"); offset += 1
    
    # Pool value parsing
    tick_spacing, = struct.unpack_from("<H", raw_data, offset); offset += 2
    liquidity = int.from_bytes(raw_data[offset:offset+16], "little"); offset += 16
    sqrt_price_x64 = int.from_bytes(raw_data[offset:offset+16], "little"); offset += 16
    
    tick_current, = struct.unpack_from("<i", raw_data, offset); offset += 4
    
    padding_3 = int.from_bytes(raw_data[offset:offset + 2], "little"); offset += 2
    padding_4 = int.from_bytes(raw_data[offset:offset + 2], "little"); offset += 2
    
    fee_growth_global_0_x64, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    fee_growth_global_1_x64, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    
    protocol_fees_token_0, = struct.unpack_from("<Q", raw_data, offset); offset += 8
    protocol_fees_token_1, = struct.unpack_from("<Q", raw_data, offset); offset += 8
    
    swap_in_amount_token_0, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    swap_out_amount_token_1, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    swap_in_amount_token_1, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    swap_out_amount_token_0, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    
    status = int.from_bytes(raw_data[offset:offset + 1], "little"); offset += 1
    
    offset += 7

    reward_infos, offset = parse_reward_info(raw_data, offset)
    
    return {
        "token_mint_0": str(token_mint_0),
        "token_mint_1": str(token_mint_1),
        "mint_decimals_0": mint_decimals_0,
        "mint_decimals_1": mint_decimals_1,
        "liquidity": liquidity,
        "sqrt_price_x64": sqrt_price_x64,
        "tick_current": tick_current,
        "fee_growth_global_0_x64": fee_growth_global_0_x64,
        "fee_growth_global_1_x64": fee_growth_global_1_x64,
        "tick_spacing": tick_spacing,
        "reward_infos": reward_infos,
        "amm_config": amm_config
    }

def decode_metaplex_metadata(data: bytes) -> dict:
    if len(data) < 500:
        return {"error": "Dữ liệu quá ngắn cho Metadata"}

    offset = 65

    # Đọc name
    name_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
    name = data[offset:offset + name_len].decode("utf-8"); offset += name_len

    # Symbol
    symbol_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
    symbol = data[offset:offset + symbol_len].decode("utf-8"); offset += symbol_len

    # URI
    uri_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
    uri = data[offset:offset + uri_len].decode("utf-8"); offset += uri_len

    # Seller Fee BPS (u16)
    seller_fee_basis_points = struct.unpack_from("<H", data, offset)[0]

    return {
        "name": name,
        "symbol": symbol,
        "uri": uri,
        "seller_fee_basis_points": seller_fee_basis_points
    }

def decode_amm_config(data: bytes) -> dict:
    offset = 8  # skip anchor discriminator

    bump = data[offset]; offset += 1
    index = int.from_bytes(data[offset:offset+2], "little"); offset += 2

    owner, offset = decode_pubkey(data, offset)

    protocol_fee_rate = struct.unpack_from("<I", data, offset)[0]; offset += 4
    trade_fee_rate = struct.unpack_from("<I", data, offset)[0]; offset += 4
    tick_spacing = int.from_bytes(data[offset:offset+2], "little"); offset += 2
    fund_fee_rate = struct.unpack_from("<I", data, offset)[0]; offset += 4
    padding_u32 = struct.unpack_from("<I", data, offset)[0]; offset += 4

    fund_owner, offset = decode_pubkey(data, offset)

    # padding [u64; 3]
    padding = [struct.unpack_from("<Q", data, offset + i*8)[0] for i in range(3)]
    offset += 8*3

    return {
        "bump": bump,
        "index": index,
        "owner": owner,
        "protocol_fee_rate": protocol_fee_rate,
        "trade_fee_rate": trade_fee_rate,
        "tick_spacing": tick_spacing,
        "fund_fee_rate": fund_fee_rate,
        "fund_owner": fund_owner,
        "padding": padding
    }

def get_metadata_account(mint: str, METAPLEX_PROGRAM_ID: Pubkey) -> Pubkey:
    seed = [
        b"metadata",
        bytes(METAPLEX_PROGRAM_ID),
        bytes(Pubkey.from_string(mint)),
    ]
    metadata_pubkey, _ = Pubkey.find_program_address(seed, METAPLEX_PROGRAM_ID)
    return metadata_pubkey

def get_symbols_tokens(token_accounts: list[str], client) -> dict[str, str]:
    metadata_accounts = [get_metadata_account(token, METAPLEX_PROGRAM_ID) for token in token_accounts]
    response = client.get_multiple_accounts(metadata_accounts)

    result = {}
    for token, acc_info in zip(token_accounts, response.value):
        try:
            if acc_info is None:
                raise Exception("Không có metadata account")

            decoded = decode_metaplex_metadata(acc_info.data)
            symbol = decoded["symbol"]
            result[token] = symbol

        except Exception as e:
            log.warning(f"[⚠️] Lỗi khi lấy symbol cho token {token}: {e}")
            result[token] = None  # fallback dùng address luôn

    return result

def get_token_reward_symbol(client: Client, token_mint: str) -> str:
    
    # ✅ Hardcode for CAKE to avoid API rate limit
    if token_mint == "4qQeZ5LwSz6HuupUu8jCtgXyW1mYQcNbFAW1sWZp89HL":
        return "CAKE"
    else:
        try:
            metadata_account = get_metadata_account(token_mint, METAPLEX_PROGRAM_ID)
            resp = client.get_account_info(metadata_account)

            if resp.value is None:
                raise ValueError("Không lấy được dữ liệu Metadata Account")

            raw_data = bytes(resp.value.data)

            result = decode_metaplex_metadata(raw_data)
            symbol = result.get("symbol", "").strip("\x00")

            return symbol if symbol else token_mint[:6]  

        except Exception as e:
            log.warning(f"[⚠️] Lỗi khi xử lý token {token_mint}: {e}")
            return token_mint[:6]  

def upsert_pool_sol_info(db_cursor, pool_infos, chain: str):
    # Vietnam timezone current time
    vietnam_timezone = timezone(timedelta(hours=7))
    current_time = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")
    
    insert_query = """
        INSERT INTO pool_sol_info (
            chain, pool_account,
            token0_mint, token1_mint, token0_symbol, token1_symbol,
            token0_decimals, token1_decimals,
            reward_state, open_time, end_time,
            reward_claimed, reward_total_emissioned,
            reward_account, reward_symbol, weekly_rewards,
            cake_reward_1h, total_valid_liquidity, total_inactive_staked_liquidity, total_current_liquidity, fee,
            timestamp
        )
        VALUES (
            %(chain)s, %(pool_account)s,
            %(token0_mint)s, %(token1_mint)s, %(token0_symbol)s, %(token1_symbol)s,
            %(token0_decimals)s, %(token1_decimals)s,
            %(reward_state)s, %(open_time)s, %(end_time)s,
            %(reward_claimed)s, %(reward_total_emissioned)s,
            %(reward_account)s, %(reward_symbol)s, %(weekly_rewards)s,
            %(cake_reward_1h)s, %(total_valid_liquidity)s, %(total_inactive_staked_liquidity)s,
            %(total_current_liquidity)s, %(fee)s, %(timestamp)s
        )
        ON DUPLICATE KEY UPDATE
            reward_state = VALUES(reward_state),
            open_time = VALUES(open_time),
            end_time = VALUES(end_time),
            reward_claimed = VALUES(reward_claimed),
            reward_total_emissioned = VALUES(reward_total_emissioned),
            reward_account = VALUES(reward_account),
            reward_symbol = VALUES(reward_symbol),
            weekly_rewards = VALUES(weekly_rewards),
            cake_reward_1h = VALUES(cake_reward_1h),
            total_valid_liquidity = VALUES(total_valid_liquidity),
            total_inactive_staked_liquidity = VALUES(total_inactive_staked_liquidity),
            total_current_liquidity = VALUES(total_current_liquidity), 
            fee = VALUES(fee),
            timestamp = VALUES(timestamp);
    """

    for info in pool_infos:
        if info["reward_idx"] != 0:
            continue  # Chỉ lưu reward_idx = 0 vào bảng pool_sol_info

        db_cursor.execute(insert_query, {
            "chain": chain,
            "pool_account": info["pool_account"],
            "token0_mint": info["token0_mint"],
            "token1_mint": info["token1_mint"],
            "token0_symbol": info["token0_symbol"],
            "token1_symbol": info["token1_symbol"],
            "token0_decimals": info["token0_decimals"],
            "token1_decimals": info["token1_decimals"],
            "reward_state": info["reward_state"],
            "open_time": info["open_time"],
            "end_time": info["end_time"],
            "reward_claimed": info["reward_claimed"],
            "reward_total_emissioned": info["reward_total_emissioned"],
            "reward_account": info["reward_account"],
            "reward_symbol": info["reward_symbol"],
            "weekly_rewards": info["weekly_rewards"],
            "cake_reward_1h": info["cake_reward_1h"],
            "total_valid_liquidity": info["total_valid_liquidity"],
            "total_inactive_staked_liquidity": info["total_inactive_staked_liquidity"],
            "total_current_liquidity": info["total_current_liquidity"],
            "fee": info["fee"],
            "timestamp": current_time
        })

def get_all_amm_config(client, program_id):
    resp = client.get_program_accounts(
        program_id,
        filters=[
            117
        ]
    )
    accounts = resp.value
    amm_configs = {}
    for acc in accounts:
        pubkey = str(acc.pubkey)
        data = acc.account.data
        amm_configs[pubkey] = decode_amm_config(data)
    
    return amm_configs

def get_symbol_token(token_account, client):
    try:
        metadata_account = get_metadata_account(token_account, METAPLEX_PROGRAM_ID)
        log.info(f"Metadata Account: {metadata_account} Mint: {token_account}")
        resp = client.get_account_info(metadata_account).value
        if resp is None:
            raise Exception("Không lấy được dữ liệu Metadata Account")

        metadata_account_data = resp.data
        result = decode_metaplex_metadata(metadata_account_data)
        symbol_token = result['symbol']

        return symbol_token

    except Exception as e:
        log.warning(f"[⚠️] Lỗi khi lấy symbol cho token {token_account}: {e}")
        resp = client.get_account_info(Pubkey.from_string(token_account)).value
        if resp is None:
            log.warning(f"[⚠️] Lỗi khi lấy dữ liệu cho token {token_account}")
        return token_account  # hoặc "UNKNOWN" tùy mục đích sử dụng

def get_all_pool_infos(client, program_id):
    filters = [
        1544  # filter theo dataSize để lấy đúng loại account
    ]

    resp = client.get_program_accounts(
        program_id,
        filters=filters
    )
    accounts = resp.value
    log.info(f"Found {len(accounts)} accounts for program {program_id}")

    pool_infos = []  # list chứa nhiều dict
    token_mint_set = set()
    
    # Get cake price
    cake_price = get_cake_price_usd()
    if not cake_price:
        cake_price = 2.5
    log.info(f"CAKE price: {cake_price}")
    
    amgm_configs = get_all_amm_config(client, program_id)
    log.info(f"Found {len(amgm_configs)} AMM Configs")
    
    for acc in accounts:
        pubkey = str(acc.pubkey)
        data = acc.account.data

        try:
            pool_state = parse_pool_state(data)
            token_mint_0 = pool_state["token_mint_0"]
            token_mint_1 = pool_state["token_mint_1"]
            decimals_0 = pool_state["mint_decimals_0"]
            decimals_1 = pool_state["mint_decimals_1"]
            reward_infos = pool_state["reward_infos"]
            amm_config = pool_state["amm_config"]
            amm_cfg = amgm_configs.get(amm_config, None)
            
            if amm_cfg:
                trade_fee_rate = amm_cfg.get("trade_fee_rate", 0)
                log.info(f"Pool {pubkey} uses AMM Config {amm_config} with trade fee rate: {trade_fee_rate}%")
            else:
                trade_fee_rate = 0
                log.warning(f"[⚠️] Pool {pubkey} has unknown AMM Config {amm_config}")
                
        except Exception as e:
            log.warning(f"[!] Failed parsing pool {pubkey}: {e}")
            continue
        
        token_mint_set.add(token_mint_0)
        token_mint_set.add(token_mint_1)
        
        if reward_infos[0].get('reward_state') == 2:
            total_valid_liquidity, total_inactive_liquidity = get_pool_tvl(client, pool_state, Pubkey.from_string(pubkey), program_id)
        else:
            total_valid_liquidity, total_inactive_liquidity = 0, 0
        
        # Get total cake reward 1h    
        total_cake_reward_1h, total_nft, total_pending_increase_1h = get_total_cake_reward_1h_pool("SOL", pubkey)
        log.info(f"Total cake reward 1h opened position for pool {pubkey}: {total_cake_reward_1h}")
        log.info(f"Total NFT reward 1h opened position for pool {pubkey}: {total_nft}")
        log.info(f"Total pending increase 1h opened position for pool {pubkey}: {total_pending_increase_1h}")

        # Get total current liquidity
        total_current_liquidity, _ = get_total_current_liquidity_on_pool("SOL", pubkey)
        log.info(f"Total current liquidity for pool {pubkey}: {total_current_liquidity}")
        
        # Get token symbol
        pool_db_info = get_pool_sol_info(str(pubkey))
        if pool_db_info is not None:
            token_mint_0_symbol = str(pool_db_info.get("token0_symbol", "Unknown"))
            token_mint_1_symbol = str(pool_db_info.get("token1_symbol", "Unknown"))
        else:
            token_mint_0_symbol = get_symbol_token(token_mint_0, client) or "Unknown"
            token_mint_1_symbol = get_symbol_token(token_mint_1, client) or "Unknown"
        
        for reward_idx, reward in enumerate(reward_infos):
            if reward["token_mint"] != "11111111111111111111111111111111":
                weekly_rewards = calc_cake_weekly_reward(client, reward)
                reward_symbol = get_token_reward_symbol(client, reward["token_mint"])
            else:
                weekly_rewards = 0
                reward_symbol = None
            
            calc_cake_per_day = weekly_rewards / 7
            if total_valid_liquidity == 0:
                percent = 0
            else:
                percent = calc_cake_per_day * cake_price / total_valid_liquidity * 100
                log.info(f"Pool {pubkey} reward {reward_idx}, token {reward_symbol} has percent: {percent}")
            
            if calc_cake_per_day <= 0:
                farming_pos_cake = 0
            else:
                farming_pos_cake = (float(total_pending_increase_1h*12)/calc_cake_per_day) * 100
            
            if total_valid_liquidity <= 0:
                farming_pos_lp = 0
            else:
                farming_pos_lp = (total_current_liquidity/total_valid_liquidity) * 100
                
            pool_url = f'https://solscan.io/account/{pubkey}'
            msg = f'✅ Pool(SOL) [{pubkey[:6]}...{pubkey[-6:]}]({pool_url}) ({token_mint_0_symbol}-{token_mint_1_symbol}) | Reward:'
            msg += f' {calc_cake_per_day} cake/day = {round(calc_cake_per_day * cake_price)} / {round(total_valid_liquidity)} = {percent:.2f}% - Farming {total_nft} pos:'
            msg += f' cake {round(float(total_pending_increase_1h*12), 3)} / {round(calc_cake_per_day)} = {(farming_pos_cake):.0f}%,'
            msg += f' LP {round(total_current_liquidity)} / {round(total_valid_liquidity)} = {(farming_pos_lp):.0f}%'
            
            if percent > 1:
                if (total_cake_reward_1h == 0) or (0 < total_cake_reward_1h <= 50):
                    log.info(msg)
                    notify_discord(msg, DISCORD_WEBHOOK_URL)
            
            pool_infos.append({
                "pool_account": pubkey,
                "token0_mint": token_mint_0,
                "token1_mint": token_mint_1,
                "token0_decimals": decimals_0,
                "token1_decimals": decimals_1,
                "reward_idx": reward_idx,
                "reward_state": reward["reward_state"],
                "open_time": reward["open_time"],
                "end_time": reward["end_time"],
                "reward_claimed": reward["reward_claimed"],
                "reward_total_emissioned": reward["reward_total_emissioned"],
                "reward_account": reward["token_mint"],
                "reward_symbol": reward_symbol,
                "weekly_rewards": weekly_rewards,
                "cake_reward_1h": total_cake_reward_1h,
                "total_valid_liquidity": total_valid_liquidity,
                "total_inactive_staked_liquidity": total_inactive_liquidity,
                "total_current_liquidity": total_current_liquidity,
                "fee": trade_fee_rate
            })
        
    # ✅ Lấy symbol 1 lần cho toàn bộ pool
    symbols = get_symbols_tokens(list(token_mint_set), client)
    for pool in pool_infos:
        pool["token0_symbol"] = symbols.get(pool["token0_mint"], pool["token0_mint"])
        pool["token1_symbol"] = symbols.get(pool["token1_mint"], pool["token1_mint"])

    return pool_infos

if __name__ == "__main__":
    client = Client("https://mainnet.helius-rpc.com/?api-key=bb4fcdca-d41d-4930-ada1-6490968dabe4")
    
    pool_infos = get_all_pool_infos(client, PROGRAM_ID)
    
    connection = get_connection()
    cursor = connection.cursor()
    
    upsert_pool_sol_info(cursor, pool_infos, chain="SOL")
    
    connection.commit()
    cursor.close()