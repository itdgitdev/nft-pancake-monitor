from solana.rpc.api import Client
from solders.pubkey import Pubkey
import struct
from datetime import datetime, timedelta, timezone
from create_db import get_connection, create_database_and_table
import mysql.connector
import requests
from decimal import Decimal, getcontext
import time
from logging_setup import active_farm_sol_logger as log

getcontext().prec = 50

TWO_64 = Decimal(2) ** 64
WEEK_SECONDS = Decimal(7 * 24 * 3600)

PROGRAM_ID = "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq"
METAPLEX_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")

DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1386555618751549520/i6GTfThX2VckPF4isp9ktn7ds1B0Ik7YWGGPR016nCO79uPIqm4ukYXPK-PR21_YvYyT"
# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1430823982701084773/OwPTDTjFQBgKSXgdizi8rU-Kc6V9n15UbD4ORajuMY3rv1lL6scCV2yPhb0rDQ0U-W5j"

create_database_and_table()

### WEEKLY REWARDS ###
def get_token_reward_decimals(client: Client, mint_address: str):
    if mint_address == "4qQeZ5LwSz6HuupUu8jCtgXyW1mYQcNbFAW1sWZp89HL":
        return 9
    else:
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
    token_mint_decimals = Decimal(get_token_reward_decimals(client, token_mint))
    
    duration = Decimal(reward_info.get("end_time", 0)) - Decimal(reward_info.get("open_time", 0))
    eps_x64 = Decimal(reward_info.get("emissions_per_second_x64", 0))
    eps_base = eps_x64 / TWO_64
    total_base = eps_base * duration
    total_token_reward = total_base / (Decimal(10) ** token_mint_decimals)
    
    weeks = duration / WEEK_SECONDS
    weekly_token = total_token_reward / weeks
    
    return float(weekly_token)

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

# Insert new pool if detect new farm
def upsert_pool_info(
        connection, chain, pool_account, token_mint_0, token_mint_1, token_mint_0_symbol, 
        token_mint_1_symbol, token_mint_0_decimals, token_mint_1_decimals, reward_data,
        reward_account, reward_symbol, weekly_rewards
        ):

    with connection.cursor() as cursor:
        # kiểm tra tồn tại
        cursor.execute("""
            SELECT id FROM pool_sol_info WHERE pool_account = %s
        """, (pool_account,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("""
                INSERT INTO pool_sol_info (
                    chain, pool_account,
                    token0_mint, token1_mint,
                    token0_symbol, token1_symbol,
                    token0_decimals, token1_decimals,
                    reward_state, open_time, end_time,
                    reward_claimed, reward_total_emissioned,
                    reward_account, reward_symbol, weekly_rewards,
                    timestamp
                ) VALUES (
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    NOW()
                )
            """, (
                chain, pool_account,
                token_mint_0, token_mint_1,
                token_mint_0_symbol, token_mint_1_symbol,
                token_mint_0_decimals, token_mint_1_decimals,
                reward_data["reward_state"], reward_data["open_time"], reward_data["end_time"],
                reward_data["reward_claimed"], reward_data["reward_total_emissioned"],
                reward_account, reward_symbol, weekly_rewards
            ))
        else:
            cursor.execute("""
                UPDATE pool_sol_info
                SET token0_symbol = %s, token1_symbol = %s,
                    token0_decimals = %s, token1_decimals = %s,
                    reward_state = %s, open_time = %s, end_time = %s,
                    reward_claimed = %s, reward_total_emissioned = %s,
                    reward_account = %s, reward_symbol = %s, weekly_rewards = %s,
                    timestamp = NOW()
                WHERE pool_account = %s
            """, (
                token_mint_0_symbol, token_mint_1_symbol,
                token_mint_0_decimals, token_mint_1_decimals,
                reward_data["reward_state"], reward_data["open_time"], reward_data["end_time"],
                reward_data["reward_claimed"], reward_data["reward_total_emissioned"],
                reward_account, reward_symbol, weekly_rewards,
                pool_account
            ))

    connection.commit()

### DECODE FUNCTIONS ###
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

def parse_pool_state(raw_data) -> dict:
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
    liquidity, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    sqrt_price_x96, = struct.unpack_from("<Q", raw_data, offset); offset += 16
    
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
    
    return token_mint_0, token_mint_1, reward_infos

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

def get_metadata_account(mint: str, METAPLEX_PROGRAM_ID: Pubkey) -> Pubkey:
    seed = [
        b"metadata",
        bytes(METAPLEX_PROGRAM_ID),
        bytes(Pubkey.from_string(mint)),
    ]
    metadata_pubkey, _ = Pubkey.find_program_address(seed, METAPLEX_PROGRAM_ID)
    return metadata_pubkey

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
        log.error(f"Error fetching pool info: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
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

def is_farm_active(reward, reference_ts=None):
    if not reward:
        return False
    
    try:
        now_ts = reference_ts if reference_ts else int(datetime.now().timestamp())
        
        state = int(reward.get("reward_state", 0))
        weekly_reward = float(reward.get("weekly_reward", reward.get("weekly_rewards", 0)))
        end_time = int(reward.get("end_time", 0))
        open_time = int(reward.get("open_time", 0))
        
        # 3. Logic kiểm tra
        is_state_active = (state == 2)
        has_rewards = (weekly_reward > 0)
        is_ongoing = (open_time <= now_ts < end_time)

        return is_state_active and has_rewards and is_ongoing

    except (ValueError, TypeError):
        return False
    
def filter_active_farm(client, PROGRAM_ID):
    filters = [
        1544  # filter dataSize (giả định từ context trước đó)
    ]

    # Lấy danh sách tất cả account thuộc program
    resp = client.get_program_accounts(
        Pubkey.from_string(PROGRAM_ID),
        filters=filters
    )
    accounts = resp.value

    result = {}
    token_accounts = {}
    active_pool_count = 0

    for i, acc in enumerate(accounts):
        pubkey = acc.pubkey
        data_field = acc.account.data

        try:
            token_mint_0, token_mint_1, reward_infos = parse_pool_state(data_field)
        except Exception as e:
            log.warning(f"[!] Error parsing pool {pubkey}: {e}")
            continue

        valid_rewards = {}
        for reward_idx, reward_info in enumerate(reward_infos):
            reward_info["weekly_reward"] = calc_cake_weekly_reward(client, reward_info)
            if is_farm_active(reward_info):
                valid_rewards[reward_idx] = reward_info  # dùng dict thay vì list

        if valid_rewards:
            active_pool_count += 1
            result[str(pubkey)] = valid_rewards  # rewards là dict
            token_accounts[str(pubkey)] = token_mint_0, token_mint_1
    
    return result, token_accounts

def notify_discord(message, retries=3):
    data = {"content": message}
    for attempt in range(retries):
        try:
            response = requests.post(DISCORD_WEBHOOK_URL, json=data)
            
            # ✅ Thành công
            if response.status_code == 204:
                return True

            # ⚠️ Bị rate limit
            if response.status_code == 429:
                error_data = response.json()
                retry_after = error_data.get("retry_after", 1)
                log.info(f"⏳ Discord rate limited. Retry after {retry_after}s")
                time.sleep(retry_after)
                continue  # thử lại sau sleep

            # ❌ Lỗi khác
            log.error(f"❌ Discord notify failed ({response.status_code}): {response.text}")
            return False

        except Exception as e:
            log.error(f"❌ Discord exception: {e}")
            time.sleep(1)

    return False
        
def load_state(connection):
    state = {}
    first_run = False

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT *
            FROM farm_state_sol
        """)
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                pool_account = row[2]
                reward_idx = row[3]
                if pool_account not in state:
                    state[pool_account] = {}

                state[pool_account][reward_idx] = {
                    "token_mint": row[4],
                    "reward_state": row[5],
                    "open_time": row[6],
                    "end_time": row[7],
                    "reward_claimed": row[8],
                    "reward_total_emissioned": row[9],
                    "token_reward_symbol": row[10],
                    "token_reward_decimals": row[11],
                    "weekly_rewards": row[12],
                    "timestamp": row[13]
                }

        else:
            first_run = True

    return state, first_run

def save_state(state, connection, chain, client):
    with connection.cursor() as cursor:
        for pool_account, rewards in state.items():
            for reward_idx, reward_data in rewards.items():
                # Check existing of reward
                cursor.execute("""
                    SELECT reward_state, reward_total_emissioned, weekly_rewards, open_time, end_time, reward_claimed
                    FROM farm_state_sol
                    WHERE pool_account = %s AND reward_idx = %s
                """, (pool_account, reward_idx))
                result = cursor.fetchone()
                
                # token reward symbol
                token_reward_symbol = get_token_reward_symbol(client, reward_data['token_mint'])
                
                # weekly reward 
                weekly_reward = calc_cake_weekly_reward(client, reward_data)
                
                # token reward decimals
                token_reward_decimals = get_token_reward_decimals(client, reward_data['token_mint'])
                
                # Vietnam timezone current time
                vietnam_timezone = timezone(timedelta(hours=7))
                current_time = datetime.now(vietnam_timezone).strftime("%Y-%m-%d %H:%M:%S")
                
                if result:
                    # if not (is_farm_active(reward_data)):
                    #     reward_data['reward_state'] = 0
                        
                    # Update only if changed
                    db_reward_state, db_reward_total_emissioned, db_weekly_rewards, db_open_time, db_end_time, db_reward_claimed = result
                    
                    if (
                        db_reward_state != reward_data['reward_state']
                        or weekly_reward != db_weekly_rewards
                        or db_reward_total_emissioned != reward_data['reward_total_emissioned']
                        or db_open_time != reward_data['open_time']
                        or db_end_time != reward_data['end_time']
                        or db_reward_claimed != reward_data['reward_claimed']
                    ):
                        cursor.execute("""
                            UPDATE farm_state_sol
                            SET token_mint = %s, reward_state = %s, open_time = %s, end_time = %s,
                                reward_claimed = %s, reward_total_emissioned = %s,
                                token_reward_symbol = %s, token_reward_decimals = %s, weekly_rewards = %s,
                                timestamp = %s
                            WHERE pool_account = %s AND reward_idx = %s
                        """, (
                            reward_data['token_mint'],
                            reward_data['reward_state'],
                            reward_data['open_time'],
                            reward_data['end_time'],
                            reward_data['reward_claimed'],
                            reward_data['reward_total_emissioned'],
                            token_reward_symbol,
                            token_reward_decimals,
                            weekly_reward,
                            current_time,
                            pool_account, reward_idx
                        ))
                else:
                    cursor.execute("""
                        INSERT INTO farm_state_sol (
                            chain, pool_account, reward_idx, token_mint, reward_state,
                            open_time, end_time, reward_claimed, reward_total_emissioned,
                            token_reward_symbol, token_reward_decimals, weekly_rewards, timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        chain,
                        pool_account, reward_idx,
                        reward_data['token_mint'],
                        reward_data['reward_state'],
                        reward_data['open_time'],
                        reward_data['end_time'],
                        reward_data['reward_claimed'],
                        reward_data['reward_total_emissioned'],
                        token_reward_symbol,
                        token_reward_decimals,
                        weekly_reward,
                        current_time
                    ))

    connection.commit()

def check_farm(client, program_id, first_run, old_state, connection, chain):
    new_state, token_accounts = filter_active_farm(client, program_id)

    if first_run:
        log.info("⏳ First run - no notify, only saving state.")
        for pool_account, rewards in new_state.items():
            for reward_idx, reward in rewards.items():
                reward["weekly_reward"] = calc_cake_weekly_reward(client, reward)

        log.info(f"New state: {new_state}")
        return new_state

    new_farms = []
    stopped_farms = []
    changed_farms = []

    # Vietnam timezone current time
    vietnam_timezone = timezone(timedelta(hours=7))
    now_ts = int(datetime.now(vietnam_timezone).timestamp())

    # So sánh từ new_state → tìm farm mới hoặc restart
    for pool_account, rewards in new_state.items():
        token_mint_0, token_mint_1 = token_accounts.get(pool_account)
        log.info(f"Pool: {pool_account}, Token 0: {token_mint_0}, Token 1: {token_mint_1}")
        for reward_idx, new_reward in rewards.items():
            # Tính và gắn weekly_reward vào new_reward
            new_reward["weekly_reward"] = calc_cake_weekly_reward(client, new_reward)
            log.info(f"New reward {reward_idx}: {new_reward['weekly_reward']}")
            
            old_rewards = old_state.get(pool_account, {})
            old_reward = old_rewards.get(reward_idx)

            if old_reward is None:
                # Truly new reward entry
                if new_reward["weekly_reward"] > 0:
                    new_farms.append((pool_account, reward_idx, new_reward, token_mint_0, token_mint_1))
                    
            else:
                if "weekly_reward" not in old_reward:
                    old_reward["weekly_reward"] = old_reward.get("weekly_rewards", 0)
                    log.info(f"Old reward {reward_idx}: {old_reward['weekly_reward']}")
                
                was_active = is_farm_active(old_reward)
                now_active = is_farm_active(new_reward)
                log.info(f"Was active: {was_active}, Now active: {now_active}")
                if not was_active and now_active:
                    # Farm restart
                    if new_reward["weekly_reward"] > 0 and old_reward["weekly_reward"] == 0:
                        new_farms.append((pool_account, reward_idx, new_reward, token_mint_0, token_mint_1))
                        
                elif was_active and now_active:
                    # Farm vẫn active nhưng config thay đổi
                    if (
                        old_reward["reward_state"] != new_reward["reward_state"] or
                        old_reward["weekly_reward"] != new_reward["weekly_reward"]
                    ):
                        changed_farms.append((pool_account, reward_idx, old_reward, new_reward, token_mint_0, token_mint_1))

    # So sánh từ old_state → tìm farm đã dừng
    for pool_account, rewards in old_state.items():
        for reward_idx, old_reward in rewards.items():
            new_rewards = new_state.get(pool_account, {})
            if pool_account == "D2itnNiHft6My73G41TpNdGcYc3UYy1ihtY76rjkxVwC":
                print(f"New rewards: {new_rewards}")
                print(f"Old reward: {old_reward}")

            new_reward = new_rewards.get(reward_idx)  # ✅ reward_idx đã chuẩn
            
            if "weekly_reward" not in old_reward:
                old_reward["weekly_reward"] = old_reward.get("weekly_rewards", 0)
                # Nếu vẫn không có (ví dụ old_reward lấy từ DB mà thiếu field), mới tính lại
                if old_reward["weekly_reward"] == 0 and "emissions_per_second_x64" in old_reward:
                    old_reward["weekly_reward"] = calc_cake_weekly_reward(client, old_reward)

            # ĐÁNH GIÁ TRẠNG THÁI CŨ (was_active)
            # Không dùng is_farm_active cho dữ liệu cũ vì nếu farm đã hết hạn trước khi script chạy, 
            # nó sẽ trả về False, dẫn đến không phát hiện được sự thay đổi trạng thái.
            # Ta tin tưởng vào trạng thái reward_state = 2 đã lưu trong DB.
            was_active = (int(old_reward.get("reward_state", 0)) == 2 and float(old_reward.get("weekly_reward", 0)) > 0)
            
            now_active = is_farm_active(new_reward) if new_reward else False

            print(f"Pool: {pool_account}, Reward idx: {reward_idx}, Was active: {was_active}, Now active: {now_active}")

            # Nếu farm trước đó active và giờ không còn active → đã dừng
            if was_active and not now_active:
                # Lấy thông tin pool từ DB để lấy token mint chính xác
                pool_db_info = get_pool_sol_info(str(pool_account))
                if pool_db_info:
                    t0_mint = pool_db_info.get("token0_mint", "Unknown")
                    t1_mint = pool_db_info.get("token1_mint", "Unknown")
                else:
                    t0_mint = "Unknown"
                    t1_mint = "Unknown"
                
                stopped_farms.append((pool_account, reward_idx, old_reward, t0_mint, t1_mint))
                log.info(f"Stopped farm: {pool_account}, Reward idx: {reward_idx}, Old reward: {old_reward}")
                
                # CẬP NHẬT TRẠNG THÁI ĐÃ DỪNG VÀO new_state ĐỂ save_state LƯU VÀO DB
                if pool_account not in new_state:
                    new_state[pool_account] = {}
                
                # Tạo một bản sao dữ liệu và đánh dấu là dừng (state = 0)
                stopped_reward_data = dict(old_reward)
                stopped_reward_data["reward_state"] = 0 
                # Lưu vào new_state để cuối cùng save_state sẽ ghi đè lên DB
                new_state[pool_account][reward_idx] = stopped_reward_data
            
    # Notify
    for pool_account, reward_idx, data, token_mint_0, token_mint_1 in new_farms:
        open_time = datetime.fromtimestamp(data["open_time"]).strftime("%Y-%m-%d")
        end_time = datetime.fromtimestamp(data["end_time"]).strftime("%Y-%m-%d")
        reward_total_emissioned = data["reward_total_emissioned"]
        weekly_reward = data["weekly_reward"]
        
        pool_db_info = get_pool_sol_info(str(pool_account))
        if pool_db_info is not None:
            token_mint_0_symbol = str(pool_db_info.get("token0_symbol", "Unknown"))
            token_mint_1_symbol = str(pool_db_info.get("token1_symbol", "Unknown"))
            token_mint_0_decimals = pool_db_info.get("token0_decimals", 18)
            token_mint_1_decimals = pool_db_info.get("token1_decimals", 18)
        else:
            token_mint_0_symbol = get_symbol_token(token_mint_0, client)
            token_mint_1_symbol = get_symbol_token(token_mint_1, client)
            token_mint_0_decimals = get_token_reward_decimals(client, token_mint_0)
            token_mint_1_decimals = get_token_reward_decimals(client, token_mint_1)
            
        token_reward_decimals = get_token_reward_decimals(client, data["token_mint"])
        reward_account = data["token_mint"]
        reward_symbol = get_token_reward_symbol(client, reward_account)
        
        # Update pool sol info table
        upsert_pool_info(
            connection, chain, pool_account, token_mint_0, token_mint_1, token_mint_0_symbol,
            token_mint_1_symbol, token_mint_0_decimals, token_mint_1_decimals, data,
            reward_account, reward_symbol, weekly_reward
        )
        
        sym0 = token_mint_0_symbol.rstrip('\x00')
        sym1 = token_mint_1_symbol.rstrip('\x00')
        
        reward_state = data["reward_state"]
        msg = (
            f"🎉 [NEW FARM ACTIVE] on SOLANA\n"
            f"🔹 Pool Account: `{pool_account}`\n"
            f"🔸 Reward Index: `{reward_idx+1}`\n"
            f"💎 Reward State: `{reward_state}`\n"
            f"💰 Pair: `{sym0} - {sym1}`\n"
            f"📈 Total Reward Emissioned: {reward_total_emissioned / (10**token_reward_decimals):.2f} (assume 9 decimals)\n"
            f"📈 Weekly Reward: {weekly_reward} CAKE\n"
            f"🕒 Active Time: {open_time} → {end_time}\n"
            f"📌 Explorer: https://solana.pancakeswap.finance/clmm/create-position/?pool_id={pool_account}\n"
        )
        notify_discord(msg)

    for pool_account, reward_idx, old_data, data, token_mint_0, token_mint_1 in changed_farms:
        open_time = datetime.fromtimestamp(data["open_time"]).strftime("%Y-%m-%d")
        end_time = datetime.fromtimestamp(data["end_time"]).strftime("%Y-%m-%d")
        reward_total_emissioned = data["reward_total_emissioned"]
        
        old_weekly_reward = old_data["weekly_reward"]
        weekly_reward = data["weekly_reward"]
        
        pool_db_info = get_pool_sol_info(str(pool_account))
        if pool_db_info is not None:
            token_mint_0_symbol = str(pool_db_info.get("token0_symbol", "Unknown"))
            token_mint_1_symbol = str(pool_db_info.get("token1_symbol", "Unknown"))
            token_mint_0_decimals = pool_db_info.get("token0_decimals", 18)
            token_mint_1_decimals = pool_db_info.get("token1_decimals", 18)
        else:
            token_mint_0_symbol = get_symbol_token(token_mint_0, client)
            token_mint_1_symbol = get_symbol_token(token_mint_1, client)
            token_mint_0_decimals = get_token_reward_decimals(client, token_mint_0)
            token_mint_1_decimals = get_token_reward_decimals(client, token_mint_1)
        
        reward_state = data["reward_state"]
        token_reward_decimals = get_token_reward_decimals(client, data["token_mint"])
        
        # Update pool sol info table khi farm thay đổi
        upsert_pool_info(
            connection, chain, pool_account, token_mint_0, token_mint_1, token_mint_0_symbol,
            token_mint_1_symbol, token_mint_0_decimals, token_mint_1_decimals, data,
            data["token_mint"], get_token_reward_symbol(client, data["token_mint"]),
            weekly_reward
        )
        
        sym0 = token_mint_0_symbol.rstrip('\x00')
        sym1 = token_mint_1_symbol.rstrip('\x00')
        
        msg = (
            f"🔁 [FARM CHANGED] on SOLANA\n"
            f"🔹 Pool Account: `{pool_account}`\n"
            f"🔸 Reward Index: `{reward_idx+1}`\n"
            f"💎 Reward State: `{reward_state}`\n"
            f"💰 Pair: `{sym0} - {sym1}`\n"
            f"📈 Weekly Reward: {old_weekly_reward} → {weekly_reward} CAKE\n"
            f"📈 Total Reward Emissioned: {reward_total_emissioned / (10**token_reward_decimals):.2f} (assume 9 decimals)\n"
            f"🕒 Active Time: {open_time} → {end_time}\n"
            f"📌 Explorer: https://solana.pancakeswap.finance/clmm/create-position/?pool_id={pool_account}\n"
        )
        notify_discord(msg)

    for pool_account, reward_idx, data, token_mint_0, token_mint_1 in stopped_farms:
        open_time = datetime.fromtimestamp(data["open_time"]).strftime("%Y-%m-%d")
        end_time = datetime.fromtimestamp(data["end_time"]).strftime("%Y-%m-%d")
        reward_total_emissioned = data["reward_total_emissioned"]
        weekly_reward = data["weekly_reward"]
        
        pool_db_info = get_pool_sol_info(str(pool_account))
        if pool_db_info is not None:
            token_mint_0_symbol = str(pool_db_info.get("token0_symbol", "Unknown"))
            token_mint_1_symbol = str(pool_db_info.get("token1_symbol", "Unknown"))
            token_mint_0_decimals = pool_db_info.get("token0_decimals", 18)
            token_mint_1_decimals = pool_db_info.get("token1_decimals", 18)
        else:
            token_mint_0_symbol = get_symbol_token(token_mint_0, client)
            token_mint_1_symbol = get_symbol_token(token_mint_1, client)
            token_mint_0_decimals = 18
            token_mint_1_decimals = 18
        
        reward_state = data["reward_state"]
        token_reward_decimals = get_token_reward_decimals(client, data["token_mint"])
        
        stopped_data = dict(data)

        upsert_pool_info(
            connection, chain, pool_account, token_mint_0, token_mint_1, token_mint_0_symbol,
            token_mint_1_symbol, token_mint_0_decimals, token_mint_1_decimals, stopped_data,
            data["token_mint"], get_token_reward_symbol(client, data["token_mint"]), 0
        )
        
        sym0 = token_mint_0_symbol.rstrip('\x00')
        sym1 = token_mint_1_symbol.rstrip('\x00')
        
        msg = (
            f"🛑 [FARM STOPPED] on SOLANA\n"
            f"🔹 Pool Account: `{pool_account}`\n"
            f"🔸 Reward Index: `{reward_idx+1}`\n"
            f"💎 Reward State: `{reward_state}`\n"
            f"💰 Pair: `{sym0} - {sym1}`\n"
            f"📈 Total Reward Emissioned: {reward_total_emissioned / (10**token_reward_decimals):.2f}\n"
            f"📈 Weekly Reward: {weekly_reward} CAKE\n"
            f"🕒 Active Time: {open_time} → {end_time}\n"
            f"📌 Explorer: https://solana.pancakeswap.finance/clmm/create-position/?pool_id={pool_account}\n"
        )
        notify_discord(msg)

    return new_state

def main():
    client = Client("https://mainnet.helius-rpc.com/?api-key=bb4fcdca-d41d-4930-ada1-6490968dabe4")

    connection = get_connection()
    state, first_run = load_state(connection)

    state = check_farm(client, PROGRAM_ID, first_run, state, connection, chain="SOL")
    
    save_state(state, connection, "SOL", client=client)
    
if __name__ == "__main__":
    main()
    
