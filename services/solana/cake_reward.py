import sys
import os
# Lấy path tới root của project
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

from services.solana.decode_account import *
from config import *
from decimal import Decimal, getcontext
import base64

getcontext().prec = 50

TWO_64 = Decimal(2) ** 64
WEEK_SECONDS = Decimal(7 * 24 * 3600)

def calc_cake_weekly_reward_per_pool(client, pool_id):
    pool_data = decode_pool_state(client, pool_id)
    reward_infos = pool_data.get("reward_infos")
    
    if reward_infos is None:
        return None

    results = {}
    for reward_info in reward_infos:
        if reward_info.get("reward_state") != 2:
            continue
        
        token_mint = reward_info.get("token_mint", "")
        token_mint_info = decode_metadata_pda(client, token_mint)
        token_symbol = token_mint_info.get("symbol", "").strip("\x00")
        token_mint_decimals = Decimal(get_token_decimals(client, token_mint))
        
        duration = Decimal(reward_info.get("end_time", 0)) - Decimal(reward_info.get("open_time", 0))
        eps_x64 = Decimal(reward_info.get("emissions_per_second_x64", 0))
        eps_base = eps_x64 / TWO_64
        total_base = eps_base * duration
        total_token_reward = total_base / (Decimal(10) ** token_mint_decimals)
        
        weeks = duration / WEEK_SECONDS
        weekly_token = total_token_reward / weeks

        results[token_symbol] = float(weekly_token)
        
    return results

def calc_cake_weekly_reward(client, reward_info):
    if reward_info.get("reward_state") != 2:
        return 0
    
    token_mint = reward_info.get("token_mint", "")
    token_mint_decimals = Decimal(get_token_decimals(client, token_mint))
    
    duration = Decimal(reward_info.get("end_time", 0)) - Decimal(reward_info.get("open_time", 0))
    eps_x64 = Decimal(reward_info.get("emissions_per_second_x64", 0))
    eps_base = eps_x64 / TWO_64
    total_base = eps_base * duration
    total_token_reward = total_base / (Decimal(10) ** token_mint_decimals)
    
    weeks = duration / WEEK_SECONDS
    weekly_token = total_token_reward / weeks
    
    return float(weekly_token)

if __name__ == "__main__":
    pool_id = "AdqrAZpRM4msAz1kSASsQmfJ5km84T9PhBiBZbHRQHdP"
    
    print(calc_cake_weekly_reward_per_pool(CLIENT, pool_id))
    