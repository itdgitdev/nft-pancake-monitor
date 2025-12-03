import json
from datetime import datetime
from services.execute_data import insert_nft_data
from services.solana.get_wallet_info import process_nft_mint_data_sol
from services.list_farm_pancake import process_nft_mint_data_evm, get_abi, get_contract, get_web3
from config import CHAIN_API_MAP, NPM_ADDRESSES, FACTORY_ADDRESSES, MASTERCHEF_ADDRESSES
from services.update_query import get_total_alloc_point_each_chain, get_total_cake_per_day_on_chain

def serialize_value(v):
    if isinstance(v, datetime):
        return v.isoformat()
    elif isinstance(v, (list, tuple)):
        return [serialize_value(x) for x in v]
    elif isinstance(v, dict):
        return {k: serialize_value(val) for k, val in v.items()}
    return v

def process_nft(nft_id, chain, wallet):
    # Lấy dữ liệu NFT trực tiếp
    data = process_nft_mint_data_sol(
        nft_id, chain, wallet,
        status_map={}, position_map={}, pool_map={}, inactived_nft_ids=[]
    )
    
    if data is None:
        # Không có data → báo lỗi
        return False, None

    # Ghi trực tiếp vào DB
    insert_nft_data([data])

    # Nếu cần serialize dữ liệu để trả về
    serialized_data = serialize_value(data)

    # Trả về dữ liệu luôn, không publish qua Redis
    return True, serialized_data

def process_nft_evm(nft_id, chain, wallet):
    w3 = get_web3(chain)
    chain_api = CHAIN_API_MAP.get(chain)
    # Get multipliers for each chain
    multiplier_chain = get_total_alloc_point_each_chain(chain=chain)

    # Get total cake reward per second on chain
    total_cake_per_day_of_chain = get_total_cake_per_day_on_chain(chain)
    
    # Total cake reward per second each chain
    cake_per_second = total_cake_per_day_of_chain / 86400

    npm_address = NPM_ADDRESSES.get(chain, "unknown")
    factory_address = FACTORY_ADDRESSES.get(chain, "unknown")
    masterchef_address = MASTERCHEF_ADDRESSES.get(chain, "unknown")
    
    npm_abi = get_abi(chain, npm_address)
    factory_abi = get_abi(chain, factory_address)
    masterchef_abi = get_abi(chain, masterchef_address)
    
    npm_contract = get_contract(w3, npm_address, npm_abi)
    factory_contract = get_contract(w3, factory_address, factory_abi)
    masterchef_contract = get_contract(w3, masterchef_address, masterchef_abi)
    
    data = process_nft_mint_data_evm(
        chain, wallet, nft_id,
        status_map={}, position_map={}, factory_contract=factory_contract,
        w3=w3, chain_api=chain_api,multiplier_chain=multiplier_chain,
        cake_per_second=cake_per_second, npm_contract=npm_contract,
        masterchef_contract=masterchef_contract, inactived_nft_ids=[],
        npm_abi=npm_abi, masterchef_abi=masterchef_abi, mode="cron"
    )
    
    if data is None:
        # Không có data → báo lỗi
        return False, None
    
    insert_nft_data([data])
    
    # Nếu cần serialize dữ liệu để trả về
    serialized_data = serialize_value(data)

    # Trả về dữ liệu luôn, không publish qua Redis
    return True, serialized_data
