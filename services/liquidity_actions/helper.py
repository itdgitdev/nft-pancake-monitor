import json
import os
import requests
import time
from config import *
from services.db_connect import *
import math
from solana.rpc.api import Client
from solders.pubkey import Pubkey
from services.db_connect import get_connection
import mysql.connector

def get_target_token_address(web3: Web3, token_address: str) -> str:
    token_address = web3.to_checksum_address(token_address)

    # EIP-1967
    try:
        slot = web3.keccak(text="eip1967.proxy.implementation")
        impl_slot = int.from_bytes(slot, byteorder='big') - 1
        raw = web3.eth.get_storage_at(token_address, impl_slot)
        if raw and len(raw) == 32 and int(raw.hex(), 16) != 0:
            impl_address = web3.to_checksum_address('0x' + raw.hex()[-40:])
            return impl_address
    except: pass

    # EIP-897
    try:
        selector = web3.keccak(text='implementation()')[:4]
        result = web3.eth.call({'to': token_address, 'data': selector.hex()})
        if result and len(result) == 32:
            impl_address = web3.to_checksum_address('0x' + result.hex()[-40:])
            return impl_address
    except: pass

    # EIP-1167 minimal proxy
    try:
        bytecode = web3.eth.get_code(token_address).hex()
        if bytecode.startswith('0x363d3d373d3d3d363d73') and bytecode.endswith('5af43d82803e903d91602b57fd5bf3'):
            impl_address = '0x' + bytecode[22:62]
            impl_address = web3.to_checksum_address(impl_address)
            return impl_address
    except: pass

    # Nếu không phải proxy, trả về token gốc
    return token_address

# Get ABI of contract address
abi_memory_cache = {}
def get_abi(chain, contract_address):
    global abi_memory_cache
    key = f"{chain}_{contract_address.lower()}"

    # ✅ Ưu tiên dùng cache trong bộ nhớ
    if key in abi_memory_cache:
        #print(f"✅ Loaded ABI from memory cache for {contract_address}")
        return abi_memory_cache[key]

    # ✅ Tạo đường dẫn cache file
    abi_cache_dir = "./abi_cached"
    os.makedirs(abi_cache_dir, exist_ok=True)
    cache_path = os.path.join(abi_cache_dir, f"{key}.json")

    # ✅ Nếu có file cache → dùng luôn
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                abi = json.load(f)
                abi_memory_cache[key] = abi  # cache vào bộ nhớ
                #print(f"✅ Loaded ABI from file cache for {contract_address}")
                return abi
        except Exception as e:
            print(f"⚠️ Error reading cached ABI: {e}, retrying from API...")

    # ✅ Nếu không có → gọi API
    if chain not in API_URLS or chain not in API_KEYS:
        print(f"❌ No API URL or API Key for {chain}")
        return None

    etherscan_url = API_URLS[chain]
    params = {
        "module": "contract",
        "action": "getabi",
        "address": contract_address,
        "apikey": API_KEYS['ETH']  # Bạn có thể sửa theo chain nếu cần
    }

    try:
        response = requests.get(etherscan_url, params=params)
        response_json = response.json()

        if response.status_code == 200 and response_json["status"] == "1":
            try:
                abi = json.loads(response_json["result"])
                abi_memory_cache[key] = abi  # cache vào bộ nhớ

                # ✅ Lưu vào file cache
                with open(cache_path, "w") as f:
                    json.dump(abi, f)

                print(f"✅ Fetched and cached ABI for {contract_address}")
                return abi
            except json.JSONDecodeError:
                print("❌ Error while decoding JSON ABI")
                return None
        else:
            print(f"❌ Failed to fetch ABI: {response_json.get('result')}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Error retrieving contract ABI: {e}")
        return None
    
def get_token_info(w3, chain, token_address):
    usdc_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    target_token_address = get_target_token_address(w3, token_address)
    if token_address.lower() == usdc_address.lower():
        target_token_address = "0x43506849d7c04f9138d1a2050bbf3a0c054402dd"
    
    token_abi = get_abi(chain, target_token_address)
    token_contract = w3.eth.contract(address=token_address, abi=token_abi)
    
    symbol = token_contract.functions.symbol().call()
    decimals = token_contract.functions.decimals().call()
    
    return  symbol, decimals

# Connect to web3
def get_web3_connection(chain_name):
    provider_url = RPC_URLS.get(chain_name)
    if not provider_url:
        raise Exception(f"❌ No RPC URL for {chain_name}")
    return Web3(Web3.HTTPProvider(provider_url))

# Create Instance contract
def get_contract(web3, contract_address, abi):
    contract = web3.eth.contract(address=contract_address, abi=abi)
    return contract

def tick_to_price(tick):
    return 1.0001 ** tick

def round_tick(tick, tick_spacing):
    remainder = tick % tick_spacing
    if remainder >= tick_spacing / 2:
        return tick + (tick_spacing - remainder)
    else:
        return tick - remainder

def build_transaction_safely(chain_name, contract_function, from_address, value=0):
    w3 = get_web3_connection(chain_name)
    if not w3:
        print(f"❌ Không thể kết nối Web3 tới {chain_name}.")
        return
    try:
        # Estimate gas và thêm buffer
        gas_estimate = contract_function.estimate_gas({'from': from_address, 'value': w3.to_wei(value, "ether")})
        gas_limit = int(gas_estimate * 1.2)  # thêm 20% để phòng sai số

        # Lấy gas price hiện tại
        gas_price = w3.eth.gas_price

        # Tính chi phí giao dịch
        required_balance = gas_limit * gas_price + value

        # Lấy balance của ví
        balance = w3.eth.get_balance(from_address)

        # Kiểm tra đủ balance không
        if balance < required_balance:
            error_message = (
                f"   [Chain: {chain_name}] Không đủ native token để gửi giao dịch.\n"
                f"   Cần: {w3.from_wei(required_balance, 'ether')} > Có: {w3.from_wei(balance, 'ether')}\n"
                f"   Gas Limit: {gas_limit}, Gas Price: {w3.from_wei(gas_price, 'gwei')} Gwei"
            )
            # send_discord_webhook_message(error_message, DISCORD_WEBHOOK_URL)  # Gửi thông báo lỗi đến Discord
            raise Exception(error_message)

        # Lấy nonce
        nonce = w3.eth.get_transaction_count(from_address, 'pending')

        # Build transaction
        txn = contract_function.build_transaction({
            'from': from_address,
            'nonce': nonce,
            'gas': gas_limit,
            'gasPrice': gas_price,
            'value': w3.to_wei(value, "ether")
        })

        return txn

    except Exception as e:
        error_message = f"❌ Lỗi khi xây dựng giao dịch: {e}"
        raise Exception(error_message)

def ensure_wrapped_token_balance(chain_name, account, token_address, token_decimals, needed_amount, private_key):
    w3 = get_web3_connection(chain_name)
    if not w3:
        print(f"❌ Không thể kết nối Web3 tới {chain_name}.")
        return

    # Địa chỉ token wrapped tương ứng (VD: BSC -> WBNB, ETH -> WETH)
    wrapped_token_address = WRAPPED_TOKENS.get(chain_name)
    if not wrapped_token_address:
        raise Exception(f"❌ Không tìm thấy wrapped token cho {chain_name}")

    if token_address.lower() == wrapped_token_address.lower():
        # Lấy ABI của wrapped token
        token_abi = get_abi(chain_name, wrapped_token_address)
        token_contract = get_contract(w3, wrapped_token_address, token_abi)

        # Số dư wrapped token hiện tại
        wrapped_balance = token_contract.functions.balanceOf(account).call()
        native_balance = w3.eth.get_balance(account)

        print(f"💰 Wrapped balance: {wrapped_balance}")
        print(f"💰 Native balance: {native_balance}")
        print(f"💰 Needed amount: {needed_amount}")
        
        needed_amount_wei = needed_amount * (10 ** token_decimals)
        print(f"💰 Needed amount in decimals: {needed_amount_wei}")
        
        if wrapped_balance < needed_amount_wei:
            missing = needed_amount_wei - wrapped_balance
            print(f"⚠️ Cần wrap thêm {missing} native token.")
            missing += 1_000
            if native_balance >= missing:
                # Gọi hàm deposit() của WBNB/WETH contract để wrap
                wrap_tx = token_contract.functions.deposit()
                build_wrap_tx = build_transaction_safely(
                    chain_name,
                    wrap_tx,
                    account,
                    value=(missing / (10 ** token_decimals))  # chỉ wrap phần thiếu
                )

                signed_wrap_tx = w3.eth.account.sign_transaction(build_wrap_tx, private_key)
                tx_hash_wrap = w3.eth.send_raw_transaction(signed_wrap_tx.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash_wrap)
                
                if receipt.status == 1:
                    print(f"✅ Wrapped thành công: {tx_hash_wrap.hex()}")
                else:
                    raise Exception("❌ Wrap transaction failed!")
            else:
                raise Exception("❌ Không đủ native token để wrap.")
        else:
            print("✅ Đã đủ wrapped token balance.")

    else:
        pass

def approve_token_if_needed(w3, chain_name, token_address, token_decimals, spender, amount, account, private_key):
    """
    Approve token cho spender nếu allowance hiện tại nhỏ hơn amount cần.
    """
    target_token_address = get_target_token_address(w3, token_address)
    
    token_abi = get_abi(chain_name, target_token_address)
    token_contract = get_contract(w3, token_address, token_abi)

    current_allowance = token_contract.functions.allowance(account, spender).call()
    target_amount = int(amount * (10 ** token_decimals))

    if current_allowance < target_amount:
        approve_tx = token_contract.functions.approve(spender, target_amount)
        build_tx = build_transaction_safely(chain_name, approve_tx, account)
        
        signed_tx = w3.eth.account.sign_transaction(build_tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"✅ Approved {token_address} for {spender}: {tx_hash.hex()}")
    else:
        print(f"✅ Allowance đủ, không cần approve lại cho {token_address}.")

def get_current_amounts(liquidity, sqrt_price_x96, tick_lower, tick_upper):
    sqrt_price = float(sqrt_price_x96) / 2**96
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

def get_pool_address(chain, w3, token0_address, token1_address, fee_tier):
    factory_address = FACTORY_ADDRESSES.get(chain, "unknown")
    if factory_address == "unknown":
        return "unknown"
    
    factory_abi = get_abi(chain, factory_address)
    factory_contract = get_contract(w3, factory_address, factory_abi)

    pool_address = factory_contract.functions.getPool(token0_address, token1_address, fee_tier).call()
    return pool_address

def get_position_info(chain, token_id):
    w3 = get_web3_connection(chain)
    if not w3:
        print(f"❌ Không thể kết nối Web3 tới {chain}.")
        return 
    
    npm_address = NPM_ADDRESSES.get(chain, "unknown")
    npm_abi = get_abi(chain, npm_address)
    npm_contract = get_contract(w3, npm_address, npm_abi)

    position_info = npm_contract.functions.positions(token_id).call()
    token0_address = position_info[2]
    token1_address = position_info[3]
    fee = position_info[4]
    tick_lower = position_info[5]
    tick_upper = position_info[6]
    liquidity = position_info[7]
    tokens_owed0 = position_info[10]
    tokens_owed1 = position_info[11]
    
    pool_address = get_pool_address(chain, w3, token0_address, token1_address, fee)
    print(f"Pool address: {pool_address}")
    
    pool_info = get_pool_info_from_db(chain, pool_address)
    token0_symbol = pool_info['token0_symbol']
    token1_symbol = pool_info['token1_symbol']
    
    return {
        "tick_lower": tick_lower,
        "tick_upper": tick_upper,
        "pool_address": pool_address,
        "token0_address": token0_address,
        "token1_address": token1_address,
        "token0_symbol": token0_symbol,
        "token1_symbol": token1_symbol,
        "fee_tier": fee,
        "liquidity": liquidity,
        "tokens_owed0": tokens_owed0,
        "tokens_owed1": tokens_owed1,
    }

def get_pool_info_from_db(chain_name, pool_address):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ 1. Check trong DB
        cursor.execute("""
            SELECT * FROM pool_info 
            WHERE pool_address = %s AND chain = %s
        """, (pool_address, chain_name))
        result = cursor.fetchone()
        if result:
            print("✅ Pool data found in DB")
            pool_address = Web3.to_checksum_address(result["pool_address"])
            return {
                "chain": result["chain"],
                "pool_address": pool_address,
                "token0_address": result["token0_address"],
                "token1_address": result["token1_address"],
                "token0_symbol": result["token0_symbol"],
                "token1_symbol": result["token1_symbol"],
                "token0_decimals": result["token0_decimals"],
                "token1_decimals": result["token1_decimals"],
                "fee": result["fee"],
                "pid": result["pid"],
                "source": "db"
            }

    except Exception as e:
        print(f"❌ Error in get_pool_info_from_db: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_pool_sol_info_from_db(chain_name, pool_account):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ 1. Check trong DB
        cursor.execute("""
            SELECT * FROM pool_sol_info 
            WHERE pool_account = %s AND chain = %s
        """, (pool_account, chain_name))
        result = cursor.fetchone()
        if result:
            print("✅ Pool data found in DB")
            return {
                "chain": result["chain"],
                "pool_address": pool_account,
                "token0_mint": result["token0_mint"],
                "token1_mint": result["token1_mint"],
                "token0_symbol": result["token0_symbol"],
                "token1_symbol": result["token1_symbol"],
                "token0_decimals": result["token0_decimals"],
                "token1_decimals": result["token1_decimals"],
                "fee": result["fee"],
                "source": "db"
            }

    except Exception as e:
        print(f"❌ Error in get_pool_sol_info_from_db: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
def get_tick_array_bitmap_account_from_db(pool_id):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ 1. Check trong DB
        cursor.execute("""
            SELECT tick_array_bitmap_extension_account FROM extreme_price_range_pool_sol 
            WHERE pool_id = %s
        """, (pool_id, ))
        result = cursor.fetchone()
        if result:
            print("✅ Tick array bitmap account found in DB")
            return result["tick_array_bitmap_extension_account"]

    except Exception as e:
        print(f"❌ Error in get_tick_array_bitmap_account_from_db: {e}")
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()