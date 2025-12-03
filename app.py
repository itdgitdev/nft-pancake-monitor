from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
from functools import wraps
from services.list_farm_pancake import get_nft_data
from services.execute_data import (
    insert_nft_data, fetch_list_bond_data, insert_bond_data, update_bond_status, delete_bond_contract, 
    update_bond_data, fetch_bond_data_by_contract_address, fetch_and_update_bonds, get_connection
)
from services.update_query import (
    fetch_latest_nft_id, fetch_latest_nft_by_wallet, fetch_latest_nft_by_wallet_and_chain,
    fetch_nft_history_by_id, count_nft_history_records_by_id, toggle_blacklist, fetch_blacklist_nft_ids,
    fetch_latest_summary_by_token, fetch_latest_summary_by_wallet_and_chain, get_latest_total_pending_cake_by_wallet,
    get_latest_total_pending_cake_by_wallet_and_chain, fetch_all_pool_info, fetch_all_pool_sol_info, enrich_with_pool_info, filter_by_token,
    get_futures_positions_binance_data_by_wallet, get_futures_orders_binance_data_by_wallet
)

from services.transaction_history.tx_his import get_transactions_with_filter
from services.transaction_history.sol_tx_his import get_transaction_sol_with_filter
from services.solana.get_price_range_pool import analyze_pool_ticks
from services.excute_transaction import get_existing_wallet, get_transaction

from services.liquidity_actions.mint_position_sol import get_data_mint_sol, get_mint_params_sol, build_mint_position_tx_sol_v8
from services.helper import merge_summary

import os, secrets
from eth_account.messages import encode_defunct
from eth_account import Account
from web3 import Web3
from datetime import datetime
import json
import time
from solders.pubkey import Pubkey
from config import *
from services.solana.get_wallet_info import * 
from logging_config import system_logger as log
import base58
import nacl.signing
import nacl.exceptions
from services.solana.refresh_nft_data import process_nft, process_nft_evm

app = Flask(__name__)

# Thông tin đăng nhập cơ bản
USERNAME = "admin"
PASSWORD = "it.d@2025"  # Thay bằng mật khẩu mạnh của bạn

# Kiểm tra xác thực Basic Auth
def check_auth(username, password):
    return username == USERNAME and password == PASSWORD

# Trả về response yêu cầu đăng nhập
def authenticate():
    return Response(
        "You must be logged in.", 401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'}
    )

# Middleware yêu cầu đăng nhập trước mỗi request
@app.before_request
def require_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

app.secret_key = 'nhat12398'

@app.route("/api/me", methods=["GET"])
def me():
    user = session.get("user")
    if not user:
        return jsonify(logged_in=False)
    return jsonify(logged_in=True, address=user)

# API lấy nonce
@app.route("/api/get_nonce", methods=["GET"])
def get_nonce():
    nonce = secrets.token_hex(16)  # chuỗi random
    session["login_nonce"] = nonce
    return jsonify(nonce=nonce)

@app.route("/api/verify_signature", methods=["POST"])
def verify_signature():
    data = request.json
    address = data.get("address")
    signature = data.get("signature")
    chain = data.get("chain", "evm")  # mặc định là EVM
    nonce = session.get("login_nonce")

    if not nonce:
        return jsonify(error="NO_NONCE"), 400

    try:
        # ============ EVM (Ethereum, BSC, Polygon, ...) ============
        if chain.lower() == "evm":
            message = encode_defunct(text=nonce)
            recovered = Account.recover_message(message, signature=signature)
            if recovered.lower() == address.lower():
                session["user"] = address
                return jsonify(success=True, address=address, chain="evm")
            else:
                return jsonify(success=False, error="SIGNATURE_INVALID"), 401

        # ============ SOLANA ============
        elif chain.lower() == "solana":
            # ✅ Solana ký bằng Ed25519 → verify bằng PyNaCl
            public_key_bytes = base58.b58decode(address)
            signature_bytes = bytes.fromhex(signature)
            message_bytes = nonce.encode("utf-8")

            verify_key = nacl.signing.VerifyKey(public_key_bytes)
            try:
                verify_key.verify(message_bytes, signature_bytes)
                session["user"] = address
                return jsonify(success=True, address=address, chain="solana")
            except nacl.exceptions.BadSignatureError:
                return jsonify(success=False, error="SIGNATURE_INVALID"), 401

        else:
            return jsonify(error="UNSUPPORTED_CHAIN"), 400

    except Exception as e:
        return jsonify(error=str(e)), 400

@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()  # Xóa tất cả dữ liệu session
    return jsonify({"success": True})

# Hàm định dạng theo token symbol
def format_token_amount(value, token_symbol):
    token = token_symbol.upper()
    if "ETH" in token or "BNB" in token:
        return f"{value:,.3f}"
    elif "BTC" in token:
        return f"{value:,.4f}"
    elif "SOL" in token:
        return f"{value:,.3f}"
    else:
        return f"{int(value):,}"

# Đăng ký cho Jinja2
app.jinja_env.globals.update(format_token_amount=format_token_amount)

last_update_time = None

def is_evm_address(wallet: str) -> bool:
    return wallet.startswith("0x")

def is_solana_address(wallet: str) -> bool:
    return not wallet.startswith("0x") 

wallet_list = [
    # "0x88DE2AB47352779494547CaCCB31eE1A133dd334",
    # "0x349F8F068120E04B359556E442A579Af41ebF486",
    # "0x065994BeC6cA97AeF488f76824580814Be4E024F",
    # "0xafCf63AbF4d061fC000Ad1244c74851e52F67b01",
    "0xf1A0b11fcF3580a9C80Cb56aEF95fceB949c60a2",
    # "CJoUCt78FNbJJcKW3CnmLG9CVq6ANSTiXWV1tyN5dXw9",
    # "4rDyyA4vydw4T5uekxY5La4Ywv43nSZ2PgG7rfBfvQAJ",
    # "DGHsf8b99KyWPErCbVuXcPUxAXwaC7bqndPgEVvmSAFn",
    # "8x4zj74myKzox48jUMHskfNo4NHuAzXeLyXs7HLUSYzL"
]

def save_update_status(wallets, chains):
    update_data = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "wallets": wallets,
        "chains": chains
    }
    with open("/home/dev/nft_pancake_app/flask_app/update_status.json", "w") as f:
    # with open("update_status.json", "w") as f:
        json.dump(update_data, f)

last_fetched_wallets = []
last_fetched_chains = []

def auto_fetch_data():
    global last_update_time, last_fetched_wallets, last_fetched_chains

    # reset mỗi lần chạy
    last_fetched_wallets = []
    last_fetched_chains = []
    errors = []

    for wallet in wallet_list:
        # xác định chain
        if is_evm_address(wallet):
            # chains = ["BNB", "ETH", "ARB", "BAS", "LIN"]
            chains = ["MON"]
        elif is_solana_address(wallet):
            try:
                Pubkey.from_string(wallet)  # check hợp lệ
                chains = ["SOL"]
            except Exception:
                log.warning(f"⚠️ Wallet {wallet} is not a valid Solana address. Skipping...")
                continue
        else:
            log.warning(f"⚠️ Wallet {wallet} is not a valid EVM or Solana address. Skipping...")
            continue

        # fetch data từng chain
        for chain_name in chains:
            try:
                print(f"🔍 Getting data for {wallet} on chain {chain_name}")

                if chain_name != "SOL":
                    cs_wallet = Web3.to_checksum_address(wallet)
                    nft_data = get_nft_data(cs_wallet, chain_name)
                else:
                    wallet_pubkey = Pubkey.from_string(wallet)
                    nft_data = get_nft_solana_data(wallet_pubkey, TOKEN_ACCOUNT_OPTS, chain_name)

                if nft_data:
                    insert_nft_data(nft_data)
                    log.info(f"✅ Inserted {len(nft_data)} NFT(s) for {wallet} on {chain_name}")
                else:
                    log.warning(f"ℹ️ No NFT data for {wallet} on {chain_name}")

                last_fetched_chains.append(chain_name)

            except Exception as e:
                log.error(f"❌ Failed to fetch data for {wallet} on {chain_name}: {e}")
                errors.append((wallet, chain_name, str(e)))

            time.sleep(1)  # tránh spam RPC

        last_fetched_wallets.append(str(wallet))
        time.sleep(2)

    last_update_time = datetime.now()
    save_update_status(last_fetched_wallets, last_fetched_chains)

    # summary cuối cùng
    log.info(f"🏁 Finished auto_fetch_data at {last_update_time}")
    log.info(f"✅ Processed wallets: {len(last_fetched_wallets)} | chains: {len(last_fetched_chains)}")
    if errors:
        log.error(f"⚠️ Encountered {len(errors)} errors")
        for w, c, e in errors:
            log.error(f"   - Wallet {w} | Chain {c} | Error: {e}")

@app.route('/check_update')
def check_update():
    try:
        # Đọc dữ liệu từ file JSON
        with open("/home/dev/nft_pancake_app/flask_app/update_status.json") as f:
        # with open("update_status.json") as f:
            update_data = json.load(f)
            return jsonify({
                "status": "success",
                "last_update": update_data.get("last_update"),
                "wallets": update_data.get("wallets"),
                "chains": update_data.get("chains"),
                "message": f"✅ Fetched and saved data for wallet: {', '.join(update_data.get('wallets', []))} on chains: {', '.join(update_data.get('chains', []))}"
            })
    except Exception as e:
        return jsonify({"status": "no_update", "message": None})

@app.route('/', methods=['GET', 'POST'])
def index():
    wallet_address = None
    message = None
    start_date = None
    end_date = None
    nft_data = []
    summary_data = []

    page = int(request.args.get("page", 1))  # Lấy số trang từ query param
    per_page = 30  # Số item mỗi trang
    total_pages = 0
    total_pending_cake = 0
    
    token = request.args.get('token', '').strip()
    
    # New param để lấy lịch sử của 1 NFT cụ thể
    nft_id = request.args.get('nft_id')

    if request.method == 'POST':
        wallet_address = request.form.get('wallet_address', '').strip()
        chain_name = request.form.get('chain', '').strip()
        action = request.form.get('action')
        chain_name = request.form.get('chain')
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')
        
        # Binance API key
        # binance_api_key = request.form.get("binance_api_key")
        # binance_secret_key = request.form.get("binance_secret_key")
        
        # session["binance_api_key"] = binance_api_key
        # session["binance_secret_key"] = binance_secret_key

        if not wallet_address:
            session['message'] = "⚠️ Please enter a wallet address."
        else:
            try:
                if is_evm_address(wallet_address):
                    checksum_wallet = Web3.to_checksum_address(wallet_address)
                elif is_solana_address(wallet_address):
                    checksum_wallet = Pubkey.from_string(wallet_address)
                else:
                    session['message'] = "⚠️ Please enter a valid wallet address."
                    return redirect(url_for("index"))

                if action == 'fetch_and_store':
                    if chain_name == "SOL":
                        fetched_nft_data = get_nft_solana_data(checksum_wallet, TOKEN_ACCOUNT_OPTS, chain_name)
                    else:
                        fetch_data_time = int((datetime.now() - timedelta(hours=2)).timestamp()) 
                        fetched_nft_data = get_nft_data(checksum_wallet, chain_name, six_months_ago=fetch_data_time)
                        
                    if fetched_nft_data:
                        insert_nft_data(fetched_nft_data)
                        session['message'] = f"✅ Fetched and saved data for wallet: {wallet_address}"
                    else:
                        session['message'] = f"❌ Not found data for wallet: {wallet_address}"

                    return redirect(url_for("index"))

                elif action == 'filter_only':
                    return redirect(url_for("index", wallet_address=wallet_address, start_date=start_date_str, end_date=end_date_str, action='filter_only'))
                
                elif action == 'filter_by_wallet_and_chain':
                    return redirect(url_for("index", wallet_address=wallet_address, chain=chain_name, start_date=start_date_str, end_date=end_date_str, action='filter_by_wallet_and_chain'))
                
            except Exception as e:
                session['message'] = f"❌ No data found for wallet: {wallet_address} in the last 2 hours."

    elif request.method == 'GET':
        wallet_address = request.args.get('wallet_address')
        chain_name = request.args.get('chain')
        action = request.args.get('action')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        
        # Binance API key
        # binance_api_key = session.get("binance_api_key")
        # binance_secret_key = session.get("binance_secret_key")
        
        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            except ValueError:
                start_date = None

        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError:
                end_date = None
        
        if nft_id:
            try:
                nft_id = nft_id.strip()
                offset = (page - 1) * per_page
                nft_data = fetch_nft_history_by_id(nft_id, limit=per_page, offset=offset)
                total_items = count_nft_history_records_by_id(nft_id)
                total_pages = (total_items + per_page - 1) // per_page
                session['message'] = f"✅ Show history for NFT ID: {nft_id}"
            except Exception as e:
                session['message'] = f"⚠️ Error fetching history for NFT ID {nft_id}: {str(e)}"
        elif token:
            try:
                nft_data = fetch_latest_nft_id(status=None)
                if nft_data:
                    nft_data = enrich_with_pool_info(nft_data)
                    nft_data = filter_by_token(nft_data, token)
                    session['message'] = f"✅ Found {len(nft_data)} records for token: {token.lower()}"
                else:
                    session['message'] = f"❌ No data found for token: {token.lower()}"
                    
            except Exception as e:
                session['message'] = f"⚠️ Error fetching data for token {token.lower()}: {str(e)}"
        
        elif action == 'filter_only' and wallet_address:
            try:
                if is_evm_address(wallet_address):
                    checksum_wallet = Web3.to_checksum_address(wallet_address)
                elif is_solana_address(wallet_address):
                    checksum_wallet = Pubkey.from_string(wallet_address)
                else:
                    session['message'] = "⚠️ Please enter a valid wallet address."
                    return redirect(url_for("index"))
                
                # offset = (page - 1) * per_page
                print(f"Filtering by wallet {wallet_address} with dates {start_date} to {end_date}")
                # nft_data = fetch_nft_data_by_wallet_address(checksum_wallet, start_date, end_date, limit=per_page, offset=offset)
                # total_items = count_nft_by_wallet(checksum_wallet, start_date, end_date)
                # if total_items is None:
                #     total_items = 0
                # total_pages = (total_items + per_page - 1) // per_page
                
                nft_data = fetch_latest_nft_by_wallet(str(checksum_wallet))
                summary_data = fetch_latest_summary_by_token(str(checksum_wallet))
                total_pending_cake = get_latest_total_pending_cake_by_wallet(str(checksum_wallet), start_date, end_date)
                if total_pending_cake is None:
                    total_pending_cake = 0
    
                if checksum_wallet:
                    print("🔑 Fetching Binance Futures positions...")
                    positions = get_futures_positions_binance_data_by_wallet(str(checksum_wallet))
                    print(f"- Binance Futures positions: {positions}")

                    # Merge Binance positions vào summary
                    summary_data = merge_summary(summary_data, positions)
                else:
                    positions = []

                # print(f"- Summary data: {summary_data}")
                if nft_data:
                    session['message'] = f"✅ Show NFT data for wallet: {wallet_address}"
                else:
                    session['message'] = f"❌ No NFT data for wallet: {wallet_address}"
            except Exception as e:
                session['message'] = f"⚠️ Error: {str(e)}"
        elif action == 'filter_by_wallet_and_chain' and wallet_address and chain_name:
            try:
                if is_evm_address(wallet_address):
                    checksum_wallet = Web3.to_checksum_address(wallet_address)
                elif is_solana_address(wallet_address):
                    checksum_wallet = Pubkey.from_string(wallet_address)
                else:
                    session['message'] = "⚠️ Please enter a valid wallet address."
                    return redirect(url_for("index"))
                
                # offset = (page - 1) * per_page
                print(f"Filtering by wallet {wallet_address} and chain {chain_name} with dates {start_date} to {end_date}")
                # nft_data = fetch_nft_data_by_wallet_and_chain(chain_name, checksum_wallet, start_date, end_date, limit=per_page, offset=offset)
                # total_items = count_nft_by_wallet_and_chain(chain_name, checksum_wallet, start_date, end_date)
                # if total_items is None:
                #     total_items = 0
                # total_pages = (total_items + per_page - 1) // per_page
                
                nft_data = fetch_latest_nft_by_wallet_and_chain(str(checksum_wallet), chain_name)
                summary_data = fetch_latest_summary_by_wallet_and_chain(str(checksum_wallet), chain_name)
                total_pending_cake = get_latest_total_pending_cake_by_wallet_and_chain(str(checksum_wallet), chain_name, start_date, end_date)
                if total_pending_cake is None:
                    total_pending_cake = 0
                    
                if checksum_wallet:
                    print("🔑 Fetching Binance Futures positions...")
                    positions = get_futures_positions_binance_data_by_wallet(str(checksum_wallet))
                    print(f"- Binance Futures positions: {positions}")

                    # Merge Binance positions vào summary
                    summary_data = merge_summary(summary_data, positions)
                else:
                    positions = []
                    
                if nft_data:
                    session['message'] = f"✅ Show NFT data for wallet {wallet_address} on chain {chain_name}"
                else:
                    session['message'] = f"❌ No NFT data for wallet {wallet_address} on chain {chain_name}"
            except Exception as e:
                session['message'] = f"⚠️ Error: {str(e)}"
        else:
            # nft_data = fetch_nft_data(limit=per_page, offset=(page - 1) * per_page)
            # total_items = count_all_nft()
            # if total_items is None:
            #     total_items = 0
            # total_pages = (total_items + per_page - 1) // per_page
            nft_data = fetch_latest_nft_id(status='Burned')

    message = session.pop('message', None)
    has_closed = any(nft.get("status") == "Burned" for nft in nft_data)
    
    return render_template(
        'index.html',
        nft_data=nft_data,
        summary_data=summary_data,
        total_pending_cake=total_pending_cake,
        message=message,
        wallet_address=wallet_address,
        chain_name=chain_name,
        action=action,
        start_date=start_date_str,
        end_date=end_date_str,
        page=page,
        total_pages=total_pages,
        nft_id=nft_id,
        token=token,
        has_closed=has_closed
    )
    
@app.route('/add_blacklist', methods=['POST'])
def add_blacklist_route():
    wallet_address = request.form.get('wallet_address')
    chain = request.form.get('chain')
    nft_id = request.form.get('nft_id')

    result = toggle_blacklist(wallet_address, chain, nft_id)  # Đây là dict thuần
    session['message'] = result['message']
    return redirect(url_for('index'))

@app.route('/remove_blacklist', methods=['POST'])
def remove_blacklist_route():
    wallet_address = request.form.get('wallet_address')
    chain = request.form.get('chain')
    nft_id = request.form.get('nft_id')

    result = toggle_blacklist(wallet_address, chain, nft_id)  # Đây là dict thuần
    session['message'] = result['message']
    return redirect(url_for('nft_blacklist'))

@app.route('/list-bond')
def list_bond():
    list_bond = fetch_list_bond_data()
    return render_template('bonds/list_bond.html', bonds=list_bond, title='List Bonds Apebond')

@app.route('/api/toggle_stake_track', methods=['POST'])
def toggle_stake_track():
    data = request.get_json()
    chain = data.get('chain')
    pool_address = data.get('pool_address')

    if not chain or not pool_address:
        return jsonify({'success': False, 'message': 'Missing parameters'}), 400

    return toggle_stake_track_api(chain, pool_address)

@app.route('/add-bond', methods=['GET', 'POST'])
def add_bond():
    if request.method == 'POST':
        chain = request.form['chain']
        contract_address = request.form['contract_address']
        token_symbol = request.form['token_symbol']
        status = request.form['status']

        insert_bond_data(chain, contract_address, token_symbol, status)
        
        return redirect(url_for('list_bond'))
    return render_template('bonds/manage_bond.html', bond_data=None, title='Add Bond')

@app.route('/update_status/<contract_address>', methods=['POST'])
def update_status(contract_address):
    new_status = request.form['status']

    update_bond_status(contract_address, new_status)

    return redirect(url_for('list_bond'))

@app.route('/update_bond/<contract_address>', methods=['GET', 'POST'])
def update_bond(contract_address):
    if request.method == 'POST':
        chain = request.form.get('chain')
        new_contract_address = request.form.get('contract_address')
        token_symbol = request.form.get('token_symbol')
        status = request.form.get('status')

        update_bond_data(chain, new_contract_address, token_symbol, status, contract_address)
        return redirect(url_for('list_bond'))

    # GET: lấy bond hiện tại từ DB
    bond_data = fetch_bond_data_by_contract_address(contract_address)
    return render_template('bonds/manage_bond.html', bond_data=bond_data, title='Edit Bond')

@app.route('/delete_bond/<contract_address>', methods=['POST'])
def delete_bond(contract_address):
    delete_bond_contract(contract_address)
    
    return redirect(url_for('list_bond'))

@app.route('/update_bonds_from_api')
def update_bonds_from_api():
    fetch_and_update_bonds()
    return redirect(url_for('list_bond'))
    
@app.route('/nft_blacklist')
def nft_blacklist():
    nft_blacklist = fetch_blacklist_nft_ids()
    return render_template('nft_blacklist.html', nft_blacklist=nft_blacklist, title='NFT ID Blacklist')

def convert_timestamps(pool_list):
    for pool in pool_list:
        pool["open_time"] = datetime.fromtimestamp(pool["open_time"]).strftime("%Y-%m-%d")
        pool["end_time"] = datetime.fromtimestamp(pool["end_time"]).strftime("%Y-%m-%d")
    return pool_list

@app.route('/list_pool')
def list_pool():
    pools = fetch_all_pool_info()
    total_cake_per_day_chain = get_total_cake_per_day_each_chain()
    total_weekly_rewards_sol = get_total_weekly_rewards_sol()
    total_cake_per_day_sol = total_weekly_rewards_sol / 7
    pools_sol = fetch_all_pool_sol_info()
    pools_sol = convert_timestamps(pools_sol)

    explorers = {
        "ARB":"https://pancakeswap.finance/liquidity/pool/arb/",
        "BAS":"https://pancakeswap.finance/liquidity/pool/base/",
        "BNB":"https://pancakeswap.finance/liquidity/pool/bsc/",
        "ETH":"https://pancakeswap.finance/liquidity/pool/eth/",
        "LIN":"https://pancakeswap.finance/liquidity/pool/linea/",
        "POL":"https://pancakeswap.finance/liquidity/pool/polygon-zkevm/",
        "SOL":"https://solana.pancakeswap.finance/clmm/create-position/?pool_id="
    }
    
    return render_template(
        'pools/list_pool.html', pools=pools, pools_sol=pools_sol, total_cake_per_day_chain=total_cake_per_day_chain, 
        total_cake_per_day_sol=total_cake_per_day_sol, explorers=explorers, title='List Pool Farm'
    )

@app.route("/api/transactions", methods=['POST'])
def get_transactions():
  filters = request.get_json()
  wallet_address = filters.get("walletAddress")
  chains = filters.get("chains")
  date_from = filters.get("dateFrom")
  date_to = filters.get("dateTo")
  symbol = filters.get("symbol")
  contract_address = filters.get("contract")
  print(f"{wallet_address}, {chains}, {date_from}, {date_to}, {symbol}, {contract_address}")

  existing_wallet = get_existing_wallet(wallet_address)
  if existing_wallet:
    transaction_history = get_transaction(wallet_address, chains, date_from, date_to, symbol, contract_address)

  else:
    if chains[0] == "SOL":
      transaction_history = get_transaction_sol_with_filter(wallet_address, date_from, date_to, symbol, contract_address)
    else:
      transaction_history = get_transactions_with_filter(wallet_address, chains, date_from, date_to, symbol,
                                                     contract_address)
  print(transaction_history)
  return jsonify(transaction_history)

CACHE = {}
CACHE_TTL = 600  # 10 phút

def get_cache(amm, pool_id):
    key = f"{amm}:{pool_id}"
    if key in CACHE:
        data, ts = CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return data
        else:
            # hết hạn, xóa
            del CACHE[key]
    return None

def set_cache(amm, pool_id, data):
    key = f"{amm}:{pool_id}"
    CACHE[key] = (data, time.time())


@app.route("/price_range_pool_sol", methods=['GET', 'POST'])
def price_range_pool_sol():
    pool_id = None
    amm = "pancake"   # mặc định pancake
    pool_ranges = None

    if request.method == 'POST':
        pool_id = request.form.get('pool_id')
        amm = request.form.get('amm', 'pancake')
        
        if pool_id and amm:
            pool_ranges = get_cache(amm, pool_id)
            if not pool_ranges:
                if amm == "pancake":
                    session['message'] = "✅ Analyzing PancakeSwap pool..."
                    pool_ranges = analyze_pool_ticks(
                        HELIUS_CLIENT,
                        PANCAKE_PROGRAM_ID,
                        Pubkey.from_string(pool_id)
                    )
                elif amm == "raydium":
                    session['message'] = "✅ Analyzing Raydium pool..."
                    pool_ranges = analyze_pool_ticks(
                        HELIUS_CLIENT,
                        RAYDIUM_PROGRAM_ID,
                        Pubkey.from_string(pool_id)
                    )
                else:
                    session['message'] = "❌ Unsupported AMM selected."
                    pool_ranges = None

                set_cache(amm, pool_id, pool_ranges)

            session["last_pool_id"] = pool_id
            session["last_amm"] = amm
    else:
        pool_id = session.get("last_pool_id")
        amm = session.get("last_amm", "pancake")
        if pool_id and amm:
            pool_ranges = get_cache(amm, pool_id)

    message = session.pop('message', None)
    
    return render_template(
        "pool_ranges/price_range_pool_sol.html",
        pool_id=pool_id,
        amm=amm,
        pool_ranges=pool_ranges,
        message=message,
        title='Price Range Pool Sol'
    )

@app.route("/api/current-tick-update")
def api_current_tick_update():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT t1.pool_address, t1.current_price
        FROM wallet_nft_position t1
        INNER JOIN (
            SELECT nft_id, MAX(created_at) AS max_created_at
            FROM wallet_nft_position
            GROUP BY nft_id
        ) t2 ON t1.nft_id = t2.nft_id AND t1.created_at = t2.max_created_at
        LEFT JOIN nft_blacklist b
            ON t1.chain = b.chain
            AND t1.nft_id = b.nft_id
        WHERE t1.status != 'Burned'
            AND b.id IS NULL
        ORDER BY t1.created_at DESC
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return jsonify(results)

@app.route("/transactions", methods=['GET'])
def view_transactions():
  return render_template("transactions/transactions.html", title='Transactions History')

@app.route('/mint_position/<chain>/<pool_address>')
def mint_position_data(chain, pool_address):
    if chain == "SOL":
        mint_data = get_data_mint_sol(chain, CLIENT, pool_address)
        return render_template('pools_liquidity/mint_position_sol.html', mint_data=mint_data, chain=chain, pool_address=pool_address)
    
    mint_data = get_data_mint(chain, pool_address)
    return render_template('pools_liquidity/mint_position.html', mint_data=mint_data, chain=chain, pool_address=pool_address)

def safe_float(value: str):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip().replace("e ", "e").replace("E ", "E")
        try:
            return float(value)
        except ValueError:
            return None  # hoặc raise Exception tùy logic
    return None

def convert_token_amount(amount: float, decimals: int) -> int:
    """Convert readable token amount -> raw integer based on decimals"""
    if amount is None:
        return 0
    return int(amount * (10 ** decimals))

@app.route("/get_mint_params")
def get_mint_params_api_sol():
    chain = request.args.get("chain")
    pool_address = request.args.get("pool_address")
    min_price = safe_float(request.args.get("min_price", 100))
    max_price = safe_float(request.args.get("max_price", 150))
    amount0 = safe_float(request.args.get("amount0"))
    amount1 = safe_float(request.args.get("amount1"))
    payer_pubkey = request.args.get("payer")
    token0_decimals = int(request.args.get("token0_decimals"))
    token1_decimals = int(request.args.get("token1_decimals"))
    print(f"chain: {chain}, pool_address: {pool_address}, min_price: {min_price}, max_price: {max_price}, payer_pubkey: {payer_pubkey}, token0_decimals: {token0_decimals}, token1_decimals: {token1_decimals}")
    
    amount_0_max = convert_token_amount(amount0, token0_decimals)
    amount_1_max = convert_token_amount(amount1, token1_decimals)
    print(f"amount0: {amount0}, amount1: {amount1}")
    print(f"amount_0_max: {amount_0_max}, amount_1_max: {amount_1_max}")
    amount_1_max = math.ceil(amount_1_max)
    amount_1_max = int(amount_1_max * 1.0006)  # thêm buffer 0.03%
    print(f"Final amount_1_max with buffer: {amount_1_max}")
    
    if chain == "SOL":
        params = get_mint_params_sol(chain, CLIENT, PANCAKE_PROGRAM_ID, pool_address, min_price, max_price, payer_pubkey)
        results = build_mint_position_tx_sol_v8(
            CLIENT, 
            payer_pubkey,
            payer_pubkey,
            pool_address,    
            params,
            amount_0_max,
            amount_1_max,
            liquidity=0,
            with_metadata=True,
            base_flag=True,
        )
        rent_lamports = CLIENT.get_minimum_balance_for_rent_exemption(82).value
        balance = CLIENT.get_balance(Pubkey.from_string(payer_pubkey)).value
        rent_ata_token0 = CLIENT.get_minimum_balance_for_rent_exemption(170).value
        rent_ata_token1 = CLIENT.get_minimum_balance_for_rent_exemption(170).value
        
        print("Rent mint:", rent_lamports / 1e9, "SOL")
        print("Rent ATA token0:", rent_ata_token0 / 1e9, "SOL")
        print("Rent ATA token1:", rent_ata_token1 / 1e9, "SOL")
        print("User balance:", balance / 1e9, "SOL")
        
        print((results))
        print(f"params: {params}")
        
    return jsonify(results)

# ========= REFRESH NFT DATA =========
@app.route("/refresh", methods=["POST"])
def refresh_nft():
    data = request.json
    chain = data.get("chain")
    wallet_address = data.get("wallet_address")
    nft_id = data.get("nft_id")

    if not chain or not wallet_address or not nft_id:
        return jsonify({"error": "Missing parameter"}), 400
    if chain == "SOL":
        success, nft_data = process_nft(nft_id, chain, wallet_address)
    else:
        success, nft_data = process_nft_evm(nft_id, chain, wallet_address)
    
    if not success:
        return jsonify({"status": "error",
                        "chain": chain, 
                        "wallet": wallet_address, 
                        "nft_id": nft_id,
                        "data": None
                       }), 500

    return jsonify({
        "status": "done",
        "chain": chain,
        "wallet": wallet_address,
        "nft_id": nft_id,
        "data": nft_data
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
