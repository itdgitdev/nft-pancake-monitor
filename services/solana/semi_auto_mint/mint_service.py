import os
import sys

# Giả lập đường dẫn import (giữ nguyên cấu trúc dự án của bạn)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from solders.pubkey import Pubkey
from solana.rpc.api import Client
from services.solana.semi_auto_mint.reward_estimator import RewardEstimator
from services.solana.semi_auto_mint.scan_pool import analyze_pool_ticks, get_position_owner
from services.solana.semi_auto_mint.swapper import JupiterSwapper
from services.solana.semi_auto_mint.executor import PositionExecutor # Import mới

class MintingService:
    def __init__(self, rpc_url, jupiter_api_key):
        self.rpc_client = Client(rpc_url)
        self.jupiter_api_key = jupiter_api_key
        self.swapper = JupiterSwapper(self.rpc_client, api_key=jupiter_api_key)
        self.executor = PositionExecutor(self.rpc_client) # Init Executor

    def get_pool_and_best_position(self, pool_address_str, program_id_str="HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq"):
        """
        Bước 1 (Nặng): Quét Pool và tìm Position tốt nhất.
        """
        pool_pubkey = Pubkey.from_string(pool_address_str)
        program_id = Pubkey.from_string(program_id_str)

        # Gọi Module 1 (Scanner)
        print(f"--- Scanning Pool {pool_address_str} ---")
        pool_data = analyze_pool_ticks(self.rpc_client, program_id, pool_pubkey)
        
        # Trích xuất thông tin cần thiết
        mock_pool_info = pool_data.get("pool_info", {})
        personal_positions = pool_data.get("personal_price_ranges", [])
        
        # Tìm Best Position
        estimator = RewardEstimator(pool_info=mock_pool_info)
        best_position = estimator.find_best_position_to_copy(personal_positions, strategy='max_liquidity')
        
        if not best_position:
            raise Exception("No active position found in this pool to copy.")
        
        nft_mint = best_position.get('nft_mint') # Đảm bảo scanner trả về field này
        best_position_owner = None
        
        if nft_mint:
            print(f"🔍 Fetching owner for Position NFT: {nft_mint}")
            best_position_owner = get_position_owner(self.rpc_client, nft_mint)
            best_position['owner_address'] = best_position_owner
            print(f"👤 Position Owner: {best_position_owner}")
            
        if "pool_id" not in mock_pool_info:
            mock_pool_info["pool_id"] = pool_address_str

        # Trả về dữ liệu thô để Client/Backend lưu lại
        return {
            "pool_info": mock_pool_info,
            "best_position": best_position,
            "token_metadata": {
                "token0": mock_pool_info.get("token_mint_0"),
                "token1": mock_pool_info.get("token_mint_1"),
                "decimals0": mock_pool_info.get("mint_decimals_0"),
                "decimals1": mock_pool_info.get("mint_decimals_1"),
                "symbol0": pool_data.get("token0_symbol"),
                "symbol1": pool_data.get("token1_symbol")
            }
        }

    def calculate_mint_plan(self, user_wallet_str, multiplier, pool_context_data, slippage_bps=50):
        """
        Bước 2 (Nhẹ): Tính toán Plan và Tạo Transaction Mint + Swap.
        """
        pool_info = pool_context_data['pool_info']
        best_position = pool_context_data['best_position']
        metadata = pool_context_data['token_metadata']
        
        position_owner = best_position.get('owner_address', None)
        is_own_position = False
        warning_msg = None
        
        if position_owner and user_wallet_str.lower() == position_owner.lower():
            is_own_position = True
            warning_msg = "You are attempting to copy your own position."
            print(f"⚠️ Warning: User is copying their own position: {position_owner}, ({user_wallet_str})")
        
        # --- MODULE 2: ESTIMATOR ---
        estimator = RewardEstimator(pool_info=pool_info)
        estimate_result = estimator.estimate_by_multiplier(best_position, multiplier)
        required_assets = estimate_result['required_assets']
        
        # --- MODULE 3: SWAPPER ---
        req_for_swap = {
            'token0_amount': required_assets['token0_amount'],
            'token1_amount': required_assets['token1_amount']
        }
        
        mints_map = {
            'token0': metadata['token0'],
            'token1': metadata['token1'],
            'decimals0': metadata['decimals0'],
            'decimals1': metadata['decimals1'],
            'symbol0': metadata['symbol0'],
            'symbol1': metadata['symbol1']
        }

        swap_transactions, price_impact_percent = self.swapper.calculate_and_prepare_swaps(
            user_pubkey_str=user_wallet_str,
            required_assets=req_for_swap,
            current_balances=None, 
            mints_map=mints_map,
            slippage_bps=slippage_bps
        )
        
        print(f"DEBUG: Price Impact: {price_impact_percent}%")
        print(f"DEBUG: Swap Transactions: {swap_transactions}")
        
        # Check nếu có lỗi funds thì return sớm
        for tx in swap_transactions:
            if tx.get('type') == 'ERROR':
                return {
                    "summary": {
                        "error": tx['description'],
                        "estimated_reward_share": estimate_result['reward_share_percent'],
                        "liquidity_minted": estimate_result['estimated_liquidity'],
                        "is_range_active": estimate_result['range_info']['is_active'],
                        "range_safety": estimate_result['range_info'].get('safety', {}),
                        "self_copy_warning": {
                            "is_own": is_own_position,
                            "message": warning_msg
                        }
                    },
                    "requirements": {
                        "token0": {
                            "symbol": metadata['symbol0'],
                            "amount": required_assets['token0_amount'],
                            "mint": metadata['token0']
                        },
                        "token1": {
                            "symbol": metadata['symbol1'],
                            "amount": required_assets['token1_amount'],
                            "mint": metadata['token1']
                        }
                    },
                    "actions": {
                        "swaps": swap_transactions, 
                        "can_mint": False,
                        "price_impact": price_impact_percent
                    }
                }

        # --- MODULE 4: EXECUTOR (TẠO LỆNH MINT) ---
        # Tính toán Amount Max với Slippage Buffer (1% mặc định)
        SLIPPAGE_BUFFER = 1.95
        amount0_max_raw = int(required_assets['token0_amount'] * (10**metadata['decimals0']) * SLIPPAGE_BUFFER)
        amount1_max_raw = int(required_assets['token1_amount'] * (10**metadata['decimals1']) * SLIPPAGE_BUFFER)
        print(f"Amount0 Max (raw): {amount0_max_raw}, Amount1 Max (raw): {amount1_max_raw}")
        
        mint_tx_result = None
        try:
            # Lấy tick bitmap extension nếu có (Logic nâng cao, tạm thời để None)
            bitmap_ext = None
            
            mint_tx_result = self.executor.build_mint_transaction(
                payer_address=user_wallet_str,
                pool_address=str(pool_info.get("pool_id") or "4FSrFjSMePHfRZiNaT9WxrRV8wqLcNnevjruG4zJWbpQ"), # Fallback hoặc lấy từ input
                token0_mint=metadata['token0'],
                token1_mint=metadata['token1'],
                token_vault_0=pool_info.get('token_vault_0', ''),
                token_vault_1=pool_info.get('token_vault_1', ''),
                tick_lower_index=best_position['tick_low'],
                tick_upper_index=best_position['tick_up'],
                tick_spacing=pool_info['tick_spacing'],
                liquidity=int(estimate_result['estimated_liquidity']),
                amount0_max=amount0_max_raw,
                amount1_max=amount1_max_raw,
                bitmap_extension_address=bitmap_ext
            )
        except Exception as e:
            print(f"Error building mint tx: {e}")
            mint_tx_result = {"error": str(e)}

        # --- TỔNG HỢP KẾT QUẢ TRẢ VỀ CHO UI ---
        return {
            "summary": {
                "multiplier": multiplier,
                "estimated_reward_share": estimate_result['reward_share_percent'],
                "liquidity_minted": estimate_result['estimated_liquidity'],
                "is_range_active": estimate_result['range_info']['is_active'],
                "range_safety": estimate_result['range_info'].get('safety', {}),
                "self_copy_warning": {
                    "is_own": is_own_position,
                    "message": warning_msg
                }
            },
            "requirements": {
                "token0": {
                    "symbol": metadata['symbol0'],
                    "amount": required_assets['token0_amount'],
                    "mint": metadata['token0']
                },
                "token1": {
                    "symbol": metadata['symbol1'],
                    "amount": required_assets['token1_amount'],
                    "mint": metadata['token1']
                }
            },
            "actions": {
                "swaps": swap_transactions,
                "price_impact": price_impact_percent,
                "mint_tx": mint_tx_result, # Chứa tx_base64 đã ký một phần
                "can_mint": mint_tx_result and "tx_base64" in mint_tx_result
            }
        }

    def execute_ultra_swap(self, signed_tx: str, request_id: str):
        """
        Gói lại hàm execute từ Swapper để gọi từ Controller (app.py).
        """
        return self.swapper.get_execute_ultra_tx(signed_tx, request_id)

# --- TEST ---
# if __name__ == "__main__":
#     RPC_URL = "https://shy-spring-card.solana-mainnet.quiknode.pro/6a97979ed162924bd71e878f5517215efab54766"
#     API_KEY = "87eef807-0114-49ba-a50c-7ec86337a08d"
#     USER_WALLET = "HJncdQqZwAjD5sCTP2dxqqxzSF1XQrFdXwPYJgAj1dma"
#     POOL_ADDRESS = "GuaLthm8FCmMymqL1UHaeFmSaszC8jsqguhoXkNLM8Sd"

#     service = MintingService(RPC_URL, API_KEY)
    
#     print("\n⏳ Đang tải dữ liệu Pool (Bước 1)...")
#     context_data = service.get_pool_and_best_position(POOL_ADDRESS) 
#     if context_data:
#         print(f"✅ Lấy dữ liệu Pool và Position thành công!")
#         print("\n🎚️ User chọn Multiplier: x1.0")
#         plan = service.calculate_mint_plan(USER_WALLET, 0.2, context_data)
#         print(f"Plan Summary: {plan}")
        
#         print(f"Mint TX Ready: {plan['actions']['can_mint']}")
#         if plan['actions']['can_mint']:
#             print(f"TX Base64: {plan['actions']['mint_tx']['tx_base64'][:50]}...")
#     else:
#         print("❌ Không thể lấy dữ liệu Pool hoặc Position.")
        
    # Ở đây ta Mock context data để test flow Module 4
    # context_data = {
    #     'pool_info': {
    #         'pool_id': POOL_ADDRESS,
    #         'token_mint_0': 'So11111111111111111111111111111111111111112',
    #         'token_mint_1': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    #         'mint_decimals_0': 9, 'mint_decimals_1': 6,
    #         'tick_spacing': 60, 'liquidity': 1000000000,
    #         'tick_current': -22000, 'sqrt_price_x64': 55432607702857685461,
    #         'token_vault_0': '2L4T...', # Mock
    #         'token_vault_1': '5T6Y...'  # Mock
    #     },
    #     'best_position': {'tick_low': -22500, 'tick_up': -21500, 'liquidity': 100000000},
    #     'token_metadata': {
    #         'token0': 'So11111111111111111111111111111111111111112',
    #         'token1': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    #         'decimals0': 9, 'decimals1': 6,
    #         'symbol0': 'SOL', 'symbol1': 'USDC'
    #     }
    # }