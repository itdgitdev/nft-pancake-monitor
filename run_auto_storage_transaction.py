from services.excute_transaction import get_lasted_block, get_lasted_signature
from services.transaction_history.save_sol_tx_his import fetch_all_transactions
from services.transaction_history.save_tx_his import get_new_transactions, CHAIN_ID
from services.excute_transaction import get_transaction, get_existing_wallet, test_get_transaction

WALLET_ADDRESS_EVM = [
  "0x88de2ab47352779494547caccb31ee1a133dd334",
  # "0x349F8F068120E04B359556E442A579Af41ebF486"
]
WALLET_ADDRESS_SOLANA = [
  "4rDyyA4vydw4T5uekxY5La4Ywv43nSZ2PgG7rfBfvQAJ",
  "CJoUCt78FNbJJcKW3CnmLG9CVq6ANSTiXWV1tyN5dXw9"
]

if __name__=="__main__":
  # Get transaction of list wallet in EVMS chain
  for wallet in WALLET_ADDRESS_EVM:
    for chain, id in CHAIN_ID.items():
      lasted_block = get_lasted_block(wallet, chain)
      print(f"Lasted block in database: {lasted_block}")
      get_new_transactions(wallet, chain, lasted_block)
    
  # # Get transactions of list wallet of solana chain
  # for wallet in WALLET_ADDRESS_SOLANA:
  #   lasted_signature = get_lasted_signature(wallet)
  #   print(f"lasted signature of wallet {wallet} is {lasted_signature}")
  #   fetch_all_transactions(wallet, lasted_signature)
  
  # transactions = get_transaction("0x88de2ab47352779494547caccb31ee1a133dd334",["BAS", "BSC"],"2025-10-10","2025-10-30","CAKE")
  # print(transactions)
  
  # data = test_get_transaction()
  # print(data)