from web3 import Web3
from solders.pubkey import Pubkey
from solana.rpc.types import TokenAccountOpts
from solana.rpc.api import Client
import os
from dotenv import load_dotenv

load_dotenv()

CHAIN_ID_MAP = {
    "BNB": "56",
    "ETH": "1",
    "BAS": "8453",
    "ARB": "42161",
    "LIN": "59144",
    "POL": "137",
    "SON": "146",
    "BER": "80094",
    "UNI": "10143"
}

ID_CHAIN_MAP = {
    56: "BNB",
    1: "ETH",
    8453: "BAS",
    42161: "ARB",
    59144: "LIN",
    137: "POL",
    146: "SON",
    80094: "BER",
    10143: "UNI"
}

API_URLS = {
    'ETH': f'https://api.etherscan.io/v2/api?chainid=1',
    'BAS': f'https://api.etherscan.io/v2/api?chainid=8453',
    'POL': f'https://api.etherscan.io/v2/api?chainid=137',
    'BNB': f'https://api.etherscan.io/v2/api?chainid=56',
    'ARB': f'https://api.etherscan.io/v2/api?chainid=42161',
    'LIN': f'https://api.etherscan.io/v2/api?chainid=59144',
}

CHAIN_SCAN_URLS = {
    'ETH': 'https://etherscan.io/address/',
    'BAS': 'https://basescan.org/address/',
    'POL': 'https://polygonscan.com/address/',
    'BNB': 'https://bscscan.com/address/',      
    'ARB': 'https://arbiscan.io/address/',
    'LIN': 'https://lineascan.build/address/',
    'SON': 'https://sonicscan.com/address/',
}

CHAIN_SCAN_KEY1 = os.getenv("CHAIN_SCAN_KEY1")
CHAIN_SCAN_KEY2 = os.getenv("CHAIN_SCAN_KEY2")
CHAIN_SCAN_KEY3 = os.getenv("CHAIN_SCAN_KEY3")

API_KEYS = {
    'ETH': CHAIN_SCAN_KEY1,
    'BAS': CHAIN_SCAN_KEY2,
    'POL': CHAIN_SCAN_KEY2,
    'BNB': CHAIN_SCAN_KEY3,
    'ARB': CHAIN_SCAN_KEY2,
    'LIN': CHAIN_SCAN_KEY1,
}

# Config Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# TELEGRAM_CHAT_ID = "5696892272"

# API Key Infura
API_KEY_INFURA = os.getenv("API_KEY_INFURA")

# PancakeSwap NPM V3 Smart contract
NPM_ADDRESS = Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364")
NPM_ADDRESSES = {
    'ETH': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"),
    'BNB': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"),
    'BAS': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"),
    'ARB': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"),
    'LIN': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"),
    'POL': Web3.to_checksum_address("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364")
}

# PancakeSwap Factory V3 Smart contract
FACTORY_ADDRESS = Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865")
FACTORY_ADDRESSES = {
    'ETH': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'BNB': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'BAS': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'ARB': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'LIN': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'POL': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865")
}

# PancakeSwap Masterchef V3 Smart contract
MASTERCHEF_ADDRESS = Web3.to_checksum_address("0x556B9306565093C855AEA9AE92A594704c2Cd59e")
MASTERCHEF_ADDRESSES = {
    'ETH': Web3.to_checksum_address("0x556B9306565093C855AEA9AE92A594704c2Cd59e"),
    'BNB': Web3.to_checksum_address("0x556B9306565093C855AEA9AE92A594704c2Cd59e"),
    'BAS': Web3.to_checksum_address("0xC6A2Db661D5a5690172d8eB0a7DEA2d3008665A3"),
    'ARB': Web3.to_checksum_address("0x5e09ACf80C0296740eC5d6F643005a4ef8DaA694"),
    'LIN': Web3.to_checksum_address("0x22E2f236065B780FA33EC8C4E58b99ebc8B55c57"),
    'POL': Web3.to_checksum_address("0xe9c7f3196ab8c09f6616365e8873daeb207c0391")
}

RPC_URLS = {
    "BNB": "https://bsc-dataseed.binance.org",
    "ETH": f"https://mainnet.infura.io/v3/{API_KEY_INFURA}",
    "BAS": f"https://base-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "ARB": f"https://arbitrum-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "LIN": f"https://linea-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "POL": f"https://polygon-mainnet.infura.io/v3/{API_KEY_INFURA}"
}

ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY")
MORALIS_API_KEY = os.environ.get("MORALIS_API_KEY")

RPC_BACKUP_LIST = {
    "BNB": [
        f"https://bnb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
        f"https://site1.moralis-nodes.com/bsc/{MORALIS_API_KEY}"
    ],
    "BAS": [
        f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
        f"https://site1.moralis-nodes.com/base/{MORALIS_API_KEY}"
    ],
    "ETH": [
        f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    ],
    "ARB": [
        f"https://arb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    ],
    "LIN": [
        f"https://linea-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    ],
    "POL": [
        f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    ]
}

CHAIN_API_MAP = {
    "BNB": "bsc",
    "ETH": "ethereum",
    "BAS": "base",
    "ARB": "arbitrum",
    "LIN": "linea",
    "POL": "polygon"
}

CHAIN_NAME_PANCAKE = {
    "BNB": "bsc",
    "ETH": "eth",
    "BAS": "base",
    "ARB": "arb",
    "LIN": "linea",
    "POL": "polygon"
}

CHAIN_KEY_MORALIS_EVM = {
    "BNB": "bsc",
    "ETH": "eth",
    "BAS": "base",
    "ARB": "arbitrum",
    "LIN": "linea",
    "POL": "polygon"
}

CAKE_PER_SECOND_ON_CHAIN = {
    "BNB": 0.06644,
    "BAS": 0.07958,
    "ETH": 0.00572,
    "ARB": 0.02745,
}

DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1376423408262189056/MHkTODxyxl0YSry5sey06cD86O2Mww8imltXMV_pxrqdDs-1sAWzx05-wS7JAz8z8zwD"
# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1377961748925124681/4L4i0oxq6PD1jLlBUV2IxH-G2vobb-ESm2VhKWL30dQztF4sRVg8IkgOoWe4W2EB0IFS"

### SOLANA CONFIG ###
QUICKNODE_KEY = os.getenv("QUICKNODE_KEY")
WSS_QUICKNODE_KEY = os.getenv("WSS_QUICKNODE_KEY")
HELIUS_KEY = os.getenv("HELIUS_KEY")

TOKEN_ACCOUNT_OPTS = TokenAccountOpts(program_id=Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"))
SPL_TOKEN_PROGRAM = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
PANCAKE_PROGRAM_ID = Pubkey.from_string("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq")
RAYDIUM_PROGRAM_ID = Pubkey.from_string("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK")
METADATA_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
CLIENT = Client(f"https://dawn-blissful-pallet.solana-mainnet.quiknode.pro/{QUICKNODE_KEY}")
WSS_URL = f"wss://shy-spring-card.solana-mainnet.quiknode.pro/{WSS_QUICKNODE_KEY}"

HELIUS_CLIENT = Client(f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")

MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImQ3ZGUwNDc4LTVhODktNDc5OS05MjkwLTJkNDE3Mjc3Zjk4MyIsIm9yZ0lkIjoiNDcxMzUzIiwidXNlcklkIjoiNDg0ODg5IiwidHlwZUlkIjoiYzM3NjhjMmYtMTFhMy00Zjc0LTkxMGQtODkwN2Q4YmU3ZjFhIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NTgxODM5NDEsImV4cCI6NDkxMzk0Mzk0MX0.ASKuhTXcm4Pb4SwTtVK9-sV8tBNe0rAzFD8pz17s_TE"

# Solana RPC endpoints with Helius and Solana mainnet
RPC_SOL_ENDPOINTS = [
    "https://solana-mainnet.g.alchemy.com/v2/objZhkoyHIkTOpSJLSCkC",
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}",
    "https://api.mainnet-beta.solana.com"
]

CLIENTS_SOL_ENDPOINTS = [
    Client("https://solana-mainnet.g.alchemy.com/v2/objZhkoyHIkTOpSJLSCkC"),
    Client("https://mainnet.helius-rpc.com/?api-key=bb4fcdca-d41d-4930-ada1-6490968dabe4"),
    Client("https://api.mainnet-beta.solana.com")
]

WS_RPC_URLS = {
    "BNB": f"wss://bsc-mainnet.infura.io/ws/v3/{API_KEY_INFURA}",
    "ETH": f"wss://mainnet.infura.io/ws/v3/{API_KEY_INFURA}",
    "BAS": f"wss://base-mainnet.infura.io/ws/v3/{API_KEY_INFURA}",
    "ARB": f"wss://arbitrum-mainnet.infura.io/ws/v3/{API_KEY_INFURA}",
    "LIN": f"wss://linea-mainnet.infura.io/ws/v3/{API_KEY_INFURA}",
    "POL": f"wss://polygon-mainnet.infura.io/ws/v3/{API_KEY_INFURA}"
}

### TOKEN NATIVE ###
WRAPPED_TOKENS = {
    'BNB': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
    'ETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    'BAS': '0x4200000000000000000000000000000000000006',
    'POL': '0x0d500B1d8E8eF31E21C99d1DB9A6444d3ADf1270',
    'ARB': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
    'LIN': '0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f',
}