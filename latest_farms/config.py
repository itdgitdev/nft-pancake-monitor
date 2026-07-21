import os
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

MASTERCHEF_V3_ADDRESSES = {
    "ETH": "0x556B9306565093C855AEA9AE92A594704c2Cd59e",
    "BNB": "0x556B9306565093C855AEA9AE92A594704c2Cd59e",
    "BAS": "0xC6A2Db661D5a5690172d8eB0a7DEA2d3008665A3",
    "LIN": "0x22E2f236065B780FA33EC8C4E58b99ebc8B55c57",
    "ARB": "0x5e09ACf80C0296740eC5d6F643005a4ef8DaA694",
    "MON": "0x5e09ACf80C0296740eC5d6F643005a4ef8DaA694",
    "POL": "0xe9c7f3196ab8c09f6616365e8873daeb207c0391",
    # "ERA": "0x4c615E78c5fCA1Ad31e4d66eb0D8688d84307463"
}

NPM_ADDRESSES = {
    "ETH": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "BNB": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "BAS": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "ARB": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "LIN": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "MON": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    "POL": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"
}

AERODROME_FACTORY_NPM_ADDRESSES = {
    "BAS": {
        Web3.to_checksum_address("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"):
            Web3.to_checksum_address("0x827922686190790b37229fd06084350E74485b72"),
        Web3.to_checksum_address("0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a"):
            Web3.to_checksum_address("0xa990C6a764b73BF43cee5Bb40339c3322FB9D55F"),
        Web3.to_checksum_address("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef"):
            Web3.to_checksum_address("0xe1f8cd9AC4e4A65F54f38a5CdAfCA44f6dD68b53"),
    }
}

AERODROME_NPM_ADDRESSES = {
    chain: list(factory_npm_map.values())
    for chain, factory_npm_map in AERODROME_FACTORY_NPM_ADDRESSES.items()
}

AERODROME_FACTORY_ADDRESSES = {
    chain: next(iter(factory_npm_map))
    for chain, factory_npm_map in AERODROME_FACTORY_NPM_ADDRESSES.items()
}

FACTORY_ADDRESSES = {
    'ETH': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'BNB': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'BAS': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'ARB': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'LIN': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'MON': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"),
    'POL': Web3.to_checksum_address("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865")
}

FACTORY_DEPLOYED_BLOCK = {
    'ETH': 16950686,
    'BNB': 26956207,
    'BAS': 2912007,
    'ARB': 101028949,
    'LIN': 1444,
    'MON': 37024264,
    'POL': 26900000
}

MASTERCHEF_DEPLOYED_BLOCK = {
    'ETH': 16945103,
    'BNB': 26933904,
    'BAS': 2948222,
    'ARB': 105053701,
    'LIN': 15395,
    'MON': 37024264,
    'POL': 26900000
}

API_URLS = {
    'ETH': f'https://api.etherscan.io/v2/api?chainid=1',
    'BAS': f'https://api.etherscan.io/v2/api?chainid=8453',
    'POL': f'https://api.etherscan.io/v2/api?chainid=137',
    'BNB': f'https://api.etherscan.io/v2/api?chainid=56',
    'ARB': f'https://api.etherscan.io/v2/api?chainid=42161',
    'MON': f'https://api.etherscan.io/v2/api?chainid=143',
    'LIN': f'https://api.etherscan.io/v2/api?chainid=59144',
}

CHAIN_SCAN_URLS = {
    'ETH': 'https://etherscan.io/address/',
    'BAS': 'https://basescan.org/address/',
    'POL': 'https://zkevm.polygonscan.com/address/',
    'ERA': 'https://explorer.zksync.io/address/',
    'BNB': 'https://bscscan.com/address/',      
    'ARB': 'https://arbiscan.io/address/',
    'LIN': 'https://lineascan.build/address/',
    'MON': 'https://monadvision.com/address/',
}

API_KEYS = {
    'ETH': '82F74VAYNQUN42RVXM37JXUX2F24JPJ8FS',
    'BAS': 'W961R5X6KISNKFMJ2QWVA7999S2IQDNF6U',
    'POL': '82F74VAYNQUN42RVXM37JXUX2F24JPJ8FS',
    'BNB': 'UV4PHPTKIZJ3Z1MK9M5TA74Y1RNN29DJ6F',
    'ARB': '82F74VAYNQUN42RVXM37JXUX2F24JPJ8FS',
    'LIN': '82F74VAYNQUN42RVXM37JXUX2F24JPJ8FS',
    'MON': '82F74VAYNQUN42RVXM37JXUX2F24JPJ8FS',
}

# API KEY RPC Infura
API_KEY_INFURA = os.getenv("API_KEY_INFURA")
RPC_URLS = {
    "BNB": f"https://bsc-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "ETH": f"https://mainnet.infura.io/v3/{API_KEY_INFURA}",
    "BAS": f"https://base-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "ARB": f"https://arbitrum-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "LIN": f"https://linea-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "POL": f"https://zkevm-rpc.com",
    "ERA": f"https://zksync-mainnet.infura.io/v3/{API_KEY_INFURA}",
    "MON": f"https://monad-mainnet.infura.io/v3/{API_KEY_INFURA}",
}

API_KEY_INFURA_1 = os.getenv("API_KEY_INFURA_1")
API_KEY_INFURA_2 = os.getenv("API_KEY_INFURA_2")
API_KEY_INFURA_3 = os.getenv("API_KEY_INFURA_3")
RPC_URLS_2 = {
    "BNB": f"https://bsc-mainnet.infura.io/v3/{API_KEY_INFURA_1}",
    "ETH": f"https://mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "BAS": f"https://base-mainnet.infura.io/v3/{API_KEY_INFURA_3}",
    "ARB": f"https://arbitrum-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "LIN": f"https://linea-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "POL": f"https://zkevm-rpc.com",
    "ERA": f"https://zksync-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "MON": f"https://monad-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
}

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_NFT_API_KEY = os.getenv("ALCHEMY_NFT_API_KEY")
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY")

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

SWAPPER_0X_API_KEY = os.getenv("SWAPPER_0X_API_KEY")
SWAPPER_OKX_API_KEY = os.getenv("SWAPPER_OKX_API_KEY")
SWAPPER_OKX_SECRET_KEY = os.getenv("SWAPPER_OKX_SECRET_KEY")
SWAPPER_OKX_PASSPHRASE = os.getenv("SWAPPER_OKX_PASSPHRASE")
SWAPPER_KYBER_CLIENT_ID = os.getenv("SWAPPER_KYBER_CLIENT_ID")

CHAIN_ID_MAP = {
    "BNB": "56",
    "ETH": "1",
    "BAS": "8453",
    "ARB": "42161",
    "LIN": "59144",
    "POL": "1101",
    "ERA": "324",
    "SOL": "7565164",
    "MON": "143"
}

PANCAKE_CHAIN_MAP = {
    "BNB": "bsc",
    "ETH": "eth",
    "BAS": "base",
    "ARB": "arb",
    "LIN": "linea",
    "POL": "polygonZkEVM",
    "ERA": "zkSync",
    "MON": "monad"
}

BLACKLIST_POOLS = {
    "BNB": [],
    "ETH": [],
    "BAS": [],
    "ARB": [],
    "LIN": [],
    "POL": [],
    "ERA": [],
    "MON": []
}