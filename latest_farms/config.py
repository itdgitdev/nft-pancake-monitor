from web3 import Web3

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
API_KEY_INFURA = "afb06acf1c3542aca75c89203c9f9a28"
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

API_KEY_INFURA_1 = "92cf6964acae46008404ef57df3020b7"
API_KEY_INFURA_2 = "afb06acf1c3542aca75c89203c9f9a28"
API_KEY_INFURA_3 = "b0cdf677b6fc411297e25fd395fec257"
RPC_URLS_2 = {
    "BNB": f"https://bsc-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "ETH": f"https://mainnet.infura.io/v3/{API_KEY_INFURA_3}",
    "BAS": f"https://base-mainnet.infura.io/v3/{API_KEY_INFURA_3}",
    "ARB": f"https://arbitrum-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "LIN": f"https://linea-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "POL": f"https://zkevm-rpc.com",
    "ERA": f"https://zksync-mainnet.infura.io/v3/{API_KEY_INFURA_2}",
    "MON": f"https://monad-mainnet.infura.io/v3/{API_KEY_INFURA_3}",
}

ALCHEMY_API_KEY = "xA7-sWnseDzu0v8MsC6J9GpilYRgMtqW"
MORALIS_API_KEY = "7fe3328c4535474d9ac5952534d50fcb"

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

SWAPPER_0X_API_KEY = "dfc27316-a8fe-4a4b-aa74-b8f2e9c49559"
SWAPPER_OKX_API_KEY = "1ef5d201-1cec-46db-9658-c58a67008797"
SWAPPER_OKX_SECRET_KEY = "E05BA24E99B675FC9E9B9F7EE32CD232"
SWAPPER_OKX_PASSPHRASE = "@Shin12398"
SWAPPER_KYBER_CLIENT_ID = "NftApp"

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

# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1377961748925124681/4L4i0oxq6PD1jLlBUV2IxH-G2vobb-ESm2VhKWL30dQztF4sRVg8IkgOoWe4W2EB0IFS"
# DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1386555618751549520/i6GTfThX2VckPF4isp9ktn7ds1B0Ik7YWGGPR016nCO79uPIqm4ukYXPK-PR21_YvYyT"
