from __future__ import annotations

from web3 import Web3

from ..abi import ERC20_ABI, MAX_UINT128, MAX_UINT256


ERC721_ENUMERABLE_ABI = [
    {
        "name": "balanceOf",
        "inputs": [{"name": "owner", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "tokenOfOwnerByIndex",
        "inputs": [{"name": "owner", "type": "address"}, {"name": "index", "type": "uint256"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "ownerOf",
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]


COLLECT_ABI = {
    "name": "collect",
    "inputs": [{
        "name": "params",
        "type": "tuple",
        "components": [
            {"name": "tokenId", "type": "uint256"},
            {"name": "recipient", "type": "address"},
            {"name": "amount0Max", "type": "uint128"},
            {"name": "amount1Max", "type": "uint128"},
        ],
    }],
    "outputs": [{"name": "amount0", "type": "uint256"}, {"name": "amount1", "type": "uint256"}],
    "stateMutability": "nonpayable",
    "type": "function",
}


INCREASE_LIQUIDITY_ABI = {
    "name": "increaseLiquidity",
    "inputs": [{
        "name": "params",
        "type": "tuple",
        "components": [
            {"name": "tokenId", "type": "uint256"},
            {"name": "amount0Desired", "type": "uint256"},
            {"name": "amount1Desired", "type": "uint256"},
            {"name": "amount0Min", "type": "uint256"},
            {"name": "amount1Min", "type": "uint256"},
            {"name": "deadline", "type": "uint256"},
        ],
    }],
    "outputs": [
        {"name": "liquidity", "type": "uint128"},
        {"name": "amount0", "type": "uint256"},
        {"name": "amount1", "type": "uint256"},
    ],
    "stateMutability": "payable",
    "type": "function",
}


POSITION_ABI = {
    "name": "positions",
    "inputs": [{"name": "tokenId", "type": "uint256"}],
    "outputs": [
        {"name": "nonce", "type": "uint96"},
        {"name": "operator", "type": "address"},
        {"name": "token0", "type": "address"},
        {"name": "token1", "type": "address"},
        {"name": "feeOrTickSpacing", "type": "uint24"},
        {"name": "tickLower", "type": "int24"},
        {"name": "tickUpper", "type": "int24"},
        {"name": "liquidity", "type": "uint128"},
        {"name": "feeGrowthInside0LastX128", "type": "uint256"},
        {"name": "feeGrowthInside1LastX128", "type": "uint256"},
        {"name": "tokensOwed0", "type": "uint128"},
        {"name": "tokensOwed1", "type": "uint128"},
    ],
    "stateMutability": "view",
    "type": "function",
}


COMPOUND_NPM_ABI = [*ERC721_ENUMERABLE_ABI, POSITION_ABI, COLLECT_ABI, INCREASE_LIQUIDITY_ABI]
COMPOUND_MASTERCHEF_ABI = [COLLECT_ABI, INCREASE_LIQUIDITY_ABI]

COLLECT_TOPIC = "0x" + Web3.keccak(text="Collect(uint256,address,uint256,uint256)").hex().removeprefix("0x")
INCREASE_LIQUIDITY_TOPIC = "0x" + Web3.keccak(
    text="IncreaseLiquidity(uint256,uint128,uint256,uint256)"
).hex().removeprefix("0x")

__all__ = [
    "COLLECT_TOPIC",
    "COMPOUND_MASTERCHEF_ABI",
    "COMPOUND_NPM_ABI",
    "ERC20_ABI",
    "INCREASE_LIQUIDITY_TOPIC",
    "MAX_UINT128",
    "MAX_UINT256",
]
