from __future__ import annotations

import requests


CAKE_TOKEN_MAP = {
    "BNB": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
    "ETH": "0x152649eA73beAb28c5b49B26eb48f7EAD6d4c898",
    "BAS": "0x3055913c90Fcc1A6CE9a358911721eEb942013A1",
    "ARB": "0x1b896893dfc86bb67Cf57767298b9073D2c1bA2c",
}

PRICE_CHAIN_IDS = {
    "BNB": "56",
    "ETH": "1",
    "BAS": "8453",
    "ARB": "42161",
    "LIN": "59144",
    "POL": "137",
}

COINGECKO_PLATFORMS = {
    "BNB": "binance-smart-chain",
    "ETH": "ethereum",
    "BAS": "base",
    "ARB": "arbitrum-one",
    "LIN": "linea",
    "POL": "polygon-pos",
}


def pancake_reward_token(chain: str) -> str | None:
    return CAKE_TOKEN_MAP.get(chain.upper())


def token_price_usd(chain: str, token_address: str, warnings: list[str] | None = None) -> float | None:
    chain_key = chain.upper()
    return (
        _price_from_pancake(chain_key, token_address, warnings)
        or _price_from_dexscreener(token_address, warnings)
        or _price_from_coingecko(chain_key, token_address, warnings)
    )


def _price_from_pancake(chain: str, token_address: str, warnings: list[str] | None) -> float | None:
    chain_id = PRICE_CHAIN_IDS.get(chain)
    if not chain_id:
        return None
    url = f"https://wallet-api.pancakeswap.com/v1/prices/list/{chain_id}%3A{token_address}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        response.raise_for_status()
        data = response.json()
        price = data.get(f"{chain_id}:{token_address}") or data.get(f"{chain_id}:{token_address.lower()}")
        return float(price) if price else None
    except Exception as exc:
        if warnings is not None:
            warnings.append(f"pancake token price lookup failed for {chain}:{token_address}: {exc}")
        return None


def _price_from_dexscreener(token_address: str, warnings: list[str] | None) -> float | None:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        response.raise_for_status()
        pairs = response.json().get("pairs") or []
        if not pairs:
            return None
        best_pair = max(pairs, key=lambda item: item.get("liquidity", {}).get("usd", 0) or 0)
        price = best_pair.get("priceUsd")
        return float(price) if price else None
    except Exception as exc:
        if warnings is not None:
            warnings.append(f"dexscreener token price lookup failed for {token_address}: {exc}")
        return None


def _price_from_coingecko(chain: str, token_address: str, warnings: list[str] | None) -> float | None:
    platform = COINGECKO_PLATFORMS.get(chain)
    if not platform:
        return None
    url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
    try:
        response = requests.get(
            url,
            params={"contract_addresses": token_address, "vs_currencies": "usd"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        price = data.get(token_address.lower(), {}).get("usd")
        return float(price) if price else None
    except Exception as exc:
        if warnings is not None:
            warnings.append(f"coingecko token price lookup failed for {chain}:{token_address}: {exc}")
        return None
