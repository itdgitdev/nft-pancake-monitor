from __future__ import annotations

import time

from web3 import Web3

from .models import GasPolicy


try:
    from latest_farms.config import CHAIN_ID_MAP, RPC_BACKUP_LIST, RPC_URLS_2
except ImportError:  # pragma: no cover - allows running from latest_farms cwd
    from config import CHAIN_ID_MAP, RPC_BACKUP_LIST, RPC_URLS_2


def web3_connection(chain: str, timeout: int = 30) -> Web3:
    urls = [RPC_URLS_2.get(chain)] + RPC_BACKUP_LIST.get(chain, [])
    for url in [item for item in urls if item]:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": timeout}))
            if chain == "BNB":
                try:
                    from web3.middleware import ExtraDataToPOAMiddleware

                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                except ImportError:
                    from web3.middleware import geth_poa_middleware

                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            if w3.is_connected():
                return w3
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"No working RPC for chain={chain}")


def get_chain_id(chain: str) -> int:
    return int(CHAIN_ID_MAP.get(chain, 56))


DEFAULT_GAS_POLICIES = {
    "BNB": GasPolicy(mode="fixed", gas_price_gwei=0.05, max_fee_gwei=0.08),
    "BAS": GasPolicy(
        mode="eip1559",
        base_fee_multiplier=2.0,
        priority_fee_cap_gwei=0.01,
        swap_priority_fee_cap_gwei=0.02,
        swap_priority_fee_floor_gwei=0.005,
        max_fee_gwei=0.10,
    ),
}


def get_gas_params(
    w3: Web3,
    chain: str,
    action: str = "default",
    policy: GasPolicy | None = None,
) -> dict:
    chain_key = chain.upper()
    gas_policy = policy or DEFAULT_GAS_POLICIES.get(chain_key) or GasPolicy()
    mode = gas_policy.mode.lower()
    if mode == "fixed":
        gas_price_gwei = gas_policy.gas_price_gwei
        if gas_price_gwei is None:
            gas_price_gwei = 0.05 if chain_key == "BNB" else float(Web3.from_wei(w3.eth.gas_price, "gwei"))
        gas_price = Web3.to_wei(gas_price_gwei, "gwei")
        return {"maxFeePerGas": gas_price, "maxPriorityFeePerGas": gas_price}

    priority_tip = _priority_fee(w3, gas_policy, action)
    try:
        fee_history = w3.eth.fee_history(1, "latest", [50])
        base_fee = fee_history["baseFeePerGas"][-1]
    except Exception:
        base_fee = w3.eth.gas_price
    max_fee = int(base_fee * gas_policy.base_fee_multiplier + priority_tip)
    if max_fee < priority_tip:
        max_fee = priority_tip + int(base_fee)
    return {"maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority_tip}


def validate_gas_cap(gas_params: dict, max_fee_gwei: float | None) -> float:
    max_fee = float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei"))
    if max_fee_gwei is not None and max_fee > max_fee_gwei:
        raise RuntimeError(f"gas too high: {max_fee:.6f} gwei > cap {max_fee_gwei:.6f} gwei")
    return max_fee


def _priority_fee(w3: Web3, policy: GasPolicy, action: str) -> int:
    cap_gwei = policy.swap_priority_fee_cap_gwei if action == "swap" else policy.priority_fee_cap_gwei
    suggested = None
    try:
        suggested = int(w3.eth.max_priority_fee)
    except Exception:
        pass
    if suggested is None:
        try:
            fee_history = w3.eth.fee_history(3, "latest", [50])
            rewards = [int(row[0]) for row in fee_history.get("reward", []) if row]
            if rewards:
                suggested = max(rewards)
        except Exception:
            pass
    if suggested is None:
        suggested = Web3.to_wei(0.05, "gwei")
    if action == "swap" and policy.swap_priority_fee_floor_gwei is not None:
        suggested = max(suggested, Web3.to_wei(policy.swap_priority_fee_floor_gwei, "gwei"))
    if cap_gwei is not None:
        suggested = min(suggested, Web3.to_wei(cap_gwei, "gwei"))
    return max(0, int(suggested))
