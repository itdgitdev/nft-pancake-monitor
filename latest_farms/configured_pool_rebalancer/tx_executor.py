from __future__ import annotations

import os
from itertools import count

from web3 import Web3

from .evm import DEFAULT_GAS_POLICIES, get_chain_id, get_gas_params, validate_gas_cap
from .models import GasPolicy, PoolConfig, TxResult, WorkerConfig


class TxExecutor:
    def __init__(self, w3: Web3, pool: PoolConfig, dry_run: bool, worker_config: WorkerConfig | None = None):
        self.w3 = w3
        self.pool = pool
        self.dry_run = dry_run
        self.worker_config = worker_config
        self._nonce_counter = None

    def _next_nonce(self) -> int:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        if self._nonce_counter is None:
            start = self.w3.eth.get_transaction_count(wallet, "pending")
            self._nonce_counter = count(start)
        return next(self._nonce_counter)

    def send(self, call_fn, label: str, gas: int | None = None, value: int = 0) -> TxResult:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        if self.dry_run:
            return TxResult(tx_hash=f"dry-run:{label}", dry_run=True, metadata={"label": label})

        gas_policy = self.gas_policy()
        gas_params = get_gas_params(self.w3, self.pool.chain, action=label, policy=gas_policy)
        cap = gas_policy.max_fee_gwei if gas_policy.max_fee_gwei is not None else self.pool.max_gas_gwei
        validate_gas_cap(gas_params, cap)

        if gas is None:
            try:
                estimated = call_fn.estimate_gas({"from": wallet, "value": value})
                gas = max(120000, int(estimated * 1.3))
            except Exception:
                gas = 900000

        tx = call_fn.build_transaction(
            {
                "from": wallet,
                "nonce": self._next_nonce(),
                "gas": gas,
                "value": value,
                **gas_params,
                "chainId": get_chain_id(self.pool.chain),
            }
        )
        private_key = os.getenv(self.pool.private_key_env)
        if not private_key:
            raise RuntimeError(f"missing private key env {self.pool.private_key_env}")
        signed = self.w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        if receipt["status"] != 1:
            raise RuntimeError(f"{label} reverted: {tx_hash.hex()}")
        effective_gas_price = int(receipt.get("effectiveGasPrice") or gas_params["maxFeePerGas"])
        return TxResult(
            tx_hash=tx_hash.hex(),
            gas_used=int(receipt["gasUsed"]),
            gas_price_gwei=float(Web3.from_wei(effective_gas_price, "gwei")),
            metadata={"label": label},
        )

    def gas_policy(self) -> GasPolicy:
        chain = self.pool.chain.upper()
        if self.worker_config and chain in self.worker_config.gas_policies:
            return self.worker_config.gas_policies[chain]
        return DEFAULT_GAS_POLICIES.get(chain) or GasPolicy()
