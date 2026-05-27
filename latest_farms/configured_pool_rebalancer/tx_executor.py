from __future__ import annotations

import logging
import os
from itertools import count

from web3 import Web3
from web3.exceptions import TimeExhausted

from .evm import DEFAULT_GAS_POLICIES, get_chain_id, get_gas_params, validate_gas_cap
from .logging_utils import log_block, pool_context
from .models import GasPolicy, PoolConfig, TxResult, WorkerConfig

log = logging.getLogger("configured_pool_rebalancer")


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
        metadata = self._safe_tx_metadata(label, tx, gas_params)
        private_key = os.getenv(self.pool.private_key_env)
        if not private_key:
            raise RuntimeError(f"missing private key env {self.pool.private_key_env}")
        signed = self.w3.eth.account.sign_transaction(tx, private_key)
        signed_tx_hash = Web3.keccak(signed.raw_transaction).hex()
        if not signed_tx_hash.startswith("0x"):
            signed_tx_hash = "0x" + signed_tx_hash
        metadata["signed_tx_hash"] = signed_tx_hash
        log_block(
            log,
            logging.INFO,
            f"{label} broadcast",
            pool_context(self.pool),
            {
                "stage": "broadcast",
                "action": label,
                "to": metadata.get("to"),
                "value": metadata.get("value"),
                "nonce": metadata.get("nonce"),
                "gas_limit": metadata.get("gas_limit"),
                "max_fee_gwei": f"{metadata.get('max_fee_per_gas_gwei', 0.0):.9f}",
                "priority_fee_gwei": f"{metadata.get('max_priority_fee_per_gas_gwei', 0.0):.9f}",
                "data_length": metadata.get("data_length"),
                "signed_tx_hash": signed_tx_hash,
            },
        )
        try:
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        except Exception as exc:
            if label != "mint":
                raise
            log_block(
                log,
                logging.WARNING,
                f"{label} broadcast unknown",
                pool_context(self.pool),
                {
                    "stage": "broadcast",
                    "action": label,
                    "status": "BROADCAST_UNKNOWN",
                    "signed_tx_hash": signed_tx_hash,
                    "reason": exc,
                    "next_action": "journal recovery will try receipt lookup",
                },
            )
            return TxResult(
                tx_hash=signed_tx_hash,
                status="BROADCAST_UNKNOWN",
                metadata={**metadata, "error": str(exc), "broadcast_error": str(exc)},
            )
        tx_hash_hex = tx_hash.hex()
        if not tx_hash_hex.startswith("0x"):
            tx_hash_hex = "0x" + tx_hash_hex
        metadata["broadcast_tx_hash"] = tx_hash_hex
        log_block(
            log,
            logging.INFO,
            f"{label} broadcast accepted",
            pool_context(self.pool),
            {
                "stage": "broadcast_accepted",
                "action": label,
                "tx_hash": tx_hash_hex,
                "signed_tx_hash": signed_tx_hash,
                "nonce": metadata.get("nonce"),
            },
        )
        try:
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        except TimeExhausted as exc:
            if label != "mint":
                raise
            log_block(
                log,
                logging.WARNING,
                f"{label} receipt timeout",
                pool_context(self.pool),
                {
                    "stage": "receipt_wait",
                    "action": label,
                    "status": "PENDING",
                    "tx_hash": tx_hash_hex,
                    "signed_tx_hash": signed_tx_hash,
                    "reason": exc,
                    "next_action": "journal recovery will try receipt lookup",
                },
            )
            return TxResult(
                tx_hash=tx_hash_hex,
                status="PENDING",
                metadata={**metadata, "error": str(exc)},
            )
        if receipt["status"] != 1:
            raise RuntimeError(f"{label} reverted: {tx_hash_hex}")
        effective_gas_price = int(receipt.get("effectiveGasPrice") or gas_params["maxFeePerGas"])
        receipt_block = int(receipt.get("blockNumber") or 0)
        gas_used = int(receipt["gasUsed"])
        gas_price_gwei = float(Web3.from_wei(effective_gas_price, "gwei"))
        log_block(
            log,
            logging.INFO,
            f"{label} receipt",
            pool_context(self.pool),
            {
                "stage": "receipt",
                "action": label,
                "status": receipt.get("status"),
                "tx_hash": tx_hash_hex,
                "block": receipt_block,
                "gas_used": gas_used,
                "effective_gas_price_gwei": f"{gas_price_gwei:.9f}",
            },
        )
        return TxResult(
            tx_hash=tx_hash_hex,
            gas_used=gas_used,
            gas_price_gwei=gas_price_gwei,
            metadata={**metadata, "receipt_block": receipt_block},
        )

    def gas_policy(self) -> GasPolicy:
        chain = self.pool.chain.upper()
        if self.worker_config and chain in self.worker_config.gas_policies:
            return self.worker_config.gas_policies[chain]
        return DEFAULT_GAS_POLICIES.get(chain) or GasPolicy()

    def _safe_tx_metadata(self, label: str, tx: dict, gas_params: dict) -> dict:
        return {
            "label": label,
            "chain_id": tx.get("chainId"),
            "nonce": tx.get("nonce"),
            "gas_limit": tx.get("gas"),
            "from": tx.get("from"),
            "to": tx.get("to"),
            "value": str(tx.get("value") or 0),
            "data_length": len(str(tx.get("data") or "")),
            "max_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei")),
            "max_priority_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxPriorityFeePerGas"], "gwei")),
        }
