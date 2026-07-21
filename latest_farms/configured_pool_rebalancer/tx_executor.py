from __future__ import annotations

import logging
from itertools import count
from urllib.parse import urlparse

from web3 import Web3
from web3.exceptions import TimeExhausted, TransactionNotFound

from .evm import DEFAULT_GAS_POLICIES, get_chain_id, get_gas_params, validate_gas_cap
from .logging_utils import log_block, pool_context
from .models import GasPolicy, PoolConfig, TxResult, WorkerConfig
from .signer import RuntimeSigner

log = logging.getLogger("configured_pool_rebalancer")


class TxExecutor:
    def __init__(
        self,
        w3: Web3,
        pool: PoolConfig,
        dry_run: bool,
        worker_config: WorkerConfig | None = None,
        signer: RuntimeSigner | None = None,
    ):
        self.w3 = w3
        self.pool = pool
        self.dry_run = dry_run
        self.worker_config = worker_config
        self.signer = signer
        self._nonce_counter = None
        if not self.dry_run and self.signer is None:
            raise RuntimeError("runtime signer is required for live transactions")

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
        signed = self.sign_transaction(wallet, tx)
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
        tx_hash = None
        broadcast_w3 = self.w3
        broadcast_errors = []
        receipt = None
        accepted_pending = False
        for rpc_label, candidate_w3 in self._broadcast_attempts(label):
            if rpc_label != "primary":
                log_block(
                    log,
                    logging.WARNING,
                    f"{label} broadcast fallback",
                    pool_context(self.pool),
                    {
                        "stage": "broadcast",
                        "action": label,
                        "status": "TRYING",
                        "rpc": rpc_label,
                        "signed_tx_hash": signed_tx_hash,
                        "next_action": "try same raw transaction on this RPC",
                    },
                )
            try:
                tx_hash = candidate_w3.eth.send_raw_transaction(signed.raw_transaction)
                broadcast_w3 = candidate_w3
                metadata["broadcast_rpc"] = rpc_label
                break
            except Exception as exc:
                if label != "mint":
                    raise
                broadcast_errors.append(f"{rpc_label}: {exc}")
                if self._is_known_transaction_error(exc):
                    lookup = self._lookup_mint_tx_with_fallback(signed_tx_hash)
                    if lookup.get("receipt") is not None:
                        receipt = lookup["receipt"]
                        tx_hash = receipt.get("transactionHash")
                        broadcast_w3 = lookup.get("w3") or candidate_w3
                        metadata["broadcast_rpc"] = lookup.get("rpc_label") or rpc_label
                        metadata["known_transaction_recovered"] = True
                        break
                    if lookup.get("tx_found"):
                        accepted_pending = True
                        metadata["broadcast_rpc"] = lookup.get("rpc_label") or rpc_label
                        metadata["known_transaction_pending"] = True
                        break
                log_block(
                    log,
                    logging.WARNING,
                    f"{label} broadcast failed",
                    pool_context(self.pool),
                    {
                        "stage": "broadcast",
                        "action": label,
                        "status": "FAILED",
                        "rpc": rpc_label,
                        "signed_tx_hash": signed_tx_hash,
                        "reason": exc,
                        "next_action": "try next RPC fallback",
                    },
                )
        if accepted_pending:
            return TxResult(
                tx_hash=signed_tx_hash,
                status="PENDING",
                metadata={
                    **metadata,
                    "error": "mint transaction known by RPC but receipt is not available",
                    "broadcast_errors": broadcast_errors,
                },
            )
        if tx_hash is None:
            if label != "mint":
                raise RuntimeError("transaction broadcast failed")
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
                    "reason": "; ".join(broadcast_errors[-3:]) or "broadcast failed",
                    "next_action": "journal recovery will treat signed hash as local-only unless found on-chain",
                },
            )
            return TxResult(
                tx_hash=signed_tx_hash,
                status="BROADCAST_UNKNOWN",
                metadata={
                    **metadata,
                    "error": "; ".join(broadcast_errors[-3:]) or "broadcast failed",
                    "broadcast_errors": broadcast_errors,
                },
            )
        tx_hash_hex = self._hex_value(tx_hash)
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
                "rpc": metadata.get("broadcast_rpc"),
            },
        )
        if receipt is None:
            try:
                receipt = broadcast_w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
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
                        "rpc": metadata.get("broadcast_rpc"),
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

    def _broadcast_attempts(self, label: str) -> list[tuple[str, Web3]]:
        attempts = [("primary", self.w3)]
        if label != "mint":
            return attempts
        current_url = getattr(getattr(self.w3, "provider", None), "endpoint_uri", None)
        seen = {current_url} if current_url else set()
        try:
            from latest_farms.config import RPC_BACKUP_LIST, RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_BACKUP_LIST, RPC_URLS_2

        urls = [RPC_URLS_2.get(self.pool.chain)] + RPC_BACKUP_LIST.get(self.pool.chain, [])
        fallback_index = 1
        for url in [item for item in urls if item]:
            if url in seen:
                continue
            seen.add(url)
            attempts.append((f"backup-{fallback_index}:{self._rpc_label(url)}", self._web3_for_rpc(url)))
            fallback_index += 1
        return attempts

    def sign_transaction(self, wallet: str, tx: dict):
        if self.signer is None:
            raise RuntimeError("runtime signer is required for live transactions")
        return self.signer.sign_transaction(wallet, tx)

    def _lookup_mint_tx_with_fallback(self, tx_hash: str) -> dict:
        for rpc_label, candidate_w3 in self._broadcast_attempts("mint"):
            try:
                receipt = candidate_w3.eth.get_transaction_receipt(tx_hash)
                return {"receipt": receipt, "tx_found": True, "rpc_label": rpc_label, "w3": candidate_w3}
            except TransactionNotFound:
                pass
            except Exception:
                pass
            try:
                candidate_w3.eth.get_transaction(tx_hash)
                return {"receipt": None, "tx_found": True, "rpc_label": rpc_label, "w3": candidate_w3}
            except TransactionNotFound:
                continue
            except Exception:
                continue
        return {"receipt": None, "tx_found": False}

    def _web3_for_rpc(self, url: str) -> Web3:
        candidate = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        if self.pool.chain.upper() == "BNB":
            try:
                from web3.middleware import ExtraDataToPOAMiddleware

                candidate.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware

                candidate.middleware_onion.inject(geth_poa_middleware, layer=0)
        return candidate

    @staticmethod
    def _is_known_transaction_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return "already known" in text or "known transaction" in text or "already imported" in text

    @staticmethod
    def _rpc_label(url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc or "unknown-rpc"

    @staticmethod
    def _hex_value(value) -> str:
        if value is None:
            return "0x"
        if isinstance(value, str):
            text = value
        elif hasattr(value, "hex"):
            text = value.hex()
        else:
            text = str(value)
        if text and not text.startswith("0x"):
            text = "0x" + text
        return text

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
