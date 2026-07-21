from __future__ import annotations

import logging
from urllib.parse import urlparse

from web3 import Web3
from web3.exceptions import TimeExhausted, TransactionNotFound

from ..evm import get_chain_id, get_gas_params, validate_gas_cap
from ..models import PoolConfig, TxResult, WorkerConfig
from ..signer import RuntimeSigner
from .journal import CompoundJournal
from .models import CompoundJobState


log = logging.getLogger("configured_pool_rebalancer.auto_compound")


class CompoundExecutor:
    """Journal-first transaction sender isolated from the rebalance executor."""

    def __init__(
        self,
        w3: Web3,
        pool: PoolConfig,
        worker_config: WorkerConfig,
        journal: CompoundJournal,
        signer: RuntimeSigner | None,
    ):
        self.w3 = w3
        self.pool = pool
        self.worker_config = worker_config
        self.journal = journal
        self.signer = signer
        self.dry_run = worker_config.dry_run
        if not self.dry_run and signer is None:
            raise RuntimeError("runtime signer is required for live compound transactions")

    def send_call(
        self,
        job_id: int | None,
        call_fn,
        action: str,
        pending_state: CompoundJobState,
        success_state: CompoundJobState,
        tx_field: str,
        *,
        gas: int | None = None,
        value: int = 0,
    ) -> TxResult:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        if self.dry_run:
            try:
                call_fn.call({"from": wallet, "value": int(value)})
            except Exception as exc:
                raise RuntimeError(f"{action} simulation failed: {exc}") from exc
            return TxResult(
                tx_hash=f"dry-run:{action.lower()}",
                dry_run=True,
                metadata={"action": action, "success_state": success_state.value, "tx_field": tx_field},
            )

        policy = self.worker_config.gas_policies.get(self.pool.chain.upper())
        gas_params = get_gas_params(self.w3, self.pool.chain, action=action.lower(), policy=policy)
        effective_policy = policy
        if effective_policy is None:
            from ..evm import DEFAULT_GAS_POLICIES

            effective_policy = DEFAULT_GAS_POLICIES.get(self.pool.chain.upper())
        cap = (
            effective_policy.max_fee_gwei
            if effective_policy and effective_policy.max_fee_gwei is not None
            else self.pool.max_gas_gwei
        )
        validate_gas_cap(gas_params, cap)
        if gas is None:
            estimated = int(call_fn.estimate_gas({"from": wallet, "value": int(value)}))
            gas = max(100_000, int(estimated * 1.30))
        nonce = int(self.w3.eth.get_transaction_count(wallet, "pending"))
        tx = call_fn.build_transaction(
            {
                "from": wallet,
                "nonce": nonce,
                "gas": int(gas),
                "value": int(value),
                "chainId": get_chain_id(self.pool.chain),
                **gas_params,
            }
        )
        return self._sign_broadcast_wait(job_id, tx, action, pending_state, success_state, tx_field)

    def send_raw(
        self,
        job_id: int | None,
        raw: dict,
        action: str,
        pending_state: CompoundJobState,
        success_state: CompoundJobState,
        tx_field: str,
    ) -> TxResult:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        if self.dry_run:
            self.w3.eth.call(
                {"from": wallet, "to": Web3.to_checksum_address(raw["to"]), "data": raw["data"], "value": int(raw.get("value", 0))}
            )
            return TxResult(tx_hash=f"dry-run:{action.lower()}", dry_run=True, metadata={"action": action})
        policy = self.worker_config.gas_policies.get(self.pool.chain.upper())
        gas_params = get_gas_params(self.w3, self.pool.chain, action="swap", policy=policy)
        effective_policy = policy
        if effective_policy is None:
            from ..evm import DEFAULT_GAS_POLICIES

            effective_policy = DEFAULT_GAS_POLICIES.get(self.pool.chain.upper())
        cap = (
            effective_policy.max_fee_gwei
            if effective_policy and effective_policy.max_fee_gwei is not None
            else self.pool.max_gas_gwei
        )
        validate_gas_cap(gas_params, cap)
        nonce = int(self.w3.eth.get_transaction_count(wallet, "pending"))
        base = {
            "from": wallet,
            "to": Web3.to_checksum_address(raw["to"]),
            "data": raw["data"],
            "value": int(raw.get("value", 0)),
            "nonce": nonce,
            "chainId": get_chain_id(self.pool.chain),
            **gas_params,
        }
        try:
            estimated = int(self.w3.eth.estimate_gas(base))
        except Exception as exc:
            raise RuntimeError(f"{action} simulation failed: {exc}") from exc
        tx = {**base, "gas": max(120_000, int(estimated * 1.30))}
        return self._sign_broadcast_wait(job_id, tx, action, pending_state, success_state, tx_field)

    def _sign_broadcast_wait(
        self,
        job_id: int | None,
        tx: dict,
        action: str,
        pending_state: CompoundJobState,
        success_state: CompoundJobState,
        tx_field: str,
    ) -> TxResult:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        signed = self.signer.sign_transaction(wallet, tx)
        signed_hash = Web3.to_hex(Web3.keccak(signed.raw_transaction))
        if job_id is not None:
            self.journal.mark_pending(job_id, pending_state, action, int(tx["nonce"]), signed_hash)

        tx_hash = None
        broadcast_w3 = self.w3
        errors: list[str] = []
        for label, candidate in self._rpc_attempts():
            try:
                tx_hash = candidate.eth.send_raw_transaction(signed.raw_transaction)
                broadcast_w3 = candidate
                break
            except Exception as exc:
                errors.append(f"{label}: {exc}")
                if self._known_transaction(exc):
                    tx_hash = signed_hash
                    broadcast_w3 = candidate
                    break
        if tx_hash is None:
            return TxResult(
                tx_hash=signed_hash,
                status="BROADCAST_UNKNOWN",
                metadata={"action": action, "signed_tx_hash": signed_hash, "errors": errors},
            )
        tx_hash_hex = tx_hash if isinstance(tx_hash, str) else Web3.to_hex(tx_hash)
        if job_id is not None:
            self.journal.record_broadcast(job_id, tx_hash_hex)
        try:
            receipt = broadcast_w3.eth.wait_for_transaction_receipt(tx_hash_hex, timeout=300)
        except TimeExhausted as exc:
            return TxResult(
                tx_hash=tx_hash_hex,
                status="PENDING",
                metadata={"action": action, "signed_tx_hash": signed_hash, "error": str(exc)},
            )
        if int(receipt.get("status", 0)) != 1:
            return TxResult(
                tx_hash=tx_hash_hex,
                status="FAILED",
                gas_used=int(receipt.get("gasUsed") or 0),
                metadata={"action": action, "signed_tx_hash": signed_hash, "receipt": receipt},
            )
        if job_id is not None:
            self.journal.complete_transaction(job_id, success_state, tx_field, tx_hash_hex)
        gas_price = int(receipt.get("effectiveGasPrice") or tx.get("maxFeePerGas") or tx.get("gasPrice") or 0)
        return TxResult(
            tx_hash=tx_hash_hex,
            gas_used=int(receipt.get("gasUsed") or 0),
            gas_price_gwei=float(Web3.from_wei(gas_price, "gwei")) if gas_price else 0.0,
            metadata={
                "action": action,
                "signed_tx_hash": signed_hash,
                "receipt": receipt,
                "receipt_block": int(receipt.get("blockNumber") or 0),
            },
        )

    def lookup_pending(self, job: dict) -> dict:
        tx_hash = job.get("pending_broadcast_tx_hash") or job.get("pending_signed_tx_hash")
        if not tx_hash:
            return {"status": "MISSING_HASH"}
        tx_found = False
        for label, candidate in self._rpc_attempts():
            try:
                receipt = candidate.eth.get_transaction_receipt(tx_hash)
                return {"status": "SUCCESS" if int(receipt.get("status", 0)) == 1 else "REVERTED", "receipt": receipt, "rpc": label}
            except TransactionNotFound:
                pass
            except Exception:
                pass
            try:
                candidate.eth.get_transaction(tx_hash)
                tx_found = True
            except Exception:
                pass
        return {"status": "PENDING" if tx_found else "UNKNOWN"}

    def _rpc_attempts(self) -> list[tuple[str, Web3]]:
        attempts = [("primary", self.w3)]
        try:
            from latest_farms.config import RPC_BACKUP_LIST, RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_BACKUP_LIST, RPC_URLS_2
        current = getattr(getattr(self.w3, "provider", None), "endpoint_uri", None)
        seen = {current} if current else set()
        urls = [RPC_URLS_2.get(self.pool.chain), *RPC_BACKUP_LIST.get(self.pool.chain, [])]
        for url in filter(None, urls):
            if url in seen:
                continue
            seen.add(url)
            candidate = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
            if self.pool.chain.upper() == "BNB":
                try:
                    from web3.middleware import ExtraDataToPOAMiddleware

                    candidate.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                except ImportError:  # pragma: no cover
                    from web3.middleware import geth_poa_middleware

                    candidate.middleware_onion.inject(geth_poa_middleware, layer=0)
            attempts.append((urlparse(url).netloc or "fallback", candidate))
        return attempts

    @staticmethod
    def _known_transaction(exc: Exception) -> bool:
        value = str(exc).lower()
        return "already known" in value or "known transaction" in value or "already imported" in value
