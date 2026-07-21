from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import CompoundJobState, TERMINAL_STATES

try:
    from latest_farms.create_db import get_connection
except ImportError:  # pragma: no cover
    from create_db import get_connection


_TERMINAL_VALUES = tuple(state.value for state in TERMINAL_STATES)


class CompoundJournal:
    TABLE = "configured_compound_jobs"

    _UPDATABLE_FIELDS = {
        "status",
        "current_action",
        "retry_count",
        "error_reason",
        "anchor_block",
        "current_tick",
        "tick_lower",
        "tick_upper",
        "liquidity_before",
        "liquidity_added",
        "liquidity_after",
        "quoted_amount0_raw",
        "quoted_amount1_raw",
        "collected_amount0_raw",
        "collected_amount1_raw",
        "reserved_amount0_raw",
        "reserved_amount1_raw",
        "swap_token_in",
        "swap_token_out",
        "swap_amount_in_raw",
        "swap_amount_out_raw",
        "amount0_used_raw",
        "amount1_used_raw",
        "dust0_raw",
        "dust1_raw",
        "fee_value_usd",
        "estimated_gas_usd",
        "actual_gas_native",
        "swap_provider",
        "collect_tx_hash",
        "swap_approval_tx_hash",
        "swap_tx_hash",
        "increase_approval0_tx_hash",
        "increase_approval1_tx_hash",
        "increase_tx_hash",
        "pending_action",
        "pending_nonce",
        "pending_signed_tx_hash",
        "pending_broadcast_tx_hash",
        "pending_since",
        "completed_at",
    }

    def migrate(self) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS configured_compound_jobs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    idempotency_key VARCHAR(160) NOT NULL,
                    chain VARCHAR(10) NOT NULL,
                    pool_address VARCHAR(42) NOT NULL,
                    wallet_address VARCHAR(42) NOT NULL,
                    npm_address VARCHAR(42) NOT NULL,
                    token_id BIGINT NOT NULL,
                    dex_type VARCHAR(40) NOT NULL,
                    stake_mode VARCHAR(20) NOT NULL,
                    status VARCHAR(40) NOT NULL,
                    current_action VARCHAR(40) NULL,
                    retry_count INT NOT NULL DEFAULT 0,
                    error_reason VARCHAR(500) NULL,
                    anchor_block BIGINT NULL,
                    current_tick INT NULL,
                    tick_lower INT NULL,
                    tick_upper INT NULL,
                    liquidity_before VARCHAR(80) NULL,
                    liquidity_added VARCHAR(80) NULL,
                    liquidity_after VARCHAR(80) NULL,
                    quoted_amount0_raw VARCHAR(80) NULL,
                    quoted_amount1_raw VARCHAR(80) NULL,
                    collected_amount0_raw VARCHAR(80) NULL,
                    collected_amount1_raw VARCHAR(80) NULL,
                    reserved_amount0_raw VARCHAR(80) NULL,
                    reserved_amount1_raw VARCHAR(80) NULL,
                    swap_token_in VARCHAR(42) NULL,
                    swap_token_out VARCHAR(42) NULL,
                    swap_amount_in_raw VARCHAR(80) NULL,
                    swap_amount_out_raw VARCHAR(80) NULL,
                    amount0_used_raw VARCHAR(80) NULL,
                    amount1_used_raw VARCHAR(80) NULL,
                    dust0_raw VARCHAR(80) NULL,
                    dust1_raw VARCHAR(80) NULL,
                    fee_value_usd DECIMAL(20,8) NULL,
                    estimated_gas_usd DECIMAL(20,8) NULL,
                    actual_gas_native DECIMAL(38,18) NULL,
                    swap_provider VARCHAR(30) NULL,
                    collect_tx_hash VARCHAR(66) NULL,
                    swap_approval_tx_hash VARCHAR(66) NULL,
                    swap_tx_hash VARCHAR(66) NULL,
                    increase_approval0_tx_hash VARCHAR(66) NULL,
                    increase_approval1_tx_hash VARCHAR(66) NULL,
                    increase_tx_hash VARCHAR(66) NULL,
                    pending_action VARCHAR(40) NULL,
                    pending_nonce BIGINT NULL,
                    pending_signed_tx_hash VARCHAR(66) NULL,
                    pending_broadcast_tx_hash VARCHAR(66) NULL,
                    pending_since DATETIME NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    completed_at DATETIME NULL,
                    UNIQUE KEY uniq_compound_idempotency (idempotency_key),
                    KEY idx_compound_position (chain, npm_address, token_id, status),
                    KEY idx_compound_wallet_pending (chain, wallet_address, pending_action)
                )
                """
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def create_job(self, values: dict[str, Any]) -> int:
        now = self._now()
        fields = [
            "idempotency_key", "chain", "pool_address", "wallet_address", "npm_address",
            "token_id", "dex_type", "stake_mode", "status", "anchor_block", "current_tick",
            "tick_lower", "tick_upper", "liquidity_before", "quoted_amount0_raw",
            "quoted_amount1_raw", "fee_value_usd", "estimated_gas_usd", "created_at", "updated_at",
        ]
        payload = {**values, "status": CompoundJobState.PREPARED.value, "created_at": now, "updated_at": now}
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {self.TABLE} ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})",
                tuple(payload.get(field) for field in fields),
            )
            job_id = int(cursor.lastrowid)
            conn.commit()
            return job_id
        finally:
            cursor.close()
            conn.close()

    def update(self, job_id: int, **values: Any) -> None:
        invalid = set(values) - self._UPDATABLE_FIELDS
        if invalid:
            raise ValueError(f"unsupported compound journal fields: {sorted(invalid)}")
        if not values:
            return
        values["updated_at"] = self._now()
        assignments = ",".join(f"{field}=%s" for field in values)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"UPDATE {self.TABLE} SET {assignments} WHERE id=%s",
                (*values.values(), int(job_id)),
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def mark_pending(
        self,
        job_id: int,
        state: CompoundJobState,
        action: str,
        nonce: int,
        signed_hash: str,
    ) -> None:
        self.update(
            job_id,
            status=state.value,
            current_action=action,
            pending_action=action,
            pending_nonce=int(nonce),
            pending_signed_tx_hash=signed_hash,
            pending_broadcast_tx_hash=None,
            pending_since=self._now(),
            error_reason=None,
        )

    def record_broadcast(self, job_id: int, tx_hash: str) -> None:
        self.update(job_id, pending_broadcast_tx_hash=tx_hash)

    def complete_transaction(
        self,
        job_id: int,
        next_state: CompoundJobState,
        tx_field: str,
        tx_hash: str,
    ) -> None:
        if tx_field not in self._UPDATABLE_FIELDS or not tx_field.endswith("_tx_hash"):
            raise ValueError(f"unsupported transaction field {tx_field}")
        self.update(
            job_id,
            status=next_state.value,
            current_action=None,
            pending_action=None,
            pending_nonce=None,
            pending_signed_tx_hash=None,
            pending_broadcast_tx_hash=None,
            pending_since=None,
            error_reason=None,
            **{tx_field: tx_hash},
        )

    def get(self, job_id: int) -> dict[str, Any] | None:
        return self._fetch_one(f"SELECT * FROM {self.TABLE} WHERE id=%s", (int(job_id),))

    def get_open_job(self, chain: str, npm_address: str, token_id: int) -> dict[str, Any] | None:
        placeholders = ",".join(["%s"] * len(_TERMINAL_VALUES))
        return self._fetch_one(
            f"SELECT * FROM {self.TABLE} WHERE chain=%s AND LOWER(npm_address)=LOWER(%s) "
            f"AND token_id=%s AND status NOT IN ({placeholders}) ORDER BY id DESC LIMIT 1",
            (chain, npm_address, int(token_id), *_TERMINAL_VALUES),
        )

    def fetch_open_jobs(self, chain: str | None = None) -> list[dict[str, Any]]:
        placeholders = ",".join(["%s"] * len(_TERMINAL_VALUES))
        if chain:
            return self._fetch_all(
                f"SELECT * FROM {self.TABLE} WHERE chain=%s AND status NOT IN ({placeholders}) ORDER BY id",
                (chain, *_TERMINAL_VALUES),
            )
        return self._fetch_all(
            f"SELECT * FROM {self.TABLE} WHERE status NOT IN ({placeholders}) ORDER BY id",
            _TERMINAL_VALUES,
        )

    def fetch_pending_jobs(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            f"SELECT * FROM {self.TABLE} WHERE pending_action IS NOT NULL ORDER BY id",
            (),
        )

    def last_completed_at(self, chain: str, npm_address: str, token_id: int):
        row = self._fetch_one(
            f"SELECT completed_at FROM {self.TABLE} WHERE chain=%s AND LOWER(npm_address)=LOWER(%s) "
            "AND token_id=%s AND status=%s ORDER BY completed_at DESC LIMIT 1",
            (chain, npm_address, int(token_id), CompoundJobState.COMPLETED.value),
        )
        return row.get("completed_at") if row else None

    def _fetch_one(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        rows = self._fetch_all(query, params)
        return rows[0] if rows else None

    def _fetch_all(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(query, params)
            return list(cursor.fetchall())
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)
