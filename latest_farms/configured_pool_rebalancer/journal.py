from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import logging
from typing import Iterator

import mysql.connector

from .models import PositionState, RebalancePlan, TxResult

log = logging.getLogger("configured_pool_rebalancer")


try:
    from latest_farms.create_db import get_connection
except ImportError:  # pragma: no cover
    from create_db import get_connection


class RebalanceJournal:
    def migrate(self) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS configured_rebalance_jobs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    chain VARCHAR(10) NOT NULL,
                    pool_address VARCHAR(42) NOT NULL,
                    wallet_address VARCHAR(42) NOT NULL,
                    old_token_id BIGINT NOT NULL,
                    new_token_id BIGINT NULL,
                    status VARCHAR(40) NOT NULL,
                    old_tick_lower INT NULL,
                    old_tick_upper INT NULL,
                    new_tick_lower INT NULL,
                    new_tick_upper INT NULL,
                    amount0_desired VARCHAR(80) NULL,
                    amount1_desired VARCHAR(80) NULL,
                    swap_tx_hash VARCHAR(66) NULL,
                    withdraw_tx_hash VARCHAR(66) NULL,
                    mint_tx_hash VARCHAR(66) NULL,
                    stake_tx_hash VARCHAR(66) NULL,
                    burn_tx_hash VARCHAR(66) NULL,
                    error_reason VARCHAR(500) NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    KEY idx_pool_status (chain, pool_address, status),
                    UNIQUE KEY uniq_chain_old_token (chain, old_token_id)
                )
                """
            )
            for column_name, column_def in [
                ("new_mint_tick", "INT NULL"),
                ("new_mint_tick_lower", "INT NULL"),
                ("new_mint_tick_upper", "INT NULL"),
                ("range_lower_percent", "DECIMAL(12,6) NULL"),
                ("range_upper_percent", "DECIMAL(12,6) NULL"),
                ("range_percent_source", "VARCHAR(40) NULL"),
                ("claimed_reward_token", "VARCHAR(42) NULL"),
                ("claimed_reward_raw", "VARCHAR(80) NULL"),
                ("claimed_reward_amount", "DECIMAL(38,18) NULL"),
                ("claimed_reward_price_usd", "DECIMAL(20,8) NULL"),
                ("claimed_reward_usd", "DECIMAL(20,8) NULL"),
                ("claimed_reward_source", "VARCHAR(40) NULL"),
                ("discord_pnl_notified_at", "DATETIME NULL"),
                ("discord_pending_notified_at", "DATETIME NULL"),
                ("discord_notify_error", "VARCHAR(500) NULL"),
                ("pre_balance0_raw", "VARCHAR(80) NULL"),
                ("pre_balance1_raw", "VARCHAR(80) NULL"),
                ("post_withdraw_balance0_raw", "VARCHAR(80) NULL"),
                ("post_withdraw_balance1_raw", "VARCHAR(80) NULL"),
                ("post_swap_balance0_raw", "VARCHAR(80) NULL"),
                ("post_swap_balance1_raw", "VARCHAR(80) NULL"),
                ("reserved_token0_address", "VARCHAR(42) NULL"),
                ("reserved_token1_address", "VARCHAR(42) NULL"),
                ("reserved_token0_raw", "VARCHAR(80) NULL"),
                ("reserved_token1_raw", "VARCHAR(80) NULL"),
                ("reservation_updated_at", "DATETIME NULL"),
                ("recovery_attempts", "INT NOT NULL DEFAULT 0"),
                ("last_recovery_error", "VARCHAR(500) NULL"),
                ("recovery_notified_at", "DATETIME NULL"),
            ]:
                self._add_column_if_missing(cursor, "configured_rebalance_jobs", column_name, column_def)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def _add_column_if_missing(self, cursor, table_name: str, column_name: str, column_def: str) -> None:
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table_name, column_name),
        )
        row = cursor.fetchone()
        count = row[0] if isinstance(row, tuple) else row["cnt"]
        if int(count) == 0:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    def create_or_update_plan(self, chain: str, pool_address: str, wallet: str, plan: RebalancePlan) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        lower_percent = plan.metadata.get("lower_percent")
        upper_percent = plan.metadata.get("upper_percent")
        percent_source = plan.metadata.get("range_percent_source")
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    INSERT INTO configured_rebalance_jobs (
                        chain, pool_address, wallet_address, old_token_id, status,
                        old_tick_lower, old_tick_upper, new_tick_lower, new_tick_upper,
                        amount0_desired, amount1_desired,
                        range_lower_percent, range_upper_percent, range_percent_source,
                        created_at, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        status=IF(
                            withdraw_tx_hash IS NULL
                            AND mint_tx_hash IS NULL
                            AND stake_tx_hash IS NULL
                            AND burn_tx_hash IS NULL,
                            VALUES(status),
                            status
                        ),
                        new_tick_lower=VALUES(new_tick_lower),
                        new_tick_upper=VALUES(new_tick_upper),
                        amount0_desired=VALUES(amount0_desired),
                        amount1_desired=VALUES(amount1_desired),
                        range_lower_percent=VALUES(range_lower_percent),
                        range_upper_percent=VALUES(range_upper_percent),
                        range_percent_source=VALUES(range_percent_source),
                        updated_at=VALUES(updated_at)
                    """,
                    (
                        chain,
                        pool_address,
                        wallet,
                        plan.old_token_id,
                        PositionState.PLANNED.value,
                        plan.old_tick_lower,
                        plan.old_tick_upper,
                        plan.new_tick_lower,
                        plan.new_tick_upper,
                        str(plan.amount0_desired),
                        str(plan.amount1_desired),
                        lower_percent,
                        upper_percent,
                        percent_source,
                        now,
                        now,
                    ),
                )
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                cursor.execute(
                    """
                    INSERT INTO configured_rebalance_jobs (
                        chain, pool_address, wallet_address, old_token_id, status,
                        old_tick_lower, old_tick_upper, new_tick_lower, new_tick_upper,
                        amount0_desired, amount1_desired, created_at, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        status=IF(
                            withdraw_tx_hash IS NULL
                            AND mint_tx_hash IS NULL
                            AND stake_tx_hash IS NULL
                            AND burn_tx_hash IS NULL,
                            VALUES(status),
                            status
                        ),
                        new_tick_lower=VALUES(new_tick_lower),
                        new_tick_upper=VALUES(new_tick_upper),
                        amount0_desired=VALUES(amount0_desired),
                        amount1_desired=VALUES(amount1_desired),
                        updated_at=VALUES(updated_at)
                    """,
                    (
                        chain,
                        pool_address,
                        wallet,
                        plan.old_token_id,
                        PositionState.PLANNED.value,
                        plan.old_tick_lower,
                        plan.old_tick_upper,
                        plan.new_tick_lower,
                        plan.new_tick_upper,
                        str(plan.amount0_desired),
                        str(plan.amount1_desired),
                        now,
                        now,
                    ),
                )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def mark_status(
        self,
        chain: str,
        old_token_id: int,
        status: PositionState,
        tx_label: str | None = None,
        tx_result: TxResult | None = None,
        new_token_id: int | None = None,
        error_reason: str | None = None,
        mint_tick: int | None = None,
        mint_tick_lower: int | None = None,
        mint_tick_upper: int | None = None,
        range_lower_percent: float | None = None,
        range_upper_percent: float | None = None,
        range_percent_source: str | None = None,
        claimed_reward_token: str | None = None,
        claimed_reward_raw: str | None = None,
        claimed_reward_amount: float | None = None,
        claimed_reward_price_usd: float | None = None,
        claimed_reward_usd: float | None = None,
        claimed_reward_source: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        assignments: list[tuple[str, object]] = [("status", status.value), ("updated_at", now)]
        success_statuses = {PositionState.MINTED_UNSTAKED, PositionState.REMINTED, PositionState.BURNED}
        if new_token_id is not None:
            assignments.append(("new_token_id", new_token_id))
        if mint_tick is not None:
            assignments.append(("new_mint_tick", mint_tick))
        if mint_tick_lower is not None:
            assignments.append(("new_mint_tick_lower", mint_tick_lower))
        if mint_tick_upper is not None:
            assignments.append(("new_mint_tick_upper", mint_tick_upper))
        if range_lower_percent is not None:
            assignments.append(("range_lower_percent", range_lower_percent))
        if range_upper_percent is not None:
            assignments.append(("range_upper_percent", range_upper_percent))
        if range_percent_source is not None:
            assignments.append(("range_percent_source", range_percent_source[:40]))
        if claimed_reward_token is not None:
            assignments.append(("claimed_reward_token", claimed_reward_token[:42]))
        if claimed_reward_raw is not None:
            assignments.append(("claimed_reward_raw", str(claimed_reward_raw)[:80]))
        if claimed_reward_amount is not None:
            assignments.append(("claimed_reward_amount", claimed_reward_amount))
        if claimed_reward_price_usd is not None:
            assignments.append(("claimed_reward_price_usd", claimed_reward_price_usd))
        if claimed_reward_usd is not None:
            assignments.append(("claimed_reward_usd", claimed_reward_usd))
        if claimed_reward_source is not None:
            assignments.append(("claimed_reward_source", claimed_reward_source[:40]))
        if error_reason:
            assignments.append(("error_reason", error_reason[:500]))
        elif status in success_statuses:
            assignments.append(("error_reason", None))
            assignments.append(("last_recovery_error", None))
            assignments.append(("reserved_token0_raw", "0"))
            assignments.append(("reserved_token1_raw", "0"))
            assignments.append(("reservation_updated_at", now))
            assignments.append(("recovery_notified_at", None))
        if tx_label and tx_result:
            column = f"{tx_label}_tx_hash"
            if column in {"swap_tx_hash", "withdraw_tx_hash", "mint_tx_hash", "stake_tx_hash", "burn_tx_hash"}:
                assignments.append((column, tx_result.tx_hash))

        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                fields = [f"{name}=%s" for name, _ in assignments]
                values = [value for _, value in assignments]
                values.extend([chain, old_token_id])
                cursor.execute(
                    f"UPDATE configured_rebalance_jobs SET {', '.join(fields)} WHERE chain=%s AND old_token_id=%s",
                    tuple(values),
                )
                self._warn_if_missing_job(cursor, chain, old_token_id)
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                unsupported = {
                    "new_mint_tick",
                    "new_mint_tick_lower",
                    "new_mint_tick_upper",
                    "range_lower_percent",
                    "range_upper_percent",
                    "range_percent_source",
                    "claimed_reward_token",
                    "claimed_reward_raw",
                    "claimed_reward_amount",
                    "claimed_reward_price_usd",
                    "claimed_reward_usd",
                    "claimed_reward_source",
                    "last_recovery_error",
                    "reserved_token0_raw",
                    "reserved_token1_raw",
                    "reservation_updated_at",
                    "recovery_notified_at",
                }
                legacy_assignments = [(name, value) for name, value in assignments if name not in unsupported]
                fields = [f"{name}=%s" for name, _ in legacy_assignments]
                values = [value for _, value in legacy_assignments]
                values.extend([chain, old_token_id])
                cursor.execute(
                    f"UPDATE configured_rebalance_jobs SET {', '.join(fields)} WHERE chain=%s AND old_token_id=%s",
                    tuple(values),
                )
                self._warn_if_missing_job(cursor, chain, old_token_id)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def _warn_if_missing_job(self, cursor, chain: str, old_token_id: int) -> None:
        if cursor.rowcount != 0:
            return
        cursor.execute(
            "SELECT 1 FROM configured_rebalance_jobs WHERE chain=%s AND old_token_id=%s LIMIT 1",
            (chain, int(old_token_id)),
        )
        if cursor.fetchone():
            return
        log.warning("job status update matched no row chain=%s old_token_id=%s", chain, old_token_id)

    def record_balance_snapshot(
        self,
        chain: str,
        old_token_id: int,
        stage: str,
        balance0_raw: int,
        balance1_raw: int,
    ) -> None:
        columns_by_stage = {
            "pre": ("pre_balance0_raw", "pre_balance1_raw"),
            "post_withdraw": ("post_withdraw_balance0_raw", "post_withdraw_balance1_raw"),
            "post_swap": ("post_swap_balance0_raw", "post_swap_balance1_raw"),
        }
        if stage not in columns_by_stage:
            raise ValueError(f"unsupported balance snapshot stage {stage}")
        col0, col1 = columns_by_stage[stage]
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    f"""
                    UPDATE configured_rebalance_jobs
                    SET {col0}=%s, {col1}=%s, updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (str(int(balance0_raw)), str(int(balance1_raw)), now, chain, int(old_token_id)),
                )
                self._warn_if_missing_job(cursor, chain, old_token_id)
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                log.warning(
                    "balance snapshot columns missing; run configured rebalancer with --migrate "
                    "before relying on partial recovery"
                )
        finally:
            cursor.close()
            conn.close()

    def record_reservation(
        self,
        chain: str,
        old_token_id: int,
        token0_address: str,
        token1_address: str,
        reserved0_raw: int,
        reserved1_raw: int,
    ) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    UPDATE configured_rebalance_jobs
                    SET reserved_token0_address=%s,
                        reserved_token1_address=%s,
                        reserved_token0_raw=%s,
                        reserved_token1_raw=%s,
                        reservation_updated_at=%s,
                        updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (
                        token0_address[:42],
                        token1_address[:42],
                        str(max(0, int(reserved0_raw)))[:80],
                        str(max(0, int(reserved1_raw)))[:80],
                        now,
                        now,
                        chain,
                        int(old_token_id),
                    ),
                )
                self._warn_if_missing_job(cursor, chain, old_token_id)
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                log.warning(
                    "reservation columns missing; run configured rebalancer with --migrate "
                    "before relying on partial recovery"
                )
        finally:
            cursor.close()
            conn.close()

    def mark_recovery_error(self, chain: str, old_token_id: int, error_reason: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    UPDATE configured_rebalance_jobs
                    SET recovery_attempts=COALESCE(recovery_attempts, 0) + 1,
                        recovery_notified_at=IF(last_recovery_error <=> %s, recovery_notified_at, NULL),
                        last_recovery_error=%s,
                        error_reason=%s,
                        updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (
                        error_reason[:500],
                        error_reason[:500],
                        error_reason[:500],
                        now,
                        chain,
                        int(old_token_id),
                    ),
                )
                self._warn_if_missing_job(cursor, chain, old_token_id)
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                log.warning("recovery columns missing; error was not persisted for tokenId=%s", old_token_id)
        finally:
            cursor.close()
            conn.close()

    def clear_recovery_error(self, chain: str, old_token_id: int) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    UPDATE configured_rebalance_jobs
                    SET last_recovery_error=NULL,
                        recovery_notified_at=NULL,
                        updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (now, chain, int(old_token_id)),
                )
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
        finally:
            cursor.close()
            conn.close()

    def mark_recovery_notified(self, chain: str, old_token_id: int) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    UPDATE configured_rebalance_jobs
                    SET recovery_notified_at=%s,
                        updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (now, now, chain, int(old_token_id)),
                )
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
        finally:
            cursor.close()
            conn.close()

    def recovery_already_notified(self, chain: str, old_token_id: int) -> bool:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    SELECT recovery_notified_at
                    FROM configured_rebalance_jobs
                    WHERE chain=%s AND old_token_id=%s
                    LIMIT 1
                    """,
                    (chain, int(old_token_id)),
                )
                row = cursor.fetchone()
                if not row:
                    return False
                value = row[0] if isinstance(row, tuple) else row.get("recovery_notified_at")
                return value is not None
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return False
                raise
        finally:
            cursor.close()
            conn.close()

    def get_mint_tick_basis(
        self,
        chain: str,
        pool_address: str,
        wallet: str,
        token_id: int,
    ) -> tuple[int, int, int] | None:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    SELECT new_mint_tick, new_mint_tick_lower, new_mint_tick_upper
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND new_token_id=%s
                      AND new_mint_tick IS NOT NULL
                      AND new_mint_tick_lower IS NOT NULL
                      AND new_mint_tick_upper IS NOT NULL
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (chain, pool_address, wallet, int(token_id)),
                )
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return None
                raise
            row = cursor.fetchone()
            if not row:
                return None
            return int(row[0]), int(row[1]), int(row[2])
        finally:
            cursor.close()
            conn.close()

    def mark_discord_pnl_notified(self, chain: str, old_token_id: int) -> None:
        self._mark_discord_column(chain, old_token_id, "discord_pnl_notified_at")

    def mark_discord_pending_notified(self, chain: str, old_token_id: int) -> None:
        self._mark_discord_column(chain, old_token_id, "discord_pending_notified_at")

    def mark_discord_error(self, chain: str, old_token_id: int, error: str) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    UPDATE configured_rebalance_jobs
                    SET discord_notify_error=%s, updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (
                        error[:500],
                        datetime.now(timezone.utc).replace(tzinfo=None),
                        chain,
                        int(old_token_id),
                    ),
                )
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
        finally:
            cursor.close()
            conn.close()

    def _mark_discord_column(self, chain: str, old_token_id: int, column: str) -> None:
        if column not in {"discord_pnl_notified_at", "discord_pending_notified_at"}:
            raise ValueError(f"unsupported discord notification column {column}")
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                cursor.execute(
                    f"""
                    UPDATE configured_rebalance_jobs
                    SET {column}=%s, discord_notify_error=NULL, updated_at=%s
                    WHERE chain=%s AND old_token_id=%s
                    """,
                    (now, now, chain, int(old_token_id)),
                )
                conn.commit()
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
        finally:
            cursor.close()
            conn.close()

    def fetch_discord_pnl_pending_jobs(self, chain: str, pool_address: str, wallet: str) -> list[dict]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            try:
                cursor.execute(
                    """
                    SELECT old_token_id, new_token_id, status, wallet_address,
                           discord_pending_notified_at
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND status IN ('REMINTED', 'BURNED', 'MINTED_UNSTAKED')
                      AND new_token_id IS NOT NULL
                      AND discord_pnl_notified_at IS NULL
                    ORDER BY updated_at ASC, id ASC
                    """,
                    (chain, pool_address, wallet),
                )
                return list(cursor.fetchall())
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return []
                raise
        finally:
            cursor.close()
            conn.close()

    def fetch_swap_pending_jobs(self, chain: str, pool_address: str, wallet: str) -> list[dict]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            try:
                cursor.execute(
                    """
                    SELECT old_token_id, swap_tx_hash, status, wallet_address
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND status='SWAP_PENDING'
                      AND swap_tx_hash IS NOT NULL
                      AND mint_tx_hash IS NULL
                    ORDER BY updated_at ASC, id ASC
                    """,
                    (chain, pool_address, wallet),
                )
                return list(cursor.fetchall())
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return []
                raise
        finally:
            cursor.close()
            conn.close()

    def fetch_recoverable_jobs(self, chain: str, pool_address: str, wallet: str) -> list[dict]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            try:
                cursor.execute(
                    """
                    SELECT old_token_id, new_token_id, status, wallet_address,
                           old_tick_lower, old_tick_upper, new_tick_lower, new_tick_upper,
                           amount0_desired, amount1_desired,
                           swap_tx_hash, withdraw_tx_hash, mint_tx_hash, stake_tx_hash, burn_tx_hash,
                           pre_balance0_raw, pre_balance1_raw,
                           post_withdraw_balance0_raw, post_withdraw_balance1_raw,
                           post_swap_balance0_raw, post_swap_balance1_raw,
                           reserved_token0_address, reserved_token1_address,
                           reserved_token0_raw, reserved_token1_raw, reservation_updated_at,
                           recovery_attempts, last_recovery_error, recovery_notified_at, error_reason
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND (
                            (
                                status='MINTED_UNSTAKED'
                                AND new_token_id IS NOT NULL
                                AND stake_tx_hash IS NULL
                            )
                            OR (
                                status IN ('WITHDRAWN_UNBURNED', 'SWAP_BLOCKED', 'FAILED')
                                AND withdraw_tx_hash IS NOT NULL
                                AND mint_tx_hash IS NULL
                            )
                      )
                    ORDER BY updated_at ASC, id ASC
                    """,
                    (chain, pool_address, wallet),
                )
                return list(cursor.fetchall())
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return []
                raise
        finally:
            cursor.close()
            conn.close()

    def fetch_wallet_token_reservations(self, chain: str, wallet: str) -> list[dict]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            try:
                cursor.execute(
                    """
                    SELECT token_address, SUM(reserved_raw) AS reserved_raw
                    FROM (
                        SELECT LOWER(reserved_token0_address) AS token_address,
                               CAST(COALESCE(NULLIF(reserved_token0_raw, ''), '0') AS DECIMAL(65,0)) AS reserved_raw
                        FROM configured_rebalance_jobs
                        WHERE chain=%s
                          AND LOWER(wallet_address)=LOWER(%s)
                          AND mint_tx_hash IS NULL
                          AND reserved_token0_address IS NOT NULL
                          AND reserved_token0_raw IS NOT NULL
                          AND status IN ('WITHDRAWN_UNBURNED', 'SWAP_BLOCKED', 'SWAP_PENDING', 'FAILED')
                        UNION ALL
                        SELECT LOWER(reserved_token1_address) AS token_address,
                               CAST(COALESCE(NULLIF(reserved_token1_raw, ''), '0') AS DECIMAL(65,0)) AS reserved_raw
                        FROM configured_rebalance_jobs
                        WHERE chain=%s
                          AND LOWER(wallet_address)=LOWER(%s)
                          AND mint_tx_hash IS NULL
                          AND reserved_token1_address IS NOT NULL
                          AND reserved_token1_raw IS NOT NULL
                          AND status IN ('WITHDRAWN_UNBURNED', 'SWAP_BLOCKED', 'SWAP_PENDING', 'FAILED')
                    ) reservations
                    WHERE token_address IS NOT NULL
                    GROUP BY token_address
                    HAVING SUM(reserved_raw) > 0
                    """,
                    (chain, wallet, chain, wallet),
                )
                return list(cursor.fetchall())
            except mysql.connector.Error as exc:
                if exc.errno in (1054, 1146):
                    return []
                raise
        finally:
            cursor.close()
            conn.close()

    def get_wallet_position_first_tick_basis(
        self,
        chain: str,
        pool_address: str,
        wallet: str,
        token_id: int,
    ) -> tuple[int, int, int] | None:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute(
                    """
                    SELECT current_price, lower_price, upper_price
                    FROM wallet_nft_position
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND nft_id=%s
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    (chain, pool_address, wallet, int(token_id)),
                )
            except mysql.connector.Error as exc:
                if exc.errno == 1146:
                    return None
                raise
            row = cursor.fetchone()
            if not row or row[0] is None or row[1] is None or row[2] is None:
                return None
            return int(row[0]), int(row[1]), int(row[2])
        finally:
            cursor.close()
            conn.close()


@contextmanager
def mysql_advisory_lock(lock_name: str, timeout_seconds: int) -> Iterator[None]:
    conn = get_connection()
    cursor = conn.cursor()
    acquired = False
    try:
        cursor.execute("SELECT GET_LOCK(%s, %s)", (lock_name[:64], timeout_seconds))
        row = cursor.fetchone()
        acquired = bool(row and row[0] == 1)
        if not acquired:
            raise TimeoutError(f"could not acquire DB lock {lock_name}")
        yield
    finally:
        if acquired:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name[:64],))
            cursor.fetchone()
        cursor.close()
        conn.close()
