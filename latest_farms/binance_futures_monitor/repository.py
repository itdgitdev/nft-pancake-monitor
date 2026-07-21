from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterator

from latest_farms.create_db import get_connection

from .models import MarketType, MonitorConfig, NormalizedPosition


ConnectionFactory = Callable[[], object]


class BinanceMonitorRepository:
    def __init__(self, connection_factory: ConnectionFactory = get_connection):
        self._connection_factory = connection_factory

    def migrate(self) -> None:
        connection = self._connection_factory()
        cursor = connection.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS binance_account_wallet_links (
                    account_alias VARCHAR(64) NOT NULL,
                    wallet_type VARCHAR(16) NOT NULL,
                    wallet_address VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (account_alias, wallet_type, wallet_address),
                    KEY idx_binance_wallet_link (wallet_type, wallet_address)
                ) ENGINE=InnoDB
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS binance_futures_sync_state (
                    account_alias VARCHAR(64) NOT NULL,
                    market_type VARCHAR(16) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    stale_after_seconds INT NOT NULL DEFAULT 180,
                    last_attempt_at DATETIME NULL,
                    last_success_at DATETIME NULL,
                    error_code VARCHAR(64) NULL,
                    error_message VARCHAR(500) NULL,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (account_alias, market_type)
                ) ENGINE=InnoDB
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS binance_futures_positions_current (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    account_alias VARCHAR(64) NOT NULL,
                    market_type VARCHAR(16) NOT NULL,
                    symbol VARCHAR(40) NOT NULL,
                    pair VARCHAR(40) NOT NULL,
                    contract_type VARCHAR(32) NULL,
                    delivery_date BIGINT NULL,
                    raw_base_asset VARCHAR(32) NOT NULL,
                    base_asset VARCHAR(32) NOT NULL,
                    quote_asset VARCHAR(32) NOT NULL,
                    margin_asset VARCHAR(32) NOT NULL,
                    contract_multiplier DECIMAL(38,18) NOT NULL,
                    contract_size_quote DECIMAL(38,18) NULL,
                    position_side VARCHAR(16) NOT NULL,
                    position_amt DECIMAL(38,18) NOT NULL,
                    position_amt_unit VARCHAR(16) NOT NULL,
                    signed_base_exposure DECIMAL(38,18) NOT NULL,
                    entry_price DECIMAL(38,18) NULL,
                    break_even_price DECIMAL(38,18) NULL,
                    mark_price DECIMAL(38,18) NOT NULL,
                    unrealized_pnl DECIMAL(38,18) NULL,
                    pnl_asset VARCHAR(32) NOT NULL,
                    notional_value DECIMAL(38,18) NULL,
                    notional_asset VARCHAR(32) NOT NULL,
                    liquidation_price DECIMAL(38,18) NULL,
                    isolated_margin DECIMAL(38,18) NULL,
                    leverage INT NULL,
                    margin_type VARCHAR(16) NULL,
                    binance_update_time BIGINT NULL,
                    synced_at DATETIME NOT NULL,
                    UNIQUE KEY uq_binance_current_position (
                        account_alias, market_type, symbol, position_side
                    ),
                    KEY idx_binance_current_account (account_alias, market_type)
                ) ENGINE=InnoDB
            """)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    def sync_wallet_links(self, config: MonitorConfig) -> None:
        rows = [
            (account.alias, wallet.wallet_type, wallet.wallet_address)
            for account in config.accounts
            for wallet in account.linked_wallets
        ]
        aliases = [account.alias for account in config.accounts]
        connection = self._connection_factory()
        cursor = connection.cursor()
        lock_acquired = False
        try:
            cursor.execute("SELECT GET_LOCK(%s, %s)", ("binance:wallet-links", 5))
            lock_acquired = cursor.fetchone()[0] == 1
            if not lock_acquired:
                raise TimeoutError("Binance wallet mapping lock is busy")
            cursor.execute("DELETE FROM binance_account_wallet_links")
            cursor.executemany(
                """
                INSERT INTO binance_account_wallet_links (
                    account_alias, wallet_type, wallet_address
                ) VALUES (%s, %s, %s)
                """,
                rows,
            )
            placeholders = ",".join(["%s"] * len(aliases))
            cursor.execute(
                f"DELETE FROM binance_futures_positions_current WHERE account_alias NOT IN ({placeholders})",
                aliases,
            )
            cursor.execute(
                f"DELETE FROM binance_futures_sync_state WHERE account_alias NOT IN ({placeholders})",
                aliases,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            try:
                if lock_acquired:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", ("binance:wallet-links",))
                    cursor.fetchone()
            finally:
                try:
                    cursor.close()
                finally:
                    connection.close()

    def mark_running(
        self,
        account_alias: str,
        market: MarketType,
        stale_after_seconds: int,
    ) -> None:
        now = _utc_now()
        self._execute_state_update(
            """
            INSERT INTO binance_futures_sync_state (
                account_alias, market_type, status, stale_after_seconds,
                last_attempt_at, error_code, error_message
            ) VALUES (%s, %s, 'RUNNING', %s, %s, NULL, NULL)
            ON DUPLICATE KEY UPDATE
                status = 'RUNNING',
                stale_after_seconds = VALUES(stale_after_seconds),
                last_attempt_at = VALUES(last_attempt_at),
                error_code = NULL,
                error_message = NULL
            """,
            (account_alias, market.value, stale_after_seconds, now),
        )

    def replace_positions_success(
        self,
        account_alias: str,
        market: MarketType,
        stale_after_seconds: int,
        positions: list[NormalizedPosition],
    ) -> None:
        now = _utc_now()
        connection = self._connection_factory()
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM binance_futures_positions_current
                WHERE account_alias = %s AND market_type = %s
                """,
                (account_alias, market.value),
            )
            if positions:
                cursor.executemany(
                    """
                    INSERT INTO binance_futures_positions_current (
                        account_alias, market_type, symbol, pair, contract_type,
                        delivery_date, raw_base_asset, base_asset, quote_asset,
                        margin_asset, contract_multiplier, contract_size_quote,
                        position_side, position_amt, position_amt_unit,
                        signed_base_exposure, entry_price, break_even_price,
                        mark_price, unrealized_pnl, pnl_asset, notional_value,
                        notional_asset, liquidation_price, isolated_margin,
                        leverage, margin_type, binance_update_time, synced_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    [self._position_row(position, now) for position in positions],
                )
            cursor.execute(
                """
                UPDATE binance_futures_sync_state
                SET status = 'SUCCESS', stale_after_seconds = %s,
                    last_attempt_at = %s, last_success_at = %s,
                    error_code = NULL, error_message = NULL
                WHERE account_alias = %s AND market_type = %s
                """,
                (stale_after_seconds, now, now, account_alias, market.value),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    def mark_failed(
        self,
        account_alias: str,
        market: MarketType,
        stale_after_seconds: int,
        error_code: str,
        error_message: str,
    ) -> None:
        now = _utc_now()
        self._execute_state_update(
            """
            INSERT INTO binance_futures_sync_state (
                account_alias, market_type, status, stale_after_seconds,
                last_attempt_at, error_code, error_message
            ) VALUES (%s, %s, 'FAILED', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = 'FAILED',
                stale_after_seconds = VALUES(stale_after_seconds),
                last_attempt_at = VALUES(last_attempt_at),
                error_code = VALUES(error_code),
                error_message = VALUES(error_message)
            """,
            (
                account_alias,
                market.value,
                stale_after_seconds,
                now,
                error_code[:64],
                error_message[:500],
            ),
        )

    @contextmanager
    def account_market_lock(
        self, account_alias: str, market: MarketType, timeout_seconds: int = 0
    ) -> Iterator[None]:
        connection = self._connection_factory()
        cursor = connection.cursor()
        lock_name = f"binance:{account_alias}:{market.value}"[:64]
        acquired = False
        try:
            cursor.execute("SELECT GET_LOCK(%s, %s)", (lock_name, timeout_seconds))
            row = cursor.fetchone()
            acquired = bool(row and row[0] == 1)
            if not acquired:
                raise TimeoutError(f"could not acquire DB lock for {account_alias}/{market.value}")
            yield
        finally:
            if acquired:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                cursor.fetchone()
            cursor.close()
            connection.close()

    def _execute_state_update(self, query: str, params: tuple) -> None:
        connection = self._connection_factory()
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def _position_row(position: NormalizedPosition, synced_at: datetime) -> tuple:
        return (
            position.account_alias,
            position.market_type.value,
            position.symbol,
            position.pair,
            position.contract_type,
            position.delivery_date,
            position.raw_base_asset,
            position.base_asset,
            position.quote_asset,
            position.margin_asset,
            position.contract_multiplier,
            position.contract_size_quote,
            position.position_side,
            position.position_amt,
            position.position_amt_unit,
            position.signed_base_exposure,
            position.entry_price,
            position.break_even_price,
            position.mark_price,
            position.unrealized_pnl,
            position.pnl_asset,
            position.notional_value,
            position.notional_asset,
            position.liquidation_price,
            position.isolated_margin,
            position.leverage,
            position.margin_type,
            position.binance_update_time,
            synced_at,
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
