from __future__ import annotations

import logging

from .client import BinanceFuturesClient, BinanceMonitorError
from .models import MonitorConfig, RuntimeBinanceCredentials
from .repository import BinanceMonitorRepository


log = logging.getLogger("binance_futures_monitor")


class BinanceFuturesMonitor:
    def __init__(
        self,
        config: MonitorConfig,
        credentials: RuntimeBinanceCredentials,
        client: BinanceFuturesClient | None = None,
        repository: BinanceMonitorRepository | None = None,
    ):
        self.config = config
        self.credentials = credentials
        self.client = client or BinanceFuturesClient()
        self.repository = repository or BinanceMonitorRepository()

    def run_once(self) -> list[dict]:
        self.repository.sync_wallet_links(self.config)
        results = []
        for account in self.config.accounts:
            credentials = self.credentials.for_account(account.alias)
            for market in account.markets:
                result = {
                    "account_alias": account.alias,
                    "market_type": market.value,
                    "status": "FAILED",
                    "position_count": 0,
                }
                try:
                    with self.repository.account_market_lock(account.alias, market):
                        self.repository.mark_running(
                            account.alias, market, self.config.stale_after_seconds
                        )
                        positions = self.client.fetch_positions(
                            account.alias, market, credentials
                        )
                        self.repository.replace_positions_success(
                            account.alias,
                            market,
                            self.config.stale_after_seconds,
                            positions,
                        )
                    result.update(status="SUCCESS", position_count=len(positions))
                    log.info(
                        "sync success account=%s market=%s positions=%s",
                        account.alias,
                        market.value,
                        len(positions),
                    )
                except TimeoutError:
                    result["status"] = "SKIPPED"
                    result["error_code"] = "LOCK_BUSY"
                    log.warning(
                        "sync skipped because lock is busy account=%s market=%s",
                        account.alias,
                        market.value,
                    )
                except Exception as exc:
                    error_code, error_message = self._safe_error(exc, credentials)
                    try:
                        self.repository.mark_failed(
                            account.alias,
                            market,
                            self.config.stale_after_seconds,
                            error_code,
                            error_message,
                        )
                    except Exception:
                        log.exception(
                            "could not persist failed sync account=%s market=%s",
                            account.alias,
                            market.value,
                        )
                    result["error_code"] = error_code
                    log.error(
                        "sync failed account=%s market=%s code=%s message=%s",
                        account.alias,
                        market.value,
                        error_code,
                        error_message,
                    )
                results.append(result)
        return results

    @staticmethod
    def _safe_error(exc: Exception, credentials) -> tuple[str, str]:
        if isinstance(exc, BinanceMonitorError):
            code = exc.category if exc.code is None else f"{exc.category}:{exc.code}"
        else:
            code = exc.__class__.__name__.upper()
        message = str(exc) or exc.__class__.__name__
        for secret in (credentials.api_key, credentials.secret_key):
            if secret:
                message = message.replace(secret, "[REDACTED]")
        return code[:64], message[:500]
