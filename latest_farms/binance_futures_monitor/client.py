from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import requests

from .models import (
    BinanceCredentials,
    MarketType,
    NormalizedPosition,
    SymbolMetadata,
)


MARKET_ENDPOINTS = {
    MarketType.USD_M: {
        "base_url": "https://fapi.binance.com",
        "time": "/fapi/v1/time",
        "exchange_info": "/fapi/v1/exchangeInfo",
        "positions": "/fapi/v3/positionRisk",
    },
    MarketType.COIN_M: {
        "base_url": "https://dapi.binance.com",
        "time": "/dapi/v1/time",
        "exchange_info": "/dapi/v1/exchangeInfo",
        "positions": "/dapi/v1/positionRisk",
    },
}
MULTIPLIER_PREFIXES = ("1000000", "10000", "1000")


class BinanceMonitorError(RuntimeError):
    def __init__(self, category: str, message: str, code: str | int | None = None):
        super().__init__(message[:500])
        self.category = category
        self.code = str(code) if code is not None else None


def sign_query(secret_key: str, params: dict[str, Any]) -> str:
    payload = urlencode(params)
    signature = hmac.new(
        secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"{payload}&signature={signature}"


def _decimal(value: Any, field: str, *, required: bool = False) -> Decimal | None:
    if value in (None, ""):
        if required:
            raise BinanceMonitorError("PAYLOAD", f"missing {field}")
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BinanceMonitorError("PAYLOAD", f"invalid {field}") from exc


def _canonical_asset(raw_base_asset: str) -> tuple[str, Decimal]:
    normalized = raw_base_asset.upper().strip()
    for prefix in MULTIPLIER_PREFIXES:
        if normalized.startswith(prefix) and len(normalized) > len(prefix):
            return normalized[len(prefix):], Decimal(prefix)
    return normalized, Decimal(1)


def _effective_amount(position_side: str, amount: Decimal) -> Decimal:
    side = position_side.upper()
    if side == "LONG":
        return abs(amount)
    if side == "SHORT":
        return -abs(amount)
    if side == "BOTH":
        return amount
    raise BinanceMonitorError("PAYLOAD", f"unsupported position side {side}")


class BinanceFuturesClient:
    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()
        self._clock_offsets_ms = {market: 0 for market in MarketType}
        self._last_timestamps_ms = {market: 0 for market in MarketType}
        self._metadata: dict[MarketType, dict[str, SymbolMetadata]] = {}
        self._metadata_loaded_at: dict[MarketType, float] = {}

    def fetch_positions(
        self,
        account_alias: str,
        market: MarketType,
        credentials: BinanceCredentials,
    ) -> list[NormalizedPosition]:
        metadata = self._get_metadata(market)
        payload = self._signed_get(market, credentials)
        if not isinstance(payload, list):
            raise BinanceMonitorError("PAYLOAD", "position response must be an array")

        positions = []
        for item in payload:
            amount = _decimal(item.get("positionAmt"), "positionAmt", required=True)
            assert amount is not None
            if amount == 0:
                continue
            symbol = str(item.get("symbol") or "").upper()
            symbol_metadata = metadata.get(symbol)
            if symbol_metadata is None:
                raise BinanceMonitorError("METADATA", f"missing metadata for symbol {symbol}")
            positions.append(
                self._normalize_position(account_alias, market, item, amount, symbol_metadata)
            )
        return positions

    def _normalize_position(
        self,
        account_alias: str,
        market: MarketType,
        item: dict[str, Any],
        amount: Decimal,
        metadata: SymbolMetadata,
    ) -> NormalizedPosition:
        side = str(item.get("positionSide") or "BOTH").upper()
        effective_amount = _effective_amount(side, amount)
        mark_price = _decimal(item.get("markPrice"), "markPrice", required=True)
        assert mark_price is not None

        if market is MarketType.COIN_M:
            if metadata.contract_size_quote is None or metadata.contract_size_quote <= 0:
                raise BinanceMonitorError("METADATA", f"missing contractSize for {metadata.symbol}")
            if mark_price <= 0:
                raise BinanceMonitorError("PAYLOAD", f"invalid markPrice for {metadata.symbol}")
            signed_base_exposure = (
                effective_amount
                * metadata.contract_size_quote
                / mark_price
                * metadata.contract_multiplier
            )
            position_amt_unit = "CONTRACT"
            notional_value = effective_amount * metadata.contract_size_quote
        else:
            signed_base_exposure = effective_amount * metadata.contract_multiplier
            position_amt_unit = "BASE_ASSET"
            notional_value = _decimal(item.get("notional"), "notional")

        leverage_raw = item.get("leverage")
        leverage = int(leverage_raw) if leverage_raw not in (None, "") else None
        margin_type = str(item.get("marginType") or "").lower() or None
        pnl_asset = metadata.margin_asset or metadata.quote_asset

        return NormalizedPosition(
            account_alias=account_alias,
            market_type=market,
            symbol=metadata.symbol,
            pair=metadata.pair,
            contract_type=metadata.contract_type,
            delivery_date=metadata.delivery_date,
            raw_base_asset=metadata.raw_base_asset,
            base_asset=metadata.base_asset,
            quote_asset=metadata.quote_asset,
            margin_asset=metadata.margin_asset,
            contract_multiplier=metadata.contract_multiplier,
            contract_size_quote=metadata.contract_size_quote,
            position_side=side,
            position_amt=amount,
            position_amt_unit=position_amt_unit,
            signed_base_exposure=signed_base_exposure,
            entry_price=_decimal(item.get("entryPrice"), "entryPrice"),
            break_even_price=_decimal(item.get("breakEvenPrice"), "breakEvenPrice"),
            mark_price=mark_price,
            unrealized_pnl=_decimal(item.get("unRealizedProfit"), "unRealizedProfit"),
            pnl_asset=pnl_asset,
            notional_value=notional_value,
            notional_asset=metadata.quote_asset,
            liquidation_price=_decimal(item.get("liquidationPrice"), "liquidationPrice"),
            isolated_margin=_decimal(item.get("isolatedMargin"), "isolatedMargin"),
            leverage=leverage,
            margin_type=margin_type,
            binance_update_time=(
                int(item["updateTime"]) if item.get("updateTime") not in (None, "") else None
            ),
        )

    def _signed_get(
        self,
        market: MarketType,
        credentials: BinanceCredentials,
    ) -> Any:
        endpoints = MARKET_ENDPOINTS[market]
        clock_retry_used = False
        transient_failures = 0
        while True:
            params = {
                "timestamp": self._next_timestamp_ms(market),
                "recvWindow": 5000,
            }
            query = sign_query(credentials.secret_key, params)
            try:
                response = self._session.get(
                    f"{endpoints['base_url']}{endpoints['positions']}?{query}",
                    headers={"X-MBX-APIKEY": credentials.api_key},
                    timeout=(3.05, 10),
                )
            except requests.RequestException as exc:
                transient_failures += 1
                if transient_failures > 2:
                    raise BinanceMonitorError("NETWORK", "Binance position request failed") from exc
                continue

            if response.status_code >= 500:
                transient_failures += 1
                if transient_failures <= 2:
                    continue
                raise BinanceMonitorError("BINANCE_5XX", "Binance service error")
            if response.status_code in (418, 429):
                raise BinanceMonitorError("RATE_LIMIT", "Binance rate limit reached")
            if response.status_code in (401, 403):
                raise BinanceMonitorError(
                    "AUTH_OR_IP", "Binance credential, permission, or IP validation failed"
                )
            data = self._response_json(response)
            code = data.get("code") if isinstance(data, dict) else None
            if code == -1021 and not clock_retry_used:
                self._sync_clock(market)
                clock_retry_used = True
                continue
            if code == -2015:
                raise BinanceMonitorError(
                    "AUTH_OR_IP", "Binance credential, permission, or IP validation failed", code
                )
            if response.status_code >= 400 or (isinstance(code, int) and code < 0):
                message = str(data.get("msg") or "Binance request rejected")
                raise BinanceMonitorError("BINANCE_API", message, code)
            return data

    def _get_metadata(self, market: MarketType) -> dict[str, SymbolMetadata]:
        loaded_at = self._metadata_loaded_at.get(market, 0)
        if market in self._metadata and time.time() - loaded_at < 21600:
            return self._metadata[market]

        endpoints = MARKET_ENDPOINTS[market]
        try:
            payload = self._public_get_json(
                f"{endpoints['base_url']}{endpoints['exchange_info']}",
                "Binance exchangeInfo request failed",
            )
            if not isinstance(payload, dict):
                raise BinanceMonitorError("METADATA", "Binance exchangeInfo request failed")
            metadata = self._parse_metadata(payload)
            if not metadata:
                raise BinanceMonitorError("METADATA", "Binance exchangeInfo returned no symbols")
        except Exception:
            if market in self._metadata:
                return self._metadata[market]
            raise

        self._metadata[market] = metadata
        self._metadata_loaded_at[market] = time.time()
        return metadata

    @staticmethod
    def _parse_metadata(payload: dict[str, Any]) -> dict[str, SymbolMetadata]:
        result = {}
        for item in payload.get("symbols", []):
            symbol = str(item.get("symbol") or "").upper()
            raw_base_asset = str(item.get("baseAsset") or "").upper()
            if not symbol or not raw_base_asset:
                continue
            base_asset, multiplier = _canonical_asset(raw_base_asset)
            contract_size = _decimal(item.get("contractSize"), "contractSize")
            delivery_raw = item.get("deliveryDate")
            result[symbol] = SymbolMetadata(
                symbol=symbol,
                pair=str(item.get("pair") or symbol).upper(),
                contract_type=str(item.get("contractType") or "").upper(),
                delivery_date=int(delivery_raw) if delivery_raw not in (None, "", 0) else None,
                raw_base_asset=raw_base_asset,
                base_asset=base_asset,
                quote_asset=str(item.get("quoteAsset") or "").upper(),
                margin_asset=str(item.get("marginAsset") or "").upper(),
                contract_multiplier=multiplier,
                contract_size_quote=contract_size,
            )
        return result

    def _sync_clock(self, market: MarketType) -> None:
        endpoints = MARKET_ENDPOINTS[market]
        try:
            payload = self._public_get_json(
                f"{endpoints['base_url']}{endpoints['time']}",
                "could not synchronize Binance server time",
            )
            server_time = int(payload["serverTime"])
        except Exception as exc:
            raise BinanceMonitorError("CLOCK", "could not synchronize Binance server time") from exc
        self._clock_offsets_ms[market] = server_time - int(time.time() * 1000)
        self._last_timestamps_ms[market] = 0

    def _next_timestamp_ms(self, market: MarketType) -> int:
        timestamp = int(time.time() * 1000) + self._clock_offsets_ms[market]
        timestamp = max(timestamp, self._last_timestamps_ms[market] + 1)
        self._last_timestamps_ms[market] = timestamp
        return timestamp

    def _public_get_json(self, url: str, error_message: str) -> Any:
        transient_failures = 0
        while True:
            try:
                response = self._session.get(url, timeout=(3.05, 10))
            except requests.RequestException as exc:
                transient_failures += 1
                if transient_failures > 2:
                    raise BinanceMonitorError("NETWORK", error_message) from exc
                continue
            if response.status_code >= 500:
                transient_failures += 1
                if transient_failures <= 2:
                    continue
                raise BinanceMonitorError("BINANCE_5XX", error_message)
            if response.status_code in (418, 429):
                raise BinanceMonitorError("RATE_LIMIT", "Binance rate limit reached")
            if response.status_code >= 400:
                raise BinanceMonitorError("BINANCE_API", error_message)
            return self._response_json(response)

    @staticmethod
    def _response_json(response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise BinanceMonitorError("PAYLOAD", "Binance returned invalid JSON") from exc
