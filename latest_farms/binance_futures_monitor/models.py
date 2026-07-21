from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Mapping


class MarketType(str, Enum):
    USD_M = "USD_M"
    COIN_M = "COIN_M"


@dataclass(frozen=True)
class WalletLink:
    wallet_type: str
    wallet_address: str


@dataclass(frozen=True)
class AccountConfig:
    alias: str
    markets: tuple[MarketType, ...]
    linked_wallets: tuple[WalletLink, ...]


@dataclass(frozen=True)
class MonitorConfig:
    accounts: tuple[AccountConfig, ...]
    interval_seconds: int = 60
    stale_after_seconds: int = 180


@dataclass(frozen=True, repr=False)
class BinanceCredentials:
    api_key: str
    secret_key: str

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("Binance API key cannot be empty")
        if not self.secret_key.strip():
            raise ValueError("Binance secret key cannot be empty")


@dataclass(frozen=True, repr=False)
class PartialBinanceCredentials:
    api_key: str
    secret_key_prefix: str

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("Binance API key cannot be empty")
        if not self.secret_key_prefix:
            raise ValueError("Binance secret key prefix cannot be empty")


class RuntimeBinanceCredentials:
    def __init__(self, credentials_by_alias: Mapping[str, BinanceCredentials]):
        self._credentials = dict(credentials_by_alias)

    def for_account(self, account_alias: str) -> BinanceCredentials:
        try:
            return self._credentials[account_alias]
        except KeyError as exc:
            raise RuntimeError(f"missing runtime credentials for account {account_alias}") from exc

    def __repr__(self) -> str:
        aliases = ", ".join(sorted(self._credentials))
        return f"RuntimeBinanceCredentials(accounts=[{aliases}])"


@dataclass(frozen=True)
class SymbolMetadata:
    symbol: str
    pair: str
    contract_type: str
    delivery_date: int | None
    raw_base_asset: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    contract_multiplier: Decimal
    contract_size_quote: Decimal | None


@dataclass(frozen=True)
class NormalizedPosition:
    account_alias: str
    market_type: MarketType
    symbol: str
    pair: str
    contract_type: str
    delivery_date: int | None
    raw_base_asset: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    contract_multiplier: Decimal
    contract_size_quote: Decimal | None
    position_side: str
    position_amt: Decimal
    position_amt_unit: str
    signed_base_exposure: Decimal
    entry_price: Decimal | None
    break_even_price: Decimal | None
    mark_price: Decimal
    unrealized_pnl: Decimal | None
    pnl_asset: str
    notional_value: Decimal | None
    notional_asset: str
    liquidation_price: Decimal | None
    isolated_margin: Decimal | None
    leverage: int | None
    margin_type: str | None
    binance_update_time: int | None
