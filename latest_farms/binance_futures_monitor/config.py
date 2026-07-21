from __future__ import annotations

import json
from pathlib import Path

from solders.pubkey import Pubkey
from web3 import Web3

from .models import AccountConfig, MarketType, MonitorConfig, WalletLink


def _wallet_link(value: object) -> WalletLink:
    address = str(value or "").strip()
    if Web3.is_address(address):
        return WalletLink("EVM", address.lower())
    try:
        return WalletLink("SOLANA", str(Pubkey.from_string(address)))
    except Exception as exc:
        raise ValueError(f"invalid linked wallet {address!r}") from exc


def load_monitor_config(path: str | Path) -> MonitorConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    forbidden_keys = {
        "api_key",
        "apikey",
        "secret_key",
        "secretkey",
        "api_secret",
        "secret_prefix",
        "secret_key_prefix",
    }
    for account in raw.get("accounts", []):
        if isinstance(account, dict):
            normalized_keys = {str(key).lower() for key in account}
            if normalized_keys & forbidden_keys:
                raise ValueError("Binance credentials are not allowed in monitor config")

    if raw.get("version", 1) != 1:
        raise ValueError("config.version must be 1")

    interval_seconds = int(raw.get("interval_seconds", 60))
    stale_after_seconds = int(raw.get("stale_after_seconds", 180))
    if interval_seconds < 1:
        raise ValueError("interval_seconds must be at least 1")
    if stale_after_seconds < 1:
        raise ValueError("stale_after_seconds must be at least 1")

    raw_accounts = raw.get("accounts")
    if not isinstance(raw_accounts, list) or not raw_accounts:
        raise ValueError("config.accounts must be a non-empty array")

    aliases: set[str] = set()
    accounts = []
    for index, item in enumerate(raw_accounts):
        if not isinstance(item, dict):
            raise ValueError(f"accounts[{index}] must be an object")
        alias = str(item.get("alias") or "").strip()
        if not alias:
            raise ValueError(f"accounts[{index}].alias is required")
        if len(alias) > 64:
            raise ValueError(f"account alias {alias!r} exceeds 64 characters")
        if alias in aliases:
            raise ValueError(f"duplicate account alias {alias!r}")
        aliases.add(alias)

        raw_markets = item.get("markets")
        if not isinstance(raw_markets, list) or not raw_markets:
            raise ValueError(f"account {alias} must define at least one market")
        try:
            markets = tuple(dict.fromkeys(MarketType(str(value).upper()) for value in raw_markets))
        except ValueError as exc:
            raise ValueError(f"account {alias} has unsupported market") from exc

        raw_wallets = item.get("linked_wallets")
        if not isinstance(raw_wallets, list) or not raw_wallets:
            raise ValueError(f"account {alias} must define at least one linked wallet")
        wallets = tuple(dict.fromkeys(_wallet_link(value) for value in raw_wallets))
        accounts.append(AccountConfig(alias, markets, wallets))

    return MonitorConfig(tuple(accounts), interval_seconds, stale_after_seconds)
