from __future__ import annotations

import getpass
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from dotenv import dotenv_values

from .models import (
    AccountConfig,
    BinanceCredentials,
    PartialBinanceCredentials,
    RuntimeBinanceCredentials,
)


SECRET_SUFFIX_LENGTH = 10


def credential_env_alias(account_alias: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", account_alias.upper()).strip("_")


def load_partial_credentials(
    path: str | Path,
    accounts: Sequence[AccountConfig],
) -> dict[str, PartialBinanceCredentials]:
    credentials_path = Path(path)
    if not credentials_path.is_file():
        raise FileNotFoundError(
            f"Binance credentials env file not found: {credentials_path}"
        )

    values = dotenv_values(credentials_path, interpolate=False)
    aliases_by_env: dict[str, str] = {}
    credentials: dict[str, PartialBinanceCredentials] = {}

    for account in accounts:
        env_alias = credential_env_alias(account.alias)
        if not env_alias:
            raise ValueError(
                f"account alias {account.alias!r} cannot form credential variable names"
            )
        existing_alias = aliases_by_env.get(env_alias)
        if existing_alias is not None:
            raise ValueError(
                "credential environment alias collision between "
                f"{existing_alias!r} and {account.alias!r}"
            )
        aliases_by_env[env_alias] = account.alias

        api_key_name = f"BINANCE_{env_alias}_API_KEY"
        secret_prefix_name = f"BINANCE_{env_alias}_SECRET_PREFIX"
        api_key = values.get(api_key_name)
        secret_key_prefix = values.get(secret_prefix_name)
        if not isinstance(api_key, str) or not api_key:
            raise ValueError(
                f"missing {api_key_name} for Binance account {account.alias!r}"
            )
        if not isinstance(secret_key_prefix, str) or not secret_key_prefix:
            raise ValueError(
                f"missing {secret_prefix_name} for Binance account {account.alias!r}"
            )
        credentials[account.alias] = PartialBinanceCredentials(
            api_key=api_key,
            secret_key_prefix=secret_key_prefix,
        )

    return credentials


def prompt_runtime_credentials(
    partial_credentials: Mapping[str, PartialBinanceCredentials],
    prompt: Callable[[str], str] | None = None,
) -> RuntimeBinanceCredentials:
    prompt_fn = prompt or getpass.getpass
    credentials = {}
    for account_alias, partial in partial_credentials.items():
        suffix = prompt_fn(
            f"Last {SECRET_SUFFIX_LENGTH} characters of Binance secret "
            f"for {account_alias}: "
        )
        if len(suffix) != SECRET_SUFFIX_LENGTH:
            raise ValueError(
                f"Binance secret suffix for {account_alias!r} must contain exactly "
                f"{SECRET_SUFFIX_LENGTH} characters"
            )
        credentials[account_alias] = BinanceCredentials(
            api_key=partial.api_key,
            secret_key=partial.secret_key_prefix + suffix,
        )
    return RuntimeBinanceCredentials(credentials)
