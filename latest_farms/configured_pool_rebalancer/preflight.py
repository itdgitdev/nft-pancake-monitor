from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

from .evm import get_chain_id, web3_connection
from .models import WorkerConfig
from .settings import load_worker_config


PREFIX_LENGTH = 54
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")
REQUIRED_JOURNAL_TABLES = (
    "configured_rebalance_jobs",
    "configured_compound_jobs",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only configured rebalancer preflight")
    parser.add_argument("--project-root", default=".", help="Project root containing .env")
    parser.add_argument("--config", default="my_rebalance_config.json", help="Config JSON path")
    return parser.parse_args(argv)


def run_preflight(
    project_root: str | os.PathLike[str],
    config_path: str | os.PathLike[str],
    *,
    output: Callable[[str], None] = print,
) -> bool:
    root = Path(project_root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root / config_file

    ok = True
    env_file = root / ".env"
    if env_file.is_file():
        output("[PASS] .env found")
        load_dotenv(env_file, override=False)
    else:
        output("[FAIL] .env not found in project root (check that it is not named .env.txt)")
        ok = False

    config = None
    raw_config = None
    if not config_file.is_file():
        output(f"[FAIL] Config file not found: {config_file.name}")
        ok = False
    else:
        try:
            with config_file.open("r", encoding="utf-8") as handle:
                raw_config = json.load(handle)
            placeholders = sorted(_find_placeholders(raw_config))
            if placeholders:
                output(f"[FAIL] Replace all config placeholders: {', '.join(placeholders)}")
                ok = False
            else:
                output("[PASS] JSON syntax and placeholders")
        except (OSError, json.JSONDecodeError) as exc:
            output(f"[FAIL] JSON syntax: {exc}")
            ok = False

    if raw_config is not None and not _find_placeholders(raw_config):
        try:
            config = load_worker_config(config_file, dry_run=True)
            output(f"[PASS] Configured pools: {len(config.pools)}")
            output(f"[PASS] Wallet address: {_wallet_summary(config)}")
        except Exception as exc:
            output(f"[FAIL] Config validation: {exc}")
            ok = False

    try:
        connection = _open_db_connection()
        try:
            output("[PASS] DB connection")
            cursor = connection.cursor()
            try:
                for table in REQUIRED_JOURNAL_TABLES:
                    cursor.execute("SHOW TABLES LIKE %s", (table,))
                    if cursor.fetchone():
                        output(f"[PASS] Journal table: {table}")
                    else:
                        output(f"[FAIL] Journal table missing: {table}; contact the administrator")
                        ok = False
            finally:
                cursor.close()
        finally:
            connection.close()
    except Exception as exc:
        output(f"[FAIL] DB connection: {exc}")
        ok = False

    if config is not None:
        prefix_errors = _prefix_errors(config)
        if prefix_errors:
            for error in prefix_errors:
                output(f"[FAIL] {error}")
            ok = False
        else:
            output("[PASS] Private-key prefix length for all wallets")

        for chain in sorted({pool.chain for pool in config.pools}):
            try:
                w3 = web3_connection(chain)
                actual_chain_id = int(w3.eth.chain_id)
                expected_chain_id = get_chain_id(chain)
                if actual_chain_id != expected_chain_id:
                    output(
                        f"[FAIL] RPC chain ID for {chain}: "
                        f"expected {expected_chain_id}, received {actual_chain_id}"
                    )
                    ok = False
                else:
                    output(f"[PASS] RPC connection and chain ID: {chain} ({actual_chain_id})")
            except Exception as exc:
                output(f"[FAIL] RPC connection for {chain}: {exc}")
                ok = False

    output("READY FOR LIVE RUN" if ok else "NOT READY - FIX ALL [FAIL] ITEMS")
    return ok


def _find_placeholders(value) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for item in value.values():
            found.update(_find_placeholders(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_find_placeholders(item))
    elif isinstance(value, str):
        normalized = value.upper()
        if "YOUR_" in normalized or "REPLACE_" in normalized:
            found.add(value)
    return found


def _prefix_errors(config: WorkerConfig) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    env_by_wallet: dict[str, str] = {}
    for pool in config.pools:
        key = (pool.bot_wallet.lower(), str(pool.private_key_prefix_env or ""))
        if key in seen:
            continue
        seen.add(key)
        env_name = key[1].strip()
        previous_env = env_by_wallet.get(key[0])
        if previous_env is not None and previous_env != env_name:
            errors.append(
                f"wallet {pool.bot_wallet} uses conflicting private-key prefix variables: "
                f"{previous_env} and {env_name}"
            )
            continue
        env_by_wallet[key[0]] = env_name
        if not env_name:
            errors.append(f"private_key_prefix_env is missing for wallet {pool.bot_wallet}")
            continue
        value = os.getenv(env_name, "").strip()
        if len(value) != PREFIX_LENGTH or not HEX_PATTERN.fullmatch(value):
            errors.append(
                f"{env_name} must contain exactly {PREFIX_LENGTH} hexadecimal characters "
                f"for wallet {pool.bot_wallet}"
            )
    return errors


def _wallet_summary(config: WorkerConfig) -> str:
    wallets = sorted({pool.bot_wallet for pool in config.pools})
    return ", ".join(wallets)


def _open_db_connection():
    from latest_farms.create_db import get_connection

    return get_connection()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return 0 if run_preflight(args.project_root, args.config) else 1


if __name__ == "__main__":
    raise SystemExit(main())
