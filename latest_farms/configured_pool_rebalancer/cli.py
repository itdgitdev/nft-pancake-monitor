from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Callable

from dotenv import load_dotenv
from web3 import Web3

from .models import WorkerConfig
from .settings import load_worker_config
from .signer import RuntimeSigner
from .worker import ConfiguredPoolRebalancer
from .pnl_report import ConfiguredPoolPnlReporter
from .automation_worker import ConfiguredPoolAutomationWorker

log = logging.getLogger("configured_pool_rebalancer")

PRIVATE_KEY_PREFIX_LENGTH = 54
PRIVATE_KEY_SUFFIX_LENGTH = 10
PRIVATE_KEY_HEX_LENGTH = 64
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configured multi-pool V3 LP rebalancer")
    parser.add_argument(
        "--config",
        default=os.getenv("CONFIGURED_REBALANCER_CONFIG", "latest_farms/configured_pool_rebalancer/sample_config.json"),
        help="Path to JSON config file",
    )
    parser.add_argument("--execute", action="store_true", help="Send transactions. Default is dry-run.")
    parser.add_argument("--loop", action="store_true", help="Run continuously every interval_seconds.")
    parser.add_argument("--migrate", action="store_true", help="Create journal tables before running.")
    parser.add_argument("--pnl-report", action="store_true", help="Generate PnL report only. Does not rebalance or migrate.")
    parser.add_argument("--pnl-output-dir", default="latest_farms/logs", help="Directory for PnL report files.")
    parser.add_argument("--pnl-format", choices=("json", "csv", "both"), default="both", help="PnL report output format.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    for logger_name in ("mysql", "mysql.connector", "mysql.connector.plugins"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def prompt_runtime_signer(config: WorkerConfig) -> RuntimeSigner | None:
    if config.dry_run:
        return None

    prefix_env_by_wallet: dict[str, str] = {}
    for pool in config.pools:
        wallet = Web3.to_checksum_address(pool.bot_wallet)
        env_name = str(pool.private_key_prefix_env or "").strip()
        if not env_name:
            raise ValueError(f"private_key_prefix_env is required for bot_wallet {wallet}")
        existing_env_name = prefix_env_by_wallet.get(wallet)
        if existing_env_name and existing_env_name != env_name:
            raise ValueError(
                f"conflicting private_key_prefix_env values for bot_wallet {wallet}: "
                f"{existing_env_name} and {env_name}"
            )
        prefix_env_by_wallet[wallet] = env_name

    prefix_by_wallet: dict[str, str] = {}
    for wallet, env_name in prefix_env_by_wallet.items():
        prefix = os.getenv(env_name, "").strip()
        if not prefix:
            raise ValueError(
                f"missing private key prefix in environment variable {env_name} "
                f"for bot_wallet {wallet}"
            )
        prefix_by_wallet[wallet] = _validate_private_key_segment(
            prefix,
            expected_length=PRIVATE_KEY_PREFIX_LENGTH,
            label=f"private key prefix from environment variable {env_name}",
            wallet=wallet,
        )

    private_keys_by_wallet: dict[str, str] = {}
    for wallet, prefix in prefix_by_wallet.items():
        suffix = _validate_private_key_segment(
            getpass.getpass(f"Last 10 private-key hex characters for bot_wallet {wallet}: ").strip(),
            expected_length=PRIVATE_KEY_SUFFIX_LENGTH,
            label="private key suffix",
            wallet=wallet,
        )
        private_key_hex = f"{prefix}{suffix}"
        if len(private_key_hex) != PRIVATE_KEY_HEX_LENGTH or not HEX_PATTERN.fullmatch(private_key_hex):
            raise ValueError(f"invalid reconstructed private key for bot_wallet {wallet}")
        private_keys_by_wallet[wallet] = f"0x{private_key_hex}"
    return RuntimeSigner(private_keys_by_wallet)


def _validate_private_key_segment(
    value: str,
    *,
    expected_length: int,
    label: str,
    wallet: str,
) -> str:
    if len(value) != expected_length or not HEX_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} for bot_wallet {wallet} must be exactly {expected_length} hexadecimal characters"
        )
    return value


def run_loop(
    worker: ConfiguredPoolRebalancer,
    config: WorkerConfig,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    time_fn: Callable[[], float] = time.time,
) -> int:
    cycle = 1
    interval_seconds = max(0, int(config.interval_seconds))
    while True:
        started_ts = time_fn()
        try:
            records = worker.run_once()
            status = "SUCCESS"
            error = None
        except KeyboardInterrupt:
            log.info("loop stopped by operator during cycle")
            return 0
        except Exception as exc:
            log.exception("loop cycle failed: %s", exc)
            records = []
            status = "ERROR"
            error = str(exc)

        finished_ts = time_fn()
        next_run_ts = started_ts + interval_seconds
        payload = {
            "cycle": cycle,
            "status": status,
            "started_at": _iso_utc(started_ts),
            "finished_at": _iso_utc(finished_ts),
            "next_run_at": _iso_utc(next_run_ts),
            "interval_seconds": interval_seconds,
            "records": records,
        }
        if error:
            payload["error"] = error
        log.info(
            "loop cycle finished cycle=%s status=%s next_run_at=%s interval_seconds=%s",
            cycle,
            status,
            payload["next_run_at"],
            interval_seconds,
        )
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)

        try:
            _sleep_until(next_run_ts, sleep_fn=sleep_fn, time_fn=time_fn)
        except KeyboardInterrupt:
            log.info("loop stopped by operator during sleep")
            return 0
        cycle += 1


def _sleep_until(
    target_ts: float,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    time_fn: Callable[[], float] = time.time,
) -> None:
    while True:
        remaining = target_ts - time_fn()
        if remaining <= 0:
            return
        sleep_fn(min(1.0, remaining))


def _iso_utc(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=False)
    args = parse_args(argv)
    configure_logging(args.log_level)
    config = load_worker_config(args.config, dry_run=not args.execute)
    if args.pnl_report:
        result = ConfiguredPoolPnlReporter(config).write_report(args.pnl_output_dir, args.pnl_format)
        print(json.dumps({"records": result.records, "written_files": result.written_files}, indent=2, sort_keys=True))
        return 0

    signer = prompt_runtime_signer(config)
    compound_enabled = any(pool.auto_compound.enabled for pool in config.pools)
    if compound_enabled:
        worker = ConfiguredPoolAutomationWorker(config, migrate=args.migrate, signer=signer)
    else:
        worker = ConfiguredPoolRebalancer(config, migrate=args.migrate, signer=signer)
    if args.loop:
        return run_loop(worker, config)
    result = worker.run_once()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
