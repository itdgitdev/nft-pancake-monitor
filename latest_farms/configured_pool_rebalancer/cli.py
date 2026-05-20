from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from .settings import load_worker_config
from .worker import ConfiguredPoolRebalancer
from .pnl_report import ConfiguredPoolPnlReporter


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configured multi-pool V3 LP rebalancer")
    parser.add_argument(
        "--config",
        default=os.getenv("CONFIGURED_REBALANCER_CONFIG", "latest_farms/configured_pool_rebalancer/sample_config.json"),
        help="Path to JSON config file",
    )
    parser.add_argument("--execute", action="store_true", help="Send transactions. Default is dry-run.")
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    config = load_worker_config(args.config, dry_run=not args.execute)
    if args.pnl_report:
        result = ConfiguredPoolPnlReporter(config).write_report(args.pnl_output_dir, args.pnl_format)
        print(json.dumps({"records": result.records, "written_files": result.written_files}, indent=2, sort_keys=True))
        return 0

    worker = ConfiguredPoolRebalancer(config, migrate=args.migrate)
    result = worker.run_once()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
