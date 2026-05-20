from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from web3 import Web3

from .models import DexType, GasPolicy, PoolConfig, WorkerConfig


def _checksum_or_none(value: str | None) -> str | None:
    if not value:
        return None
    return Web3.to_checksum_address(value)


def _wallet_tuple(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    return tuple(Web3.to_checksum_address(v) for v in values if v)


def _dict_or_empty(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _optional_non_negative_int(raw: Any, label: str) -> int | None:
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer") from exc
    if value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _bool_from_config(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return bool(raw)


def _expand_pool_dict(raw_config: dict[str, Any], raw_pool: dict[str, Any]) -> dict[str, Any]:
    pool_defaults = _dict_or_empty(raw_config.get("pool_defaults"), "pool_defaults")
    wallets = _dict_or_empty(raw_config.get("wallets"), "wallets")
    merged = {**pool_defaults, **raw_pool}

    wallet_alias = merged.pop("wallet", None)
    if wallet_alias:
        wallet_key = str(wallet_alias)
        wallet_config = wallets.get(wallet_key)
        if wallet_config is None:
            pool_name = merged.get("name") or merged.get("pool_address") or "<unknown>"
            raise ValueError(f"pool {pool_name} references unknown wallet alias {wallet_key!r}")
        wallet_config = _dict_or_empty(wallet_config, f"wallets.{wallet_key}")
        merged = {**wallet_config, **merged}

    if merged.get("bot_wallet") and not merged.get("managed_wallets"):
        merged["managed_wallets"] = [merged["bot_wallet"]]

    return merged


def _pool_from_dict(raw: dict[str, Any]) -> PoolConfig:
    rebalance_range = raw.get("rebalance_range")
    range_mode = None
    lower_percent = None
    upper_percent = None
    if rebalance_range is not None:
        if not isinstance(rebalance_range, dict):
            raise ValueError("rebalance_range must be an object")
        range_mode = str(rebalance_range.get("mode") or "")
        if range_mode != "price_percent":
            raise ValueError("rebalance_range.mode must be 'price_percent'")
        if "lower_percent" not in rebalance_range or "upper_percent" not in rebalance_range:
            raise ValueError("rebalance_range must define lower_percent and upper_percent")
        lower_percent = float(rebalance_range["lower_percent"])
        upper_percent = float(rebalance_range["upper_percent"])
        if lower_percent >= 0:
            raise ValueError("rebalance_range.lower_percent must be negative")
        if lower_percent <= -100:
            raise ValueError("rebalance_range.lower_percent must be greater than -100")
        if upper_percent <= 0:
            raise ValueError("rebalance_range.upper_percent must be positive")

    return PoolConfig(
        name=raw.get("name") or raw["pool_address"],
        chain=str(raw["chain"]).upper(),
        pool_address=Web3.to_checksum_address(raw["pool_address"]),
        dex_type=DexType(raw.get("dex_type", DexType.PANCAKE_V3_MASTERCHEF.value)),
        managed_wallets=_wallet_tuple(raw.get("managed_wallets", [])),
        bot_wallet=Web3.to_checksum_address(raw["bot_wallet"]),
        private_key_env=raw.get("private_key_env", "PARASITE_BOT_PRIVATE_KEY"),
        token0_address=_checksum_or_none(raw.get("token0_address")),
        token1_address=_checksum_or_none(raw.get("token1_address")),
        token0_decimals=raw.get("token0_decimals"),
        token1_decimals=raw.get("token1_decimals"),
        fee=raw.get("fee"),
        tick_spacing=raw.get("tick_spacing"),
        pid=raw.get("pid"),
        npm_address=_checksum_or_none(raw.get("npm_address")),
        staking_address=_checksum_or_none(raw.get("staking_address")),
        start_block=int(raw.get("start_block", 0)),
        bootstrap_start_block=_optional_non_negative_int(
            raw.get("bootstrap_start_block"),
            f"pool {raw.get('name') or raw.get('pool_address') or '<unknown>'}.bootstrap_start_block",
        ),
        auto_bootstrap_start_block=_bool_from_config(raw.get("auto_bootstrap_start_block"), True),
        seed_token_ids=tuple(int(x) for x in raw.get("seed_token_ids", [])),
        slippage_bps=int(raw.get("slippage_bps", 50)),
        max_gas_gwei=float(raw.get("max_gas_gwei", 10.0)),
        max_swap_price_impact_pct=float(raw.get("max_swap_price_impact_pct", 1.0)),
        min_swap_input_usd=float(raw.get("min_swap_input_usd", 0.25)),
        min_swap_output_usd=float(raw.get("min_swap_output_usd", 0.10)),
        min_swap_recovered_pct=float(raw.get("min_swap_recovered_pct", 0.005)),
        deadline_seconds=int(raw.get("deadline_seconds", 300)),
        log_chunk_size=int(raw.get("log_chunk_size", 5000)),
        max_jobs_per_cycle=int(raw.get("max_jobs_per_cycle", 1)),
        execute_burn=bool(raw.get("execute_burn", True)),
        rebalance_range_mode=range_mode,
        rebalance_range_lower_percent=lower_percent,
        rebalance_range_upper_percent=upper_percent,
    )


def _gas_policy_from_dict(raw: dict[str, Any]) -> GasPolicy:
    return GasPolicy(
        mode=str(raw.get("mode", "auto")).lower(),
        gas_price_gwei=float(raw["gas_price_gwei"]) if raw.get("gas_price_gwei") is not None else None,
        base_fee_multiplier=float(raw.get("base_fee_multiplier", 1.5)),
        priority_fee_cap_gwei=(
            float(raw["priority_fee_cap_gwei"]) if raw.get("priority_fee_cap_gwei") is not None else None
        ),
        swap_priority_fee_cap_gwei=(
            float(raw["swap_priority_fee_cap_gwei"])
            if raw.get("swap_priority_fee_cap_gwei") is not None
            else None
        ),
        swap_priority_fee_floor_gwei=(
            float(raw["swap_priority_fee_floor_gwei"])
            if raw.get("swap_priority_fee_floor_gwei") is not None
            else None
        ),
        max_fee_gwei=float(raw["max_fee_gwei"]) if raw.get("max_fee_gwei") is not None else None,
    )


def load_worker_config(path: str | os.PathLike[str], dry_run: bool | None = None) -> WorkerConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    raw_pools = raw.get("pools", [])
    if not isinstance(raw_pools, list):
        raise ValueError("pools must be an array")
    expanded_pools = []
    for index, pool in enumerate(raw_pools):
        if not isinstance(pool, dict):
            raise ValueError(f"pools[{index}] must be an object")
        expanded_pools.append(_expand_pool_dict(raw, pool))

    pools = tuple(_pool_from_dict(pool) for pool in expanded_pools)
    if not pools:
        raise ValueError("config must define at least one pool")
    for pool in pools:
        if not pool.managed_wallets:
            raise ValueError(f"pool {pool.name} must define managed_wallets")

    pnl_config = raw.get("pnl", {}) or {}
    native_prices = {
        str(chain).upper(): float(price)
        for chain, price in (pnl_config.get("native_prices_usd", {}) or {}).items()
        if price is not None
    }
    discord_config = raw.get("discord", {}) or {}
    gas_policies = {
        str(chain).upper(): _gas_policy_from_dict(policy or {})
        for chain, policy in (raw.get("gas_policy", {}) or {}).items()
    }

    return WorkerConfig(
        pools=pools,
        interval_seconds=int(raw.get("interval_seconds", 1800)),
        cache_dir=raw.get("cache_dir", "latest_farms/configured_pool_rebalancer/cache"),
        legacy_position_cache_dir=raw.get("legacy_position_cache_dir", "latest_farms/positions_cache"),
        use_legacy_position_cache=bool(raw.get("use_legacy_position_cache", True)),
        lock_timeout_seconds=int(raw.get("lock_timeout_seconds", 60)),
        dry_run=bool(raw.get("dry_run", True) if dry_run is None else dry_run),
        pnl_native_prices_usd=native_prices,
        discord_enabled=bool(discord_config.get("enabled", False)),
        discord_webhook_url_env=str(discord_config.get("webhook_url_env", "CONFIGURED_REBALANCER_DISCORD_WEBHOOK")),
        discord_pnl_delay_seconds=int(discord_config.get("pnl_delay_seconds", 90)),
        discord_notify_pending_if_snapshot_missing=bool(
            discord_config.get("notify_pending_if_snapshot_missing", False)
        ),
        gas_policies=gas_policies,
    )
