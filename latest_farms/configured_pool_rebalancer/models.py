from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DexType(str, Enum):
    PANCAKE_V3_MASTERCHEF = "pancake_v3_masterchef"
    AERODROME_GAUGE = "aerodrome_gauge"


class PositionState(str, Enum):
    IN_RANGE = "IN_RANGE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    PLANNED = "PLANNED"
    WITHDRAWN_UNBURNED = "WITHDRAWN_UNBURNED"
    SWAP_PENDING = "SWAP_PENDING"
    SWAP_BLOCKED = "SWAP_BLOCKED"
    MINTED_UNSTAKED = "MINTED_UNSTAKED"
    REMINTED = "REMINTED"
    BURNED = "BURNED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class GasPolicy:
    mode: str = "auto"
    gas_price_gwei: float | None = None
    base_fee_multiplier: float = 1.5
    priority_fee_cap_gwei: float | None = None
    swap_priority_fee_cap_gwei: float | None = None
    swap_priority_fee_floor_gwei: float | None = None
    max_fee_gwei: float | None = None


@dataclass(frozen=True)
class PoolConfig:
    name: str
    chain: str
    pool_address: str
    dex_type: DexType
    managed_wallets: tuple[str, ...]
    bot_wallet: str
    private_key_env: str = "PARASITE_BOT_PRIVATE_KEY"
    token0_address: str | None = None
    token1_address: str | None = None
    token0_decimals: int | None = None
    token1_decimals: int | None = None
    fee: int | None = None
    tick_spacing: int | None = None
    pid: int | None = None
    npm_address: str | None = None
    staking_address: str | None = None
    start_block: int = 0
    bootstrap_start_block: int | None = None
    auto_bootstrap_start_block: bool = True
    seed_token_ids: tuple[int, ...] = ()
    slippage_bps: int = 50
    max_gas_gwei: float = 10.0
    max_swap_price_impact_pct: float = 1.0
    min_swap_input_usd: float = 0.25
    min_swap_output_usd: float = 0.10
    min_swap_recovered_pct: float = 0.005
    deadline_seconds: int = 300
    log_chunk_size: int = 5000
    max_jobs_per_cycle: int = 1
    execute_burn: bool = True
    rebalance_range_mode: str | None = None
    rebalance_range_lower_percent: float | None = None
    rebalance_range_upper_percent: float | None = None


@dataclass(frozen=True)
class WorkerConfig:
    pools: tuple[PoolConfig, ...]
    interval_seconds: int = 1800
    cache_dir: str = "latest_farms/configured_pool_rebalancer/cache"
    legacy_position_cache_dir: str = "latest_farms/positions_cache"
    use_legacy_position_cache: bool = True
    lock_timeout_seconds: int = 60
    dry_run: bool = True
    pnl_native_prices_usd: dict[str, float] = field(default_factory=dict)
    discord_enabled: bool = False
    discord_webhook_url_env: str = "CONFIGURED_REBALANCER_DISCORD_WEBHOOK"
    discord_pnl_delay_seconds: int = 90
    discord_notify_pending_if_snapshot_missing: bool = False
    gas_policies: dict[str, GasPolicy] = field(default_factory=dict)


@dataclass
class Slot0:
    sqrt_price_x96: int
    tick: int


@dataclass
class TokenBalance:
    raw: int
    decimals: int

    @property
    def human(self) -> float:
        return self.raw / (10**self.decimals)


@dataclass
class PositionSnapshot:
    token_id: int
    owner: str
    pool_address: str
    token0: str
    token1: str
    fee: int
    tick_lower: int
    tick_upper: int
    liquidity: int
    tokens_owed0: int = 0
    tokens_owed1: int = 0
    pid: int | None = None
    is_staked: bool = False
    last_updated_block: int | None = None

    @property
    def width(self) -> int:
        return self.tick_upper - self.tick_lower


@dataclass
class RebalancePlan:
    old_token_id: int
    current_tick: int
    old_tick_lower: int
    old_tick_upper: int
    new_tick_lower: int
    new_tick_upper: int
    amount0_desired: int
    amount1_desired: int
    swap_token_in: str | None = None
    swap_token_out: str | None = None
    swap_amount_in: int = 0
    reason: str = "out_of_range"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TxResult:
    tx_hash: str
    gas_used: int = 0
    gas_price_gwei: float = 0.0
    status: str = "SUCCESS"
    dry_run: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
