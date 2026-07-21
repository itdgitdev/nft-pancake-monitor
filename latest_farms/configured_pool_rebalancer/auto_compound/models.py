from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ..models import PositionSnapshot


class CompoundJobState(str, Enum):
    PREPARED = "PREPARED"
    COLLECT_PENDING = "COLLECT_PENDING"
    COLLECTED = "COLLECTED"
    SWAP_APPROVAL_PENDING = "SWAP_APPROVAL_PENDING"
    SWAP_PENDING = "SWAP_PENDING"
    SWAPPED = "SWAPPED"
    INCREASE_APPROVAL_PENDING = "INCREASE_APPROVAL_PENDING"
    INCREASE_PENDING = "INCREASE_PENDING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    WAITING_FOR_SWAP = "WAITING_FOR_SWAP"
    WAITING_FOR_REBALANCE = "WAITING_FOR_REBALANCE"
    SUPERSEDED_BY_REBALANCE = "SUPERSEDED_BY_REBALANCE"
    REVERTED = "REVERTED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    MANUAL_RECOVERY = "MANUAL_RECOVERY"


TERMINAL_STATES = {
    CompoundJobState.COMPLETED,
    CompoundJobState.CANCELLED,
    CompoundJobState.SUPERSEDED_BY_REBALANCE,
    CompoundJobState.MANUAL_RECOVERY,
}


@dataclass(frozen=True)
class CompoundPosition:
    snapshot: PositionSnapshot
    npm_address: str
    dex_type: str
    stake_mode: str


@dataclass(frozen=True)
class CompoundPolicySkip:
    token_id: int
    reason: str


@dataclass
class CompoundDiscoveryResult:
    positions: dict[int, CompoundPosition] = field(default_factory=dict)
    policy_skips: list[CompoundPolicySkip] = field(default_factory=list)


@dataclass(frozen=True)
class CompoundEligibility:
    eligible: bool
    reason: str
    fee_value_usd: float = 0.0
    gas_cost_usd: float = 0.0
    threshold_usd: float = 0.0


@dataclass(frozen=True)
class CompoundSwapPlan:
    token_in: str | None
    token_out: str | None
    amount_in: int
    target_ratio_token1_per_token0_raw: str
    imbalance_ratio: float
    skip_swap: bool = False


@dataclass(frozen=True)
class CompoundLiquidityPlan:
    amount0_desired: int
    amount1_desired: int
    amount0_min: int
    amount1_min: int
    expected_liquidity: int


@dataclass
class CompoundResult:
    pool: str
    token_id: int | None
    state: str
    reason: str | None = None
    action: str = "COMPOUND"
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "action": self.action,
            "pool": self.pool,
            "token_id": self.token_id,
            "state": self.state,
        }
        if self.reason:
            value["reason"] = self.reason
        value.update(self.metadata)
        return value
