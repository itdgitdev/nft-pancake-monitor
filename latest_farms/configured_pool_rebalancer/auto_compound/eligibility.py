from __future__ import annotations

from datetime import datetime, timezone

from ..models import AutoCompoundConfig, DexType, Slot0
from .models import CompoundEligibility, CompoundPosition


class CompoundEligibilityEvaluator:
    def evaluate_policy(self, position: CompoundPosition, slot0: Slot0, config: AutoCompoundConfig) -> CompoundEligibility:
        snapshot = position.snapshot
        if not config.enabled:
            return CompoundEligibility(False, "DISABLED")
        if position.dex_type in {DexType.AERODROME_GAUGE.value, DexType.AERODROME_V3.value} and snapshot.is_staked:
            return CompoundEligibility(False, "STAKE_POLICY")
        if snapshot.liquidity <= 0:
            return CompoundEligibility(False, "NO_LIQUIDITY")
        if not snapshot.tick_lower <= slot0.tick < snapshot.tick_upper:
            return CompoundEligibility(False, "OUT_OF_RANGE")
        width = snapshot.tick_upper - snapshot.tick_lower
        edge_distance = min(slot0.tick - snapshot.tick_lower, snapshot.tick_upper - slot0.tick)
        if width <= 0 or edge_distance / width < config.min_range_buffer_ratio:
            return CompoundEligibility(False, "NEAR_EDGE")
        return CompoundEligibility(True, "ELIGIBLE")

    def evaluate_profitability(
        self,
        config: AutoCompoundConfig,
        fee_value_usd: float,
        gas_cost_usd: float,
    ) -> CompoundEligibility:
        threshold = max(config.min_compound_usd, config.gas_cost_multiplier * gas_cost_usd)
        if fee_value_usd < threshold:
            return CompoundEligibility(False, "BELOW_THRESHOLD", fee_value_usd, gas_cost_usd, threshold)
        return CompoundEligibility(True, "ELIGIBLE", fee_value_usd, gas_cost_usd, threshold)

    @staticmethod
    def cooldown_passed(completed_at: datetime | None, interval_seconds: int, now: datetime | None = None) -> bool:
        if completed_at is None or interval_seconds <= 0:
            return True
        now = now or datetime.now(timezone.utc)
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)
        return (now - completed_at).total_seconds() >= interval_seconds
