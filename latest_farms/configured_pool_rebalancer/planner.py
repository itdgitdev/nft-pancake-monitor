from __future__ import annotations

from web3 import Web3

from .models import PoolConfig, PositionSnapshot, RebalancePlan, Slot0
from .v3_math import align_range_around_tick, align_range_by_price_percent, amount_units_per_liquidity, liquidity_from_amounts


TICK_SPACING_BY_FEE = {
    100: 1,
    500: 10,
    2500: 50,
    3000: 60,
    10000: 200,
}


class RebalancePlanner:
    def build_plan(
        self,
        pool: PoolConfig,
        position: PositionSnapshot,
        slot0: Slot0,
        recovered0: int = 0,
        recovered1: int = 0,
        lower_percent: float | None = None,
        upper_percent: float | None = None,
        range_percent_source: str | None = None,
    ) -> RebalancePlan:
        tick_spacing = pool.tick_spacing or TICK_SPACING_BY_FEE.get(pool.fee or position.fee, 60)
        metadata = {}
        if lower_percent is not None and upper_percent is not None:
            new_lower, new_upper, lower_delta, upper_delta = align_range_by_price_percent(
                slot0.tick,
                lower_percent,
                upper_percent,
                tick_spacing,
            )
            metadata.update(
                {
                    "range_mode": "price_percent",
                    "lower_percent": lower_percent,
                    "upper_percent": upper_percent,
                    "lower_tick_delta": lower_delta,
                    "upper_tick_delta": upper_delta,
                    "range_percent_source": range_percent_source or "UNKNOWN",
                }
            )
        else:
            new_lower, new_upper = align_range_around_tick(slot0.tick, position.width, tick_spacing)
            metadata.update(
                {
                    "range_mode": "center_width",
                    "range_percent_source": range_percent_source or "CENTER_FALLBACK",
                }
            )

        amount0_desired = recovered0
        amount1_desired = recovered1
        if recovered0 or recovered1:
            liquidity = liquidity_from_amounts(
                slot0.sqrt_price_x96,
                new_lower,
                new_upper,
                recovered0,
                recovered1,
            )
            a0_per_l, a1_per_l, in_range = amount_units_per_liquidity(
                slot0.sqrt_price_x96, new_lower, new_upper
            )
            if in_range and liquidity > 0:
                amount0_desired = min(recovered0, int(liquidity * a0_per_l))
                amount1_desired = min(recovered1, int(liquidity * a1_per_l))

        return RebalancePlan(
            old_token_id=position.token_id,
            current_tick=slot0.tick,
            old_tick_lower=position.tick_lower,
            old_tick_upper=position.tick_upper,
            new_tick_lower=new_lower,
            new_tick_upper=new_upper,
            amount0_desired=amount0_desired,
            amount1_desired=amount1_desired,
            metadata=metadata,
        )


class SwapPlanner:
    _SWAP_SAFETY_FACTOR = 0.995

    def build_swap_plan(
        self,
        pool: PoolConfig,
        position: PositionSnapshot,
        slot0: Slot0,
        recovered0: int,
        recovered1: int,
        lower_percent: float | None = None,
        upper_percent: float | None = None,
        range_percent_source: str | None = None,
    ) -> RebalancePlan:
        base = RebalancePlanner().build_plan(
            pool,
            position,
            slot0,
            recovered0,
            recovered1,
            lower_percent,
            upper_percent,
            range_percent_source,
        )
        if recovered0 <= 0 and recovered1 <= 0:
            return base

        a0_per_l, a1_per_l, in_range = amount_units_per_liquidity(
            slot0.sqrt_price_x96,
            base.new_tick_lower,
            base.new_tick_upper,
        )
        if not in_range or a0_per_l <= 0 or a1_per_l <= 0:
            return base

        target_ratio = a1_per_l / a0_per_l  # token1 raw units per token0 raw unit for the target range.
        sqrt_price = float(slot0.sqrt_price_x96) / (2**96)
        spot_ratio = sqrt_price * sqrt_price  # token1 raw units per token0 raw unit at current price.
        if target_ratio <= 0 or spot_ratio <= 0:
            return base

        token0 = Web3.to_checksum_address(pool.token0_address or position.token0)
        token1 = Web3.to_checksum_address(pool.token1_address or position.token1)

        balance_delta = recovered1 - (target_ratio * recovered0)
        if balance_delta < 0:
            # Need more token1. Swap token0 -> token1.
            amount_in = (target_ratio * recovered0 - recovered1) / (spot_ratio + target_ratio)
            base.swap_token_in = token0
            base.swap_token_out = token1
            base.swap_amount_in = min(recovered0, int(amount_in * self._SWAP_SAFETY_FACTOR))
        elif balance_delta > 0:
            # Need more token0. Swap token1 -> token0.
            amount_in = (recovered1 - target_ratio * recovered0) / (1 + target_ratio / spot_ratio)
            base.swap_token_in = token1
            base.swap_token_out = token0
            base.swap_amount_in = min(recovered1, int(amount_in * self._SWAP_SAFETY_FACTOR))

        base.metadata.update(
            {
                "target_ratio_token1_per_token0_raw": target_ratio,
                "spot_ratio_token1_per_token0_raw": spot_ratio,
                "swap_safety_factor": self._SWAP_SAFETY_FACTOR,
            }
        )
        return base
