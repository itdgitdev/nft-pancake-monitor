from __future__ import annotations

import math


def tick_to_sqrt_price(tick: int) -> float:
    return math.sqrt(1.0001**tick)


def align_range_around_tick(current_tick: int, width: int, tick_spacing: int) -> tuple[int, int]:
    width = max(tick_spacing, (width // tick_spacing) * tick_spacing)
    raw_lower = current_tick - width // 2
    tick_lower = (raw_lower // tick_spacing) * tick_spacing
    tick_upper = tick_lower + width
    if current_tick < tick_lower:
        tick_lower -= tick_spacing
        tick_upper -= tick_spacing
    if current_tick >= tick_upper:
        tick_lower += tick_spacing
        tick_upper += tick_spacing
    return tick_lower, tick_upper


def tick_delta_from_price_percent(percent: float) -> float:
    ratio = 1.0 + (percent / 100.0)
    if ratio <= 0:
        raise ValueError("price percent produces a non-positive price ratio")
    return math.log(ratio) / math.log(1.0001)


def price_percent_from_tick_delta(tick_delta: int | float) -> float:
    return ((1.0001 ** float(tick_delta)) - 1.0) * 100.0


def _ceil_to_spacing(value: float, tick_spacing: int) -> int:
    return math.ceil(value / tick_spacing) * tick_spacing


def align_range_by_price_percent(
    current_tick: int,
    lower_percent: float,
    upper_percent: float,
    tick_spacing: int,
) -> tuple[int, int, float, float]:
    lower_delta = tick_delta_from_price_percent(lower_percent)
    upper_delta = tick_delta_from_price_percent(upper_percent)
    raw_lower = current_tick + lower_delta
    raw_upper = current_tick + upper_delta
    tick_lower = (math.floor(raw_lower / tick_spacing)) * tick_spacing
    tick_upper = _ceil_to_spacing(raw_upper, tick_spacing)
    if tick_lower >= current_tick:
        tick_lower = (current_tick // tick_spacing) * tick_spacing - tick_spacing
    if tick_upper <= current_tick:
        tick_upper = (current_tick // tick_spacing) * tick_spacing + tick_spacing
    if tick_upper <= tick_lower:
        tick_upper = tick_lower + tick_spacing
    return tick_lower, tick_upper, lower_delta, upper_delta


def amount_units_per_liquidity(sqrt_price_x96: int, tick_lower: int, tick_upper: int) -> tuple[float, float, bool]:
    sqrt_price = float(sqrt_price_x96) / (2**96)
    sqrt_lower = tick_to_sqrt_price(tick_lower)
    sqrt_upper = tick_to_sqrt_price(tick_upper)
    if sqrt_price <= sqrt_lower:
        return (1.0 / sqrt_lower - 1.0 / sqrt_upper), 0.0, False
    if sqrt_price >= sqrt_upper:
        return 0.0, (sqrt_upper - sqrt_lower), False
    return (1.0 / sqrt_price - 1.0 / sqrt_upper), (sqrt_price - sqrt_lower), True


def amounts_for_liquidity(
    liquidity: float,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
) -> tuple[int, int]:
    a0_per_l, a1_per_l, _ = amount_units_per_liquidity(sqrt_price_x96, tick_lower, tick_upper)
    return int(liquidity * a0_per_l), int(liquidity * a1_per_l)


def liquidity_from_amounts(
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    amount0: int,
    amount1: int,
) -> float:
    a0_per_l, a1_per_l, in_range = amount_units_per_liquidity(sqrt_price_x96, tick_lower, tick_upper)
    if not in_range:
        if a0_per_l > 0:
            return amount0 / a0_per_l if amount0 > 0 else 0.0
        return amount1 / a1_per_l if amount1 > 0 else 0.0
    l0 = amount0 / a0_per_l if amount0 > 0 and a0_per_l > 0 else float("inf")
    l1 = amount1 / a1_per_l if amount1 > 0 and a1_per_l > 0 else float("inf")
    value = min(l0, l1)
    return 0.0 if value == float("inf") else value


def spot_token1_per_token0(sqrt_price_x96: int, decimals0: int, decimals1: int) -> float:
    sqrt_price = float(sqrt_price_x96) / (2**96)
    raw_ratio = sqrt_price * sqrt_price
    return raw_ratio * (10**dec0_to_dec1_adjust(decimals0, decimals1))


def dec0_to_dec1_adjust(decimals0: int, decimals1: int) -> int:
    return decimals0 - decimals1
