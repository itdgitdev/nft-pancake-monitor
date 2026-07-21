from __future__ import annotations

from decimal import Decimal, getcontext

from web3 import Web3

from ..models import Slot0
from .models import CompoundLiquidityPlan, CompoundPosition, CompoundSwapPlan


getcontext().prec = 90
Q96 = 1 << 96
MIN_TICK = -887272
MAX_TICK = 887272
SWAP_SAFETY_FACTOR = Decimal("0.995")
BALANCE_TOLERANCE = Decimal("0.005")


def sqrt_ratio_at_tick(tick: int) -> int:
    """Return canonical Uniswap TickMath sqrt(1.0001**tick) Q96."""
    if tick < MIN_TICK or tick > MAX_TICK:
        raise ValueError("tick outside supported range")
    abs_tick = abs(int(tick))
    constants = (
        0xFFFcb933BD6FAD37AA2D162D1A594001,
        0xFFF97272373D413259A46990580E213A,
        0xFFF2E50F5F656932EF12357CF3C7FDCC,
        0xFFE5CACA7E10E4E61C3624EAA0941CD0,
        0xFFCB9843D60F6159C9DB58835C926644,
        0xFF973B41FA98C081472E6896DFB254C0,
        0xFF2EA16466C96A3843EC78B326B52861,
        0xFE5DEE046A99A2A811C461F1969C3053,
        0xFCBE86C7900A88AEDCFFC83B479AA3A4,
        0xF987A7253AC413176F2B074CF7815E54,
        0xF3392B0822B70005940C7A398E4B70F3,
        0xE7159475A2C29B7443B29C7FA6E889D9,
        0xD097F3BDFD2022B8845AD8F792AA5825,
        0xA9F746462D870FDF8A65DC1F90E061E5,
        0x70D869A156D2A1B890BB3DF62BAF32F7,
        0x31BE135F97D08FD981231505542FCFA6,
        0x9AA508B5B7A84E1C677DE54F3E99BC9,
        0x5D6AF8DEDB81196699C329225EE604,
        0x2216E584F5FA1EA926041BEDFE98,
        0x48A170391F7DC42444E8FA2,
    )
    ratio = constants[0] if abs_tick & 1 else 1 << 128
    for bit, constant in enumerate(constants[1:], start=1):
        if abs_tick & (1 << bit):
            ratio = (ratio * constant) >> 128
    if tick > 0:
        ratio = ((1 << 256) - 1) // ratio
    remainder_mask = (1 << 32) - 1
    return (ratio >> 32) + (1 if ratio & remainder_mask else 0)


def amounts_for_liquidity_exact(
    liquidity: int,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
) -> tuple[int, int]:
    sqrt_a = sqrt_ratio_at_tick(tick_lower)
    sqrt_b = sqrt_ratio_at_tick(tick_upper)
    sqrt_p = int(sqrt_price_x96)
    liquidity = max(0, int(liquidity))
    if sqrt_p <= sqrt_a:
        return liquidity * (sqrt_b - sqrt_a) * Q96 // (sqrt_a * sqrt_b), 0
    if sqrt_p < sqrt_b:
        amount0 = liquidity * (sqrt_b - sqrt_p) * Q96 // (sqrt_p * sqrt_b)
        amount1 = liquidity * (sqrt_p - sqrt_a) // Q96
        return amount0, amount1
    return 0, liquidity * (sqrt_b - sqrt_a) // Q96


def liquidity_from_amounts_exact(
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    amount0: int,
    amount1: int,
) -> int:
    sqrt_a = sqrt_ratio_at_tick(tick_lower)
    sqrt_b = sqrt_ratio_at_tick(tick_upper)
    sqrt_p = int(sqrt_price_x96)
    amount0 = max(0, int(amount0))
    amount1 = max(0, int(amount1))
    if sqrt_p <= sqrt_a:
        return amount0 * sqrt_a * sqrt_b // (Q96 * (sqrt_b - sqrt_a))
    if sqrt_p < sqrt_b:
        liquidity0 = amount0 * sqrt_p * sqrt_b // (Q96 * (sqrt_b - sqrt_p))
        liquidity1 = amount1 * Q96 // (sqrt_p - sqrt_a)
        return min(liquidity0, liquidity1)
    return amount1 * Q96 // (sqrt_b - sqrt_a)


class CompoundPlanner:
    def build_swap_plan(
        self,
        position: CompoundPosition,
        slot0: Slot0,
        amount0: int,
        amount1: int,
    ) -> CompoundSwapPlan:
        snapshot = position.snapshot
        reference_liquidity = 10**24
        unit0, unit1 = amounts_for_liquidity_exact(
            reference_liquidity,
            slot0.sqrt_price_x96,
            snapshot.tick_lower,
            snapshot.tick_upper,
        )
        if unit0 <= 0 or unit1 <= 0:
            return CompoundSwapPlan(None, None, 0, "0", 1.0, skip_swap=True)

        target_ratio = Decimal(unit1) / Decimal(unit0)
        spot_ratio = (Decimal(slot0.sqrt_price_x96) / Decimal(Q96)) ** 2
        delta = Decimal(amount1) - target_ratio * Decimal(amount0)
        total_value1 = Decimal(amount1) + Decimal(amount0) * spot_ratio
        imbalance = abs(delta) / max(Decimal(1), total_value1)
        if imbalance <= BALANCE_TOLERANCE:
            return CompoundSwapPlan(None, None, 0, str(target_ratio), float(imbalance), skip_swap=True)

        token0 = Web3.to_checksum_address(snapshot.token0)
        token1 = Web3.to_checksum_address(snapshot.token1)
        if delta < 0:
            raw = (target_ratio * Decimal(amount0) - Decimal(amount1)) / (spot_ratio + target_ratio)
            amount_in = min(int(amount0), max(0, int(raw * SWAP_SAFETY_FACTOR)))
            return CompoundSwapPlan(token0, token1, amount_in, str(target_ratio), float(imbalance))

        raw = (Decimal(amount1) - target_ratio * Decimal(amount0)) / (
            Decimal(1) + target_ratio / spot_ratio
        )
        amount_in = min(int(amount1), max(0, int(raw * SWAP_SAFETY_FACTOR)))
        return CompoundSwapPlan(token1, token0, amount_in, str(target_ratio), float(imbalance))

    def build_liquidity_plan(
        self,
        position: CompoundPosition,
        slot0: Slot0,
        amount0: int,
        amount1: int,
        slippage_bps: int,
    ) -> CompoundLiquidityPlan:
        snapshot = position.snapshot
        liquidity = liquidity_from_amounts_exact(
            slot0.sqrt_price_x96,
            snapshot.tick_lower,
            snapshot.tick_upper,
            amount0,
            amount1,
        )
        desired0, desired1 = amounts_for_liquidity_exact(
            liquidity,
            slot0.sqrt_price_x96,
            snapshot.tick_lower,
            snapshot.tick_upper,
        )
        desired0 = min(int(amount0), desired0)
        desired1 = min(int(amount1), desired1)
        factor = max(0, 10_000 - int(slippage_bps))
        return CompoundLiquidityPlan(
            amount0_desired=desired0,
            amount1_desired=desired1,
            amount0_min=desired0 * factor // 10_000,
            amount1_min=desired1 * factor // 10_000,
            expected_liquidity=liquidity,
        )
