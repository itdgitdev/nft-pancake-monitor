from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from web3 import Web3

from ..models import PoolConfig
from ..swapper import V3Swapper


@dataclass(frozen=True)
class CompoundSwapQuote:
    provider: str
    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    allowance_target: str | None
    transaction: dict
    price_impact_pct: float


class CompoundSwapper:
    MAX_REFINEMENTS = 3

    def __init__(self, pool: PoolConfig):
        self.pool = pool
        try:
            from latest_farms.config import RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_URLS_2
        self.provider = V3Swapper(pool.chain, RPC_URLS_2.get(pool.chain))

    def best_quote(self, token_in: str, token_out: str, amount_in: int, wallet: str) -> CompoundSwapQuote | None:
        routes = self.provider.get_swap_routes(
            token_in,
            token_out,
            int(amount_in),
            Web3.to_checksum_address(wallet),
            self.pool.slippage_bps,
        )
        for route in routes:
            try:
                impact = float(route.get("price_impact", 0) or 0)
                amount_out = int(route.get("buyAmount") or 0)
            except (TypeError, ValueError):
                continue
            if impact > self.pool.max_swap_price_impact_pct or amount_out <= 0:
                continue
            return CompoundSwapQuote(
                provider=str(route.get("provider") or "unknown"),
                token_in=Web3.to_checksum_address(token_in),
                token_out=Web3.to_checksum_address(token_out),
                amount_in=int(amount_in),
                amount_out=amount_out,
                allowance_target=(
                    Web3.to_checksum_address(route["allowanceTarget"])
                    if route.get("allowanceTarget")
                    else None
                ),
                transaction={
                    "to": Web3.to_checksum_address(route["to"]),
                    "data": route["data"],
                    "value": self._int_value(route.get("value", 0)),
                },
                price_impact_pct=impact,
            )
        return None

    def best_refined_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        wallet: str,
        score: Callable[[CompoundSwapQuote], int],
    ) -> CompoundSwapQuote | None:
        candidates = []
        for numerator in (9800, 10_000, 10_200):
            candidate = max(1, int(amount_in) * numerator // 10_000)
            if candidate not in candidates:
                candidates.append(candidate)
        quotes = [
            quote
            for quote in (
                self.best_quote(token_in, token_out, candidate, wallet)
                for candidate in candidates[: self.MAX_REFINEMENTS]
            )
            if quote is not None
        ]
        return max(quotes, key=score) if quotes else None

    @staticmethod
    def _int_value(value) -> int:
        if isinstance(value, str):
            return int(value, 16) if value.lower().startswith("0x") else int(value or 0)
        return int(value or 0)
