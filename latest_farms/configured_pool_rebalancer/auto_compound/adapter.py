from __future__ import annotations

import time

from web3 import Web3

from ..adapter import DexAdapter
from ..models import DexType, PoolConfig, PositionSnapshot, Slot0, TokenBalance, TxResult
from .abi import COMPOUND_MASTERCHEF_ABI, COMPOUND_NPM_ABI, ERC20_ABI, MAX_UINT128, MAX_UINT256
from .executor import CompoundExecutor
from .models import CompoundJobState, CompoundLiquidityPlan, CompoundPosition


class CompoundAdapter:
    def __init__(
        self,
        w3: Web3,
        pool: PoolConfig,
        read_adapter: DexAdapter,
        executor: CompoundExecutor,
    ):
        self.w3 = w3
        self.pool = pool
        self.read_adapter = read_adapter
        self.executor = executor
        self.npm_address = Web3.to_checksum_address(pool.npm_address)
        self.npm = w3.eth.contract(address=self.npm_address, abi=COMPOUND_NPM_ABI)
        self.staking_address = Web3.to_checksum_address(pool.staking_address) if pool.staking_address else None
        self.masterchef = (
            w3.eth.contract(address=self.staking_address, abi=COMPOUND_MASTERCHEF_ABI)
            if self.staking_address and pool.dex_type in {DexType.PANCAKE_V3, DexType.PANCAKE_V3_MASTERCHEF}
            else None
        )

    def read_slot0(self) -> Slot0:
        return self.read_adapter.read_slot0()

    def read_balances(self) -> tuple[TokenBalance, TokenBalance]:
        return self.read_adapter.read_balances(self.pool.bot_wallet)

    def read_position(self, position: CompoundPosition) -> PositionSnapshot:
        found = self.read_adapter.read_staked_positions([position.snapshot.token_id])
        if position.snapshot.token_id in found:
            snapshot = found[position.snapshot.token_id]
            snapshot.is_staked = True
            if snapshot.owner.lower() != self.pool.bot_wallet.lower():
                raise RuntimeError(f"position logical owner mismatch: {snapshot.owner}")
        else:
            owner = Web3.to_checksum_address(self.npm.functions.ownerOf(position.snapshot.token_id).call())
            if owner != Web3.to_checksum_address(self.pool.bot_wallet):
                raise RuntimeError(f"position owner mismatch: {owner}")
            snapshot = self.read_adapter.read_npm_position(position.snapshot.token_id, owner=owner)
            snapshot.is_staked = False
        if self.pool.token0_address and snapshot.token0.lower() != self.pool.token0_address.lower():
            raise RuntimeError("position token0 does not match configured pool")
        if self.pool.token1_address and snapshot.token1.lower() != self.pool.token1_address.lower():
            raise RuntimeError("position token1 does not match configured pool")
        expected = (
            self.pool.tick_spacing
            if self.pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}
            else self.pool.fee
        )
        if expected is not None and int(snapshot.fee) != int(expected):
            raise RuntimeError("position fee/tick-spacing does not match configured pool")
        return snapshot

    def collect_contract(self, position: CompoundPosition):
        if position.snapshot.is_staked:
            if self.masterchef is None:
                raise RuntimeError("Pancake MasterChef is not configured")
            return self.masterchef
        return self.npm

    def increase_spender(self, position: CompoundPosition) -> str:
        if position.snapshot.is_staked:
            if not self.staking_address:
                raise RuntimeError("Pancake MasterChef is not configured")
            return self.staking_address
        return self.npm_address

    def quote_collect(self, position: CompoundPosition, block_identifier="latest") -> tuple[int, int]:
        params = (position.snapshot.token_id, self.pool.bot_wallet, MAX_UINT128, MAX_UINT128)
        result = self.collect_contract(position).functions.collect(params).call(
            {"from": Web3.to_checksum_address(self.pool.bot_wallet)},
            block_identifier=block_identifier,
        )
        return int(result[0]), int(result[1])

    def collect(self, job_id: int | None, position: CompoundPosition) -> TxResult:
        params = (position.snapshot.token_id, self.pool.bot_wallet, MAX_UINT128, MAX_UINT128)
        call_fn = self.collect_contract(position).functions.collect(params)
        return self.executor.send_call(
            job_id,
            call_fn,
            "COLLECT",
            CompoundJobState.COLLECT_PENDING,
            CompoundJobState.COLLECTED,
            "collect_tx_hash",
            gas=400_000,
        )

    def allowance(self, token: str, spender: str) -> int:
        contract = self.w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        return int(contract.functions.allowance(self.pool.bot_wallet, Web3.to_checksum_address(spender)).call())

    def approve(
        self,
        job_id: int | None,
        token: str,
        spender: str,
        pending_state: CompoundJobState,
        success_state: CompoundJobState,
        tx_field: str,
        action: str = "APPROVE",
    ) -> TxResult:
        contract = self.w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        spender_cs = Web3.to_checksum_address(spender)
        try:
            return self.executor.send_call(
                job_id,
                contract.functions.approve(spender_cs, MAX_UINT256),
                action,
                pending_state,
                success_state,
                tx_field,
                gas=120_000,
            )
        except RuntimeError:
            current = int(contract.functions.allowance(self.pool.bot_wallet, spender_cs).call())
            if current <= 0 or self.executor.dry_run:
                raise
            zero_result = self.executor.send_call(
                job_id,
                contract.functions.approve(spender_cs, 0),
                f"{action}_ZERO",
                pending_state,
                success_state,
                tx_field,
                gas=120_000,
            )
            if zero_result.status != "SUCCESS":
                return zero_result
            return self.executor.send_call(
                job_id,
                contract.functions.approve(spender_cs, MAX_UINT256),
                action,
                pending_state,
                success_state,
                tx_field,
                gas=120_000,
            )

    def increase(
        self,
        job_id: int | None,
        position: CompoundPosition,
        plan: CompoundLiquidityPlan,
    ) -> TxResult:
        deadline = int(time.time()) + self.pool.deadline_seconds
        params = (
            position.snapshot.token_id,
            int(plan.amount0_desired),
            int(plan.amount1_desired),
            int(plan.amount0_min),
            int(plan.amount1_min),
            deadline,
        )
        contract = self.collect_contract(position)
        return self.executor.send_call(
            job_id,
            contract.functions.increaseLiquidity(params),
            "INCREASE",
            CompoundJobState.INCREASE_PENDING,
            CompoundJobState.INCREASE_PENDING,
            "increase_tx_hash",
            gas=650_000,
        )
