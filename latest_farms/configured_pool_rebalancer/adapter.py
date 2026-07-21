from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Iterable

from web3 import Web3
from web3.exceptions import TimeExhausted, TransactionNotFound
from w3multicall.multicall import W3Multicall

from .abi import (
    AERODROME_GAUGE_ABI,
    AERODROME_NPM_ABI,
    AERODROME_POOL_ABI,
    ERC20_ABI,
    INCREASE_LIQUIDITY_TOPIC,
    MASTERCHEF_V3_ABI,
    MAX_UINT128,
    MAX_UINT256,
    NPM_ABI,
    V3_POOL_ABI,
)
from .logging_utils import log_block, pool_context
from .models import PoolConfig, PositionSnapshot, Slot0, TokenBalance, TxResult
from .reward import token_price_usd
from .tx_executor import TxExecutor
from .v3_math import amounts_for_liquidity

log = logging.getLogger("configured_pool_rebalancer")


@dataclass(frozen=True)
class MintValidationResult:
    status: str
    reason: str | None = None
    position: PositionSnapshot | None = None
    rpc_label: str = "primary"

    @property
    def can_stake(self) -> bool:
        return self.status in {"VALID", "VALID_WITH_RANGE_WARNING"}


try:
    from latest_farms.config import AERODROME_FACTORY_NPM_ADDRESSES, MASTERCHEF_V3_ADDRESSES, NPM_ADDRESSES
except ImportError:  # pragma: no cover
    from config import AERODROME_FACTORY_NPM_ADDRESSES, MASTERCHEF_V3_ADDRESSES, NPM_ADDRESSES


MASTER_CHEF_DEPOSIT_TOPIC = "0x" + Web3.keccak(text="Deposit(address,uint256,uint256,uint256,int24,int24)").hex().lower().replace("0x", "")
MASTER_CHEF_WITHDRAW_TOPIC = "0x" + Web3.keccak(text="Withdraw(address,address,uint256,uint256)").hex().lower().replace("0x", "")
AERODROME_GAUGE_DEPOSIT_TOPIC = "0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211"
AERODROME_GAUGE_WITHDRAW_TOPIC = "0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


class DexAdapter:
    def __init__(self, w3: Web3, pool: PoolConfig, executor: TxExecutor):
        self.w3 = w3
        self.pool = pool
        self.executor = executor

    def read_slot0(self) -> Slot0:
        raise NotImplementedError

    def discover_pool_metadata(self) -> PoolConfig:
        raise NotImplementedError

    def read_balances(self, wallet: str) -> tuple[TokenBalance, TokenBalance]:
        raise NotImplementedError

    def read_token_balance(self, token_address: str, wallet: str) -> TokenBalance:
        raise NotImplementedError

    def read_staked_positions(self, token_ids: Iterable[int]) -> dict[int, PositionSnapshot]:
        raise NotImplementedError

    def read_npm_position(self, token_id: int, owner: str | None = None) -> PositionSnapshot:
        raise NotImplementedError

    def read_npm_positions(
        self,
        token_ids: Iterable[int],
        owners: dict[int, str] | None = None,
    ) -> dict[int, PositionSnapshot]:
        owners = owners or {}
        out: dict[int, PositionSnapshot] = {}
        for token_id in sorted({int(value) for value in token_ids}):
            try:
                out[token_id] = self.read_npm_position(token_id, owner=owners.get(token_id))
            except Exception:
                continue
        return out

    def decrease_collect_withdraw(self, position: PositionSnapshot, slot0: Slot0) -> TxResult:
        raise NotImplementedError

    def withdraw_staked_position(self, position: PositionSnapshot) -> TxResult | None:
        return None

    def swap(self, token_in: str, token_out: str, amount_in: int) -> TxResult | None:
        raise NotImplementedError

    def mint(self, plan) -> tuple[TxResult, int | None]:
        raise NotImplementedError

    def should_stake(self) -> bool:
        return False

    def stake(self, token_id: int) -> TxResult:
        raise NotImplementedError

    def staking_owner_address(self) -> str | None:
        return None

    def stake_event_contract_address(self) -> str | None:
        return None

    def stake_event_topics(self) -> list[str]:
        return []

    def parse_stake_event(self, event) -> tuple[str, int] | None:
        return None

    def reward_token_address(self) -> str | None:
        return None


class PancakeV3MasterChefAdapter(DexAdapter):
    def __init__(self, w3: Web3, pool: PoolConfig, executor: TxExecutor):
        super().__init__(w3, pool, executor)
        self.npm_address = Web3.to_checksum_address(pool.npm_address or NPM_ADDRESSES[pool.chain])
        self.masterchef_address = Web3.to_checksum_address(
            pool.staking_address or MASTERCHEF_V3_ADDRESSES[pool.chain]
        )
        self.npm = w3.eth.contract(address=self.npm_address, abi=NPM_ABI)
        self.masterchef = w3.eth.contract(address=self.masterchef_address, abi=MASTERCHEF_V3_ABI)

    def should_stake(self) -> bool:
        return self.pool.pid is not None

    def staking_owner_address(self) -> str | None:
        return self.masterchef_address

    def stake_event_contract_address(self) -> str | None:
        return self.masterchef_address

    def stake_event_topics(self) -> list[str]:
        return [MASTER_CHEF_DEPOSIT_TOPIC, MASTER_CHEF_WITHDRAW_TOPIC]

    def parse_stake_event(self, event) -> tuple[str, int] | None:
        topics = event.get("topics") or []
        if len(topics) < 4:
            return None
        topic0 = Web3.to_hex(topics[0]).lower()
        if topic0 == MASTER_CHEF_DEPOSIT_TOPIC.lower():
            action = "stake"
        elif topic0 == MASTER_CHEF_WITHDRAW_TOPIC.lower():
            action = "unstake"
        else:
            return None
        return action, int.from_bytes(topics[3], "big")

    def reward_token_address(self) -> str | None:
        try:
            from .reward import pancake_reward_token

            return pancake_reward_token(self.pool.chain)
        except Exception:
            return None

    def discover_pool_metadata(self) -> PoolConfig:
        pool_contract = self.w3.eth.contract(address=self.pool.pool_address, abi=V3_POOL_ABI)
        token0 = Web3.to_checksum_address(self.pool.token0_address or pool_contract.functions.token0().call())
        token1 = Web3.to_checksum_address(self.pool.token1_address or pool_contract.functions.token1().call())
        fee = int(self.pool.fee or pool_contract.functions.fee().call())
        dec0 = self.pool.token0_decimals or self._erc20(token0).functions.decimals().call()
        dec1 = self.pool.token1_decimals or self._erc20(token1).functions.decimals().call()
        return PoolConfig(
            **{
                **self.pool.__dict__,
                "token0_address": token0,
                "token1_address": token1,
                "token0_decimals": int(dec0),
                "token1_decimals": int(dec1),
                "fee": fee,
                "npm_address": self.npm_address,
                "staking_address": self.masterchef_address,
            }
        )

    def read_slot0(self) -> Slot0:
        pool_contract = self.w3.eth.contract(address=self.pool.pool_address, abi=V3_POOL_ABI)
        res = pool_contract.functions.slot0().call()
        return Slot0(sqrt_price_x96=int(res[0]), tick=int(res[1]))

    def read_farm_alloc_point(self) -> int | None:
        if self.pool.pid is None:
            return None
        try:
            info = self.masterchef.functions.poolInfo(int(self.pool.pid)).call()
        except Exception as exc:
            log.warning("could not read farm allocPoint pool=%s pid=%s: %s", self.pool.name, self.pool.pid, exc)
            return None
        try:
            return int(info[0])
        except (IndexError, TypeError, ValueError) as exc:
            log.warning("invalid farm poolInfo response pool=%s pid=%s: %s", self.pool.name, self.pool.pid, exc)
            return None

    def read_balances(self, wallet: str) -> tuple[TokenBalance, TokenBalance]:
        token0 = self._erc20(self.pool.token0_address)
        token1 = self._erc20(self.pool.token1_address)
        wallet_cs = Web3.to_checksum_address(wallet)
        dec0 = int(self.pool.token0_decimals or token0.functions.decimals().call())
        dec1 = int(self.pool.token1_decimals or token1.functions.decimals().call())
        return (
            TokenBalance(raw=int(token0.functions.balanceOf(wallet_cs).call()), decimals=dec0),
            TokenBalance(raw=int(token1.functions.balanceOf(wallet_cs).call()), decimals=dec1),
        )

    def read_token_balance(self, token_address: str, wallet: str) -> TokenBalance:
        token = self._erc20(token_address)
        wallet_cs = Web3.to_checksum_address(wallet)
        decimals = int(token.functions.decimals().call())
        return TokenBalance(raw=int(token.functions.balanceOf(wallet_cs).call()), decimals=decimals)

    def read_staked_positions(self, token_ids: Iterable[int]) -> dict[int, PositionSnapshot]:
        out: dict[int, PositionSnapshot] = {}
        ids = sorted({int(tid) for tid in token_ids})
        for start in range(0, len(ids), 150):
            batch = ids[start : start + 150]
            mc = W3Multicall(self.w3)
            for token_id in batch:
                mc.add(
                    W3Multicall.Call(
                        self.masterchef_address,
                        "userPositionInfos(uint256)(uint128,uint128,int24,int24,uint256,uint256,address,uint256,uint256)",
                        token_id,
                    )
                )
            results = mc.call()
            active: dict[int, tuple] = {}
            for i, data in enumerate(results):
                if not data:
                    continue
                token_id = batch[i]
                liquidity, _, tick_lower, tick_upper, _, _, user, pid, _ = data
                if int(liquidity) <= 0:
                    continue
                if self.pool.pid is not None and int(pid) != int(self.pool.pid):
                    continue
                active[token_id] = data
            npm_positions = self._batch_npm_position_values(active)
            for token_id, data in active.items():
                npm_pos = npm_positions.get(token_id)
                if not npm_pos:
                    continue
                liquidity, _, tick_lower, tick_upper, _, _, user, pid, _ = data
                token0 = Web3.to_checksum_address(npm_pos[2])
                token1 = Web3.to_checksum_address(npm_pos[3])
                fee = int(npm_pos[4])
                if not self._matches_pool(token0, token1, fee):
                    continue
                out[token_id] = PositionSnapshot(
                    token_id=token_id,
                    owner=Web3.to_checksum_address(user),
                    pool_address=self.pool.pool_address,
                    token0=token0,
                    token1=token1,
                    fee=fee,
                    tick_lower=int(tick_lower),
                    tick_upper=int(tick_upper),
                    liquidity=int(liquidity),
                    tokens_owed0=int(npm_pos[10]),
                    tokens_owed1=int(npm_pos[11]),
                    pid=int(pid),
                    is_staked=True,
                )
        return out

    def _batch_npm_position_values(self, token_ids: Iterable[int]) -> dict[int, tuple]:
        ordered = sorted({int(value) for value in token_ids})
        out: dict[int, tuple] = {}
        signature = (
            "positions(uint256)"
            "(uint96,address,address,address,uint24,int24,int24,uint128,uint256,uint256,uint128,uint128)"
        )
        for start in range(0, len(ordered), 150):
            batch = ordered[start : start + 150]
            multicall = W3Multicall(self.w3)
            for token_id in batch:
                multicall.add(W3Multicall.Call(self.npm_address, signature, token_id))
            try:
                values = multicall.call()
            except Exception as exc:
                log.warning("Pancake NPM position batch failed pool=%s candidates=%s: %s", self.pool.name, len(batch), exc)
                continue
            for token_id, value in zip(batch, values):
                if value:
                    out[token_id] = value
        return out

    def read_npm_positions(
        self,
        token_ids: Iterable[int],
        owners: dict[int, str] | None = None,
    ) -> dict[int, PositionSnapshot]:
        owners = owners or {}
        out: dict[int, PositionSnapshot] = {}
        for token_id, pos in self._batch_npm_position_values(token_ids).items():
            out[token_id] = PositionSnapshot(
                token_id=token_id,
                owner=Web3.to_checksum_address(owners.get(token_id) or self.pool.bot_wallet),
                pool_address=self.pool.pool_address,
                token0=Web3.to_checksum_address(pos[2]),
                token1=Web3.to_checksum_address(pos[3]),
                fee=int(pos[4]),
                tick_lower=int(pos[5]),
                tick_upper=int(pos[6]),
                liquidity=int(pos[7]),
                tokens_owed0=int(pos[10]),
                tokens_owed1=int(pos[11]),
                pid=self.pool.pid,
                is_staked=False,
            )
        return out

    def read_npm_position(self, token_id: int, owner: str | None = None) -> PositionSnapshot:
        pos = self.npm.functions.positions(int(token_id)).call()
        actual_owner = owner
        try:
            actual_owner = self.npm.functions.ownerOf(int(token_id)).call()
        except Exception:
            pass
        return PositionSnapshot(
            token_id=int(token_id),
            owner=Web3.to_checksum_address(actual_owner or self.pool.bot_wallet),
            pool_address=self.pool.pool_address,
            token0=Web3.to_checksum_address(pos[2]),
            token1=Web3.to_checksum_address(pos[3]),
            fee=int(pos[4]),
            tick_lower=int(pos[5]),
            tick_upper=int(pos[6]),
            liquidity=int(pos[7]),
            tokens_owed0=int(pos[10]),
            tokens_owed1=int(pos[11]),
            pid=self.pool.pid,
            is_staked=False,
        )

    def decrease_collect_withdraw(self, position: PositionSnapshot, slot0: Slot0) -> TxResult:
        deadline = int(time.time()) + self.pool.deadline_seconds
        contract = self.masterchef if position.is_staked else self.npm
        calls = []
        if position.liquidity > 0:
            expected0, expected1 = amounts_for_liquidity(
                position.liquidity, slot0.sqrt_price_x96, position.tick_lower, position.tick_upper
            )
            min0 = self._apply_slippage(expected0)
            min1 = self._apply_slippage(expected1)
            calls.append(
                contract.encode_abi(
                    "decreaseLiquidity",
                    args=[(position.token_id, position.liquidity, min0, min1, deadline)],
                )
            )
        calls.append(
            contract.encode_abi(
                "collect",
                args=[(position.token_id, self.pool.bot_wallet, MAX_UINT128, MAX_UINT128)],
            )
        )
        if position.is_staked:
            calls.append(self.masterchef.encode_abi("withdraw", args=[position.token_id, self.pool.bot_wallet]))
        return self.executor.send(contract.functions.multicall(calls), "withdraw", gas=1_100_000)

    def approve_if_needed(self, token: str, spender: str, amount: int) -> TxResult | None:
        if amount <= 0:
            return None
        token_contract = self._erc20(token)
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        spender_cs = Web3.to_checksum_address(spender)
        allowance = int(token_contract.functions.allowance(wallet, spender_cs).call())
        if allowance >= amount:
            log.info(
                "approve skipped pool=%s chain=%s token=%s spender=%s required=%s allowance=%s",
                self.pool.name,
                self.pool.chain,
                Web3.to_checksum_address(token),
                spender_cs,
                amount,
                allowance,
            )
            return None
        log.info(
            "approve required pool=%s chain=%s token=%s spender=%s required=%s allowance=%s",
            self.pool.name,
            self.pool.chain,
            Web3.to_checksum_address(token),
            spender_cs,
            amount,
            allowance,
        )
        return self.executor.send(token_contract.functions.approve(spender_cs, MAX_UINT256), "approve", gas=100000)

    def swap(self, token_in: str, token_out: str, amount_in: int) -> TxResult | None:
        if amount_in <= 0:
            return None
        if self.executor.dry_run:
            return TxResult(
                tx_hash="dry-run:swap",
                dry_run=True,
                metadata={
                    "label": "swap",
                    "token_in": Web3.to_checksum_address(token_in),
                    "token_out": Web3.to_checksum_address(token_out),
                    "amount_in": str(int(amount_in)),
                },
            )
        from .swapper import V3Swapper

        try:
            from latest_farms.config import RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_URLS_2

        swapper = V3Swapper(self.pool.chain, RPC_URLS_2.get(self.pool.chain))
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        if hasattr(swapper, "get_swap_routes"):
            quotes = swapper.get_swap_routes(
                token_in,
                token_out,
                amount_in,
                wallet,
                self.pool.slippage_bps,
            )
        else:  # pragma: no cover - compatibility for older test doubles
            best_quote = swapper.get_best_swap_route(token_in, token_out, amount_in, wallet, self.pool.slippage_bps)
            quotes = [best_quote] if best_quote else []
        if not quotes:
            return None
        simulation_errors = []
        rejected_reasons = []
        for quote in quotes:
            log_block(
                log,
                logging.INFO,
                "swap quote selected",
                pool_context(self.pool),
                {
                    "stage": "quote",
                    "action": "swap",
                    "provider": quote.get("provider"),
                    "route": quote.get("route_display"),
                    "token_in": Web3.to_checksum_address(token_in),
                    "token_out": Web3.to_checksum_address(token_out),
                    "amount_in_raw": amount_in,
                    "quote_buy_amount_raw": quote.get("buyAmount"),
                    "price_impact": quote.get("price_impact"),
                    "allowance_target": quote.get("allowanceTarget"),
                    "estimated_gas": quote.get("estimatedGas"),
                },
            )
            try:
                price_impact = float(quote.get("price_impact", 0) or 0)
            except (TypeError, ValueError):
                price_impact = 0.0
            if price_impact > self.pool.max_swap_price_impact_pct:
                reason = f"price impact {price_impact} exceeds cap {self.pool.max_swap_price_impact_pct}"
                rejected_reasons.append(f"{quote.get('provider')}: {reason}")
                log_block(
                    log,
                    logging.WARNING,
                    "swap quote rejected",
                    pool_context(self.pool),
                    {
                        "stage": "quote",
                        "status": "REJECTED",
                        "provider": quote.get("provider"),
                        "route": quote.get("route_display"),
                        "reason": reason,
                        "next_action": "try next swap route",
                    },
                )
                continue
            dust_result = self._dust_output_result(token_out, quote)
            if dust_result:
                return dust_result
            allowance_target = quote.get("allowanceTarget")
            if allowance_target:
                self.approve_if_needed(token_in, allowance_target, amount_in)
            tx = {
                "to": Web3.to_checksum_address(quote["to"]),
                "data": quote["data"],
                "value": self._quote_value_int(quote.get("value", 0)),
            }
            metadata = {
                "label": "swap",
                "provider": quote.get("provider"),
                "route_display": quote.get("route_display"),
                "allowance_target": quote.get("allowanceTarget"),
                "estimated_gas": quote.get("estimatedGas"),
                "token_in": Web3.to_checksum_address(token_in),
                "token_out": Web3.to_checksum_address(token_out),
                "amount_in": str(int(amount_in)),
                "quote_buy_amount": str(int(quote.get("buyAmount") or 0)),
                "price_impact": quote.get("price_impact"),
            }
            simulation_ok, simulation_reason = self._simulate_swap(tx, metadata)
            if not simulation_ok:
                simulation_errors.append(f"{quote.get('provider')}: {simulation_reason}")
                continue
            # Web3 fallback helpers cannot carry data reliably across versions, so
            # build the raw transaction through the account API.
            return self._send_raw_swap(tx, metadata=metadata)
        if simulation_errors:
            return TxResult(
                tx_hash="failed:swap-simulation",
                status="FAILED",
                metadata={
                    "label": "swap",
                    "token_in": Web3.to_checksum_address(token_in),
                    "token_out": Web3.to_checksum_address(token_out),
                    "amount_in": str(int(amount_in)),
                    "error": "all swap routes failed simulation",
                    "simulation_errors": simulation_errors,
                    "quote_rejections": rejected_reasons,
                },
            )
        return None

    def mint(self, plan) -> tuple[TxResult, int | None]:
        self.approve_if_needed(self.pool.token0_address, self.npm_address, plan.amount0_desired)
        self.approve_if_needed(self.pool.token1_address, self.npm_address, plan.amount1_desired)
        deadline = int(time.time()) + self.pool.deadline_seconds
        min0 = self._apply_slippage(plan.amount0_desired)
        min1 = self._apply_slippage(plan.amount1_desired)
        params = (
            self.pool.token0_address,
            self.pool.token1_address,
            int(self.pool.fee),
            int(plan.new_tick_lower),
            int(plan.new_tick_upper),
            int(plan.amount0_desired),
            int(plan.amount1_desired),
            min0,
            min1,
            Web3.to_checksum_address(self.pool.bot_wallet),
            deadline,
        )
        log_block(
            log,
            logging.INFO,
            "mint start",
            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
            {
                "stage": "mint_start",
                "action": "mint",
                "token0": self.pool.token0_address,
                "token1": self.pool.token1_address,
                "fee": int(self.pool.fee),
                "tick_lower": int(plan.new_tick_lower),
                "tick_upper": int(plan.new_tick_upper),
                "amount0_desired_raw": int(plan.amount0_desired),
                "amount1_desired_raw": int(plan.amount1_desired),
                "amount0_min_raw": min0,
                "amount1_min_raw": min1,
                "deadline": deadline,
            },
        )
        result = self.executor.send(self.npm.functions.mint(params), "mint", gas=700000)
        if result.dry_run:
            return result, None
        receipt = self._mint_receipt_from_result(result)
        if receipt is None:
            log_block(
                log,
                logging.WARNING,
                "mint receipt unavailable",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
                {
                    "stage": "mint_receipt",
                    "status": result.status,
                    "tx_hash": result.tx_hash,
                    "signed_tx_hash": result.metadata.get("signed_tx_hash") if result.metadata else None,
                    "next_action": "journal recovery will try receipt lookup",
                },
            )
            return result, None
        if int(receipt.get("status", 0)) != 1:
            result.metadata["error"] = "mint transaction reverted"
            log_block(
                log,
                logging.WARNING,
                "mint reverted",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
                {
                    "stage": "mint_receipt",
                    "status": "REVERTED",
                    "tx_hash": result.tx_hash,
                    "block": receipt.get("blockNumber"),
                    "reason": "execution reverted",
                },
            )
            return result, None
        token_id = self._new_token_id_from_mint_receipt(receipt)
        if token_id is None:
            log_block(
                log,
                logging.WARNING,
                "mint validation",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
                {
                    "stage": "mint_receipt",
                    "status": "TOKEN_ID_MISSING",
                    "tx_hash": result.tx_hash,
                    "block": receipt.get("blockNumber"),
                    "reason": "IncreaseLiquidity token id not found",
                },
            )
            return result, None
        receipt_block = int(receipt.get("blockNumber") or 0)
        log_block(
            log,
            logging.INFO,
            "mint receipt",
            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
            {
                "stage": "mint_receipt",
                "status": "SUCCESS",
                "tx_hash": self._hex_value(receipt.get("transactionHash")) or result.tx_hash,
                "block": receipt_block,
            },
        )
        validation = self._validate_minted_position_detail(
            token_id,
            plan,
            receipt_block=receipt_block,
        )
        if validation.status == "VALID_WITH_RANGE_WARNING":
            result.metadata["mint_validation_warning"] = validation.reason
            log_block(
                log,
                logging.WARNING,
                "mint validation warning",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
                {
                    "stage": "mint_validation",
                    "validation_status": validation.status,
                    "reason": validation.reason,
                    "rpc": validation.rpc_label,
                    "expected_range": [int(plan.new_tick_lower), int(plan.new_tick_upper)],
                    "actual_range": (
                        [validation.position.tick_lower, validation.position.tick_upper]
                        if validation.position
                        else None
                    ),
                    "actual_owner": validation.position.owner if validation.position else None,
                    "liquidity": validation.position.liquidity if validation.position else None,
                    "next_action": "stake anyway because owner/pool/liquidity are valid",
                },
            )
        if not validation.can_stake:
            result.metadata["error"] = f"mint receipt token validation failed: {validation.reason}"
            log_block(
                log,
                logging.WARNING,
                "mint validation result",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
                {
                    "stage": "mint_validation",
                    "validation_status": validation.status,
                    "reason": validation.reason,
                    "rpc": validation.rpc_label,
                    "next_action": "mark recovery required",
                },
            )
            return result, None
        log_block(
            log,
            logging.INFO,
            "mint validation result",
            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
            {
                "stage": "mint_validation",
                "validation_status": validation.status,
                "reason": validation.reason,
                "rpc": validation.rpc_label,
                "actual_owner": validation.position.owner if validation.position else None,
                "actual_range": (
                    [validation.position.tick_lower, validation.position.tick_upper] if validation.position else None
                ),
                "liquidity": validation.position.liquidity if validation.position else None,
                "next_action": "stake minted token",
            },
        )
        if result.status in {"BROADCAST_UNKNOWN", "PENDING"}:
            result.status = "RECOVERED"
            receipt_hash = self._hex_value(receipt.get("transactionHash"))
            if self._normalize_tx_hash_for_rpc(receipt_hash):
                result.tx_hash = receipt_hash
            effective_gas_price = int(receipt.get("effectiveGasPrice") or 0)
            result.gas_used = int(receipt.get("gasUsed") or 0)
            result.gas_price_gwei = (
                float(Web3.from_wei(effective_gas_price, "gwei")) if effective_gas_price else 0.0
            )
            result.metadata["receipt_block"] = receipt_block
            result.metadata["recovered_from_receipt"] = True
        return result, token_id

    def _mint_receipt_from_result(self, result: TxResult):
        candidates = [result.tx_hash]
        signed_hash = result.metadata.get("signed_tx_hash") if result.metadata else None
        if signed_hash and signed_hash not in candidates:
            candidates.append(signed_hash)
        for candidate in candidates:
            normalized = self._normalize_tx_hash_for_rpc(candidate)
            if not normalized:
                continue
            try:
                return self.w3.eth.get_transaction_receipt(normalized)
            except TransactionNotFound:
                continue
            except Exception as exc:
                log.warning("could not recover mint receipt pool=%s tx=%s: %s", self.pool.name, normalized, exc)
        return None

    def _new_token_id_from_mint_receipt(self, receipt) -> int | None:
        topic = INCREASE_LIQUIDITY_TOPIC.lower()
        npm_address = self.npm_address.lower()
        candidates = []
        ignored_non_npm = 0
        for ev in receipt.get("logs", []):
            topics = ev.get("topics") or []
            if len(topics) < 2:
                continue
            if self._hex_value(topics[0]).lower() != topic:
                continue
            log_address = str(ev.get("address") or "").lower()
            if log_address != npm_address:
                ignored_non_npm += 1
                continue
            try:
                candidates.append(int(self._hex_value(topics[1]), 16))
            except ValueError:
                continue
        selected = candidates[0] if candidates else None
        log_block(
            log,
            logging.INFO,
            "mint receipt parse",
            pool_context(self.pool, new_token_id=selected),
            {
                "stage": "mint_receipt_parse",
                "tx_hash": self._hex_value(receipt.get("transactionHash")),
                "npm_address": self.npm_address,
                "candidate_count": len(candidates),
                "selected_token_id": selected,
                "ignored_non_npm_logs": ignored_non_npm,
                "status": "TOKEN_ID_FOUND" if selected is not None else "TOKEN_ID_MISSING",
            },
        )
        return selected

    def _minted_position_matches_plan(self, token_id: int, plan) -> bool:
        return self._validate_minted_position(token_id, plan)[0]

    def _validate_minted_position(
        self,
        token_id: int,
        plan,
        receipt_block: int | None = None,
        attempts: int = 6,
        sleep_seconds: float = 10.0,
    ) -> tuple[bool, str | None]:
        result = self._validate_minted_position_detail(
            token_id,
            plan,
            receipt_block=receipt_block,
            attempts=attempts,
            sleep_seconds=sleep_seconds,
        )
        return result.can_stake, result.reason

    def _validate_minted_position_detail(
        self,
        token_id: int,
        plan,
        receipt_block: int | None = None,
        attempts: int = 6,
        sleep_seconds: float = 10.0,
    ) -> MintValidationResult:
        primary_attempts = max(1, int(attempts))
        fallback_attempts = max(1, min(2, primary_attempts))
        fallback_sleep = min(2.0, float(sleep_seconds))
        sources = self._mint_validation_rpc_sources()
        last_result = MintValidationResult("READ_FAILED", "validation did not run")
        for source_index, (rpc_label, validation_w3) in enumerate(sources):
            source_attempts = primary_attempts if source_index == 0 else fallback_attempts
            source_sleep = sleep_seconds if source_index == 0 else fallback_sleep
            if source_index == 0:
                self._wait_until_receipt_block_visible(receipt_block, attempts=source_attempts, sleep_seconds=source_sleep)
            for attempt in range(1, source_attempts + 1):
                result = self._validate_minted_position_once(token_id, plan, validation_w3, rpc_label)
                last_result = result
                if result.can_stake:
                    if attempt > 1 or source_index > 0:
                        log_block(
                            log,
                            logging.INFO,
                            "mint validation retry",
                            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
                            self._mint_validation_log_fields(
                                result,
                                plan,
                                attempt=attempt,
                                max_attempts=source_attempts,
                                next_action="stake minted token",
                            ),
                        )
                    return result
                log_block(
                    log,
                    logging.WARNING,
                    "mint validation retry",
                    pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
                    self._mint_validation_log_fields(
                        result,
                        plan,
                        attempt=attempt,
                        max_attempts=source_attempts,
                        next_action=(
                            "retry same RPC"
                            if attempt < source_attempts
                            else ("try next RPC fallback" if source_index < len(sources) - 1 else "mark recovery required")
                        ),
                    ),
                )
                if attempt < source_attempts:
                    time.sleep(source_sleep)
        log_block(
            log,
            logging.WARNING,
            "mint validation result",
            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None), new_token_id=token_id),
            self._mint_validation_log_fields(
                last_result,
                plan,
                attempt=None,
                max_attempts=None,
                next_action="mark recovery required",
            ),
        )
        return last_result

    def _validate_minted_position_once(self, token_id: int, plan, validation_w3=None, rpc_label: str = "primary") -> MintValidationResult:
        try:
            position = self._read_npm_position_with_w3(validation_w3 or self.w3, int(token_id), owner=self.pool.bot_wallet)
        except Exception as exc:
            return MintValidationResult("READ_FAILED", f"position read failed: {exc}", rpc_label=rpc_label)
        owner = position.owner.lower()
        staking_owner = self.staking_owner_address()
        valid_owners = {self.pool.bot_wallet.lower()}
        if staking_owner:
            valid_owners.add(staking_owner.lower())
        if owner not in valid_owners:
            return MintValidationResult("INVALID_OWNER", f"owner mismatch: owner={position.owner}", position, rpc_label)
        if not self._matches_pool(position.token0, position.token1, position.fee):
            return MintValidationResult(
                "INVALID_POOL",
                "pool mismatch: "
                f"actual_token0={position.token0} actual_token1={position.token1} actual_fee={position.fee} "
                f"expected_token0={self.pool.token0_address} expected_token1={self.pool.token1_address} "
                f"expected_fee={self.pool.fee}",
                position,
                rpc_label,
            )
        if int(position.liquidity) <= 0:
            return MintValidationResult("INVALID_LIQUIDITY", "zero liquidity", position, rpc_label)
        if int(position.tick_lower) != int(plan.new_tick_lower) or int(position.tick_upper) != int(plan.new_tick_upper):
            return MintValidationResult(
                "VALID_WITH_RANGE_WARNING",
                "range mismatch: "
                f"expected=({plan.new_tick_lower},{plan.new_tick_upper}) "
                f"actual=({position.tick_lower},{position.tick_upper})",
                position,
                rpc_label,
            )
        return MintValidationResult("VALID", None, position, rpc_label)

    def _read_npm_position_with_w3(self, w3: Web3, token_id: int, owner: str | None = None) -> PositionSnapshot:
        if w3 is self.w3:
            return self.read_npm_position(token_id, owner=owner)
        npm = w3.eth.contract(address=self.npm_address, abi=NPM_ABI)
        pos = npm.functions.positions(int(token_id)).call()
        actual_owner = owner
        try:
            actual_owner = npm.functions.ownerOf(int(token_id)).call()
        except Exception:
            pass
        return PositionSnapshot(
            token_id=int(token_id),
            owner=Web3.to_checksum_address(actual_owner or self.pool.bot_wallet),
            pool_address=self.pool.pool_address,
            token0=Web3.to_checksum_address(pos[2]),
            token1=Web3.to_checksum_address(pos[3]),
            fee=int(pos[4]),
            tick_lower=int(pos[5]),
            tick_upper=int(pos[6]),
            liquidity=int(pos[7]),
            tokens_owed0=int(pos[10]),
            tokens_owed1=int(pos[11]),
            pid=self.pool.pid,
            is_staked=False,
        )

    def _mint_validation_rpc_sources(self) -> list[tuple[str, Web3]]:
        sources = [("primary", self.w3)]
        try:
            from latest_farms.config import RPC_BACKUP_LIST, RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_BACKUP_LIST, RPC_URLS_2

        current_url = getattr(getattr(self.w3, "provider", None), "endpoint_uri", None)
        seen = {current_url} if current_url else set()
        rpc_urls = [RPC_URLS_2.get(self.pool.chain)] + RPC_BACKUP_LIST.get(self.pool.chain, [])
        fallback_index = 1
        for url in [item for item in rpc_urls if item]:
            if url in seen:
                continue
            seen.add(url)
            sources.append((f"fallback-{fallback_index}:{self._rpc_label(url)}", self._web3_for_rpc(url)))
            fallback_index += 1
        return sources

    def _mint_validation_log_fields(
        self,
        result: MintValidationResult,
        plan,
        attempt: int | None,
        max_attempts: int | None,
        next_action: str,
    ) -> dict:
        position = result.position
        return {
            "stage": "mint_validation",
            "attempt": attempt,
            "max_attempts": max_attempts,
            "rpc": result.rpc_label,
            "validation_status": result.status,
            "reason": result.reason,
            "actual_owner": position.owner if position else None,
            "actual_token0": position.token0 if position else None,
            "actual_token1": position.token1 if position else None,
            "actual_fee": position.fee if position else None,
            "expected_range": [int(plan.new_tick_lower), int(plan.new_tick_upper)],
            "actual_range": [position.tick_lower, position.tick_upper] if position else None,
            "liquidity": position.liquidity if position else None,
            "next_action": next_action,
        }

    def _wait_until_receipt_block_visible(
        self,
        receipt_block: int | None,
        attempts: int,
        sleep_seconds: float,
    ) -> None:
        if not receipt_block:
            return
        for attempt in range(1, max(1, attempts) + 1):
            try:
                latest_block = int(self.w3.eth.block_number)
            except Exception as exc:
                log.warning(
                    "could not read latest block before mint validation pool=%s chain=%s receipt_block=%s error=%s",
                    self.pool.name,
                    self.pool.chain,
                    receipt_block,
                    exc,
                )
                return
            if latest_block >= int(receipt_block):
                return
            log.warning(
                "waiting for rpc to reach mint receipt block pool=%s chain=%s latest_block=%s receipt_block=%s attempt=%s/%s",
                self.pool.name,
                self.pool.chain,
                latest_block,
                receipt_block,
                attempt,
                attempts,
            )
            if attempt < attempts:
                time.sleep(sleep_seconds)

    def stake(self, token_id: int) -> TxResult:
        data = self.w3.codec.encode(["uint256"], [int(self.pool.pid or 0)])
        log_block(
            log,
            logging.INFO,
            "stake start",
            pool_context(self.pool, new_token_id=token_id),
            {
                "stage": "stake_start",
                "action": "stake",
                "pid": self.pool.pid,
                "masterchef": self.masterchef_address,
            },
        )
        return self.executor.send(
            self.npm.functions.safeTransferFrom(
                Web3.to_checksum_address(self.pool.bot_wallet),
                self.masterchef_address,
                int(token_id),
                data,
            ),
            "stake",
            gas=600000,
        )

    def burn_if_empty_and_owned(self, token_id: int) -> TxResult | None:
        if not self.pool.execute_burn:
            return None
        pos = self.npm.functions.positions(int(token_id)).call()
        if int(pos[7]) != 0 or int(pos[10]) != 0 or int(pos[11]) != 0:
            return None
        owner = Web3.to_checksum_address(self.npm.functions.ownerOf(int(token_id)).call())
        if owner != Web3.to_checksum_address(self.pool.bot_wallet):
            return None
        return self.executor.send(self.npm.functions.burn(int(token_id)), "burn", gas=180000)

    def _simulate_swap(self, tx_payload: dict, metadata: dict) -> tuple[bool, str | None]:
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        call_tx = {
            "from": wallet,
            "to": tx_payload["to"],
            "data": tx_payload["data"],
            "value": tx_payload["value"],
        }
        try:
            self.w3.eth.call(call_tx)
        except Exception as exc:
            reason = str(exc)
            log_block(
                log,
                logging.WARNING,
                "swap simulation",
                pool_context(self.pool),
                {
                    "stage": "swap_simulation",
                    "status": "FAILED",
                    "provider": metadata.get("provider"),
                    "route": metadata.get("route_display"),
                    "token_in": metadata.get("token_in"),
                    "token_out": metadata.get("token_out"),
                    "amount_in_raw": metadata.get("amount_in"),
                    "to": tx_payload.get("to"),
                    "value": tx_payload.get("value"),
                    "reason": reason,
                    "next_action": "try next swap route",
                },
            )
            return False, reason
        log_block(
            log,
            logging.INFO,
            "swap simulation",
            pool_context(self.pool),
            {
                "stage": "swap_simulation",
                "status": "PASSED",
                "provider": metadata.get("provider"),
                "route": metadata.get("route_display"),
                "token_in": metadata.get("token_in"),
                "token_out": metadata.get("token_out"),
                "amount_in_raw": metadata.get("amount_in"),
                "to": tx_payload.get("to"),
                "value": tx_payload.get("value"),
                "next_action": "broadcast simulated route",
            },
        )
        return True, None

    def _send_raw_swap(self, tx_payload: dict, metadata: dict | None = None) -> TxResult:
        from .evm import get_chain_id, get_gas_params, validate_gas_cap

        try:
            from latest_farms.config import RPC_BACKUP_LIST, RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_BACKUP_LIST, RPC_URLS_2

        metadata = {"label": "swap", **(metadata or {})}
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        gas_policy = self.executor.gas_policy()
        gas_params = get_gas_params(self.w3, self.pool.chain, action="swap", policy=gas_policy)
        cap = gas_policy.max_fee_gwei if gas_policy.max_fee_gwei is not None else self.pool.max_gas_gwei
        validate_gas_cap(gas_params, cap)
        gas_limit = 900000
        provider_estimated_gas = self._safe_int(metadata.get("estimated_gas"))
        rpc_estimated_gas = 0
        gas_source = "fallback"
        try:
            rpc_estimated_gas = int(
                self.w3.eth.estimate_gas(
                    {
                        "from": wallet,
                        "to": tx_payload["to"],
                        "data": tx_payload["data"],
                        "value": tx_payload["value"],
                    }
                )
            )
        except Exception as exc:
            metadata["rpc_estimate_error"] = str(exc)
        best_estimate = max(provider_estimated_gas, rpc_estimated_gas)
        if best_estimate > 0:
            gas_limit = max(120000, int(best_estimate * 1.3))
            if provider_estimated_gas > 0 and rpc_estimated_gas > 0:
                gas_source = "provider+rpc"
            elif rpc_estimated_gas > 0:
                gas_source = "rpc"
            else:
                gas_source = "provider"
        tx = {
            "from": wallet,
            "to": tx_payload["to"],
            "data": tx_payload["data"],
            "value": tx_payload["value"],
            "nonce": self.executor._next_nonce(),
            "gas": gas_limit,
            **gas_params,
            "chainId": get_chain_id(self.pool.chain),
        }
        safe_tx_metadata = {
            "chain_id": tx["chainId"],
            "nonce": tx["nonce"],
            "gas_limit": gas_limit,
            "gas_source": gas_source,
            "provider_estimated_gas": provider_estimated_gas or None,
            "rpc_estimated_gas": rpc_estimated_gas or None,
            "to": tx["to"],
            "value": str(tx["value"]),
            "data_length": len(str(tx["data"] or "")),
            "max_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei")),
            "max_priority_fee_per_gas_gwei": float(
                Web3.from_wei(gas_params["maxPriorityFeePerGas"], "gwei")
            ),
        }
        metadata = {**metadata, **safe_tx_metadata}
        signed = self.executor.sign_transaction(wallet, tx)
        signed_tx_hash = Web3.keccak(signed.raw_transaction).hex()
        if not signed_tx_hash.startswith("0x"):
            signed_tx_hash = "0x" + signed_tx_hash
        metadata["signed_tx_hash"] = signed_tx_hash
        log_block(
            log,
            logging.INFO,
            "swap broadcast",
            pool_context(self.pool),
            {
                "stage": "swap_broadcast",
                "action": "swap",
                "token_in": metadata.get("token_in"),
                "token_out": metadata.get("token_out"),
                "amount_in_raw": metadata.get("amount_in"),
                "to": tx["to"],
                "value": tx["value"],
                "nonce": tx["nonce"],
                "gas_limit": gas_limit,
                "gas_source": metadata.get("gas_source"),
                "provider_estimated_gas": metadata.get("provider_estimated_gas"),
                "rpc_estimated_gas": metadata.get("rpc_estimated_gas"),
                "max_fee_gwei": f"{metadata['max_fee_per_gas_gwei']:.9f}",
                "priority_fee_gwei": f"{metadata['max_priority_fee_per_gas_gwei']:.9f}",
                "data_length": metadata["data_length"],
                "signed_tx_hash": signed_tx_hash,
            },
        )
        tx_hash = None
        broadcast_w3 = self.w3
        broadcast_errors = []
        rpc_urls = [RPC_URLS_2.get(self.pool.chain)] + RPC_BACKUP_LIST.get(self.pool.chain, [])
        rpc_urls = [url for url in rpc_urls if url]
        attempts = [("primary", self.w3)]
        for index, url in enumerate(rpc_urls[1:], start=1):
            attempts.append((f"backup-{index}:{self._rpc_label(url)}", self._web3_for_rpc(url)))
        for rpc_label, candidate_w3 in attempts:
            try:
                tx_hash = candidate_w3.eth.send_raw_transaction(signed.raw_transaction)
                broadcast_w3 = candidate_w3
                metadata["broadcast_rpc"] = rpc_label
                tx_hash_hex = tx_hash.hex()
                if not tx_hash_hex.startswith("0x"):
                    tx_hash_hex = "0x" + tx_hash_hex
                metadata["broadcast_tx_hash"] = tx_hash_hex
                log_block(
                    log,
                    logging.INFO,
                    "swap broadcast accepted",
                    pool_context(self.pool),
                    {
                        "stage": "swap_broadcast",
                        "status": "ACCEPTED",
                        "tx_hash": tx_hash_hex,
                        "signed_tx_hash": signed_tx_hash,
                        "rpc": rpc_label,
                        "nonce": metadata.get("nonce"),
                    },
                )
                break
            except Exception as exc:
                broadcast_errors.append(f"{rpc_label}: {exc}")
                log_block(
                    log,
                    logging.WARNING,
                    "swap broadcast failed",
                    pool_context(self.pool),
                    {
                        "stage": "swap_broadcast",
                        "status": "FAILED",
                        "rpc": rpc_label,
                        "token_in": metadata.get("token_in"),
                        "token_out": metadata.get("token_out"),
                        "amount_in_raw": metadata.get("amount_in"),
                        "reason": exc,
                        "next_action": "try next RPC fallback",
                    },
                )
        if tx_hash is None:
            return TxResult(
                tx_hash="failed:swap-broadcast",
                status="FAILED",
                metadata={
                    **metadata,
                    "error": "; ".join(broadcast_errors[-3:]) or "swap broadcast failed",
                    "broadcast_errors": broadcast_errors,
                },
            )
        try:
            receipt = broadcast_w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        except TimeExhausted as exc:
            tx_hash_hex = tx_hash.hex()
            if not tx_hash_hex.startswith("0x"):
                tx_hash_hex = "0x" + tx_hash_hex
            log_block(
                log,
                logging.WARNING,
                "swap receipt timeout",
                pool_context(self.pool),
                {
                    "stage": "swap_receipt",
                    "status": "PENDING",
                    "tx_hash": tx_hash_hex,
                    "signed_tx_hash": signed_tx_hash,
                    "rpc": metadata.get("broadcast_rpc"),
                    "reason": exc,
                    "next_action": "recovery will inspect pending swap",
                },
            )
            return TxResult(
                tx_hash=tx_hash_hex,
                status="PENDING",
                metadata={
                    **metadata,
                    "error": str(exc),
                },
            )
        if receipt["status"] != 1:
            tx_hash_hex = tx_hash.hex()
            if not tx_hash_hex.startswith("0x"):
                tx_hash_hex = "0x" + tx_hash_hex
            gas_used = int(receipt.get("gasUsed") or 0)
            gas_used_ratio = (gas_used / gas_limit) if gas_limit else 0.0
            revert_classification = self._classify_swap_revert(gas_used, gas_limit)
            error_reason = f"swap transaction reverted: {revert_classification}"
            log_block(
                log,
                logging.WARNING,
                "swap reverted",
                pool_context(self.pool),
                {
                    "stage": "swap_receipt",
                    "status": "SWAP_BLOCKED",
                    "tx_hash": tx_hash_hex,
                    "block": receipt.get("blockNumber"),
                    "provider": metadata.get("provider"),
                    "route": metadata.get("route_display"),
                    "gas_used": gas_used,
                    "gas_limit": gas_limit,
                    "gas_used_ratio": f"{gas_used_ratio:.4f}",
                    "revert_classification": revert_classification,
                    "reason": "execution reverted",
                    "next_action": "recovery will retry swap from reservation",
                },
            )
            return TxResult(
                tx_hash=tx_hash_hex,
                status="FAILED",
                gas_used=gas_used,
                metadata={
                    **metadata,
                    "error": error_reason,
                    "revert_classification": revert_classification,
                    "gas_used_ratio": gas_used_ratio,
                },
            )
        effective_gas_price = int(receipt.get("effectiveGasPrice") or gas_params["maxFeePerGas"])
        tx_hash_hex = tx_hash.hex()
        if not tx_hash_hex.startswith("0x"):
            tx_hash_hex = "0x" + tx_hash_hex
        gas_used = int(receipt["gasUsed"])
        gas_price_gwei = float(Web3.from_wei(effective_gas_price, "gwei"))
        receipt_block = int(receipt["blockNumber"])
        log_block(
            log,
            logging.INFO,
            "swap receipt",
            pool_context(self.pool),
            {
                "stage": "swap_receipt",
                "status": receipt.get("status"),
                "tx_hash": tx_hash_hex,
                "block": receipt_block,
                "gas_used": gas_used,
                "effective_gas_price_gwei": f"{gas_price_gwei:.9f}",
                "rpc": metadata.get("broadcast_rpc"),
            },
        )
        return TxResult(
            tx_hash=tx_hash_hex,
            gas_used=gas_used,
            gas_price_gwei=gas_price_gwei,
            metadata={
                **metadata,
                "receipt_block": receipt_block,
                "gas_limit": gas_limit,
                "max_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei")),
                "max_priority_fee_per_gas_gwei": float(
                    Web3.from_wei(gas_params["maxPriorityFeePerGas"], "gwei")
                ),
            },
        )

    @staticmethod
    def _classify_swap_revert(gas_used: int, gas_limit: int) -> str:
        if gas_limit > 0 and gas_used / gas_limit >= 0.98:
            return "OUT_OF_GAS_LIKELY"
        return "ROUTE_OR_SLIPPAGE_REVERT"

    @staticmethod
    def _quote_value_int(value) -> int:
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        text = str(value).strip()
        if not text:
            return 0
        return int(text, 0)

    @staticmethod
    def _safe_int(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _web3_for_rpc(self, url: str) -> Web3:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        if self.pool.chain.upper() == "BNB":
            try:
                from web3.middleware import ExtraDataToPOAMiddleware

                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware

                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        return w3

    def _rpc_label(self, url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc or "unknown-rpc"

    @staticmethod
    def _normalize_tx_hash_for_rpc(value: str | None) -> str | None:
        if not value:
            return None
        text = str(value).strip()
        bare = text[2:] if text.lower().startswith("0x") else text
        if len(bare) != 64:
            return None
        try:
            int(bare, 16)
        except ValueError:
            return None
        return "0x" + bare.lower()

    @staticmethod
    def _hex_value(value) -> str:
        if value is None:
            return "0x"
        if isinstance(value, str):
            text = value
        elif hasattr(value, "hex"):
            text = value.hex()
        else:
            text = str(value)
        if text and not text.startswith("0x"):
            text = "0x" + text
        return text

    def _dust_output_result(self, token_out: str, quote: dict) -> TxResult | None:
        if self.pool.min_swap_output_usd <= 0:
            return None
        buy_amount = int(quote.get("buyAmount") or 0)
        if buy_amount <= 0:
            return None
        price = token_price_usd(self.pool.chain, token_out, warnings=None)
        if price is None:
            return None
        decimals = int(self._erc20(token_out).functions.decimals().call())
        output_usd = (buy_amount / (10**decimals)) * price
        if output_usd >= self.pool.min_swap_output_usd:
            return None
        return TxResult(
            tx_hash="skipped:dust-swap-output",
            status="SKIPPED",
            metadata={
                "label": "swap",
                "reason": "dust output below threshold",
                "output_usd": output_usd,
                "threshold_usd": self.pool.min_swap_output_usd,
            },
        )

    def _erc20(self, token: str):
        return self.w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)

    def _matches_pool(self, token0: str, token1: str, fee: int) -> bool:
        return (
            token0.lower() == self.pool.token0_address.lower()
            and token1.lower() == self.pool.token1_address.lower()
            and int(fee) == int(self.pool.fee)
        )

    def _apply_slippage(self, amount: int) -> int:
        return max(0, int(amount * (10000 - self.pool.slippage_bps) / 10000))


class AerodromeV3GaugeAdapter(PancakeV3MasterChefAdapter):
    def __init__(self, w3: Web3, pool: PoolConfig, executor: TxExecutor):
        DexAdapter.__init__(self, w3, pool, executor)
        self.pool_contract = w3.eth.contract(address=pool.pool_address, abi=AERODROME_POOL_ABI)
        self.npm_address = Web3.to_checksum_address(pool.npm_address) if pool.npm_address else None
        self.npm = (
            w3.eth.contract(address=self.npm_address, abi=AERODROME_NPM_ABI)
            if self.npm_address
            else None
        )
        self.gauge_address = Web3.to_checksum_address(pool.staking_address) if pool.staking_address else None
        self.gauge = (
            w3.eth.contract(address=self.gauge_address, abi=AERODROME_GAUGE_ABI)
            if self.gauge_address
            else None
        )

    def discover_pool_metadata(self) -> PoolConfig:
        started_at = time.monotonic()
        anchor_block = int(self.w3.eth.block_number)
        metadata, routing_errors = self._read_aerodrome_pool_metadata(anchor_block)
        token0 = Web3.to_checksum_address(metadata["token0"])
        token1 = Web3.to_checksum_address(metadata["token1"])
        fee = int(metadata["fee"])
        tick_spacing = int(metadata["tick_spacing"])
        dec0, dec1 = self._read_token_decimals(token0, token1, anchor_block)

        db_npm = self._db_expected_npm()
        npm_address, npm_source = self._resolve_npm_address(
            metadata.get("nft"),
            routing_errors.get("nft"),
            db_npm,
            anchor_block,
        )
        gauge_address, gauge_source = self._resolve_gauge_address(
            metadata.get("gauge"),
            routing_errors.get("gauge"),
            anchor_block,
        )

        mismatches = self._configured_metadata_mismatches(
            token0=token0,
            token1=token1,
            fee=fee,
            tick_spacing=tick_spacing,
            npm_address=npm_address,
            gauge_address=gauge_address,
        )
        if db_npm and db_npm.lower() != npm_address.lower():
            mismatches.append("db_npm_address")
        if mismatches:
            log.warning(
                "AERODROME_METADATA_MISMATCH pool=%s fields=%s action=USE_RESOLVED_METADATA",
                self.pool.name,
                ",".join(mismatches),
            )

        resolved = PoolConfig(
            **{
                **self.pool.__dict__,
                "token0_address": token0,
                "token1_address": token1,
                "token0_decimals": dec0,
                "token1_decimals": dec1,
                "fee": fee,
                "tick_spacing": tick_spacing,
                "npm_address": npm_address,
                "staking_address": gauge_address,
            }
        )
        self.pool = resolved
        self.npm_address = npm_address
        self.npm = self.w3.eth.contract(address=npm_address, abi=AERODROME_NPM_ABI)
        self.gauge_address = gauge_address
        self.gauge = (
            self.w3.eth.contract(address=gauge_address, abi=AERODROME_GAUGE_ABI)
            if gauge_address
            else None
        )
        log.info(
            "aerodrome metadata resolved pool=%s anchor_block=%s npm_address=%s npm_source=%s "
            "gauge_address=%s gauge_source=%s token0=%s token1=%s fee=%s tick_spacing=%s "
            "configured_mismatches=%s metadata_duration_ms=%s",
            self.pool.name,
            anchor_block,
            npm_address,
            npm_source,
            gauge_address,
            gauge_source,
            token0,
            token1,
            fee,
            tick_spacing,
            mismatches,
            int((time.monotonic() - started_at) * 1000),
        )
        return resolved

    def _read_aerodrome_pool_metadata(self, anchor_block: int) -> tuple[dict, dict[str, Exception]]:
        signatures = [
            ("token0", "token0()(address)"),
            ("token1", "token1()(address)"),
            ("fee", "fee()(uint24)"),
            ("tick_spacing", "tickSpacing()(int24)"),
            ("gauge", "gauge()(address)"),
            ("nft", "nft()(address)"),
        ]
        multicall = W3Multicall(self.w3)
        for _, signature in signatures:
            multicall.add(W3Multicall.Call(self.pool.pool_address, signature))
        try:
            values = multicall.call(anchor_block)
            if len(values) != len(signatures):
                raise ValueError(f"expected {len(signatures)} metadata values, got {len(values)}")
            return dict(zip((name for name, _ in signatures), values)), {}
        except Exception as exc:
            log.warning(
                "Aerodrome metadata multicall unavailable pool=%s anchor_block=%s: %s",
                self.pool.name,
                anchor_block,
                exc,
            )

        metadata: dict = {}
        routing_errors: dict[str, Exception] = {}
        function_names = {
            "token0": "token0",
            "token1": "token1",
            "fee": "fee",
            "tick_spacing": "tickSpacing",
            "gauge": "gauge",
            "nft": "nft",
        }
        for field, function_name in function_names.items():
            try:
                function = getattr(self.pool_contract.functions, function_name)()
                metadata[field] = function.call(block_identifier=anchor_block)
            except Exception as field_exc:
                if field in {"gauge", "nft"}:
                    routing_errors[field] = field_exc
                    metadata[field] = None
                    continue
                raise RuntimeError(
                    f"AERODROME_METADATA_UNRESOLVED field={field} pool={self.pool.name}"
                ) from field_exc
        return metadata, routing_errors

    def _read_token_decimals(self, token0: str, token1: str, anchor_block: int) -> tuple[int, int]:
        multicall = W3Multicall(self.w3)
        multicall.add(W3Multicall.Call(token0, "decimals()(uint8)"))
        multicall.add(W3Multicall.Call(token1, "decimals()(uint8)"))
        try:
            values = multicall.call(anchor_block)
            return int(values[0]), int(values[1])
        except Exception as exc:
            log.warning("Aerodrome token decimals multicall unavailable pool=%s: %s", self.pool.name, exc)
            dec0 = self._erc20(token0).functions.decimals().call(block_identifier=anchor_block)
            dec1 = self._erc20(token1).functions.decimals().call(block_identifier=anchor_block)
            return int(dec0), int(dec1)

    def _resolve_npm_address(
        self,
        onchain_value,
        onchain_error: Exception | None,
        db_npm: str | None,
        anchor_block: int,
    ) -> tuple[str, str]:
        onchain = self._address_or_none(onchain_value)
        if onchain and self._has_contract_code(onchain, anchor_block):
            return onchain, "POOL_NFT"
        if onchain_error:
            log.warning("Aerodrome pool nft() unavailable pool=%s: %s", self.pool.name, onchain_error)
        elif onchain:
            log.warning("Aerodrome pool nft() returned address without bytecode pool=%s npm=%s", self.pool.name, onchain)

        if db_npm and self._has_contract_code(db_npm, anchor_block):
            return db_npm, "DB_FACTORY"
        configured = self._address_or_none(self.pool.npm_address)
        if configured and self._has_contract_code(configured, anchor_block):
            return configured, "CONFIG_FALLBACK"
        raise RuntimeError(f"NPM_METADATA_UNRESOLVED pool={self.pool.name}")

    def _resolve_gauge_address(
        self,
        onchain_value,
        onchain_error: Exception | None,
        anchor_block: int,
    ) -> tuple[str | None, str]:
        if onchain_error is None:
            onchain = self._address_or_none(onchain_value)
            if onchain is None and onchain_value and str(onchain_value).lower() != ZERO_ADDRESS:
                raise RuntimeError(f"GAUGE_METADATA_UNRESOLVED pool={self.pool.name} reason=invalid_address")
            if onchain is None:
                return None, "NONE"
            if not self._has_contract_code(onchain, anchor_block):
                raise RuntimeError(f"GAUGE_METADATA_UNRESOLVED pool={self.pool.name} reason=no_bytecode")
            return onchain, "POOL_GAUGE"

        log.warning("Aerodrome pool gauge() unavailable pool=%s: %s", self.pool.name, onchain_error)
        configured = self._address_or_none(self.pool.staking_address)
        if configured and self._has_contract_code(configured, anchor_block):
            return configured, "CONFIG_FALLBACK"
        raise RuntimeError(f"GAUGE_METADATA_UNRESOLVED pool={self.pool.name}")

    def _db_expected_npm(self) -> str | None:
        worker_config = getattr(self.executor, "worker_config", None)
        if not worker_config or not bool(getattr(worker_config, "use_db_position_cache", False)):
            return None
        try:
            try:
                from latest_farms.create_db import get_connection
            except ImportError:  # pragma: no cover
                from create_db import get_connection
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(
                    """
                    SELECT factory_address
                    FROM aerodrome_pool_info
                    WHERE chain = %s AND LOWER(pool_address) = LOWER(%s)
                    LIMIT 1
                    """,
                    (self.pool.chain, self.pool.pool_address),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()
                conn.close()
        except Exception as exc:
            log.warning("Aerodrome metadata DB cross-check unavailable pool=%s: %s", self.pool.name, exc)
            return None
        if not row or not row.get("factory_address"):
            return None
        factory = str(row["factory_address"]).lower()
        for configured_factory, npm_address in AERODROME_FACTORY_NPM_ADDRESSES.get(self.pool.chain, {}).items():
            if str(configured_factory).lower() == factory:
                return Web3.to_checksum_address(npm_address)
        log.warning(
            "Aerodrome factory has no NPM mapping pool=%s factory=%s",
            self.pool.name,
            row["factory_address"],
        )
        return None

    def _configured_metadata_mismatches(
        self,
        *,
        token0: str,
        token1: str,
        fee: int,
        tick_spacing: int,
        npm_address: str,
        gauge_address: str | None,
    ) -> list[str]:
        mismatches: list[str] = []
        address_fields = {
            "token0_address": (self.pool.token0_address, token0),
            "token1_address": (self.pool.token1_address, token1),
            "npm_address": (self.pool.npm_address, npm_address),
            "staking_address": (self.pool.staking_address, gauge_address),
        }
        for field, (configured, resolved) in address_fields.items():
            if configured and (not resolved or str(configured).lower() != str(resolved).lower()):
                mismatches.append(field)
        if self.pool.fee is not None and int(self.pool.fee) != int(fee):
            mismatches.append("fee")
        if self.pool.tick_spacing is not None and int(self.pool.tick_spacing) != int(tick_spacing):
            mismatches.append("tick_spacing")
        return mismatches

    def _has_contract_code(self, address: str, anchor_block: int) -> bool:
        try:
            return bool(self.w3.eth.get_code(Web3.to_checksum_address(address), anchor_block))
        except Exception as exc:
            log.warning("Aerodrome contract bytecode check failed pool=%s address=%s: %s", self.pool.name, address, exc)
            return False

    @staticmethod
    def _address_or_none(value) -> str | None:
        if not value or str(value).lower() == ZERO_ADDRESS:
            return None
        try:
            return Web3.to_checksum_address(value)
        except (TypeError, ValueError):
            return None

    def read_slot0(self) -> Slot0:
        res = self.pool_contract.functions.slot0().call()
        return Slot0(sqrt_price_x96=int(res[0]), tick=int(res[1]))

    def should_stake(self) -> bool:
        return self.gauge_address is not None

    def staking_owner_address(self) -> str | None:
        return self.gauge_address

    def stake_event_contract_address(self) -> str | None:
        return self.gauge_address

    def stake_event_topics(self) -> list[str]:
        return [AERODROME_GAUGE_DEPOSIT_TOPIC, AERODROME_GAUGE_WITHDRAW_TOPIC]

    def parse_stake_event(self, event) -> tuple[str, int] | None:
        topics = event.get("topics") or []
        if len(topics) < 3:
            return None
        topic0 = Web3.to_hex(topics[0]).lower()
        if topic0 == AERODROME_GAUGE_DEPOSIT_TOPIC.lower():
            action = "stake"
        elif topic0 == AERODROME_GAUGE_WITHDRAW_TOPIC.lower():
            action = "unstake"
        else:
            return None
        return action, int.from_bytes(topics[2], "big")

    def reward_token_address(self) -> str | None:
        if not self.gauge:
            return None
        try:
            return Web3.to_checksum_address(self.gauge.functions.rewardToken().call())
        except Exception:
            return None

    def read_staked_positions(self, token_ids: Iterable[int]) -> dict[int, PositionSnapshot]:
        out: dict[int, PositionSnapshot] = {}
        if not self.gauge:
            return out
        managed_wallets = [Web3.to_checksum_address(wallet) for wallet in self.pool.managed_wallets]
        ids = sorted({int(tid) for tid in token_ids})
        owners: dict[int, str] = {}
        signature = "stakedContains(address,uint256)(bool)"
        for start in range(0, len(ids), 150):
            batch = ids[start : start + 150]
            multicall = W3Multicall(self.w3)
            pairs: list[tuple[int, str]] = []
            for token_id in batch:
                for wallet in managed_wallets:
                    multicall.add(W3Multicall.Call(self.gauge_address, signature, (wallet, token_id)))
                    pairs.append((token_id, wallet))
            try:
                membership = multicall.call()
            except Exception as exc:
                log.warning("Aerodrome gauge membership batch failed pool=%s candidates=%s: %s", self.pool.name, len(batch), exc)
                continue
            for (token_id, wallet), contains in zip(pairs, membership):
                if contains and token_id not in owners:
                    owners[token_id] = wallet

        for token_id, position in self.read_npm_positions(owners, owners).items():
            if not self._matches_pool(position.token0, position.token1, position.fee):
                continue
            if int(position.liquidity) <= 0:
                continue
            position.owner = owners[token_id]
            position.is_staked = True
            out[token_id] = position
        return out

    def _batch_npm_position_values(self, token_ids: Iterable[int]) -> dict[int, tuple]:
        ordered = sorted({int(value) for value in token_ids})
        out: dict[int, tuple] = {}
        signature = (
            "positions(uint256)"
            "(uint96,address,address,address,int24,int24,int24,uint128,uint256,uint256,uint128,uint128)"
        )
        for start in range(0, len(ordered), 150):
            batch = ordered[start : start + 150]
            multicall = W3Multicall(self.w3)
            for token_id in batch:
                multicall.add(W3Multicall.Call(self.npm_address, signature, token_id))
            try:
                values = multicall.call()
            except Exception as exc:
                log.warning("Aerodrome NPM position batch failed pool=%s candidates=%s: %s", self.pool.name, len(batch), exc)
                continue
            for token_id, value in zip(batch, values):
                if value:
                    out[token_id] = value
        return out

    def read_npm_positions(
        self,
        token_ids: Iterable[int],
        owners: dict[int, str] | None = None,
    ) -> dict[int, PositionSnapshot]:
        owners = owners or {}
        out: dict[int, PositionSnapshot] = {}
        for token_id, pos in self._batch_npm_position_values(token_ids).items():
            out[token_id] = PositionSnapshot(
                token_id=token_id,
                owner=Web3.to_checksum_address(owners.get(token_id) or self.pool.bot_wallet),
                pool_address=self.pool.pool_address,
                token0=Web3.to_checksum_address(pos[2]),
                token1=Web3.to_checksum_address(pos[3]),
                fee=int(pos[4]),
                tick_lower=int(pos[5]),
                tick_upper=int(pos[6]),
                liquidity=int(pos[7]),
                tokens_owed0=int(pos[10]),
                tokens_owed1=int(pos[11]),
                pid=None,
                is_staked=False,
            )
        return out

    def read_npm_position(self, token_id: int, owner: str | None = None) -> PositionSnapshot:
        pos = self.npm.functions.positions(int(token_id)).call()
        actual_owner = owner
        if actual_owner is None:
            try:
                actual_owner = self.npm.functions.ownerOf(int(token_id)).call()
            except Exception:
                actual_owner = self.pool.bot_wallet
        tick_spacing = int(pos[4])
        return PositionSnapshot(
            token_id=int(token_id),
            owner=Web3.to_checksum_address(actual_owner or self.pool.bot_wallet),
            pool_address=self.pool.pool_address,
            token0=Web3.to_checksum_address(pos[2]),
            token1=Web3.to_checksum_address(pos[3]),
            fee=tick_spacing,
            tick_lower=int(pos[5]),
            tick_upper=int(pos[6]),
            liquidity=int(pos[7]),
            tokens_owed0=int(pos[10]),
            tokens_owed1=int(pos[11]),
            pid=None,
            is_staked=False,
        )

    def withdraw_staked_position(self, position: PositionSnapshot) -> TxResult | None:
        if not position.is_staked:
            return None
        if not self.gauge:
            raise RuntimeError("Aerodrome gauge address is not configured")
        return self.executor.send(self.gauge.functions.withdraw(int(position.token_id)), "unstake", gas=450000)

    def decrease_collect_withdraw(self, position: PositionSnapshot, slot0: Slot0) -> TxResult:
        deadline = int(time.time()) + self.pool.deadline_seconds
        decrease_tx = None
        if position.liquidity > 0:
            expected0, expected1 = amounts_for_liquidity(
                position.liquidity,
                slot0.sqrt_price_x96,
                position.tick_lower,
                position.tick_upper,
            )
            min0 = self._apply_slippage(expected0)
            min1 = self._apply_slippage(expected1)
            decrease_tx = self.executor.send(
                self.npm.functions.decreaseLiquidity(
                    (position.token_id, position.liquidity, min0, min1, deadline)
                ),
                "withdraw",
                gas=550000,
            )
        collect_tx = self.executor.send(
            self.npm.functions.collect(
                (position.token_id, self.pool.bot_wallet, MAX_UINT128, MAX_UINT128)
            ),
            "withdraw",
            gas=350000,
        )
        if decrease_tx:
            collect_tx.metadata["decrease_tx_hash"] = decrease_tx.tx_hash
            collect_tx.metadata["decrease_status"] = decrease_tx.status
        return collect_tx

    def mint(self, plan) -> tuple[TxResult, int | None]:
        self.approve_if_needed(self.pool.token0_address, self.npm_address, plan.amount0_desired)
        self.approve_if_needed(self.pool.token1_address, self.npm_address, plan.amount1_desired)
        deadline = int(time.time()) + self.pool.deadline_seconds
        min0 = self._apply_slippage(plan.amount0_desired)
        min1 = self._apply_slippage(plan.amount1_desired)
        tick_spacing = int(self.pool.tick_spacing or self.pool.fee)
        params = (
            self.pool.token0_address,
            self.pool.token1_address,
            tick_spacing,
            int(plan.new_tick_lower),
            int(plan.new_tick_upper),
            int(plan.amount0_desired),
            int(plan.amount1_desired),
            min0,
            min1,
            Web3.to_checksum_address(self.pool.bot_wallet),
            deadline,
            0,
        )
        log_block(
            log,
            logging.INFO,
            "mint start",
            pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
            {
                "stage": "mint_start",
                "action": "mint",
                "token0": self.pool.token0_address,
                "token1": self.pool.token1_address,
                "tick_spacing": tick_spacing,
                "tick_lower": int(plan.new_tick_lower),
                "tick_upper": int(plan.new_tick_upper),
                "amount0_desired_raw": int(plan.amount0_desired),
                "amount1_desired_raw": int(plan.amount1_desired),
                "amount0_min_raw": min0,
                "amount1_min_raw": min1,
                "sqrt_price_x96": 0,
                "deadline": deadline,
            },
        )
        result = self.executor.send(self.npm.functions.mint(params), "mint", gas=800000)
        if result.dry_run:
            return result, None
        receipt = self._mint_receipt_from_result(result)
        if receipt is None:
            log_block(
                log,
                logging.WARNING,
                "mint receipt unavailable",
                pool_context(self.pool, old_token_id=getattr(plan, "old_token_id", None)),
                {
                    "stage": "mint_receipt",
                    "status": result.status,
                    "tx_hash": result.tx_hash,
                    "signed_tx_hash": result.metadata.get("signed_tx_hash") if result.metadata else None,
                    "next_action": "journal recovery will try receipt lookup",
                },
            )
            return result, None
        if int(receipt.get("status", 0)) != 1:
            result.metadata["error"] = "mint transaction reverted"
            return result, None
        token_id = self._new_token_id_from_mint_receipt(receipt)
        if token_id is None:
            return result, None
        validation = self._validate_minted_position_detail(
            token_id,
            plan,
            receipt_block=int(receipt.get("blockNumber") or 0),
        )
        if not validation.can_stake:
            result.metadata["error"] = f"mint receipt token validation failed: {validation.reason}"
            return result, None
        return result, token_id

    def stake(self, token_id: int) -> TxResult:
        if not self.gauge_address or not self.gauge:
            raise RuntimeError("Aerodrome gauge address is not configured")
        approved = None
        try:
            approved = self.npm.functions.getApproved(int(token_id)).call()
        except Exception:
            approved = None
        if not approved or str(approved).lower() != self.gauge_address.lower():
            self.executor.send(self.npm.functions.approve(self.gauge_address, int(token_id)), "approve", gas=160000)
        log_block(
            log,
            logging.INFO,
            "stake start",
            pool_context(self.pool, new_token_id=token_id),
            {
                "stage": "stake_start",
                "action": "stake",
                "gauge": self.gauge_address,
            },
        )
        return self.executor.send(self.gauge.functions.deposit(int(token_id)), "stake", gas=450000)

    def burn_if_empty_and_owned(self, token_id: int) -> TxResult | None:
        if not self.pool.execute_burn:
            return None
        pos = self.npm.functions.positions(int(token_id)).call()
        if int(pos[7]) != 0 or int(pos[10]) != 0 or int(pos[11]) != 0:
            return None
        owner = Web3.to_checksum_address(self.npm.functions.ownerOf(int(token_id)).call())
        if owner != Web3.to_checksum_address(self.pool.bot_wallet):
            return None
        return self.executor.send(self.npm.functions.burn(int(token_id)), "burn", gas=180000)

    def _read_npm_position_with_w3(self, w3: Web3, token_id: int, owner: str | None = None) -> PositionSnapshot:
        if w3 is self.w3:
            return self.read_npm_position(token_id, owner=owner)
        npm = w3.eth.contract(address=self.npm_address, abi=AERODROME_NPM_ABI)
        pos = npm.functions.positions(int(token_id)).call()
        actual_owner = owner
        try:
            actual_owner = npm.functions.ownerOf(int(token_id)).call()
        except Exception:
            pass
        tick_spacing = int(pos[4])
        return PositionSnapshot(
            token_id=int(token_id),
            owner=Web3.to_checksum_address(actual_owner or self.pool.bot_wallet),
            pool_address=self.pool.pool_address,
            token0=Web3.to_checksum_address(pos[2]),
            token1=Web3.to_checksum_address(pos[3]),
            fee=tick_spacing,
            tick_lower=int(pos[5]),
            tick_upper=int(pos[6]),
            liquidity=int(pos[7]),
            tokens_owed0=int(pos[10]),
            tokens_owed1=int(pos[11]),
            pid=None,
            is_staked=False,
        )

    def _matches_pool(self, token0: str, token1: str, fee: int) -> bool:
        expected_spacing = int(self.pool.tick_spacing or self.pool.fee or 0)
        return (
            token0.lower() == self.pool.token0_address.lower()
            and token1.lower() == self.pool.token1_address.lower()
            and int(fee) == expected_spacing
        )


AerodromeGaugeAdapter = AerodromeV3GaugeAdapter
