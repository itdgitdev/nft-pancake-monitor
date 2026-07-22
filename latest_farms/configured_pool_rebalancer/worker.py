from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from web3 import Web3
from web3.exceptions import TransactionNotFound

from .adapter import AerodromeGaugeAdapter, DexAdapter, PancakeV3MasterChefAdapter
from .discord_notifier import DiscordNotifier
from .evm import web3_connection
from .journal import RebalanceJournal, mysql_advisory_lock
from .logging_utils import log_block, pool_context
from .models import (
    CompoundCandidate,
    DexType,
    PoolConfig,
    PositionSnapshot,
    PositionState,
    PositionStrategy,
    RebalanceCycleOutcome,
    RebalancePlan,
    StakeMode,
    TokenBalance,
    TxResult,
    WorkerConfig,
)
from .pnl_report import ConfiguredPoolPnlReporter
from .planner import RebalancePlanner, SwapPlanner
from .position_index import PositionIndex
from .reward import token_price_usd
from .signer import RuntimeSigner
from .tx_executor import TxExecutor
from .v3_math import price_percent_from_tick_delta


log = logging.getLogger("configured_pool_rebalancer")
TRANSFER_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex().lower().replace("0x", "")
MAX_STAKE_RECOVERY_ATTEMPTS = 3

ADAPTER_REGISTRY = {
    DexType.PANCAKE_V3_MASTERCHEF: PancakeV3MasterChefAdapter,
    DexType.PANCAKE_V3: PancakeV3MasterChefAdapter,
    DexType.AERODROME_V3: AerodromeGaugeAdapter,
    DexType.AERODROME_GAUGE: AerodromeGaugeAdapter,
}


class ConfiguredPoolRebalancer:
    def __init__(self, config: WorkerConfig, migrate: bool = False, signer: RuntimeSigner | None = None):
        self.config = config
        self.signer = signer
        self.index = PositionIndex(
            config.cache_dir,
            legacy_cache_dir=config.legacy_position_cache_dir,
            use_legacy_cache=config.use_legacy_position_cache,
            use_db_cache=config.use_db_position_cache,
            db_cache_source=config.db_position_cache_source,
        )
        self.journal = RebalanceJournal()
        self.notifier = DiscordNotifier(config)
        self._collect_compound_candidates = False
        self._cycle_compound_candidates: dict[str, list[CompoundCandidate]] = {}
        if migrate:
            self.journal.migrate()

    def run_once(self) -> list[dict]:
        results: list[dict] = []
        for pool in self.config.pools:
            try:
                results.extend(self._run_pool(pool))
                self._retry_discord_pnl_notifications(pool)
            except Exception as exc:
                log_block(
                    log,
                    logging.ERROR,
                    "error context",
                    pool_context(pool),
                    {
                        "stage": "pool_cycle",
                        "action": "run_pool",
                        "reason": exc,
                        "recovery_impact": "pool skipped for this cycle",
                    },
                )
                log.exception("pool %s failed: %s", pool.name, exc)
                results.append({"pool": pool.name, "status": "ERROR", "error": str(exc)})
        return results

    def run_once_with_outcome(self) -> RebalanceCycleOutcome:
        self._cycle_compound_candidates = {}
        self._collect_compound_candidates = True
        try:
            records = self.run_once()
        finally:
            self._collect_compound_candidates = False

        wallets_by_pool: dict[str, set[str]] = {}
        for pool in self.config.pools:
            wallets_by_pool.setdefault(pool.name, set()).add(pool.bot_wallet.lower())
        blocked_wallets: set[str] = set()
        for record in records:
            safe = record.get("state") == PositionState.IN_RANGE.value and record.get("status") != "ERROR"
            if not safe:
                blocked_wallets.update(wallets_by_pool.get(str(record.get("pool")), set()))
        return RebalanceCycleOutcome(
            records=records,
            compound_candidates={
                pool_name: tuple(candidates)
                for pool_name, candidates in self._cycle_compound_candidates.items()
            },
            blocked_wallets=blocked_wallets,
        )

    def _run_pool(self, raw_pool: PoolConfig) -> list[dict]:
        w3 = web3_connection(raw_pool.chain)
        executor = TxExecutor(w3, raw_pool, self.config.dry_run, self.config, signer=self.signer)
        adapter = self._build_adapter(w3, raw_pool, executor)
        pool = adapter.discover_pool_metadata()
        if pool != raw_pool:
            executor = TxExecutor(w3, pool, self.config.dry_run, self.config, signer=self.signer)
            adapter = self._build_adapter(w3, pool, executor)
        log_block(
            log,
            logging.INFO,
            "pool start",
            pool_context(pool),
            {
                "stage": "pool_start",
                "pid": pool.pid,
                "wallet": pool.bot_wallet,
                "dry_run": self.config.dry_run,
                "max_jobs_per_cycle": pool.max_jobs_per_cycle,
            },
        )

        pending_swap_results = self._recover_pending_swaps(w3, pool)
        if any(item.get("state") == PositionState.SWAP_PENDING.value for item in pending_swap_results):
            log_block(
                log,
                logging.INFO,
                "pool paused",
                pool_context(pool),
                {
                    "stage": "pending_swap_check",
                    "status": "SWAP_PENDING",
                    "pending_results": len(pending_swap_results),
                    "next_action": "wait for pending swap recovery",
                },
            )
            return pending_swap_results

        recovery_results = self._recover_partial_jobs(w3, pool, adapter)
        positions = self.index.refresh(w3, pool, adapter)
        log_block(
            log,
            logging.INFO,
            "position index",
            pool_context(pool),
            {
                "stage": "position_index",
                "positions": len(positions),
                "recovery_results": len(recovery_results),
            },
        )
        self._notify_inactive_farm_if_needed(pool, adapter, positions)
        slot0 = adapter.read_slot0()
        out: list[dict] = list(recovery_results)
        jobs_started = sum(1 for item in recovery_results if item.get("action_taken"))

        for position in positions.values():
            in_range = position.tick_lower <= slot0.tick < position.tick_upper
            log_block(
                log,
                logging.INFO,
                "position check",
                pool_context(pool, token_id=position.token_id),
                {
                    "stage": "position_check",
                    "tick": slot0.tick,
                    "old_range": [position.tick_lower, position.tick_upper],
                    "liquidity": position.liquidity,
                    "owner": position.owner,
                    "in_range": in_range,
                    "next_action": "skip rebalance" if in_range else "create rebalance plan",
                },
            )
            if in_range:
                self._log_strategy_mismatch(pool, position)
                if (
                    self._collect_compound_candidates
                    and pool.npm_address
                    and position.owner.lower() == pool.bot_wallet.lower()
                ):
                    self._cycle_compound_candidates.setdefault(pool.name, []).append(
                        CompoundCandidate(
                            chain=pool.chain,
                            pool_name=pool.name,
                            pool_address=pool.pool_address,
                            wallet=pool.bot_wallet,
                            npm_address=pool.npm_address,
                            token_id=position.token_id,
                            stake_mode=StakeMode.STAKED if position.is_staked else StakeMode.UNSTAKED,
                            anchor_block=position.last_updated_block,
                        )
                    )
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.IN_RANGE.value,
                        "tick": slot0.tick,
                    }
                )
                continue

            planner = RebalancePlanner()
            lower_percent, upper_percent, percent_source = self._resolve_range_percent(pool, position)
            dry_plan = planner.build_plan(
                pool,
                position,
                slot0,
                lower_percent=lower_percent,
                upper_percent=upper_percent,
                range_percent_source=percent_source,
            )
            self._try_record_plan(pool, position, dry_plan)
            log_block(
                log,
                logging.INFO,
                "plan created",
                pool_context(pool, token_id=position.token_id),
                {
                    "stage": "plan_created",
                    "old_range": [position.tick_lower, position.tick_upper],
                    "new_range": [dry_plan.new_tick_lower, dry_plan.new_tick_upper],
                    "range_source": dry_plan.metadata.get("range_percent_source"),
                    "range_mode": dry_plan.metadata.get("range_mode"),
                    "amount0_desired_raw": getattr(dry_plan, "amount0_desired", None),
                    "amount1_desired_raw": getattr(dry_plan, "amount1_desired", None),
                    "swap_amount_in_raw": getattr(dry_plan, "swap_amount_in", None),
                    "swap_token_in": getattr(dry_plan, "swap_token_in", None),
                    "swap_token_out": getattr(dry_plan, "swap_token_out", None),
                    "dry_run": self.config.dry_run,
                    "next_action": "dry-run output only" if self.config.dry_run else "execute rebalance",
                },
            )
            out.append(
                {
                    "pool": pool.name,
                    "token_id": position.token_id,
                    "state": PositionState.PLANNED.value,
                    "old_range": [position.tick_lower, position.tick_upper],
                    "new_range": [dry_plan.new_tick_lower, dry_plan.new_tick_upper],
                    "range_mode": dry_plan.metadata.get("range_mode"),
                    "lower_percent": dry_plan.metadata.get("lower_percent"),
                    "upper_percent": dry_plan.metadata.get("upper_percent"),
                    "range_percent_source": dry_plan.metadata.get("range_percent_source"),
                    "dry_run": self.config.dry_run,
                }
            )

            if self.config.dry_run:
                continue
            if jobs_started >= pool.max_jobs_per_cycle:
                continue
            if position.owner.lower() != pool.bot_wallet.lower():
                message = (
                    "signer mismatch: position owner is not bot_wallet. "
                    "Create a separate pool config entry with that owner as bot_wallet."
                )
                self.journal.mark_status(
                    pool.chain,
                    position.token_id,
                    PositionState.FAILED,
                    error_reason=message,
                )
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.FAILED.value,
                        "error": message,
                    }
                )
                continue

            try:
                self._execute_position(w3, pool, adapter, position)
            except Exception as exc:
                log_block(
                    log,
                    logging.ERROR,
                    "error context",
                    pool_context(pool, token_id=position.token_id),
                    {
                        "stage": "rebalance_execute",
                        "action": "execute_position",
                        "reason": exc,
                        "recovery_impact": "mark job FAILED for this cycle",
                    },
                )
                log.exception("rebalance job failed pool=%s tokenId=%s: %s", pool.name, position.token_id, exc)
                try:
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.FAILED,
                        error_reason=str(exc),
                    )
                except Exception as journal_exc:
                    log.warning(
                        "could not mark failed job pool=%s tokenId=%s: %s",
                        pool.name,
                        position.token_id,
                        journal_exc,
                    )
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.FAILED.value,
                        "error": str(exc),
                    }
                )
            jobs_started += 1

        log_block(
            log,
            logging.INFO,
            "pool end",
            pool_context(pool),
            {
                "stage": "pool_end",
                "results": len(out),
                "jobs_started": jobs_started,
            },
        )
        return out

    @staticmethod
    def _log_strategy_mismatch(pool: PoolConfig, position: PositionSnapshot) -> None:
        expected_staked = pool.expected_position_strategy == PositionStrategy.FARM
        if bool(position.is_staked) == expected_staked:
            return
        log.warning(
            "position strategy mismatch pool=%s chain=%s tokenId=%s expected_strategy=%s "
            "actual_stake_mode=%s action=NONE reason=STRATEGY_MISMATCH",
            pool.name,
            pool.chain,
            position.token_id,
            pool.expected_position_strategy.value.upper(),
            StakeMode.STAKED.value if position.is_staked else StakeMode.UNSTAKED.value,
        )

    def _execute_position(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        position: PositionSnapshot,
    ) -> None:
        lower_percent, upper_percent, percent_source = self._resolve_range_percent(pool, position)
        wallet_lock = f"rebalance:{pool.chain}:{pool.bot_wallet.lower()}"
        pool_lock = f"rebalance:{pool.chain}:{pool.pool_address.lower()}"
        with mysql_advisory_lock(wallet_lock, self.config.lock_timeout_seconds):
            with mysql_advisory_lock(pool_lock, self.config.lock_timeout_seconds):
                position = self._revalidate_position_for_execution(pool, adapter, position)
                restore_staked = bool(position.is_staked)
                if hasattr(self.journal, "set_restore_stake_mode"):
                    self.journal.set_restore_stake_mode(
                        pool.chain,
                        position.token_id,
                        StakeMode.STAKED.value if restore_staked else StakeMode.UNSTAKED.value,
                    )
                log_block(
                    log,
                    logging.INFO,
                    "rebalance start",
                    pool_context(pool, old_token_id=position.token_id),
                    {
                        "stage": "rebalance_start",
                        "action": "withdraw",
                        "wallet": pool.bot_wallet,
                        "old_range": [position.tick_lower, position.tick_upper],
                        "liquidity": position.liquidity,
                    },
                )
                pre0, pre1 = adapter.read_balances(pool.bot_wallet)
                self.journal.record_balance_snapshot(pool.chain, position.token_id, "pre", pre0.raw, pre1.raw)
                reward_token = self._adapter_reward_token(adapter)
                pre_reward = None
                if reward_token and reward_token.lower() not in {
                    str(pool.token0_address).lower(),
                    str(pool.token1_address).lower(),
                }:
                    try:
                        pre_reward = adapter.read_token_balance(reward_token, pool.bot_wallet)
                    except Exception as exc:
                        log.warning("could not read pre reward balance for %s: %s", pool.name, exc)
                slot_before_withdraw = adapter.read_slot0()
                unstake_tx = adapter.withdraw_staked_position(position)
                if unstake_tx:
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.UNSTAKED_UNWITHDRAWN,
                        "unstake",
                        unstake_tx,
                    )
                    position.is_staked = False
                withdraw_tx = adapter.decrease_collect_withdraw(position, slot_before_withdraw)
                log_block(
                    log,
                    logging.INFO,
                    "withdraw result",
                    pool_context(pool, old_token_id=position.token_id),
                    {
                        "stage": "withdraw_result",
                        "action": "withdraw",
                        "status": withdraw_tx.status,
                        "tx_hash": withdraw_tx.tx_hash,
                        "gas_used": withdraw_tx.gas_used,
                        "next_action": "record reservation from receipt",
                    },
                )
                self.journal.mark_status(pool.chain, position.token_id, PositionState.WITHDRAWN_UNBURNED, "withdraw", withdraw_tx)

                post0, post1 = self._read_recovered_balances_with_retry(adapter, pool, pre0, pre1)
                self.journal.record_balance_snapshot(
                    pool.chain,
                    position.token_id,
                    "post_withdraw",
                    post0.raw,
                    post1.raw,
                )
                try:
                    reward_update = self._module_reward_update(pool, adapter, reward_token, pre_reward)
                except Exception as exc:
                    reward_update = None
                    log.warning("could not record module reward for %s tokenId=%s: %s", pool.name, position.token_id, exc)
                if reward_update:
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.WITHDRAWN_UNBURNED,
                        claimed_reward_token=reward_update["token"],
                        claimed_reward_raw=reward_update["raw"],
                        claimed_reward_amount=reward_update["amount"],
                        claimed_reward_price_usd=reward_update["price_usd"],
                        claimed_reward_usd=reward_update["usd"],
                        claimed_reward_source=reward_update["source"],
                    )
                balance_delta0 = max(0, post0.raw - pre0.raw)
                balance_delta1 = max(0, post1.raw - pre1.raw)
                receipt_inflows = self._receipt_token_inflows(
                    w3,
                    pool,
                    withdraw_tx.tx_hash,
                    pool.bot_wallet,
                )
                reservation_source = "withdraw_receipt"
                if receipt_inflows is None or (receipt_inflows[0] <= 0 and receipt_inflows[1] <= 0):
                    recovered0 = balance_delta0
                    recovered1 = balance_delta1
                    reservation_source = "balance_delta_fallback"
                    log.warning(
                        "withdraw receipt token inflows unavailable; using balance delta fallback "
                        "pool=%s tokenId=%s tx=%s delta0=%s delta1=%s",
                        pool.name,
                        position.token_id,
                        withdraw_tx.tx_hash,
                        balance_delta0,
                        balance_delta1,
                    )
                else:
                    recovered0, recovered1 = receipt_inflows
                    if recovered0 != balance_delta0 or recovered1 != balance_delta1:
                        log.warning(
                            "withdraw receipt/balance delta mismatch pool=%s tokenId=%s tx=%s "
                            "receipt0=%s receipt1=%s delta0=%s delta1=%s",
                            pool.name,
                            position.token_id,
                            withdraw_tx.tx_hash,
                            recovered0,
                            recovered1,
                            balance_delta0,
                            balance_delta1,
                        )
                if reservation_source == "balance_delta_fallback" and recovered0 <= 0 and recovered1 <= 0:
                    reason = (
                        "withdraw receipt token inflows unavailable and balance delta is zero; "
                        "manual recovery required"
                    )
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.WITHDRAWN_UNBURNED,
                        error_reason=reason,
                    )
                    self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                    self._notify_recovery_required(pool, position.token_id, reason)
                    return
                reserved0 = recovered0
                reserved1 = recovered1
                self.journal.record_reservation(
                    pool.chain,
                    position.token_id,
                    pool.token0_address,
                    pool.token1_address,
                    reserved0,
                    reserved1,
                )
                log.info(
                    "rebalance checkpoint pool=%s tokenId=%s stage=post_withdraw "
                    "pre0=%s pre1=%s post_withdraw0=%s post_withdraw1=%s "
                    "recovered_after_withdraw0=%s recovered_after_withdraw1=%s "
                    "reserved0=%s reserved1=%s reservation_source=%s",
                    pool.name,
                    position.token_id,
                    pre0.raw,
                    pre1.raw,
                    post0.raw,
                    post1.raw,
                    recovered0,
                    recovered1,
                    reserved0,
                    reserved1,
                    reservation_source,
                )
                coverage_error = self._reservation_coverage_error(pool, adapter, "post_withdraw")
                if coverage_error:
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.WITHDRAWN_UNBURNED,
                        error_reason=coverage_error,
                    )
                    self.journal.mark_recovery_error(pool.chain, position.token_id, coverage_error)
                    self._notify_recovery_required(pool, position.token_id, coverage_error)
                    return

                slot_for_mint = adapter.read_slot0()
                swap_plan = SwapPlanner().build_swap_plan(
                    pool,
                    position,
                    slot_for_mint,
                    recovered0,
                    recovered1,
                    lower_percent,
                    upper_percent,
                    percent_source,
                )
                self._try_record_plan(pool, position, swap_plan)
                if swap_plan.swap_amount_in > 0 and swap_plan.swap_token_in and swap_plan.swap_token_out:
                    dust_reason = self._swap_dust_reason(pool, swap_plan, recovered0, recovered1)
                    if dust_reason:
                        log.info(
                            "skip dust swap pool=%s tokenId=%s amountIn=%s reason=%s",
                            pool.name,
                            position.token_id,
                            swap_plan.swap_amount_in,
                            dust_reason,
                        )
                        reason = f"dust swap required before mint; {dust_reason}"
                        self.journal.mark_status(
                            pool.chain,
                            position.token_id,
                            PositionState.WITHDRAWN_UNBURNED,
                            error_reason=reason,
                        )
                        self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                        self._notify_recovery_required(pool, position.token_id, reason)
                        return
                    else:
                        swap_tx = adapter.swap(
                            swap_plan.swap_token_in,
                            swap_plan.swap_token_out,
                            swap_plan.swap_amount_in,
                        )
                        if not swap_tx:
                            reason = "swap quote unavailable or price impact too high"
                            self.journal.mark_status(
                                pool.chain,
                                position.token_id,
                                PositionState.SWAP_BLOCKED,
                                error_reason=reason,
                            )
                            self._notify_partial_action(
                                pool,
                                position.token_id,
                                "swap",
                                PositionState.SWAP_BLOCKED,
                                reason,
                                "recovery will retry swap planning from reservation",
                            )
                            return
                        if swap_tx.status == "PENDING":
                            reason = swap_tx.metadata.get("error") or "swap receipt timeout"
                            self.journal.mark_status(
                                pool.chain,
                                position.token_id,
                                PositionState.SWAP_PENDING,
                                "swap",
                                swap_tx,
                                error_reason=reason,
                            )
                            self._notify_partial_action(
                                pool,
                                position.token_id,
                                "swap",
                                PositionState.SWAP_PENDING,
                                reason,
                                "worker will inspect swap receipt on the next cycle",
                                tx_hash=swap_tx.tx_hash,
                                signed_tx_hash=(swap_tx.metadata or {}).get("signed_tx_hash"),
                            )
                            return
                        if swap_tx.status == "FAILED":
                            tx_label = "swap" if str(swap_tx.tx_hash).startswith("0x") else None
                            reason = swap_tx.metadata.get("error") or "swap transaction failed"
                            self.journal.mark_status(
                                pool.chain,
                                position.token_id,
                                PositionState.SWAP_BLOCKED,
                                tx_label,
                                swap_tx if tx_label else None,
                                error_reason=reason,
                            )
                            self._notify_partial_action(
                                pool,
                                position.token_id,
                                "swap",
                                PositionState.SWAP_BLOCKED,
                                reason,
                                "recovery will retry swap from reservation if safe",
                                tx_hash=swap_tx.tx_hash if tx_label else None,
                                signed_tx_hash=(swap_tx.metadata or {}).get("signed_tx_hash"),
                            )
                            return
                        if swap_tx.status == "SKIPPED":
                            log.info(
                                "skip swap pool=%s tokenId=%s reason=%s",
                                pool.name,
                                position.token_id,
                                swap_tx.metadata.get("reason", "swap skipped"),
                            )
                        else:
                            self.journal.mark_status(
                                pool.chain,
                                position.token_id,
                                PositionState.WITHDRAWN_UNBURNED,
                                "swap",
                                swap_tx,
                            )
                            swap_receipt_inflows = self._receipt_token_inflows(
                                w3,
                                pool,
                                swap_tx.tx_hash,
                                pool.bot_wallet,
                            )
                            post_swap = self._read_post_swap_balances_with_retry(
                                w3,
                                adapter,
                                pool,
                                position,
                                post0,
                                post1,
                                swap_tx,
                            )
                            if post_swap is None:
                                if not self._swap_receipt_confirms_output(pool, swap_tx, swap_receipt_inflows):
                                    reason = "post-swap balance and receipt output not confirmed; mint skipped"
                                    self.journal.mark_status(
                                        pool.chain,
                                        position.token_id,
                                        PositionState.RECOVERY_REQUIRED,
                                        "swap",
                                        swap_tx,
                                        error_reason=reason,
                                    )
                                    self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                                    self._notify_recovery_required(pool, position.token_id, reason)
                                    return
                                log.warning(
                                    "post-swap balance confirmation failed but receipt output was verified; "
                                    "continuing with receipt reservation pool=%s tokenId=%s tx=%s inflows=%s",
                                    pool.name,
                                    position.token_id,
                                    swap_tx.tx_hash,
                                    swap_receipt_inflows,
                                )
                                post_swap0, post_swap1 = adapter.read_balances(pool.bot_wallet)
                            else:
                                post_swap0, post_swap1 = post_swap
                            self.journal.record_balance_snapshot(
                                pool.chain,
                                position.token_id,
                                "post_swap",
                                post_swap0.raw,
                                post_swap1.raw,
                            )
                            if swap_receipt_inflows is None or (
                                swap_receipt_inflows[0] <= 0 and swap_receipt_inflows[1] <= 0
                            ):
                                log.warning(
                                    "swap receipt token inflows unavailable; using balance delta fallback "
                                    "pool=%s tokenId=%s tx=%s",
                                    pool.name,
                                    position.token_id,
                                    swap_tx.tx_hash,
                                )
                            reserved0, reserved1 = self._reservation_after_swap(
                                pool,
                                reserved0,
                                reserved1,
                                post0,
                                post1,
                                post_swap0,
                                post_swap1,
                                swap_tx,
                                swap_receipt_inflows,
                            )
                            self.journal.record_reservation(
                                pool.chain,
                                position.token_id,
                                pool.token0_address,
                                pool.token1_address,
                                reserved0,
                                reserved1,
                            )
                            recovered0 = max(0, int(reserved0))
                            recovered1 = max(0, int(reserved1))
                            slot_for_mint = adapter.read_slot0()
                            swap_plan = RebalancePlanner().build_plan(
                                pool,
                                position,
                                slot_for_mint,
                                recovered0,
                                recovered1,
                                lower_percent,
                                upper_percent,
                                percent_source,
                            )
                            log.info(
                                "rebalance checkpoint pool=%s tokenId=%s stage=post_swap "
                                "swap_tx=%s swap_receipt_block=%s post_swap0=%s post_swap1=%s "
                                "recovered_after_swap0=%s recovered_after_swap1=%s "
                                "planned_amount0_desired=%s planned_amount1_desired=%s "
                                "reserved0=%s reserved1=%s",
                                pool.name,
                                position.token_id,
                                swap_tx.tx_hash,
                                swap_tx.metadata.get("receipt_block"),
                                post_swap0.raw,
                                post_swap1.raw,
                                recovered0,
                                recovered1,
                                swap_plan.amount0_desired,
                                swap_plan.amount1_desired,
                                reserved0,
                                reserved1,
                            )

                pre_mint0, pre_mint1 = adapter.read_balances(pool.bot_wallet)
                coverage_error = self._reservation_coverage_error(pool, adapter, "pre_mint")
                if coverage_error:
                    self._mark_manual_recovery(pool, position.token_id, coverage_error)
                    return
                original_amount0, original_amount1, available0, available1 = self._clamp_plan_to_reservation(
                    swap_plan,
                    reserved0,
                    reserved1,
                    pre_mint0,
                    pre_mint1,
                )
                log.info(
                    "rebalance checkpoint pool=%s tokenId=%s stage=pre_mint "
                    "pre_mint0=%s pre_mint1=%s available0_for_mint=%s available1_for_mint=%s "
                    "original_amount0_desired=%s original_amount1_desired=%s "
                    "clamped_amount0_desired=%s clamped_amount1_desired=%s",
                    pool.name,
                    position.token_id,
                    pre_mint0.raw,
                    pre_mint1.raw,
                    available0,
                    available1,
                    original_amount0,
                    original_amount1,
                    swap_plan.amount0_desired,
                    swap_plan.amount1_desired,
                )
                if (
                    original_amount0 != swap_plan.amount0_desired
                    or original_amount1 != swap_plan.amount1_desired
                ):
                    self._try_record_plan(pool, position, swap_plan)

                if swap_plan.amount0_desired <= 0 and swap_plan.amount1_desired <= 0:
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.WITHDRAWN_UNBURNED,
                        error_reason="zero mint amounts after pre-mint balance clamp; skipped mint",
                    )
                    log.warning(
                        "skip mint with zero amounts pool=%s tokenId=%s recovered0=%s recovered1=%s",
                        pool.name,
                        position.token_id,
                        recovered0,
                        recovered1,
                    )
                    return

                if not (swap_plan.new_tick_lower <= slot_for_mint.tick < swap_plan.new_tick_upper):
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.FAILED,
                        error_reason="current tick moved outside new range before mint",
                    )
                    return

                mint_tx, new_token_id = adapter.mint(swap_plan)
                if not new_token_id:
                    reason = (
                        mint_tx.metadata.get("error")
                        if mint_tx and mint_tx.metadata
                        else "mint token id was not parsed"
                    )
                    if mint_tx and mint_tx.status in {"BROADCAST_UNKNOWN", "PENDING"}:
                        signed_hash = (mint_tx.metadata or {}).get("signed_tx_hash")
                        reason = (
                            f"{reason}; tx_status={mint_tx.status}; tx_hash={mint_tx.tx_hash}; "
                            f"signed_tx_hash={signed_hash}; next_action=journal recovery will inspect receipt"
                        )
                    mint_tx_label = None if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN" else "mint"
                    mint_tx_for_journal = None if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN" else mint_tx
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.RECOVERY_REQUIRED,
                        mint_tx_label,
                        mint_tx_for_journal,
                        error_reason=f"mint reconciliation required: {reason}",
                    )
                    self.journal.mark_recovery_error(pool.chain, position.token_id, f"mint reconciliation required: {reason}")
                    if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN":
                        self._notify_partial_action(
                            pool,
                            position.token_id,
                            "mint",
                            "MINT_BROADCAST_UNKNOWN",
                            f"mint broadcast unknown: {reason}",
                            "recovery will retry mint only after confirming the signed hash is not on-chain",
                            signed_tx_hash=(mint_tx.metadata or {}).get("signed_tx_hash"),
                        )
                    self._notify_recovery_required(pool, position.token_id, f"mint reconciliation required: {reason}")
                    return
                actual_lower_percent = price_percent_from_tick_delta(
                    swap_plan.new_tick_lower - slot_for_mint.tick
                )
                actual_upper_percent = price_percent_from_tick_delta(
                    swap_plan.new_tick_upper - slot_for_mint.tick
                )
                minted_state = (
                    PositionState.MINTED_UNSTAKED
                    if restore_staked
                    else PositionState.REMINTED_UNSTAKED
                )
                self.journal.mark_status(
                    pool.chain,
                    position.token_id,
                    minted_state,
                    "mint",
                    mint_tx,
                    new_token_id,
                    mint_tick=slot_for_mint.tick,
                    mint_tick_lower=swap_plan.new_tick_lower,
                    mint_tick_upper=swap_plan.new_tick_upper,
                    range_lower_percent=actual_lower_percent,
                    range_upper_percent=actual_upper_percent,
                    range_percent_source=swap_plan.metadata.get("range_percent_source"),
                )

                if restore_staked:
                    stake_tx = None
                    try:
                        stake_tx = adapter.stake(new_token_id)
                        stake_confirmed = self._confirm_stake_with_retry(
                            pool,
                            adapter,
                            position.token_id,
                            new_token_id,
                        )
                    except Exception as exc:
                        reason = f"stake failed: {exc}"
                        failed_state = (
                            PositionState.MINTED_UNSTAKED
                            if self._is_definite_prebroadcast_stake_error(exc)
                            else PositionState.RECOVERY_REQUIRED
                        )
                        self.journal.mark_status(
                            pool.chain,
                            position.token_id,
                            failed_state,
                            error_reason=reason,
                        )
                        if failed_state == PositionState.MINTED_UNSTAKED:
                            self._notify_partial_action(
                                pool,
                                position.token_id,
                                "stake",
                                failed_state,
                                reason,
                                "recovery will retry staking the minted NFT",
                                new_token_id=new_token_id,
                            )
                        else:
                            self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                            self._notify_recovery_required(pool, position.token_id, reason)
                        self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)
                        return
                    if not stake_confirmed:
                        reason = "stake transaction succeeded but staking contract membership was not confirmed"
                        self.journal.mark_status(
                            pool.chain,
                            position.token_id,
                            PositionState.RECOVERY_REQUIRED,
                            "stake",
                            stake_tx,
                            new_token_id,
                            error_reason=reason,
                        )
                        self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                        self._notify_recovery_required(pool, position.token_id, reason)
                        self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)
                        return
                    self.journal.mark_status(pool.chain, position.token_id, PositionState.REMINTED, "stake", stake_tx, new_token_id)

                try:
                    burn_tx = adapter.burn_if_empty_and_owned(position.token_id)
                    if burn_tx:
                        self.journal.mark_status(pool.chain, position.token_id, PositionState.BURNED, "burn", burn_tx, new_token_id)
                except Exception as exc:
                    log.warning("burn failed after remint for %s tokenId=%s: %s", pool.name, position.token_id, exc)
                self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)

    def _revalidate_position_for_execution(
        self,
        pool: PoolConfig,
        adapter: DexAdapter,
        discovered: PositionSnapshot,
    ) -> PositionSnapshot:
        try:
            staked = adapter.read_staked_positions([discovered.token_id]).get(discovered.token_id)
        except Exception as exc:
            raise RuntimeError(f"could not revalidate staking state: {exc}") from exc
        if staked is not None:
            current = staked
            current.is_staked = True
        else:
            try:
                current = adapter.read_npm_position(discovered.token_id)
            except Exception as exc:
                raise RuntimeError(f"could not revalidate unstaked position: {exc}") from exc
            if current.owner.lower() != pool.bot_wallet.lower():
                raise RuntimeError(f"position owner changed before execution: {current.owner}")
            current.is_staked = False
        if current.owner.lower() != pool.bot_wallet.lower():
            raise RuntimeError(f"position logical owner is not bot wallet: {current.owner}")
        if not self._matches_pool(pool, current):
            raise RuntimeError("position no longer matches configured pool")
        if int(current.liquidity) <= 0:
            raise RuntimeError("position has zero liquidity before execution")
        return current

    def _recover_pending_swaps(self, w3, pool: PoolConfig) -> list[dict]:
        if self.config.dry_run:
            return []
        out = []
        for job in self.journal.fetch_swap_pending_jobs(pool.chain, pool.pool_address, pool.bot_wallet):
            old_token_id = int(job["old_token_id"])
            tx_hash = str(job["swap_tx_hash"])
            try:
                receipt = w3.eth.get_transaction_receipt(tx_hash)
            except TransactionNotFound:
                try:
                    w3.eth.get_transaction(tx_hash)
                    out.append(
                        {
                            "pool": pool.name,
                            "token_id": old_token_id,
                            "state": PositionState.SWAP_PENDING.value,
                            "swap_tx_hash": tx_hash,
                        }
                    )
                    continue
                except TransactionNotFound:
                    reason = "pending swap tx not found on RPC; likely dropped"
                    self.journal.mark_status(
                        pool.chain,
                        old_token_id,
                        PositionState.SWAP_BLOCKED,
                        error_reason=reason,
                    )
                    self._notify_partial_action(
                        pool,
                        old_token_id,
                        "swap",
                        PositionState.SWAP_BLOCKED,
                        reason,
                        "recovery will retry swap from reservation if safe",
                        tx_hash=tx_hash,
                    )
                    out.append(
                        {
                            "pool": pool.name,
                            "token_id": old_token_id,
                            "state": PositionState.SWAP_BLOCKED.value,
                            "swap_tx_hash": tx_hash,
                            "error": "pending swap tx not found on RPC; likely dropped",
                        }
                    )
                    continue
            if int(receipt.get("status", 0)) == 1:
                effective_gas_price = int(receipt.get("effectiveGasPrice") or 0)
                self.journal.mark_status(
                    pool.chain,
                    old_token_id,
                    PositionState.WITHDRAWN_UNBURNED,
                    "swap",
                    tx_result=TxResult(
                        tx_hash=tx_hash,
                        gas_used=int(receipt.get("gasUsed") or 0),
                        gas_price_gwei=float(Web3.from_wei(effective_gas_price, "gwei")) if effective_gas_price else 0.0,
                    ),
                    error_reason="swap mined after timeout; mint resume requires next recovery phase",
                )
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": old_token_id,
                        "state": PositionState.WITHDRAWN_UNBURNED.value,
                        "swap_tx_hash": tx_hash,
                        "note": "swap mined after timeout; mint resume requires next recovery phase",
                    }
                )
            else:
                reason = "pending swap transaction reverted"
                self.journal.mark_status(
                    pool.chain,
                    old_token_id,
                    PositionState.SWAP_BLOCKED,
                    error_reason=reason,
                )
                self._notify_partial_action(
                    pool,
                    old_token_id,
                    "swap",
                    PositionState.SWAP_BLOCKED,
                    reason,
                    "recovery will retry swap from reservation if safe",
                    tx_hash=tx_hash,
                )
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": old_token_id,
                        "state": PositionState.SWAP_BLOCKED.value,
                        "swap_tx_hash": tx_hash,
                        "error": "pending swap transaction reverted",
                    }
                )
        return out

    def _recover_partial_jobs(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
    ) -> list[dict]:
        if self.config.dry_run:
            return []
        out: list[dict] = []
        action_count = 0
        for job in self.journal.fetch_recoverable_jobs(pool.chain, pool.pool_address, pool.bot_wallet):
            if action_count >= pool.max_jobs_per_cycle:
                break
            old_token_id = int(job["old_token_id"])
            status = str(job["status"])
            log_block(
                log,
                logging.INFO,
                "recovery start",
                pool_context(
                    pool,
                    job_id=job.get("id") or job.get("job_id"),
                    old_token_id=old_token_id,
                    new_token_id=job.get("new_token_id"),
                ),
                {
                    "stage": "recovery_start",
                    "job_status": status,
                    "withdraw_tx_hash": job.get("withdraw_tx_hash"),
                    "swap_tx_hash": job.get("swap_tx_hash"),
                    "mint_tx_hash": job.get("mint_tx_hash"),
                    "stake_tx_hash": job.get("stake_tx_hash"),
                    "burn_tx_hash": job.get("burn_tx_hash"),
                    "reserved_token0_raw": job.get("reserved_token0_raw"),
                    "reserved_token1_raw": job.get("reserved_token1_raw"),
                    "next_action": "stake minted token" if status == PositionState.MINTED_UNSTAKED.value else "inspect partial job",
                },
            )
            try:
                wallet_lock = f"rebalance:{pool.chain}:{pool.bot_wallet.lower()}"
                pool_lock = f"rebalance:{pool.chain}:{pool.pool_address.lower()}"
                with mysql_advisory_lock(wallet_lock, self.config.lock_timeout_seconds):
                    with mysql_advisory_lock(pool_lock, self.config.lock_timeout_seconds):
                        if status == PositionState.MINTED_UNSTAKED.value:
                            result = self._recover_minted_unstaked(pool, adapter, job)
                        elif status == PositionState.UNSTAKED_UNWITHDRAWN.value:
                            result = self._recover_unstaked_unwithdrawn(w3, pool, adapter, job)
                        else:
                            result = self._recover_withdrawn_unminted(w3, pool, adapter, job)
                out.append(result)
                log_block(
                    log,
                    logging.INFO,
                    "recovery end",
                    pool_context(pool, old_token_id=old_token_id, new_token_id=result.get("new_token_id")),
                    {
                        "stage": "recovery_end",
                        "result_state": result.get("state"),
                        "recovery": result.get("recovery"),
                        "action_taken": result.get("action_taken"),
                    },
                )
                if result.get("action_taken"):
                    action_count += 1
            except Exception as exc:
                recovery_attempt = int(job.get("recovery_attempts") or 0) + 1
                retry_stake = (
                    status == PositionState.MINTED_UNSTAKED.value
                    and recovery_attempt < MAX_STAKE_RECOVERY_ATTEMPTS
                    and self._is_definite_prebroadcast_stake_error(exc)
                )
                if retry_stake:
                    reason = f"partial recovery failed: {exc}"
                    log.warning(
                        "stake recovery deferred pool=%s tokenId=%s attempt=%s/%s reason=%s",
                        pool.name,
                        old_token_id,
                        recovery_attempt,
                        MAX_STAKE_RECOVERY_ATTEMPTS,
                        exc,
                    )
                    self.journal.mark_status(
                        pool.chain,
                        old_token_id,
                        PositionState.MINTED_UNSTAKED,
                        error_reason=reason,
                    )
                    self.journal.mark_recovery_error(pool.chain, old_token_id, reason)
                    out.append(
                        {
                            "pool": pool.name,
                            "token_id": old_token_id,
                            "state": PositionState.MINTED_UNSTAKED.value,
                            "recovery": "STAKE_RETRY_QUEUED",
                            "recovery_attempt": recovery_attempt,
                            "error": str(exc),
                        }
                    )
                    action_count += 1
                    continue
                log_block(
                    log,
                    logging.ERROR,
                    "error context",
                    pool_context(pool, old_token_id=old_token_id),
                    {
                        "stage": "partial_recovery",
                        "action": "recover_partial_job",
                        "reason": exc,
                        "recovery_impact": "mark RECOVERY_REQUIRED and notify if enabled",
                    },
                )
                log.exception("partial recovery failed pool=%s tokenId=%s: %s", pool.name, old_token_id, exc)
                self.journal.mark_status(
                    pool.chain,
                    old_token_id,
                    PositionState.RECOVERY_REQUIRED,
                    error_reason=f"partial recovery failed: {exc}",
                )
                self.journal.mark_recovery_error(pool.chain, old_token_id, f"partial recovery failed: {exc}")
                self._notify_recovery_required(pool, old_token_id, f"partial recovery failed: {exc}")
                out.append(
                    {
                        "pool": pool.name,
                        "token_id": old_token_id,
                        "state": PositionState.RECOVERY_REQUIRED.value,
                        "recovery": "FAILED",
                        "error": str(exc),
                    }
                )
                action_count += 1
        return out

    def _recover_minted_unstaked(
        self,
        pool: PoolConfig,
        adapter: DexAdapter,
        job: dict,
    ) -> dict:
        old_token_id = int(job["old_token_id"])
        new_token_id = int(job["new_token_id"])
        position = self._position_from_job(pool, job)
        if position is None:
            return self._mark_manual_recovery(pool, old_token_id, "missing old range data for recovery")
        restore_staked = self._job_restore_staked(job, pool, adapter)

        new_position = adapter.read_npm_position(new_token_id, owner=pool.bot_wallet)
        log.info(
            "minted unstaked recovery check pool=%s chain=%s old_tokenId=%s new_tokenId=%s "
            "owner=%s liquidity=%s range=(%s,%s)",
            pool.name,
            pool.chain,
            old_token_id,
            new_token_id,
            new_position.owner,
            new_position.liquidity,
            new_position.tick_lower,
            new_position.tick_upper,
        )
        if new_position.owner.lower() != pool.bot_wallet.lower():
            staking_owner = self._adapter_staking_owner(adapter)
            if staking_owner and new_position.owner.lower() == staking_owner.lower():
                staked_positions = adapter.read_staked_positions([new_token_id])
                staked_position = staked_positions.get(new_token_id)
                if staked_position and self._matches_pool(pool, staked_position):
                    if not restore_staked:
                        return self._mark_manual_recovery(
                            pool,
                            old_token_id,
                            "new token was manually staked while recovery requires UNSTAKED custody",
                        )
                    self.journal.mark_status(
                        pool.chain,
                        old_token_id,
                        PositionState.REMINTED,
                        new_token_id=new_token_id,
                    )
                    try:
                        burn_tx = adapter.burn_if_empty_and_owned(old_token_id)
                        if burn_tx:
                            self.journal.mark_status(pool.chain, old_token_id, PositionState.BURNED, "burn", burn_tx, new_token_id)
                    except Exception as exc:
                        log.warning("burn failed during staked recovery for %s tokenId=%s: %s", pool.name, old_token_id, exc)
                    self.journal.clear_recovery_error(pool.chain, old_token_id)
                    self._notify_discord_pnl_after_delay(pool, position.owner, old_token_id, new_token_id)
                    return {
                        "pool": pool.name,
                        "token_id": old_token_id,
                        "new_token_id": new_token_id,
                        "state": PositionState.REMINTED.value,
                        "recovery": "JOURNAL_REPAIRED_STAKED_TOKEN",
                    }
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                f"new token {new_token_id} is not owned by bot wallet",
            )
        if not self._matches_pool(pool, new_position):
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                f"new token {new_token_id} does not match configured pool",
            )
        if int(new_position.liquidity) <= 0:
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                f"new token {new_token_id} has zero liquidity",
            )
        if restore_staked:
            log.info(
                "stake recovery attempt pool=%s chain=%s old_tokenId=%s new_tokenId=%s",
                pool.name,
                pool.chain,
                old_token_id,
                new_token_id,
            )
            stake_tx = adapter.stake(new_token_id)
            stake_confirmed = self._confirm_stake_with_retry(pool, adapter, old_token_id, new_token_id)
            if not stake_confirmed:
                reason = "stake transaction succeeded but staking contract membership was not confirmed"
                self.journal.mark_status(
                    pool.chain,
                    old_token_id,
                    PositionState.RECOVERY_REQUIRED,
                    "stake",
                    stake_tx,
                    new_token_id,
                    error_reason=reason,
                )
                self.journal.mark_recovery_error(pool.chain, old_token_id, reason)
                self._notify_recovery_required(pool, old_token_id, reason)
                return {
                    "pool": pool.name,
                    "token_id": old_token_id,
                    "new_token_id": new_token_id,
                    "state": PositionState.RECOVERY_REQUIRED.value,
                    "recovery": "STAKE_CONFIRMATION_REQUIRED",
                    "action_taken": True,
                    "error": reason,
                }
            self.journal.mark_status(pool.chain, old_token_id, PositionState.REMINTED, "stake", stake_tx, new_token_id)
        else:
            self.journal.mark_status(
                pool.chain,
                old_token_id,
                PositionState.REMINTED_UNSTAKED,
                new_token_id=new_token_id,
            )
        try:
            burn_tx = adapter.burn_if_empty_and_owned(old_token_id)
            if burn_tx:
                self.journal.mark_status(pool.chain, old_token_id, PositionState.BURNED, "burn", burn_tx, new_token_id)
        except Exception as exc:
            log.warning("burn failed during recovery for %s tokenId=%s: %s", pool.name, old_token_id, exc)
        self.journal.clear_recovery_error(pool.chain, old_token_id)
        self._notify_discord_pnl_after_delay(pool, position.owner, old_token_id, new_token_id)
        return {
            "pool": pool.name,
            "token_id": old_token_id,
            "new_token_id": new_token_id,
            "state": PositionState.REMINTED.value if restore_staked else PositionState.REMINTED_UNSTAKED.value,
            "recovery": "STAKE_RETRIED" if restore_staked else "UNSTAKED_MODE_CONFIRMED",
            "action_taken": restore_staked,
        }

    def _recover_unstaked_unwithdrawn(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        job: dict,
    ) -> dict:
        old_token_id = int(job["old_token_id"])
        position = self._position_from_job(pool, job)
        if position is None:
            return self._mark_manual_recovery(pool, old_token_id, "missing old range data for unstake recovery")
        try:
            live_position = adapter.read_npm_position(old_token_id)
        except Exception as exc:
            return self._mark_manual_recovery(pool, old_token_id, f"could not read unstaked token owner: {exc}")
        staking_owner = self._adapter_staking_owner(adapter)
        if live_position.owner.lower() != pool.bot_wallet.lower():
            if staking_owner and live_position.owner.lower() == staking_owner.lower():
                return self._mark_manual_recovery(
                    pool,
                    old_token_id,
                    "unstake tx is recorded but NFT is still owned by staking contract",
                )
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                f"unstaked token owner mismatch: owner={live_position.owner}",
            )

        restore_staked = bool(position.is_staked)
        position.is_staked = False
        pre0, pre1 = adapter.read_balances(pool.bot_wallet)
        slot0 = adapter.read_slot0()
        withdraw_tx = adapter.decrease_collect_withdraw(position, slot0)
        self.journal.mark_status(pool.chain, old_token_id, PositionState.WITHDRAWN_UNBURNED, "withdraw", withdraw_tx)
        post0, post1 = self._read_recovered_balances_with_retry(adapter, pool, pre0, pre1)
        self.journal.record_balance_snapshot(pool.chain, old_token_id, "post_withdraw", post0.raw, post1.raw)
        receipt_inflows = self._receipt_token_inflows(w3, pool, withdraw_tx.tx_hash, pool.bot_wallet)
        if receipt_inflows is None or (receipt_inflows[0] <= 0 and receipt_inflows[1] <= 0):
            recovered0 = max(0, post0.raw - pre0.raw)
            recovered1 = max(0, post1.raw - pre1.raw)
        else:
            recovered0, recovered1 = receipt_inflows
        if recovered0 <= 0 and recovered1 <= 0:
            reason = "unstake recovery withdraw produced no token inflows; manual recovery required"
            self.journal.mark_recovery_error(pool.chain, old_token_id, reason)
            return self._mark_manual_recovery(pool, old_token_id, reason)
        self.journal.record_reservation(
            pool.chain,
            old_token_id,
            pool.token0_address,
            pool.token1_address,
            recovered0,
            recovered1,
        )
        repaired_position = PositionSnapshot(
            **{
                **position.__dict__,
                "owner": pool.bot_wallet,
                "is_staked": restore_staked,
            }
        )
        return self._resume_mint_from_reservation(
            w3,
            pool,
            adapter,
            repaired_position,
            recovered0,
            recovered1,
            allow_swap=True,
        )

    def _recover_withdrawn_unminted(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        job: dict,
    ) -> dict:
        old_token_id = int(job["old_token_id"])
        position = self._position_from_job(pool, job)
        if position is None:
            return self._mark_manual_recovery(pool, old_token_id, "missing old range data for recovery")
        mint_tx_hash = str(job.get("mint_tx_hash") or "")
        if self._real_tx_hash(mint_tx_hash):
            log_block(
                log,
                logging.INFO,
                "recovery decision",
                pool_context(pool, old_token_id=old_token_id),
                {
                    "stage": "recovery_decision",
                    "job_status": job.get("status"),
                    "mint_tx_hash": mint_tx_hash,
                    "next_action": "recover mint receipt and stake if valid",
                },
            )
            return self._recover_unknown_mint(w3, pool, adapter, job, position, mint_tx_hash)
        reservation = self._reservation_pair_from_job(job, pool)
        if reservation is None:
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                "missing reservation ledger for recovery; manual recovery required",
            )

        swap_tx_hash = str(job.get("swap_tx_hash") or "")
        if self._real_tx_hash(swap_tx_hash):
            log_block(
                log,
                logging.INFO,
                "recovery decision",
                pool_context(pool, old_token_id=old_token_id),
                {
                    "stage": "recovery_decision",
                    "job_status": job.get("status"),
                    "swap_tx_hash": swap_tx_hash,
                    "reserved_token0_raw": reservation[0],
                    "reserved_token1_raw": reservation[1],
                    "next_action": "inspect previous swap receipt",
                },
            )
            normalized_swap_hash = self._normalize_tx_hash_for_rpc(swap_tx_hash)
            swap_receipt = self._fetch_receipt_with_rpc_fallback(w3, pool, old_token_id, normalized_swap_hash)
            if swap_receipt is None:
                return self._mark_manual_recovery(
                    pool,
                    old_token_id,
                    "swap tx exists but receipt is unavailable",
                )
            if int(swap_receipt.get("status", 0)) != 1:
                reason = f"previous swap tx reverted on-chain: {normalized_swap_hash}; retrying from reservation"
                log_block(
                    log,
                    logging.WARNING,
                    "swap reverted",
                    pool_context(pool, old_token_id=old_token_id),
                    {
                        "stage": "swap_receipt",
                        "status": "SWAP_BLOCKED",
                        "tx_hash": normalized_swap_hash,
                        "block": swap_receipt.get("blockNumber"),
                        "gas_used": swap_receipt.get("gasUsed"),
                        "reserved_token0_raw": reservation[0],
                        "reserved_token1_raw": reservation[1],
                        "reason": "previous swap tx reverted on-chain",
                        "next_action": "retry swap from reservation",
                    },
                )
                self.journal.mark_status(
                    pool.chain,
                    old_token_id,
                    PositionState.SWAP_BLOCKED,
                    error_reason=reason,
                )
                self._notify_partial_action(
                    pool,
                    old_token_id,
                    "swap",
                    PositionState.SWAP_BLOCKED,
                    reason,
                    "recovery will retry swap from reservation",
                    tx_hash=normalized_swap_hash,
                )
                return self._resume_mint_from_reservation(
                    w3,
                    pool,
                    adapter,
                    position,
                    reservation[0],
                    reservation[1],
                    allow_swap=True,
                )
            post_swap = self._snapshot_pair_from_job(job, "post_swap", pool)
            if post_swap is None:
                reconciled = self._reconcile_existing_swap_reservation(
                    w3,
                    pool,
                    adapter,
                    position,
                    swap_tx_hash,
                    reservation[0],
                    reservation[1],
                    receipt=swap_receipt,
                )
                if isinstance(reconciled, dict):
                    return reconciled
                reservation = reconciled
            return self._resume_mint_from_reservation(
                w3,
                pool,
                adapter,
                position,
                reservation[0],
                reservation[1],
                allow_swap=False,
            )

        log_block(
            log,
            logging.INFO,
            "recovery decision",
            pool_context(pool, old_token_id=old_token_id),
            {
                "stage": "recovery_decision",
                "job_status": job.get("status"),
                "reserved_token0_raw": reservation[0],
                "reserved_token1_raw": reservation[1],
                "allow_swap": True,
                "next_action": "resume swap/mint from reservation",
            },
        )
        return self._resume_mint_from_reservation(
            w3,
            pool,
            adapter,
            position,
            reservation[0],
            reservation[1],
            allow_swap=True,
        )

    def _reconcile_existing_swap_reservation(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        position: PositionSnapshot,
        swap_tx_hash: str,
        reserved0: int,
        reserved1: int,
        receipt=None,
    ) -> tuple[int, int] | dict:
        normalized = self._normalize_tx_hash_for_rpc(swap_tx_hash)
        if not normalized:
            return self._mark_manual_recovery(pool, position.token_id, "swap tx hash is invalid")
        if receipt is None:
            receipt = self._fetch_receipt_with_rpc_fallback(w3, pool, position.token_id, normalized)
        if receipt is None:
            return self._mark_manual_recovery(pool, position.token_id, "swap tx exists but receipt is unavailable")
        if int(receipt.get("status", 0)) != 1:
            return self._mark_manual_recovery(pool, position.token_id, "swap tx reverted")

        sent0, sent1, received0, received1 = self._token_movements_from_receipt(receipt, pool, pool.bot_wallet)
        if sent0 <= 0 and sent1 <= 0 and received0 <= 0 and received1 <= 0:
            return self._mark_manual_recovery(pool, position.token_id, "swap receipt has no token movement for wallet")

        old_reserved0 = max(0, int(reserved0))
        old_reserved1 = max(0, int(reserved1))
        new_reserved0 = max(0, old_reserved0 - int(sent0) + int(received0))
        new_reserved1 = max(0, old_reserved1 - int(sent1) + int(received1))
        try:
            post_swap0, post_swap1 = adapter.read_balances(pool.bot_wallet)
        except Exception as exc:
            log.warning(
                "could not read post-swap snapshot after reservation reconcile pool=%s tokenId=%s tx=%s: %s",
                pool.name,
                position.token_id,
                swap_tx_hash,
                exc,
            )
            return self._mark_manual_recovery(
                pool,
                position.token_id,
                "swap reservation reconciled but post-swap snapshot could not be recorded",
            )
        self.journal.record_reservation(
            pool.chain,
            position.token_id,
            pool.token0_address,
            pool.token1_address,
            new_reserved0,
            new_reserved1,
        )
        self.journal.record_balance_snapshot(
            pool.chain,
            position.token_id,
            "post_swap",
            post_swap0.raw,
            post_swap1.raw,
        )
        log.info(
            "recovery swap reservation reconciled pool=%s tokenId=%s tx=%s "
            "old_reserved0=%s old_reserved1=%s sent0=%s sent1=%s received0=%s received1=%s "
            "new_reserved0=%s new_reserved1=%s",
            pool.name,
            position.token_id,
            swap_tx_hash,
            old_reserved0,
            old_reserved1,
            sent0,
            sent1,
            received0,
            received1,
            new_reserved0,
            new_reserved1,
        )
        return new_reserved0, new_reserved1

    def _fetch_receipt_with_rpc_fallback(self, w3, pool: PoolConfig, token_id: int, tx_hash: str):
        attempts = [("primary", w3)]
        for label, url in self._receipt_rpc_urls(pool):
            attempts.append((label, self._web3_for_rpc(pool, url)))

        last_error = None
        for rpc_label, candidate_w3 in attempts:
            try:
                receipt = candidate_w3.eth.get_transaction_receipt(tx_hash)
                if rpc_label != "primary":
                    log.info(
                        "recovered swap receipt via rpc fallback pool=%s tokenId=%s tx=%s rpc=%s",
                        pool.name,
                        token_id,
                        tx_hash,
                        rpc_label,
                    )
                return receipt
            except Exception as exc:
                last_error = exc
                log.warning(
                    "swap receipt lookup failed pool=%s tokenId=%s tx=%s rpc=%s error=%s",
                    pool.name,
                    token_id,
                    tx_hash,
                    rpc_label,
                    exc,
                )
        log.warning(
            "swap receipt lookup failed on all rpc attempts pool=%s tokenId=%s tx=%s attempts=%s last_error=%s",
            pool.name,
            token_id,
            tx_hash,
            len(attempts),
            last_error,
        )
        return None

    def _receipt_rpc_urls(self, pool: PoolConfig) -> list[tuple[str, str]]:
        try:
            from latest_farms.config import RPC_BACKUP_LIST, RPC_URLS_2
        except ImportError:  # pragma: no cover
            from config import RPC_BACKUP_LIST, RPC_URLS_2

        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        primary = RPC_URLS_2.get(pool.chain)
        if primary:
            seen.add(primary)
            out.append((f"configured:{self._rpc_label(primary)}", primary))
        for index, url in enumerate(RPC_BACKUP_LIST.get(pool.chain, []), start=1):
            if not url or url in seen:
                continue
            seen.add(url)
            out.append((f"backup-{index}:{self._rpc_label(url)}", url))
        return out

    def _web3_for_rpc(self, pool: PoolConfig, url: str) -> Web3:
        candidate = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        if pool.chain.upper() == "BNB":
            try:
                from web3.middleware import ExtraDataToPOAMiddleware

                candidate.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware

                candidate.middleware_onion.inject(geth_poa_middleware, layer=0)
        return candidate

    @staticmethod
    def _rpc_label(url: str) -> str:
        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            return parsed.netloc or "unknown-rpc"
        except Exception:
            return "unknown-rpc"

    def _lookup_mint_tx_with_rpc_fallback(self, w3, pool: PoolConfig, token_id: int, tx_hash: str) -> dict:
        attempts = [("primary", w3)]
        for label, url in self._receipt_rpc_urls(pool):
            attempts.append((label, self._web3_for_rpc(pool, url)))

        errors = []
        not_found_count = 0
        for rpc_label, candidate_w3 in attempts:
            try:
                receipt = candidate_w3.eth.get_transaction_receipt(tx_hash)
                log_block(
                    log,
                    logging.INFO,
                    "mint tx lookup",
                    pool_context(pool, old_token_id=token_id),
                    {
                        "stage": "mint_tx_lookup",
                        "status": "RECEIPT_FOUND",
                        "tx_hash": tx_hash,
                        "rpc": rpc_label,
                        "receipt_status": receipt.get("status"),
                        "block": receipt.get("blockNumber"),
                    },
                )
                return {"status": "RECEIPT_SUCCESS" if int(receipt.get("status", 0)) == 1 else "RECEIPT_REVERTED", "receipt": receipt, "rpc_label": rpc_label}
            except TransactionNotFound:
                pass
            except Exception as exc:
                errors.append(f"{rpc_label}: receipt: {exc}")
                log_block(
                    log,
                    logging.WARNING,
                    "mint tx lookup",
                    pool_context(pool, old_token_id=token_id),
                    {
                        "stage": "mint_tx_lookup",
                        "status": "RECEIPT_LOOKUP_FAILED",
                        "tx_hash": tx_hash,
                        "rpc": rpc_label,
                        "reason": exc,
                    },
                )
                continue

            try:
                candidate_w3.eth.get_transaction(tx_hash)
                log_block(
                    log,
                    logging.WARNING,
                    "mint tx lookup",
                    pool_context(pool, old_token_id=token_id),
                    {
                        "stage": "mint_tx_lookup",
                        "status": "TX_PENDING_OR_ACCEPTED",
                        "tx_hash": tx_hash,
                        "rpc": rpc_label,
                        "next_action": "wait for receipt; do not retry mint",
                    },
                )
                return {"status": "TX_PENDING_OR_ACCEPTED", "receipt": None, "rpc_label": rpc_label}
            except TransactionNotFound:
                not_found_count += 1
                log_block(
                    log,
                    logging.INFO,
                    "mint tx lookup",
                    pool_context(pool, old_token_id=token_id),
                    {
                        "stage": "mint_tx_lookup",
                        "status": "TX_NOT_FOUND_ON_RPC",
                        "tx_hash": tx_hash,
                        "rpc": rpc_label,
                    },
                )
            except Exception as exc:
                errors.append(f"{rpc_label}: tx: {exc}")
                log_block(
                    log,
                    logging.WARNING,
                    "mint tx lookup",
                    pool_context(pool, old_token_id=token_id),
                    {
                        "stage": "mint_tx_lookup",
                        "status": "TX_LOOKUP_FAILED",
                        "tx_hash": tx_hash,
                        "rpc": rpc_label,
                        "reason": exc,
                    },
                )
        if errors:
            return {"status": "LOOKUP_INCONCLUSIVE", "receipt": None, "errors": errors}
        log_block(
            log,
            logging.WARNING,
            "mint tx not found",
            pool_context(pool, old_token_id=token_id),
            {
                "stage": "mint_tx_lookup",
                "status": "TX_NOT_FOUND",
                "tx_hash": tx_hash,
                "rpc_attempts": len(attempts),
                "not_found_count": not_found_count,
            },
        )
        return {"status": "TX_NOT_FOUND", "receipt": None}

    @staticmethod
    def _job_age_seconds(job: dict) -> float:
        updated_at = job.get("updated_at")
        if updated_at is None:
            return 999999.0
        if isinstance(updated_at, str):
            try:
                updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            except ValueError:
                return 999999.0
        if getattr(updated_at, "tzinfo", None) is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds())

    def _recover_unknown_mint(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        job: dict,
        position: PositionSnapshot,
        mint_tx_hash: str,
    ) -> dict:
        old_token_id = int(job["old_token_id"])
        new_tick_lower = self._int_metadata(job.get("new_tick_lower"))
        new_tick_upper = self._int_metadata(job.get("new_tick_upper"))
        if new_tick_lower is None or new_tick_upper is None:
            return self._mark_manual_recovery(pool, old_token_id, "mint tx exists but new range is missing")
        plan = RebalancePlan(
            old_token_id=old_token_id,
            current_tick=0,
            old_tick_lower=position.tick_lower,
            old_tick_upper=position.tick_upper,
            new_tick_lower=new_tick_lower,
            new_tick_upper=new_tick_upper,
            amount0_desired=self._int_metadata(job.get("amount0_desired")) or 0,
            amount1_desired=self._int_metadata(job.get("amount1_desired")) or 0,
        )
        lookup = self._lookup_mint_tx_with_rpc_fallback(w3, pool, old_token_id, mint_tx_hash)
        receipt = lookup.get("receipt")
        if receipt is None:
            status = lookup.get("status")
            if status == "TX_NOT_FOUND":
                age_seconds = self._job_age_seconds(job)
                if age_seconds >= 600:
                    reason = (
                        "mint hash not found on-chain after RPC fallback; "
                        "clearing stale mint hash and retrying from reservation"
                    )
                    log_block(
                        log,
                        logging.WARNING,
                        "mint hash cleared for retry",
                        pool_context(pool, old_token_id=old_token_id),
                        {
                            "stage": "mint_tx_lookup",
                            "status": "MINT_TX_NOT_FOUND",
                            "mint_tx_hash": mint_tx_hash,
                            "job_age_seconds": int(age_seconds),
                            "reason": reason,
                            "next_action": "retry mint from reservation",
                        },
                    )
                    if hasattr(self.journal, "clear_mint_tx_for_retry"):
                        self.journal.clear_mint_tx_for_retry(pool.chain, old_token_id, reason)
                    else:
                        self.journal.mark_status(
                            pool.chain,
                            old_token_id,
                            PositionState.WITHDRAWN_UNBURNED,
                            error_reason=reason,
                        )
                    self._notify_partial_action(
                        pool,
                        old_token_id,
                        "mint",
                        "MINT_TX_NOT_FOUND",
                        reason,
                        "retry mint from reservation",
                        signed_tx_hash=mint_tx_hash,
                    )
                    repaired_job = {**job, "mint_tx_hash": None, "status": PositionState.WITHDRAWN_UNBURNED.value}
                    return self._recover_withdrawn_unminted(w3, pool, adapter, repaired_job)
                return self._mark_manual_recovery(
                    pool,
                    old_token_id,
                    "mint tx not found on-chain yet; waiting before retrying mint",
                )
            if status == "TX_PENDING_OR_ACCEPTED":
                return self._mark_manual_recovery(
                    pool,
                    old_token_id,
                    "mint tx pending/accepted but receipt is not available",
                )
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                "mint tx lookup inconclusive; receipt is not available",
            )
        if int(receipt.get("status", 0)) != 1:
            return self._mark_manual_recovery(pool, old_token_id, "mint tx exists but transaction reverted")
        new_token_id = adapter._new_token_id_from_mint_receipt(receipt)
        if new_token_id is None:
            return self._mark_manual_recovery(pool, old_token_id, "mint tx receipt has no IncreaseLiquidity token id")
        if hasattr(adapter, "_validate_minted_position_detail"):
            validation = adapter._validate_minted_position_detail(
                new_token_id,
                plan,
                receipt_block=int(receipt.get("blockNumber") or 0),
            )
            validation_ok = validation.can_stake
            validation_reason = validation.reason
            if validation.status == "VALID_WITH_RANGE_WARNING":
                log_block(
                    log,
                    logging.WARNING,
                    "mint validation warning",
                    pool_context(pool, old_token_id=old_token_id, new_token_id=new_token_id),
                    {
                        "stage": "mint_validation",
                        "validation_status": validation.status,
                        "reason": validation.reason,
                        "rpc": validation.rpc_label,
                        "next_action": "stake recovered mint because owner/pool/liquidity are valid",
                    },
                )
        elif hasattr(adapter, "_validate_minted_position"):
            validation_ok, validation_reason = adapter._validate_minted_position(
                new_token_id,
                plan,
                receipt_block=int(receipt.get("blockNumber") or 0),
            )
        else:
            validation_ok = adapter._minted_position_matches_plan(new_token_id, plan)
            validation_reason = "minted token does not match recovery job"
        if not validation_ok:
            return self._mark_manual_recovery(
                pool,
                old_token_id,
                f"minted token does not match recovery job: {validation_reason}",
            )

        effective_gas_price = int(receipt.get("effectiveGasPrice") or 0)
        mint_tx = TxResult(
            tx_hash=self._hex_value(receipt.get("transactionHash")),
            status="RECOVERED",
            gas_used=int(receipt.get("gasUsed") or 0),
            gas_price_gwei=float(Web3.from_wei(effective_gas_price, "gwei")) if effective_gas_price else 0.0,
            metadata={
                "label": "mint",
                "recovered_from_receipt": True,
                "receipt_block": int(receipt.get("blockNumber") or 0),
            },
        )
        self.journal.mark_status(
            pool.chain,
            old_token_id,
            PositionState.MINTED_UNSTAKED,
            "mint",
            mint_tx,
            new_token_id,
            mint_tick_lower=new_tick_lower,
            mint_tick_upper=new_tick_upper,
        )
        repaired_job = {**job, "new_token_id": new_token_id, "status": PositionState.MINTED_UNSTAKED.value}
        return self._recover_minted_unstaked(pool, adapter, repaired_job)

    def _resume_mint_from_reservation(
        self,
        w3,
        pool: PoolConfig,
        adapter: DexAdapter,
        position: PositionSnapshot,
        reserved0: int,
        reserved1: int,
        allow_swap: bool,
    ) -> dict:
        lower_percent, upper_percent, percent_source = self._resolve_range_percent(pool, position)
        recovered0 = max(0, int(reserved0))
        recovered1 = max(0, int(reserved1))
        slot_for_mint = adapter.read_slot0()
        log.info(
            "resume mint from reservation start pool=%s chain=%s tokenId=%s allow_swap=%s "
            "reserved0=%s reserved1=%s current_tick=%s",
            pool.name,
            pool.chain,
            position.token_id,
            allow_swap,
            recovered0,
            recovered1,
            slot_for_mint.tick,
        )
        if allow_swap:
            swap_plan = SwapPlanner().build_swap_plan(
                pool,
                position,
                slot_for_mint,
                recovered0,
                recovered1,
                lower_percent,
                upper_percent,
                percent_source,
            )
        else:
            swap_plan = RebalancePlanner().build_plan(
                pool,
                position,
                slot_for_mint,
                recovered0,
                recovered1,
                lower_percent,
                upper_percent,
                percent_source,
            )
        self._try_record_plan(pool, position, swap_plan)
        log.info(
            "recovery plan decision pool=%s chain=%s tokenId=%s old_range=(%s,%s) new_range=(%s,%s) "
            "range_source=%s amount0_desired=%s amount1_desired=%s swap_amount_in=%s swap_token_in=%s swap_token_out=%s",
            pool.name,
            pool.chain,
            position.token_id,
            position.tick_lower,
            position.tick_upper,
            swap_plan.new_tick_lower,
            swap_plan.new_tick_upper,
            swap_plan.metadata.get("range_percent_source"),
            swap_plan.amount0_desired,
            swap_plan.amount1_desired,
            getattr(swap_plan, "swap_amount_in", None),
            getattr(swap_plan, "swap_token_in", None),
            getattr(swap_plan, "swap_token_out", None),
        )

        if allow_swap and swap_plan.swap_amount_in > 0 and swap_plan.swap_token_in and swap_plan.swap_token_out:
            coverage_error = self._reservation_coverage_error(pool, adapter, "recovery_pre_swap")
            if coverage_error:
                return self._mark_manual_recovery(pool, position.token_id, coverage_error)
            pre_swap0, pre_swap1 = adapter.read_balances(pool.bot_wallet)
            token_in = Web3.to_checksum_address(swap_plan.swap_token_in).lower()
            token0 = Web3.to_checksum_address(pool.token0_address).lower()
            token1 = Web3.to_checksum_address(pool.token1_address).lower()
            if token_in == token0:
                swap_plan.swap_amount_in = min(int(swap_plan.swap_amount_in), recovered0)
            elif token_in == token1:
                swap_plan.swap_amount_in = min(int(swap_plan.swap_amount_in), recovered1)
            dust_reason = None
            if swap_plan.swap_amount_in > 0:
                dust_reason = self._swap_dust_reason(pool, swap_plan, recovered0, recovered1)
            if dust_reason:
                log.info(
                    "skip recovery dust swap pool=%s tokenId=%s amountIn=%s reason=%s",
                    pool.name,
                    position.token_id,
                    swap_plan.swap_amount_in,
                    dust_reason,
                )
                return self._mark_manual_recovery(
                    pool,
                    position.token_id,
                    f"dust swap required before mint; {dust_reason}",
                )
            if swap_plan.swap_amount_in > 0:
                swap_tx = adapter.swap(
                    swap_plan.swap_token_in,
                    swap_plan.swap_token_out,
                    swap_plan.swap_amount_in,
                )
                if not swap_tx:
                    reason = "recovery swap quote unavailable or price impact too high"
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.SWAP_BLOCKED,
                        error_reason=reason,
                    )
                    self._notify_partial_action(
                        pool,
                        position.token_id,
                        "swap",
                        PositionState.SWAP_BLOCKED,
                        reason,
                        "recovery will retry swap planning on the next cycle",
                    )
                    return {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.SWAP_BLOCKED.value,
                        "recovery": "SWAP_BLOCKED",
                    }
                if swap_tx.status == "PENDING":
                    reason = swap_tx.metadata.get("error") or "recovery swap receipt timeout"
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.SWAP_PENDING,
                        "swap",
                        swap_tx,
                        error_reason=reason,
                    )
                    self._notify_partial_action(
                        pool,
                        position.token_id,
                        "swap",
                        PositionState.SWAP_PENDING,
                        reason,
                        "worker will inspect swap receipt on the next cycle",
                        tx_hash=swap_tx.tx_hash,
                        signed_tx_hash=(swap_tx.metadata or {}).get("signed_tx_hash"),
                    )
                    return {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.SWAP_PENDING.value,
                        "swap_tx_hash": swap_tx.tx_hash,
                        "recovery": "SWAP_PENDING",
                        "action_taken": True,
                    }
                if swap_tx.status == "FAILED":
                    tx_label = "swap" if str(swap_tx.tx_hash).startswith("0x") else None
                    reason = swap_tx.metadata.get("error") or "recovery swap transaction failed"
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.SWAP_BLOCKED,
                        tx_label,
                        swap_tx if tx_label else None,
                        error_reason=reason,
                    )
                    self._notify_partial_action(
                        pool,
                        position.token_id,
                        "swap",
                        PositionState.SWAP_BLOCKED,
                        reason,
                        "recovery will retry swap from reservation if safe",
                        tx_hash=swap_tx.tx_hash if tx_label else None,
                        signed_tx_hash=(swap_tx.metadata or {}).get("signed_tx_hash"),
                    )
                    return {
                        "pool": pool.name,
                        "token_id": position.token_id,
                        "state": PositionState.SWAP_BLOCKED.value,
                        "recovery": "SWAP_FAILED",
                        "action_taken": True,
                    }
                if swap_tx.status != "SKIPPED":
                    self.journal.mark_status(
                        pool.chain,
                        position.token_id,
                        PositionState.WITHDRAWN_UNBURNED,
                        "swap",
                        swap_tx,
                    )
                    swap_receipt_inflows = self._receipt_token_inflows(
                        w3,
                        pool,
                        swap_tx.tx_hash,
                        pool.bot_wallet,
                    )
                    post_swap = self._read_post_swap_balances_with_retry(
                        w3,
                        adapter,
                        pool,
                        position,
                        pre_swap0,
                        pre_swap1,
                        swap_tx,
                    )
                    if post_swap is None:
                        if not self._swap_receipt_confirms_output(pool, swap_tx, swap_receipt_inflows):
                            return self._mark_manual_recovery(
                                pool,
                                position.token_id,
                                "recovery post-swap balance and receipt output not confirmed; mint skipped",
                            )
                        log.warning(
                            "recovery post-swap balance confirmation failed but receipt output was verified; "
                            "continuing with receipt reservation pool=%s tokenId=%s tx=%s inflows=%s",
                            pool.name,
                            position.token_id,
                            swap_tx.tx_hash,
                            swap_receipt_inflows,
                        )
                        post_swap0, post_swap1 = adapter.read_balances(pool.bot_wallet)
                    else:
                        post_swap0, post_swap1 = post_swap
                    self.journal.record_balance_snapshot(
                        pool.chain,
                        position.token_id,
                        "post_swap",
                        post_swap0.raw,
                        post_swap1.raw,
                    )
                    if swap_receipt_inflows is None or (
                        swap_receipt_inflows[0] <= 0 and swap_receipt_inflows[1] <= 0
                    ):
                        log.warning(
                            "recovery swap receipt token inflows unavailable; using balance delta fallback "
                            "pool=%s tokenId=%s tx=%s",
                            pool.name,
                            position.token_id,
                            swap_tx.tx_hash,
                        )
                    reserved0, reserved1 = self._reservation_after_swap(
                        pool,
                        reserved0,
                        reserved1,
                        pre_swap0,
                        pre_swap1,
                        post_swap0,
                        post_swap1,
                        swap_tx,
                        swap_receipt_inflows,
                    )
                    self.journal.record_reservation(
                        pool.chain,
                        position.token_id,
                        pool.token0_address,
                        pool.token1_address,
                        reserved0,
                        reserved1,
                    )
                    recovered0 = max(0, int(reserved0))
                    recovered1 = max(0, int(reserved1))
                    slot_for_mint = adapter.read_slot0()
                    swap_plan = RebalancePlanner().build_plan(
                        pool,
                        position,
                        slot_for_mint,
                        recovered0,
                        recovered1,
                        lower_percent,
                        upper_percent,
                        percent_source,
                    )

        coverage_error = self._reservation_coverage_error(pool, adapter, "recovery_pre_mint")
        if coverage_error:
            return self._mark_manual_recovery(pool, position.token_id, coverage_error)
        pre_mint0, pre_mint1 = adapter.read_balances(pool.bot_wallet)
        original_amount0, original_amount1, available0, available1 = self._clamp_plan_to_reservation(
            swap_plan,
            reserved0,
            reserved1,
            pre_mint0,
            pre_mint1,
        )
        log.info(
            "rebalance recovery checkpoint pool=%s tokenId=%s stage=pre_mint "
            "pre_mint0=%s pre_mint1=%s available0_for_mint=%s available1_for_mint=%s "
            "original_amount0_desired=%s original_amount1_desired=%s "
            "clamped_amount0_desired=%s clamped_amount1_desired=%s",
            pool.name,
            position.token_id,
            pre_mint0.raw,
            pre_mint1.raw,
            available0,
            available1,
            original_amount0,
            original_amount1,
            swap_plan.amount0_desired,
            swap_plan.amount1_desired,
        )
        if original_amount0 != swap_plan.amount0_desired or original_amount1 != swap_plan.amount1_desired:
            self._try_record_plan(pool, position, swap_plan)
        if swap_plan.amount0_desired <= 0 and swap_plan.amount1_desired <= 0:
            self.journal.mark_status(
                pool.chain,
                position.token_id,
                PositionState.WITHDRAWN_UNBURNED,
                error_reason="zero recovery mint amounts after pre-mint balance clamp; skipped mint",
            )
            return {
                "pool": pool.name,
                "token_id": position.token_id,
                "state": PositionState.WITHDRAWN_UNBURNED.value,
                "recovery": "ZERO_MINT_AMOUNTS",
            }
        if not (swap_plan.new_tick_lower <= slot_for_mint.tick < swap_plan.new_tick_upper):
            self.journal.mark_status(
                pool.chain,
                position.token_id,
                PositionState.FAILED,
                error_reason="current tick moved outside recovery range before mint",
            )
            return {
                "pool": pool.name,
                "token_id": position.token_id,
                "state": PositionState.FAILED.value,
                "recovery": "TICK_OUTSIDE_NEW_RANGE",
            }

        mint_tx, new_token_id = adapter.mint(swap_plan)
        if not new_token_id:
            reason = (
                mint_tx.metadata.get("error")
                if mint_tx and mint_tx.metadata
                else "recovery mint token id was not parsed"
            )
            if mint_tx and mint_tx.status in {"BROADCAST_UNKNOWN", "PENDING"}:
                signed_hash = (mint_tx.metadata or {}).get("signed_tx_hash")
                reason = (
                    f"{reason}; tx_status={mint_tx.status}; tx_hash={mint_tx.tx_hash}; "
                    f"signed_tx_hash={signed_hash}; next_action=journal recovery will inspect receipt"
                )
            mint_tx_label = None if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN" else "mint"
            mint_tx_for_journal = None if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN" else mint_tx
            self.journal.mark_status(
                pool.chain,
                position.token_id,
                PositionState.RECOVERY_REQUIRED,
                mint_tx_label,
                mint_tx_for_journal,
                error_reason=f"recovery mint reconciliation required: {reason}",
            )
            self.journal.mark_recovery_error(
                pool.chain,
                position.token_id,
                f"recovery mint reconciliation required: {reason}",
            )
            if mint_tx and mint_tx.status == "BROADCAST_UNKNOWN":
                self._notify_partial_action(
                    pool,
                    position.token_id,
                    "mint",
                    "MINT_BROADCAST_UNKNOWN",
                    f"recovery mint broadcast unknown: {reason}",
                    "recovery will retry mint only after confirming the signed hash is not on-chain",
                    signed_tx_hash=(mint_tx.metadata or {}).get("signed_tx_hash"),
                )
            self._notify_recovery_required(pool, position.token_id, f"recovery mint reconciliation required: {reason}")
            return {
                "pool": pool.name,
                "token_id": position.token_id,
                "state": PositionState.RECOVERY_REQUIRED.value,
                "recovery": "MINT_RECONCILIATION_REQUIRED",
                "action_taken": True,
                "error": f"recovery mint reconciliation required: {reason}",
            }
        actual_lower_percent = price_percent_from_tick_delta(swap_plan.new_tick_lower - slot_for_mint.tick)
        actual_upper_percent = price_percent_from_tick_delta(swap_plan.new_tick_upper - slot_for_mint.tick)
        minted_state = (
            PositionState.MINTED_UNSTAKED
            if position.is_staked
            else PositionState.REMINTED_UNSTAKED
        )
        self.journal.mark_status(
            pool.chain,
            position.token_id,
            minted_state,
            "mint",
            mint_tx,
            new_token_id,
            mint_tick=slot_for_mint.tick,
            mint_tick_lower=swap_plan.new_tick_lower,
            mint_tick_upper=swap_plan.new_tick_upper,
            range_lower_percent=actual_lower_percent,
            range_upper_percent=actual_upper_percent,
            range_percent_source=swap_plan.metadata.get("range_percent_source"),
        )
        final_state = minted_state
        if position.is_staked:
            stake_tx = None
            try:
                stake_tx = adapter.stake(new_token_id)
                stake_confirmed = self._confirm_stake_with_retry(
                    pool,
                    adapter,
                    position.token_id,
                    new_token_id,
                )
            except Exception as exc:
                reason = f"recovery stake failed: {exc}"
                failed_state = (
                    PositionState.MINTED_UNSTAKED
                    if self._is_definite_prebroadcast_stake_error(exc)
                    else PositionState.RECOVERY_REQUIRED
                )
                self.journal.mark_status(
                    pool.chain,
                    position.token_id,
                    failed_state,
                    error_reason=reason,
                )
                if failed_state == PositionState.MINTED_UNSTAKED:
                    self._notify_partial_action(
                        pool,
                        position.token_id,
                        "stake",
                        failed_state,
                        reason,
                        "recovery will retry staking the minted NFT",
                        new_token_id=new_token_id,
                    )
                else:
                    self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                    self._notify_recovery_required(pool, position.token_id, reason)
                self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)
                return {
                    "pool": pool.name,
                    "token_id": position.token_id,
                    "new_token_id": new_token_id,
                    "state": failed_state.value,
                    "recovery": "MINTED_STAKE_FAILED",
                    "action_taken": True,
                }
            if not stake_confirmed:
                reason = "stake transaction succeeded but staking contract membership was not confirmed"
                self.journal.mark_status(
                    pool.chain,
                    position.token_id,
                    PositionState.RECOVERY_REQUIRED,
                    "stake",
                    stake_tx,
                    new_token_id,
                    error_reason=reason,
                )
                self.journal.mark_recovery_error(pool.chain, position.token_id, reason)
                self._notify_recovery_required(pool, position.token_id, reason)
                self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)
                return {
                    "pool": pool.name,
                    "token_id": position.token_id,
                    "new_token_id": new_token_id,
                    "state": PositionState.RECOVERY_REQUIRED.value,
                    "recovery": "STAKE_CONFIRMATION_REQUIRED",
                    "action_taken": True,
                    "error": reason,
                }
            self.journal.mark_status(
                pool.chain,
                position.token_id,
                PositionState.REMINTED,
                "stake",
                stake_tx,
                new_token_id,
            )
            final_state = PositionState.REMINTED
        try:
            burn_tx = adapter.burn_if_empty_and_owned(position.token_id)
            if burn_tx:
                self.journal.mark_status(pool.chain, position.token_id, PositionState.BURNED, "burn", burn_tx, new_token_id)
                final_state = PositionState.BURNED
        except Exception as exc:
            log.warning("burn failed after recovery remint for %s tokenId=%s: %s", pool.name, position.token_id, exc)
        self.journal.clear_recovery_error(pool.chain, position.token_id)
        self._notify_discord_pnl_after_delay(pool, position.owner, position.token_id, new_token_id)
        return {
            "pool": pool.name,
            "token_id": position.token_id,
            "new_token_id": new_token_id,
            "state": final_state.value,
            "recovery": "REMINTED",
            "action_taken": True,
        }

    def _confirm_stake_with_retry(
        self,
        pool: PoolConfig,
        adapter: DexAdapter,
        old_token_id: int,
        new_token_id: int,
        attempts: int = 4,
        sleep_seconds: float = 6.0,
    ) -> bool:
        if not hasattr(adapter, "read_staked_positions"):
            log.warning(
                "stake confirmation skipped pool=%s chain=%s old_tokenId=%s new_tokenId=%s reason=adapter lacks read_staked_positions",
                pool.name,
                pool.chain,
                old_token_id,
                new_token_id,
            )
            return False
        last_error = None
        for attempt in range(1, max(1, attempts) + 1):
            try:
                staked = adapter.read_staked_positions([new_token_id])
                if int(new_token_id) in staked:
                    log.info(
                        "stake confirmed pool=%s chain=%s old_tokenId=%s new_tokenId=%s attempt=%s",
                        pool.name,
                        pool.chain,
                        old_token_id,
                        new_token_id,
                        attempt,
                    )
                    return True
                last_error = "new token not found in staking contract"
            except Exception as exc:
                last_error = str(exc)
            if attempt < attempts:
                log.warning(
                    "stake confirmation retry pool=%s chain=%s old_tokenId=%s new_tokenId=%s attempt=%s/%s reason=%s",
                    pool.name,
                    pool.chain,
                    old_token_id,
                    new_token_id,
                    attempt,
                    attempts,
                    last_error,
                )
                time.sleep(sleep_seconds)
        log.warning(
            "stake confirmation not verified pool=%s chain=%s old_tokenId=%s new_tokenId=%s reason=%s",
            pool.name,
            pool.chain,
            old_token_id,
            new_token_id,
            last_error,
        )
        return False

    @staticmethod
    def _is_definite_prebroadcast_stake_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "gas required exceeds",
                "intrinsic gas too low",
                "always failing transaction",
                "execution reverted",
            )
        )

    def _mark_manual_recovery(self, pool: PoolConfig, old_token_id: int, reason: str) -> dict:
        log_block(
            log,
            logging.WARNING,
            "recovery required",
            pool_context(pool, old_token_id=old_token_id),
            {
                "stage": "manual_recovery",
                "status": PositionState.RECOVERY_REQUIRED.value,
                "wallet": pool.bot_wallet,
                "reason": reason,
                "next_action": "manual review required before closing or resuming job",
            },
        )
        self.journal.mark_status(
            pool.chain,
            old_token_id,
            PositionState.RECOVERY_REQUIRED,
            error_reason=reason,
        )
        self.journal.mark_recovery_error(pool.chain, old_token_id, reason)
        self._notify_recovery_required(pool, old_token_id, reason)
        return {
            "pool": pool.name,
            "token_id": old_token_id,
            "state": PositionState.RECOVERY_REQUIRED.value,
            "recovery": "MANUAL_REQUIRED",
            "error": reason,
        }

    def _notify_partial_action(
        self,
        pool: PoolConfig,
        old_token_id: int,
        action: str,
        status: PositionState | str,
        reason: str,
        next_action: str,
        tx_hash: str | None = None,
        signed_tx_hash: str | None = None,
        new_token_id: int | None = None,
    ) -> None:
        if self.config.dry_run or not self.config.discord_enabled:
            return
        status_text = status.value if isinstance(status, PositionState) else str(status)
        tx_key = tx_hash or signed_tx_hash or reason
        notify_key = f"{status_text}:{action}:{tx_key}"[:180]
        try:
            if hasattr(self.journal, "partial_already_notified") and self.journal.partial_already_notified(
                pool.chain,
                old_token_id,
                notify_key,
            ):
                return
            self.notifier.send(
                self.notifier.partial_action_message(
                    pool.name,
                    pool.chain,
                    pool.bot_wallet,
                    old_token_id,
                    action,
                    status_text,
                    reason,
                    next_action,
                    tx_hash=tx_hash,
                    signed_tx_hash=signed_tx_hash,
                    new_token_id=new_token_id,
                )
            )
            if hasattr(self.journal, "mark_discord_partial_notified"):
                self.journal.mark_discord_partial_notified(pool.chain, old_token_id, notify_key)
        except Exception as exc:
            if hasattr(self.journal, "mark_discord_partial_error"):
                try:
                    self.journal.mark_discord_partial_error(pool.chain, old_token_id, str(exc))
                except Exception:
                    pass
            log.warning(
                "discord partial notify failed for %s tokenId=%s action=%s status=%s: %s",
                pool.name,
                old_token_id,
                action,
                status_text,
                exc,
            )

    def _notify_recovery_required(self, pool: PoolConfig, old_token_id: int, reason: str, already_notified=False) -> None:
        if self.config.dry_run or not self.config.discord_enabled or already_notified:
            return
        try:
            if self.journal.recovery_already_notified(pool.chain, old_token_id):
                return
            self.notifier.send(
                self.notifier.recovery_required_message(
                    pool.name,
                    pool.chain,
                    pool.bot_wallet,
                    old_token_id,
                    reason,
                )
            )
            self.journal.mark_recovery_notified(pool.chain, old_token_id)
        except Exception as exc:
            log.warning("discord recovery notify failed for %s tokenId=%s: %s", pool.name, old_token_id, exc)

    def _notify_inactive_farm_if_needed(
        self,
        pool: PoolConfig,
        adapter: DexAdapter,
        positions: dict[int, PositionSnapshot],
    ) -> None:
        if self.config.dry_run or not self.notifier.enabled():
            return
        if pool.pid is None or not isinstance(adapter, PancakeV3MasterChefAdapter):
            return
        alloc_point = adapter.read_farm_alloc_point()
        if alloc_point is None:
            return
        key = self._inactive_farm_cache_key(pool)
        if alloc_point > 0:
            self._clear_inactive_farm_notified(key)
            return
        if not positions or self._inactive_farm_already_notified(key):
            return
        token_ids = sorted(int(token_id) for token_id in positions)
        try:
            self.notifier.send(
                self.notifier.inactive_farm_message(
                    pool.name,
                    pool.chain,
                    pool.bot_wallet,
                    int(pool.pid),
                    int(alloc_point),
                    token_ids,
                )
            )
            self._mark_inactive_farm_notified(key, pool, alloc_point, token_ids)
        except Exception as exc:
            log.warning("discord inactive farm notify failed for %s pid=%s: %s", pool.name, pool.pid, exc)

    def _inactive_farm_cache_path(self) -> Path:
        return Path(self.config.cache_dir) / "inactive_farm_notifications.json"

    def _inactive_farm_cache_key(self, pool: PoolConfig) -> str:
        return ":".join(
            [
                str(pool.chain).upper(),
                str(pool.pool_address).lower(),
                str(pool.pid),
                str(pool.bot_wallet).lower(),
            ]
        )

    def _load_inactive_farm_cache(self) -> dict:
        path = self._inactive_farm_cache_path()
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("could not read inactive farm notify cache %s: %s", path, exc)
            return {}
        return data if isinstance(data, dict) else {}

    def _save_inactive_farm_cache(self, data: dict) -> None:
        path = self._inactive_farm_cache_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            log.warning("could not write inactive farm notify cache %s: %s", path, exc)

    def _inactive_farm_already_notified(self, key: str) -> bool:
        return key in self._load_inactive_farm_cache()

    def _mark_inactive_farm_notified(
        self,
        key: str,
        pool: PoolConfig,
        alloc_point: int,
        token_ids: list[int],
    ) -> None:
        data = self._load_inactive_farm_cache()
        data[key] = {
            "chain": pool.chain,
            "pool": pool.name,
            "pool_address": pool.pool_address,
            "pid": pool.pid,
            "wallet": pool.bot_wallet,
            "alloc_point": int(alloc_point),
            "token_ids": token_ids,
            "notified_at": int(time.time()),
        }
        self._save_inactive_farm_cache(data)

    def _clear_inactive_farm_notified(self, key: str) -> None:
        data = self._load_inactive_farm_cache()
        if key not in data:
            return
        del data[key]
        self._save_inactive_farm_cache(data)

    def _position_from_job(self, pool: PoolConfig, job: dict) -> PositionSnapshot | None:
        if job.get("old_tick_lower") is None or job.get("old_tick_upper") is None:
            return None
        if not pool.token0_address or not pool.token1_address or (pool.fee is None and pool.tick_spacing is None):
            return None
        return PositionSnapshot(
            token_id=int(job["old_token_id"]),
            owner=Web3.to_checksum_address(job.get("wallet_address") or pool.bot_wallet),
            pool_address=pool.pool_address,
            token0=Web3.to_checksum_address(pool.token0_address),
            token1=Web3.to_checksum_address(pool.token1_address),
            fee=int(pool.tick_spacing or pool.fee),
            tick_lower=int(job["old_tick_lower"]),
            tick_upper=int(job["old_tick_upper"]),
            liquidity=0,
            pid=pool.pid,
            is_staked=self._job_restore_staked(job, pool),
        )

    @staticmethod
    def _job_restore_staked(job: dict, pool: PoolConfig, adapter=None) -> bool:
        mode = str(job.get("restore_stake_mode") or "").upper()
        if mode == StakeMode.STAKED.value:
            return True
        if mode == StakeMode.UNSTAKED.value:
            return False
        if adapter is not None:
            return ConfiguredPoolRebalancer._adapter_should_stake(pool, adapter)
        if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}:
            return pool.staking_address is not None
        return pool.pid is not None

    def _snapshot_pair_from_job(
        self,
        job: dict,
        prefix: str,
        pool: PoolConfig,
    ) -> tuple[TokenBalance, TokenBalance] | None:
        raw0 = self._int_metadata(job.get(f"{prefix}_balance0_raw"))
        raw1 = self._int_metadata(job.get(f"{prefix}_balance1_raw"))
        if raw0 is None or raw1 is None:
            return None
        return (
            TokenBalance(raw=raw0, decimals=int(pool.token0_decimals or 18)),
            TokenBalance(raw=raw1, decimals=int(pool.token1_decimals or 18)),
        )

    def _reservation_pair_from_job(self, job: dict, pool: PoolConfig) -> tuple[int, int] | None:
        raw0 = self._int_metadata(job.get("reserved_token0_raw"))
        raw1 = self._int_metadata(job.get("reserved_token1_raw"))
        token0 = str(job.get("reserved_token0_address") or "")
        token1 = str(job.get("reserved_token1_address") or "")
        if raw0 is None or raw1 is None or not token0 or not token1:
            return None
        if token0.lower() != str(pool.token0_address).lower():
            return None
        if token1.lower() != str(pool.token1_address).lower():
            return None
        return max(0, raw0), max(0, raw1)

    def _reservation_coverage_error(
        self,
        pool: PoolConfig,
        adapter: DexAdapter,
        stage: str,
    ) -> str | None:
        reservations = self.journal.fetch_wallet_token_reservations(pool.chain, pool.bot_wallet)
        if not reservations:
            return None
        balance0, balance1 = adapter.read_balances(pool.bot_wallet)
        balances = {
            str(pool.token0_address).lower(): int(balance0.raw),
            str(pool.token1_address).lower(): int(balance1.raw),
        }
        for row in reservations:
            token_address = str(row.get("token_address") or "").lower()
            if token_address not in balances:
                continue
            required_raw = self._int_metadata(row.get("reserved_raw")) or 0
            actual_raw = balances[token_address]
            if actual_raw < required_raw:
                return (
                    f"reservation coverage failed at {stage}: token={token_address} "
                    f"required_raw={required_raw} actual_raw={actual_raw}"
                )
        return None

    def _matches_pool(self, pool: PoolConfig, position: PositionSnapshot) -> bool:
        expected_fee_or_spacing = int(pool.tick_spacing or pool.fee or 0)
        return (
            position.token0.lower() == str(pool.token0_address).lower()
            and position.token1.lower() == str(pool.token1_address).lower()
            and int(position.fee) == expected_fee_or_spacing
        )

    @staticmethod
    def _real_tx_hash(value: str | None) -> bool:
        return ConfiguredPoolRebalancer._normalize_tx_hash_for_rpc(value) is not None

    def _receipt_token_inflows(
        self,
        w3,
        pool: PoolConfig,
        tx_hash: str | None,
        wallet: str,
    ) -> tuple[int, int] | None:
        normalized = self._normalize_tx_hash_for_rpc(tx_hash)
        if not normalized:
            return None
        try:
            receipt = w3.eth.get_transaction_receipt(normalized)
        except Exception as exc:
            log.warning("could not fetch receipt for reservation tx=%s pool=%s: %s", tx_hash, pool.name, exc)
            return None
        return self._token_inflows_from_receipt(receipt, pool, wallet)

    def _token_inflows_from_receipt(self, receipt, pool: PoolConfig, wallet: str) -> tuple[int, int]:
        token0 = str(pool.token0_address).lower()
        token1 = str(pool.token1_address).lower()
        wallet_hex = str(wallet).lower().replace("0x", "")
        amount0 = 0
        amount1 = 0
        for event in receipt.get("logs", []):
            address = str(event.get("address") or "").lower()
            if address not in {token0, token1}:
                continue
            topics = event.get("topics") or []
            if len(topics) < 3:
                continue
            if self._hex_value(topics[0]).lower() != TRANSFER_TOPIC:
                continue
            to_topic = self._hex_value(topics[2]).lower().replace("0x", "")
            if not to_topic.endswith(wallet_hex):
                continue
            raw_value = self._hex_value(event.get("data"))
            try:
                value = int(raw_value, 16) if raw_value not in {"", "0x"} else 0
            except ValueError:
                continue
            if address == token0:
                amount0 += max(0, value)
            elif address == token1:
                amount1 += max(0, value)
        return amount0, amount1

    def _token_movements_from_receipt(self, receipt, pool: PoolConfig, wallet: str) -> tuple[int, int, int, int]:
        token0 = str(pool.token0_address).lower()
        token1 = str(pool.token1_address).lower()
        wallet_hex = str(wallet).lower().replace("0x", "")
        sent0 = sent1 = received0 = received1 = 0
        for event in receipt.get("logs", []):
            address = str(event.get("address") or "").lower()
            if address not in {token0, token1}:
                continue
            topics = event.get("topics") or []
            if len(topics) < 3:
                continue
            if self._hex_value(topics[0]).lower() != TRANSFER_TOPIC:
                continue
            from_topic = self._hex_value(topics[1]).lower().replace("0x", "")
            to_topic = self._hex_value(topics[2]).lower().replace("0x", "")
            raw_value = self._hex_value(event.get("data"))
            try:
                value = int(raw_value, 16) if raw_value not in {"", "0x"} else 0
            except ValueError:
                continue
            if value <= 0:
                continue
            if address == token0:
                if from_topic.endswith(wallet_hex):
                    sent0 += value
                if to_topic.endswith(wallet_hex):
                    received0 += value
            elif address == token1:
                if from_topic.endswith(wallet_hex):
                    sent1 += value
                if to_topic.endswith(wallet_hex):
                    received1 += value
        return sent0, sent1, received0, received1

    def _swap_receipt_confirms_output(
        self,
        pool: PoolConfig,
        swap_tx: TxResult,
        receipt_inflows: tuple[int, int] | None,
    ) -> bool:
        if not receipt_inflows:
            return False
        token_out = str((swap_tx.metadata or {}).get("token_out") or "").lower()
        token0 = str(pool.token0_address).lower()
        token1 = str(pool.token1_address).lower()
        if token_out == token0:
            return int(receipt_inflows[0]) > 0
        if token_out == token1:
            return int(receipt_inflows[1]) > 0
        return False

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

    def _reservation_after_swap(
        self,
        pool: PoolConfig,
        reserved0: int,
        reserved1: int,
        before0: TokenBalance,
        before1: TokenBalance,
        after0: TokenBalance,
        after1: TokenBalance,
        swap_tx: TxResult,
        receipt_inflows: tuple[int, int] | None = None,
    ) -> tuple[int, int]:
        metadata = swap_tx.metadata or {}
        token_in = str(metadata.get("token_in") or "").lower()
        token_out = str(metadata.get("token_out") or "").lower()
        amount_in = self._int_metadata(metadata.get("amount_in")) or 0
        token0 = str(pool.token0_address).lower()
        token1 = str(pool.token1_address).lower()
        out0 = max(0, int(reserved0))
        out1 = max(0, int(reserved1))

        if token_in == token0:
            spent = amount_in or max(0, int(before0.raw) - int(after0.raw))
            out0 = max(0, out0 - min(out0, int(spent)))
        elif token_in == token1:
            spent = amount_in or max(0, int(before1.raw) - int(after1.raw))
            out1 = max(0, out1 - min(out1, int(spent)))

        receipt0 = receipt1 = 0
        if receipt_inflows is not None:
            receipt0, receipt1 = receipt_inflows

        if token_out == token0:
            received = int(receipt0) if receipt0 > 0 else max(0, int(after0.raw) - int(before0.raw))
            out0 += received
        elif token_out == token1:
            received = int(receipt1) if receipt1 > 0 else max(0, int(after1.raw) - int(before1.raw))
            out1 += received

        log.info(
            "reservation after swap pool=%s chain=%s token_in=%s token_out=%s amount_in=%s "
            "before_reserved0=%s before_reserved1=%s before_balance0=%s before_balance1=%s "
            "after_balance0=%s after_balance1=%s receipt_inflow0=%s receipt_inflow1=%s "
            "new_reserved0=%s new_reserved1=%s tx=%s",
            pool.name,
            pool.chain,
            token_in,
            token_out,
            amount_in,
            reserved0,
            reserved1,
            before0.raw,
            before1.raw,
            after0.raw,
            after1.raw,
            receipt0,
            receipt1,
            out0,
            out1,
            swap_tx.tx_hash,
        )
        return out0, out1

    def _swap_dust_reason(self, pool: PoolConfig, plan, recovered0: int, recovered1: int) -> str | None:
        token_in = Web3.to_checksum_address(plan.swap_token_in)
        token0 = Web3.to_checksum_address(pool.token0_address)
        token1 = Web3.to_checksum_address(pool.token1_address)
        amount_in = int(plan.swap_amount_in or 0)
        if amount_in <= 0:
            return None

        price0 = token_price_usd(pool.chain, token0, warnings=None)
        price1 = token_price_usd(pool.chain, token1, warnings=None)
        amount_in_usd = None
        recovered_usd = None
        if token_in.lower() == token0.lower() and price0 is not None:
            amount_in_usd = (amount_in / (10 ** int(pool.token0_decimals or 18))) * price0
        elif token_in.lower() == token1.lower() and price1 is not None:
            amount_in_usd = (amount_in / (10 ** int(pool.token1_decimals or 18))) * price1

        if price0 is not None and price1 is not None:
            recovered_usd = (
                (recovered0 / (10 ** int(pool.token0_decimals or 18))) * price0
                + (recovered1 / (10 ** int(pool.token1_decimals or 18))) * price1
            )

        if amount_in_usd is not None and amount_in_usd < pool.min_swap_input_usd:
            return f"swap input ${amount_in_usd:.6f} below ${pool.min_swap_input_usd:.6f}"
        if (
            amount_in_usd is not None
            and recovered_usd is not None
            and recovered_usd > 0
            and amount_in_usd / recovered_usd < pool.min_swap_recovered_pct
        ):
            return (
                f"swap input is {(amount_in_usd / recovered_usd) * 100:.4f}% of recovered capital, "
                f"below {pool.min_swap_recovered_pct * 100:.4f}%"
            )
        return None

    def _read_post_swap_balances_with_retry(
        self,
        w3,
        adapter: DexAdapter,
        pool: PoolConfig,
        position: PositionSnapshot,
        post_withdraw0,
        post_withdraw1,
        swap_tx: TxResult,
    ):
        metadata = swap_tx.metadata or {}
        receipt_block = self._int_metadata(metadata.get("receipt_block"))
        token_in = str(metadata.get("token_in") or "").lower()
        token_out = str(metadata.get("token_out") or "").lower()
        amount_in = self._int_metadata(metadata.get("amount_in")) or 0
        quote_buy_amount = self._int_metadata(metadata.get("quote_buy_amount")) or 0
        min_input_spent = int(amount_in * 0.95) if amount_in > 0 else 0
        min_output_received = int(quote_buy_amount * 0.95) if quote_buy_amount > 0 else 0
        token0 = Web3.to_checksum_address(pool.token0_address).lower()
        token1 = Web3.to_checksum_address(pool.token1_address).lower()
        last_post0 = last_post1 = None
        last_reason = "no balance read"

        for attempt in range(1, 7):
            latest_block = None
            if receipt_block is not None:
                try:
                    latest_block = int(w3.eth.block_number)
                except Exception as exc:
                    last_reason = f"could not read latest block: {exc}"
                    log.warning(
                        "post-swap latest block read failed pool=%s tokenId=%s tx=%s attempt=%s error=%s",
                        pool.name,
                        position.token_id,
                        swap_tx.tx_hash,
                        attempt,
                        exc,
                    )
                if latest_block is not None and latest_block < receipt_block:
                    last_reason = f"latest block {latest_block} behind swap receipt block {receipt_block}"
                    log.info(
                        "waiting for RPC to reach swap receipt block pool=%s tokenId=%s tx=%s "
                        "attempt=%s latest_block=%s receipt_block=%s",
                        pool.name,
                        position.token_id,
                        swap_tx.tx_hash,
                        attempt,
                        latest_block,
                        receipt_block,
                    )
                    if attempt < 6:
                        time.sleep(3)
                    continue

            last_post0, last_post1 = adapter.read_balances(pool.bot_wallet)
            delta0 = last_post0.raw - post_withdraw0.raw
            delta1 = last_post1.raw - post_withdraw1.raw
            ok = True
            reasons = []

            if token_in == token0 and min_input_spent > 0:
                spent = -delta0
                if spent < min_input_spent:
                    ok = False
                    reasons.append(f"token0 spent {spent} < {min_input_spent}")
            elif token_in == token1 and min_input_spent > 0:
                spent = -delta1
                if spent < min_input_spent:
                    ok = False
                    reasons.append(f"token1 spent {spent} < {min_input_spent}")

            if token_out == token0 and min_output_received > 0:
                if delta0 < min_output_received:
                    ok = False
                    reasons.append(f"token0 received {delta0} < {min_output_received}")
            elif token_out == token1 and min_output_received > 0:
                if delta1 < min_output_received:
                    ok = False
                    reasons.append(f"token1 received {delta1} < {min_output_received}")

            if ok:
                return last_post0, last_post1

            last_reason = "; ".join(reasons) or "swap balance delta not confirmed"
            log.info(
                "post-swap balance not confirmed pool=%s tokenId=%s tx=%s attempt=%s "
                "receipt_block=%s latest_block=%s delta0=%s delta1=%s reason=%s",
                pool.name,
                position.token_id,
                swap_tx.tx_hash,
                attempt,
                receipt_block,
                latest_block,
                delta0,
                delta1,
                last_reason,
            )
            if attempt < 6:
                time.sleep(10)

        log.warning(
            "post-swap balance confirmation failed pool=%s tokenId=%s tx=%s "
            "post_swap0=%s post_swap1=%s reason=%s",
            pool.name,
            position.token_id,
            swap_tx.tx_hash,
            last_post0.raw if last_post0 else None,
            last_post1.raw if last_post1 else None,
            last_reason,
        )
        return None

    def _clamp_plan_to_available_balances(self, plan, pre0, pre1, pre_mint0, pre_mint1):
        original_amount0 = int(plan.amount0_desired or 0)
        original_amount1 = int(plan.amount1_desired or 0)
        available0 = max(0, int(pre_mint0.raw) - int(pre0.raw))
        available1 = max(0, int(pre_mint1.raw) - int(pre1.raw))
        plan.amount0_desired = min(original_amount0, available0)
        plan.amount1_desired = min(original_amount1, available1)
        plan.metadata.update(
            {
                "pre_mint_balance0": str(pre_mint0.raw),
                "pre_mint_balance1": str(pre_mint1.raw),
                "available0_for_mint": str(available0),
                "available1_for_mint": str(available1),
                "original_amount0_desired": str(original_amount0),
                "original_amount1_desired": str(original_amount1),
                "clamped_amount0_desired": str(plan.amount0_desired),
                "clamped_amount1_desired": str(plan.amount1_desired),
            }
        )
        return original_amount0, original_amount1, available0, available1

    def _clamp_plan_to_reservation(self, plan, reserved0: int, reserved1: int, pre_mint0, pre_mint1):
        original_amount0 = int(plan.amount0_desired or 0)
        original_amount1 = int(plan.amount1_desired or 0)
        available0 = min(max(0, int(reserved0)), max(0, int(pre_mint0.raw)))
        available1 = min(max(0, int(reserved1)), max(0, int(pre_mint1.raw)))
        plan.amount0_desired = min(original_amount0, available0)
        plan.amount1_desired = min(original_amount1, available1)
        plan.metadata.update(
            {
                "pre_mint_balance0": str(pre_mint0.raw),
                "pre_mint_balance1": str(pre_mint1.raw),
                "reserved0_for_mint": str(max(0, int(reserved0))),
                "reserved1_for_mint": str(max(0, int(reserved1))),
                "available0_for_mint": str(available0),
                "available1_for_mint": str(available1),
                "original_amount0_desired": str(original_amount0),
                "original_amount1_desired": str(original_amount1),
                "clamped_amount0_desired": str(plan.amount0_desired),
                "clamped_amount1_desired": str(plan.amount1_desired),
            }
        )
        return original_amount0, original_amount1, available0, available1

    @staticmethod
    def _int_metadata(value) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _module_reward_update(self, pool: PoolConfig, adapter: DexAdapter, reward_token, pre_reward):
        if not reward_token or pre_reward is None:
            return None
        post_reward = adapter.read_token_balance(reward_token, pool.bot_wallet)
        claimed_raw = max(0, post_reward.raw - pre_reward.raw)
        claimed_amount = claimed_raw / (10**post_reward.decimals)
        price = token_price_usd(pool.chain, reward_token, warnings=None) if claimed_raw > 0 else None
        if price is not None:
            claimed_usd = claimed_amount * price
        else:
            claimed_usd = 0.0 if claimed_raw == 0 else None
        return {
            "token": reward_token,
            "raw": str(claimed_raw),
            "amount": claimed_amount,
            "price_usd": price,
            "usd": claimed_usd,
            "source": "MODULE_WITHDRAW",
        }

    def _read_recovered_balances_with_retry(
        self,
        adapter: DexAdapter,
        pool: PoolConfig,
        pre0,
        pre1,
    ):
        post0 = post1 = None
        for attempt in range(1, 7):
            post0, post1 = adapter.read_balances(pool.bot_wallet)
            recovered0 = max(0, post0.raw - pre0.raw)
            recovered1 = max(0, post1.raw - pre1.raw)
            if recovered0 > 0 or recovered1 > 0:
                return post0, post1
            if attempt < 6:
                time.sleep(3)
        return post0, post1

    def _notify_discord_pnl_after_delay(self, pool: PoolConfig, wallet: str, old_token_id: int, new_token_id: int) -> None:
        if self.config.dry_run or not self.config.discord_enabled:
            return
        delay = max(0, int(self.config.discord_pnl_delay_seconds))
        if delay:
            time.sleep(delay)
        self._notify_discord_pnl_for_job(pool, wallet, old_token_id, new_token_id, allow_pending=True)

    def _retry_discord_pnl_notifications(self, pool: PoolConfig) -> None:
        if self.config.dry_run or not self.config.discord_enabled:
            return
        wallets = pool.managed_wallets or (pool.bot_wallet,)
        for wallet in wallets:
            for job in self.journal.fetch_discord_pnl_pending_jobs(pool.chain, pool.pool_address, wallet):
                old_token_id = int(job["old_token_id"])
                new_token_id = int(job["new_token_id"])
                allow_pending = not bool(job.get("discord_pending_notified_at"))
                self._notify_discord_pnl_for_job(pool, wallet, old_token_id, new_token_id, allow_pending=allow_pending)

    def _notify_discord_pnl_for_job(
        self,
        pool: PoolConfig,
        wallet: str,
        old_token_id: int,
        new_token_id: int,
        allow_pending: bool,
    ) -> None:
        try:
            record = self._find_pnl_record(pool, wallet, new_token_id)
            if record and self._pnl_record_ready(record):
                self.notifier.send(self.notifier.pnl_message(record))
                self.journal.mark_discord_pnl_notified(pool.chain, old_token_id)
                return
            if allow_pending and self.config.discord_notify_pending_if_snapshot_missing:
                self.notifier.send(
                    self.notifier.pending_message(pool.name, pool.chain, wallet, old_token_id, new_token_id)
                )
                self.journal.mark_discord_pending_notified(pool.chain, old_token_id)
        except Exception as exc:
            log.warning("discord pnl notify failed for %s tokenId=%s: %s", pool.name, old_token_id, exc)
            self.journal.mark_discord_error(pool.chain, old_token_id, str(exc))

    def _find_pnl_record(self, pool: PoolConfig, wallet: str, new_token_id: int) -> dict | None:
        records = ConfiguredPoolPnlReporter(self.config).build_records()
        wallet_lower = wallet.lower()
        for record in records:
            if str(record.get("pool")) != pool.name:
                continue
            if str(record.get("chain")).upper() != pool.chain.upper():
                continue
            if str(record.get("wallet_address", "")).lower() != wallet_lower:
                continue
            if int(record.get("current_token_id") or 0) == int(new_token_id):
                return record
        return None

    def _pnl_record_ready(self, record: dict) -> bool:
        warnings = record.get("warnings") or []
        if record.get("basis_source") == "MISSING":
            return False
        if record.get("current_position_value_usd") is None:
            return False
        return not any("missing latest wallet_nft_position snapshot for current token" in str(item) for item in warnings)

    def _try_record_plan(self, pool, position, plan) -> None:
        if self.config.dry_run:
            return
        self.journal.create_or_update_plan(pool.chain, pool.pool_address, position.owner, plan)

    def _resolve_range_percent(
        self,
        pool: PoolConfig,
        position: PositionSnapshot,
    ) -> tuple[float | None, float | None, str]:
        if pool.rebalance_range_mode == "price_percent":
            return (
                pool.rebalance_range_lower_percent,
                pool.rebalance_range_upper_percent,
                "CONFIG",
            )

        basis = self.journal.get_mint_tick_basis(pool.chain, pool.pool_address, position.owner, position.token_id)
        resolved = self._percent_from_tick_basis(basis)
        if resolved:
            return resolved[0], resolved[1], "JOURNAL_MINT_TICK"

        basis = self.journal.get_wallet_position_first_tick_basis(
            pool.chain,
            pool.pool_address,
            position.owner,
            position.token_id,
        )
        resolved = self._percent_from_tick_basis(basis)
        if resolved:
            return resolved[0], resolved[1], "WALLET_NFT_POSITION_FIRST_SNAPSHOT"

        return None, None, "CENTER_FALLBACK"

    def _percent_from_tick_basis(self, basis: tuple[int, int, int] | None) -> tuple[float, float] | None:
        if not basis:
            return None
        mint_tick, tick_lower, tick_upper = basis
        if not (tick_lower < mint_tick < tick_upper):
            return None
        lower_percent = price_percent_from_tick_delta(tick_lower - mint_tick)
        upper_percent = price_percent_from_tick_delta(tick_upper - mint_tick)
        if lower_percent >= 0 or lower_percent <= -100 or upper_percent <= 0:
            return None
        return lower_percent, upper_percent

    @staticmethod
    def _adapter_should_stake(pool: PoolConfig, adapter) -> bool:
        if hasattr(adapter, "should_stake"):
            try:
                return bool(adapter.should_stake())
            except Exception:
                return False
        return pool.pid is not None

    @staticmethod
    def _adapter_staking_owner(adapter) -> str | None:
        if hasattr(adapter, "staking_owner_address"):
            try:
                return adapter.staking_owner_address()
            except Exception:
                return None
        return getattr(adapter, "masterchef_address", None)

    @staticmethod
    def _adapter_reward_token(adapter) -> str | None:
        if hasattr(adapter, "reward_token_address"):
            try:
                return adapter.reward_token_address()
            except Exception:
                return None
        return None

    def _build_adapter(self, w3, pool: PoolConfig, executor: TxExecutor):
        adapter_cls = ADAPTER_REGISTRY.get(pool.dex_type)
        if adapter_cls:
            return adapter_cls(w3, pool, executor)
        raise ValueError(f"unsupported dex_type={pool.dex_type}")
