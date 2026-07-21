from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from web3 import Web3

from ..adapter import AerodromeGaugeAdapter, PancakeV3MasterChefAdapter
from ..evm import get_gas_params, web3_connection
from ..journal import RebalanceJournal, mysql_advisory_lock
from ..models import CompoundCandidate, DexType, PoolConfig, PositionSnapshot, WorkerConfig
from ..pnl_report import WRAPPED_NATIVE_TOKENS
from ..reward import token_price_usd
from ..signer import RuntimeSigner
from ..tx_executor import TxExecutor
from .adapter import CompoundAdapter
from .abi import COLLECT_TOPIC, INCREASE_LIQUIDITY_TOPIC
from .eligibility import CompoundEligibilityEvaluator
from .executor import CompoundExecutor
from .journal import CompoundJournal
from .models import CompoundJobState, CompoundPosition, CompoundResult
from .planner import CompoundPlanner
from .planner import liquidity_from_amounts_exact
from .position_index import CompoundPositionIndex
from .swapper import CompoundSwapQuote, CompoundSwapper


log = logging.getLogger("configured_pool_rebalancer.auto_compound")


class ConfiguredPoolCompounder:
    MAX_RETRIES = 3
    BASE_GAS_UNITS = 400_000 + 500_000 + 650_000
    APPROVAL_GAS_UNITS = 120_000

    def __init__(self, config: WorkerConfig, migrate: bool = False, signer: RuntimeSigner | None = None):
        self.config = config
        self.signer = signer
        self.journal = CompoundJournal()
        self.rebalance_journal = RebalanceJournal()
        self.index = CompoundPositionIndex(config.cache_dir, config.legacy_position_cache_dir)
        self.eligibility = CompoundEligibilityEvaluator()
        self.planner = CompoundPlanner()
        self._price_cache: dict[tuple[str, str], float | None] = {}
        if migrate:
            self.journal.migrate()

    def reconcile_pending_wallets(self) -> set[str]:
        """Reconcile hashes before either subsystem sends a new wallet transaction."""
        blocked: set[str] = set()
        try:
            jobs = self.journal.fetch_pending_jobs()
        except Exception as exc:
            log.warning("compound pending reconciliation unavailable: %s", exc)
            return blocked
        pools = {(p.chain.upper(), p.bot_wallet.lower()): p for p in self.config.pools}
        for job in jobs:
            key = (str(job["chain"]).upper(), str(job["wallet_address"]).lower())
            pool = pools.get(key)
            if pool is None:
                blocked.add(key[1])
                continue
            w3, discovered_pool, _, executor = self._runtime(pool)
            status = executor.lookup_pending(job)
            if status["status"] == "SUCCESS":
                try:
                    self._finish_recovered_transaction(job, status["receipt"], discovered_pool)
                except Exception as exc:
                    self.journal.update(
                        int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value,
                        error_reason=f"pending receipt reconciliation failed: {exc}",
                    )
                    blocked.add(key[1])
            elif status["status"] == "REVERTED":
                self._rollback_reverted_transaction(job)
            else:
                blocked.add(key[1])
                log.warning(
                    "compound wallet recovery barrier chain=%s wallet=%s job=%s action=%s status=%s",
                    key[0], key[1], job["id"], job.get("pending_action"), status["status"],
                )
        return blocked

    def run_once(
        self,
        blocked_wallets: set[str] | None = None,
        compound_candidates: dict[str, tuple[CompoundCandidate, ...]] | None = None,
    ) -> list[dict]:
        blocked = {wallet.lower() for wallet in (blocked_wallets or set())}
        candidates_by_pool = compound_candidates or {}
        results: list[dict] = []
        self._price_cache.clear()
        for raw_pool in self.config.pools:
            if not raw_pool.auto_compound.enabled:
                continue
            if raw_pool.bot_wallet.lower() in blocked:
                results.append(CompoundResult(raw_pool.name, None, "SKIPPED", "ACTIVE_REBALANCE").as_dict())
                continue
            try:
                results.extend(self._run_pool(raw_pool, candidates_by_pool.get(raw_pool.name, ())))
            except Exception as exc:
                log.exception("compound pool failed pool=%s: %s", raw_pool.name, exc)
                results.append(CompoundResult(raw_pool.name, None, "ERROR", str(exc)).as_dict())
        return results

    def _run_pool(
        self,
        raw_pool: PoolConfig,
        candidates: tuple[CompoundCandidate, ...] | list[CompoundCandidate] = (),
    ) -> list[dict]:
        w3, pool, read_adapter, executor = self._runtime(raw_pool)
        adapter = CompoundAdapter(w3, pool, read_adapter, executor)
        results: list[dict] = []

        open_jobs = self.journal.fetch_open_jobs(pool.chain) if not self.config.dry_run else []
        pool_jobs = [
            job for job in open_jobs
            if str(job["pool_address"]).lower() == pool.pool_address.lower()
            and str(job["wallet_address"]).lower() == pool.bot_wallet.lower()
        ]
        for job in pool_jobs:
            results.append(self._resume_job(w3, pool, adapter, job))
            if results[-1].get("action_taken"):
                return results

        discovery = self.index.revalidate_candidates(w3, pool, read_adapter, candidates)
        for skipped in discovery.policy_skips:
            results.append(CompoundResult(pool.name, skipped.token_id, "SKIPPED", skipped.reason).as_dict())
        started = 0
        for position in discovery.positions.values():
            if started >= pool.auto_compound.max_jobs_per_cycle:
                break
            result = self._evaluate_and_execute(w3, pool, adapter, position)
            results.append(result)
            if result.get("action_taken"):
                started += 1
        return results

    def _evaluate_and_execute(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: CompoundAdapter,
        position: CompoundPosition,
    ) -> dict:
        token_id = position.snapshot.token_id
        slot0 = adapter.read_slot0()
        policy = self.eligibility.evaluate_policy(position, slot0, pool.auto_compound)
        if not policy.eligible:
            return CompoundResult(pool.name, token_id, "SKIPPED", policy.reason).as_dict()

        if not self.config.dry_run:
            existing = self.journal.get_open_job(pool.chain, position.npm_address, token_id)
            if existing:
                return CompoundResult(pool.name, token_id, "SKIPPED", "ACTIVE_COMPOUND").as_dict()
            completed_at = self.journal.last_completed_at(pool.chain, position.npm_address, token_id)
            if not self.eligibility.cooldown_passed(completed_at, pool.auto_compound.min_interval_seconds):
                return CompoundResult(pool.name, token_id, "SKIPPED", "COOLDOWN").as_dict()
            if self.rebalance_journal.fetch_wallet_token_reservations(pool.chain, pool.bot_wallet):
                return CompoundResult(pool.name, token_id, "SKIPPED", "ACTIVE_REBALANCE").as_dict()

        anchor_block = int(w3.eth.block_number)
        quoted0, quoted1 = adapter.quote_collect(position, anchor_block)
        if quoted0 <= 0 and quoted1 <= 0:
            return CompoundResult(pool.name, token_id, "SKIPPED", "NO_FEES").as_dict()

        price0 = self._token_price(pool.chain, position.snapshot.token0)
        price1 = self._token_price(pool.chain, position.snapshot.token1)
        if price0 is None or price1 is None:
            return CompoundResult(pool.name, token_id, "SKIPPED", "PRICE_UNAVAILABLE").as_dict()
        fee_value = self._usd_value(pool, quoted0, quoted1, price0, price1)
        metadata = {
            "fee_value_usd": fee_value,
            "min_compound_usd": pool.auto_compound.min_compound_usd,
            "reinvestable_value_usd": fee_value,
            "swap_input_usd": 0.0,
            "swap_decision": "NOT_EVALUATED",
        }
        if fee_value < pool.auto_compound.min_compound_usd:
            return CompoundResult(
                pool.name,
                token_id,
                "SKIPPED",
                "BELOW_MIN_COMPOUND",
                metadata=metadata,
            ).as_dict()

        native_price = self._native_price(pool.chain)
        if native_price is None:
            return CompoundResult(
                pool.name,
                token_id,
                "SKIPPED",
                "PRICE_UNAVAILABLE",
                metadata=metadata,
            ).as_dict()

        preliminary_plan = self.planner.build_swap_plan(position, slot0, quoted0, quoted1)
        preliminary_quote = None
        gas_amount0 = quoted0
        gas_amount1 = quoted1
        profitability_value = fee_value
        metadata["swap_decision"] = "NO_SWAP_BALANCED"
        if preliminary_plan.skip_swap or preliminary_plan.amount_in <= 0:
            no_swap_plan = self.planner.build_liquidity_plan(
                position, slot0, quoted0, quoted1, pool.slippage_bps
            )
            if no_swap_plan.expected_liquidity <= 0:
                metadata["reinvestable_value_usd"] = 0.0
                return CompoundResult(
                    pool.name,
                    token_id,
                    "SKIPPED",
                    "NO_REINVESTABLE_LIQUIDITY",
                    metadata=metadata,
                ).as_dict()
            gas_amount0 = no_swap_plan.amount0_desired
            gas_amount1 = no_swap_plan.amount1_desired
            profitability_value = self._usd_value(
                pool, gas_amount0, gas_amount1, price0, price1
            )
        else:
            swap_input_usd = self._token_amount_usd(
                pool,
                position,
                preliminary_plan.token_in,
                preliminary_plan.amount_in,
                price0,
                price1,
            )
            metadata["swap_input_usd"] = swap_input_usd
            if swap_input_usd < pool.min_swap_input_usd:
                metadata["swap_decision"] = "NO_SWAP_INPUT_DUST"
                no_swap_plan = self.planner.build_liquidity_plan(
                    position, slot0, quoted0, quoted1, pool.slippage_bps
                )
                if no_swap_plan.expected_liquidity <= 0:
                    metadata["reinvestable_value_usd"] = 0.0
                    return CompoundResult(
                        pool.name,
                        token_id,
                        "SKIPPED",
                        "NO_REINVESTABLE_LIQUIDITY",
                        metadata=metadata,
                    ).as_dict()
                gas_amount0 = no_swap_plan.amount0_desired
                gas_amount1 = no_swap_plan.amount1_desired
                profitability_value = self._usd_value(
                    pool, gas_amount0, gas_amount1, price0, price1
                )
            else:
                preliminary_quote = CompoundSwapper(pool).best_refined_quote(
                    preliminary_plan.token_in,
                    preliminary_plan.token_out,
                    preliminary_plan.amount_in,
                    pool.bot_wallet,
                    self._quote_scorer(position, slot0, quoted0, quoted1),
                )
                if preliminary_quote is None:
                    metadata["swap_decision"] = "SWAP_ROUTE_UNAVAILABLE"
                    return CompoundResult(
                        pool.name,
                        token_id,
                        "SKIPPED",
                        "SWAP_ROUTE_UNAVAILABLE",
                        metadata=metadata,
                    ).as_dict()
                dust_reason = self._swap_dust_reason(pool, preliminary_quote, position, price0, price1)
                if dust_reason:
                    metadata["swap_decision"] = f"NO_SWAP_{dust_reason}"
                    preliminary_quote = None
                    no_swap_plan = self.planner.build_liquidity_plan(
                        position, slot0, quoted0, quoted1, pool.slippage_bps
                    )
                    if no_swap_plan.expected_liquidity <= 0:
                        metadata["reinvestable_value_usd"] = 0.0
                        return CompoundResult(
                            pool.name,
                            token_id,
                            "SKIPPED",
                            "NO_REINVESTABLE_LIQUIDITY",
                            metadata=metadata,
                        ).as_dict()
                    gas_amount0 = no_swap_plan.amount0_desired
                    gas_amount1 = no_swap_plan.amount1_desired
                    profitability_value = self._usd_value(
                        pool, gas_amount0, gas_amount1, price0, price1
                    )
                else:
                    metadata["swap_decision"] = f"SWAP_{preliminary_quote.provider.upper()}"

        metadata["reinvestable_value_usd"] = profitability_value
        if profitability_value < pool.auto_compound.min_compound_usd:
            return CompoundResult(
                pool.name,
                token_id,
                "SKIPPED",
                "BELOW_MIN_COMPOUND",
                metadata=metadata,
            ).as_dict()

        gas_cost = self._estimate_gas_usd(
            w3,
            pool,
            adapter,
            position,
            preliminary_quote,
            gas_amount0,
            gas_amount1,
            native_price,
        )
        metadata["estimated_gas_usd"] = gas_cost
        gas_threshold = pool.auto_compound.gas_cost_multiplier * gas_cost
        if profitability_value < gas_threshold:
            metadata["profitability_threshold_usd"] = max(
                pool.auto_compound.min_compound_usd,
                gas_threshold,
            )
            return CompoundResult(
                pool.name,
                token_id,
                "SKIPPED",
                "BELOW_GAS_PROFITABILITY",
                metadata=metadata,
            ).as_dict()

        metadata.update({
            "quoted_amount0_raw": str(quoted0),
            "quoted_amount1_raw": str(quoted1),
            "current_tick": slot0.tick,
            "stake_mode": position.stake_mode,
        })
        if self.config.dry_run:
            return CompoundResult(pool.name, token_id, CompoundJobState.PREPARED.value, metadata=metadata).as_dict()

        wallet_lock = f"rebalance:{pool.chain}:{pool.bot_wallet.lower()}"
        pool_lock = f"rebalance:{pool.chain}:{pool.pool_address.lower()}"
        with mysql_advisory_lock(wallet_lock, self.config.lock_timeout_seconds):
            with mysql_advisory_lock(pool_lock, self.config.lock_timeout_seconds):
                if self.journal.get_open_job(pool.chain, position.npm_address, token_id):
                    return CompoundResult(pool.name, token_id, "SKIPPED", "ACTIVE_COMPOUND").as_dict()
                if self.rebalance_journal.fetch_wallet_token_reservations(pool.chain, pool.bot_wallet):
                    return CompoundResult(pool.name, token_id, "SKIPPED", "ACTIVE_REBALANCE").as_dict()
                refreshed = adapter.read_position(position)
                refreshed_position = replace(
                    position,
                    snapshot=refreshed,
                    stake_mode="STAKED" if refreshed.is_staked else "UNSTAKED",
                )
                refreshed_slot = adapter.read_slot0()
                refreshed_policy = self.eligibility.evaluate_policy(refreshed_position, refreshed_slot, pool.auto_compound)
                if not refreshed_policy.eligible:
                    return CompoundResult(pool.name, token_id, "SKIPPED", refreshed_policy.reason).as_dict()
                quoted0, quoted1 = adapter.quote_collect(refreshed_position)
                if quoted0 <= 0 and quoted1 <= 0:
                    return CompoundResult(pool.name, token_id, "SKIPPED", "NO_FEES").as_dict()
                refreshed_fee_value = self._usd_value(pool, quoted0, quoted1, price0, price1)
                refreshed_metadata = {
                    "fee_value_usd": refreshed_fee_value,
                    "min_compound_usd": pool.auto_compound.min_compound_usd,
                    "estimated_gas_usd": gas_cost,
                    "swap_input_usd": metadata["swap_input_usd"],
                    "swap_decision": metadata["swap_decision"],
                }
                refreshed_profitability_value = refreshed_fee_value
                if preliminary_quote is None:
                    refreshed_plan = self.planner.build_liquidity_plan(
                        refreshed_position,
                        refreshed_slot,
                        quoted0,
                        quoted1,
                        pool.slippage_bps,
                    )
                    if refreshed_plan.expected_liquidity <= 0:
                        refreshed_metadata["reinvestable_value_usd"] = 0.0
                        return CompoundResult(
                            pool.name,
                            token_id,
                            "SKIPPED",
                            "NO_REINVESTABLE_LIQUIDITY",
                            metadata=refreshed_metadata,
                        ).as_dict()
                    refreshed_profitability_value = self._usd_value(
                        pool,
                        refreshed_plan.amount0_desired,
                        refreshed_plan.amount1_desired,
                        price0,
                        price1,
                    )
                refreshed_metadata["reinvestable_value_usd"] = refreshed_profitability_value
                if refreshed_profitability_value < pool.auto_compound.min_compound_usd:
                    return CompoundResult(
                        pool.name,
                        token_id,
                        "SKIPPED",
                        "BELOW_MIN_COMPOUND",
                        metadata=refreshed_metadata,
                    ).as_dict()
                if refreshed_profitability_value < pool.auto_compound.gas_cost_multiplier * gas_cost:
                    return CompoundResult(
                        pool.name,
                        token_id,
                        "SKIPPED",
                        "BELOW_GAS_PROFITABILITY",
                        metadata=refreshed_metadata,
                    ).as_dict()
                return self._start_job(
                    w3, pool, adapter, refreshed_position, refreshed_slot, anchor_block,
                    quoted0, quoted1, refreshed_fee_value, gas_cost,
                )

    def _start_job(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: CompoundAdapter,
        position: CompoundPosition,
        slot0,
        anchor_block: int,
        quoted0: int,
        quoted1: int,
        fee_value: float,
        gas_cost: float,
    ) -> dict:
        token_id = position.snapshot.token_id
        job_id = self.journal.create_job(
            {
                "idempotency_key": f"{pool.chain}:{position.npm_address.lower()}:{token_id}:{anchor_block}:{uuid4().hex[:12]}",
                "chain": pool.chain,
                "pool_address": pool.pool_address,
                "wallet_address": pool.bot_wallet,
                "npm_address": position.npm_address,
                "token_id": token_id,
                "dex_type": position.dex_type,
                "stake_mode": position.stake_mode,
                "anchor_block": anchor_block,
                "current_tick": slot0.tick,
                "tick_lower": position.snapshot.tick_lower,
                "tick_upper": position.snapshot.tick_upper,
                "liquidity_before": str(position.snapshot.liquidity),
                "quoted_amount0_raw": str(quoted0),
                "quoted_amount1_raw": str(quoted1),
                "fee_value_usd": fee_value,
                "estimated_gas_usd": gas_cost,
            }
        )
        pre0, pre1 = adapter.read_balances()
        tx = adapter.collect(job_id, position)
        if tx.status in {"PENDING", "BROADCAST_UNKNOWN"}:
            return CompoundResult(pool.name, token_id, CompoundJobState.COLLECT_PENDING.value, metadata={"job_id": job_id, "action_taken": True, "tx_hash": tx.tx_hash}).as_dict()
        if tx.status == "FAILED":
            self._record_revert(job_id, CompoundJobState.PREPARED, "collect reverted")
            return CompoundResult(pool.name, token_id, CompoundJobState.REVERTED.value, "COLLECT_REVERTED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        post0, post1 = adapter.read_balances()
        collected0 = max(0, post0.raw - pre0.raw)
        collected1 = max(0, post1.raw - pre1.raw)
        event_amounts = self._event_amounts(tx.metadata.get("receipt"), "COLLECT")
        if event_amounts and event_amounts != (collected0, collected1):
            self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="collect event and wallet balance delta disagree")
            return CompoundResult(pool.name, token_id, CompoundJobState.RECOVERY_REQUIRED.value, "COLLECT_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        if collected0 <= 0 and collected1 <= 0:
            self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="collect receipt had no confirmed wallet balance inflow")
            return CompoundResult(pool.name, token_id, CompoundJobState.RECOVERY_REQUIRED.value, "COLLECT_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        self.journal.update(
            job_id,
            status=CompoundJobState.COLLECTED.value,
            collected_amount0_raw=str(collected0),
            collected_amount1_raw=str(collected1),
            reserved_amount0_raw=str(collected0),
            reserved_amount1_raw=str(collected1),
        )
        job = self.journal.get(job_id)
        return self._continue_after_collect(w3, pool, adapter, position, job, collected0, collected1)

    def _continue_after_collect(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: CompoundAdapter,
        position: CompoundPosition,
        job: dict,
        reserved0: int,
        reserved1: int,
    ) -> dict:
        job_id = int(job["id"])
        slot0 = adapter.read_slot0()
        policy = self.eligibility.evaluate_policy(position, slot0, pool.auto_compound)
        if not policy.eligible:
            self.journal.update(job_id, status=CompoundJobState.WAITING_FOR_REBALANCE.value, error_reason=policy.reason)
            return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.WAITING_FOR_REBALANCE.value, policy.reason, metadata={"job_id": job_id, "action_taken": True}).as_dict()

        swap_plan = self.planner.build_swap_plan(position, slot0, reserved0, reserved1)
        if not swap_plan.skip_swap and swap_plan.amount_in > 0:
            price0 = self._token_price(pool.chain, position.snapshot.token0)
            price1 = self._token_price(pool.chain, position.snapshot.token1)
            if price0 is None or price1 is None:
                self.journal.update(job_id, status=CompoundJobState.WAITING_FOR_SWAP.value, error_reason="token price unavailable after collect")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.WAITING_FOR_SWAP.value, "PRICE_UNAVAILABLE", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            swap_input_usd = self._token_amount_usd(
                pool,
                position,
                swap_plan.token_in,
                swap_plan.amount_in,
                price0,
                price1,
            )
            if swap_input_usd < pool.min_swap_input_usd:
                no_swap_plan = self.planner.build_liquidity_plan(
                    position, slot0, reserved0, reserved1, pool.slippage_bps
                )
                if no_swap_plan.expected_liquidity <= 0:
                    self.journal.update(
                        job_id,
                        status=CompoundJobState.WAITING_FOR_SWAP.value,
                        error_reason="NO_REINVESTABLE_LIQUIDITY",
                    )
                    return CompoundResult(
                        pool.name,
                        position.snapshot.token_id,
                        CompoundJobState.WAITING_FOR_SWAP.value,
                        "NO_REINVESTABLE_LIQUIDITY",
                        metadata={
                            "job_id": job_id,
                            "action_taken": True,
                            "swap_input_usd": swap_input_usd,
                            "swap_decision": "NO_SWAP_INPUT_DUST",
                        },
                    ).as_dict()
                return self._increase(pool, adapter, position, job_id, reserved0, reserved1)

            quote = CompoundSwapper(pool).best_refined_quote(
                swap_plan.token_in, swap_plan.token_out, swap_plan.amount_in, pool.bot_wallet,
                self._quote_scorer(position, slot0, reserved0, reserved1),
            )
            if quote is None:
                self.journal.update(job_id, status=CompoundJobState.WAITING_FOR_SWAP.value, error_reason="swap route unavailable")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.WAITING_FOR_SWAP.value, "SWAP_ROUTE_UNAVAILABLE", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            dust_reason = self._swap_dust_reason(pool, quote, position, price0, price1)
            if dust_reason:
                no_swap_plan = self.planner.build_liquidity_plan(
                    position, slot0, reserved0, reserved1, pool.slippage_bps
                )
                if no_swap_plan.expected_liquidity <= 0:
                    self.journal.update(
                        job_id,
                        status=CompoundJobState.WAITING_FOR_SWAP.value,
                        error_reason="NO_REINVESTABLE_LIQUIDITY",
                    )
                    return CompoundResult(
                        pool.name,
                        position.snapshot.token_id,
                        CompoundJobState.WAITING_FOR_SWAP.value,
                        "NO_REINVESTABLE_LIQUIDITY",
                        metadata={
                            "job_id": job_id,
                            "action_taken": True,
                            "swap_input_usd": swap_input_usd,
                            "swap_decision": f"NO_SWAP_{dust_reason}",
                        },
                    ).as_dict()
                return self._increase(pool, adapter, position, job_id, reserved0, reserved1)
            token_in_is_0 = quote.token_in.lower() == position.snapshot.token0.lower()
            reserved_in = reserved0 if token_in_is_0 else reserved1
            if quote.amount_in > reserved_in:
                self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="swap amount exceeds reservation")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "RESERVATION_EXCEEDED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            if quote.allowance_target and adapter.allowance(quote.token_in, quote.allowance_target) < quote.amount_in:
                approval = adapter.approve(
                    job_id, quote.token_in, quote.allowance_target,
                    CompoundJobState.SWAP_APPROVAL_PENDING, CompoundJobState.COLLECTED,
                    "swap_approval_tx_hash", action="APPROVE_SWAP",
                )
                if approval.status != "SUCCESS":
                    return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.SWAP_APPROVAL_PENDING.value, metadata={"job_id": job_id, "action_taken": True, "tx_hash": approval.tx_hash}).as_dict()
            self.journal.update(
                job_id,
                swap_token_in=quote.token_in,
                swap_token_out=quote.token_out,
                swap_amount_in_raw=str(quote.amount_in),
                swap_provider=quote.provider,
            )
            pre0, pre1 = adapter.read_balances()
            swap_tx = adapter.executor.send_raw(
                job_id, quote.transaction, "SWAP", CompoundJobState.SWAP_PENDING,
                CompoundJobState.SWAPPED, "swap_tx_hash",
            )
            if swap_tx.status in {"PENDING", "BROADCAST_UNKNOWN"}:
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.SWAP_PENDING.value, metadata={"job_id": job_id, "action_taken": True, "tx_hash": swap_tx.tx_hash}).as_dict()
            if swap_tx.status == "FAILED":
                self._record_revert(job_id, CompoundJobState.COLLECTED, "swap reverted")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.REVERTED.value, "SWAP_REVERTED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            post0, post1 = adapter.read_balances()
            if token_in_is_0:
                actual_in, actual_out = max(0, pre0.raw - post0.raw), max(0, post1.raw - pre1.raw)
                reserved0, reserved1 = max(0, reserved0 - actual_in), reserved1 + actual_out
            else:
                actual_in, actual_out = max(0, pre1.raw - post1.raw), max(0, post0.raw - pre0.raw)
                reserved1, reserved0 = max(0, reserved1 - actual_in), reserved0 + actual_out
            receipt_movement = self._swap_movement(self.journal.get(job_id) or {}, swap_tx.metadata.get("receipt"))
            if receipt_movement and receipt_movement != (actual_in, actual_out):
                self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="swap transfer logs and wallet balance delta disagree")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "SWAP_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            if actual_in <= 0 or actual_out <= 0:
                self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="swap receipt had no confirmed balance movement")
                return CompoundResult(pool.name, position.snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "SWAP_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            self.journal.update(
                job_id,
                status=CompoundJobState.SWAPPED.value,
                swap_token_in=quote.token_in,
                swap_token_out=quote.token_out,
                swap_amount_in_raw=str(actual_in),
                swap_amount_out_raw=str(actual_out),
                swap_provider=quote.provider,
                reserved_amount0_raw=str(reserved0),
                reserved_amount1_raw=str(reserved1),
            )
        elif swap_plan.skip_swap:
            no_swap_plan = self.planner.build_liquidity_plan(
                position, slot0, reserved0, reserved1, pool.slippage_bps
            )
            if no_swap_plan.expected_liquidity <= 0:
                self.journal.update(
                    job_id,
                    status=CompoundJobState.WAITING_FOR_SWAP.value,
                    error_reason="NO_REINVESTABLE_LIQUIDITY",
                )
                return CompoundResult(
                    pool.name,
                    position.snapshot.token_id,
                    CompoundJobState.WAITING_FOR_SWAP.value,
                    "NO_REINVESTABLE_LIQUIDITY",
                    metadata={
                        "job_id": job_id,
                        "action_taken": True,
                        "swap_decision": "NO_SWAP_BALANCED",
                    },
                ).as_dict()
        return self._increase(pool, adapter, position, job_id, reserved0, reserved1)

    def _increase(
        self,
        pool: PoolConfig,
        adapter: CompoundAdapter,
        position: CompoundPosition,
        job_id: int,
        reserved0: int,
        reserved1: int,
    ) -> dict:
        snapshot = adapter.read_position(position)
        current_position = replace(
            position,
            snapshot=snapshot,
            stake_mode="STAKED" if snapshot.is_staked else "UNSTAKED",
        )
        slot0 = adapter.read_slot0()
        policy = self.eligibility.evaluate_policy(current_position, slot0, pool.auto_compound)
        if not policy.eligible:
            self.journal.update(job_id, status=CompoundJobState.WAITING_FOR_REBALANCE.value, error_reason=policy.reason)
            return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.WAITING_FOR_REBALANCE.value, policy.reason, metadata={"job_id": job_id, "action_taken": True}).as_dict()
        plan = self.planner.build_liquidity_plan(current_position, slot0, reserved0, reserved1, pool.slippage_bps)
        if plan.expected_liquidity <= 0 or (plan.amount0_desired <= 0 and plan.amount1_desired <= 0):
            self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="zero increase liquidity plan")
            return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "ZERO_LIQUIDITY_PLAN", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        spender = adapter.increase_spender(current_position)
        current_job = self.journal.get(job_id) or {}
        approval_success_state = (
            CompoundJobState.SWAPPED if current_job.get("swap_tx_hash") else CompoundJobState.COLLECTED
        )
        for index, (token, amount) in enumerate(((snapshot.token0, plan.amount0_desired), (snapshot.token1, plan.amount1_desired))):
            if amount <= 0 or adapter.allowance(token, spender) >= amount:
                continue
            approval = adapter.approve(
                job_id, token, spender,
                CompoundJobState.INCREASE_APPROVAL_PENDING,
                approval_success_state,
                f"increase_approval{index}_tx_hash",
                action=f"APPROVE_INCREASE_{index}",
            )
            if approval.status != "SUCCESS":
                return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.INCREASE_APPROVAL_PENDING.value, metadata={"job_id": job_id, "action_taken": True, "tx_hash": approval.tx_hash}).as_dict()
        pre0, pre1 = adapter.read_balances()
        before_liquidity = snapshot.liquidity
        tx = adapter.increase(job_id, current_position, plan)
        if tx.status in {"PENDING", "BROADCAST_UNKNOWN"}:
            return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.INCREASE_PENDING.value, metadata={"job_id": job_id, "action_taken": True, "tx_hash": tx.tx_hash}).as_dict()
        if tx.status == "FAILED":
            self._record_revert(job_id, CompoundJobState.SWAPPED, "increase reverted")
            return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.REVERTED.value, "INCREASE_REVERTED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        after = adapter.read_position(current_position)
        post0, post1 = adapter.read_balances()
        used0, used1 = max(0, pre0.raw - post0.raw), max(0, pre1.raw - post1.raw)
        event_amounts = self._event_amounts(tx.metadata.get("receipt"), "INCREASE")
        if event_amounts:
            event_liquidity, event_used0, event_used1 = event_amounts
            if (event_used0, event_used1) != (used0, used1):
                self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="increase event and wallet balance delta disagree")
                return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "INCREASE_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
            if event_liquidity != after.liquidity - before_liquidity:
                self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="increase event and position liquidity disagree")
                return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "INCREASE_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        if after.liquidity <= before_liquidity:
            self.journal.update(job_id, status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="increase receipt did not increase liquidity")
            return CompoundResult(pool.name, snapshot.token_id, CompoundJobState.RECOVERY_REQUIRED.value, "INCREASE_RECONCILIATION_FAILED", metadata={"job_id": job_id, "action_taken": True}).as_dict()
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.journal.update(
            job_id,
            status=CompoundJobState.COMPLETED.value,
            current_action=None,
            liquidity_added=str(after.liquidity - before_liquidity),
            liquidity_after=str(after.liquidity),
            amount0_used_raw=str(used0),
            amount1_used_raw=str(used1),
            dust0_raw=str(max(0, reserved0 - used0)),
            dust1_raw=str(max(0, reserved1 - used1)),
            reserved_amount0_raw="0",
            reserved_amount1_raw="0",
            completed_at=completed_at,
        )
        return CompoundResult(
            pool.name, snapshot.token_id, CompoundJobState.COMPLETED.value,
            metadata={
                "job_id": job_id, "action_taken": True, "tx_hash": tx.tx_hash,
                "liquidity_added": str(after.liquidity - before_liquidity),
                "amount0_used_raw": str(used0), "amount1_used_raw": str(used1),
                "dust0_raw": str(max(0, reserved0 - used0)), "dust1_raw": str(max(0, reserved1 - used1)),
            },
        ).as_dict()

    def _resume_job(self, w3: Web3, pool: PoolConfig, adapter: CompoundAdapter, job: dict) -> dict:
        state = CompoundJobState(str(job["status"]))
        token_id = int(job["token_id"])
        if job.get("pending_action"):
            return CompoundResult(pool.name, token_id, state.value, "PENDING_TRANSACTION", metadata={"job_id": job["id"]}).as_dict()
        job_npm = str(job.get("npm_address") or "").lower()
        runtime_npm = str(pool.npm_address or "").lower()
        if not job_npm or not runtime_npm or job_npm != runtime_npm:
            reason = f"NPM address changed: job={job.get('npm_address')} runtime={pool.npm_address}"
            self.journal.update(
                int(job["id"]),
                status=CompoundJobState.RECOVERY_REQUIRED.value,
                error_reason=reason,
            )
            return CompoundResult(
                pool.name,
                token_id,
                CompoundJobState.RECOVERY_REQUIRED.value,
                "NPM_ADDRESS_CHANGED",
                metadata={"job_id": job["id"]},
            ).as_dict()
        try:
            if str(job.get("stake_mode")) == "STAKED":
                found = adapter.read_adapter.read_staked_positions([token_id])
                snapshot = found[token_id]
            else:
                placeholder = PositionSnapshot(
                    token_id=token_id,
                    owner=pool.bot_wallet,
                    pool_address=pool.pool_address,
                    token0=pool.token0_address,
                    token1=pool.token1_address,
                    fee=int(pool.tick_spacing or pool.fee or 0),
                    tick_lower=int(job.get("tick_lower") or 0),
                    tick_upper=int(job.get("tick_upper") or 0),
                    liquidity=int(job.get("liquidity_before") or 0),
                    is_staked=False,
                )
                position = CompoundPosition(placeholder, str(job["npm_address"]), str(job["dex_type"]), str(job["stake_mode"]))
                snapshot = adapter.read_position(position)
            position = CompoundPosition(snapshot, str(job["npm_address"]), str(job["dex_type"]), str(job["stake_mode"]))
        except Exception as exc:
            reserved0 = int(job.get("reserved_amount0_raw") or 0)
            reserved1 = int(job.get("reserved_amount1_raw") or 0)
            self.journal.update(
                int(job["id"]), status=CompoundJobState.SUPERSEDED_BY_REBALANCE.value,
                dust0_raw=str(reserved0), dust1_raw=str(reserved1),
                reserved_amount0_raw="0", reserved_amount1_raw="0", error_reason=str(exc),
            )
            return CompoundResult(pool.name, token_id, CompoundJobState.SUPERSEDED_BY_REBALANCE.value, str(exc), metadata={"job_id": job["id"]}).as_dict()
        reserved0 = int(job.get("reserved_amount0_raw") or 0)
        reserved1 = int(job.get("reserved_amount1_raw") or 0)
        if state == CompoundJobState.PREPARED:
            self.journal.update(int(job["id"]), status=CompoundJobState.CANCELLED.value, error_reason="safe restart after pre-collect revert")
            return CompoundResult(pool.name, token_id, CompoundJobState.CANCELLED.value, "PRE_COLLECT_RETRY_CANCELLED", metadata={"job_id": job["id"]}).as_dict()
        if state == CompoundJobState.COLLECTED and reserved0 <= 0 and reserved1 <= 0 and job.get("collect_tx_hash"):
            receipt = self._receipt(w3, job["collect_tx_hash"])
            amounts = self._event_amounts(receipt, "COLLECT") if receipt else None
            if amounts:
                reserved0, reserved1 = amounts
                self.journal.update(
                    int(job["id"]), collected_amount0_raw=str(reserved0), collected_amount1_raw=str(reserved1),
                    reserved_amount0_raw=str(reserved0), reserved_amount1_raw=str(reserved1),
                )
        if state == CompoundJobState.SWAPPED and not job.get("swap_amount_out_raw") and job.get("swap_tx_hash"):
            receipt = self._receipt(w3, job["swap_tx_hash"])
            movement = self._swap_movement(job, receipt) if receipt else None
            if movement:
                actual_in, actual_out = movement
                token_in_is_0 = str(job.get("swap_token_in")).lower() == snapshot.token0.lower()
                if token_in_is_0:
                    reserved0, reserved1 = max(0, reserved0 - actual_in), reserved1 + actual_out
                else:
                    reserved1, reserved0 = max(0, reserved1 - actual_in), reserved0 + actual_out
                self.journal.update(
                    int(job["id"]), swap_amount_in_raw=str(actual_in), swap_amount_out_raw=str(actual_out),
                    reserved_amount0_raw=str(reserved0), reserved_amount1_raw=str(reserved1),
                )
        if state in {CompoundJobState.COLLECTED, CompoundJobState.WAITING_FOR_SWAP, CompoundJobState.WAITING_FOR_REBALANCE, CompoundJobState.REVERTED}:
            return self._continue_after_collect(w3, pool, adapter, position, job, reserved0, reserved1)
        if state == CompoundJobState.INCREASE_PENDING and job.get("increase_tx_hash"):
            receipt = self._receipt(w3, job["increase_tx_hash"])
            if receipt and self._finalize_increase_receipt(job, receipt):
                return CompoundResult(pool.name, token_id, CompoundJobState.COMPLETED.value, metadata={"job_id": job["id"], "tx_hash": job["increase_tx_hash"]}).as_dict()
            self.journal.update(int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="could not reconcile confirmed increase receipt")
            return CompoundResult(pool.name, token_id, CompoundJobState.RECOVERY_REQUIRED.value, "INCREASE_RECONCILIATION_FAILED", metadata={"job_id": job["id"]}).as_dict()
        if state in {CompoundJobState.SWAPPED, CompoundJobState.INCREASE_APPROVAL_PENDING}:
            return self._increase(pool, adapter, position, int(job["id"]), reserved0, reserved1)
        self.journal.update(int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason=f"unsupported recovery state {state.value}")
        return CompoundResult(pool.name, token_id, CompoundJobState.RECOVERY_REQUIRED.value, f"unsupported recovery state {state.value}", metadata={"job_id": job["id"]}).as_dict()

    def _finish_recovered_transaction(self, job: dict, receipt, pool: PoolConfig) -> None:
        action = str(job.get("pending_action") or "")
        tx_hash = Web3.to_hex(receipt.get("transactionHash"))
        mapping = {
            "COLLECT": (CompoundJobState.COLLECTED, "collect_tx_hash"),
            "APPROVE_SWAP": (CompoundJobState.COLLECTED, "swap_approval_tx_hash"),
            "APPROVE_SWAP_ZERO": (CompoundJobState.COLLECTED, "swap_approval_tx_hash"),
            "SWAP": (CompoundJobState.SWAPPED, "swap_tx_hash"),
            "APPROVE_INCREASE_0": (CompoundJobState.SWAPPED, "increase_approval0_tx_hash"),
            "APPROVE_INCREASE_0_ZERO": (CompoundJobState.SWAPPED, "increase_approval0_tx_hash"),
            "APPROVE_INCREASE_1": (CompoundJobState.SWAPPED, "increase_approval1_tx_hash"),
            "APPROVE_INCREASE_1_ZERO": (CompoundJobState.SWAPPED, "increase_approval1_tx_hash"),
            "INCREASE": (CompoundJobState.INCREASE_PENDING, "increase_tx_hash"),
        }
        if action not in mapping:
            self.journal.update(
                int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value,
                pending_action=None, pending_nonce=None, pending_signed_tx_hash=None,
                pending_broadcast_tx_hash=None, pending_since=None,
                error_reason=f"unknown pending action {action}",
            )
            return
        state, field = mapping[action]
        self.journal.complete_transaction(int(job["id"]), state, field, tx_hash)
        if action == "COLLECT":
            amounts = self._event_amounts(receipt, action)
            if not amounts:
                self.journal.update(int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="could not decode recovered collect receipt")
                return
            self.journal.update(
                int(job["id"]), collected_amount0_raw=str(amounts[0]), collected_amount1_raw=str(amounts[1]),
                reserved_amount0_raw=str(amounts[0]), reserved_amount1_raw=str(amounts[1]),
            )
        elif action == "SWAP":
            movement = self._swap_movement(job, receipt)
            if not movement:
                self.journal.update(int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="could not decode recovered swap receipt")
                return
            actual_in, actual_out = movement
            reserved0 = int(job.get("reserved_amount0_raw") or 0)
            reserved1 = int(job.get("reserved_amount1_raw") or 0)
            if not pool.token0_address:
                raise RuntimeError("pool token0 metadata unavailable during swap recovery")
            if str(job.get("swap_token_in")).lower() == pool.token0_address.lower():
                reserved0, reserved1 = max(0, reserved0 - actual_in), reserved1 + actual_out
            else:
                reserved1, reserved0 = max(0, reserved1 - actual_in), reserved0 + actual_out
            self.journal.update(
                int(job["id"]), swap_amount_in_raw=str(actual_in), swap_amount_out_raw=str(actual_out),
                reserved_amount0_raw=str(reserved0), reserved_amount1_raw=str(reserved1),
            )
        elif action == "INCREASE":
            if not self._finalize_increase_receipt(job, receipt):
                self.journal.update(int(job["id"]), status=CompoundJobState.RECOVERY_REQUIRED.value, error_reason="could not decode recovered increase receipt")
                return

    def _rollback_reverted_transaction(self, job: dict) -> None:
        action = str(job.get("pending_action") or "")
        retry = int(job.get("retry_count") or 0) + 1
        if retry >= self.MAX_RETRIES:
            state = CompoundJobState.MANUAL_RECOVERY
        elif action == "COLLECT":
            state = CompoundJobState.PREPARED
        elif action in {"SWAP", "APPROVE_SWAP", "APPROVE_SWAP_ZERO"}:
            state = CompoundJobState.COLLECTED
        else:
            state = CompoundJobState.SWAPPED
        self.journal.update(
            int(job["id"]), status=state.value, retry_count=retry,
            pending_action=None, pending_nonce=None, pending_signed_tx_hash=None,
            pending_broadcast_tx_hash=None, pending_since=None,
            error_reason=f"{action} reverted on-chain",
        )

    def _record_revert(self, job_id: int, rollback: CompoundJobState, reason: str) -> None:
        job = self.journal.get(job_id) or {}
        retry = int(job.get("retry_count") or 0) + 1
        state = CompoundJobState.MANUAL_RECOVERY if retry >= self.MAX_RETRIES else rollback
        self.journal.update(
            job_id, status=state.value, retry_count=retry, current_action=None,
            pending_action=None, pending_nonce=None, pending_signed_tx_hash=None,
            pending_broadcast_tx_hash=None, pending_since=None, error_reason=reason,
        )

    def _runtime(self, raw_pool: PoolConfig):
        w3 = web3_connection(raw_pool.chain)
        read_executor = TxExecutor(w3, raw_pool, self.config.dry_run, self.config, signer=self.signer)
        if raw_pool.dex_type in {DexType.PANCAKE_V3, DexType.PANCAKE_V3_MASTERCHEF}:
            read_adapter = PancakeV3MasterChefAdapter(w3, raw_pool, read_executor)
        else:
            read_adapter = AerodromeGaugeAdapter(w3, raw_pool, read_executor)
        pool = read_adapter.discover_pool_metadata()
        if pool != raw_pool:
            read_executor = TxExecutor(w3, pool, self.config.dry_run, self.config, signer=self.signer)
            if pool.dex_type in {DexType.PANCAKE_V3, DexType.PANCAKE_V3_MASTERCHEF}:
                read_adapter = PancakeV3MasterChefAdapter(w3, pool, read_executor)
            else:
                read_adapter = AerodromeGaugeAdapter(w3, pool, read_executor)
        executor = CompoundExecutor(w3, pool, self.config, self.journal, self.signer)
        return w3, pool, read_adapter, executor

    def _estimate_gas_usd(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: CompoundAdapter,
        position: CompoundPosition,
        quote: CompoundSwapQuote | None,
        amount0: int,
        amount1: int,
        native_price: float,
    ) -> float:
        units = self.BASE_GAS_UNITS if quote else self.BASE_GAS_UNITS - 500_000
        if quote and quote.allowance_target and adapter.allowance(quote.token_in, quote.allowance_target) < quote.amount_in:
            units += self.APPROVAL_GAS_UNITS
        spender = adapter.increase_spender(position)
        if amount0 > 0 and adapter.allowance(position.snapshot.token0, spender) < amount0:
            units += self.APPROVAL_GAS_UNITS
        if amount1 > 0 and adapter.allowance(position.snapshot.token1, spender) < amount1:
            units += self.APPROVAL_GAS_UNITS
        policy = self.config.gas_policies.get(pool.chain.upper())
        gas_params = get_gas_params(w3, pool.chain, action="swap", policy=policy)
        gas_price = int(gas_params.get("maxFeePerGas") or gas_params.get("gasPrice") or 0)
        return float(Web3.from_wei(units * gas_price, "ether")) * native_price

    def _token_price(self, chain: str, token: str) -> float | None:
        key = (chain.upper(), token.lower())
        if key not in self._price_cache:
            self._price_cache[key] = token_price_usd(chain, token)
        return self._price_cache[key]

    def _native_price(self, chain: str) -> float | None:
        wrapped = WRAPPED_NATIVE_TOKENS.get(chain.upper())
        if wrapped:
            price = self._token_price(chain, wrapped)
            if price is not None:
                return price
        return self.config.pnl_native_prices_usd.get(chain.upper())

    @staticmethod
    def _usd_value(pool: PoolConfig, amount0: int, amount1: int, price0: float, price1: float) -> float:
        decimals0 = int(pool.token0_decimals or 18)
        decimals1 = int(pool.token1_decimals or 18)
        return amount0 / (10**decimals0) * price0 + amount1 / (10**decimals1) * price1

    @staticmethod
    def _token_amount_usd(
        pool: PoolConfig,
        position: CompoundPosition,
        token: str | None,
        amount: int,
        price0: float,
        price1: float,
    ) -> float:
        if token and token.lower() == position.snapshot.token0.lower():
            return amount / (10 ** int(pool.token0_decimals or 18)) * price0
        return amount / (10 ** int(pool.token1_decimals or 18)) * price1

    @staticmethod
    def _swap_dust_reason(
        pool: PoolConfig,
        quote: CompoundSwapQuote,
        position: CompoundPosition,
        price0: float,
        price1: float,
    ) -> str | None:
        if quote.token_in.lower() == position.snapshot.token0.lower():
            in_decimals, out_decimals, in_price, out_price = int(pool.token0_decimals or 18), int(pool.token1_decimals or 18), price0, price1
        else:
            in_decimals, out_decimals, in_price, out_price = int(pool.token1_decimals or 18), int(pool.token0_decimals or 18), price1, price0
        if quote.amount_in / (10**in_decimals) * in_price < pool.min_swap_input_usd:
            return "SWAP_INPUT_DUST"
        if quote.amount_out / (10**out_decimals) * out_price < pool.min_swap_output_usd:
            return "SWAP_OUTPUT_DUST"
        return None

    @staticmethod
    def _quote_scorer(position: CompoundPosition, slot0, amount0: int, amount1: int):
        token0 = position.snapshot.token0.lower()

        def score(quote: CompoundSwapQuote) -> int:
            if quote.token_in.lower() == token0:
                post0 = max(0, int(amount0) - quote.amount_in)
                post1 = int(amount1) + quote.amount_out
            else:
                post1 = max(0, int(amount1) - quote.amount_in)
                post0 = int(amount0) + quote.amount_out
            return liquidity_from_amounts_exact(
                slot0.sqrt_price_x96,
                position.snapshot.tick_lower,
                position.snapshot.tick_upper,
                post0,
                post1,
            )

        return score

    @staticmethod
    def _receipt(w3: Web3, tx_hash: str):
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except Exception:
            return None

    @staticmethod
    def _event_amounts(receipt, action: str) -> tuple[int, ...] | None:
        if not receipt:
            return None
        expected = COLLECT_TOPIC.lower() if action == "COLLECT" else INCREASE_LIQUIDITY_TOPIC.lower()
        words_needed = 2 if action == "COLLECT" else 3
        for event in receipt.get("logs", []):
            topics = event.get("topics") or []
            if not topics or ConfiguredPoolCompounder._hex(topics[0]).lower() != expected:
                continue
            raw = event.get("data", b"")
            if isinstance(raw, str):
                raw = bytes.fromhex(raw.removeprefix("0x"))
            else:
                raw = bytes(raw)
            if len(raw) < 32 * words_needed:
                continue
            tail = raw[-32 * words_needed :]
            return tuple(int.from_bytes(tail[index : index + 32], "big") for index in range(0, len(tail), 32))
        return None

    @staticmethod
    def _swap_movement(job: dict, receipt) -> tuple[int, int] | None:
        if not receipt or not job.get("swap_token_in") or not job.get("swap_token_out"):
            return None
        transfer_topic = Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")).lower()
        wallet = str(job["wallet_address"]).lower()
        token_in = str(job["swap_token_in"]).lower()
        token_out = str(job["swap_token_out"]).lower()
        amount_in = 0
        amount_out = 0
        for event in receipt.get("logs", []):
            topics = event.get("topics") or []
            if len(topics) < 3 or ConfiguredPoolCompounder._hex(topics[0]).lower() != transfer_topic:
                continue
            address = str(event.get("address") or "").lower()
            from_address = "0x" + ConfiguredPoolCompounder._hex(topics[1]).removeprefix("0x")[-40:]
            to_address = "0x" + ConfiguredPoolCompounder._hex(topics[2]).removeprefix("0x")[-40:]
            raw = event.get("data", b"")
            if isinstance(raw, str):
                value = int(raw, 16)
            else:
                value = int.from_bytes(bytes(raw), "big")
            if address == token_in and from_address.lower() == wallet:
                amount_in += value
            if address == token_out and to_address.lower() == wallet:
                amount_out += value
        return (amount_in, amount_out) if amount_in > 0 and amount_out > 0 else None

    def _finalize_increase_receipt(self, job: dict, receipt) -> bool:
        amounts = self._event_amounts(receipt, "INCREASE")
        if not amounts:
            return False
        liquidity_added, used0, used1 = amounts
        reserved0 = int(job.get("reserved_amount0_raw") or 0)
        reserved1 = int(job.get("reserved_amount1_raw") or 0)
        before = int(job.get("liquidity_before") or 0)
        self.journal.update(
            int(job["id"]), status=CompoundJobState.COMPLETED.value, current_action=None,
            liquidity_added=str(liquidity_added), liquidity_after=str(before + liquidity_added),
            amount0_used_raw=str(used0), amount1_used_raw=str(used1),
            dust0_raw=str(max(0, reserved0 - used0)), dust1_raw=str(max(0, reserved1 - used1)),
            reserved_amount0_raw="0", reserved_amount1_raw="0",
            completed_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        return True

    @staticmethod
    def _hex(value) -> str:
        return value if isinstance(value, str) else Web3.to_hex(value)
