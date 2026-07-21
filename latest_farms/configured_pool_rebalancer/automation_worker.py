from __future__ import annotations

from dataclasses import replace

from .auto_compound.worker import ConfiguredPoolCompounder
from .models import RebalanceCycleOutcome, WorkerConfig
from .signer import RuntimeSigner
from .worker import ConfiguredPoolRebalancer


class ConfiguredPoolAutomationWorker:
    """Run the unchanged rebalancer first, then the isolated compound pass."""

    def __init__(self, config: WorkerConfig, migrate: bool = False, signer: RuntimeSigner | None = None):
        self.config = config
        self.signer = signer
        self.rebalancer = ConfiguredPoolRebalancer(config, migrate=migrate, signer=signer)
        self.compounder = ConfiguredPoolCompounder(config, migrate=migrate, signer=signer)

    def run_once(self) -> list[dict]:
        pending_wallets = self.compounder.reconcile_pending_wallets()
        outcome = RebalanceCycleOutcome()
        runnable_pools = tuple(
            pool for pool in self.config.pools if pool.bot_wallet.lower() not in pending_wallets
        )
        if runnable_pools:
            if len(runnable_pools) == len(self.config.pools):
                outcome = self.rebalancer.run_once_with_outcome()
            else:
                filtered = replace(self.config, pools=runnable_pools)
                outcome = ConfiguredPoolRebalancer(
                    filtered,
                    migrate=False,
                    signer=self.signer,
                ).run_once_with_outcome()
        rebalance_results = list(outcome.records)
        for pool in self.config.pools:
            if pool.bot_wallet.lower() in pending_wallets:
                rebalance_results.append(
                    {
                        "pool": pool.name,
                        "status": "SKIPPED",
                        "reason": "COMPOUND_PENDING_NONCE_GUARD",
                    }
                )

        blocked = set(pending_wallets)
        blocked.update(outcome.blocked_wallets)
        compound_results = self.compounder.run_once(
            blocked_wallets=blocked,
            compound_candidates=outcome.compound_candidates,
        )
        return [*rebalance_results, *compound_results]
