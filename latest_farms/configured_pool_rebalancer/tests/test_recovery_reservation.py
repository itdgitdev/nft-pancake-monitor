from __future__ import annotations

import unittest

from latest_farms.configured_pool_rebalancer.discord_notifier import DiscordNotifier
from latest_farms.configured_pool_rebalancer.models import (
    DexType,
    PoolConfig,
    RebalancePlan,
    TokenBalance,
    TxResult,
    WorkerConfig,
)
from latest_farms.configured_pool_rebalancer.worker import ConfiguredPoolRebalancer


TOKEN0 = "0x0000000000000000000000000000000000000003"
TOKEN1 = "0x0000000000000000000000000000000000000004"
WALLET = "0x0000000000000000000000000000000000000002"


def make_pool() -> PoolConfig:
    return PoolConfig(
        name="TEST-USDC-POP",
        chain="BASE",
        pool_address="0x0000000000000000000000000000000000000001",
        dex_type=DexType.PANCAKE_V3_MASTERCHEF,
        managed_wallets=(WALLET,),
        bot_wallet=WALLET,
        token0_address=TOKEN0,
        token1_address=TOKEN1,
        token0_decimals=6,
        token1_decimals=18,
        fee=2500,
        pid=1,
    )


def make_worker(discord_enabled: bool = False):
    worker = ConfiguredPoolRebalancer.__new__(ConfiguredPoolRebalancer)
    worker.config = WorkerConfig(pools=(make_pool(),), dry_run=False, discord_enabled=discord_enabled)
    return worker


class FakeJournal:
    def __init__(self, reservations=None, already_notified=False):
        self.reservations = reservations or []
        self.already_notified = already_notified
        self.errors = []
        self.notified = []

    def fetch_wallet_token_reservations(self, chain, wallet):
        return self.reservations

    def mark_recovery_error(self, chain, old_token_id, error_reason):
        self.errors.append((chain, old_token_id, error_reason))

    def recovery_already_notified(self, chain, old_token_id):
        return self.already_notified

    def mark_recovery_notified(self, chain, old_token_id):
        self.already_notified = True
        self.notified.append((chain, old_token_id))


class FakeAdapter:
    def __init__(self, balance0: int, balance1: int):
        self.balance0 = TokenBalance(raw=balance0, decimals=6)
        self.balance1 = TokenBalance(raw=balance1, decimals=18)

    def read_balances(self, wallet):
        return self.balance0, self.balance1


class FakeNotifier:
    def __init__(self):
        self.sent = []

    def recovery_required_message(self, pool_name, chain, wallet, token_id, reason):
        return f"{pool_name}|{chain}|{wallet}|{token_id}|{reason}"

    def send(self, message):
        self.sent.append(message)


class ReservationLedgerTests(unittest.TestCase):
    def test_reservation_after_swap_moves_input_to_output_token(self):
        worker = make_worker()
        pool = make_pool()
        swap_tx = TxResult(
            tx_hash="0x" + "1" * 64,
            metadata={
                "token_in": pool.token1_address,
                "token_out": pool.token0_address,
                "amount_in": "30",
            },
        )

        reserved0, reserved1 = worker._reservation_after_swap(
            pool,
            reserved0=10,
            reserved1=100,
            before0=TokenBalance(raw=1000, decimals=6),
            before1=TokenBalance(raw=1000, decimals=18),
            after0=TokenBalance(raw=1025, decimals=6),
            after1=TokenBalance(raw=970, decimals=18),
            swap_tx=swap_tx,
        )

        self.assertEqual((reserved0, reserved1), (35, 70))

    def test_mint_amount_is_clamped_by_job_reservation(self):
        worker = make_worker()
        plan = RebalancePlan(
            old_token_id=123,
            current_tick=0,
            old_tick_lower=-100,
            old_tick_upper=100,
            new_tick_lower=-50,
            new_tick_upper=50,
            amount0_desired=100,
            amount1_desired=100,
        )

        original0, original1, available0, available1 = worker._clamp_plan_to_reservation(
            plan,
            reserved0=35,
            reserved1=70,
            pre_mint0=TokenBalance(raw=1000, decimals=6),
            pre_mint1=TokenBalance(raw=1000, decimals=18),
        )

        self.assertEqual((original0, original1), (100, 100))
        self.assertEqual((available0, available1), (35, 70))
        self.assertEqual((plan.amount0_desired, plan.amount1_desired), (35, 70))

    def test_reservation_coverage_blocks_when_wallet_cannot_cover_total(self):
        worker = make_worker()
        pool = make_pool()
        worker.journal = FakeJournal(
            reservations=[
                {
                    "token_address": pool.token0_address.lower(),
                    "reserved_raw": "120",
                }
            ]
        )

        error = worker._reservation_coverage_error(
            pool,
            FakeAdapter(balance0=100, balance1=1000),
            stage="recovery_pre_mint",
        )

        self.assertIsNotNone(error)
        self.assertIn("reservation coverage failed", error)
        self.assertIn("required_raw=120", error)
        self.assertIn("actual_raw=100", error)

    def test_reservation_coverage_allows_shared_token_when_wallet_can_cover_total(self):
        worker = make_worker()
        pool = make_pool()
        worker.journal = FakeJournal(
            reservations=[
                {
                    "token_address": pool.token0_address.lower(),
                    "reserved_raw": "120",
                }
            ]
        )

        error = worker._reservation_coverage_error(
            pool,
            FakeAdapter(balance0=130, balance1=1000),
            stage="recovery_pre_mint",
        )

        self.assertIsNone(error)

    def test_recovery_requires_manual_action_when_reservation_is_missing(self):
        worker = make_worker()
        pool = make_pool()
        worker.journal = FakeJournal()
        worker.notifier = FakeNotifier()

        result = worker._recover_withdrawn_unminted(
            w3=None,
            pool=pool,
            adapter=None,
            job={
                "old_token_id": 123,
                "status": "FAILED",
                "wallet_address": pool.bot_wallet,
                "old_tick_lower": -100,
                "old_tick_upper": 100,
            },
        )

        self.assertEqual(result["state"], "RECOVERY_REQUIRED")
        self.assertEqual(result["recovery"], "MANUAL_REQUIRED")
        self.assertIn("missing reservation ledger", worker.journal.errors[0][2])


class RecoveryDiscordTests(unittest.TestCase):
    def test_recovery_notify_is_sent_once_for_same_error(self):
        worker = make_worker(discord_enabled=True)
        pool = make_pool()
        worker.journal = FakeJournal()
        worker.notifier = FakeNotifier()

        worker._notify_recovery_required(pool, 123, "missing reservation ledger")
        worker._notify_recovery_required(pool, 123, "missing reservation ledger")

        self.assertEqual(len(worker.notifier.sent), 1)
        self.assertEqual(worker.journal.notified, [("BASE", 123)])

    def test_recovery_required_message_contains_operator_context(self):
        notifier = DiscordNotifier(WorkerConfig(pools=(make_pool(),)))
        message = notifier.recovery_required_message(
            pool_name="USDC-POP-0.25",
            chain="BASE",
            wallet=WALLET,
            token_id=2013280,
            reason="missing reservation ledger",
        )

        self.assertIn("Recovery Required", message)
        self.assertIn("USDC-POP-0.25", message)
        self.assertIn("2013280", message)
        self.assertIn("missing reservation ledger", message)


if __name__ == "__main__":
    unittest.main()
