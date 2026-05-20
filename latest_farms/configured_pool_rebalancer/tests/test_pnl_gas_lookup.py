from __future__ import annotations

import unittest
from unittest.mock import patch

from latest_farms.configured_pool_rebalancer.models import WorkerConfig
from latest_farms.configured_pool_rebalancer.pnl_report import ConfiguredPoolPnlReporter, _normalize_tx_hash


HASH = "27913079d016317abfe485acc61ebaa6c243d75e6dbdfc8e8d74f0f81f4cbe86"


class FakeGasReporter(ConfiguredPoolPnlReporter):
    def __init__(self, db_fees=None, receipt_fees=None):
        super().__init__(WorkerConfig(pools=()))
        self.db_fees = db_fees or {}
        self.receipt_fees = receipt_fees or {}
        self.db_calls = []
        self.receipt_calls = []

    def _tx_fee_from_db(self, chain: str, wallet: str, tx_hash: str):
        self.db_calls.append((chain, wallet, tx_hash))
        return self.db_fees.get(tx_hash)

    def _tx_fee_from_receipt(self, chain: str, tx_hash: str, warnings: list[str]):
        self.receipt_calls.append((chain, tx_hash))
        return self.receipt_fees.get(tx_hash)


class FakeCursor:
    def __init__(self):
        self.executions = []
        self.row = None
        self.closed = False

    def execute(self, sql, params):
        self.executions.append((sql, params))
        self.row = {"transaction_fee": "0.0000027855"} if params[2] == HASH else None

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self, dictionary=True, buffered=True):
        return self._cursor

    def close(self):
        self.closed = True


class PnlGasLookupTests(unittest.TestCase):
    def test_normalize_tx_hash_accepts_with_and_without_prefix(self):
        self.assertEqual(_normalize_tx_hash(HASH), (HASH, f"0x{HASH}"))
        self.assertEqual(_normalize_tx_hash(f"0x{HASH}"), (HASH, f"0x{HASH}"))

    def test_normalize_tx_hash_rejects_pseudo_and_invalid_hashes(self):
        self.assertIsNone(_normalize_tx_hash("failed:swap-broadcast"))
        self.assertIsNone(_normalize_tx_hash("dry-run:swap"))
        self.assertIsNone(_normalize_tx_hash("skipped:dust-swap-output"))
        self.assertIsNone(_normalize_tx_hash("not-a-hash"))

    def test_gas_lookup_uses_bare_hash_for_db_when_journal_hash_has_no_prefix(self):
        reporter = FakeGasReporter(db_fees={HASH: 0.123})
        warnings = []

        gas = reporter._gas_cost_native("BAS", "0xwallet", [HASH], warnings)

        self.assertEqual(gas, 0.123)
        self.assertEqual(reporter.db_calls, [("BAS", "0xwallet", HASH)])
        self.assertEqual(reporter.receipt_calls, [])
        self.assertEqual(warnings, [])

    def test_gas_lookup_dedupes_hash_with_and_without_prefix(self):
        reporter = FakeGasReporter(db_fees={HASH: 0.123})

        gas = reporter._gas_cost_native("BAS", "0xwallet", [HASH, f"0x{HASH}"], [])

        self.assertEqual(gas, 0.123)
        self.assertEqual(len(reporter.db_calls), 1)

    def test_gas_lookup_skips_pseudo_hash_without_warning_or_rpc(self):
        reporter = FakeGasReporter()
        warnings = []

        gas = reporter._gas_cost_native(
            "BAS",
            "0xwallet",
            ["failed:swap-broadcast", "dry-run:swap", "skipped:dust-swap-output"],
            warnings,
        )

        self.assertEqual(gas, 0.0)
        self.assertEqual(reporter.db_calls, [])
        self.assertEqual(reporter.receipt_calls, [])
        self.assertEqual(warnings, [])

    def test_gas_lookup_warns_for_invalid_non_pseudo_hash(self):
        reporter = FakeGasReporter()
        warnings = []

        gas = reporter._gas_cost_native("BAS", "0xwallet", ["not-a-hash"], warnings)

        self.assertEqual(gas, 0.0)
        self.assertEqual(reporter.db_calls, [])
        self.assertEqual(reporter.receipt_calls, [])
        self.assertEqual(len(warnings), 1)
        self.assertIn("invalid tx hash skipped", warnings[0])

    def test_gas_lookup_falls_back_to_rpc_with_prefixed_hash(self):
        reporter = FakeGasReporter(receipt_fees={f"0x{HASH}": 0.456})

        gas = reporter._gas_cost_native("BAS", "0xwallet", [HASH], [])

        self.assertEqual(gas, 0.456)
        self.assertEqual(reporter.db_calls, [("BAS", "0xwallet", HASH)])
        self.assertEqual(reporter.receipt_calls, [("BAS", f"0x{HASH}")])

    def test_db_lookup_strips_prefix_in_sql_match(self):
        reporter = ConfiguredPoolPnlReporter(WorkerConfig(pools=()))
        cursor = FakeCursor()
        connection = FakeConnection(cursor)

        with patch("latest_farms.configured_pool_rebalancer.pnl_report.get_connection", return_value=connection):
            fee = reporter._tx_fee_from_db("BAS", "0xwallet", f"0x{HASH}")

        self.assertEqual(fee, 0.0000027855)
        self.assertIn("REPLACE(LOWER(hash), '0x', '')=%s", cursor.executions[0][0])
        self.assertEqual(cursor.executions[0][1], ("BAS", "0xwallet", HASH))
        self.assertTrue(cursor.closed)
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()
