from __future__ import annotations

import unittest
from unittest.mock import patch

from latest_farms.configured_pool_rebalancer.adapter import PancakeV3MasterChefAdapter
from latest_farms.configured_pool_rebalancer.models import DexType, PoolConfig, TxResult


TOKEN0 = "0x0000000000000000000000000000000000000003"
TOKEN1 = "0x0000000000000000000000000000000000000004"
WALLET = "0x0000000000000000000000000000000000000002"


class FakeExecutor:
    dry_run = False


class FakeSwapper:
    quote = None

    def __init__(self, chain_name, rpc_url):
        self.chain_name = chain_name
        self.rpc_url = rpc_url

    def get_best_swap_route(self, *args):
        return self.quote


def make_pool(price_impact_cap=1.0, min_swap_output_usd=0.0):
    return PoolConfig(
        name="TEST",
        chain="BAS",
        pool_address="0x0000000000000000000000000000000000000001",
        dex_type=DexType.PANCAKE_V3_MASTERCHEF,
        managed_wallets=(WALLET,),
        bot_wallet=WALLET,
        token0_address=TOKEN0,
        token1_address=TOKEN1,
        token0_decimals=6,
        token1_decimals=18,
        fee=2500,
        max_swap_price_impact_pct=price_impact_cap,
        min_swap_output_usd=min_swap_output_usd,
    )


def make_adapter(pool):
    adapter = PancakeV3MasterChefAdapter.__new__(PancakeV3MasterChefAdapter)
    adapter.pool = pool
    adapter.executor = FakeExecutor()
    adapter.approvals = []
    adapter.sent_payloads = []
    adapter.approve_if_needed = lambda token, spender, amount: adapter.approvals.append((token, spender, amount))

    def send_raw_swap(tx, metadata=None):
        adapter.sent_payloads.append((tx, metadata))
        return TxResult(tx_hash="0x" + "1" * 64, metadata=metadata or {})

    adapter._send_raw_swap = send_raw_swap
    return adapter


class AdapterSwapperIntegrationTests(unittest.TestCase):
    def test_swap_uses_internal_swapper_and_preserves_metadata(self):
        pool = make_pool()
        adapter = make_adapter(pool)
        FakeSwapper.quote = {
            "provider": "0x",
            "to": "0x0000000000000000000000000000000000000012",
            "data": "0x1234",
            "value": "0",
            "allowanceTarget": "0x0000000000000000000000000000000000000013",
            "buyAmount": "1500",
            "price_impact": 0.2,
        }

        with patch("latest_farms.configured_pool_rebalancer.swapper.V3Swapper", FakeSwapper):
            result = adapter.swap(TOKEN0, TOKEN1, 1000)

        self.assertIsNotNone(result)
        self.assertEqual(adapter.approvals, [(TOKEN0, "0x0000000000000000000000000000000000000013", 1000)])
        tx, metadata = adapter.sent_payloads[0]
        self.assertEqual(tx["to"], "0x0000000000000000000000000000000000000012")
        self.assertEqual(metadata["token_in"], TOKEN0)
        self.assertEqual(metadata["token_out"], TOKEN1)
        self.assertEqual(metadata["amount_in"], "1000")
        self.assertEqual(metadata["quote_buy_amount"], "1500")
        self.assertEqual(metadata["price_impact"], 0.2)

    def test_swap_returns_none_when_price_impact_exceeds_cap(self):
        pool = make_pool(price_impact_cap=0.1)
        adapter = make_adapter(pool)
        FakeSwapper.quote = {
            "provider": "0x",
            "to": "0x0000000000000000000000000000000000000012",
            "data": "0x1234",
            "value": "0",
            "buyAmount": "1500",
            "price_impact": 0.2,
        }

        with patch("latest_farms.configured_pool_rebalancer.swapper.V3Swapper", FakeSwapper):
            result = adapter.swap(TOKEN0, TOKEN1, 1000)

        self.assertIsNone(result)
        self.assertEqual(adapter.sent_payloads, [])


if __name__ == "__main__":
    unittest.main()
