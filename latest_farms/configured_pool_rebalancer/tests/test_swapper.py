from __future__ import annotations

import unittest
from unittest.mock import patch

from latest_farms.configured_pool_rebalancer.swapper import V3Swapper


TOKEN_IN = "0x0000000000000000000000000000000000000003"
TOKEN_OUT = "0x0000000000000000000000000000000000000004"
WALLET = "0x0000000000000000000000000000000000000002"


class FakeResponse:
    def __init__(self, data, status_code=200, text=""):
        self._data = data
        self.status_code = status_code
        self.text = text or str(data)

    def json(self):
        return self._data


class SwapperProviderTests(unittest.TestCase):
    def test_kyber_route_and_build_parse_quote(self):
        swapper = V3Swapper("BAS", "http://localhost")
        route_payload = {
            "code": 0,
            "data": {
                "routeSummary": {
                    "amountOut": "1200",
                    "amountInUsd": "1.00",
                    "amountOutUsd": "0.99",
                    "route": [[{"exchange": "PancakeSwap"}]],
                }
            },
        }
        build_payload = {
            "code": 0,
            "data": {
                "routerAddress": "0x0000000000000000000000000000000000000011",
                "data": "0xabcdef",
                "value": "0",
            },
        }

        with patch(
            "latest_farms.configured_pool_rebalancer.swapper.requests.get",
            return_value=FakeResponse(route_payload),
        ), patch(
            "latest_farms.configured_pool_rebalancer.swapper.requests.post",
            return_value=FakeResponse(build_payload),
        ):
            route = swapper.get_kyber_route(TOKEN_IN, TOKEN_OUT, 1000)
            tx = swapper.build_kyber_swap_data(route, WALLET, 50)
            quote = swapper._kyber_quote(tx, route["routeSummary"])

        self.assertEqual(quote["provider"], "KyberSwap")
        self.assertEqual(quote["buyAmount"], "1200")
        self.assertEqual(quote["to"], "0x0000000000000000000000000000000000000011")
        self.assertEqual(quote["allowanceTarget"], "0x0000000000000000000000000000000000000011")

    def test_0x_quote_parse_transaction_and_allowance(self):
        swapper = V3Swapper("BAS", "http://localhost")
        response = {
            "buyAmount": "1500",
            "transaction": {
                "to": "0x0000000000000000000000000000000000000012",
                "data": "0x1234",
                "value": "0",
                "gas": "210000",
                "gasPrice": "1",
            },
            "issues": {"allowance": {"spender": "0x0000000000000000000000000000000000000013"}},
            "route": {"fills": [{"source": "Uniswap_V3"}]},
        }

        with patch(
            "latest_farms.configured_pool_rebalancer.swapper.requests.get",
            return_value=FakeResponse(response),
        ):
            quote = swapper.get_0x_swap_quote(TOKEN_IN, TOKEN_OUT, 1000, WALLET, 50)

        self.assertEqual(quote["provider"], "0x")
        self.assertEqual(quote["buyAmount"], "1500")
        self.assertEqual(quote["to"], "0x0000000000000000000000000000000000000012")
        self.assertEqual(quote["allowanceTarget"], "0x0000000000000000000000000000000000000013")

    def test_okx_quote_parse_transaction_and_approve_address(self):
        swapper = V3Swapper("BAS", "http://localhost")
        response = {
            "code": "0",
            "data": [
                {
                    "approveAddress": "0x0000000000000000000000000000000000000014",
                    "tx": {
                        "to": "0x0000000000000000000000000000000000000015",
                        "data": "0xbeef",
                        "value": "0",
                        "gas": "220000",
                    },
                    "routerResult": {
                        "toTokenAmount": "1700",
                        "priceImpactPercent": "0.2",
                        "dexRouterList": [{"dexProtocol": {"dexName": "OKX Dex"}}],
                    },
                }
            ],
        }

        with patch(
            "latest_farms.configured_pool_rebalancer.swapper.requests.get",
            return_value=FakeResponse(response),
        ):
            quote = swapper.get_okx_swap_quote(TOKEN_IN, TOKEN_OUT, 1000, WALLET, 50)

        self.assertEqual(quote["provider"], "OKX")
        self.assertEqual(quote["buyAmount"], "1700")
        self.assertEqual(quote["to"], "0x0000000000000000000000000000000000000015")
        self.assertEqual(quote["allowanceTarget"], "0x0000000000000000000000000000000000000014")

    def test_best_route_selects_highest_buy_amount_and_ignores_failures(self):
        swapper = V3Swapper("BAS", "http://localhost")
        swapper.get_kyber_route = lambda *args, **kwargs: None
        swapper.get_0x_swap_quote = lambda *args, **kwargs: {
            "provider": "0x",
            "to": "0x0000000000000000000000000000000000000012",
            "data": "0x1234",
            "buyAmount": "1000",
        }
        swapper.get_okx_swap_quote = lambda *args, **kwargs: {
            "provider": "OKX",
            "to": "0x0000000000000000000000000000000000000015",
            "data": "0xbeef",
            "buyAmount": "2000",
        }

        quote = swapper.get_best_swap_route(TOKEN_IN, TOKEN_OUT, 1000, WALLET, 50)

        self.assertEqual(quote["provider"], "OKX")
        self.assertEqual(quote["buyAmount"], "2000")

    def test_best_route_returns_none_when_all_providers_fail(self):
        swapper = V3Swapper("BAS", "http://localhost")
        swapper.get_kyber_route = lambda *args, **kwargs: None
        swapper.get_0x_swap_quote = lambda *args, **kwargs: None
        swapper.get_okx_swap_quote = lambda *args, **kwargs: None

        self.assertIsNone(swapper.get_best_swap_route(TOKEN_IN, TOKEN_OUT, 1000, WALLET, 50))


if __name__ == "__main__":
    unittest.main()
