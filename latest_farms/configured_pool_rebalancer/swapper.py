from __future__ import annotations

import base64
import datetime
import hmac
import logging
from urllib.parse import urlencode

import requests
from requests.exceptions import RequestException, Timeout
from web3 import Web3


try:
    from latest_farms.config import (
        SWAPPER_0X_API_KEY,
        SWAPPER_KYBER_CLIENT_ID,
        SWAPPER_OKX_API_KEY,
        SWAPPER_OKX_PASSPHRASE,
        SWAPPER_OKX_SECRET_KEY,
    )
except ImportError:  # pragma: no cover
    from config import (
        SWAPPER_0X_API_KEY,
        SWAPPER_KYBER_CLIENT_ID,
        SWAPPER_OKX_API_KEY,
        SWAPPER_OKX_PASSPHRASE,
        SWAPPER_OKX_SECRET_KEY,
    )


log = logging.getLogger("configured_pool_rebalancer")

NATIVE_TOKEN_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


class V3Swapper:
    """Aggregator quote helper used by configured_pool_rebalancer.

    It returns transaction payloads only. Signing, approval, gas policy and
    broadcast remain owned by adapter.py / tx_executor.py.
    """

    def __init__(self, chain_name: str, rpc_url: str | None):
        self.chain_name = str(chain_name or "").upper()
        self.w3 = Web3(Web3.HTTPProvider(rpc_url)) if rpc_url else None
        self.chain_configs = {
            "BSC": "bsc",
            "BNB": "bsc",
            "ARB": "arbitrum",
            "BAS": "base",
            "ETH": "ethereum",
            "POL": "polygon",
            "POLYGON": "polygon",
        }
        self.chain_ids = {
            "ETH": 1,
            "BSC": 56,
            "BNB": 56,
            "POL": 137,
            "POLYGON": 137,
            "ARB": 42161,
            "BAS": 8453,
        }
        self.chain_id = self.chain_configs.get(self.chain_name, "")
        self.chain_id_0x = self.chain_ids.get(self.chain_name, 56)
        self.api_base_url = f"https://aggregator-api.kyberswap.com/{self.chain_id}/api/v1"
        self.api_base_url_0x = "https://api.0x.org/swap/allowance-holder/quote"
        self.okx_api_base = "https://www.okx.com/api/v6/dex/aggregator"

    def get_kyber_route(self, token_in: str, token_out: str, amount_wei: int) -> dict | None:
        if not self.chain_id:
            log.warning("kyber quote skipped: unsupported chain=%s", self.chain_name)
            return None
        url = f"{self.api_base_url}/routes"
        headers = {"X-Client-Id": SWAPPER_KYBER_CLIENT_ID, "Accept": "*/*"}
        params = {
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": str(int(amount_wei)),
            "saveGas": "true",
            "maxSplits": 1,
            "maxHops": 2,
        }
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            data = response.json()
            if data.get("code") == 0:
                return data.get("data")
            log.warning("kyber route unavailable chain=%s response=%s", self.chain_name, data)
            return None
        except Exception as exc:
            log.warning("kyber route error chain=%s token_in=%s token_out=%s amount=%s error=%s", self.chain_name, token_in, token_out, amount_wei, exc)
            return None

    def build_kyber_swap_data(self, route_summary: dict, sender_address: str, slippage_bps: int = 10) -> dict | None:
        if not self.chain_id:
            return None
        url = f"{self.api_base_url}/route/build"
        headers = {"X-Client-Id": SWAPPER_KYBER_CLIENT_ID, "Accept": "*/*"}
        payload = {
            "routeSummary": route_summary,
            "sender": Web3.to_checksum_address(sender_address),
            "recipient": Web3.to_checksum_address(sender_address),
            "slippageTolerance": int(slippage_bps),
            "deadline": int(datetime.datetime.now(datetime.timezone.utc).timestamp()) + 1200,
            "source": "configured_pool_rebalancer",
        }
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            data = response.json()
            if data.get("code") == 0:
                return data.get("data")
            log.warning("kyber build unavailable chain=%s response=%s", self.chain_name, data)
            return None
        except Exception as exc:
            log.warning("kyber build error chain=%s error=%s", self.chain_name, exc)
            return None

    def get_0x_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
        taker_address: str,
        slippage_bps: int = 10,
    ) -> dict | None:
        if not SWAPPER_0X_API_KEY:
            log.warning("0x quote skipped: missing SWAPPER_0X_API_KEY")
            return None
        headers = {"0x-api-key": SWAPPER_0X_API_KEY, "0x-version": "v2"}
        params = {
            "chainId": self.chain_id_0x,
            "buyToken": token_out,
            "sellToken": token_in,
            "sellAmount": str(int(amount_in_wei)),
            "taker": Web3.to_checksum_address(taker_address),
            "slippageBps": str(int(slippage_bps)),
            "skipValidation": "true",
        }
        try:
            log.info("0x quote request chain=%s amount=%s", self.chain_name, amount_in_wei)
            response = requests.get(self.api_base_url_0x, params=params, headers=headers, timeout=15)
            if response.status_code != 200:
                log.warning("0x quote failed chain=%s status=%s body=%s", self.chain_name, response.status_code, response.text[:300])
                return None
            data = response.json()
            tx_data = data.get("transaction") or {}
            if not tx_data:
                log.warning("0x quote missing transaction data chain=%s response=%s", self.chain_name, data)
                return None
            buy_amount = str(data.get("buyAmount") or "0")
            route_data = data.get("route") or {}
            fills = route_data.get("fills") or []
            route_names = []
            for fill in fills:
                source = fill.get("source", "Unknown")
                if source not in route_names:
                    route_names.append(source)
            is_native = token_in.lower() == NATIVE_TOKEN_ADDRESS
            allowance_target = None
            if not is_native:
                allowance_info = (data.get("issues") or {}).get("allowance") or {}
                allowance_target = allowance_info.get("spender") or data.get("allowanceTarget")
            return {
                "provider": "0x",
                "to": tx_data.get("to"),
                "data": tx_data.get("data"),
                "value": tx_data.get("value", "0"),
                "allowanceTarget": allowance_target,
                "buyAmount": buy_amount,
                "estimatedGas": tx_data.get("gas", "0"),
                "gasPrice": tx_data.get("gasPrice", "0"),
                "route_display": " -> ".join(route_names) if route_names else "0x Matcha Route",
                "price_impact": 0.0,
            }
        except Timeout:
            log.warning("0x quote timeout chain=%s amount=%s", self.chain_name, amount_in_wei)
            return None
        except RequestException as exc:
            log.warning("0x quote request error chain=%s error=%s", self.chain_name, exc)
            return None
        except Exception as exc:
            log.warning("0x quote parse error chain=%s error=%s", self.chain_name, exc)
            return None

    def get_okx_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
        user_address: str,
        slippage_bps: int = 10,
    ) -> dict | None:
        if not (SWAPPER_OKX_API_KEY and SWAPPER_OKX_SECRET_KEY and SWAPPER_OKX_PASSPHRASE):
            log.warning("okx quote skipped: missing OKX swapper config")
            return None
        v6_path = "/api/v6/dex/aggregator/swap"
        url = f"https://www.okx.com{v6_path}"
        params = {
            "chainIndex": self.chain_id_0x,
            "amount": str(int(amount_in_wei)),
            "fromTokenAddress": token_in,
            "toTokenAddress": token_out,
            "slippagePercent": str(float(slippage_bps) / 100),
            "userWalletAddress": Web3.to_checksum_address(user_address),
        }
        timestamp = self._okx_timestamp()
        headers = self._okx_headers(timestamp, "GET", v6_path, params)
        try:
            log.info("okx quote request chain=%s amount=%s", self.chain_name, amount_in_wei)
            response = requests.get(url, params=params, headers=headers, timeout=15)
            data = response.json()
            if data.get("code") != "0" or not data.get("data"):
                log.warning("okx quote failed chain=%s response=%s", self.chain_name, data)
                return None
            swap_data = data["data"][0]
            tx_data = swap_data.get("tx") or {}
            router_result = swap_data.get("routerResult") or {}
            route_names = []
            for dex_info in router_result.get("dexRouterList", []):
                protocol = dex_info.get("dexProtocol") or {}
                name = protocol.get("dexName", "Unknown")
                if name not in route_names:
                    route_names.append(name)
            is_native = token_in.lower() == NATIVE_TOKEN_ADDRESS
            allowance_target = None if is_native else swap_data.get("approveAddress")
            if not allowance_target and not is_native:
                allowance_target = self._okx_approve_address(token_in, amount_in_wei, timestamp, headers)
            if not allowance_target and not is_native:
                allowance_target = swap_data.get("routerAddress") or tx_data.get("to")
            target_to = tx_data.get("to") or swap_data.get("routerAddress")
            target_value = tx_data.get("value") or ("0" if not is_native else str(int(amount_in_wei)))
            return {
                "provider": "OKX",
                "to": target_to,
                "data": tx_data.get("data"),
                "value": target_value,
                "allowanceTarget": allowance_target,
                "buyAmount": str(router_result.get("toTokenAmount") or "0"),
                "estimatedGas": tx_data.get("gas", "0"),
                "gasPrice": tx_data.get("gasPrice", "0"),
                "route_display": " -> ".join(route_names) if route_names else "OKX Route",
                "price_impact": float(router_result.get("priceImpactPercent", 0) or 0),
            }
        except Exception as exc:
            log.warning("okx quote error chain=%s error=%s", self.chain_name, exc)
            return None

    def get_best_swap_route(
        self,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
        user_address: str,
        slippage_bps: int = 50,
    ) -> dict | None:
        routes = self.get_swap_routes(token_in, token_out, amount_in_wei, user_address, slippage_bps)
        if not routes:
            return None
        best_quote = routes[0]
        log.info("best swap route chain=%s provider=%s buy_amount=%s", self.chain_name, best_quote["provider"], best_quote["buyAmount"])
        return best_quote

    def get_swap_routes(
        self,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
        user_address: str,
        slippage_bps: int = 50,
    ) -> list[dict]:
        quotes = []
        kyber_route = self.get_kyber_route(token_in, token_out, amount_in_wei)
        if kyber_route:
            route_summary = kyber_route.get("routeSummary") or {}
            kyber_tx = self.build_kyber_swap_data(route_summary, user_address, slippage_bps)
            if kyber_tx and route_summary:
                quotes.append(self._kyber_quote(kyber_tx, route_summary))

        zero_x_quote = self.get_0x_swap_quote(token_in, token_out, amount_in_wei, user_address, slippage_bps)
        if zero_x_quote:
            quotes.append(zero_x_quote)

        okx_quote = self.get_okx_swap_quote(token_in, token_out, amount_in_wei, user_address, slippage_bps)
        if okx_quote:
            quotes.append(okx_quote)

        valid_quotes = [quote for quote in quotes if self._quote_is_usable(quote)]
        if not valid_quotes:
            return []
        valid_quotes.sort(key=lambda item: int(item["buyAmount"]), reverse=True)
        log.info(
            "swap routes found chain=%s count=%s providers=%s",
            self.chain_name,
            len(valid_quotes),
            ",".join(str(quote.get("provider")) for quote in valid_quotes),
        )
        return valid_quotes

    def _kyber_quote(self, kyber_tx: dict, route_summary: dict) -> dict:
        impact = 0.0
        try:
            amount_in_usd = float(route_summary.get("amountInUsd", 0) or 0)
            amount_out_usd = float(route_summary.get("amountOutUsd", 0) or 0)
            if amount_in_usd > 0:
                impact = ((amount_in_usd - amount_out_usd) / amount_in_usd) * 100
        except Exception:
            impact = 0.0
        route_display = "KyberSwap"
        try:
            first_route = route_summary.get("route", [[]])[0]
            names = [hop.get("exchange") for hop in first_route if hop.get("exchange")]
            if names:
                route_display = "Kyber -> " + " -> ".join(names)
        except Exception:
            pass
        router_address = kyber_tx.get("routerAddress") or kyber_tx.get("to")
        return {
            "provider": "KyberSwap",
            "buyAmount": str(route_summary.get("amountOut") or "0"),
            "data": kyber_tx.get("data"),
            "to": router_address,
            "value": kyber_tx.get("value", "0"),
            "allowanceTarget": router_address,
            "estimatedGas": kyber_tx.get("gas", "0"),
            "gasPrice": kyber_tx.get("gasPrice", "0"),
            "price_impact": impact,
            "route_display": route_display,
        }

    def _okx_approve_address(self, token_in: str, amount_in_wei: int, timestamp: str, base_headers: dict) -> str | None:
        try:
            path = "/api/v6/dex/aggregator/approve-transaction"
            url = f"https://www.okx.com{path}"
            params = {
                "chainIndex": self.chain_id_0x,
                "tokenContractAddress": token_in,
                "approveAmount": str(int(amount_in_wei)),
            }
            headers = dict(base_headers)
            headers.update(self._okx_headers(timestamp, "GET", path, params))
            response = requests.get(url, params=params, headers=headers, timeout=5)
            data = response.json()
            if data.get("code") == "0" and data.get("data"):
                return data["data"][0].get("dexContractAddress")
        except Exception as exc:
            log.warning("okx approve address lookup failed chain=%s error=%s", self.chain_name, exc)
        return None

    def _okx_headers(self, timestamp: str, method: str, path: str, params: dict) -> dict:
        query_string = urlencode(params)
        request_path = f"{path}?{query_string}"
        message = timestamp + method.upper() + request_path
        digest = hmac.new(
            bytes(SWAPPER_OKX_SECRET_KEY, encoding="utf-8"),
            bytes(message, encoding="utf-8"),
            digestmod="sha256",
        ).digest()
        signature = base64.b64encode(digest).decode("utf-8")
        return {
            "OK-ACCESS-KEY": SWAPPER_OKX_API_KEY,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": SWAPPER_OKX_PASSPHRASE,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @staticmethod
    def _okx_timestamp() -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    @staticmethod
    def _quote_is_usable(quote: dict) -> bool:
        try:
            return bool(quote.get("to") and quote.get("data") and int(quote.get("buyAmount") or 0) > 0)
        except (TypeError, ValueError):
            return False
