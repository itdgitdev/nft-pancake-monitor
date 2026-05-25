from __future__ import annotations

import logging
import os
import time
from urllib.parse import urlparse
from typing import Iterable

from web3 import Web3
from web3.exceptions import TimeExhausted, TransactionNotFound
from w3multicall.multicall import W3Multicall

from .abi import ERC20_ABI, INCREASE_LIQUIDITY_TOPIC, MASTERCHEF_V3_ABI, MAX_UINT128, MAX_UINT256, NPM_ABI, V3_POOL_ABI
from .models import PoolConfig, PositionSnapshot, Slot0, TokenBalance, TxResult
from .reward import token_price_usd
from .tx_executor import TxExecutor
from .v3_math import amounts_for_liquidity

log = logging.getLogger("configured_pool_rebalancer")


try:
    from latest_farms.config import MASTERCHEF_V3_ADDRESSES, NPM_ADDRESSES
except ImportError:  # pragma: no cover
    from config import MASTERCHEF_V3_ADDRESSES, NPM_ADDRESSES


class DexAdapter:
    def __init__(self, w3: Web3, pool: PoolConfig, executor: TxExecutor):
        self.w3 = w3
        self.pool = pool
        self.executor = executor

    def read_slot0(self) -> Slot0:
        raise NotImplementedError

    def discover_pool_metadata(self) -> PoolConfig:
        raise NotImplementedError


class PancakeV3MasterChefAdapter(DexAdapter):
    def __init__(self, w3: Web3, pool: PoolConfig, executor: TxExecutor):
        super().__init__(w3, pool, executor)
        self.npm_address = Web3.to_checksum_address(pool.npm_address or NPM_ADDRESSES[pool.chain])
        self.masterchef_address = Web3.to_checksum_address(
            pool.staking_address or MASTERCHEF_V3_ADDRESSES[pool.chain]
        )
        self.npm = w3.eth.contract(address=self.npm_address, abi=NPM_ABI)
        self.masterchef = w3.eth.contract(address=self.masterchef_address, abi=MASTERCHEF_V3_ABI)

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
            for i, data in enumerate(results):
                if not data:
                    continue
                token_id = batch[i]
                liquidity, _, tick_lower, tick_upper, _, _, user, pid, _ = data
                if int(liquidity) <= 0:
                    continue
                if self.pool.pid is not None and int(pid) != int(self.pool.pid):
                    continue
                npm_pos = self.npm.functions.positions(token_id).call()
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
        calls = []
        if position.liquidity > 0:
            expected0, expected1 = amounts_for_liquidity(
                position.liquidity, slot0.sqrt_price_x96, position.tick_lower, position.tick_upper
            )
            min0 = self._apply_slippage(expected0)
            min1 = self._apply_slippage(expected1)
            calls.append(
                self.masterchef.encode_abi(
                    "decreaseLiquidity",
                    args=[(position.token_id, position.liquidity, min0, min1, deadline)],
                )
            )
        calls.append(
            self.masterchef.encode_abi(
                "collect",
                args=[(position.token_id, self.pool.bot_wallet, MAX_UINT128, MAX_UINT128)],
            )
        )
        if position.is_staked:
            calls.append(self.masterchef.encode_abi("withdraw", args=[position.token_id, self.pool.bot_wallet]))
        return self.executor.send(self.masterchef.functions.multicall(calls), "withdraw", gas=1_100_000)

    def approve_if_needed(self, token: str, spender: str, amount: int) -> TxResult | None:
        if amount <= 0:
            return None
        token_contract = self._erc20(token)
        wallet = Web3.to_checksum_address(self.pool.bot_wallet)
        spender_cs = Web3.to_checksum_address(spender)
        allowance = int(token_contract.functions.allowance(wallet, spender_cs).call())
        if allowance >= amount:
            return None
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
        quote = swapper.get_best_swap_route(
            token_in,
            token_out,
            amount_in,
            Web3.to_checksum_address(self.pool.bot_wallet),
            self.pool.slippage_bps,
        )
        if not quote:
            return None
        if float(quote.get("price_impact", 0)) > self.pool.max_swap_price_impact_pct:
            return None
        dust_result = self._dust_output_result(token_out, quote)
        if dust_result:
            return dust_result
        allowance_target = quote.get("allowanceTarget")
        if allowance_target:
            self.approve_if_needed(token_in, allowance_target, amount_in)
        tx = {
            "to": Web3.to_checksum_address(quote["to"]),
            "data": quote["data"],
            "value": int(quote.get("value", 0)),
        }
        metadata = {
            "label": "swap",
            "token_in": Web3.to_checksum_address(token_in),
            "token_out": Web3.to_checksum_address(token_out),
            "amount_in": str(int(amount_in)),
            "quote_buy_amount": str(int(quote.get("buyAmount") or 0)),
            "price_impact": quote.get("price_impact"),
        }
        # Web3 fallback helpers cannot carry data reliably across versions, so
        # build the raw transaction through the account API.
        return self._send_raw_swap(tx, metadata=metadata)

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
        result = self.executor.send(self.npm.functions.mint(params), "mint", gas=700000)
        if result.dry_run:
            return result, None
        receipt = self._mint_receipt_from_result(result)
        if receipt is None:
            return result, None
        if int(receipt.get("status", 0)) != 1:
            result.metadata["error"] = "mint transaction reverted"
            return result, None
        token_id = self._new_token_id_from_mint_receipt(receipt)
        if token_id is None:
            return result, None
        if not self._minted_position_matches_plan(token_id, plan):
            result.metadata["error"] = "mint receipt token does not match requested pool/range"
            return result, None
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
            result.metadata["receipt_block"] = int(receipt.get("blockNumber") or 0)
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
        for ev in receipt.get("logs", []):
            topics = ev.get("topics") or []
            if len(topics) < 2:
                continue
            if self._hex_value(topics[0]).lower() != topic:
                continue
            try:
                return int(self._hex_value(topics[1]), 16)
            except ValueError:
                return None
        return None

    def _minted_position_matches_plan(self, token_id: int, plan) -> bool:
        try:
            position = self.read_npm_position(int(token_id), owner=self.pool.bot_wallet)
        except Exception as exc:
            log.warning("could not validate recovered mint position pool=%s tokenId=%s: %s", self.pool.name, token_id, exc)
            return False
        owner = position.owner.lower()
        if owner not in {self.pool.bot_wallet.lower(), self.masterchef_address.lower()}:
            log.warning(
                "recovered mint token owner mismatch pool=%s tokenId=%s owner=%s",
                self.pool.name,
                token_id,
                position.owner,
            )
            return False
        if not self._matches_pool(position.token0, position.token1, position.fee):
            log.warning("recovered mint token pool mismatch pool=%s tokenId=%s", self.pool.name, token_id)
            return False
        if int(position.tick_lower) != int(plan.new_tick_lower) or int(position.tick_upper) != int(plan.new_tick_upper):
            log.warning(
                "recovered mint token range mismatch pool=%s tokenId=%s expected=(%s,%s) actual=(%s,%s)",
                self.pool.name,
                token_id,
                plan.new_tick_lower,
                plan.new_tick_upper,
                position.tick_lower,
                position.tick_upper,
            )
            return False
        return int(position.liquidity) > 0

    def stake(self, token_id: int) -> TxResult:
        data = self.w3.codec.encode(["uint256"], [int(self.pool.pid or 0)])
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
        gas_limit = 600000
        try:
            gas_limit = max(
                120000,
                int(
                    self.w3.eth.estimate_gas(
                        {
                            "from": wallet,
                            "to": tx_payload["to"],
                            "data": tx_payload["data"],
                            "value": tx_payload["value"],
                        }
                    )
                    * 1.3
                ),
            )
        except Exception:
            pass
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
            "to": tx["to"],
            "value": str(tx["value"]),
            "data_length": len(str(tx["data"] or "")),
            "max_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei")),
            "max_priority_fee_per_gas_gwei": float(
                Web3.from_wei(gas_params["maxPriorityFeePerGas"], "gwei")
            ),
        }
        metadata = {**metadata, **safe_tx_metadata}
        private_key = os.getenv(self.pool.private_key_env)
        if not private_key:
            raise RuntimeError(f"missing private key env {self.pool.private_key_env}")
        signed = self.w3.eth.account.sign_transaction(tx, private_key)
        log.info(
            "broadcast swap tx pool=%s chain=%s token_in=%s token_out=%s amount_in=%s "
            "to=%s value=%s nonce=%s gas=%s max_fee_gwei=%.9f priority_fee_gwei=%.9f data_length=%s",
            self.pool.name,
            self.pool.chain,
            metadata.get("token_in"),
            metadata.get("token_out"),
            metadata.get("amount_in"),
            tx["to"],
            tx["value"],
            tx["nonce"],
            gas_limit,
            metadata["max_fee_per_gas_gwei"],
            metadata["max_priority_fee_per_gas_gwei"],
            metadata["data_length"],
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
                break
            except Exception as exc:
                broadcast_errors.append(f"{rpc_label}: {exc}")
                log.warning(
                    "swap tx broadcast failed pool=%s token_in=%s token_out=%s amount_in=%s "
                    "rpc=%s error=%s",
                    self.pool.name,
                    metadata.get("token_in"),
                    metadata.get("token_out"),
                    metadata.get("amount_in"),
                    rpc_label,
                    exc,
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
            return TxResult(
                tx_hash=tx_hash.hex(),
                status="PENDING",
                metadata={
                    **metadata,
                    "error": str(exc),
                },
            )
        if receipt["status"] != 1:
            return TxResult(
                tx_hash=tx_hash.hex(),
                status="FAILED",
                gas_used=int(receipt.get("gasUsed") or 0),
                metadata={**metadata, "error": "swap transaction reverted"},
            )
        effective_gas_price = int(receipt.get("effectiveGasPrice") or gas_params["maxFeePerGas"])
        return TxResult(
            tx_hash=tx_hash.hex(),
            gas_used=int(receipt["gasUsed"]),
            gas_price_gwei=float(Web3.from_wei(effective_gas_price, "gwei")),
            metadata={
                **metadata,
                "receipt_block": int(receipt["blockNumber"]),
                "gas_limit": gas_limit,
                "max_fee_per_gas_gwei": float(Web3.from_wei(gas_params["maxFeePerGas"], "gwei")),
                "max_priority_fee_per_gas_gwei": float(
                    Web3.from_wei(gas_params["maxPriorityFeePerGas"], "gwei")
                ),
            },
        )

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


class AerodromeGaugeAdapter(DexAdapter):
    def read_slot0(self) -> Slot0:
        raise NotImplementedError("aerodrome_gauge adapter is reserved for the next integration phase")

    def discover_pool_metadata(self) -> PoolConfig:
        raise NotImplementedError("aerodrome_gauge adapter is reserved for the next integration phase")
