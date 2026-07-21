from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from urllib.parse import urlparse

from web3 import Web3
from w3multicall.multicall import W3Multicall

from .adapter import DexAdapter
from .models import DexType, PoolConfig, PositionSnapshot
from .position_cache_snapshot import fetch_position_cache_snapshot


log = logging.getLogger("configured_pool_rebalancer")


DEPOSIT_TOPIC = "0x" + Web3.keccak(text="Deposit(address,uint256,uint256,uint256,int24,int24)").hex()
WITHDRAW_TOPIC = "0x" + Web3.keccak(text="Withdraw(address,address,uint256,uint256)").hex()
POOL_CREATED_TOPIC = "0x" + Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)").hex()
AERODROME_POSITION_CACHE_SOURCE = "aerodrome_positions_cache"
TRANSFER_TOPIC = Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)"))


class LogRangeTooLarge(RuntimeError):
    pass


try:
    from latest_farms.config import (
        FACTORY_ADDRESSES,
        FACTORY_DEPLOYED_BLOCK,
        MASTERCHEF_DEPLOYED_BLOCK,
        RPC_BACKUP_LIST,
        RPC_URLS_2,
    )
except ImportError:  # pragma: no cover
    from config import (
        FACTORY_ADDRESSES,
        FACTORY_DEPLOYED_BLOCK,
        MASTERCHEF_DEPLOYED_BLOCK,
        RPC_BACKUP_LIST,
        RPC_URLS_2,
    )


class PositionIndex:
    def __init__(
        self,
        cache_dir: str,
        legacy_cache_dir: str | None = None,
        use_legacy_cache: bool = True,
        use_db_cache: bool = True,
        db_cache_source: str = "positions_cache",
    ):
        self.cache_dir = Path(cache_dir)
        self.legacy_cache_dir = Path(legacy_cache_dir) if legacy_cache_dir else None
        self.use_legacy_cache = use_legacy_cache
        self.use_db_cache = use_db_cache
        self.db_cache_source = db_cache_source
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def refresh(self, w3: Web3, pool: PoolConfig, adapter: DexAdapter) -> dict[int, PositionSnapshot]:
        started_at = time.monotonic()
        cached = self._load(pool)
        token_ids = set(cached.get("token_ids", []))
        cached_staked_ids = {
            int(token_id)
            for token_id, info in (cached.get("positions") or {}).items()
            if isinstance(info, dict) and bool(info.get("is_staked"))
        }
        db_snapshot = self._load_db_pool_candidates(pool, cached)
        token_ids.update(db_snapshot["token_ids"])
        legacy = self._load_legacy_pool_candidates(pool)
        token_ids.update(legacy["token_ids"])
        token_ids.update(pool.seed_token_ids)

        latest_block = int(w3.eth.get_block("latest")["number"])
        sync = self._resolve_sync_window(w3, cached, db_snapshot, legacy, pool, latest_block)
        from_block = int(sync["from_block"])
        swept_logs = False
        stake_candidates = set(cached_staked_ids)

        if from_block <= latest_block:
            event_contract = self._stake_event_contract_address(adapter)
            event_topics = self._stake_event_topics(adapter)
            if event_contract and event_topics:
                staked, unstaked = self._sweep_stake_logs(
                    w3,
                    pool,
                    adapter,
                    event_contract,
                    event_topics,
                    from_block,
                    latest_block,
                    pool.log_chunk_size,
                )
                token_ids.update(staked)
                token_ids.update(unstaked)
                stake_candidates.update(staked)
                stake_candidates.difference_update(unstaked)
                swept_logs = True

        enumerable_ids, enumerable_complete = self._enumerate_owned_token_ids(w3, pool)
        token_ids.update(enumerable_ids)
        transfer_ids: set[int] = set()
        transfer_complete = False
        transfer_checkpoint = int(cached.get("npm_transfer_last_synced_block") or 0)
        if not enumerable_complete:
            transfer_ids, transfer_complete = self._sweep_npm_transfer_logs(
                w3,
                pool,
                transfer_checkpoint,
                latest_block,
            )
            token_ids.update(transfer_ids)

        # Any candidate may have moved into a farm/gauge since the last checkpoint.
        # On-chain membership is authoritative, so verify the complete candidate set.
        stake_candidates.update(token_ids)
        positions = adapter.read_staked_positions(stake_candidates)
        managed = {wallet.lower() for wallet in pool.managed_wallets}
        managed.add(pool.bot_wallet.lower())
        filtered: dict[int, PositionSnapshot] = {
            token_id: pos
            for token_id, pos in positions.items()
            if pos.owner.lower() in managed
        }

        owned = self._batch_owned_token_ids(w3, pool, token_ids, managed)
        if hasattr(adapter, "read_npm_positions"):
            npm_positions = adapter.read_npm_positions(owned, owners=owned)
        else:
            npm_positions = {}
            for token_id, owner in owned.items():
                try:
                    npm_positions[token_id] = adapter.read_npm_position(token_id, owner=owner)
                except Exception:
                    continue
        for token_id, owner in owned.items():
            if token_id in filtered:
                continue
            position = npm_positions.get(token_id)
            if position is None:
                continue
            if int(position.liquidity) <= 0 or not self._matches_pool(pool, position):
                continue
            position.owner = owner
            position.is_staked = False
            position.last_updated_block = latest_block
            filtered[token_id] = position

        for position in filtered.values():
            position.last_updated_block = latest_block

        next_transfer_checkpoint = latest_block if enumerable_complete or transfer_complete else transfer_checkpoint
        self._save(
            pool,
            filtered,
            latest_block,
            legacy,
            db_snapshot,
            sync={
                **sync,
                "swept_logs": swept_logs,
                "npm_enumerable_complete": enumerable_complete,
                "npm_transfer_swept": transfer_complete,
                "npm_transfer_last_synced_block": next_transfer_checkpoint,
            },
        )
        log.info(
            "position discovery pool=%s cache=%s db=%s legacy=%s seed=%s enumerable=%s "
            "transfers=%s owner_checked=%s staked=%s unstaked=%s anchor_block=%s duration_ms=%s",
            pool.name,
            len(cached.get("token_ids", [])),
            len(db_snapshot.get("token_ids", [])),
            len(legacy.get("token_ids", [])),
            len(pool.seed_token_ids),
            len(enumerable_ids),
            len(transfer_ids),
            len(token_ids),
            sum(1 for value in filtered.values() if value.is_staked),
            sum(1 for value in filtered.values() if not value.is_staked),
            latest_block,
            int((time.monotonic() - started_at) * 1000),
        )
        return filtered

    def _enumerate_owned_token_ids(self, w3: Web3, pool: PoolConfig) -> tuple[set[int], bool]:
        if not pool.npm_address:
            return set(), False
        try:
            npm = w3.eth.contract(
                address=Web3.to_checksum_address(pool.npm_address),
                abi=[
                    {
                        "inputs": [{"name": "owner", "type": "address"}],
                        "name": "balanceOf",
                        "outputs": [{"name": "", "type": "uint256"}],
                        "stateMutability": "view",
                        "type": "function",
                    }
                ],
            )
            balance = int(npm.functions.balanceOf(pool.bot_wallet).call())
            token_ids: set[int] = set()
            for start in range(0, balance, 150):
                multicall = W3Multicall(w3)
                for index in range(start, min(balance, start + 150)):
                    multicall.add(
                        W3Multicall.Call(
                            pool.npm_address,
                            "tokenOfOwnerByIndex(address,uint256)(uint256)",
                            (pool.bot_wallet, index),
                        )
                    )
                token_ids.update(int(value) for value in multicall.call() if value is not None)
            return token_ids, True
        except Exception as exc:
            log.warning("NPM enumerable discovery unavailable pool=%s: %s", pool.name, exc)
            return set(), False

    def _batch_owned_token_ids(
        self,
        w3: Web3,
        pool: PoolConfig,
        token_ids: set[int],
        managed_wallets: set[str],
    ) -> dict[int, str]:
        if not pool.npm_address:
            return {}

        def resolve_batch(batch: list[int]) -> dict[int, str]:
            if not batch:
                return {}
            multicall = W3Multicall(w3)
            for token_id in batch:
                multicall.add(
                    W3Multicall.Call(
                        pool.npm_address,
                        "ownerOf(uint256)(address)",
                        token_id,
                    )
                )
            try:
                owners = multicall.call()
            except Exception as exc:
                if self._is_multicall_subcall_revert(exc):
                    if len(batch) == 1:
                        log.warning(
                            "NPM owner candidate skipped pool=%s tokenId=%s reason=ownerOf reverted",
                            pool.name,
                            batch[0],
                        )
                        return {}
                    middle = len(batch) // 2
                    log.info(
                        "NPM owner batch split pool=%s candidates=%s left=%s right=%s",
                        pool.name,
                        len(batch),
                        middle,
                        len(batch) - middle,
                    )
                    return {
                        **resolve_batch(batch[:middle]),
                        **resolve_batch(batch[middle:]),
                    }
                log.warning("NPM owner batch unavailable pool=%s candidates=%s: %s", pool.name, len(batch), exc)
                return {}
            resolved: dict[int, str] = {}
            for token_id, owner in zip(batch, owners):
                try:
                    checksummed = Web3.to_checksum_address(owner)
                except (TypeError, ValueError):
                    continue
                if checksummed.lower() in managed_wallets:
                    resolved[token_id] = checksummed
            return resolved

        owned: dict[int, str] = {}
        ordered = sorted({int(token_id) for token_id in token_ids})
        for start in range(0, len(ordered), 150):
            owned.update(resolve_batch(ordered[start : start + 150]))
        return owned

    @staticmethod
    def _is_multicall_subcall_revert(exc: Exception) -> bool:
        message = str(exc).lower()
        return "multicall3: call failed" in message or "execution reverted" in message

    def _sweep_npm_transfer_logs(
        self,
        w3: Web3,
        pool: PoolConfig,
        checkpoint: int,
        latest_block: int,
    ) -> tuple[set[int], bool]:
        configured_start = pool.bootstrap_start_block if pool.bootstrap_start_block is not None else pool.start_block
        if not configured_start and not checkpoint:
            return set(), False
        from_block = max(int(configured_start or 0), int(checkpoint) + 1)
        if from_block > latest_block:
            return set(), True
        wallet_topic = "0x" + pool.bot_wallet.lower()[2:].rjust(64, "0")
        token_ids: set[int] = set()
        complete = True
        for start in range(from_block, latest_block + 1, max(1, pool.log_chunk_size)):
            end = min(latest_block, start + max(1, pool.log_chunk_size) - 1)
            for topics in ([TRANSFER_TOPIC, None, wallet_topic], [TRANSFER_TOPIC, wallet_topic]):
                try:
                    events = w3.eth.get_logs(
                        {
                            "fromBlock": start,
                            "toBlock": end,
                            "address": pool.npm_address,
                            "topics": topics,
                        }
                    )
                except Exception:
                    complete = False
                    continue
                for event in events:
                    topics_value = event.get("topics") or []
                    if len(topics_value) >= 4:
                        token_ids.add(int.from_bytes(topics_value[3], "big"))
        return token_ids, complete

    @staticmethod
    def _matches_pool(pool: PoolConfig, position: PositionSnapshot) -> bool:
        if pool.token0_address and position.token0.lower() != pool.token0_address.lower():
            return False
        if pool.token1_address and position.token1.lower() != pool.token1_address.lower():
            return False
        expected = (
            pool.tick_spacing
            if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}
            else pool.fee
        )
        return expected is None or int(position.fee) == int(expected)

    def _resolve_sync_window(
        self,
        w3: Web3,
        cached: dict,
        db_snapshot: dict,
        legacy: dict,
        pool: PoolConfig,
        latest_block: int,
    ) -> dict:
        cached_block = int(cached.get("last_synced_block") or 0)
        db_block = int(db_snapshot.get("last_synced_block") or 0)
        db_token_count = len(db_snapshot.get("token_ids", []))
        db_pid_bootstrapped = bool(db_snapshot.get("pid_bootstrapped", False))
        legacy_block = int(legacy.get("last_synced_block") or 0)
        legacy_token_count = len(legacy.get("token_ids", []))
        if db_block > cached_block and (db_token_count or db_pid_bootstrapped):
            source = "db_position_cache"
            from_block = db_block + 1
        elif cached_block:
            source = "module_cache"
            from_block = cached_block + 1
        elif legacy_block and legacy_token_count:
            source = "legacy_cache"
            from_block = legacy_block + 1
        elif pool.bootstrap_start_block is not None:
            source = "bootstrap_start_block"
            from_block = int(pool.bootstrap_start_block)
        elif pool.start_block:
            source = "start_block"
            from_block = int(pool.start_block)
        elif pool.auto_bootstrap_start_block:
            auto_block = self._resolve_auto_bootstrap_start_block(w3, pool, latest_block)
            if auto_block is not None:
                source = "auto_pool_created_block"
                from_block = auto_block
            else:
                source = "skip_historical"
                from_block = latest_block + 1
        else:
            source = "skip_historical"
            from_block = latest_block + 1

        if pool.start_block and from_block < pool.start_block:
            from_block = int(pool.start_block)
            source = f"{source}+start_block_floor"

        return {
            "source": source,
            "from_block": from_block,
            "latest_block": latest_block,
            "cached_block": cached_block,
            "db_block": db_block,
            "db_token_count": db_token_count,
            "db_pid_bootstrapped": db_pid_bootstrapped,
            "legacy_block": legacy_block,
            "legacy_token_count": legacy_token_count,
            "auto_bootstrap_start_block": pool.auto_bootstrap_start_block,
            "historical_skipped": source == "skip_historical",
        }

    def _resolve_auto_bootstrap_start_block(
        self,
        w3: Web3,
        pool: PoolConfig,
        latest_block: int,
    ) -> int | None:
        if not pool.token0_address or not pool.token1_address or pool.fee is None:
            return None
        factory = FACTORY_ADDRESSES.get(pool.chain.upper())
        if not factory:
            return None

        from_block = int(
            FACTORY_DEPLOYED_BLOCK.get(
                pool.chain.upper(),
                MASTERCHEF_DEPLOYED_BLOCK.get(pool.chain.upper(), 0),
            )
            or 0
        )
        try:
            logs = w3.eth.get_logs(
                {
                    "fromBlock": from_block,
                    "toBlock": latest_block,
                    "address": Web3.to_checksum_address(factory),
                    "topics": [
                        POOL_CREATED_TOPIC,
                        self._topic_address(pool.token0_address),
                        self._topic_address(pool.token1_address),
                        self._topic_uint(int(pool.fee)),
                    ],
                }
            )
        except Exception:
            return None
        if not logs:
            return None
        return min(int(log["blockNumber"]) for log in logs)

    def _topic_address(self, address: str) -> str:
        return "0x" + Web3.to_checksum_address(address).lower()[2:].rjust(64, "0")

    def _topic_uint(self, value: int) -> str:
        return "0x" + hex(int(value))[2:].rjust(64, "0")

    def _load_legacy_pool_candidates(self, pool: PoolConfig) -> dict:
        if not self.use_legacy_cache or not self.legacy_cache_dir:
            return {"token_ids": set(), "last_synced_block": 0, "source": None}

        path = self.legacy_cache_dir / f"positions_cache_{pool.chain}.json"
        if not path.exists():
            return {"token_ids": set(), "last_synced_block": 0, "source": str(path)}

        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        candidates: set[int] = set()
        positions = data.get("positions", {})
        for raw_token_id, info in positions.items():
            if not isinstance(info, dict):
                continue
            if pool.pid is not None:
                try:
                    if int(info.get("pid", -1)) != int(pool.pid):
                        continue
                except (TypeError, ValueError):
                    continue
            elif not self._candidate_info_matches_pool(info, pool):
                continue
            try:
                candidates.add(int(raw_token_id))
            except (TypeError, ValueError):
                continue

        return {
            "token_ids": candidates,
            "last_synced_block": int(data.get("last_synced_block") or 0),
            "source": str(path),
        }

    def _load_db_pool_candidates(self, pool: PoolConfig, cached: dict) -> dict:
        if not self.use_db_cache:
            return {"token_ids": set(), "last_synced_block": 0, "source": None, "pid_bootstrapped": False}
        if int(cached.get("last_synced_block") or 0) and pool.dex_type not in {
            DexType.AERODROME_V3,
            DexType.AERODROME_GAUGE,
        }:
            return {"token_ids": set(), "last_synced_block": 0, "source": None, "pid_bootstrapped": False}
        source = self._db_cache_source_for_pool(pool)
        try:
            snapshot = fetch_position_cache_snapshot(pool.chain, source)
        except Exception as exc:
            log.warning("could not load DB position cache snapshot chain=%s source=%s: %s", pool.chain, source, exc)
            return {"token_ids": set(), "last_synced_block": 0, "source": "db:error", "pid_bootstrapped": False}
        if not snapshot:
            return {"token_ids": set(), "last_synced_block": 0, "source": "db:missing", "pid_bootstrapped": False}
        data = snapshot.get("snapshot") or {}
        candidates, pid_bootstrapped = self._candidate_token_ids_from_snapshot(data, pool)
        return {
            "token_ids": candidates,
            "last_synced_block": int(snapshot.get("last_synced_block") or data.get("last_synced_block") or 0),
            "source": f"db:{source}",
            "pid_bootstrapped": pid_bootstrapped,
            "position_count": int(snapshot.get("position_count") or 0),
        }

    def _db_cache_source_for_pool(self, pool: PoolConfig) -> str:
        if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}:
            return AERODROME_POSITION_CACHE_SOURCE
        return self.db_cache_source

    def _candidate_token_ids_from_snapshot(self, data: dict, pool: PoolConfig) -> tuple[set[int], bool]:
        candidates: set[int] = set()
        positions = data.get("positions", {})
        if not isinstance(positions, dict):
            return candidates, False
        pid_bootstrapped = False
        if pool.pid is not None:
            try:
                pid_bootstrapped = int(pool.pid) in {int(pid) for pid in data.get("bootstrapped_pids", [])}
            except (TypeError, ValueError):
                pid_bootstrapped = False
        else:
            try:
                pid_bootstrapped = pool.pool_address.lower() in {
                    str(pool_address).lower()
                    for pool_address in data.get("bootstrapped_pools", [])
                }
            except (TypeError, ValueError):
                pid_bootstrapped = False
        for raw_token_id, info in positions.items():
            if not isinstance(info, dict):
                continue
            if pool.pid is not None:
                try:
                    if int(info.get("pid", -1)) != int(pool.pid):
                        continue
                except (TypeError, ValueError):
                    continue
            elif not self._candidate_info_matches_pool(info, pool):
                continue
            try:
                candidates.add(int(raw_token_id))
            except (TypeError, ValueError):
                continue
        return candidates, pid_bootstrapped

    def _candidate_info_matches_pool(self, info: dict, pool: PoolConfig) -> bool:
        pool_value = info.get("pool_address") or info.get("pool") or info.get("v3_pool")
        if pool_value and str(pool_value).lower() != str(pool.pool_address).lower():
            return False
        staking_value = info.get("staking_address") or info.get("gauge_address") or info.get("masterchef_address")
        if staking_value and pool.staking_address and str(staking_value).lower() != str(pool.staking_address).lower():
            return False
        if pool.pid is None and not pool_value and not staking_value:
            return False
        return True

    def _stake_event_contract_address(self, adapter: DexAdapter) -> str | None:
        if hasattr(adapter, "stake_event_contract_address"):
            try:
                return adapter.stake_event_contract_address()
            except Exception:
                return None
        return getattr(adapter, "masterchef_address", None)

    def _stake_event_topics(self, adapter: DexAdapter) -> list[str]:
        if hasattr(adapter, "stake_event_topics"):
            try:
                topics = adapter.stake_event_topics()
                if topics:
                    return topics
            except Exception:
                pass
        return [DEPOSIT_TOPIC, WITHDRAW_TOPIC]

    def _parse_stake_event(self, adapter: DexAdapter, event) -> tuple[str, int] | None:
        if hasattr(adapter, "parse_stake_event"):
            parsed = adapter.parse_stake_event(event)
            if parsed:
                return parsed
        topics = event.get("topics") or []
        if len(topics) < 4:
            return None
        topic0 = Web3.to_hex(topics[0]).lower()
        token_id = int.from_bytes(topics[3], "big")
        if topic0 == DEPOSIT_TOPIC.lower():
            return "stake", token_id
        if topic0 == WITHDRAW_TOPIC.lower():
            return "unstake", token_id
        return None

    def _sweep_stake_logs(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: DexAdapter,
        event_contract_address: str,
        event_topics: list[str],
        from_block: int,
        to_block: int,
        chunk_size: int,
    ) -> tuple[set[int], set[int]]:
        staked: set[int] = set()
        unstaked: set[int] = set()
        rpc_sources = self._log_rpc_sources(w3, pool)
        for start in range(from_block, to_block + 1, chunk_size):
            end = min(start + chunk_size - 1, to_block)
            query = {
                "fromBlock": start,
                "toBlock": end,
                "address": Web3.to_checksum_address(event_contract_address),
                "topics": [event_topics],
            }
            logs = self._get_logs_with_adaptive_range(rpc_sources, pool, start, end, query)
            for event in logs:
                parsed = self._parse_stake_event(adapter, event)
                if not parsed:
                    continue
                action, token_id = parsed
                if action == "stake":
                    staked.add(token_id)
                    unstaked.discard(token_id)
                elif action == "unstake":
                    unstaked.add(token_id)
                    staked.discard(token_id)
        return staked, unstaked

    def _sweep_masterchef_logs(
        self,
        w3: Web3,
        pool: PoolConfig,
        masterchef_address: str,
        from_block: int,
        to_block: int,
        chunk_size: int,
    ) -> tuple[set[int], set[int]]:
        class _MasterChefEventAdapter:
            def stake_event_topics(self):
                return [DEPOSIT_TOPIC, WITHDRAW_TOPIC]

            def parse_stake_event(self, event):
                return None

        return self._sweep_stake_logs(
            w3,
            pool,
            _MasterChefEventAdapter(),
            masterchef_address,
            [DEPOSIT_TOPIC, WITHDRAW_TOPIC],
            from_block,
            to_block,
            chunk_size,
        )

    def _get_logs_with_adaptive_range(
        self,
        rpc_sources: list[tuple[str, Web3]],
        pool: PoolConfig,
        from_block: int,
        to_block: int,
        query: dict,
    ) -> list:
        logs: list = []
        ranges = [(from_block, to_block)]
        while ranges:
            start, end = ranges.pop()
            range_query = {**query, "fromBlock": start, "toBlock": end}
            try:
                logs.extend(self._get_logs_with_rpc_fallback(rpc_sources, pool, start, end, range_query))
            except LogRangeTooLarge:
                if start >= end:
                    raise
                mid = (start + end) // 2
                log.info(
                    "stake log sweep range split pool=%s chain=%s from_block=%s mid_block=%s to_block=%s",
                    pool.name,
                    pool.chain,
                    start,
                    mid,
                    end,
                )
                ranges.append((mid + 1, end))
                ranges.append((start, mid))
        return logs

    def _get_logs_with_rpc_fallback(
        self,
        rpc_sources: list[tuple[str, Web3]],
        pool: PoolConfig,
        from_block: int,
        to_block: int,
        query: dict,
    ) -> list:
        last_error: Exception | None = None
        for rpc_label, candidate_w3 in rpc_sources:
            attempts = 2 if rpc_label == "primary" else 1
            for attempt in range(1, attempts + 1):
                try:
                    logs = candidate_w3.eth.get_logs(query)
                    if rpc_label != "primary" or attempt > 1:
                        log.info(
                            "masterchef log sweep recovered pool=%s chain=%s rpc=%s attempt=%s from_block=%s to_block=%s logs=%s",
                            pool.name,
                            pool.chain,
                            rpc_label,
                            attempt,
                            from_block,
                            to_block,
                            len(logs),
                        )
                    return logs
                except Exception as exc:
                    last_error = exc
                    if self._is_log_range_too_large_error(exc):
                        log.warning(
                            "stake log sweep range too large pool=%s chain=%s rpc=%s attempt=%s/%s from_block=%s to_block=%s error=%s",
                            pool.name,
                            pool.chain,
                            rpc_label,
                            attempt,
                            attempts,
                            from_block,
                            to_block,
                            exc,
                        )
                        raise LogRangeTooLarge(str(exc)) from exc
                    log.warning(
                        "masterchef log sweep failed pool=%s chain=%s rpc=%s attempt=%s/%s from_block=%s to_block=%s error=%s",
                        pool.name,
                        pool.chain,
                        rpc_label,
                        attempt,
                        attempts,
                        from_block,
                        to_block,
                        exc,
                    )
                    if attempt < attempts:
                        time.sleep(min(2.0, 0.5 * attempt))
        log.error(
            "masterchef log sweep failed on all rpc attempts pool=%s chain=%s from_block=%s to_block=%s attempts=%s last_error=%s",
            pool.name,
            pool.chain,
            from_block,
            to_block,
            len(rpc_sources),
            last_error,
        )
        if last_error:
            raise last_error
        raise RuntimeError("masterchef log sweep failed without rpc attempts")

    @staticmethod
    def _is_log_range_too_large_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return (
            "query returned more than" in text
            or "more than 10000 results" in text
            or ("-32005" in text and "limit" in text)
        )

    def _log_rpc_sources(self, w3: Web3, pool: PoolConfig) -> list[tuple[str, Web3]]:
        sources = [("primary", w3)]
        current_url = getattr(getattr(w3, "provider", None), "endpoint_uri", None)
        seen = {current_url} if current_url else set()
        rpc_urls = [RPC_URLS_2.get(pool.chain)] + RPC_BACKUP_LIST.get(pool.chain, [])
        fallback_index = 1
        for url in [item for item in rpc_urls if item]:
            if url in seen:
                continue
            seen.add(url)
            sources.append((f"fallback-{fallback_index}:{self._rpc_label(url)}", self._web3_for_rpc(pool, url)))
            fallback_index += 1
        return sources

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
            parsed = urlparse(url)
            return parsed.netloc or "unknown-rpc"
        except Exception:
            return "unknown-rpc"

    def _path(self, pool: PoolConfig) -> Path:
        return self.cache_dir / f"{pool.chain}_{pool.pool_address.lower()}.json"

    def _load(self, pool: PoolConfig) -> dict:
        path = self._path(pool)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save(
        self,
        pool: PoolConfig,
        positions: dict[int, PositionSnapshot],
        last_synced_block: int,
        legacy: dict | None = None,
        db_snapshot: dict | None = None,
        sync: dict | None = None,
    ) -> None:
        sync = sync or {}
        data = {
            "last_synced_block": last_synced_block,
            "npm_transfer_last_synced_block": int(sync.get("npm_transfer_last_synced_block") or 0),
            "token_ids": sorted(positions.keys()),
            "sync": {
                "source": sync.get("source"),
                "from_block": sync.get("from_block"),
                "to_block": sync.get("latest_block", last_synced_block),
                "swept_logs": bool(sync.get("swept_logs", False)),
                "historical_skipped": bool(sync.get("historical_skipped", False)),
                "cached_block": sync.get("cached_block", 0),
                "db_block": sync.get("db_block", 0),
                "db_token_count": sync.get("db_token_count", 0),
                "db_pid_bootstrapped": bool(sync.get("db_pid_bootstrapped", False)),
                "legacy_block": sync.get("legacy_block", 0),
                "legacy_token_count": sync.get("legacy_token_count", 0),
                "bootstrap_start_block": pool.bootstrap_start_block,
                "auto_bootstrap_start_block": pool.auto_bootstrap_start_block,
                "start_block": pool.start_block,
                "seed_token_count": len(pool.seed_token_ids),
            },
            "legacy_bootstrap": {
                "enabled": self.use_legacy_cache,
                "source": legacy.get("source") if legacy else None,
                "last_synced_block": legacy.get("last_synced_block") if legacy else 0,
                "candidate_count": len(legacy.get("token_ids", [])) if legacy else 0,
            },
            "db_bootstrap": {
                "enabled": self.use_db_cache,
                "source": db_snapshot.get("source") if db_snapshot else None,
                "last_synced_block": db_snapshot.get("last_synced_block") if db_snapshot else 0,
                "candidate_count": len(db_snapshot.get("token_ids", [])) if db_snapshot else 0,
                "pid_bootstrapped": bool(db_snapshot.get("pid_bootstrapped", False)) if db_snapshot else False,
            },
            "positions": {
                str(token_id): {
                    "owner": pos.owner,
                    "tick_lower": pos.tick_lower,
                    "tick_upper": pos.tick_upper,
                    "liquidity": str(pos.liquidity),
                    "pid": pos.pid,
                    "is_staked": pos.is_staked,
                }
                for token_id, pos in positions.items()
            },
        }
        with self._path(pool).open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
