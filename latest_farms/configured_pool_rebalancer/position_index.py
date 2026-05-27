from __future__ import annotations

import json
import logging
from pathlib import Path

from web3 import Web3

from .adapter import PancakeV3MasterChefAdapter
from .models import PoolConfig, PositionSnapshot
from .position_cache_snapshot import fetch_position_cache_snapshot


log = logging.getLogger("configured_pool_rebalancer")


DEPOSIT_TOPIC = "0x" + Web3.keccak(text="Deposit(address,uint256,uint256,uint256,int24,int24)").hex()
WITHDRAW_TOPIC = "0x" + Web3.keccak(text="Withdraw(address,address,uint256,uint256)").hex()
POOL_CREATED_TOPIC = "0x" + Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)").hex()

try:
    from latest_farms.config import FACTORY_ADDRESSES, FACTORY_DEPLOYED_BLOCK, MASTERCHEF_DEPLOYED_BLOCK
except ImportError:  # pragma: no cover
    from config import FACTORY_ADDRESSES, FACTORY_DEPLOYED_BLOCK, MASTERCHEF_DEPLOYED_BLOCK


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

    def refresh(self, w3: Web3, pool: PoolConfig, adapter: PancakeV3MasterChefAdapter) -> dict[int, PositionSnapshot]:
        cached = self._load(pool)
        token_ids = set(cached.get("token_ids", []))
        db_snapshot = self._load_db_pool_candidates(pool, cached)
        token_ids.update(db_snapshot["token_ids"])
        legacy = self._load_legacy_pool_candidates(pool)
        token_ids.update(legacy["token_ids"])
        token_ids.update(pool.seed_token_ids)

        latest_block = int(w3.eth.get_block("latest")["number"])
        sync = self._resolve_sync_window(w3, cached, db_snapshot, legacy, pool, latest_block)
        from_block = int(sync["from_block"])
        swept_logs = False

        if from_block <= latest_block:
            staked, unstaked = self._sweep_masterchef_logs(
                w3,
                adapter.masterchef_address,
                from_block,
                latest_block,
                pool.log_chunk_size,
            )
            token_ids.update(staked)
            token_ids.difference_update(unstaked)
            swept_logs = True

        positions = adapter.read_staked_positions(token_ids)
        managed = {wallet.lower() for wallet in pool.managed_wallets}
        filtered = {
            token_id: pos
            for token_id, pos in positions.items()
            if pos.owner.lower() in managed
        }
        self._save(pool, filtered, latest_block, legacy, db_snapshot, sync={**sync, "swept_logs": swept_logs})
        return filtered

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
        if cached_block:
            source = "module_cache"
            from_block = cached_block + 1
        elif db_block and (db_token_count or db_pid_bootstrapped):
            source = "db_position_cache"
            from_block = db_block + 1
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
            if pool.pid is not None:
                try:
                    if int(info.get("pid", -1)) != int(pool.pid):
                        continue
                except (TypeError, ValueError):
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
        if int(cached.get("last_synced_block") or 0):
            return {"token_ids": set(), "last_synced_block": 0, "source": None, "pid_bootstrapped": False}
        try:
            snapshot = fetch_position_cache_snapshot(pool.chain, self.db_cache_source)
        except Exception as exc:
            log.warning("could not load DB position cache snapshot chain=%s source=%s: %s", pool.chain, self.db_cache_source, exc)
            return {"token_ids": set(), "last_synced_block": 0, "source": "db:error", "pid_bootstrapped": False}
        if not snapshot:
            return {"token_ids": set(), "last_synced_block": 0, "source": "db:missing", "pid_bootstrapped": False}
        data = snapshot.get("snapshot") or {}
        candidates, pid_bootstrapped = self._candidate_token_ids_from_snapshot(data, pool)
        return {
            "token_ids": candidates,
            "last_synced_block": int(snapshot.get("last_synced_block") or data.get("last_synced_block") or 0),
            "source": f"db:{self.db_cache_source}",
            "pid_bootstrapped": pid_bootstrapped,
            "position_count": int(snapshot.get("position_count") or 0),
        }

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
        for raw_token_id, info in positions.items():
            if not isinstance(info, dict):
                continue
            if pool.pid is not None:
                try:
                    if int(info.get("pid", -1)) != int(pool.pid):
                        continue
                except (TypeError, ValueError):
                    continue
            try:
                candidates.add(int(raw_token_id))
            except (TypeError, ValueError):
                continue
        return candidates, pid_bootstrapped

    def _sweep_masterchef_logs(
        self,
        w3: Web3,
        masterchef_address: str,
        from_block: int,
        to_block: int,
        chunk_size: int,
    ) -> tuple[set[int], set[int]]:
        staked: set[int] = set()
        unstaked: set[int] = set()
        for start in range(from_block, to_block + 1, chunk_size):
            end = min(start + chunk_size - 1, to_block)
            logs = w3.eth.get_logs(
                {
                    "fromBlock": start,
                    "toBlock": end,
                    "address": Web3.to_checksum_address(masterchef_address),
                    "topics": [[DEPOSIT_TOPIC, WITHDRAW_TOPIC]],
                }
            )
            for event in logs:
                topic0 = Web3.to_hex(event["topics"][0]).lower()
                if len(event["topics"]) < 4:
                    continue
                token_id = int.from_bytes(event["topics"][3], "big")
                if topic0 == DEPOSIT_TOPIC.lower():
                    staked.add(token_id)
                    unstaked.discard(token_id)
                elif topic0 == WITHDRAW_TOPIC.lower():
                    unstaked.add(token_id)
                    staked.discard(token_id)
        return staked, unstaked

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
