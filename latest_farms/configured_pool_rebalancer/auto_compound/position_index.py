from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from web3 import Web3
from w3multicall.multicall import W3Multicall

from ..adapter import DexAdapter
from ..models import CompoundCandidate, DexType, PoolConfig, PositionSnapshot
from ..position_index import PositionIndex
from ..position_cache_snapshot import fetch_position_cache_snapshot
from .abi import COMPOUND_NPM_ABI
from .models import CompoundDiscoveryResult, CompoundPolicySkip, CompoundPosition


log = logging.getLogger("configured_pool_rebalancer.auto_compound")
TRANSFER_TOPIC = Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)"))
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class _DbCandidates:
    unstaked: set[int]
    managed_staked: set[int]


class CompoundPositionIndex:
    """Discover compound positions without feeding unstaked NFTs into rebalancing."""

    def __init__(self, cache_dir: str, legacy_cache_dir: str | None = None):
        self.cache_dir = Path(cache_dir) / "auto_compound"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.staked_index = PositionIndex(
            str(self.cache_dir / "staked"),
            legacy_cache_dir=legacy_cache_dir,
            use_legacy_cache=True,
            use_db_cache=True,
        )

    def refresh(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: DexAdapter,
        candidate_hints: set[int] | None = None,
    ) -> CompoundDiscoveryResult:
        started_at = time.monotonic()
        if not pool.npm_address:
            raise ValueError(f"pool {pool.name} has no NPM address")
        npm = w3.eth.contract(address=Web3.to_checksum_address(pool.npm_address), abi=COMPOUND_NPM_ABI)
        out: dict[int, CompoundPosition] = {}
        policy_skips: list[CompoundPolicySkip] = []
        hints = {int(value) for value in (candidate_hints or set())}

        if pool.dex_type in {DexType.PANCAKE_V3, DexType.PANCAKE_V3_MASTERCHEF}:
            try:
                for token_id, snapshot in self.staked_index.refresh(w3, pool, adapter).items():
                    out[token_id] = self._wrap(pool, snapshot, "STAKED")
            except Exception as exc:
                log.warning("compound staked discovery failed pool=%s: %s", pool.name, exc)

        seed_ids = set(pool.seed_token_ids)
        candidate_ids = set(seed_ids)
        cached = self._load(pool)
        cached_ids = {int(value) for value in cached.get("token_ids", [])}
        candidate_ids.update(cached_ids)
        db_candidates = self._db_candidates(pool)
        candidate_ids.update(db_candidates.unstaked)
        candidate_ids.update(hints)
        latest_block = int(w3.eth.block_number)
        checkpoint_block = int(cached.get("last_synced_block") or 0)
        enumerable_ids: set[int] = set()
        enumerable_complete = False
        transfer_ids: set[int] = set()
        transfer_complete = False
        try:
            balance = int(npm.functions.balanceOf(pool.bot_wallet).call())
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
                enumerable_ids.update(int(value) for value in multicall.call() if value is not None)
            candidate_ids.update(enumerable_ids)
            enumerable_complete = True
        except Exception as exc:
            log.warning("NPM enumerable discovery unavailable pool=%s: %s", pool.name, exc)
            transfer_ids, transfer_complete = self._sweep_transfers(w3, pool, cached, latest_block)
            candidate_ids.update(transfer_ids)

        wallet = Web3.to_checksum_address(pool.bot_wallet)
        owned_ids = self._batch_owned_token_ids(w3, pool, candidate_ids, wallet)
        for token_id in sorted(owned_ids):
            try:
                snapshot = adapter.read_npm_position(token_id, owner=wallet)
            except Exception:
                continue
            if snapshot.liquidity <= 0 or not self._matches_pool(pool, snapshot):
                continue
            snapshot.is_staked = False
            out[token_id] = self._wrap(pool, snapshot, "UNSTAKED")

        if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}:
            staked_candidates = set(db_candidates.managed_staked)
            staked_candidates.update(hints)
            try:
                verified_staked = adapter.read_staked_positions(staked_candidates)
            except Exception as exc:
                log.warning("compound staked policy discovery failed pool=%s: %s", pool.name, exc)
                verified_staked = {}
            for token_id in sorted(verified_staked):
                out.pop(token_id, None)
                policy_skips.append(CompoundPolicySkip(token_id, "STAKE_POLICY"))

        if enumerable_complete or transfer_complete:
            checkpoint_block = latest_block
        self._save(pool, out, checkpoint_block)
        log.info(
            "compound position discovery pool=%s seed=%s cache=%s db_unstaked=%s "
            "db_managed_staked=%s hints=%s enumerable=%s transfers=%s owner_candidates=%s "
            "positions=%s policy_skips=%s checkpoint=%s duration_ms=%s",
            pool.name,
            len(seed_ids),
            len(cached_ids),
            len(db_candidates.unstaked),
            len(db_candidates.managed_staked),
            len(hints),
            len(enumerable_ids),
            len(transfer_ids),
            len(candidate_ids),
            len(out),
            len(policy_skips),
            checkpoint_block,
            int((time.monotonic() - started_at) * 1000),
        )
        return CompoundDiscoveryResult(out, policy_skips)

    def revalidate_candidates(
        self,
        w3: Web3,
        pool: PoolConfig,
        adapter: DexAdapter,
        candidates: tuple[CompoundCandidate, ...] | list[CompoundCandidate],
    ) -> CompoundDiscoveryResult:
        """Revalidate rebalancer handoff without running an independent discovery pass."""
        if not pool.npm_address:
            raise ValueError(f"pool {pool.name} has no NPM address")
        wallet = Web3.to_checksum_address(pool.bot_wallet)
        positions: dict[int, CompoundPosition] = {}
        skips: list[CompoundPolicySkip] = []
        ordered_ids = sorted({int(candidate.token_id) for candidate in candidates})

        try:
            staked_positions = adapter.read_staked_positions(ordered_ids)
        except Exception as exc:
            log.warning("compound stake revalidation failed pool=%s candidates=%s: %s", pool.name, len(ordered_ids), exc)
            return CompoundDiscoveryResult(
                {},
                [CompoundPolicySkip(token_id, "POSITION_REVALIDATION_FAILED") for token_id in ordered_ids],
            )

        unstaked_ids = set(ordered_ids).difference(staked_positions)
        owned_ids = self._batch_owned_token_ids(w3, pool, unstaked_ids, wallet)
        owners = {token_id: wallet for token_id in owned_ids}
        if hasattr(adapter, "read_npm_positions"):
            npm_positions = adapter.read_npm_positions(owned_ids, owners=owners)
        else:
            npm_positions = {}
            for token_id in owned_ids:
                try:
                    npm_positions[token_id] = adapter.read_npm_position(token_id, owner=wallet)
                except Exception:
                    continue

        for token_id in ordered_ids:
            if token_id in staked_positions:
                snapshot = staked_positions[token_id]
                snapshot.is_staked = True
                if snapshot.owner.lower() != wallet.lower():
                    skips.append(CompoundPolicySkip(token_id, "OWNER_CHANGED"))
                    continue
            else:
                if token_id not in owned_ids:
                    skips.append(CompoundPolicySkip(token_id, "OWNER_CHANGED"))
                    continue
                snapshot = npm_positions.get(token_id)
                if snapshot is None:
                    skips.append(CompoundPolicySkip(token_id, "POSITION_REVALIDATION_FAILED"))
                    continue
                snapshot.is_staked = False

            if not self._matches_pool(pool, snapshot):
                skips.append(CompoundPolicySkip(token_id, "POOL_MISMATCH"))
                continue
            if int(snapshot.liquidity) <= 0:
                skips.append(CompoundPolicySkip(token_id, "ZERO_LIQUIDITY"))
                continue
            if snapshot.is_staked and pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE}:
                skips.append(CompoundPolicySkip(token_id, "STAKE_POLICY"))
                continue
            positions[token_id] = self._wrap(
                pool,
                snapshot,
                "STAKED" if snapshot.is_staked else "UNSTAKED",
            )

        log.info(
            "compound candidate revalidation pool=%s candidates=%s positions=%s policy_skips=%s",
            pool.name,
            len(candidates),
            len(positions),
            len(skips),
        )
        return CompoundDiscoveryResult(positions, skips)

    def _sweep_transfers(
        self,
        w3: Web3,
        pool: PoolConfig,
        cached: dict,
        latest_block: int,
    ) -> tuple[set[int], bool]:
        configured_start = pool.bootstrap_start_block if pool.bootstrap_start_block is not None else pool.start_block
        if not configured_start and not cached.get("last_synced_block"):
            return set(), False
        from_block = max(int(configured_start or 0), int(cached.get("last_synced_block") or 0) + 1)
        if from_block > latest_block:
            return set(), True
        wallet_topic = "0x" + pool.bot_wallet.lower()[2:].rjust(64, "0")
        ids: set[int] = set()
        complete = True
        for start in range(from_block, latest_block + 1, max(1, pool.log_chunk_size)):
            end = min(latest_block, start + max(1, pool.log_chunk_size) - 1)
            for topics in ([TRANSFER_TOPIC, None, wallet_topic], [TRANSFER_TOPIC, wallet_topic]):
                try:
                    logs = w3.eth.get_logs(
                        {"fromBlock": start, "toBlock": end, "address": pool.npm_address, "topics": topics}
                    )
                except Exception:
                    complete = False
                    continue
                for event in logs:
                    event_topics = event.get("topics") or []
                    if len(event_topics) >= 4:
                        ids.add(int.from_bytes(event_topics[3], "big"))
        return ids, complete

    @staticmethod
    def _batch_owned_token_ids(
        w3: Web3,
        pool: PoolConfig,
        candidate_ids: set[int],
        wallet: str,
    ) -> set[int]:
        owned: set[int] = set()
        ordered = sorted({int(value) for value in candidate_ids})
        for start in range(0, len(ordered), 150):
            token_ids = ordered[start : start + 150]
            multicall = W3Multicall(w3)
            for token_id in token_ids:
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
                log.warning(
                    "compound owner batch unavailable pool=%s candidates=%s: %s",
                    pool.name,
                    len(token_ids),
                    exc,
                )
                continue
            for token_id, owner in zip(token_ids, owners):
                try:
                    if Web3.to_checksum_address(owner) == wallet:
                        owned.add(token_id)
                except (TypeError, ValueError):
                    continue
        return owned

    @staticmethod
    def _db_candidates(pool: PoolConfig) -> _DbCandidates:
        source = "aerodrome_positions_cache" if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE} else "positions_cache"
        try:
            row = fetch_position_cache_snapshot(pool.chain, source)
        except Exception:
            return _DbCandidates(set(), set())
        snapshot = (row or {}).get("snapshot") or {}
        positions = snapshot.get("positions") or {}
        stake_owners_by_pool = snapshot.get("stake_owners") or {}
        stake_owners = next(
            (
                value
                for key, value in stake_owners_by_pool.items()
                if str(key).lower() == pool.pool_address.lower()
            ),
            {},
        )
        managed = {address.lower() for address in pool.managed_wallets}
        managed.add(pool.bot_wallet.lower())
        unstaked: set[int] = set()
        managed_staked: set[int] = set()
        for raw_token_id, info in positions.items():
            if not isinstance(info, dict):
                continue
            pool_address = info.get("pool_address") or info.get("pool") or info.get("v3_pool")
            pid = info.get("pid")
            if pool.pid is not None and pid is not None and int(pid) != int(pool.pid):
                continue
            if pool.pid is None and not pool_address:
                continue
            if pool_address and str(pool_address).lower() != pool.pool_address.lower():
                continue
            npm_address = info.get("npm_address") or info.get("position_manager")
            if npm_address and pool.npm_address and str(npm_address).lower() != pool.npm_address.lower():
                continue
            try:
                token_id = int(raw_token_id)
            except (TypeError, ValueError):
                continue
            owner = str(info.get("owner") or "").lower()
            stake_owner = str(stake_owners.get(str(raw_token_id)) or "").lower()
            if bool(info.get("is_staked")):
                if owner in managed or stake_owner in managed:
                    managed_staked.add(token_id)
                continue
            if owner and owner != ZERO_ADDRESS and owner not in managed:
                continue
            unstaked.add(token_id)
        return _DbCandidates(unstaked, managed_staked)

    @staticmethod
    def _matches_pool(pool: PoolConfig, snapshot: PositionSnapshot) -> bool:
        if pool.token0_address and snapshot.token0.lower() != pool.token0_address.lower():
            return False
        if pool.token1_address and snapshot.token1.lower() != pool.token1_address.lower():
            return False
        expected = pool.tick_spacing if pool.dex_type in {DexType.AERODROME_V3, DexType.AERODROME_GAUGE} else pool.fee
        return expected is None or int(snapshot.fee) == int(expected)

    @staticmethod
    def _wrap(pool: PoolConfig, snapshot: PositionSnapshot, stake_mode: str) -> CompoundPosition:
        return CompoundPosition(
            snapshot=snapshot,
            npm_address=Web3.to_checksum_address(pool.npm_address),
            dex_type=pool.dex_type.value,
            stake_mode=stake_mode,
        )

    def _path(self, pool: PoolConfig) -> Path:
        return self.cache_dir / f"{pool.chain}_{pool.pool_address.lower()}.json"

    def _load(self, pool: PoolConfig) -> dict:
        path = self._path(pool)
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return {}

    def _save(self, pool: PoolConfig, positions: dict[int, CompoundPosition], block: int) -> None:
        path = self._path(pool)
        payload = {"last_synced_block": int(block), "token_ids": sorted(positions)}
        temp = path.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
        temp.replace(path)
