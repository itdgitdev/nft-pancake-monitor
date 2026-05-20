from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import mysql.connector
import requests
from web3 import Web3

from .evm import web3_connection
from .models import PoolConfig, WorkerConfig
from .reward import pancake_reward_token


try:
    from latest_farms.create_db import get_connection
except ImportError:  # pragma: no cover
    from create_db import get_connection


TX_HASH_COLUMNS = (
    "withdraw_tx_hash",
    "swap_tx_hash",
    "mint_tx_hash",
    "stake_tx_hash",
    "burn_tx_hash",
)

WRAPPED_NATIVE_TOKENS = {
    "BNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
    "ETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "BAS": "0x4200000000000000000000000000000000000006",
    "ARB": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "LIN": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
    "POL": "0x0d500B1d8E8eF31E21C99d1DB9A6444d3ADf1270",
}

PRICE_CHAIN_IDS = {
    "BNB": "56",
    "ETH": "1",
    "BAS": "8453",
    "ARB": "42161",
    "LIN": "59144",
    "POL": "137",
}

COINGECKO_PLATFORMS = {
    "BNB": "binance-smart-chain",
    "ETH": "ethereum",
    "BAS": "base",
    "ARB": "arbitrum-one",
    "LIN": "linea",
    "POL": "polygon-pos",
}


@dataclass(frozen=True)
class PnlReportResult:
    records: list[dict[str, Any]]
    written_files: list[str]


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: float | None, digits: int = 8) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _normalize_tx_hash(value: Any) -> tuple[str, str] | None:
    text = str(value or "").strip()
    if not text:
        return None
    if ":" in text:
        return None
    if text.lower().startswith("0x"):
        text = text[2:]
    if len(text) != 64:
        return None
    try:
        int(text, 16)
    except ValueError:
        return None
    bare = text.lower()
    return bare, f"0x{bare}"


class ConfiguredPoolPnlReporter:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self._receipt_gas_cache: dict[tuple[str, str], float] = {}
        self._native_price_cache: dict[str, float | None] = {}

    def build_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for pool in self.config.pools:
            for wallet in pool.managed_wallets:
                records.extend(self._build_pool_wallet_records(pool, wallet))
        return records

    def write_report(self, output_dir: str | Path, output_format: str = "both") -> PnlReportResult:
        records = self.build_records()
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        written: list[str] = []
        fmt = output_format.lower()
        if fmt not in {"json", "csv", "both"}:
            raise ValueError("pnl format must be one of: json, csv, both")

        if fmt in {"json", "both"}:
            json_path = output_path / "configured_rebalancer_pnl.json"
            with json_path.open("w", encoding="utf-8") as fh:
                json.dump(records, fh, indent=2, sort_keys=True)
            written.append(str(json_path))

        if fmt in {"csv", "both"}:
            csv_path = output_path / "configured_rebalancer_pnl.csv"
            self._write_csv(csv_path, records)
            written.append(str(csv_path))

        return PnlReportResult(records=records, written_files=written)

    def _build_pool_wallet_records(self, pool: PoolConfig, wallet: str) -> list[dict[str, Any]]:
        warnings: list[str] = []
        jobs = self._fetch_rebalance_jobs(pool, wallet, warnings)
        latest_positions = self._fetch_latest_positions(pool, wallet, warnings)

        candidate_token_ids = set(latest_positions)
        old_to_new: dict[int, int] = {}
        new_token_ids: set[int] = set()
        tx_hashes_by_old: dict[int, list[str]] = {}
        job_status_by_old: dict[int, str] = {}
        module_reward_by_old: dict[int, dict[str, Any]] = {}

        for job in jobs:
            old_id = _as_int(job.get("old_token_id"))
            new_id = _as_int(job.get("new_token_id"))
            if old_id is None:
                continue
            candidate_token_ids.add(old_id)
            job_status_by_old[old_id] = str(job.get("status") or "")
            if new_id is not None:
                old_to_new[old_id] = new_id
                new_token_ids.add(new_id)
                candidate_token_ids.add(new_id)
            tx_hashes_by_old[old_id] = [
                str(job[col])
                for col in TX_HASH_COLUMNS
                if job.get(col)
            ]
            module_reward_by_old[old_id] = job

        if not candidate_token_ids:
            return [
                self._empty_pool_record(
                    pool,
                    wallet,
                    warnings + ["no positions found in wallet_nft_position and no configured rebalance jobs"],
                )
            ]

        first_positions = self._fetch_positions_by_ids(pool, wallet, candidate_token_ids, latest=False, warnings=warnings)
        latest_positions.update(
            self._fetch_positions_by_ids(pool, wallet, candidate_token_ids, latest=True, warnings=warnings)
        )
        summaries = self._fetch_summaries(pool, wallet, candidate_token_ids, warnings)

        roots = sorted(candidate_token_ids - new_token_ids)
        if not roots:
            roots = sorted(candidate_token_ids)

        records: list[dict[str, Any]] = []
        seen_chains: set[tuple[int, ...]] = set()
        for root_id in roots:
            chain = self._follow_chain(root_id, old_to_new)
            chain_key = tuple(chain)
            if chain_key in seen_chains:
                continue
            seen_chains.add(chain_key)
            records.append(
                self._build_chain_record(
                    pool,
                    wallet,
                    chain,
                    first_positions,
                    latest_positions,
                    summaries,
                    tx_hashes_by_old,
                    job_status_by_old,
                    module_reward_by_old,
                    warnings,
                )
            )
        return records

    def _build_chain_record(
        self,
        pool: PoolConfig,
        wallet: str,
        token_chain: list[int],
        first_positions: dict[int, dict[str, Any]],
        latest_positions: dict[int, dict[str, Any]],
        summaries: dict[int, dict[str, Any]],
        tx_hashes_by_old: dict[int, list[str]],
        job_status_by_old: dict[int, str],
        module_reward_by_old: dict[int, dict[str, Any]],
        shared_warnings: list[str],
    ) -> dict[str, Any]:
        warnings = list(shared_warnings)
        root_id = token_chain[0]
        current_id = token_chain[-1]
        root_latest = latest_positions.get(root_id)
        root_first = first_positions.get(root_id)
        current_latest = latest_positions.get(current_id)
        if not pancake_reward_token(pool.chain):
            warnings.append(f"no Pancake reward token mapping configured for {pool.chain}; module reward PnL is unavailable")

        if not root_latest and not root_first:
            warnings.append(f"missing wallet_nft_position snapshots for root token {root_id}")
        if not current_latest:
            warnings.append(f"missing latest wallet_nft_position snapshot for current token {current_id}")

        initial_capital_usd = 0.0
        basis_source = "MISSING"
        if root_latest and _as_float(root_latest.get("initial_total_value")) > 0:
            initial_capital_usd = _as_float(root_latest.get("initial_total_value"))
            basis_source = "WALLET_NFT_POSITION_INITIAL_TOTAL"
        elif root_first and _as_float(root_first.get("current_total_value")) > 0:
            initial_capital_usd = _as_float(root_first.get("current_total_value"))
            basis_source = "FIRST_DETECTED_SNAPSHOT"
            warnings.append(f"token {root_id} has no initial_total_value; using first snapshot current_total_value")
        else:
            warnings.append(f"token {root_id} has no usable initial capital")

        current_position_value_usd = _as_float(current_latest.get("current_total_value")) if current_latest else 0.0
        current_unclaimed_usd = self._current_unclaimed_usd(current_latest) if current_latest else 0.0
        current_value_usd = current_position_value_usd + current_unclaimed_usd
        gross_pnl_usd = current_value_usd - initial_capital_usd

        tx_hashes = []
        for token_id in token_chain:
            tx_hashes.extend(tx_hashes_by_old.get(token_id, []))
        gas_cost_native = self._gas_cost_native(pool.chain, wallet, tx_hashes, warnings)
        native_price = self._native_price_usd(pool.chain, warnings)
        gas_cost_usd = gas_cost_native * native_price if native_price is not None else None

        net_pnl_usd_after_gas = gross_pnl_usd - gas_cost_usd if gas_cost_usd is not None else None
        module_claimed_reward_usd = self._module_claimed_reward_usd(pool, token_chain, module_reward_by_old, warnings)
        net_pnl_usd_after_gas_and_module_reward = (
            net_pnl_usd_after_gas + module_claimed_reward_usd
            if net_pnl_usd_after_gas is not None
            else None
        )
        current_summary = summaries.get(current_id)
        wallet_summary_pnl_reference_usd = (
            _as_float(current_summary.get("pnl_value_usd")) if current_summary else None
        )
        if current_summary is None:
            warnings.append(f"missing wallet_nft_summary for current token {current_id}")

        claimed_fee_reference_usd = 0.0
        summary_claimed_reward_reference_usd = 0.0
        for token_id in token_chain:
            summary = summaries.get(token_id)
            if not summary:
                continue
            claimed_fee_reference_usd += _as_float(summary.get("total_claimed_fee_usd"))
            summary_claimed_reward_reference_usd += _as_float(summary.get("total_claimed_reward_usd"))
        claimed_reference_usd = claimed_fee_reference_usd + summary_claimed_reward_reference_usd
        external_claimed_reward_estimate_usd = max(
            0.0,
            summary_claimed_reward_reference_usd - module_claimed_reward_usd,
        )

        endpoint_job_status = job_status_by_old.get(current_id)
        if endpoint_job_status and current_id in tx_hashes_by_old:
            status = endpoint_job_status
            warnings.append(f"rebalance job for token {current_id} has no linked new_token_id")
        else:
            status = str(current_latest.get("status") or "") if current_latest else ""
        if not status:
            status = job_status_by_old.get(root_id) or "UNKNOWN"

        return {
            "pool": pool.name,
            "chain": pool.chain,
            "wallet_address": wallet,
            "token_id_chain": token_chain,
            "root_token_id": root_id,
            "current_token_id": current_id,
            "initial_capital_usd": _round_or_none(initial_capital_usd),
            "current_position_value_usd": _round_or_none(current_position_value_usd),
            "current_unclaimed_usd": _round_or_none(current_unclaimed_usd),
            "current_value_usd": _round_or_none(current_value_usd),
            "gross_pnl_usd": _round_or_none(gross_pnl_usd),
            "gas_cost_native": _round_or_none(gas_cost_native, 12),
            "gas_cost_usd": _round_or_none(gas_cost_usd),
            "net_pnl_usd_after_gas": _round_or_none(net_pnl_usd_after_gas),
            "module_claimed_reward_usd": _round_or_none(module_claimed_reward_usd),
            "summary_claimed_reward_reference_usd": _round_or_none(summary_claimed_reward_reference_usd),
            "external_claimed_reward_estimate_usd": _round_or_none(external_claimed_reward_estimate_usd),
            "claimed_fee_reference_usd": _round_or_none(claimed_fee_reference_usd),
            "net_pnl_usd_after_gas_and_module_reward": _round_or_none(
                net_pnl_usd_after_gas_and_module_reward
            ),
            "basis_source": basis_source,
            "wallet_summary_pnl_reference_usd": _round_or_none(wallet_summary_pnl_reference_usd),
            "wallet_summary_claimed_reference_usd": _round_or_none(claimed_reference_usd),
            "status": status,
            "warnings": sorted(set(warnings)),
        }

    def _empty_pool_record(self, pool: PoolConfig, wallet: str, warnings: list[str]) -> dict[str, Any]:
        return {
            "pool": pool.name,
            "chain": pool.chain,
            "wallet_address": wallet,
            "token_id_chain": [],
            "root_token_id": None,
            "current_token_id": None,
            "initial_capital_usd": None,
            "current_position_value_usd": None,
            "current_unclaimed_usd": None,
            "current_value_usd": None,
            "gross_pnl_usd": None,
            "gas_cost_native": 0.0,
            "gas_cost_usd": None,
            "net_pnl_usd_after_gas": None,
            "module_claimed_reward_usd": 0.0,
            "summary_claimed_reward_reference_usd": 0.0,
            "external_claimed_reward_estimate_usd": 0.0,
            "claimed_fee_reference_usd": 0.0,
            "net_pnl_usd_after_gas_and_module_reward": None,
            "basis_source": "MISSING",
            "wallet_summary_pnl_reference_usd": None,
            "wallet_summary_claimed_reference_usd": None,
            "status": "NO_POSITION",
            "warnings": sorted(set(warnings)),
        }

    def _fetch_rebalance_jobs(self, pool: PoolConfig, wallet: str, warnings: list[str]) -> list[dict[str, Any]]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            try:
                cursor.execute(
                    """
                    SELECT old_token_id, new_token_id, status,
                           withdraw_tx_hash, swap_tx_hash, mint_tx_hash, stake_tx_hash, burn_tx_hash,
                           claimed_reward_token, claimed_reward_raw, claimed_reward_amount,
                           claimed_reward_price_usd, claimed_reward_usd, claimed_reward_source
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                    ORDER BY created_at ASC, id ASC
                    """,
                    (pool.chain, pool.pool_address, wallet),
                )
            except mysql.connector.Error as exc:
                if exc.errno != 1054:
                    raise
                warnings.append("configured_rebalance_jobs reward columns are missing; run --migrate for module reward PnL")
                cursor.execute(
                    """
                    SELECT old_token_id, new_token_id, status,
                           withdraw_tx_hash, swap_tx_hash, mint_tx_hash, stake_tx_hash, burn_tx_hash
                    FROM configured_rebalance_jobs
                    WHERE chain=%s
                      AND LOWER(pool_address)=LOWER(%s)
                      AND LOWER(wallet_address)=LOWER(%s)
                    ORDER BY created_at ASC, id ASC
                    """,
                    (pool.chain, pool.pool_address, wallet),
                )
            return list(cursor.fetchall())
        except mysql.connector.Error as exc:
            if exc.errno == 1146:
                warnings.append("configured_rebalance_jobs table is missing; reporting current snapshots only")
                return []
            raise
        finally:
            cursor.close()
            conn.close()

    def _fetch_latest_positions(self, pool: PoolConfig, wallet: str, warnings: list[str]) -> dict[int, dict[str, Any]]:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(
                """
                SELECT p.*
                FROM wallet_nft_position p
                INNER JOIN (
                    SELECT nft_id, MAX(created_at) AS max_created_at
                    FROM wallet_nft_position
                    WHERE chain=%s
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND LOWER(pool_address)=LOWER(%s)
                    GROUP BY nft_id
                ) latest
                  ON p.nft_id = latest.nft_id AND p.created_at = latest.max_created_at
                WHERE p.chain=%s
                  AND LOWER(p.wallet_address)=LOWER(%s)
                  AND LOWER(p.pool_address)=LOWER(%s)
                  AND COALESCE(p.status, '') NOT IN ('Burned')
                """,
                (pool.chain, wallet, pool.pool_address, pool.chain, wallet, pool.pool_address),
            )
            return {int(row["nft_id"]): row for row in cursor.fetchall() if row.get("nft_id") is not None}
        except mysql.connector.Error as exc:
            warnings.append(f"failed to read wallet_nft_position latest snapshots: {exc}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def _fetch_positions_by_ids(
        self,
        pool: PoolConfig,
        wallet: str,
        token_ids: set[int],
        latest: bool,
        warnings: list[str],
    ) -> dict[int, dict[str, Any]]:
        if not token_ids:
            return {}
        order = "DESC" if latest else "ASC"
        rows: dict[int, dict[str, Any]] = {}
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            for token_id in sorted(token_ids):
                cursor.execute(
                    f"""
                    SELECT *
                    FROM wallet_nft_position
                    WHERE chain=%s
                      AND LOWER(wallet_address)=LOWER(%s)
                      AND LOWER(pool_address)=LOWER(%s)
                      AND nft_id=%s
                    ORDER BY created_at {order}
                    LIMIT 1
                    """,
                    (pool.chain, wallet, pool.pool_address, token_id),
                )
                row = cursor.fetchone()
                if row:
                    rows[token_id] = row
            return rows
        except mysql.connector.Error as exc:
            label = "latest" if latest else "first"
            warnings.append(f"failed to read wallet_nft_position {label} snapshots: {exc}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def _fetch_summaries(
        self,
        pool: PoolConfig,
        wallet: str,
        token_ids: set[int],
        warnings: list[str],
    ) -> dict[int, dict[str, Any]]:
        if not token_ids:
            return {}
        placeholders = ",".join(["%s"] * len(token_ids))
        conn = get_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(
                f"""
                SELECT nft_id, wallet_address, chain, total_cash_injected, invested_capital_base,
                       total_claimed_fee_usd, total_claimed_reward_usd,
                       pnl_value_usd, pnl_value_base, status, updated_at
                FROM wallet_nft_summary
                WHERE chain=%s
                  AND LOWER(wallet_address)=LOWER(%s)
                  AND nft_id IN ({placeholders})
                """,
                tuple([pool.chain, wallet] + sorted(token_ids)),
            )
            return {int(row["nft_id"]): row for row in cursor.fetchall() if row.get("nft_id") is not None}
        except mysql.connector.Error as exc:
            if exc.errno == 1146:
                warnings.append("wallet_nft_summary table is missing")
                return {}
            warnings.append(f"failed to read wallet_nft_summary: {exc}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def _current_unclaimed_usd(self, row: dict[str, Any] | None) -> float:
        if not row:
            return 0.0
        fee0 = _as_float(row.get("unclaimed_fee_token0")) * _as_float(row.get("price_token0"))
        fee1 = _as_float(row.get("unclaimed_fee_token1")) * _as_float(row.get("price_token1"))
        reward = _as_float(row.get("pending_cake")) * _as_float(row.get("reward_price"))
        return fee0 + fee1 + reward

    def _module_claimed_reward_usd(
        self,
        pool: PoolConfig,
        token_chain: list[int],
        module_reward_by_old: dict[int, dict[str, Any]],
        warnings: list[str],
    ) -> float:
        total = 0.0
        token0 = str(pool.token0_address or "").lower()
        token1 = str(pool.token1_address or "").lower()
        for token_id in token_chain:
            job = module_reward_by_old.get(token_id)
            if not job:
                continue
            reward_token = str(job.get("claimed_reward_token") or "").lower()
            claimed_raw = _as_float(job.get("claimed_reward_raw"))
            if reward_token and reward_token in {token0, token1}:
                warnings.append(
                    f"module reward token for job {token_id} matches LP token; reward excluded to avoid double count"
                )
                continue
            claimed_usd = _as_float(job.get("claimed_reward_usd"))
            if claimed_raw > 0 and claimed_usd <= 0:
                warnings.append(f"module reward claimed for job {token_id} but USD price was unavailable")
            total += claimed_usd
        return total

    def _follow_chain(self, root_id: int, old_to_new: dict[int, int]) -> list[int]:
        chain = [root_id]
        seen = {root_id}
        current = root_id
        while current in old_to_new:
            nxt = old_to_new[current]
            if nxt in seen:
                break
            chain.append(nxt)
            seen.add(nxt)
            current = nxt
        return chain

    def _gas_cost_native(self, chain: str, wallet: str, tx_hashes: list[str], warnings: list[str]) -> float:
        total = 0.0
        normalized_hashes: dict[str, str] = {}
        for tx_hash in tx_hashes:
            normalized = _normalize_tx_hash(tx_hash)
            if not normalized:
                if tx_hash and ":" not in str(tx_hash):
                    warnings.append(f"invalid tx hash skipped for gas lookup: {tx_hash}")
                continue
            bare_hash, receipt_hash = normalized
            normalized_hashes[bare_hash] = receipt_hash

        for bare_hash, receipt_hash in sorted(normalized_hashes.items()):
            db_fee = self._tx_fee_from_db(chain, wallet, bare_hash)
            if db_fee is not None:
                total += db_fee
                continue
            receipt_fee = self._tx_fee_from_receipt(chain, receipt_hash, warnings)
            if receipt_fee is not None:
                total += receipt_fee
        return total

    def _native_price_usd(self, chain: str, warnings: list[str]) -> float | None:
        chain_key = chain.upper()
        if chain_key in self._native_price_cache:
            return self._native_price_cache[chain_key]

        wrapped = WRAPPED_NATIVE_TOKENS.get(chain_key)
        if not wrapped:
            warnings.append(f"no wrapped native token configured for {chain_key}")
            self._native_price_cache[chain_key] = None
            return None

        price = (
            self._native_price_from_pancake(chain_key, wrapped, warnings)
            or self._native_price_from_dexscreener(wrapped, warnings)
            or self._native_price_from_coingecko(chain_key, wrapped, warnings)
        )

        if not price:
            fallback = self.config.pnl_native_prices_usd.get(chain_key)
            if fallback and fallback > 0:
                price = fallback
                warnings.append(f"using fallback pnl.native_prices_usd.{chain_key} because market price APIs failed")

        if not price:
            warnings.append(f"native market price unavailable for {chain_key}; net PnL after gas is unavailable")
            self._native_price_cache[chain_key] = None
            return None

        self._native_price_cache[chain_key] = float(price)
        return float(price)

    def _native_price_from_pancake(self, chain: str, token_address: str, warnings: list[str]) -> float | None:
        chain_id = PRICE_CHAIN_IDS.get(chain)
        if not chain_id:
            return None
        url = f"https://wallet-api.pancakeswap.com/v1/prices/list/{chain_id}%3A{token_address}"
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            response.raise_for_status()
            data = response.json()
            price = data.get(f"{chain_id}:{token_address}") or data.get(f"{chain_id}:{token_address.lower()}")
            return float(price) if price else None
        except Exception as exc:
            warnings.append(f"pancake native price lookup failed for {chain}: {exc}")
            return None

    def _native_price_from_dexscreener(self, token_address: str, warnings: list[str]) -> float | None:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            response.raise_for_status()
            pairs = response.json().get("pairs") or []
            if not pairs:
                return None
            best_pair = max(pairs, key=lambda item: item.get("liquidity", {}).get("usd", 0) or 0)
            price = best_pair.get("priceUsd")
            return float(price) if price else None
        except Exception as exc:
            warnings.append(f"dexscreener native price lookup failed for {token_address}: {exc}")
            return None

    def _native_price_from_coingecko(self, chain: str, token_address: str, warnings: list[str]) -> float | None:
        platform = COINGECKO_PLATFORMS.get(chain)
        if not platform:
            return None
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
        try:
            response = requests.get(
                url,
                params={"contract_addresses": token_address, "vs_currencies": "usd"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            price = data.get(token_address.lower(), {}).get("usd")
            return float(price) if price else None
        except Exception as exc:
            warnings.append(f"coingecko native price lookup failed for {chain}: {exc}")
            return None

    def _tx_fee_from_db(self, chain: str, wallet: str, tx_hash: str) -> float | None:
        normalized = _normalize_tx_hash(tx_hash)
        bare_hash = normalized[0] if normalized else str(tx_hash or "").lower().removeprefix("0x")
        for table in ("transaction_history_v2_bk", "transaction_history_v2"):
            conn = get_connection()
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute(
                    f"""
                    SELECT transaction_fee
                    FROM {table}
                    WHERE chain=%s
                      AND LOWER(wallet)=LOWER(%s)
                      AND REPLACE(LOWER(hash), '0x', '')=%s
                    ORDER BY date_time DESC
                    LIMIT 1
                    """,
                    (chain, wallet, bare_hash),
                )
                row = cursor.fetchone()
                if row and _as_float(row.get("transaction_fee")) > 0:
                    return _as_float(row.get("transaction_fee"))
            except mysql.connector.Error as exc:
                if exc.errno != 1146:
                    raise
            finally:
                cursor.close()
                conn.close()
        return None

    def _tx_fee_from_receipt(self, chain: str, tx_hash: str, warnings: list[str]) -> float | None:
        normalized = _normalize_tx_hash(tx_hash)
        if not normalized:
            warnings.append(f"invalid tx hash skipped for gas receipt lookup: {tx_hash}")
            return None
        bare_hash, receipt_hash = normalized
        key = (chain.upper(), bare_hash)
        if key in self._receipt_gas_cache:
            return self._receipt_gas_cache[key]
        try:
            w3 = web3_connection(chain)
            checksum_hash = Web3.to_hex(hexstr=receipt_hash)
            receipt = w3.eth.get_transaction_receipt(checksum_hash)
            gas_used = int(receipt.get("gasUsed", 0))
            gas_price = receipt.get("effectiveGasPrice")
            if gas_price is None:
                tx = w3.eth.get_transaction(checksum_hash)
                gas_price = tx.get("gasPrice", 0)
            fee = (gas_used * int(gas_price or 0)) / 10**18
            self._receipt_gas_cache[key] = fee
            return fee
        except Exception as exc:
            warnings.append(f"could not fetch gas receipt for {receipt_hash}: {exc}")
            return None

    def _write_csv(self, path: Path, records: list[dict[str, Any]]) -> None:
        fields = [
            "pool",
            "chain",
            "wallet_address",
            "token_id_chain",
            "root_token_id",
            "current_token_id",
            "initial_capital_usd",
            "current_position_value_usd",
            "current_unclaimed_usd",
            "current_value_usd",
            "gross_pnl_usd",
            "gas_cost_native",
            "gas_cost_usd",
            "net_pnl_usd_after_gas",
            "module_claimed_reward_usd",
            "summary_claimed_reward_reference_usd",
            "external_claimed_reward_estimate_usd",
            "claimed_fee_reference_usd",
            "net_pnl_usd_after_gas_and_module_reward",
            "basis_source",
            "wallet_summary_pnl_reference_usd",
            "wallet_summary_claimed_reference_usd",
            "status",
            "warnings",
        ]
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            for record in records:
                row = dict(record)
                row["token_id_chain"] = "->".join(str(x) for x in record.get("token_id_chain", []))
                row["warnings"] = " | ".join(record.get("warnings", []))
                writer.writerow(row)
