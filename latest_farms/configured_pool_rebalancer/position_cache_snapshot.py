from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from latest_farms.create_db import get_connection
except ImportError:  # pragma: no cover
    from create_db import get_connection


log = logging.getLogger("configured_pool_rebalancer")
DEFAULT_CACHE_DIR = "latest_farms/positions_cache"
DEFAULT_SOURCE = "positions_cache"
SNAPSHOT_TABLE = "configured_position_cache_snapshots"


def migrate_position_cache_snapshot_table(cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            chain VARCHAR(20) NOT NULL,
            source VARCHAR(80) NOT NULL,
            snapshot_json LONGTEXT NOT NULL,
            last_synced_block BIGINT NOT NULL DEFAULT 0,
            position_count INT NOT NULL DEFAULT 0,
            content_hash CHAR(64) NOT NULL,
            file_mtime DATETIME NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uniq_chain_source (chain, source),
            KEY idx_chain_updated (chain, updated_at)
        )
        """
    )


def push_position_cache_snapshot(
    chain: str,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
    source: str = DEFAULT_SOURCE,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    logger = logger or log
    chain_key = str(chain).upper()
    cache_path = Path(cache_dir) / f"positions_cache_{chain_key}.json"
    raw_text = cache_path.read_text(encoding="utf-8")
    data = json.loads(raw_text)
    positions = data.get("positions")
    if not isinstance(positions, dict):
        raise ValueError(f"{cache_path} must contain object field 'positions'")
    last_synced_block = int(data.get("last_synced_block") or 0)
    content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime, timezone.utc).replace(tzinfo=None)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        migrate_position_cache_snapshot_table(cursor)
        cursor.execute(
            f"""
            SELECT content_hash
            FROM {SNAPSHOT_TABLE}
            WHERE chain=%s AND source=%s
            """,
            (chain_key, source),
        )
        row = cursor.fetchone()
        current_hash = _row_get(row, "content_hash", 0) if row else None
        if current_hash == content_hash:
            conn.commit()
            logger.info(
                "[%s] position cache snapshot unchanged source=%s block=%s positions=%s hash=%s",
                chain_key,
                source,
                last_synced_block,
                len(positions),
                content_hash[:12],
            )
            return {
                "chain": chain_key,
                "source": source,
                "status": "skipped",
                "last_synced_block": last_synced_block,
                "position_count": len(positions),
                "content_hash": content_hash,
            }

        cursor.execute(
            f"""
            INSERT INTO {SNAPSHOT_TABLE} (
                chain, source, snapshot_json, last_synced_block, position_count,
                content_hash, file_mtime, created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                snapshot_json=VALUES(snapshot_json),
                last_synced_block=VALUES(last_synced_block),
                position_count=VALUES(position_count),
                content_hash=VALUES(content_hash),
                file_mtime=VALUES(file_mtime),
                updated_at=VALUES(updated_at)
            """,
            (
                chain_key,
                source,
                raw_text,
                last_synced_block,
                len(positions),
                content_hash,
                file_mtime,
                now,
                now,
            ),
        )
        conn.commit()
        logger.info(
            "[%s] position cache snapshot updated source=%s block=%s positions=%s hash=%s",
            chain_key,
            source,
            last_synced_block,
            len(positions),
            content_hash[:12],
        )
        return {
            "chain": chain_key,
            "source": source,
            "status": "updated",
            "last_synced_block": last_synced_block,
            "position_count": len(positions),
            "content_hash": content_hash,
        }
    finally:
        cursor.close()
        conn.close()


def fetch_position_cache_snapshot(chain: str, source: str = DEFAULT_SOURCE) -> dict[str, Any] | None:
    chain_key = str(chain).upper()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        migrate_position_cache_snapshot_table(cursor)
        cursor.execute(
            f"""
            SELECT snapshot_json, last_synced_block, position_count, updated_at, content_hash
            FROM {SNAPSHOT_TABLE}
            WHERE chain=%s AND source=%s
            """,
            (chain_key, source),
        )
        row = cursor.fetchone()
        if not row:
            return None
        raw_json = _row_get(row, "snapshot_json", 0)
        data = json.loads(raw_json)
        if not isinstance(data.get("positions"), dict):
            return None
        return {
            "chain": chain_key,
            "source": source,
            "snapshot": data,
            "last_synced_block": int(_row_get(row, "last_synced_block", 1) or 0),
            "position_count": int(_row_get(row, "position_count", 2) or 0),
            "updated_at": _row_get(row, "updated_at", 3),
            "content_hash": _row_get(row, "content_hash", 4),
        }
    finally:
        cursor.close()
        conn.close()


def _row_get(row, key: str, index: int):
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    return row[index]
