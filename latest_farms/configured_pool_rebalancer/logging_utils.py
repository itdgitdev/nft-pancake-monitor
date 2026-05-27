from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any


_SENSITIVE_KEY_PARTS = (
    "private_key",
    "raw_transaction",
    "raw_tx",
    "webhook",
    "secret",
)


def log_block(
    logger: logging.Logger,
    level: int,
    title: str,
    context: Mapping[str, Any] | None = None,
    fields: Mapping[str, Any] | None = None,
) -> None:
    logger.log(level, format_log_block(title, context=context, fields=fields))


def format_log_block(
    title: str,
    context: Mapping[str, Any] | None = None,
    fields: Mapping[str, Any] | None = None,
) -> str:
    context_parts = []
    for key, value in (context or {}).items():
        if _is_sensitive_key(key) or value is None:
            continue
        context_parts.append(f"{key}={_format_value(value)}")

    header = f"=== {str(title).upper()}"
    if context_parts:
        header += " | " + " | ".join(context_parts)
    header += " ==="

    body_lines = []
    for key, value in (fields or {}).items():
        if _is_sensitive_key(key):
            continue
        body_lines.append(f"- {key}: {_format_value(value)}")

    if not body_lines:
        return "\n" + header
    return "\n" + header + "\n" + "\n".join(body_lines)


def pool_context(pool, **extra: Any) -> dict[str, Any]:
    context = {
        "pool": getattr(pool, "name", None),
        "chain": getattr(pool, "chain", None),
    }
    context.update(extra)
    return context


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_format_value(item) for item in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(f"{key}={_format_value(val)}" for key, val in value.items())
        return "{" + items + "}"
    return str(value)


def _is_sensitive_key(key: str) -> bool:
    normalized = str(key).lower()
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)
