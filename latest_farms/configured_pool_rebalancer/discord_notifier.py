from __future__ import annotations

import os
from typing import Any

import requests

from .models import WorkerConfig


def _fmt_usd(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"${float(value):,.4f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_value(value: Any) -> str:
    if value is None:
        return "N/A"
    return str(value)


def _short_address(value: Any) -> str:
    text = str(value or "")
    if len(text) <= 12:
        return text or "N/A"
    return f"{text[:6]}...{text[-4:]}"


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class DiscordNotifier:
    def __init__(self, config: WorkerConfig):
        self.config = config

    def enabled(self) -> bool:
        return bool(self.config.discord_enabled and self.webhook_url())

    def webhook_url(self) -> str:
        return os.getenv(self.config.discord_webhook_url_env, "").strip()

    def send(self, message: str) -> None:
        webhook_url = self.webhook_url()
        if not webhook_url:
            raise RuntimeError(f"missing Discord webhook env {self.config.discord_webhook_url_env}")
        if webhook_url.startswith("https://discordapp.com/"):
            webhook_url = webhook_url.replace("https://discordapp.com/", "https://discord.com/", 1)
        response = requests.post(webhook_url, json={"content": message[:1900]}, timeout=15)
        if response.status_code >= 300:
            raise RuntimeError(f"discord webhook failed {response.status_code}: {response.text[:300]}")

    def pnl_message(self, record: dict[str, Any]) -> str:
        warnings = record.get("warnings") or []
        token_chain = " -> ".join(str(x) for x in record.get("token_id_chain", []))
        nft_text = token_chain or f"{record.get('root_token_id')} -> {record.get('current_token_id')}"
        lines = [
            "**Configured Rebalancer PnL**",
            f"Pool: `{record.get('pool')}` | Chain: `{record.get('chain')}`",
            f"Wallet: `{_short_address(record.get('wallet_address'))}`",
            f"NFT: `{nft_text}`",
            "",
            f"Initial: `{_fmt_usd(record.get('initial_capital_usd'))}`",
            f"Current LP: `{_fmt_usd(record.get('current_position_value_usd'))}`",
            f"Gas: `{_fmt_usd(record.get('gas_cost_usd'))}`",
            f"Module reward: `{_fmt_usd(record.get('module_claimed_reward_usd'))}`",
            "",
            f"Net PnL: `{_fmt_usd(record.get('net_pnl_usd_after_gas'))}`",
            f"Net incl. reward: `{_fmt_usd(record.get('net_pnl_usd_after_gas_and_module_reward'))}`",
            f"Status: `{record.get('status')}`",
        ]
        if _as_float(record.get("current_unclaimed_usd")) > 0:
            lines.insert(6, f"Current unclaimed: `{_fmt_usd(record.get('current_unclaimed_usd'))}`")
        if _as_float(record.get("summary_claimed_reward_reference_usd")) > 0:
            lines.append(f"Summary reward reference: `{_fmt_usd(record.get('summary_claimed_reward_reference_usd'))}`")
        if _as_float(record.get("external_claimed_reward_estimate_usd")) > 0:
            lines.append(f"External reward estimate: `{_fmt_usd(record.get('external_claimed_reward_estimate_usd'))}`")
        if _as_float(record.get("claimed_fee_reference_usd")) > 0:
            lines.append(f"Claimed fee reference: `{_fmt_usd(record.get('claimed_fee_reference_usd'))}`")
        if warnings:
            lines.append("Warnings:")
            lines.extend(f"- {item}" for item in warnings[:5])
        return "\n".join(lines)

    def pending_message(self, pool_name: str, chain: str, wallet: str, old_token_id: int, new_token_id: int) -> str:
        return "\n".join(
            [
                "**Configured Rebalancer PnL Pending**",
                f"Pool: `{pool_name}` | Chain: `{chain}`",
                f"Wallet: `{wallet}`",
                f"NFT: `{old_token_id}` -> `{new_token_id}`",
                "Rebalance completed, but `wallet_nft_position` has not indexed the new NFT snapshot yet.",
                "PnL will be retried on the next worker cycle.",
            ]
        )

    def recovery_required_message(
        self,
        pool_name: str,
        chain: str,
        wallet: str,
        token_id: int,
        reason: str,
    ) -> str:
        return "\n".join(
            [
                "**Configured Rebalancer Recovery Required**",
                f"Pool: `{pool_name}` | Chain: `{chain}`",
                f"Wallet: `{wallet}`",
                f"Old NFT: `{token_id}`",
                f"Reason: `{reason[:900]}`",
                "",
                "Worker stopped before swap/mint to avoid using the wrong token balance.",
                "Manual review is required before this job can be closed or resumed.",
            ]
        )

    def inactive_farm_message(
        self,
        pool_name: str,
        chain: str,
        wallet: str,
        pid: int,
        alloc_point: int,
        token_ids: list[int],
    ) -> str:
        nft_text = ", ".join(str(token_id) for token_id in token_ids[:10]) or "N/A"
        if len(token_ids) > 10:
            nft_text = f"{nft_text}, ... (+{len(token_ids) - 10} more)"
        return "\n".join(
            [
                "**Configured Rebalancer Farm Inactive**",
                f"Pool: `{pool_name}` | Chain: `{chain}`",
                f"Wallet: `{wallet}`",
                f"PID: `{pid}`",
                f"allocPoint: `{alloc_point}`",
                f"Staked NFTs: `{nft_text}`",
                "",
                "Farm reward appears inactive. Worker will keep normal rebalance behavior for fee optimization.",
                "Withdraw manually if you no longer want to keep this LP.",
            ]
        )
