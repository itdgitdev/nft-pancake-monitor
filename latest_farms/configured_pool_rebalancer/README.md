# Configured Pool Rebalancer

Standalone worker for rebalancing configured users' own V3 LP positions across one or more configured pools.

It does not copy competitor ranges and does not modify the existing `parasite_bot` engines.

## Behavior

- Scans staked V3 NFT positions for configured pools.
- Filters positions by `managed_wallets`.
- Bootstraps tokenIds from the legacy `latest_farms/positions_cache/positions_cache_{CHAIN}.json` cache when enabled, then verifies every candidate on-chain.
- Skips in-range positions.
- For out-of-range positions, computes a new range, withdraws/collects, optionally swaps only recovered tokens, mints a new position, stakes it, then burns the old empty NFT when safe.
- By default the new range keeps the old width and recenters around current tick; when `rebalance_range.mode` is `price_percent`, the range is built around current price using lower/upper percentages.
- Defaults to dry-run. Use `--execute` only after reviewing the printed plan.

## Run

```powershell
python -m latest_farms.configured_pool_rebalancer.cli --config latest_farms/configured_pool_rebalancer/sample_config.json
```

## Config v2

The worker supports both the original per-pool config shape and the easier v2 shape used by `sample_config.json`.

In v2, shared wallet and pool settings are defined once:

```json
{
  "version": 2,
  "wallets": {
    "main": {
      "bot_wallet": "0x...",
      "private_key_env": "PARASITE_BOT_PRIVATE_KEY"
    }
  },
  "pool_defaults": {
    "dex_type": "pancake_v3_masterchef",
    "wallet": "main",
    "slippage_bps": 10,
    "max_swap_price_impact_pct": 0.5,
    "execute_burn": false
  },
  "pools": [
    {
      "name": "USDT-GENIUS",
      "chain": "BNB",
      "pool_address": "0x...",
      "pid": 554
    }
  ]
}
```

Each pool is expanded from the selected wallet alias, then `pool_defaults`, then the pool's own overrides. If `managed_wallets` is omitted, it defaults to `[bot_wallet]`, which is the normal single-signer setup. Use explicit `managed_wallets` only when the position owner list differs from the signer wallet.

## Local position bootstrap

For local runs, you do not need the server-maintained `latest_farms/positions_cache` file if you configure a bootstrap source.

The worker can discover staked positions in three ways:

- `seed_token_ids`: manually list known NFT tokenIds for a quick canary run.
- `bootstrap_start_block`: sweep MasterChef `Deposit`/`Withdraw` logs from this block on the first local run, then keep syncing incrementally from the module cache.
- `auto_bootstrap_start_block`: enabled by default; if no manual block is configured, the worker tries to find the V3 Factory `PoolCreated` block and uses it as the first sweep block.
- Legacy cache: copy `latest_farms/positions_cache/positions_cache_{CHAIN}.json` from a server and keep `use_legacy_position_cache=true`.

For a self-contained local config, prefer:

```json
{
  "use_legacy_position_cache": false,
  "pools": [
    {
      "name": "USDT-GENIUS",
      "chain": "BNB",
      "pool_address": "0x...",
      "pid": 554,
      "bootstrap_start_block": null,
      "auto_bootstrap_start_block": true
    }
  ]
}
```

If there is no module cache, no legacy cache, no `seed_token_ids`, no manual block, and automatic pool-created-block lookup fails, the first run intentionally skips historical logs and may not find old positions.

Create journal tables and run live:

```powershell
python -m latest_farms.configured_pool_rebalancer.cli --config path\to\config.json --migrate --execute
```

Generate a PnL report only:

```powershell
python -m latest_farms.configured_pool_rebalancer.cli --config path\to\config.json --pnl-report
```

The PnL report reads existing data from `wallet_nft_position`, `wallet_nft_summary`, and `configured_rebalance_jobs`. It writes:

- `latest_farms/logs/configured_rebalancer_pnl.json`
- `latest_farms/logs/configured_rebalancer_pnl.csv`

For net PnL after gas in USD, the report fetches the native token market price dynamically. Lookup order is PancakeSwap price API, DexScreener, then CoinGecko.

Optional Discord PnL notification after live rebalance:

```json
{
  "discord": {
    "enabled": true,
    "webhook_url_env": "CONFIGURED_REBALANCER_DISCORD_WEBHOOK",
    "pnl_delay_seconds": 90,
    "notify_pending_if_snapshot_missing": false
  }
}
```

Set the webhook URL in PowerShell before running live:

```powershell
$env:CONFIGURED_REBALANCER_DISCORD_WEBHOOK="https://discord.com/api/webhooks/..."
```

The worker waits `pnl_delay_seconds` after remint/stake, then runs the in-memory PnL reporter and sends the matching record to Discord. If the new NFT snapshot is still missing, it retries the PnL notification on later worker cycles. Pending notifications are disabled by default.

Optional chain-specific gas policy:

```json
{
  "gas_policy": {
    "BNB": {
      "mode": "fixed",
      "gas_price_gwei": 0.05,
      "max_fee_gwei": 0.08
    },
    "BAS": {
      "mode": "eip1559",
      "base_fee_multiplier": 1.2,
      "priority_fee_cap_gwei": 0.002,
      "swap_priority_fee_cap_gwei": 0.005,
      "max_fee_gwei": 0.05
    }
  }
}
```

If omitted, the worker defaults to a low fixed BNB gas policy and a capped dynamic EIP-1559 Base policy. Other chains keep the generic EIP-1559 behavior.

You can optionally configure a fallback price for local/offline runs:

```json
{
  "pnl": {
    "native_prices_usd": {
      "BNB": 590.0
    }
  }
}
```

Optional price-percent rebalance range per pool:

```json
{
  "rebalance_range": {
    "mode": "price_percent",
    "lower_percent": -9.0,
    "upper_percent": 20.0
  }
}
```

This means the new range is approximately current price minus 9% to current price plus 20%. If omitted, the worker tries to infer the original percent band from the module journal or the first `wallet_nft_position` snapshot, then falls back to the old center-width behavior.

Configured percentages are treated as the target strategy. After minting, the journal stores the actual lower/upper percentages derived from the aligned minted ticks, so future fallback behavior reflects the real on-chain range.

## Notes

- V1 implements PancakeSwap V3 MasterChef style staking.
- Aerodrome adapter is intentionally reserved for the next phase.
- `use_legacy_position_cache` is enabled by default to avoid a large first log sweep. Use `seed_token_ids` for canary runs or for positions missing from the legacy cache.
- Live execution requires `bot_wallet` to be the owner/user of the position being rebalanced. For multiple user wallets on the same pool, create one pool config entry per signer/private key.
- `--pnl-report` is read-only from the database perspective: it does not rebalance, migrate, or send transactions.
- Discord notifications are disabled for dry-run and are not sent by `--pnl-report`.
- `pnl.native_prices_usd` is only a fallback if market price APIs are unavailable.
