# Binance Futures Monitor

Interactive local worker for reading Binance USD-M and COIN-M positions and
publishing current-state data to MySQL. A module-local `.env` stores the full API
key and the secret key without its final 10 characters. The missing suffix is
prompted with hidden terminal input and the full secret exists in process memory
only.

See [OPERATIONS_GUIDE_VI.md](OPERATIONS_GUIDE_VI.md) for the detailed Vietnamese
security report, Binance API setup, IP restriction, and job runbook.
See [OPERATIONS_GUIDE_V2_VI.md](OPERATIONS_GUIDE_V2_VI.md) for the concise V2
operations and security runbook.

## Configure credentials

```powershell
Copy-Item `
  latest_farms\binance_futures_monitor\.env.example `
  latest_farms\binance_futures_monitor\.env
```

For every configured account, store the full API key and secret prefix using the
normalized account alias shown in `.env.example`. Never store the final 10 secret
characters in this file. The module reads this file directly without adding its
values to `os.environ`.

## Run

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --migrate `
  --loop
```

The machine running this process must use an outbound IP allowed by each
Binance API key. Restarting the process requires entering each account's final
10 secret characters again.

The configured account alias is a local stable name, not a Binance UID. Keep
the alias unchanged when rotating API keys.
