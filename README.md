# ALTCOIN_RADAR

Stage A is a read-only CoinGecko spot-market scanner. It fetches the configured watchlist, records market data, and mechanically flags coins whose absolute 1-hour or 24-hour price change is at least 10%.

It does **not** trade, connect to an exchange account or wallet, produce price predictions, or provide buy/sell advice.

## Setup

1. Create a Python virtual environment if desired.
2. Install dependencies: `python -m pip install -r requirements.txt`
3. Optionally copy `.env.example` to `.env`, then load `COINGECKO_API_KEY` into the process environment. The scanner never reads trading credentials.

PowerShell example:

```powershell
$env:COINGECKO_API_KEY = "your-demo-key"
python radar_scan.py
```

An unauthenticated request is attempted when the environment variable is absent; CoinGecko may rate-limit this mode.

## Outputs

- `output/latest_snapshot.json`: raw numeric market fields plus calculated 24h range and turnover ratio.
- `output/latest_triggers.json`: every coin meeting any ±10% 1h/24h rule.
- `output/latest_top_movers.json`: top 10 gainers and losers for 1h and 24h.
- `output/mapping_review.json`: ambiguous or unconfirmed symbols requiring manual review.
- `output/scan_status.json`: coverage and assertion status.

`NO NEW PRICE THRESHOLD SIGNAL` is printed only when all mapped assets were returned by the API.

## Persistent signal state (Stage A.2)

After a complete Stage A scan, initialize the state machine once and explicitly suppress notifications for signals that already exist:

```powershell
python radar_state.py --bootstrap
```

Subsequent evaluations use:

```powershell
python radar_state.py
```

State is updated only when Stage A reports `MARKET_DATA_COMPLETE` and returns every mapped asset. Incomplete or failed scans produce `STATE_UPDATE_SKIPPED` and leave `state/radar_state.json` unchanged. Notification candidates are limited to new triggers, severity-tier escalations, direction changes, and genuine reentries.

The canonical release contains 165 fully mapped assets. A newly added asset, or a
same-symbol project replacement listed in `config/identity_migrations.json`, receives
a one-asset baseline on its first complete scan. That baseline is propagated to the
technical and divergence stages so the migration cannot manufacture `NEW_TRIGGER`,
technical, or `DIVERGENCE_NEW` notifications. Unchanged assets retain their episode,
technical, divergence-signature, and delivery state; a global bootstrap is not used.

## Tests

```powershell
python -m unittest discover -s tests -v
```

## Cloud-readiness entrypoint

Run the complete fail-closed pipeline with one command:

```powershell
python run_pipeline.py
python health_check.py
```

`run_pipeline.py` prevents overlapping local runs, recovers a lock older than one hour,
records an atomic run ledger in `state/run_status.json`, and updates
`state/heartbeat.json` only after every stage succeeds. `cloud_state_manifest.json`
lists the files that a future cloud host must keep in durable storage. A CI cache is
not an authoritative state store.

The production workflow is `.github/workflows/altcoin-radar.yml`. It runs every 15
minutes in UTC and can also be started manually. Runtime state is restored from and
written back to the separate `radar-state` branch. The workflow fails closed when the
pipeline, health check, or durable-state push fails. This heartbeat is internal; an
independent external dead-man monitor is still required.

## Importing future CoinMarketCap additions

`import_cmc_watchlist.py` accepts a JSON array (or an object containing `items`, `watchlist`, `cryptoCurrencyList`, or `data`) and appends only confirmed new assets:

```powershell
python import_cmc_watchlist.py cmc_watchlist.json --dry-run
python import_cmc_watchlist.py cmc_watchlist.json
```

Each input asset must contain `symbol`. Include `coingecko_id` whenever it is confirmed; optional identity fields are `name`, `canonical_asset`, and `cmc_id`. The importer reports `IGNORE_DUPLICATE`, `NEW_ASSET`, or `NEEDS_REVIEW`. It never deletes an existing asset and never overwrites a same-symbol conflict.

## Trigger enrichment (Stage B.1)

Stage B.1 runs only after a complete Stage A scan and state evaluation. It analyzes only currently active price-trigger assets, using public read-only Spot data from the allowed exchanges in `config/exchange_priority.json`:

```powershell
python radar_scan.py
python radar_state.py
python stage_b_enrich.py --bootstrap
python stage_b_enrich.py
```

The bootstrap saves current RSI, volume, source, range, turnover, and cross-exchange context while suppressing technical notifications. HTX and Upbit are explicitly excluded. Stage B.1 does not calculate divergence, send notifications, access an exchange account, or place trades.

## Confirmed RSI divergence (Stage B.2)

Stage B.2 uses only completed Daily and Weekly candles from the Stage B.1 history source. Initialize once, then evaluate normally:

```powershell
python stage_b_divergence.py --bootstrap
python stage_b_divergence.py
```

The detector uses configured confirmed pivots, Wilder RSI(14), price/RSI noise filters, pivot separation, persistent signatures, and atomic state replacement. It does not calculate tentative candles as confirmed divergence and does not send trading or notification messages.

## Telegram notification delivery (implemented, not yet activated)

`notification_merger.py` deterministically combines the current run's Price,
Technical, and Divergence notification events into at most one Telegram-sized
message. Events for the same symbol share one block. A run without a new event
returns `NO_NOTIFICATION`; `CONTINUING` price events are suppressed.

`notification_dispatcher.py` provides the delivery interface and reads
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` only from the process environment.
`notification_delivery.py` stores the durable at-most-once delivery ledger in
`state/notification_delivery.json`. Timeout or an interrupted durable `SENDING`
claim becomes `UNKNOWN_TIMEOUT` and is never retried automatically. Only an
explicit `FAILED_RETRYABLE` result may be claimed again.

Delivery is intentionally not connected to the production workflow until the
two repository secrets have been configured and a real Telegram send has been
verified. No credential value belongs in `.env.example`, source files, logs, or
the public repository. Healthchecks.io integration follows that verification.
