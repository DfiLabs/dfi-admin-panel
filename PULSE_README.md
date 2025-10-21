## Pulse – Runtime, Data Flow, and Operations

This document captures how Pulse works end‑to‑end today (after the entry/alias/path fixes), the exact files/paths it relies on, and how to deploy, verify, and roll back safely.

### High‑level
- Frontend: static HTML served at `https://pulse.dfi-labs.com/signal-dashboard/index.html`.
- Data sources: S3 in bucket `dfi-signal-dashboard` behind CloudFront. The Pulse distribution maps `/signal-dashboard/*` to the S3 origin path `/descartes-ml` (important for URLs vs actual S3 keys).
- Independence: Pulse does not read Admin paths. It uses its own strategy prefix and environment root.

### Paths and ownership (Pulse)
- Request URLs used by the browser (do not include `/descartes-ml`):
  - Strategy prefix (CSV + baseline): `/signal-dashboard/data/combined_descartes_unravel/`
    - `latest.json` – points to the active CSV filename
    - `pre_execution.json` – contains `pv_pre` and `executed_at_utc`
    - `daily_baseline.json` – contains `weights`, `ref_prices` (entry), `universe`, `executed_at_utc`
  - Environment root (shared across strategies): `/signal-dashboard/data/`
    - `latest_prices.json` – current Binance Futures USDT‑perp mark prices (per minute)
    - `portfolio_value_log.jsonl` – time series of PV, daily P&L, total P&L

- Backing S3 keys (what gets written/read under the hood):
  - `descartes-ml/signal-dashboard/data/combined_descartes_unravel/*`
  - `descartes-ml/signal-dashboard/data/*`

### Services
1) Price Writer (per minute)
   - Fetches all Binance Futures USDT perpetual mark prices.
   - Writes Pulse copy to: `descartes-ml/signal-dashboard/data/latest_prices.json`.

2) Execution Orchestrator (S3 PutObject trigger on strategy CSV uploads)
   - Trigger: CSV uploaded to `descartes-ml/signal-dashboard/data/combined_descartes_unravel/*.csv`.
   - Actions:
     - Parse/normalize CSV (unicode minus, `%`, trims, uppercases tickers).
     - Build `daily_baseline.json` with:
       - `ref_prices`: Binance snapshot at `executed_at_utc` (entry), numeric.
       - `weights`: from CSV.
       - `universe`: normalized tickers.
     - Write `pre_execution.json` with `pv_pre` (from env PV log) and `executed_at_utc`.
     - Append zero‑PnL execution anchor to `portfolio_value_log.jsonl` (same timestamp, `daily_pnl=0`, `portfolio_value=pv_pre`).
     - Send SES email with diagnostics: counts, alias usage, missing marks/entries, and sources.
     - Notes: 1000‑perp aliasing unified (e.g., `SHIBUSDT <-> 1000SHIBUSDT`, `PEPEUSDT <-> 1000PEPEUSDT`).

3) PV Logger (per minute)
   - Reads baseline from strategy prefix and prices from env root.
   - Computes PV and daily P&L; appends to `portfolio_value_log.jsonl`.
   - Deviation guard is bypassed for the first tick after an execution anchor to allow the reset.

### Frontend behavior
- Prefixes (Pulse):
  - `DATA_PREFIX = /signal-dashboard/data/`
  - `CSV_PREFIX  = /signal-dashboard/data/combined_descartes_unravel/`
- Flow:
  1) Load `latest.json` → active CSV filename
  2) Load CSV → positions
  3) Load `daily_baseline.json` → Entry Price map (`ref_prices`)
  4) Load `latest_prices.json` → Mark Price map
  5) Load `portfolio_value_log.jsonl` → charts and card P&L
- Symbol handling:
  - Normalize: trim + uppercase + remove `_`/`-` before lookups.
  - Aliasing: forward and reverse for 1000‑tickers (e.g., `SHIBUSDT ↔ 1000SHIBUSDT`).
  - Positions count: does not drop rows when a price is missing; rendering shows `-` or `Loading…` until available.

### Deploy and invalidate (frontend)
- Upload Pulse index:
  - Primary: `s3://dfi-signal-dashboard/descartes-ml/signal-dashboard/index.html`
  - Some workflows also mirror: `s3://dfi-signal-dashboard/signal-dashboard/index.html`
- Invalidate CloudFront (Pulse):
  - Paths: `/signal-dashboard/*` (and optionally `/descartes-ml/*`)

### Runbook – execution test
1) Upload CSV to: `s3://dfi-signal-dashboard/descartes-ml/signal-dashboard/data/combined_descartes_unravel/`.
2) Confirm orchestrator results:
   - `latest.json` points to the new CSV.
   - `daily_baseline.json` has `executed_at_utc ~ now` and `keys` equals the CSV symbol count.
   - Zero‑PnL anchor present at the same timestamp in `portfolio_value_log.jsonl`.
3) Hard refresh Pulse with `?diag`:
   - Console should show `CSV_PREFIX = /signal-dashboard/data/combined_descartes_unravel/`.
   - `baseline.universe_count` matches CSV rows; sample shows entries.
   - `latest_prices symbols` is large (500+).

### Rollback and versioning

Option A – Git tag (recommended)
1) Tag this exact repo state and push:
   - `git tag -a pulse-2025-10-21-stable -m "Pulse stable after entry/path fixes"`
   - `git push origin --tags`
2) To roll back code, checkout the tag and redeploy HTML/Lambdas from that ref.

Option B – S3 object versioning (frontend)
1) Ensure bucket versioning is enabled for `dfi-signal-dashboard`.
2) List versions and restore a prior HTML:
   - `aws s3api list-object-versions --bucket dfi-signal-dashboard --prefix descartes-ml/signal-dashboard/index.html`
   - `aws s3api copy-object --bucket dfi-signal-dashboard --key descartes-ml/signal-dashboard/index.html --copy-source dfi-signal-dashboard/descartes-ml/signal-dashboard/index.html?versionId=PREV_VERSION`
3) Invalidate Pulse CF: `/signal-dashboard/*`.

Option C – Lambda versions and aliases (backend)
1) Publish versions of both Lambdas after a known‑good deploy:
   - `aws lambda publish-version --function-name pulse-pv-logger`
   - `aws lambda publish-version --function-name execution-orchestrator`
2) Point alias `prod` at the published version:
   - `aws lambda update-alias --function-name pulse-pv-logger --name prod --function-version X`
   - `aws lambda update-alias --function-name execution-orchestrator --name prod --function-version Y`
3) Rollback = re‑point `prod` to a previous good version.

### Troubleshooting quick refs
- 404 for `daily_baseline.json` or `pre_execution.json` in Pulse:
  - Ensure client uses `/signal-dashboard/...` (not `/descartes-ml/...`). CF already maps to `/descartes-ml` at the origin.
- Entry shows `-` for a symbol that is in CSV:
  - Confirm `baseline.universe_count` and that `ref_prices` includes the symbol (normalized). Check 1000‑ticker aliasing.
- Daily P&L didn’t reset at execution:
  - Verify anchor in `portfolio_value_log.jsonl` at `executed_at_utc` and that PV logger wrote the first tick after anchor.

### Invariants
- After execution: `daily_pnl` resets to 0 at the anchor; thereafter PV evolves with per‑minute logs.
- Entry Price in UI is sourced only from `daily_baseline.ref_prices` for consistency.
- Pulse and Admin are independent: no cross‑reads.

### Contact points
- S3 bucket: `dfi-signal-dashboard`
- Pulse CloudFront distribution: maps `/signal-dashboard/*` → origin path `/descartes-ml`.


