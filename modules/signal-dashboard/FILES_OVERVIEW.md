DFI Signal Dashboard — File Inventory and Roles

Scope: Every file under `modules/signal-dashboard/` with a concise, high‑signal description of purpose, inputs/outputs, and how it fits in the system.

Top level

- dashboard.html
  - Single‑page dashboard application. Fetches S3 artifacts (portfolio_value_log.jsonl, latest.json, daily_baseline.json, latest_prices.json) and renders metrics, positions, and the time‑series chart (Chart.js). Supports strategy switching via DATA_PREFIX; Unravel (`/signal-dashboard/data/`) and Beta (`/signal-dashboard/descartes-beta/data/`). Prefers S3 as single source of truth.

- styles.css
  - Global visual design for the dashboard (cards, charts, tables, typography, responsive rules). Contains component‑level styles for summary cards, chart header, controls, positions table, top performers, etc.

- LOGIC_CHANGE_SPEC.txt
  - Design document describing the S3‑driven architecture, data contracts (what each S3 object contains), formulas used by the UI, and rollout/validation plan.

- assets/images/dfi-labs-logo.png
  - UI asset used in the dashboard header.

- .DS_Store
  - macOS metadata file. Ignored by the application; safe to delete.

scripts/ — operational tools (EC2, Lambda, S3) and analysis

- csv-monitor-email.py
  - EC2 systemd service script that watches a local CSV directory, picks the latest `*2355.csv`, copies it to `/tmp`, uploads it to S3 (`S3_KEY_PREFIX`), writes `latest.json`, optionally stamps the dashboard HTML, and writes audit artifacts:
    - pre_execution_pending.json (at CSV detection)
    - daily_baseline.json (post‑execution baseline of prices/PV)
    - portfolio_daily_log.csv (appends pre/post rows)
  - Also contains a detector‑only 1‑minute timeseries writer (optional; behind PV_WRITER_ENABLED). Uses `EmailNotifier` for a once‑per‑day summary mail.
  - Key env:
    - CSV_DIRECTORY (local folder to watch)
    - S3_BUCKET_NAME (default dfi-signal-dashboard)
    - S3_KEY_PREFIX (e.g., `signal-dashboard/data/` or `signal-dashboard/descartes-beta/data/`)

- deploy_daily_executor.py
  - Deploys/updates the “daily-executor” Lambda that finalizes daily execution and snapshots. Sets env (e.g., S3_PREFIX) and configures an EventBridge schedule.

- execute_daily_trades.py
  - Core daily execution logic used by the daily executor workflow. Reads latest CSV, computes P&L and post‑execution snapshots, and writes: `pre_execution.json`, `daily_baseline.json`, `portfolio_value_log.jsonl` entry where relevant. Parameterized by `S3_PREFIX`.

- sync_calculations.py
  - Creates a synchronized calculation snapshot combining S3 baselines and live/current prices to confirm math used in the UI. Parameterized by `S3_PREFIX`.

- reset_portfolio_value.py
  - Utility to reset portfolio value artifacts to a known baseline (e.g., $1,000,000 day‑1) for a given S3 prefix. Used during environment initialization or re‑seeding.

- update_lambda_pv_logger.py
  - Builds/deploys the PV logger Lambda (minute cadence writer of `portfolio_value_log.jsonl`). Passes env (e.g., S3_KEY_PREFIX) and updates configuration.

- trigger_pv_logger.py
  - Manually invokes the PV logger Lambda for immediate testing.

- write_latest_prices.py / latest_prices_writer.py
  - Price writer utilities that publish `latest_prices.json` to S3 in the format the dashboard expects (`{"timestamp_utc","prices":{SYMBOL: price}}`). Used for ad‑hoc or scheduled runs.

- execution_email_notifier.py
  - Composes and sends an HTML summary email of the day’s execution using the same S3 artifacts the UI reads; leverages `email_notifier.py`.

- email_notifier.py
  - Small helper class used by scripts to send emails (with once‑per‑day guard/dedup behavior).

- investigate_last_night.py
  - Forensics helper to inspect PV logs around the execution window (near midnight UTC). Prints focused windows to validate continuity.

- verify_all_calculations.py
  - End‑to‑end verification harness. Loads S3 artifacts, recomputes key metrics (per‑row, totals), and asserts identities used by the UI. Useful for regression checks.

- audit_calculations.py
  - Computes/compares per‑symbol and aggregated P&L using baseline vs current marks to audit the numbers shown on the dashboard, with logging for discrepancies.

- prove_s3_consistency.py
  - Confirms the “single source of truth” premise by comparing values retrieved from S3 (backend) to what the dashboard displays.

- fix_system_now.py / verify_system_now.py
  - Small ops utilities to verify or correct system time/clock skew on the EC2 box (so minute‑cadence writers and cutoff windows align). Useful when detector timestamps look off.

- test_next_execution.py
  - Helper to compute/print the next nightly execution/check window (e.g., 23:55–01:25 UTC) for the monitor service.

- start_pv_logger.sh
  - Convenience shell wrapper to start a local/simple PV logger (or to bootstrap a process), primarily used during earlier experiments or local testing.

- update_lambda_pv_logger.py (listed above)

- daily-executor-inline.zip
  - Zipped Lambda payload used by the `deploy_daily_executor.py` script for fast inline deployments (no external build step).

- __pycache__/*.pyc
  - Python bytecode caches; not part of the runtime logic. Can be ignored/cleaned.

s3_reports/ — reference snapshots and samples

- s3_reports/s3_snapshot.json
  - Example manifest of S3 keys for audit/debug (what exists under a prefix at a point in time).

- s3_reports/tmp/portfolio_value_log.jsonl
  - Sample PV timeseries file in JSONL format used for local testing.

- s3_reports/tmp/latest_prices.json
  - Sample of the atomic price document consumed by the UI and by some scripts.

- s3_reports/tmp/portfolio_daily_log.csv
  - Sample of daily pre/post execution log file (used by monitor/emailer and for analytics).

- s3_reports/tmp/latest.json
  - Sample of the “which CSV is current” pointer file (as written by the monitor) used by the dashboard.

- s3_reports/tmp/daily_baseline.json
  - Sample of the post‑execution daily baseline (prices at the reset moment and PV), used as the day’s reference for per‑row and daily P&L.

Operational notes

- Strategy separation
  - Unravel prefix: `signal-dashboard/data/`
  - Beta prefix: `signal-dashboard/descartes-beta/data/`
  - The same scripts work for both by changing `S3_KEY_PREFIX`/`S3_PREFIX` and, on EC2, `CSV_DIRECTORY` in the corresponding systemd env file.

- Authoritative S3 objects the UI depends on
  - `portfolio_value_log.jsonl` — drives PV chart and PV card (latest point)
  - `latest.json` — tells the UI which CSV to load for positions
  - `daily_baseline.json` — prices and PV at daily reset, used for per‑row/daily P&L
  - `latest_prices.json` — current marks for table and intraday refreshes

- Typical flow (daily)
  1) Monitor detects the `*2355.csv` ➜ uploads to S3 and writes `latest.json`
  2) Daily executor runs ➜ writes `pre_execution.json`, `daily_baseline.json`, appends logging
  3) PV logger writes a point each minute ➜ `portfolio_value_log.jsonl`
  4) Dashboard reads only S3 ➜ positions + PV/metrics remain consistent and auditable





