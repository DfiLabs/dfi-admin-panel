<!-- a18bac78-e399-4c6c-a2ed-aabcc9373438 30438f40-6377-400b-97e8-81ac8f0eeebf -->
# Pulse Final Stabilization + Execution Plan

## Current Problems

- pv_pre is wrong on Pulse because an intraday mirror overwrote `pre_execution.json` from Admin.
- CSV pointer must always reflect the newest Unravel CSV; Pulse must auto-update like Admin.
- Frontend needs robust guards: use Pulse prefix, cache-busting, tolerant `latest_prices.json` parsing, safe symbol resolution.
- No daily executor yet to write pv_pre + baseline at 00:30 UTC.

## Target State

- 7D/1M/SI use EOD PV from `eod_pv.csv`; 1D uses intraday.
- PV(t) = pv_pre (yesterday EOD) + Daily P&L; SI P&L = PV − 1,000,000.
- `pulse-csv-mirror` copies the newest Unravel CSV to Pulse and writes `latest.json` when Admin uploads.
- `pulse-daily-executor` writes `pre_execution.json`, `daily_baseline.json`, and (fallback) `latest.json` at 00:30 UTC.
- Intraday mirror excludes `pre_execution.json` and `daily_baseline.json` so pv_pre cannot be overwritten again.
- Alarms confirm freshness of prices and daily outputs.

## Steps (with proofs)

1) Lock mirrors (prevent overwrite)

- Ensure `pulse-intraday-mirror` KEYS = `latest.json,latest_prices.json` only.
- Proof: `aws lambda get-function-configuration` env shows KEYS without `pre_execution.json`/`daily_baseline.json`.

2) Re-apply today’s pv_pre and guard

- Write `/signal-dashboard/data/pre_execution.json` with:
```json
{ "timestamp_utc": "2025-10-13T00:30:00Z", "pv_pre": 1375335.19, "csv_filename": "lpxd_external_advisors_DF_20251012-2355.csv" }
```

- CF invalidate `/signal-dashboard/data/pre_execution.json`.
- Proof: CloudFront link returns pv_pre=1375335.19; dashboard PV ≈ pv_pre + Daily.

3) Implement `pulse-daily-executor` (00:30 UTC)

- Input: `eod_pv.csv`, Unravel CSV bucket/prefix, price source.
- Outputs (to Pulse path):
  - `pre_execution.json` (pv_pre=yesterday EOD; timestamp_utc).
  - `daily_baseline.json` (00:30 prices for universe; include `pv_pre`).
  - `latest.json` (today CSV pointer, `updated_utc`, `source:'unravel'`).
- Headers: `Cache-Control: no-store`.
- Schedule: EventBridge `cron(30 0 * * ? *)`.
- Proof: S3 LastModified ≈ 00:30Z; CloudWatch logs show keys written.

4) Implement `pulse-csv-mirror` (event-driven)

- Trigger: S3 ObjectCreated for Admin path and key pattern `lpxd_external_advisors_DF_*`.
- Action: copy CSV to Pulse path; write Pulse `latest.json`; set `Cache-Control: no-store`.
- Proof: On next Admin upload, Pulse CSV + `latest.json` update within seconds.

5) Frontend hardening (already begun)

- DATA_PREFIX = `/signal-dashboard/data/`.
- Fetch with `?cb=Date.now()`, `{cache:'no-store'}`.
- Parse `latest_prices.json` when it is `{prices:{...}}` or flat `{SYM: price}`.
- Safe symbol resolver for positions; guard `response.ok` before parsing CSV (avoid XML errors).
- PV = pv_pre + Daily; SI Total P&L = PV − 1,000,000.
- Proof: console logs show pv_pre, daily sum, expected PV; timeframe sources correct.

6) Price writer freshness

- Confirm `latest_prices.json` is written to Pulse every ≤ 2 minutes.
- If missing, deploy `price-writer-pulse` or fix env of the existing writer.
- Proof: two GETs 60s apart differ; `cache-control: no-store` present.

7) Monitoring & alerts (CloudWatch)

- Alarm: `latest_prices.json` freshness ≤ 3m.
- Alarm: 00:30 outputs present daily (all three keys written; alert if missing).
- Optional: alarm if `latest.json` points > 48h old CSV.

8) Acceptance & rollback

- Acceptance: SI/1M/7D correct; PV = pv_pre + Daily; new CSVs auto-mirror; 00:30 outputs roll daily; all alarms green.
- Rollback: S3 object versioning; revert Lambda env; disable triggers.

## Verification Links

- `/signal-dashboard/data/pre_execution.json?cb=NOW`
- `/signal-dashboard/data/latest.json?cb=NOW`
- `/signal-dashboard/data/eod_pv.csv?cb=NOW`
- `/signal-dashboard/data/latest_prices.json?cb=NOW`

## Notes

- We keep Pulse’s own EOD history independent of Admin; only mirror CSV and marks. Admin remains untouched.

### To-dos

- [ ] Ensure pulse-intraday-mirror excludes pre_execution.json & daily_baseline.json
- [ ] Set pv_pre=1375335.19 in pre_execution.json; invalidate CF
- [ ] Create pulse-daily-executor; write pre_execution, baseline, latest at 00:30Z
- [ ] Create pulse-csv-mirror; on Admin CSV upload copy to Pulse and write latest.json
- [ ] Finalize FE guards: DATA_PREFIX, cache-busting, prices parser, symbol resolver
- [ ] Confirm latest_prices.json freshness or deploy price-writer-pulse
- [ ] Add alarms for prices freshness and daily executor outputs
- [ ] Verify PV = pv_pre + Daily and timeframe sources from console