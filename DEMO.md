# Demo signal-dashboard setup (independent from admin)

Status: active

- CloudFront (demo): `EQRNZD6D6ZWVM` (`demo.dfi-labs.com`)
- Data/app bucket: `dfi-signal-dashboard`
- Admin prefix (unchanged): `signal-dashboard/`
- Demo prefix (new): `signal-dashboard-demo/`

## What was done

1) Mirrored the current admin app and data into a demo-specific prefix:
   - Copied `s3://dfi-signal-dashboard/signal-dashboard/*` → `s3://dfi-signal-dashboard/signal-dashboard-demo/*`.
   - Kept the original admin paths untouched.

2) Backfilled PV history for demo only:
   - Converted provided Date,PV (comma decimals → dot) into JSONL of daily PV at `23:59:59Z`, e.g.
     `{ "timestamp": "2025-01-01T23:59:59Z", "portfolio_value": 1017368.022 }`.
   - Prepended backfill JSONL to the current admin PV log and uploaded to:
     `s3://dfi-signal-dashboard/signal-dashboard-demo/data/portfolio_value_log.jsonl`.
   - Admin’s `portfolio_value_log.jsonl` was not modified.

3) Pointed demo to demo data prefix:
   - Ensured demo root redirects to `/signal-dashboard/index.html`.
   - Injected a lightweight root page that sets `localStorage.DATA_PREFIX = "/signal-dashboard-demo/data/"` for demo and then navigates to the app. Admin retains its own `DATA_PREFIX` logic.

4) Cache invalidation:
   - Created CloudFront invalidations for demo after each change.

## Effect

- `demo.dfi-labs.com` now loads the same UI bundle as admin but reads data from `signal-dashboard-demo/` so we can:
  - Extend PV history for demo-only;
  - Tweak demo look & feel later without touching admin.
- All PV-derived metrics (Corr vs BTC, Hit Rate, Max Drawdown, chart, Avg Daily Return/Vol/Sharpe) recalculate automatically from the backfilled PV log.

## Operate / rollback

- To switch demo back to admin data, either remove the `DATA_PREFIX` injection on the demo root or set it to `/signal-dashboard/data/` and invalidate demo CloudFront.
- To revert demo PV to the admin-only history, replace `signal-dashboard-demo/data/portfolio_value_log.jsonl` with the admin version and invalidate.

## Notes

- Admin (`admin.dfi-labs.com/signal-dashboard/`) remains unchanged and continues to use `signal-dashboard/` as data source.
- The current admin snapshot reference (UTC): "13:40Z S3" (dashboard.html LastModified `2025-10-09T13:40:23Z`).




