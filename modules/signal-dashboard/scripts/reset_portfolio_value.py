#!/usr/bin/env python3
"""
Signal Dashboard — Full Fresh Start

What it does (in this order):
1) Backs up current S3 logs:
   - portfolio_value_log.jsonl  -> backups/
   - portfolio_daily_log.csv    -> backups/
2) Resets PV log to a single entry at $1,000,000 (daily_pnl=0, total_pnl=0).
3) Resets daily log to an empty file with header (so the chart & daily events start clean).
4) Deletes snapshots: pre_execution.json, daily_baseline.json
5) (Optional) Deletes latest_prices.json (use --wipe-latest-prices to remove)

Default bucket/prefix match your current layout:
  s3://dfi-signal-dashboard/signal-dashboard/data/

Usage:
  python3 reset_signal_dashboard.py --yes
  python3 reset_signal_dashboard.py --bucket dfi-signal-dashboard --prefix signal-dashboard/data/ --yes
  python3 reset_signal_dashboard.py --yes --wipe-latest-prices
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
import sys
import tempfile
from pathlib import Path

# --------------------------
# Helpers (CLI wrapper)
# --------------------------

def sh(args: list[str], timeout: int = 30) -> tuple[int, str, str]:
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr

def s3_ls(bucket: str, key: str) -> bool:
    rc, out, err = sh(["aws", "s3", "ls", f"s3://{bucket}/{key}"])
    return rc == 0 and out.strip() != ""

def s3_cp(src: str, dst: str) -> bool:
    rc, _, err = sh(["aws", "s3", "cp", src, dst])
    if rc != 0:
        print(f"❌ s3 cp failed: {err.strip()}")
        return False
    return True

def s3_rm(bucket: str, key: str) -> bool:
    if not s3_ls(bucket, key):
        return True
    rc, _, err = sh(["aws", "s3", "rm", f"s3://{bucket}/{key}"])
    if rc != 0:
        print(f"❌ s3 rm failed for {key}: {err.strip()}")
        return False
    return True

def s3_cp_s3(bucket: str, src_key: str, dst_key: str) -> bool:
    rc, _, err = sh(["aws", "s3", "cp", f"s3://{bucket}/{src_key}", f"s3://{bucket}/{dst_key}"])
    if rc != 0:
        print(f"❌ s3 cp s3->s3 failed ({src_key} -> {dst_key}): {err.strip()}")
        return False
    return True

# --------------------------
# Reset operations
# --------------------------

def backup_if_exists(bucket: str, key: str, backup_prefix: str) -> bool:
    if not s3_ls(bucket, key):
        print(f"ℹ️  No existing {key} to back up.")
        return True
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    name = Path(key).name
    backup_key = f"{backup_prefix}{name}.{ts}"
    print(f"💾 Backing up s3://{bucket}/{key}  ->  s3://{bucket}/{backup_key}")
    return s3_cp_s3(bucket, key, backup_key)

def write_temp_and_upload(bucket: str, key: str, content: str) -> bool:
    with tempfile.NamedTemporaryFile("w", delete=False) as tf:
        tf.write(content)
        temp_path = tf.name
    print(f"⬆️  Uploading reset to s3://{bucket}/{key}")
    ok = s3_cp(temp_path, f"s3://{bucket}/{key}")
    try:
        Path(temp_path).unlink(missing_ok=True)
    except Exception:
        pass
    return ok

def reset_pv_log(bucket: str, pv_key: str) -> bool:
    print("🔄 Resetting Portfolio Value log to $1,000,000 …")
    reset_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "portfolio_value": 1_000_000.0,
        "daily_pnl": 0.0,
        "total_pnl": 0.0
    }
    return write_temp_and_upload(bucket, pv_key, json.dumps(reset_entry) + "\n")

def reset_daily_log(bucket: str, daily_key: str) -> bool:
    print("🔄 Resetting portfolio_daily_log.csv …")
    # Match the append_log_row schema (extra fields are OK to stay blank)
    header = (
        "timestamp,date,time_utc,time_paris,csv_filename,action,"
        "portfolio_value,positions_count,total_notional,daily_pnl,pre_value_for_day\n"
    )
    return write_temp_and_upload(bucket, daily_key, header)

def delete_snapshots(bucket: str, pre_key: str, baseline_key: str, wipe_latest: bool, latest_key: str) -> bool:
    ok = True
    print("🗑️  Deleting snapshots …")
    ok &= s3_rm(bucket, pre_key)
    ok &= s3_rm(bucket, baseline_key)
    if wipe_latest:
        ok &= s3_rm(bucket, latest_key)
    return ok

# --------------------------
# Main
# --------------------------

def main():
    ap = argparse.ArgumentParser(description="Signal Dashboard – Full Fresh Start")
    ap.add_argument("--bucket", default="dfi-signal-dashboard")
    ap.add_argument("--prefix", default="signal-dashboard/data/", help="S3 prefix ending with /")
    ap.add_argument("--yes", action="store_true", help="Proceed without interactive prompt")
    ap.add_argument("--wipe-latest-prices", action="store_true", help="Also remove latest_prices.json")
    args = ap.parse_args()

    bucket = args.bucket
    prefix = args.prefix if args.prefix.endswith("/") else args.prefix + "/"

    pv_key       = f"{prefix}portfolio_value_log.jsonl"
    daily_key    = f"{prefix}portfolio_daily_log.csv"
    pre_key      = f"{prefix}pre_execution.json"
    baseline_key = f"{prefix}daily_baseline.json"
    latest_key   = f"{prefix}latest_prices.json"
    backup_prefix = f"{prefix}backups/"

    print("🚀 Signal Dashboard — FULL FRESH START")
    print(f"   S3 bucket : {bucket}")
    print(f"   S3 prefix : {prefix}")
    print(f"   Will reset: {pv_key}, {daily_key}")
    print(f"   Will delete: {pre_key}, {baseline_key}" + (f", {latest_key}" if args.wipe_latest_prices else ""))
    print("⚠️  TIP: briefly stop pv-logger/csv-monitor/price-writer to avoid race conditions during the reset.")

    if not args.yes:
        resp = input("Proceed with FULL reset? (yes/no): ").strip().lower()
        if resp not in ("yes", "y"):
            print("❌ Cancelled.")
            sys.exit(1)

    # Ensure backups folder exists (aws s3 doesn't need pre-create, but it's nice to log intent)
    print(f"🗂️  Backups will go under s3://{bucket}/{backup_prefix}")

    # 1) Backups
    ok = True
    ok &= backup_if_exists(bucket, pv_key, backup_prefix)
    ok &= backup_if_exists(bucket, daily_key, backup_prefix)

    # 2) Reset PV
    ok &= reset_pv_log(bucket, pv_key)

    # 3) Reset daily log
    ok &= reset_daily_log(bucket, daily_key)

    # 4) Delete snapshots
    ok &= delete_snapshots(bucket, pre_key, baseline_key, args.wipe_latest_prices, latest_key)

    if ok:
        print("\n🎉 RESET COMPLETE")
        print("Next steps:")
        print("  • Restart price-writer, pv-logger, csv-monitor")
        print("  • Invalidate CDN for /signal-dashboard/* and hard-refresh the dashboard")
        print("  • Drop a NEW CSV (different SHA) to generate fresh t0/t1 snapshots")
        print("  • Verify in DevTools → Network: exactly one GET to latest_prices.json, no binance.com calls")
    else:
        print("\n❌ RESET COMPLETED WITH ERRORS — check messages above and rerun if needed.")
        sys.exit(2)

if __name__ == "__main__":
    main()
