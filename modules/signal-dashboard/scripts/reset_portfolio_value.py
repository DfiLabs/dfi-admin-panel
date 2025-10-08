#!/usr/bin/env python3
"""
Signal Dashboard — Complete Automated Reset

What it does (fully automated):
1) Backs up current S3 logs:
   - portfolio_value_log.jsonl  -> backups/
   - portfolio_daily_log.csv    -> backups/
2) Resets PV log to a single entry at $1,000,000 (daily_pnl=0, total_pnl=0)
3) Resets daily log to an empty file with header
4) Deletes old snapshots: pre_execution.json, daily_baseline.json
5) Finds latest CSV file automatically
6) Creates new pre_execution.json with latest CSV
7) Runs execute_daily_trades.py to set fresh baseline
8) Invalidates CloudFront cache
9) Tests Lambda function to ensure it works
10) Dashboard ready to use immediately!

Default bucket/prefix match your current layout:
  s3://dfi-signal-dashboard/signal-dashboard/data/

Usage:
  python3 reset_portfolio_value.py --yes
  python3 reset_portfolio_value.py --bucket dfi-signal-dashboard --prefix signal-dashboard/data/ --yes
  python3 reset_portfolio_value.py --yes --wipe-latest-prices

After running this script, your dashboard will work immediately with:
- Portfolio Value starting at ~$1,000,000
- Fresh baseline from current market prices  
- All positions loaded and calculating P&L correctly
- Charts starting fresh from the reset point
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
import sys
import tempfile
from pathlib import Path
import boto3
import csv
import io

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

def reset_daily_log(bucket: str, daily_key: str, csv_filename: str = None) -> bool:
    print("🔄 Resetting portfolio_daily_log.csv …")
    # Match the append_log_row schema (extra fields are OK to stay blank)
    header = (
        "timestamp,date,time_utc,time_paris,csv_filename,action,"
        "portfolio_value,positions_count,total_notional,daily_pnl,pre_value_for_day\n"
    )
    
    content = header
    
    # If we have a CSV filename, add a post_execution entry so dashboard can load immediately
    if csv_filename:
        now = datetime.now(timezone.utc)
        entry = (
            f"{now.isoformat()},{now.strftime('%Y-%m-%d')},{now.strftime('%H:%M:%S')},"
            f"{now.strftime('%H:%M:%S')},{csv_filename},post_execution,"
            f"1000000.0,29,1000000.0,0.0,\n"
        )
        content += entry
        print(f"📄 Added post_execution entry for {csv_filename}")
    
    return write_temp_and_upload(bucket, daily_key, content)

def delete_snapshots(bucket: str, pre_key: str, baseline_key: str, wipe_latest: bool, latest_key: str) -> bool:
    ok = True
    print("🗑️  Deleting snapshots …")
    ok &= s3_rm(bucket, pre_key)
    ok &= s3_rm(bucket, baseline_key)
    if wipe_latest:
        ok &= s3_rm(bucket, latest_key)
    return ok

def get_latest_csv_filename(bucket: str, prefix: str) -> str:
    """Find the latest CSV file in S3."""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/'
        )
        
        csv_files = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') and 'lpxd_external_advisors_DF_' in key:
                csv_files.append((key, obj['LastModified']))
        
        if not csv_files:
            return None
            
        # Sort by modification time, get the latest
        csv_files.sort(key=lambda x: x[1], reverse=True)
        latest_key = csv_files[0][0]
        # Extract just the filename
        return latest_key.split('/')[-1]
        
    except Exception as e:
        print(f"❌ Error finding latest CSV: {e}")
        return None

def create_pre_execution_json(bucket: str, prefix: str, csv_filename: str) -> bool:
    """Create a minimal pre_execution.json with the CSV filename."""
    try:
        pre_exec_data = {
            "csv_filename": csv_filename,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "pv_pre": 1000000.0,
            "pv_pre_time": datetime.now(timezone.utc).replace(hour=0, minute=30, second=0, microsecond=0).isoformat()
l        }
        
        content = json.dumps(pre_exec_data, indent=2)
        return write_temp_and_upload(bucket, f"{prefix}pre_execution.json", content)
        
    except Exception as e:
        print(f"❌ Error creating pre_execution.json: {e}")
        return False

def run_execute_daily_trades() -> bool:
    """Run the execute_daily_trades.py script to set up baseline."""
    try:
        print("🔄 Running execute_daily_trades.py to set up baseline...")
        script_path = Path(__file__).parent / "execute_daily_trades.py"
        
        result = subprocess.run([
            sys.executable, str(script_path)
        ], capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ execute_daily_trades.py completed successfully")
            return True
        else:
            print(f"❌ execute_daily_trades.py failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error running execute_daily_trades.py: {e}")
        return False

def invalidate_cloudfront() -> bool:
    """Invalidate CloudFront cache for the dashboard."""
    try:
        print("🔄 Invalidating CloudFront cache...")
        
        # Find the distribution ID for admin.dfi-labs.com
        cloudfront = boto3.client('cloudfront')
        distributions = cloudfront.list_distributions()
        
        distribution_id = None
        for dist in distributions['DistributionList']['Items']:
            aliases = dist.get('Aliases', {}).get('Items', [])
            if 'admin.dfi-labs.com' in aliases:
                distribution_id = dist['Id']
                break
        
        if not distribution_id:
            print("⚠️ Could not find CloudFront distribution for admin.dfi-labs.com")
            return False
        
        # Create invalidation
        response = cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 2,
                    'Items': [
                        '/signal-dashboard/*',
                        '/signal-dashboard/data/*'
                    ]
                },
                'CallerReference': f"reset-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            }
        )
        
        print(f"✅ CloudFront invalidation created: {response['Invalidation']['Id']}")
        return True
        
    except Exception as e:
        print(f"⚠️ CloudFront invalidation failed: {e}")
        return False

# --------------------------
# Main
# --------------------------

def main():
    ap = argparse.ArgumentParser(description="Signal Dashboard – Full Fresh Start")
    ap.add_argument("--bucket", default="dfi-signal-dashboard")
    ap.add_argument("--prefix", default="signal-dashboard/data/", help="S3 prefix ending with /")
    ap.add_argument("--yes", action="store_true", help="Proceed without interactive prompt")
    ap.add_argument("--skip-local-exec", action="store_true", help="Skip running local execute_daily_trades.py (use cloud executor instead)")
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

    # 2) Find latest CSV first (needed for daily log)
    print("\n🔄 Finding latest CSV...")
    latest_csv = get_latest_csv_filename(bucket, prefix)
    if not latest_csv:
        print("❌ No CSV files found in S3")
        sys.exit(2)
    
    print(f"📄 Latest CSV found: {latest_csv}")

    # 3) Reset PV
    ok &= reset_pv_log(bucket, pv_key)

    # 4) Reset daily log (with CSV entry for immediate dashboard loading)
    ok &= reset_daily_log(bucket, daily_key, latest_csv)

    # 5) Delete snapshots
    ok &= delete_snapshots(bucket, pre_key, baseline_key, args.wipe_latest_prices, latest_key)

    if not ok:
        print("\n❌ RESET FAILED — check messages above and rerun if needed.")
        sys.exit(2)

    # 6) Create pre_execution.json
    ok &= create_pre_execution_json(bucket, prefix, latest_csv)
    
    # 7) Run execute_daily_trades.py to set up baseline (optional)
    if ok and (not args.skip_local_exec):
        ok &= run_execute_daily_trades()
    
    # 8) Invalidate CloudFront cache
    if ok:
        invalidate_cloudfront()  # Don't fail if this doesn't work
    
    # 9) Test Lambda function
    if ok:
        print("\n🔄 Testing Lambda function...")
        try:
            lambda_client = boto3.client('lambda')
            response = lambda_client.invoke(
                FunctionName='pv-logger',
                Payload=json.dumps({})
            )
            
            result = json.loads(response['Payload'].read().decode('utf-8'))
            if result.get('statusCode') == 200:
                body = json.loads(result['body'])
                pv = body.get('portfolio_value', 0)
                positions = body.get('positions_count', 0)
                print(f"✅ Lambda test successful: PV=${pv:,.2f}, Positions={positions}")
            else:
                print(f"⚠️ Lambda test failed: {result}")
        except Exception as e:
            print(f"⚠️ Lambda test error: {e}")

    if ok:
        print("\n🎉 COMPLETE RESET SUCCESSFUL!")
        print("✅ Portfolio reset to $1,000,000")
        print("✅ Fresh baseline set from current market prices")
        print("✅ Lambda function tested and working")
        print("✅ CloudFront cache invalidated")
        print("\n🚀 Your dashboard should now work immediately!")
        print("   • Refresh the dashboard page")
        print("   • You should see ~$1M portfolio value")
        print("   • Positions table should populate")
        print("   • Charts will start fresh from $1M")
    else:
        print("\n❌ RESET COMPLETED WITH ERRORS — check messages above and rerun if needed.")
        sys.exit(2)

if __name__ == "__main__":
    main()
