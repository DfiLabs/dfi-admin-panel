#!/usr/bin/env python3
"""
Deploy or update the daily-executor AWS Lambda in eu-west-3 and schedule it at 00:30 UTC.

By default, this script NO LONGER invokes the function immediately after deploy.
To run it once right now (e.g., for same-day initialization), pass --invoke-now explicitly.

Requirements: boto3 configured (env/credentials) with permissions to manage Lambda and EventBridge.
"""

import io
import json
import sys
import zipfile
from datetime import datetime, timezone
import argparse

import boto3
from botocore.exceptions import ClientError


AWS_REGION = "eu-west-3"
FUNCTION_NAME = "daily-executor"
ROLE_ARN = "arn:aws:iam::004150946930:role/lambda-execution-role"
S3_BUCKET = "dfi-signal-dashboard"
S3_PREFIX = "signal-dashboard/data/"  # default Unravel
RULE_NAME = "daily-executor-0030"
SCHEDULE = "cron(30 0 * * ? *)"


def build_lambda_zip() -> bytes:
    code = r"""#!/usr/bin/env python3
import os, json, boto3
from datetime import datetime, timezone, timedelta

S3_BUCKET = os.environ.get('S3_BUCKET', 'dfi-signal-dashboard')
S3_PREFIX = os.environ.get('S3_PREFIX', 'signal-dashboard/data/')

s3 = boto3.client('s3')

def log(m):
    print('[' + datetime.now(timezone.utc).isoformat() + '] ' + str(m))

def get_pv_pre_before_cutoff(cutoff):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'portfolio_value_log.jsonl')['Body'].read().decode('utf-8')
    except Exception as e:
        log('Failed to read PV log: ' + str(e))
        return 1000000.0, 'N/A'
    pv_pre, ts = None, None
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            ts_s = rec.get('timestamp') or rec.get('ts_utc')
            if not ts_s:
                continue
            dt = datetime.fromisoformat(ts_s.replace('Z','+00:00'))
            if dt < cutoff:
                pv_pre = rec.get('portfolio_value') or rec.get('portfolioValue')
                ts = ts_s
            else:
                break
        except Exception:
            continue
    if pv_pre is None:
        # Continuity fallback: use the most recent PV in the log if nothing strictly before cutoff
        try:
            last_pv, last_ts = None, None
            for line in reversed(body.splitlines()):
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                last_pv = rec.get('portfolio_value') or rec.get('portfolioValue')
                last_ts = rec.get('timestamp') or rec.get('ts_utc')
                if last_pv is not None:
                    break
            if last_pv is not None:
                return float(last_pv), last_ts or 'N/A'
        except Exception:
            pass
        return 1000000.0, 'N/A'
    return float(pv_pre), ts

def _get_bool(x):
    try:
        return str(x).strip().lower() in ("1","true","yes","y","on")
    except Exception:
        return False

def handler(event, context):
    now = datetime.now(timezone.utc)
    cutoff = now.replace(hour=0, minute=30, second=0, microsecond=0)
    if now < cutoff:
        from datetime import timedelta
        cutoff = cutoff - timedelta(days=1)
    log('Executor starting; cutoff=' + cutoff.isoformat())
    # Hard guard: only proceed within 00:30±30 minutes UTC unless forced
    force = False
    try:
        if isinstance(event, dict):
            force = _get_bool(event.get('force'))
    except Exception:
        force = False
    bypass_env = _get_bool(os.environ.get('BYPASS_WINDOW','0'))
    if not (force or bypass_env):
        if abs((now - cutoff).total_seconds()) > 30*60:
            log('Outside 00:30±30m; exiting without writes')
            return {'statusCode':200,'body':'skipped: outside window'}

    # Resolve CSV from latest.json
    try:
        latest_raw = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'latest.json')['Body'].read().decode('utf-8')
        latest = json.loads(latest_raw)
        csv_filename = latest.get('filename') or latest.get('latest_csv')
    except Exception as e:
        log('Failed to resolve latest.json: ' + str(e))
        return {'statusCode':500,'body':'no latest.json'}

    pv_pre, pv_ts = get_pv_pre_before_cutoff(cutoff)
    log('pv_pre=' + ('%.2f' % pv_pre) + ' at ' + str(pv_ts))

    # Baseline prices snapshot at execution time
    prices = {}
    try:
        lp_raw = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'latest_prices.json')['Body'].read().decode('utf-8')
        lp = json.loads(lp_raw)
        prices = lp.get('prices',{})
    except Exception as e:
        log('Failed to read latest_prices.json: ' + str(e))

    # Write daily_baseline.json
    baseline = {
        'timestamp_utc': now.isoformat(),
        'csv_filename': csv_filename,
        'pv_pre': pv_pre,
        'prices': prices
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'daily_baseline.json', Body=json.dumps(baseline).encode('utf-8'), ContentType='application/json', CacheControl='no-store')
    log('daily_baseline.json written')

    # Finalize pre_execution.json
    pre = {
        'timestamp_utc': now.isoformat(),
        'csv_filename': csv_filename,
        'pv_pre_time': cutoff.isoformat(),
        'pv_pre': pv_pre,
        'executed_at_utc': now.isoformat(),
        'finalized': True
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'pre_execution.json', Body=json.dumps(pre).encode('utf-8'), ContentType='application/json', CacheControl='no-store')
    log('pre_execution.json written (finalized)')

    latest_exec = {'timestamp_utc': now.isoformat(), 'csv_filename': csv_filename}
    s3.put_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'latest_executed.json', Body=json.dumps(latest_exec).encode('utf-8'), ContentType='application/json', CacheControl='no-store')
    log('latest_executed.json written')

    return {'statusCode':200,'body':'ok'}
"""
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return zbuf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy daily-executor Lambda and schedule it at 00:30 UTC.")
    parser.add_argument("--invoke-now", action="store_true", help="Invoke the Lambda once immediately after deploy (guarded by time window).")
    parser.add_argument("--name", default=FUNCTION_NAME, help="Lambda function name (default: daily-executor)")
    parser.add_argument("--rule-name", default=RULE_NAME, help="EventBridge rule name (default: daily-executor-0030)")
    parser.add_argument("--s3-prefix", default=S3_PREFIX, help="S3 key prefix for strategy (e.g., signal-dashboard/data/ or signal-dashboard/descartes-beta/data/)")
    parser.add_argument("--region", default=AWS_REGION, help="AWS region for Lambda/EventBridge (default from script)")
    args = parser.parse_args()

    # Resolve effective config
    function_name = args.name
    rule_name = args.rule_name
    s3_prefix = args.s3_prefix if args.s3_prefix.endswith('/') else (args.s3_prefix + '/') if args.s3_prefix else S3_PREFIX
    region = args.region or AWS_REGION

    lambda_client = boto3.client("lambda", region_name=region)
    events = boto3.client("events", region_name=region)

    zip_bytes = build_lambda_zip()

    # Create or update function
    try:
        lambda_client.get_function(FunctionName=function_name)
        exists = True
    except ClientError:
        exists = False

    if not exists:
        print(f"Creating Lambda function {function_name} in {region} ...")
        lambda_client.create_function(
            FunctionName=function_name,
            Role=ROLE_ARN,
            Runtime="python3.12",
            Timeout=60,
            MemorySize=256,
            Code={"ZipFile": zip_bytes},
            Handler="lambda_function.handler",
            Environment={"Variables": {"S3_BUCKET": S3_BUCKET, "S3_PREFIX": s3_prefix}},
        )
    else:
        print(f"Updating Lambda code/config for {function_name} ...")
        lambda_client.update_function_code(FunctionName=function_name, ZipFile=zip_bytes)
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": {"S3_BUCKET": S3_BUCKET, "S3_PREFIX": s3_prefix}},
        )

    # Schedule at 00:30 UTC
    print(f"Configuring EventBridge schedule {rule_name} @ 00:30 UTC in {region} ...")
    events.put_rule(Name=rule_name, ScheduleExpression=SCHEDULE, State="ENABLED")
    fn_arn = lambda_client.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId="allow-events",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{region}:004150946930:rule/{rule_name}",
        )
    except ClientError:
        pass
    events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])

    if args.invoke_now:
        print("Invoking once now ...")
        resp = lambda_client.invoke(FunctionName=function_name, InvocationType="RequestResponse", Payload=b"{}")
        payload = resp.get("Payload").read().decode("utf-8")
        print("Invoke response:", payload)

    # Quick verification (metadata only)
    s3c = boto3.client("s3")
    try:
        pre_obj = s3c.get_object(Bucket=S3_BUCKET, Key=s3_prefix + "pre_execution.json")
        pre = json.loads(pre_obj["Body"].read().decode("utf-8"))
        print("pre_execution.json:", json.dumps({k: pre.get(k) for k in ["timestamp_utc", "pv_pre_time", "pv_pre", "finalized", "csv_filename"]}, separators=(",", ":")))
    except Exception as e:
        print("pre_execution.json not present or unreadable (this is expected if not yet invoked):", str(e))

    try:
        baseline_obj = s3c.get_object(Bucket=S3_BUCKET, Key=s3_prefix + "daily_baseline.json")
        baseline = json.loads(baseline_obj["Body"].read().decode("utf-8"))
        print("daily_baseline.json:", json.dumps({k: baseline.get(k) for k in ["timestamp_utc", "pv_pre", "csv_filename"]}, separators=(",", ":")))
    except Exception as e:
        print("daily_baseline.json not present or unreadable (this is expected if not yet invoked):", str(e))

    return 0


if __name__ == "__main__":
    sys.exit(main())



