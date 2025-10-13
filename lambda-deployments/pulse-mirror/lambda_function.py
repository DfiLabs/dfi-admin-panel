#!/usr/bin/env python3
"""
Lambda: Pulse Intraday Mirror
Copies dynamic intraday files from Admin prefix to one or more destination prefixes
so Daily P&L and metadata match Admin everywhere the dashboard reads from.

Env:
  BUCKET        = dfi-signal-dashboard
  SRC_PREFIX    = signal-dashboard/data/
  DST_PREFIX    = descartes-ml/signal-dashboard/data/                 # backward compat (single dest)
  DST_PREFIXES  = descartes-ml/signal-dashboard/data/,signal-dashboard-demo/signal-dashboard/data/
  KEYS          = pre_execution.json,daily_baseline.json,latest.json,latest_prices.json
"""
import os
import json
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ.get("BUCKET", "dfi-signal-dashboard")
SRC_PREFIX = os.environ.get("SRC_PREFIX", "signal-dashboard/data/")
# Support multiple destinations; fall back to single DST_PREFIX if DST_PREFIXES not set
_dst_env = os.environ.get("DST_PREFIXES") or os.environ.get("DST_PREFIX", "descartes-ml/signal-dashboard/data/")
DST_PREFIXES = [p.strip() for p in _dst_env.split(",") if p.strip()]
KEYS = [k.strip() for k in os.environ.get("KEYS", "pre_execution.json,daily_baseline.json,latest.json,latest_prices.json").split(",") if k.strip()]

s3 = boto3.client("s3")

def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

def _content_type_for(key: str) -> str:
    if key.endswith(".json"):
        return "application/json"
    if key.endswith(".csv"):
        return "text/csv; charset=utf-8"
    return "application/octet-stream"

def copy_one(key: str, dest_prefix: str) -> bool:
    src_key = f"{SRC_PREFIX}{key}"
    dst_key = f"{dest_prefix}{key}"
    try:
        # Ensure source exists (head once to get type if needed)
        head = s3.head_object(Bucket=BUCKET, Key=src_key)
        content_type = head.get("ContentType") or _content_type_for(key)
    except ClientError as e:
        log(f"❌ head_object failed for {src_key}: {e}")
        return False

    try:
        s3.copy_object(
            Bucket=BUCKET,
            Key=dst_key,
            CopySource={"Bucket": BUCKET, "Key": src_key},
            MetadataDirective="REPLACE",
            CacheControl="no-store, max-age=0",
            ContentType=content_type,
        )
        log(f"✅ copied {src_key} -> {dst_key}")
        return True
    except ClientError as e:
        log(f"❌ copy_object failed for {src_key} -> {dst_key}: {e}")
        return False

def lambda_handler(event, _):
    overall_ok = 0
    per_dest_counts = {}
    for dest in DST_PREFIXES:
        count_ok = 0
        for k in KEYS:
            if copy_one(k, dest):
                count_ok += 1
                overall_ok += 1
        per_dest_counts[dest] = count_ok
    body = {
        "ok": overall_ok,
        "per_dest": per_dest_counts,
        "total_per_dest": len(KEYS),
        "bucket": BUCKET,
        "src": SRC_PREFIX,
        "dsts": DST_PREFIXES,
    }
    log(json.dumps(body))
    return {"statusCode": 200, "body": json.dumps(body)}


