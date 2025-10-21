#!/usr/bin/env python3
"""
Lambda: pulse-csv-mirror

Purpose:
  - On Admin S3 upload of a new positions CSV (lpxd_*.csv) under signal-dashboard/data/,
    copy it to the Pulse prefix (descartes-ml/signal-dashboard/data/) and update latest.json.

Env Vars:
  BUCKET       = dfi-signal-dashboard
  SRC_PREFIX   = signal-dashboard/data/
  DST_PREFIX   = descartes-ml/signal-dashboard/data/
  FILENAME_PREFIX = lpxd_

Notes:
  - Sets Cache-Control to no-store for copied CSV and latest.json
  - Overwrites latest.json atomically with the new filename
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any

import boto3


BUCKET = os.environ.get("BUCKET", "dfi-signal-dashboard")
SRC_PREFIX = os.environ.get("SRC_PREFIX", "signal-dashboard/data/")
DST_PREFIX = os.environ.get("DST_PREFIX", "descartes-ml/signal-dashboard/data/")
FILENAME_PREFIX = os.environ.get("FILENAME_PREFIX", "lpxd_")
ORCH_FUNCTION = os.environ.get("ORCH_FUNCTION", "execution-orchestrator")

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_target_csv(key: str) -> bool:
    """Accept any CSV under SRC_PREFIX.

    Legacy used FILENAME_PREFIX (e.g., 'lpxd_'), but new monitor files are
    named 'monitor_signal_DF_YYYYMMDD-HHMM.csv'. To support both, we simply
    require the key to be under SRC_PREFIX and end with .csv.
    """
    if not key.startswith(SRC_PREFIX):
        return False
    return key.lower().endswith('.csv')


def _copy_csv_and_update_latest(src_bucket: str, key: str) -> Dict[str, Any]:
    name = key.split("/")[-1]
    dst_key = f"{DST_PREFIX}{name}"

    # Copy the CSV
    s3.copy_object(
        Bucket=BUCKET,
        Key=dst_key,
        CopySource={"Bucket": src_bucket, "Key": key},
        MetadataDirective="REPLACE",
        CacheControl="no-store, max-age=0",
        ContentType="text/csv",
    )

    # Update latest.json
    latest = {
        "filename": name,
        "latest_csv": name,
        "updated_utc": _now_iso(),
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{DST_PREFIX}latest.json",
        Body=json.dumps(latest).encode("utf-8"),
        CacheControl="no-store, max-age=0",
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    
    # Asynchronously invoke orchestrator for Admin (source key) and Pulse (dest key)
    try:
        lambda_client.invoke(FunctionName=ORCH_FUNCTION, InvocationType='Event', Payload=json.dumps({
            "bucket": src_bucket,
            "key": key
        }).encode('utf-8'))
    except Exception as e:
        # log but do not fail copy
        print(f"orchestrator invoke (admin) failed: {e}")
    try:
        lambda_client.invoke(FunctionName=ORCH_FUNCTION, InvocationType='Event', Payload=json.dumps({
            "bucket": BUCKET,
            "key": dst_key
        }).encode('utf-8'))
    except Exception as e:
        print(f"orchestrator invoke (pulse) failed: {e}")

    return {"copied": dst_key, "latest": latest}


def lambda_handler(event, _):
    results = []
    records = event.get("Records") or []
    for r in records:
        s3rec = r.get("s3") or {}
        bucket = s3rec.get("bucket", {}).get("name") or BUCKET
        key = s3rec.get("object", {}).get("key")
        if not key:
            continue
        if _is_target_csv(key):
            results.append(_copy_csv_and_update_latest(bucket, key))
    return {
        "statusCode": 200,
        "body": json.dumps({"ok": True, "handled": results})
    }





