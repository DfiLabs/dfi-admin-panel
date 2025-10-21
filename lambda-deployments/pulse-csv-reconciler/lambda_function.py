#!/usr/bin/env python3
"""
Lambda: pulse-csv-reconciler

Purpose:
  - On a fixed schedule (e.g., every 5 minutes), ensure Pulse has the exact same
    positions CSV body as Admin, even if Admin overwrote the same filename.
  - If Admin CSV ETag (and VersionId when present) differs from Pulse copy, copy it
    to Pulse and refresh Pulse latest.json.

Env Vars:
  BUCKET       = dfi-signal-dashboard
  ADMIN_PREFIX = signal-dashboard/data/
  PULSE_PREFIX = descartes-ml/signal-dashboard/data/
  LATEST_NAME  = latest.json

Security:
  - The execution role must have read access to Admin prefix objects used here
    (latest.json and lpxd_*.csv) and write access only to Pulse prefix.
  - No Admin writes; no Pulse reads required beyond HEAD/GET of the target CSV.
"""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

import boto3


BUCKET = os.environ.get("BUCKET", "dfi-signal-dashboard")
ADMIN_PREFIX = os.environ.get("ADMIN_PREFIX", "signal-dashboard/data/")
PULSE_PREFIX = os.environ.get("PULSE_PREFIX", "descartes-ml/signal-dashboard/data/")
LATEST_NAME = os.environ.get("LATEST_NAME", "latest.json")

s3 = boto3.client("s3")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_admin_latest() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (filename, etag, version_id) from Admin latest.json and HEAD of CSV.
    ETag is derived from the CSV object; VersionId included if bucket versioning is on.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{ADMIN_PREFIX}{LATEST_NAME}")
        latest = json.loads(obj["Body"].read().decode("utf-8", "ignore"))
    except Exception:
        return None, None, None

    name = latest.get("latest_csv") or latest.get("filename")
    if not name:
        return None, None, None

    try:
        head = s3.head_object(Bucket=BUCKET, Key=f"{ADMIN_PREFIX}{name}")
        etag = head.get("ETag", "").strip('"')
        version_id = head.get("VersionId")
        return name, etag, version_id
    except Exception:
        return name, None, None


def _get_pulse_head(name: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (etag, version_id) for Pulse copy of the CSV name if present."""
    try:
        head = s3.head_object(Bucket=BUCKET, Key=f"{PULSE_PREFIX}{name}")
        return head.get("ETag", "").strip('"'), head.get("VersionId")
    except Exception:
        return None, None


def _copy_admin_to_pulse(name: str) -> Tuple[bool, Optional[str]]:
    """Copy Admin CSV body to Pulse path. Returns (copied, pulse_key)."""
    src_key = f"{ADMIN_PREFIX}{name}"
    dst_key = f"{PULSE_PREFIX}{name}"
    try:
        s3.copy_object(
            Bucket=BUCKET,
            Key=dst_key,
            CopySource={"Bucket": BUCKET, "Key": src_key},
            MetadataDirective="REPLACE",
            CacheControl="no-store, max-age=0",
            ContentType="text/csv",
        )
        return True, dst_key
    except Exception:
        return False, None


def _write_pulse_latest(name: str, etag: Optional[str], version_id: Optional[str]) -> None:
    latest = {
        "filename": name,
        "latest_csv": name,
        "etag": etag,
        "version_id": version_id,
        "updated_utc": _now_iso(),
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PULSE_PREFIX}{LATEST_NAME}",
        Body=json.dumps(latest).encode("utf-8"),
        CacheControl="no-store, max-age=0",
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def lambda_handler(event, _):
    name, admin_etag, admin_version = _get_admin_latest()
    if not name or not admin_etag:
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no_admin_csv"})}

    pulse_etag, _pulse_version = _get_pulse_head(name)
    if pulse_etag != admin_etag:
        copied, _ = _copy_admin_to_pulse(name)
        if not copied:
            return {"statusCode": 200, "body": json.dumps({"ok": False, "copied": False})}
        # Refresh Pulse latest.json to reflect the new content/etag/version
        _write_pulse_latest(name, admin_etag, admin_version)
        return {"statusCode": 200, "body": json.dumps({"ok": True, "copied": True, "name": name, "etag": admin_etag})}

    # Ensure latest.json still refreshed even if etag matches (idempotent metadata update)
    _write_pulse_latest(name, admin_etag, admin_version)
    return {"statusCode": 200, "body": json.dumps({"ok": True, "copied": False, "name": name, "etag": admin_etag})}




