#!/usr/bin/env python3
"""
Scheduled daily execution at 00:30 UTC.

Responsibilities
  - Load pre_execution.json (written at CSV detection time)
  - Determine pv_pre as the last PV strictly before 00:30 UTC
  - Read the referenced CSV and latest mark prices (latest_prices.json)
  - Compute scale = pv_pre / sum(|target_notional|)
  - Compute target quantities per row and (optionally) place delta orders
  - Snapshot baseline prices at execution time to daily_baseline.json
  - Update pre_execution.json with pv_pre and pv_pre_time

Safety
  - Default DRY_RUN=true (no orders sent). Set EXECUTE_TRADES=true to enable sending orders.
  - Order placement function is a stub; integrate with exchange client if needed.

Environment
  S3_BUCKET                (default: dfi-signal-dashboard)
  S3_PREFIX                (default: signal-dashboard/data/)
  REGION                   (default: eu-west-1)
  EXECUTE_TRADES           (default: false)
  SCHEDULE_HHMM            (default: 0030)  # used for cutoff computation
"""

from __future__ import annotations

import os
import sys
import json
import csv
import io
import math
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("execute_daily_trades")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


log = setup_logger().info
warn = setup_logger().warning
err = setup_logger().error


def env(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v is not None else default


S3_BUCKET = env("S3_BUCKET", "dfi-signal-dashboard")
S3_PREFIX = env("S3_PREFIX", "signal-dashboard/data/")
REGION = env("REGION", "eu-west-1")
EXECUTE_TRADES = env("EXECUTE_TRADES", "false").lower() == "true"
SCHEDULE_HHMM = env("SCHEDULE_HHMM", "0030")  # 00:30 UTC


def s3() -> boto3.client:
    return boto3.client("s3", region_name=REGION)


def read_json_from_s3(key: str) -> dict:
    c = s3()
    obj = c.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def write_json_to_s3(key: str, payload: dict) -> None:
    c = s3()
    c.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )


def get_pv_pre(cutoff_utc: datetime) -> Tuple[float, str]:
    """Return (pv_pre, timestamp_iso) from portfolio_value_log.jsonl strictly before cutoff.
    Fallback to 1_000_000 if none found.
    """
    key = f"{S3_PREFIX}portfolio_value_log.jsonl"
    c = s3()
    try:
        raw = c.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    except ClientError as e:
        warn(f"No PV log found: {e}")
        return 1_000_000.0, "N/A"
    pv_pre = None
    pv_ts = "N/A"
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            ts = rec.get("timestamp") or rec.get("ts_utc")
            if not ts:
                continue
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt < cutoff_utc:
                pv_pre = rec.get("portfolio_value") or rec.get("portfolioValue")
                pv_ts = ts
            else:
                break
        except Exception:
            continue
    if pv_pre is None:
        return 1_000_000.0, "N/A"
    return float(pv_pre), pv_ts


def read_csv_from_s3(filename: str) -> List[dict]:
    key = f"{S3_PREFIX}{filename}"
    raw = s3().get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(raw)))


def load_marks() -> Dict[str, float]:
    latest = read_json_from_s3(f"{S3_PREFIX}latest_prices.json")
    return {k: float(v) for k, v in (latest.get("prices") or {}).items()}


def sum_abs_notional(rows: List[dict]) -> float:
    total = 0.0
    for r in rows:
        try:
            n = float(r.get("target_notional") or 0)
            total += abs(n)
        except Exception:
            pass
    return total


def step_constraints(symbol: str) -> Tuple[float, float, float]:
    """Return (stepSize, minQty, minNotional).
    Placeholder defaults; integrate exchangeInfo if available.
    """
    # Conservative defaults; adjust per symbol if needed
    return 0.0001, 0.0, 0.0


def round_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return math.floor(qty / step) * step


def compute_targets(rows: List[dict], marks: Dict[str, float], scale: float) -> List[dict]:
    targets = []
    for r in rows:
        sym = (r.get("ticker") or r.get("ric") or r.get("internal_code") or "").replace("_", "")
        if not sym:
            continue
        try:
            notional = float(r.get("target_notional") or 0)
            contracts = float(r.get("target_contracts") or 0)
        except Exception:
            continue
        side = 1 if contracts > 0 else -1
        price = marks.get(sym)
        if price is None or price <= 0:
            continue
        notional_target = abs(notional) * scale
        raw_qty = side * (notional_target / price)
        step, min_qty, min_notional = step_constraints(sym)
        qty = round_to_step(abs(raw_qty), step)
        qty = math.copysign(qty, raw_qty)
        # Optional min checks
        if abs(qty) < min_qty or (abs(qty) * price) < min_notional:
            qty = 0.0
        targets.append({
            "symbol": sym,
            "side": "BUY" if qty > 0 else "SELL" if qty < 0 else "FLAT",
            "qty_target": qty,
            "price": price,
            "notional_target": notional_target,
            "step": step,
        })
    return targets


def fetch_current_positions(symbols: List[str]) -> Dict[str, float]:
    """Return current position qty per symbol. Stubbed to 0.0 (flat) by default."""
    # Integrate with exchange positions API if available
    return {s: 0.0 for s in symbols}


def place_order(symbol: str, delta_qty: float) -> None:
    """Place a market order. DRY_RUN unless EXECUTE_TRADES=true."""
    action = "BUY" if delta_qty > 0 else "SELL"
    log(f"ORDER: {action} {abs(delta_qty)} {symbol} (DRY_RUN={not EXECUTE_TRADES})")
    if not EXECUTE_TRADES or abs(delta_qty) == 0:
        return
    # TODO: integrate with exchange client safely
    raise NotImplementedError("Live trading integration not configured.")


def main() -> None:
    log("Starting scheduled execution script (00:30 UTC)")
    # 1) Load pre_execution.json
    pre_key = f"{S3_PREFIX}pre_execution.json"
    try:
        pre = read_json_from_s3(pre_key)
    except ClientError as e:
        err(f"pre_execution.json missing: {e}")
        sys.exit(1)

    csv_filename = pre.get("csv_filename")
    if not csv_filename:
        err("pre_execution.json missing csv_filename")
        sys.exit(1)

    # 2) Determine cutoff (today 00:30 UTC)
    now = datetime.now(timezone.utc)
    cutoff = now.replace(hour=0, minute=30, second=0, microsecond=0)
    # If running after 00:30 already, use today's 00:30; else go to previous day
    if now < cutoff:
        cutoff = cutoff - timedelta(days=1)
    pv_pre, pv_pre_ts = get_pv_pre(cutoff)
    log(f"pv_pre={pv_pre:.2f} at {pv_pre_ts} (cutoff={cutoff.isoformat()})")

    # 3) Load CSV and latest marks
    rows = read_csv_from_s3(csv_filename)
    marks = load_marks()

    # 4) Compute scale and targets
    gross = sum_abs_notional(rows)
    if gross <= 0:
        err("Gross base S is zero; aborting")
        sys.exit(1)
    scale = pv_pre / gross
    log(f"Gross S={gross:.2f}, scale={scale:.8f}")
    targets = compute_targets(rows, marks, scale)

    # 5) Compute deltas vs current positions and (optionally) place orders
    symbols = [t["symbol"] for t in targets]
    current = fetch_current_positions(symbols)
    plan: List[dict] = []
    for t in targets:
        sym = t["symbol"]
        target_qty = t["qty_target"]
        cur = current.get(sym, 0.0)
        delta = target_qty - cur
        t["qty_current"] = cur
        t["qty_delta"] = delta
        plan.append(t)
        try:
            place_order(sym, delta)
        except NotImplementedError as e:
            warn(str(e))

    # 6) Snapshot baseline prices at execution time
    baseline_payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "csv_filename": csv_filename,
        " portfolio_value_at_pv_pre": pv_pre,
        "prices": marks,
    }
    write_json_to_s3(f"{S3_PREFIX}daily_baseline.json", baseline_payload)
    log("daily_baseline.json written")

    # 7) Update pre_execution.json with pv_pre and time
    pre["pv_pre_time"] = cutoff.isoformat()
    pre["pv_pre"] = pv_pre
    write_json_to_s3(pre_key, pre)
    log("pre_execution.json updated with pv_pre")

    # 8) Record execution plan for audit
    result = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "csv_filename": csv_filename,
        "pv_pre": pv_pre,
        "scale": scale,
        "orders": plan,
        "dry_run": not EXECUTE_TRADES,
    }
    write_json_to_s3(f"{S3_PREFIX}execution_result.json", result)
    log("execution_result.json written (plan)")


if __name__ == "__main__":
    main()


