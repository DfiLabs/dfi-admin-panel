#!/usr/bin/env python3
"""
AWS Lambda: Pulse Per‑Minute PV Logger

Goal:
- Keep Pulse PV continuous with no 00:30Z gap
- Anchor to Pulse's own base (first PV today, else yesterday EOD)
- Compute intraday P&L from executed baseline (weights + ref_prices) and live prices

Env Vars:
- S3_BUCKET   (default: dfi-signal-dashboard)
- READ_PREFIX (default: signal-dashboard/data/)          # Admin inputs
- WRITE_PREFIX(default: descartes-ml/signal-dashboard/data/)  # Pulse outputs
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple
import os
import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
# Read per-strategy baseline from Pulse path (independent) and read prices from Pulse root
BASELINE_PREFIX = os.environ.get("BASELINE_PREFIX", "descartes-ml/signal-dashboard/data/combined_descartes_unravel/")
PRICES_PREFIX = os.environ.get("PRICES_PREFIX", "descartes-ml/signal-dashboard/data/")
WRITE_PREFIX = os.environ.get("WRITE_PREFIX", "descartes-ml/signal-dashboard/data/")

s3 = boto3.client("s3")


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def load_json(key: str) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        log(f"load_json error for {key}: {e}")
        return {}
    except Exception as e:
        log(f"load_json parse error for {key}: {e}")
        return {}


def load_text(key: str) -> str:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except ClientError as e:
        log(f"load_text error for {key}: {e}")
        return ""


def parse_jsonl(text: str) -> List[dict]:
    out: List[dict] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
            if "portfolio_value" not in d and "portfolioValue" in d:
                d["portfolio_value"] = d["portfolioValue"]
            out.append(d)
        except Exception:
            continue
    return out


def dt_parse(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def get_pulse_base() -> Tuple[float, dict]:
    """Return (base_value, metadata) where base is first PV today if exists else yesterday EOD.
    Falls back to 1,000,000.0 if no history.
    """
    key = f"{WRITE_PREFIX}portfolio_value_log.jsonl"
    try:
        text = load_text(key)
        rows = parse_jsonl(text)
    except Exception:
        rows = []
    if not rows:
        return 1_000_000.0, {"reason": "no_history"}

    today = datetime.now(timezone.utc).date()
    today_rows = [(dt_parse(r["timestamp"]), float(r.get("portfolio_value", 0))) for r in rows if r.get("timestamp")]
    today_rows = [(t, v) for t, v in today_rows if t.date() == today]
    if today_rows:
        t0, v0 = sorted(today_rows, key=lambda x: x[0])[0]
        return v0, {"reason": "first_today", "timestamp": t0.isoformat()}

    # else yesterday EOD
    yday = today - timedelta(days=1)
    y_rows = [(dt_parse(r["timestamp"]), float(r.get("portfolio_value", 0))) for r in rows if r.get("timestamp")]
    y_rows = [(t, v) for t, v in y_rows if t.date() == yday]
    if y_rows:
        t_last, v_last = sorted(y_rows, key=lambda x: x[0])[-1]
        return v_last, {"reason": "yday_eod", "timestamp": t_last.isoformat()}

    return float(rows[-1].get("portfolio_value", 1_000_000.0)), {"reason": "fallback_last_any"}


def get_latest_csv_filename() -> str | None:
    # For audit only; PV calc does not read the CSV anymore
    latest = load_json(f"{READ_PREFIX}latest.json")
    fn = latest.get("latest_csv") or latest.get("filename")
    if fn:
        return str(fn)
    pre = load_json(f"{READ_PREFIX}pre_execution.json")
    if pre.get("csv_filename"):
        return str(pre["csv_filename"])
    return None


def compute_daily_pnl_weight_based(baseline: dict, latest_prices: dict, capital: float = 1_000_000.0) -> Tuple[float, int]:
    """Compute daily P&L using executed weights and ref_prices vs live prices.

    notional_i = capital * weight_i
    pnl_i = sign(weight_i) * (price_i/ref_price_i - 1) * |notional_i|
    """
    if not isinstance(baseline, dict):
        return 0.0, 0
    weights: Dict[str, float] = baseline.get("weights") or {}
    ref_prices: Dict[str, float] = baseline.get("ref_prices") or baseline.get("prices") or {}
    if not isinstance(weights, dict) or not isinstance(ref_prices, dict):
        return 0.0, 0

    lp_container = latest_prices if isinstance(latest_prices, dict) else {}
    live_prices: Dict[str, float] = lp_container.get("prices", lp_container) if isinstance(lp_container, dict) else {}

    daily_pnl = 0.0
    count = 0
    for ticker, w in weights.items():
        try:
            weight = float(w)
        except Exception:
            continue
        if weight == 0:
            continue
        p0 = ref_prices.get(ticker)
        p1 = live_prices.get(ticker)
        if not p0 or not p1:
            continue
        notional = abs(weight) * capital
        side = 1.0 if weight > 0 else -1.0
        pnl = side * ((float(p1) / float(p0)) - 1.0) * notional
        daily_pnl += pnl
        count += 1
    return daily_pnl, count


def append_jsonl(key: str, entry: dict) -> None:
    try:
        existing = load_text(key)
    except Exception:
        existing = ""
    body = (existing or "") + json.dumps(entry, separators=(",", ":")) + "\n"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/x-ndjson", CacheControl="no-store, max-age=0")


def ensure_execution_anchor(baseline: dict, pv_pre: float, write_prefix: str) -> None:
    """Write a zero-PnL anchor at executed_at_utc if not already present.

    Idempotent: if an entry with the same timestamp already exists, do nothing.
    """
    try:
        ts = str(baseline.get("executed_at_utc") or "").strip()
        if not ts:
            return
        log_key = f"{write_prefix}portfolio_value_log.jsonl"
        text = load_text(log_key)
        # quick idempotency check: does this exact timestamp exist already?
        if ts and text and ts in text:
            return
        anchor = {
            "timestamp": ts,
            "portfolio_value": float(pv_pre),
            "daily_pnl": 0.0,
            "total_pnl": float(pv_pre) - 1_000_000.0,
            "audit": {
                "source": "execution-anchor",
                "note": "zero PnL at execution"
            }
        }
        append_jsonl(log_key, anchor)
        log(f"execution anchor written at {ts}")
    except Exception as e:
        log(f"anchor write failed: {e}")


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    log("pulse-pv-logger start")

    # Inputs: executed baseline and live prices
    baseline = load_json(f"{BASELINE_PREFIX}daily_baseline.json")
    latest_prices = load_json(f"{PRICES_PREFIX}latest_prices.json")
    # For audit only; try per-strategy latest.json then pre_execution.json
    def _get_latest_csv_filename() -> str | None:
        doc = load_json(f"{BASELINE_PREFIX}latest.json")
        fn = doc.get("latest_csv") or doc.get("filename")
        if fn:
            return str(fn)
        pre = load_json(f"{BASELINE_PREFIX}pre_execution.json")
        if pre.get("csv_filename"):
            return str(pre["csv_filename"])
        return None
    latest_csv = _get_latest_csv_filename()

    # Compute P&L (weight-based)
    daily_pnl, positions_count = compute_daily_pnl_weight_based(baseline, latest_prices, capital=1_000_000.0)

    # Determine Pulse base
    pulse_base, base_meta = get_pulse_base()

    # Ensure zero-PnL execution anchor once per baseline
    try:
        pv_pre_for_anchor = float(load_json(f"{BASELINE_PREFIX}pre_execution.json").get("pv_pre", 1_000_000.0))
    except Exception:
        pv_pre_for_anchor = 1_000_000.0
    ensure_execution_anchor(baseline, pv_pre_for_anchor, WRITE_PREFIX)

    # New PV
    portfolio_value = pulse_base + daily_pnl

    # Optional sanity vs last point; allow immediate post-execution jump
    last_ok = True
    try:
        log_key = f"{WRITE_PREFIX}portfolio_value_log.jsonl"
        text = load_text(log_key)
        rows = parse_jsonl(text)
        if rows:
            last = rows[-1]
            last_pv = float(last.get("portfolio_value", last.get("portfolioValue", 0)))
            # If last entry is execution-anchor, skip guard once
            last_is_anchor = isinstance(last.get("audit"), dict) and last["audit"].get("source") == "execution-anchor"
            if not last_is_anchor and last_pv > 0 and abs(portfolio_value - last_pv) / last_pv > 0.05:
                log(f"deviation guard: new {portfolio_value:.2f} last {last_pv:.2f}")
                last_ok = False
    except Exception:
        pass

    if not last_ok:
        return {"statusCode": 200, "body": json.dumps({"skip": "deviation_guard"})}

    entry = {
        "timestamp": now.isoformat(),
        "portfolio_value": portfolio_value,
        "daily_pnl": daily_pnl,
        "total_pnl": portfolio_value - 1_000_000.0,
        "audit": {
            "source": "pulse-per-minute",
            "positions_processed": positions_count,
            "pulse_base": pulse_base,
            "pulse_base_reason": base_meta.get("reason"),
            "csv_filename": latest_csv,
            "inputs": {
                "baseline": f"{BASELINE_PREFIX}daily_baseline.json",
                "latest_prices": f"{PRICES_PREFIX}latest_prices.json"
            }
        }
    }

    append_jsonl(f"{WRITE_PREFIX}portfolio_value_log.jsonl", entry)

    return {"statusCode": 200, "body": json.dumps({"ok": True, "pv": portfolio_value, "positions": positions_count})}

























