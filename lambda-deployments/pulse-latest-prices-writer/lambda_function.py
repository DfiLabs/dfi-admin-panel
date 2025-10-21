#!/usr/bin/env python3
"""
AWS Lambda: Latest Prices Writer (USDT-M Perpetuals)

Fetches all Binance USDT‑margined PERPETUAL mark prices and writes a unified
price map to S3 for both Admin and Pulse environments.

Outputs:
- s3://<S3_BUCKET>/signal-dashboard/data/latest_prices.json
- s3://<S3_BUCKET>/descartes-ml/signal-dashboard/data/latest_prices.json

Environment (optional):
- S3_BUCKET (default: dfi-signal-dashboard)
- ADMIN_KEY (default: signal-dashboard/data/latest_prices.json)
- PULSE_KEY (default: descartes-ml/signal-dashboard/data/latest_prices.json)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, List
import urllib.request
import ssl

import boto3


BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex"

S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "signal-dashboard/data/latest_prices.json")
PULSE_KEY = os.environ.get("PULSE_KEY", "descartes-ml/signal-dashboard/data/latest_prices.json")

s3 = boto3.client("s3")


def _http_get_json(url: str, timeout_s: int = 12):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "dfi-prices-writer/1.0"})
    with urllib.request.urlopen(req, context=ctx, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def discover_usdtm_perp_symbols() -> List[str]:
    """Return all Binance USDT‑M PERPETUAL symbols currently TRADING."""
    try:
        data = _http_get_json(BINANCE_EXCHANGE_INFO, timeout_s=20)
        symbols = []
        for info in data.get("symbols", []) if isinstance(data, dict) else []:
            try:
                if (
                    info.get("contractType") == "PERPETUAL"
                    and info.get("quoteAsset") == "USDT"
                    and info.get("status") == "TRADING"
                ):
                    sym = str(info.get("symbol") or "").strip()
                    if sym:
                        symbols.append(sym)
            except Exception:
                continue
        return symbols
    except Exception:
        # Fallback: empty list → we'll still write whatever we can from premiumIndex
        return []


def fetch_all_marks() -> Dict[str, float]:
    """Fetch mark prices for all instruments from premiumIndex and return {sym: price}."""
    out: Dict[str, float] = {}
    try:
        data = _http_get_json(BINANCE_PREMIUM_INDEX, timeout_s=20)
        if isinstance(data, list):
            for item in data:
                try:
                    sym = str(item.get("symbol") or "").strip()
                    if not sym.endswith("USDT"):
                        continue
                    mp = float(item.get("markPrice", 0) or 0)
                    if mp > 0:
                        out[sym] = mp
                except Exception:
                    continue
    except Exception:
        pass
    return out


def write_prices(prices: Dict[str, float]) -> None:
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "prices": prices,
    }
    body = json.dumps(payload, separators=(",", ":"))
    for key in (ADMIN_KEY, PULSE_KEY):
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-store, max-age=0",
        )


def lambda_handler(event, context):
    # Discover the trading USDT-M perp symbol universe
    universe = set(discover_usdtm_perp_symbols())
    # Fetch all marks and intersect with universe if available
    marks = fetch_all_marks()
    if universe:
        marks = {k: v for k, v in marks.items() if k in universe}
    # Write if we have a meaningful set; otherwise write what we have to avoid gaps
    write_prices(marks)
    return {"ok": True, "symbols": len(marks)}



