#!/usr/bin/env python3
"""
Periodic writer for S3 latest_prices.json

Purpose
  - Fetch Binance mark prices for the symbols present in the latest portfolio CSV
  - Write a single source of truth JSON to S3:
      s3://<BUCKET>/signal-dashboard/data/latest_prices.json

JSON shape
  {
    "timestamp_utc": "2025-09-21T12:34:56.789012+00:00",
    "prices": { "BTCUSDT": 115000.12, ... }
  }

Notes
  - Uses Binance futures premiumIndex for mark prices
  - Runs forever with a 60s cadence and robust error handling
  - Uploads with CacheControl=no-store and ContentType=application/json

Environment (optional)
  LATEST_PRICES_BUCKET     (default: dfi-signal-dashboard)
  LATEST_PRICES_KEY        (default: signal-dashboard/data/latest_prices.json)
  LATEST_META_KEY          (default: signal-dashboard/data/latest.json)
  REGION                   (default: eu-west-1)
  LOOP_SECONDS             (default: 60)
"""

from __future__ import annotations

import os
import sys
import json
import time
import csv
import io
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import botocore
import requests


BINANCE_MARK_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("write_latest_prices")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = setup_logger()


def get_env(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val else default


BUCKET = get_env("LATEST_PRICES_BUCKET", "dfi-signal-dashboard")
LATEST_KEY = get_env("LATEST_PRICES_KEY", "signal-dashboard/data/latest_prices.json")
LATEST_META_KEY = get_env("LATEST_META_KEY", "signal-dashboard/data/latest.json")
REGION = get_env("REGION", "eu-west-1")
LOOP_SECONDS = int(get_env("LOOP_SECONDS", "60"))


def s3_client():
    return boto3.client("s3", region_name=REGION)


def read_s3_text(bucket: str, key: str) -> str:
    client = s3_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def discover_symbols_from_latest_csv() -> List[str]:
    """Resolve latest CSV from latest.json and return normalized symbol list (e.g., BTCUSDT)."""
    try:
        meta_raw = read_s3_text(BUCKET, LATEST_META_KEY)
        meta = json.loads(meta_raw)
        csv_name = meta.get("latest_csv") or meta.get("filename") or meta.get("latest")
        if not csv_name:
            logger.warning("latest.json missing latest CSV reference; falling back to empty symbol list")
            return []
        csv_raw = read_s3_text(BUCKET, f"signal-dashboard/data/{csv_name}")
        reader = csv.DictReader(io.StringIO(csv_raw))
        symbols: List[str] = []
        for row in reader:
            sym = (row.get("ticker") or row.get("ric") or row.get("internal_code") or "").strip()
            if not sym:
                continue
            sym = sym.replace("_", "")
            if sym not in symbols:
                symbols.append(sym)
        return symbols
    except botocore.exceptions.ClientError as e:
        logger.error("Failed to read latest CSV from S3: %s", e)
        return []
    except Exception as e:
        logger.error("Unexpected error discovering symbols: %s", e)
        return []


def fetch_mark_price(symbol: str) -> float | None:
    """Fetch Binance futures mark price for a symbol (e.g., BTCUSDT)."""
    try:
        r = requests.get(BINANCE_MARK_URL, params={"symbol": symbol}, timeout=10)
        if r.status_code != 200:
            logger.warning("Binance mark price HTTP %s for %s", r.status_code, symbol)
            return None
        data = r.json()
        # premiumIndex returns a dict with 'markPrice'
        mark = float(data.get("markPrice"))
        return mark
    except Exception as e:
        logger.warning("Failed to fetch mark for %s: %s", symbol, e)
        return None


def fetch_all_marks(symbols: List[str]) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    for sym in symbols:
        price = fetch_mark_price(sym)
        if price is not None:
            prices[sym] = price
        # Light pacing to avoid burst
        time.sleep(0.05)
    return prices


def write_latest_prices(prices: Dict[str, float]) -> None:
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "prices": prices,
    }
    body = json.dumps(payload, separators=(",", ":"))
    client = s3_client()
    client.put_object(
        Bucket=BUCKET,
        Key=LATEST_KEY,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
        ServerSideEncryption="AES256",
    )


def main() -> None:
    logger.info("Starting latest_prices writer: bucket=%s key=%s region=%s", BUCKET, LATEST_KEY, REGION)
    # Safety guard: refuse to write to prod key unless explicitly allowed
    if os.getenv("ALLOW_PROD_WRITE") != "1" and BUCKET == "dfi-signal-dashboard" and LATEST_KEY == "signal-dashboard/data/latest_prices.json":
        logger.info("Refusing to write primary latest_prices.json without ALLOW_PROD_WRITE=1 (safe default).")
        logger.info("Set LATEST_PRICES_KEY to an alternate path or export ALLOW_PROD_WRITE=1 for intentional prod writes.")
        sys.exit(0)
    # Warm-up discovery
    symbols = discover_symbols_from_latest_csv()
    if not symbols:
        logger.warning("No symbols discovered initially. Will retry later.")

    while True:
        try:
            if not symbols:
                symbols = discover_symbols_from_latest_csv()
                if not symbols:
                    logger.warning("Still no symbols discovered. Sleeping.")
                    time.sleep(LOOP_SECONDS)
                    continue

            prices = fetch_all_marks(symbols)
            logger.info("Fetched %d/%d marks", len(prices), len(symbols))
            write_latest_prices(prices)
            logger.info("Uploaded latest_prices.json with %d symbols", len(prices))
        except Exception as e:
            logger.exception("Top-level error in writer loop: %s", e)

        # Randomize a bit to avoid alignment with other jobs
        sleep_s = LOOP_SECONDS + random.uniform(-2.0, 2.0)
        if sleep_s < 5:
            sleep_s = 5
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()


