import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
S3_PREFIX = os.environ.get("S3_PREFIX", "signal-dashboard/benchmarks/")
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")

s3 = boto3.client("s3")


def _utc_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def fetch_binance_1d(symbol: str, limit: int = 1500):
    url = (
        f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit={limit}"
    )
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode("utf-8"))
    # Each kline: [openTime, open, high, low, close, volume, closeTime, ...]
    out = []
    for k in data:
        open_ts, close_price, close_ts = int(k[0]), float(k[4]), int(k[6])
        out.append(
            {
                "date": _utc_iso(close_ts)[:10],
                "open_time": _utc_iso(open_ts),
                "close_time": _utc_iso(close_ts),
                "close": close_price,
            }
        )
    return out


def compute_daily_returns(rows):
    # rows: [{date, close}]
    rows = sorted(rows, key=lambda x: x["date"])
    out = []
    prev = None
    for r in rows:
        if prev is not None and prev > 0:
            ret = (r["close"] - prev) / prev * 100.0
            out.append({"Date": r["date"], "Performance": round(ret, 2)})
        prev = r["close"]
    return out


def put_text(key: str, body: str, content_type: str):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=content_type,
        CacheControl="no-store, max-age=0",
        ACL="bucket-owner-full-control",
    )


def lambda_handler(event, context):
    rows = fetch_binance_1d(SYMBOL, limit=1500)
    # Write jsonl of daily close
    jsonl_key = f"{S3_PREFIX}btc_daily_close.jsonl"
    jsonl_body = "\n".join(
        [json.dumps({"date": r["date"], "close": r["close"]}) for r in rows]
    )
    put_text(jsonl_key, jsonl_body + "\n", "application/json")

    # Write CSV of daily returns
    returns = compute_daily_returns(rows)
    csv_key = f"{S3_PREFIX}btc_daily_returns.csv"
    csv_body = "Date,Performance\n" + "\n".join(
        [f"{r['Date']},{r['Performance']:.2f}" for r in returns]
    )
    put_text(csv_key, csv_body + "\n", "text/csv; charset=utf-8")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "btc daily written",
                "jsonl_key": jsonl_key,
                "csv_key": csv_key,
                "rows": len(rows),
                "returns": len(returns),
            }
        ),
    }




