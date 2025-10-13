#!/usr/bin/env python3
"""
AWS Lambda: Latest Prices Writer - Fetches Binance marks and writes to S3
Provides live price data for S3-driven UI without browser API calls
Triggered by CloudWatch Events every 60 seconds
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import ssl
import os

# Configuration - can be overridden by environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "signal-dashboard/data/")
# Mirror writes for demo and pulse so both have identical marks at the same second
DEMO_KEY_PREFIX = os.environ.get("DEMO_KEY_PREFIX", "signal-dashboard-demo/signal-dashboard/data/")
PULSE_KEY_PREFIX = os.environ.get("PULSE_KEY_PREFIX", "descartes-ml/signal-dashboard/data/")
BENCH_PREFIX = os.environ.get("BENCH_PREFIX", "signal-dashboard/benchmarks/")
BINANCE_FUTURES_API = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_SPOT_KLINES = "https://api.binance.com/api/v3/klines"

# List of symbols to fetch prices for
SYMBOLS_TO_FETCH = [
    'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'SOLUSDT', 'TRXUSDT',
    'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'ICPUSDT', 'XLMUSDT',
    'HBARUSDT', 'FETUSDT', 'BCHUSDT', 'LTCUSDT', 'DOTUSDT', 'TONUSDT',
    'RENDERUSDT', 'SUIUSDT', 'UNIUSDT', 'NEARUSDT', 'ETCUSDT', 'AAVEUSDT',
    'VETUSDT', 'ATOMUSDT', 'ALGOUSDT', 'APTUSDT', 'FILUSDT'
]

def log(msg: str) -> None:
    """Simple logging function for Lambda."""
    print(f"[{datetime.now().isoformat()}] {msg}")

def get_mark_prices(symbols: list[str]) -> dict:
    """Fetches current mark prices for a list of symbols from Binance Futures API."""
    prices = {}

    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    for symbol in symbols:
        try:
            params = urllib.parse.urlencode({'symbol': symbol})
            url = f"{BINANCE_FUTURES_API}?{params}"

            with urllib.request.urlopen(url, context=ssl_context, timeout=10) as response:
                data = json.loads(response.read().decode())
                mark_price = float(data['markPrice'])
                prices[symbol] = mark_price
                log(f"✅ {symbol}: {mark_price}")
        except Exception as e:
            log(f"❌ Error fetching price for {symbol}: {e}")
    return prices


def fetch_btc_daily_klines(limit: int = 1500) -> list[dict]:
    """Fetch BTCUSDT spot 1D klines from Binance.

    Returns list of {date: YYYY-MM-DD, close: float}.
    """
    params = urllib.parse.urlencode({
        'symbol': 'BTCUSDT',
        'interval': '1d',
        'limit': str(limit)
    })
    url = f"{BINANCE_SPOT_KLINES}?{params}"

    # Reuse a permissive SSL context like get_mark_prices
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    with urllib.request.urlopen(url, context=ssl_context, timeout=10) as response:
        data = json.loads(response.read().decode())

    out: list[dict] = []
    for k in data:
        # [openTime, open, high, low, close, volume, closeTime, ...]
        close_ts = int(k[6])
        close_price = float(k[4])
        day = datetime.fromtimestamp(close_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
        out.append({'date': day, 'close': close_price})
    return out


def compute_daily_returns(rows: list[dict]) -> list[dict]:
    rows = sorted(rows, key=lambda r: r['date'])
    out: list[dict] = []
    prev_close: float | None = None
    for r in rows:
        c = r['close']
        if prev_close and prev_close > 0:
            ret = (c - prev_close) / prev_close * 100.0
            out.append({'Date': r['date'], 'Performance': round(ret, 2)})
        prev_close = c
    return out


def write_btc_benchmark(s3_client) -> None:
    """Write BTC daily close JSONL and daily returns CSV under BENCH_PREFIX."""
    try:
        rows = fetch_btc_daily_klines(limit=1500)
        if not rows:
            log("❌ No BTC klines fetched")
            return
        # JSONL of closes
        jsonl_body = "\n".join(json.dumps({'date': r['date'], 'close': r['close']}) for r in rows) + "\n"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=BENCH_PREFIX + 'btc_daily_close.jsonl',
            Body=jsonl_body.encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-store, max-age=0',
            ACL='public-read'
        )
        # CSV of daily returns
        rets = compute_daily_returns(rows)
        csv_lines = ['Date,Performance'] + [f"{r['Date']},{r['Performance']:.2f}" for r in rets]
        csv_body = "\n".join(csv_lines) + "\n"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=BENCH_PREFIX + 'btc_daily_returns.csv',
            Body=csv_body.encode('utf-8'),
            ContentType='text/csv; charset=utf-8',
            CacheControl='no-store, max-age=0',
            ACL='public-read'
        )
        log(f"✅ Wrote BTC benchmark: {len(rows)} closes, {len(rets)} returns")
    except Exception as e:
        log(f"❌ BTC benchmark write failed: {e}")

def lambda_handler(event, context):
    """AWS Lambda handler for price writing."""
    log("🚀 Starting Latest Prices Writer Lambda...")

    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')

        # Get current prices
        log(f"📊 Fetching prices for {len(SYMBOLS_TO_FETCH)} symbols...")
        current_prices = get_mark_prices(SYMBOLS_TO_FETCH)

        if not current_prices:
            log("❌ No prices fetched, skipping S3 write")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No prices available',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }

        log(f"📊 Successfully fetched {len(current_prices)}/{len(SYMBOLS_TO_FETCH)} prices")

        # Create price data structure
        timestamp = datetime.now(timezone.utc).isoformat()
        prices_data = {
            'timestamp_utc': timestamp,
            'prices': current_prices
        }

        # Write to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY_PREFIX + 'latest_prices.json',
            Body=json.dumps(prices_data, indent=2),
            ContentType='application/json',
            CacheControl='no-store, max-age=0'
        )

        # Mirror to demo path
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=DEMO_KEY_PREFIX + 'latest_prices.json',
                Body=json.dumps(prices_data, indent=2),
                ContentType='application/json',
                CacheControl='no-store, max-age=0'
            )
            log("✅ Mirrored latest_prices.json to demo path")
        except Exception as mirror_err:
            log(f"⚠️ Failed to mirror latest_prices.json to demo path: {mirror_err}")

        # Mirror to pulse path
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=PULSE_KEY_PREFIX + 'latest_prices.json',
                Body=json.dumps(prices_data, indent=2),
                ContentType='application/json',
                CacheControl='no-store, max-age=0'
            )
            log("✅ Mirrored latest_prices.json to pulse path")
        except Exception as mirror_err2:
            log(f"⚠️ Failed to mirror latest_prices.json to pulse path: {mirror_err2}")

        log("✅ Latest prices updated successfully")

        # Also update BTC daily benchmark artifacts (JSONL closes + CSV returns)
        try:
            write_btc_benchmark(s3_client)
        except Exception as _e:
            # Already logged inside write_btc_benchmark
            pass

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Latest prices updated successfully',
                'prices_count': len(current_prices),
                'timestamp': timestamp
            })
        }

    except Exception as e:
        log(f"❌ Lambda execution failed: {str(e)}")
        import traceback
        log(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

