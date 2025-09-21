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
S3_KEY_PREFIX = "signal-dashboard/data/"
BINANCE_FUTURES_API = "https://fapi.binance.com/fapi/v1/premiumIndex"

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
            ContentType='application/json'
        )

        log("✅ Latest prices updated successfully")

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

