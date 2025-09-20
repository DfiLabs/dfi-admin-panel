#!/usr/bin/env python3
"""
Latest Prices Writer - Fetches Binance marks every 60s and writes to S3
Provides live price data for S3-driven UI without browser API calls
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import subprocess
import os
import ssl

# Configuration
S3_BUCKET = "dfi-signal-dashboard"
S3_KEY = "signal-dashboard/data/latest_prices.json"
BINANCE_API = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Load baseline to get portfolio symbols
def load_baseline_symbols():
    try:
        result = subprocess.run([
            'aws', 's3', 'cp',
            f's3://{S3_BUCKET}/signal-dashboard/data/daily_baseline.json',
            '/tmp/baseline.json'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            with open('/tmp/baseline.json', 'r') as f:
                baseline = json.load(f)
            return list(baseline.get('prices', {}).keys())
        else:
            print(f"Error loading baseline: {result.stderr}")
            return []
    except Exception as e:
        print(f"Error loading baseline symbols: {e}")
        return []

# Get current prices from Binance Futures
def get_current_prices(symbols):
    prices = {}

    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    for symbol in symbols:
        try:
            url = f"{BINANCE_API}?symbol={symbol}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, context=ssl_context, timeout=10) as response:
                data = json.loads(response.read().decode())
                prices[symbol] = float(data.get('markPrice', 0))
                print(f"✅ {symbol}: {prices[symbol]}")
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            prices[symbol] = None

    return prices

# Write latest prices to S3 using AWS CLI
def write_prices_to_s3(prices):
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        data = {
            'timestamp_utc': timestamp,
            'prices': prices
        }

        # Write to temporary file first
        with open('/tmp/latest_prices.json', 'w') as f:
            json.dump(data, f, indent=2)

        # Upload to S3 atomically
        result = subprocess.run([
            'aws', 's3', 'cp',
            '/tmp/latest_prices.json',
            f's3://{S3_BUCKET}/{S3_KEY}'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            print(f"✅ Latest prices updated at {timestamp}")
            return True
        else:
            print(f"Error uploading prices: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error writing prices to S3: {e}")
        return False

def main():
    print("🚀 Starting Latest Prices Writer...")
    print("Fetching Binance marks every 60s and writing to S3")

    while True:
        try:
            # Load symbols from baseline
            symbols = load_baseline_symbols()

            if not symbols:
                print("❌ No symbols loaded from baseline, using fallback list")
                symbols = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'SOLUSDT', 'TRXUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT']

            print(f"📊 Fetching prices for {len(symbols)} symbols...")

            # Fetch current prices
            prices = get_current_prices(symbols)

            # Filter out failed fetches
            valid_prices = {k: v for k, v in prices.items() if v is not None}

            if valid_prices:
                # Write to S3
                write_prices_to_s3(valid_prices)
                print(f"📊 Successfully wrote {len(valid_prices)}/{len(symbols)} prices")
            else:
                print("❌ No valid prices to write")

        except Exception as e:
            print(f"❌ Error in main loop: {e}")

        # Wait 60 seconds
        print("⏰ Waiting 60s before next update...")
        time.sleep(60)

if __name__ == "__main__":
    main()
