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
PULSE_KEY = "descartes-ml/signal-dashboard/data/latest_prices.json"
BINANCE_API = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Load baseline to get portfolio symbols
def load_baseline_symbols():
    """Backwards-compatible: load symbols from Admin baseline if needed.
    Not used by the new bulk fetch path, but kept as a fallback.
    """
    try:
        result = subprocess.run([
            'aws', 's3', 'cp',
            f's3://{S3_BUCKET}/signal-dashboard/data/daily_baseline.json',
            '/tmp/baseline.json'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            with open('/tmp/baseline.json', 'r') as f:
                baseline = json.load(f)
            # Accept both legacy {prices:{TICKER:price}} and new {ref_prices:{...}}
            prices = baseline.get('prices') or baseline.get('ref_prices') or {}
            return list(prices.keys())
        else:
            print(f"Error loading baseline: {result.stderr}")
            return []
    except Exception as e:
        print(f"Error loading baseline symbols: {e}")
        return []

# Get current prices from Binance Futures
def fetch_all_mark_prices():
    """Fetch all Binance Futures USDT perpetual mark prices in one call.

    Returns a dict { SYMBOL: markPrice } filtered to *USDT symbols.
    """
    prices = {}
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    try:
        url = BINANCE_API  # without symbol → returns array of all instruments
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ssl_context, timeout=15) as response:
            data = json.loads(response.read().decode())
            if isinstance(data, list):
                for item in data:
                    try:
                        sym = str(item.get('symbol', ''))
                        if not sym.endswith('USDT'):
                            continue
                        mp = float(item.get('markPrice', 0))
                        if mp > 0:
                            prices[sym] = mp
                    except Exception:
                        continue
            else:
                # Fallback: if API returns a single object (unexpected), ignore
                pass
    except Exception as e:
        print(f"Error bulk fetching marks: {e}")
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
        # Admin root
        result = subprocess.run([
            'aws', 's3', 'cp',
            '/tmp/latest_prices.json',
            f's3://{S3_BUCKET}/{S3_KEY}'
        ], capture_output=True, text=True, timeout=30)
        ok_admin = (result.returncode == 0)
        if not ok_admin:
            print(f"Error uploading prices (ADMIN): {result.stderr}")

        # Pulse root
        result2 = subprocess.run([
            'aws', 's3', 'cp',
            '/tmp/latest_prices.json',
            f's3://{S3_BUCKET}/{PULSE_KEY}'
        ], capture_output=True, text=True, timeout=30)
        ok_pulse = (result2.returncode == 0)
        if not ok_pulse:
            print(f"Error uploading prices (PULSE): {result2.stderr}")

        if ok_admin or ok_pulse:
            print(f"✅ Latest prices updated at {timestamp} (admin={ok_admin}, pulse={ok_pulse})")
            return True
        return False
    except Exception as e:
        print(f"Error writing prices to S3: {e}")
        return False

def main():
    print("🚀 Starting Latest Prices Writer...")
    print("Fetching Binance marks every 60s and writing to S3")

    while True:
        try:
            # New path: fetch all USDT perp marks in one call (covers 200+ symbols)
            all_prices = fetch_all_mark_prices()
            if all_prices:
                write_prices_to_s3(all_prices)
                print(f"📊 Successfully wrote {len(all_prices)} symbols (bulk fetch)")
            else:
                # Fallback: try baseline-limited symbols if bulk fails
                symbols = load_baseline_symbols()
                if symbols:
                    print(f"⚠️ Bulk fetch failed; falling back to {len(symbols)} baseline symbols…")
                    # Reuse old per-symbol path to get something on S3
                    # Import lazily to avoid unused warnings
                    def _get_current_prices(symbols_local):
                        prices_local = {}
                        ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
                        for sym in symbols_local:
                            try:
                                url = f"{BINANCE_API}?symbol={sym}"
                                with urllib.request.urlopen(urllib.request.Request(url), context=ctx, timeout=10) as resp:
                                    d = json.loads(resp.read().decode()); prices_local[sym]=float(d.get('markPrice',0))
                            except Exception:
                                prices_local[sym]=None
                        return {k:v for k,v in prices_local.items() if v is not None}
                    limited = _get_current_prices(symbols)
                    if limited:
                        write_prices_to_s3(limited)
                        print(f"📊 Wrote {len(limited)} symbols (fallback)")
                    else:
                        print("❌ No valid prices to write")

        except Exception as e:
            print(f"❌ Error in main loop: {e}")

        # Wait 60 seconds
        print("⏰ Waiting 60s before next update...")
        time.sleep(60)

if __name__ == "__main__":
    main()
