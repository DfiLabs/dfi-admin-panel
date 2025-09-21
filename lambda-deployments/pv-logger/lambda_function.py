#!/usr/bin/env python3
"""
AWS Lambda: Simple Portfolio Value Logger
Logs PV data to S3 using boto3 (Lambda compatible)
Triggered by CloudWatch Events every 5 minutes
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

def load_s3_json(s3_client, key: str) -> dict:
    """Load a JSON file from S3 using boto3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        log(f"Error loading {key}: {e}")
        return {}
    except json.JSONDecodeError as e:
        log(f"Error parsing JSON from {key}: {e}")
        return {}

def lambda_handler(event, context):
    """AWS Lambda handler for PV logging."""
    log("🚀 Starting Simple PV Logger Lambda...")
    
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get current prices
        log("📊 Fetching current market prices...")
        current_prices = get_mark_prices(SYMBOLS_TO_FETCH)
        
        if not current_prices:
            log("❌ No prices fetched, skipping PV calculation")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No prices available',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        log(f"✅ Fetched {len(current_prices)}/{len(SYMBOLS_TO_FETCH)} prices")
        
        # Load baseline data
        baseline_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'daily_baseline.json')
        if not baseline_data:
            log("❌ Failed to load baseline data")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to load baseline data',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        # Calculate P&L for each position
        total_pnl = 0.0
        positions_count = 0
        
        # Use positions from baseline
        positions = baseline_data.get('positions', [])
        
        for pos in positions:
            symbol = pos.get('ticker', '').replace('_', '')
            if symbol in current_prices:
                baseline_price = pos.get('ref_price', 0)
                current_price = current_prices[symbol]
                notional = pos.get('target_notional', 0)
                contracts = pos.get('target_contracts', 0)
                
                if baseline_price > 0 and notional != 0:
                    # Use same P&L formula as dashboard
                    side_multiplier = 1 if contracts > 0 else -1
                    pnl = side_multiplier * (current_price - baseline_price) / baseline_price * abs(notional)
                    total_pnl += pnl
                    positions_count += 1
        
        # Portfolio Value = Initial Capital + Daily P&L
        initial_capital = 1000000.0
        portfolio_value = initial_capital + total_pnl
        
        log(f"💰 Calculated PV: ${portfolio_value:,.2f} (P&L: ${total_pnl:,.2f}, Positions: {positions_count})")
        
        # Create log entry
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'portfolio_value': portfolio_value,
            'daily_pnl': total_pnl,
            'total_pnl': total_pnl  # For now, daily = total
        }
        
        # Load existing log file
        try:
            existing_content = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_PREFIX + 'portfolio_value_log.jsonl')['Body'].read().decode('utf-8')
            new_content = existing_content + json.dumps(log_entry) + '\n'
        except ClientError:
            # File doesn't exist, create new content
            new_content = json.dumps(log_entry) + '\n'
        
        # Write updated log file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY_PREFIX + 'portfolio_value_log.jsonl',
            Body=new_content,
            ContentType='application/jsonl'
        )
        
        log("✅ PV logging completed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'PV logging completed successfully',
                'portfolio_value': portfolio_value,
                'daily_pnl': total_pnl,
                'timestamp': datetime.now(timezone.utc).isoformat()
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
