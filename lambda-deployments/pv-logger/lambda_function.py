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
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "signal-dashboard/data/")
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

def load_s3_text(s3_client, key: str) -> str:
    """Load text file from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        log(f"Error loading {key}: {e}")
        return ""

def get_latest_csv_filename(s3_client) -> str:
    """Get the latest CSV filename from portfolio_daily_log.csv, fallback to pre_execution.json."""
    try:
        # First try portfolio_daily_log.csv
        daily_log = load_s3_text(s3_client, S3_KEY_PREFIX + 'portfolio_daily_log.csv')
        if daily_log:
            lines = daily_log.strip().split('\n')
            for line in reversed(lines):
                if 'post_execution' in line:
                    parts = line.split(',')
                    if len(parts) > 4:
                        log(f"📄 CSV from daily log: {parts[4]}")
                        return parts[4]
        
        # Fallback to pre_execution.json
        log("📄 Daily log empty, checking pre_execution.json...")
        pre_exec_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'pre_execution.json')
        if pre_exec_data and 'csv_filename' in pre_exec_data:
            csv_filename = pre_exec_data['csv_filename']
            log(f"📄 CSV from pre_execution.json: {csv_filename}")
            return csv_filename
            
            return None
    except Exception as e:
        log(f"Error getting latest CSV filename: {e}")
        return None

def get_pv_pre(s3_client) -> float:
    """Get pv_pre from pre_execution.json, fallback to 1M."""
    try:
        pre_exec_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'pre_execution.json')
        pv_pre = float(pre_exec_data.get('pv_pre', 1000000.0))
        log(f"📊 PV_pre from pre_execution.json: ${pv_pre:,.2f}")
        return pv_pre
    except Exception as e:
        log(f"⚠️ Could not load PV_pre, using fallback: {e}")
        return 1000000.0

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
        
        # Load baseline data (for baseline prices)
        baseline_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'daily_baseline.json')
        if not baseline_data or 'prices' not in baseline_data:
            log("❌ Failed to load baseline prices")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to load baseline prices',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        # Get pv_pre (correct starting point)
        pv_pre = get_pv_pre(s3_client)
        
        # Get latest CSV filename
        latest_csv = get_latest_csv_filename(s3_client)
        if not latest_csv:
            log("❌ No CSV filename found")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'No CSV filename found',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        log(f"📄 Using CSV: {latest_csv}")
        
        # Load CSV content to get positions
        csv_content = load_s3_text(s3_client, S3_KEY_PREFIX + latest_csv)
        if not csv_content:
            log("❌ Failed to load CSV content")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to load CSV content',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        # Parse CSV and calculate daily P&L
        lines = csv_content.strip().split('\n')
        if len(lines) < 2:
            log("❌ CSV has no position data")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'CSV has no position data',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        
        headers = lines[0].split(',')
        daily_pnl = 0.0
        positions_count = 0

        baseline_prices = baseline_data['prices']

        for i in range(1, len(lines)):
            if lines[i].strip():
                values = lines[i].split(',')
                position = {}
                for j, header in enumerate(headers):
                    position[header.strip()] = values[j].strip() if j < len(values) else ''

                symbol = position.get('ticker', '').replace('_', '')
                baseline_price = baseline_prices.get(symbol)
                current_price = current_prices.get(symbol)
                
                try:
                    notional = float(position.get('target_notional', 0))
                    contracts = float(position.get('target_contracts', 0))
                except (ValueError, TypeError):
                    continue

                if baseline_price and current_price and notional != 0:
                    side = 1 if contracts > 0 else -1
                    pnl = side * (current_price - baseline_price) / baseline_price * abs(notional)
                    daily_pnl += pnl
                    positions_count += 1
                    
                    # Debug key positions
                    if symbol in ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']:
                        log(f"🔍 {symbol}: baseline={baseline_price}, current={current_price}, side={side}, pnl=${pnl:.2f}")
        
        # CORRECT FORMULA: Portfolio Value = pv_pre + Daily P&L
        portfolio_value = pv_pre + daily_pnl
        total_pnl = portfolio_value - 1000000.0
        
        log(f"💰 Calculated PV: ${portfolio_value:,.2f} (Daily P&L: ${daily_pnl:,.2f}, Positions: {positions_count})")
        log(f"📊 PV_pre: ${pv_pre:,.2f}, Daily P&L: ${daily_pnl:,.2f}, Total P&L: ${total_pnl:,.2f}")
        
        # Create log entry
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'portfolio_value': portfolio_value,
            'daily_pnl': daily_pnl,
            'total_pnl': total_pnl,
            'audit': {
                'source': 'lambda-fixed',
                'positions_processed': positions_count,
                'pv_pre': pv_pre,
                'csv_filename': latest_csv
            }
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
            Body=new_content.encode('utf-8'),
            ContentType='application/x-ndjson'
        )
        
        log("✅ PV logging completed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'PV logged successfully',
                'portfolio_value': portfolio_value,
                'daily_pnl': daily_pnl,
                'positions_count': positions_count,
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
