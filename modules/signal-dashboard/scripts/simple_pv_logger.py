#!/usr/bin/env python3
"""
Simple Portfolio Value Logger - No external dependencies
Logs PV data every 5 minutes to S3 using AWS CLI
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import subprocess
import os

# Configuration
S3_BUCKET = "dfi-signal-dashboard"
S3_KEY = "signal-dashboard/data/portfolio_value_log.jsonl"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"

# Load baseline data using AWS CLI
def load_baseline():
    try:
        result = subprocess.run([
            'aws', 's3', 'cp', 
            f's3://{S3_BUCKET}/signal-dashboard/data/daily_baseline.json',
            '/tmp/baseline.json'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            with open('/tmp/baseline.json', 'r') as f:
                baseline = json.load(f)
            return baseline
        else:
            print(f"Error loading baseline: {result.stderr}")
            return None
    except Exception as e:
        print(f"Error loading baseline: {e}")
        return None

# Get current prices from Binance
def get_current_prices(symbols):
    import ssl
    prices = {}
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    for symbol in symbols:
        try:
            url = f"{BINANCE_API}?symbol={symbol}"
            request = urllib.request.Request(url)
            with urllib.request.urlopen(request, context=ssl_context) as response:
                data = json.loads(response.read())
                prices[symbol] = float(data['price'])
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
    return prices

# Calculate portfolio value using actual position data
def calculate_portfolio_value(baseline, current_prices):
    if not baseline or 'prices' not in baseline:
        return None
    
    # Load the current CSV to get actual positions
    try:
        result = subprocess.run([
            'aws', 's3', 'cp', 
            f's3://{S3_BUCKET}/signal-dashboard/data/portfolio_daily_log.csv',
            '/tmp/daily_log.csv'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print("Error loading daily log")
            return None
            
        with open('/tmp/daily_log.csv', 'r') as f:
            lines = f.read().split('\n')
        
        # Find latest post_execution row
        latest_csv = None
        for line in reversed(lines):
            if 'post_execution' in line:
                parts = line.split(',')
                if len(parts) > 4:
                    latest_csv = parts[4]
                    break
        
        if not latest_csv:
            print("No CSV found in daily log")
            return None
            
        # Load the CSV file
        result = subprocess.run([
            'aws', 's3', 'cp', 
            f's3://{S3_BUCKET}/signal-dashboard/data/{latest_csv}',
            '/tmp/current.csv'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error loading CSV: {latest_csv}")
            return None
            
        with open('/tmp/current.csv', 'r') as f:
            csv_content = f.read()
        
        lines = csv_content.split('\n')
        headers = lines[0].split(',')
        
        total_value = 1000000  # Start with initial capital
        daily_pnl = 0
        processed_count = 0
        
        for i in range(1, len(lines)):
            if lines[i].strip():
                values = lines[i].split(',')
                position = {}
                for j, header in enumerate(headers):
                    position[header.strip()] = values[j].strip() if j < len(values) else ''
                
                symbol = position.get('ticker', '').replace('_', '')
                baseline_price = baseline['prices'].get(symbol)
                current_price = current_prices.get(symbol)
                notional = float(position.get('target_notional', 0))
                contracts = float(position.get('target_contracts', 0))
                
                if baseline_price and current_price and notional != 0:
                    side = 1 if contracts > 0 else -1
                    pnl = side * (current_price - baseline_price) / baseline_price * abs(notional)
                    daily_pnl += pnl
                    processed_count += 1
                    
                    # Debug first 3 positions and some shorts
                    if symbol in ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LINKUSDT', 'FETUSDT', 'LTCUSDT']:
                        print(f"🔍 {symbol}: baseline={baseline_price}, current={current_price}, side={side}, notional={notional:.0f}, pnl={pnl:.2f}")
                else:
                    print(f"⚠️ Skipped {symbol}: baseline={baseline_price}, current={current_price}, notional={notional}")
        
        print(f"📊 Processed {processed_count} positions")
        
        print(f"📊 Total Daily P&L: ${daily_pnl:.2f}")
        print(f"📊 Portfolio Value: ${1000000 + daily_pnl:.2f}")
        
        # Portfolio Value = $1M + Daily P&L (same as dashboard)
        portfolio_value = 1000000 + daily_pnl
        
        return {
            'portfolio_value': portfolio_value,
            'daily_pnl': daily_pnl,
            'total_pnl': daily_pnl  # For now, assume daily = total
        }
        
    except Exception as e:
        print(f"Error calculating portfolio value: {e}")
        return None

# Log PV data to S3 using AWS CLI
def log_pv_to_s3(pv_data):
    try:
        # Create log entry
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'portfolio_value': pv_data['portfolio_value'],
            'daily_pnl': pv_data['daily_pnl'],
            'total_pnl': pv_data['total_pnl']
        }
        
        # Get existing data
        try:
            result = subprocess.run([
                'aws', 's3', 'cp',
                f's3://{S3_BUCKET}/{S3_KEY}',
                '/tmp/pv_log.jsonl'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                with open('/tmp/pv_log.jsonl', 'r') as f:
                    existing_data = f.read()
            else:
                existing_data = ""
        except:
            existing_data = ""
        
        # Append new data
        log_line = json.dumps(log_entry) + '\n'
        new_data = existing_data + log_line
        
        # Write to temp file
        with open('/tmp/pv_log.jsonl', 'w') as f:
            f.write(new_data)
        
        # Upload to S3
        result = subprocess.run([
            'aws', 's3', 'cp',
            '/tmp/pv_log.jsonl',
            f's3://{S3_BUCKET}/{S3_KEY}'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Logged PV: ${pv_data['portfolio_value']:,.2f} at {log_entry['timestamp']}")
            return True
        else:
            print(f"❌ Error uploading to S3: {result.stderr}")
            return False
        
    except Exception as e:
        print(f"❌ Error logging to S3: {e}")
        return False

# Main function
def main():
    print("🚀 Starting Simple PV Logger...")
    
    # Load baseline
    baseline = load_baseline()
    if not baseline:
        print("❌ Failed to load baseline data")
        return
    
    # Get symbols from baseline
    symbols = list(baseline['prices'].keys())
    print(f"📊 Monitoring {len(symbols)} symbols")
    
    # Get current prices
    current_prices = get_current_prices(symbols)
    if not current_prices:
        print("❌ Failed to get current prices")
        return
    
    print(f"📊 Got prices for {len(current_prices)} symbols")
    print(f"📊 Symbols with prices: {list(current_prices.keys())}")
    print(f"📊 Missing symbols: {[s for s in symbols if s not in current_prices]}")
    
    # Calculate portfolio value
    pv_data = calculate_portfolio_value(baseline, current_prices)
    if not pv_data:
        print("❌ Failed to calculate portfolio value")
        return
    
    # Log to S3
    success = log_pv_to_s3(pv_data)
    if success:
        print("✅ PV logging completed successfully")
    else:
        print("❌ PV logging failed")

if __name__ == "__main__":
    main()
