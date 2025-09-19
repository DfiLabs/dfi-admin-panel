#!/usr/bin/env python3
"""
Portfolio Value Logger - Logs PV data every 5 minutes to S3
Simple, reliable system that actually works
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3
import os

# Configuration
S3_BUCKET = "dfi-signal-dashboard"
S3_KEY_PREFIX = "signal-dashboard/data/portfolio_value_log.jsonl"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"

# Load baseline data
def load_baseline():
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key="signal-dashboard/data/daily_baseline.json")
        baseline = json.loads(response['Body'].read())
        return baseline
    except Exception as e:
        print(f"Error loading baseline: {e}")
        return None

# Get current prices from Binance
def get_current_prices(symbols):
    prices = {}
    for symbol in symbols:
        try:
            url = f"{BINANCE_API}?symbol={symbol}"
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read())
                prices[symbol] = float(data['price'])
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
    return prices

# Calculate portfolio value
def calculate_portfolio_value(baseline, current_prices):
    if not baseline or 'prices' not in baseline:
        return None
    
    total_value = 0
    daily_pnl = 0
    
    for symbol, baseline_price in baseline['prices'].items():
        if symbol in current_prices:
            current_price = current_prices[symbol]
            # Simple calculation: assume equal weight for now
            # In reality, you'd use the actual position data
            value_change = (current_price - baseline_price) / baseline_price
            total_value += 1000000 / len(baseline['prices']) * (1 + value_change)
            daily_pnl += 1000000 / len(baseline['prices']) * value_change
    
    return {
        'portfolio_value': total_value,
        'daily_pnl': daily_pnl,
        'total_pnl': total_value - 1000000
    }

# Log PV data to S3
def log_pv_to_s3(pv_data):
    try:
        s3 = boto3.client('s3')
        
        # Create log entry
        log_entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'portfolio_value': pv_data['portfolio_value'],
            'daily_pnl': pv_data['daily_pnl'],
            'total_pnl': pv_data['total_pnl']
        }
        
        # Append to JSONL file
        log_line = json.dumps(log_entry) + '\n'
        
        # Get existing data
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_PREFIX)
            existing_data = response['Body'].read().decode('utf-8')
        except:
            existing_data = ""
        
        # Append new data
        new_data = existing_data + log_line
        
        # Upload back to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY_PREFIX,
            Body=new_data,
            ContentType='application/json'
        )
        
        print(f"✅ Logged PV: ${pv_data['portfolio_value']:,.2f} at {log_entry['timestamp']}")
        return True
        
    except Exception as e:
        print(f"❌ Error logging to S3: {e}")
        return False

# Main function
def main():
    print("🚀 Starting PV Logger...")
    
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
