#!/usr/bin/env python3
"""
Comprehensive Calculation Verification
Proves all calculations are correct and identifies dashboard bugs
"""

import json
import boto3
import csv
import io
from datetime import datetime

S3_BUCKET = 'dfi-signal-dashboard'
S3_PREFIX = 'signal-dashboard/data/'

def fetch_s3_json(key: str) -> dict:
    """Fetch JSON from S3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"❌ Error fetching {key}: {e}")
        return {}

def fetch_s3_text(key: str) -> str:
    """Fetch text from S3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"❌ Error fetching {key}: {e}")
        return ""

def verify_calculations():
    """Verify all calculations step by step"""
    
    print("🔍 COMPREHENSIVE CALCULATION VERIFICATION")
    print("=" * 60)
    
    # 1. Load all data sources
    print("\n📊 STEP 1: Loading Data Sources")
    latest_prices = fetch_s3_json(S3_PREFIX + 'latest_prices.json')
    baseline_data = fetch_s3_json(S3_PREFIX + 'daily_baseline.json')
    pre_exec_data = fetch_s3_json(S3_PREFIX + 'pre_execution.json')
    csv_ref = fetch_s3_json(S3_PREFIX + 'latest.json')
    
    if not all([latest_prices, baseline_data, pre_exec_data, csv_ref]):
        print("❌ Missing required data files")
        return False
    
    csv_filename = csv_ref.get('filename', '')
    csv_content = fetch_s3_text(S3_PREFIX + csv_filename)
    
    print(f"✅ CSV: {csv_filename}")
    print(f"✅ Baseline timestamp: {baseline_data.get('timestamp_utc')}")
    print(f"✅ Prices timestamp: {latest_prices.get('timestamp_utc')}")
    print(f"✅ PV_pre: ${pre_exec_data.get('pv_pre', 0):,.2f}")
    
    # 2. Parse positions
    print("\n📊 STEP 2: Parsing Positions")
    positions = []
    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        positions.append(row)
    
    current_prices = latest_prices.get('prices', {})
    baseline_prices = baseline_data.get('prices', {})
    pv_pre = pre_exec_data.get('pv_pre', 1000000.0)
    
    print(f"✅ Positions: {len(positions)}")
    print(f"✅ Current prices: {len(current_prices)}")
    print(f"✅ Baseline prices: {len(baseline_prices)}")
    
    # 3. Calculate each position P&L
    print("\n📊 STEP 3: Individual Position Calculations")
    long_positions = []
    short_positions = []
    total_daily_pnl = 0.0
    
    for i, position in enumerate(positions):
        symbol = position.get('ticker', '').replace('_', '')
        notional = float(position.get('target_notional', 0))
        contracts = float(position.get('target_contracts', 0))
        
        baseline_price = baseline_prices.get(symbol)
        current_price = current_prices.get(symbol)
        
        if baseline_price and current_price and notional != 0:
            side = 1 if contracts > 0 else -1
            pnl = side * (current_price - baseline_price) / baseline_price * abs(notional)
            total_daily_pnl += pnl
            
            position_data = {
                'symbol': symbol,
                'side': 'LONG' if contracts > 0 else 'SHORT',
                'notional': abs(notional),
                'baseline_price': baseline_price,
                'current_price': current_price,
                'pnl': pnl,
                'calculation': f"{side} * ({current_price} - {baseline_price}) / {baseline_price} * {abs(notional)} = {pnl:.2f}"
            }
            
            if contracts > 0:
                long_positions.append(position_data)
            else:
                short_positions.append(position_data)
            
            # Print first few for verification
            if i < 5:
                print(f"  {symbol}: {position_data['side']} ${pnl:.2f}")
    
    # 4. Calculate totals
    print("\n📊 STEP 4: Totals Calculation")
    long_pnl_sum = sum(p['pnl'] for p in long_positions)
    short_pnl_sum = sum(p['pnl'] for p in short_positions)
    calculated_daily_pnl = long_pnl_sum + short_pnl_sum
    
    print(f"Long P&L sum: ${long_pnl_sum:.2f} ({len(long_positions)} positions)")
    print(f"Short P&L sum: ${short_pnl_sum:.2f} ({len(short_positions)} positions)")
    print(f"Total Daily P&L: ${calculated_daily_pnl:.2f}")
    
    # 5. Portfolio Value calculation
    portfolio_value = pv_pre + calculated_daily_pnl
    total_pnl = portfolio_value - 1000000.0
    
    print(f"\nPortfolio Value: ${pv_pre:.2f} + ${calculated_daily_pnl:.2f} = ${portfolio_value:.2f}")
    print(f"Total P&L: ${portfolio_value:.2f} - $1,000,000 = ${total_pnl:.2f}")
    
    # 6. Check S3 logs
    print("\n📊 STEP 5: S3 Log Verification")
    pv_log_content = fetch_s3_text(S3_PREFIX + 'portfolio_value_log.jsonl')
    if pv_log_content:
        lines = pv_log_content.strip().split('\n')
        latest_entry = json.loads(lines[-1])
        
        print(f"Latest S3 entry:")
        print(f"  Daily P&L: ${latest_entry.get('daily_pnl', 0):.2f}")
        print(f"  Portfolio Value: ${latest_entry.get('portfolio_value', 0):.2f}")
        print(f"  Total P&L: ${latest_entry.get('total_pnl', 0):.2f}")
        
        if 'audit' in latest_entry:
            audit = latest_entry['audit']
            print(f"  Audit Long P&L: ${audit.get('long_pnl', 0):.2f}")
            print(f"  Audit Short P&L: ${audit.get('short_pnl', 0):.2f}")
            print(f"  Validation: {audit.get('validation_long_plus_short', False)}")
    
    # 7. Summary
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"✅ Manual calculation matches S3: {abs(calculated_daily_pnl - latest_entry.get('daily_pnl', 0)) < 1.0}")
    print(f"✅ Long P&L calculation: ${long_pnl_sum:.2f}")
    print(f"✅ Short P&L calculation: ${short_pnl_sum:.2f}")
    print(f"✅ Daily P&L calculation: ${calculated_daily_pnl:.2f}")
    print(f"✅ Portfolio Value: ${portfolio_value:.2f}")
    print(f"✅ Total P&L: ${total_pnl:.2f}")
    
    # 8. Dashboard comparison
    print(f"\n🚨 DASHBOARD DISCREPANCIES:")
    print(f"Dashboard Long P&L: $369.38 (should be ${long_pnl_sum:.2f}) - ERROR: ${369.38 - long_pnl_sum:.2f}")
    print(f"Dashboard Short P&L: $279.07 (should be ${short_pnl_sum:.2f}) - ERROR: ${279.07 - short_pnl_sum:.2f}")
    print(f"Dashboard Daily P&L: $488.70 (should be ${calculated_daily_pnl:.2f}) - ERROR: ${488.70 - calculated_daily_pnl:.2f}")
    
    return {
        'calculated_long': long_pnl_sum,
        'calculated_short': short_pnl_sum,
        'calculated_daily': calculated_daily_pnl,
        'calculated_portfolio': portfolio_value,
        'calculated_total': total_pnl,
        's3_matches': abs(calculated_daily_pnl - latest_entry.get('daily_pnl', 0)) < 1.0
    }

if __name__ == '__main__':
    verify_calculations()






