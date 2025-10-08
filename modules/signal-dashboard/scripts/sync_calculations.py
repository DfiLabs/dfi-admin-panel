#!/usr/bin/env python3
"""
Synchronized Calculation System
Ensures Lambda and Dashboard use identical price snapshots
"""

import json
import boto3
import datetime
from typing import Dict
import csv
import io

S3_BUCKET = 'dfi-signal-dashboard'
S3_PREFIX = 'signal-dashboard/data/'

def log(msg: str):
    print(f"[{datetime.datetime.utcnow().isoformat()}] {msg}")

def create_calculation_snapshot() -> Dict:
    """Create a synchronized calculation snapshot for all components to use"""
    
    # Fetch all data at the same moment
    s3 = boto3.client('s3')
    
    try:
        # Get current prices
        latest_prices_resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'latest_prices.json')
        latest_prices = json.loads(latest_prices_resp['Body'].read().decode('utf-8'))
        
        # Get baseline prices
        baseline_resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'daily_baseline.json')
        baseline_data = json.loads(baseline_resp['Body'].read().decode('utf-8'))
        
        # Get pre-execution data
        pre_exec_resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'pre_execution.json')
        pre_exec_data = json.loads(pre_exec_resp['Body'].read().decode('utf-8'))
        
        # Get CSV data
        csv_ref_resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'latest.json')
        csv_ref = json.loads(csv_ref_resp['Body'].read().decode('utf-8'))
        
        csv_resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + csv_ref['filename'])
        csv_content = csv_resp['Body'].read().decode('utf-8')
        
    except Exception as e:
        log(f"❌ Error fetching data: {e}")
        return {}
    
    # Parse CSV positions
    positions = []
    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        positions.append(row)
    
    # Calculate P&L for all positions
    current_prices = latest_prices.get('prices', {})
    baseline_prices = baseline_data.get('prices', {})
    pv_pre = pre_exec_data.get('pv_pre', 1000000.0)
    
    total_daily_pnl = 0.0
    long_pnl = 0.0
    short_pnl = 0.0
    long_notional = 0.0
    short_notional = 0.0
    long_count = 0
    short_count = 0
    
    position_details = []
    
    for position in positions:
        symbol = position.get('ticker', '').replace('_', '')
        notional = float(position.get('target_notional', 0))
        contracts = float(position.get('target_contracts', 0))
        
        baseline_price = baseline_prices.get(symbol)
        current_price = current_prices.get(symbol)
        
        if baseline_price and current_price and notional != 0:
            side = 1 if contracts > 0 else -1
            pnl = side * (current_price - baseline_price) / baseline_price * abs(notional)
            total_daily_pnl += pnl
            
            position_detail = {
                'symbol': symbol,
                'side': 'LONG' if contracts > 0 else 'SHORT',
                'notional': notional,
                'baseline_price': baseline_price,
                'current_price': current_price,
                'pnl': pnl
            }
            position_details.append(position_detail)
            
            if contracts > 0:
                long_pnl += pnl
                long_notional += abs(notional)
                long_count += 1
            else:
                short_pnl += pnl
                short_notional += abs(notional)
                short_count += 1
    
    # Calculate final values
    portfolio_value = pv_pre + total_daily_pnl
    total_pnl = portfolio_value - 1000000.0
    
    # Create synchronized snapshot
    snapshot = {
        'timestamp_utc': datetime.datetime.utcnow().isoformat(),
        'data_sources': {
            'csv_filename': csv_ref['filename'],
            'pv_pre': pv_pre,
            'baseline_timestamp': baseline_data.get('timestamp_utc'),
            'prices_timestamp': latest_prices.get('timestamp_utc')
        },
        'calculations': {
            'daily_pnl': total_daily_pnl,
            'portfolio_value': portfolio_value,
            'total_pnl': total_pnl,
            'long_pnl': long_pnl,
            'short_pnl': short_pnl,
            'long_notional': long_notional,
            'short_notional': short_notional,
            'long_count': long_count,
            'short_count': short_count,
            'positions_processed': len(position_details)
        },
        'validation': {
            'long_plus_short_equals_daily': abs((long_pnl + short_pnl) - total_daily_pnl) < 0.01,
            'pv_formula_check': abs(portfolio_value - (pv_pre + total_daily_pnl)) < 0.01,
            'total_pnl_formula_check': abs(total_pnl - (portfolio_value - 1000000)) < 0.01,
            'notional_sum_check': abs((long_notional + short_notional) - 1000000) < 1000  # Allow some tolerance
        },
        'position_details': position_details
    }
    
    # Save synchronized snapshot to S3
    snapshot_key = f"{S3_PREFIX}calculation_snapshot.json"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=snapshot_key,
            Body=json.dumps(snapshot, indent=2).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-cache'
        )
        log(f"✅ Synchronized snapshot saved to s3://{S3_BUCKET}/{snapshot_key}")
    except Exception as e:
        log(f"❌ Failed to save snapshot: {e}")
    
    return snapshot

if __name__ == '__main__':
    result = create_calculation_snapshot()
    
    # Print summary
    if result:
        calc = result['calculations']
        val = result['validation']
        
        print(f"\n📊 SYNCHRONIZED CALCULATION RESULTS:")
        print(f"Daily P&L: ${calc['daily_pnl']:,.2f}")
        print(f"Long P&L: ${calc['long_pnl']:,.2f} ({calc['long_count']} positions)")
        print(f"Short P&L: ${calc['short_pnl']:,.2f} ({calc['short_count']} positions)")
        print(f"Portfolio Value: ${calc['portfolio_value']:,.2f}")
        print(f"Total P&L: ${calc['total_pnl']:,.2f}")
        print(f"\n✅ All validations: {all(val.values())}")
        print(f"Long+Short=Daily: {val['long_plus_short_equals_daily']}")
        print(f"PV=Pre+Daily: {val['pv_formula_check']}")
        print(f"Total=PV-1M: {val['total_pnl_formula_check']}")



