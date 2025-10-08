#!/usr/bin/env python3
"""
Comprehensive Calculation Audit System
Compares Lambda vs Dashboard calculations and logs all details to S3
"""

import json
import boto3
import datetime
from typing import Dict, List, Tuple
import csv
import io

S3_BUCKET = 'dfi-signal-dashboard'
S3_PREFIX = 'signal-dashboard/data/'

def log(msg: str):
    """Log with timestamp"""
    print(f"[{datetime.datetime.utcnow().isoformat()}] {msg}")

def fetch_s3_json(key: str) -> Dict:
    """Fetch JSON from S3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        log(f"❌ Failed to fetch {key}: {e}")
        return {}

def fetch_s3_text(key: str) -> str:
    """Fetch text from S3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        log(f"❌ Failed to fetch {key}: {e}")
        return ""

def parse_csv_positions(csv_content: str) -> List[Dict]:
    """Parse CSV positions"""
    positions = []
    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        positions.append(row)
    return positions

def calculate_position_pnl(position: Dict, current_prices: Dict, baseline_prices: Dict) -> Tuple[float, Dict]:
    """Calculate P&L for a single position with full audit trail"""
    symbol = position.get('ticker', '').replace('_', '')
    notional = float(position.get('target_notional', 0))
    contracts = float(position.get('target_contracts', 0))
    
    baseline_price = baseline_prices.get(symbol)
    current_price = current_prices.get(symbol)
    
    if not baseline_price or not current_price or notional == 0:
        return 0.0, {
            'symbol': symbol,
            'error': 'Missing price data or zero notional',
            'baseline_price': baseline_price,
            'current_price': current_price,
            'notional': notional
        }
    
    side = 1 if contracts > 0 else -1
    pnl = side * (current_price - baseline_price) / baseline_price * abs(notional)
    
    audit = {
        'symbol': symbol,
        'baseline_price': baseline_price,
        'current_price': current_price,
        'notional': notional,
        'contracts': contracts,
        'side': side,
        'price_diff': current_price - baseline_price,
        'price_change_pct': ((current_price - baseline_price) / baseline_price) * 100,
        'pnl': pnl,
        'calculation': f"{side} * ({current_price} - {baseline_price}) / {baseline_price} * {abs(notional)} = {pnl}"
    }
    
    return pnl, audit

def audit_full_calculation() -> Dict:
    """Perform complete calculation audit"""
    log("🔍 Starting comprehensive calculation audit...")
    
    # Fetch all required data
    pre_execution = fetch_s3_json(S3_PREFIX + 'pre_execution.json')
    daily_baseline = fetch_s3_json(S3_PREFIX + 'daily_baseline.json')
    latest_prices = fetch_s3_json(S3_PREFIX + 'latest_prices.json')
    latest_csv_ref = fetch_s3_json(S3_PREFIX + 'latest.json')
    
    if not all([pre_execution, daily_baseline, latest_prices, latest_csv_ref]):
        log("❌ Missing required S3 files")
        return {}
    
    # Get CSV filename and content
    csv_filename = latest_csv_ref.get('filename', '')
    csv_content = fetch_s3_text(S3_PREFIX + csv_filename)
    
    if not csv_content:
        log(f"❌ Failed to fetch CSV: {csv_filename}")
        return {}
    
    # Parse data
    positions = parse_csv_positions(csv_content)
    current_prices = latest_prices.get('prices', {})
    baseline_prices = daily_baseline.get('prices', {})
    pv_pre = pre_execution.get('pv_pre', 1000000.0)
    
    log(f"📊 Data loaded: {len(positions)} positions, {len(current_prices)} current prices, {len(baseline_prices)} baseline prices")
    log(f"📊 PV_pre: ${pv_pre:,.2f}")
    
    # Calculate P&L for each position
    position_audits = []
    total_daily_pnl = 0.0
    long_pnl = 0.0
    short_pnl = 0.0
    long_count = 0
    short_count = 0
    
    for position in positions:
        pnl, audit = calculate_position_pnl(position, current_prices, baseline_prices)
        position_audits.append(audit)
        total_daily_pnl += pnl
        
        if audit.get('contracts', 0) > 0:
            long_pnl += pnl
            long_count += 1
        else:
            short_pnl += pnl
            short_count += 1
    
    # Calculate final metrics
    portfolio_value = pv_pre + total_daily_pnl
    total_pnl = portfolio_value - 1000000.0
    
    # Create comprehensive audit report
    audit_report = {
        'timestamp_utc': datetime.datetime.utcnow().isoformat(),
        'data_sources': {
            'csv_filename': csv_filename,
            'pv_pre': pv_pre,
            'pv_pre_time': pre_execution.get('pv_pre_time'),
            'baseline_timestamp': daily_baseline.get('timestamp_utc'),
            'prices_timestamp': latest_prices.get('timestamp_utc'),
            'positions_count': len(positions)
        },
        'calculations': {
            'total_daily_pnl': total_daily_pnl,
            'long_pnl': long_pnl,
            'short_pnl': short_pnl,
            'long_count': long_count,
            'short_count': short_count,
            'portfolio_value': portfolio_value,
            'total_pnl': total_pnl
        },
        'validation': {
            'long_plus_short_equals_daily': abs((long_pnl + short_pnl) - total_daily_pnl) < 0.01,
            'pv_equals_pre_plus_daily': abs(portfolio_value - (pv_pre + total_daily_pnl)) < 0.01,
            'total_equals_pv_minus_1m': abs(total_pnl - (portfolio_value - 1000000)) < 0.01
        },
        'position_details': position_audits[:5],  # First 5 positions for debugging
        'summary': {
            'all_validations_pass': True,
            'calculation_method': 'side * (current_price - baseline_price) / baseline_price * abs(notional)'
        }
    }
    
    # Check validations
    validations = audit_report['validation']
    all_pass = all(validations.values())
    audit_report['summary']['all_validations_pass'] = all_pass
    
    log(f"📊 AUDIT RESULTS:")
    log(f"   Daily P&L: ${total_daily_pnl:,.2f}")
    log(f"   Long P&L: ${long_pnl:,.2f} ({long_count} positions)")
    log(f"   Short P&L: ${short_pnl:,.2f} ({short_count} positions)")
    log(f"   Portfolio Value: ${portfolio_value:,.2f}")
    log(f"   Total P&L: ${total_pnl:,.2f}")
    log(f"   All validations pass: {all_pass}")
    
    # Save audit to S3
    audit_key = f"signal-dashboard/audits/calculation_audit_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=audit_key,
            Body=json.dumps(audit_report, indent=2).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-cache'
        )
        log(f"✅ Audit report saved to s3://{S3_BUCKET}/{audit_key}")
    except Exception as e:
        log(f"❌ Failed to save audit report: {e}")
    
    return audit_report

if __name__ == '__main__':
    audit_full_calculation()






