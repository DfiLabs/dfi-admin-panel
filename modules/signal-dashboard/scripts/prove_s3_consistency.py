#!/usr/bin/env python3
"""
Prove S3 Consistency - Verify dashboard uses S3 data correctly
"""

import json
import boto3
import requests
from datetime import datetime

S3_BUCKET = 'dfi-signal-dashboard'
S3_PREFIX = 'signal-dashboard/data/'

def get_s3_latest_entry():
    """Get the latest S3 PV log entry"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'portfolio_value_log.jsonl')
        content = response['Body'].read().decode('utf-8')
        lines = content.strip().split('\n')
        latest = json.loads(lines[-1])
        return latest
    except Exception as e:
        print(f"❌ Error fetching S3 data: {e}")
        return None

def get_dashboard_data():
    """Get what the dashboard is serving"""
    try:
        response = requests.get('https://admin.dfi-labs.com/signal-dashboard/data/portfolio_value_log.jsonl')
        if response.ok:
            lines = response.text.strip().split('\n')
            latest = json.loads(lines[-1])
            return latest
    except Exception as e:
        print(f"❌ Error fetching dashboard data: {e}")
        return None

def prove_consistency():
    """Prove the system is consistent"""
    
    print("🔍 S3 CONSISTENCY VERIFICATION")
    print("=" * 50)
    
    # Get S3 data directly
    s3_data = get_s3_latest_entry()
    if not s3_data:
        print("❌ Could not fetch S3 data")
        return
    
    # Get dashboard data
    dashboard_data = get_dashboard_data()
    if not dashboard_data:
        print("❌ Could not fetch dashboard data")
        return
    
    print(f"📊 S3 Direct Access:")
    print(f"   Timestamp: {s3_data.get('timestamp')}")
    print(f"   Daily P&L: ${s3_data.get('daily_pnl', 0):.2f}")
    print(f"   Portfolio Value: ${s3_data.get('portfolio_value', 0):.2f}")
    print(f"   Total P&L: ${s3_data.get('total_pnl', 0):.2f}")
    
    if 'audit' in s3_data:
        audit = s3_data['audit']
        print(f"   Long P&L: ${audit.get('long_pnl', 0):.2f}")
        print(f"   Short P&L: ${audit.get('short_pnl', 0):.2f}")
        print(f"   Validation: {audit.get('validation_long_plus_short', False)}")
    
    print(f"\n📊 Dashboard Access:")
    print(f"   Timestamp: {dashboard_data.get('timestamp')}")
    print(f"   Daily P&L: ${dashboard_data.get('daily_pnl', 0):.2f}")
    print(f"   Portfolio Value: ${dashboard_data.get('portfolio_value', 0):.2f}")
    print(f"   Total P&L: ${dashboard_data.get('total_pnl', 0):.2f}")
    
    if 'audit' in dashboard_data:
        audit = dashboard_data['audit']
        print(f"   Long P&L: ${audit.get('long_pnl', 0):.2f}")
        print(f"   Short P&L: ${audit.get('short_pnl', 0):.2f}")
        print(f"   Validation: {audit.get('validation_long_plus_short', False)}")
    
    # Verify consistency
    print(f"\n✅ CONSISTENCY CHECK:")
    same_timestamp = s3_data.get('timestamp') == dashboard_data.get('timestamp')
    same_daily_pnl = abs(s3_data.get('daily_pnl', 0) - dashboard_data.get('daily_pnl', 0)) < 0.01
    
    print(f"Same timestamp: {same_timestamp}")
    print(f"Same Daily P&L: {same_daily_pnl}")
    print(f"Data is consistent: {same_timestamp and same_daily_pnl}")
    
    # Mathematical verification
    if 'audit' in s3_data:
        audit = s3_data['audit']
        long_pnl = audit.get('long_pnl', 0)
        short_pnl = audit.get('short_pnl', 0)
        daily_pnl = s3_data.get('daily_pnl', 0)
        
        print(f"\n🔢 MATHEMATICAL VERIFICATION:")
        print(f"Long + Short = ${long_pnl:.2f} + ${short_pnl:.2f} = ${long_pnl + short_pnl:.2f}")
        print(f"Daily P&L = ${daily_pnl:.2f}")
        print(f"Math correct: {abs((long_pnl + short_pnl) - daily_pnl) < 0.01}")
        
        # Portfolio Value check
        pv_pre = audit.get('pv_pre', 998344.77)
        portfolio_value = s3_data.get('portfolio_value', 0)
        expected_pv = pv_pre + daily_pnl
        
        print(f"\nPortfolio Value check:")
        print(f"PV_pre + Daily = ${pv_pre:.2f} + ${daily_pnl:.2f} = ${expected_pv:.2f}")
        print(f"Actual PV = ${portfolio_value:.2f}")
        print(f"PV formula correct: {abs(expected_pv - portfolio_value) < 0.01}")

if __name__ == '__main__':
    prove_consistency()






