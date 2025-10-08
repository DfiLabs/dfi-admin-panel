#!/usr/bin/env python3
"""
Verify the system is working correctly right now
"""

import boto3
import json
from datetime import datetime

def main():
    s3 = boto3.client('s3', region_name='eu-west-3')
    bucket = 'dfi-signal-dashboard'
    prefix = 'signal-dashboard/data/'
    
    print('🔍 VERIFYING CURRENT SYSTEM STATE...')
    print('=' * 60)
    
    try:
        # 1. Check latest PV log entry
        response = s3.get_object(Bucket=bucket, Key=prefix + 'portfolio_value_log.jsonl')
        lines = response['Body'].read().decode('utf-8').strip().split('\n')
        latest = json.loads(lines[-1])
        
        print('\n1️⃣ Latest PV Log Entry:')
        print(f'   Timestamp: {latest["timestamp"]}')
        print(f'   Portfolio Value: ${latest["portfolio_value"]:,.2f}')
        print(f'   Daily P&L: ${latest["daily_pnl"]:,.2f}')
        print(f'   Total P&L: ${latest["total_pnl"]:,.2f}')
        
        audit_pv_pre = latest.get("audit", {}).get("pv_pre", "N/A")
        if audit_pv_pre != "N/A":
            print(f'   PV_pre used: ${audit_pv_pre:,.2f}')
        else:
            print(f'   PV_pre used: N/A')
        
        # 2. Check pre_execution.json
        response = s3.get_object(Bucket=bucket, Key=prefix + 'pre_execution.json')
        pre_exec = json.loads(response['Body'].read().decode('utf-8'))
        
        print('\n2️⃣ Current pre_execution.json:')
        pv_pre_value = pre_exec.get("pv_pre")
        if pv_pre_value:
            print(f'   pv_pre: ${pv_pre_value:,.2f}')
        else:
            print(f'   pv_pre: null')
        print(f'   pv_pre_time: {pre_exec.get("pv_pre_time")}')
        print(f'   CSV: {pre_exec.get("csv_filename")}')
        print(f'   Finalized: {pre_exec.get("finalized", False)}')
        
        # 3. Verify calculations
        pv = latest['portfolio_value']
        pv_pre = audit_pv_pre if audit_pv_pre != "N/A" else 1000000
        daily_pnl = latest['daily_pnl']
        total_pnl = latest['total_pnl']
        
        print('\n3️⃣ Calculation Verification:')
        calculated_pv = pv_pre + daily_pnl
        print(f'   PV_pre + Daily P&L = ${pv_pre:,.2f} + ${daily_pnl:,.2f} = ${calculated_pv:,.2f}')
        print(f'   Actual PV: ${pv:,.2f}')
        pv_match = abs(calculated_pv - pv) < 0.01
        print(f'   ✅ Match: {pv_match}' if pv_match else f'   ❌ Mismatch by ${abs(calculated_pv - pv):,.2f}')
        
        calculated_total = pv - 1000000
        print(f'\n   PV - 1M = ${pv:,.2f} - $1,000,000 = ${calculated_total:,.2f}')
        print(f'   Actual Total P&L: ${total_pnl:,.2f}')
        total_match = abs(calculated_total - total_pnl) < 0.01
        print(f'   ✅ Match: {total_match}' if total_match else f'   ❌ Mismatch by ${abs(calculated_total - total_pnl):,.2f}')
        
        # 4. Check if values are different
        values_different = abs(daily_pnl - total_pnl) > 0.01
        print(f'\n   Daily P&L != Total P&L? {values_different} (Should be True)')
        if values_different:
            print(f'   ✅ Daily (${daily_pnl:,.2f}) != Total (${total_pnl:,.2f})')
        else:
            print(f'   ❌ Daily and Total are the same: ${daily_pnl:,.2f}')
        
        # 5. Final status
        print('\n' + '=' * 60)
        if pv_pre == 1000000:
            print('⚠️  WARNING: Still using default pv_pre = 1,000,000')
            print('   This suggests execution has not run yet today')
        else:
            print(f'✅ Using correct pv_pre = ${pv_pre:,.2f}')
        
        if values_different and pv_match and total_match:
            print('✅ SYSTEM IS WORKING CORRECTLY!')
        else:
            print('⚠️  Some calculations need attention')
            
        # 6. Next execution info
        print(f'\n📅 Next execution: 00:30 UTC')
        hours_until = (24 - datetime.utcnow().hour + 0.5) % 24
        print(f'   Time until execution: ~{hours_until:.1f} hours')
        
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()





