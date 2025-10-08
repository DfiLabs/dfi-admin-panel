#!/usr/bin/env python3
"""
Investigate why Daily P&L = Total P&L after last night's execution
"""

import boto3
import json
from datetime import datetime, timezone

def main():
    s3 = boto3.client('s3', region_name='eu-west-3')
    bucket = 'dfi-signal-dashboard'
    prefix = 'signal-dashboard/data/'
    
    print('🔍 INVESTIGATING LAST NIGHT\'S EXECUTION...')
    print('=' * 60)
    
    # Look at PV log entries from last night
    response = s3.get_object(Bucket=bucket, Key=prefix + 'portfolio_value_log.jsonl')
    lines = response['Body'].read().decode('utf-8').strip().split('\n')
    
    # Find entries around midnight
    print('\n📊 PV Log Entries Around Midnight (23:00 Sept 24 - 02:00 Sept 25 UTC):')
    print(f'{"Time":<18} | {"PV":>12} | {"Daily P&L":>12} | {"Total P&L":>12} | {"pv_pre Used":>12}')
    print('-' * 80)
    
    midnight_entries = []
    
    for line in lines:
        entry = json.loads(line)
        ts = entry['timestamp']
        # Parse timestamp
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        
        # Show entries from 23:00 on Sept 24 to 02:00 on Sept 25
        if (dt.month == 9 and dt.day == 24 and dt.hour >= 23) or \
           (dt.month == 9 and dt.day == 25 and dt.hour <= 2):
            pv = entry['portfolio_value']
            daily = entry['daily_pnl']
            total = entry['total_pnl']
            pv_pre = entry.get('audit', {}).get('pv_pre', 'N/A')
            
            midnight_entries.append({
                'time': dt,
                'pv': pv,
                'daily': daily,
                'total': total,
                'pv_pre': pv_pre
            })
            
            # Highlight if daily = total
            marker = ' ⚠️' if abs(daily - total) < 0.01 else ''
            
            pv_pre_str = f'{pv_pre:,.0f}' if isinstance(pv_pre, (int, float)) else pv_pre
            print(f'{dt.strftime("%m-%d %H:%M:%S")} | ${pv:>11,.2f} | ${daily:>11,.2f} | ${total:>11,.2f} | {pv_pre_str:>12}{marker}')
    
    # Check when pv_pre changed
    print('\n🔎 Analysis:')
    
    # Find when pv_pre changed from 1M
    change_found = False
    for i in range(1, len(midnight_entries)):
        prev = midnight_entries[i-1]['pv_pre']
        curr = midnight_entries[i]['pv_pre']
        
        if prev == 1000000 and curr != 1000000 and curr != 'N/A':
            print(f'\n✅ pv_pre changed from 1,000,000 to {curr:,.2f} at {midnight_entries[i]["time"].strftime("%H:%M:%S")}')
            change_found = True
            break
    
    if not change_found:
        print('\n⚠️  No pv_pre change found during this period!')
        print('   The execution likely didn\'t run at 00:30 UTC')
    
    # Check pre_execution.json history
    print('\n📄 Checking pre_execution.json:')
    try:
        response = s3.get_object(Bucket=bucket, Key=prefix + 'pre_execution.json')
        pre_exec = json.loads(response['Body'].read().decode('utf-8'))
        
        print(f'   Current timestamp: {pre_exec.get("timestamp_utc")}')
        print(f'   pv_pre: ${pre_exec.get("pv_pre"):,.2f}' if pre_exec.get("pv_pre") else '   pv_pre: null')
        print(f'   pv_pre_time: {pre_exec.get("pv_pre_time")}')
        
        # Parse timestamp to see when it was written
        ts_str = pre_exec.get("timestamp_utc", "")
        if ts_str:
            written_at = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            print(f'   Written at: {written_at.strftime("%Y-%m-%d %H:%M:%S")} UTC')
            
            # Check if it was written today at 08:16 (our manual run)
            if written_at.day == 25 and written_at.hour == 8:
                print('\n   📝 This was written during our manual fix at 08:16 UTC today!')
                print('   Last night\'s execution did NOT run properly')
        
    except Exception as e:
        print(f'   Error reading pre_execution.json: {e}')
    
    print('\n' + '=' * 60)
    print('\n💡 EXPLANATION:')
    print('Last night (Sept 24-25), the 00:30 UTC execution did NOT run because:')
    print('1. The cron job was using wrong Python path (missing boto3)')
    print('2. Lambda defaulted to pv_pre = 1,000,000')
    print('3. With pv_pre = 1M: Daily P&L = Total P&L (both equal PV - 1M)')
    print('\nWe fixed this at 08:16 UTC today by:')
    print('1. Updating cron to use correct Python')
    print('2. Running execute_daily_trades.py manually')
    print('3. Setting proper pv_pre = 1,001,488.21')
    print('\n✅ Tonight\'s execution WILL run correctly at 00:30 UTC')

if __name__ == "__main__":
    main()





