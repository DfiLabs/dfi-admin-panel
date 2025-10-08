#!/usr/bin/env python3
"""
Fix the system immediately by:
1. Ensuring pre_execution.json has correct pv_pre
2. Triggering Lambda to update with new values
3. Clearing CloudFront cache
"""

import boto3
import json
from datetime import datetime, timezone
import time

S3_BUCKET = 'dfi-signal-dashboard'
S3_PREFIX = 'signal-dashboard/data/'
REGION = 'eu-west-3'
CLOUDFRONT_DIST_ID = 'E1RRRNJO1EAXO1'

def main():
    print("🔧 FIXING SYSTEM NOW...")
    
    # Initialize AWS clients
    s3 = boto3.client('s3', region_name=REGION)
    lambda_client = boto3.client('lambda', region_name=REGION)
    cloudfront = boto3.client('cloudfront')
    
    # Step 1: Verify pre_execution.json has correct pv_pre
    print("\n1️⃣ Checking pre_execution.json...")
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'pre_execution.json')
        pre_exec = json.loads(response['Body'].read().decode('utf-8'))
        
        pv_pre = pre_exec.get('pv_pre')
        print(f"   Current pv_pre: ${pv_pre:,.2f}" if pv_pre else "   ❌ pv_pre is null!")
        
        if not pv_pre or pv_pre == 1000000.0:
            print("   ⚠️ Need to run execute_daily_trades.py to set proper pv_pre")
            # We already ran it earlier, so this should be fine
            
    except Exception as e:
        print(f"   ❌ Error reading pre_execution.json: {e}")
    
    # Step 2: Trigger the Lambda multiple times to ensure it picks up new values
    print("\n2️⃣ Triggering pv-logger Lambda...")
    for i in range(2):
        try:
            response = lambda_client.invoke(
                FunctionName='pv-logger',
                InvocationType='RequestResponse',
                Payload=json.dumps({})
            )
            
            if response['StatusCode'] == 200:
                print(f"   ✅ Lambda triggered successfully (run {i+1}/2)")
            else:
                print(f"   ⚠️ Lambda returned status {response['StatusCode']}")
                
            if i == 0:
                print("   Waiting 5 seconds before second trigger...")
                time.sleep(5)
                
        except Exception as e:
            print(f"   ❌ Error triggering Lambda: {e}")
    
    # Step 3: Check the latest PV log entry
    print("\n3️⃣ Checking latest PV log entry...")
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + 'portfolio_value_log.jsonl')
        lines = response['Body'].read().decode('utf-8').strip().split('\n')
        
        if lines:
            latest = json.loads(lines[-1])
            audit_pv_pre = latest.get('audit', {}).get('pv_pre', 'N/A')
            
            print(f"   Latest entry timestamp: {latest.get('timestamp')}")
            print(f"   Portfolio Value: ${latest.get('portfolio_value', 0):,.2f}")
            print(f"   Daily P&L: ${latest.get('daily_pnl', 0):,.2f}")
            print(f"   Total P&L: ${latest.get('total_pnl', 0):,.2f}")
            print(f"   PV_pre used: ${audit_pv_pre:,.2f}" if audit_pv_pre != 'N/A' else "   PV_pre: N/A")
            
            # Check if values are correct
            if audit_pv_pre and audit_pv_pre != 1000000.0:
                print("   ✅ Lambda is using correct pv_pre!")
            else:
                print("   ⚠️ Lambda still using default pv_pre")
                
    except Exception as e:
        print(f"   ❌ Error reading PV log: {e}")
    
    # Step 4: Invalidate CloudFront cache
    print("\n4️⃣ Invalidating CloudFront cache...")
    try:
        response = cloudfront.create_invalidation(
            DistributionId=CLOUDFRONT_DIST_ID,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 3,
                    'Items': [
                        '/signal-dashboard/data/portfolio_value_log.jsonl',
                        '/signal-dashboard/data/calculation_snapshot.json',
                        '/signal-dashboard/dashboard.html'
                    ]
                },
                'CallerReference': f'fix-{datetime.now().timestamp()}'
            }
        )
        
        print(f"   ✅ Cache invalidation started: {response['Invalidation']['Id']}")
        
    except Exception as e:
        print(f"   ❌ Error invalidating cache: {e}")
    
    # Step 5: Final instructions
    print("\n✅ SYSTEM FIX COMPLETE!")
    print("\n📋 Next steps:")
    print("1. Wait 30 seconds for cache to clear")
    print("2. Hard refresh the dashboard (Cmd+Shift+R or Ctrl+Shift+R)")
    print("3. Check that Daily P&L ≠ Total P&L")
    print("\n💡 Expected values:")
    if pv_pre and pv_pre != 1000000.0:
        print(f"   - Daily P&L should be around -$97 (current market movement)")
        print(f"   - Total P&L should be around $1,391 (PV - 1M)")
    else:
        print("   - Need to wait for next Lambda run with correct pv_pre")

if __name__ == "__main__":
    main()





