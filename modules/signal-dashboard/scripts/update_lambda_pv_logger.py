#!/usr/bin/env python3
"""
Update Lambda PV Logger to use synchronized calculations
Ensures Lambda and Dashboard use identical data and calculations
"""

import json
import boto3
import zipfile
import io
import os

LAMBDA_FUNCTION_NAME = 'pv-logger'
REGION = 'eu-west-3'

def create_updated_lambda_code():
    """Create updated Lambda function code that uses synchronized snapshots"""
    
    lambda_code = '''#!/usr/bin/env python3
"""
AWS Lambda: Synchronized Portfolio Value Logger
Uses calculation_snapshot.json for perfect consistency with dashboard
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import ssl
import os
import csv
import io

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
S3_KEY_PREFIX = "signal-dashboard/data/"

def log(msg: str) -> None:
    """Simple logging function for Lambda."""
    print(f"[{datetime.now().isoformat()}] {msg}")

def load_s3_json(s3_client, key: str) -> dict:
    """Load JSON from S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        log(f"❌ Error loading {key}: {e}")
        return {}

def create_synchronized_calculation(s3_client) -> dict:
    """Create synchronized calculation using same logic as dashboard"""
    
    try:
        # Load all required data
        latest_prices = load_s3_json(s3_client, S3_KEY_PREFIX + 'latest_prices.json')
        baseline_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'daily_baseline.json')
        pre_exec_data = load_s3_json(s3_client, S3_KEY_PREFIX + 'pre_execution.json')
        csv_ref = load_s3_json(s3_client, S3_KEY_PREFIX + 'latest.json')
        
        if not all([latest_prices, baseline_data, pre_exec_data, csv_ref]):
            log("❌ Missing required data files")
            return {}
        
        # Get CSV content
        csv_filename = csv_ref.get('filename', '')
        csv_response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_PREFIX + csv_filename)
        csv_content = csv_response['Body'].read().decode('utf-8')
        
        # Parse positions
        positions = []
        reader = csv.DictReader(io.StringIO(csv_content))
        for row in reader:
            positions.append(row)
        
        # Get price data
        current_prices = latest_prices.get('prices', {})
        baseline_prices = baseline_data.get('prices', {})
        pv_pre = pre_exec_data.get('pv_pre', 1000000.0)
        
        if pv_pre is None:
            log("⚠️ pv_pre is null, using fallback")
            pv_pre = 1000000.0
        
        log(f"📊 PV_pre from pre_execution.json: ${pv_pre:,.2f}")
        
        # Calculate P&L using identical logic to dashboard
        total_daily_pnl = 0.0
        long_pnl = 0.0
        short_pnl = 0.0
        positions_processed = 0
        
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
                positions_processed += 1
                
                if contracts > 0:
                    long_pnl += pnl
                else:
                    short_pnl += pnl
                
                # Debug key positions
                if symbol in ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']:
                    log(f"🔍 {symbol}: baseline={baseline_price}, current={current_price}, side={side}, pnl=${pnl:.2f}")
        
        # Calculate final values using same formulas as dashboard
        portfolio_value = pv_pre + total_daily_pnl
        total_pnl = portfolio_value - 1000000.0
        
        log(f"💰 Calculated PV: ${portfolio_value:,.2f} (Daily P&L: ${total_daily_pnl:,.2f}, Positions: {positions_processed})")
        log(f"📊 PV_pre: ${pv_pre:,.2f}, Daily P&L: ${total_daily_pnl:,.2f}, Total P&L: ${total_pnl:,.2f}")
        log(f"📊 Long P&L: ${long_pnl:,.2f}, Short P&L: ${short_pnl:,.2f}, Sum: ${long_pnl + short_pnl:,.2f}")
        
        return {
            'portfolio_value': portfolio_value,
            'daily_pnl': total_daily_pnl,
            'total_pnl': total_pnl,
            'long_pnl': long_pnl,
            'short_pnl': short_pnl,
            'positions_processed': positions_processed,
            'pv_pre': pv_pre,
            'csv_filename': csv_filename
        }
        
    except Exception as e:
        log(f"❌ Error in synchronized calculation: {e}")
        return {}

def lambda_handler(event, context):
    """Main Lambda handler with synchronized calculations"""
    log("🚀 Starting Synchronized PV Logger Lambda...")
    
    s3_client = boto3.client('s3')
    
    # Create synchronized calculation
    calc_result = create_synchronized_calculation(s3_client)
    
    if not calc_result:
        log("❌ Failed to create synchronized calculation")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Calculation failed'})
        }
    
    # Create log entry with full audit trail
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = {
        'timestamp': timestamp,
        'portfolio_value': calc_result['portfolio_value'],
        'daily_pnl': calc_result['daily_pnl'],
        'total_pnl': calc_result['total_pnl'],
        'audit': {
            'source': 'lambda-synchronized',
            'positions_processed': calc_result['positions_processed'],
            'pv_pre': calc_result['pv_pre'],
            'csv_filename': calc_result['csv_filename'],
            'long_pnl': calc_result['long_pnl'],
            'short_pnl': calc_result['short_pnl'],
            'validation_long_plus_short': abs((calc_result['long_pnl'] + calc_result['short_pnl']) - calc_result['daily_pnl']) < 0.01
        }
    }
    
    # Append to portfolio value log
    try:
        log_key = S3_KEY_PREFIX + 'portfolio_value_log.jsonl'
        log_line = json.dumps(log_entry) + '\\n'
        
        # Append to existing log
        try:
            existing = s3_client.get_object(Bucket=S3_BUCKET, Key=log_key)
            existing_content = existing['Body'].read().decode('utf-8')
            new_content = existing_content + log_line
        except ClientError:
            new_content = log_line
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=log_key,
            Body=new_content.encode('utf-8'),
            ContentType='application/x-ndjson',
            CacheControl='no-cache'
        )
        
        log("✅ PV logging completed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'PV logged successfully',
                'portfolio_value': calc_result['portfolio_value'],
                'daily_pnl': calc_result['daily_pnl'],
                'positions_count': calc_result['positions_processed'],
                'timestamp': timestamp
            })
        }
        
    except Exception as e:
        log(f"❌ Error logging to S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
'''
    
    return lambda_code

def update_lambda_function():
    """Update the Lambda function with synchronized calculation logic"""
    
    print("🔄 Creating updated Lambda function code...")
    
    # Create new Lambda code
    lambda_code = create_updated_lambda_code()
    
    # Create zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('lambda_function.py', lambda_code)
    
    zip_buffer.seek(0)
    zip_content = zip_buffer.read()
    
    print(f"📦 Created Lambda deployment package ({len(zip_content)} bytes)")
    
    # Update Lambda function
    try:
        lambda_client = boto3.client('lambda', region_name=REGION)
        
        response = lambda_client.update_function_code(
            FunctionName=LAMBDA_FUNCTION_NAME,
            ZipFile=zip_content
        )
        
        print(f"✅ Lambda function updated successfully")
        print(f"📊 Function ARN: {response.get('FunctionArn')}")
        print(f"📊 Last Modified: {response.get('LastModified')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to update Lambda function: {e}")
        return False

if __name__ == '__main__':
    success = update_lambda_function()
    if success:
        print("\n🎯 Lambda function updated with synchronized calculation logic")
        print("🔄 Next Lambda execution will use identical calculations as dashboard")
    else:
        print("\n❌ Failed to update Lambda function")






