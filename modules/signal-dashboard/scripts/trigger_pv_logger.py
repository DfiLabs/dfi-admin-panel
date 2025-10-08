#!/usr/bin/env python3
"""
Manually trigger the pv-logger Lambda to force an update with the new pv_pre value
"""

import boto3
import json
from datetime import datetime

def trigger_lambda():
    """Invoke the pv-logger Lambda function manually"""
    
    # Initialize Lambda client
    lambda_client = boto3.client('lambda', region_name='eu-west-3')
    
    print(f"🚀 Triggering pv-logger Lambda at {datetime.now().isoformat()}")
    
    try:
        # Invoke the Lambda function
        response = lambda_client.invoke(
            FunctionName='pv-logger',
            InvocationType='RequestResponse',  # Synchronous execution
            Payload=json.dumps({})  # Empty event
        )
        
        # Read the response
        response_payload = json.loads(response['Payload'].read())
        
        print(f"✅ Lambda execution completed")
        print(f"Status Code: {response['StatusCode']}")
        print(f"Response: {json.dumps(response_payload, indent=2)}")
        
        # Check if there was an error
        if response['StatusCode'] != 200:
            print(f"⚠️ Lambda returned non-200 status: {response['StatusCode']}")
        
        return response_payload
        
    except Exception as e:
        print(f"❌ Error triggering Lambda: {e}")
        return None

if __name__ == "__main__":
    result = trigger_lambda()
    
    if result:
        print("\n📊 Lambda execution successful!")
        print("Check the dashboard in a few seconds to see updated values.")
    else:
        print("\n❌ Failed to trigger Lambda")


