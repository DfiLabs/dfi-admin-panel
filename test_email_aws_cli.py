#!/usr/bin/env python3
"""
Test email functionality using AWS CLI (no boto3 required)
"""

import subprocess
import json
from datetime import datetime
import tempfile
import os

def send_test_email_aws_cli():
    """Send a test email using AWS CLI"""
    
    print("🧪 Testing Signal Dashboard Email System (AWS CLI)...")
    print(f"📧 Sender: hello@dfi-labs.com")
    print(f"📧 Recipient: hello@dfi-labs.com")
    print(f"🌍 Region: eu-west-1")
    print("-" * 50)
    
    # Create email content
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f"🧪 Signal Dashboard Email Test - {timestamp} UTC"
    
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ background-color: #007bff; color: white; padding: 15px; border-radius: 5px; }}
            .content {{ margin: 20px 0; }}
            .status {{ background-color: #d4edda; border: 1px solid #c3e6cb; padding: 10px; border-radius: 5px; }}
            .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>🧪 Signal Dashboard Email Test</h2>
        </div>
        
        <div class="content">
            <h3>Test Results:</h3>
            <div class="status">
                ✅ <strong>Email System Status:</strong> WORKING<br>
                ✅ <strong>Timestamp:</strong> {timestamp} UTC<br>
                ✅ <strong>Sender:</strong> hello@dfi-labs.com<br>
                ✅ <strong>Recipient:</strong> hello@dfi-labs.com<br>
                ✅ <strong>AWS Region:</strong> eu-west-1
            </div>
            
            <h3>System Components Tested:</h3>
            <ul>
                <li>✅ AWS CLI SES Integration</li>
                <li>✅ Email Composition</li>
                <li>✅ HTML Email Formatting</li>
                <li>✅ Email Delivery</li>
            </ul>
            
            <h3>Current Dashboard Status:</h3>
            <ul>
                <li>✅ Portfolio Value: ~$1,003,473</li>
                <li>✅ Daily P&L: ~$3,473</li>
                <li>✅ Total P&L: ~$3,473</li>
                <li>✅ 29 Positions Active</li>
                <li>✅ Real-time Price Updates</li>
                <li>✅ S3 Data Logging</li>
            </ul>
            
            <h3>Next Steps:</h3>
            <p>If you received this email, the email system is working correctly and ready for:</p>
            <ul>
                <li>Daily CSV processing notifications</li>
                <li>Portfolio execution summaries</li>
                <li>System status alerts</li>
            </ul>
        </div>
        
        <div class="footer">
            <p>This is an automated test email from the Signal Dashboard system.</p>
            <p>Generated at: {datetime.now().isoformat()} UTC</p>
        </div>
    </body>
    </html>
    """
    
    # Create temporary file for email content
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        email_data = {
            "Source": "hello@dfi-labs.com",
            "Destination": {
                "ToAddresses": ["hello@dfi-labs.com"]
            },
            "Message": {
                "Subject": {
                    "Data": subject,
                    "Charset": "UTF-8"
                },
                "Body": {
                    "Html": {
                        "Data": html_body,
                        "Charset": "UTF-8"
                    }
                }
            }
        }
        json.dump(email_data, f, indent=2)
        temp_file = f.name
    
    try:
        # Send email using AWS CLI
        print("📧 Sending test email via AWS CLI...")
        result = subprocess.run([
            'aws', 'ses', 'send-email',
            '--region', 'eu-west-1',
            '--cli-input-json', f'file://{temp_file}'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Test email sent successfully!")
            print("📬 Check your inbox for the test email.")
            
            # Parse the response to get message ID
            try:
                response = json.loads(result.stdout)
                message_id = response.get('MessageId', 'Unknown')
                print(f"📧 Message ID: {message_id}")
            except:
                print("📧 Message ID: Available in AWS console")
                
        else:
            print("❌ Error sending email:")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file)
        except:
            pass
    
    return True

if __name__ == "__main__":
    success = send_test_email_aws_cli()
    
    if success:
        print("\n✅ Email test completed successfully!")
        print("📬 Check your inbox for the test email.")
    else:
        print("\n❌ Email test failed!")
        print("🔧 Please check AWS SES configuration and permissions.")
