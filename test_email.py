#!/usr/bin/env python3
"""
Test email functionality for the signal dashboard system
"""

import boto3
import json
from datetime import datetime
import os

# Email configuration
SENDER_EMAIL = "leo@dfi-labs.com"
RECIPIENT_EMAIL = "leo@dfi-labs.com"  # Change this to your email
AWS_REGION = "us-east-1"

def send_test_email():
    """Send a test email to verify the email system is working"""
    
    try:
        # Initialize SES client
        ses_client = boto3.client('ses', region_name=AWS_REGION)
        
        # Test email content
        subject = f"🧪 Signal Dashboard Email Test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
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
                    ✅ <strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
                    ✅ <strong>Sender:</strong> {SENDER_EMAIL}<br>
                    ✅ <strong>Recipient:</strong> {RECIPIENT_EMAIL}<br>
                    ✅ <strong>AWS Region:</strong> {AWS_REGION}
                </div>
                
                <h3>System Components Tested:</h3>
                <ul>
                    <li>✅ AWS SES Client Initialization</li>
                    <li>✅ Email Composition</li>
                    <li>✅ HTML Email Formatting</li>
                    <li>✅ Email Delivery</li>
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
        
        # Send email
        response = ses_client.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [RECIPIENT_EMAIL]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {'Html': {'Data': html_body, 'Charset': 'UTF-8'}}
            }
        )
        
        print(f"✅ Test email sent successfully!")
        print(f"📧 Message ID: {response['MessageId']}")
        print(f"📧 To: {RECIPIENT_EMAIL}")
        print(f"📧 Subject: {subject}")
        print(f"📧 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        return True
        
    except Exception as e:
        print(f"❌ Error sending test email: {str(e)}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Signal Dashboard Email System...")
    print(f"📧 Sender: {SENDER_EMAIL}")
    print(f"📧 Recipient: {RECIPIENT_EMAIL}")
    print(f"🌍 Region: {AWS_REGION}")
    print("-" * 50)
    
    success = send_test_email()
    
    if success:
        print("\n✅ Email test completed successfully!")
        print("📬 Check your inbox for the test email.")
    else:
        print("\n❌ Email test failed!")
        print("🔧 Please check AWS SES configuration and permissions.")
