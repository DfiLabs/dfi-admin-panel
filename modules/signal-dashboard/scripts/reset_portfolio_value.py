#!/usr/bin/env python3
"""
Portfolio Value Reset Script
Resets the portfolio value to $1,000,000 and clears the chart history
"""

import json
import subprocess
from datetime import datetime, timezone

# Configuration
S3_BUCKET = "dfi-signal-dashboard"
S3_KEY = "signal-dashboard/data/portfolio_value_log.jsonl"

def reset_portfolio_value():
    """Reset portfolio value to $1,000,000 and clear chart history"""
    
    print("🔄 Resetting Portfolio Value to $1,000,000...")
    
    # Create a new baseline entry
    reset_entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'portfolio_value': 1000000.0,
        'daily_pnl': 0.0,
        'total_pnl': 0.0
    }
    
    # Create new data with just the reset entry
    new_data = json.dumps(reset_entry) + '\n'
    
    # Write to temp file
    with open('/tmp/reset_pv_log.jsonl', 'w') as f:
        f.write(new_data)
    
    # Upload to S3 (this replaces the entire file)
    try:
        result = subprocess.run([
            'aws', 's3', 'cp',
            '/tmp/reset_pv_log.jsonl',
            f's3://{S3_BUCKET}/{S3_KEY}'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Portfolio Value Reset Successfully!")
            print(f"📊 New Portfolio Value: $1,000,000.00")
            print(f"📊 Daily P&L: $0.00")
            print(f"📊 Total P&L: $0.00")
            print(f"📊 Reset Time: {reset_entry['timestamp']}")
            print("---")
            print("🎯 Chart will now start fresh from $1,000,000")
            print("🔄 New data points will be logged from this baseline")
            return True
        else:
            print(f"❌ Error uploading reset data: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error during reset: {e}")
        return False

def backup_current_data():
    """Backup current data before reset"""
    
    print("💾 Creating backup of current data...")
    
    try:
        # Download current data
        result = subprocess.run([
            'aws', 's3', 'cp',
            f's3://{S3_BUCKET}/{S3_KEY}',
            '/tmp/backup_pv_log.jsonl'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            # Create backup with timestamp
            backup_filename = f"portfolio_value_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            backup_key = f"signal-dashboard/backups/{backup_filename}"
            
            # Upload backup
            backup_result = subprocess.run([
                'aws', 's3', 'cp',
                '/tmp/backup_pv_log.jsonl',
                f's3://{S3_BUCKET}/{backup_key}'
            ], capture_output=True, text=True, timeout=30)
            
            if backup_result.returncode == 0:
                print(f"✅ Backup created: {backup_filename}")
                return True
            else:
                print(f"⚠️ Could not create backup: {backup_result.stderr}")
                return False
        else:
            print(f"⚠️ Could not download current data for backup: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"⚠️ Error creating backup: {e}")
        return False

def main():
    print("🚀 Portfolio Value Reset Tool")
    print("=" * 50)
    
    # Ask for confirmation
    response = input("⚠️  This will reset your portfolio value to $1,000,000 and clear all chart history.\n   Do you want to continue? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("❌ Reset cancelled.")
        return
    
    # Create backup first
    backup_success = backup_current_data()
    if not backup_success:
        print("⚠️  Backup failed, but continuing with reset...")
    
    # Perform reset
    success = reset_portfolio_value()
    
    if success:
        print("\n🎉 RESET COMPLETE!")
        print("Your dashboard chart will now start fresh from $1,000,000")
        print("New data points will be logged from this baseline")
    else:
        print("\n❌ RESET FAILED!")
        print("Please check the error messages above")

if __name__ == "__main__":
    main()
