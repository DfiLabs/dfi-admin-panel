#!/bin/bash

# Start PV Logger - Runs every 5 minutes
# This script sets up a cron job to log PV data every 5 minutes
# Uses simple_pv_logger.py which includes Phase 3-5 enhancements

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PV_LOGGER="$SCRIPT_DIR/simple_pv_logger.py"

echo "🚀 Setting up PV Logger to run every 5 minutes..."
echo "📊 Using simple_pv_logger.py with Phase 3-5 enhancements"

# Make sure the script is executable
chmod +x "$PV_LOGGER"

# Add cron job to run every 5 minutes
(crontab -l 2>/dev/null; echo "*/5 * * * * cd $SCRIPT_DIR && python3 simple_pv_logger.py >> /tmp/pv_logger.log 2>&1") | crontab -

echo "✅ PV Logger cron job added!"
echo "📊 PV data will be logged every 5 minutes to S3"
echo "📝 Logs will be written to /tmp/pv_logger.log"

# Run it once immediately to test
echo "🧪 Running PV Logger once to test..."
python3 simple_pv_logger.py

echo "✅ PV Logger setup complete!"
