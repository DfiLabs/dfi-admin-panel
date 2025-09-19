#!/bin/bash

# CSV Monitoring Server Startup Script
echo "🚀 Starting CSV Monitoring System..."
echo "=================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Check if CSV files exist in current directory
if [ ! -f "lpxd_external_advisors_DF_20250915-2355.csv" ]; then
    echo "❌ Preferred 2355.csv file not found in current directory"
    echo "   Please ensure the CSV files are available locally"
    exit 1
fi

echo "📁 Monitoring directory: $(pwd)"
echo "📁 Found 2355.csv file: lpxd_external_advisors_DF_20250915-2355.csv"
echo "🌐 Server will run on: http://localhost:8001"
echo ""

# Start the monitoring server
echo "🔄 Starting CSV monitor server..."
python3 csv-monitor-server.py

echo "🛑 CSV monitoring server stopped"
