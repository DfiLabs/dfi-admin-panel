#!/bin/bash

# DFI Labs Factsheet Platform - Production Startup Script
echo "🚀 Starting DFI Labs Factsheet Platform in Production Mode"

# Set production environment
export NODE_ENV=production
export PORT=3000
export HOSTNAME=0.0.0.0

# Check if build exists
if [ ! -d ".next" ]; then
    echo "📦 Building application..."
    npm run build
fi

# Start the application
echo "🌐 Starting production server on port 3000..."
echo "📍 Application will be available at: https://admin.factsheet.dfi-labs.com"
echo "🔍 Health check: https://admin.factsheet.dfi-labs.com/api/health"

# Use PM2 if available, otherwise use npm start
if command -v pm2 &> /dev/null; then
    echo "📊 Using PM2 for process management..."
    pm2 start npm --name "dfi-factsheet" -- start
    pm2 save
    pm2 startup
else
    echo "🔄 Starting with npm start..."
    npm start
fi
