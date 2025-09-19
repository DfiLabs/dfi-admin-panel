#!/bin/bash

# DFI Labs Factsheet Platform Deployment Script
echo "🚀 Deploying DFI Labs Factsheet Platform to admin.factsheet.dfi-labs.com"

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    echo "❌ Error: package.json not found. Please run this script from the project root."
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Build the project
echo "🔨 Building the project..."
npm run build

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
else
    echo "❌ Build failed. Please check the errors above."
    exit 1
fi

# Start the production server
echo "🌐 Starting production server..."
echo "📍 The application will be available at: https://admin.factsheet.dfi-labs.com"
echo "🔒 Make sure your domain is properly configured to point to this server."

npm start
