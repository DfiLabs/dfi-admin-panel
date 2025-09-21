#!/bin/bash

# Test Lambda functions locally
# This script tests the Lambda packages before deploying to AWS

set -e

echo "🧪 Testing Lambda Functions Locally..."
echo

# Test PV Logger
echo "📊 Testing PV Logger Lambda..."
cd pv-logger
python3 lambda_function.py
cd ..

echo
echo "💰 Testing Price Writer Lambda..."
cd price-writer
python3 lambda_function.py
cd ..

echo
echo "✅ Local testing completed successfully!"
echo "📝 Both Lambda functions can run locally without errors."
echo "🚀 Ready for AWS deployment!"
