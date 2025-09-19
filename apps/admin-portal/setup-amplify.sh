#!/bin/bash

# DFI Labs Factsheet Platform - AWS Amplify Setup Script
echo "🚀 Setting up AWS Amplify for DFI Labs Factsheet Platform"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Installing..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf aws awscliv2.zip
fi

# Check if Amplify CLI is installed
if ! command -v amplify &> /dev/null; then
    echo "❌ Amplify CLI not found. Installing..."
    npm install -g @aws-amplify/cli
fi

echo "🔧 Configuring AWS credentials..."
echo "Please provide your AWS credentials:"
echo ""

# Configure AWS CLI
aws configure

echo ""
echo "✅ AWS CLI configured!"
echo ""

# Initialize Amplify project
echo "🎯 Initializing Amplify project..."
amplify init --yes

# Add hosting
echo "🌐 Adding hosting to Amplify..."
amplify add hosting --yes

# Deploy
echo "🚀 Deploying to AWS Amplify..."
amplify publish --yes

echo ""
echo "🎉 Deployment complete!"
echo "📍 Your factsheet platform is now live!"
echo ""
echo "📋 Next steps:"
echo "1. Go to AWS Amplify Console"
echo "2. Add custom domain: admin.factsheet.dfi-labs.com"
echo "3. Configure SSL certificate"
echo "4. Update DNS records"
echo ""
echo "🔗 Amplify Console: https://console.aws.amazon.com/amplify/"
