#!/bin/bash

# DFI Labs Factsheet Platform - AWS Deployment Script
echo "🚀 Deploying DFI Labs Factsheet Platform to AWS"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Please install it first:"
    echo "   curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'"
    echo "   unzip awscliv2.zip"
    echo "   sudo ./aws/install"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    echo "❌ Error: package.json not found. Please run this script from the project root."
    exit 1
fi

echo "📦 Building static export for AWS S3..."
npm run build

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
else
    echo "❌ Build failed. Please check the errors above."
    exit 1
fi

echo "📁 Uploading to S3..."
# Replace 'your-bucket-name' with your actual S3 bucket name
BUCKET_NAME="admin-factsheet-dfi-labs-com"

# Create bucket if it doesn't exist
aws s3 mb s3://$BUCKET_NAME --region us-east-1 2>/dev/null || echo "Bucket already exists"

# Upload files
aws s3 sync out/ s3://$BUCKET_NAME --delete

# Set bucket policy for public read
aws s3api put-bucket-policy --bucket $BUCKET_NAME --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadGetObject",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::'$BUCKET_NAME'/*"
    }
  ]
}'

# Configure website hosting
aws s3 website s3://$BUCKET_NAME --index-document index.html --error-document 404.html

echo "🌐 Website URL: http://$BUCKET_NAME.s3-website-us-east-1.amazonaws.com"
echo "🔗 Custom Domain: https://admin.factsheet.dfi-labs.com"
echo ""
echo "📋 Next Steps:"
echo "1. Configure CloudFront distribution"
echo "2. Set up SSL certificate in Certificate Manager"
echo "3. Add DNS record in Route 53"
echo "4. Configure custom domain in CloudFront"
