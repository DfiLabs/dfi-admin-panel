# 🚀 AWS Lambda Serverless Deployment

This directory contains AWS Lambda deployment packages for your signal dashboard scripts.

## 📦 What's Included

### Lambda Functions
1. **pv-logger** - Logs portfolio value data every 5 minutes
2. **price-writer** - Fetches Binance prices every 60 seconds

### Files
- `pv-logger-lambda.zip` - PV logger deployment package
- `price-writer-lambda.zip` - Price writer deployment package
- `deploy-lambdas.sh` - Automated deployment script
- `README.md` - This file

## 🚀 Quick Deployment

### Prerequisites
1. AWS CLI installed and configured
2. IAM role with Lambda execution permissions
3. S3 bucket access permissions

### Step 1: Configure AWS CLI
```bash
aws configure
# Enter your AWS Access Key ID, Secret Access Key, region, and output format
```

### Step 2: Create IAM Role (if not exists)
```bash
aws iam create-role \
    --role-name lambda-execution-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach basic execution role policy
aws iam attach-role-policy \
    --role-name lambda-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Attach S3 permissions (customize bucket name)
aws iam attach-role-policy \
    --role-name lambda-execution-role \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### Step 3: Deploy Lambda Functions
```bash
chmod +x deploy-lambdas.sh
./deploy-lambdas.sh
```

## 📋 Manual Deployment (Alternative)

### Create PV Logger Lambda
```bash
aws lambda create-function \
    --function-name pv-logger \
    --runtime python3.9 \
    --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://pv-logger-lambda.zip \
    --timeout 300 \
    --memory-size 512 \
    --environment "Variables={S3_BUCKET=dfi-signal-dashboard}"
```

### Create Price Writer Lambda
```bash
aws lambda create-function \
    --function-name price-writer \
    --runtime python3.9 \
    --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://price-writer-lambda.zip \
    --timeout 60 \
    --memory-size 256 \
    --environment "Variables={S3_BUCKET=dfi-signal-dashboard}"
```

### Create CloudWatch Events Rules

#### PV Logger (5 minutes)
```bash
# Create rule
aws events put-rule \
    --name pv-logger-trigger \
    --schedule-expression "rate(5 minutes)" \
    --state ENABLED

# Add permission
aws lambda add-permission \
    --function-name pv-logger \
    --statement-id "CloudWatchEvents-pv-logger-trigger" \
    --action 'lambda:InvokeFunction' \
    --principal events.amazonaws.com

# Add target
aws events put-targets \
    --rule pv-logger-trigger \
    --targets "Id=1,Arn=arn:aws:lambda:YOUR_REGION:YOUR_ACCOUNT_ID:function:pv-logger"
```

#### Price Writer (1 minute)
```bash
# Create rule
aws events put-rule \
    --name price-writer-trigger \
    --schedule-expression "rate(1 minute)" \
    --state ENABLED

# Add permission
aws lambda add-permission \
    --function-name price-writer \
    --statement-id "CloudWatchEvents-price-writer-trigger" \
    --action 'lambda:InvokeFunction' \
    --principal events.amazonaws.com

# Add target
aws events put-targets \
    --rule price-writer-trigger \
    --targets "Id=1,Arn=arn:aws:lambda:YOUR_REGION:YOUR_ACCOUNT_ID:function:price-writer"
```

## 🔍 Monitoring & Logs

### Check Lambda Function Logs
```bash
# PV Logger logs
aws logs tail /aws/lambda/pv-logger --follow

# Price Writer logs
aws logs tail /aws/lambda/price-writer --follow
```

### Monitor Function Performance
1. Go to AWS Lambda Console
2. Select your function (pv-logger or price-writer)
3. Click on "Monitor" tab
4. View metrics: Invocations, Duration, Errors, Throttles

### Check CloudWatch Events
```bash
# List rules
aws events list-rules

# Check rule status
aws events describe-rule --name pv-logger-trigger
aws events describe-rule --name price-writer-trigger
```

## 📊 Verify Deployment

### Test Lambda Functions
```bash
# Test PV Logger
aws lambda invoke --function-name pv-logger --payload '{}' response.json
cat response.json

# Test Price Writer
aws lambda invoke --function-name price-writer --payload '{}' response.json
cat response.json
```

### Check S3 Data
```bash
# Check PV log
aws s3 ls s3://dfi-signal-dashboard/signal-dashboard/data/portfolio_value_log.jsonl

# Check latest prices
aws s3 ls s3://dfi-signal-dashboard/signal-dashboard/data/latest_prices.json
```

## 🔧 Configuration

### Environment Variables
- `S3_BUCKET` - S3 bucket name (default: dfi-signal-dashboard)
- `S3_KEY` - S3 key path for data files

### Memory & Timeout Settings
- **pv-logger**: 512MB memory, 300s timeout
- **price-writer**: 256MB memory, 60s timeout

## 🚨 Troubleshooting

### Common Issues

1. **Permission Denied**
   ```bash
   # Check IAM role has correct permissions
   aws iam get-role --role-name lambda-execution-role
   ```

2. **Function Timeout**
   - Increase timeout in Lambda configuration
   - Check CloudWatch logs for timeout errors

3. **S3 Access Issues**
   ```bash
   # Test S3 permissions
   aws s3 ls s3://dfi-signal-dashboard/
   ```

4. **CloudWatch Events Not Triggering**
   ```bash
   # Check rule status
   aws events describe-rule --name pv-logger-trigger
   aws events list-targets-by-rule --rule pv-logger-trigger
   ```

### Debug Mode
Set log level to DEBUG in Lambda configuration for detailed logging.

## 📈 Cost Estimation

### Monthly Costs (approximate)
- **pv-logger**: 12 invocations/day × 30 days × $0.0000002 = $0.00007
- **price-writer**: 1440 invocations/day × 30 days × $0.0000002 = $0.00864
- **Total**: ~$0.01/month

### Lambda Pricing
- First 1M requests: FREE
- $0.20 per 1M requests thereafter
- $0.000000002 per millisecond of execution time

## 🎯 Next Steps

1. Deploy the Lambda functions using the script above
2. Monitor execution in CloudWatch logs
3. Verify S3 data is being written correctly
4. Update your dashboard to use the new serverless backend
5. Consider setting up CloudWatch alarms for monitoring

## 📞 Support

For issues with this deployment:
1. Check CloudWatch logs first
2. Verify AWS permissions
3. Test Lambda functions manually
4. Check S3 bucket access
