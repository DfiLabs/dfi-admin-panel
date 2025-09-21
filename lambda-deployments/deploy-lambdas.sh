#!/bin/bash

# AWS Lambda Deployment Script
# Deploys PV Logger and Price Writer Lambda functions

set -e

# Configuration - use configured AWS region
REGION=$(aws configure get region)
S3_BUCKET="dfi-signal-dashboard"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 AWS Lambda Deployment Script${NC}"
echo -e "${YELLOW}Region: $REGION${NC}"
echo -e "${YELLOW}S3 Bucket: $S3_BUCKET${NC}"
echo

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}❌ AWS CLI not configured. Please run 'aws configure' first${NC}"
    exit 1
fi

echo -e "${GREEN}✅ AWS CLI configured${NC}"

# Function to create Lambda function
create_lambda() {
    local function_name=$1
    local zip_file=$2
    local handler=$3
    local timeout=$4
    local memory=$5

    echo -e "${YELLOW}📦 Creating/updating Lambda function: $function_name${NC}"

    # Create the function
    aws lambda create-function \
        --function-name "$function_name" \
        --runtime python3.9 \
        --role arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/lambda-execution-role \
        --handler "$handler" \
        --zip-file "fileb://$zip_file" \
        --timeout "$timeout" \
        --memory-size "$memory" \
        --environment "Variables={S3_BUCKET=$S3_BUCKET}" \
        --region "$REGION" || \
    aws lambda update-function-code \
        --function-name "$function_name" \
        --zip-file "fileb://$zip_file" \
        --region "$REGION"

    echo -e "${GREEN}✅ Lambda function $function_name created/updated${NC}"
}

# Deploy PV Logger (5-minute intervals)
echo -e "${YELLOW}📊 Deploying PV Logger Lambda...${NC}"
create_lambda "pv-logger" "pv-logger-lambda.zip" "lambda_function.lambda_handler" 300 512

# Deploy Price Writer (60-second intervals)
echo -e "${YELLOW}💰 Deploying Price Writer Lambda...${NC}"
create_lambda "price-writer" "price-writer-lambda.zip" "lambda_function.lambda_handler" 60 256

# Create CloudWatch Events rules
create_cloudwatch_rule() {
    local rule_name=$1
    local schedule=$2
    local lambda_name=$3

    echo -e "${YELLOW}⏰ Creating CloudWatch Events rule: $rule_name${NC}"

    # Create or update the rule
    aws events put-rule \
        --name "$rule_name" \
        --schedule-expression "rate($schedule)" \
        --state ENABLED \
        --region "$REGION" || \
    aws events enable-rule \
        --name "$rule_name" \
        --region "$REGION"

    # Add permission for CloudWatch to invoke Lambda
    aws lambda add-permission \
        --function-name "$lambda_name" \
        --statement-id "CloudWatchEvents-$rule_name" \
        --action 'lambda:InvokeFunction' \
        --principal events.amazonaws.com \
        --region "$REGION"

    # Create the target
    aws events put-targets \
        --rule "$rule_name" \
        --targets "Id=1,Arn=arn:aws:lambda:$REGION:$(aws sts get-caller-identity --query Account --output text):function:$lambda_name" \
        --region "$REGION"

    echo -e "${GREEN}✅ CloudWatch Events rule $rule_name created${NC}"
}

# Create CloudWatch Events triggers
echo -e "${YELLOW}⏰ Setting up CloudWatch Events triggers...${NC}"

# PV Logger - every 5 minutes
create_cloudwatch_rule "pv-logger-trigger" "5 minutes" "pv-logger"

# Price Writer - every 60 seconds
create_cloudwatch_rule "price-writer-trigger" "1 minute" "price-writer"

echo
echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo
echo -e "${YELLOW}📊 Functions deployed:${NC}"
echo -e "  • pv-logger (runs every 5 minutes)"
echo -e "  • price-writer (runs every 60 seconds)"
echo
echo -e "${YELLOW}📝 Next steps:${NC}"
echo -e "  1. Monitor Lambda functions in AWS Console"
echo -e "  2. Check CloudWatch logs for execution logs"
echo -e "  3. Verify S3 bucket has new data files"
echo -e "  4. Test dashboard still works correctly"
echo
echo -e "${GREEN}✅ All set! Your serverless architecture is now running.${NC}"
