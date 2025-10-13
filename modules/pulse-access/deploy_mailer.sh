#!/usr/bin/env bash
set -euo pipefail

ROLE_NAME="pulse-mailer-role"
FUNC_NAME="pulse-access-mailer"
REGION="${AWS_REGION:-us-east-1}"
SES_FROM="hello@dfi-labs.com"
BASE_URL="https://pulse.dfi-labs.com/pulse/access/verify.html"
TABLE="pulse_tokens"

aws dynamodb describe-table --table-name "$TABLE" >/dev/null 2>&1 || aws dynamodb create-table \
  --table-name "$TABLE" \
  --attribute-definitions AttributeName=token,AttributeType=S \
  --key-schema AttributeName=token,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null

if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  TMP=$(mktemp)
  cat >"$TMP" <<JSON
{ "Version": "2012-10-17", "Statement": [ { "Effect": "Allow", "Principal": { "Service": "lambda.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }
JSON
  aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file://"$TMP" >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/AmazonSESFullAccess >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess >/dev/null
  sleep 8
fi

zip -j /tmp/pulse_mailer.zip "$(dirname "$0")/pulse_mailer.py"

ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query Role.Arn --output text)
if aws lambda get-function --function-name "$FUNC_NAME" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FUNC_NAME" --zip-file fileb:///tmp/pulse_mailer.zip >/dev/null
  aws lambda update-function-configuration --function-name "$FUNC_NAME" \
    --runtime python3.11 --role "$ROLE_ARN" --handler pulse_mailer.lambda_handler \
    --timeout 15 --memory-size 256 \
    --environment Variables="{SES_FROM=$SES_FROM,BASE_URL=$BASE_URL,TABLE=$TABLE}" >/dev/null
else
  aws lambda create-function --function-name "$FUNC_NAME" \
    --runtime python3.11 --role "$ROLE_ARN" --handler pulse_mailer.lambda_handler \
    --zip-file fileb:///tmp/pulse_mailer.zip --timeout 15 --memory-size 256 \
    --environment Variables="{SES_FROM=$SES_FROM,BASE_URL=$BASE_URL,TABLE=$TABLE}" >/dev/null
fi

if aws lambda get-function-url-config --function-name "$FUNC_NAME" >/dev/null 2>&1; then :; else
  aws lambda create-function-url-config --function-name "$FUNC_NAME" --auth-type NONE \
    --cors AllowOrigins='["*"]',AllowMethods='["POST","OPTIONS"]',AllowHeaders='["*"]',MaxAge=86400 >/dev/null
fi
aws lambda add-permission --function-name "$FUNC_NAME" --action lambda:InvokeFunctionUrl --statement-id pulse-furl --principal '*' >/dev/null 2>&1 || true

aws lambda get-function-url-config --function-name "$FUNC_NAME" --query FunctionUrl --output text


