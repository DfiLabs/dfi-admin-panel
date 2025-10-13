# Pulse Mailer (Magic Link) — AWS Deploy Guide

Prereqs:
- AWS CLI configured to the target account/region
- SES identity verified for dfi-labs.com (sandbox ok)

Steps:
1) Create DynamoDB table
```
aws dynamodb create-table \
  --table-name pulse_tokens \
  --attribute-definitions AttributeName=token,AttributeType=S \
  --key-schema AttributeName=token,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

2) Create IAM role for Lambda with trust and policies:
- Trust policy: Service = lambda.amazonaws.com
- Attach: AWSLambdaBasicExecutionRole, AmazonSESFullAccess, AmazonDynamoDBFullAccess

3) Package Lambda
```
zip -j /tmp/pulse_mailer.zip pulse_mailer.py
```

4) Create function
```
aws lambda create-function \
  --function-name pulse-access-mailer \
  --runtime python3.11 \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME> \
  --handler pulse_mailer.lambda_handler \
  --zip-file fileb:///tmp/pulse_mailer.zip \
  --timeout 15 --memory-size 256 \
  --environment Variables={SES_FROM=hello@dfi-labs.com,BASE_URL=https://pulse.dfi-labs.com/pulse/access/verify.html,TABLE=pulse_tokens}
```

5) Create Function URL (public, with CORS)
```
aws lambda create-function-url-config \
  --function-name pulse-access-mailer \
  --auth-type NONE \
  --cors AllowOrigins='["*"]',AllowMethods='["POST","OPTIONS"]',AllowHeaders='["*"]',MaxAge=86400
aws lambda add-permission \
  --function-name pulse-access-mailer \
  --action lambda:InvokeFunctionUrl \
  --statement-id pulse-furl \
  --principal '*'
aws lambda get-function-url-config --function-name pulse-access-mailer --query FunctionUrl --output text
```

6) Configure frontend
- Upload modules/pulse-access/config.json with ACCESS_API_URL set to the function URL.
- Ensure CloudFront/S3 origins serve /pulse/access/* paths.

Notes:
- In SES sandbox, the API returns { ok:false, link: ... } and the page shows a copyable link.
- Once SES moves to production, emails will be sent to any address.



