# 🚀 AWS Deployment Guide for admin.factsheet.dfi-labs.com

## 📋 Prerequisites

- AWS CLI configured with your credentials
- Domain `dfi-labs.com` already in Route 53
- SSL certificate for `admin.factsheet.dfi-labs.com`

## 🎯 Quick Deployment (3 Steps)

### Step 1: Request SSL Certificate
```bash
# In AWS Certificate Manager
aws acm request-certificate \
  --domain-name admin.factsheet.dfi-labs.com \
  --validation-method DNS \
  --region us-east-1
```

### Step 2: Deploy Infrastructure
```bash
# Deploy CloudFormation stack
aws cloudformation create-stack \
  --stack-name dfi-factsheet-platform \
  --template-body file://aws-cloudformation.yaml \
  --parameters ParameterKey=CertificateArn,ParameterValue=YOUR_CERTIFICATE_ARN \
  --capabilities CAPABILITY_IAM
```

### Step 3: Deploy Application
```bash
# Build and upload to S3
./deploy-aws.sh
```

## 🔧 Manual Setup (Alternative)

### 1. Create S3 Bucket
```bash
aws s3 mb s3://admin-factsheet-dfi-labs-com --region us-east-1
```

### 2. Configure Static Website Hosting
```bash
aws s3 website s3://admin-factsheet-dfi-labs-com \
  --index-document index.html \
  --error-document 404.html
```

### 3. Create CloudFront Distribution
- Origin: S3 bucket
- Aliases: admin.factsheet.dfi-labs.com
- SSL Certificate: Your certificate ARN
- Default Root Object: index.html

### 4. Add Route 53 Record
- Type: A (Alias)
- Alias Target: CloudFront distribution
- Name: admin.factsheet.dfi-labs.com

## 📊 Cost Estimation

- **S3**: ~$0.023/GB/month (minimal for static site)
- **CloudFront**: ~$0.085/GB (first 10TB)
- **Route 53**: $0.50/hosted zone/month
- **Certificate**: Free with ACM

**Total**: ~$1-5/month for typical usage

## 🔄 Updates

To update the application:
```bash
npm run build
aws s3 sync out/ s3://admin-factsheet-dfi-labs-com --delete
```

## 🚨 Troubleshooting

### Common Issues:

1. **Certificate not in us-east-1**: CloudFront requires certificates in us-east-1
2. **DNS propagation**: Can take up to 48 hours
3. **Cache issues**: CloudFront cache can take time to update

### Health Check:
```bash
curl -I https://admin.factsheet.dfi-labs.com
```

---

**🎉 Your factsheet platform will be live at: https://admin.factsheet.dfi-labs.com**
