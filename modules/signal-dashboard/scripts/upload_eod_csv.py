#!/usr/bin/env python3
"""
Upload the canonical EOD PV CSV for Pulse to S3 and optionally invalidate CloudFront.

Examples:
  python3 upload_eod_csv.py \
    --csv-path "../eod_pv.csv" \
    --s3-uri "s3://<BUCKET>/descartes-ml/signal-dashboard/data/eod_pv.csv" \
    --cf-distribution-id <DIST_ID>

Requires AWS credentials in your environment (profile/role/keys).
"""

import argparse
import os
import sys
from urllib.parse import urlparse
from datetime import datetime

try:
    import boto3  # type: ignore
    from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    boto3 = None


def parse_s3_uri(s3_uri: str):
    parsed = urlparse(s3_uri)
    if parsed.scheme != 's3' or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def summarize_csv(csv_path: str):
    first_date, last_date, num_rows = None, None, 0
    with open(csv_path, 'r', encoding='utf-8') as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line:
                continue
            num_rows += 1
            try:
                d = line.split(',')[0]
                if not first_date:
                    first_date = d
                last_date = d
            except Exception:
                continue
    return first_date, last_date, num_rows


def main():
    parser = argparse.ArgumentParser(description='Upload Pulse EOD PV CSV to S3')
    parser.add_argument('--csv-path', default=os.path.join(os.path.dirname(__file__), '..', 'eod_pv.csv'),
                        help='Path to local eod_pv.csv (default: ../eod_pv.csv)')
    parser.add_argument('--s3-uri', required=True, help='Dest S3 URI, e.g. s3://bucket/descartes-ml/signal-dashboard/data/eod_pv.csv')
    parser.add_argument('--cf-distribution-id', default=None, help='Optional CloudFront Distribution ID to invalidate')
    args = parser.parse_args()

    if boto3 is None:
        print('boto3 is required. pip install boto3', file=sys.stderr)
        sys.exit(2)

    if not os.path.exists(args.csv_path):
        print(f'CSV not found: {args.csv_path}', file=sys.stderr)
        sys.exit(1)

    bucket, key = parse_s3_uri(args.s3_uri)

    s3 = boto3.client('s3')

    first, last, rows = summarize_csv(args.csv_path)
    print(f'Uploading EOD CSV: {args.csv_path} rows={rows} range={first}..{last}')

    try:
        with open(args.csv_path, 'rb') as f:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=f,
                ContentType='text/csv',
                CacheControl='no-store'
            )
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head.get('ContentLength')
        etag = head.get('ETag')
        lm = head.get('LastModified')
        print(f'✅ Uploaded to s3://{bucket}/{key} size={size} etag={etag} last_modified={lm}')
    except (BotoCoreError, ClientError) as e:
        print('Upload failed:', e, file=sys.stderr)
        sys.exit(3)

    if args.cf_distribution_id:
        cf = boto3.client('cloudfront')
        try:
            res = cf.create_invalidation(
                DistributionId=args.cf_distribution_id,
                InvalidationBatch={
                    'Paths': {
                        'Quantity': 1,
                        'Items': ['/signal-dashboard/data/eod_pv.csv']
                    },
                    'CallerReference': f'eod-pv-upload-{datetime.utcnow().timestamp()}'
                }
            )
            inv_id = res['Invalidation']['Id']
            print(f'🚀 CF invalidation started: {inv_id}')
        except (BotoCoreError, ClientError) as e:
            print('CloudFront invalidation failed:', e, file=sys.stderr)


if __name__ == '__main__':
    main()


