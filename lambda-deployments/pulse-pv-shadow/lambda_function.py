#!/usr/bin/env python3
"""
AWS Lambda: Pulse PV Shadow Logger

Purpose:
- Read Admin's baseline, pre_execution, and positions CSV
- Compute PV using pv_pre + intraday P&L
- Write results to Pulse shadow prefix (does not affect production Pulse log)

Env Vars:
- S3_BUCKET: dfi-signal-dashboard
- READ_PREFIX: signal-dashboard/data/            # Admin data source
- WRITE_PREFIX: descartes-ml/signal-dashboard/data-shadow/  # Pulse shadow sink
"""
from __future__ import annotations

import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import ssl
import os

# Configuration via environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "dfi-signal-dashboard")
READ_PREFIX = os.environ.get("READ_PREFIX", "signal-dashboard/data/")
WRITE_PREFIX = os.environ.get("WRITE_PREFIX", "descartes-ml/signal-dashboard/data-shadow/")

BINANCE_FUTURES_API = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Symbols to fetch
SYMBOLS_TO_FETCH = [
    'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'SOLUSDT', 'TRXUSDT',
    'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'ICPUSDT', 'XLMUSDT',
    'HBARUSDT', 'FETUSDT', 'BCHUSDT', 'LTCUSDT', 'DOTUSDT', 'TONUSDT',
    'RENDERUSDT', 'SUIUSDT', 'UNIUSDT', 'NEARUSDT', 'ETCUSDT', 'AAVEUSDT',
    'VETUSDT', 'ATOMUSDT', 'ALGOUSDT', 'APTUSDT', 'FILUSDT'
]


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def get_mark_prices(symbols: list[str]) -> dict:
    prices: dict[str, float] = {}
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    for symbol in symbols:
        try:
            params = urllib.parse.urlencode({'symbol': symbol})
            url = f"{BINANCE_FUTURES_API}?{params}"
            with urllib.request.urlopen(url, context=ssl_context, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                prices[symbol] = float(data['markPrice'])
        except Exception as e:
            log(f"Error fetching price for {symbol}: {e}")
    return prices


def load_s3_json(s3_client, key: str) -> dict:
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        log(f"Error loading {key}: {e}")
        return {}
    except json.JSONDecodeError as e:
        log(f"Error parsing JSON from {key}: {e}")
        return {}


def load_s3_text(s3_client, key: str) -> str:
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        log(f"Error loading {key}: {e}")
        return ""


def get_latest_csv_filename(s3_client) -> str | None:
    """Find latest CSV via Admin daily log or pre_execution.json under READ_PREFIX."""
    # Try portfolio_daily_log.csv
    daily_log = load_s3_text(s3_client, READ_PREFIX + 'portfolio_daily_log.csv')
    if daily_log:
        lines = daily_log.strip().split('\n')
        for line in reversed(lines):
            if 'post_execution' in line:
                parts = line.split(',')
                if len(parts) > 4:
                    return parts[4]

    # Fallback to pre_execution.json
    pre_exec = load_s3_json(s3_client, READ_PREFIX + 'pre_execution.json')
    if pre_exec and 'csv_filename' in pre_exec:
        return str(pre_exec['csv_filename'])
    return None


def get_pv_pre(s3_client) -> float:
    try:
        pre_exec = load_s3_json(s3_client, READ_PREFIX + 'pre_execution.json')
        return float(pre_exec.get('pv_pre', 1000000.0))
    except Exception as e:
        log(f"Could not load pv_pre: {e}")
        return 1000000.0


def lambda_handler(event, context):
    log("Starting Pulse PV Shadow logger …")
    s3_client = boto3.client('s3')

    # Prices
    prices = get_mark_prices(SYMBOLS_TO_FETCH)
    if not prices:
        log("No prices fetched; skipping")
        return {"statusCode": 200, "body": json.dumps({"message": "no prices"})}

    # Baseline
    baseline_data = load_s3_json(s3_client, READ_PREFIX + 'daily_baseline.json')
    if not baseline_data or 'prices' not in baseline_data:
        log("Missing baseline prices; abort")
        return {"statusCode": 500, "body": json.dumps({"error": "missing baseline"})}

    pv_pre = get_pv_pre(s3_client)
    latest_csv = get_latest_csv_filename(s3_client)
    if not latest_csv:
        log("No CSV filename found; abort")
        return {"statusCode": 500, "body": json.dumps({"error": "no csv filename"})}

    csv_text = load_s3_text(s3_client, READ_PREFIX + latest_csv)
    if not csv_text:
        log("Could not load CSV content; abort")
        return {"statusCode": 500, "body": json.dumps({"error": "no csv content"})}

    headers = []
    lines = [ln for ln in csv_text.strip().split('\n') if ln.strip()]
    if not lines or len(lines) < 2:
        log("CSV has no data rows")
        return {"statusCode": 500, "body": json.dumps({"error": "empty csv"})}

    headers = lines[0].split(',')
    header_index = {h.strip(): i for i, h in enumerate(headers)}

    baseline_prices: dict[str, float] = baseline_data['prices']
    daily_pnl = 0.0
    positions_count = 0

    for row in lines[1:]:
        values = row.split(',')
        # Extract fields safely
        ticker = values[header_index.get('ticker', -1)].strip().replace('_', '') if header_index.get('ticker', -1) >= 0 and header_index['ticker'] < len(values) else ''
        try:
            target_notional = float(values[header_index.get('target_notional', -1)]) if header_index.get('target_notional', -1) >= 0 and header_index['target_notional'] < len(values) else 0.0
        except Exception:
            target_notional = 0.0
        try:
            target_contracts = float(values[header_index.get('target_contracts', -1)]) if header_index.get('target_contracts', -1) >= 0 and header_index['target_contracts'] < len(values) else 0.0
        except Exception:
            target_contracts = 0.0

        baseline_price = baseline_prices.get(ticker)
        current_price = prices.get(ticker)
        if baseline_price and current_price and target_notional != 0:
            side = 1 if target_contracts > 0 else -1
            pnl = side * (current_price - baseline_price) / baseline_price * abs(target_notional)
            daily_pnl += pnl
            positions_count += 1

    portfolio_value = pv_pre + daily_pnl
    total_pnl = portfolio_value - 1000000.0

    entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'portfolio_value': portfolio_value,
        'daily_pnl': daily_pnl,
        'total_pnl': total_pnl,
        'audit': {
            'source': 'lambda-shadow',
            'positions_processed': positions_count,
            'pv_pre': pv_pre,
            'csv_filename': latest_csv
        }
    }

    # Append to shadow log under WRITE_PREFIX
    key = WRITE_PREFIX + 'portfolio_value_log.jsonl'
    try:
        existing = s3_client.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read().decode('utf-8')
        body = existing + json.dumps(entry) + '\n'
    except ClientError:
        body = json.dumps(entry) + '\n'

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode('utf-8'),
        ContentType='application/x-ndjson',
        CacheControl='no-store, max-age=0'
    )

    log(f"Wrote shadow PV entry to s3://{S3_BUCKET}/{key}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'shadow pv logged',
            'portfolio_value': portfolio_value,
            'daily_pnl': daily_pnl,
            'positions_count': positions_count
        })
    }



