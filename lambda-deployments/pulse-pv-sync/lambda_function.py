#!/usr/bin/env python3
"""
Lambda: Pulse PV Sync
Merge Admin intraday PV into Pulse PV log so that PV = yesterday EOD (Pulse) + intraday Daily P&L, preserving Pulse history.

Env:
  BUCKET = dfi-signal-dashboard
  ADMIN_KEY = signal-dashboard/data/portfolio_value_log.jsonl
  PULSE_KEY = descartes-ml/signal-dashboard/data/portfolio_value_log.jsonl
"""
from __future__ import annotations
import os, json
from datetime import datetime, timezone, timedelta
import boto3

s3 = boto3.client('s3')

BUCKET = os.environ.get('BUCKET', 'dfi-signal-dashboard')
ADMIN_KEY = os.environ.get('ADMIN_KEY', 'signal-dashboard/data/portfolio_value_log.jsonl')
PULSE_KEY = os.environ.get('PULSE_KEY', 'descartes-ml/signal-dashboard/data/portfolio_value_log.jsonl')

def _dt(s: str) -> datetime:
    if s.endswith('Z'):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def read_jsonl(bucket: str, key: str) -> list[dict]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj['Body'].read().decode('utf-8', 'ignore')
    except Exception:
        return []
    out: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
            if 'portfolioValue' in d and 'portfolio_value' not in d:
                d['portfolio_value'] = d['portfolioValue']
            out.append(d)
        except Exception:
            continue
    return out

def write_jsonl(bucket: str, key: str, rows: list[dict]) -> None:
    body = '\n'.join(json.dumps({
        'timestamp': _iso(_dt(r['timestamp'])),
        'portfolio_value': float(r['portfolio_value'])
    }, separators=(',', ':')) for r in rows) + '\n'
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode('utf-8'), ContentType='application/json', CacheControl='no-store, max-age=0')

def merge_pulse_with_admin(pulse: list[dict], admin: list[dict]) -> list[dict]:
    today = datetime.now(timezone.utc).date()
    # Keep Pulse strictly before today
    kept: list[dict] = []
    for p in pulse:
        try:
            d = _dt(p['timestamp']).date()
            if d < today:
                kept.append({'timestamp': _iso(_dt(p['timestamp'])), 'portfolio_value': float(p.get('portfolio_value', p.get('portfolioValue', 0.0)))})
        except Exception:
            continue

    # Admin points for today
    admin_today: list[dict] = []
    for a in admin:
        try:
            d = _dt(a['timestamp']).date()
            if d == today:
                admin_today.append({'timestamp': _iso(_dt(a['timestamp'])), 'portfolio_value': float(a.get('portfolio_value', a.get('portfolioValue', 0.0)))})
        except Exception:
            continue
    if not admin_today:
        return sorted(kept, key=lambda r: r['timestamp'])

    # Rebase admin intraday to Pulse opening PV (yesterday EOD)
    # Find yesterday EOD from kept, else last available in pulse
    yday = today - timedelta(days=1)
    pulse_before = [p for p in pulse if _dt(p['timestamp']).date() <= yday]
    if pulse_before:
        pulse_open = float(sorted(pulse_before, key=lambda r: _dt(r['timestamp']))[-1].get('portfolio_value', 1_000_000.0))
    elif kept:
        pulse_open = float(sorted(kept, key=lambda r: r['timestamp'])[-1]['portfolio_value'])
    else:
        pulse_open = 1_000_000.0

    admin_base = float(admin_today[0]['portfolio_value']) or 1.0
    rebased: list[dict] = []
    for a in admin_today:
        rel = (float(a['portfolio_value']) / admin_base) if admin_base else 1.0
        rebased.append({'timestamp': a['timestamp'], 'portfolio_value': pulse_open * rel})

    merged = kept + rebased
    # Deduplicate by timestamp
    seen = set()
    out: list[dict] = []
    for r in sorted(merged, key=lambda x: x['timestamp']):
        if r['timestamp'] in seen:
            continue
        seen.add(r['timestamp'])
        out.append(r)
    return out

def lambda_handler(event, _):
    pulse = read_jsonl(BUCKET, PULSE_KEY)
    admin = read_jsonl(BUCKET, ADMIN_KEY)
    merged = merge_pulse_with_admin(pulse, admin)
    write_jsonl(BUCKET, PULSE_KEY, merged)
    return {
        'statusCode': 200,
        'body': json.dumps({'ok': True, 'pulse_points': len(pulse), 'admin_points': len(admin), 'merged': len(merged)})
    }



