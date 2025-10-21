#!/usr/bin/env python3
"""
Lambda: pulse-access-report (and logger)

Endpoints (Function URL, auth: NONE):
- POST { action: 'redeem', t, email, name, entity } -> marks token redeemed
- POST { action: 'ping', email } -> upserts lastSeenAt for the latest token & users aggregator
- GET  /?format=json|csv[&days=14] -> returns last N days issued/redeemed tokens
- GET  /?scope=users[&format=json|csv] -> returns persistent users aggregator (all time)

Env:
  TABLE=pulse_tokens
  USERS_TABLE=pulse_users
"""
import os, json, csv, io
from datetime import datetime, timezone, timedelta
import boto3

TABLE = os.environ.get('TABLE', 'pulse_tokens')
USERS_TABLE = os.environ.get('USERS_TABLE', 'pulse_users')
ddb = boto3.client('dynamodb')

def _resp(status, body, headers=None):
    # Do not set Access-Control-Allow-Origin here; Function URL CORS will inject it.
    h = { 'Content-Type': 'application/json' }
    if headers: h.update(headers)
    return { 'statusCode': status, 'headers': h, 'body': json.dumps(body) }

def _now():
    return datetime.now(timezone.utc)

def _unmarshall(item):
    out = {}
    for k,v in (item or {}).items():
        if 'S' in v: out[k] = v['S']
        elif 'N' in v: out[k] = float(v['N']) if '.' in v['N'] else int(v['N'])
        else: out[k] = list(v.values())[0]
    return out

def mark_redeemed(t: str, email: str, name: str, entity: str):
    ddb.update_item(
        TableName=TABLE,
        Key={'token': {'S': t}},
        UpdateExpression='SET #s=:s, redeemedAt=:r, email=:e, name=:n, entity=:y',
        ExpressionAttributeNames={'#s':'status'},
        ExpressionAttributeValues={
            ':s': {'S':'redeemed'},
            ':r': {'S': _now().isoformat()},
            ':e': {'S': email or ''},
            ':n': {'S': name or ''},
            ':y': {'S': entity or ''},
        },
    )
    # Update persistent users aggregator
    try:
        ddb.update_item(
            TableName=USERS_TABLE,
            Key={'email': {'S': (email or '').lower()}},
            UpdateExpression='ADD redeemedCount :one SET lastSeenAt=:t, #n=if_not_exists(#n,:n), entity=if_not_exists(entity,:y), firstIssued=if_not_exists(firstIssued,:t)',
            ExpressionAttributeNames={'#n':'name'},
            ExpressionAttributeValues={
                ':one': {'N':'1'},
                ':t': {'S': _now().isoformat()},
                ':n': {'S': name or ''},
                ':y': {'S': entity or ''}
            }
        )
    except Exception:
        pass

def mark_ping(email: str, source_ip: str = '', ua: str = ''):
    # Find the most recent token for this email and update lastSeenAt
    scan = ddb.scan(
        TableName=TABLE,
        FilterExpression='email = :e',
        ExpressionAttributeValues={':e': {'S': email}}
    )
    items = [_unmarshall(i) for i in scan.get('Items', [])]
    if not items:
        return
    items.sort(key=lambda x: x.get('issuedAt',''), reverse=True)
    tok = items[0].get('token')
    if not tok:
        return
    ddb.update_item(
        TableName=TABLE,
        Key={'token': {'S': tok}},
        UpdateExpression='SET lastSeenAt=:t, lastIp=:ip, ua=:ua',
        ExpressionAttributeValues={
            ':t': {'S': _now().isoformat()},
            ':ip': {'S': source_ip or ''},
            ':ua': {'S': ua or ''}
        }
    )
    # Mirror to persistent users aggregator
    try:
        ddb.update_item(
            TableName=USERS_TABLE,
            Key={'email': {'S': (email or '').lower()}},
            UpdateExpression='SET lastSeenAt=:t ADD issuedCount :zero, redeemedCount :zero',
            ExpressionAttributeValues={
                ':t': {'S': _now().isoformat()},
                ':zero': {'N': '0'}
            }
        )
    except Exception:
        pass

def list_recent(days=7):
    """Return recent token items for the last N days (paginated scan)."""
    cutoff_iso = (_now() - timedelta(days=days)).isoformat()
    items = []
    eks = None
    while True:
        params = {
            'TableName': TABLE,
            'ConsistentRead': False,
            'FilterExpression': 'attribute_exists(issuedAt) AND issuedAt >= :cut',
            'ExpressionAttributeValues': { ':cut': { 'S': cutoff_iso } }
        }
        if eks:
            params['ExclusiveStartKey'] = eks
        resp = ddb.scan(**params)
        items.extend(_unmarshall(i) for i in resp.get('Items', []))
        eks = resp.get('LastEvaluatedKey')
        if not eks:
            break
    items.sort(key=lambda x: x.get('issuedAt',''), reverse=True)
    return items

def list_users():
    """Return all persistent users (aggregated by email)."""
    items = []
    eks = None
    while True:
        params = { 'TableName': USERS_TABLE, 'ConsistentRead': False }
        if eks:
            params['ExclusiveStartKey'] = eks
        resp = ddb.scan(**params)
        items.extend(_unmarshall(i) for i in resp.get('Items', []))
        eks = resp.get('LastEvaluatedKey')
        if not eks:
            break
    # Normalize numeric fields
    for it in items:
        it['issuedCount'] = int(it.get('issuedCount', 0) or 0)
        it['redeemedCount'] = int(it.get('redeemedCount', 0) or 0)
        it['email'] = (it.get('email') or '').lower()
    items.sort(key=lambda x: (x.get('lastSeenAt','') or x.get('lastIssued','') or ''), reverse=True)
    return items

def to_csv(rows):
    buf = io.StringIO()
    fields = ['token','email','name','entity','status','issuedAt','expiresAt','redeemedAt','next']
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k,'') for k in fields})
    return buf.getvalue()

def lambda_handler(event, _):
    method = event.get('requestContext',{}).get('http',{}).get('method','GET')
    if method == 'POST':
        try:
            body = json.loads(event.get('body') or '{}')
            if body.get('action') == 'redeem' and body.get('t'):
                mark_redeemed(body['t'], body.get('email',''), body.get('name',''), body.get('entity',''))
                return _resp(200, {'ok': True})
            if body.get('action') == 'ping' and body.get('email'):
                httpc = event.get('requestContext',{}).get('http',{})
                mark_ping(body['email'], httpc.get('sourceIp',''), httpc.get('userAgent',''))
                return _resp(200, {'ok': True})
        except Exception as _e:
            return _resp(400, {'error':'bad request'})
        return _resp(400, {'error':'invalid action'})
    # GET report
    fmt = 'json'
    scope = 'tokens'
    days = 14
    try:
        q = event.get('rawQueryString') or ''
        if 'format=csv' in q: fmt = 'csv'
        if 'scope=users' in q: scope = 'users'
        if 'days=' in q:
            try:
                part = q.split('days=')[1].split('&')[0]
                if part == 'all':
                    days = 36500
                else:
                    days = max(1, int(part))
            except Exception:
                days = 14
    except Exception:
        pass

    if scope == 'users':
        rows = list_users()
        if fmt == 'csv':
            # CSV for users aggregator
            buf = io.StringIO()
            fields = ['email','name','entity','issuedCount','redeemedCount','firstIssued','lastIssued','lastSeenAt']
            w = csv.DictWriter(buf, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k,'') for k in fields})
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/csv; charset=utf-8',
                    'Content-Disposition': 'attachment; filename="pulse_users.csv"'
                },
                'body': buf.getvalue()
            }
        return _resp(200, {'items': rows})

    rows = list_recent(days=days)
    if fmt == 'csv':
        csv_body = to_csv(rows)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/csv; charset=utf-8',
                'Content-Disposition': 'attachment; filename="pulse_access.csv"'
            },
            'body': csv_body
        }
    return _resp(200, {'items': rows})


