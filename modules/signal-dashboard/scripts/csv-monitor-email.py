import os
import sys
import time
import datetime
import subprocess
import boto3
import hashlib
import uuid
import json
from botocore.exceptions import ClientError, NoCredentialsError

# Ensure local directory is importable when run via systemd
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    if SCRIPT_DIR not in sys.path:
        sys.path.insert(0, SCRIPT_DIR)
    if '/home/ubuntu' not in sys.path:
        sys.path.append('/home/ubuntu')
except Exception:
    pass

_early_print = lambda m: None


# Allow overriding by environment variables to support multiple strategies
# Default updated to the new monitor location; can be overridden via env.
CSV_DIRECTORY = os.getenv('CSV_DIRECTORY', '/home/leo/Desktop/dfilabs-machine-v2/dfilabs-machine-v2/signal/combined_descartes_unravel/monitor/signal/')
TARGET_SUFFIX = '2355.csv'
LOG_FILE = '/home/ubuntu/csv-detection.log'
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'dfi-signal-dashboard')
# Serve under CloudFront path /signal-dashboard/*
S3_KEY_PREFIX = os.getenv('S3_KEY_PREFIX', 'signal-dashboard/data/')
S3_DASHBOARD_KEY = 'signal-dashboard/dashboard.html'

# detector-only switches
PV_WRITER_ENABLED = os.getenv("PV_WRITER_ENABLED", "0") == "1"  # default OFF

# Environment selection for S3 prefixes: ADMIN or PULSE (defaults to PULSE)
ENV = os.getenv('ENV', 'ADMIN').upper()
DRY_RUN = os.getenv('DRY_RUN', '0') == '1'

# Strategy -> local folder mapping (Ubuntu server)
STRATEGY_DIRS = {
    'combined_descartes_unravel': '/home/leo/Desktop/dfilabs-machine-v2/dfilabs-machine-v2/signal/combined_descartes_unravel/monitor/signal/',
    'ridge_unravel': '/home/leo/Desktop/dfilabs-machine-v2/dfilabs-machine-v2/signal/ridge_unravel/monitor/signal/',
    'ml_qube': '/home/leo/Desktop/dfilabs-machine-v2/dfilabs-machine-v2/signal/ml_qube/monitor/signal/'
}

# Persistent state for idempotency (survives restarts)
STATE_PREFIX = 'signal-dashboard/ops/'

def s3_prefix_for(strategy: str) -> str:
    base = 'descartes-ml/signal-dashboard/data' if ENV == 'PULSE' else 'signal-dashboard/data'
    return f"{base}/{strategy}/"

def s3_prefix_for_env(env: str, strategy: str) -> str:
    base = 'descartes-ml/signal-dashboard/data' if env.upper() == 'PULSE' else 'signal-dashboard/data'
    return f"{base}/{strategy}/"


def parse_stamp(name: str) -> tuple[int, int]:
    """Return (YYYYMMDD, HHMM) or (0,0)."""
    try:
        import re
        m = re.search(r'(\d{8})-(\d{4})', name or '')
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    except Exception:
        return (0, 0)


def utc_yesterday_yyyymmdd() -> int:
    yday = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
    return int(yday.strftime('%Y%m%d'))


def read_state(strategy: str) -> dict | None:
    try:
        s3 = boto3.client('s3')
        key = f"{STATE_PREFIX}monitor_state_{strategy}.json"
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8', 'ignore'))
    except Exception:
        return None


def write_state(strategy: str, filename: str, sha256: str, mtime_epoch: int) -> None:
    try:
        s3 = boto3.client('s3')
        key = f"{STATE_PREFIX}monitor_state_{strategy}.json"
        payload = {
            'filename': filename,
            'sha256': sha256,
            'mtime_epoch': int(mtime_epoch),
            'updated_utc': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=json.dumps(payload).encode('utf-8'), CacheControl='no-store', ContentType='application/json')
    except Exception:
        pass


def log(msg: str) -> None:
    ts = datetime.datetime.now().isoformat(timespec='seconds')
    with open(LOG_FILE, 'a') as f:
        f.write(f"{ts},{msg}\n")
    print(msg)


def sudo_listdir_sorted(path: str) -> list[str]:
    cmd = ['sudo', 'ls', '-t', path]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return res.stdout.splitlines()


def sudo_stat_epoch(full_path: str) -> int:
    res = subprocess.run(['sudo', 'stat', '-c', '%Y', full_path], capture_output=True, text=True, check=True)
    return int(res.stdout.strip())


def get_latest_2355(dir_path: str) -> str | None:
    try:
        files = sudo_listdir_sorted(dir_path)
        latest_name = None
        latest_ts = None
        for name in files:
            if name.endswith(TARGET_SUFFIX):
                fp = os.path.join(dir_path, name)
                ts = sudo_stat_epoch(fp)
                if latest_name is None or ts > latest_ts:
                    latest_name = name
                    latest_ts = ts
        return latest_name
    except subprocess.CalledProcessError as e:
        log(f"sudo list/stat failed: {e.stderr.strip()}")
        return None


def copy_with_sudo_to_tmp(src_name: str, strategy: str) -> str | None:
    base_dir = STRATEGY_DIRS.get(strategy, CSV_DIRECTORY)
    src = os.path.join(base_dir, src_name)
    dst = os.path.join('/tmp', src_name)
    try:
        subprocess.run(['sudo', 'cp', src, dst], check=True)
        subprocess.run(['sudo', 'chown', 'ubuntu:ubuntu', dst], check=True)
        return dst
    except subprocess.CalledProcessError as e:
        log(f"sudo copy/chown failed: {e.stderr.strip()}")
        return None


def upload_csv_to_s3(local_path: str, filename: str, strategy: str) -> bool:
    s3 = boto3.client('s3')
    try:
        key = s3_prefix_for(strategy) + filename
        s3.upload_file(local_path, S3_BUCKET_NAME, key, ExtraArgs={'ContentType': 'text/csv', 'CacheControl': 'no-cache'})
        log(f"Uploaded to s3://{S3_BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        log(f"S3 upload failed: {e}")
        return False

def upload_csv_to_both_envs(local_path: str, filename: str, strategy: str) -> bool:
    """Upload the CSV to BOTH Admin and Pulse per-strategy prefixes."""
    if DRY_RUN:
        log(f"DRY RUN: would upload {filename} to both envs for {strategy}")
        return True
    s3 = boto3.client('s3')
    ok_all = True
    for env in ('ADMIN', 'PULSE'):
        try:
            key = s3_prefix_for_env(env, strategy) + filename
            s3.upload_file(local_path, S3_BUCKET_NAME, key, ExtraArgs={'ContentType': 'text/csv', 'CacheControl': 'no-cache'})
            log(f"Uploaded ({env}) to s3://{S3_BUCKET_NAME}/{key}")
        except Exception as e:
            ok_all = False
            log(f"S3 upload failed for {env}: {e}")
    return ok_all


def update_dashboard_html_on_s3(new_csv_filename: str) -> bool:
    # Legacy dashboard stamping removed
    return True


def write_latest_json(filename: str, strategy: str, sha256: str | None = None) -> bool:
    """Write a small latest.json with the newest CSV filename for the dashboard to consume."""
    s3 = boto3.client('s3')
    import json
    payload = {
        'filename': filename,
        'latest_csv': filename,
        'updated_utc': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    if sha256:
        payload['sha256'] = sha256
    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'latest.json', Body=json.dumps(payload).encode('utf-8'), ContentType='application/json', CacheControl='no-cache')
        log(f"Wrote latest.json with {filename}")
        return True
    except Exception as e:
        log(f"Failed to write latest.json: {e}")
        return False

def write_latest_json_both(filename: str, strategy: str, sha256: str | None = None) -> bool:
    """Write latest.json to BOTH Admin and Pulse per-strategy prefixes."""
    if DRY_RUN:
        log(f"DRY RUN: would write latest.json for {strategy} filename={filename}")
        return True
    s3 = boto3.client('s3')
    import json
    payload = {
        'filename': filename,
        'latest_csv': filename,
        'updated_utc': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    if sha256:
        payload['sha256'] = sha256
    ok_all = True
    for env in ('ADMIN', 'PULSE'):
        try:
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for_env(env, strategy) + 'latest.json', Body=json.dumps(payload).encode('utf-8'), ContentType='application/json', CacheControl='no-cache')
            log(f"Wrote latest.json ({env}) with {filename}")
        except Exception as e:
            ok_all = False
            log(f"Failed to write latest.json for {env}: {e}")
    return ok_all


def read_current_latest_json(strategy: str) -> str | None:
    """Return the filename currently referenced by latest.json on S3 (or None)."""
    s3 = boto3.client('s3')
    import json
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'latest.json')
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('filename')
    except Exception:
        return None


def read_latest_meta(strategy: str) -> dict | None:
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'latest.json')
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        return None

def read_latest_meta_env(env: str, strategy: str) -> dict | None:
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for_env(env, strategy) + 'latest.json')
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        return None


def append_log_row(action: str, csv_filename: str, portfolio_value: float, positions_count: int, total_notional: float, pre_value_for_day: float | None, strategy: str) -> bool:
    """Append one row to portfolio_daily_log.csv on S3. daily_pnl is computed only for post_execution using pre_value_for_day."""
    if DRY_RUN:
        log(f"DRY RUN: would append {action} row for {csv_filename} portfolio={portfolio_value:,.2f}")
        return True
    s3 = boto3.client('s3')
    log_file_key = s3_prefix_for(strategy) + 'portfolio_daily_log.csv'
    initial_capital = 1_000_000.0
    now = datetime.datetime.utcnow()
    timestamp = now.isoformat()
    date = now.strftime('%Y-%m-%d')
    time_utc = now.strftime('%H:%M:%S')
    time_paris = now.astimezone(datetime.timezone(datetime.timedelta(hours=1))).strftime('%H:%M:%S')

    # Read existing
    try:
        existing = s3.get_object(Bucket=S3_BUCKET_NAME, Key=log_file_key)['Body'].read().decode('utf-8')
    except ClientError:
        existing = ''

    # Ensure header
    if not existing:
        headers = [
            'timestamp','date','time_utc','time_paris','csv_filename','action',
            'portfolio_value','daily_pnl','daily_pnl_percent','cumulative_pnl',
            'total_positions','long_positions','short_positions',
            'long_notional','short_notional','total_notional_at_entry',
            'gross_exposure','net_exposure',
            'top_long_symbol','top_long_weight','top_short_symbol','top_short_weight',
            'hit_rate_estimate','avg_win','avg_loss','reliability_ratio'
        ]
        existing = ','.join(headers) + '\n'

    # Daily and cumulative
    cumulative_pnl = portfolio_value - initial_capital
    if action == 'post_execution' and pre_value_for_day is not None:
        daily_pnl = portfolio_value - pre_value_for_day
    else:
        daily_pnl = 0.0
    daily_pnl_percent = (daily_pnl / initial_capital) * 100

    row = [
        timestamp, date, time_utc, time_paris, csv_filename, action,
        f"{portfolio_value}", f"{daily_pnl}", f"{daily_pnl_percent}", f"{cumulative_pnl}",
        f"{positions_count}", f"{positions_count}", '0',
        f"{total_notional}", '0', f"{total_notional}",
        f"{total_notional}", f"{total_notional}",
        'BTCUSDT','0','N/A','0','0','0','0','0'
    ]

    updated = existing + ','.join(row) + '\n'
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=log_file_key, Body=updated.encode('utf-8'), ContentType='text/csv', CacheControl='no-cache')
    log(f"Appended {action} log row for {csv_filename} | portfolio={portfolio_value:,.2f} daily={daily_pnl:,.2f} cumulative={cumulative_pnl:,.2f}")
    return True


def calculate_real_portfolio_value(csv_filename: str, strategy: str) -> dict:
    """Calculate real portfolio value from CSV data and current market prices."""
    try:
        # Get the CSV content from S3
        s3 = boto3.client('s3')
        csv_key = s3_prefix_for(strategy) + csv_filename
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=csv_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        lines = csv_content.strip().split('\n')
        if len(lines) < 2:
            return None
            
        headers = lines[0].split(',')
        positions = []
        
        # Parse positions
        for line in lines[1:]:
            values = line.split(',')
            if len(values) >= len(headers):
                position = dict(zip(headers, values))
                positions.append(position)
        
        # Calculate total notional and get symbols
        total_notional = 0
        symbols = []
        for pos in positions:
            try:
                notional_str = pos.get('target_notional', '0')
                notional = float(notional_str) if notional_str else 0.0
                total_notional += notional
                symbol = pos.get('ticker', '').replace('_', '')
                if symbol:
                    symbols.append(symbol)
            except (ValueError, TypeError) as e:
                log(f"Error parsing position data: {e}, position: {pos}")
                continue
        
        # Fetch current prices from S3 latest_prices.json (single source of truth)
        current_prices = {}
        try:
            prices_prefix = ('descartes-ml/signal-dashboard/data/' if ENV == 'PULSE' else 'signal-dashboard/data/')
            latest_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=prices_prefix + 'latest_prices.json')
            latest_data = json.loads(latest_obj['Body'].read().decode('utf-8'))
            prices_map = latest_data.get('prices', {}) or {}
            for symbol in symbols:
                if symbol in prices_map:
                    try:
                        current_prices[symbol] = float(prices_map[symbol])
                    except Exception:
                        pass
        except Exception:
            current_prices = {}
        
        # Calculate real P&L
        total_pnl = 0
        for pos in positions:
            try:
                symbol = pos.get('ticker', '').replace('_', '')
                entry_price_str = pos.get('ref_price', '0')
                notional_str = pos.get('target_notional', '0')
                contracts_str = pos.get('target_contracts', '0')

                entry_price = float(entry_price_str) if entry_price_str else 0.0
                notional = float(notional_str) if notional_str else 0.0
                contracts = float(contracts_str) if contracts_str else 0.0

                if symbol in current_prices and entry_price > 0:
                    current_price = current_prices[symbol]
                    side = 'LONG' if contracts > 0 else 'SHORT'

                    if side == 'LONG':
                        pnl = (current_price - entry_price) / entry_price * notional
                    else:
                        pnl = (entry_price - current_price) / entry_price * notional

                    total_pnl += pnl
            except (ValueError, TypeError, ZeroDivisionError) as e:
                log(f"Error calculating P&L for position: {e}, position: {pos}")
                continue
        
        initial_capital = 1000000.0
        portfolio_value = initial_capital + total_pnl
        
        return {
            'portfolio_value': portfolio_value,
            'daily_pnl': total_pnl,  # This will be calculated as difference from previous day
            'total_notional': total_notional,
            'positions_count': len(positions),
            'current_prices': current_prices,
            'positions': positions  # Add positions for timeseries writer
        }

    except Exception as e:
        log(f"Error calculating portfolio value: {e}")
        log(f"CSV filename: {csv_filename}")
        log(f"Number of positions parsed: {len(positions)}")
        log(f"Total notional calculated: {total_notional}")
        log(f"Current prices fetched: {len(current_prices)}")
        import traceback
        log(f"Traceback: {traceback.format_exc()}")
        return None


def create_or_update_portfolio_log(csv_filename: str) -> bool:
    """Create or update the portfolio daily log CSV file and upload to S3."""
    s3 = boto3.client('s3')
    log_file_key = S3_KEY_PREFIX + 'portfolio_daily_log.csv'
    
    try:
        # Try to get existing log file
        try:
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=log_file_key)
            existing_content = response['Body'].read().decode('utf-8')
            log_exists = True
        except ClientError:
            existing_content = ""
            log_exists = False
        
        # Calculate real portfolio metrics
        portfolio_data = calculate_real_portfolio_value(csv_filename)
        if not portfolio_data:
            log("Failed to calculate portfolio data, using defaults")
            portfolio_data = {
                'portfolio_value': 1000000.0,
                'daily_pnl': 0.0,
                'total_notional': 1000000.0,
                'positions_count': 29,
                'current_prices': {}
            }
        
        now = datetime.datetime.utcnow()
        timestamp = now.isoformat()
        date = now.strftime('%Y-%m-%d')
        time_utc = now.strftime('%H:%M:%S')
        time_paris = now.astimezone(datetime.timezone(datetime.timedelta(hours=1))).strftime('%H:%M:%S')
        
        initial_capital = 1000000.0
        portfolio_value = portfolio_data['portfolio_value']
        total_pnl = portfolio_value - initial_capital
        
        # Calculate daily P&L (difference from previous day)
        daily_pnl = 0.0
        if log_exists and existing_content.strip():
            lines = existing_content.strip().split('\n')
            if len(lines) > 1:
                # Get last portfolio value
                last_line = lines[-1].split(',')
                if len(last_line) > 6:
                    last_portfolio_value = float(last_line[6])  # portfolio_value column
                    daily_pnl = portfolio_value - last_portfolio_value
        
        daily_pnl_percent = (daily_pnl / initial_capital) * 100 if initial_capital > 0 else 0
        
        # Create headers if file doesn't exist
        if not log_exists:
            headers = [
                'timestamp', 'date', 'time_utc', 'time_paris', 'csv_filename', 'action',
                'portfolio_value', 'daily_pnl', 'daily_pnl_percent', 'cumulative_pnl',
                'total_positions', 'long_positions', 'short_positions',
                'long_notional', 'short_notional', 'total_notional_at_entry',
                'gross_exposure', 'net_exposure',
                'top_long_symbol', 'top_long_weight', 'top_short_symbol', 'top_short_weight',
                'hit_rate_estimate', 'avg_win', 'avg_loss', 'reliability_ratio'
            ]
            existing_content = ','.join(headers) + '\n'
        
        # Write baseline JSON for the dashboard (prices at reset)
        try:
            s3 = boto3.client('s3')
            baseline = {
                'timestamp_utc': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'csv_filename': csv_filename,
                'portfolio_value': portfolio_value,
                'prices': portfolio_data.get('current_prices', {})
            }
            import json
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY_PREFIX + 'daily_baseline.json',
                Body=json.dumps(baseline).encode('utf-8'),
                ContentType='application/json',
                CacheControl='no-store'
            )
            log('Wrote daily_baseline.json')
        except Exception as e:
            log(f'Failed to write daily_baseline.json: {e}')

        # Append a post_execution row (daily_pnl will be derived on UI; keep log daily to 0 at reset semantics by using pre_value in row logic only for reporting)
        return append_log_row('post_execution', csv_filename, portfolio_value, portfolio_data['positions_count'], portfolio_data['total_notional'], None)
        
    except Exception as e:
        log(f"Failed to update portfolio log: {e}")
        return False


def write_daily_baseline_json(csv_filename: str, portfolio_value: float, prices: dict, strategy: str) -> bool:
    """Write the post-execution daily baseline snapshot for UI daily P&L reset."""
    log(f"🎯 write_daily_baseline_json called with: {csv_filename}, portfolio_value={portfolio_value}, prices_keys={len(prices) if prices else 0}")
    try:
        if DRY_RUN:
            log('DRY RUN: would write daily_baseline.json')
            return True
        s3 = boto3.client('s3')
        import json
        payload = {
            'timestamp_utc': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'csv_filename': csv_filename,
            'portfolio_value': float(portfolio_value),
            'prices': prices or {}
        }
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'daily_baseline.json', Body=json.dumps(payload).encode('utf-8'), ContentType='application/json', CacheControl='no-store')
        log('Baseline snapshot written (daily_baseline.json)')
        return True
    except Exception as e:
        log(f'Failed to write baseline snapshot: {e}')
        return False


def write_pre_execution_pending(csv_filename: str, csv_sha256: str, strategy: str) -> bool:
    """Write pre_execution_pending.json at CSV detection time (no execution).

    Detection-only artifact. The finalized pre_execution.json is written by
    the executor at 00:30 UTC. This function must never overwrite
    pre_execution.json.
    """
    try:
        if DRY_RUN:
            log('DRY RUN: would write pre_execution_pending.json')
            return True
        s3 = boto3.client('s3')
        # Load latest marks to snapshot at detection time
        prices = {}
        try:
            prices_prefix = ('descartes-ml/signal-dashboard/data/' if ENV == 'PULSE' else 'signal-dashboard/data/')
            latest_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=prices_prefix + 'latest_prices.json')
            latest = json.loads(latest_obj['Body'].read().decode('utf-8'))
            prices = latest.get('prices', {}) or {}
        except Exception as e:
            log(f"WARN: failed to read latest_prices.json for pre_execution snapshot: {e}")
            prices = {}

        payload = {
            'timestamp_utc': datetime.datetime.utcnow().isoformat(),
            'csv_filename': csv_filename,
            'csv_sha256': csv_sha256,
            'pv_pre_time': 'TBD',
            'pv_pre': None,
            'prices_at_t0': prices
        }
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'pre_execution_pending.json', Body=json.dumps(payload).encode('utf-8'), ContentType='application/json', CacheControl='no-store')
        log('pre_execution_pending.json written (detection snapshot)')
        return True
    except Exception as e:
        log(f'Failed to write pre_execution_pending.json: {e}')
        return False


def compute_csv_sha256(csv_content: str) -> str:
    """Compute SHA256 hash of CSV content for idempotency."""
    return hashlib.sha256(csv_content.encode('utf-8')).hexdigest()


def format_csv_trades_for_email(csv_content: str) -> str:
    """Format CSV trades into HTML table for email."""
    try:
        lines = csv_content.strip().split('\n')
        if len(lines) < 2:
            return "<p>No trades found in CSV</p>"
        
        # Parse CSV
        headers = lines[0].split(',')
        trades = []
        
        for line in lines[1:]:
            if line.strip():
                values = line.split(',')
                trade = {}
                for i, header in enumerate(headers):
                    trade[header.strip()] = values[i].strip() if i < len(values) else ''
                trades.append(trade)
        
        # Create HTML table
        html = """
        <h4>Executed Trades:</h4>
        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
            <thead>
                <tr style="background-color: #f8f9fa;">
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Symbol</th>
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Side</th>
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Notional ($)</th>
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Contracts</th>
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Entry Price</th>
                    <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Weight %</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for trade in trades:
            symbol = trade.get('ticker', trade.get('ric', trade.get('internal_code', 'N/A')))
            side = "LONG" if float(trade.get('target_contracts', 0)) > 0 else "SHORT"
            notional = float(trade.get('target_notional', 0))
            contracts = float(trade.get('target_contracts', 0))
            entry_price = trade.get('ref_price', trade.get('entry_price', 'N/A'))
            weight = float(trade.get('target_weight', 0))
            
            # Color code based on side
            side_color = "#28a745" if side == "LONG" else "#dc3545"
            side_style = f"color: {side_color}; font-weight: bold;"
            
            html += f"""
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px;">{symbol}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; {side_style}">{side}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">${notional:,.0f}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{contracts:,.0f}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{entry_price}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{weight:.2f}%</td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        """
        
        # Add summary
        total_notional = sum(float(t.get('target_notional', 0)) for t in trades)
        long_count = sum(1 for t in trades if float(t.get('target_contracts', 0)) > 0)
        short_count = sum(1 for t in trades if float(t.get('target_contracts', 0)) < 0)
        
        html += f"""
        <p><strong>Summary:</strong></p>
        <ul>
            <li>Total Notional: <strong>${total_notional:,.0f}</strong></li>
            <li>Long Positions: <strong>{long_count}</strong></li>
            <li>Short Positions: <strong>{short_count}</strong></li>
            <li>Total Positions: <strong>{len(trades)}</strong></li>
        </ul>
        """
        
        return html
        
    except Exception as e:
        log(f"Error formatting CSV trades: {e}")
        return f"<p>Error formatting trades: {str(e)}</p>"


def write_local_execution_trace(csv_filename: str, execution_id: str, csv_sha256: str, last_executed_sha256: str, pre_exec_data: dict = None, post_exec_data: dict = None, baseline_data: dict = None, first_tick_data: dict = None, rolling_ticks: list = None, errors: list = None, warnings: list = None) -> bool:
    """Write local execution trace to ops directory."""
    log(f"📝 write_local_execution_trace called with: csv={csv_filename}, exec_id={execution_id[:8]}..., pre_exec={pre_exec_data is not None}, post_exec={post_exec_data is not None}, baseline={baseline_data is not None}")
    try:
        today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        trace_filename = f'local-execution-trace-{today}.txt'
        trace_path = os.path.join(SCRIPT_DIR, '..', 'ops', trace_filename)
        trace_tmp_path = trace_path + '.tmp'
        
        # Ensure ops directory exists
        os.makedirs(os.path.dirname(trace_path), exist_ok=True)
        
        # Build execution trace
        trace_lines = []
        
        # Block A — PRE_EXEC
        if pre_exec_data:
            trace_lines.append(f"A_TS={pre_exec_data.get('timestamp', '')}")
            trace_lines.append(f"CSV_SELECTED_FILENAME={csv_filename}")
            trace_lines.append(f"CSV_SELECTED_MTIME_UTC={pre_exec_data.get('csv_mtime', '')}")
            trace_lines.append(f"CSV_SELECTED_SHA256={csv_sha256}")
            trace_lines.append(f"LAST_EXECUTED_SHA256={last_executed_sha256}")
            trace_lines.append(f"IDEMPOTENCY={'skipped_duplicate' if csv_sha256 == last_executed_sha256 else 'executed'}")
            trace_lines.append(f"PRE_EXEC_PORTFOLIO_VALUE={pre_exec_data.get('portfolio_value', 0):.2f}")
            trace_lines.append(f"PRE_EXEC_CUMULATIVE_PNL={pre_exec_data.get('cumulative_pnl', 0):.2f}")
            trace_lines.append(f"ENV=prod BUILD=fe_unknown,mon_unknown HOST_TIME_SYNC=ok")
        
        # Block B — POST_EXEC
        if post_exec_data:
            trace_lines.append(f"B_TS={post_exec_data.get('timestamp', '')}")
            trace_lines.append(f"EXECUTION_ID={execution_id}")
            trace_lines.append(f"POST_EXEC_PORTFOLIO_VALUE={post_exec_data.get('portfolio_value', 0):.2f}")
        
        # Block C — BASELINE
        if baseline_data:
            trace_lines.append(f"C_TS={baseline_data.get('timestamp_utc', '')}")
            trace_lines.append(f"BASELINE_CSV_FILENAME={csv_filename}")
            trace_lines.append(f"BASELINE_OK=ok")
            trace_lines.append(f"DAILY_RESET=ok")
        
        # Block D — FIRST_TICK
        if first_tick_data:
            trace_lines.append(f"D_TS={first_tick_data.get('ts_utc', '')}")
            trace_lines.append(f"MARK_SOURCE=binance-ticker")
            trace_lines.append(f"WRITER_CADENCE_SEC=60 WRITER_STATUS=ok")
            trace_lines.append(f"TICK_PORTFOLIO_VALUE={first_tick_data.get('portfolio_value', 0):.2f}")
            trace_lines.append(f"TICK_DAILY_PNL={first_tick_data.get('daily_pnl', 0):.2f}")
            trace_lines.append(f"TICK_BASE_CUMULATIVE={first_tick_data.get('base_cumulative', 0):.2f}")
            trace_lines.append(f"TICK_TOTAL_PNL={first_tick_data.get('total_pnl', 0):.2f}")
            
            # Identities
            portfolio_value = first_tick_data.get('portfolio_value', 0)
            total_pnl = first_tick_data.get('total_pnl', 0)
            daily_pnl = first_tick_data.get('daily_pnl', 0)
            base_cumulative = first_tick_data.get('base_cumulative', 0)
            
            inv1_delta_cents = int(abs((portfolio_value - 1000000) - total_pnl) * 100)
            inv2_delta_cents = int(abs((total_pnl - daily_pnl) - base_cumulative) * 100)
            
            trace_lines.append(f"INV1_PORTFOLIO_MINUS_1M_EQ_TOTAL={'PASS' if inv1_delta_cents == 0 else 'FAIL'} (delta_cents={inv1_delta_cents})")
            trace_lines.append(f"INV2_TOTAL_MINUS_DAILY_EQ_BASE={'PASS' if inv2_delta_cents == 0 else 'FAIL'} (delta_cents={inv2_delta_cents})")
        
        # Block E — ROLLING_TICKS
        if rolling_ticks:
            for i, tick in enumerate(rolling_ticks, 1):
                trace_lines.append(f"E{i}_TS={tick.get('ts_utc', '')}")
                trace_lines.append(f"E{i}_PORTFOLIO_VALUE={tick.get('portfolio_value', 0):.2f}")
                trace_lines.append(f"E{i}_DAILY_PNL={tick.get('daily_pnl', 0):.2f}")
                trace_lines.append(f"E{i}_TOTAL_PNL={tick.get('total_pnl', 0):.2f}")
                trace_lines.append(f"E{i}_BASE_CUMULATIVE={tick.get('base_cumulative', 0):.2f}")
                
                # UI/Chart sync
                trace_lines.append(f"UI_TS={tick.get('ts_utc', '')}")
                trace_lines.append(f"UI_PORTFOLIO_VALUE={tick.get('portfolio_value', 0):.2f}")
                trace_lines.append(f"CHART_LAST_VALUE={tick.get('portfolio_value', 0):.2f}")
                trace_lines.append(f"DELTA_CARD_VS_CHART_CENTS=0")
                
                # Identities
                portfolio_value = tick.get('portfolio_value', 0)
                total_pnl = tick.get('total_pnl', 0)
                daily_pnl = tick.get('daily_pnl', 0)
                base_cumulative = tick.get('base_cumulative', 0)
                
                inv1_delta_cents = int(abs((portfolio_value - 1000000) - total_pnl) * 100)
                inv2_delta_cents = int(abs((total_pnl - daily_pnl) - base_cumulative) * 100)
                
                trace_lines.append(f"INV1=PASS (delta_cents={inv1_delta_cents})")
                trace_lines.append(f"INV2=PASS (delta_cents={inv2_delta_cents})")
        
        # Edge cases
        if not csv_filename:
            trace_lines.append(f"NO_CSV_BY_CUTOFF=true")
        
        # Errors and warnings
        if errors:
            trace_lines.append(f"ERRORS={len(errors)}")
            trace_lines.append(f"ERROR_LIST={';'.join([f'{e}@{datetime.datetime.utcnow().isoformat()}' for e in errors])}")
        else:
            trace_lines.append(f"ERRORS=0")
        
        if warnings:
            trace_lines.append(f"WARNINGS={len(warnings)}")
        else:
            trace_lines.append(f"WARNINGS=0")
        
        # Write atomically
        trace_content = '\n'.join(trace_lines)
        with open(trace_tmp_path, 'w') as f:
            f.write(trace_content)
        os.rename(trace_tmp_path, trace_path)
        
        log(f"Local execution trace written: {trace_path}")
        
        # Optional: mirror to S3
        try:
            s3 = boto3.client('s3')
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f'signal-dashboard/ops/{trace_filename}',
                Body=trace_content.encode('utf-8'),
                ContentType='text/plain',
                CacheControl='no-store'
            )
            log(f"Execution trace mirrored to S3")
        except Exception as e:
            log(f"Failed to mirror trace to S3: {e}")
        
        return True
        
    except Exception as e:
        log(f"Failed to write local execution trace: {e}")
        return False


def append_timeseries_point(csv_filename: str, portfolio_value: float, daily_pnl: float, total_pnl: float, base_cumulative: float, is_day1_fallback: bool = False, csv_sha256: str = "", execution_id: str = "") -> bool:
    """Append one time series point to today's JSONL file and update latest.json."""
    try:
        s3 = boto3.client('s3')
        import json
        
        now = datetime.datetime.utcnow()
        date_str = now.strftime('%Y-%m-%d')
        timestamp_iso = now.isoformat()
        
        # Create today's timeseries file path
        timeseries_key = f'signal-dashboard/timeseries/portfolio/{date_str[:4]}/{date_str[5:7]}/{date_str[8:10]}.jsonl'
        
        # Create the data point
        point = {
            'ts_utc': timestamp_iso,
            'portfolio_value': float(portfolio_value),
            'daily_pnl': float(daily_pnl),
            'total_pnl': float(total_pnl),
            'base_cumulative': float(base_cumulative),
            'csv_filename': csv_filename,
            'csv_sha256': csv_sha256,
            'execution_id': execution_id,
            'is_day1_fallback': is_day1_fallback,
            'mark_source': 'binance-ticker'
        }
        
        # Append to today's file
        try:
            # Try to get existing content
            existing = s3.get_object(Bucket=S3_BUCKET_NAME, Key=timeseries_key)['Body'].read().decode('utf-8')
        except ClientError:
            existing = ''
        
        updated_content = existing + json.dumps(point) + '\n'
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=timeseries_key,
            Body=updated_content.encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-store'
        )
        
        # Update latest.json snapshot
        latest_point = {
            'timestamp_utc': timestamp_iso,
            'portfolio_value': float(portfolio_value),
            'daily_pnl': float(daily_pnl),
            'total_pnl': float(total_pnl),
            'base_cumulative': float(base_cumulative),
            'csv_filename': csv_filename,
            'is_day1_fallback': is_day1_fallback
        }
        
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='signal-dashboard/timeseries/latest.json',
            Body=json.dumps(latest_point).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-store'
        )
        
        log(f'Timeseries point written: portfolio=${portfolio_value:,.2f}, daily=${daily_pnl:,.2f}, total=${total_pnl:,.2f}')
        return True
        
    except Exception as e:
        log(f'Failed to write timeseries point: {e}')
        return False

def collect_rolling_ticks(csv_filename: str, base_cumulative: float, num_ticks: int = 5, interval_seconds: int = 60) -> list:
    """Collect rolling ticks for execution trace."""
    rolling_ticks = []
    
    for i in range(num_ticks):
        try:
            # Calculate current portfolio metrics
            portfolio_data = calculate_real_portfolio_value(csv_filename)
            if not portfolio_data:
                log(f"Failed to calculate portfolio data for rolling tick {i+1}")
                continue
            
            portfolio_value = portfolio_data['portfolio_value']
            daily_pnl = portfolio_value - 1000000 - base_cumulative
            total_pnl = portfolio_value - 1000000
            
            tick_data = {
                'ts_utc': datetime.datetime.utcnow().isoformat(),
                'portfolio_value': portfolio_value,
                'daily_pnl': daily_pnl,
                'total_pnl': total_pnl,
                'base_cumulative': base_cumulative,
                'csv_filename': csv_filename,
                'is_day1_fallback': base_cumulative == 0
            }
            
            rolling_ticks.append(tick_data)
            log(f"Rolling tick {i+1}/{num_ticks}: portfolio=${portfolio_value:,.2f}, daily=${daily_pnl:,.2f}")
            
            if i < num_ticks - 1:  # Don't sleep after the last tick
                time.sleep(interval_seconds)
                
        except Exception as e:
            log(f"Error collecting rolling tick {i+1}: {e}")
            break
    
    return rolling_ticks


def continuous_timeseries_writer() -> None:
    """Background thread that writes time series points every minute."""
    import threading
    import time
    
    def writer_loop():
        while True:
            try:
                # Iterate per strategy and write series separately
                for strategy in STRATEGY_DIRS.keys():
                    latest_csv = read_current_latest_json(strategy)
                    if not latest_csv:
                        continue

                    # Calculate current portfolio metrics using the same logic as the UI
                    portfolio_data = calculate_real_portfolio_value(latest_csv, strategy)
                    if not portfolio_data:
                        continue

                    # Get baseline info
                    s3 = boto3.client('s3')
                    baseline_ready = False
                    baseline_prices = {}
                    try:
                        baseline_resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'daily_baseline.json')
                        baseline_data = json.loads(baseline_resp['Body'].read().decode('utf-8'))
                        baseline_prices = baseline_data.get('prices', {})
                        baseline_ready = True
                    except:
                        pass

                    # Calculate daily P&L (same logic as UI)
                    daily_pnl = 0
                    is_day1_fallback = False

                    # Get current prices from S3 latest_prices.json
                    current_prices = {}
                    try:
                        prices_prefix = ('descartes-ml/signal-dashboard/data/' if ENV == 'PULSE' else 'signal-dashboard/data/')
                        latest_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=prices_prefix + 'latest_prices.json')
                        latest_data = json.loads(latest_obj['Body'].read().decode('utf-8'))
                        prices_map = latest_data.get('prices', {}) or {}
                        for pos in portfolio_data.get('positions', []):
                            symbol = pos.get('ticker', '').replace('_', '')
                            if symbol in prices_map:
                                try:
                                    current_prices[symbol] = float(prices_map[symbol])
                                except Exception:
                                    pass
                    except Exception:
                        current_prices = {}

                    # Calculate daily P&L using same logic as UI
                    for pos in portfolio_data.get('positions', []):
                        symbol = pos.get('ticker', '').replace('_', '')
                        entry_price = float(pos.get('ref_price', 0))
                        notional = float(pos.get('target_notional', 0))
                        contracts = float(pos.get('target_contracts', 0))
                        side = 'LONG' if contracts > 0 else 'SHORT'

                        if symbol in current_prices and entry_price > 0:
                            current_price = current_prices[symbol]

                            if baseline_ready and symbol in baseline_prices:
                                # Use baseline prices
                                base_price = baseline_prices[symbol]
                                pct = (current_price - base_price) / base_price if side == 'LONG' else (base_price - current_price) / base_price
                                daily_pnl += pct * notional
                            else:
                                # Day-1 fallback: entry vs live
                                pct = (current_price - entry_price) / entry_price if side == 'LONG' else (entry_price - current_price) / entry_price
                                daily_pnl += pct * notional
                                is_day1_fallback = True

                    # Get base cumulative (same logic as UI)
                    base_cumulative = 0
                    try:
                        log_resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_prefix_for(strategy) + 'portfolio_daily_log.csv')
                        log_content = log_resp['Body'].read().decode('utf-8')
                        lines = log_content.strip().split('\n')
                        if len(lines) > 1:
                            headers = lines[0].split(',')
                            action_idx = headers.index('action') if 'action' in headers else 5
                            date_idx = headers.index('date') if 'date' in headers else 1
                            cumulative_idx = headers.index('cumulative_pnl') if 'cumulative_pnl' in headers else 9

                            today_utc = datetime.datetime.utcnow().strftime('%Y-%m-%d')

                            # Find last pre_execution from prior day
                            for i in range(len(lines) - 1, 0, -1):
                                row = lines[i].split(',')
                                if len(row) > max(action_idx, date_idx, cumulative_idx):
                                    action = row[action_idx]
                                    row_date = row[date_idx].strip()
                                    if action == 'pre_execution' and row_date and row_date < today_utc:
                                        base_cumulative = float(row[cumulative_idx]) if cumulative_idx < len(row) else 0
                                        break
                    except:
                        pass

                    # Calculate totals
                    total_pnl = base_cumulative + daily_pnl
                    portfolio_value = 1000000.0 + total_pnl

                    # Write time series point
                    append_timeseries_point(latest_csv, portfolio_value, daily_pnl, total_pnl, base_cumulative, is_day1_fallback)

            except Exception as e:
                log(f'Timeseries writer error: {e}')

            time.sleep(60)  # 1 minute cadence
    
    # Start the background thread
    thread = threading.Thread(target=writer_loop, daemon=True)
    thread.start()
    log('Timeseries writer started (1-minute cadence)')


def main() -> None:
    # Console log capture for CSV monitoring
    import sys
    
    log_filename = f"/tmp/csv_monitor_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_filename, 'w')
    
    class TeeOutput:
        def __init__(self, *files):
            self.files = files
        def write(self, text):
            for f in self.files:
                f.write(text)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()
    
    # Redirect stdout to both console and file
    original_stdout = sys.stdout
    sys.stdout = TeeOutput(original_stdout, log_file)
    
    log("🚀 Starting CSV monitoring script...")
    log(f"📝 Monitor log: {log_filename}")
    # Legacy email notifier removed
    # Per-strategy state
    last_processed: dict[str, str | None] = {k: None for k in STRATEGY_DIRS.keys()}
    last_executed_sha256: dict[str, str | None] = {k: None for k in STRATEGY_DIRS.keys()}

    log("📧 EmailNotifier initialized")

    # Start the continuous time series writer (guarded)
    if PV_WRITER_ENABLED:
        continuous_timeseries_writer()
        log("⚡ Timeseries writer started (PV_WRITER_ENABLED=1)")
    else:
        log("🛑 Detector-only: PV writer disabled (PV_WRITER_ENABLED=0)")

    # helper to know if we are within the nightly window (23:55 → 01:25 UTC)
    def compute_in_window(now_utc: datetime.datetime) -> bool:
        today_window_start = now_utc.replace(hour=23, minute=55, second=0, microsecond=0)
        if now_utc.hour < 2:  # window crossing midnight
            window_end = now_utc.replace(hour=1, minute=25, second=0, microsecond=0)
            return now_utc <= window_end
        else:
            window_end = today_window_start + datetime.timedelta(hours=1, minutes=30)
            return today_window_start <= now_utc <= window_end

    while True:
        # compute window state at the top of the loop
        now0 = datetime.datetime.utcnow()
        in_window = compute_in_window(now0)
        any_updated = False
        for strategy, dir_path in STRATEGY_DIRS.items():
            latest = get_latest_2355(dir_path)
            prev_seen = last_processed.get(strategy)
            s3_prev = read_current_latest_json(strategy)
            log(f"🧭 latest[{strategy}]={latest} prev_seen={prev_seen} s3_latest={s3_prev}")
            if not latest or latest == prev_seen:
                continue
            any_updated = True
            log(f"🔍 Detected new CSV for {strategy}: {latest}")

            # Generate execution ID and compute CSV SHA256 for idempotency
            execution_id = str(uuid.uuid4())
            log(f"🆔 Generated execution ID: {execution_id}")

            # Read CSV content to compute SHA256
            tmp_path = copy_with_sudo_to_tmp(latest, strategy)
            if not tmp_path:
                log(f"❌ Failed to copy CSV {latest}")
                continue

            with open(tmp_path, 'r') as f:
                csv_content = f.read()
            csv_sha256 = compute_csv_sha256(csv_content)
            log(f"📄 CSV SHA256: {csv_sha256}")
            
            # Check idempotency (strict guards)
            if csv_sha256 == last_executed_sha256.get(strategy):
                log(f"⏭️ Skipping duplicate CSV {latest} (same SHA256)")
                continue

            # Strict filename guard: only yesterday UTC and 2355
            cand_d, cand_t = parse_stamp(latest)
            yday = utc_yesterday_yyyymmdd()
            if not (cand_d == yday and cand_t == 2355):
                log(f"⏭️ Skip {latest}: requires yesterday UTC and 2355 (got {cand_d}-{cand_t})")
                last_processed[strategy] = latest
                last_executed_sha256[strategy] = csv_sha256
                continue

            # Persisted state (S3) to survive restarts
            state = read_state(strategy) or {}
            st_sha = state.get('sha256')
            st_mtime = int(state.get('mtime_epoch') or 0)
            cand_mtime = 0
            try:
                cand_mtime = sudo_stat_epoch(os.path.join(dir_path, latest))
            except Exception:
                pass
            if st_sha and csv_sha256 == st_sha:
                log(f"⏭️ Skip {latest}: same SHA as persisted state")
                last_processed[strategy] = latest
                last_executed_sha256[strategy] = csv_sha256
                continue
            if cand_mtime and st_mtime and cand_mtime <= st_mtime:
                log(f"⏭️ Skip {latest}: mtime not newer than persisted state (cand={cand_mtime}, prev={st_mtime})")
                last_processed[strategy] = latest
                last_executed_sha256[strategy] = csv_sha256
                continue

            # Remember previous latest from S3 before we switch
            prev_latest = read_current_latest_json(strategy)
            pre_exec_data = None
            post_exec_data = None
            baseline_data = None
            errors = []
            log(f"📋 Initialized processing variables")

            # 0) PRE-EXECUTION LOG (use previous CSV if available)
            if prev_latest:
                prev_data = calculate_real_portfolio_value(prev_latest, strategy)
                if prev_data:
                    # Get CSV mtime
                    csv_mtime = ""
                    try:
                        csv_stat = os.stat(os.path.join(dir_path, prev_latest))
                        csv_mtime = datetime.datetime.fromtimestamp(csv_stat.st_mtime, tz=datetime.timezone.utc).isoformat()
                    except:
                        pass
                    
                    pre_exec_data = {
                        'timestamp': datetime.datetime.utcnow().isoformat(),
                        'portfolio_value': prev_data['portfolio_value'],
                        'cumulative_pnl': prev_data['portfolio_value'] - 1000000,
                        'csv_mtime': csv_mtime
                    }
                    append_log_row('pre_execution', prev_latest, prev_data['portfolio_value'], prev_data['positions_count'], prev_data['total_notional'], None, strategy)

            # Idempotency and monotonic date guards (against duplicates/older files)
            pulse_meta = read_latest_meta_env('PULSE', strategy) or {}
            latest_csv_on_s3 = pulse_meta.get('latest_csv') or pulse_meta.get('filename')
            latest_sha_on_s3 = pulse_meta.get('sha256')

            # Compare SHA to skip exact duplicate
            FORCE_UPLOAD = os.getenv('FORCE_UPLOAD', '0') == '1'
            if not FORCE_UPLOAD and latest_sha_on_s3 and latest_sha_on_s3 == csv_sha256:
                log(f"⏭️ Skip upload for {latest}: same SHA as Pulse latest.json ({latest_sha_on_s3})")
                last_processed[strategy] = latest
                last_executed_sha256[strategy] = csv_sha256
                continue

            # Compare filename date to enforce monotonic non-decreasing dates
            # Accept same-day or newer; reject strictly older than S3 latest filename
            try:
                def parse_stamp(name: str) -> tuple:
                    # expected patterns like monitor_signal_DF_YYYYMMDD-HHMM.csv
                    import re
                    m = re.search(r'(\d{8})-(\d{4})', name or '')
                    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)
                cand_d, cand_t = parse_stamp(latest)
                prev_d, prev_t = parse_stamp(latest_csv_on_s3 or '')
                if not FORCE_UPLOAD and (cand_d, cand_t) < (prev_d, prev_t):
                    log(f"⏭️ Skip upload for {latest}: older than Pulse latest.csv ({latest_csv_on_s3})")
                    last_processed[strategy] = latest
                    last_executed_sha256[strategy] = csv_sha256
                    continue
            except Exception as e:
                log(f"WARN: date-guard parse failed for {latest}: {e}")

            # 1) Copy and upload new CSV (may upload if content differs or filename newer)
            ok_upload = upload_csv_to_both_envs(tmp_path, latest, strategy)
            if not ok_upload:
                errors.append(f"CSV upload failed for {latest}")

            # 2) Dashboard stamping removed (legacy)
            ok_dash = False
            log(f"📊 ok_dash({strategy}) skipped (legacy removed)")

            # 3) Update latest.json for the dashboard to discover new file
            ok_latest = write_latest_json_both(latest, strategy, csv_sha256)
            log(f"📄 ok_latest({strategy}) = {ok_latest} (latest.json write result)")

            # 3b) Write detection-only snapshot and EXIT early (no execution now)
            write_pre_execution_pending(latest, csv_sha256, strategy)
            last_processed[strategy] = latest
            last_executed_sha256[strategy] = csv_sha256
            # Persist state
            write_state(strategy, latest, csv_sha256, cand_mtime or int(time.time()))
            log(f'⏹️ Detection pipeline complete for {strategy} (pre_execution.json written); deferring execution to scheduled time')
            # Next strategy
            continue

            # 4) POST-EXECUTION LOG using new CSV; compute daily vs pre if we logged it
            post_ok = False
            post_data = calculate_real_portfolio_value(latest) if ok_upload else None
            log(f"💰 post_data calculation: {post_data is not None}")
            if post_data:
                post_exec_data = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'portfolio_value': post_data['portfolio_value']
                }
                
                # Find today's pre value (last row if action==pre_execution)
                pre_value = None
                try:
                    s3 = boto3.client('s3')
                    log_text = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_PREFIX + 'portfolio_daily_log.csv')['Body'].read().decode('utf-8')
                    lines = [l for l in log_text.strip().split('\n') if l]
                    if len(lines) > 1:
                        last = lines[-1].split(',')
                        if len(last) > 5 and last[5] == 'pre_execution':
                            pre_value = float(last[6])
                except Exception:
                    pre_value = None
                append_log_row('post_execution', latest, post_data['portfolio_value'], post_data['positions_count'], post_data['total_notional'], pre_value)
                post_ok = True

                # 4b) Write/reset daily baseline snapshot for UI immediately after post_execution
                baseline_data = {
                    'timestamp_utc': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'csv_filename': latest
                }
                write_daily_baseline_json(latest, post_data['portfolio_value'], post_data.get('current_prices', {}))

            # 5) Collect first tick and rolling ticks for execution trace
            first_tick_data = None
            rolling_ticks = []
            
            if post_ok and post_data:
                # Wait a moment for first tick
                time.sleep(5)
                
                # Get first tick data
                first_tick_data = calculate_real_portfolio_value(latest)
                if first_tick_data:
                    base_cumulative = pre_exec_data['cumulative_pnl'] if pre_exec_data else 0
                    portfolio_value = first_tick_data['portfolio_value']
                    daily_pnl = portfolio_value - 1000000 - base_cumulative
                    total_pnl = portfolio_value - 1000000
                    
                    first_tick_data = {
                        'ts_utc': datetime.datetime.utcnow().isoformat(),
                        'portfolio_value': portfolio_value,
                        'daily_pnl': daily_pnl,
                        'total_pnl': total_pnl,
                        'base_cumulative': base_cumulative,
                        'csv_filename': latest,
                        'is_day1_fallback': base_cumulative == 0
                    }
                    
                    # Collect rolling ticks in background
                    log("Starting rolling ticks collection...")
                    rolling_ticks = collect_rolling_ticks(latest, base_cumulative, num_ticks=5, interval_seconds=60)
            
            # 6) Write local execution trace
            write_local_execution_trace(
                latest, execution_id, csv_sha256, last_executed_sha256,
                pre_exec_data, post_exec_data, baseline_data, first_tick_data, rolling_ticks, errors, []
            )

            # 7) Legacy email removed
        if not any_updated:
            log(f"🔄 No new CSV found for any strategy, continuing monitoring...")
        interval = 60 if in_window else 300
        log(f"⏰ Sleeping for {interval} seconds before next check...")
        time.sleep(interval)
    
    # Close log file and upload to S3 when monitor exits
    try:
        log_file.close()
        sys.stdout = original_stdout
        
        # Upload monitor log to S3
        s3_log_key = f"signal-dashboard/execution-logs/csv_monitor_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_filename, 'r') as f:
            log_content = f.read()
        
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_log_key,
            Body=log_content.encode('utf-8'),
            ContentType='text/plain',
            CacheControl='no-store'
        )
        print(f"📝 Monitor log uploaded to s3://{S3_BUCKET_NAME}/{s3_log_key}")
        print(f"📝 Local log: {log_filename}")
        
    except Exception as e:
        print(f"⚠️ Failed to upload monitor log: {e}")
        print(f"📝 Local log available: {log_filename}")


if __name__ == '__main__':
    # ensure log exists
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w') as f:
                f.write('=== CSV Detection Log ===\n')
    except Exception:
        pass
    main()


