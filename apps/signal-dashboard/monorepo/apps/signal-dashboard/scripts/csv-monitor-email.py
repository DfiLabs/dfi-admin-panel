import os
import time
import datetime
import subprocess
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from email_notifier import EmailNotifier


CSV_DIRECTORY = '/home/leo/Desktop/dfilabs-machine-v2/dfilabs-machine-v2/send_to_qube/'
TARGET_SUFFIX = '2355.csv'
LOG_FILE = '/home/ubuntu/csv-detection.log'
S3_BUCKET_NAME = 'dfi-signal-dashboard'
# Serve under CloudFront path /signal-dashboard/*
S3_KEY_PREFIX = 'signal-dashboard/data/'
S3_DASHBOARD_KEY = 'signal-dashboard/dashboard.html'


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


def get_latest_2355() -> str | None:
    try:
        files = sudo_listdir_sorted(CSV_DIRECTORY)
        latest_name = None
        latest_ts = None
        for name in files:
            if name.endswith(TARGET_SUFFIX):
                fp = os.path.join(CSV_DIRECTORY, name)
                ts = sudo_stat_epoch(fp)
                if latest_name is None or ts > latest_ts:
                    latest_name = name
                    latest_ts = ts
        return latest_name
    except subprocess.CalledProcessError as e:
        log(f"sudo list/stat failed: {e.stderr.strip()}")
        return None


def copy_with_sudo_to_tmp(src_name: str) -> str | None:
    src = os.path.join(CSV_DIRECTORY, src_name)
    dst = os.path.join('/tmp', src_name)
    try:
        subprocess.run(['sudo', 'cp', src, dst], check=True)
        subprocess.run(['sudo', 'chown', 'ubuntu:ubuntu', dst], check=True)
        return dst
    except subprocess.CalledProcessError as e:
        log(f"sudo copy/chown failed: {e.stderr.strip()}")
        return None


def upload_csv_to_s3(local_path: str, filename: str) -> bool:
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_path, S3_BUCKET_NAME, S3_KEY_PREFIX + filename, ExtraArgs={'ContentType': 'text/csv', 'CacheControl': 'no-cache'})
        log(f"Uploaded to s3://{S3_BUCKET_NAME}/{S3_KEY_PREFIX}{filename}")
        return True
    except Exception as e:
        log(f"S3 upload failed: {e}")
        return False


def update_dashboard_html_on_s3(new_csv_filename: str) -> bool:
    s3 = boto3.client('s3')
    try:
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_DASHBOARD_KEY)
        html = resp['Body'].read().decode('utf-8')
        local_csv = os.path.join('/tmp', new_csv_filename)
        with open(local_csv, 'r') as f:
            csv_text = f.read()

        import re
        html = re.sub(r"const csvData = `[^`]*`;", f"const csvData = `{csv_text.strip()}`;", html, flags=re.DOTALL)
        html = re.sub(r"id=\"csv-timestamp\">[^<]*<", f"id=\"csv-timestamp\">(loaded: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})<", html)

        s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_DASHBOARD_KEY, Body=html.encode('utf-8'), ContentType='text/html', CacheControl='no-cache')
        log("Dashboard updated on S3")
        return True
    except Exception as e:
        log(f"Dashboard update failed: {e}")
        return False


def write_latest_json(filename: str) -> bool:
    """Write a small latest.json with the newest CSV filename for the dashboard to consume."""
    s3 = boto3.client('s3')
    import json
    payload = {
        'filename': filename,
        'updated_utc': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_KEY_PREFIX + 'latest.json',
            Body=json.dumps(payload).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-cache'
        )
        log(f"Wrote latest.json with {filename}")
        return True
    except Exception as e:
        log(f"Failed to write latest.json: {e}")
        return False


def main() -> None:
    notifier = EmailNotifier()
    last_processed = None

    while True:
        latest = get_latest_2355()
        if latest and latest != last_processed:
            log(f"Detected new CSV: {latest}")

            # 1) Copy and upload
            tmp_path = copy_with_sudo_to_tmp(latest)
            ok_upload = tmp_path and upload_csv_to_s3(tmp_path, latest)

            # 2) Update dashboard
            ok_dash = ok_upload and update_dashboard_html_on_s3(latest)

            # 3) Update latest.json for the dashboard to discover new file
            ok_latest = write_latest_json(latest)

            # 4) If all good, send email once per day
            if ok_dash and ok_latest:
                subject = f"Signal Dashboard: New CSV executed ({latest})"
                html = f"""
                <h3>New CSV executed</h3>
                <p><strong>File</strong>: {latest}</p>
                <p>Status: detected ➜ uploaded to S3 ➜ dashboard updated</p>
                <p>Time (UTC): {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}</p>
                """
                notifier.send_once_per_day(subject, html)
                last_processed = latest
        # Dynamic polling: faster during 00:00–02:00 UTC, slower otherwise
        now = datetime.datetime.utcnow()
        interval = 10 if 0 <= now.hour < 2 else 120
        time.sleep(interval)


if __name__ == '__main__':
    # ensure log exists
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w') as f:
                f.write('=== CSV Detection Log ===\n')
    except Exception:
        pass
    main()


