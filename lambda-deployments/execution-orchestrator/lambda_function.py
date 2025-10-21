import os
import json
import time
import datetime as dt
from typing import Dict, Any, Tuple, Optional

import boto3
from botocore.exceptions import ClientError


BUCKET_ENV = os.getenv("BUCKET")  # Optional override; otherwise taken from event

# Known env base prefixes
ADMIN_BASE = "signal-dashboard/data"
PULSE_BASE = "descartes-ml/signal-dashboard/data"

# Fixed capital for P&L
CAPITAL = 1_000_000.0
# Optional delay to allow mirrors and pre-pv snapshots to settle
MIN_DELAY_SECONDS = int(os.getenv("MIN_DELAY_SECONDS", "300"))

# Alias mapping for Binance 1000-quoted perps
ALIAS_MAP = {
    "SHIBUSDT": "1000SHIBUSDT",
    "PEPEUSDT": "1000PEPEUSDT",
    "XECUSDT": "1000XECUSDT",
    "SATSUSDT": "1000SATSUSDT",
    "RATSUSDT": "1000RATSUSDT",
    "FLOKIUSDT": "1000FLOKIUSDT",
    "LUNCUSDT": "1000LUNCUSDT",
    "XUSDT": "1000XUSDT",
}


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_env_and_strategy(key: str) -> Tuple[str, str]:
    """Infer environment (ADMIN|PULSE) and strategy from S3 key.

    Expected forms:
      - signal-dashboard/data/{strategy}/filename.csv (ADMIN)
      - descartes-ml/signal-dashboard/data/{strategy}/filename.csv (PULSE)
    """
    parts = key.split("/")
    if len(parts) < 4:
        raise ValueError(f"Key too short for expected layout: {key}")

    if parts[0] == "signal-dashboard" and parts[1] == "data":
        env = "ADMIN"
        strategy = parts[2]
    elif parts[0] == "descartes-ml" and parts[1] == "signal-dashboard" and parts[2] == "data":
        env = "PULSE"
        strategy = parts[3]
    else:
        raise ValueError(f"Unrecognized key layout: {key}")

    return env, strategy


def base_prefix_for(env: str) -> str:
    return ADMIN_BASE if env == "ADMIN" else PULSE_BASE


def latest_prices_key_for(env: str) -> str:
    # Latest prices live at the env root (no strategy)
    return f"{base_prefix_for(env)}/latest_prices.json"


def read_object_text(s3, bucket: str, key: str) -> Optional[str]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return None
        raise


def object_age_seconds(s3, bucket: str, key: str) -> Optional[float]:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        last_mod = head["LastModified"]  # datetime with tz
        now = dt.datetime.now(dt.timezone.utc)
        return (now - last_mod).total_seconds()
    except ClientError:
        return None


def read_last_pv(s3, bucket: str, env: str, strategy: str) -> float:
    """Return last portfolio_value from env-level portfolio_value_log.jsonl, or 1_000_000 if missing.

    Note: PV log is maintained at the env root, not per strategy.
    """
    key = f"{base_prefix_for(env)}/portfolio_value_log.jsonl"
    txt = read_object_text(s3, bucket, key)
    if not txt:
        return CAPITAL
    last_pv = None
    for line in txt.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            pv = float(row.get("portfolio_value", row.get("portfolioValue", 0.0)))
            if pv:
                last_pv = pv
        except Exception:
            continue
    return last_pv if last_pv is not None else CAPITAL


def parse_weights_from_csv(csv_text: str) -> Dict[str, float]:
    """Parse CSV with schema including columns: product,target_weight.

    Returns mapping TICKER->target_weight (float). Missing/invalid weights are skipped.
    """
    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    if not lines:
        return {}
    headers = [h.strip() for h in lines[0].split(",")]
    try:
        idx_product = headers.index("product")
        idx_weight = headers.index("target_weight")
    except ValueError:
        # Fallback: try ticker/target_weight or ric/target_weight
        try:
            idx_product = headers.index("ticker")
            idx_weight = headers.index("target_weight")
        except ValueError:
            return {}

    weights: Dict[str, float] = {}
    for ln in lines[1:]:
        cols = [c.strip() for c in ln.split(",")]
        if len(cols) <= max(idx_product, idx_weight):
            continue
        prod = cols[idx_product].replace("_", "")
        try:
            raw_w = cols[idx_weight].strip()
            # Normalize unicode minus signs and stray percent symbols
            raw_w = raw_w.replace("−", "-").replace("–", "-").replace("%", "")
            w = float(raw_w)
        except Exception:
            continue
        if prod:
            weights[prod] = w
    return weights


def build_baseline(weights: Dict[str, float], prices: Dict[str, Any]) -> Tuple[Dict[str, float], Dict[str, float], list]:
    """Filter to tickers with available prices; return (weights, ref_prices, universe).

    Supports alias mapping for Binance 1000-quoted perps (e.g., SHIB, PEPE):
    if a CSV ticker is not found in prices, we try an alias and still record
    the ref price under the original CSV ticker to keep UI keys consistent.
    """
    ref_prices: Dict[str, float] = {}
    filtered_weights: Dict[str, float] = {}
    for ticker, w in weights.items():
        price_key = ticker if ticker in prices else ALIAS_MAP.get(ticker)
        if price_key and price_key in prices:
            try:
                ref_prices[ticker] = float(prices[price_key])
                filtered_weights[ticker] = float(w)
            except Exception:
                continue
    universe = sorted(filtered_weights.keys())
    return filtered_weights, ref_prices, universe


def extract_csv_symbols(csv_text: str) -> list:
    try:
        lines = [ln for ln in (csv_text or '').splitlines() if ln.strip()]
        if not lines:
            return []
        headers = [h.strip() for h in lines[0].split(',')]
        idx = None
        for key in ("product", "ticker", "ric", "internal_code"):
            if key in headers:
                idx = headers.index(key)
                break
        if idx is None:
            return []
        out = []
        for ln in lines[1:]:
            parts = [p.strip() for p in ln.split(',')]
            if len(parts) <= idx:
                continue
            sym = parts[idx].replace('_', '')
            if sym:
                out.append(sym)
        return out
    except Exception:
        return []


def write_json(s3, bucket: str, key: str, payload: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store, max-age=0",
    )


def read_env_pv_tail(s3, bucket: str, env: str, tail: int = 3) -> list:
    """Return last N entries from env-level PV log as parsed dicts (best-effort)."""
    key = f"{base_prefix_for(env)}/portfolio_value_log.jsonl"
    txt = read_object_text(s3, bucket, key) or ""
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    out = []
    for ln in lines[-tail:]:
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out


def send_execution_email(ses_region: str, sender: str, recipient: str, subject: str, body: str) -> None:
    ses = boto3.client("ses", region_name=ses_region)
    ses.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
        },
    )

def _append_jsonl(s3, bucket: str, key: str, entry: Dict[str, Any]) -> None:
    try:
        existing = read_object_text(s3, bucket, key) or ""
    except Exception:
        existing = ""
    body = (existing or "") + json.dumps(entry, separators=(",", ":")) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
        CacheControl="no-store, max-age=0",
    )


def ensure_execution_anchor(s3, bucket: str, env: str, pv_pre: float, executed_at_utc: str) -> None:
    """Idempotently append a zero-PnL anchor at executed_at_utc to the env PV log."""
    if not executed_at_utc:
        return
    log_key = f"{base_prefix_for(env)}/portfolio_value_log.jsonl"
    try:
        text = read_object_text(s3, bucket, log_key) or ""
        if executed_at_utc in text:
            return
    except Exception:
        text = ""
    anchor = {
        "timestamp": executed_at_utc,
        "portfolio_value": float(pv_pre),
        "daily_pnl": 0.0,
        "total_pnl": float(pv_pre) - CAPITAL,
        "audit": {"source": "execution-anchor", "note": "zero PnL at execution"},
    }
    _append_jsonl(s3, bucket, log_key, anchor)


def handler(event, context):
    s3 = boto3.client("s3")

    # Support direct invocation payloads as well as S3 events
    records = event.get("Records") if isinstance(event, dict) else None
    if records is None and isinstance(event, dict) and event.get("bucket") and event.get("key"):
        records = [{"s3": {"bucket": {"name": event["bucket"]}, "object": {"key": event["key"]}}}]
    if not records:
        return {"status": "no_records"}

    for rec in records:
        bucket = BUCKET_ENV or rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]

        if not key.endswith(".csv"):
            continue

        # Identify env/strategy
        env, strategy = parse_env_and_strategy(key)
        base_prefix = base_prefix_for(env)

        # Delay if too fresh, to allow mirrors to publish and pv_pre to be logged
        age = object_age_seconds(s3, bucket, key)
        if age is not None and age < MIN_DELAY_SECONDS:
            time.sleep(MIN_DELAY_SECONDS - age)

        # Read uploaded CSV
        csv_text = read_object_text(s3, bucket, key)
        if not csv_text:
            # Nothing to do
            continue

        # Snapshot pv_pre from PV log
        pv_pre = read_last_pv(s3, bucket, env, strategy)

        # Read latest prices for this env
        latest_prices_txt = read_object_text(s3, bucket, latest_prices_key_for(env))
        prices_map: Dict[str, Any] = {}
        if latest_prices_txt:
            try:
                prices_doc = json.loads(latest_prices_txt)
                prices_map = prices_doc.get("prices", {}) or {}
            except Exception:
                prices_map = {}

        # Parse CSV weights and build baseline at live prices
        weights = parse_weights_from_csv(csv_text)
        filtered_weights, ref_prices, universe = build_baseline(weights, prices_map)

        now_iso = _utc_now_iso()

        # Write pre_execution.json
        pre_exec_payload = {
            "pv_pre": float(pv_pre),
            "pv_pre_time": now_iso,
            "executed_at_utc": now_iso,
            "strategy": strategy,
        }
        write_json(s3, bucket, f"{base_prefix}/{strategy}/pre_execution.json", pre_exec_payload)

        # Write daily_baseline.json
        baseline_payload = {
            "capital": float(CAPITAL),
            "weights": filtered_weights,
            "ref_prices": ref_prices,
            "universe": universe,
            "executed_at_utc": now_iso,
            "audit": {
                "csv_key": key,
                "price_feed_key": latest_prices_key_for(env),
            },
        }
        write_json(s3, bucket, f"{base_prefix}/{strategy}/daily_baseline.json", baseline_payload)

        # Update latest.json under the strategy path
        latest_payload = {
            "filename": key.split("/")[-1],
            "latest_csv": key.split("/")[-1],
            "updated_utc": now_iso,
        }
        write_json(s3, bucket, f"{base_prefix}/{strategy}/latest.json", latest_payload)

        # Idempotent zero-PnL anchor at execution time to the env PV log
        ensure_execution_anchor(s3, bucket, env, float(pv_pre), now_iso)

        # Optional: email execution report (Pulse combined only unless configured otherwise)
        try:
            ses_from = os.getenv("SES_FROM", "")
            ses_to = os.getenv("SES_TO", "")
            ses_region = os.getenv("SES_REGION", "eu-west-3")
            enabled = os.getenv("EMAIL_ON_EXECUTION", "1") == "1"
            if enabled and ses_from and ses_to and env == "PULSE" and strategy == "combined_descartes_unravel":
                lp_raw = read_object_text(s3, bucket, latest_prices_key_for(env)) or "{}"
                try:
                    lp_doc = json.loads(lp_raw)
                    symbol_count = len((lp_doc.get("prices") or lp_doc) or {})
                except Exception:
                    symbol_count = 0
                pv_tail = read_env_pv_tail(s3, bucket, env, 3)
                pv_tail_compact = [
                    {
                        "t": r.get("timestamp"),
                        "pv": r.get("portfolio_value"),
                        "d": r.get("daily_pnl"),
                        "src": (r.get("audit") or {}).get("source"),
                    }
                    for r in pv_tail
                ]
                uni_count = len(universe)
                # CSV diagnostics
                csv_syms = extract_csv_symbols(csv_text)
                csv_count = len(csv_syms)
                missing_entry = sorted([sym for sym in csv_syms if sym not in ref_prices and ALIAS_MAP.get(sym) not in ref_prices])
                price_map = (lp_doc.get("prices") or {}) if isinstance(lp_doc, dict) else {}
                missing_mark = sorted([sym for sym in csv_syms if (sym not in price_map and ALIAS_MAP.get(sym) not in price_map)])
                alias_used = sorted([sym for sym in csv_syms if ALIAS_MAP.get(sym) and ALIAS_MAP.get(sym) in price_map])

                # Entry/Mark details for all portfolio symbols
                baseline_key = f"{base_prefix}/{strategy}/daily_baseline.json"
                mark_source = latest_prices_key_for(env)
                symbol_sources = []
                for sym in csv_syms:
                    entry_price = ref_prices.get(sym)
                    mk_key = sym if sym in price_map else ALIAS_MAP.get(sym)
                    mk_price = price_map.get(mk_key) if (mk_key and mk_key in price_map) else None
                    symbol_sources.append({
                        "symbol": sym,
                        "entry_key": sym,
                        "entry_price": entry_price,
                        "mark_key": mk_key or "missing",
                        "mark_price": mk_price,
                    })
                subject = f"Pulse Execution: {strategy} @ {now_iso}"
                body = (
                    f"Env: {env}\n"
                    f"Strategy: {strategy}\n"
                    f"CSV: {key}\n"
                    f"Executed at (UTC): {now_iso}\n"
                    f"pv_pre: {pv_pre:.2f}\n"
                    f"universe_count: {uni_count} (csv_count={csv_count})\n"
                    f"latest_prices symbols: {symbol_count}\n"
                    f"missing_entry_from_baseline: {missing_entry}\n"
                    f"missing_mark_from_prices: {missing_mark}\n"
                    f"alias_used_for_marks: {alias_used}\n"
                    f"pv_tail(3): {json.dumps(pv_tail_compact)}\n"
                    f"entry_source: s3://{bucket}/{baseline_key} (ref_prices)\n"
                    f"mark_source: s3://{bucket}/{mark_source}\n"
                    f"symbol_sources: {json.dumps(symbol_sources)}\n"
                )
                try:
                    send_execution_email(ses_region, ses_from, ses_to, subject, body)
                    print(f"[EXEC EMAIL] sent to {ses_to} from {ses_from} @ {now_iso} (universe={uni_count}, symbols={symbol_count})")
                except Exception as e:
                    print(f"[EXEC EMAIL] failed: {e}")
        except Exception:
            print("[EXEC EMAIL] outer failure (ignored)")

    return {"status": "ok"}





















