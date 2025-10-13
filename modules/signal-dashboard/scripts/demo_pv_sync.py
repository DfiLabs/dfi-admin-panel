#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Dict


BUCKET = os.environ.get("DFI_BUCKET", "dfi-signal-dashboard")
ADMIN_KEY = os.environ.get("ADMIN_PV_KEY", "signal-dashboard/data/portfolio_value_log.jsonl")
DEMO_KEY = os.environ.get("DEMO_PV_KEY", "signal-dashboard-demo/signal-dashboard/data/portfolio_value_log.jsonl")


def run(cmd: List[str], stdin: str | None = None) -> str:
    """Run a shell command and return stdout as text. Raise on non-zero exit."""
    result = subprocess.run(
        cmd,
        input=stdin.encode("utf-8") if stdin is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{result.stderr.decode('utf-8', 'ignore')}")
    return result.stdout.decode("utf-8", "ignore")


def read_s3_text(bucket: str, key: str) -> str:
    return run(["aws", "s3", "cp", f"s3://{bucket}/{key}", "-", "--only-show-errors"])  # type: ignore


def write_s3_text(bucket: str, key: str, content: str) -> None:
    run([
        "aws", "s3", "cp", "-", f"s3://{bucket}/{key}",
        "--content-type", "application/json",
        "--cache-control", "no-store, max-age=0",
        "--only-show-errors",
    ], stdin=content)


def parse_jsonl(text: str) -> List[Dict]:
    out: List[Dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            # Normalize field name
            if "portfolioValue" in obj and "portfolio_value" not in obj:
                obj["portfolio_value"] = obj["portfolioValue"]
            out.append(obj)
        except Exception:
            # Skip malformed lines silently for robustness
            continue
    return out


def dt_from_iso(s: str) -> datetime:
    # Accept both 'Z' and '+00:00' suffixes
    if s.endswith("Z"):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    # Fallback: fromisoformat
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso_from_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resample_linear(points: List[Dict], every_minutes: int = 5) -> List[Dict]:
    """
    Given intraday points with timestamp and portfolio_value, linearly interpolate
    to an even grid for smooth rendering. Assumes points are for the same UTC day.
    """
    if not points:
        return []

    # Sort by time
    pts = sorted(points, key=lambda p: dt_from_iso(p["timestamp"]))
    times = [dt_from_iso(p["timestamp"]) for p in pts]
    values = [float(p["portfolio_value"]) for p in pts]

    # Build time grid from first to last (now) at 5-min spacing
    grid: List[datetime] = []
    start = times[0].replace(second=0, microsecond=0)
    # Align to 5-minute boundary
    minute_mod = start.minute % every_minutes
    if minute_mod:
        start = start - timedelta(minutes=minute_mod)
    end = times[-1]
    t = start
    while t <= end:
        grid.append(t)
        t += timedelta(minutes=every_minutes)

    # Interpolate
    res: List[Dict] = []
    j = 0
    for g in grid:
        # Advance j while next time is before g
        while j + 1 < len(times) and times[j + 1] <= g:
            j += 1
        if j == len(times) - 1:
            v = values[j]
        else:
            t0, t1 = times[j], times[j + 1]
            v0, v1 = values[j], values[j + 1]
            if t1 == t0:
                v = v0
            else:
                alpha = (g - t0).total_seconds() / (t1 - t0).total_seconds()
                v = v0 + (v1 - v0) * max(0.0, min(1.0, alpha))
        res.append({"timestamp": iso_from_dt(g), "portfolio_value": float(v)})
    return res


def ensure_demo_eod_points(demo: List[Dict]) -> List[Dict]:
    """
    Ensure specific EOD points exist in demo series using provided daily returns.
    Currently fills 2025-10-10 using +2.27% over 2025-10-09 EOD if missing.
    """
    # Index demo by day and keep the latest timestamp per day as EOD
    by_day: Dict[str, Dict] = {}
    for p in demo:
        try:
            ts = dt_from_iso(p["timestamp"])
        except Exception:
            continue
        day = ts.date().isoformat()
        cur = by_day.get(day)
        if cur is None or dt_from_iso(cur["timestamp"]) < ts:
            by_day[day] = {
                "timestamp": iso_from_dt(ts),
                "portfolio_value": float(p.get("portfolio_value", p.get("portfolioValue", 0.0)))
            }

    # Helper to check if a day has an EOD close around 23:59:59Z
    def has_eod(day: str) -> bool:
        if day not in by_day:
            return False
        # Consider any point on that calendar day as EOD if it's the latest for that day
        return True

    # Insert 2025-10-10 EOD if absent, using +2.27% over 2025-10-09 EOD
    d9 = "2025-10-09"
    d10 = "2025-10-10"
    if has_eod(d9) and not has_eod(d10):
        base9 = by_day[d9]["portfolio_value"]
        eod10 = base9 * (1.0 + 0.0227)
        demo.append({
            "timestamp": f"{d10}T23:59:59Z",
            "portfolio_value": float(eod10)
        })
    # Return sorted list
    return sorted([
        {"timestamp": iso_from_dt(dt_from_iso(p["timestamp"])), "portfolio_value": float(p.get("portfolio_value", p.get("portfolioValue", 0.0)))}
        for p in demo
    ], key=lambda x: x["timestamp"])


def merge_demo_with_admin(demo: List[Dict], admin: List[Dict]) -> List[Dict]:
    # Ensure required EODs are present in demo first (e.g., 2025-10-10)
    demo = ensure_demo_eod_points(demo)
    today = datetime.utcnow().date()
    # Keep demo records strictly before today (UTC)
    kept: List[Dict] = []
    for p in demo:
        try:
            d = dt_from_iso(p["timestamp"]).date()
            if d < today:
                kept.append({"timestamp": iso_from_dt(dt_from_iso(p["timestamp"])),
                             "portfolio_value": float(p.get("portfolio_value", p.get("portfolioValue", 0.0)))})
        except Exception:
            continue

    # Take admin points for today only
    admin_today = []
    for p in admin:
        try:
            d = dt_from_iso(p["timestamp"]).date()
            if d == today:
                admin_today.append({"timestamp": iso_from_dt(dt_from_iso(p["timestamp"])),
                                    "portfolio_value": float(p.get("portfolio_value", p.get("portfolioValue", 0.0)))})
        except Exception:
            continue

    # If admin has no intraday today, just keep demo as-is
    if not admin_today:
        return sorted(kept, key=lambda p: p["timestamp"])  # no change

    # Rebase today's admin PV to demo's opening PV (demo EOD from yesterday)
    # Find demo EOD for yesterday (today - 1 day). If not found, use last point before today.
    yesterday = (today - timedelta(days=1)).isoformat()
    demo_open_candidates = [p for p in demo if dt_from_iso(p["timestamp"]).date().isoformat() <= yesterday]
    demo_open_pv = None
    if demo_open_candidates:
        demo_open_pv = float(sorted(demo_open_candidates, key=lambda p: p["timestamp"]) [-1]["portfolio_value"])
    else:
        demo_open_pv = float(demo[-1]["portfolio_value"]) if demo else 1_000_000.0

    # Admin first point of today as base
    admin_base = float(admin_today[0]["portfolio_value"])
    rebased_today = []
    for p in admin_today:
        rel = float(p["portfolio_value"]) / admin_base if admin_base else 1.0
        rebased_today.append({
            "timestamp": p["timestamp"],
            "portfolio_value": demo_open_pv * rel
        })

    resampled_today = resample_linear(rebased_today, every_minutes=5)
    merged = kept + resampled_today
    # Ensure strictly increasing by timestamp and dedupe
    seen = set()
    unique: List[Dict] = []
    for p in sorted(merged, key=lambda x: x["timestamp"]):
        if p["timestamp"] in seen:
            continue
        seen.add(p["timestamp"])
        unique.append(p)
    return unique


def to_jsonl(points: List[Dict]) -> str:
    lines = []
    for p in points:
        lines.append(json.dumps({
            "timestamp": p["timestamp"],
            "portfolio_value": round(float(p["portfolio_value"]), 6)
        }, separators=(",", ":")))
    return "\n".join(lines) + ("\n" if lines else "")


def sync_once(verbose: bool = True) -> None:
    admin_text = read_s3_text(BUCKET, ADMIN_KEY)
    try:
        demo_text = read_s3_text(BUCKET, DEMO_KEY)
    except Exception:
        demo_text = ""

    admin_points = parse_jsonl(admin_text)
    demo_points = parse_jsonl(demo_text)

    merged = merge_demo_with_admin(demo_points, admin_points)
    out_text = to_jsonl(merged)

    write_s3_text(BUCKET, DEMO_KEY, out_text)

    if verbose:
        last = merged[-1] if merged else None
        print(f"Synced demo PV: {len(merged)} points. Latest: {last}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync demo PV with admin intraday, 5-min smoothed")
    parser.add_argument("--loop", action="store_true", help="Run forever, syncing every 5 minutes")
    args = parser.parse_args()

    if not args.loop:
        sync_once()
        return

    # Loop forever
    try:
        while True:
            start = datetime.utcnow()
            try:
                sync_once()
            except Exception as e:
                print(f"Sync iteration failed: {e}", file=sys.stderr)
            # Sleep until next 5-minute boundary
            now = datetime.utcnow()
            elapsed = (now - start).total_seconds()
            sleep_seconds = max(30, 300 - int(elapsed))
            try:
                import time
                time.sleep(sleep_seconds)
            except Exception:
                pass
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()


