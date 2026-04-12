"""
daily_update.py — Main entry point for the daily IEX HIST update.

Pipeline:
  1. Determine the previous trading day
  2. Fetch the IEX HIST manifest for that day
  3. Download the TOPS pcap.gz file (~9 GB compressed)
  4. Run Go-based pcap_extract binary to filter trades → CSV (~50 MB)
  5. Read trades CSV with Python, build 1-min bars
  6. For each ticker with new bars:
       a. Download existing raw parquet from R2
       b. Append new bars (deduped by timestamp)
       c. Upload back to R2 (raw)
       d. Apply 9-step cleaning → upload as clean
       e. Regenerate 7 aggregated timeframes (raw + clean) → upload
  7. Update metadata.json with new timestamps and bar counts
  8. Send success/failure email via Resend

Usage:
  python daily_update.py             # process previous trading day
  python daily_update.py 2026-04-09  # process a specific date
  python daily_update.py --dry-run   # dry run, no R2 writes
"""
from __future__ import annotations
import os
import sys
import json
import time
import argparse
import subprocess
import tempfile
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Set, Dict, List, Optional
import urllib.request

import pandas as pd

# Pipeline modules (same directory)
sys.path.insert(0, str(Path(__file__).parent))
from trading_days import previous_trading_day
from iex_manifest import get_tops_pcap_for_date
from tops_parser import parse_trades_csv, Trade
from build_bars import build_bars, bars_to_dataframe
from clean_pipeline import clean_bars
from aggregate import aggregate_all, TIMEFRAMES
from r2_client import (
    get_client, get_bucket, download_parquet, upload_parquet, upload_csv,
)

PIPELINE_DIR = Path(__file__).parent
GO_EXTRACTOR = PIPELINE_DIR / "pcap_extract" / "pcap_extract"


METADATA_PATH = Path(__file__).parent.parent / "data" / "metadata.json"
TICKERS_PATH = Path(__file__).parent.parent / "data" / "tickers.json"


def load_universe() -> Set[str]:
    """Load the 1,391 ticker universe from data/tickers.json."""
    if not TICKERS_PATH.exists():
        raise RuntimeError(
            f"{TICKERS_PATH} not found. Run pipeline/seed_tickers.py first to generate it."
        )
    with open(TICKERS_PATH) as f:
        tickers = json.load(f)
    return set(tickers)


def download_pcap(url: str, dest_path: str) -> int:
    """Download a remote pcap file to a local path. Returns bytes downloaded."""
    print(f"[download] Downloading to {dest_path}...", flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "hfdatalibrary-pipeline/1.0"})
    bytes_total = 0
    last_report = time.time()
    with urllib.request.urlopen(req, timeout=600) as response, open(dest_path, "wb") as out:
        while True:
            chunk = response.read(8 * 1024 * 1024)  # 8 MB chunks
            if not chunk:
                break
            out.write(chunk)
            bytes_total += len(chunk)
            now = time.time()
            if now - last_report > 10:
                print(f"[download]   {bytes_total/1e9:.2f} GB downloaded...", flush=True)
                last_report = now
    return bytes_total


def run_go_extractor(pcap_path: str, tickers_path: str, output_csv: str) -> None:
    """Run the Go-based pcap_extract binary to filter trades from a pcap.gz file.

    The Go binary reads the full pcap (~9 GB), parses IEX-TP/TOPS at native speed,
    filters to the ticker universe, and outputs a small CSV of trade reports.
    Expected runtime: 2-5 minutes for a full day's pcap.
    """
    extractor = str(GO_EXTRACTOR)
    if not os.path.isfile(extractor):
        raise RuntimeError(
            f"Go extractor not found at {extractor}. "
            "Build it with: cd pipeline/pcap_extract && go build -o pcap_extract ."
        )

    cmd = [
        extractor,
        "-input", pcap_path,
        "-tickers", tickers_path,
        "-output", output_csv,
    ]
    print(f"[go_extract] Running: {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, capture_output=False, timeout=1800)  # 30 min max
    if result.returncode != 0:
        raise RuntimeError(f"Go extractor failed with exit code {result.returncode}")


def parse_day(d: date, universe: Set[str]) -> pd.DataFrame:
    """Fetch, extract, and parse the TOPS pcap for one date.

    Steps:
      1. Download pcap.gz from IEX HIST (~9 GB compressed)
      2. Run Go extractor to filter trades → CSV (~50 MB)
      3. Delete the pcap.gz to free disk
      4. Read the CSV with Python

    Returns a DataFrame of all 1-minute bars for all tickers in the universe.
    Returns an empty DataFrame if no pcap is published for that date.
    """
    print(f"[parse_day] {d}: looking up manifest", flush=True)
    entry = get_tops_pcap_for_date(d)
    if entry is None:
        print(f"[parse_day] {d}: no TOPS pcap published", flush=True)
        return pd.DataFrame()

    url = entry["link"]
    raw_size = entry.get("size") or 0
    try:
        size_mb = float(raw_size) / 1e6
    except (TypeError, ValueError):
        size_mb = 0
    print(f"[parse_day] {d}: source pcap ~{size_mb:.0f} MB at {url}", flush=True)

    tmp_dir = tempfile.mkdtemp(prefix="iex_")
    pcap_path = os.path.join(tmp_dir, f"iex_tops_{d.strftime('%Y%m%d')}.pcap.gz")
    csv_path = os.path.join(tmp_dir, "trades.csv")

    try:
        # Step 1: Download
        t0 = time.time()
        bytes_downloaded = download_pcap(url, pcap_path)
        t_dl = time.time() - t0
        print(f"[parse_day] {d}: downloaded {bytes_downloaded/1e9:.2f} GB in {t_dl/60:.1f} min", flush=True)

        # Step 2: Run Go extractor
        t1 = time.time()
        tickers_path = str(TICKERS_PATH)
        run_go_extractor(pcap_path, tickers_path, csv_path)
        t_extract = time.time() - t1
        csv_size = os.path.getsize(csv_path) if os.path.exists(csv_path) else 0
        print(f"[parse_day] {d}: Go extracted trades in {t_extract/60:.1f} min ({csv_size/1e6:.1f} MB CSV)", flush=True)

        # Step 3: Delete pcap to free disk
        os.remove(pcap_path)
        print(f"[parse_day] {d}: deleted pcap, freed {bytes_downloaded/1e9:.2f} GB", flush=True)

        # Step 4: Read trades CSV with Python
        trades_count = 0
        by_symbol: Dict[str, List] = {}

        for trade in parse_trades_csv(csv_path, universe=universe):
            trades_count += 1
            by_symbol.setdefault(trade.symbol, []).append(trade)

        print(f"[parse_day] {d}: read {trades_count:,} trades for {len(by_symbol):,} tickers", flush=True)

    finally:
        # Cleanup temp files
        for f in [pcap_path, csv_path]:
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass

    if trades_count == 0:
        return pd.DataFrame()

    # Build bars from trades
    print(f"[parse_day] building 1-min bars...")
    bars_by_symbol = {}
    for symbol, trades in by_symbol.items():
        symbol_bars = build_bars(trades).get(symbol, [])
        if symbol_bars:
            bars_by_symbol[symbol] = symbol_bars

    print(f"[parse_day] built bars for {len(bars_by_symbol)} tickers")

    # Combine into one DataFrame keyed by ticker
    rows = []
    for symbol, bars in bars_by_symbol.items():
        for b in bars:
            rows.append({
                "ticker": symbol,
                "datetime": b.minute_start,
                "Open": b.open,
                "High": b.high,
                "Low": b.low,
                "Close": b.close,
                "Volume": b.volume,
                "source": "iex",
            })

    return pd.DataFrame(rows)


def merge_ticker(client, ticker: str, new_bars: pd.DataFrame, dry_run: bool = False) -> dict:
    """Merge a single ticker's new bars into R2.

    Steps:
      1. Download existing raw parquet
      2. Append new bars (dedup on datetime)
      3. Upload raw
      4. Clean → upload clean
      5. Aggregate → upload all timeframes (raw + clean)
      6. CSV versions for raw and clean

    Returns a stats dict.
    """
    stats = {"ticker": ticker, "new_bars": len(new_bars), "uploaded_bytes": 0}

    # Standard columns we care about (keeps schema consistent across legacy + new data)
    STANDARD_COLS = ["datetime", "Open", "High", "Low", "Close", "Volume", "source"]

    # Strip the ticker column and ensure naive ET datetime
    new_bars = new_bars.drop(columns=["ticker"]).copy()
    new_bars["datetime"] = pd.to_datetime(new_bars["datetime"])
    if new_bars["datetime"].dt.tz is not None:
        new_bars["datetime"] = new_bars["datetime"].dt.tz_localize(None)
    # Keep only standard columns, in standard order
    new_bars = new_bars[[c for c in STANDARD_COLS if c in new_bars.columns]]

    # 1. Download existing raw
    existing_raw = download_parquet(client, "raw", ticker)
    if existing_raw is not None and len(existing_raw) > 0:
        # Drop legacy boolean flags and any other extra columns
        existing_raw = existing_raw[[c for c in STANDARD_COLS if c in existing_raw.columns]]
        merged_raw = pd.concat([existing_raw, new_bars], ignore_index=True)
    else:
        merged_raw = new_bars

    # 2. Dedupe by datetime, keep last (newest data wins on conflict)
    merged_raw = merged_raw.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    merged_raw = merged_raw.reset_index(drop=True)
    stats["raw_total_bars"] = len(merged_raw)

    if dry_run:
        print(f"[merge_ticker] DRY RUN {ticker}: would write {len(merged_raw)} raw bars")
        return stats

    # 3. Upload raw 1-min
    stats["uploaded_bytes"] += upload_parquet(client, merged_raw, "raw", ticker, "1min")
    stats["uploaded_bytes"] += upload_csv(client, merged_raw, "raw", ticker, "1min")

    # 4. Clean
    cleaned = clean_bars(merged_raw)
    stats["clean_total_bars"] = len(cleaned)
    stats["uploaded_bytes"] += upload_parquet(client, cleaned, "clean", ticker, "1min")
    stats["uploaded_bytes"] += upload_csv(client, cleaned, "clean", ticker, "1min")

    # 5. Aggregate raw + clean for all timeframes
    raw_aggs = aggregate_all(merged_raw)
    clean_aggs = aggregate_all(cleaned)

    for tf_name in TIMEFRAMES:
        if not raw_aggs[tf_name].empty:
            stats["uploaded_bytes"] += upload_parquet(client, raw_aggs[tf_name], "raw", ticker, tf_name)
        if not clean_aggs[tf_name].empty:
            stats["uploaded_bytes"] += upload_parquet(client, clean_aggs[tf_name], "clean", ticker, tf_name)

    return stats


def update_metadata(d: date, total_new_bars: int, tickers_updated: int) -> None:
    """Update metadata.json with the new data timestamp and counts."""
    if not METADATA_PATH.exists():
        print(f"[update_metadata] WARN: {METADATA_PATH} not found, skipping")
        return

    with open(METADATA_PATH) as f:
        meta = json.load(f)

    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    meta["data_updated"] = now_iso
    meta["website_updated"] = now_iso
    meta["end_date"] = d.isoformat()
    meta["update_summary"] = (
        f"Daily update: added trading data for {d.isoformat()} "
        f"({total_new_bars:,} new bars across {tickers_updated:,} tickers)."
    )

    with open(METADATA_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[update_metadata] {METADATA_PATH} updated")


def send_email(subject: str, body_html: str) -> None:
    """Send a notification email via Resend."""
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        print("[send_email] RESEND_API_KEY not set, skipping email")
        return
    import requests
    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "from": "HF Data Library Pipeline <noreply@hfdatalibrary.com>",
            "to": ["admin@hfdatalibrary.com"],
            "subject": subject,
            "html": body_html,
        },
        timeout=30,
    )
    if not r.ok:
        print(f"[send_email] FAILED: {r.status_code} {r.text[:300]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", help="YYYY-MM-DD (default: previous trading day)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to R2")
    args = parser.parse_args()

    start = time.time()

    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        d = previous_trading_day()
    print(f"=== Daily update for {d} ===")

    try:
        # 1. Load universe
        universe = load_universe()
        print(f"Universe: {len(universe)} tickers")

        # 2. Parse the day
        new_bars = parse_day(d, universe)
        if new_bars.empty:
            send_email(
                f"Daily pipeline: no data for {d}",
                f"<p>No TOPS pcap published for {d}, or no trades for our universe. Skipping update.</p>"
            )
            print("Done (no data).")
            return 0

        print(f"Total new bars: {len(new_bars):,}")

        # 3. Merge per ticker
        if not args.dry_run:
            client = get_client()
        else:
            client = None
            print("DRY RUN — no R2 writes")

        tickers_updated = 0
        total_uploaded = 0
        per_ticker = new_bars.groupby("ticker")
        total_tickers = len(per_ticker)

        for i, (ticker, group) in enumerate(per_ticker, 1):
            if i % 50 == 0:
                print(f"  [{i}/{total_tickers}] processing {ticker} ...")
            try:
                stats = merge_ticker(client, ticker, group, dry_run=args.dry_run)
                tickers_updated += 1
                total_uploaded += stats.get("uploaded_bytes", 0)
            except Exception as e:
                print(f"  [{i}/{total_tickers}] {ticker} FAILED: {e}")

        # 4. Update metadata
        if not args.dry_run:
            update_metadata(d, len(new_bars), tickers_updated)

        elapsed = time.time() - start
        body = (
            f"<h2>Daily update succeeded</h2>"
            f"<p><strong>Date:</strong> {d}</p>"
            f"<p><strong>Tickers updated:</strong> {tickers_updated:,}</p>"
            f"<p><strong>New bars:</strong> {len(new_bars):,}</p>"
            f"<p><strong>Bytes uploaded:</strong> {total_uploaded/1e6:.1f} MB</p>"
            f"<p><strong>Elapsed:</strong> {elapsed/60:.1f} minutes</p>"
        )
        send_email(f"Daily update OK: {d} ({tickers_updated} tickers)", body)
        print(f"Done in {elapsed/60:.1f} min")
        return 0

    except Exception as e:
        tb = traceback.format_exc()
        print(f"FATAL: {e}\n{tb}")
        send_email(
            f"Daily pipeline FAILED: {d}",
            f"<h2>Daily update failed</h2><p><strong>Date:</strong> {d}</p>"
            f"<pre>{tb}</pre>"
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
