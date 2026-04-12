"""
daily_update.py — Main entry point for the daily IEX HIST update.

Pipeline:
  1. Determine the previous trading day
  2. Fetch the IEX HIST manifest for that day
  3. Stream-download the TOPS pcap (gzipped, decompress on the fly)
  4. Parse trades and filter to our 1,391 ticker universe
  5. Build 1-min bars
  6. For each ticker with new bars:
       a. Download existing raw parquet from R2
       b. Append new bars (deduped by timestamp)
       c. Upload back to R2 (raw)
       d. Apply 9-step cleaning → upload as clean
       e. Regenerate 7 aggregated timeframes (raw + clean) → upload
       f. Regenerate CSV versions → upload
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
import gzip
import json
import time
import argparse
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
from tops_parser import parse_tops_pcap, Trade
from build_bars import build_bars, bars_to_dataframe
from clean_pipeline import clean_bars
from aggregate import aggregate_all, TIMEFRAMES
from r2_client import (
    get_client, get_bucket, download_parquet, upload_parquet, upload_csv,
)


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


def stream_pcap(url: str) -> "io.BufferedReader":
    """Open a streaming reader over a remote pcap (or pcap.gz) URL.

    The reader is wrapped in gzip.GzipFile if the URL ends with .gz.
    No full file is ever written to disk.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "hfdatalibrary-pipeline/1.0"})
    response = urllib.request.urlopen(req, timeout=600)
    if url.endswith(".gz"):
        return gzip.GzipFile(fileobj=response)
    return response


def parse_day(d: date, universe: Set[str]) -> pd.DataFrame:
    """Fetch and parse the TOPS pcap for one date.

    Returns a DataFrame of all 1-minute bars for all tickers in the universe.
    Returns an empty DataFrame if no pcap is published for that date.
    """
    print(f"[parse_day] {d}: looking up manifest")
    entry = get_tops_pcap_for_date(d)
    if entry is None:
        print(f"[parse_day] {d}: no TOPS pcap published")
        return pd.DataFrame()

    url = entry["link"]
    raw_size = entry.get("size") or 0
    try:
        size_mb = float(raw_size) / 1e6
    except (TypeError, ValueError):
        size_mb = 0
    print(f"[parse_day] {d}: streaming {url} (~{size_mb:.0f} MB)")

    pcap_stream = stream_pcap(url)

    trades_count = 0
    by_symbol: Dict[str, List] = {}

    for trade in parse_tops_pcap(pcap_stream, universe=universe):
        trades_count += 1
        by_symbol.setdefault(trade.symbol, []).append(trade)
        if trades_count % 1_000_000 == 0:
            print(f"[parse_day]   parsed {trades_count:,} trades...")

    print(f"[parse_day] {d}: parsed {trades_count:,} trades for {len(by_symbol):,} tickers")

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

    # Strip the ticker column and ensure naive ET datetime
    new_bars = new_bars.drop(columns=["ticker"]).copy()
    new_bars["datetime"] = pd.to_datetime(new_bars["datetime"])
    if new_bars["datetime"].dt.tz is not None:
        new_bars["datetime"] = new_bars["datetime"].dt.tz_localize(None)

    # 1. Download existing raw
    existing_raw = download_parquet(client, "raw", ticker)
    if existing_raw is not None and len(existing_raw) > 0:
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
