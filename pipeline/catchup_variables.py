"""catchup_variables.py - one-time fill of the variables gap 2026-03-28 -> today.

The historical backfill was computed from the local 1-min snapshot (ends
2026-03-27); the daily variables refresh only ships when the branch merges. This
script closes the hole using the CURRENT 1-min bars already in R2 (the daily
OHLCV pipeline has kept those fresh): for every {version}/variables/{ticker} it
downloads the live bars and lets sync_ticker_variables compute+merge the missing
days (bounded compute, byte-exact vs full recompute).

Resumable: a ticker whose variables already reach the last expected trading day
is skipped after one small variables-file read (no bars download).

    python pipeline/catchup_variables.py            # both versions
    python pipeline/catchup_variables.py --version clean
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from r2_client import get_client, get_bucket, download_parquet, list_prefix  # noqa: E402
from variables_sync import sync_ticker_variables  # noqa: E402

MAX_NEW = 100  # ~65 trading days missing + slack


def last_expected_trading_day(today: date) -> date:
    """Most recent weekday strictly before today (holiday-agnostic: a holiday
    just makes the skip-check conservative by one day, never wrong)."""
    d = today - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", choices=["clean", "raw", "both"], default="both")
    a = ap.parse_args()
    versions = ["clean", "raw"] if a.version == "both" else [a.version]

    client = get_client()
    bucket = get_bucket()
    target = pd.Timestamp(last_expected_trading_day(date.today()))
    print(f"catch-up target: variables through >= {target.date()}", flush=True)

    for version in versions:
        keys = list_prefix(client, f"{version}/variables/", bucket)
        tickers = sorted(k.rsplit("/", 1)[1][:-8] for k in keys if k.endswith(".parquet"))
        print(f"[{version}] {len(tickers)} tickers", flush=True)
        done = updated = skipped = errors = 0
        for ticker in tickers:
            done += 1
            try:
                existing = download_parquet(client, version, ticker, timeframe="variables")
                if existing is not None and not existing.empty:
                    last = pd.to_datetime(existing["trade_date"]).max().normalize()
                    if last >= target:
                        skipped += 1
                        continue
                bars = download_parquet(client, version, ticker)  # live 1-min
                if bars is None or bars.empty:
                    errors += 1
                    print(f"  [{version}] {ticker}: no 1-min bars in R2", flush=True)
                    continue
                st = sync_ticker_variables(client, version, ticker, bars, max_new=MAX_NEW)
                updated += 1
                if updated % 50 == 0 or done == len(tickers):
                    print(f"  [{version}] {done}/{len(tickers)} processed "
                          f"(updated {updated}, current {skipped}, errors {errors}); "
                          f"last {ticker}: +{st['new_rows']} days", flush=True)
            except Exception as e:
                errors += 1
                print(f"  [{version}] {ticker}: ERROR {str(e)[:100]}", flush=True)
        print(f"[{version}] DONE: updated {updated}, already-current {skipped}, "
              f"errors {errors} of {len(tickers)}", flush=True)


if __name__ == "__main__":
    main()
