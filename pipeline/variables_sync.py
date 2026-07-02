"""variables_sync.py - per-ticker, in-pipeline refresh of the 25 academic
variables (+ quality subset) to R2.

Day-forward companion to compute_variables.py. Called from inside
daily_update.merge_ticker with the in-memory 1-minute bars already built for a
ticker, so it never bulk-downloads the corpus and reuses the pipeline's boto3
client + env credentials.

R2 keys (via r2_client.parquet_key's timeframe slot):
    {version}/variables/{ticker}.parquet   (full per-ticker daily history, 25 vars)
    {version}/quality/{ticker}.parquet      (data-quality subset for /v1/quality)

Idempotent: only genuinely-new trading days are computed (bounded to the new
day(s) + one prior day for cross-day context), then merged into the existing
per-ticker file and deduped on trade_date (keep=last). Safe to re-run.
"""
from __future__ import annotations

import pandas as pd

from compute_variables import compute_recent_days
from r2_client import download_parquet, upload_parquet

# Kept in sync with upload_variables.QUALITY_COLS.
QUALITY_COLS = ["trade_date", "ticker", "gap_rate", "observed_bars",
                "longest_gap", "max_bars_since_trade"]


def sync_ticker_variables(client, version: str, ticker: str, bars: pd.DataFrame,
                          max_new: int = 5) -> dict:
    """Compute the new day's variables from `bars` (a ticker's full in-memory
    1-minute frame for `version`) and merge them into R2. Returns a small stats
    dict. Raises on error; the caller isolates failures (see daily_update).
    max_new: cap on missing days computed in one call (5 = daily pipeline;
    catchup_variables.py passes a large value to close historical gaps)."""
    existing = download_parquet(client, version, ticker, timeframe="variables")
    existing_dates = None
    if existing is not None and not existing.empty:
        existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.normalize()
        existing_dates = existing["trade_date"].tolist()

    new_rows = compute_recent_days(bars, ticker, existing_dates=existing_dates,
                                   max_new=max_new)
    if new_rows.empty:
        return {"version": version, "ticker": ticker, "new_rows": 0}
    new_rows["trade_date"] = pd.to_datetime(new_rows["trade_date"]).dt.normalize()

    if existing is not None and not existing.empty:
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows
    merged = (merged.sort_values("trade_date")
                    .drop_duplicates(subset=["trade_date"], keep="last")
                    .reset_index(drop=True))

    upload_parquet(client, merged, version, ticker, timeframe="variables")
    qcols = [c for c in QUALITY_COLS if c in merged.columns]
    upload_parquet(client, merged[qcols], version, ticker, timeframe="quality")

    return {"version": version, "ticker": ticker, "new_rows": int(len(new_rows))}
