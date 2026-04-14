"""
clean_pipeline.py — Apply the documented 9-step cleaning pipeline to a DataFrame
of 1-minute OHLCV bars.

The rules are documented in pages/docs.html under "Methodology". This script
re-implements them from spec so the repo is self-contained.

Steps (applied in order; a bar that fails any step is removed):
  1. Remove bars outside 09:30–16:00 ET
  2. Remove bars with non-positive prices
  3. Remove OHLC violations (High < Low)
  4. Remove bars where Open or Close falls outside [Low, High]
  5. Remove duplicate timestamps (keep first)
  6. Remove bars with zero volume
  7. Remove extreme outliers (|log return| > 25%)
  8. Brownlees-Gallo adaptive filter (3 × MAD over 50-bar centered window)
  9. Splice-boundary check at PiTrading/IEX transition (March 2022)
"""
from __future__ import annotations
from typing import Optional
import math

import numpy as np
import pandas as pd
from datetime import time as time_t


MARKET_OPEN = time_t(9, 30)
MARKET_CLOSE = time_t(16, 0)
BG_WINDOW = 50
BG_MAD_K = 3.0
EXTREME_RETURN_THRESHOLD = 0.25  # 25%


def _ensure_datetime_naive_eastern(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the datetime column is parsed; assume already in Eastern Time (naive or aware).

    Bars in the library are stored with naive ET timestamps. New bars from the
    pipeline come with tz-aware ET datetimes; we strip tz to match storage.
    """
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df


def step1_market_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only bars within 09:30-15:59 ET inclusive."""
    times = df["datetime"].dt.time
    mask = (times >= MARKET_OPEN) & (times < MARKET_CLOSE)
    return df[mask]


def step2_positive_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bars where any of O/H/L/C is <= 0."""
    mask = (df["Open"] > 0) & (df["High"] > 0) & (df["Low"] > 0) & (df["Close"] > 0)
    return df[mask]


def step3_high_ge_low(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bars where High < Low (impossible)."""
    return df[df["High"] >= df["Low"]]


def step4_open_close_in_range(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bars where Open or Close falls outside [Low, High]."""
    mask = (
        (df["Open"] >= df["Low"]) & (df["Open"] <= df["High"]) &
        (df["Close"] >= df["Low"]) & (df["Close"] <= df["High"])
    )
    return df[mask]


def step5_dedupe_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Keep first occurrence of each timestamp."""
    return df.drop_duplicates(subset=["datetime"], keep="first")


def step6_nonzero_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bars with zero volume."""
    return df[df["Volume"] > 0]


def step7_extreme_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bars whose log return exceeds the extreme threshold."""
    if len(df) < 2:
        return df
    df = df.sort_values("datetime").reset_index(drop=True)
    log_close = np.log(df["Close"].values)
    log_ret = np.diff(log_close)
    extreme = np.abs(log_ret) > EXTREME_RETURN_THRESHOLD
    # Drop the bar where the extreme return ends (i+1)
    keep = np.ones(len(df), dtype=bool)
    keep[1:] = ~extreme
    return df[keep]


def step8_brownlees_gallo(df: pd.DataFrame) -> pd.DataFrame:
    """Brownlees-Gallo adaptive outlier filter — numba-accelerated.

    For each bar, compare its Close to the median of a 50-bar centered window
    (excluding the bar itself). If |close - median| > 3 * MAD of the window,
    drop the bar. Exact leave-one-out, JIT-compiled for ~15-20x speedup.
    """
    if len(df) < BG_WINDOW + 1:
        return df

    df = df.sort_values("datetime").reset_index(drop=True)
    closes = df["Close"].values.astype(np.float64)

    try:
        from numba import njit

        @njit(cache=True)
        def _bg_filter(closes, half, k):
            n = len(closes)
            keep = np.ones(n, dtype=np.bool_)
            for i in range(n):
                lo = max(0, i - half)
                hi = min(n, i + half + 1)
                wsize = hi - lo - 1
                if wsize < 5:
                    continue
                w = np.empty(wsize)
                idx = 0
                for j in range(lo, hi):
                    if j != i:
                        w[idx] = closes[j]
                        idx += 1
                med = np.median(w)
                mad = np.median(np.abs(w - med))
                if mad == 0:
                    continue
                if abs(closes[i] - med) > k * mad:
                    keep[i] = False
            return keep

        keep = _bg_filter(closes, BG_WINDOW // 2, BG_MAD_K)
    except ImportError:
        # Fallback: original Python loop if numba not available
        n = len(closes)
        keep = np.ones(n, dtype=bool)
        half = BG_WINDOW // 2
        for i in range(n):
            lo = max(0, i - half)
            hi = min(n, i + half + 1)
            window = np.concatenate([closes[lo:i], closes[i + 1:hi]])
            if len(window) < 5:
                continue
            median = np.median(window)
            mad = np.median(np.abs(window - median))
            if mad == 0:
                continue
            if abs(closes[i] - median) > BG_MAD_K * mad:
                keep[i] = False

    return df[keep]


def step9_splice_check(df: pd.DataFrame) -> pd.DataFrame:
    """Verify continuity at the PiTrading/IEX splice (March 2022).

    For new IEX data, this is a no-op since we never have a splice within new bars.
    Kept for documentation and pipeline compatibility.
    """
    return df


def clean_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full 9-step cleaning pipeline. Returns a cleaned, sorted DataFrame."""
    if df.empty:
        return df
    df = _ensure_datetime_naive_eastern(df)
    df = step1_market_hours(df)
    df = step2_positive_prices(df)
    df = step3_high_ge_low(df)
    df = step4_open_close_in_range(df)
    df = step5_dedupe_timestamps(df)
    df = step6_nonzero_volume(df)
    df = step7_extreme_returns(df)
    df = step8_brownlees_gallo(df)
    df = step9_splice_check(df)
    return df.sort_values("datetime").reset_index(drop=True)


if __name__ == "__main__":
    # Smoke test
    sample = pd.DataFrame({
        "datetime": pd.to_datetime([
            "2026-04-10 09:30:00",
            "2026-04-10 09:31:00",
            "2026-04-10 09:32:00",
            "2026-04-10 09:33:00",  # outlier
            "2026-04-10 09:34:00",
            "2026-04-10 16:30:00",  # after hours
            "2026-04-10 09:35:00",
        ]),
        "Open":   [100.0, 100.5, 100.2, 200.0, 100.4, 100.0, 100.6],
        "High":   [100.6, 100.8, 100.5, 200.5, 100.7, 100.0, 100.9],
        "Low":    [99.9,  100.3, 100.0, 199.5, 100.2, 100.0, 100.4],
        "Close":  [100.5, 100.4, 100.3, 200.2, 100.5, 100.0, 100.7],
        "Volume": [1000, 1500, 800, 500, 1200, 0, 900],
        "source": ["iex"] * 7,
    })
    print("Before:", len(sample), "bars")
    cleaned = clean_bars(sample)
    print("After:", len(cleaned), "bars")
    print(cleaned)
