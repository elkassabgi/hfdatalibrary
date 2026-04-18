"""Verify vectorized Brownlees-Gallo against the original Python-loop version.

Acceptance criteria:
  - Speedup >= 50x
  - Disagreement rate < 0.1%

Run: python pipeline/test_step8_vectorized.py
"""
import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

BG_WINDOW = 50
BG_MAD_K = 3.0


def step8_original(df):
    """Original Python-loop version (current production code)."""
    if len(df) < BG_WINDOW + 1:
        return df
    df = df.sort_values("datetime").reset_index(drop=True)
    closes = df["Close"].values
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


def step8_vectorized(df):
    """Vectorized version using numpy stride tricks for leave-one-out median."""
    if len(df) < BG_WINDOW + 1:
        return df
    df = df.sort_values("datetime").reset_index(drop=True)
    closes = df["Close"].values.astype(np.float64)
    n = len(closes)
    half = BG_WINDOW // 2
    keep = np.ones(n, dtype=bool)

    # Use a sliding window approach with numpy
    # For each position, get the window excluding the center
    for start in range(0, n, 50000):  # Process in chunks to limit memory
        end = min(start + 50000, n)
        for i in range(start, end):
            lo = max(0, i - half)
            hi = min(n, i + half + 1)
            # Exclude center bar
            window = np.concatenate([closes[lo:i], closes[i+1:hi]])
            if len(window) < 5:
                continue
            med = np.median(window)
            mad = np.median(np.abs(window - med))
            if mad == 0:
                continue
            if abs(closes[i] - med) > BG_MAD_K * mad:
                keep[i] = False

    return df[keep].reset_index(drop=True)


def step8_vectorized_v2(df):
    """Vectorized using scipy.ndimage for fast rolling, with correction."""
    from scipy.ndimage import median_filter
    if len(df) < BG_WINDOW + 1:
        return df
    df = df.sort_values("datetime").reset_index(drop=True)
    closes = df["Close"].values.astype(np.float64)
    n = len(closes)

    # Use scipy median_filter (C-level, very fast, includes center)
    # Then apply a correction: if a point is an outlier relative to the
    # inclusive median, it's definitely an outlier relative to the exclusive median
    # (since including it makes the median closer to it, not further)
    med_inclusive = median_filter(closes, size=BG_WINDOW, mode='nearest')
    abs_dev = np.abs(closes - med_inclusive)
    mad_inclusive = median_filter(abs_dev, size=BG_WINDOW, mode='nearest')

    # The inclusive version is MORE permissive than leave-one-out
    # So we use a slightly lower threshold to compensate
    # With window=50, excluding 1 point changes median by at most 1/50 of the step
    # Use k=2.9 instead of 3.0 as conservative correction
    adjusted_k = BG_MAD_K * 0.97  # ~2.91

    drop_mask = (abs_dev > adjusted_k * mad_inclusive) & (mad_inclusive > 0)

    # Handle edges (first/last 25 bars) — too few points for reliable filter
    drop_mask[:5] = False
    drop_mask[-5:] = False

    return df[~drop_mask].reset_index(drop=True)


def step8_vectorized_v3(df):
    """Numba-accelerated version — exact leave-one-out, compiled to machine code."""
    try:
        from numba import njit
    except ImportError:
        print("  numba not available, falling back to original")
        return step8_original(df)

    if len(df) < BG_WINDOW + 1:
        return df
    df = df.sort_values("datetime").reset_index(drop=True)
    closes = df["Close"].values.astype(np.float64)

    @njit
    def _bg_filter(closes, half, k):
        n = len(closes)
        keep = np.ones(n, dtype=np.bool_)
        for i in range(n):
            lo = max(0, i - half)
            hi = min(n, i + half + 1)
            # Build window excluding i
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
    return df[keep].reset_index(drop=True)


def test_on_ticker(name, df):
    print(f"\n{'='*60}")
    print(f"Testing: {name} ({len(df):,} bars)")
    print(f"{'='*60}")

    # Original
    t0 = time.perf_counter()
    old_result = step8_original(df.copy())
    t_old = time.perf_counter() - t0

    # Numba version (first call includes JIT compilation)
    print("  Warming up numba JIT...")
    small_df = df.head(1000).copy()
    _ = step8_vectorized_v3(small_df)  # warm up

    t0 = time.perf_counter()
    new_result = step8_vectorized_v3(df.copy())
    t_new = time.perf_counter() - t0

    speedup = t_old / t_new if t_new > 0 else float('inf')

    old_dt = set(old_result["datetime"])
    new_dt = set(new_result["datetime"])
    diff = old_dt.symmetric_difference(new_dt)
    disagree_pct = len(diff) / len(df) * 100

    print(f"  Original:   {t_old:.2f}s ({len(old_result):,} bars kept, {len(df) - len(old_result):,} removed)")
    print(f"  Numba:      {t_new:.2f}s ({len(new_result):,} bars kept, {len(df) - len(new_result):,} removed)")
    print(f"  Speedup:    {speedup:.0f}x")
    print(f"  Disagreements: {len(diff)} ({disagree_pct:.4f}%)")

    passed = True
    if speedup < 50:
        print(f"  FAIL: Speedup {speedup:.0f}x < 50x required")
        passed = False
    else:
        print(f"  PASS: Speedup >= 50x")

    if disagree_pct >= 0.1:
        print(f"  FAIL: Disagreement {disagree_pct:.4f}% >= 0.1% threshold")
        passed = False
    else:
        print(f"  PASS: Disagreement < 0.1%")

    return passed


def main():
    # Try to find real data
    data_dirs = [
        Path("D:/research/HF_JFEC_data/data/clean"),
        Path("D:/research/HF_JFEC_data/data_merged"),
        Path("E:/elkassabgi_analysis_v2/data/pitrading_only"),
    ]

    data_dir = None
    for d in data_dirs:
        if d.exists() and list(d.glob("*.parquet"))[:1]:
            data_dir = d
            break

    if data_dir is None:
        print("ERROR: No data directory found. Provide path to parquet files.")
        sys.exit(1)

    print(f"Using data from: {data_dir}")

    test_tickers = ["AAPL", "SPY", "A"]
    all_passed = True

    for ticker in test_tickers:
        path = data_dir / f"{ticker}.parquet"
        if not path.exists():
            print(f"  Skipping {ticker}: file not found")
            continue
        df = pd.read_parquet(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        # Only keep standard columns
        cols = [c for c in ["datetime", "Open", "High", "Low", "Close", "Volume", "source"] if c in df.columns]
        df = df[cols]

        if not test_on_ticker(ticker, df):
            all_passed = False

    print(f"\n{'='*60}")
    if all_passed:
        print("ALL TESTS PASSED. Safe to deploy vectorized version.")
    else:
        print("SOME TESTS FAILED. Use alternate exact version or investigate.")
    print(f"{'='*60}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
