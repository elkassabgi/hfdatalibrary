"""compute_variables.py - the 25 advertised academic variables, computed correctly.

Computes EXACTLY the 25 variables published at hfdatalibrary.com/pages/dictionary,
per ticker per trading day, from 1-minute bars - for a given cleaning version
(clean or raw). Replaces the orphaned legacy_scripts/compute_academic_vars.py,
which computed a different ~16-measure set (missing ~13 of the advertised 25) and
only for the clean version.

The 25 (dictionary numbering):
  Volatility:   1 rv_5min  2 rv_1min  3 bipower_variation  4 parkinson  5 rogers_satchell
  Spreads:      6 roll_spread_bps  7 corwin_schultz_bps
  Autocorr:     8 ac1  9 vr5  10 vr10
  Jumps:        11 bns_z  12 bns_jump_1pct  13 bns_jump_5pct
  Liquidity:    14 amihud_illiquidity  15 dollar_volume  16 share_volume  17 num_trades
  Data quality: 18 gap_rate  19 observed_bars  20 longest_gap  21 max_bars_since_trade
  Returns:      22 open_to_close_return  23 overnight_return  24 hl_range  25 intraday_return_std

NOTE ON #11 (BNS z): the dictionary prints a simplified form. This implements the
standard Barndorff-Nielsen & Shephard (2006) / Huang-Tauchen (2005) *ratio* jump
statistic with tri-power quarticity - the defensible academic test the dictionary
references. (Flagged for a dictionary wording update.) #5 yang_zhang uses the
per-day Rogers-Satchell component (the drift-independent core of Yang-Zhang 2000;
the full multi-day YZ also needs overnight/open variances).

Usage:
    from pipeline.compute_variables import compute_ticker
    daily = compute_ticker("path/to/AAPL.parquet", ticker="AAPL")   # -> daily DataFrame

    python pipeline/compute_variables.py --version clean --tickers AAPL MSFT
    python pipeline/compute_variables.py --version clean   # all tickers (multiprocessing)
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---- constants (BNS) -------------------------------------------------------
_MU43 = 2.0 ** (2.0 / 3.0) * math.gamma(7.0 / 6.0) / math.gamma(0.5)  # E|Z|^(4/3)
_THETA = (math.pi ** 2) / 4.0 + math.pi - 5.0        # ~0.6090 (Huang-Tauchen)
SESSION_MINUTES = 390                                 # regular 09:30-16:00 grid

# default local source (the existing 1-min stores) + output
SRC = {"clean": Path("D:/research/HF_JFEC_data/data_versions/clean"),
       "raw": Path("D:/research/HF_JFEC_data/data_versions/raw")}
OUT = Path("D:/research/hfdatalibrary/data/variables")   # {version}/{ticker}_vars.parquet


def _log_returns(prices: np.ndarray) -> np.ndarray:
    p = prices[prices > 0]
    if p.size < 2:
        return np.empty(0)
    return np.diff(np.log(p))


def _day_vars(g: pd.DataFrame) -> pd.Series:
    """All within-day variables for one trading day (bars sorted by time)."""
    close = g["Close"].to_numpy(dtype=float)
    high = g["High"].to_numpy(dtype=float)
    low = g["Low"].to_numpy(dtype=float)
    vol = g["Volume"].to_numpy(dtype=float)
    o = float(g["Open"].iloc[0]); c = float(close[-1])
    hi = float(np.nanmax(high)); lo = float(np.nanmin(low[low > 0])) if (low > 0).any() else np.nan

    # clock-based sampling (NOT every-Nth-bar, which is wrong when bars are missing)
    ts = pd.to_datetime(g["datetime"].to_numpy())
    cs = pd.Series(np.where(close > 0, close, np.nan), index=ts)
    r1 = _log_returns(close)
    # right-closed/right-labelled so 5/10-min closes land on the open-aligned grid
    # (09:30,09:35,...,16:00) and each k-min return == sum of its k 1-min returns
    c5 = cs.resample("5min", label="right", closed="right").last().dropna().to_numpy()
    c10 = cs.resample("10min", label="right", closed="right").last().dropna().to_numpy()
    r5 = np.diff(np.log(c5)) if c5.size >= 2 else np.empty(0)
    r10 = np.diff(np.log(c10)) if c10.size >= 2 else np.empty(0)
    M = r1.size

    # --- volatility ---
    rv_1min = float(np.sum(r1 ** 2)) if M else np.nan
    rv_5min = float(np.sum(r5 ** 2)) if r5.size else np.nan
    if M >= 2:
        ar = np.abs(r1)
        bipower = (math.pi / 2.0) * float(np.sum(ar[1:] * ar[:-1]))
    else:
        bipower = np.nan
    parkinson = (math.log(hi / lo) ** 2) / (4.0 * math.log(2.0)) if (hi > 0 and lo > 0 and hi >= lo) else np.nan
    # Rogers-Satchell (the drift-independent per-day core of Yang-Zhang 2000)
    if hi > 0 and lo > 0 and o > 0 and c > 0:
        rogers_satchell = (math.log(hi / o) * math.log(hi / c) + math.log(lo / o) * math.log(lo / c))
        rogers_satchell = max(rogers_satchell, 0.0)
    else:
        rogers_satchell = np.nan

    # --- spreads (basis points) ---
    if M >= 3:
        cov = np.cov(r1[1:], r1[:-1])[0, 1]
        roll_bps = (2.0 * math.sqrt(-cov)) * 1e4 if cov < 0 else 0.0
    else:
        roll_bps = np.nan
    corwin_schultz_bps = _corwin_schultz(high, low) * 1e4

    # --- autocorrelation / variance ratios ---
    if M >= 3 and np.std(r1) > 0:
        ac1 = float(np.corrcoef(r1[1:], r1[:-1])[0, 1])
    else:
        ac1 = np.nan
    v1 = np.var(r1) if M else np.nan
    vr5 = float(np.var(r5) / (5.0 * v1)) if (r5.size and v1 and v1 > 0) else np.nan
    vr10 = float(np.var(r10) / (10.0 * v1)) if (r10.size and v1 and v1 > 0) else np.nan

    # --- jumps (BNS 2006 ratio test, tri-power quarticity, 5-min sampling) ---
    # Run at 5-min, not 1-min: 1-min RV is inflated by microstructure noise vs BV,
    # which biases the ratio test toward massive over-detection (~50% of days).
    bns_z = _bns_z(r5)
    bns_1 = (1 if (bns_z == bns_z and bns_z > 2.326) else 0)   # NaN-safe
    bns_5 = (1 if (bns_z == bns_z and bns_z > 1.645) else 0)

    # --- liquidity ---
    dollar_volume = float(np.sum(close * vol))
    share_volume = float(np.sum(vol))
    num_trades = int((vol > 0).sum())   # 1-minute bars with actual trading (Volume>0)

    # --- data quality (390-minute session grid) ---
    obs_bars, gap_rate, longest_gap, max_since = _quality(g)

    # --- returns (open-to-close, hl range; overnight added across days) ---
    o2c = math.log(c / o) if (o > 0 and c > 0) else np.nan
    hl_range = math.log(hi / lo) if (hi > 0 and lo > 0 and hi >= lo) else np.nan
    intraday_std = float(np.std(r1)) if M else np.nan

    return pd.Series({
        "rv_5min": rv_5min, "rv_1min": rv_1min, "bipower_variation": bipower,
        "parkinson": parkinson, "rogers_satchell": rogers_satchell,
        "roll_spread_bps": roll_bps, "corwin_schultz_bps": corwin_schultz_bps,
        "ac1": ac1, "vr5": vr5, "vr10": vr10,
        "bns_z": bns_z, "bns_jump_1pct": bns_1, "bns_jump_5pct": bns_5,
        "amihud_illiquidity": np.nan,  # overwritten in compute_ticker (needs prior close)
        "dollar_volume": dollar_volume,
        "share_volume": share_volume, "num_trades": num_trades,
        "gap_rate": gap_rate, "observed_bars": obs_bars,
        "longest_gap": longest_gap, "max_bars_since_trade": max_since,
        "open_to_close_return": o2c, "hl_range": hl_range,
        "intraday_return_std": intraday_std,
    })


def _bns_z(r: np.ndarray) -> float:
    """BNS (2006) / Huang-Tauchen (2005) ratio jump statistic, computed on the
    given (5-minute) return series with tri-power quarticity. ~N(0,1) under no jump."""
    M = r.size
    if M < 4:
        return np.nan
    rv = float(np.sum(r ** 2))
    ar = np.abs(r)
    bv = (math.pi / 2.0) * float(np.sum(ar[1:] * ar[:-1]))
    if not (bv > 0 and rv > 0):
        return np.nan
    arq = ar ** (4.0 / 3.0)
    tp = M * (_MU43 ** -3) * float(np.sum(arq[2:] * arq[1:-1] * arq[:-2]))
    rj = (rv - bv) / rv
    denom = math.sqrt(_THETA * max(1.0, tp / (bv ** 2)))
    if denom <= 0:
        return np.nan
    return math.sqrt(M) * rj / denom


def _corwin_schultz(high: np.ndarray, low: np.ndarray) -> float:
    """Corwin-Schultz (2012) high-low spread, two consecutive half-day windows."""
    n = high.size
    if n < 20:
        return np.nan
    mid = n // 2
    h1, l1 = float(np.max(high[:mid])), float(np.min(low[:mid][low[:mid] > 0])) if (low[:mid] > 0).any() else np.nan
    h2, l2 = float(np.max(high[mid:])), float(np.min(low[mid:][low[mid:] > 0])) if (low[mid:] > 0).any() else np.nan
    hf, lf = float(np.max(high)), float(np.min(low[low > 0])) if (low > 0).any() else np.nan
    if not all(x == x and x > 0 for x in (h1, l1, h2, l2, hf, lf)) or h1 <= l1 or h2 <= l2 or hf <= lf:
        return np.nan
    beta = math.log(h1 / l1) ** 2 + math.log(h2 / l2) ** 2
    gamma = math.log(hf / lf) ** 2
    den = 3.0 - 2.0 * math.sqrt(2.0)
    alpha = (math.sqrt(2.0 * beta) - math.sqrt(beta)) / den - math.sqrt(gamma / den)
    spread = 2.0 * (math.exp(alpha) - 1.0) / (1.0 + math.exp(alpha))
    return max(spread, 0.0)


def _quality(g: pd.DataFrame):
    """Observed bars, gap rate, longest gap, max bars since last trade vs the
    390-minute regular session grid (09:30-15:59)."""
    dt = g["datetime"]
    mins = dt.dt.hour * 60 + dt.dt.minute
    sess = mins[(mins >= 570) & (mins <= 959)]          # 9:30=570 .. 15:59=959
    idx = np.unique((sess - 570).to_numpy())            # minute-of-session 0..389
    obs = int(idx.size)
    if obs == 0:
        return 0, 1.0, SESSION_MINUTES, SESSION_MINUTES
    # Early-close-aware grid: a dense session ending by ~13:10 (minute 220) is an
    # NYSE early-close half-day (1pm close -> 210 bars), not a gappy full day, so
    # score it against the shorter session - else the full 390-grid.
    last_ms = int(idx.max())
    expected = (last_ms + 1) if (last_ms <= 220 and obs >= 100) else SESSION_MINUTES
    gap_rate = float(min(max(1.0 - obs / expected, 0.0), 1.0))
    present = np.zeros(expected, dtype=bool)
    present[idx[idx < expected]] = True
    # longest run of consecutive missing minutes
    longest = cur = 0
    for v in present:
        cur = 0 if v else cur + 1
        longest = max(longest, cur)
    # max bars between consecutive observed bars
    diffs = np.diff(idx)
    max_since = int(diffs.max()) if diffs.size else 1
    return obs, gap_rate, int(longest), max_since


def compute_ticker(path, ticker: str) -> pd.DataFrame:
    """Compute the 25 daily variables for one ticker's 1-minute parquet file."""
    df = pd.read_parquet(path, columns=["datetime", "Open", "High", "Low", "Close", "Volume"])
    if df.empty:
        return pd.DataFrame()
    if df["datetime"].dtype == "object":
        df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")
    df["trade_date"] = df["datetime"].dt.normalize()
    daily = df.groupby("trade_date", sort=True).apply(_day_vars, include_groups=False)
    daily = daily.reset_index()
    # #23 overnight return = ln(open_today / close_yesterday) - needs prior day
    first_open = df.groupby("trade_date")["Open"].first()
    last_close = df.groupby("trade_date")["Close"].last()
    overnight = np.log(first_open / last_close.shift(1))
    daily["overnight_return"] = daily["trade_date"].map(overnight).to_numpy()
    # Amihud (#14): |close-to-close daily return| / dollar volume (Amihud 2002)
    ctc = np.log(last_close / last_close.shift(1))
    dv = daily.set_index("trade_date")["dollar_volume"]
    daily["amihud_illiquidity"] = daily["trade_date"].map(ctc.abs() / dv).to_numpy()
    daily["ticker"] = ticker
    return daily


# ---- batch runner ----------------------------------------------------------
def _process(args):
    path, ticker, version, resume = args
    out_dir = OUT / version
    out_file = out_dir / f"{ticker}_vars.parquet"
    if resume and out_file.exists():
        return ticker, "skipped"
    try:
        daily = compute_ticker(path, ticker)
        if daily.empty:
            return ticker, "empty"
        out_dir.mkdir(parents=True, exist_ok=True)
        daily.to_parquet(out_file, compression="zstd")
        return ticker, f"ok:{len(daily)}"
    except Exception as e:  # noqa: BLE001
        return ticker, f"error:{e}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", choices=["clean", "raw"], default="clean")
    ap.add_argument("--tickers", nargs="*", help="specific tickers (default: all)")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    a = ap.parse_args()

    src = SRC[a.version]
    files = sorted(src.glob("*.parquet"))
    if a.tickers:
        want = {t.upper() for t in a.tickers}
        files = [f for f in files if f.stem.upper() in want]
    tasks = [(f, f.stem.upper(), a.version, a.resume) for f in files]
    print(f"computing 25 variables for {len(tasks)} tickers ({a.version}) -> {OUT/a.version}", flush=True)

    from multiprocessing import Pool
    ok = err = skip = 0
    with Pool(a.workers) as pool:
        for i, (ticker, status) in enumerate(pool.imap_unordered(_process, tasks), 1):
            if status.startswith("ok"): ok += 1
            elif status == "skipped": skip += 1
            elif status.startswith("error"): err += 1; print(f"  {ticker}: {status}", flush=True)
            if i % 100 == 0:
                print(f"  {i}/{len(tasks)} done (ok={ok} skip={skip} err={err})", flush=True)
    print(f"DONE: ok={ok} skipped={skip} errors={err}", flush=True)


if __name__ == "__main__":
    main()
