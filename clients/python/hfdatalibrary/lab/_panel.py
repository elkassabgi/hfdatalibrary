"""load_panel - pull one field for many tickers into a wide, aligned DataFrame.

Handles the realities of the live client (verified in client.py):
  * ``get()`` RAISES ``HFDLError`` on 404 (it does not return None) -> we catch
    per ticker, record the miss in ``coverage.missing_tickers``, and continue.
  * the client does NO inter-request throttling -> we sleep ``throttle`` seconds
    between requests to stay under the 100/min download limit.
  * the per-ticker frame carries a ``source`` column ('pitrading'/'iex'); we
    collect the observed values so the IEX caveat is based on real data, not a
    date guess.

The returned wide frame has a ``CoverageReport`` on ``df.attrs['coverage']``.
"""
from __future__ import annotations

import hashlib
import os
import time

from ..client import HFDLError, get as _default_get
from ._coverage import audit_panel


def _to_series(df, field, ticker):
    """Extract a (DatetimeIndex -> field) Series from one ticker's frame."""
    import pandas as pd

    if field not in df.columns:
        raise HFDLError(f"field {field!r} not in columns {list(df.columns)} for {ticker}")

    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
    elif "datetime" in df.columns:
        idx = pd.to_datetime(df["datetime"], errors="coerce")
    elif "Date" in df.columns and "Time" in df.columns:
        idx = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str),
                             errors="coerce")
    elif "Date" in df.columns:
        idx = pd.to_datetime(df["Date"], errors="coerce")
    else:
        raise HFDLError(f"no datetime/Date column in {list(df.columns)} for {ticker}")

    s = pd.Series(df[field].to_numpy(), index=pd.DatetimeIndex(idx), name=ticker)
    return s[~s.index.isna()].sort_index()


def _observed_sources(df, into):
    if "source" in getattr(df, "columns", []):
        try:
            into.update(str(x).lower() for x in df["source"].dropna().unique())
        except Exception:
            pass


def _cache_path(cache_dir, tickers, field, timeframe, version):
    base = cache_dir or os.path.join(os.path.expanduser("~"), ".cache", "hfdatalibrary")
    key = hashlib.md5(("|".join(sorted(tickers)) + f"|{field}|{timeframe}|{version}")
                      .encode()).hexdigest()[:16]
    return os.path.join(base, f"panel_{version}_{timeframe}_{field}_{key}.parquet")


def load_panel(tickers, field="Close", timeframe="daily", start=None, end=None,
               version="clean", cache=True, cache_dir=None, throttle=0.65,
               getter=None):
    """Load ``field`` for ``tickers`` into a wide DataFrame (index=dates, cols=tickers).

    Parameters
    ----------
    tickers : list[str]
    field : str
        OHLCV column to pull (default "Close").
    timeframe : str
        Default "daily" - the right granularity (and RAM scale) for backtesting;
        1-minute is available but enormous and not needed for daily strategies.
    start, end : str | None
        Optional date filter applied after loading (the API serves whole files).
    version : "clean" | "raw"
    cache : bool
        Cache the assembled panel locally (keyed by ticker set + field/timeframe/version).
    throttle : float
        Seconds to sleep between requests (100/min download limit).
    getter : callable, optional
        Injected for testing; defaults to the module-level ``hfdatalibrary.get``.
    """
    import pandas as pd

    tickers = [t.upper() for t in tickers]
    fetch = getter or _default_get

    # --- cache hit? ---
    cpath = _cache_path(cache_dir, tickers, field, timeframe, version)
    wide = None
    if cache and os.path.exists(cpath):
        try:
            wide = pd.read_parquet(cpath)
        except Exception:
            wide = None

    missing, sources = [], set()
    if wide is None:
        series = []
        for i, t in enumerate(tickers):
            try:
                df = fetch(t, version=version, timeframe=timeframe)
            except HFDLError:
                missing.append(t)         # 404 etc. -> honest skip, never silent
                continue
            try:
                series.append(_to_series(df, field, t))
                _observed_sources(df, sources)
            except HFDLError:
                missing.append(t)
            if throttle and i < len(tickers) - 1:
                time.sleep(throttle)
        if not series:
            raise HFDLError(
                f"no data for any of {len(tickers)} tickers "
                f"(missing: {', '.join(missing[:10])}{' ...' if len(missing) > 10 else ''})")
        wide = pd.concat(series, axis=1).sort_index()
        if cache:
            try:
                os.makedirs(os.path.dirname(cpath), exist_ok=True)
                wide.to_parquet(cpath)
            except Exception:
                pass  # caching is best-effort; pyarrow may be absent

    # --- date filter ---
    if start is not None:
        wide = wide.loc[wide.index >= pd.Timestamp(start)]
    if end is not None:
        wide = wide.loc[wide.index <= pd.Timestamp(end)]

    cov = audit_panel(wide, missing_tickers=missing,
                      sources=sources or None, version=version)
    wide.attrs["coverage"] = cov
    return wide
