"""
aggregate.py — Aggregate 1-minute OHLCV bars to higher timeframes.

Standard rules: first open, max high, min low, last close, sum volume.
Used to regenerate 5min/15min/30min/hourly/daily/weekly/monthly versions
after the daily pipeline appends new bars.
"""
from __future__ import annotations
import pandas as pd

TIMEFRAMES = {
    "5min":    "5min",
    "15min":   "15min",
    "30min":   "30min",
    "hourly":  "1h",
    "daily":   "1D",
    "weekly":  "W-FRI",
    "monthly": "MS",
}


def aggregate_ohlcv(df_1min: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Resample a 1-minute OHLCV DataFrame to a higher timeframe."""
    if df_1min.empty or "datetime" not in df_1min.columns:
        return pd.DataFrame()

    df = df_1min.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime")

    agg_dict = {
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    }
    if "source" in df.columns:
        agg_dict["source"] = "last"

    out = df.resample(freq).agg(agg_dict).dropna(subset=["Open"])
    out = out.reset_index()
    return out


def aggregate_all(df_1min: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return a dict of {timeframe_name: aggregated_df} for all standard timeframes."""
    return {name: aggregate_ohlcv(df_1min, freq) for name, freq in TIMEFRAMES.items()}
