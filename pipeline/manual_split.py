"""manual_split.py — human-confirmed corporate-action rescale for one ticker.

Companion to daily_update._detect_and_apply_split: ratios below the 3:1
auto-apply floor (a 2:1 split is numerically identical to a clean 50% crash
that holds all day) are alerted, not applied. After confirming the split is
real (issuer announcement), apply it with this tool:

    python -m pipeline.manual_split TICKER RATIO
    # RATIO = new_price / old_price, e.g. 0.5 for a 2:1 forward split,
    #         2 for a 1:2 reverse split.

Rescales the served raw history (price x RATIO, volume / RATIO with a >=1
floor on originally-nonzero minutes), full re-clean, re-aggregate, re-upload,
and force-full variables/quality recompute — the same post-rescale path the
daily pipeline uses. Asks for confirmation before writing.
"""
from __future__ import annotations

import sys

import pandas as pd

from aggregate import aggregate_all
from clean_pipeline import clean_bars
from r2_client import download_parquet, get_client, upload_csv, upload_parquet
from variables_sync import sync_ticker_variables

TIMEFRAMES = ["5min", "15min", "30min", "hourly", "daily", "weekly", "monthly"]


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    ticker = sys.argv[1].upper()
    ratio = float(sys.argv[2])
    if not (0.001 < ratio < 1000) or abs(ratio - 1.0) < 0.2:
        raise SystemExit(f"ratio {ratio} looks wrong (must be a real split ratio, not ~1)")

    client = get_client()
    raw = download_parquet(client, "raw", ticker)
    if raw is None or raw.empty:
        raise SystemExit(f"no served raw data for {ticker}")
    raw["datetime"] = pd.to_datetime(raw["datetime"])
    if raw["datetime"].dt.tz is not None:
        raw["datetime"] = raw["datetime"].dt.tz_localize(None)

    last_day = raw["datetime"].dt.normalize().max()
    last_close = float(raw.loc[raw["datetime"].dt.normalize() == last_day, "Close"].median())
    print(f"{ticker}: {len(raw):,} bars through {last_day.date()}, last close ~{last_close}")
    print(f"will rescale ENTIRE history: price x{ratio}  volume x{1 / ratio:.6g}")
    print(f"  -> last close becomes ~{last_close * ratio:.4f}")
    if input("type the ticker to confirm: ").strip().upper() != ticker:
        raise SystemExit("aborted")

    for c in ("Open", "High", "Low", "Close"):
        raw[c] = (raw[c] * ratio).round(6)
    vol = (raw["Volume"] / ratio).round()
    vol[(raw["Volume"] > 0) & (vol == 0)] = 1
    raw["Volume"] = vol.astype("int64")

    clean = clean_bars(raw)
    print(f"re-cleaned: {len(raw):,} raw -> {len(clean):,} clean; uploading...")
    n = 0
    for version, df in (("raw", raw), ("clean", clean)):
        upload_parquet(client, df, version, ticker, "1min")
        upload_csv(client, df, version, ticker, "1min")
        n += 2
        aggs = aggregate_all(df)
        for tf in TIMEFRAMES:
            if tf in aggs and not aggs[tf].empty:
                upload_parquet(client, aggs[tf], version, ticker, tf)
                n += 1
        sync_ticker_variables(client, version, ticker, df, force_full=True)
        n += 2
    print(f"DONE: {n} objects re-uploaded for {ticker} on the new basis")


if __name__ == "__main__":
    main()
