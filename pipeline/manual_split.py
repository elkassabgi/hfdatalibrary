"""manual_split.py — human-confirmed corporate-action rescale for one ticker.

Companion to daily_update._detect_and_apply_split: ratios below the 3:1
auto-apply floor (a 2:1 split is numerically identical to a clean 50% crash
that holds all day) are alerted, not applied. After confirming the split is
real (issuer announcement), apply it with this tool:

    python -m pipeline.manual_split TICKER RATIO [CA_DATE]
    # RATIO   = new_price / old_price, e.g. 0.5 for a 2:1 forward split,
    #           2 for a 1:2 reverse split.
    # CA_DATE = YYYY-MM-DD, the FIRST session already trading on the new
    #           basis. Only bars STRICTLY BEFORE it are rescaled.

WHY CA_DATE EXISTS (added 2026-09-04). `merge_ticker` appends the day's bars
whether or not the corporate action was applied — daily_update.py returns
`(existing_raw, False)` from the detector and the very next line concatenates
regardless. So from the first daily run after an unapplied alert, the served
raw history is MIXED BASIS: old bars pre-split, the newest session(s) already
post-split. This tool used to multiply every bar unconditionally, which would
have rescaled those newest bars a SECOND time — a 2:1 case leaves them at a
quarter of the true price — and the error grows by one session per day of
delay. With CA_DATE the cutoff is explicit and the post-split bars are left
alone.

Omitting CA_DATE is still allowed for the clean case, but the tool first
checks the tail for a basis break of about RATIO. If it finds one it REFUSES
and prints the exact command to re-run, rather than silently double-scaling.

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
    if len(sys.argv) not in (3, 4):
        raise SystemExit(__doc__)
    ticker = sys.argv[1].upper()
    ratio = float(sys.argv[2])
    if not (0.001 < ratio < 1000) or abs(ratio - 1.0) < 0.2:
        raise SystemExit(f"ratio {ratio} looks wrong (must be a real split ratio, not ~1)")
    ca_date = None
    if len(sys.argv) == 4:
        try:
            ca_date = pd.Timestamp(sys.argv[3]).normalize()
        except Exception:
            raise SystemExit(f"CA_DATE {sys.argv[3]!r} is not a date (want YYYY-MM-DD)")

    client = get_client()
    raw = download_parquet(client, "raw", ticker)
    if raw is None or raw.empty:
        raise SystemExit(f"no served raw data for {ticker}")
    raw["datetime"] = pd.to_datetime(raw["datetime"])
    if raw["datetime"].dt.tz is not None:
        raw["datetime"] = raw["datetime"].dt.tz_localize(None)

    days = raw["datetime"].dt.normalize()
    last_day = days.max()
    last_close = float(raw.loc[days == last_day, "Close"].median())
    print(f"{ticker}: {len(raw):,} bars through {last_day.date()}, last close ~{last_close}")

    # MIXED-BASIS GUARD. If the newest session is already trading on the new basis - which is
    # what the daily append leaves behind after an unapplied alert - rescaling everything would
    # halve it twice. Detect that break the same way the detector does, by comparing the last
    # two sessions' median closes, and refuse rather than corrupt.
    if ca_date is None:
        uniq = sorted(days.unique())
        if len(uniq) >= 2:
            prev_med = float(raw.loc[days == uniq[-2], "Close"].median())
            if prev_med > 0:
                step = last_close / prev_med
                if abs(step - ratio) < 0.15 * max(ratio, 1.0 / ratio):
                    raise SystemExit(
                        f"REFUSING: {ticker}'s last session ({pd.Timestamp(uniq[-1]).date()}) is "
                        f"already about x{step:.3f} off the previous one, i.e. it looks like it "
                        f"is ALREADY on the post-action basis. Rescaling the whole history would "
                        f"apply x{ratio} to it a second time.\n"
                        f"Re-run with the corporate-action date so only earlier bars move:\n"
                        f"    python -m pipeline.manual_split {ticker} {ratio:.6g} "
                        f"{pd.Timestamp(uniq[-1]).date()}")

    if ca_date is None:
        mask = pd.Series(True, index=raw.index)
        scope = "ENTIRE history"
    else:
        mask = raw["datetime"] < ca_date
        scope = f"bars BEFORE {ca_date.date()} ({int(mask.sum()):,} of {len(raw):,})"
        if not mask.any():
            raise SystemExit(f"no bars before {ca_date.date()} - nothing to rescale")
        if mask.all():
            print(f"NOTE: every bar predates {ca_date.date()}; this is the whole history.")

    print(f"will rescale {scope}: price x{ratio}  volume x{1 / ratio:.6g}")
    if ca_date is None:
        print(f"  -> last close becomes ~{last_close * ratio:.4f}")
    else:
        pre = raw.loc[mask, "Close"]
        print(f"  -> pre-action closes {pre.min():.4f}..{pre.max():.4f} become "
              f"{pre.min() * ratio:.4f}..{pre.max() * ratio:.4f}")
        print(f"  -> the {int((~mask).sum()):,} bar(s) on/after {ca_date.date()} are UNCHANGED "
              f"(last close stays ~{last_close:.4f})")
    if input("type the ticker to confirm: ").strip().upper() != ticker:
        raise SystemExit("aborted")

    for c in ("Open", "High", "Low", "Close"):
        raw.loc[mask, c] = (raw.loc[mask, c] * ratio).round(6)
    vol = (raw.loc[mask, "Volume"] / ratio).round()
    vol[(raw.loc[mask, "Volume"] > 0) & (vol == 0)] = 1
    raw.loc[mask, "Volume"] = vol
    raw["Volume"] = raw["Volume"].astype("int64")

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
