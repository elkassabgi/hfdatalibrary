"""For each ticker: read the SERVED clean daily bars and, for every Yahoo-recorded split event after
2022-03-04, measure the close-to-close step across the event date. Read-only.

If an event was APPLIED to the history, the served series is continuous across it (step ~ 1). If it was NOT
applied, the served series steps by the event ratio on the event date - the state R719 measured on MNST
(91.42 -> 45.545 on 2026-08-11 with Yahoo flat). This is the observation R719 rule 1 demands: the served data
across the event itself, not a downstream residual.

    python check_later_events_applied.py MNST REW SCO ...
"""
import sys
sys.path.insert(0, "D:/temp/claude/hf_wt_main/pipeline")
import pandas as pd
from r2_client import get_client, download_parquet
import seam_rebase

client = get_client()
for t in sys.argv[1:]:
    try:
        _h, spl, _div = seam_rebase.yahoo(t)          # (history, split events after 2022-03-04, ex-dates)
        events = [(d.date().isoformat(), float(r)) for d, r in spl.items()] if spl is not None and len(spl) else []
    except Exception as ex:                                   # noqa: BLE001
        events = None
        print(f"{t}: could not read events ({type(ex).__name__}: {ex})")
    daily = download_parquet(client, "clean", t, timeframe="daily")
    if daily is None or daily.empty:
        print(f"{t}: no served clean daily file"); continue
    dcol = "datetime" if "datetime" in daily.columns else daily.columns[0]
    d = daily.copy()
    d[dcol] = pd.to_datetime(d[dcol])
    d = d.sort_values(dcol).set_index(dcol)
    print(f"{t}: served clean daily {d.index.min().date()}..{d.index.max().date()}, {len(d):,} sessions")
    if not events:
        print(f"   (no event list available from seam_rebase; pass dates manually)")
        continue
    for date, ratio in events:
        ts = pd.Timestamp(date)
        before = d.loc[:ts - pd.Timedelta(days=1), "Close"]
        after = d.loc[ts:, "Close"]
        if before.empty or after.empty:
            print(f"   {date} ratio {ratio}: outside the served range"); continue
        c0, c1 = float(before.iloc[-1]), float(after.iloc[0])
        step = c1 / c0 if c0 else float("nan")
        # applied => the history was rescaled, so the served step is ~1 (continuous)
        # not applied => the served step is ~ratio (the market's own halving/doubling stands in the data)
        verdict = ("APPLIED (series continuous)" if abs(step - 1) < 0.12 else
                   f"NOT APPLIED (served steps x{step:.3f} ~ the event ratio {ratio})" if abs(step - ratio) / ratio < 0.12 else
                   f"UNCLEAR (served step x{step:.3f}, event {ratio})")
        print(f"   {date} ratio {ratio}: {before.index[-1].date()} close {c0:.4f} -> {after.index[0].date()} close {c1:.4f}  = x{step:.4f}  {verdict}")
