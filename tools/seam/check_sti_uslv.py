"""Did today's daily run (2026-09-05T09:42Z) rescale STI / USLV, as R732 predicted it would?

R732: with PR #12 unmerged, the run re-appends the NEW owner's prints and `_detect_and_apply_split`
snaps STI 7.69/70.16 = 0.1096 -> 1/9 and USLV 16.85/66.59 = 0.253 -> 1/4, rescaling the whole history.

The test is not the log's silence. It is the served bytes against an anchor that predates the hazard:
F:/hf_r2_snapshot_20260713. Both tickers' kept halves are on their original basis there (R732).

Read-only. Downloads two served objects to a temp dir; writes nothing back.
"""
import os
import sys
import datetime as dt

sys.path.insert(0, r"D:\temp\claude\hf_wt_main\pipeline")
import pandas as pd
import r2_client                                                   # noqa: E402

SNAP = r"F:\hf_r2_snapshot_20260713"
OUT = r"D:\temp\claude\sti_uslv_check"
os.makedirs(OUT, exist_ok=True)
TICKERS = ("STI", "USLV")


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


print(f"start {stamp()}")
client = r2_client.get_client()
print(f"bucket {r2_client.get_bucket()}")

for t in TICKERS:
    for tier in ("raw", "clean"):
        try:
            served = r2_client.download_parquet(client, tier, t)
        except Exception as ex:                                     # noqa: BLE001
            print(f"  {t} {tier}: DOWNLOAD RAISED {type(ex).__name__}: {str(ex)[:90]}")
            continue
        if served is None:
            print(f"  {t} {tier}: served object MISSING at {r2_client.parquet_key(tier, t)}")
            continue
        snap_path = os.path.join(SNAP, tier, f"{t}.parquet")
        if not os.path.exists(snap_path):
            print(f"  {t} {tier}: no snapshot at {snap_path}")
            continue
        snap = pd.read_parquet(snap_path)

        lower = {c.lower(): c for c in served.columns}
        dcol = lower.get("datetime") or lower.get("date") or served.columns[0]
        ccol = lower.get("close")
        if ccol is None:
            print(f"  {t} {tier}: no close column in {list(served.columns)}")
            continue
        s = served.copy()
        n = snap.copy()
        s[dcol] = pd.to_datetime(s[dcol])
        n[dcol] = pd.to_datetime(n[dcol])

        # compare ONLY the range the snapshot covers; anything after it is new data, not a rescale
        hi = n[dcol].max()
        s_in = s[s[dcol] <= hi]

        m = s_in.merge(n, on=dcol, suffixes=("_served", "_snap"))
        if m.empty:
            print(f"  {t} {tier}: NO OVERLAP with the snapshot - cannot judge")
            continue
        ratio = (m[f"{ccol}_served"] / m[f"{ccol}_snap"]).replace([float("inf"), float("-inf")], pd.NA).dropna()
        print(f"  {t} {tier}: served rows {len(s)}, snapshot rows {len(n)}, compared {len(m)} "
              f"through {hi.date()}")
        print(f"      served/snapshot close: min {ratio.min():.6f}  max {ratio.max():.6f}  "
              f"median {ratio.median():.6f}   exact-1.0 on {(ratio == 1.0).sum()}/{len(ratio)}")
        newest = s[dcol].max()
        print(f"      newest served bar: {newest}")

print(f"done {stamp()}")
