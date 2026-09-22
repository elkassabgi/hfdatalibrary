"""What price level is served for STI / USLV over time, and when did it change owner?

R732 predicts the split detector snaps STI 7.69/70.16 = 1/9 and USLV 16.85/66.59 = 1/4. That ratio is
"today's foreign price over the OLD owner's close". It can only arise if the series' PREVIOUS bar is
the old owner's. If foreign prints have already been accumulating for months, the previous bar is
itself foreign, the ratio is ~1, and nothing fires - which is what today's run shows.

So the question is not "did it fire today" but "what would make it fire". Read-only.
"""
import datetime as dt
import sys

sys.path.insert(0, r"D:\temp\claude\hf_wt_main\pipeline")
import pandas as pd
import r2_client                                                   # noqa: E402


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


print(f"start {stamp()}")
client = r2_client.get_client()

for t in ("STI", "USLV"):
    df = r2_client.download_parquet(client, "raw", t)
    if df is None:
        print(f"{t}: served raw MISSING")
        continue
    lower = {c.lower(): c for c in df.columns}
    dcol, ccol = lower["datetime"], lower["close"]
    df[dcol] = pd.to_datetime(df[dcol])
    daily = df.set_index(dcol)[ccol].resample("D").last().dropna()

    print()
    print(f"=== {t}: {len(df):,} bars, {daily.index.min().date()} .. {daily.index.max().date()} ===")

    # the largest day-over-day jump in the daily close tells us where an owner change or split sits
    r = (daily / daily.shift(1)).dropna()
    worst = r.reindex(r.sub(1).abs().sort_values(ascending=False).index)[:6]
    print("  largest day-over-day close ratios (a reassignment or unapplied split shows up here):")
    for d, v in worst.items():
        prev = daily.shift(1).loc[d]
        print(f"    {d.date()}  {prev:>10.4f} -> {daily.loc[d]:>10.4f}   ratio {v:.6f}")

    print("  last 6 daily closes served:")
    for d, v in daily.tail(6).items():
        print(f"    {d.date()}  {v:.4f}")

print()
print(f"done {stamp()}")
