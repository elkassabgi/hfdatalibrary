"""The decisive test: is the price series CONTINUOUS across the 2022-03-07 seam on served data now?

K-bookkeeping against a July anchor can be confounded by any other repair applied since (APH also got
a separate 2:1 fix on 09-04). The seam defect itself is simpler to state: the last pre-seam session's
close and the first post-seam session's close belonged to two different price bases, so their ratio
jumped by the split factor. After the repair that jump must be an ordinary overnight move.

Compares each rebased ticker's seam step against the SAME step in the pre-repair July snapshot.
Read-only.
"""
import datetime as dt
import sys

sys.path.insert(0, r"D:\temp\claude\hf_wt_main\pipeline")
import pandas as pd
import r2_client                                                    # noqa: E402

SNAP = r"F:\hf_r2_snapshot_20260713"
SEAM = pd.Timestamp("2022-03-07")
TICKERS = [t.strip() for t in open(r"D:\temp\claude\_pass1_done_tickers.txt") if t.strip()]


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def step(df):
    """close of the first post-seam session / close of the last pre-seam session."""
    lo = {c.lower(): c for c in df.columns}
    d, c = lo["datetime"], lo["close"]
    df = df[[d, c]].copy()
    df[d] = pd.to_datetime(df[d])
    daily = df.set_index(d)[c].resample("D").last().dropna()
    before = daily[daily.index < SEAM]
    after = daily[daily.index >= SEAM]
    if before.empty or after.empty:
        return None
    return after.iloc[0] / before.iloc[-1]


print(f"start {stamp()}  {len(TICKERS)} rebased tickers")
client = r2_client.get_client()

rows, failed = [], []
for t in TICKERS:
    served = r2_client.download_parquet(client, "raw", t)
    if served is None:
        failed.append((t, "served missing"))
        continue
    try:
        snap = pd.read_parquet(f"{SNAP}\\raw\\{t}.parquet")
    except Exception as ex:                                         # noqa: BLE001
        failed.append((t, f"no anchor: {type(ex).__name__}"))
        continue
    s_now, s_then = step(served), step(snap)
    if s_now is None or s_then is None:
        failed.append((t, "no bars on one side of the seam"))
        continue
    rows.append((t, s_then, s_now))

df = pd.DataFrame(rows, columns=["ticker", "step_july", "step_now"])
df["improved"] = (df.step_now.sub(1).abs() < df.step_july.sub(1).abs())
print()
print(f"measured {len(df)} of {len(TICKERS)}; could not measure {len(failed)}")
for t, why in failed:
    print(f"  UNMEASURED {t}: {why}")
print()
print("  seam step = first post-seam close / last pre-seam close (1.0 = continuous)")
print(f"  BEFORE (July snapshot): median |step-1| = {df.step_july.sub(1).abs().median():.4f}, "
      f"worst {df.step_july.sub(1).abs().max():.4f}")
print(f"  NOW    (served today) : median |step-1| = {df.step_now.sub(1).abs().median():.4f}, "
      f"worst {df.step_now.sub(1).abs().max():.4f}")
print(f"  improved on {int(df.improved.sum())}/{len(df)}; within 10 % of continuous now: "
      f"{int((df.step_now.sub(1).abs() <= 0.10).sum())}/{len(df)} "
      f"(was {int((df.step_july.sub(1).abs() <= 0.10).sum())}/{len(df)})")
print()
print("  the five furthest from continuous NOW:")
for _, r in df.reindex(df.step_now.sub(1).abs().sort_values(ascending=False).index).head(5).iterrows():
    print(f"    {r.ticker:6} july {r.step_july:>9.4f}  ->  now {r.step_now:>9.4f}")
df.to_csv(r"D:\temp\claude\seam_step_now.csv", index=False)
print()
print(f"done {stamp()}  table -> D:\\temp\\claude\\seam_step_now.csv")
