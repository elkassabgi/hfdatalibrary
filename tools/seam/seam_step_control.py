"""CONTROL for seam_step_now.py: how much of the "residual" is just the real market on 2022-03-07?

My headline said the rebased tickers' seam step is now a median 6.6 % away from continuous. But the
step spans 2022-03-04 (Friday) to 2022-03-07 (Monday), and that Monday was a violent session. Some
of the 6.6 % is a REAL overnight move, not leftover defect. Four tickers make this concrete: FTNT,
GME, CMG and CHPT have a recorded dividend factor of ~1.0 and still show ~9-11 % residual.

The control: tickers the tool itself flagged as having NO seam to repair (K == 1 and D == 1). They
were never rebased, so their step measures ONLY the genuine market move plus idiosyncratic noise.
If the control is centred well away from 1.0, my residual figure is overstated by that amount.

Read-only.
"""
import datetime as dt
import sys

sys.path.insert(0, r"D:\temp\claude\hf_wt_main\pipeline")
import pandas as pd
import r2_client                                                    # noqa: E402

SEAM = pd.Timestamp("2022-03-07")
N = 40


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def step(df):
    lo = {c.lower(): c for c in df.columns}
    d, c = lo["datetime"], lo["close"]
    df = df[[d, c]].copy()
    df[d] = pd.to_datetime(df[d])
    daily = df.set_index(d)[c].resample("D").last().dropna()
    b, a = daily[daily.index < SEAM], daily[daily.index >= SEAM]
    return None if (b.empty or a.empty) else a.iloc[0] / b.iloc[-1]


k = pd.read_csv(r"D:\temp\claude\hf_wt_main\tools\seam\seam_K.csv")
rebased = {t.strip() for t in open(r"D:\temp\claude\_pass1_done_tickers.txt") if t.strip()}

flat = k[(k.K.sub(1).abs() <= 1e-6) & (k.D.sub(1).abs() <= 1e-3) & (~k.ticker.isin(rebased))]
sample = flat.ticker.head(N).tolist()
print(f"start {stamp()}")
print(f"control set: {len(flat)} tickers with K == 1 and D == 1 and never rebased; sampling {len(sample)}")

client = r2_client.get_client()
rows = []
for t in sample:
    df = r2_client.download_parquet(client, "raw", t)
    if df is None:
        continue
    s = step(df)
    if s is not None:
        rows.append((t, s))

c = pd.DataFrame(rows, columns=["ticker", "step"])
print(f"measured {len(c)} of {len(sample)}")
print()
print("  CONTROL - tickers with no seam, so this is the real 2022-03-04 -> 03-07 move:")
print(f"    median step {c.step.median():.4f}   mean {c.step.mean():.4f}")
print(f"    median |step-1| {c.step.sub(1).abs().median():.4f}   "
      f"10th-90th pct {c.step.quantile(0.1):.4f} .. {c.step.quantile(0.9):.4f}")
print(f"    within 10 % of 1.0: {(c.step.sub(1).abs() <= 0.10).sum()}/{len(c)}")

r = pd.read_csv(r"D:\temp\claude\seam_step_now.csv")
print()
print("  REBASED (from seam_step_now.csv), for comparison:")
print(f"    median step {r.step_now.median():.4f}   median |step-1| {r.step_now.sub(1).abs().median():.4f}")
print()
print("  => the honest statement of what the repair left behind is the rebased figure MEASURED")
print("     AGAINST this control, not against a naive 1.0.")
c.to_csv(r"D:\temp\claude\seam_step_control.csv", index=False)
print(f"done {stamp()}")
