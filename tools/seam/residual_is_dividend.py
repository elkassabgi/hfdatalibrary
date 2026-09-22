"""Is the residual seam step, after the split-mode repair, actually the DIVIDEND factor?

I was about to tell Ahmed "the 13 tickers still 10-25 % off are the dividend factor the repair
deliberately leaves in place". That is a causal story, and R761 was written today for asserting
exactly this kind of story without measuring it.

The test: the tool recorded a per-ticker dividend factor D in tools/seam/seam_K.csv, measured
independently of today's post-repair step. If the story is right, my measured step_now should equal
D. If it does not, the residual is something else and the story is wrong.
"""
import pandas as pd

step = pd.read_csv(r"D:\temp\claude\seam_step_now.csv")
k = pd.read_csv(r"D:\temp\claude\hf_wt_main\tools\seam\seam_K.csv")

m = step.merge(k[["ticker", "P", "D", "K", "seam_step_served"]], on="ticker", how="left")
missing = m[m.D.isna()]
print(f"matched {len(m) - len(missing)} of {len(step)} rebased tickers to a recorded D")
if len(missing):
    print("  no seam_K row for:", ", ".join(missing.ticker))

m = m.dropna(subset=["D"]).copy()
m["ratio"] = m.step_now / m.D
print()
print("  step_now / D  (1.0 => the residual IS exactly the recorded dividend factor)")
print(f"    median {m.ratio.median():.4f}   min {m.ratio.min():.4f}   max {m.ratio.max():.4f}")
print(f"    within 1 % of 1.0 : {(m.ratio.sub(1).abs() <= 0.01).sum()}/{len(m)}")
print(f"    within 5 % of 1.0 : {(m.ratio.sub(1).abs() <= 0.05).sum()}/{len(m)}")

print()
print("  the 13 furthest from continuous, against their recorded D:")
far = m.reindex(m.step_now.sub(1).abs().sort_values(ascending=False).index).head(13)
for _, r in far.iterrows():
    print(f"    {r.ticker:6} step_now {r.step_now:>8.4f}   recorded D {r.D:>8.4f}   "
          f"step/D {r.ratio:>7.4f}")

print()
worst = m.reindex(m.ratio.sub(1).abs().sort_values(ascending=False).index).head(5)
print("  where the story fits WORST (these are the ones that would falsify it):")
for _, r in worst.iterrows():
    print(f"    {r.ticker:6} step_now {r.step_now:>8.4f}   recorded D {r.D:>8.4f}   "
          f"step/D {r.ratio:>7.4f}   P {r.P:>6.3f}  K {r.K:>8.4f}")
