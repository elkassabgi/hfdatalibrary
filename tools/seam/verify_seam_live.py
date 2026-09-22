"""Is the HF seam fix actually live and holding on served data, hours after it was applied?

The repair: pre-seam bars (< 2022-03-07) get prices x K and volumes / K, where K = 1 / (product of
split ratios recorded after the seam). Post-seam bars are untouched.

So against the pre-repair anchor F:/hf_r2_snapshot_20260713 the served data must show:
    pre-seam  served/snapshot close == K       (exactly, on every bar)
    post-seam served/snapshot close == 1.0     (exactly, on every bar)

This is an INDEPENDENT re-measurement, not a re-read of the tool's own VERIFY line. It matters most
for NVDA, whose batch line was an exit 4 ("served state UNKNOWN") that I released by hand.

Read-only.
"""
import datetime as dt
import sys

sys.path.insert(0, r"D:\temp\claude\hf_wt_main\pipeline")
import pandas as pd
import r2_client                                                    # noqa: E402

SNAP = r"F:\hf_r2_snapshot_20260713"
SEAM = pd.Timestamp("2022-03-07")
# K as the tool measured and recorded it in the batch log / _RESULT.txt
CASES = {"NVDA": 0.1, "APH": 0.5, "AVGO": 0.1}


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


print(f"start {stamp()}")
client = r2_client.get_client()

for t, K in CASES.items():
    for tier in ("raw", "clean"):
        served = r2_client.download_parquet(client, tier, t)
        if served is None:
            print(f"{t} {tier}: served object MISSING")
            continue
        try:
            snap = pd.read_parquet(f"{SNAP}\\{tier}\\{t}.parquet")
        except Exception as ex:                                     # noqa: BLE001
            print(f"{t} {tier}: no anchor ({type(ex).__name__})")
            continue

        lo = {c.lower(): c for c in served.columns}
        d, c, v = lo["datetime"], lo["close"], lo.get("volume")
        for f in (served, snap):
            f[d] = pd.to_datetime(f[d])

        m = served.merge(snap, on=d, suffixes=("_s", "_n"))
        if m.empty:
            print(f"{t} {tier}: NO OVERLAP with the anchor")
            continue
        pre, post = m[m[d] < SEAM], m[m[d] >= SEAM]

        def report(part, label, expect):
            if part.empty:
                print(f"  {t:5} {tier:5} {label:9}: no bars")
                return
            r = (part[f"{c}_s"] / part[f"{c}_n"]).replace([float("inf"), float("-inf")], pd.NA).dropna()
            near = (r.sub(expect).abs() <= 1e-9).sum()
            print(f"  {t:5} {tier:5} {label:9}: {len(part):>9,} bars  expect {expect:<8}"
                  f" min {r.min():.6f} max {r.max():.6f}  on-target {near:,}/{len(r):,}")

        report(pre, "pre-seam", K)
        report(post, "post-seam", 1.0)

        if v and not pre.empty:
            rv = (pre[f"{v}_s"] / pre[f"{v}_n"]).replace([float("inf"), float("-inf")], pd.NA).dropna()
            rv = rv[rv > 0]
            if len(rv):
                print(f"  {t:5} {tier:5} pre-vol  : expect {1/K:<8.4g} "
                      f"min {rv.min():.4f} max {rv.max():.4f} median {rv.median():.4f}")

print(f"done {stamp()}")
