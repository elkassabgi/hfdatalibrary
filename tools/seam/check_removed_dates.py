"""For the session-dates the rebase removed from the variables/quality objects, ask whether the SERVED bars
still carry those dates - and with how many bars. Read-only.

If the served 1-minute file has a full session on a date the variables no longer describe, the two disagree
about which sessions exist, which is a worse condition than the one disclosed. If the file has no bars, or a
handful, the removal is the recompute declining to describe a session that is not there."""
import json, sys
sys.path.insert(0, "D:/temp/claude/hf_wt_main/pipeline")
import pandas as pd
from r2_client import get_client, download_parquet

rows = json.load(open("D:/temp/claude/_verify_variables_delta.json", encoding="utf-8"))
lost = {}
for r in rows:
    for key in ("clean_variables", "clean_quality", "raw_variables", "raw_quality"):
        v = r.get(key)
        if isinstance(v, dict):
            for x in (v.get("dates_lost") or []):
                lost.setdefault(r["ticker"], set()).add(x)

client = get_client()
targets = sys.argv[1:] or sorted(lost)
print(f"{len(lost)} tickers have removed dates; checking {len(targets)}")
grand = {"no bars": 0, "thin (<20 bars)": 0, "FULL SESSION": 0}
bar_counts = {"clean": [], "raw": []}          # every per-date bar count, so the RANGE is derived not typed
for t in targets:
    if t not in lost:
        continue
    for version in ("clean", "raw"):
        b = download_parquet(client, version, t)
        if b is None or b.empty:
            print(f"  {t} {version}: no served 1-minute file"); continue
        b["datetime"] = pd.to_datetime(b["datetime"])
        counts = b.groupby(b["datetime"].dt.normalize()).size()
        out = []
        for x in sorted(lost[t]):
            n = int(counts.get(pd.Timestamp(x), 0))
            bucket = "no bars" if n == 0 else ("thin (<20 bars)" if n < 20 else "FULL SESSION")
            grand[bucket] += 1
            bar_counts[version].append(n)
            out.append(f"{x}:{n}")
        print(f"  {t} {version}: " + ", ".join(out))
print("\nacross every removed date and both versions: " + ", ".join(f"{k} {v}" for k, v in grand.items()))
print("A 'FULL SESSION' count would mean the served bars and the served variables disagree about which sessions exist.")
summary = {v: {"n_dates": len(c), "min": (min(c) if c else None), "max": (max(c) if c else None),
               "zero": sum(1 for x in c if x == 0)} for v, c in bar_counts.items()}
print("per-version bar counts on the removed dates:", json.dumps(summary))
json.dump(summary, open("D:/temp/claude/_removed_dates_summary.json", "w", encoding="utf-8"), indent=1)
