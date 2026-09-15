"""Coverage accounting for the vendor-seam repair (read-only): of the 1,391 served tickers, how many carry a
seam, how many are repaired, how many are refused with a stated reason, and how many are in NO bucket at all.
The last number is the one that matters - an unaccounted ticker is a series still on the wrong basis that
nobody has decided about.

Inputs: D:/temp/claude/seam_K.csv (the fleet measurement: P, D, K, spread and a flag per ticker),
the pass lists, and the pass-1 batch log (what actually ran). Prints the buckets and writes the
unaccounted list to D:/temp/claude/_seam_unaccounted.txt."""
import csv, os

K = {r["ticker"]: r for r in csv.DictReader(open("D:/temp/claude/seam_K.csv", encoding="utf-8"))}
def lst(p):
    return [l.strip() for l in open(p, encoding="utf-8") if l.strip()] if os.path.exists(p) else []

pass1 = set(lst("D:/temp/claude/seam_pass1_plan67.txt"))
pass1b = {"BIRD", "EVX", "PCAR", "RENT"}
pass2 = set(lst("D:/temp/claude/seam_pass2_plan27.txt"))

applied, other_exit = set(), {}
for line in open("D:/temp/claude/seam_rebase_batch_pass1.log", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    if len(p) >= 5:
        if p[2] == "0" and ("DONE" in p[4] or "release:" in p[4]):
            applied.add(p[1])
        elif p[2] != "0":
            other_exit[p[1]] = p[2]

print(f"served tickers measured in seam_K.csv: {len(K):,}")
print(f"  APPLIED (pass 1, verified)              : {len(applied):3d}")
print(f"  PLANNED, not yet applied                : {len(pass1b | pass2):3d}  (pass 1b {len(pass1b)}, pass 2 {len(pass2)})")

buckets, unaccounted = {}, []
for t, r in K.items():
    if t in applied or t in pass1b or t in pass2 or t in pass1:
        continue
    flag = (r.get("flag") or "").strip()
    if flag:
        key = flag
    else:
        # no flag: the measurement ran and produced a K. Is it a no-op?
        try:
            k = float(r["K"]); p = float(r["P"])
        except (TypeError, ValueError):
            key = "unparseable K/P"; buckets[key] = buckets.get(key, 0) + 1; unaccounted.append((t, key)); continue
        if abs(p - 1.0) < 1e-9:
            key = "no seam: P = 1 (the two halves agree)"
        else:
            key = "NO FLAG, P != 1 - UNACCOUNTED"
            unaccounted.append((t, f"P={p} K={k} spread_P={r.get('spread_P')}"))
    buckets[key] = buckets.get(key, 0) + 1

print("\nevery other ticker, by the measurement's own verdict:")
for k, v in sorted(buckets.items(), key=lambda x: -x[1]):
    mark = "  <-- NEEDS A DECISION" if "UNACCOUNTED" in k else ""
    print(f"  {v:4d}  {k}{mark}")

if other_exit:
    print("\nnon-zero exits in the pass-1 log:", ", ".join(f"{t} exit {c}" for t, c in other_exit.items()))
tot = len(applied) + len(pass1b | pass2) + sum(buckets.values())
print(f"\ntotal accounted: {tot:,} of {len(K):,}" + ("" if tot == len(K) else "  <-- SHORTFALL, investigate"))
with open("D:/temp/claude/_seam_unaccounted.txt", "w", encoding="utf-8", newline="\n") as f:
    for t, why in unaccounted:
        f.write(f"{t}\t{why}\n")
print(f"unaccounted list ({len(unaccounted)}) -> D:/temp/claude/_seam_unaccounted.txt")
