"""Which quantity in seam_K.csv, banded how, reproduces the disclosure's 216 / 54 / 401 / 325 / 122?

R337: reproduce the other measurement's exact question before overturning it. My first attempt banded
|P - 1| and got 441/519/6/17/135 with the same total, which means I was measuring a different quantity,
not that the doc is wrong. This tries every numeric column and both distance conventions."""
import csv

TARGET = [216, 54, 401, 325, 122]
rows = list(csv.DictReader(open("D:/temp/claude/seam_K.csv", encoding="utf-8")))
UNMEASURABLE = ("no_seam_window", "no_yahoo_history", "no_overlap_with_yahoo")
keep = [r for r in rows if not any(u in (r.get("flag") or "") for u in UNMEASURABLE)]
print(f"measurable rows: {len(keep)}")

cols = [c for c in rows[0] if c not in ("ticker", "flag")]


def bands(vals, mode):
    c = [0, 0, 0, 0, 0]
    for v in vals:
        if mode == "ratio":                      # distance of a ratio from 1, folded so 0.5 and 2 agree
            d = abs(v - 1.0) if v >= 1 else abs(1.0 / v - 1.0) if v else 9e9
        else:                                    # plain absolute distance from 1
            d = abs(v - 1.0)
        if d < 1e-9:
            c[0] += 1
        elif d <= 0.02:
            c[1] += 1
        elif d <= 0.10:
            c[2] += 1
        elif d <= 0.50:
            c[3] += 1
        else:
            c[4] += 1
    return c


for col in cols:
    vals = []
    for r in keep:
        try:
            vals.append(float(r[col]))
        except (TypeError, ValueError):
            pass
    if len(vals) < len(keep) * 0.9:
        continue
    for mode in ("plain", "ratio"):
        got = bands(vals, mode)
        mark = "  <== MATCHES THE DOC" if got == TARGET else ""
        print(f"  {col:18} {mode:6} -> {got}{mark}")
print(f"\ndoc: {TARGET}")
