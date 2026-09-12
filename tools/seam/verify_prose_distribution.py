"""Re-derive the disclosure's headline sentence from seam_K.csv: "1,391 served tickers, 1,118 could be
measured and roughly three-quarters carry a seam (216 none, 54 within 2 %, 401 at 2-10 %, 325 at 10-50 %,
122 beyond 50 %)". Read-only; prints what the data says next to what the doc says."""
import csv

rows = list(csv.DictReader(open("D:/temp/claude/seam_K.csv", encoding="utf-8")))
print(f"rows in seam_K.csv: {len(rows):,}")

UNMEASURABLE = ("no_seam_window", "no_yahoo_history", "no_overlap_with_yahoo")
measurable = []
for r in rows:
    flag = (r.get("flag") or "")
    if any(u in flag for u in UNMEASURABLE):
        continue
    try:
        measurable.append(float(r["P"]))
    except (TypeError, ValueError):
        pass
print(f"measurable (flag carries none of {UNMEASURABLE}): {len(measurable):,}")

# "seam size" = how far the measured ratio sits from 1
def band(p):
    d = abs(p - 1.0)
    if d < 1e-9:
        return "none"
    if d <= 0.02:
        return "within 2 %"
    if d <= 0.10:
        return "2-10 %"
    if d <= 0.50:
        return "10-50 %"
    return "beyond 50 %"


counts = {}
for p in measurable:
    counts[band(p)] = counts.get(band(p), 0) + 1
print("\nmeasured distribution:")
for k in ("none", "within 2 %", "2-10 %", "10-50 %", "beyond 50 %"):
    print(f"   {k:12} {counts.get(k, 0):5,}")
print(f"   {'TOTAL':12} {sum(counts.values()):5,}")
print("\ndoc says: 1,118 measurable; 216 none, 54 within 2 %, 401 at 2-10 %, 325 at 10-50 %, 122 beyond 50 %")
doc = {"none": 216, "within 2 %": 54, "2-10 %": 401, "10-50 %": 325, "beyond 50 %": 122}
print("match:", all(counts.get(k, 0) == v for k, v in doc.items()) and sum(doc.values()) == len(measurable))
carry = len(measurable) - counts.get("none", 0)
print(f"'roughly three-quarters carry a seam': {carry:,} of {len(measurable):,} = {carry/len(measurable):.1%}")
