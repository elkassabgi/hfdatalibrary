"""seam_dryrun_all.py — read-only pre-classification of a ticker list with seam_rebase.py (no --apply).

    python seam_dryrun_all.py TICKERFILE [OUTLOG]

Runs seam_rebase.py for each ticker in the same interpreter environment (credentials and sibling
imports as the real run), keeps the full output per ticker, and ends with a one-line-per-ticker
summary: PLAN / NOTHING / ALREADY / REFUSED / UNMEASURABLE / ERROR. Costs only R2 reads. The apply
run re-measures everything anyway; this exists to surface tool defects and refusals on the whole
population before the apply window opens.
"""
from __future__ import annotations
import datetime as dt, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
tickers = [l.strip().upper() for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
out = sys.argv[2] if len(sys.argv) > 2 else r"D:\temp\claude\_seam_pass1_dry.log"
summary = []
with open(out, "w", encoding="utf-8") as f:
    f.write(f"dry run over {len(tickers)} tickers, started {dt.datetime.now():%H:%M:%S}\n")
    for i, t in enumerate(tickers, 1):
        t0 = dt.datetime.now()
        p = subprocess.run([sys.executable, "-u", os.path.join(HERE, "seam_rebase.py"), t],
                           capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=HERE, timeout=600)
        text = "\n".join(l for l in (p.stdout + p.stderr).splitlines() if "Warning" not in l and "warn" not in l)
        f.write(f"\n=== {t} {t0:%H:%M:%S} exit={p.returncode}\n{text}\n"); f.flush()
        if "rebase plan" in text: v = "PLAN"
        elif "ALREADY" in text: v = "ALREADY"
        elif "nothing to rebase" in text or "no PiTrading/IEX splice" in text: v = "NOTHING"
        elif "REFUSED" in text: v = "REFUSED: " + re.sub(r"\s+", " ", text.split("REFUSED:", 1)[1])[:90]
        elif p.returncode == 3: v = "UNMEASURABLE"
        else: v = f"ERROR exit={p.returncode}: {text.strip().splitlines()[-1][:80] if text.strip() else ''}"
        summary.append((t, v))
        print(f"  {i}/{len(tickers)} {t:6} {v[:100]}", flush=True)
    f.write("\n\nSUMMARY\n" + "\n".join(f"{t}\t{v}" for t, v in summary) + f"\nPASS DRY RUN COMPLETE {dt.datetime.now():%H:%M:%S}\n")
from collections import Counter
print("\ncounts:", dict(Counter(v.split(":")[0] for _, v in summary)))
print("PASS DRY RUN COMPLETE")
