"""seam_rebase_batch.py — drive seam_rebase.py (v2, self-measuring) across the served population,
one ticker at a time. The tool decides per ticker; this driver only sequences and records.

    python seam_rebase_batch.py [--apply] [--limit N] [--tickers A,B,...] [--candidates CSV]

Candidates: every ticker with a PiTrading/IEX seam window — from seam_measure_K.py's CSV (rows
whose flag does not say no_seam_window), or the explicit --tickers list. The tool re-measures each
one on the 3 seam-adjacent sessions, so a stale factor in the CSV cannot reach the data.

Exit codes from seam_rebase.py and what the driver does with them:
    0  rebased and verified, or nothing to rebase in this mode   -> record, continue
    2  refused (non-integer P, split-event mismatch, unstable)    -> record, continue (manual list)
    3  unmeasurable (no market reference / no overlap)            -> record, continue (disclose list)
    1  applied but NOT VERIFIED                                    -> record, STOP the batch
One line per ticker is appended to the log AFTER the tool exits — never before — so a crash leaves
an honest record (ledger R591). Re-runs skip tickers already logged with exit 0.
"""
from __future__ import annotations
import argparse, datetime as dt, os, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--tickers", default=None); ap.add_argument("--candidates", default=r"D:\temp\claude\seam_K.csv")
    ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--log", default=r"D:\temp\claude\seam_rebase_batch.log")
    ap.add_argument("--snapshot-root", default=os.path.join("F:\\", f"hf_r2_snapshot_seam_{dt.date.today():%Y%m%d}"))
    a = ap.parse_args()
    if a.tickers and os.path.isfile(a.tickers):
        cands = [l.strip().upper() for l in open(a.tickers, encoding="utf-8") if l.strip()]   # one ticker per line
    elif a.tickers:
        cands = [t.strip().upper() for t in a.tickers.split(",") if t.strip()]
    else:
        d = pd.read_csv(a.candidates); d["flag"] = d.flag.fillna("")
        cands = sorted(d[~d.flag.str.contains("no_seam_window")].ticker)
    done = set()
    if os.path.exists(a.log):
        for line in open(a.log, encoding="utf-8"):
            p = line.rstrip("\n").split("\t")
            if len(p) >= 3 and p[2] == "0":
                done.add(p[1])
    todo = [t for t in cands if t not in done]
    print(f"candidates {len(cands)}; already exit-0 in log {len(done & set(cands))}; to do {len(todo)}; mode {a.mode}; apply {a.apply}")
    if not a.apply:
        print("  first 20:", " ".join(todo[:20])); print("(dry run - pass --apply to run the batch)"); return 0
    n = 0; stopped = False
    for t in todo:
        if a.limit is not None and n >= a.limit:
            break
        cmd = [sys.executable, "-u", os.path.join(HERE, "seam_rebase.py"), t, "--mode", a.mode, "--apply",
               "--snapshot-dir", os.path.join(a.snapshot_root, t)]
        t0 = dt.datetime.now()
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=HERE)
        last = (p.stdout.strip().splitlines() or [""])[-1]
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{t0:%Y-%m-%d %H:%M:%S}\t{t}\t{p.returncode}\t{(dt.datetime.now() - t0).total_seconds():.0f}s\t{last[:200]}\n")
        print(f"  {t:6} exit {p.returncode}  {last[:130]}", flush=True)
        n += 1
        if p.returncode == 1:
            print("STOPPING: the last ticker was written but did not verify.\n" + p.stdout[-2500:] + p.stderr[-800:]); stopped = True; break
    print(f"batch {'stopped' if stopped else 'done'}: {n} processed this run; log {a.log}")
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
