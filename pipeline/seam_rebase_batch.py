"""seam_rebase_batch.py — drive seam_rebase.py (v5, self-measuring) across the served population,
one ticker at a time. The tool decides per ticker; this driver only sequences and records.

    python seam_rebase_batch.py [--apply] [--limit N] [--tickers A,B,...|FILE] [--candidates CSV] [--events-file CSV]

Candidates: every ticker with a PiTrading/IEX seam window — from seam_measure_K.py's CSV (rows
whose flag does not say no_seam_window), or the explicit --tickers list / one-per-line file. The
tool re-measures each one, so a stale factor in the CSV cannot reach the data.

Exit codes from seam_rebase.py and what the driver does with each (one meaning per code, R731):
    0  rebased and verified, or nothing to rebase / already on target -> record, continue
    2  refused before any write (measurement says no)                  -> record, continue (manual list)
    3  unmeasurable (no market reference)                              -> record, continue (disclose list)
    5  aborted before any write (missing object, snapshot check, ...)  -> record, continue (retry list)
    6  prices verified, variables/quality sync FAILED                  -> record, continue, LIST at the end
    1  written then RESTORED from its snapshot; served = pre-rebase    -> record, STOP (read why)
    4  written and the RESTORE FAILED; served state UNKNOWN            -> record, STOP; --restore first
The driver refuses to START while the log's last line is an exit 4 (the previous ticker's served
state is unknown until its --restore has run). One line per ticker is appended to the log AFTER
the tool exits — never before — so a crash leaves an honest record (ledger R591). Re-runs skip
tickers already logged with exit 0.
"""
from __future__ import annotations
import argparse, datetime as dt, os, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))

STOP_TEXT = {
    1: ("STOPPING: the last ticker was written and then RESTORED from its snapshot (a failure between the first "
        "upload and the last VERIFY line, or a VERIFY mismatch); served state is the pre-rebase state. Read why "
        "before continuing."),
    4: ("STOPPING - SERVED STATE UNKNOWN: the last ticker was written AND its automatic restore failed; the "
        "rescaled objects may be LIVE. Run the printed --restore command before anything else."),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--tickers", default=None); ap.add_argument("--candidates", default=r"D:\temp\claude\seam_K.csv")
    ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--log", default=r"D:\temp\claude\seam_rebase_batch.log")
    ap.add_argument("--snapshot-root", default=os.path.join("F:\\", f"hf_r2_snapshot_seam_{dt.date.today():%Y%m%d}"))
    ap.add_argument("--events-file", default=None, help="passed through to seam_rebase.py (issuer-recorded split events, see its docstring)")
    a = ap.parse_args()
    if a.tickers and os.path.isfile(a.tickers):
        cands = [l.strip().upper() for l in open(a.tickers, encoding="utf-8") if l.strip()]   # one ticker per line
    elif a.tickers:
        cands = [t.strip().upper() for t in a.tickers.split(",") if t.strip()]
    else:
        d = pd.read_csv(a.candidates); d["flag"] = d.flag.fillna("")
        cands = sorted(d[~d.flag.str.contains("no_seam_window")].ticker)
    done = set(); last_line = None
    if os.path.exists(a.log):
        for line in open(a.log, encoding="utf-8"):
            p = line.rstrip("\n").split("\t")
            if len(p) >= 3:
                last_line = p
                if p[2] == "0":
                    done.add(p[1])
    if last_line is not None and last_line[2] == "4":
        print(f"REFUSING TO START: the log's last line is an exit 4 for {last_line[1]} ({last_line[0]}) - its served state is "
              f"UNKNOWN until `python seam_rebase.py {last_line[1]} --restore <its snapshot dir>` has run. Then append a line to "
              f"the log recording the restore (any exit code but 4) and re-run.")
        return 1
    todo = [t for t in cands if t not in done]
    print(f"candidates {len(cands)}; already exit-0 in log {len(done & set(cands))}; to do {len(todo)}; mode {a.mode}; apply {a.apply}")
    if not a.apply:
        print("  first 20:", " ".join(todo[:20])); print("(dry run - pass --apply to run the batch)"); return 0
    n = 0; stopped = False; incomplete = []; aborted = []
    for t in todo:
        if a.limit is not None and n >= a.limit:
            break
        cmd = [sys.executable, "-u", os.path.join(HERE, "seam_rebase.py"), t, "--mode", a.mode, "--apply",
               "--snapshot-dir", os.path.join(a.snapshot_root, t)]
        if a.events_file:
            cmd += ["--events-file", a.events_file]
        t0 = dt.datetime.now()
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=HERE)
        last = (p.stdout.strip().splitlines() or [""])[-1]
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{t0:%Y-%m-%d %H:%M:%S}\t{t}\t{p.returncode}\t{(dt.datetime.now() - t0).total_seconds():.0f}s\t{last[:200]}\n")
        print(f"  {t:6} exit {p.returncode}  {last[:130]}", flush=True)
        n += 1
        if p.returncode in STOP_TEXT:
            print(STOP_TEXT[p.returncode] + "\n" + p.stdout[-2500:] + p.stderr[-800:])
            stopped = True; break
        if p.returncode == 6:
            incomplete.append(t)
        elif p.returncode == 5:
            aborted.append(t)
        elif p.returncode not in (0, 2, 3):
            # an exit code the tool does not define (an uncaught crash in a pre-write section, a
            # missing dependency): the tool wrote nothing only if it never reached snapshot(); stop and read
            print(f"STOPPING: undefined exit code {p.returncode} from seam_rebase.py for {t}\n" + p.stdout[-2500:] + p.stderr[-1200:])
            stopped = True; break
    if incomplete:
        print(f"SERVING INCOMPLETE for {len(incomplete)} ticker(s) - prices rebased and verified, variables/quality sync failed: "
              f"{' '.join(incomplete)}. Run sync_ticker_variables(force_full=True) for each (a re-run of the tool will not).")
    if aborted:
        print(f"aborted before any write (exit 5), re-runnable: {' '.join(aborted)}")
    print(f"batch {'stopped' if stopped else 'done'}: {n} processed this run; log {a.log}")
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
