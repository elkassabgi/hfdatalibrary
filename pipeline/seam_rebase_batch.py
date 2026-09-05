"""seam_rebase_batch.py — drive seam_rebase.py (v5, self-measuring) across the served population,
one ticker at a time. The tool decides per ticker; this driver only sequences and records.

    python seam_rebase_batch.py [--apply] [--limit N] [--tickers A,B,...|FILE] [--candidates CSV] [--events-file CSV]

Candidates: every ticker with a PiTrading/IEX seam window — from seam_measure_K.py's CSV (rows
whose flag does not say no_seam_window), or the explicit --tickers list / one-per-line file. The
tool re-measures each one, so a stale factor in the CSV cannot reach the data.

Exit codes from seam_rebase.py and what the driver does with each (one meaning per code, R731/R735):
    0  rebased and verified, or nothing to rebase / already on target -> record, continue
    2  refused before any write (measurement says no)                  -> record, continue (manual list)
    3  unmeasurable (no market reference / reassigned symbol)          -> record, continue (disclose list)
    5  aborted before any write (missing object, snapshot check, crash) -> record, continue (retry list)
    6  prices verified, variables/quality sync FAILED                  -> record, continue, LIST at the end
    1  written then RESTORED from its snapshot; served = pre-rebase    -> record, STOP (read why)
    4  written and the RESTORE FAILED, or an inconsistent served set   -> record, STOP; --restore first
    7  deferred: the daily window (resync_variables.py only)            -> record, continue, LIST at the end
--tool resync_variables.py drives that tool instead (same header, same record grammar, its own snapshot);
--mode / --convention-decided / --events-file are seam_rebase.py's; --reviewed is resync_variables.py's.
The driver refuses to START while the log's last line is an exit 4. THE CHILD IS NOT KILLED BY AN
INTERRUPT (R735): it runs in its own process group, and a Ctrl-C on the driver waits for the running
ticker to finish (the tool restores itself if it must), logs it, and then stops the batch. One line
per ticker is appended to the log AFTER the tool exits — never before — with a UTC-labelled stamp
(ledger R591/R729), and the ticker's FULL stdout+stderr goes to a detail file next to the log
(seam_detail_<UTC>_<TICKER>.txt), so the VERIFY lines exist somewhere. Re-runs skip tickers already
logged with exit 0. --mode full needs --convention-decided on the driver too; it is never implied.
THE SOURCE GUARD (R742/R743/R744): the tool AND the five modules it imports live (aggregate, r2_client,
variables_sync, compute_variables, symbol_map) are hashed at start and before every launch; any change
stops the batch before the next child. The child's header hash must equal the launch hash: a different
hash under a write is logged 4 (served state unknown); without a write it stops the batch.
"""
from __future__ import annotations
import argparse, datetime as dt, os, re, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RELEASE = ("To release the batch after the restore: append ONE line to the log in its own format - "
           "<UTC stamp>\\t<TICKER>\\t<code>\\t0s\\t<note> - with code 0 if the restore succeeded (e.g. "
           "`2026-09-05T13:00:00Z\\tXYZ\\t0\\t0s\\trestored by hand from <snap dir>`); any other code keeps the refusal.")

STOP_TEXT = {
    1: ("STOPPING: the last ticker was written and then RESTORED from its snapshot (a failure between the first "
        "upload and the last VERIFY line, or a VERIFY mismatch); served state is the pre-rebase state. Read why "
        "before continuing."),
    4: ("STOPPING - SERVED STATE UNKNOWN or INCONSISTENT: the last ticker was written AND its automatic restore failed, "
        "or the tool found a partial earlier write. Run the printed --restore command (or re-aggregate) before anything "
        "else. " + RELEASE),
}


def _utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


GUARDED = ("seam_rebase.py", "aggregate.py", "r2_client.py", "variables_sync.py", "compute_variables.py", "symbol_map.py")


def _source_sha256s(names=GUARDED) -> dict:
    """sha256 of the tool AND of every module it imports live (R744 finding 4): a guard that watched
    seam_rebase.py alone let an edited aggregate.py run under the batch."""
    import hashlib
    out = {}
    for name in names:
        try:
            with open(os.path.join(HERE, name), "rb") as f:
                out[name] = hashlib.sha256(f.read()).hexdigest()
        except OSError:
            out[name] = "missing"
    return out


HEADER_RE = re.compile(r"tool source sha256 (\S+) \(.*\) pid (\d+)")


RECORD_RE = re.compile(r"^(\S+)\t(?:pid=(\d+)\t)?(EXIT \d+.*)$")


def _run_child(cmd: list[str]):
    """Run the tool in its own process group and wait for it even through a Ctrl-C on the driver:
    a TerminateProcess mid-restore would leave a half-restored set (R735 finding 1). Returns
    (returncode, stdout, stderr, interrupted)."""
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8",
                         errors="replace", cwd=HERE, creationflags=flags)
    interrupted = False
    while True:
        try:
            out, err = p.communicate()
            return p.returncode, out or "", err or "", interrupted, p.pid
        except KeyboardInterrupt:
            interrupted = True
            try:
                print("  interrupt received - waiting for the running ticker to finish (the tool restores itself if it must); "
                      "the batch stops after it", flush=True)
            except Exception:                                    # noqa: BLE001
                pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--tickers", default=None); ap.add_argument("--candidates", default=r"D:\temp\claude\seam_K.csv")
    ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--convention-decided", action="store_true", help="required with --mode full; passed through to the tool")
    ap.add_argument("--log", default=r"D:\temp\claude\seam_rebase_batch.log")
    ap.add_argument("--snapshot-root", default=os.path.join("F:\\", f"hf_r2_snapshot_seam_{dt.datetime.now(dt.timezone.utc):%Y%m%d}"))
    ap.add_argument("--events-file", default=None, help="passed through to seam_rebase.py (issuer-recorded split events, see its docstring)")
    ap.add_argument("--tool", default="seam_rebase.py", choices=("seam_rebase.py", "resync_variables.py"),
                    help="the per-ticker tool to drive; both print the same header and write the same record grammar")
    ap.add_argument("--reviewed", default=None, help="resync_variables.py only: the PASSED.md id passed through as --reviewed")
    a = ap.parse_args()
    seam = a.tool == "seam_rebase.py"
    if a.mode == "full" and not a.convention_decided:
        print("--mode full folds dividends into the pre-2022 half; that is the convention decision. Pass --convention-decided "
              "on the driver only once it is recorded (every ticker would exit 5 otherwise)."); return 1
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
              f"UNKNOWN until `python seam_rebase.py {last_line[1]} --restore <its snapshot dir>` has run. " + RELEASE)
        return 1
    todo = [t for t in cands if t not in done]
    print(f"candidates {len(cands)}; already exit-0 in log {len(done & set(cands))}; to do {len(todo)}; mode {a.mode}; apply {a.apply}")
    if not a.apply:
        print("  first 20:", " ".join(todo[:20])); print("(dry run - pass --apply to run the batch)"); return 0
    detail_dir = os.path.dirname(os.path.abspath(a.log))
    # THE SOURCE GUARD (R743, after R742): the tool file is hashed at start and re-hashed before every
    # child; a batch never runs bytes that were not there when it started. An edit under the batch
    # stops it - the edit waits, or the batch is restarted deliberately on the new file.
    guarded = tuple(dict.fromkeys((a.tool,) + GUARDED))     # the launched tool first, then the live imports
    shas = _source_sha256s(guarded); tool_sha = shas[a.tool]
    print(f"tool {a.tool} source sha256 {tool_sha} at start; guarded modules: " + ", ".join(f"{k} {v[:12]}" for k, v in shas.items())
          + "; the batch refuses to launch a child if any of them changes", flush=True)
    n = 0; stopped = False; incomplete = []; aborted = []; refused = []; unmeasurable = []; deferred = []
    for t in todo:
        if a.limit is not None and n >= a.limit:
            break
        now = _source_sha256s(guarded)
        changed = [k for k in guarded if now.get(k) != shas.get(k)]
        if changed:
            print(f"STOPPING before {t}: {', '.join(changed)} changed under the batch ("
                  + "; ".join(f"{k} {shas[k][:12]} -> {now.get(k, 'missing')[:12]}" for k in changed)
                  + "). Nothing launched on the new file(s); restart the batch deliberately if they are the ones to run.", flush=True)
            stopped = True; break
        hash_breach = None
        cmd = [sys.executable, "-u", os.path.join(HERE, a.tool), t] + (["--mode", a.mode] if seam else []) + \
              ["--apply", "--snapshot-dir", os.path.join(a.snapshot_root, t)]
        if seam and a.mode == "full":
            cmd += ["--convention-decided"]
        if seam and a.events_file:
            cmd += ["--events-file", a.events_file]
        if not seam and a.reviewed:
            cmd += ["--reviewed", a.reviewed]
        t0 = dt.datetime.now(dt.timezone.utc)
        stamp = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
        rc, out, err, interrupted, child_pid = _run_child(cmd)
        raw_rc = rc                                          # the child's own code, kept in the detail header
        last = (out.strip().splitlines() or [""])[-1]
        # THE RECORD OUTRANKS THE EXIT CODE (R738 finding 1). A child killed from outside
        # (TerminateProcess) exits 1 - the code that means "written then RESTORED" - with nothing
        # restored and no record. Once the tool printed its "snapshot:" line, writes may have
        # happened, and the exit code is believed only when <snap_dir>/_RESULT.txt's last line says
        # the same thing; otherwise the ticker is logged as 4: served state UNKNOWN.
        # Keyed on the snapshot SUCCESS line, not the word "snapshot:" (R739): the four pre-write
        # abort lines ("snapshot: ... aborting before any write", exit 5) also carry the word, and
        # the first form of this check rewrote every one of them to 4 and stopped the batch.
        if re.search(r"^\s+snapshot: \d+ objects -> ", out, re.M):
            rec_path = os.path.join(a.snapshot_root, t, "_RESULT.txt")
            rec_last = None
            try:
                lines = [ln.rstrip("\n") for ln in open(rec_path, encoding="utf-8") if ln.strip()]
                rec_last = lines[-1] if lines else None
            except OSError:
                pass
            m = RECORD_RE.match(rec_last) if rec_last else None
            rec_stamp = m.group(1) if m else ""
            rec_pid = m.group(2) if m else None
            said = m.group(3) if m else (rec_last or "")
            # the child prints "pid <n>" in its header (v5.4+): a record without a pid under such a child,
            # or a child without the header at all, is not believed (R743 finding 4)
            hdr = HEADER_RE.search(out)
            hdr_sha = hdr.group(1) if hdr else None
            hdr_pid = hdr.group(2) if hdr else None
            # the header's hash must be the launch hash (R744 finding 5): the ~100 ms between the guard's
            # read and the child's module-top read is seen by nothing else
            hash_ok = hdr_sha == tool_sha
            # the record must be THIS run's: a parseable line, stamped at/after the child's start, written
            # by the child itself (R741 finding 5: another actor's "EXIT 1 RESTORED" after t0 was believed)
            fresh = bool(m) and rec_stamp >= stamp and hdr is not None and hash_ok and rec_pid == hdr_pid == str(child_pid)
            if not fresh or not (said.startswith(f"EXIT {rc} ") or said == f"EXIT {rc}"):
                if not rec_last:
                    why = "died without its record"
                elif not m:
                    why = "record line not in the grammar"
                elif hdr is None:
                    why = "child printed no hash/pid header"
                elif not hash_ok:
                    why = f"child header hash {hdr_sha[:12]} differs from the launch hash {tool_sha[:12]}"
                elif hdr_pid != str(child_pid):
                    why = "header pid differs from the launched pid (a wrapper or launcher between driver and tool)"
                elif rec_pid is None:
                    why = "record without a pid under a child that printed one"
                elif rec_pid != str(child_pid):
                    why = "record from another run or actor"
                elif rec_stamp < stamp:
                    why = "stale record from an earlier run"
                else:
                    why = "record disagrees with the exit code"
                last = (f"{why} (exit {rc}, _RESULT.txt says {said[:80]!r} at {rec_stamp or 'no stamp'} pid {rec_pid or '?'} vs child "
                        f"{child_pid}) - served state UNKNOWN; inspect {os.path.join(a.snapshot_root, t)} and --restore if the objects "
                        f"there differ from R2")
                rc = 4
        else:
            # no write reached; a header whose hash is not the launch hash still means other bytes ran (R744)
            hdr = HEADER_RE.search(out)
            if hdr and hdr.group(1) != tool_sha:
                hash_breach = f"child header hash {hdr.group(1)[:12]} differs from the launch hash {tool_sha[:12]} (no write reached; child exit {rc})"
                last = hash_breach + " - " + last
        detail = os.path.join(detail_dir, f"seam_detail_{t0:%Y%m%dT%H%M%SZ}_{t}.txt")
        try:
            with open(detail, "w", encoding="utf-8") as f:
                f.write(f"# {stamp} {' '.join(cmd)}\n# child exit {raw_rc}; logged as {rc}; child pid {child_pid}; tool sha256 at launch {tool_sha}; "
                        f"guarded " + ", ".join(f"{k} {v[:12]}" for k, v in shas.items()) + "\n"
                        f"--- stdout ---\n{out}\n--- stderr ---\n{err}\n")
        except Exception as e:                                   # noqa: BLE001
            print(f"  (detail file not written: {e})", flush=True)
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{stamp}\t{t}\t{rc}\t{(dt.datetime.now(dt.timezone.utc) - t0).total_seconds():.0f}s\t{last[:200]}\n")
        print(f"  {t:6} exit {rc}  {last[:130]}", flush=True)
        n += 1
        if hash_breach:
            print(f"STOPPING: {hash_breach} for {t} - the batch ran bytes other than the ones it hashed; restart deliberately", flush=True)
            stopped = True; break
        if rc in STOP_TEXT:
            print(STOP_TEXT[rc] + "\n" + out[-2500:] + err[-800:])
            stopped = True; break
        if rc == 6:
            incomplete.append(t)
        elif rc == 5:
            aborted.append(t)
        elif rc == 2:
            refused.append(t)
        elif rc == 3:
            unmeasurable.append(t)
        elif rc == 7:
            deferred.append(t)                                   # resync_variables.py: the daily window; run again later
        elif rc != 0:
            print(f"STOPPING: undefined exit code {rc} from seam_rebase.py for {t}\n" + out[-2500:] + err[-1200:])
            stopped = True; break
        if interrupted:
            print("batch stopped by the interrupt after the running ticker finished and was logged")
            stopped = True; break
    if incomplete:
        print(f"SERVING INCOMPLETE for {len(incomplete)} ticker(s) - prices rebased and verified, variables/quality sync failed: "
              f"{' '.join(incomplete)}. Run sync_ticker_variables(force_full=True) for each (a re-run of the tool will not).")
    if aborted:
        print(f"aborted before any write (exit 5) - read each one's line before re-running (a malformed events file or a kept "
              f"manifest is not fixed by a re-run): {' '.join(aborted)}")
    if refused:
        print(f"refused before any write (exit 2) - the manual list: {' '.join(refused)}")
    if unmeasurable:
        print(f"unmeasurable (exit 3) - the disclose list: {' '.join(unmeasurable)}")
    if deferred:
        print(f"deferred by the daily window (exit 7) - run again outside it: {' '.join(deferred)}")
    print(f"batch {'stopped' if stopped else 'done'}: {n} processed this run; log {a.log}; details in {detail_dir}/seam_detail_*.txt")
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
