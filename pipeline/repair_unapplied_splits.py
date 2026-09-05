"""repair_unapplied_splits.py — apply the stock splits that the daily detector alerted on and nobody
acted on, one ticker at a time, with the proven manual_split path (this worktree's manual_split takes
a CA_DATE and rescales only bars STRICTLY BEFORE it, exactly as APH was repaired on 2026-09-04).

    python repair_unapplied_splits.py D:\\temp\\claude\\unapplied_repair_list.txt [--apply] [--limit N]
    list lines:  TICKER RATIO CA_DATE     (RATIO = new_price/old_price; from unapplied_repair_list.py)

Per event, in order:
  1. PRE-CHECK from the served daily against Yahoo: the served close step across CA_DATE divided by
     Yahoo's adjusted step must equal 1/RATIO within 5 % (the split really is unapplied right now);
     otherwise the event is skipped and reported (already applied, or not what the list says).
  2. SNAPSHOT every served object for the ticker (raw/, clean/, variables/, quality/, csv/) to
     F:\\hf_r2_snapshot_splits_<date>\\TICKER with size and MD5/ETag checks (seam_rebase.snapshot).
  3. manual_split TICKER RATIO CA_DATE, non-interactive (its confirmation prompt is answered with the
     ticker on stdin), output captured.
  4. POST-CHECK: re-download the served daily; the step across CA_DATE divided by Yahoo's must now be
     1.00 within 2 %, and the last close must be unchanged (bars on/after CA_DATE untouched).
  5. one log line AFTER the event finishes (ledger R591); a failed post-check STOPS the batch and
     names the snapshot to restore (python ..\\..\\hf_wt_main\\pipeline\\seam_rebase.py T --restore DIR).
Tickers with two events (IPW) are handled event by event in list order; each pre-check is against
the then-current served data, so the second event sees the first one's result.
"""
from __future__ import annotations
import argparse, datetime as dt, os, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MAIN_PIPE = os.path.normpath(os.path.join(HERE, "..", "..", "hf_wt_main", "pipeline"))
sys.path.insert(0, MAIN_PIPE)                     # seam_rebase.snapshot / yahoo live in the main worktree
from r2_client import download_parquet, get_client  # noqa: E402  (this worktree's, identical to main's)
from seam_rebase import snapshot, yahoo             # noqa: E402


def step_rel(client, t: str, ca: pd.Timestamp, y: pd.Series):
    d = download_parquet(client, "raw", t, "daily"); d["datetime"] = pd.to_datetime(d["datetime"])
    dd = d.set_index("datetime").sort_index()["Close"]
    prev = dd[dd.index < ca]; post = dd[dd.index >= ca]
    if prev.empty or post.empty:
        return None, None, None
    p_d, q_d = prev.index[-1], post.index[0]
    if p_d not in y.index or q_d not in y.index:
        return None, None, None
    rel = float(post.iloc[0] / prev.iloc[-1]) / float(y[q_d] / y[p_d])
    return rel, float(dd.iloc[-1]), (p_d.date(), q_d.date())


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("listfile"); ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None); ap.add_argument("--log", default=r"D:\temp\claude\repair_unapplied_splits.log")
    ap.add_argument("--snapshot-root", default=os.path.join("F:\\", f"hf_r2_snapshot_splits_{dt.date.today():%Y%m%d}"))
    a = ap.parse_args()
    events = []
    for line in open(a.listfile, encoding="utf-8"):
        p = line.split()
        if len(p) >= 3:
            events.append((p[0].upper(), float(p[1]), pd.Timestamp(p[2])))
    print(f"events in list: {len(events)}; apply={a.apply}")
    client = get_client(); n = 0
    for t, ratio, ca in events:
        if a.limit is not None and n >= a.limit:
            break
        h, _, _ = yahoo(t)
        if h is None:
            print(f"  {t} {ca.date()}: no Yahoo reference - SKIP"); continue
        y = h["Close"]
        rel, last_close, days = step_rel(client, t, ca, y)
        if rel is None:
            print(f"  {t} {ca.date()}: cannot measure the step (no session on a side) - SKIP"); continue
        # RATIO in the list is new_price/old_price; an UNAPPLIED split shows exactly that ratio as the
        # served step across CA_DATE (BKNG: served/Yahoo step 0.0400 for RATIO 0.04), so expect RATIO itself
        expect = ratio
        if abs(rel / expect - 1) > 0.05:
            print(f"  {t} {ca.date()}: pre-check: served/Yahoo step {rel:.4f} is not ~{expect:.4f} - not unapplied as listed - SKIP"); continue
        print(f"  {t} {ca.date()}: pre-check OK (step rel {rel:.4f} ~ {expect:.4f}, sessions {days}); plan manual_split {t} {ratio:g} {ca.date()}")
        if not a.apply:
            n += 1; continue
        snap_dir = os.path.join(a.snapshot_root, t + "_" + ca.strftime("%Y%m%d"))
        n_snap = snapshot(client, t, snap_dir)
        print(f"    snapshot {n_snap} objects -> {snap_dir}")
        t0 = dt.datetime.now()
        p = subprocess.run([sys.executable, "-u", os.path.join(HERE, "manual_split.py"), t, f"{ratio:g}", ca.strftime("%Y-%m-%d")],
                           input=t + "\n", capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=HERE)
        tail = (p.stdout.strip().splitlines() or [""])[-1]
        rel2, last2, _ = step_rel(client, t, ca, y)
        ok = p.returncode == 0 and rel2 is not None and abs(rel2 - 1) <= 0.02 and last2 is not None and abs(last2 / last_close - 1) < 1e-9
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{t0:%Y-%m-%d %H:%M:%S}\t{t}\t{ca.date()}\t{ratio:g}\t{'OK' if ok else 'FAIL'}\trel {rel:.4f}->{rel2}\tlast {last_close}->{last2}\texit {p.returncode}\t{snap_dir}\n")
        print(f"    {'OK' if ok else 'FAIL'}: step rel {rel:.4f} -> {rel2}; last close {last_close} -> {last2}; manual_split exit {p.returncode}; {tail[:100]}")
        n += 1
        if not ok:
            print("STOPPING. manual_split output:\n" + p.stdout[-2500:] + p.stderr[-800:] + f"\nrestore: python {os.path.join(MAIN_PIPE, 'seam_rebase.py')} {t} --restore \"{snap_dir}\"")
            return 1
    print(f"{'dry run' if not a.apply else 'batch'} done: {n} events{' would be' if not a.apply else ''} processed; log {a.log}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
