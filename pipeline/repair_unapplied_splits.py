"""repair_unapplied_splits.py — apply the stock splits that the daily detector alerted on and nobody
acted on, one event at a time, with the proven manual_split path (this worktree's manual_split takes
a CA_DATE and rescales only bars STRICTLY BEFORE it, exactly as APH was repaired on 2026-09-04).
Version 2, after adversarial review (2026-09-05).

    python repair_unapplied_splits.py D:\\temp\\claude\\unapplied_repair_list.txt [--apply] [--limit N] [--allow-queued]
    list lines:  TICKER RATIO CA_DATE     (RATIO = new_price/old_price; from unapplied_repair_list.py)

Per event, in order:
  0. daily-run guard: a Daily Data Update run in progress (or queued without --allow-queued) refuses —
     the run re-uploads every object from old-basis frames and would overwrite the repair.
  1. PRE-CHECK, daily: served raw/daily close step across CA_DATE ÷ Yahoo's adjusted step must equal
     RATIO within 5 % (the split really is unapplied right now).
  2. PRE-CHECK, 1-minute (the review's double-scaling guard): the served raw AND clean 1-minute files'
     own step across CA_DATE (last bar before ÷ first bar on/after, ÷ Yahoo) must ALSO equal RATIO
     within 5 %, and raw and clean must agree within 1 %. manual_split rewrites raw/1-min first and
     raw/daily six uploads later; without this check a crash in that window would be rescaled twice
     on re-run. A disagreement means an interrupted earlier run: restore before anything else.
  3. SNAPSHOT every served object (raw/, clean/, variables/, quality/, csv/) to
     F:\\hf_r2_snapshot_splits_<date>\\TICKER_<CA> with size and MD5/ETag checks (seam_rebase.snapshot).
  4. manual_split TICKER RATIO CA_DATE, non-interactive (confirmation answered on stdin); its
     "re-cleaned:" line is kept in the log (the full re-clean flips ~0.1 % of pre-event clean bars at
     MAD float ties — measured BKNG 2,463 of 1.83 M, APH 442 — same in kind as the daily path).
  5. POST-CHECK: raw/daily AND clean/daily re-downloaded; the step across CA_DATE ÷ Yahoo must now be
     rel/RATIO within 0.5 % (exactly what the rescale predicts) and within 5 % of 1.0; last close of
     both unchanged (bars on/after CA_DATE untouched).
  6. one log line per event, written in a finally: so it lands even if the post-check itself raises;
     SKIPs are logged too. A FAIL stops the batch and names the snapshot to restore
     (python ..\\..\\hf_wt_main\\pipeline\\seam_rebase.py T --restore DIR).
Tickers with two events (IPW) are handled in list order; each pre-check reads the then-current data.
"""
from __future__ import annotations
import argparse, datetime as dt, os, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
MAIN_PIPE = os.path.normpath(os.path.join(HERE, "..", "..", "hf_wt_main", "pipeline"))
sys.path.insert(0, MAIN_PIPE)                     # seam_rebase.snapshot / yahoo / daily_run_state live in the main worktree
from r2_client import download_parquet, get_client  # noqa: E402  (main's copy; byte-identical to this worktree's)
from seam_rebase import snapshot, yahoo, daily_run_state  # noqa: E402


def daily_step(client, version: str, t: str, ca: pd.Timestamp, y: pd.Series):
    d = download_parquet(client, version, t, "daily"); d["datetime"] = pd.to_datetime(d["datetime"])
    s = d.set_index("datetime").sort_index()["Close"]
    prev = s[s.index < ca]; post = s[s.index >= ca]
    if prev.empty or post.empty:
        return None, None
    p_d, q_d = prev.index[-1], post.index[0]
    if p_d not in y.index or q_d not in y.index:
        return None, None
    return float(post.iloc[0] / prev.iloc[-1]) / float(y[q_d] / y[p_d]), float(s.iloc[-1])


def minute_step(client, version: str, t: str, ca: pd.Timestamp, y: pd.Series):
    d = download_parquet(client, version, t); dtm = pd.to_datetime(d["datetime"])
    if dtm.dt.tz is not None:
        dtm = dtm.dt.tz_localize(None)
    d = d.assign(_dt=dtm).sort_values("_dt")
    prev = d[d["_dt"] < ca]; post = d[d["_dt"] >= ca]
    if prev.empty or post.empty:
        return None
    p_d, q_d = prev["_dt"].iloc[-1].normalize(), post["_dt"].iloc[0].normalize()
    if p_d not in y.index or q_d not in y.index:
        return None
    return float(post["Close"].iloc[0] / prev["Close"].iloc[-1]) / float(y[q_d] / y[p_d])


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("listfile"); ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None); ap.add_argument("--allow-queued", action="store_true")
    ap.add_argument("--log", default=r"D:\temp\claude\repair_unapplied_splits.log")
    ap.add_argument("--snapshot-root", default=os.path.join("F:\\", f"hf_r2_snapshot_splits_{dt.date.today():%Y%m%d}"))
    a = ap.parse_args()
    events = []
    for line in open(a.listfile, encoding="utf-8"):
        p = line.split()
        if len(p) >= 3:
            events.append((p[0].upper(), float(p[1]), pd.Timestamp(p[2])))
    print(f"events in list: {len(events)}; apply={a.apply}")
    client = get_client(); n = 0

    def log(t, ca, ratio, verdict, detail, snap=""):
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}\t{t}\t{ca.date()}\t{ratio:g}\t{verdict}\t{detail}\t{snap}\n")

    for t, ratio, ca in events:
        if a.limit is not None and n >= a.limit:
            break
        h, _, _ = yahoo(t)
        if h is None:
            print(f"  {t} {ca.date()}: no Yahoo reference - SKIP"); log(t, ca, ratio, "SKIP", "no yahoo"); continue
        y = h["Close"]
        rel, last_close = daily_step(client, "raw", t, ca, y)
        if rel is None:
            print(f"  {t} {ca.date()}: cannot measure the daily step - SKIP"); log(t, ca, ratio, "SKIP", "no session"); continue
        if abs(rel / ratio - 1) > 0.05:
            print(f"  {t} {ca.date()}: pre-check: served/Yahoo daily step {rel:.4f} is not ~{ratio:.4f} - not unapplied as listed - SKIP")
            log(t, ca, ratio, "SKIP", f"daily rel {rel:.4f} != ratio"); continue
        m_raw = minute_step(client, "raw", t, ca, y); m_clean = minute_step(client, "clean", t, ca, y)
        if m_raw is None or m_clean is None:
            print(f"  {t} {ca.date()}: cannot measure the 1-minute step - SKIP"); log(t, ca, ratio, "SKIP", "no 1-min session"); continue
        if abs(m_raw / m_clean - 1) > 0.01:
            print(f"  {t} {ca.date()}: REFUSED - raw and clean 1-minute files disagree across the event (raw {m_raw:.4f}, clean {m_clean:.4f}): an earlier run was interrupted; restore first")
            log(t, ca, ratio, "REFUSED", f"1-min raw {m_raw:.4f} vs clean {m_clean:.4f}"); return 1
        if abs(m_raw / ratio - 1) > 0.05:
            print(f"  {t} {ca.date()}: REFUSED - 1-minute step {m_raw:.4f} disagrees with the daily step {rel:.4f} / ratio {ratio:g}")
            log(t, ca, ratio, "REFUSED", f"1-min {m_raw:.4f} vs daily {rel:.4f}"); return 1
        print(f"  {t} {ca.date()}: pre-checks OK (daily rel {rel:.4f}, 1-min raw {m_raw:.4f} clean {m_clean:.4f} ~ {ratio:.4f}); plan manual_split {t} {ratio:g} {ca.date()}")
        if not a.apply:
            n += 1; continue
        st = daily_run_state()
        if st == "in_progress" or (st == "queued" and not a.allow_queued) or st == "unknown":
            print(f"  REFUSED: Daily Data Update workflow is {st} - a repair inside its window is overwritten; stop here")
            log(t, ca, ratio, "REFUSED", f"daily run {st}"); return 2
        snap_dir = os.path.join(a.snapshot_root, t + "_" + ca.strftime("%Y%m%d"))
        n_snap = snapshot(client, t, snap_dir)
        print(f"    snapshot {n_snap} objects -> {snap_dir}")
        t0 = dt.datetime.now(); verdict = "FAIL"; detail = "post-check did not run"
        try:
            p = subprocess.run([sys.executable, "-u", os.path.join(HERE, "manual_split.py"), t, f"{ratio:g}", ca.strftime("%Y-%m-%d")],
                               input=t + "\n", capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=HERE)
            lines = p.stdout.strip().splitlines() or [""]
            cleaned = next((l for l in lines if "re-cleaned" in l), ""); tail = lines[-1]
            rel2, last2 = daily_step(client, "raw", t, ca, y); rel2c, last2c = daily_step(client, "clean", t, ca, y)
            expect2 = rel / ratio
            ok = (p.returncode == 0 and rel2 is not None and rel2c is not None
                  and abs(rel2 / expect2 - 1) <= 0.005 and abs(rel2 - 1) <= 0.05
                  and abs(rel2c / expect2 - 1) <= 0.005 and abs(rel2c - 1) <= 0.05
                  and last2 is not None and abs(last2 / last_close - 1) < 1e-9)
            verdict = "OK" if ok else "FAIL"
            detail = (f"daily rel {rel:.4f}->{rel2} (clean {rel2c}), expected {expect2:.4f}; last {last_close}->{last2}; "
                      f"exit {p.returncode}; {cleaned or tail}"[:300])
            print(f"    {verdict}: {detail}")
            if not ok:
                print("STOPPING. manual_split output:\n" + p.stdout[-2500:] + p.stderr[-800:] +
                      f"\nrestore: python {os.path.join(MAIN_PIPE, 'seam_rebase.py')} {t} --restore \"{snap_dir}\"")
        finally:
            log(t, ca, ratio, verdict, detail + f"; {(dt.datetime.now() - t0).total_seconds():.0f}s", snap_dir)
        n += 1
        if verdict != "OK":
            return 1
    print(f"{'dry run' if not a.apply else 'batch'} done: {n} events{' would be' if not a.apply else ''} processed; log {a.log}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
