"""resync_variables.py - make the served variables/quality objects agree with the served bars, one ticker at a
time, with a snapshot, a measurement before, a force_full recompute, and a verification after.

WHY (2026-09-05, handoff section 15, ledger R741/R743). The served clean variables and quality objects
disagree with the served clean 1-minute bars fleet-wide: on ten tickers outside the seam work they differ
from a fresh recompute with the pipeline's own compute_recent_days on 61-81 % of all sessions (AAPL 3,662 of
5,959; observed_bars median 2.3 %, rv_1min 5.3 %, overnight_return 40 %), every year 2002-2026, and the
2026-07-13 snapshot's own variables already disagreed with its own bars - the daily path computes only
the newest sessions (max_new=5) and never revisits older ones, so a re-clean or merge that changed the bars
left the variables describing bars that no longer exist. The 47 tickers rebased on 2026-09-05 are
consistent by construction (the seam tool re-syncs with force_full). This tool does the same for any list.

WHAT IT DOES for each ticker:
  1. downloads the served raw and clean 1-minute files;
  2. MEASURES: served variables vs compute_recent_days(bars, existing_dates=set()) for raw and clean -
     sessions differing, columns, first/last (the dry run stops here and prints it);
  3. with --apply (and --reviewed <PASSED.md line id>, never implied): content-checked snapshot of all
     22 served objects (seam_rebase.snapshot; exit 5 on any check), then sync_ticker_variables(...,
     force_full=True) for raw and clean, then VERIFY: the served variables and quality objects, read
     back, equal a fresh recompute on every session and column (raw and clean); a mismatch restores
     the snapshot (exit 1), a failed restore exits 4, a read-back failure exits 3 (data live, nothing
     restored). Every outcome is recorded in <snap_dir>/_RESULT.txt before it is printed (R735/R738).
Exit codes: 0 done and verified | 1 written then restored | 2 nothing to do (already consistent) |
3 unverifiable | 4 restore failed | 5 aborted before any write | 6 written, partly verified (see line).

    python resync_variables.py TICKER [--apply --reviewed AR-0NN] [--snapshot-dir DIR]
Run from inside pipeline/ of a MAIN-based tree.
"""
from __future__ import annotations
import argparse
import datetime as dt
import io
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from r2_client import get_client, download_parquet, download_to_buffer   # noqa: E402
from compute_variables import compute_recent_days                         # noqa: E402
from variables_sync import sync_ticker_variables, QUALITY_COLS            # noqa: E402
import seam_rebase                                                        # noqa: E402

_say, _record = seam_rebase._say, seam_rebase._record


def _own_sha256() -> str:
    import hashlib
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


_SOURCE_SHA = _own_sha256()          # taken at import, before anything else runs


def served_obj(client, version: str, t: str, kind: str):
    data = download_to_buffer(client, f"{version}/{kind}/{t}.parquet")
    return pd.read_parquet(io.BytesIO(data)) if data else None


def compare(served: pd.DataFrame | None, fresh: pd.DataFrame):
    """(sessions differing, columns differing {col: n}, common, only_served, only_fresh, first, last)"""
    if served is None or served.empty:
        return None
    dcol = "trade_date" if "trade_date" in served.columns else ("date" if "date" in served.columns else served.columns[0])
    s = served.copy(); f = fresh.copy()
    s[dcol] = pd.to_datetime(s[dcol]); f[dcol] = pd.to_datetime(f[dcol])
    s = s.set_index(dcol).sort_index(); f = f.set_index(dcol).sort_index()
    common = s.index.intersection(f.index)
    cols = [c for c in s.columns if c in f.columns]
    diff_days, colset = set(), {}
    for c in cols:
        x = s.loc[common, c]; y = f.loc[common, c]
        if np.issubdtype(x.dtype, np.number) and np.issubdtype(y.dtype, np.number):
            ne = ~np.isclose(x.astype(float).to_numpy(), y.astype(float).to_numpy(), rtol=1e-9, atol=1e-9, equal_nan=True)
        else:
            ne = (x.astype(str).to_numpy() != y.astype(str).to_numpy())
        if ne.any():
            colset[c] = int(ne.sum()); diff_days.update(common[ne])
    return (len(diff_days), colset, len(common), int(len(s.index.difference(f.index))), int(len(f.index.difference(s.index))),
            min(diff_days).date().isoformat() if diff_days else None, max(diff_days).date().isoformat() if diff_days else None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewed", default=None, help="the PASSED.md line id of the adversarial review that cleared this tool for --apply")
    ap.add_argument("--snapshot-dir", default=None)
    a = ap.parse_args(); t = a.ticker.upper()
    _say(f"  tool source sha256 {_SOURCE_SHA} ({os.path.abspath(__file__)}) pid {os.getpid()}; seam_rebase.py sha256 {seam_rebase._source_sha256()}")
    if a.apply and not a.reviewed:
        _say("--apply needs --reviewed <PASSED.md id>: a fleet-wide rewrite of served objects runs only after its review (exit 5)"); return 5
    client = get_client()
    bars = {}
    for version in ("raw", "clean"):
        b = download_parquet(client, version, t)
        if b is None or b.empty:
            _say(f"{t}: served {version} 1-minute file missing - aborted before any write"); return 5
        b["datetime"] = pd.to_datetime(b["datetime"]); bars[version] = b
    # MEASURE
    before = {}
    for version in ("raw", "clean"):
        fresh = compute_recent_days(bars[version], t, existing_dates=None, max_new=10 ** 9)   # exactly what force_full passes
        for kind in ("variables",):
            r = compare(served_obj(client, version, t, kind), fresh)
            before[f"{version}_{kind}"] = r
            if r is None:
                _say(f"  {version}/{kind}: served object missing")
            else:
                _say(f"  {version}/{kind}: {r[0]:,} of {r[2]:,} sessions differ from a fresh recompute ({r[5]}..{r[6]}); "
                     f"only-served {r[3]}, only-fresh {r[4]}; columns {sorted(r[1])[:8]}")
    stale = sum((r[0] if r else 0) for r in before.values())
    if stale == 0 and all(r is not None for r in before.values()):
        _say(f"  {t}: served variables already equal a fresh recompute on every session - nothing to do"); return 2
    if not a.apply:
        _say("(dry run - the measurement above is the finding; pass --apply --reviewed <id> to rewrite)"); return 0

    st = seam_rebase.daily_run_state()
    if st == "in_progress" or st == "queued" or st == "unknown":
        _say(f"  REFUSED: Daily Data Update workflow is {st}; a resync inside its window is overwritten"); return 2
    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_vars_{dt.datetime.now(dt.timezone.utc):%Y%m%d}", t)
    n_snap = seam_rebase.snapshot(client, t, snap_dir)             # exit 5 on any check
    _say(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")
    seam_rebase._STATE["wrote"] = True
    written = []
    try:
        for version in ("raw", "clean"):
            sync_ticker_variables(client, version, t, bars[version], force_full=True)
            written.append(version)
        # VERIFY: read back, compare with a fresh recompute - every session, every column
        bad = []
        for version in ("raw", "clean"):
            fresh = compute_recent_days(bars[version], t, existing_dates=None, max_new=10 ** 9)   # exactly what force_full passes
            try:
                srv = served_obj(client, version, t, "variables")
            except Exception as ex:                                   # noqa: BLE001
                _record(snap_dir, f"EXIT 3 UNVERIFIABLE read-back {version}: {type(ex).__name__}: {str(ex)[:160]}")
                _say(f"  UNVERIFIABLE, DATA LIVE: read-back of {version}/variables failed ({type(ex).__name__}); nothing restored"); return 3
            r = compare(srv, fresh)
            if r is None or r[0] != 0 or r[3] != 0 or r[4] != 0:
                bad.append((version, r))
            _say(f"  VERIFY {version}/variables: {'OK' if (r and r[0] == 0 and r[3] == 0 and r[4] == 0) else 'MISMATCH'} "
                 f"({r[0] if r else 'missing'} differing of {r[2] if r else 0})")
            # the quality object is the variables' column subset (variables_sync: merged[qcols]); read it back too
            try:
                q = served_obj(client, version, t, "quality")
            except Exception as ex:                                   # noqa: BLE001
                _record(snap_dir, f"EXIT 3 UNVERIFIABLE read-back {version}/quality: {type(ex).__name__}: {str(ex)[:160]}")
                _say(f"  UNVERIFIABLE, DATA LIVE: read-back of {version}/quality failed ({type(ex).__name__}); nothing restored"); return 3
            qcols = [c for c in QUALITY_COLS if c in (srv.columns if srv is not None else [])]
            rq = compare(q, srv[qcols]) if (srv is not None and q is not None) else None
            q_ok = bool(rq and rq[0] == 0 and rq[3] == 0 and rq[4] == 0 and list(q.columns) == qcols)
            if not q_ok:
                bad.append((f"{version}/quality", rq))
            _say(f"  VERIFY {version}/quality: {'OK' if q_ok else 'MISMATCH'} (columns {list(q.columns) if q is not None else 'missing'})")
        if bad:
            raise RuntimeError(f"verify mismatch: {[(v, r[0] if r else None) for v, r in bad]}")
    except BaseException as ex:                                       # noqa: BLE001
        why = f"{type(ex).__name__}: {str(ex)[:200]}"
        try:
            n_back = seam_rebase.restore(client, snap_dir)
        except BaseException as ex2:                                  # noqa: BLE001
            _record(snap_dir, f"EXIT 4 RESTORE FAILED after writing {written}; cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            _say(f"  FAILED ({why}) and the RESTORE FAILED ({type(ex2).__name__}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        _record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after writing {written}; cause {why}")
        _say(f"  FAILED ({why}) - restored {n_back} objects from {snap_dir}; served state is the pre-resync state"); return 1
    _record(snap_dir, f"EXIT 0 DONE variables+quality resynced (force_full) raw+clean; before: "
                      + "; ".join(f"{k} {v[0] if v else 'missing'} stale" for k, v in before.items()))
    _say(f"  DONE: {t} variables and quality resynced from the served bars and verified; snapshot kept at {snap_dir}")
    return 0


def _guarded_main() -> int:
    st = seam_rebase._STATE
    try:
        return main()
    except SystemExit as ex:
        if isinstance(ex.code, int) or ex.code is None:
            raise
        _say(f"  {ex.code}"); return 4 if st["wrote"] else 5
    except BaseException as ex:                                       # noqa: BLE001
        _say(f"  {'ESCAPED AFTER WRITES - served state UNKNOWN (exit 4)' if st['wrote'] else 'ABORTED before any write (exit 5)'}: "
             f"{type(ex).__name__}: {str(ex)[:300]}"); return 4 if st["wrote"] else 5


if __name__ == "__main__":
    sys.exit(_guarded_main())
