"""resync_variables.py v2 - make the served variables/quality objects agree with the served bars, one ticker at a
time, with a scoped snapshot, a measurement before, a full recompute, and an EXACT verification after.

WHY (2026-09-05, handoff section 15, ledger R741/R743/R745). The served clean variables and quality objects
disagree with the served clean 1-minute bars fleet-wide: on ten tickers outside the seam work they differ from a
fresh recompute with the pipeline's own compute_recent_days on 61-81 % of all sessions (AAPL 3,662 of 5,959) - the
daily path computes only the newest sessions (max_new=5) and never revisits older ones, so a re-clean that changed
the bars left the variables describing bars that no longer exist. The 47+ tickers rebased on 2026-09-05 are
consistent by construction (the seam tool re-syncs with force_full). This tool does the same for any ticker.

WHAT IT DOES for each ticker:
  1. downloads the served raw and clean 1-minute files;
  2. MEASURES: served variables vs compute_recent_days(bars, ticker, existing_dates=None, max_new=1e9) - the exact
     call sync_ticker_variables(force_full=True) makes - for raw and clean: sessions differing (relative tolerance
     1e-9, NO absolute tolerance: R745 - amihud_illiquidity is ~1e-11 and an absolute 1e-9 hid it), columns, dates.
     The dry run (default) stops here and prints it.
  3. with --apply --reviewed <PASSED.md line id> (validated against PASSED.md; never implied): a content-checked
     snapshot of THE FOUR OBJECTS THIS TOOL WRITES ({raw,clean}/{variables,quality}/<T>.parquet; R745: the seam
     tool's 22-object snapshot would restore bars this tool never touched), then the measured frames - the same
     frames force_full would write, prepared the same way - are uploaded directly (no second compute after the
     write flag: R745), then VERIFY: each object read back must equal its frame EXACTLY (every finite value equal,
     NaN where NaN, identical column lists, no extra sessions on either side) and the quality object must equal the
     variables' QUALITY_COLS subset; a mismatch restores the snapshot (exit 1), a failed restore exits 4, a read-back
     failure exits 3 (data live, nothing restored). Every outcome after the measurement is recorded in
     <snap_dir>/_RESULT.txt BEFORE it is printed (R735/R738), with this file's sha256 and the review id.
Exit codes (one meaning each, R731/R735): 0 done and verified | 1 written then restored | 2 nothing to do (already
consistent) | 3 unverifiable (read-back failed; data live) | 4 restore failed, or an escape after the first upload |
5 aborted before any write (missing input, snapshot check, unvalidated --reviewed, crash) | 7 deferred: the Daily
Data Update workflow is running/queued/unknown (nothing written; run again outside its window).

    python resync_variables.py TICKER [--apply --reviewed AR-0NN] [--snapshot-dir DIR] [--passed-file PATH]
Run from inside pipeline/ of a MAIN-based tree, under seam_rebase_batch.py --tool resync_variables.py for a list.
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import io
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _own_sha256() -> str:
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


_SOURCE_SHA = _own_sha256()          # taken at import, before anything else runs (R741)

from r2_client import get_client, download_parquet, download_to_buffer, upload_parquet   # noqa: E402
from compute_variables import compute_recent_days                                        # noqa: E402
from variables_sync import QUALITY_COLS                                                  # noqa: E402
import seam_rebase                                                                       # noqa: E402
from seam_rebase import _say, _record, md5_of, BUCKET                                    # noqa: E402

PASSED_DEFAULT = r"D:\research\hfdatalibrary\.claude\skills\adversarial-review\PASSED.md"


def keys_of(t: str) -> list[str]:
    return [f"{v}/{k}/{t}.parquet" for v in ("raw", "clean") for k in ("variables", "quality")]


def served_obj(client, key: str):
    data = download_to_buffer(client, key)
    return pd.read_parquet(io.BytesIO(data)) if data else None


def prepare(fresh: pd.DataFrame) -> pd.DataFrame:
    """Exactly what sync_ticker_variables(force_full=True) does to the computed frame before uploading it."""
    f = fresh.copy()
    f["trade_date"] = pd.to_datetime(f["trade_date"]).dt.normalize()
    return f.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)


def compare(served: pd.DataFrame | None, fresh: pd.DataFrame, exact: bool):
    """(sessions differing, {col: n}, common, only_served, only_fresh, first, last, columns_identical)
    exact=False (MEASURE): |a-b| <= 1e-9 |b|, NaN == NaN. exact=True (VERIFY): a == b, NaN == NaN, nothing else."""
    if served is None or served.empty:
        return None
    s = served.copy(); f = fresh.copy()
    s["trade_date"] = pd.to_datetime(s["trade_date"]); f["trade_date"] = pd.to_datetime(f["trade_date"])
    s = s.set_index("trade_date").sort_index(); f = f.set_index("trade_date").sort_index()
    common = s.index.intersection(f.index)
    cols_identical = list(s.columns) == list(f.columns)
    cols = [c for c in s.columns if c in f.columns]
    diff_days, colset = set(), {}
    for c in cols:
        x = s.loc[common, c]; y = f.loc[common, c]
        if np.issubdtype(x.dtype, np.number) and np.issubdtype(y.dtype, np.number):
            xv = x.astype(float).to_numpy(); yv = y.astype(float).to_numpy()
            both_nan = np.isnan(xv) & np.isnan(yv)
            ne = ~((xv == yv) | both_nan) if exact else ~np.isclose(xv, yv, rtol=1e-9, atol=0, equal_nan=True)
        else:
            ne = (x.astype(str).to_numpy() != y.astype(str).to_numpy())
        if ne.any():
            colset[c] = int(ne.sum()); diff_days.update(common[ne])
    return (len(diff_days), colset, len(common), int(len(s.index.difference(f.index))), int(len(f.index.difference(s.index))),
            min(diff_days).date().isoformat() if diff_days else None, max(diff_days).date().isoformat() if diff_days else None,
            cols_identical)


def verify_ok(r) -> bool:
    return bool(r) and r[0] == 0 and r[3] == 0 and r[4] == 0 and r[7]


def snapshot4(client, t: str, out_dir: str) -> int:
    """The four objects this tool writes, content-checked (size, MD5 vs ETag), manifest in seam_rebase's format so
    seam_rebase.restore() restores it. Never overwrites an earlier manifest (R731). Any failure raises SystemExit(5):
    nothing has been uploaded yet."""
    if os.path.exists(os.path.join(out_dir, "_MANIFEST.txt")):
        _say(f"  snapshot: {out_dir} already holds a _MANIFEST.txt from an earlier run - refusing to overwrite it (R731); "
             f"if that run ended with exit 4 restore it first, otherwise pass a fresh --snapshot-dir")
        raise SystemExit(5)
    os.makedirs(out_dir, exist_ok=True)
    keys = []
    for k in keys_of(t):
        try:
            h = client.head_object(Bucket=BUCKET, Key=k)
        except Exception as ex:                                       # noqa: BLE001
            _say(f"  snapshot: {k} is not served ({type(ex).__name__}) - aborting before any write"); raise SystemExit(5)
        keys.append((k, int(h["ContentLength"]), h["ETag"].strip('"')))
    for k, size, etag in keys:
        dest = os.path.join(out_dir, k.replace("/", "__"))
        client.download_file(BUCKET, k, dest)
        if os.path.getsize(dest) != size:
            _say(f"  snapshot: {k} size {size} on R2 vs {os.path.getsize(dest)} on disk - aborting before any write"); raise SystemExit(5)
        if "-" not in etag and md5_of(dest) != etag:
            _say(f"  snapshot: {k} MD5 {md5_of(dest)} != ETag {etag} - aborting before any write"); raise SystemExit(5)
    with open(os.path.join(out_dir, "_MANIFEST.txt"), "w", encoding="utf-8") as f:
        for k, size, etag in keys:
            f.write(f"{size}\t{etag}\t{k}\n")
    return len(keys)


def reviewed_ok(review_id: str, passed_file: str) -> bool:
    """The id must be on a PASSED.md line that names this tool."""
    try:
        for ln in open(passed_file, encoding="utf-8", errors="replace"):
            if review_id in ln and "resync_variables" in ln:
                return True
    except OSError:
        return False
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewed", default=None, help="the PASSED.md line id of the adversarial review that cleared this tool for --apply")
    ap.add_argument("--snapshot-dir", default=None)
    ap.add_argument("--passed-file", default=PASSED_DEFAULT)
    a = ap.parse_args(); t = a.ticker.upper()
    _say(f"  tool source sha256 {_SOURCE_SHA} ({os.path.abspath(__file__)}) pid {os.getpid()}; seam_rebase.py sha256 {seam_rebase._source_sha256()}")
    if a.apply and not a.reviewed:
        _say("--apply needs --reviewed <PASSED.md id>: a rewrite of served objects runs only after its review (exit 5)"); return 5
    if a.apply and not reviewed_ok(a.reviewed, a.passed_file):
        _say(f"--reviewed {a.reviewed!r} is not on a line of {a.passed_file} that names resync_variables - refused (exit 5)"); return 5
    client = get_client()
    bars = {}
    for version in ("raw", "clean"):
        b = download_parquet(client, version, t)
        if b is None or b.empty:
            _say(f"{t}: served {version} 1-minute file missing - aborted before any write (exit 5)"); return 5
        b["datetime"] = pd.to_datetime(b["datetime"]); bars[version] = b
    # MEASURE - the frames computed here are the ones uploaded under --apply (no second compute: R745)
    fresh, before = {}, {}
    for version in ("raw", "clean"):
        fresh[version] = prepare(compute_recent_days(bars[version], t, existing_dates=None, max_new=10 ** 9))
        r = compare(served_obj(client, f"{version}/variables/{t}.parquet"), fresh[version], exact=False)
        before[version] = r
        if r is None:
            _say(f"  {version}/variables: served object missing")
        else:
            _say(f"  {version}/variables: {r[0]:,} of {r[2]:,} sessions differ from a fresh recompute ({r[5]}..{r[6]}); "
                 f"only-served {r[3]}, only-fresh {r[4]}; columns identical {r[7]}; differing columns {sorted(r[1])[:10]}")
    stale = sum((r[0] + r[3] + r[4]) if r else 0 for r in before.values())
    consistent = stale == 0 and all(r is not None and r[7] for r in before.values())
    if not a.apply:
        _say("  already consistent - nothing to do" if consistent else
             "(dry run - the measurement above is the finding; pass --apply --reviewed <id> to rewrite)")
        return 2 if consistent else 0
    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_vars_{dt.datetime.now(dt.timezone.utc):%Y%m%d}", t)
    os.makedirs(snap_dir, exist_ok=True)               # every outcome from here on is recorded
    if consistent:
        _record(snap_dir, "EXIT 2 nothing to do: served variables equal a fresh recompute on every session and column")
        _say(f"  {t}: already consistent - nothing to do (exit 2)"); return 2
    st = seam_rebase.daily_run_state()
    if st in ("in_progress", "queued", "unknown"):
        _record(snap_dir, f"EXIT 7 DEFERRED: Daily Data Update workflow is {st}; nothing written")
        _say(f"  DEFERRED (exit 7): Daily Data Update workflow is {st}; a resync inside its window is overwritten - run again later"); return 7
    written = []
    try:
        n_snap = snapshot4(client, t, snap_dir)
        _say(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")
        seam_rebase._STATE["wrote"] = True             # the first upload starts now
        for version in ("raw", "clean"):
            f = fresh[version]; qcols = [c for c in QUALITY_COLS if c in f.columns]
            upload_parquet(client, f, version, t, timeframe="variables"); written.append(f"{version}/variables/{t}.parquet")
            upload_parquet(client, f[qcols], version, t, timeframe="quality"); written.append(f"{version}/quality/{t}.parquet")
        # VERIFY: read back, EXACT equality with the frames just written (R745), every session and column, both objects
        bad = []
        for version in ("raw", "clean"):
            f = fresh[version]; qcols = [c for c in QUALITY_COLS if c in f.columns]
            try:
                srv = served_obj(client, f"{version}/variables/{t}.parquet")
                q = served_obj(client, f"{version}/quality/{t}.parquet")
            except Exception as ex:                                   # noqa: BLE001
                _record(snap_dir, f"EXIT 3 UNVERIFIABLE read-back {version}: {type(ex).__name__}: {str(ex)[:160]}; written {written}")
                _say(f"  UNVERIFIABLE, DATA LIVE: read-back of {version} failed ({type(ex).__name__}); nothing restored (exit 3)"); return 3
            r = compare(srv, f, exact=True); rq = compare(q, f[qcols], exact=True)
            ok_v = verify_ok(r); ok_q = verify_ok(rq) and q is not None and list(q.columns) == qcols
            _say(f"  VERIFY {version}/variables: {'OK' if ok_v else 'MISMATCH'} ({r[0] if r else 'missing'} differing of {r[2] if r else 0}; "
                 f"columns identical {r[7] if r else False})")
            _say(f"  VERIFY {version}/quality: {'OK' if ok_q else 'MISMATCH'} (columns {list(q.columns) if q is not None else 'missing'})")
            if not ok_v:
                bad.append((f"{version}/variables", r))
            if not ok_q:
                bad.append((f"{version}/quality", rq))
        if bad:
            raise RuntimeError("verify mismatch: " + "; ".join(f"{k} differing {r[0] if r else 'missing'} columns-identical {r[7] if r else False}" for k, r in bad))
    except BaseException as ex:                                       # noqa: BLE001
        why = f"{type(ex).__name__}: {str(ex)[:200]}"
        if not seam_rebase._STATE["wrote"]:
            _record(snap_dir, f"EXIT 5 aborted before any write: {why}")
            _say(f"  ABORTED before any write (exit 5): {why}"); return 5
        try:
            n_back = seam_rebase.restore(client, snap_dir)
        except BaseException as ex2:                                  # noqa: BLE001
            _record(snap_dir, f"EXIT 4 RESTORE FAILED after writing {written}; cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            _say(f"  FAILED ({why}) and the RESTORE FAILED ({type(ex2).__name__}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        _record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after writing {written}; cause {why}")
        _say(f"  FAILED ({why}) - restored {n_back} objects from {snap_dir}; served state is the pre-resync state (exit 1)"); return 1
    _record(snap_dir, f"EXIT 0 DONE variables+quality resynced raw+clean and verified exact; wrote {written}; reviewed {a.reviewed}; "
                      f"tool sha256 {_SOURCE_SHA}; seam_rebase.py sha256 {seam_rebase._source_sha256()}; before: "
                      + "; ".join(f"{k} {v[0] if v else 'missing'} stale" for k, v in before.items()))
    _say(f"  DONE: {t} variables and quality resynced from the served bars and verified exact; snapshot kept at {snap_dir}")
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
