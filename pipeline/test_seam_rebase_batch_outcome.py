"""The batch driver's outcome line and its "achieved nothing" paragraph, on the REAL main().

WHY THIS FILE EXISTS. Two consecutive reviews found defects in this epilogue with a throwaway
harness and nothing in `pipeline/` covered it: R870 #2 (an all-exit-6 batch, prices LIVE, told the
operator "Nothing was written.") and then R871 #2 (the fix for it went the other way and claimed
an all-exit-3 batch HAD written, while seam_rebase.py returns 3 only at "no market reference at
Yahoo", before snapshot() and before the first upload). A throwaway harness is not a guard, so
this one is checked in - the same reason `test_repair_apply_path.py` exists.

`_run_child` is replaced by a canned child that prints the header, sibling-hash and snapshot lines
the driver checks for and writes the matching `_RESULT.txt`, so every terminal exit code can be
driven without R2, without a network call and without a write outside a temp directory.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import io
import os
import shutil
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import seam_rebase_batch as B    # noqa: E402


def _drive(tool, codes, snapshotted=True):
    """codes: {ticker: rc}. Returns (driver_rc, stdout)."""
    root = tempfile.mkdtemp(prefix="batch_outcome_")
    log = os.path.join(root, "batch.log")
    snaproot = os.path.join(root, "snaps")
    shas = B._source_sha256s(tuple(dict.fromkeys((tool,) + B.GUARDED)))
    sib = ", ".join(f"{k[:-3]} {shas[k][:12]}" for k in B.GUARDED)
    tool_sha = shas[tool]

    def fake_child(cmd):
        t = None
        for i, c in enumerate(cmd):
            if c.endswith(".py") and i + 1 < len(cmd):
                t = cmd[i + 1]
                break
        rc = codes[t]
        pid = 4242
        out = [f"  tool source sha256 {tool_sha} ({os.path.join(HERE, tool)}) pid {pid}; "
               f"seam_rebase.py sha256 {shas['seam_rebase.py']}",
               f"  imported module sha256: {sib}"]
        if snapshotted and rc not in (5, 2, 7):
            out.append("  snapshot: 4 objects -> " + os.path.join(snaproot, t)
                       + " (size + MD5/ETag verified)")
            os.makedirs(os.path.join(snaproot, t), exist_ok=True)
            stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            with open(os.path.join(snaproot, t, "_RESULT.txt"), "w", encoding="utf-8") as fh:
                fh.write(f"{stamp}\tpid={pid}\tEXIT {rc} canned\n")
        out.append(f"  canned child for {t}: exit {rc}")
        out.append(f"  imported module sha256 at exit: {sib}")
        return rc, "\n".join(out) + "\n", "", False, pid

    argv = [sys.argv[0], "--tool", tool, "--tickers", ",".join(codes), "--log", log,
            "--snapshot-root", snaproot, "--apply"]
    if tool == "resync_variables.py":
        argv += ["--reviewed", "AR-000"]
    old_child, old_argv, old_cwd = B._run_child, sys.argv, os.getcwd()
    buf = io.StringIO()
    try:
        B._run_child = fake_child
        sys.argv = argv
        os.chdir(HERE)
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            rc = B.main()
    finally:
        B._run_child, sys.argv = old_child, old_argv
        os.chdir(old_cwd)
        shutil.rmtree(root, ignore_errors=True)
    return rc, buf.getvalue()


ONE_CAUSE = "almost always ONE cause"


def test_an_all_exit_3_batch_wrote_nothing():
    """R871 #2. seam_rebase.py returns 3 in exactly one place - "no market reference at Yahoo -
    cannot measure; disclose, do not repair" - which is BEFORE snapshot() and before the first
    upload_parquet. `unmeasurable` can hold nothing else: stop_text_for() halts the batch on a
    resync exit 3, and that path never reaches this paragraph."""
    rc, out = _drive("seam_rebase.py", {"A": 3, "B": 3})
    assert rc == 1
    assert "Nothing was written." in out, out
    assert "could not be verified (exit 3)" not in out, out


def test_an_all_exit_6_batch_did_write():
    """R870 #2, the other direction and the reason the fix above must be careful: exit 6 is
    "prices verified, variables/quality sync FAILED - not restoring", so those objects are live."""
    rc, out = _drive("seam_rebase.py", {"A": 6, "B": 6})
    assert rc == 1
    assert "Nothing was written." not in out, out
    assert "DID write their price objects" in out, out


def test_the_one_cause_paragraph_survives_an_exit_6_beside_the_no_writes():
    """R871 #2, lesser half: gating the paragraph on "nothing was written anywhere" meant a single
    exit 6 beside forty exit 5s hid the stale-`--reviewed`-id diagnosis, which is the commonest
    cause of a whole-batch no-op there is - every edit to the tool invalidates the approval bound
    to its hash."""
    rc, out = _drive("seam_rebase.py", {"A": 6, "B": 5})
    assert rc == 1
    assert ONE_CAUSE in out, out
    assert "stale --reviewed id" in out, out


def test_an_all_refused_batch_is_not_told_to_hunt_for_one_cause():
    """The mirror. A seam exit 2 is a per-ticker measurement verdict, and none of the three causes
    that paragraph names can produce one - so naming them would point at nothing."""
    rc, out = _drive("seam_rebase.py", {"A": 2, "B": 2})
    assert rc == 1
    assert "Nothing was written." in out, out
    assert ONE_CAUSE not in out, out


@pytest.mark.parametrize("tool,codes,want_rc,want_line", [
    ("seam_rebase.py", {"A": 6, "B": 0}, 0, "batch done: 2 processed this run, 1 completed"),
    ("seam_rebase.py", {"A": 0, "B": 0}, 0, "batch done: 2 processed this run, 2 completed"),
    # resync exit 2 is "already consistent", a success, not a manual item (R750 finding 3)
    ("resync_variables.py", {"A": 2, "B": 2}, 0, "batch done: 2 processed this run, 2 completed"),
])
def test_a_normal_partial_run_is_not_mislabelled(tool, codes, want_rc, want_line):
    """R866 #4/R868 #4 made the outcome derive from what was ACHIEVED; this pins that a run which
    achieved something still reports "done", so the guard cannot drift into refusing every batch."""
    rc, out = _drive(tool, codes)
    assert rc == want_rc
    assert want_line in out, out
