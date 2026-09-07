"""The --apply path, run end to end against a local dict. No R2, no F:, no network.

WHY THIS FILE EXISTS. Three consecutive reviews found the defect in the FIX rather than in the
thing fixed, and every one of them lived past the `if not a.apply: return 0` line — where a dry
run stops and where, until now, no test reached. R865 #1 (`_served_read` stamping "datetime" on
objects keyed by `trade_date`) made every apply roll back its own 22 correct objects; R867 #1
(the counter split leaving `n` unbound in the restore handler) killed exit 1 for the whole upload
phase and wrote no `_RESULT.txt`. Both were found by an adversarial reviewer's throwaway harness
in under a second. A throwaway harness is not a guard, so the harness is checked in.

The pre-upload gates are stubbed flat-OK on purpose: the seven dry runs and
`test_repair_reassigned.py` cover them, and what is under test here is the EXIT-CODE SELECTION and
the restore/record behaviour, which is the part of the contract the docstring makes promises about.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import repair_reassigned as R    # noqa: E402
import seam_rebase               # noqa: E402

CUT = dt.date(2024, 2, 5)
T = "ZZTEST"
PRE = [dt.date(2021, 6, 1) + dt.timedelta(days=k) for k in range(4)]
KEPT = [dt.date(2023, 6, 1) + dt.timedelta(days=k) for k in range(4)]
FOREIGN = [dt.date(2024, 2, 5) + dt.timedelta(days=k) for k in range(3)]

OKROW = (CUT, "stub", 1.0, 1.0, 1.0, 1, 1, True)
BADROW = (CUT, "stub", 1.0, 2.0, 0.5, 1, 9, False)


def _frame(days, n_min=5, base=10.0):
    rows = []
    for i, d in enumerate(days):
        for m in range(n_min):
            rows.append({"datetime": pd.Timestamp(d) + pd.Timedelta(hours=10, minutes=m),
                         "Open": base + i, "High": base + i, "Low": base + i, "Close": base + i,
                         "Volume": 100 + m, "source": "iex"})
    return pd.DataFrame(rows)


SERVED = _frame(PRE + KEPT + FOREIGN)


def _install(monkeypatch, scenario, n_snap):
    """Wire every outward call to a dict. monkeypatch restores them all afterwards (R860: popping
    or overwriting a module global without restoring it breaks later tests in the same session)."""
    store = {("raw", T, "1min"): SERVED.copy(), ("clean", T, "1min"): SERVED.copy()}
    vdf = pd.DataFrame({"trade_date": pd.to_datetime(PRE + KEPT + FOREIGN), "ticker": T,
                        "gap_rate": 0.0, "observed_bars": 5, "longest_gap": 0,
                        "max_bars_since_trade": 0})
    for v in ("raw", "clean"):
        store[(v, T, "variables")] = vdf.copy()
        store[(v, T, "quality")] = vdf.copy()
    calls = {"uploads": [], "restores": 0, "reads": 0, "basis": 0, "prewin": 0, "records": []}

    def download_parquet(client, version, ticker, timeframe="1min"):
        calls["reads"] += 1
        if scenario in ("readback_unverifiable", "sync_fail_then_unverifiable") and calls["reads"] > 2:
            raise OSError("simulated R2 transport error on read-back")
        return store.get((version, ticker, timeframe))

    def upload_parquet(client, df, version, ticker, timeframe="1min"):
        if scenario == "upload_raises" and len(calls["uploads"]) == 5:
            raise OSError("simulated R2 transport error mid-upload")
        store[(version, ticker, timeframe)] = df.copy()
        calls["uploads"].append((version, ticker, timeframe))
        if scenario == "verify_a" and (version, timeframe) == ("raw", "daily"):
            extra = store[(version, ticker, timeframe)].iloc[[0]].copy()
            extra["datetime"] = pd.Timestamp("2024-02-09 10:00:00")
            store[(version, ticker, timeframe)] = pd.concat(
                [store[(version, ticker, timeframe)], extra], ignore_index=True)
        if scenario == "verify_c" and (version, timeframe) == ("raw", "1min"):
            store[(version, ticker, timeframe)] = store[(version, ticker, timeframe)].iloc[:-1]
        if scenario == "verify_g" and (version, timeframe) == ("clean", "1min"):
            d = store[(version, ticker, timeframe)].copy()
            d.loc[d.index[0], "Close"] = float(d.loc[d.index[0], "Close"]) + 1.0
            store[(version, ticker, timeframe)] = d
        return 1

    def sync(client, version, ticker, bars, force_full=False):
        if scenario in ("sync_raises", "sync_fail_then_unverifiable"):
            raise RuntimeError("simulated R2 error inside variables sync")
        if scenario == "sync_norows":
            return {"version": version, "ticker": ticker, "new_rows": 0}
        dates = sorted(set(bars["datetime"].dt.date))
        if scenario == "vars_stale":
            dates = dates + FOREIGN
        newv = pd.DataFrame({"trade_date": pd.to_datetime(dates), "ticker": ticker, "gap_rate": 0.0,
                             "observed_bars": 5, "longest_gap": 0, "max_bars_since_trade": 0})
        store[(version, ticker, "variables")] = newv
        store[(version, ticker, "quality")] = newv
        return {"version": version, "ticker": ticker, "new_rows": int(len(newv))}

    def restore(client, snap_dir):
        calls["restores"] += 1
        store[("raw", T, "1min")] = SERVED.copy()
        store[("clean", T, "1min")] = SERVED.copy()
        return n_snap

    def basis_gate(*a, **k):
        calls["basis"] += 1
        bad = scenario == "verify_d" and calls["basis"] >= 2
        return ([BADROW if bad else OKROW], not bad)

    def prewin(frame, version, ticker, cut):
        calls["prewin"] += 1
        bad = scenario == "verify_f" and calls["prewin"] >= 3
        return (1, 1, 1, 1, 0, 0) if bad else (0, 0, 0, 0, 0, 0)

    monkeypatch.setattr(R, "download_parquet", download_parquet)
    monkeypatch.setattr(R, "upload_parquet", upload_parquet)
    def upload_csv(*a, **k):
        # R869 #4 named this scenario and R872 #3 measured that nothing covered it: the fix that
        # counts a CSV put toward the upload tally is invisible unless a CSV put is the thing that
        # fails. Without it the restore line reads "after 0 upload(s)" while 1 object is live.
        if scenario == "csv_raises":
            raise OSError("simulated R2 transport error on the first CSV put")
        calls["uploads"].append(("csv", a[2], a[3]))
        return 1
    monkeypatch.setattr(R, "upload_csv", upload_csv)
    monkeypatch.setattr(R, "aggregate_all", lambda df: {tf: df.copy() for tf in R.TIMEFRAMES})
    monkeypatch.setattr(R, "sync_ticker_variables", sync)
    monkeypatch.setattr(R, "get_client", lambda: object())
    monkeypatch.setattr(R, "cut_gate", lambda *a, **k: ([("CUT-STUB", "stubbed OK", "OK")], True))
    monkeypatch.setattr(R, "basis_gate", basis_gate)
    monkeypatch.setattr(R, "pre_window_equality", prewin)
    monkeypatch.setattr(seam_rebase, "snapshot", lambda c, t, d: (os.makedirs(d, exist_ok=True), n_snap)[1])
    monkeypatch.setattr(seam_rebase, "restore", restore)
    monkeypatch.setattr(seam_rebase, "daily_run_state", lambda: "idle")
    monkeypatch.setattr(seam_rebase, "_record", lambda d, text: calls["records"].append(text))
    monkeypatch.setitem(seam_rebase._STATE, "wrote", False)
    return calls


def _run(monkeypatch, tmp_path, scenario, n_snap=22):
    calls = _install(monkeypatch, scenario, n_snap)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(),
                                      "--apply", "--snapshot-dir", str(tmp_path / scenario)])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    return code, calls


# ---------------------------------------------------------------- the exit-code contract

@pytest.mark.parametrize("scenario,want,why", [
    ("healthy",                    0, "done and verified"),
    ("upload_raises",              1, "written then RESTORED - the docstring's exit 1"),
    ("verify_a",                   1, "a stray served session at or after the cut"),
    ("verify_c",                   1, "served bar count != what was uploaded"),
    ("verify_d",                   1, "the basis gate FAILS on the served file"),
    ("verify_f",                   1, "served pre-window half != the 2026-07-13 snapshot"),
    ("verify_g",                   1, "a served clean price != the served raw price"),
    ("readback_unverifiable",      3, "read-back raised: DATA LIVE, nothing restored"),
    ("sync_raises",                6, "prices verified, variables sync failed"),
    ("sync_norows",                6, "the sync computed nothing and uploaded nothing"),
    ("vars_stale",                 6, "the four objects read back carrying foreign trade_dates"),
    ("sync_fail_then_unverifiable", 3, "exit 3 must ALSO name the stale variables objects"),
])
def test_the_documented_exit_code_comes_out(monkeypatch, tmp_path, scenario, want, why):
    code, _calls = _run(monkeypatch, tmp_path, scenario)
    assert code == want, f"{scenario}: got {code}, want {want} ({why})"


def test_a_short_price_upload_restores(monkeypatch, tmp_path):
    """n_price != n_snap - 4 means an object the snapshot holds was never written back."""
    code, calls = _run(monkeypatch, tmp_path, "healthy", n_snap=30)
    assert code == 1
    assert calls["restores"] == 1


# ---------------------------------------------------------------- what the operator is told

def test_a_restore_writes_a_RESULT_line_naming_the_upload_count(monkeypatch, tmp_path):
    """R867 #1. The counter split left `n` unbound in the `except BaseException` handler, so the
    handler itself raised: exit 4 instead of 1, NO _RESULT.txt, and an operator told the served
    state was UNKNOWN when the restore had in fact succeeded. An error handler that can raise is
    worse than no error handler."""
    code, calls = _run(monkeypatch, tmp_path, "upload_raises")
    assert code == 1
    assert calls["restores"] == 1
    assert calls["records"], "no _RESULT.txt line was written for a restore"
    assert "EXIT 1 RESTORED" in calls["records"][-1]
    assert "upload(s)" in calls["records"][-1]


def test_exit_three_names_the_stale_variables_objects(monkeypatch, tmp_path):
    """An exit 3 must never hide an exit 6 (R736)."""
    code, calls = _run(monkeypatch, tmp_path, "sync_fail_then_unverifiable")
    assert code == 3
    assert any("STALE" in r for r in calls["records"]), calls["records"]


def test_every_terminal_path_records_its_outcome(monkeypatch, tmp_path):
    for scenario in ("healthy", "upload_raises", "verify_g", "sync_raises",
                     "readback_unverifiable", "sync_norows"):
        _code, calls = _run(monkeypatch, tmp_path, scenario)
        assert calls["records"], f"{scenario} wrote no _RESULT.txt line"


# ---------------------------------------------------------------- refusals before any write

@pytest.mark.parametrize("extra,why", [
    (["--basis-samples", "0"], "checks no window session at all"),
    (["--own-split", "2030-01-01:1.0001"], "a near-1 factor swaps the exact volume test for a band"),
    (["--own-split", "2030-01-01:1.0"], "a factor of 1 is not a split"),
    (["--own-split", "2030-01-01:1.25"], "5:4 is below the 3:2 floor and never fires the detector"),
    (["--own-split", "2030-01-01:0"], "a zero factor is not a ratio"),
])
def test_an_input_that_would_weaken_the_gate_is_refused_before_any_write(monkeypatch, tmp_path, extra, why):
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(),
                                      "--apply", "--snapshot-dir", str(tmp_path / "refuse")] + extra)
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 5, why
    assert calls["uploads"] == [], "it must refuse BEFORE any write"


@pytest.mark.parametrize("factor", ["80", "0.125", "6", "1.5", "0.6666666"])
def test_a_real_split_ratio_is_still_accepted(monkeypatch, tmp_path, factor):
    """The positive control. A refusal rule with no accepting case is a refusal rule that has not
    been shown able to pass - VRM's declared 1-for-80 is the live one it must not block."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / ("ok" + factor)),
                                      "--own-split", "2030-01-01:" + factor])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 0, f"a declared {factor} own split was refused"
    # 18 PRICE objects: the four variables/quality ones are written by the sync stub, which does
    # not route through upload_parquet. VERIFY (i) is what asserts 18 + 4 against the snapshot.
    assert len(calls["uploads"]) == 18


def test_two_own_splits_that_each_pass_cannot_multiply_back_into_the_band(monkeypatch, tmp_path):
    """R869 #2. `basis_gate` expects the PRODUCT of the factors dated after a session, and `_cmp`
    swaps the exact volume test for a 0.60-1.05 band whenever that product is not 1. So checking
    each typed factor was checking the wrong number: 2 and 0.5001 each clear the floor and
    multiply to 1.0002."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "prod"),
                                      "--own-split", "2030-01-01:2", "--own-split", "2030-01-02:0.5001"])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 5
    assert calls["uploads"] == []


def test_two_own_splits_whose_product_is_a_real_split_are_still_accepted(monkeypatch, tmp_path):
    """The control. 2 and 40 multiply to 80 - VRM's real ratio - and must not be refused."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "prod_ok"),
                                      "--own-split", "2030-01-01:2", "--own-split", "2030-01-02:40"])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 0, "a product of 80 is a real split and must pass"
    assert len(calls["uploads"]) == 18


def test_a_rebuild_without_an_independent_oracle_is_refused(monkeypatch, tmp_path):
    """R869 #5. Without --verify-against, VERIFY (b) is skipped and every remaining check on the
    rebuilt range derives from the prints it was built from."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "norebuild"),
                                      "--rebuild-from-cs", "B", "--until", "2026-03-27"])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 5
    assert calls["uploads"] == []


def test_a_cut_gate_failure_is_exit_2_and_names_the_check_that_refused(monkeypatch, tmp_path, capsys):
    """R869 #1. The CUT-4 branch keyed on `failed == ["CUT-4"]`, and for the only ticker CUT-4
    exists for, CUT-1 fails on every run - including the correct one - so `failed` is always
    ["CUT-1", "CUT-4"] and the branch was dead. It must key on membership."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "cut_gate", lambda *a, **k: (
        [("CUT-1", "served discontinuity: 0 weekday(s)", "FAIL"),
         ("CUT-4", "anchored by the main pass", "FAIL")], False))
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "cut4"),
                                      "--rebuild-from-cs", "B", "--until", "2026-03-27",
                                      "--verify-against", "B"])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 2
    assert calls["uploads"] == []
    out = capsys.readouterr().out
    assert "--rebuild-from-cs" in out, out
    assert "--cut-gap-min" not in out, "a CUT-4 failure must not point at an override that cannot help"


def test_a_failed_restore_after_writes_is_exit_4(monkeypatch, tmp_path):
    """Exit 4 - written, restore FAILED, served state UNKNOWN - had no test at all."""
    # The failure has to happen AFTER the first upload: a pre-write refusal is exit 2, which is
    # what my first version of this test actually produced.
    calls = _install(monkeypatch, "upload_raises", 22)

    def boom(client, snap_dir):
        calls["restores"] += 1
        raise OSError("simulated R2 failure during the restore")
    monkeypatch.setattr(seam_rebase, "restore", boom)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "exit4")])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 4
    assert calls["restores"] == 1
    assert any("RESTORE FAILED" in r for r in calls["records"]), calls["records"]
    assert calls["uploads"], "exit 4 must mean objects were written and the restore failed"


def test_the_done_record_names_every_input_that_could_change_the_verdict(monkeypatch, tmp_path):
    """R867 #3: --own-split could relax the gate and was absent from the run record, while its own
    help text says "Cite the source in the run record"."""
    _code, calls = _run(monkeypatch, tmp_path, "healthy")
    rec = calls["records"][-1]
    for field in ("cut=", "until=", "unscale=", "kept_from=", "cut_gap_min=", "own_split=",
                  "anchors=", "basis_samples=", "rebuild_from_cs=", "verify_against="):
        assert field in rec, f"{field} missing from the EXIT 0 record: {rec}"


# ---------------------------------------------------------------- R872: what the ninth review found

def test_a_CSV_put_counts_toward_the_restore_tally(monkeypatch, tmp_path):
    """R872 #3. Reverting the fix that counts CSV puts left the whole suite green, so the fix was
    uncovered: the only way to see it is to make a CSV put be the thing that fails. Committed says
    "after 1 upload(s)", reverted says "after 0", while one object is live either way - and the
    upload count is what the operator reads to decide how much was written."""
    code, calls = _run(monkeypatch, tmp_path, "csv_raises")
    assert code == 1
    assert calls["restores"] == 1
    rec = calls["records"][-1]
    assert "EXIT 1 RESTORED" in rec, rec
    assert "after 0 upload(s)" not in rec, f"a CSV put that succeeded before the failure was not counted: {rec}"


def test_allow_queued_says_that_it_fired(monkeypatch, tmp_path):
    """R872 #4. The override used to pass through in silence: a `queued` run and an `idle` run
    printed the same lines and both recorded `allow_queued=True`, so neither the console nor
    _RESULT.txt could tell an override that suppressed a refusal from one that was never needed."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(seam_rebase, "daily_run_state", lambda: "queued")
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--allow-queued", "--snapshot-dir", str(tmp_path / "queued")])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 0
    rec = calls["records"][-1]
    assert "FIRED: state was queued" in rec, rec
    assert "daily_run_state=queued" in rec, rec


def test_queued_without_the_flag_still_refuses(monkeypatch, tmp_path):
    """The mirror: in_progress and unknown are never overridable, and queued is only overridable
    on purpose. A test that only proves the override works would pass on a gate that never fires."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(seam_rebase, "daily_run_state", lambda: "queued")
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--snapshot-dir", str(tmp_path / "queued2")])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 2
    assert not calls["uploads"]


def test_an_unreachable_oracle_refuses_BEFORE_the_write(monkeypatch, tmp_path):
    """R872 #5. VERIFY (b) is mandatory with --verify-against but runs once 22 objects are live, so
    an unreachable Yahoo ended the run at exit 3 - DATA LIVE, unverifiable - where a pre-flight
    fetch ends it at 5 with nothing written."""
    calls = _install(monkeypatch, "healthy", 22)

    def boom(symbol, start, end):
        raise OSError("simulated Yahoo transport failure")
    monkeypatch.setattr(R, "_yahoo_close", boom)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--verify-against", "B", "--rebuild-from-cs", "B",
                                      "--until", "2026-03-27",
                                      "--snapshot-dir", str(tmp_path / "oracle")])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 5, f"an unreachable oracle must abort before any write, got {code}"
    assert not calls["uploads"], "nothing may be written when the oracle cannot be reached"


def test_the_oracle_preflight_does_not_fire_when_VERIFY_b_would_check_nothing(monkeypatch, tmp_path):
    """R876 #2. VERIFY (b) compares REBUILT sessions, so without --rebuild-from-cs there are none
    and it reports "0/0 within 1 % -> OK" - a no-op. Firing the pre-flight there aborted a correct
    repair at exit 5 for a check that decides nothing."""
    calls = _install(monkeypatch, "healthy", 22)
    called = []

    def boom(symbol, start, end):
        called.append(symbol)
        raise OSError("the oracle must not be consulted on this path")
    monkeypatch.setattr(R, "_yahoo_close", boom)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--verify-against", "B",
                                      "--snapshot-dir", str(tmp_path / "oracle2")])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    assert code == 0, f"a repair whose VERIFY (b) is a no-op must not abort, got {code}"
    assert called == [], called
    assert calls["uploads"]


def test_the_daily_run_state_is_printed_on_every_apply(monkeypatch, tmp_path, capsys):
    """R876 #6 - reverting this console line left the whole suite green, so the behavioural half
    of the R872 #4 fix was uncovered. The record in `_RESULT.txt` was tested; what the operator
    watching the run sees was not, and those are different surfaces."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(seam_rebase, "daily_run_state", lambda: "queued")
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--allow-queued", "--snapshot-dir", str(tmp_path / "dailyline")])
    try:
        R._guarded_main()
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "daily run state: queued" in out, out
    assert "OVERRIDDEN by --allow-queued" in out, out
    assert calls["uploads"]


def test_an_idle_apply_says_so_without_claiming_an_override(monkeypatch, tmp_path, capsys):
    """The mirror: the line must distinguish the two, which is the whole point of adding it."""
    _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(), "--apply",
                                      "--allow-queued", "--snapshot-dir", str(tmp_path / "idleline")])
    try:
        R._guarded_main()
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "daily run state: idle" in out, out
    assert "OVERRIDDEN" not in out, out


def test_both_apply_only_flags_carry_help_text():
    """R876 #6 - the two --help strings had no failing revert either. `--help` is the only place an
    operator meets a flag that silently relaxes the write-window gate."""
    import subprocess
    p = subprocess.run([sys.executable, os.path.join(HERE, "repair_reassigned.py"), "--help"],
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, p.stderr[-400:]
    for flag, phrase in (("--allow-queued", "QUEUED"), ("--snapshot-dir", "_MANIFEST.txt")):
        assert flag in p.stdout, p.stdout
        assert phrase in p.stdout, f"{flag} has no help text mentioning {phrase}"


def test_the_oracle_dependency_is_declared():
    """R876 #6 - VERIFY (b) is mandatory with --verify-against and imports yfinance lazily; the
    declaration was added with nothing to keep it there."""
    req = open(os.path.join(HERE, "requirements.txt"), encoding="utf-8").read()
    assert "yfinance" in req, req


def test_a_CUT_3_failure_is_named_even_when_CUT_4_also_fails(monkeypatch, tmp_path, capsys):
    """R872 #2. `"CUT-4" in failed` shadowed CUT-3, so a cut that is too EARLY - the first dropped
    session having printed in the main pass - was reported only as an anchor problem."""
    _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "cut_gate", lambda *a, **k: (
        [("CUT-3", "the first dropped session printed in the main pass", "FAIL"),
         ("CUT-4", "the last kept session is not cs-anchored", "FAIL")], False))
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat(),
                                      "--rebuild-from-cs", "B", "--verify-against", "B",
                                      "--until", "2026-03-27"])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    out = capsys.readouterr().out
    assert code == 2
    assert "CUT-3" in out.split("REFUSED:")[-1], f"CUT-3 was not named in the refusal: {out}"


def test_a_failing_CUT_1_is_not_named_when_CUT_2_carried_the_gate(monkeypatch, tmp_path, capsys):
    """R869's second recommendation. CUT-1 and CUT-2 are alternatives; GOLD's CUT-1 is 0 weekdays
    on every run, the correct one included, so naming it pointed the reader at a check that was
    never the obstacle."""
    _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "cut_gate", lambda *a, **k: (
        [("CUT-1", "0 weekdays of served discontinuity", "FAIL"),
         ("CUT-2", "142 silent sessions", "OK"),
         ("CUT-3", "the first dropped session printed in the main pass", "FAIL")], False))
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", T, "--cut", CUT.isoformat()])
    try:
        code = R._guarded_main()
    except SystemExit as ex:
        code = ex.code
    out = capsys.readouterr().out
    assert code == 2
    tail = out.split("REFUSED:")[-1]
    assert "CUT-3" in tail, tail
    assert "CUT-1" not in tail, f"CUT-1 failed but CUT-2 carried the gate, so it is not a cause: {tail}"
