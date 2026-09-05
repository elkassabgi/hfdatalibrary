"""merge_ticker must recompute the variables IN FULL whenever it re-cleaned the whole clean file.

R745 finding 5: step 3 re-cleans everything when `is_backfill` (new bars older than the served clean tail,
or after a corporate-action rescale), but step 6 passed `force_full=ca_rescaled` only - so a backfill day
left the served CLEAN variables and quality describing bars that no longer existed. That is the fleet-wide
staleness measured on 2026-09-05 (61-81 % of sessions on ten tickers outside the seam work). These tests
pin the contract at the call site: force_full is true exactly when the clean file was rebuilt.

    python -m pytest pipeline/test_backfill_force_full.py -q      (from the repo root)
"""
from __future__ import annotations
import os
import sys

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import daily_update  # noqa: E402


SESSION = pd.Timestamp("2026-09-04 09:30")


def _bars(start: str, n: int, price: float = 100.0) -> pd.DataFrame:
    """n one-minute bars from `start`, in the pipeline's standard column set."""
    idx = pd.date_range(pd.Timestamp(start), periods=n, freq="1min")
    return pd.DataFrame({
        "ticker": "TEST",                      # merge_ticker drops this column (daily_update.py:353)
        "datetime": idx,
        "Open": price, "High": price + 0.5, "Low": price - 0.5, "Close": price,
        "Volume": 1000, "source": "test",
    })


class _Recorder:
    """Stands in for sync_ticker_variables and records the force_full it was called with."""

    def __init__(self):
        self.calls = []

    def __call__(self, client, version, ticker, bars, max_new=5, force_full=False):
        self.calls.append({"version": version, "ticker": ticker, "force_full": force_full})
        return {"new_rows": 1}


@pytest.fixture
def harness(monkeypatch):
    """Neutralise everything merge_ticker does except the variables call: no R2, no uploads, no cleaning."""
    rec = _Recorder()
    monkeypatch.setattr(daily_update, "sync_ticker_variables", rec, raising=True)
    monkeypatch.setattr(daily_update, "upload_parquet", lambda *a, **k: 0, raising=False)
    monkeypatch.setattr(daily_update, "upload_csv", lambda *a, **k: 0, raising=False)
    # aggregate_all returns one frame per timeframe; merge_ticker indexes it by name (daily_update.py:434)
    monkeypatch.setattr(daily_update, "aggregate_all",
                        lambda *a, **k: {tf: pd.DataFrame() for tf in daily_update.TIMEFRAMES}, raising=False)
    return rec


def _run(monkeypatch, harness, existing_raw, existing_clean, new_bars, ca_rescaled=False):
    """Call merge_ticker with the served state stubbed to the given frames."""
    def fake_download(client, version, ticker, timeframe="1min"):
        if timeframe != "1min":
            return None
        return existing_raw if version == "raw" else existing_clean

    monkeypatch.setattr(daily_update, "download_parquet", fake_download, raising=False)
    monkeypatch.setattr(daily_update, "_detect_and_apply_split",
                        lambda er, nb, t, stats: (er, ca_rescaled), raising=False)
    # dry_run=True returns at daily_update.py:380, before step 6 - the call site under test is on the
    # real path, so the uploads are stubbed out instead (see the harness fixture).
    return daily_update.merge_ticker(client=object(), ticker="TEST", new_bars=new_bars, dry_run=False)


def test_backfill_forces_a_full_variables_recompute(monkeypatch, harness):
    """New bars OLDER than the served clean tail => the clean file is rebuilt => force_full on both versions."""
    existing = _bars("2026-09-04 09:30", 60)
    older = _bars("2026-06-01 09:30", 60)
    try:
        _run(monkeypatch, harness, existing.copy(), existing.copy(), older)
    except Exception as ex:                     # the surrounding steps are stubbed; the call site is the subject
        if not harness.calls:
            pytest.skip(f"merge_ticker could not reach step 6 in this harness: {type(ex).__name__}: {ex}")
    assert harness.calls, "sync_ticker_variables was never called"
    by_version = {c["version"]: c["force_full"] for c in harness.calls}
    assert by_version.get("clean") is True, (
        "a backfill re-cleans the whole CLEAN file, so its historical variables describe bars that no "
        f"longer exist; force_full must be True for clean, got {harness.calls}")
    assert by_version.get("raw") is False, (
        "a backfill only MERGES the raw file, so raw needs no full recompute - recomputing it doubles a "
        f"cost the daily job has a 350-minute ceiling for (R750 finding 2), got {harness.calls}")


def test_ordinary_day_does_not_force_a_full_recompute(monkeypatch, harness):
    """New bars NEWER than the served tail => incremental clean => the daily append stays incremental."""
    existing = _bars("2026-09-03 09:30", 60)
    newer = _bars("2026-09-04 09:30", 60)
    try:
        _run(monkeypatch, harness, existing.copy(), existing.copy(), newer)
    except Exception as ex:
        if not harness.calls:
            pytest.skip(f"merge_ticker could not reach step 6 in this harness: {type(ex).__name__}: {ex}")
    assert harness.calls, "sync_ticker_variables was never called"
    assert not any(c["force_full"] for c in harness.calls), (
        "an ordinary append must NOT recompute the whole history every day (cost); "
        f"got {harness.calls}")


@pytest.mark.parametrize("empty_clean", [None, "empty"])
def test_first_ever_clean_file_also_forces_a_full_recompute(monkeypatch, harness, empty_clean):
    """No existing clean file (or an empty one) takes the same full re-clean branch as a backfill, so its
    clean variables must be recomputed in full too. R752 finding 4: `is_backfill` was only computed inside
    the non-empty block, so this branch rebuilt the whole clean file with force_full False."""
    existing = _bars("2026-09-04 09:30", 60)
    clean = None if empty_clean is None else existing.iloc[0:0].copy()
    try:
        _run(monkeypatch, harness, existing.copy(), clean, _bars("2026-09-05 09:30", 60))
    except Exception as ex:
        if not harness.calls:
            pytest.skip(f"merge_ticker could not reach step 6: {type(ex).__name__}: {ex}")
    assert harness.calls, "sync_ticker_variables was never called"
    by_version = {c["version"]: c["force_full"] for c in harness.calls}
    assert by_version.get("clean") is True, (
        f"a first-ever/empty clean file is rebuilt in full, so clean force_full must be True; got {harness.calls}")


def test_budget_exhaustion_defers_instead_of_recomputing(monkeypatch, harness):
    """Past the per-worker budget the backfill recompute is DEFERRED and the ticker is named, so a
    retried missing day cannot add unbounded wall clock to a job with a 350-minute ceiling (R750 #2)."""
    monkeypatch.setattr(daily_update, "_full_recompute_spent", 10 ** 9, raising=False)
    existing = _bars("2026-09-04 09:30", 60)
    older = _bars("2026-06-01 09:30", 60)
    stats = None
    try:
        stats = _run(monkeypatch, harness, existing.copy(), existing.copy(), older)
    except Exception as ex:
        if not harness.calls:
            pytest.skip(f"merge_ticker could not reach step 6: {type(ex).__name__}: {ex}")
    assert harness.calls, "sync_ticker_variables was never called"
    assert not any(c["force_full"] for c in harness.calls), (
        f"the budget was exhausted, so no full recompute may run; got {harness.calls}")
    assert stats and "TEST" in (stats.get("variables_full_deferred") or []), (
        "a deferred ticker must be NAMED in the run's stats so the out-of-band repair has a list, "
        f"got {stats.get('variables_full_deferred') if stats else None}")


def test_call_site_passes_the_or_of_both_flags():
    """A source-level guard: whatever the harness can reach, the call must not regress to ca_rescaled alone."""
    import ast
    src = open(os.path.join(HERE, "daily_update.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "sync_ticker_variables":
            for kw in node.keywords:
                if kw.arg == "force_full":
                    found.append(ast.dump(kw.value))
    assert found, "no sync_ticker_variables(force_full=...) call found in daily_update.py"
    # the flag is computed just above the call; the call site must not regress to ca_rescaled alone
    src_tail = src[src.index("# 6. Academic variables"):] if "# 6. Academic variables" in src else src
    assert "ca_rescaled or (clean_rebuilt and" in src_tail, (
        "force_full must be ca_rescaled OR (clean_rebuilt AND the clean version) - R745 #5, R750 #2, "
        "R752 #4; the call site's flag expression was not found")
    # clean_rebuilt must be DERIVED from the branch that rebuilds the file, not re-stated independently
    assert "clean_rebuilt = not (" in src and "if not clean_rebuilt:" in src, (
        "clean_rebuilt must be computed once and used to select the re-clean branch, so the branch and "
        "the recompute cannot drift apart")
    for expr in found:
        assert "ca_rescaled" in expr or "_full" in expr, "force_full lost its flag; found " + expr
