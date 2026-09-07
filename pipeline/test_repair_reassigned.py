"""The reassigned-symbol repair tool had NO tests, and that is how R865 #1 shipped.

Nothing in `pipeline/` imported `repair_reassigned` (0 of 8 test files), so a defect on the
`--apply` path could not be caught by anything but a real run over served objects — and a dry run
returns two hundred lines before reaching it. The specific defect: `_served_read` stamped
`df["datetime"]` on every object it read, and the four variables/quality objects are keyed on
`trade_date`. The `KeyError` was neither `Unverifiable` nor `RuntimeError`, so VERIFY (h)'s handler
missed it and the guarded try's `except BaseException` caught it instead — RESTORE, exit 1. Every
`--apply` run would have rolled back its own 22 correct objects.

These are the parts that can be tested without R2. The apply path's exit-code routing lives in
`test_repair_apply_path.py`, in this directory and in CI - it used to say "the reviewer's harness,
not from here", which pointed at a scratchpad file that was never checked in and was exactly what
R867 #2 failed this tool for.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import repair_reassigned as rr   # noqa: E402
import symbol_map                # noqa: E402


# --------------------------------------------------------------- _served_read

def _install_reader(monkeypatch, frame):
    monkeypatch.setattr(rr, "download_parquet", lambda *a, **kw: frame)


def test_a_variables_object_is_keyed_on_trade_date_and_must_not_raise(monkeypatch):
    """The R865 #1 defect, exactly: raw/variables/<T>.parquet has trade_date, not datetime."""
    v = pd.DataFrame({"trade_date": pd.to_datetime(["2026-03-26", "2026-03-27"]),
                      "ticker": ["GOLD", "GOLD"], "gap_rate": [0.0, 0.0]})
    _install_reader(monkeypatch, v)
    out = rr._served_read(object(), "raw", "GOLD", "variables")
    assert list(out["trade_date"]) == list(pd.to_datetime(["2026-03-26", "2026-03-27"]))


def test_a_bars_object_is_still_keyed_on_datetime(monkeypatch):
    b = pd.DataFrame({"datetime": ["2026-03-27 09:30:00"], "Open": [1.0], "High": [1.0],
                      "Low": [1.0], "Close": [1.0], "Volume": [1], "source": ["iex"]})
    _install_reader(monkeypatch, b)
    out = rr._served_read(object(), "raw", "GOLD")
    assert str(out["datetime"].dtype).startswith("datetime64")


def test_an_object_with_neither_key_is_a_runtime_error_not_a_key_error(monkeypatch):
    """RuntimeError is what VERIFY (h) catches and what the caller turns into a restore decision.
    A KeyError escapes to `except BaseException` and restores without ever printing why."""
    _install_reader(monkeypatch, pd.DataFrame({"nonsense": [1]}))
    with pytest.raises(RuntimeError):
        rr._served_read(object(), "raw", "GOLD", "quality")


def test_an_empty_object_after_an_upload_is_a_runtime_error(monkeypatch):
    _install_reader(monkeypatch, pd.DataFrame())
    with pytest.raises(RuntimeError):
        rr._served_read(object(), "raw", "GOLD")


# --------------------------------------------------------------- the cut gate

def test_the_seven_declared_cuts_are_exactly_symbol_maps_handover_dates():
    """CUT-0 rests on this equality, so it is asserted rather than assumed."""
    declared = {"GOLD": "2025-12-02", "STI": "2024-02-05", "SKK": "2024-10-08", "IPW": "2021-05-12",
                "VRM": "2025-02-20", "USLV": "2026-05-27", "PARA": "2026-08-07"}
    for t, d in declared.items():
        assert symbol_map.REASSIGNED[t][0] == d, t


def test_a_ticker_that_was_never_reassigned_is_refused_outright():
    rows, ok = rr.cut_gate("AAPL", dt.date(2024, 2, 5), dt.date(2024, 2, 2), dt.date(2024, 2, 5), None, 20)
    assert ok is False
    assert rows[0][0] == "CUT-0" and "not in symbol_map.REASSIGNED" in rows[0][1]


def test_a_cut_that_is_not_the_recorded_handover_is_refused_before_any_measurement(monkeypatch):
    """The R864 reproducer, now stopped by one equality before a single file is read."""
    def _boom(_day):
        raise AssertionError("CUT-0 must refuse before any print-stream measurement")
    monkeypatch.setattr(rr, "_main_pass_symbols", _boom)
    rows, ok = rr.cut_gate("GOLD", dt.date(2025, 12, 15), dt.date(2025, 12, 12), dt.date(2025, 12, 15), "B", 20)
    assert ok is False
    assert [r for r in rows if r[0] == "CUT-0"][0][2] == "FAIL"


def test_the_gap_floor_cannot_be_overridden_past_the_recorded_date():
    """`--cut-gap-min 0` used to turn a FAIL into a PASS. It still relaxes CUT-1 and CUT-2 — but
    CUT-0 is not a threshold, so a wrong date is refused at any floor."""
    rows, ok = rr.cut_gate("STI", dt.date(2024, 3, 5), dt.date(2024, 3, 4), dt.date(2024, 3, 5), None, 0)
    assert ok is False


def test_a_cut_that_drops_nothing_is_refused():
    rows, ok = rr.cut_gate("STI", dt.date(2024, 2, 5), dt.date(2019, 12, 6), None, None, 20)
    assert ok is False
    assert "nothing to repair" in rows[0][1]


def test_weekdays_between_counts_strictly_between():
    assert rr._weekdays_between(dt.date(2025, 12, 1), dt.date(2025, 12, 2)) == 0
    assert rr._weekdays_between(dt.date(2025, 12, 1), dt.date(2025, 12, 3)) == 1
    assert rr._weekdays_between(dt.date(2025, 12, 5), dt.date(2025, 12, 8)) == 0    # a weekend


# --------------------------------------------------------------- the --until gate

def test_last_cs_session_walks_back_from_the_window_end(monkeypatch):
    """A correct --until costs one file; a short one finds the true last session immediately."""
    monkeypatch.setattr(rr.os.path, "exists", lambda p: True)
    monkeypatch.setattr(rr, "_bars_from_cs", lambda d, sym, tkr: [{"x": 1}] if d == rr.WINDOW[1] else [])
    assert rr.last_cs_session("B", "GOLD") == rr.WINDOW[1]


def test_last_cs_session_is_bounded_and_returns_none_for_a_symbol_that_never_prints(monkeypatch):
    seen = []
    monkeypatch.setattr(rr.os.path, "exists", lambda p: True)

    def _none(d, sym, tkr):
        seen.append(d)
        return []
    monkeypatch.setattr(rr, "_bars_from_cs", _none)
    assert rr.last_cs_session("NOTASYMBOL", "GOLD", limit=5) is None
    assert len(seen) == 5, "the walk must stop at `limit`, not scan the whole window"
