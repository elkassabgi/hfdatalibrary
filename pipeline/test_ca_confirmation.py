"""The recorded-event lookup must never authorise a rescale, and its three failure states must stay
distinguishable.

WHY. An earlier version of this branch let a matching recorded split event permit the whole-history
rescale that `main` refuses, in both the inconsistent-ratio branch and the sub-3:1 branch. The
lookup calls `yf.Ticker(ticker).splits`, and that is keyed on the SYMBOL: Yahoo attributes a split
to whoever holds the symbol NOW. R732 is the ledger entry recording that exact lookup applying
iPower's 1:8 and 1:9 and SKK Holdings' 1:10 to two ETF histories that died in 2017 and 2015. Putting
it on the 06:00Z cron, unattended, with no snapshot and no rollback, is R732 with a scheduler.

So the contract these tests hold:
  * a recorded event CHANGES THE ALERT TEXT AND NOTHING ELSE - never `applied`;
  * a reassigned symbol is never looked up at all;
  * an empty answer from yfinance is NOT "no split": a 404 is swallowed and returns an empty
    Series, so an unresolved spelling must read as `lookup_failed`, never as `no_match`.

Every test is offline: `yfinance` is replaced in `sys.modules` through monkeypatch, which restores
it afterwards. R860: popping a module instead of restoring it silently breaks later monkeypatches
in the same session.
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import daily_update  # noqa: E402
import symbol_map    # noqa: E402


class _FakeTicker:
    def __init__(self, splits=None, history_rows=0, raise_on_splits=None):
        self._splits = splits
        self._history_rows = history_rows
        self._raise = raise_on_splits

    @property
    def splits(self):
        if self._raise is not None:
            raise self._raise
        if self._splits is None:
            return pd.Series(dtype=float)          # what yfinance returns for a 404
        idx = pd.to_datetime([d for d, _ in self._splits])
        return pd.Series([v for _, v in self._splits], index=idx, dtype=float)

    def history(self, period=None):
        if not self._history_rows:
            return pd.DataFrame()
        idx = pd.date_range("2026-09-01", periods=self._history_rows, freq="D")
        return pd.DataFrame({"Close": [1.0] * self._history_rows}, index=idx)


def _install(monkeypatch, factory):
    class _Mod:
        Ticker = staticmethod(factory)
    monkeypatch.setitem(sys.modules, "yfinance", _Mod)


def _bars(day: str, close: float, ticker: str = "APH", n: int = 60) -> pd.DataFrame:
    idx = pd.date_range(f"{day} 09:30", periods=n, freq="min")
    return pd.DataFrame({"datetime": idx, "Open": close, "High": close, "Low": close,
                         "Close": close, "Volume": 600, "source": "iex", "ticker": ticker})


# ----------------------------------------------------------------- the lookup itself

def test_a_real_recorded_split_matches(monkeypatch):
    _install(monkeypatch, lambda t: _FakeTicker(splits=[("2026-09-04", 2.0)], history_rows=5))
    status, ratio = daily_update._confirm_split_event("APH", pd.Timestamp("2026-09-04"), 0.5, 0.5)
    assert status == "match"
    assert ratio == pytest.approx(0.5)


def test_a_symbol_with_history_and_no_split_is_no_match(monkeypatch):
    _install(monkeypatch, lambda t: _FakeTicker(splits=None, history_rows=5))
    assert daily_update._confirm_split_event("CHPT", pd.Timestamp("2026-09-04"), 0.5)[0] == "no_match"


def test_an_unresolved_spelling_is_lookup_failed_not_no_match(monkeypatch):
    """`PRN-` 404s at Yahoo: yfinance swallows that into an empty Series, and the alert then said
    "no recorded split event matches" about a ticker it never looked up. A symbol that resolves has
    some price history; this one does not.

    FISV USED TO BE THE SECOND EXAMPLE HERE AND IT WAS FALSE (review R873 #4). The claim was that
    Fiserv's rename to FI left `FISV` dead - but `pipeline/symbol_map.py` in this same tree records
    the opposite, that Fiserv "moved back to Nasdaq as FISV" on 2025-11-11, and a live probe on
    2026-09-07 resolved it in 0.29 s and returned no_match. Forcing `history_rows=0` for it made
    the fixture assert a state the world does not have - R854's class, a test that passes because
    the fixture was written to agree with the docstring."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=None, history_rows=0))
    assert daily_update._confirm_split_event("PRN-", pd.Timestamp("2026-09-04"), 0.5)[0] == "lookup_failed"


def test_a_symbol_that_resolves_is_never_reported_as_a_failed_lookup(monkeypatch):
    """The mirror of the case above, and the one FISV actually belongs in: a spelling that DOES
    resolve and has no recorded split is a genuine no_match, not a lookup failure. Reporting it as
    failed would make the tri-state useless in the other direction."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=None, history_rows=5))
    assert daily_update._confirm_split_event("FISV", pd.Timestamp("2026-09-04"), 0.5)[0] == "no_match"


def test_a_raising_lookup_is_lookup_failed(monkeypatch):
    _install(monkeypatch, lambda t: _FakeTicker(raise_on_splits=RuntimeError("429 Too Many Requests")))
    assert daily_update._confirm_split_event("AAPL", pd.Timestamp("2026-09-04"), 0.5)[0] == "lookup_failed"


def test_every_reassigned_symbol_is_skipped_without_any_lookup(monkeypatch):
    """The seven whose IEX symbol passed to a different issuer. A lookup keyed on them answers for
    the new company: STI still offers Solidion's 1:50 against our SunTrust series."""
    def _boom(_t):
        raise AssertionError("a REASSIGNED symbol must never reach the network")
    _install(monkeypatch, _boom)
    assert set(symbol_map.REASSIGNED) >= {"GOLD", "STI", "IPW", "SKK", "VRM", "USLV", "PARA"}
    for t in symbol_map.REASSIGNED:
        assert daily_update._confirm_split_event(t, pd.Timestamp("2026-09-04"), 0.125)[0] == "skipped"


def test_an_adjustment_factor_is_not_a_split(monkeypatch):
    """Yahoo records spin-off factors as "splits" (DD 2.39, RTX 1.589, EBAY 2.376)."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=[("2026-09-04", 2.39)], history_rows=5))
    assert daily_update._confirm_split_event("DD", pd.Timestamp("2026-09-04"), 1.0 / 2.39)[0] == "no_match"


# ----------------------------------------------------------------- what it may NOT do

def test_a_matching_event_below_three_to_one_still_does_not_apply(monkeypatch):
    """The 3:1 floor stands. A recorded 2:1 on the day makes the alert say so; it does not rescale."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=[("2026-09-04", 2.0)], history_rows=5))
    existing = pd.concat([_bars("2026-09-02", 100.0), _bars("2026-09-03", 100.0)], ignore_index=True)
    new = _bars("2026-09-04", 50.0)
    stats: dict = {}
    out, applied = daily_update._detect_and_apply_split(existing, new, "APH", stats, dry_run=True)
    assert applied is False
    assert out.equals(existing)
    assert "ca_applied" not in stats
    # ONE command in the alert, not two (R873 #2): the recorded event is reported, and the only
    # runnable rescale is the one main built from the price snap.
    assert "CONSISTENT with the move" in stats["ca_alert"], stats["ca_alert"]
    assert stats["ca_alert"].count("manual_split") == 1, stats["ca_alert"]


def test_a_matching_event_on_an_inconsistent_move_still_does_not_apply(monkeypatch):
    """Price evidence that is split-sized but not round: main alerts, and so does this."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=[("2026-09-04", 10.0)], history_rows=5))
    existing = pd.concat([_bars("2026-09-02", 100.0), _bars("2026-09-03", 100.0)], ignore_index=True)
    new = _bars("2026-09-04", 100.0 / 10.6)          # 10.6 snaps to nothing at 3 %
    stats: dict = {}
    out, applied = daily_update._detect_and_apply_split(existing, new, "APH", stats, dry_run=True)
    assert applied is False
    assert out.equals(existing)
    assert "ca_applied" not in stats
    assert "NOT applied" in stats["ca_alert"]


def test_a_clean_ten_to_one_still_applies_on_price_alone(monkeypatch):
    """The negative control. Removing the automation this PR added must not disable the automation
    main already has: a round ratio at or above 3:1, stable all day, is still applied - and the
    lookup is not consulted for it, so a dead network cannot stop it."""
    # COUNT THE CALLS, DO NOT RAISE (review R873 #1). The first version of this test raised
    # `AssertionError` from the fake Ticker - and `_confirm_split_event` wraps its lookup in
    # `except Exception`, of which AssertionError is one. The control was INERT: a mutant that
    # inserted `_confirm_split_event(...)` straight onto the >=3:1 apply path left this file at
    # 10 passed with the lookup measurably called. The one hunk the commit message asserts was the
    # one hunk nothing held.
    calls = []
    _install(monkeypatch, lambda t: calls.append(t) or _FakeTicker(splits=None, history_rows=5))
    existing = pd.concat([_bars("2026-09-02", 100.0), _bars("2026-09-03", 100.0)], ignore_index=True)
    new = _bars("2026-09-04", 10.0)
    stats: dict = {}
    out, applied = daily_update._detect_and_apply_split(existing, new, "APH", stats, dry_run=True)
    assert applied is True
    assert "ca_applied" in stats
    assert float(out["Close"].iloc[0]) == pytest.approx(10.0)
    assert calls == [], f"the >=3:1 apply path consulted the recorded-event lookup: {calls}"


def test_the_alert_names_the_state_it_is_in():
    assert "FAILED" in daily_update._event_phrase("lookup_failed", None, 0.5)
    assert "not" in daily_update._event_phrase("lookup_failed", None, 0.5)
    assert "SKIPPED" in daily_update._event_phrase("skipped", None, 0.5)
    assert "No recorded split event" in daily_update._event_phrase("no_match", None, 0.5)


def test_the_match_phrase_offers_no_second_command_and_claims_no_uniqueness():
    """R873 #2 and #3. It used to append a runnable `manual_split` built from the RECORDED ratio
    beside the alert's own, built from the price snap - reproduced at observed x2.000 against a
    recorded x2.5, both copy-pasteable, one wrong by 25 %, and the wrong one carrying the word
    MATCHES. And "MATCHES" implied the recorded ratio was pinned when the +-20 % band admits up to
    three distinct round ratios for one observation."""
    m = daily_update._event_phrase("match", 2.5, 2.0)
    assert "manual_split" not in m, m
    assert "MATCHES" not in m, m
    assert "2.5" in m and "2" in m, m
    assert "more than one round ratio" in m, m


# ----------------------------------------------------------------- the durable queue

def _queue(monkeypatch, tmp_path):
    p = tmp_path / "ca_alerts.jsonl"
    monkeypatch.setattr(daily_update, "CA_ALERTS_PATH", str(p))
    return p


def _lines(p):
    import json
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def test_the_durable_queue_appends_a_parseable_line(monkeypatch, tmp_path):
    """R873 #6: all five statements of `_record_ca_event` and all four of its call sites never
    executed under the suite, because every test passed `dry_run=True` - the flag that suppresses
    them. 29 statements of the new code were unreachable by any test."""
    p = _queue(monkeypatch, tmp_path)
    daily_update._record_ca_event("ALERT", "APH", pd.Timestamp("2026-09-04"), "a message")
    daily_update._record_ca_event("APPLIED", "KLAC", pd.Timestamp("2026-09-05"), "another")
    rows = _lines(p)
    assert [r["kind"] for r in rows] == ["ALERT", "APPLIED"]
    assert rows[0]["ticker"] == "APH" and rows[0]["day"] == "2026-09-04"
    assert rows[0]["msg"] == "a message"
    assert rows[0]["logged"].endswith("Z")


def test_a_broken_queue_never_breaks_the_append(monkeypatch, tmp_path):
    """The ledger is a convenience; the day's data is not. A write failure must be printed and
    swallowed, never raised into the merge."""
    monkeypatch.setattr(daily_update, "CA_ALERTS_PATH", str(tmp_path / "no_such_dir" / "x.jsonl"))
    daily_update._record_ca_event("ALERT", "APH", pd.Timestamp("2026-09-04"), "a message")


def test_a_long_gap_alert_reaches_the_queue(monkeypatch, tmp_path):
    """R873 #6, the second half: the `gap_days > MAX_OVERNIGHT_GAP_DAYS` branch alerted and
    returned WITHOUT recording, while the function's own docstring claimed it persisted every
    alert - and that branch is the one that fires on the REASSIGNED population."""
    p = _queue(monkeypatch, tmp_path)
    existing = _bars("2026-06-01", 100.0)
    new = _bars("2026-09-04", 10.0)
    stats: dict = {}
    _out, applied = daily_update._detect_and_apply_split(existing, new, "STI", stats, dry_run=False)
    assert applied is False
    assert "days later" in stats["ca_alert"]
    rows = _lines(p)
    assert len(rows) == 1 and rows[0]["ticker"] == "STI", rows


def test_dry_run_writes_nothing_to_the_queue(monkeypatch, tmp_path):
    """The mirror, and the reason the coverage hole existed: dry_run suppresses every write."""
    p = _queue(monkeypatch, tmp_path)
    existing = _bars("2026-06-01", 100.0)
    new = _bars("2026-09-04", 10.0)
    daily_update._detect_and_apply_split(existing, new, "STI", {}, dry_run=True)
    assert _lines(p) == []
