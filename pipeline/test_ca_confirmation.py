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
    """`PRN-` 404s and `FISV` no longer exists (Fiserv renamed to FI). yfinance swallows both into
    an empty Series, and the alert then said "no recorded split event matches" about a ticker it
    never looked up. A symbol that resolves has some price history; these do not."""
    _install(monkeypatch, lambda t: _FakeTicker(splits=None, history_rows=0))
    for spelling in ("PRN-", "FISV"):
        assert daily_update._confirm_split_event(spelling, pd.Timestamp("2026-09-04"), 0.5)[0] == "lookup_failed"


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
    assert "MATCHES" in stats["ca_alert"] and "manual_split" in stats["ca_alert"]


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
    def _boom(_t):
        raise AssertionError("the >=3:1 apply path must not consult the recorded-event lookup")
    _install(monkeypatch, _boom)
    existing = pd.concat([_bars("2026-09-02", 100.0), _bars("2026-09-03", 100.0)], ignore_index=True)
    new = _bars("2026-09-04", 10.0)
    stats: dict = {}
    out, applied = daily_update._detect_and_apply_split(existing, new, "APH", stats, dry_run=True)
    assert applied is True
    assert "ca_applied" in stats
    assert float(out["Close"].iloc[0]) == pytest.approx(10.0)


def test_the_alert_names_the_state_it_is_in():
    day = pd.Timestamp("2026-09-04").date()
    assert "FAILED" in daily_update._event_phrase("lookup_failed", None, "AAPL", day)
    assert "not" in daily_update._event_phrase("lookup_failed", None, "AAPL", day)
    assert "SKIPPED" in daily_update._event_phrase("skipped", None, "STI", day)
    assert "No recorded split event" in daily_update._event_phrase("no_match", None, "CHPT", day)
    m = daily_update._event_phrase("match", 0.1, "KLAC", day)
    assert "MATCHES" in m and "manual_split KLAC 0.1 2026-09-04" in m
