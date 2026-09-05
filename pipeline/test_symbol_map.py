"""symbol_map: the daily path must see the IEX spellings and map them back, with date bounds.

Run as a script (prints RESULT) or under pytest. Every case is one measured defect:
  * BRK.B / BF.B / PRN dropped on the daily path since 2026-03-28 (served series end 2026-03-27);
  * "B" belonged to Barnes Group before 2025 and to Barrick after — an unbounded map would have
    spliced Barnes prints into the GOLD series.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import symbol_map as sm  # noqa: E402

U = {"BRK-B", "BF-B", "PRN-", "GOLD", "FISV", "AAPL", "B"}   # "B" is NOT in the real universe; here to prove the rule
D = dt.date


def test_extractor_universe_adds_iex_spellings():
    e = sm.extractor_universe({"BRK-B", "BF-B", "PRN-", "AAPL"})
    assert {"BRK.B", "BF.B", "PRN", "BRK-B", "BF-B", "PRN-", "AAPL"} <= e
    assert "B" not in e and "FI" not in e          # their targets are not in this universe


def test_class_shares_map_back_on_any_day():
    for day in (D(2022, 3, 7), D(2026, 3, 27), D(2026, 9, 4)):
        assert sm.dataset_ticker("BRK.B", day, U) == "BRK-B"
        assert sm.dataset_ticker("BF.B", day, U) == "BF-B"
        assert sm.dataset_ticker("PRN", day, U) == "PRN-"


def test_rename_is_date_bounded():
    assert sm.dataset_ticker("B", D(2025, 6, 2), U) == "GOLD"     # Barrick-as-B era
    assert sm.dataset_ticker("B", D(2024, 6, 3), U) == "B"        # Barnes era: stays B (only because B is in U here)
    assert sm.dataset_ticker("B", D(2024, 6, 3), U - {"B"}) is None
    assert sm.dataset_ticker("FI", D(2024, 1, 2), U) == "FISV"
    assert sm.dataset_ticker("FI", D(2022, 1, 3), U) is None


def test_reassigned_symbol_is_dropped_in_its_new_owner_era():
    """GOLD: Barrick until 2025-05-08 (prints as GOLD), Barrick as B from 2025-05-09 (mapped B->GOLD),
    Gold.com prints as GOLD from 2025-12-02 (must be dropped, never merged into Barrick's series)."""
    assert sm.dataset_ticker("GOLD", D(2025, 4, 1), U) == "GOLD"       # Barrick still printed GOLD
    assert sm.dataset_ticker("B", D(2025, 12, 2), U) == "GOLD"         # Barrick's prints
    assert sm.dataset_ticker("GOLD", D(2025, 12, 2), U) is None        # Gold.com's prints: dropped
    assert sm.dataset_ticker("GOLD", D(2026, 9, 4), U) is None


def test_plain_tickers_pass_through_and_strangers_drop():
    assert sm.dataset_ticker("AAPL", D(2026, 9, 4), U) == "AAPL"
    assert sm.dataset_ticker("ZZZZ", D(2026, 9, 4), U) is None


def test_negative_control_exact_match_drops_the_dot_symbols():
    """The behaviour this module replaces: exact membership against the dash universe."""
    assert "BRK.B" not in {"BRK-B", "BF-B", "PRN-"}


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"   OK   {name}")
            except AssertionError as e:
                fails += 1
                print(f"   FAIL {name}: {e}")
    print("   RESULT:", "all checks passed" if not fails else f"{fails} FAILED")
    sys.exit(1 if fails else 0)
