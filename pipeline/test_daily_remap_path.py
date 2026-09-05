"""The daily path, end to end on a tiny trades CSV: extractor universe -> parse_trades_csv ->
remap_trades -> build_bars, asserting the bars come out under the DATASET tickers.

Measured defect this pins (2026-09-05): IEX printed 3,087 BRK.B, 5,636 BF.B and 17 PRN trades
on 2026-03-27 and the daily path kept none of them, because the exact-match filter never saw
the dot spellings; the served BRK-B / BF-B / PRN- series end on that day.

Run as a script (prints RESULT) or under pytest. Needs only the pipeline's own modules.
"""
from __future__ import annotations
import datetime as dt
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import symbol_map                              # noqa: E402
from tops_parser import parse_trades_csv       # noqa: E402
from build_bars import build_bars              # noqa: E402
import daily_update                            # noqa: E402

UNIVERSE = {"BRK-B", "BF-B", "PRN-", "GOLD", "AAPL"}
# 2026-09-04 09:30:00 ET in ns (any consistent minute works for grouping)
T0 = 1788_615_000_000_000_000
ROWS = [
    ("BRK.B", T0 + 1_000, "475.17", "4", "1"),
    ("BRK.B", T0 + 2_000, "475.20", "6", "2"),
    ("BF.B",  T0 + 3_000, "37.39", "14", "3"),
    ("PRN",   T0 + 4_000, "50.10", "2", "4"),
    ("B",     T0 + 5_000, "20.00", "100", "5"),   # Barrick-as-B era on 2026-09-04 -> GOLD
    ("AAPL",  T0 + 6_000, "230.00", "10", "6"),
    ("BRK-B", T0 + 7_000, "475.30", "1", "7"),    # a dash print (never happens at IEX; must still pass)
]


def _csv():
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w", newline="") as fh:
        fh.write("symbol,timestamp_ns,price,size,trade_id\n")
        for s, t, p, z, i in ROWS:
            fh.write(f"{s},{t},{p},{z},{i}\n")
    return path


def test_remap_path_groups_under_dataset_tickers():
    path = _csv()
    try:
        ext = symbol_map.extractor_universe(UNIVERSE)
        by_symbol, n, remapped, dropped = daily_update.remap_trades(
            parse_trades_csv(path, universe=ext), dt.date(2026, 9, 4), UNIVERSE)
    finally:
        os.remove(path)
    assert n == 7, n
    assert set(by_symbol) == {"BRK-B", "BF-B", "PRN-", "GOLD", "AAPL"}, set(by_symbol)
    assert len(by_symbol["BRK-B"]) == 3
    assert remapped == {"BRK-B": 2, "BF-B": 1, "PRN-": 1, "GOLD": 1}, remapped
    assert dropped == {}, dropped
    bars = {}
    for symbol, trades in by_symbol.items():
        bars[symbol] = build_bars(trades).get(symbol, [])
    assert all(bars[s] for s in ("BRK-B", "BF-B", "PRN-", "GOLD", "AAPL")), {s: len(b) for s, b in bars.items()}
    assert bars["BRK-B"][0].volume == 11


def test_out_of_bounds_symbol_is_counted_not_kept():
    path = _csv()
    try:
        ext = symbol_map.extractor_universe(UNIVERSE)
        # 2024-06-03: "B" is Barnes Group, not Barrick -> dropped and counted
        by_symbol, n, remapped, dropped = daily_update.remap_trades(
            parse_trades_csv(path, universe=ext), dt.date(2024, 6, 3), UNIVERSE)
    finally:
        os.remove(path)
    assert "GOLD" not in by_symbol
    assert dropped == {"B": 1}, dropped
    assert n == 6


def test_negative_control_old_exact_filter_loses_the_class_shares():
    path = _csv()
    try:
        kept = [t.symbol for t in parse_trades_csv(path, universe=UNIVERSE)]   # the OLD call shape
    finally:
        os.remove(path)
    assert kept == ["AAPL", "BRK-B"], kept          # BRK.B / BF.B / PRN / B all gone


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
