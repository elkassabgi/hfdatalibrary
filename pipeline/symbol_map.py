"""symbol_map.py — the ONE place where an IEX print symbol becomes a dataset ticker.

WHY THIS FILE EXISTS. The dataset spells class shares with a dash (BRK-B, BF-B) and keeps a
company's series under its ORIGINAL ticker through a rename (the served GOLD series continues
at Barrick prices straight through the GOLD->B rename; FISV through FISV->FI). IEX prints
BRK.B, BF.B, and the NEW symbol after a rename. `pipeline/pcap_extract` and
`tops_parser.parse_trades_csv` filter by EXACT symbol against data/tickers.json, so every one
of those prints was dropped on the daily path. The 2022-03-07..2026-03-27 window only has them
because the backfill re-scanned all 1,019 window pcaps with a second, remapped universe
(hist_backfill_classshares.py, whose REMAP this file now owns). The daily path never got that
pass: measured 2026-09-05, the served BRK-B, BF-B and PRN- series end on 2026-03-27, the last
window day, while IEX printed 3,087 / 5,636 / 17 trades for them on that very day.

USE. Two calls, both pure:
  extractor_universe(universe) -> the symbol set to hand the Go extractor (dataset tickers PLUS
                                  every IEX spelling that maps into the universe);
  dataset_ticker(symbol, day)  -> the dataset ticker an IEX print belongs to on that day, or
                                  None when the symbol is outside the universe or the mapping's
                                  date bounds (an unrelated instrument that used the symbol
                                  earlier/later: "B" was Barnes Group until January 2025).

THE MAP. IEX symbol -> (dataset ticker, valid_from, valid_to), ISO dates or None (open-ended).
Bounds are deliberately generous on the INSIDE edge (the merge dedups on datetime, so an
overlapping day costs nothing) and exact on the OUTSIDE edge (that is what keeps a different
company's prints out). Adding a rename here is a corporate-action decision: verify the
effective date from the exchange notice before adding it, and note that a rename WITH a split
(the Invesco RSP* ETFs, 2023-07-17) also needs the split applied to the old history — a map
entry alone would splice two price bases. Those are NOT in this map on purpose.
"""
from __future__ import annotations
import datetime as dt
from typing import Dict, Iterable, Optional, Set, Tuple

# IEX symbol -> (dataset ticker, valid_from, valid_to)
REMAP: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {
    "BRK.B": ("BRK-B", None, None),
    "BF.B":  ("BF-B",  None, None),
    "PRN":   ("PRN-",  None, None),           # Invesco DWA Industrials ETF — the dataset ticker
                                              # carries a trailing dash; IEX prints plain PRN
    "B":     ("GOLD",  "2025-05-01", None),   # Barrick traded as GOLD until 2025-05-08, as B after
    "FI":    ("FISV",  "2023-06-01", None),   # Fiserv traded as FISV until mid-2023, as FI after
}


def _in_bounds(day: dt.date, lo: Optional[str], hi: Optional[str]) -> bool:
    if lo is not None and day < dt.date.fromisoformat(lo):
        return False
    if hi is not None and day > dt.date.fromisoformat(hi):
        return False
    return True


def extractor_universe(universe: Iterable[str]) -> Set[str]:
    """Dataset tickers plus every IEX spelling that maps into them (bounds ignored here: the
    extractor sees one day at a time and the remap applies the bounds)."""
    u = set(universe)
    return u | {iex for iex, (ticker, _lo, _hi) in REMAP.items() if ticker in u}


def dataset_ticker(symbol: str, day: dt.date, universe: Set[str]) -> Optional[str]:
    """The dataset ticker an IEX print of `symbol` on `day` belongs to, or None.

    A symbol that is itself a dataset ticker maps to itself — unless the map says that on this
    day the symbol belongs to ANOTHER dataset series (the "B" -> GOLD era), in which case the
    company the series follows wins, exactly as the backfill decided."""
    m = REMAP.get(symbol)
    if m is not None:
        ticker, lo, hi = m
        if ticker in universe and _in_bounds(day, lo, hi):
            return ticker
    return symbol if symbol in universe else None
