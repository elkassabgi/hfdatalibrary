"""symbol_map.py — the ONE place where an IEX print symbol becomes a dataset ticker.

WHY THIS FILE EXISTS. The dataset spells class shares with a dash (BRK-B, BF-B) and keeps a
company's series under its ORIGINAL ticker through a rename (GOLD follows Barrick, FISV follows
Fiserv). IEX prints BRK.B, BF.B, and the NEW symbol after a rename. `pipeline/pcap_extract` and
`tops_parser.parse_trades_csv` filter by EXACT symbol against data/tickers.json, so every one
of those prints was dropped on the daily path. The 2022-03-07..2026-03-27 window only has them
because the backfill re-scanned all 1,019 window pcaps with a second, remapped universe
(hist_backfill_classshares.py, whose REMAP this file now owns). The daily path never got that
pass: measured 2026-09-05, the served BRK-B, BF-B and PRN- series end on 2026-03-27, the last
window day, while IEX printed 3,087 / 5,636 / 17 trades for them on that very day.

THE OTHER HALF OF THE SAME DEFECT (R727). An exact-symbol filter also keeps a symbol's prints
after the symbol passes to ANOTHER company. The served GOLD series is Barrick's through
2025-12-01 (the backfill's B->GOLD recovery worked) and Gold.com, Inc.'s from 2025-12-02, the
day Gold.com took the NYSE symbol (raw prints: 2025-12-01 B 12,075 last 42.34 = the served bar
exactly; 2025-12-02 GOLD 321 @ 28.91-30.09 = the served bar exactly): 191 contaminated sessions
to 2026-09-04, in raw and clean. The backfill's own docstring said the GOLD series "continues at
Barrick prices straight through the GOLD->B rename" - measured only at the 2025-05 boundary,
where it was true. REASSIGNED below drops such prints; the served objects need a separate repair.
The same census found the pattern on STI (Solidion Technology's prints under SunTrust's symbol
since 2022), IPW, SKK, VRM (the post-bankruptcy equity), USLV and PARA - see the handoff.

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
    "FI":    ("FISV",  "2023-06-01", "2025-11-10"),   # Fiserv traded as FI on the NYSE 2023-06-07..2025-11-10,
                                                      # then moved back to Nasdaq as FISV (raw prints: FI 3,895 on
                                                      # 11-10 -> 0; FISV 0 -> 1,950 on 11-11). Closed so the next
                                                      # issuer to take "FI" is not fed to Fiserv's series.
}


# IEX symbols that were REASSIGNED to a different company while the dataset ticker keeps following
# the original one. Prints of the symbol inside the window are another security and are dropped
# (counted, never merged). Measured 2026-09-05: NYSE "GOLD" passed from Barrick (now B) to Gold.com,
# Inc. on 2025-12-02 - our served GOLD daily closes equal Barrick through 2025-12-01 and Gold.com
# from 2025-12-02 (42.34 -> 29.82 overnight while B printed 41.03), 190 contaminated sessions.
REASSIGNED: Dict[str, Tuple[Optional[str], Optional[str]]] = {
    # symbol: (first session the NEW owner printed under it, None = still theirs). Dates measured
    # 2026-09-05 as the first served session whose close follows the new owner's Yahoo series
    # (>= 60 % of the next 20 sessions), see the handoff §11e.
    "GOLD": ("2025-12-02", None),   # Barrick -> Gold.com, Inc. (Barrick prints as B; REMAP above)
    "STI":  ("2024-02-05", None),   # SunTrust (last bar 2019-12-06, merged into Truist) -> Solidion Technology, Inc.
                                    # (our first foreign bar 2024-02-05 @ 145.00)
    "IPW":  ("2021-05-12", None),   # "SPDR S&P International Energy Sector ETF" (SPDR Index Shares Funds, 485BPOS
                                    # 2008-07-16, acc. 0000950135-08-004982; series 2008-2017) -> iPower Inc.
    "SKK":  ("2024-10-08", None),   # "ProShares UltraShort Russell2000 Growth (SKK)" (ProShares Trust, 497 2014-12-23,
                                    # acc. 0001193125-14-452796; series 2007-2015) -> SKK Holdings Limited
    "VRM":  ("2025-02-20", None),   # Vroom's cancelled equity -> the post-Chapter-11 Vroom, Inc. (a new security)
    "USLV": ("2026-05-27", None),   # VelocityShares 3x Silver ETN (delisted 2020) -> Direxion Daily Silver Bull 2X ETF
    "PARA": ("2026-08-07", None),   # Paramount Global (last bar 2025-08-06, -> PSKY) -> Banzai International, Inc.
                                    # (our first foreign bar 2026-08-07 @ 1.84)
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
    r = REASSIGNED.get(symbol)
    if r is not None and _in_bounds(day, r[0], r[1]):
        return None            # the symbol now belongs to another company; the series does not
    return symbol if symbol in universe else None
