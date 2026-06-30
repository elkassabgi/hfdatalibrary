"""Named ticker universes from the bundled snapshot of ``ticker_meta.json``.

HONESTY NOTE: index helpers return the constituents PRESENT IN THIS SURVIVOR-
SNAPSHOT universe, which is *not* the nominal index size. Verified counts in the
bundled snapshot: S&P 500 -> 550, Nasdaq 100 -> 81 (of 100), Dow 30 -> 30.
``universe()`` emits a ``CaveatWarning`` when fewer than the nominal count are
present (a survivorship gap: delisted/replaced names are simply absent).
"""
from __future__ import annotations

import json
import os
import warnings

from ._coverage import CaveatWarning

_DATA = os.path.join(os.path.dirname(__file__), "_data", "ticker_meta.json")

# friendly name -> (label as stored in ticker_meta "indices", nominal index size)
_INDEX = {
    "sp500": ("S&P 500", 500),
    "nasdaq100": ("Nasdaq 100", 100),
    "dow30": ("Dow 30", 30),
}
_TYPE = {"etf": "ETF", "stock": "Stock"}

_meta = None


def _load() -> dict:
    global _meta
    if _meta is None:
        with open(_DATA, encoding="utf-8") as fh:
            _meta = json.load(fh)
    return _meta


def universe(name: str) -> list:
    """Return the sorted tickers for a named universe.

    ``name`` is one of:
      * ``"sp500"``, ``"nasdaq100"``, ``"dow30"`` - index membership
        (emits a CaveatWarning when fewer than the nominal index size are present)
      * ``"etf"``, ``"stock"`` - by instrument type
      * ``"all"`` - every ticker in the snapshot
      * ``"sector:<Sector>"`` - e.g. ``"sector:Healthcare"``
    """
    meta = _load()
    key = name.strip().lower()

    if key in _INDEX:
        label, nominal = _INDEX[key]
        out = sorted(t for t, m in meta.items() if label in (m.get("indices") or []))
        got = len(out)
        if nominal and got < nominal:
            warnings.warn(
                f"universe('{key}'): {got} of {nominal} constituents present; "
                f"{nominal - got} absent from this survivor-snapshot universe "
                "(delisted/replaced members are missing - a survivorship gap).",
                CaveatWarning, stacklevel=2)
        return out

    if key in _TYPE:
        return sorted(t for t, m in meta.items() if m.get("type") == _TYPE[key])

    if key == "all":
        return sorted(meta.keys())

    if key.startswith("sector:"):
        sector = name.split(":", 1)[1].strip()
        out = sorted(t for t, m in meta.items()
                     if (m.get("sector") or "").lower() == sector.lower())
        if not out:
            raise ValueError(f"no tickers found for sector {sector!r}")
        return out

    raise ValueError(
        f"unknown universe {name!r}; expected one of: sp500, nasdaq100, dow30, "
        "etf, stock, all, or 'sector:<Sector>'.")


def index_coverage() -> dict:
    """Diagnostic: {friendly_name: {'present': n, 'nominal': n}} for the indices,
    so callers can SEE the survivorship gap rather than assume nominal sizes."""
    meta = _load()
    out = {}
    for key, (label, nominal) in _INDEX.items():
        present = sum(1 for m in meta.values() if label in (m.get("indices") or []))
        out[key] = {"present": present, "nominal": nominal}
    return out
