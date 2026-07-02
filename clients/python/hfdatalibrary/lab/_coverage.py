"""Honesty guardrails for the hfdatalibrary recipe layer (`lab`).

These make the dataset's documented limitations *loud and never silent*:

* SURVIVORSHIP BIAS - the 1,391-ticker universe is a recent snapshot, so any
  long/cumulative backtest starting before 2022 is optimistically biased
  ("most severely before 2022" - https://hfdatalibrary.com/pages/docs).
  This is a HARD STOP (``SurvivorshipBiasError``) for cumulative exposure
  unless the caller explicitly acknowledges it (rule SURV-1).
* IEX SOURCE BREAK (2022-03-01) - from March 2022 the feed is IEX, roughly
  2-3% of consolidated volume (https://hfdatalibrary.com/pages/issues), so
  volume-based signals are unreliable on the recent segment.
* 1-MINUTE BARS, NOT TICK - never treated as tick/quote data.

Caveats are returned as VALUES (on ``CoverageReport``) *and* emitted as
``CaveatWarning`` so they cannot be silently dropped.  A teaching/CI run can
escalate them to hard errors with::

    import warnings
    warnings.simplefilter("error", CaveatWarning)
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from datetime import date

# Verified constants (pages/issues.html, pages/docs.html, data/metadata.json).
IEX_BREAK = date(2022, 3, 1)              # feed switches to IEX
SURVIVORSHIP_SAFE_FROM = date(2022, 1, 1)  # "2022-onward is close to point-in-time"

DISCLAIMER = "Research/education only. Not investment advice."


class CaveatWarning(UserWarning):
    """A data-limitation caveat. Escalate to errors in teaching/CI with
    ``warnings.simplefilter("error", CaveatWarning)``."""


class SurvivorshipBiasError(RuntimeError):
    """Raised when a long/cumulative backtest would run over the survivor-biased
    pre-2022 window without explicit acknowledgement (rule SURV-1)."""


def _as_date(x):
    """Coerce a Timestamp/datetime/ISO-string/date to ``datetime.date`` or None."""
    if x is None:
        return None
    if isinstance(x, date) and not hasattr(x, "hour"):
        return x
    try:
        import pandas as pd
        return pd.Timestamp(x).date()
    except Exception:
        try:
            return date.fromisoformat(str(x)[:10])
        except Exception:
            return None


@dataclass
class CoverageReport:
    """What the toolkit actually knows about the data behind a result.

    Always attached (``df.attrs['coverage']`` / ``result.coverage``) and returned,
    so the caveats travel with the data instead of living in the docs.
    """
    start: "date | None" = None
    end: "date | None" = None
    n_tickers: int = 0
    missing_tickers: list = field(default_factory=list)
    sources: list = field(default_factory=list)     # observed `source` values
    survivorship: str = "none"                       # none | partial | severe
    iex_segment: bool = False                        # data reaches the IEX era
    uses_volume: bool = False
    point_in_time_safe: bool = False
    acknowledged: bool = False
    warnings: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    @property
    def pre_2022(self) -> bool:
        return self.start is not None and self.start < SURVIVORSHIP_SAFE_FROM

    def __str__(self) -> str:
        head = (f"CoverageReport({self.start}..{self.end}, {self.n_tickers} tickers, "
                f"survivorship={self.survivorship}, iex_segment={self.iex_segment}, "
                f"point_in_time_safe={self.point_in_time_safe})")
        lines = [head]
        for w in self.warnings:
            lines.append(f"  [warn] {w}")
        for n in self.notes:
            lines.append(f"  [note] {n}")
        lines.append(f"  {DISCLAIMER}")
        return "\n".join(lines)


def audit_panel(prices=None, *, start=None, end=None, n_tickers=None,
                missing_tickers=None, sources=None, uses_volume=False,
                version="clean") -> CoverageReport:
    """Build a :class:`CoverageReport` from a wide price panel or explicit args.

    Parameters
    ----------
    prices : pandas.DataFrame, optional
        Wide panel (DatetimeIndex, one column per ticker). If given, ``start``/
        ``end``/``n_tickers`` are inferred from it.
    sources : iterable of str, optional
        The ``source`` values actually observed in the pulled data
        (e.g. ``{"pitrading", "iex"}``). When absent, IEX exposure is inferred
        from the date range alone (``end >= 2022-03-01``).
    uses_volume : bool
        Whether the downstream signal reads the Volume column. The IEX-volume
        caveat fires *only* when True (price-only strategies are not nagged).
    """
    if prices is not None:
        try:
            idx = prices.index
            if start is None:
                start = idx.min()
            if end is None:
                end = idx.max()
            if n_tickers is None:
                n_tickers = prices.shape[1] if getattr(prices, "ndim", 1) > 1 else 1
        except Exception:
            pass

    s, e = _as_date(start), _as_date(end)
    rep = CoverageReport(
        start=s, end=e, n_tickers=int(n_tickers or 0),
        missing_tickers=list(missing_tickers or []),
        sources=sorted({str(x).lower() for x in sources}) if sources else [],
        uses_volume=bool(uses_volume),
    )

    # --- survivorship severity (from the effective start) ---
    if s is not None:
        if s >= SURVIVORSHIP_SAFE_FROM:
            rep.survivorship = "none"
        elif s >= date(2021, 1, 1):
            rep.survivorship = "partial"
        else:
            rep.survivorship = "severe"
        rep.point_in_time_safe = s >= SURVIVORSHIP_SAFE_FROM

    # --- IEX exposure (prefer the real source column, else date-based) ---
    if rep.sources:
        rep.iex_segment = "iex" in rep.sources
    elif e is not None:
        rep.iex_segment = e >= IEX_BREAK

    # --- assemble caveats ---
    if rep.survivorship == "severe":
        rep.warnings.append(
            "SURVIVORSHIP: the 1,391-ticker universe is a recent snapshot; a backtest "
            f"starting {s} is survivorship-biased (most severely before 2022) and will "
            "overstate long-run returns. Pass acknowledge_survivorship=True to proceed.")
    elif rep.survivorship == "partial":
        rep.warnings.append(
            f"SURVIVORSHIP (partial): the window starts {s} (just before the 2022 "
            "point-in-time boundary); the pre-2022 portion is survivor-conditioned.")
    if rep.iex_segment:
        if rep.uses_volume:
            rep.warnings.append(
                "IEX VOLUME: from 2022-03-01 the feed is IEX (~2-3% of consolidated "
                "volume); volume-based signals are unreliable on the post-break segment.")
        else:
            rep.notes.append(
                "IEX SEGMENT: data includes the post-2022-03-01 IEX era (~2-3% of "
                "consolidated volume); price OHLC is usable, raw share-volume is not.")
    if rep.missing_tickers:
        rep.notes.append(
            f"{len(rep.missing_tickers)} ticker(s) had no data and were skipped: "
            f"{', '.join(map(str, rep.missing_tickers[:10]))}"
            f"{' ...' if len(rep.missing_tickers) > 10 else ''}.")
    return rep


def enforce_survivorship(report: CoverageReport, acknowledge: bool = False,
                         cumulative: bool = True) -> CoverageReport:
    """Rule SURV-1 hard stop, then (re-)emit every caveat as ``CaveatWarning``.

    Raises :class:`SurvivorshipBiasError` for a long/cumulative backtest whose
    effective start is before 2022-01-01, unless ``acknowledge=True``. Non-
    cumulative constructions (dollar-neutral, single-day, from-2022) are not
    stopped, only warned. The acknowledgement is recorded in the report so a
    reproduced study shows the bias was knowingly accepted.
    """
    if cumulative and report.pre_2022 and not acknowledge:
        raise SurvivorshipBiasError(
            report.warnings[0] if report.warnings else
            "Survivorship-biased pre-2022 backtest; pass acknowledge_survivorship=True.")
    if acknowledge and report.pre_2022:
        report.acknowledged = True
        report.notes.append("SURV_ACK: survivorship bias knowingly accepted by the caller.")
    for w in report.warnings:
        warnings.warn(w, CaveatWarning, stacklevel=2)
    return report
