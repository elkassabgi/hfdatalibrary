"""
trading_days.py — Compute previous US trading day (skip weekends and NYSE holidays).

Used by the daily IEX HIST pipeline to determine which day's pcap file to fetch.
Uses pandas_market_calendars for accurate NYSE holiday handling.
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Optional


def previous_trading_day(reference: Optional[date] = None) -> date:
    """
    Return the most recent NYSE trading day strictly before `reference`.

    Args:
        reference: The date to look backward from. Defaults to today (UTC).

    Returns:
        A `datetime.date` representing the previous trading day.

    Examples:
        - Tuesday → previous trading day = Monday
        - Monday → previous trading day = Friday
        - Day after Thanksgiving → previous trading day = day before Thanksgiving
    """
    if reference is None:
        reference = datetime.utcnow().date()

    try:
        import pandas_market_calendars as mcal
    except ImportError as e:
        raise RuntimeError(
            "pandas_market_calendars is required. Install: pip install pandas_market_calendars"
        ) from e

    nyse = mcal.get_calendar("NYSE")
    # Look back 14 days to safely cover long holidays
    start = reference - timedelta(days=14)
    end = reference - timedelta(days=1)
    schedule = nyse.schedule(start_date=start.isoformat(), end_date=end.isoformat())

    if schedule.empty:
        raise RuntimeError(f"No trading days found in window {start}..{end}")

    last_session = schedule.index[-1].to_pydatetime().date()
    return last_session


def is_trading_day(d: date) -> bool:
    """Return True if `d` is an NYSE trading day."""
    import pandas_market_calendars as mcal

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=d.isoformat(), end_date=d.isoformat())
    return not schedule.empty


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        ref = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        ref = None

    prev = previous_trading_day(ref)
    print(f"Previous trading day before {ref or 'today'}: {prev}")
    print(f"  YYYYMMDD format: {prev.strftime('%Y%m%d')}")
