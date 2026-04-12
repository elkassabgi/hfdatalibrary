"""
build_bars.py — Aggregate parsed Trade objects into 1-minute OHLCV bars.

Bars are bucketed by Eastern Time (America/New_York) to match standard US
equity market conventions. Trades outside regular trading hours (09:30-15:59 ET)
are dropped.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, time as time_t
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from tops_parser import Trade

NY_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time_t(9, 30)
MARKET_CLOSE = time_t(16, 0)


@dataclass
class _BarBuilder:
    """Mutable accumulator for a single 1-minute bar."""
    minute_start: datetime  # tz-aware UTC datetime at the start of the minute
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0
    started: bool = False

    def add(self, price: float, size: int) -> None:
        if not self.started:
            self.open = price
            self.high = price
            self.low = price
            self.started = True
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += size


@dataclass
class Bar:
    """An emitted 1-minute OHLCV bar."""
    symbol: str
    minute_start: datetime  # tz-aware in America/New_York
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str = "iex"


def _ns_to_ny_minute(ns: int) -> Optional[datetime]:
    """Convert a nanosecond UTC timestamp to a tz-aware NY-time minute boundary.

    Returns None if the trade is outside regular trading hours.
    """
    seconds = ns // 1_000_000_000
    utc_dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    ny_dt = utc_dt.astimezone(NY_TZ)
    t = ny_dt.time()
    if t < MARKET_OPEN or t >= MARKET_CLOSE:
        return None
    # Truncate to minute
    return ny_dt.replace(second=0, microsecond=0)


def build_bars(trades: Iterable[Trade]) -> Dict[str, List[Bar]]:
    """Consume an iterable of trades and return per-symbol lists of 1-minute bars.

    The result is a dict: symbol → sorted list of Bar objects.

    Memory note: this holds all bars for the day in memory. For one trading day
    with 1,391 tickers and ~390 minutes per ticker, that's at most ~543K bars,
    each ~150 bytes → ~80 MB peak memory. Well within limits.
    """
    builders: Dict[tuple[str, datetime], _BarBuilder] = {}

    for trade in trades:
        minute = _ns_to_ny_minute(trade.timestamp_ns)
        if minute is None:
            continue
        key = (trade.symbol, minute)
        b = builders.get(key)
        if b is None:
            b = _BarBuilder(minute_start=minute)
            builders[key] = b
        b.add(trade.price, trade.size)

    by_symbol: Dict[str, List[Bar]] = defaultdict(list)
    for (symbol, minute), b in builders.items():
        by_symbol[symbol].append(
            Bar(
                symbol=symbol,
                minute_start=minute,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
        )

    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda x: x.minute_start)

    return dict(by_symbol)


def bars_to_dataframe(bars: List[Bar]):
    """Convert a list of bars to a pandas DataFrame matching the existing schema."""
    import pandas as pd

    if not bars:
        return pd.DataFrame(columns=["datetime", "Open", "High", "Low", "Close", "Volume", "source"])

    return pd.DataFrame({
        "datetime": [b.minute_start for b in bars],
        "Open":     [b.open for b in bars],
        "High":     [b.high for b in bars],
        "Low":      [b.low for b in bars],
        "Close":    [b.close for b in bars],
        "Volume":   [b.volume for b in bars],
        "source":   [b.source for b in bars],
    })


if __name__ == "__main__":
    # Quick smoke test
    sample = [
        Trade(symbol="AAPL", timestamp_ns=int(datetime(2026, 4, 10, 13, 35, 12, tzinfo=timezone.utc).timestamp() * 1e9), price=187.50, size=100, sale_condition=0, trade_id=1),
        Trade(symbol="AAPL", timestamp_ns=int(datetime(2026, 4, 10, 13, 35, 45, tzinfo=timezone.utc).timestamp() * 1e9), price=187.55, size=200, sale_condition=0, trade_id=2),
        Trade(symbol="AAPL", timestamp_ns=int(datetime(2026, 4, 10, 13, 36, 5,  tzinfo=timezone.utc).timestamp() * 1e9), price=187.40, size=150, sale_condition=0, trade_id=3),
    ]
    bars = build_bars(sample)
    for sym, bar_list in bars.items():
        print(f"\n{sym}: {len(bar_list)} bars")
        for b in bar_list:
            print(f"  {b.minute_start} O={b.open} H={b.high} L={b.low} C={b.close} V={b.volume}")
