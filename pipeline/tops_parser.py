"""
tops_parser.py — Read trade reports from a CSV produced by the Go pcap_extract tool.

The Go binary reads the raw IEX HIST pcap.gz file (8+ GB) at native speed,
filters to our ticker universe, and outputs a small CSV (~50 MB) of trades.
This module reads that CSV and yields Trade objects for bar construction.

CSV format (from pcap_extract):
  symbol,timestamp_ns,price,size,trade_id
"""
from __future__ import annotations
import csv
from dataclasses import dataclass
from typing import Generator, Optional, Set


@dataclass(frozen=True)
class Trade:
    """A single executed trade from the IEX TOPS feed."""
    symbol: str
    timestamp_ns: int
    price: float
    size: int
    sale_condition: int
    trade_id: int


def parse_trades_csv(
    csv_path: str,
    universe: Optional[Set[str]] = None,
) -> Generator[Trade, None, None]:
    """Read trades from a CSV file produced by pcap_extract.

    Args:
        csv_path: path to the trades.csv file
        universe: optional additional filter (usually already applied by Go)

    Yields:
        Trade instances.
    """
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row["symbol"]
            if universe is not None and symbol not in universe:
                continue
            yield Trade(
                symbol=symbol,
                timestamp_ns=int(row["timestamp_ns"]),
                price=float(row["price"]),
                size=int(row["size"]),
                sale_condition=0,
                trade_id=int(row["trade_id"]),
            )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python tops_parser.py trades.csv [TICKER1 TICKER2 ...]")
        sys.exit(1)

    path = sys.argv[1]
    universe = set(sys.argv[2:]) or None

    count = 0
    by_symbol = {}
    for trade in parse_trades_csv(path, universe):
        count += 1
        by_symbol[trade.symbol] = by_symbol.get(trade.symbol, 0) + 1
        if count <= 5:
            print(f"  {trade.symbol} @ {trade.price} x {trade.size}")

    print(f"\nTotal trades: {count:,}")
    print(f"Unique symbols: {len(by_symbol):,}")
