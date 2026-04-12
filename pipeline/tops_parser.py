"""
tops_parser.py — Parse the IEX TOPS protocol from a pcap.gz file.

IEX HIST publishes "pcap.gz" files that use a non-standard libpcap variant.
Standard pcap parsers (like dpkt) cannot read them. We use the rob-blackbourn
`iex_parser` library, which is purpose-built for IEX HIST files.

Reference:
  - iex_parser: https://github.com/rob-blackbourn/iex_parser
  - PyPI: https://pypi.org/project/iex-parser/
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Generator, Optional, Set


@dataclass(frozen=True)
class Trade:
    """A single executed trade from the IEX TOPS feed."""
    symbol: str          # ticker symbol (stripped of trailing spaces)
    timestamp_ns: int    # nanoseconds since Unix epoch (UTC)
    price: float         # share price in dollars
    size: int            # number of shares
    sale_condition: int  # raw sale condition flags (best-effort decoded)
    trade_id: int        # IEX-assigned trade identifier


def parse_tops_pcap(
    pcap_path: str,
    universe: Optional[Set[str]] = None,
) -> Generator[Trade, None, None]:
    """Parse an IEX HIST TOPS pcap.gz file and yield trade messages.

    Args:
        pcap_path: path to the .pcap.gz file (the iex_parser library handles
            decompression internally)
        universe: optional set of ticker symbols to filter to. Trades for
            symbols not in the universe are dropped silently.

    Yields:
        Trade instances, one per executed trade in the pcap.

    Note:
        iex_parser only accepts a file path, not a stream. The caller is
        responsible for downloading the pcap.gz file to a local path before
        calling this function.
    """
    try:
        from iex_parser import Parser, TOPS_1_6
    except ImportError as e:
        raise RuntimeError(
            "iex_parser is required. Install: pip install iex_parser"
        ) from e

    with Parser(pcap_path, TOPS_1_6) as reader:
        for msg in reader:
            if msg.get("type") != "trade_report":
                continue

            # Symbol is bytes, may have trailing spaces/nulls
            sym_raw = msg.get("symbol", b"")
            if isinstance(sym_raw, bytes):
                symbol = sym_raw.rstrip(b" \x00").decode("ascii", errors="ignore")
            else:
                symbol = str(sym_raw).strip()

            if not symbol:
                continue
            if universe is not None and symbol not in universe:
                continue

            # timestamp is a datetime.datetime (tz-aware UTC)
            ts = msg.get("timestamp")
            if ts is None:
                continue
            try:
                ts_ns = int(ts.timestamp() * 1_000_000_000)
            except AttributeError:
                continue

            # price is a Decimal
            price_raw = msg.get("price", 0)
            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                continue

            # size is an int
            try:
                size = int(msg.get("size", 0))
            except (TypeError, ValueError):
                continue

            if size <= 0 or price <= 0:
                continue

            yield Trade(
                symbol=symbol,
                timestamp_ns=ts_ns,
                price=price,
                size=size,
                sale_condition=int(msg.get("flags", 0) or 0),
                trade_id=int(msg.get("trade_id", 0) or 0),
            )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python tops_parser.py PCAP_PATH [TICKER1 TICKER2 ...]")
        sys.exit(1)

    path = sys.argv[1]
    universe = set(sys.argv[2:]) or None

    count = 0
    by_symbol = {}
    for trade in parse_tops_pcap(path, universe):
        count += 1
        by_symbol[trade.symbol] = by_symbol.get(trade.symbol, 0) + 1
        if count <= 5:
            print(f"  {trade.symbol} @ {trade.price} x {trade.size} (ts={trade.timestamp_ns})")

    print(f"\nTotal trades parsed: {count:,}")
    print(f"Unique symbols: {len(by_symbol):,}")
    if universe:
        print(f"(Filtered to universe of {len(universe)} symbols)")
