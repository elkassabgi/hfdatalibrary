"""
tops_parser.py — Parse the IEX TOPS protocol from a pcap stream.

The IEX HIST pcap files contain UDP packets carrying IEX-TP segments. Each
segment contains zero or more IEX TOPS messages. We only care about Trade
Report Messages (message type 'T') for OHLCV bar construction.

Reference specifications:
  - IEX-TP transport: https://iextrading.com/docs/IEX%20Transport%20Specification.pdf
  - IEX TOPS message types: https://iextrading.com/docs/IEX%20TOPS%20Specification.pdf
  - Reference Go implementation: github.com/timpalpant/go-iex

Binary format (all little-endian):

  IEX-TP segment header (40 bytes):
    0  1  Version (uint8) — IEX-TP version, currently 1
    1  1  Reserved (uint8)
    2  2  Message Protocol ID (uint16) — TOPS = 0x8003, DEEP = 0x8004
    4  4  Channel ID (uint32)
    8  4  Session ID (uint32)
    12 2  Payload Length (uint16) — total bytes of message data after the header
    14 2  Message Count (uint16) — number of messages in this segment
    16 8  Stream Offset (int64)
    24 8  First Message Sequence Number (int64)
    32 8  Send Time (int64) — nanoseconds since Unix epoch (UTC)

  Then `Message Count` messages follow. Each is prefixed with:
    0  2  Message Length (uint16) — bytes in this message body

  Followed by the message body, which begins with a 1-byte message type.

Trade Report Message (type 'T' = 0x54), version 1.6, 38 bytes total:
    0  1  Message Type (0x54)
    1  1  Sale Condition Flags
    2  8  Timestamp (int64, nanoseconds since Unix epoch UTC)
    10 8  Symbol (8-byte right-padded ASCII)
    18 4  Size (uint32, shares)
    22 8  Price (int64, fixed-point with 4 decimal places — divide by 10000)
    30 8  Trade ID (int64)

We discard quote updates, price level updates, security event messages, and
all other message types — we only need trades to build OHLCV bars.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Generator, Iterable, Iterator, BinaryIO, Optional, Set
import struct

# IEX-TP segment header struct: little-endian, 40 bytes total
# version, reserved, protocol_id, channel_id, session_id,
# payload_len, msg_count, stream_offset, first_seq, send_time
IEX_TP_HEADER = struct.Struct("<BBHIIHHqqq")
assert IEX_TP_HEADER.size == 40

TOPS_PROTOCOL_ID = 0x8003

# Trade report message: type, condition, timestamp, symbol, size, price, trade_id
TRADE_MSG = struct.Struct("<BB q 8s I q q")
TRADE_MSG_TYPE = 0x54  # 'T'


@dataclass(frozen=True)
class Trade:
    """A single executed trade from the IEX TOPS feed."""
    symbol: str          # ticker symbol (stripped of trailing spaces)
    timestamp_ns: int    # nanoseconds since Unix epoch (UTC)
    price: float         # share price in dollars
    size: int            # number of shares
    sale_condition: int  # raw sale condition flags
    trade_id: int        # IEX-assigned trade identifier


def parse_tops_messages(
    segment_payload: memoryview,
    msg_count: int,
    universe: Optional[Set[str]] = None,
) -> Iterator[Trade]:
    """Parse the messages inside a single IEX-TP segment payload.

    Args:
        segment_payload: a memoryview over the bytes of one IEX-TP segment payload
        msg_count: number of messages in the segment (from segment header)
        universe: if provided, only yield trades whose symbol is in this set

    Yields:
        Trade instances for each Trade Report Message in the segment.
    """
    offset = 0
    payload_len = len(segment_payload)
    parsed = 0
    while parsed < msg_count and offset < payload_len:
        if offset + 2 > payload_len:
            return
        # Each message is prefixed with a 2-byte little-endian length
        msg_len = int.from_bytes(segment_payload[offset:offset + 2], "little")
        offset += 2
        if msg_len == 0 or offset + msg_len > payload_len:
            return

        # First byte of the message body is the message type
        if msg_len >= 1 and segment_payload[offset] == TRADE_MSG_TYPE and msg_len >= TRADE_MSG.size:
            (
                msg_type, sale_cond, ts_ns, sym_bytes, size, price_fixed, trade_id,
            ) = TRADE_MSG.unpack_from(segment_payload, offset)
            symbol = sym_bytes.rstrip(b" \x00").decode("ascii", errors="ignore")
            if universe is None or symbol in universe:
                yield Trade(
                    symbol=symbol,
                    timestamp_ns=ts_ns,
                    price=price_fixed / 10000.0,
                    size=size,
                    sale_condition=sale_cond,
                    trade_id=trade_id,
                )

        offset += msg_len
        parsed += 1


def iter_tops_segments(udp_payloads: Iterable[bytes]) -> Iterator[tuple[memoryview, int]]:
    """Walk a stream of UDP payloads from a pcap file, yielding (payload, msg_count)
    for each TOPS-protocol IEX-TP segment.

    Filters out non-TOPS protocols (e.g. DEEP) and corrupt segments.
    """
    for udp in udp_payloads:
        if len(udp) < IEX_TP_HEADER.size:
            continue
        try:
            (
                version, _reserved, protocol_id, _channel, _session,
                payload_len, msg_count, _stream_offset, _first_seq, _send_time,
            ) = IEX_TP_HEADER.unpack_from(udp, 0)
        except struct.error:
            continue
        if version != 1 or protocol_id != TOPS_PROTOCOL_ID:
            continue
        if msg_count == 0:
            continue
        body_start = IEX_TP_HEADER.size
        body_end = body_start + payload_len
        if body_end > len(udp):
            continue
        yield memoryview(udp)[body_start:body_end], msg_count


def parse_tops_from_udp_payloads(
    udp_payloads: Iterable[bytes],
    universe: Optional[Set[str]] = None,
) -> Generator[Trade, None, None]:
    """High-level helper: take an iterable of UDP payload bytes and yield Trade objects."""
    for segment_payload, msg_count in iter_tops_segments(udp_payloads):
        yield from parse_tops_messages(segment_payload, msg_count, universe)


def iter_udp_payloads_from_pcap(pcap_stream: BinaryIO) -> Iterator[bytes]:
    """Walk a pcap stream (file-like, binary mode) and yield UDP payload bytes
    from each captured packet.

    Uses dpkt for pcap framing and Ethernet/IP/UDP unwrapping. dpkt is a small,
    pure-Python library — installs in seconds.
    """
    try:
        import dpkt
    except ImportError as e:
        raise RuntimeError("dpkt is required. Install: pip install dpkt") from e

    pcap = dpkt.pcap.Reader(pcap_stream)
    for _ts, buf in pcap:
        try:
            eth = dpkt.ethernet.Ethernet(buf)
        except Exception:
            continue
        ip_pkt = eth.data
        if not isinstance(ip_pkt, (dpkt.ip.IP, dpkt.ip6.IP6)):
            continue
        udp = ip_pkt.data
        if not isinstance(udp, dpkt.udp.UDP):
            continue
        yield bytes(udp.data)


def parse_tops_pcap(
    pcap_stream: BinaryIO,
    universe: Optional[Set[str]] = None,
) -> Generator[Trade, None, None]:
    """Top-level entry point: parse a pcap stream end-to-end and yield Trade objects.

    Args:
        pcap_stream: file-like binary stream of an IEX HIST pcap file
        universe: optional set of ticker symbols to filter to

    Yields:
        Trade instances, one per executed trade in the pcap.
    """
    udp_payloads = iter_udp_payloads_from_pcap(pcap_stream)
    yield from parse_tops_from_udp_payloads(udp_payloads, universe)


if __name__ == "__main__":
    import sys, gzip

    if len(sys.argv) < 2:
        print("Usage: python tops_parser.py PCAP_FILE_OR_GZ [TICKER1 TICKER2 ...]")
        sys.exit(1)

    path = sys.argv[1]
    universe = set(sys.argv[2:]) or None

    if path.endswith(".gz"):
        f = gzip.open(path, "rb")
    else:
        f = open(path, "rb")

    count = 0
    by_symbol = {}
    for trade in parse_tops_pcap(f, universe):
        count += 1
        by_symbol[trade.symbol] = by_symbol.get(trade.symbol, 0) + 1
        if count <= 5:
            print(f"  {trade.symbol} @ {trade.price} x {trade.size} (ts={trade.timestamp_ns})")
    f.close()

    print(f"\nTotal trades parsed: {count:,}")
    print(f"Unique symbols: {len(by_symbol):,}")
    if universe:
        print(f"(Filtered to universe of {len(universe)} symbols)")
