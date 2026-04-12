"""
iex_manifest.py — Fetch the IEX HIST manifest for a given date and locate the TOPS pcap file.

IEX HIST publishes daily pcap captures of their TOPS feed at
https://iextrading.com/api/1.0/hist. The endpoint returns JSON keyed by
YYYYMMDD with metadata for each available feed (TOPS, DEEP) for that day.

We only care about the TOPS feed (top-of-book trades and quotes) — DEEP is the
full order book and would be massive overkill for OHLCV bars.
"""
from __future__ import annotations
from datetime import date
from typing import Optional, Dict, Any
import json
import requests

IEX_HIST_API = "https://iextrading.com/api/1.0/hist"


def fetch_full_manifest() -> Dict[str, Any]:
    """Fetch the entire IEX HIST manifest. Large response — prefer fetch_for_date."""
    r = requests.get(IEX_HIST_API, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_for_date(d: date) -> Dict[str, Any]:
    """Fetch the manifest entries for a specific date.

    Returns the manifest entry dict for the date or {} if no data is published.
    """
    yyyymmdd = d.strftime("%Y%m%d")
    url = f"{IEX_HIST_API}?date={yyyymmdd}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        return {}
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {yyyymmdd: data}
    return {}


def find_tops_url(manifest_for_date: Dict[str, Any], d: date) -> Optional[Dict[str, Any]]:
    """Locate the TOPS pcap entry inside a manifest response.

    Returns a dict with keys: link, date, feed, version, protocol, size (bytes)
    Or None if no TOPS entry is found for that date.
    """
    yyyymmdd = d.strftime("%Y%m%d")
    if yyyymmdd not in manifest_for_date:
        return None

    entries = manifest_for_date[yyyymmdd]
    if not isinstance(entries, list):
        return None

    # Pick the latest TOPS entry (sometimes IEX publishes multiple versions per day)
    tops_entries = [e for e in entries if e.get("feed", "").upper() == "TOPS"]
    if not tops_entries:
        return None

    # Prefer the highest version number
    def version_key(e):
        v = e.get("version", "0")
        try:
            return tuple(int(p) for p in str(v).split("."))
        except (ValueError, TypeError):
            return (0,)

    tops_entries.sort(key=version_key, reverse=True)
    return tops_entries[0]


def get_tops_pcap_for_date(d: date) -> Optional[Dict[str, Any]]:
    """Convenience: fetch manifest and locate the TOPS pcap entry for a date.

    Returns:
        {"date": d, "link": "https://...", "size": bytes, "version": "1.6", ...}
        or None if no TOPS data exists for that date.
    """
    manifest = fetch_for_date(d)
    if not manifest:
        return None
    entry = find_tops_url(manifest, d)
    if entry is None:
        return None
    return {
        "date": d,
        "link": entry["link"],
        "size": entry.get("size"),
        "version": entry.get("version"),
        "protocol": entry.get("protocol", "IEXTP1"),
        "feed": entry.get("feed", "TOPS"),
    }


if __name__ == "__main__":
    import sys
    from datetime import datetime

    if len(sys.argv) < 2:
        print("Usage: python iex_manifest.py YYYY-MM-DD")
        sys.exit(1)

    d = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    entry = get_tops_pcap_for_date(d)
    if entry is None:
        print(f"No TOPS pcap published for {d}")
        sys.exit(2)

    print(f"TOPS pcap for {d}:")
    print(json.dumps(entry, indent=2, default=str))
