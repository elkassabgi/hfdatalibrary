"""
seed_tickers.py — One-time helper to generate data/tickers.json from R2.

Lists all parquet files under clean/ in R2, extracts ticker symbols, and writes
them to data/tickers.json. The pipeline reads this file to filter trades.

Run this once after the R2 bucket is populated, then commit data/tickers.json
to the repo. Re-run only when the universe changes.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from r2_client import get_client, list_prefix


def seed():
    client = get_client()
    print("Listing clean/*.parquet from R2...")
    keys = list_prefix(client, "clean/")
    # Keep only top-level files (not aggregated ones in clean/5min/, etc.)
    tickers = []
    for k in keys:
        if not k.endswith(".parquet"):
            continue
        rel = k[len("clean/"):]
        if "/" in rel:
            continue  # nested timeframe directory
        tickers.append(rel.replace(".parquet", ""))
    tickers.sort()

    out_path = Path(__file__).parent.parent / "data" / "tickers.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(tickers, f, indent=2)

    print(f"Wrote {len(tickers)} tickers to {out_path}")


if __name__ == "__main__":
    seed()
