"""hist_backfill_bars.py — stage 2 of the IEX HIST re-derivation: turn each
day's trades CSV into a per-day bars parquet, using the SAME parser and bar
builder as the production pipeline (tops_parser.parse_trades_csv +
build_bars.build_bars), so the output is byte-for-byte the schema merge_ticker
expects: [ticker, datetime, Open, High, Low, Close, Volume, source='iex'].

Continuously scans for trades_<ymd>.csv.ok sentinels (written by the parse
fleet), builds bars 14-wide in a process pool, writes bars_<ymd>.parquet +
.ok sentinel (row count inside). Resumable; exits when all manifest sessions
have bars sentinels.

Run: python pipeline/hist_backfill_bars.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

ROOT = r"E:\iex_hist_backfill"
PIPELINE = os.path.dirname(os.path.abspath(__file__))
WORKERS = 14
SCAN_SLEEP = 60


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def build_one(ymd: str) -> tuple[str, str]:
    # imports inside the worker (ProcessPool on Windows spawns fresh interpreters)
    import pandas as pd

    sys.path.insert(0, PIPELINE)
    from build_bars import build_bars
    from tops_parser import parse_trades_csv

    d_dir = os.path.join(ROOT, ymd)
    csv_path = os.path.join(d_dir, f"trades_{ymd}.csv")
    out = os.path.join(d_dir, f"bars_{ymd}.parquet")
    ok = out + ".ok"
    try:
        universe_path = os.path.join(os.path.dirname(PIPELINE), "data", "tickers.json")
        with open(universe_path) as f:
            universe = set(json.load(f))
        by_symbol: dict[str, list] = {}
        n_trades = 0
        for trade in parse_trades_csv(csv_path, universe=universe):
            n_trades += 1
            by_symbol.setdefault(trade.symbol, []).append(trade)
        rows = []
        for symbol, trades in by_symbol.items():
            for b in build_bars(trades).get(symbol, []):
                rows.append({"ticker": symbol, "datetime": b.minute_start,
                             "Open": b.open, "High": b.high, "Low": b.low,
                             "Close": b.close, "Volume": b.volume, "source": "iex"})
        df = pd.DataFrame(rows)
        if len(df) == 0:
            # defense in depth: parse stage already fails empty CSVs (review fix)
            return (ymd, "FAIL 0 bars built — refusing sentinel")
        tmp = out + ".tmp"
        df.to_parquet(tmp, index=False)
        os.replace(tmp, out)
        with open(ok, "w") as f:
            f.write(f"{len(df)} bars from {n_trades} trades, {len(by_symbol)} tickers")
        return (ymd, f"ok {len(df):,} bars / {n_trades:,} trades / {len(by_symbol)} tickers")
    except Exception as e:
        return (ymd, f"FAIL {type(e).__name__}: {str(e)[:150]}")


def main() -> None:
    with open(os.path.join(ROOT, "_manifest_window.json")) as f:
        manifest = json.load(f)
    log(f"bars fleet over {len(manifest)} sessions, {WORKERS} workers")
    inflight: set[str] = set()
    failures: dict[str, str] = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        while True:
            done = {y for y in manifest
                    if os.path.exists(os.path.join(ROOT, y, f"bars_{y}.parquet.ok"))}
            # exit when every session is done or failed here, or failed upstream
            # in the parse stage (review fix: no infinite spin on permanent failures)
            parse_failed = set()
            pf_path = os.path.join(ROOT, "_parse_failures.json")
            if os.path.exists(pf_path):
                with open(pf_path) as pf:
                    parse_failed = set(json.load(pf))
            if len(done) + len(failures) + len(parse_failed - done) >= len(manifest) and not inflight:
                break
            for ymd in sorted(manifest):
                if ymd in done or ymd in inflight or ymd in failures:
                    continue
                if not os.path.exists(os.path.join(ROOT, ymd, f"trades_{ymd}.csv.ok")):
                    continue  # parse stage not finished for this day yet
                inflight.add(ymd)
                futs[ex.submit(build_one, ymd)] = ymd
            for f in [f for f in list(futs) if f.done()]:
                ymd = futs.pop(f)
                inflight.discard(ymd)
                try:
                    y, status = f.result()
                except Exception as e:  # worker crash (review fix)
                    y, status = ymd, f"FAIL worker: {type(e).__name__}"
                if status.startswith("FAIL"):
                    failures[y] = status
                    log(f"  !! {y} {status}")
                    with open(os.path.join(ROOT, "_bars_failures.json"), "w") as bf:
                        json.dump(failures, bf, indent=1)
            log(f"bars {len(done)}/{len(manifest)} done ({len(inflight)} in flight, {len(failures)} failed)")
            time.sleep(SCAN_SLEEP)
    log(f"ALL BARS BUILT. failures: {len(failures)}")
    if failures:
        with open(os.path.join(ROOT, "_bars_failures.json"), "w") as f:
            json.dump(failures, f, indent=1)


if __name__ == "__main__":
    main()
