"""hist_backfill_parse.py — parallel parse fleet for the IEX HIST re-derivation.

Continuously scans E:\\iex_hist_backfill\\ for completed pcap downloads (size
matches _manifest_window.json), runs pcap_extract.exe on each (12 concurrent),
writes trades_<ymd>.csv + a .ok sentinel (row count inside). Exits when every
manifest session has a sentinel. Safe to re-run (skips .ok days); safe alongside
the downloader (only touches size-verified files).

Run: python pipeline/hist_backfill_parse.py
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = r"E:\iex_hist_backfill"
PIPELINE = os.path.dirname(os.path.abspath(__file__))
EXTRACTOR = os.path.join(PIPELINE, "pcap_extract", "pcap_extract.exe")
TICKERS = os.path.join(os.path.dirname(PIPELINE), "data", "tickers.json")
WORKERS = 12
SCAN_SLEEP = 60


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def parse_one(ymd: str) -> tuple[str, str]:
    d_dir = os.path.join(ROOT, ymd)
    pcap = os.path.join(d_dir, f"tops_{ymd}.pcap.gz")
    out = os.path.join(d_dir, f"trades_{ymd}.csv")
    ok = out + ".ok"
    try:
        r = subprocess.run([EXTRACTOR, "-input", pcap, "-tickers", TICKERS, "-output", out],
                           capture_output=True, timeout=2400, text=True)
        if r.returncode != 0:
            return (ymd, f"FAIL rc={r.returncode}: {(r.stderr or r.stdout)[-150:]}")
        n = -1  # count data rows (minus header)
        with open(out, "rb") as f:
            for n, _ in enumerate(f):
                pass
        if n <= 0:
            # a real session can never yield zero universe trades (review fix)
            return (ymd, f"FAIL empty trades CSV ({n} data rows) — no sentinel written")
        with open(ok, "w") as f:
            f.write(str(n))
        return (ymd, f"ok {n:,} trades")
    except Exception as e:
        return (ymd, f"FAIL {type(e).__name__}: {str(e)[:120]}")


def main() -> None:
    with open(os.path.join(ROOT, "_manifest_window.json")) as f:
        manifest = json.load(f)
    log(f"parse fleet over {len(manifest)} sessions, {WORKERS} workers")
    inflight: set[str] = set()
    failures: dict[str, str] = {}
    results: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        while True:
            done_ymds = {y for y in manifest
                         if os.path.exists(os.path.join(ROOT, y, f"trades_{y}.csv.ok"))}
            # exit when every session is either done or has a recorded failure
            # (review fix: a permanent failure must not spin the loop forever)
            if len(done_ymds) + len(failures) >= len(manifest) and not inflight:
                break
            for ymd, meta in sorted(manifest.items()):
                if ymd in done_ymds or ymd in inflight or ymd in failures:
                    continue
                pcap = os.path.join(ROOT, ymd, f"tops_{ymd}.pcap.gz")
                if not (os.path.exists(pcap) and os.path.getsize(pcap) == meta["size"]):
                    continue  # not fully downloaded yet
                inflight.add(ymd)
                futs[ex.submit(parse_one, ymd)] = ymd
            newly = [f for f in list(futs) if f.done()]
            for f in newly:
                ymd = futs.pop(f)
                inflight.discard(ymd)
                try:
                    y, status = f.result()
                except Exception as e:  # worker crash (review fix)
                    y, status = ymd, f"FAIL worker: {type(e).__name__}"
                results[y] = status
                if status.startswith("FAIL"):
                    failures[y] = status
                    log(f"  !! {y} {status}")
                    with open(os.path.join(ROOT, "_parse_failures.json"), "w") as pf:
                        json.dump(failures, pf, indent=1)
            n_ok = sum(1 for s in results.values() if s.startswith("ok"))
            log(f"parsed {len(done_ymds)}/{len(manifest)} done  (+{n_ok} this run, "
                f"{len(inflight)} in flight, {len(failures)} failed)")
            time.sleep(SCAN_SLEEP)

    log(f"ALL SESSIONS PARSED. failures: {len(failures)}")
    if failures:
        with open(os.path.join(ROOT, "_parse_failures.json"), "w") as f:
            json.dump(failures, f, indent=1)


if __name__ == "__main__":
    main()
