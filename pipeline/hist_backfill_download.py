"""hist_backfill_download.py — bulk TOPS pcap downloader for the 2022-03-07 →
2026-03-27 IEX HIST re-derivation (session 2026-07-12).

Downloads every available TOPS pcap in the window to E:\\iex_hist_backfill\\
<YYYYMMDD>\\, resumable (skip when size matches the manifest), atomic (.part →
rename), 4 concurrent downloads. Writes _manifest_window.json (the download
gate record) and prints per-file progress + a running ETA.

Run: python pipeline/hist_backfill_download.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

OUT_ROOT = r"E:\iex_hist_backfill"
START, END = date(2022, 3, 7), date(2026, 3, 27)
IEX_HIST_API = "https://iextrading.com/api/1.0/hist"
WORKERS = 16
CHUNK = 1 << 22  # 4 MiB


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def tops_entry(day_entries) -> dict | None:
    if not isinstance(day_entries, list):
        return None
    for e in day_entries:
        if isinstance(e, dict) and e.get("feed") == "TOPS":
            return e
    return None


def download_one(yyyymmdd: str, entry: dict) -> tuple[str, str, int]:
    url = entry["link"]
    size = int(entry.get("size") or 0)
    d_dir = os.path.join(OUT_ROOT, yyyymmdd)
    os.makedirs(d_dir, exist_ok=True)
    dest = os.path.join(d_dir, f"tops_{yyyymmdd}.pcap.gz")
    if os.path.exists(dest) and size and os.path.getsize(dest) == size:
        return (yyyymmdd, "already", size)
    part = dest + ".part"
    for attempt in range(5):
        try:
            with requests.get(url, stream=True, timeout=(15, 300)) as r:
                r.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in r.iter_content(CHUNK):
                        f.write(chunk)
            got = os.path.getsize(part)
            if size and got != size:
                raise IOError(f"size mismatch {got} != {size}")
            os.replace(part, dest)
            return (yyyymmdd, "ok", got)
        except Exception as e:
            if attempt == 4:
                return (yyyymmdd, f"FAILED: {type(e).__name__}: {str(e)[:80]}", 0)
            time.sleep(20 * (attempt + 1))
    return (yyyymmdd, "FAILED: unreachable", 0)


def main() -> None:
    os.makedirs(OUT_ROOT, exist_ok=True)
    log("fetching full IEX HIST manifest (one call)...")
    r = requests.get(IEX_HIST_API, timeout=120)
    r.raise_for_status()
    manifest = r.json()
    window = {}
    for ymd, entries in manifest.items():
        try:
            d = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
        except (ValueError, TypeError):
            continue
        if START <= d <= END:
            t = tops_entry(entries)
            if t:
                window[ymd] = {"link": t["link"], "size": int(t.get("size") or 0),
                               "date": t.get("date"), "version": t.get("version")}
    total_bytes = sum(v["size"] for v in window.values())
    log(f"window sessions with TOPS pcaps: {len(window)}  total {total_bytes/1e12:.2f} TB")
    with open(os.path.join(OUT_ROOT, "_manifest_window.json"), "w") as f:
        json.dump(window, f, indent=1, sort_keys=True)

    done_bytes = 0
    t0 = time.time()
    n_ok = n_skip = n_fail = 0
    failures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(download_one, ymd, e): ymd for ymd, e in sorted(window.items())}
        for i, fut in enumerate(as_completed(futs), 1):
            ymd, status, got = fut.result()
            done_bytes += got
            if status == "ok":
                n_ok += 1
            elif status == "already":
                n_skip += 1
            else:
                n_fail += 1
                failures.append((ymd, status))
                log(f"  !! {ymd} {status}")
            if i % 10 == 0 or i == len(futs):
                el = time.time() - t0
                rate = done_bytes / el if el > 0 else 0
                remaining = total_bytes - done_bytes
                eta_h = remaining / rate / 3600 if rate > 0 else -1
                log(f"progress {i}/{len(futs)}  ok={n_ok} skip={n_skip} fail={n_fail}  "
                    f"{done_bytes/1e12:.2f}/{total_bytes/1e12:.2f} TB  "
                    f"{rate/1e6:.0f} MB/s  ETA {eta_h:.1f} h")
    log(f"DONE: ok={n_ok} skip={n_skip} fail={n_fail}")
    if failures:
        with open(os.path.join(OUT_ROOT, "_failures.json"), "w") as f:
            json.dump(failures, f, indent=1)
        log(f"failures recorded to _failures.json — re-run this script to retry (resumable)")


if __name__ == "__main__":
    main()
