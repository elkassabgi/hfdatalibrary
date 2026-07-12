"""hist_backfill_shard.py — stage 2.5 of the IEX HIST re-derivation: combine
all per-day bars parquets into per-ticker shards (partitioned dataset), so the
per-ticker merge stage reads one small file instead of scanning 1,019.

Requires: every session in _manifest_window.json has bars_<ymd>.parquet.ok.
Output:  E:\\iex_hist_backfill\\_byticker\\ticker=<T>\\*.parquet (pyarrow
         partitioned write, single pass) + _byticker\\_SHARD_OK sentinel with
         totals for the verification gate.

Run: python pipeline/hist_backfill_shard.py
"""
from __future__ import annotations

import json
import os
import shutil
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = r"E:\iex_hist_backfill"
OUT = os.path.join(ROOT, "_byticker")


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def main() -> None:
    with open(os.path.join(ROOT, "_manifest_window.json")) as f:
        manifest = json.load(f)
    missing = [y for y in manifest
               if not os.path.exists(os.path.join(ROOT, y, f"bars_{y}.parquet.ok"))]
    if missing:
        raise SystemExit(f"{len(missing)} sessions lack bars sentinels (e.g. {missing[:5]}) — "
                         "run after the bars fleet completes.")

    # per-day count reconciliation (review fix: sentinel counts must match files)
    per_day = {}
    zero_days = []
    for y in sorted(manifest):
        with open(os.path.join(ROOT, y, f"bars_{y}.parquet.ok")) as f:
            claimed = int(f.read().split()[0])
        actual = pq.read_metadata(os.path.join(ROOT, y, f"bars_{y}.parquet")).num_rows
        if claimed != actual:
            raise SystemExit(f"{y}: sentinel claims {claimed} bars but parquet has {actual} — "
                             "corrupt day, re-run bars for it before sharding")
        per_day[y] = actual
        if actual == 0:
            zero_days.append(y)
    if zero_days:
        log(f"WARNING: {len(zero_days)} zero-bar days: {zero_days[:10]} — must be explained "
            "before _VERIFY_OK (a real session cannot yield zero universe bars)")

    files = [os.path.join(ROOT, y, f"bars_{y}.parquet") for y in sorted(manifest)]
    log(f"reading {len(files)} day files...")
    tables = []
    total = 0
    for i, fp in enumerate(files, 1):
        t = pq.read_table(fp)
        if t.num_rows:
            tables.append(t)
            total += t.num_rows
        if i % 100 == 0:
            log(f"  {i}/{len(files)} files, {total:,} bars so far")
    big = pa.concat_tables(tables)
    del tables
    log(f"combined table: {big.num_rows:,} bars, {big.nbytes/1e9:.1f} GB in RAM")

    n_tickers = pc.count_distinct(big.column("ticker")).as_py()
    if os.path.exists(OUT):
        shutil.rmtree(OUT)
    log(f"writing partitioned dataset ({n_tickers} tickers) -> {OUT}")
    ds.write_dataset(big, OUT, format="parquet", partitioning=["ticker"],
                     partitioning_flavor="hive",
                     max_partitions=2048, max_open_files=2048,
                     max_rows_per_file=50_000_000, max_rows_per_group=1_000_000)

    # gate record: totals must match what the merge stage later reads back
    import uuid
    sentinel = {"run_id": f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}",
                "sessions": len(manifest), "total_bars": big.num_rows,
                "n_tickers": n_tickers, "per_day_bars": per_day,
                "zero_bar_days": zero_days,
                "datetime_min": str(pc.min(big.column("datetime")).as_py()),
                "datetime_max": str(pc.max(big.column("datetime")).as_py())}
    with open(os.path.join(OUT, "_SHARD_OK"), "w") as f:
        json.dump(sentinel, f, indent=1)
    log(f"SHARD OK: {json.dumps(sentinel)}")


if __name__ == "__main__":
    main()
