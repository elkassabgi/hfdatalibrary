"""upload_variables.py - push the computed academic variables to R2.

Reads R2 credentials from the ENVIRONMENT (never hardcoded):
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT   (bucket: R2_BUCKET or hfdatalibrary-data)

For each local data/variables/{version}/{ticker}_vars.parquet it uploads:
    {version}/variables/{ticker}.parquet   (all 25 variables)
    {version}/quality/{ticker}.parquet     (the data-quality subset for /v1/quality)
mirroring the existing OHLCV key convention {version}/{timeframe}/{ticker}.parquet, so the
worker can serve them via the same raw-passthrough as handleDownload.

Resumable + idempotent (skips objects already present with the same size).

    python pipeline/upload_variables.py --dry-run            # plan only, no creds needed
    python pipeline/upload_variables.py --version clean       # real upload (needs R2_* in env)
    python pipeline/upload_variables.py                       # both versions
"""
from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VARS_DIR = ROOT / "data" / "variables"
BUCKET = os.environ.get("R2_BUCKET", "hfdatalibrary-data")
QUALITY_COLS = ["trade_date", "ticker", "gap_rate", "observed_bars",
                "longest_gap", "max_bars_since_trade"]


def _client():
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from r2_client import _load_local_env
    _load_local_env()   # pick up repo-root .env locally (no-op in CI where env is set)
    ak = os.environ.get("R2_ACCESS_KEY_ID")
    sk = os.environ.get("R2_SECRET_ACCESS_KEY")
    ep = os.environ.get("R2_ENDPOINT")
    if not (ak and sk and ep):
        raise SystemExit(
            "R2 credentials missing. Set R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / "
            "R2_ENDPOINT in the environment (rotated keys, never committed).")
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak,
                        aws_secret_access_key=sk, region_name="auto",
                        config=Config(signature_version="s3v4",
                                      retries={"max_attempts": 5, "mode": "standard"}))


def _remote_size(s3, key):
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except Exception:
        return None


def _quality_bytes(vars_path: Path) -> bytes:
    df = pd.read_parquet(vars_path)
    cols = [c for c in QUALITY_COLS if c in df.columns]
    buf = io.BytesIO()
    df[cols].to_parquet(buf, compression="zstd")
    return buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", choices=["clean", "raw", "both"], default="both")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    versions = ["clean", "raw"] if a.version == "both" else [a.version]

    plan = []  # (local_path_or_None, key, nbytes)
    for v in versions:
        vdir = VARS_DIR / v
        for f in sorted(vdir.glob("*_vars.parquet")):
            ticker = f.stem[:-5] if f.stem.endswith("_vars") else f.stem
            plan.append((f, f"{v}/variables/{ticker}.parquet", f.stat().st_size))
            plan.append((f, f"{v}/quality/{ticker}.parquet", None))  # derived

    nvar = sum(1 for _, k, _ in plan if "/variables/" in k)
    print(f"plan: {len(plan)} objects ({nvar} variables + {nvar} quality) "
          f"across {versions} -> s3://{BUCKET}/")
    if a.dry_run:
        gb = sum(s for _, k, s in plan if s) / 1e9
        print(f"DRY RUN: ~{gb:.2f} GB of variables files (+ derived quality). No R2 contact.")
        return

    s3 = _client()
    up = skip = 0
    for src, key, size in plan:
        if "/quality/" in key:
            body = _quality_bytes(src)
            if _remote_size(s3, key) == len(body):
                skip += 1; continue
            s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        else:
            if _remote_size(s3, key) == size:
                skip += 1; continue
            s3.upload_file(str(src), BUCKET, key)
        up += 1
        if up % 200 == 0:
            print(f"  uploaded {up}, skipped {skip}...", flush=True)
    print(f"done: uploaded {up}, skipped {skip} already-current")


if __name__ == "__main__":
    main()
