"""
r2_client.py — Cloudflare R2 access via the S3-compatible API (boto3).

Reads credentials from environment variables:
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
  R2_ENDPOINT
  R2_BUCKET (optional, defaults to hfdatalibrary-data)
"""
from __future__ import annotations
import io
import os
from typing import List, Optional
from datetime import datetime

DEFAULT_BUCKET = "hfdatalibrary-data"


def get_client():
    """Return a boto3 S3 client configured for Cloudflare R2."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as e:
        raise RuntimeError("boto3 is required. Install: pip install boto3") from e

    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    endpoint = os.environ.get("R2_ENDPOINT")

    if not (access_key and secret_key and endpoint):
        raise RuntimeError(
            "Missing R2 credentials. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            region_name="auto",
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "adaptive"},
            max_pool_connections=50,
            tcp_keepalive=True,
        ),
    )


def get_bucket() -> str:
    return os.environ.get("R2_BUCKET", DEFAULT_BUCKET)


def object_exists(client, key: str, bucket: Optional[str] = None) -> bool:
    bucket = bucket or get_bucket()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise


def download_to_buffer(client, key: str, bucket: Optional[str] = None) -> Optional[bytes]:
    """Download an object from R2 into a memory buffer. Returns None if not found."""
    bucket = bucket or get_bucket()
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except client.exceptions.NoSuchKey:
        return None
    except client.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return None
        raise


def upload_from_buffer(
    client, key: str, data: bytes, content_type: str = "application/octet-stream",
    bucket: Optional[str] = None,
) -> None:
    bucket = bucket or get_bucket()
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def list_prefix(client, prefix: str, bucket: Optional[str] = None) -> List[str]:
    """List all object keys under a prefix. Paginated to handle >1000 results."""
    bucket = bucket or get_bucket()
    keys: List[str] = []
    continuation = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")
    return keys


# ── High-level helpers for our specific layout ──

def parquet_key(version: str, ticker: str, timeframe: str = "1min") -> str:
    """Return the R2 key for a parquet file under our standard layout.

    Layout:
      raw/{ticker}.parquet              (1-min raw)
      clean/{ticker}.parquet            (1-min clean)
      raw/{timeframe}/{ticker}.parquet  (aggregated raw)
      clean/{timeframe}/{ticker}.parquet
    """
    if timeframe == "1min":
        return f"{version}/{ticker}.parquet"
    return f"{version}/{timeframe}/{ticker}.parquet"


def csv_key(version: str, ticker: str, timeframe: str = "1min") -> str:
    if timeframe == "1min":
        return f"csv/{version}/{ticker}.csv"
    return f"csv/{version}/{timeframe}/{ticker}.csv"


def download_parquet(client, version: str, ticker: str, timeframe: str = "1min"):
    """Download a parquet file as a pandas DataFrame, or return None if missing."""
    import pandas as pd
    data = download_to_buffer(client, parquet_key(version, ticker, timeframe))
    if data is None:
        return None
    return pd.read_parquet(io.BytesIO(data))


def upload_parquet(client, df, version: str, ticker: str, timeframe: str = "1min") -> int:
    """Serialize a DataFrame to parquet and upload to R2. Returns bytes uploaded."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    data = buf.getvalue()
    upload_from_buffer(client, parquet_key(version, ticker, timeframe), data)
    return len(data)


def upload_csv(client, df, version: str, ticker: str, timeframe: str = "1min") -> int:
    """Serialize a DataFrame to CSV and upload to R2."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")
    upload_from_buffer(client, csv_key(version, ticker, timeframe), data, content_type="text/csv")
    return len(data)


if __name__ == "__main__":
    client = get_client()
    print(f"Bucket: {get_bucket()}")
    keys = list_prefix(client, "clean/", )
    sample_keys = [k for k in keys if k.endswith(".parquet") and "/" not in k.replace("clean/", "", 1)][:5]
    print(f"Found {len(keys)} keys under clean/")
    print(f"Sample 1-min files: {sample_keys}")
