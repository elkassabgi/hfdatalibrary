"""Core client for the HF Data Library API.

Wraps https://api.hfdatalibrary.com. Auth is via an API key sent in the
X-API-Key header (get one at https://hfdatalibrary.com/pages/account).
"""
from __future__ import annotations

import io
import os
import time
from typing import Dict, List, Optional, Union

import requests

BASE_URL = os.environ.get("HFDL_BASE_URL", "https://api.hfdatalibrary.com")
VERSIONS = ("clean", "raw")
TIMEFRAMES = ("1min", "5min", "15min", "30min", "hourly", "daily", "weekly", "monthly")

_DEFAULT_KEY = os.environ.get("HFDL_API_KEY")


class HFDLError(Exception):
    """Raised on API or client errors."""


def set_key(api_key: str) -> None:
    """Set the API key for the module-level default client."""
    global _DEFAULT_KEY
    _DEFAULT_KEY = api_key


class Client:
    """A configured HF Data Library client.

    Prefer the module-level functions (hfdl.get, hfdl.symbols) for simple use;
    instantiate Client directly to hold a key explicitly or tune the session.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = BASE_URL,
        timeout: int = 120,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ):
        self.api_key = api_key or _DEFAULT_KEY
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "hfdatalibrary-python/0.1.0"})

    # ---- low-level request with retry/backoff -----------------------------
    def _request(self, path: str, params: Optional[dict] = None, auth: bool = True) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = {}
        if auth:
            if not self.api_key:
                raise HFDLError(
                    "No API key set. Call hfdl.set_key('...'), pass api_key=..., "
                    "or set the HFDL_API_KEY environment variable. "
                    "Get a key at https://hfdatalibrary.com/pages/account"
                )
            headers["X-API-Key"] = self.api_key

        last_exc = None
        for attempt in range(self.max_retries):
            try:
                r = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            except requests.RequestException as e:
                last_exc = e
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code == 200:
                return r
            if r.status_code in (401, 403):
                raise HFDLError(f"Authentication failed ({r.status_code}). Check your API key.")
            if r.status_code == 404:
                raise HFDLError(f"Not found: {path} (params={params}). Check the ticker/timeframe/version.")
            if r.status_code == 429:
                # rate limited — honor Retry-After if present
                wait = int(r.headers.get("Retry-After", 5 * (attempt + 1)))
                time.sleep(wait)
                continue
            if 500 <= r.status_code < 600:
                last_exc = HFDLError(f"Server error {r.status_code}")
                time.sleep(2.0 * (attempt + 1))
                continue
            raise HFDLError(f"HTTP {r.status_code}: {r.text[:200]}")
        raise HFDLError(f"Request to {path} failed after {self.max_retries} attempts: {last_exc}")

    # ---- public API -------------------------------------------------------
    def symbols(self) -> List[str]:
        """Return the list of available ticker symbols (no auth required)."""
        r = self._request("/v1/symbols", auth=False)
        data = r.json()
        syms = data.get("symbols", data) if isinstance(data, dict) else data
        # symbols may be list[str] or list[dict]; normalize to list[str]
        out = []
        for s in syms:
            out.append(s["ticker"] if isinstance(s, dict) else s)
        return out

    def get(
        self,
        ticker: Union[str, List[str]],
        version: str = "clean",
        timeframe: str = "1min",
        fmt: str = "parquet",
    ):
        """Fetch bars for one or many tickers.

        Returns a pandas DataFrame for a single ticker, or a dict
        {ticker: DataFrame} when `ticker` is a list.

        version:   'clean' (default) or 'raw'
        timeframe: one of TIMEFRAMES
        fmt:       'parquet' (default, needs pyarrow) or 'csv'
        """
        if version not in VERSIONS:
            raise HFDLError(f"version must be one of {VERSIONS}")
        if timeframe not in TIMEFRAMES:
            raise HFDLError(f"timeframe must be one of {TIMEFRAMES}")
        if fmt not in ("parquet", "csv"):
            raise HFDLError("fmt must be 'parquet' or 'csv'")

        if isinstance(ticker, (list, tuple, set)):
            return {t: self._get_one(t, version, timeframe, fmt) for t in ticker}
        return self._get_one(ticker, version, timeframe, fmt)

    def _get_one(self, ticker: str, version: str, timeframe: str, fmt: str):
        params = {"version": version, "timeframe": timeframe, "format": fmt}
        r = self._request(f"/v1/download/{ticker.upper()}", params=params)
        content = r.content
        try:
            import pandas as pd
        except ImportError as e:
            raise HFDLError("pandas is required to return DataFrames: pip install pandas") from e

        if fmt == "csv":
            return pd.read_csv(io.BytesIO(content))
        # parquet
        try:
            return pd.read_parquet(io.BytesIO(content))
        except ImportError as e:
            raise HFDLError(
                "pyarrow is required to read parquet: pip install pyarrow "
                "(or call get(..., fmt='csv'))"
            ) from e


# ---- module-level convenience (uses a lazily-built default client) --------
_default_client: Optional[Client] = None


def _client() -> Client:
    global _default_client
    # rebuild if key changed via set_key after first use
    if _default_client is None or _default_client.api_key != _DEFAULT_KEY:
        _default_client = Client(api_key=_DEFAULT_KEY)
    return _default_client


def symbols() -> List[str]:
    """Module-level: list available tickers."""
    return _client().symbols()


def get(ticker, version: str = "clean", timeframe: str = "1min", fmt: str = "parquet"):
    """Module-level: fetch bars. See Client.get."""
    return _client().get(ticker, version=version, timeframe=timeframe, fmt=fmt)
