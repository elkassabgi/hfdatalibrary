"""hfdatalibrary — Python client for the HF Data Library.

Free, research-grade 1-minute OHLCV data for U.S. equities and ETFs.

Quick start:
    import hfdatalibrary as hfdl
    hfdl.set_key("YOUR_API_KEY")          # or set HFDL_API_KEY in the environment

    universe = hfdl.symbols()             # list available tickers
    df  = hfdl.get("AAPL")               # clean 1-minute bars -> pandas DataFrame
    df  = hfdl.get("AAPL", version="raw", timeframe="daily")
    panel = hfdl.get(["AAPL", "MSFT"])    # dict {ticker: DataFrame}

Data is survivorship-biased (constituents fixed ~2023; pre-2022 history is
survivor-conditioned). See https://hfdatalibrary.com/pages/docs for limitations.
"""
from .client import (
    Client,
    set_key,
    symbols,
    get,
    VERSIONS,
    TIMEFRAMES,
    HFDLError,
)

__version__ = "0.1.0"
__all__ = [
    "Client", "set_key", "symbols", "get",
    "VERSIONS", "TIMEFRAMES", "HFDLError", "lab", "__version__",
]


def __getattr__(name):
    # Lazy access to the recipe layer so `import hfdatalibrary` stays light and
    # never pulls optional analysis deps at package load. `hfdl.lab` / `from
    # hfdatalibrary import lab` both work.
    if name == "lab":
        import importlib
        return importlib.import_module("hfdatalibrary.lab")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
