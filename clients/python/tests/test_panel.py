"""load_panel: pivots many tickers, skips 404s honestly (client RAISES, not None),
filters dates, and attaches a CoverageReport using the real source column."""
import pandas as pd
import pytest

from hfdatalibrary.lab import load_panel
from hfdatalibrary.client import HFDLError


def _ticker_df(start="2022-01-03", periods=200, source="iex", price=100.0):
    idx = pd.date_range(start, periods=periods, freq="B")
    return pd.DataFrame({
        "Date": idx.astype(str),
        "Open": price, "High": price + 1, "Low": price - 1, "Close": price,
        "Volume": 1000, "source": source,
    })


def test_pivots_and_skips_missing():
    data = {"AAA": _ticker_df(price=100), "BBB": _ticker_df(price=50)}

    def getter(t, version="clean", timeframe="daily"):
        if t == "CCC":
            raise HFDLError("Not found: CCC")   # the client raises on 404
        return data[t]

    wide = load_panel(["AAA", "BBB", "CCC"], field="Close", timeframe="daily",
                      cache=False, throttle=0, getter=getter)
    assert list(wide.columns) == ["AAA", "BBB"]
    assert isinstance(wide.index, pd.DatetimeIndex)
    cov = wide.attrs["coverage"]
    assert "CCC" in cov.missing_tickers
    assert cov.iex_segment is True            # source column == 'iex'


def test_date_filter():
    df = _ticker_df(start="2020-01-01", periods=800, source="pitrading")

    def getter(t, **k):
        return df

    wide = load_panel(["AAA"], cache=False, throttle=0, getter=getter,
                      start="2021-01-01", end="2021-12-31")
    assert wide.index.min() >= pd.Timestamp("2021-01-01")
    assert wide.index.max() <= pd.Timestamp("2021-12-31")


def test_all_missing_raises():
    def getter(t, **k):
        raise HFDLError("Not found")

    with pytest.raises(HFDLError):
        load_panel(["X", "Y"], cache=False, throttle=0, getter=getter)
