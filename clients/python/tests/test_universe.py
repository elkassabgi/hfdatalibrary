"""Universe counts must match the REAL survivor-snapshot, never the nominal index
size (reviewer fix #1). Verified from data/ticker_meta.json: sp500=550,
nasdaq100=81, dow30=30, etf=569, stock=822, total=1391."""
import warnings

import pytest

from hfdatalibrary.lab import universe, index_coverage, CaveatWarning


def test_sp500_count():
    assert len(universe("sp500")) == 550


def test_dow30_count():
    assert len(universe("dow30")) == 30


def test_nasdaq100_count_and_warns():
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        u = universe("nasdaq100")
    assert len(u) == 81
    assert any(issubclass(w.category, CaveatWarning) and "81 of 100" in str(w.message)
               for w in rec), "nasdaq100 must warn about the 81-of-100 survivorship gap"


def test_type_counts():
    assert len(universe("etf")) == 569
    assert len(universe("stock")) == 822


def test_all_is_full_snapshot():
    assert len(universe("all")) == 1391


def test_sector():
    assert len(universe("sector:Healthcare")) > 0


def test_unknown_raises():
    with pytest.raises(ValueError):
        universe("bogus_index")


def test_index_coverage_diagnostic():
    cov = index_coverage()
    assert cov["nasdaq100"] == {"present": 81, "nominal": 100}
    assert cov["dow30"] == {"present": 30, "nominal": 30}
