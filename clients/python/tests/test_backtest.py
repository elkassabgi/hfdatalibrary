"""backtest_momentum: runs no-look-ahead, enforces the survivorship hard stop,
and reports the pre/post-2022 split. Uses synthetic random-walk prices (no key)."""
import warnings

import numpy as np
import pandas as pd
import pytest

from hfdatalibrary.lab import backtest_momentum, BacktestResult, SurvivorshipBiasError


def _panel(start, periods, n=20, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=periods, freq="B")
    rets = rng.normal(0.0003, 0.01, size=(periods, n))
    prices = 100 * np.exp(np.cumsum(rets, axis=0))
    cols = [f"T{i:02d}" for i in range(n - 1)] + ["SPY"]
    return pd.DataFrame(prices, index=idx, columns=cols)


def test_post2022_runs_with_split_and_benchmark():
    p = _panel("2022-02-01", 400)
    res = backtest_momentum(p, lookback=60, skip=5, top_n=5, optimize="equal",
                            benchmark="SPY")
    assert isinstance(res, BacktestResult)
    assert len(res.equity) > 0
    for k in ("total_return", "sharpe", "max_drawdown", "ann_vol"):
        assert k in res.stats
    assert "pre2022" in res.stats and "post2022" in res.stats
    assert "benchmark_total_return" in res.stats


def test_pre2022_hard_stops_without_ack():
    p = _panel("2018-01-01", 800)
    with pytest.raises(SurvivorshipBiasError):
        backtest_momentum(p, lookback=60, skip=5, top_n=5, optimize="equal")


def test_pre2022_runs_with_ack_and_records():
    p = _panel("2018-01-01", 800)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = backtest_momentum(p, lookback=60, skip=5, top_n=5, optimize="equal",
                                acknowledge_survivorship=True)
    assert res.coverage.acknowledged
    assert len(res.equity) > 0


def test_no_lookahead_equity_is_clean():
    p = _panel("2022-02-01", 300)
    res = backtest_momentum(p, lookback=40, skip=3, top_n=4, optimize="equal")
    assert res.equity.notna().all()
    # momentum is price-only -> the IEX *volume* caveat must NOT be raised
    assert not any("IEX VOLUME" in w for w in res.warnings)


def test_inverse_vol_weighting_runs():
    p = _panel("2022-02-01", 300, seed=3)
    res = backtest_momentum(p, lookback=40, skip=3, top_n=6, optimize="inverse_vol")
    assert len(res.weights) > 0
