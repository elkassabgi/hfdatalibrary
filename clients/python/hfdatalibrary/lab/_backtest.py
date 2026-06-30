"""backtest_momentum - a transparent, no-look-ahead cross-sectional momentum
backtest with the dataset's honesty guardrails enforced.

Deliberately small and readable (this is the M0 reference example). The heavy
backtest/optimization engines (bt, vectorbt, Riskfolio) arrive as opt-in
adapters in M2; here we keep a pandas implementation a reader can audit.

Guardrails: a cumulative long backtest whose effective start is before 2022-01-01
RAISES SurvivorshipBiasError unless acknowledge_survivorship=True (rule SURV-1).
Momentum is price-only, so the IEX *volume* caveat does not fire (signal-aware).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ._coverage import CoverageReport, audit_panel, enforce_survivorship


@dataclass
class BacktestResult:
    stats: dict
    equity: object                      # pandas.Series
    weights: object                     # pandas.DataFrame (rebalance dates x tickers)
    coverage: CoverageReport
    warnings: list = field(default_factory=list)
    params: dict = field(default_factory=dict)

    def __str__(self) -> str:
        lines = ["BacktestResult:"]
        for k, v in self.stats.items():
            lines.append(f"  {k:>22}: {v:.4f}" if isinstance(v, float) else f"  {k:>22}: {v}")
        for w in self.warnings:
            lines.append(f"  [warn] {w}")
        lines.append(f"  {self.coverage.__class__.__module__.split('.')[0]}: "
                     "research/education only, not investment advice")
        return "\n".join(lines)

    def plot(self):
        """Equity curve vs benchmark. Needs matplotlib (pip install matplotlib)."""
        try:
            import matplotlib.pyplot as plt
        except ImportError as e:
            raise RuntimeError("matplotlib is required for .plot(): pip install matplotlib") from e
        fig, ax = plt.subplots(figsize=(10, 5))
        self.equity.plot(ax=ax, label="strategy")
        bench = self.params.get("_benchmark_equity")
        if bench is not None:
            bench.plot(ax=ax, label=self.params.get("benchmark", "benchmark"), alpha=0.7)
        ax.set_title("Momentum portfolio vs benchmark (survivorship-biased pre-2022)")
        ax.set_ylabel("growth of $1")
        ax.legend()
        return fig


def _stats(returns, periods_per_year=252):
    """Summary stats from a daily return Series."""
    import numpy as np
    r = returns.dropna()
    if len(r) == 0:
        return {"n_days": 0}
    growth = float((1 + r).prod())
    years = len(r) / periods_per_year
    cagr = growth ** (1 / years) - 1 if years > 0 and growth > 0 else float("nan")
    vol = float(r.std() * np.sqrt(periods_per_year))
    sharpe = float(r.mean() / r.std() * np.sqrt(periods_per_year)) if r.std() > 0 else float("nan")
    equity = (1 + r).cumprod()
    mdd = float((equity / equity.cummax() - 1).min())
    return {"total_return": growth - 1, "cagr": cagr, "ann_vol": vol,
            "sharpe": sharpe, "max_drawdown": mdd, "n_days": int(len(r))}


def _weights_for(selected, ret_window, optimize):
    """Return a dict {ticker: weight} for the selected names."""
    import numpy as np
    n = len(selected)
    if n == 0:
        return {}
    if optimize == "equal" or n < 2:
        return {t: 1.0 / n for t in selected}
    if optimize == "inverse_vol":
        vol = ret_window[selected].std().replace(0, np.nan)
        inv = (1.0 / vol).fillna(0.0)
        if inv.sum() == 0:
            return {t: 1.0 / n for t in selected}
        w = inv / inv.sum()
        return {t: float(w[t]) for t in selected}
    if optimize == "min_volatility":
        try:
            from pypfopt import EfficientFrontier, risk_models
            S = risk_models.CovarianceShrinkage(ret_window[selected], returns_data=True).ledoit_wolf()
            ef = EfficientFrontier(None, S, weight_bounds=(0, 1))
            ef.min_volatility()
            w = ef.clean_weights()
            tot = sum(w.values())
            if tot <= 0:
                return {t: 1.0 / n for t in selected}
            return {t: w[t] / tot for t in selected}
        except Exception:
            # singular cov / pypfopt absent -> honest fallback, never garbage weights
            return {t: 1.0 / n for t in selected}
    raise ValueError(f"unknown optimize={optimize!r}; use min_volatility|inverse_vol|equal")


def backtest_momentum(prices, lookback=126, skip=21, top_n=50, rebalance="M",
                      optimize="min_volatility", cost_bps=10.0, benchmark="SPY",
                      acknowledge_survivorship=False):
    """Cross-sectional momentum, long-only, periodic rebalance.

    Signal at each rebalance date uses only past prices (return over the window
    ending ``skip`` days earlier); weights are computed from the trailing return
    window; positions are executed the NEXT day (no look-ahead).
    """
    import numpy as np
    import pandas as pd

    prices = prices.sort_index()
    # guardrail FIRST — a long/cumulative backtest is the survivorship-trap case
    cov = prices.attrs.get("coverage") if hasattr(prices, "attrs") else None
    if not isinstance(cov, CoverageReport):
        cov = audit_panel(prices, uses_volume=False)
    cov.uses_volume = False  # momentum is price-only -> no IEX-volume caveat
    enforce_survivorship(cov, acknowledge=acknowledge_survivorship, cumulative=True)

    bench_prices = prices[benchmark] if benchmark in prices.columns else None
    tradable = prices.drop(columns=[benchmark]) if benchmark in prices.columns else prices

    rets = tradable.pct_change()
    # momentum score = price[t-skip] / price[t-skip-lookback] - 1  (past-only)
    mom = tradable.shift(skip) / tradable.shift(skip + lookback) - 1.0

    # last trading day of each period (version-agnostic; avoids resample("M") drift)
    freq = {"W": "W", "M": "M", "Q": "Q"}.get(str(rebalance).upper(), "M")
    _periods = pd.Index(tradable.index).to_period(freq)
    rebal_dates = [tradable.index[_periods == p][-1] for p in _periods.unique()]

    weights_rows, target = {}, pd.Series(0.0, index=tradable.columns)
    daily_w = pd.DataFrame(0.0, index=tradable.index, columns=tradable.columns)
    turnover_on = pd.Series(0.0, index=tradable.index)
    prev = pd.Series(0.0, index=tradable.columns)

    for d in rebal_dates:
        score = mom.loc[d].dropna()
        if score.empty:
            continue
        selected = list(score.sort_values(ascending=False).head(top_n).index)
        win = rets.loc[:d].tail(252)
        w = _weights_for(selected, win, optimize)
        target = pd.Series(0.0, index=tradable.columns)
        for t, wt in w.items():
            target[t] = wt
        weights_rows[d] = target.copy()
        # execute the day AFTER the signal date (no look-ahead)
        future = tradable.index[tradable.index > d]
        if len(future) == 0:
            continue
        eff = future[0]
        daily_w.loc[daily_w.index >= eff] = target.values
        turnover_on.loc[eff] += float((target - prev).abs().sum())
        prev = target

    # portfolio daily return = yesterday's weights applied to today's returns, net of costs
    port_ret = (daily_w.shift(1) * rets).sum(axis=1)
    port_ret = port_ret - turnover_on.reindex(port_ret.index).fillna(0.0) * (cost_bps / 1e4)
    port_ret = port_ret.loc[port_ret.ne(0).idxmax():] if port_ret.ne(0).any() else port_ret
    equity = (1 + port_ret).cumprod()

    stats = _stats(port_ret)
    # the honesty payoff: how much "edge" lives in the survivor-biased pre-2022 window
    split = pd.Timestamp("2022-01-01")
    stats["pre2022"] = _stats(port_ret.loc[port_ret.index < split])
    stats["post2022"] = _stats(port_ret.loc[port_ret.index >= split])

    bench_equity = None
    if bench_prices is not None:
        b = bench_prices.pct_change().reindex(port_ret.index)
        bench_equity = (1 + b.fillna(0)).cumprod()
        stats["benchmark_total_return"] = float(bench_equity.iloc[-1] - 1) if len(bench_equity) else float("nan")

    weights_df = pd.DataFrame(weights_rows).T if weights_rows else pd.DataFrame()
    return BacktestResult(
        stats=stats, equity=equity, weights=weights_df, coverage=cov,
        warnings=list(cov.warnings),
        params={"lookback": lookback, "skip": skip, "top_n": top_n,
                "rebalance": rebalance, "optimize": optimize, "cost_bps": cost_bps,
                "benchmark": benchmark, "acknowledged": acknowledge_survivorship,
                "_benchmark_equity": bench_equity},
    )
