"""M0 flagship example - Momentum portfolio on the S&P 500.

Pulls daily closes for the S&P 500 universe (+ SPY benchmark), runs a no-look-ahead
cross-sectional momentum strategy with min-variance weights, backtested from 2002
to the present, and shows the survivorship & coverage caveats inline - including
the pre-2022 vs post-2022 split that reveals how much "edge" lives in the
survivor-biased window.

Run (needs HFDL_API_KEY in the environment):
    python examples/momentum_sp500.py

Also writes data/playground/sp500_daily_close.parquet (the bundled demo asset that
lets the example + browser playground run with zero key later).
"""
from __future__ import annotations

import os
import sys

import hfdatalibrary as hfdl  # noqa: F401  (ensures the package imports)
from hfdatalibrary import lab

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))   # repo root
OUT = os.path.join(_ROOT, "data", "playground", "sp500_daily_close.parquet")


def main() -> None:
    if not os.environ.get("HFDL_API_KEY"):
        sys.exit("Set HFDL_API_KEY in the environment first "
                 "(get a free key at https://hfdatalibrary.com/pages/account).")

    tickers = lab.universe("sp500")
    if "SPY" not in tickers:
        tickers = tickers + ["SPY"]          # benchmark (an ETF, not an index member)
    print(f"Pulling daily closes for {len(tickers)} tickers (S&P 500 + SPY)...", flush=True)

    panel = lab.load_panel(tickers, field="Close", timeframe="daily",
                           version="clean", cache=True)
    cov = panel.attrs["coverage"]
    print(f"\nPanel: {panel.shape[0]} days x {panel.shape[1]} tickers, "
          f"{panel.index.min().date()} -> {panel.index.max().date()}")
    print(cov)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    to_save = panel.copy()
    to_save.attrs = {}   # the CoverageReport object isn't JSON-serializable for parquet metadata
    to_save.to_parquet(OUT)
    print(f"\nSaved demo asset: {OUT}  ({os.path.getsize(OUT)/1e6:.1f} MB)")

    res = lab.backtest_momentum(
        panel, lookback=126, skip=21, top_n=50, rebalance="M",
        optimize="min_volatility", cost_bps=10, benchmark="SPY",
        acknowledge_survivorship=True,        # full history is pre-2022 -> show the honest split
    )

    s = res.stats
    print("\n=== Momentum S&P 500 (6-1, top-50, monthly, min-variance, 10bps) ===")
    print(f"FULL  : return {s['total_return']:.1%}  CAGR {s['cagr']:.1%}  "
          f"Sharpe {s['sharpe']:.2f}  maxDD {s['max_drawdown']:.1%}  ({s['n_days']} days)")
    pre, post = s["pre2022"], s["post2022"]
    print(f"  pre-2022  (survivor-biased): CAGR {pre.get('cagr', float('nan')):.1%}  "
          f"Sharpe {pre.get('sharpe', float('nan')):.2f}  ({pre.get('n_days', 0)} days)")
    print(f"  post-2022 (point-in-time)  : CAGR {post.get('cagr', float('nan')):.1%}  "
          f"Sharpe {post.get('sharpe', float('nan')):.2f}  ({post.get('n_days', 0)} days)")
    print(f"  SPY buy-hold total return  : {s.get('benchmark_total_return', float('nan')):.1%}")
    for w in res.warnings:
        print(f"  [caveat] {w}")
    print(f"  optimize backend used: {res.params['optimize']}")

    try:
        fig = res.plot()
        png = os.path.join(_HERE, "momentum_sp500_equity.png")
        fig.savefig(png, dpi=120, bbox_inches="tight")
        print(f"\nSaved figure: {png}")
    except Exception as e:  # matplotlib optional
        print(f"\n(plot skipped: {e})")


if __name__ == "__main__":
    main()
