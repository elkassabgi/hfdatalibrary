"""hfdatalibrary.lab - the recipe layer on top of the thin REST client.

Turns the raw data into ``universe -> load_panel -> backtest`` recipes with the
dataset's honesty guardrails (survivorship, IEX source break, 1-minute-not-tick)
enforced in code. See TOOLKIT_IMPLEMENTATION_PLAN.md.

Quick start::

    import hfdatalibrary as hfdl
    hfdl.set_key("YOUR_KEY")
    tickers = hfdl.lab.universe("sp500")          # 550 (survivor snapshot)
    # M0a: load_panel() / backtest_momentum() land next.

Status: M0a code complete (synthetic-tested). ``universe``, ``load_panel``,
``backtest_momentum``, and the guardrail engine (``audit_panel`` /
``enforce_survivorship``) are live. Pending an API key: generating the bundled
demo parquet and running the example notebook against live data.
"""
from __future__ import annotations

from ._coverage import (
    CoverageReport,
    CaveatWarning,
    SurvivorshipBiasError,
    audit_panel,
    enforce_survivorship,
    IEX_BREAK,
    SURVIVORSHIP_SAFE_FROM,
    DISCLAIMER,
)
from ._universe import universe, index_coverage
from ._panel import load_panel
from ._backtest import backtest_momentum, BacktestResult

__all__ = [
    "universe",
    "index_coverage",
    "load_panel",
    "backtest_momentum",
    "BacktestResult",
    "CoverageReport",
    "CaveatWarning",
    "SurvivorshipBiasError",
    "audit_panel",
    "enforce_survivorship",
    "IEX_BREAK",
    "SURVIVORSHIP_SAFE_FROM",
    "DISCLAIMER",
]
