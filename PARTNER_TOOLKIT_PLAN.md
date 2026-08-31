# Partner Toolkit — Plan & Discussion Notes

_Captured 2026-06-30. Status: proposal agreed in principle; not yet built. Revisit after the econ R2 cutover._

## Goal
A "partner software" that lets traders/researchers use hfdatalibrary data for **strategy
testing, portfolio optimization, and similar analysis** — eventually folding in
econdatalibrary macro data. Hard constraint from Ahmed: **lowest cost, highest return.**

## Core decision: toolkit, NOT a hosted platform
The deciding question is *where the compute runs*:

- **Hosted platform** (users run backtests on our servers) = unbounded **compute** cost, real
  liability (edges toward investment advice), and competing with QuantConnect/Backtrader. ❌
- **Toolkit that runs on the user's own machine / in their browser** = cost stays ~$0 no matter
  how many users or how heavy their analysis; we stay the trusted *data + tools* provider. ✅

Analogy used: **recipe book vs restaurant.** A restaurant pays per customer; a recipe book is
written once and copied infinitely, with the cooking done in the user's kitchen. Our servers
keep doing the one cheap thing they already do — hand over data files.

**Decision: build the toolkit (local + browser), never host the compute.**

## What it is (two pieces)
1. **A free Python package** (`pip install`, name TBD — e.g. `hflab`). Turns fiddly
   data-wrangling into 2–3 readable lines. Runs on the user's laptop.
2. **An in-browser "playground"** on the website (DuckDB-WASM / Pyodide — already floated in
   HOSTING_RESEARCH_REPORT). Visitor picks tickers + a strategy, clicks Run, sees a chart —
   all client-side, costing us nothing. This is the visible website upgrade / shop window.

## Three layers
1. **SDK core** — extend the existing Python client into load → align → optimize → backtest,
   pulling bulk **parquet** (not looping the API per bar).
2. **Adapters, not a new engine** — first-class data connectors so the data "just works" in the
   tools quants already trust: **vectorbt**, **backtrader**, **zipline-reloaded** for
   backtesting; **PyPortfolioOpt** / **Riskfolio-Lib** for optimization. "Bring our data to
   your stack" beats "learn our platform."
3. **Browser playground** — a few canned strategies (momentum, pairs, min-variance) running in
   the visitor's browser.

## The differentiators
- The **25 pre-computed academic variables** (realized vol, Roll/Corwin-Schultz spreads,
  Amihud, BNS jumps) as first-class citizens → factor + microstructure research nobody else
  offers free.
- **Later: econdatalibrary macro series** as conditioning/regime variables (e.g. backtest
  segmented by inflation/rates regime). The hf-×-econ combination is the long-term moat.

## CRITICAL honesty guardrails (protect academic credibility)
The data has three properties that make naïve backtests misleading — the toolkit must bake in
warnings, not bury them in docs:
1. **Survivorship bias** — the 1,391-ticker universe is a recent snapshot, so pre-2022 long
   backtests are optimistically biased (the classic backtesting trap).
2. **1-minute bars, not tick/quotes** — no realistic sub-minute fills/slippage; fine for
   research and medium-frequency, wrong for execution-level claims. Never call it "tick data."
3. **Post-March-2022 = IEX (~2–3% of consolidated volume)** — volume-based signals (VWAP,
   Amihud) unreliable on the recent segment.

Turn these into a feature: *"the backtesting toolkit that won't let you fool yourself"* — print
a survivorship/coverage banner on results, expose a `coverage_warning` flag, point-in-time
guards. Also: position as research/education, **not** trade signals or investment advice
(disclaimers); third-party mirrors (Kaggle/HF) must carry the same caveats.

## Practical examples (target API, names TBD)
```python
import hflab
aapl = hflab.load("AAPL", version="clean", timeframe="daily")          # data in one line

result = hflab.backtest("momentum", universe="sp500", start="2010")     # test a strategy
result.plot(); print(result.stats)   # + prints survivorship/coverage warning

port = hflab.optimize(["AAPL","MSFT","XOM","JNJ","JPM"], goal="min_volatility")  # portfolio

calm = hflab.screen(date="2024-06-03", low="amihud_illiquidity", low2="bns_jumps")  # use the 25 vars

result = hflab.backtest("momentum", regime="inflation_rising")          # LATER: + econ macro
```

## Cheapest first step (highest return per effort)
**One polished, reproducible example + the few SDK functions it needs:** "momentum portfolio on
the S&P 500, optimized and backtested 2010→today, with the survivorship warning shown." It
proves the design, doubles as marketing, and becomes the first browser-playground demo — at ~$0
infra.

## Open decision for Ahmed (before building)
- **Audience tilt:** researchers/students (reproducible notebooks, teaching) vs working
  traders/quants (strategy + portfolio backtests)? Ahmed (2026-06-30) said he wants the
  *trader/strategy-testing/portfolio-optimization* use case, emphasizing lowest cost + highest
  return — so lean practitioner, but keep the academic guardrails central.
- **Surface order:** SDK + one killer example first (recommended), browser playground second,
  hosted compute never.

## Why the return is high
More citations (tools → DOI cites), stickiness (workflows built on the data), credibility ("free
data with proper, honest tools" — no competitor has this), and a "try it now" website draw.
