# hfdatalibrary Partner Toolkit — Build-Ready Implementation Plan

_Lead-architect synthesis. Every load-bearing claim is grounded in the discovery findings and verified repo files; items I could not byte-verify are marked **[UNVERIFIED]** or **[DECISION — Ahmed]**. Date: 2026-06-30._

---

## 1. Architecture summary + tech stack (and why)

**One sentence:** Ship a free Python SDK (`hfdatalibrary`, extended in place) that turns the existing thin REST client into a `load → align → optimize → backtest` recipe layer with **honesty guardrails enforced in code**, delegate the heavy math to mature permissive libraries via **thin adapters** (no bespoke engine), and mirror the same compute, client-side and at ~$0, in a **DuckDB-WASM browser playground** on the static Cloudflare Pages site — with a future `hfdatalibrary.macro` adapter folding in econdatalibrary series.

**Why this shape.** The hard constraint is "compute on the user's machine or browser, never our servers." The repo already does exactly the one cheap thing our servers should keep doing: hand over whole parquet files via `GET /v1/download/{ticker}` (verified in `client.py` and `api/src/index.js` handlers 2133–2217). The SDK is a recipe layer on top of that; it writes **zero numerical kernels** and inherits the test suites of vectorbt/PyPortfolioOpt/bt for free. The SDK-vs-bespoke decision was adjudicated in the SDK DECISION findings: **thin wrapper wins** (lower cost, faster first value, no credibility-landmine hand-rolled optimizers), with three named grafts (signal-aware caveats, permissive default backend, `CaveatWarning` escalation). The playground decision was adjudicated likewise: **DuckDB-WASM wins** over Pyodide (3–4 MB vs tens of MB; sub-second vs 5–15 s cold start; native range-capable parquet reads; no `requests`-in-Pyodide seam), with B's "reproduce in Python" funnel grafted on.

**Stack:**
- **SDK:** Python ≥3.9 (raise floor from current 3.8 — vectorbt/PyPortfolioOpt need ≥3.10, but the **core** stays 3.8-safe; extras dictate the higher floor per-extra), `pandas` + `requests` core only; heavy libs behind extras.
- **Backtest:** `bt` (MIT) as the **default** backend; `vectorbt` (Apache-2.0 **+ Commons Clause**) as an opt-in power-user extra. _Rationale: the SDK DECISION graft — defaulting permissive costs nothing and removes the one license/maintenance hazard; vectorbt's OSS line is feature-frozen behind PRO and its Commons Clause would block any future paid tier._
- **Optimize:** `PyPortfolioOpt` (MIT) primary; `Riskfolio-Lib` (BSD-3) advanced add-on.
- **Browser:** DuckDB-WASM (self-hosted, ~3.2 MB), Web Worker, small JS matrix solve for min-variance. Pyodide deferred (possible later "advanced" tab only).
- **Site:** existing static Cloudflare Pages — copy-a-page convention, shared `css/style.css`, `data-meta` loader, `showTab()` tabs, the `#fef9ee/#fde68a` caveat callout. No framework, no build step.

---

## 2. Repo placement + package name decision

**Package name: extend the existing `hfdatalibrary` package. Do NOT mint a new name.**
Grounded in PyPI findings (DISCOVER #6): `hfdatalibrary` is **not yet on PyPI** (404 — the repo's `pyproject.toml` declares `name="hfdatalibrary"` v0.1.0 but it was never published), so the name is claimable and there is **no install base to fragment**. One import surface, one citation, maximal stickiness. `hfdl` is **unusable** (taken — Hugging Face downloader collision). `hflab`/`hfquant`/`hfdatalib` are free if a punchier brand is ever wanted — keep as fallback, don't use now. **[DECISION — Ahmed]:** confirm "extend `hfdatalibrary` + `lab`/`adapters` subnamespaces" vs separate brand. Recommended: extend.

**SDK layout** (extend in place; flat layout retained to minimize churn — `src/` migration is optional hardening, deferred):
```
clients/python/
  pyproject.toml                 # EDIT: add extras + package list
  hfdatalibrary/
    __init__.py                  # EDIT: lazy `adapters` + `lab` re-export
    client.py                    # UNCHANGED (verified surface)
    lab/                         # NEW — the recipe layer (M0 lives here)
      __init__.py
      _universe.py               # universe() over bundled ticker_meta.json
      _panel.py                  # load_panel() — loop hfdl.get + align + cache + backoff
      _coverage.py               # CoverageReport, the guardrail engine (build FIRST)
      _backtest.py               # backtest_momentum() + BacktestResult
      _data/ticker_meta.json     # bundled copy for offline universe() resolution
    adapters/                    # NEW — thin bridges (M2)
      __init__.py                # public surface + lazy engine imports
      _core.py                   # to_wide / to_returns / Caveats (pandas-only)
      _vectorbt.py  _bt.py  _pypfopt.py  _riskfolio.py
  tests/
    test_lab.py  test_adapters.py  test_coverage.py
```

**Playground / site files** (static, served by Pages at ~$0):
```
pages/toolkit.html      # NEW hub (top-level nav)
pages/playground.html   # NEW in-browser demo
pages/recipes.html      # NEW examples gallery
pages/tutorials.html    # NEW SDK docs
js/toolkit/playground.js          # NEW DuckDB-WASM driver (Web Worker)
js/toolkit/duckdb-wasm/*          # NEW self-hosted ~3.2 MB bundle
data/playground/sp500_daily_close.parquet   # NEW bundled demo asset (also the SDK cache asset)
assets/og-toolkit.svg             # NEW social card (optional)
# EDITS: index.html (nav+FAQ+card+CTA), 404.html (nav), all pages/*.html (nav),
#        sitemap.xml, _headers (add 'wasm-unsafe-eval'), README.md
```

---

## 3. Phased roadmap (M0 … M5)

### M0 — The first killer example + minimal SDK surface  ← **BUILD THIS FIRST**
**Deliverable:** one copy-pasteable script/notebook — _"Momentum portfolio on the S&P 500, min-variance weighted, backtested 2010→today, with the survivorship & coverage warning printed inline,"_ plus the 3 functions + 1 result object it needs, plus a bundled cache asset so it runs with **zero key**.

**Files to create:**
- `hfdatalibrary/lab/_universe.py` — `lab.universe(name)` (mirror `download.html`'s `loadBundle` filter over `ticker_meta.json` `indices` field; verified 550 S&P names).
- `hfdatalibrary/lab/_panel.py` — `lab.load_panel(...)` (loop `hfdl.get`, long→wide pivot, align, **local parquet cache**, inter-request sleep + `Retry-After` backoff for the **100/min download bucket**, honest 404-skip).
- `hfdatalibrary/lab/_coverage.py` — `CoverageReport` + `_coverage_audit()` (**build before the strategy so it can never be optional**).
- `hfdatalibrary/lab/_backtest.py` — `lab.backtest_momentum(...)` + `BacktestResult` (pandas signal + PyPortfolioOpt min-variance per rebalance + stats + `.plot()`).
- `hfdatalibrary/lab/_data/ticker_meta.json` — bundled copy.
- `data/playground/sp500_daily_close.parquet` — generated cache asset (also the M3 playground demo data).
- `examples/momentum_sp500.ipynb` (+ `.py`) and `momentum_sp500_equity.png`.
- `tests/test_coverage.py`, `tests/test_lab.py`.

**Build order (smallest first, each independently shippable):** `universe()` → `load_panel()` → `_coverage_audit()`/`BacktestResult.warnings` → `backtest_momentum()` → generate cache asset + notebook + figure.

**Acceptance criteria:**
- Runs end-to-end with one free key, **or zero key against the bundled cache**, in seconds.
- Prints `result.stats` including a **pre-2022 vs post-2022 split** (the honesty payoff — shows how much "edge" lives in the survivor-biased window).
- Renders equity-vs-SPY curve; saves PNG.
- Prints **all three** caveat warnings (survivorship, IEX break, coverage), computed from the actual panel, quoting `issues.html`/`docs.html`.
- **Every displayed number is produced by the run — none hard-coded** (research-integrity).
- A long backtest starting before 2022 **refuses to return** unless `acknowledge_survivorship=True` (guardrail SURV-1, §5).

**[DECISION — Ahmed] before locking M0 headline numbers:** strategy params — recommend 6-1 momentum (`lookback=126, skip=21`), `top_n=50`, monthly rebalance, min-variance weighting, `cost_bps=10`. Confirm.

### M1 — Package hardening + publish
**Deliverables:** `pyproject.toml` extras (`vectorbt`, `bt`, `optimize`, `riskfolio`, `all`); add `hfdatalibrary.lab` (+ later `adapters`) to `[tool.setuptools] packages`; classifiers/metadata; `CaveatWarning(UserWarning)` subclass; CI install-test of every extra (settles the **[UNVERIFIED]** version floors); **PyPI Trusted Publishing** (OIDC, `pypa/gh-action-pypi-publish`, `permissions: id-token: write`, no stored tokens) gated on tag pushes; `python -m build` sdist+wheel.
**Acceptance:** `pip install hfdatalibrary` works from PyPI; `pip install hfdatalibrary[optimize]` resolves; M0 example runs from a clean venv; CI green on 3.9–3.13 matrix.

### M2 — Adapters layer
**Deliverables:** `adapters/_core.py` (`to_wide`, `to_returns`, `Caveats` — pandas-only, importable on bare install), `_vectorbt.py`, `_bt.py`, `_pypfopt.py`, `_riskfolio.py`, all lazy-imported; `tests/test_adapters.py`.
**Acceptance:** `from hfdatalibrary.adapters import to_wide, Caveats` works with no heavy deps; each engine helper raises a friendly `HFDLAdapterError` with the right `pip install` hint when its lib is absent; smoke test per lib when extra installed; **signal-aware** IEX-volume firing (price-momentum does NOT trigger the volume caveat; Amihud/turnover/VWAP does — the SDK-DECISION graft fixing the noisy-heuristic bug).

### M3 — Browser playground (DuckDB-WASM)
**Deliverables:** `pages/playground.html`, `js/toolkit/playground.js` (DuckDB-WASM in Web Worker), self-hosted bundle, the bundled demo parquet, the **"reproduce in Python" card** under every demo, the **IEX `source`-split demo** (show the ~97% post-2022 volume drop in SQL), persistent yellow caveat banner + per-run warnings + survivorship **checkbox gate** before Run.
**Acceptance:** anonymous visitor picks tickers + strategy → Run → chart in <1 s after cache, $0 server load; identical momentum/min-var logic and identical caveat text as M0; survivorship checkbox blocks pre-2022 long backtests; served from `hfdatalibrary.com` (CORS allowlisted).

### M4 — Website hub + spokes + SEO
**Deliverables:** `pages/toolkit.html` (hub), `pages/recipes.html`, `pages/tutorials.html`; nav `<li>` "Toolkit" inserted in **all** nav copies (index, 404, every `pages/*.html`), `active` only on toolkit; homepage 4th access-card + hero "Try the Playground" CTA + FAQ Q&A ("Can I backtest with this data?") in visible FAQ **and** `FAQPage` JSON-LD; `sitemap.xml` 4 new URLs; per-page OG/Twitter; `BreadcrumbList` + `SoftwareSourceCode` + `HowTo` JSON-LD; internal-link cluster (Toolkit↔Code↔Download↔Dictionary↔Issues↔Cite).
**Acceptance:** all 4 pages live, nav consistent across all files, sitemap valid, structured data validates, recipe/tutorial text is static-HTML (crawlable), every page carries the caveat callout + "not investment advice."

### M5 — Econ-macro integration (future; see §7)
Gated on the econ R2 CSV data plane coming online (currently honest `502 data_unavailable`). Thin `hfdatalibrary.macro` adapter wrapping `econdl.HttpClient`; regime/conditioning variables with explicit publication-lag alignment.

---

## 4. Exact public SDK API (M0–M1)

**M0 — `hfdatalibrary.lab`:**
```python
lab.universe(name: str) -> list[str]
    # name ∈ {"sp500","nasdaq100","dow30","etf","stock","sector:<X>"}
    # reads bundled ticker_meta.json (indices/type/sector). "sp500" -> 550 tickers (verified).

lab.load_panel(
    tickers: list[str],
    field: str = "Close",            # OHLCV column
    timeframe: str = "daily",        # daily, NOT 1min (browser/RAM scale + correct granularity)
    start: str | None = None,
    end: str | None = None,
    version: str = "clean",          # "clean" | "raw"
    cache: bool = True,
) -> pandas.DataFrame                 # wide: index=DatetimeIndex, cols=ticker; .attrs['coverage'] set

lab.backtest_momentum(
    prices: pandas.DataFrame,
    lookback: int = 126,
    skip: int = 21,
    top_n: int = 50,
    rebalance: str = "M",
    optimize: str = "min_volatility",   # weights via PyPortfolioOpt
    cost_bps: float = 10,
    benchmark: str = "SPY",
    acknowledge_survivorship: bool = False,   # SURV-1 hard-stop opt-in
) -> BacktestResult

class BacktestResult:
    stats: dict        # incl. pre-2022 vs post-2022 split
    equity: pandas.Series
    weights: pandas.DataFrame
    warnings: list[str]
    coverage: CoverageReport
    def plot(self) -> "matplotlib.figure.Figure": ...
```

**M2 — `hfdatalibrary.adapters`** (verified input: `hfdl.get` returns a `DataFrame` for one ticker or `{ticker: DataFrame}` for many):
```python
to_wide(data, field="Close", tickers=None, dropna="all") -> tuple[DataFrame, Caveats]
to_returns(prices, kind="simple") -> DataFrame
# engine helpers (lazy; raise HFDLAdapterError if lib missing):
to_vectorbt(data, field="Close", dropna="all") -> tuple[DataFrame, Caveats]
run_momentum(data, lookback=20, hold=5, top_n=1, freq="1D", init_cash=100_000.0) -> tuple[Portfolio, Caveats]
to_bt_prices(data, field="Close", dropna="all") -> tuple[DataFrame, Caveats]
backtest_weights(data, weigh="inv_vol", name="hfdl_demo") -> tuple[Result, Caveats]
to_pypfopt_prices(data, field="Close") -> tuple[DataFrame, Caveats]
min_variance(data, weight_bounds=(0,1)) -> tuple[dict, tuple, Caveats]
to_riskfolio_returns(data, field="Close", kind="simple") -> tuple[DataFrame, Caveats]
```
**Existing, verified, unchanged:** `hfdl.set_key`, `hfdl.symbols()`, `hfdl.get(ticker, version="clean", timeframe="1min", fmt="parquet")`; `X-API-Key` auth; `VERSIONS=("clean","raw")`, `TIMEFRAMES=("1min","5min","15min","30min","hourly","daily","weekly","monthly")`.

**Note (verified correction):** the existing client does **no** inter-request throttling on multi-ticker `get([...])` — it's a plain loop. `load_panel` must add the inter-request sleep + backoff itself for the 100/min download bucket.

---

## 5. Guardrails spec (the credibility core — loud-never-silent)

**Fixed constants (verified):** `IEX_BREAK = 2022-03-01`; `SURVIVORSHIP_SAFE_FROM = 2022-01-01` (docs: "2022-onward… close to point-in-time"); severe before 2022 (docs: "survivorship-biased, most severely before 2022").

**Single carrier of truth:** every data call attaches a `CoverageReport` (on `df.attrs['coverage']`, on `result.coverage`) **and returns caveats as a value** so they cannot be silently dropped. Computed from the **data actually pulled** — inspects the real `source` column (`pitrading` vs `iex`) and the real date index, so it fires only for the user's specific window and detects per-ticker coverage drops.

**Three delivery channels:** (1) `df.attrs`/`result.coverage`; (2) mandatory return value; (3) `warnings.warn(msg, CaveatWarning)` re-emitted by every high-level helper. A teaching/CI setting can `warnings.simplefilter("error", CaveatWarning)` to make caveats hard failures.

**The guards:**
| Guard | Fires when | Severity |
|---|---|---|
| Survivorship — severe | long/cumulative exposure with effective start < 2022-01-01 | **hard stop (SURV-1)** |
| Survivorship — partial | window straddles 2022-01-01 | warn |
| IEX volume | `end ≥ 2022-03-01` **AND** `signal_uses_volume` | warn |
| IEX segment (no volume signal) | `end ≥ 2022-03-01`, volume not used | info |
| Per-ticker out-of-coverage | window extends before ticker `first_bar` / after `last_bar` | warn + trim (recorded, never silent) |
| No-IEX-activity ticker | post-break `iex_coverage_ratio < 0.5` **[DECISION: threshold]** | warn |
| Raw-version + volume signal | `version="raw"` + volume signal | warn |

**Rule SURV-1 (hard stop):** a long/buy-hold/cumulative backtest with effective start < 2022-01-01 **raises `SurvivorshipBiasError`** unless `acknowledge_survivorship=True` (browser: a ticked checkbox). The acknowledgment is recorded in the report (`SURV_ACK` with timestamp) so a reproduced study shows the bias was knowingly accepted. Short-only / dollar-neutral / single-day / from-2022 constructions skip the stop. _Why a hard stop only here: a forgotten kwarg can silently publish an inflated long-run Sharpe; IEX-volume results stay interpretable, so those remain loud warnings._

**Signal-awareness (graft):** strategies/optimizers set `signal_uses_volume`; price-momentum and `min_variance` never trigger the volume caveat; Amihud/turnover/VWAP always do. (Replaces the noisy `signal in {...,"momentum"}` heuristic.)

**Per-ticker coverage — BLOCKER artifact (highest-priority dependency).** `metadata.json` has only corpus-wide dates + quintiles; `ticker_meta.json` has only name/type/sector/indices — **neither holds per-ticker first/last bar dates or per-ticker IEX coverage** (verified). The guard needs a small static `data/ticker_coverage.json` (`first_bar`, `last_bar`, `iex_days`, `iex_expected` per ticker), cheap to generate from the daily pipeline (min/max bar date + count of post-break days with ≥1 bar) and shipped as a Pages asset the browser can read too. **Fallback order, never silent:** (1) the index; (2) compute from the bars actually pulled (`source="computed_from_bars"`, honestly labeled — can't know existence before the window); (3) aggregate fallback (`status="unknown"`, explicit "coverage may be overstated"). **[DECISION — Ahmed]:** generate `data/ticker_coverage.json`.

**Point-in-time:** `point_in_time_safe = (start ≥ 2022-01-01)`; pre-2022 cross-sectional screens are never marketed as point-in-time. Backtest helpers enforce ≥1-bar signal→execution lag by default, recorded in the report.

**Verbatim caveat text** is quoted from `pages/issues.html` (IEX lines 67/76/81/85; per-ticker 82; `source` 105) and `pages/docs.html` (survivorship 178/181–182), so SDK, website, and playground never drift. "Research/education only. Not investment advice." on every result and page.

---

## 6. Website / playground build steps

1. **Copy `pages/code.html`** (it already has the tab component + `showTab()`) → `pages/toolkit.html`, `pages/playground.html`, `pages/recipes.html`, `pages/tutorials.html`. Keep its `<head>`, nav, dark hero, footer.
2. **Self-host DuckDB-WASM** under `js/toolkit/duckdb-wasm/` (~3.2 MB; first-party keeps CSP clean). Driver `js/toolkit/playground.js` runs DuckDB in a **Web Worker**; momentum = SQL window functions; min-variance = build covariance in SQL, solve the small k×k system in JS (no Pyodide/scipy).
3. **Bundle demo data** `data/playground/sp500_daily_close.parquet` (a few MB, daily, liquid post-2022 names) — the M0 cache asset doubles as the anonymous "try it now" source. _This is mandatory: `handleDownloadToken` requires a verified logged-in user (verified lines 2102–2105), so an anonymous live path is impossible — both engines must use a bundled static file._
4. **CSP:** edit `_headers` — add `'wasm-unsafe-eval'` to `script-src` (`'unsafe-eval'` already present; `connect-src 'self'` already covers fetching the bundled parquet). No new hosts if self-hosted.
5. **Nav:** insert one `<li>` "Toolkit" (between Code and AI Prompts) into **every** nav copy — root `href="pages/toolkit"`, subpages `href="toolkit"`, 404 `/pages/toolkit`; `active` only on toolkit.html. Playground/Recipes/Tutorials live in the Toolkit hub + footer, not top-level (avoids overflowing the bar that collapses at ≤1120px). **[DECISION — Ahmed]:** optionally relocate "Stats" to the footer.
6. **Homepage:** add 4th card to the access `grid-3`→`grid-4`; hero `btn-gold` "Try the Playground"; FAQ Q&A in visible FAQ + `FAQPage` JSON-LD.
7. **Honesty UI:** persistent yellow callout (`#fef9ee/#fde68a`) above every result; per-run warnings computed from the loaded panel; survivorship **checkbox** gating Run for pre-2022 long backtests; "reproduce in Python" SDK snippet under each demo (B's adoption funnel at ~$0); IEX `source`-split demo as the headline honesty example.
8. **SEO:** per-page keyword titles/meta/OG, `BreadcrumbList` + `SoftwareSourceCode` (toolkit) + `HowTo` (flagship recipe + install) JSON-LD, sitemap entries, static-HTML recipe/tutorial text, internal-link cluster. Targets the "backtesting" query cluster the site doesn't yet rank for, funneling authority back to the dataset DOI.
9. **Deploy:** existing `wrangler pages deploy . --project-name=hfdatalibrary` on push to `main` (`.github/workflows/deploy.yml`) — no change needed.

**[UNVERIFIED]:** the exact CSP additions for DuckDB-WASM should be tested in-browser; R2 has no public CORS endpoint today, so live R2 range-streaming is not available (bundled-file path sidesteps it). Adding `Range`/`Accept-Ranges` to `handleDownload` is a one-function future upgrade to unlock column-pruning over full files — not a v1 blocker.

---

## 7. Future econ-macro phase (M5)

Verified contract (econfindatalibrary, live Worker `econdl-api.elkassabgi.workers.dev`): **no auth**, CORS `*` (browser-callable), Python client `econdl` (stdlib `urllib`, not `requests`), series-id is colon-delimited `provider:INDICATOR:GEO`. **The CSV data plane is NOT live yet** — `.csv` returns honest `502 data_unavailable`; metadata/catalog/sources/last-updates ARE live. So M5 is gated on the econ R2 cutover.

**Design (when live):** thin `hfdatalibrary.macro` adapter wrapping `econdl.HttpClient` (base URL configurable — public domain `econdatalibrary.com` is **[UNVERIFIED]** as live; use `workers.dev` for now). Flow: resolve/pin `series_id`s via `search()` → `fetch_series_csv(id, from=, to=)` → align macro (monthly/quarterly/annual) onto the hf trading calendar with an **explicit publication-lag/as-of rule** (no look-ahead; the adapter owns this so users can't leak future macro prints). Reproducibility: pin **both** the hf Zenodo DOI (`10.5281/zenodo.19501605`) and an `econdl` `datapackage.json` lockfile. Carry econdl's loud-never-silent semantics through to the user. Recipes page marks the regime-conditioned backtest "coming with the econ integration" — do not imply it's live.

---

## 8. Cost statement + licensing

**~$0 ongoing infra — proof.** All compute runs on the user's laptop (SDK) or in their browser tab (playground). Our servers keep doing the one cheap thing they already do — hand over parquet files via the existing metered Worker + R2. New site assets (HTML, JS, the ~3.2 MB WASM bundle, the few-MB demo parquet) are **static files on Cloudflare Pages**, served by CDN at ~$0. The anonymous playground hits **zero** API/R2 (bundled demo file). No hosted backtesting, no per-user compute, no new always-on service. The only marginal load is users pulling their own data — already metered (300/min general, 100/min download) and unchanged by this project.

**Licensing.** SDK code MIT (matches current `pyproject.toml`); data CC BY 4.0. Extras: `bt` MIT, `PyPortfolioOpt` MIT, `Riskfolio-Lib` BSD-3 — all clean/permissive and the **defaults**. `vectorbt` is **Apache-2.0 + Commons Clause** (fair-code, **not** OSI open source): fine to depend on for a free toolkit, but it forbids selling a product whose value derives primarily from it — so it's an **opt-in extra, never the default backend**, which keeps a future paid tier unencumbered. **[DECISION — Ahmed]:** this only matters if a paid tier is ever contemplated; flagged for awareness.

---

## 9. Risks + mitigations

| Risk | Mitigation |
|---|---|
| **Silent survivorship-biased backtest** (the classic trap; credibility-killer for an academic project) | SURV-1 hard stop on pre-2022 long/cumulative backtests; acknowledgment recorded; pre/post-2022 stat split makes the bias visible. |
| **Per-ticker coverage artifact doesn't exist** (verified gap) | Generate `data/ticker_coverage.json` (cheap, daily pipeline); honest 3-tier fallback until then, never silently using corpus-wide dates per ticker. |
| **Heavy-lib dependency break** (vectorbt PRO-first/feature-frozen; backtrader dead) | All heavy libs behind extras — a break degrades an opt-in path, never the core; permissive `bt` is the default; avoid backtrader (GPLv3, Python ≤3.7, no release since 2023) and zipline-reloaded (heavy bundle ingest) as primaries. |
| **Documented-but-nonexistent endpoints** (`/v1/variables`, `/quality`, `/bars/.../daily`, `/bulk`, per-bar JSON pagination — verified absent) | Build only on the whole-file `/v1/download/{ticker}` path; `screen()`/the 25 variables computed locally from bars and labeled "toolkit-computed"; never imply the official 25-variable file is being read. **[UNVERIFIED]** where the 25 variables live — flag to Ahmed before promising variable access. |
| **PyPI name unpublished / `pip install` promised prematurely** | Publish via Trusted Publishing in M1; until then label install "from the repo today, PyPI pending." |
| **Browser memory ceiling** (wasm32 4 GiB, ~2 GiB effective, no spill) | Scope playground to a few tickers + daily bars; bundled demo only; never full-universe minute history in-tab. |
| **CORS** blocks playground | Serve from `hfdatalibrary.com` (allowlisted); bundled-parquet demo has no CORS dependency; add any new origin to `ALLOWED_ORIGINS` + redeploy if a live path is added. |
| **Unverified version floors** for heavy libs | CI install-test in M1 before pinning exact versions; floors currently from PyPI metadata, not a local install. |
| **Nav overflow** (11th top-level link) | Only "Toolkit" is top-level; spokes in hub+footer; optionally move "Stats" to footer. |
| **Daily aggregates may not exist for all 550 names [UNVERIFIED]** | `load_panel` degrades honestly (skip + warn on 404), never crashes. |

---

## 10. START HERE — the very first files and commands

**The single first thing to build is M0's guardrail, then the data layer, then the strategy — in this order, because the guardrail must exist before any result can be produced.**

1. **Create the recipe package skeleton:**
   - `clients/python/hfdatalibrary/lab/__init__.py`
   - `clients/python/hfdatalibrary/lab/_coverage.py` ← **write this first** (`CoverageReport`, `_coverage_audit`, `SurvivorshipBiasError`, `CaveatWarning`, the SURV-1 hard stop). Constants: `IEX_BREAK=2022-03-01`, `SURVIVORSHIP_SAFE_FROM=2022-01-01`.
   - `clients/python/hfdatalibrary/lab/_universe.py` (`universe()` over a bundled `ticker_meta.json`).
   - `clients/python/hfdatalibrary/lab/_panel.py` (`load_panel()` — loop `hfdl.get`, long→wide, align, cache, backoff, 404-skip).
   - `clients/python/hfdatalibrary/lab/_backtest.py` (`backtest_momentum()` + `BacktestResult`, PyPortfolioOpt min-variance).
2. **Bundle universe data:** copy `data/ticker_meta.json` → `clients/python/hfdatalibrary/lab/_data/ticker_meta.json`.
3. **Wire lazy re-export** in `clients/python/hfdatalibrary/__init__.py` (`__getattr__` for `lab` and `adapters` so heavy/optional code never imports at package load).
4. **First commands** (Git Bash / PowerShell; create a branch — do not commit on `main` until asked):
   ```
   git checkout -b feat/partner-toolkit-m0
   cd clients/python && python -m venv .venv && . .venv/Scripts/activate
   pip install -e .[optimize]      # PyPortfolioOpt for the min-variance solve
   pip install matplotlib pyarrow  # plotting + parquet
   ```
5. **Generate the cache/demo asset** with a one-off script that pulls 550 S&P daily closes (respecting 100/min backoff) → `data/playground/sp500_daily_close.parquet`, so both the M0 example and the M3 playground run with zero key.
6. **Write the example** `clients/python/examples/momentum_sp500.ipynb` and run it; confirm the three warnings print, the pre/post-2022 split shows, and **no number is hard-coded**.
7. **Tests:** `clients/python/tests/test_coverage.py` (SURV-1 raises without ack; `source="iex"` rows set IEX flags; missing ticker → `missing_tickers`) and `tests/test_lab.py` (pivot shape, DatetimeIndex, 404-skip).

**Blockers to raise with Ahmed before/at M0:** (1) confirm extend-`hfdatalibrary` + `lab` namespace; (2) confirm M0 strategy params (126/21/top-50/monthly/min-var/10bps); (3) approve shipping the bundled `sp500_daily_close.parquet`; (4) **generate `data/ticker_coverage.json`** (highest-priority data dependency for accurate per-ticker guards); (5) resolve where the 25 academic variables live (precomputed R2 file / planned endpoint / SDK-computed) before any recipe promises variable access.

**Key anchor files (all absolute):** `D:\research\hfdatalibrary\clients\python\hfdatalibrary\client.py`, `D:\research\hfdatalibrary\clients\python\pyproject.toml`, `D:\research\hfdatalibrary\clients\python\hfdatalibrary\__init__.py`, `D:\research\hfdatalibrary\data\ticker_meta.json`, `D:\research\hfdatalibrary\data\metadata.json`, `D:\research\hfdatalibrary\pages\download.html` (loadBundle + token flow), `D:\research\hfdatalibrary\pages\issues.html` + `D:\research\hfdatalibrary\pages\docs.html` (verbatim caveats), `D:\research\hfdatalibrary\pages\code.html` (page skeleton), `D:\research\hfdatalibrary\css\style.css`, `D:\research\hfdatalibrary\js\site.js`, `D:\research\hfdatalibrary\_headers`, `D:\research\hfdatalibrary\api\src\index.js` (CORS 43–47; auth gates 2102–2105; whole-file download 2133–2217), `D:\research\hfdatalibrary\sitemap.xml`, `D:\research\hfdatalibrary\PARTNER_TOOLKIT_PLAN.md`, and for M5 `D:\research\econfindatalibrary\clients\python\econdl\`.

---

## 11. Adversarial review — corrections to fold into M0 (reviewer held approval pending these)

The plan above passed factual verification; the reviewer (`approved:false`) required these fixes before M0 is "build-ready." All are now part of the spec:

1. **`universe()` coverage honesty (major).** Index helpers resolve to *survivor-snapshot* counts, not nominal index size: **sp500=550, nasdaq100=81 (of 100), dow30=30** (verified from `ticker_meta.json`). `universe()` must emit a one-line coverage note per index (e.g. "nasdaq100: 81 of 100 constituents present; 19 absent from the ~2023 survivor snapshot"), and tests must pin the **real** counts, never the nominal index size.
2. **Split M0 (major).** **M0a** ships `universe`/`load_panel`/`backtest_momentum` with the two caveats computable **today** (survivorship via the date index; IEX via the real `source` column — both verified present). Per-ticker out-of-coverage trimming is **gated behind `data/ticker_coverage.json`** (does not exist yet) with the honest `computed_from_bars`/`unknown` fallback — it is NOT a hard M0a acceptance criterion until the artifact exists.
3. **25 variables (major).** No `/v1/variables` endpoint and no bundled variable file is verified to exist. Any variable the SDK produces must be labeled **"toolkit-computed from 1-minute bars, may differ from official precomputed values"**; never imply the official 25-variable file is retrieved. Resolve with Ahmed where the 25 vars live before any recipe/playground references them (pre-M4 blocker).
4. **Browser specifics (minor).** DuckDB-WASM "~3.2 MB" is **[UNVERIFIED]** — measure the self-hosted bundle in M3. Min-variance JS solver: cap k (≤30), add shrinkage/conditioning or a long-only QP fallback; on numerical failure surface an honest error, never emit garbage weights.
5. **M5 econdl surface (minor).** Verified surface is module-level `econdl.search()/fetch()/bundle()/pull()` and `HttpClient.fetch_series_csv(series_id, fmt=...)`; date-window/as-of params are **[UNVERIFIED — confirm when the CSV plane is live]** (the earlier `from=/to=` was invented).
6. **`load_panel` 404 handling (minor).** The existing client **raises `HFDLError` on 404** (it does NOT return `None`) and does **no** inter-request throttling. `load_panel` must `try/except HFDLError` per ticker, record misses in `coverage.missing_tickers`, continue, and add an inter-request sleep to stay under the 100/min download limit.

**Net:** M0a (no new data artifacts needed) is fully buildable now; M0 items that need data Ahmed must provide — the bundled demo parquet (needs an API key to generate), `data/ticker_coverage.json`, and the 25-variable location — are explicitly gated, not assumed.