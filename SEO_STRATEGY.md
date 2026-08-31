# HF Data Library — SEO Strategy & Implementation Log

_Last updated: 2026-06-30. Author: Ahmed Elkassabgi (implementation assisted)._

## The problem (your observation)

> "When I Google **free high frequency equity data** I get hfdatalibrary, but **free high frequency stock data** doesn't bring it up."

This was correct and the root cause is a **vocabulary mismatch**, not an authority or quality problem.

## Diagnosis (grounded in real SERP checks, June 2026)

The site led with **academic phrasing** everywhere — title, H1, hero, meta, social card all said *"Free High-Frequency U.S. **Equity** Data for Research."* The practical words people actually type — **stock, intraday, minute-by-minute, 1-minute, historical, market data, backtesting, TAQ/CRSP alternative** — appeared in **no title or H1**.

Real searches confirmed it:

| Query | hfdatalibrary | Who ranks instead |
|---|---|---|
| free high frequency stock data download | **ranks (~#4)** | FirstRateData, Quora, libguides |
| free high frequency data for finance research | **ranks (~#2)** | (academic term it already owns) |
| free intraday 1-minute stock data historical | **absent** | Kibot, FirstRateData, StockData.org, EODHD |
| free minute-by-minute stock data for research | **absent** | FirstRateData, Marketstack, Kibot, Stooq |
| free TAQ alternative intraday equity data academic | **absent** | WRDS, university library guides, Intrinio |

The one term it wins (`...stock data download`) is precisely the query where **both** academic and practical words coincide — proof that adding the practical vocabulary will unlock the rest.

The fix is **additive**: keep the academic positioning (a low-competition moat the commercial vendors can't match) and weave in the practical lexicon, plus build the structural surfaces (FAQ, TAQ/CRSP page, per-ticker pages) and complete off-page discovery.

## What shipped in this pass (on-page + technical — live in the repo)

**Homepage (`index.html`)**
- Title → `Free 1-Minute Intraday Stock & ETF Data (OHLCV) - HF Data` (was equity-only; now carries stock + intraday + 1-minute + OHLCV; 57 chars).
- H1 → `Free 1-Minute Intraday U.S. Stock & ETF Data` (was the brand name only — wasted the strongest on-page heading).
- Meta description, Open Graph, Twitter cards rewritten with stock/intraday/historical/TAQ-CRSP (≤160 chars).
- JSON-LD upgraded to a `@graph`: enriched **Dataset** (added `isAccessibleForFree`, `includedInDataCatalog`, `measurementTechnique`, `creativeWorkStatus`, `dateModified`, `publisher`, CSV distribution, expanded keywords) + new **Organization**, **WebSite**, and **FAQPage** nodes.
- New visible **FAQ section** (8 Q&As seeded with the exact questions searchers ask) — also feeds the FAQPage markup.
- Link to the new TAQ/CRSP page from the comparison section + footer.

**New page** `pages/taq-crsp-alternative.html` — targets the (low-competition, academic) "free TAQ/CRSP alternative" queries with literal phrasing, an honest comparison table, and explicit caveats (not tick-level; March-2022 IEX source change; survivorship). Added to sitemap + footer.

**All 14 content sub-pages** (data, download, api, tickers, docs, dictionary, versions, code, ai-prompts, cite, stats, license, contact, changelog, issues) — keyword-bearing titles + rewritten meta descriptions + Open Graph/Twitter tags (previously **only the homepage had OG tags**).

**Sitemap** — added `<lastmod>` to every URL and `changefreq=daily` on the pipeline-driven pages (data, download, stats, changelog, home); added the new page.

**Data-accuracy fixes (E-E-A-T) — every number now matches `data/metadata.json`:**
- Academic variables **27 → 25** (homepage hero, data page, **and the `og:image` social card**).
- Cleaning versions **3 → 2** (homepage comparison table; versions page "Three cleaning levels" / "three versions").
- Stale bars-removed **388,559 (0.025%) → 47,533,401 (3.06%)** (docs + versions pages).
- Stale total-bar literals on the versions page → correct raw 1,551,364,273 / clean 1,503,830,872.
- Removed all struck-through `weekly → daily` edit scars and the contradictory "Updated every week."
- Stata sample-code bug fixed (`log_return_sq` defined before it's used).

**Standing-constraint + correctness fixes**
- Removed every remaining **"accompanying paper"** reference (index, versions, cite, HuggingFace README) per the no-working-paper rule — citations now point only to the dataset DOI.
- Added a real **BibTeX** block to the cite page (its description had promised citations it didn't show).
- Fixed a **broken GitHub link**: `CITATION.cff` pointed to `github.com/aelkassabgi/...` (404); the real repo is `github.com/elkassabgi/hfdatalibrary` (verified live). Standardized on the working URL.

## Deferred — build-pipeline work (high value, needs a generator, not hand-editing)

1. **Per-ticker landing pages (~1,391).** The single biggest long-tail opportunity ("AAPL 1-minute historical data", "SPY 1-minute data", …) — this is how FirstRateData ranks. **Deferred deliberately:** the audit's first draft would have stamped "2002–present / 5,847 trading days" on every page, but `data/ticker_meta.json` has no per-ticker dates and many tickers IPO'd later — that would be **fabricated coverage**. The correct build: a script that computes each ticker's **true** first/last date + bar count from the actual data files, renders a page per ticker (sample OHLCV table + ticker-scoped Dataset JSON-LD), and adds them to the sitemap. I can build this generator on request.
2. **Per-index pages** — `sp500-1-minute-data`, `nasdaq100-1-minute-data` listing covered constituents.
3. **Server/build-time render** the ticker rows on `tickers.html` and a baseline stats snapshot on `stats.html` (both are currently JS-only → near-invisible to crawlers).
4. **Automate `lastmod` + Dataset `dateModified`** in the daily build (stamp from `metadata.json data_updated`).
5. **BreadcrumbList JSON-LD** on every sub-page (minor).

## Off-page / discovery — actions for you (highest leverage)

These build the authority + indexing that on-page work alone can't:

1. **Google Search Console** — verify the domain, submit `sitemap.xml`, and run the **Rich Results Test** on the homepage + cite page to confirm the Dataset/FAQ markup parses. (Also the path into **Google Dataset Search**.)
2. **University library guides (LibGuides)** — ask your UCA business/finance librarian to list it as a free, DOI-citable, CC BY 4.0 TAQ/CRSP alternative; then peer libraries. These `.edu` guides own the academic SERPs and are high-authority backlinks.
3. **Kaggle + Hugging Face dataset mirrors** — publish a representative subset that **carries the same caveats** (March-2022 IEX ~2–3% volume; survivorship) with a "full dataset + API at hfdatalibrary.com" pointer. These rank directly and feed Dataset Search.
4. **awesome-quant GitHub list** — open a PR adding it under "Market Data."
5. **re3data / OpenAIRE / DataCite** — complete the registry submissions the cite page lists as pending.
6. **Curated roundups & Q&A** — request inclusion in "free stock data" listicles (QuantPedia, etc.) and answer the evergreen QuantNet/Quora threads (disclosed, genuine) where FirstRateData/Kibot already appear. Cite only the dataset DOI — never the working paper.

## How to measure

Watch Search Console **Performance → Queries** for impressions/clicks appearing on *stock / intraday / 1-minute / minute-by-minute / TAQ alternative* over the next 4–8 weeks (re-crawl + re-rank takes time). The `lastmod`/daily `changefreq` signals should speed re-crawl of the changed pages.
