# HF Data Library — Website Plan
## hfdatalibrary.com

**Author:** Ahmed Elkassabgi, University of Central Arkansas
**Date:** April 9, 2026

---

## 1. Mission

Provide free, research-grade, high-frequency U.S. equity data to academics and researchers worldwide. Fully documented, automatically updated, and permanently citable.

---

## 2. Infrastructure

| Component | Service | Purpose | Cost |
|-----------|---------|---------|------|
| Static site | Cloudflare Pages | Landing page, docs, all HTML/CSS/JS | Free |
| Data storage | Cloudflare R2 | Parquet files, bulk downloads | ~$2/mo (storage only, zero egress) |
| API | Cloudflare Workers | REST endpoints for programmatic access | Free tier (100K req/day) |
| Database | Cloudflare D1 | User accounts, API keys, usage analytics | Free tier |
| CI/CD | GitHub Actions | Weekly data pipeline, site deployment | Free for public repos |
| DNS/CDN | Cloudflare | Already registered, automatic | Free |

**Year 1 estimate:** ~$4.50/month
**Year 5 estimate:** ~$12.83/month (with data growth)
**Egress cost:** $0 (R2's critical advantage — no bandwidth fees regardless of download volume)

---

## 3. Site Structure

### Pages

| Page | URL | Content |
|------|-----|---------|
| Landing | `/` | Hero, stats, value proposition, feature overview |
| Data Overview | `/data` | Ticker search, coverage map, quality summary |
| Documentation | `/docs` | Methodology, cleaning pipeline, data sources |
| Data Dictionary | `/docs/dictionary` | All columns, types, descriptions, computed variables |
| Data Versions | `/docs/versions` | Raw / Clean / Filled — what each is, who should use which |
| API Reference | `/docs/api` | Interactive endpoint docs, request/response examples |
| Download | `/download` | Browser downloads, bulk packages, API key registration |
| Sample Code | `/code` | Python, R, Stata examples, Jupyter notebooks |
| Citation | `/cite` | BibTeX, Chicago, APA formats, one-click copy |
| License | `/license` | CC BY 4.0 full text, terms, attribution requirements |
| Known Issues | `/docs/issues` | Living errata log (like CRSP) |
| Changelog | `/docs/changelog` | Every data update versioned and documented |
| Contact | `/contact` | Ahmed Elkassabgi, UCA, ORCID, email |

---

## 4. Data Access Methods

### 4.1 Browser Downloads
- Individual ticker files (parquet)
- Pre-packaged bundles:
  - S&P 500, Nasdaq 100, Russell 2000
  - By sector (Technology, Healthcare, Financials, etc.)
  - By liquidity quintile (Q1–Q5)
- Full dataset dump (all 1,391 tickers, all versions)
- All served from R2 — no egress cost regardless of volume

### 4.2 REST API
```
Base URL: https://api.hfdatalibrary.com/v1

GET  /symbols                          List all tickers
GET  /symbols/{ticker}                 Ticker metadata (dates, source, coverage)
GET  /bars/{ticker}                    1-min bars (query params: start, end, version, format)
GET  /bars/{ticker}/daily              Daily aggregated bars
GET  /download/{ticker}                Full history download (parquet)
GET  /variables/{ticker}               27 computed academic variables
GET  /quality/{ticker}                 Data quality report
GET  /bulk/{package}                   Pre-packaged bundle download
```

- Authentication: API key (free registration)
- Rate limit: 300 requests/minute per key
- Response formats: JSON, CSV, parquet
- Powered by Cloudflare Workers (auto-scaling, global edge)

### 4.3 Python Package (future)
```python
import hfdatalibrary as hfd
hfd.set_key("your-api-key")
df = hfd.bars("AAPL", start="2020-01-01", version="clean")
```

---

## 5. Data Versions

| Version | Description | Bars | Best For |
|---------|-------------|------|----------|
| **Raw** | As received, no modifications | 1,533,403,126 | Microstructure research, missingness studies |
| **Clean** | 9-step pipeline applied, gaps preserved | 1,533,014,567 | Most empirical finance research |
| **Filled** | Clean + LOCF gap-filling, 390-bar daily grid | 2,342,519,726 | ML, backtesting, time-series models |

Each version available in 8 timeframes: 1-min, 5-min, 15-min, 30-min, hourly, daily, weekly, monthly.

---

## 6. Documentation Requirements

### On-site documentation
- **Data Dictionary:** Every column, type, description, computed variable formula
- **Methodology:** 9-step cleaning pipeline with exact parameters
- **Source Documentation:** PiTrading (CTA/UTP consolidated tape, pre-2022), IEX Exchange HIST (post-2022)
- **Adjustment Methods:** Split and dividend adjustment procedures
- **Quality Metrics:** Gap rates, outlier rates, coverage by quintile
- **API Reference:** Every endpoint, parameters, response schemas, error codes, code examples
- **Known Issues Log:** Living errata — every discovered issue documented with date and resolution
- **Changelog:** Every data update with version number, date, what changed

### Downloadable documentation
- `DATA_DICTIONARY.md` — included with every download
- `METHODOLOGY.md` — full pipeline description
- `README.md` — quick start guide
- `CHANGELOG.md` — version history

---

## 7. Licensing

### CC BY 4.0 (Creative Commons Attribution 4.0 International)

**Permits:**
- Commercial and non-commercial use
- Redistribution
- Derivative works
- No geographic or field-of-use restrictions

**Requires:**
- Attribution to Ahmed Elkassabgi / HF Data Library
- Indication of any modifications

**Why CC BY 4.0:**
- Standard for academic datasets
- Maximizes adoption (no NC restriction that blocks industry researchers)
- Far less restrictive than CRSP/TAQ (competitive advantage)
- Compatible with Zenodo, DataCite, and all major repositories

### Files to create
- `LICENSE` — full CC BY 4.0 legal text
- `LICENSE-SUMMARY.md` — plain-language summary
- License badge and link on every page of the site
- License metadata embedded in Zenodo deposit

---

## 8. Official Registration & Discovery

### Required registrations (in order of priority)

| Platform | Purpose | What it provides | URL |
|----------|---------|------------------|-----|
| **Zenodo** | Archival + DOI | Permanent DOI for each data version; citable in papers | zenodo.org |
| **DataCite** | DOI metadata | Linked through Zenodo; makes DOI discoverable | datacite.org |
| **Google Dataset Search** | Discovery | Researchers find the dataset via Google; requires JSON-LD metadata on site | datasetsearch.research.google.com |
| **re3data.org** | Repository registry | Listed as an official research data repository | re3data.org |
| **ICPSR** | Social science archive | Discoverable by political science, economics, sociology researchers | icpsr.umich.edu |
| **OpenAIRE** | European open science | Discoverable by EU-funded researchers (auto-indexed from Zenodo) | openaire.eu |
| **SSRN** | Paper-dataset link | Link the JFEc paper to the dataset | ssrn.com |
| **Papers With Code** | ML/finance discovery | Dataset listed alongside benchmark tasks | paperswithcode.com |
| **GitHub** | Code + CITATION.cff | Machine-readable citation; GitHub shows "Cite this repository" button | github.com |
| **ORCID** | Author profile | Dataset listed as research output on Ahmed's ORCID profile | orcid.org |

### Structured metadata for Google Dataset Search
JSON-LD `<script>` tag on every data page:
```json
{
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "HF Data Library",
  "description": "Free high-frequency (1-minute) OHLCV data for 1,391 U.S. equities and ETFs, 2002-present",
  "url": "https://hfdatalibrary.com",
  "license": "https://creativecommons.org/licenses/by/4.0/",
  "creator": {
    "@type": "Person",
    "name": "Ahmed Elkassabgi",
    "affiliation": "University of Central Arkansas",
    "identifier": "https://orcid.org/0000-0002-5926-7493"
  },
  "distribution": [
    {
      "@type": "DataDownload",
      "encodingFormat": "application/x-parquet",
      "contentUrl": "https://hfdatalibrary.com/download"
    }
  ],
  "temporalCoverage": "2002-12-30/..",
  "spatialCoverage": "United States",
  "variableMeasured": ["OHLCV", "realized volatility", "bipower variation", "Roll spread", "Amihud illiquidity", "BNS jump statistic"],
  "keywords": ["high-frequency data", "intraday", "OHLCV", "equity", "ETF", "market microstructure", "realized volatility"]
}
```

### CITATION.cff
```yaml
cff-version: 1.2.0
title: "HF Data Library"
message: "If you use this data, please cite it as below."
type: dataset
authors:
  - family-names: Elkassabgi
    given-names: Ahmed
    affiliation: "University of Central Arkansas"
    orcid: "https://orcid.org/0000-0002-5926-7493"
url: "https://hfdatalibrary.com"
license: CC-BY-4.0
version: "1.0"
date-released: "2026-04-09"
keywords:
  - high-frequency data
  - intraday
  - OHLCV
  - equity
  - market microstructure
  - realized volatility
```

---

## 9. Automated Weekly Pipeline

### GitHub Actions workflow (runs every Sunday)

```
Schedule: cron 0 6 * * 0  (6 AM UTC every Sunday)

Steps:
1. Pull latest IEX Exchange HIST pcap files (past week)
2. Parse pcap → 1-minute OHLCV bars
3. Apply 9-step cleaning pipeline
4. Produce Raw, Clean, Filled versions
5. Aggregate to all 8 timeframes
6. Compute 27 academic variables
7. Run quality checks (gap rates, outlier counts, coverage)
8. Upload updated parquet files to R2
9. Update metadata (coverage_report.csv, quality_scores.csv)
10. Update changelog
11. Deploy updated site to Cloudflare Pages
12. Send email notification (success/failure)
```

**Failure handling:**
- If any step fails, pipeline halts and emails Ahmed
- Previous week's data remains live (no partial updates)
- Failed run can be re-triggered manually from GitHub

**No manual intervention required.** Ahmed does nothing unless an alert fires.

---

## 10. Comparison vs. Alternatives

| Feature | HF Data Library | CRSP/TAQ | Yahoo Finance | Polygon.io |
|---------|----------------|----------|---------------|------------|
| Price | **Free** | $25,000+/yr | Free | $199+/mo |
| Frequency | 1-min bars | Tick-level | Daily | 1-min bars |
| Cleaning | 3 versions (documented) | Single version | Undocumented | Undocumented |
| Documentation | Full pipeline + errata | Minimal | None | API docs only |
| Academic variables | 27 pre-computed | None | None | None |
| License | CC BY 4.0 | Restrictive | ToS restricts | Commercial |
| API | Yes (free) | No | Unofficial | Yes (paid) |
| Citation/DOI | Yes (Zenodo) | No | No | No |
| Data quality scores | Yes | No | No | No |

---

## 11. Timeline

| Phase | Tasks | Target |
|-------|-------|--------|
| **Phase 1: Static site** | Landing page, docs, citation, license | This week |
| **Phase 2: Data hosting** | R2 bucket, upload parquet files, download page | Next week |
| **Phase 3: API** | Workers endpoints, D1 user accounts, API keys | Week 3 |
| **Phase 4: Registration** | Zenodo DOI, re3data, Google Dataset Search, ICPSR | Week 4 |
| **Phase 5: Automation** | GitHub Actions weekly pipeline | Week 5 |
| **Phase 6: Python package** | `pip install hfdatalibrary` | Future |

---

## 12. Attribution

Every page, every file, every download includes:

- **Author:** Ahmed Elkassabgi
- **Affiliation:** University of Central Arkansas
- **ORCID:** 0000-0002-5926-7493
- **Contact:** aelkassabgi@uca.edu
- **Paper:** Elkassabgi (2026), "The Sensitivity of High-Frequency Empirical Results to Data Cleaning Methodology," Journal of Financial Econometrics
- **License:** CC BY 4.0

Ahmed Elkassabgi is credited as the sole creator, maintainer, and author across all platforms, files, metadata, and registrations.
