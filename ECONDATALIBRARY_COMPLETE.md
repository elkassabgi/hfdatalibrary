# econdatalibrary — The Complete System Document

**Prepared for Ahmed Elkassabgi · 30 August 2026**

---

## What this document is

You asked for one file covering: what the econ website does and how it works; what I have done and
how; every mistake in the ledger; the problems I am seeing and the repeat mistakes I make trying to
fix them; the assumptions I make and how they turn out false; every updating mechanism; and
anything else you did not think to name.

This is that file. It is long because the honest version is long.

## How it was assembled, and how much to trust each part

Different parts of this document have different evidence behind them, and you should know which is
which.

* **Measured** — a number with a named instrument (a command, a query, a tool). These are
  reproducible. Where a figure came from a shipped tool, the tool is named.
* **Read from code** — a statement about how something works, established by reading the file that
  implements it. The file path is given.
* **Quoted** — a figure taken from the ledger or `NUMBERS.md` with its original date, where I did
  not re-measure it. These are labelled as quoted, not as current measurements.
* **NOT ESTABLISHED** — appears wherever something could not be verified. It is not an oversight;
  it is the honest state.

Nine sections were researched in parallel by separate agents working read-only against the real
repositories, then reviewed by a completeness critic whose only job was to find what was missing.
Two sections (what I did, and the problems/repeats) are first-hand.

**One caution that applies throughout.** Several figures in `NUMBERS.md` were *retracted in the same
file* after being found wrong — some of them within hours, in this session. Where a figure was
withdrawn, this document reports the corrected value and says that an earlier one was withdrawn. If
you find a number here that contradicts something I told you earlier today, the number here is the
later one and the discrepancy is deliberate.

## The system in one paragraph

`econdatalibrary.com` is an open academic data library: a catalogue of economic and financial time
series that anyone can browse, search, and download, free, with an API and Python and R clients. The
data is collected from official publishers — the IMF, Eurostat, UNCTAD, FAO, the World Bank,
national statistics offices, central banks and others — normalised into one shape, and hosted on
Cloudflare. A registry of **282 sources** drives an updater that refreshes them on a schedule and
records what it did; a health gate judges whether each source is current; and the whole library is
served from a catalogue of roughly **13.5 million series** (measured 2026-08-30 by
`tools/audit_schedule_coverage.py`) held in an **11.91 GB** local database mirrored into Cloudflare
D1, with the data itself in object storage.

## The shape of the code

Measured 2026-08-30 by directory listing in `E:/research/econfindatalibrary`:

| | count |
|---|---|
| commits in the econ repository | 1,480 |
| registry entries (`updater/registry.yaml`) | 282 |
| scheduled fetchers (`updater/strategies/fetchers/*.py`) | 296 |
| ingest jobs (`jobs/*.py`) | 212 |
| operational tools (`tools/*.py`) | 174 |
| test modules (`tests/*.py`) | 96 |
| per-source runbooks (`docs/runbook/*.md`) | 379 |
| GitHub Actions workflows | 8 |
| worker source files (`api/worker/src/*.ts`) | 15 |
| rows in the catalogue's `source` table | 349 |
| rows in the catalogue's `license` table | 71 |
| mistake-ledger entries / digest lines | 124 / 337 |

## Contents

**PART I — THE SYSTEM**

- [1. What the system is and what it does](#1-what-the-system-is-and-what-it-does)
- [2. How the serving side works](#2-how-the-serving-side-works)
- [3. How data gets in and becomes servable](#3-how-data-gets-in-and-becomes-servable)
- [4. Every updating mechanism](#4-every-updating-mechanism)

**PART II — THE RECORD**

- [5. What I have done, and how](#5-what-i-have-done-and-how)
- [6. The mistake ledger, part 1 (R0–R300)](#6-the-mistake-ledger-part-1-r0r300)
- [7. The mistake ledger, part 2 (R301 onward)](#7-the-mistake-ledger-part-2-r301-onward)

**PART III — THE ANALYSIS**

- [8. The repeating patterns](#8-the-repeating-patterns)
- [9. The problems, and the repeat mistakes](#9-the-problems-and-the-repeat-mistakes)

**PART IV — REFERENCE**

- [10. The current state](#10-the-current-state)
- [11. Glossary](#11-glossary)


---

# PART I — THE SYSTEM

## 1. What the system is and what it does

*Every claim in this section comes from a file read or a command run on 2026-08-30 against
`E:/research/econfindatalibrary` (the econ repo), `D:/research/hfdatalibrary` (the hf repo, which
also holds the mistake ledger), and the local read-only copies of `data/catalog.db` and
`data/_aqueduct/state.db`. Where a figure is quoted from the ledger or `NUMBERS.md` rather than
re-measured, it is labelled **QUOTED** with its original date. Where something could not be
established, it says **NOT ESTABLISHED** and what would establish it.*

---

### 1.1 What econdatalibrary.com is

`econdatalibrary.com` is a free, open, academic library of economic and financial **time series**.
It is not an aggregator front-end that redirects you to somebody else's download page: it holds
copies of the data, on its own storage, and serves them itself — but only for data whose publisher's
licence permits that.

The formal self-description is in the repository's own citation metadata
(`E:/research/econfindatalibrary/CITATION.cff` and `.zenodo.json`), which are the record a
researcher cites:

```yaml
# E:/research/econfindatalibrary/CITATION.cff
cff-version: 1.2.0
doi: 10.5281/zenodo.21405120
title: "Economic Data Library: Free Economic and Financial Data"
type: dataset
authors:
  - family-names: Elkassabgi
    given-names: Ahmed
    affiliation: "University of Central Arkansas"
    orcid: "https://orcid.org/0000-0002-5926-7493"
url: "https://econdatalibrary.com"
license: CC-BY-4.0
version: "1.0"
date-released: "2026-07-16"
```

Two facts about ownership and licence that shape everything else:

* **The code and the data are licensed separately, deliberately.** `README.md` states it plainly:
  the updater, connectors, Worker API, tools and clients are **MIT**; the data is **not** covered by
  that licence, "and deliberately so. Every dataset stays under the terms of the statistical agency
  that published it." The library "asserts no blanket licence over the data it redistributes and
  cannot grant one: the rights belong to the publishers."
* **The library-level DOI is `10.5281/zenodo.21405120`**, version 1.0, released 2026-07-16. The
  site's `cite.html` renders that DOI plus a BibTeX block, and tells the user that the *series-level*
  citation — the one that names the actual statistical agency — ships inside every download.

A third fact, which is an operating rule rather than a licence but defines what the library *is*:
**every source comes from its own publisher, never from an aggregator.** `E:/research/econfindatalibrary/CLAUDE.md`
opens with it — *"Do not fetch from DBnomics … Every source must come from ITS OWN PUBLISHER"* — and
gives the reason in measurable terms: 98 of the 101 datasets ever taken from that aggregator had not
been re-indexed in over 180 days (UNCTAD: 1,581 days), and because the freshness signal was the
aggregator's own hash, a frozen dataset would report "no change" for ever while the health gate saw a
source succeeding every day. Three relay fetchers (`who_hwf`, `who_rs`, `who_sdg`) are the last
survivors and are marked for migration, not refresh. The rule is enforced by a PreToolUse hook that
denies any command reaching the domain, plus a CI test that fails any push reintroducing it.

The public site carries a standing honesty banner on its homepage
(`catalog/site/index.html`), which is worth reading verbatim because it sets the expectation the
rest of the system is built to keep:

> 🚧 **Under Construction** — the Econ Data Library is being finalized. Datasets and their licensing
> are still being verified and may change. **Automated updates are being wired database by
> database**: a source joins the daily refresh only once its updater has been built and proven
> against the publisher. Until then its data is the verified initial load, not pretended current —
> the Source Status board says which is which, per database, live.

#### The two halves of the running system

| half | what it is | where it lives |
|---|---|---|
| **The site** | A **static** website — 333 HTML files, generated by `catalog/gen_site.py` from the catalogue database, with no server-side rendering. Served by the Cloudflare Pages project `econdatalibrary`. | `E:/research/econfindatalibrary/catalog/site/` |
| **The API** | A single Cloudflare Worker named `econdl-api`, 3,134 lines of TypeScript across 15 files, reading Cloudflare D1 (the catalogue) and Cloudflare R2 (the data objects). | `E:/research/econfindatalibrary/api/worker/src/` |

Both halves are **published by hand**, and this is a trap worth naming up front because it has bitten
before. `docs/DATABASE_REFERENCE.md` records that the Pages project `econdatalibrary` is a *manual
full-directory-snapshot* publish ("Git Provider: No", verified 2026-07-18) — **pushing to GitHub
publishes nothing** — and `.claude/skills/econ-updater/references/00-architecture.md` records that a
grep of all seven GitHub workflows for `wrangler deploy` returns **zero matches**, so the Worker too
is invisible until published by hand. A code change in either half is not a change to the running
system until somebody runs the command and checks the live URL.

The Worker's bindings, read from `api/worker/wrangler.toml`, are the whole serving surface:

```toml
name = "econdl-api"
[[d1_databases]]  binding = "CATALOG"          database_name = "econ-catalog"
[[d1_databases]]  binding = "CATALOG_CLIMATE"  database_name = "econ-catalog-climate"   # noaa shard
[[d1_databases]]  binding = "USERS"            database_name = "hfdatalibrary-db"       # SHARED identity
[[r2_buckets]]    binding = "SERIES_BUCKET"    bucket_name  = "econ-data"
```

Note the third one: **the user database is hfdatalibrary's**. That is the mechanism behind "one
account, every library" and is covered in §1.4.

#### The reference documents the repo already carries

Worth knowing they exist, because they answer most operational questions without a new
investigation:

| file | what it is |
|---|---|
| `DATABASE_LICENSES_VERBATIM.md` (648 KB) | The canonical, verbatim licence audit. 88 providers, 191 databases. Its header says: *"This is the single source of truth. Do NOT re-derive it from scratch."* |
| `REDISTRIBUTION_COMPLIANCE.md` | The permissions paper trail — every grant and every refusal, with the verbatim wording and the obligation each creates. |
| `docs/DATABASE_REFERENCE.md` (3,259 lines) | The incident-reference manual: every served source, its endpoints, the code that serves it, its cloud wiring, its known failure modes. Marked per-section with whether an independent adversarial pass verified it. |
| `docs/runbook/*.md` (**379 files**) | One generated page per source: real state, adapter contract, a six-step DIAGNOSE section with runnable commands, and the ledger entries about that specific source. Regenerated by `python tools/gen_runbook.py --with-store`; never hand-edited. |
| `api/CONTRACT.md` | The public API contract — one contract, three implementations (Worker, Python dev shim, `econdl` client), enforced by a pytest conformance test. |
| `ARCHITECTURE.md` | The storage decision record: one canonical long table, a global `PROVIDER/DATASET/SERIES` key, plain partitioned parquet, and an explicit "do NOT build" list. |
| `D:/research/hfdatalibrary/.claude/MISTAKES.md` and `NUMBERS.md` | The mistake ledger and the figures ledger. Every headline number and the command that measured it. |

---

### 1.2 What a user can actually do

#### 1.2.1 The pages

The site is exactly 333 HTML files. Twelve are the product; 321 are one landing page per data
source. (Verified by listing `catalog/site/*.html` and differencing against the 321 keys of
`catalog/site/sources_meta.json`.)

| page | what the visitor does there |
|---|---|
| `index.html` | Landing page. Live counters (series / observations / as-of date fetched from `/v1/stats`), six "pillar" tiles, a comparison table against FRED / DBnomics / Bloomberg, an FAQ. |
| `catalog.html` | **Browse all 321 sources.** Filter chips by pillar, dropdowns for topic and region, free-text search over name / id / licence / topic. |
| `download.html` | **The download workbench.** Search the series catalogue, tick series, download one CSV or a multi-series **ZIP assembled in the browser**. Carries an API-key bar that stores the key in `localStorage` only. |
| `api.html` | REST reference: base URL, endpoint table, which endpoints need a key, curl and Python quick-start. |
| `docs.html` | Methodology: the id grammar, the licensing rule, reproducibility, the update pipeline, the six languages. |
| `mcp.html` | Setup guide for connecting an AI assistant (Claude app, Claude Code, Cursor, Antigravity CLI, ChatGPT) to the MCP server. |
| `cite.html` | Producer-first citation guidance + the library DOI, BibTeX and reproducibility note. |
| `stats.html` | Live usage: registered users, downloads, data served, a two-tone world map (users vs visitors), most-downloaded sources, institutions represented. |
| `status.html` | **Per-database freshness board**, rendered live from `/v1/last-updates`. |
| `account.html` | Sign-in (popup SSO to `accounts.elkassabgidata.com`, or an email/password fallback form), API key display, personal MCP connect URL. |
| `contact.html` | Contact form. |
| `404.html` | Not-found page. |

**The per-source pages are substantial, not stubs.** Reading `catalog/site/worldbank_wdi.html`, a
single source page carries: the source id and name; access badges ("Open · redistributed"); the
licence name; category tags; a description; a **Coverage** block (series catalogued, temporal
coverage, frequencies, categories, measured observations); a **Licensing & provenance** block
(licence, licence URL, required attribution text, redistribution verdict, commercial-use flag,
modification flag, provider homepage, provider terms URL); a **How to cite** line; a **Processing**
note describing exactly what the pipeline did to the raw source; an **Access & mirrors** block
(download link, API instructions, canonical landing URL); the **update cadence**, whether automated
refresh is live, the **update strategy**, and the **storage layout** down to the parquet schema; and
the fetcher's full description. For `worldbank_wdi` those read, respectively: 1,486 series
catalogued, 1960-12-31 – 2025-12-31, Annual, 8,894,931 measured observations, CC BY 4.0, quarterly
cadence, `bulk snapshot if changed`.

#### 1.2.2 The REST API

Base URL, from `catalog/site/api.html`: `https://econdl-api.elkassabgi.workers.dev`

The routes are enumerated in `api/worker/src/index.ts` and specified in `api/CONTRACT.md`.

| route | returns | key needed? |
|---|---|---|
| `GET /v1/catalog` | Series search/browse. Params `q`, `source`, `limit` (max 500), `offset` (capped at 100,000), `lang`. FTS5 primary path with a LIKE fallback. Edge-cached 6 h. | No |
| `GET /v1/series/{id}.csv` | The series as long CSV `series_id,obs_date,value`, preceded by a `#`-commented citation header. Params `from`, `to`, `format`, `geo`, `raw=1`. | **Yes** |
| `GET /v1/series/{id}.metadata.json` | Title, frequency, geography, unit, coverage, licence block (incl. commercial-use flag), attribution, citation, freshness, csv_url. | No |
| `GET /v1/sources` | Every source with licence + freshness summary (`status`, `last_updated`, `cadence`). | No |
| `GET /v1/last-updates` | Per-dataset freshness, projected from `unit_state` + `source_state` + registry cadence. | No |
| `GET /v1/bundle` | A Frictionless `datapackage.json` **manifest** (never a server-built zip) with per-resource stable URLs, provenance, licences, and an `econdl:unresolved[]` list. Params `ids=` or `source=`, `snapshot=`. | No |
| `GET /v1/stats` | Store-measured headline counts, read from an R2 object, plus a live catalogue count. | No |
| `GET /v1/public-stats` | Family usage aggregates (users, downloads, countries, institutions). No PII. | No |
| `GET /v1/pv`, `/v1/pv/report` | Page-view beacon and its report. | No |
| `GET /`, `/v1` | Service description + endpoint list. | No |

**The access rule, stated in `api/worker/src/auth.ts`:** *"only DATA downloads are gated
(`/v1/series/{id}.csv`). Catalog, search, metadata, freshness, stats and the status page stay open —
browse free, download with the (free) family key."*

**The honesty rules are enforced in code, not just documented.** `CONTRACT.md` pins them and
`api/worker/src/series.ts` implements the decision tree:

```
1. id not in catalog                              -> 404 not_found
2. source has no at-rest resolver                 -> 501 not_migrated
3. unsupported dimension filter (freq=, unit=)    -> 400 unsupported_filter
4. R2 object absent (source migrated, not derived)-> 502 data_unavailable
5. object present but 0 rows in the date window   -> 502 resolver_empty
6. >= 1 row                                       -> 200 text/csv
   and, before any of the above, licence gate     -> 451 not_redistributable
```

`series.ts` states the reason in its own header: *"NEVER an empty 200, NEVER a fabricated series."*
A requested filter the store cannot honour returns 400 rather than a silently-unfiltered 200.

**The same rule applies to metadata fields that do not exist yet.** `api/worker/src/metadata.ts`
records that the contract's richer human-context fields — `description_key`,
`description_processing`, `citation_short`, `citation_long` — come from a series-tier metadata pass
that has not been run (*"verified: 0 series carry them"*), so they are **omitted, never faked**. The
handler surfaces the per-series `description` and producer `citation` strings that some series do
carry, under clearly-named keys. `obs_count` is likewise omitted from `metadata.json`, because
producing it would mean reading the parquet rows; the `.csv` body is the source of truth for the row
count.

**One stale string in that surface.** Every `/v1/catalog` response carries a `catalog_coverage`
field, whose purpose is to stop a caller reading absence as non-existence. Its value is hard-coded in
`api/worker/src/catalog.ts:19` as `"series-level for 33 sources; source-level for the rest"`. The
same constant appears in `api/CONTRACT.md`. **33 has not been the number for a long time** — the
Worker's own resolver allowlist holds 324 ids and the served count is 322. The field is emitted on
every catalogue response (`catalog.ts:218`), so this is a user-visible sentence that understates the
library by an order of magnitude. It is a one-line fix, and it is exactly the class of stale
compiled-in count that `/v1/stats` was deliberately rebuilt to avoid.

#### 1.2.3 What a downloaded CSV actually contains

This is one of the system's defining features and it is worth showing. `api/worker/src/series.ts`
builds a citation header and prepends it to every `.csv` unless the caller passes `?raw=1`. Lines
start with `#`, so `pandas.read_csv(url, comment='#')` and R's `comment.char='#'` skip them, but
anyone who opens the file sees the attribution first. The shape (from `citationHeader()`):

```
# ============================================================================
#  DATA CITATION — please credit the original source in any use or publication.
#  By downloading from the Elkassabgi Data Library you agreed to cite this source.
#
#  Series:    <title>  [<series_id>]
#  Source:    <the publisher's own required attribution string>
#  License:   <licence name>[ — NON-COMMERCIAL USE ONLY (honor it)][; attribution required]
#             [; SHARE-ALIKE — anything you build from this must carry the same licence]
#             [; NO DERIVATIVES — redistribute verbatim, unmodified]
#  Dataset:   <link back — required by the IDB written permission>
#  Homepage:  <provider homepage>
#  Terms:     <provider terms URL>
#  Cite as:   <producer-first citation>
#  Provided:  Elkassabgi Data Library — econdatalibrary.com
#  (Pipelines: pandas pd.read_csv(url, comment='#'), or append ?raw=1 for bare CSV.)
# ============================================================================
series_id,obs_date,value
...
```

The code comment explains why it exists: *"This is the primary 'attribution travels with the bytes'
mechanism and it is REQUIRED for the sources we re-host by permission (KOF, UN Comtrade, WHR, IEP)."*

#### 1.2.4 Bulk bundles

A "bundle" is a multi-source, reproducible download. `api/worker/src/bundle.ts` returns a
**manifest**, never a zip — the Worker cannot fan out to more than 50 subrequests, so the client
assembles the archive locally. The manifest is a Frictionless `datapackage.json` that doubles as a
lockfile: it pins `econdl:snapshot_date`, per-resource `sha256`, and the licence/attribution/citation
for every source. Unresolvable ids are returned under `econdl:unresolved[]` — "loud, never dropped".

#### 1.2.5 The Python client

`E:/research/econfindatalibrary/clients/python/econdl` — package `econdl`, five API calls
(from `clients/python/README.md`):

| call | does |
|---|---|
| `econdl.search(query, limit=20)` | FTS5 search over the series catalogue |
| `econdl.bundle(series_ids=[...], out=...)` | Tidy DataFrame + `datapackage.json` lockfile + one native-parquet resource per source + a `.zip` |
| `econdl.bundle(source="bls", out=...)` | Bundles every catalogued series of one source the client can resolve |
| `econdl.pull(datapackage)` | **Reproduces the pinned snapshot**, verifying every `sha256`; a corrupted or altered resource raises rather than being returned |
| `econdl.pull(datapackage, latest=True)` | Explicit opt-in to refreshed data; warns loudly on anything it cannot satisfy |
| `econdl.supported_sources()` | Sources with an at-rest resolver today |

#### 1.2.6 The MCP server (AI assistants)

`E:/research/econfindatalibrary/mcp/src/index.ts` (565 lines) is a Cloudflare Worker exposing the
**whole family** to any MCP-capable assistant at
`https://elkassabgidata-mcp.elkassabgi.workers.dev/mcp`. It registers **11 tools, 3 resources and
3 prompts**:

| tool | what it does |
|---|---|
| `search_econ_series` | Search the econ catalogue |
| `get_econ_series` | Download a series as long rows |
| `get_econ_series_metadata` | Full metadata for one series |
| `list_econ_sources` | List the econ sources |
| `get_data_freshness` | Live per-source update status |
| `get_hf_download_link` | Authenticated HF equity download instructions |
| `get_hf_variables_dictionary` | The 25 HF academic-variable definitions |
| `list_ip_bundles` | The IP library's snapshot-pinned bundles |
| `get_ip_download_link` | Authenticated IP bundle download |
| `get_family_status` | Live status across the family |
| `get_auth_status` | Whether this connection has a key |

Resources: `elkassabgidata://honesty` (a data-honesty charter), `elkassabgidata://variables`,
`elkassabgidata://about`. Prompts: `analyze_econ_series`, `compare_countries`, `hf_event_study`.

Its design rules mirror the site: *"BROWSE IS FREE, DOWNLOADS ARE KEYED"*, and *"HONESTY IS LAW:
upstream error messages (401/404/429/501/502) are relayed verbatim … truncation is always disclosed,
never silent."* The user's key passes through per request and is "never stored, logged, or echoed
back into the conversation."

#### 1.2.7 Accounts, rate limits and download logging

* Sign-in is a popup to `accounts.elkassabgidata.com`; the visitor stays on econdatalibrary.com.
  There is an email/password fallback form for pop-up-blocked browsers.
* Two credential types authorise a download (`auth.ts`): a **family access token** (`edl_at`, Bearer,
  stored hashed as `sessions.id`, bound to the request `Origin`, which must be an *active* registered
  SSO client) and an **API key** (`X-API-Key` header or `?api_key=`), validated against
  `users.api_key` with `is_active = 1` and an unexpired `api_key_expires_at`.
* **Rate limit: 100 downloads per minute per account** (`LIMIT_MAX`), 500 for VIP accounts
  (`LIMIT_MAX_VIP`), fixed 60-second window, in the shared `rate_limits` table under the
  `econ:download` namespace. Over the limit is a `429` with `retry-after: 60`.
* Every served download is written to **`econ_download_log`** — a table in the shared DB but separate
  from hf's, "so hf's download counters are never inflated by econ traffic". It records
  `user_id, series_id, ip, channel, bytes`, where `channel` is `mcp` / `web` / `api` derived from the
  `X-Elkassabgi-Client` header, the user-agent, a `?via=mcp` tag, or a family `Referer`.

#### 1.2.8 Internationalisation

`?lang=` is supported on `/v1/catalog` and `/v1/series/{id}.metadata.json` for
**en, ar, es, fr, ru, zh** (`SUPPORTED_LANGS` in `util.ts`). The contract is strict: titles come only
from the producers' own multilingual APIs (World Bank `/v2/<lang>/`, IMF/ILO SDMX `xml:lang`) and are
**never machine-translated**; an unsupported `lang=` returns `400 unsupported_language` rather than
silently falling back to English; and `lang=en` or no `lang` returns byte-for-byte the pre-i18n
response shape.

#### 1.2.9 The public status board

`catalog/site/status.html` is unusual for a data library: it publishes, per database, whether the
data is current — including when it is not. It renders live from `/v1/last-updates`, which projects
`unit_state` + `source_state` + the registry cadence, and its four states are:

* **updated & current** (or an honest `no_change` probe),
* **partial / transient issue** — retries automatically next run,
* **failed or past its expected update window** — "visible here and in the operator's daily report
  until fixed",
* **initial load** — "joins the automated rollout in an upcoming phase".

Its own footer states the guarantee: *"Statuses are never fabricated: a source's date only advances
when new observations were actually fetched and merged, and a silent upstream failure shows up red
here rather than being papered over."*

Two refinements in the page's source are worth knowing because they were added after specific
misreadings:

* Each row shows the source's **title and one-line description alongside the id**, because "the board
  used to print the bare `source_id` (`abs`, `imf_fsi`), which only helps a reader who already knows
  the namespace."
* A latest-period beyond today gets a **PROJECTION badge**, because several publishers legitimately
  ship forecasts (ABS projections to 2046 and 2071, UN WPP to 2101, IMF WEO to 2031) and the column
  previously made "2046-12-31" read as "observed data is current to 2046".

#### 1.2.10 One documented gap in what the site promises

`catalog/site/index.html` and `api.html` both say **"Python and R clients available"**, and
`.zenodo.json` says "Python/R clients". **The econ repo contains only a Python client**
(`clients/python/econdl`); there is no `clients/r` directory. The hf repo *does* have one
(`D:/research/hfdatalibrary/clients/r`). The econ repo's own `STRATEGY.md` build order, step 4, still
reads *"Then R client, the remaining migration stages…"*. **NOT ESTABLISHED:** whether an econ R
client exists anywhere outside this repo. What would establish it: a published CRAN/GitHub package
that calls `econdl-api.elkassabgi.workers.dev`, or an `R/` source tree. As it stands the site claims
a client the repository does not contain.

#### 1.2.11 Machine discoverability

Every source page is machine-readable twice over. `catalog/site/worldbank_wdi.html` carries **two**
`application/ld+json` blocks:

* a **schema.org `Dataset`** record — `name`, `description`, `url`, `identifier`,
  `isAccessibleForFree: true`, `publisher`, `includedInDataCatalog`, `creator` (the *publisher*, e.g.
  "The World Bank", not the library), `license`, `keywords`, `temporalCoverage`
  (`1960-12-31/2025-12-31`), `repeatFrequency` (`P1Y`), `creditText` and `citation`. This is what
  Google Dataset Search reads.
* a **Croissant** record (`conformsTo: http://mlcommons.org/croissant/1.0`) — the ML-community
  dataset metadata standard, which `ARCHITECTURE.md` names as the primary export format.

`catalog/site/sitemap.xml` lists **330 URLs** (`lastmod` 2026-08-24), and `robots.txt` is present.
The homepage carries a Google site-verification meta tag, canonical URL, Open Graph and Twitter card
metadata.

This matters for the library's purpose: it is how a researcher who has never heard of
econdatalibrary.com finds a series through a search engine rather than through the site.

#### 1.2.12 What the site claims publicly, and whether the claim holds

These are the promises made in Ahmed's name on the live homepage. Each is checked against the repo
or the catalogue database.

| public claim (from `catalog/site/index.html`) | verdict |
|---|---|
| "321 Sources" | **HOLDS.** 321 source landing pages, 321 keys in `sources_meta.json`, 321 entries in the browse index. |
| "2,000+ Years of History … the earliest catalogued series (Maddison Project / GGDC) begin in year 1 CE" | **HOLDS, measured.** `SELECT MIN(start_date)` over the PK range on `data/catalog.db`: `maddison` → `0001-12-31` (338 series), `ggdc` → `0001-12-31` (14,998 series). |
| "Multilingual search: 6 languages … titles are never machine-translated" | **HOLDS.** `SUPPORTED_LANGS = ["en","ar","es","fr","ru","zh"]` in `util.ts`; the contract forbids fallback-to-English for an unsupported `lang=` and requires official producer translations only. |
| "Every dataset here is served from our store … if we can't host a source, we don't list it" | **HOLDS in design and was enforced by deletion** (20 sources / 178,262 series purged 2026-07-23). **One documented breach** of the mechanism: ledger R490, where seven sources' pages rendered "Redistributable" from `catalog.db` while D1 said otherwise. |
| "Individual Series" and "Observations" live counters | **STALE, and knowingly so.** They read `/v1/stats`, which reads the R2 object `_aqueduct/stats.json` — still the July census (~79.8 B observations / ~7.73 B series). The measured current figures are 33.9 B / 3.90 B. The census tool's own >20 % publish gate is what stops the update, and releasing the new number is Ahmed's decision. See §1.3.3. |
| "Python and R clients available" | **DOES NOT HOLD for R** in this repository. See §1.2.10. |
| Comparison table claiming FRED "~800k" series, DBnomics "1B+", Bloomberg "$25,000+/yr" | These are third parties' own published figures; the page says so in a footnote (*"Series counts are approximate for third parties (their own published figures); ours is measured live on the data store"*). I did not verify the third-party numbers, and they are outside this system. |

---

### 1.3 The scale, measured

#### 1.3.1 Sources: five different numbers, all correct, for five different questions

This is the single most confusing part of the system's headline figures, so here is the whole set
with its instrument:

| number | what it counts | instrument | date |
|---|---|---|---|
| **282** | entries in the updater registry (`updater/registry.yaml`), i.e. sources with a described fetch strategy — of which **229** carry `live: true` | `yaml.safe_load` over `updater/registry.yaml`, run 2026-08-30 | 2026-08-30 |
| **321** | sources with a public landing page and a row in the site's own browse index | `catalog/site/sources_meta.json` keys (321) and the `IDX` array embedded in `catalog/site/catalog.html` (321 entries) | site build |
| **322** | sources SERVED — catalogued **and** present in the Worker's resolver allowlist | **QUOTED** `NUMBERS.md`: `py tools/audit_schedule_coverage.py`, cross-checked as `SELECT COUNT(*) FROM source_counts` on both D1 databases (321 econ-catalog + 1 econ-catalog-climate) | 2026-08-30 |
| **324** | distinct ids in `SUPPORTED_SOURCES` in `api/worker/src/util.ts` (325 literals, `unctad_cpia` written twice) | regex extraction over `util.ts`, run 2026-08-30 | 2026-08-30 |
| **349** | rows in the local catalogue's `source` table — includes gated and scaffolding sources kept as rows | `SELECT COUNT(*) FROM source` on `data/catalog.db` (read-only) | 2026-08-30 |
| **322** | sources in `catalog.db` holding **≥ 1** catalogued series (349 minus 27 empty) | my own per-source PK-range sweep, run 2026-08-30 (§1.3.2) | 2026-08-30 |

Two of these deserve a warning, both recorded in `NUMBERS.md`:

* **`registry.yaml live: true` is 229, and that is NOT the number of auto-updating sources.** The
  ledger's own caution: *"reading `registry.yaml live:true` alone gives **229**, not 270 — `live` is
  one of FOUR scheduling paths (registry, updater-heavy matrix, sec-edgar-daily, `run_local_heavy` by
  `run_location`). Use the audit, never the registry."* The audited answer is **270 of 322 sources
  scheduled for auto-update**, covering **13,148,499 of 13,486,342 series = 97.5%**; the other 52 are
  archival (publisher-retired, no fetcher possible), and the count of actionable gaps is **0**
  (**QUOTED**, `NUMBERS.md`, 2026-08-30).
* **The `source` table's 349 rows exceed the 321 shown**, because gated and scaffolding sources keep
  their descriptive row (so the licence gate can still see them) while holding zero series. I
  reconciled this exactly: **349 = 322 with series + 27 with none**, and **322 = 321 with a landing
  page + `worldbank_pink`**, which is gated. Nothing is unaccounted for. Details in §1.3.2.

The "321 + 1 = 322" arithmetic is literal: exactly one source, **`noaa`**, lives in a second D1
database. `util.ts:500` reads `export const SHARDED_SOURCES = new Set(["noaa"])`. Its 3,138,159
catalogue rows were moved to `econ-catalog-climate` because the primary database had reached 9.35 GB
of D1's 10 GB per-database ceiling. Nothing about this is user-visible — `dbFor()` routes
source-scoped queries and global search/browse/stats merge both databases — but it is a permanent
trap for any count: a primary-only `COUNT(*)` silently drops 3.1 million entries, and `/v1/sources`
did exactly that for a period, reporting 318 while noaa was fully served.

#### 1.3.2 Series

| number | meaning | instrument | date |
|---|---|---|---|
| **13,486,015** | sum of `n_series` across the 321 sources in the site's own browse index | parsed the `IDX` array out of `catalog/site/catalog.html`, summed | site build |
| **13,486,342** | series served, fleet-wide | **QUOTED** `NUMBERS.md`, `py tools/audit_schedule_coverage.py` | 2026-08-30 |
| **13,486,284** | `series` rows in D1, summed over `econ-catalog` + `econ-catalog-climate` | **QUOTED** `NUMBERS.md`, `wrangler d1 execute … "SELECT COUNT(*) FROM series"` | 2026-08-24 |
| **26,981,683** | `series_fts` rows (the full-text search index) — **2.00× the series count** | **QUOTED** `NUMBERS.md` | 2026-08-24 |

The three series counts agree to within 0.003 % and differ only by measurement timing. Treat
**≈13.5 million catalogued series** as the honest headline.

The fourth row is a defect, not a feature: the search index holds **twice** as many rows as there are
series. `NUMBERS.md` decomposes the excess as **1,052,814 orphaned** rows across 38 sources (the
source has zero series, so an INNER JOIN means they never reach a user) plus **12,442,585 surplus
duplicates** across 100 sources (`wid` alone 7,395,591; `cepii_gravity` 2,336,500). They cost storage
and D1 read volume rather than correctness.

**I re-measured this independently, and it lands on the audited figure exactly.** A per-source
primary-key range sweep over all 349 rows of the local `source` table:

```python
# read-only against E:/research/econfindatalibrary/data/catalog.db
for s in every source_id in the `source` table:
    SELECT COUNT(*) FROM series WHERE series_id >= s+':' AND series_id < s+':\uffff'
```

(The range form is what makes this cheap: `series_id` is the PRIMARY KEY, so each source is an index
range scan; a `GROUP BY source_id` has no index and full-scans the 11.91 GB file.) The run took
1,625 s under heavy disk contention from concurrent sweeps and returned:

| | |
|---|---|
| **TOTAL series rows** | **13,486,342** |
| sources with at least 1 catalogued series | **322** |
| sources with **zero** catalogued series | **27** |

**13,486,342 is the same number, to the row, that `tools/audit_schedule_coverage.py` reported on the
same day** — two different instruments over the same database agreeing exactly. That is as solid as a
count in this system gets.

The 27 empty rows also explain the source arithmetic completely:

* **349 `source` rows − 27 with no series = 322 sources served.**
* Of those 322, **321 have a public landing page**. The one that does not is **`worldbank_pink`**,
  which holds 26 catalogue rows but is on the redistribution denylist, so no page is generated and
  `/v1/catalog` excludes it at the SQL layer (see §1.6.4).
* The 27 empties are, in full: `central_banks`, `cftc`, `edgar_13f`, `fraser_efw`, `fred_releases`,
  `fsi`, `gii`, `gleif`, `imf_dbnomics`, `insee_sirene`, `pxweb`, `pxweb_bfs`, `sdmx_nso`,
  `sipri_polity`, `social_progress`, `spi`, `stat_austria`, `wiid`, `worldbank_extra`,
  `wto_bat_bv_m`, `wto_bat_bv_x`, `wto_hs_0010/0015/0020/0025/0030/0040`. Twenty of them are the
  denylist's live block; the other seven (`cftc`, `edgar_13f`, `gii`, `gleif`, `insee_sirene`,
  `pxweb`, `worldbank_extra`) are ingested-but-uncatalogued stores or scaffolding ids, not gated ones.

Largest sources by catalogued series, from my own sweep: `noaa` 3,138,159 · `wid` 2,465,197 ·
`cepii_gravity` 1,143,250 · `bea` 913,230 · `vdem` 783,100 · `un_wpp` 334,236 ·
`imf_gfssoo_direct` 319,571 · `fdic` 298,869 · `eia` 268,502 · `imf_bop_direct` 260,931 — identical to
the site index's own figures.

#### 1.3.3 Observations — and the number that is *not* on the website

The observation count is produced by a real tool, `E:/research/econfindatalibrary/tools/series_census.py`,
whose method (from its own docstring) is:

* **observations** — exact parquet **footer** row counts (`pq.read_metadata`, no data read) over every
  `.parquet` the Worker will actually resolve;
* **individual_series** — per source, DuckDB `COUNT(DISTINCT series_key)` exactly where it fits in
  memory, HyperLogLog where it does not;
* **sources_catalogued** — live `COUNT(DISTINCT source_id)` from `catalog.db`.

The three most recent census runs, read from `E:/research/econfindatalibrary/logs/`:

| census file | as_of | individual_series | observations | sources | method note |
|---|---|---|---|---|---|
| `logs/stats-2026-08-11.json` | 2026-08-11 | 36,563,064,164 | 89,741,477,749 | 315 | **LOCAL-DISK scope, superseded** |
| `logs/stats-2026-08-23.json` | 2026-08-23 | 3,187,761,836 | 24,054,980,219 | 319 | first run under the served-store scope |
| **`logs/stats-2026-08-26.json`** | **2026-08-26** | **3,901,731,326** | **33,908,707,379** | **322** | exact `COUNT(DISTINCT)` for 307 sources, HLL for 6 |

**Why the number fell by two thirds between 11 and 23 August — this matters, and it is not data
loss.** On 2026-08-23 the census tool was rescoped from "everything on local disk" to "what a user
can actually download: objects present on R2 that `api/worker/src/util.ts` will resolve". The
docstring is explicit: *"Local disk is not the product (statcan has 175 GB here and 0 bytes on R2),
and presence in the bucket is not the product either (owid is gated and 404s)."* `NUMBERS.md` records
the same conclusion: *"the corpus GREW since July … the scope change, NOT data loss, is why the
public answer moved."*

**The critical caveat: this number is not what the website serves.** `series_census.py` has a
mechanical publish gate — if `individual_series` or `observations` moves more than 20 % from the
currently-published object, it **refuses** to upload without `--force-publish`. The 2026-08-26 census
was run **without** `--publish`, and `NUMBERS.md` states: *"NOT PUBLISHED to /v1/stats: the >20% gate
refuses (published July object: 79.8B/7.73B) — quoting publicly is Ahmed's Phase-4 decision."*

So, as of this writing:

* **What the live site's counters show** (via `/v1/stats`, which reads `_aqueduct/stats.json` from
  R2): the July census, **~79.8 billion observations / ~7.73 billion series**. **QUOTED**, and per
  the census tool's own note the 7.73 B figure *"is not reproducible from the current store and
  should not be requoted."*
* **What the store actually holds and serves**, measured: **33,908,707,379 observations /
  3,901,731,326 series over 322 sources** (2026-08-26).
* **What is held in total, including data not currently on R2:** `NUMBERS.md` computes
  33,908,707,379 (served census) + 56,845,814,827 (statcan parquet footers, local only since the
  2026-08-18 cost order) = **90.75 billion observations**, with gated stores uncounted.

That gap between the published headline and the measured one is a real, open item; it is a decision
reserved for Ahmed, not a measurement problem.

#### 1.3.4 The biggest sources, measured

From `logs/stats-2026-08-26.json` (`per_source_obs` / `per_source_series`):

| source | observations | distinct series |
|---|---|---|
| `cbs_nl` (Statistics Netherlands) | 9,063,913,608 | 623,897,728 |
| `eurostat` | 8,569,094,118 | 1,488,747,042 |
| `oecd` | 7,119,072,873 | 752,941,722 |
| `unctad_biotrademerch` | 2,291,982,918 | 233,067,877 |
| `abs` (Australia) | 977,441,166 | 443,275,108 |
| `gus_dbw` (Statistics Poland) | 708,444,128 | 88,796,461 |
| `noaa` | 550,625,829 | 3,138,159 |
| `unctad_tradefoodcatbyproc` | 394,118,603 | 26,579,759 |
| `ilostat` | 390,818,247 | 30,549,506 |
| `cepii_baci` | 356,900,069 | 90,582 |
| `bls` | 328,346,715 | (not counted — no `series_key` column) |
| `eia` | 322,500,439 | (not counted — no `series_key` column) |
| `istat` | 284,138,787 | 33,448,966 |
| `ecb` | 219,428,533 | 3,729,740 |
| `faostat` | 170,645,319 | 15,760,362 |

The census reports its own method per source: **307 sources counted exactly** (`COUNT(DISTINCT)`);
**6 by HyperLogLog** because they are too large to count exactly in memory — `abs`, `cbs_nl`,
`eurostat`, `gus_dbw`, `oecd`, `unctad_biotrademerch`, i.e. precisely the six giants, so the ~1 %
estimation error sits on the largest terms of the sum; and **6 sources not series-counted at all**
because their parquet has no `series_key` column — `bls`, `eia`, `insee_bdm`, `ofr`, `sec_edgar`,
`worldbank_esg`. The last six still have their observations counted; only their series count is
absent, and the tool reports that rather than silently emitting a zero.

Note the distinction between *store* series (billions — every dimension combination in the parquet)
and *catalogued* series (13.5 million — what appears in search). Many giants are catalogued at
**table or flow grain**, one catalogue row per store file, with the dimension keys carried inside the
served CSV's own `series_id` column. `cbs_nl` is the clearest case: 9.06 billion rows served under
5,154 catalogue entries. This is a deliberate design choice recorded in `util.ts` — series grain
"would put billions of rows in the catalogue".

The grain choice is uneven across sources, and in a few places it is extreme. Measured from the site
index: **no source is listed with zero catalogued series** (a check worth having, because a
zero-series page is exactly the dead-end the "no metadata-only" policy forbids), the **median source
has 2,207 catalogued series**, and the smallest are `bcrp` 3, `nyfed` 8, `unctad_ciocgeaia` 8,
`unctad_wstbtocabgoea` 8, and **`bls` 9**. That last one is worth pausing on: BLS's store holds
328,346,715 observations and the catalogue exposes **nine** entry points into it — so almost all of
that data is reachable only by knowing which of the nine to download, not by searching for it.
`NUMBERS.md` records the same observation independently.

By *catalogued* series, the largest sources are different (from the site's `IDX` index):
`noaa` 3,138,159 · `wid` 2,465,197 · `cepii_gravity` 1,143,250 · `bea` 913,230 · `vdem` 783,100 ·
`un_wpp` 334,236 · `imf_gfssoo_direct` 319,571 · `fdic` 298,869 · `eia` 268,502 ·
`imf_bop_direct` 260,931 · `harvard_atlas` 255,217.

#### 1.3.5 Storage footprint

Measured on 2026-08-30 by an `os.walk` size sweep over the local staging store (metadata only, no
file contents read):

| store | directories | files | size |
|---|---|---|---|
| `E:/research/econfindatalibrary/data/clean_full` | 430 source dirs | 72,340 | **326.02 GB** |
| `E:/research/econfindatalibrary/data/clean_grouped` | 3 source dirs | 26,445 | **18.98 GB** |
| **total local clean store** | | **98,785** | **345.00 GB** |

Two databases dominate the rest of local disk (`ls -la`, 2026-08-30):

| file | bytes | note |
|---|---|---|
| `data/catalog.db` | 11,906,957,312 (**11.91 GB**) | the catalogue: `series`, `series_fts` + its shadow tables, `source`, `license` |
| `data/_aqueduct/state.db` | 11,300,503,552 (**11.30 GB**) | the updater's state store: `runs`, `source_state`, `unit_state`, `series_cursor`, `csv_retry_queue`, `leases` |

Cloud storage, **QUOTED** from `NUMBERS.md` (I did not query Cloudflare in this session):

* **R2 account-level storage: 925 GB** current, against a billed period mean of 1,881 GB (the 2.4 TB
  deletion landed 2026-08-18). Measured 2026-08-30 via `r2StorageAdaptiveGroups` grouped by
  date + bucketName + storageClass.
* An earlier per-bucket split of *"601 GB econ + 282 GB hf"* is marked **SUPERSEDED — WRONG
  INSTRUMENT** in `NUMBERS.md` and should not be quoted. **NOT ESTABLISHED:** the current
  econ-only bucket size. What would establish it: `npx wrangler r2 bucket info econ-data`.
* The IP library's bucket is **307 MB** (2026-08-30).
* Forward monthly bill after the 2026-08-30 noaa fix: **~$30/mo pre-tax, ~$32 with tax**
  (**QUOTED**, `py tools/billing_guard.py`).

#### 1.3.6 Freshness plumbing, measured locally

From `data/_aqueduct/state.db` (read-only, 2026-08-30):

```
source_state rows: 249      unit_state rows: 283      runs rows: 1,257
source_state.cadence:  weekly 91 · annual 63 · monthly 34 · quarterly 21 · irregular 21 · daily 14 · static 5
unit_state.status:     no_change 190 · ok 54 · partial 37 · transient_fail 2
newest last_success_utc: 2026-08-30T00:46:16+00:00
```

**Caveat, and it is a real one:** this is the *local workstation's* state store. Cloud-run sources
write to the R2-backed aqueduct state, so these 249 sources are not the whole fleet, and the "all 249
status = ok" line is the local mirror's view, not a fleet health verdict. The authoritative
user-facing view is `/v1/last-updates`, which reads D1. `NUMBERS.md` records the fleet picture
separately, and it is less rosy: **the nightly `updater-daily` CI gate failed 40 of its last 40
runs**, with no success since 2026-08-13 (**QUOTED**, `gh run list`, 2026-08-30).

---

### 1.4 The family of sites

There are four public domains, one shared identity system, and one shared design canon.

| site | what it is | headline scale |
|---|---|---|
| **hfdatalibrary.com** — HF Data Library | 1-minute OHLCV intraday bars for U.S. stocks and ETFs, December 2002 → present, in two cleaning versions (Raw and Clean), plus 25 pre-computed academic variables per ticker per day. Licensed CC BY 4.0. Positioned as a free, citable alternative to TAQ/CRSP. | **1,391 tickers · 1,551,364,273 1-minute bars · 23+ years · 25 academic variables** (read from the rendered hero of `D:/research/hfdatalibrary/index.html`; the portal labels that bar count the **raw** tier). |
| **econdatalibrary.com** — Econ Data Library | This system. | 321 source pages, ≈13.5 M catalogued series, 33.9 B served observations |
| **ipdatalibrary.com** — IP Data Library | Innovation measures computed from the complete US patent record — citations, originality and generality, firm patent panels. US patent data is public domain. | **9,454,161 patents · 540,995 patent holders · 152,631,929 citation links · 1976–2025** (**QUOTED**, `NUMBERS.md` 2026-08-30, each figure independently re-measured with a different instrument than the site generator used) |
| **elkassabgidata.com** — ElkassabgiData | The family portal: "One account. Every library." Lists the three libraries, carries an "Ask the Data" page and the MCP setup guide, and links the account server. | portal at `E:/research/econfindatalibrary/portal/` |

All four carry the same footer attribution: *"© 2026 Ahmed Elkassabgi. University of Central
Arkansas. ORCID: 0000-0002-5926-7493."*

#### What the family actually shares

1. **One identity database.** `hfdatalibrary-db` in Cloudflare D1 is bound into the econ Worker as
   `USERS`. `auth.ts` says it outright: *"hfdatalibrary's users database is THE identity provider for
   both libraries. This module validates the SAME api_keys … so every existing hf account downloads
   econ data with its current key — no separate registration, no migration."*
2. **One API key and one SSO session.** Accounts live at `accounts.elkassabgidata.com`. A family
   access token (`edl_at`) or an `X-API-Key` both authorise a download, and the same key works on
   every library.
3. **One rate-limit table, namespaced.** `rate_limits` is shared; econ writes only under the
   `econ:download` namespace, so the libraries cannot throttle each other by accident.
4. **Separate download counters.** `econ_download_log` is econ's own table inside the shared DB, so
   hf's download counts are not inflated by econ traffic.
5. **One MCP server** covering all three libraries (§1.2.6).
6. **One design canon.** The econ CSS carries comments like *"Base typography copied from
   hfdatalibrary.com css/style.css:49-79"* and *"nav links mirror hfdatalibrary.com exactly"*. My
   memory of Ahmed's standing instruction is recorded as *"hf is the design canon"* — econ, ip and
   the portal are made to match hf, across every shared region including footers.
7. **Shared legal pages.** econ's footer links Terms and Privacy at `hfdatalibrary.com/pages/terms`
   and `/pages/privacy` — one legal surface for the family.
8. **Shared usage statistics.** `/v1/public-stats` reads user counts, the country map and the
   institution list from the shared identity DB *using hf's exact aggregation*, so those figures are
   identical across libraries "by construction — one login, one user base"; only the download figures
   are per-library.

#### "Ask the Data" — the family chat assistant

The portal carries an **Ask the Data** page (`portal/ask.html`) that takes a plain-English question
("How many people live below the poverty line in Poland?", "AAPL 1-minute bars for an event study")
and answers it from the libraries, with source and licence attached. It posts to
`https://elkassabgidata-assistant.elkassabgi.workers.dev`.

The backend exists and is real code: `E:/research/econfindatalibrary/assistant/` — **1,022 lines of
TypeScript across 8 files** (`index.ts`, `agent.ts`, `llm.ts`, `tools.ts`, `state.ts`,
`turnstile.ts`, `prompt.ts`, `types.ts`). Its README describes a proxy Worker that grounds a DeepSeek
model on the *existing free endpoints* and gates the actual download behind a free account:
anonymous visitors search and preview, downloading requires registration. Its stated security
property is worth repeating because it is structural rather than a policy: *"every tool is read-only
over a public endpoint; the download tools return a link, never data rows — so a jailbroken model can
leak nothing it wasn't already free to search."* Budget is capped by an atomic Durable Object
counter, and with `DEEPSEEK_API_KEY` unset the Worker runs a deterministic mock model so the whole
flow can be exercised without spending anything.

**NOT ESTABLISHED:** whether that Worker is currently published, and whether `DEEPSEEK_API_KEY` is
set in its production environment. What would establish it: a `POST /chat` against the live URL, or
`npx wrangler secret list` for that Worker. (My own stored memory of this project said "no code yet"
— that memory is stale; the code exists.)

---

### 1.5 Who the users are, and what evidence of real use exists

#### 1.5.1 What the system measures about its users

`api/worker/src/publicStats.ts` is the whole instrument, and it is worth knowing exactly what it
counts because these are the numbers the public Stats page shows:

* `total_users` — `SELECT COUNT(*) FROM users` on the shared identity DB (all rows).
* `total_downloads`, `downloads_today`, `downloads_this_week` — counts from `econ_download_log`.
* `total_bytes_served` — `SUM(bytes)` from the same table; the code notes this is *"a rising floor"*
  because the `bytes` column post-dates some rows.
* `countries` — distinct **active** users per country, taking the union of the self-declared profile
  country and any country they have logged in from, normalised to ISO-3166 alpha-2 through a
  hand-maintained 152-key lookup that mirrors hf's `COUNTRY_TO_ISO`. Unrecognised free text is
  dropped rather than guessed.
* `institutions` — self-declared institution, with a **58-entry** blocklist of placeholders (`none`,
  `self`, `student`, `retired`, `test`, …) removed *before* ranking so junk cannot consume top slots,
  an **11-entry** alias map merging `stanford` → `Stanford University` and similar, top 50 by user
  count. Users who tick `hide_institution` are excluded. Real companies are deliberately *not*
  blocked.
* `top_sources` — the five most-downloaded sources, **whitelisted strictly against the current
  catalogue** so that a purged source (e.g. WTO) that still appears in the historical log cannot be
  resurrected on the page.
* `visitor_countries`, `total_visitors`, `total_page_views` — Cloudflare zone analytics over the last
  30 days, and only if `CF_API_TOKEN`/`CF_ZONE_ID` are configured; any failure is swallowed so an
  analytics hiccup never breaks the endpoint.

No personally identifying data leaves the endpoint; it is aggregate-only, unauthenticated, CORS `*`.

#### 1.5.2 The measured usage figures — all QUOTED, with their dates

I did **not** query D1 in this session (project rule: decide locally, verify remotely, and never scan
D1 for exploration). Everything here is quoted from the ledgers with its original date.

| figure | source | date |
|---|---|---|
| **1,006 registered users**; 845 logins in 21 days; 3,131 SSO refresh tokens; **91,557 downloads in 7 days** | `.claude/MISTAKES.md` entry **R432** — one D1 query, run to refute a claim that the SSO flow had never been exercised | **2026-08-17** |
| hf downloads **9,580–14,615 per day**, from **40–56 distinct users** and 54–73 IPs | `NUMBERS.md` — `SELECT date(timestamp), COUNT(*), COUNT(DISTINCT user_id) FROM download_log` over a 4-day window on `hfdatalibrary-db` | **2026-08-26** |
| hf **118–271 GB served per day**; described as "broad organic use", top user ≈10 GB/day over 3 days | `NUMBERS.md`, same query, `SUM(bytes_served)` | **2026-08-26** |
| **1,072 organic family downloads in 7 days** during the SSO soak (the soak's exit criterion, met 1,072×) | `.claude/MISTAKES.md`, SSO soak record | 2026-07 |
| **21,692 visitors against 603 accounts** — i.e. ~97 % of arrivals never create an account | `.claude/MISTAKES.md` entry **R219**. ⚠ **The entry carries no date**; R219 is a much earlier entry than R432, so this ratio is older than the 1,006-user figure and the two should not be combined. | undated in the ledger |
| **14 accounts over the fair-use threshold** (>50 GB downloaded in 30 days), of which **exactly one** had downloaded in the last 24 hours | `.claude/MISTAKES.md` entry **R219**, same caveat on dating | undated in the ledger |

**What that shape says about the audience.** The library is used by a small number of heavy,
programmatic consumers sitting on top of a much larger anonymous browsing population. Roughly 97 % of
arrivals never register at all — which is by design, since browsing, search, metadata and freshness
need no key. The Zenodo abstract names the intended audience directly: *"giving students and
researchers one clean, well-documented, citable place to find economic and financial statistics."*

**One correction that must travel with these numbers.** Ledger entry **R494** records that a claim of
*"~87,000 downloads/day"* was reported to Ahmed and was **wrong**: it came from D1 `rows_written`
telemetry (~6 billed index rows per `download_log` insert), not from the event table. The true rate
was 9,580–14,615/day. Never quote a download rate from write telemetry.

#### 1.5.3 What is NOT established about econ's own usage

The figures above are overwhelmingly **hf's** or **family-wide**. I could not establish, from local
files:

* econ's own `total_downloads` / `downloads_today` / `downloads_this_week`;
* econ's `total_bytes_served`;
* the country list, the institution list, or the top-5 downloaded econ sources.

All five live only in `econ_download_log` and `users` inside Cloudflare D1. **What would establish
them:** a single `GET https://econdl-api.elkassabgi.workers.dev/v1/public-stats` — which is
unauthenticated, free, and returns every one of them in one response. That is the right instrument
and it costs nothing; it simply was not run in this read-only session.

#### 1.5.4 Third-party evidence of use

`D:/research/hfdatalibrary/pages/used-by.html` is a curated, source-verified list — *"Every entry
below was checked against its source … Nothing is listed on the strength of a search result."* As of
its **2026-08-24** check it holds **2 codes, 2 listings, 1 mention, 1 research paper, 1 product**,
including:

* `stock_drawdown_risk_ml_research` (GitHub, 2026-08-12) — an ML research repo that downloads from
  the HF API, keeps an `ATTRIBUTION.md` carrying the CC BY credit and the IEX terms passthrough,
  snapshots the library's API and documentation pages with SHA-256 hashes, cites the DOI, and engages
  with the published survivorship-bias limitation.
* `borsa-dashboard-training` (GitHub, 2026-06-25) — pulls `/v1/download-token/{ticker}` live.
* A WebsiteLaunches.com daily listing (#27, 2026-07-18) for the IP Data Library.
* An `arXiv:2605.17705` mention, explicitly recorded as *a mention, not a use*, so it stops
  resurfacing as a candidate.

There is no equivalent "used by" page in the econ repo. **NOT ESTABLISHED:** independent academic
citations of `econdatalibrary.com` specifically. What would establish it: a Google Scholar / OpenAlex
search on the DOI `10.5281/zenodo.21405120`.

---

### 1.6 The licensing and redistribution posture — and why it is the load-bearing part

#### 1.6.1 Why this matters

The library's entire value proposition is that it *hosts* the data rather than linking to it. That is
also its entire legal exposure. A statistical agency that publishes data free of charge has not
thereby granted anyone the right to re-publish it; many explicitly forbid it. If the library re-hosts
data it may not re-host, the consequences are not a bug report — they are a takedown demand against a
named individual at a named university.

So the posture is asymmetric by design: **a source is served only when its terms explicitly permit
third-party redistribution and an independent adversarial check confirmed the reading.** Anything
restricted, ambiguous, unreachable, or disputed stays gated. `README.md` calls this "the one rule":

> Nothing gets cached & re-served unless its license is `reservable: true` in `configs/sources.yaml`.
> The ingest refuses everything else (see `core/licenses.py`).

#### 1.6.2 `DATABASE_LICENSES_VERBATIM.md` — the single source of truth

`E:/research/econfindatalibrary/DATABASE_LICENSES_VERBATIM.md` is 648 KB and is the canonical record.
Its own header describes how it was built:

> **Generated 2026-07-14** … for every database, an agent fetched the provider's OFFICIAL terms,
> quoted the redistribution clause VERBATIM with the source URL, and classified it; a second,
> independent adversarial agent re-fetched the URL, confirmed the quote is word-for-word, and tried
> to refute any over-permissive reading. **88 providers, 191 databases, 176 agents, 0 errors.**
>
> **This is the single source of truth. Do NOT re-derive it from scratch.**

Its decision tiers, per database:

| tier | count |
|---|---|
| CLEARED — re-host OK (attribution) | 144 |
| RESTRICTED (keep gated) | 18 |
| NEEDS HUMAN REVIEW | 11 |
| CLEARED — re-host OK | 9 |
| CLEARED — non-commercial only | 6 |
| CLEARED by WRITTEN PERMISSION | 2 |
| CLEARED by WRITTEN PERMISSION (scoped/conditional) | 1 |

Adversarial verdicts: **CONFIRMED = 184, DISPUTED = 7.** The seven disputes are exactly the cases
where a one-line licence summary would have been wrong — Bundesbank ("use-only grant", not a
redistribution grant), FAOSTAT (non-commercial + an embedded third-party carve-out that CC BY 4.0
does not impose), Our World in Data (mixed: only OWID's *own* processing is CC BY; most of the data
belongs to WHO/UN/World Bank upstreams), Freedom House, IDB, World Bank Open Data (third-party
indicators may not be redistributed), and World Bank Pink Sheet (LME, Cotlook, SICOM, ICCO/ICO prices
inside it).

#### 1.6.3 What "reservable" means, concretely

`reservable` is a **column on the licence row**, and it is the flag the whole system gates on. From
`configs/sources.yaml`:

```yaml
licenses:
  us-public-domain:     {reservable: true,  commercial_ok: true,  attribution: requested}
  cc-by-4.0:            {reservable: true,  commercial_ok: true,  attribution: required}
  cc0:                  {reservable: true,  commercial_ok: true,  attribution: false}
  ecb-attrib-nomodify:  {reservable: true,  commercial_ok: true,  attribution: required, no_modify: true}
  bis-attrib-nc:        {reservable: true,  commercial_ok: false, attribution: required}   # NON-COMMERCIAL
  audit-restricted:     {reservable: false, commercial_ok: false, attribution: required}   # gated
  dbnomics-passthrough: {reservable: per_series, note: "inherit each provider's license at ingest"}
```

In the catalogue database the licence table carries the flags directly:

```sql
CREATE TABLE license (
  license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT)
```

Measured on `data/catalog.db` (read-only, 2026-08-30): **71 licence rows**, of which **57 are
`reservable = 1`** and **14 are `reservable = 0`**. Cross-tabulated:

| reservable | commercial_ok | attribution_required | no_modify | licences |
|---|---|---|---|---|
| 1 | 0 | 1 | 0 | 28 |
| 1 | 1 | 1 | 0 | 16 |
| 1 | 0 | 1 | 1 | 6 |
| 1 | 1 | 0 | 0 | 4 |
| 1 | 1 | 1 | 1 | 2 |
| 1 | 0 | 0 | 0 | 1 |
| 0 | (various) | | | 14 |

Read that first row carefully: **28 licences permit re-hosting but forbid commercial use.** That is
why the CSV citation header (§1.2.3) shouts `NON-COMMERCIAL USE ONLY (honor it)`, and why it also
emits `SHARE-ALIKE` and `NO DERIVATIVES` lines — a code comment records that 2,866,900 served series
carry one of those two obligations (WID alone is 2,465,197 under CC BY-NC-SA 4.0) and the header
previously omitted both.

**"Reservable" is not the same as "open".** It means exactly one thing: *this library may host a copy
and serve it to third parties.* Commercial use, attribution, share-alike and no-derivatives are
separate flags, and each is surfaced to the user independently.

**A cleared source can also be cleared only in part.** `configs/sources.yaml` carries per-source
`scope` notes that bound what is actually hosted — the sharpest example being SEC EDGAR:

```yaml
sec_edgar:
  scope: "XBRL fundamentals + 13F + insider = DATA we host;
          filings = metadata POINTERS to sec.gov (NOT the documents)"
```

and `wikidata`: *"econ/finance subset only — NOT the full graph"*; and EIA, whose note records that
its ToS permits a public display service but forbids using the EIA logo or naming EIA as the source
of modified values, so derived tiers must be labelled *"derived from EIA data"*.

#### 1.6.4 The denylist — the runtime gate

`api/worker/src/denylist.ts` is a **generated** file (`python -m core.gen_denylist`), derived from
`catalog.db` so it cannot drift from the site:

> A source is blocked iff its license is not verified-redistributable (`license.reservable = 0`),
> minus the granted exceptions below, plus a legacy safety floor so a regeneration never silently
> un-gates a previously-blocked source.

It holds **49 ids** (counted 2026-08-30): **21 in the live block** plus **28 legacy/phantom** ids kept
as a safety floor.

```
LIVE BLOCK (21):
central_banks · fraser_efw · fred_releases · fsi · imf_dbnomics · pxweb_bfs · sdmx_nso
sipri_polity · social_progress · spi · stat_austria · wiid · worldbank_pink
wto_bat_bv_m · wto_bat_bv_x · wto_hs_0010 · wto_hs_0015 · wto_hs_0020 · wto_hs_0025
wto_hs_0030 · wto_hs_0040

LEGACY / PHANTOM FLOOR (28):
cboe · cow · dbnomics · famafrench · fred · freedomhouse · gus · ibge · ine_spain · irena
nbp · owid · polity · qog · shiller · sipri · tcmb · unesco_sci · unicef · who_gho
wto_hs_a_0010 · wto_hs_a_0015 · wto_hs_a_0020 · wto_hs_a_0025 · wto_hs_a_0030 · wto_hs_a_0040
wto_its_mtv_am · wto_its_mtv_ax
```

**20 of the 21 live-block ids match exactly** the sources whose licence row is `reservable = 0` in
the local `catalog.db` (verified by SQL join, 2026-08-30). **The exception is `sdmx_nso`**: it is in
the live gate block, but its local licence row is `cc-by-3.0` with `reservable = 1`. The practical
exposure is nil — my PK-range sweep shows `sdmx_nso` holds **zero** catalogued series, so there is
nothing to leak either way. But the two records disagree and I did not establish which is stale. This
is the multi-place licence drift that ledger entry **R490** documents (see §1.6.7). What would settle
it: `sdmx_nso`'s verdict in `DATABASE_LICENSES_VERBATIM.md` and its licence row in D1.

**How much data actually sits behind the gate**, measured by the same sweep, checked for all 49 ids:

* **20 of the 21 live-block sources hold zero catalogued series.** The twenty-first,
  **`worldbank_pink`, holds 26** (see §1.6.6).
* **All 28 legacy/phantom ids are absent from the `source` table entirely** — they are genuinely
  phantoms, exactly as the file claims, not quietly-present rows.

So the gate is not straining against a large body of gated-but-catalogued data; it is guarding an
almost-empty set, which is what the 2026-07 purge was for. The whole gated surface in the local
catalogue is 26 rows.

The 28 phantoms exist because of a trap the project learned the hard way, recorded in the file and in
`REDISTRIBUTION_COMPLIANCE.md`: *deleting a source row removes it from the `reservable=0` scan, so it
drops out of the gate* — `irena`, `freedomhouse` and `shiller` did leak on the first regeneration
after the 2026-07 purge. The phantom entries make un-gating impossible by accident.

**Two granted exceptions are hard-coded** with their conditions:

* `kof_globalization` — Prof. Jan-Egbert Sturm (KOF director, index co-author), 2026-07-06:
  non-commercial academic re-hosting; cite "KOF, ETH Zurich"; link back; no resale; KOF may request
  removal.
* `comtrade` — UN Comtrade, 2026-07-07: our holdings sit in the free branch ("up to 100,000
  records"). **STANDING GUARD: comtrade holdings must stay ≤ 100,000 records**; growing past that
  leaves the free branch and requires re-gating.

**Series-level carve-outs** exist for sources that are themselves redistributable but embed
third-party data:

```ts
export const SERIES_CARVEOUTS = {
  worldbank:      ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],   // IMF-sourced CPI, ILO-sourced unemployment
  worldbank_wdi:  ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],   // same indicators under the other id
  worldbank_pink: ["aluminum","copper","nickel","zinc","gold","platinum","silver"],
};
```

The `worldbank_wdi` line is instructive: the carve-out was originally keyed only on `worldbank`, so
the identical IMF- and ILO-sourced indicators **were being served** under `worldbank_wdi` until
2026-07-22, when a live probe found `worldbank_wdi:SL.UEM.TOTL.ZS` returning 401 (i.e. served) while
the same indicator under `worldbank` returned 451. The `worldbank_pink` metals are gated because LME
(base metals) and LBMA/IBA (precious metals) **refused redistribution in writing** on 2026-07-15.

The gate is applied in `index.ts` **before** authentication, so a gated series returns `451
not_redistributable` rather than `401`, with a message directing the user to the original provider.
A gated source is also hidden from `/v1/catalog`, `/v1/sources` and `/v1/bundle` — the whole surface,
not just the data endpoint.

#### 1.6.5 Permissions granted, and permissions refused

From `REDISTRIBUTION_COMPLIANCE.md` — the paper trail, "the spec for the site's citation UI".

**Granted in writing** (each with its owed obligation):

| source | grantor | date | condition |
|---|---|---|---|
| `kof_globalization` | Prof. Jan-Egbert Sturm, KOF/ETH Zurich | 2026-07-06 | NC academic; cite "KOF, ETH Zurich"; link back; KOF may request removal |
| `gpi`, `gti`, `ppi`, `etr` | Institute for Economics & Peace | 2026-07-06/07 | CC BY-NC-SA 4.0 |
| `comtrade` | UN Comtrade | 2026-07-07, confirmed 07-08 (*"You can proceed as indicated in your email below."*) | ≤ 100,000 records |
| `whr` | World Happiness Report / Gallup | 2026-07-09 | **Figure 2.1 summary ONLY** — currently **re-gated** because the catalogue served the broader annual Life-Ladder panel, beyond scope |
| `damodaran` | Aswath Damodaran, NYU Stern | 2026-07-15 | *"I don't have a problem with you hosting this data."* NC + attribution |
| `bundesbank` | Deutsche Bundesbank (inquiry 2026/005812) | 2026-07-15 | free of charge, unaltered, credit *"Copyright: Deutsche Bundesbank, Frankfurt am Main, Germany"*, send them a copy |
| `idb` | IDB Open Data Team | 2026-07-15 | *"you have our full permission to re-host and redistribute them"*; exclude third-party series; per-dataset permanent link-back (this is why `idb` CSVs carry a `Dataset:` line) |
| `defillama` | DeFiLlama | 2026-07-16 | NC + proper citation |
| `ei_statreview` | Energy Institute | 2026-07-22 | NC; **excluding any S&P Global Platts price series**; annual June refresh |
| `efw` | Fraser Institute | 2026-08-10 | NC re-host with attribution + link-back |

**Refused in writing** (permanently gated, link-out only):

| source | refuser | verbatim |
|---|---|---|
| all 8 WTO tariff/trade facets | Thomas Verbeet, Chief, Integrated Database Unit, WTO | *"I am afraid that we are not in a position to authorise the request. You are welcome to post weblinks…"* |
| LME settlement prices (Al/Cu/Ni/Zn) | LME Market Data | distribution licence required, one-off **$4,000**; no free/academic path |
| LBMA gold/silver/platinum benchmarks | ICE Benchmark Administration (case CC63240914) | *"not designed to be free and publicly available"*; the university licence *"would not allow for the data to be published on a website"*; **USD 5,000 per benchmark per year** internal-use only |

#### 1.6.6 "If we cannot host it, we do not list it" — the purge

Gating alone was judged insufficient. `REDISTRIBUTION_COMPLIANCE.md` records the owner policy, stated
three times and enforced 2026-07-23:

> **if we cannot host data, it does not live in the database.** A gated-but-present source is the same
> metadata-only pattern that is banned — so the rows are DELETED, not merely 451'd.

**Removed: 20 sources / 178,262 series.** `catalog.db` went 1,395,623 → 1,217,361 series and 309 →
289 sources; D1 matched exactly. The removed ids: the eight WTO facets, `cow`, `polity`, `sipri`,
`nbp`, `tcmb`, `cboe`, `famafrench`, `dbnomics`, `irena`, `freedomhouse`, `shiller`, `whr`.

It was made reversible on purpose: all 45,847 R2 `clean_full` objects were verified byte-identical
locally first (76 files were *larger* locally, so overwriting would have destroyed the newer copy);
full-row fixtures were round-trip verified into `data/_deleted_fixtures/`; and `catalog.db.pre_purge.bak`
was retained.

**One residual exception to that policy, found while reconciling the source counts.**
`worldbank_pink` was **not** in the purge list, and it still holds **26 catalogue rows** in
`data/catalog.db` while being gated on the denylist. No user can see them — `catalog.ts` excludes
denylisted sources at the SQL layer, the site generator produces no `worldbank_pink.html`, and its
data endpoint returns 451 — so this is invisible rather than harmful. But it *is* a gated-but-present
source, which is the exact pattern the 2026-07-23 policy deleted twenty others for. **NOT
ESTABLISHED:** whether those 26 rows also exist in D1, or only in the local catalogue. What would
establish it: a single `SELECT COUNT(*) FROM series WHERE source_id='worldbank_pink'` on
`econ-catalog`, or the source's absence from the live `/v1/sources`.

This is why the homepage can say, truthfully: *"Everything in the library is real, downloadable data
— if we can't host a source, we don't list it."* It is also consistent with the standing instruction
in my memory: **host fully or don't list it; never "metadata-only".**

#### 1.6.7 The user-visible consequence

Every one of the 321 source pages carries `reservable: true` in the site index — measured: **321 of
321** (parsed from `catalog.html`'s `IDX` array). By construction, a visitor never lands on a page for
data they cannot have.

**One documented failure of that construction, worth carrying forward.** Ledger entry **R490**
records that seven sources carried a *different* licence in `catalog.db` than in D1, and the site
generator (`catalog/gen_site.py`) reads `catalog.db` — the one that said yes. The live API's
`/v1/sources` reported `reservable=false` for `ei_statreview`, `worldbank_pink`, `istat`, `who_hwf`,
`who_rs` and `fsi_fundforpeace` while their generated pages rendered "Redistributable" with download
buttons. The gate is only as good as the agreement between the four-or-five places a licence lives.

---

### 1.7 The source families, in plain English

#### 1.7.1 By publisher family (id prefix), from the site's own index

| family | sources | catalogued series | who |
|---|---|---|---|
| `unctad_*` | **134** | 792,379 | UN Conference on Trade and Development — one id per statistical dataset: bilateral trade, maritime transport, plastics trade, creative economy, FDI, commodity prices |
| `imf_*` | **55** | 1,288,137 | International Monetary Fund — IFS, WEO, BOP, GFS, FSI, COFER, the regional economic outlooks, and the monetary/financial statistics families, each as its own id. 54 of the 55 sit under the "IMF Terms of Use (redistribution with attribution)" licence; the 55th, `imf_commodity`, carries `dbnomics-passthrough-imf_commodity` — a licence-passthrough row left from the aggregator era |
| `fao_*` | **25** | 299,536 | FAO domain datasets — production, trade, prices, food balances, emissions, land use. (`faostat` is a separate 26th id, counted among the singletons.) |
| `unesco_*` | **7** | 264,455 | UNESCO Institute for Statistics — education, culture, film, innovation, SDG-4 |
| `worldbank*` | **3** | 7,651 total, of which `worldbank_wdi` is 1,486 catalogued series carrying 8,894,931 measured observations | World Bank Open Data, WDI, Sovereign ESG |
| `who_*` | **3** | 45,127 | World Health Organization — health workforce, road safety, SDG health |
| `stat_*` | **3** | 9,533 | Statistics Estonia, Latvia, Slovenia |
| `cepii_*` | **2** | 1,233,832 | CEPII (France) — BACI bilateral trade, the Gravity database |
| `insee_*` | **2** | 103,249 | INSEE (France) — BDM macro series, Melodi |
| singletons | **87** | — | everything else, one id each |

#### 1.7.2 By institution type

*This grouping is mine, made by hand from the catalogue's own source ids and names. It is not a field
in the data, and a source can legitimately sit in two groups (`census` is both a national statistical
office and a US federal agency). The counts below are of the ids I name, not an exhaustive partition
of the 321.*

* **National and supranational statistical offices (23 named here):** `abs` (Australia), `statcan` (Canada), `istat` (Italy),
  `cbs_nl` (Netherlands), `gus_dbw` (Poland), `ons_uk` (UK), `census` (US), `insee_bdm`/`insee_melodi`
  (France), `scb` (Sweden), `ssb` (Norway), `statfin` (Finland), `dst` (Denmark), `hagstofa`
  (Iceland), `cso` (Ireland), `stat_latvia`, `stat_estonia`, `stat_slovenia`, `bfs` (Switzerland),
  `ksh_stadat` (Hungary), `stats_nz` (New Zealand), `ipea` (Brazil), `eurostat` (EU).
* **Central banks and monetary authorities (13 central banks plus the BIS):** `ecb`, `fed_board`, `boe` (England), `boc`
  (Canada), `rba` (Australia), `riksbank` (Sweden), `norgesbank`, `snb` (Switzerland), `cnb` (Czechia),
  `bcb` (Brazil), `bcrp` (Peru), `bundesbank`, `nyfed`, plus `bis` (Bank for International
  Settlements).
* **International organisations:** the IMF (55 ids), UNCTAD (134), FAO (26), UNESCO (7), WHO (3),
  the World Bank (3), `oecd`, `ilostat` (ILO), `unsdg` (UN SDG Global Database), `un_wpp` (UN
  Population Division), `undp_hdr`, `unhcr`, `comtrade`, `adb` (Asian Development Bank), `idb`
  (Inter-American Development Bank), `pip` (World Bank Poverty and Inequality Platform).
* **US federal agencies (public domain):** `bls`, `bea`, `census`, `treasury`, `fed_board`, `eia`,
  `usda`, `noaa`, `fhfa`, `fdic`, `ofr`, `sec_edgar`.
* **Energy, environment and climate (≈9 by pillar):** `eia`, `ember`, `ei_statreview`, `gcb` (Global
  Carbon Budget), `nasa_giss`, `noaa`, `gppd` (Global Power Plant Database), `yale_epi`,
  `edgar_jrc` (EU emissions).
* **Governance, conflict, inequality and well-being (27 by pillar):** `vdem`, `wgi`, `ucdp`,
  `transparency_ti`, `wid` (World Inequality Database), `swiid`, `whr`, `oxcgrt`, `gpi`, `gti`, `ppi`,
  `etr`, `fsi_fundforpeace`, `kof_globalization`, `efw`, `gapminder`.
* **Research datasets (10 by pillar):** `maddison` / `ggdc` (year 1 CE onward), `penn_world_table` /
  `pwt`, `barro_lee`, `harvard_atlas` (Growth Lab Atlas of Economic Complexity), `cepii_baci`,
  `cepii_gravity`, `epu` (Economic Policy Uncertainty), `damodaran`.
* **Markets and finance:** `sec_edgar` (XBRL fundamentals, 13F, insider forms), `fdic`, `ofr`,
  `frankfurter` (ECB FX), `defillama` (DeFi), `imf_commodity`, `wikidata`.

#### 1.7.3 By the site's own facets

**Pillars** (the six tiles on the homepage; counts from the site index):

| pillar | sources |
|---|---|
| Trade & Development | 168 |
| Macro & National Accounts | 85 |
| Institutions & Society | 27 |
| Prices, Money & Central Banks | 22 |
| Research Datasets | 10 |
| Energy & Environment | 9 |

(The Trade pillar is dominated by the 134 UNCTAD dataset ids.)

**Regions:** Global & International 271 · Europe 26 · Americas 20 · Asia-Pacific 4. The dropdown on
`catalog.html` offers exactly these four. **A note on honesty:** "Global & International" holding 271
of 321 reflects that most sources are international organisations, not that per-country coverage is
thin — `eurostat`, `oecd` and the UNCTAD family each cover most economies on earth.

**Top categories** (a source may carry several): International Trade 145 · Macroeconomics 75 ·
Agriculture & Food 27 · Money & Banking 14 · Society & Well-Being 8 · Government Finance 8 ·
Development Indicators 6 · Environment & Climate 6 · Governance & Institutions 5 · Interest Rates 4 ·
Energy 4 · Long-Run & Historical 4 · Health 4.

**Licence distribution across the 321 listed sources** (from the site index):

| licence | sources |
|---|---|
| CC BY 3.0 IGO | 136 |
| IMF Terms of Use (redistribution with attribution) | 54 |
| CC BY 4.0 | 33 |
| Redistributable, non-commercial (provider terms verified) | 26 |
| U.S. Government Work (public domain) | 14 |
| Etalab Open Licence 2.0 | 4 |
| CC BY-NC-SA 4.0 (IEP) | 4 |
| CC0 | 4 |
| UK Open Government Licence v3.0 | 2 |
| ECB terms (attribution, no modification) | 2 |
| ~40 further per-source custom or granted licences | 1 each |

#### 1.7.4 Update cadence across the registry

From `updater/registry.yaml` (282 entries, parsed 2026-08-30):

| cadence | sources | | strategy | sources |
|---|---|---|---|---|
| weekly | 105 | | `bulk_snapshot_if_changed` | 178 |
| annual | 59 | | `overwrite_if_changed` | 61 |
| monthly | 50 | | `extend_by_date` | 24 |
| irregular | 26 | | `sdmx_delta` | 15 |
| quarterly | 23 | | `giant_changed_units` | 3 |
| daily | 14 | | `manual_vintage` | 1 |
| static | 5 | | | |

---

### 1.8 Summary of what is NOT established in this section

| claim | status | what would establish it |
|---|---|---|
| An R client for econ exists | **NOT ESTABLISHED** — the site and Zenodo record claims one; the econ repo has none (only `clients/python/econdl`); `STRATEGY.md` still lists it as future work | a published R package that calls `econdl-api`, or an `R/` tree in the repo |
| Current econ-only R2 bucket size | **NOT ESTABLISHED** — the only per-bucket split on record ("601 GB econ + 282 GB hf") is marked SUPERSEDED / wrong instrument | `npx wrangler r2 bucket info econ-data` |
| econ's own download counts, bytes served, countries, institutions, top sources | **NOT ESTABLISHED** locally — they live in D1 | one unauthenticated `GET /v1/public-stats` |
| ~~My own local re-count of catalogued series~~ | **ESTABLISHED** — the PK-range sweep completed over all 349 sources and returned **13,486,342**, matching `tools/audit_schedule_coverage.py` exactly | — |
| Whether `worldbank_pink`'s 26 residual catalogue rows exist in D1 as well as locally | **NOT ESTABLISHED** | `SELECT COUNT(*) FROM series WHERE source_id='worldbank_pink'` on `econ-catalog`, or its absence from live `/v1/sources` |
| The date behind "21,692 visitors against 603 accounts" | **NOT ESTABLISHED** — ledger entry R219 carries no date | the D1 snapshot that produced it, or the session log for R219 |
| Independent academic citations of econdatalibrary.com | **NOT ESTABLISHED** — the family's `used-by` page lists hf and ip uses only | a Scholar / OpenAlex search on DOI `10.5281/zenodo.21405120` |
| Whether the fleet is healthy as opposed to the local mirror | **PARTIALLY ESTABLISHED** — local `state.db` shows 249/249 `ok`, but that is the workstation's view; the fleet's nightly CI gate has failed 40 of 40 runs since 2026-08-13 | `/v1/last-updates` against the deployed worker |
| Why `sdmx_nso` is gated in the Worker while its local licence row says `reservable = 1` | **NOT ESTABLISHED** — one of the two is stale and I did not determine which | its verdict in `DATABASE_LICENSES_VERBATIM.md` plus its licence row read from D1 |
| Whether the "Ask the Data" assistant Worker is live and keyed | **NOT ESTABLISHED** — the code exists (1,022 lines) and the page posts to it, but publication state and `DEEPSEEK_API_KEY` are cloud state | a `POST /chat` against the live URL, or `npx wrangler secret list` for that Worker |
| Whether the live `/v1/stats` object has been refreshed since July | **ESTABLISHED as NOT refreshed** by `NUMBERS.md`'s own note, but not re-verified against the live endpoint in this session | `GET /v1/stats` and read its `as_of` |

---

*End of section 1.*
## 2. How the serving side works

*Everything between a user's click and the bytes they receive.*

This section documents the **read path** of the Econ Data Library: the Cloudflare
components, every public endpoint, how a series download physically resolves, how the
catalogue is stored and queried, how search works, how login and redistribution gating
work, what the clients do, how the website is generated and deployed, and — in detail —
how the whole thing bills.

Every claim below traces to a file in `E:\research\econfindatalibrary` (the econ repo),
`D:\research\hfdatalibrary\.claude\MISTAKES.md` / `NUMBERS.md` (the ledgers), or a
read-only command run locally against `data/catalog.db`. Where something could not be
established from the code, it says **NOT ESTABLISHED** and names what would establish it.

---

### 0. The shape of the whole thing, in one picture

```
  A person's browser                     A script / R / Stata / curl        Claude / an LLM
  (econdatalibrary.com)                  (econdl, requests, httr)           (MCP client)
          |                                        |                              |
          |  static HTML + JS                      |  HTTPS                       |  HTTPS
          v                                        v                              v
  +---------------------+              +--------------------------------------------------+
  | Cloudflare PAGES    |              |  Cloudflare WORKER   "econdl-api"                 |
  | project             |  fetch()     |  https://econdl-api.elkassabgi.workers.dev       |
  | econdatalibrary     |------------->|  api/worker/src/index.ts  (the router)            |
  | (333 static files)  |              +--------------------------------------------------+
  +---------------------+                 |            |             |            |
                                          |            |             |            |
                                   D1 CATALOG    D1 CATALOG_    D1 USERS      R2 SERIES_
                                  "econ-catalog"  CLIMATE      "hfdata-      BUCKET
                                   series,        "econ-        library-db"  "econ-data"
                                   series_fts,    catalog-      users,       series/<id>.csv
                                   source,        climate"      sessions,    (one object per
                                   license,       (noaa only)   rate_limits, series)
                                   source_counts,               econ_
                                   unit_state,                  download_log
                                   source_state,
                                   source_data_through,
                                   pageview
```

Two sentences that capture the design:

1. **Cloudflare hosts, the desktop computes.** All catalogue building, deriving,
   auditing and counting happens on the workstation against local SQLite and local
   parquet; Cloudflare only *serves*. (`D:\research\hfdatalibrary\CLAUDE.md`, section
   "CLOUDFLARE HOSTS, THE DESKTOP COMPUTES"; full record in `.claude/DESKTOP_FIRST.md`.)
2. **The Worker never parses parquet.** Every series is pre-rendered to a flat CSV
   object in R2 by the desktop pipeline; the Worker's job on a download is a single
   R2 `GET` plus some text surgery. The reasoning is written into the top of
   `api/worker/src/series.ts` — the Python resolver is the one source of truth, and
   re-implementing it in TypeScript over parquet-wasm "would be a second source of
   truth that WILL drift".

---

### 1. The Cloudflare stack — which piece holds what

#### 1.1 The four product surfaces

| Cloudflare product | Name / binding | What it holds | Where it is configured |
|---|---|---|---|
| **Workers** | `econdl-api` | All API logic. Entry point `api/worker/src/index.ts` | `api/worker/wrangler.toml` |
| **D1** (SQLite at the edge) | binding `CATALOG` → database `econ-catalog` | `series`, `series_fts`, `source`, `license`, `source_counts`, `unit_state`, `source_state`, `source_data_through`, `pageview` | same |
| **D1** | binding `CATALOG_CLIMATE` → `econ-catalog-climate` | `series` + `series_fts` rows for the sharded sources only (today: `noaa`) | same |
| **D1** | binding `USERS` → `hfdatalibrary-db` | The **family identity database**: `users`, `sessions`, `sso_clients`, `rate_limits`, `login_history`, `econ_download_log` | same |
| **R2** (object storage) | binding `SERIES_BUCKET` → bucket `econ-data` | One CSV object per series at `series/<urlencoded id>.csv`, plus the canonical parquet store and `_aqueduct/stats.json` | same |
| **Pages** | project `econdatalibrary` | The static website (333 HTML files) generated by `catalog/gen_site.py` | `.github/workflows/deploy-site.yml` |
| **Durable Object** | `ElkassabgiDataMCP` in worker `elkassabgidata-mcp` | The remote MCP server (Streamable HTTP at `/mcp`), a *client* of this API | `mcp/wrangler.jsonc` |

#### 1.2 The binding block, verbatim

From `api/worker/wrangler.toml`:

```toml
name = "econdl-api"
main = "src/index.ts"
compatibility_date = "2024-09-23"
compatibility_flags = ["nodejs_compat"]
workers_dev = true
account_id = "ce51d5c7fe3859098751b89bbebeab7a"

[[d1_databases]]
binding = "CATALOG"
database_name = "econ-catalog"
database_id = "1a6d0755-ecef-46d0-a478-46cad1cf064c"

[[d1_databases]]
binding = "CATALOG_CLIMATE"
database_name = "econ-catalog-climate"
database_id = "e34114f2-c0be-43d9-bcb5-798a3952414c"

[[d1_databases]]
binding = "USERS"
database_name = "hfdatalibrary-db"
database_id = "a396506e-e78a-4978-bc38-883056f98810"

[[r2_buckets]]
binding = "SERIES_BUCKET"
bucket_name = "econ-data"

[observability]
enabled = true
```

Two optional secrets are declared in `api/worker/src/types.ts` but not in the toml
(they are set with `wrangler secret put`): `CF_API_TOKEN` and `CF_ZONE_ID`, a Zone
Analytics read token used only to add the visitor-map layer to `/v1/public-stats`.

#### 1.3 The public hostname

The API is served from **`https://econdl-api.elkassabgi.workers.dev`**. That literal
string is what every generated site page fetches (`catalog/site/api.html:249`,
`catalog/site/download.html:203`). `workers_dev = true` in the toml and the
`[[routes]]` custom-domain block is **commented out**, so the "bind a custom domain"
step in `api/DEPLOY.md` §4 has not been taken for the API.

`AUTH_SSO_HANDOFF.md:59-62` states it as a verified fact (2026-07-17) and adds a
warning worth repeating in full:

> **Econ data worker:** `econdl-api`, live at `econdl-api.elkassabgi.workers.dev`.
> **`api.econdatalibrary.com` is NXDOMAIN and `econdatalibrary.com/v1` returns the
> SPA's index.html — never "verify" API behavior against those two hosts** (a past
> session burned itself on this).

The *website* does have its custom domain: `deploy-site.yml` asserts
`https://econdatalibrary.com/` returns 200 after every deploy. And the family's **auth**
worker is a separate deployment that *does* have one: `hfdatalibrary-api` at
`api.hfdatalibrary.com` (`AUTH_SSO_HANDOFF.md:56-58`).

**NOT ESTABLISHED (still):** the *current* attachment state of any custom domain, which
is dashboard state rather than repo state; `docs/DATABASE_REFERENCE.md:327` flags the
same gap. `npx wrangler deployments domains list` (or the dashboard's Domains & Routes
panel) would settle it.

#### 1.4 What is deliberately *not* in the stack

* **No origin server, no VM, no container.** There is nothing to patch or keep alive.
* **No public R2 URL.** A repo-wide search of `catalog/site/*.html` for `r2.dev` or a
  `pub-<hash>` bucket host returns nothing. Every byte of data reaches a user through
  the Worker, which is what makes the login gate and the redistribution gate
  enforceable at all. `api/DEPLOY.md` §4 states the rule: custom domain, "NOT
  *.workers.dev / r2.dev".
* **No server-assembled ZIP.** `api/CONTRACT.md` design rule [w10]: a bundle is a
  *manifest the client fans out*, never a zip the Worker streams, so a 500-series
  bundle can never trip the Worker's 50-subrequest cap.

---

### 2. Every public endpoint

The router is a flat `if` chain in `api/worker/src/index.ts`. Non-`GET` methods get
`405`; `OPTIONS` gets a `204` CORS preflight (`access-control-allow-origin: *`,
methods `GET, OPTIONS`, max-age 86400).

#### 2.1 The endpoint table

| Path | Handler file | Reads | Returns | Cached? |
|---|---|---|---|---|
| `GET /` , `/v1`, `/v1/` | `index.ts` (inline) | nothing | Service index: name, version, list of 8 endpoints, pointer to `api/CONTRACT.md` | `max-age=300` (browser only) |
| `GET /v1/catalog` | `catalog.ts` | `CATALOG` + `CATALOG_CLIMATE` | `{total, limit, offset, catalog_coverage, results[]}` | **Yes — edge cache, `s-maxage=21600` (6 h)**, browser `max-age=300`. Only 200s are cached |
| `GET /v1/sources` | `sources.ts` | `CATALOG` (+ 1 existence probe on the shard) | `{total, sources:[{source,name,homepage,license{},freshness{}}]}` | `max-age=300` browser only |
| `GET /v1/last-updates` | `lastUpdates.ts` | `CATALOG` (`unit_state` ⟕ `source_state`) | `{generated, datasets:[…]}` per dataset unit | `max-age=300` browser only |
| `GET /v1/stats` | `index.ts` (inline) | `source_counts` on both D1s + R2 object `_aqueduct/stats.json` | Headline census figures + live `catalog_entries` | **Yes — edge cache, `s-maxage=21600` (6 h)** |
| `GET /v1/public-stats` | `publicStats.ts` | `USERS` (5 aggregate queries) + `CATALOG.source` + optionally the Cloudflare GraphQL analytics API | Users, countries, institutions, download counts, visitor map | `max-age=300` browser only |
| `GET /v1/series/{id}.metadata.json` | `metadata.ts` | `CATALOG` or shard (`series`), `CATALOG` (`source`, `license`, `unit_state`) | Series metadata + licence block + `csv_url` | `max-age=300` browser only |
| `GET /v1/series/{id}.csv` | `series.ts` | denylist → auth (`USERS`) → `CATALOG`/shard (`series`) → **R2** | `text/csv` with a citation header (or bare with `?raw=1`) | `max-age=300` header set, but **not** put in the edge cache by the Worker |
| `GET /v1/bundle` | `bundle.ts` | `CATALOG`/shard, one `series` lookup per requested id | Frictionless `datapackage.json` **manifest** (no data) | `max-age=300` browser only |
| `GET /v1/pv` | `pageview.ts` | writes `CATALOG.pageview` | 1×1 transparent GIF, always | **`no-store, no-cache, must-revalidate`** |
| `GET /v1/pv/report` | `pageview.ts` | `CATALOG.pageview` | Page-view counts by path and by day | no cache-control set |
| anything else | — | — | `404 {"error":"not_found","detail":"no route for …"}` | — |

`/v1/pv` and `/v1/pv/report` are deliberately **absent from the service index** at `/`
— they are a hidden beacon, not part of the advertised contract.

#### 2.2 Caching, precisely

Two mechanisms, easy to confuse:

1. **The default response headers.** `util.ts` sets on *every* JSON response
   (`JSON_HEADERS`) and every CSV response (`CSV_HEADERS`):
   `cache-control: public, max-age=300` plus `access-control-allow-origin: *`.
   That is a *browser/intermediary* instruction; it does not put anything in
   Cloudflare's cache.
2. **The Worker's own edge cache**, `caches.default`, used on exactly two routes:
   * `/v1/catalog` — `index.ts` lines 62-75. Cache key is the full request URL. On a
     miss it calls `handleCatalog`, and **only if the status is 200** rewrites
     `cache-control` to `public, max-age=300, s-maxage=21600` and stores it with
     `ctx.waitUntil`. The 400 "offset too deep" refusals and 5xx errors are never
     cached.
   * `/v1/stats` — same pattern, same 6-hour `s-maxage`.

   Both were added as **cost fixes**, not performance tuning; the comments say so
   explicitly ("2026-08-15 cost incident: a crawler paging one source drove 130B D1
   rows read in a day"). See §10.

An observation worth knowing, stated as fact rather than alarm: the authenticated CSV
download inherits `cache-control: public, max-age=300` from `CSV_HEADERS`. The Worker
itself never places a series response in `caches.default`, so this only affects
browsers and any intermediary proxy that ignores the absence of `private`.

#### 2.3 The honest-status contract

`api/CONTRACT.md` pins the status codes and `api/worker/src/util.ts` is the single
place they are emitted, so no handler can invent its own. The governing rule, from
the top of `util.ts`:

> `200` only with ≥1 row; `404` unknown id; `501` not_migrated; `502` resolver_empty;
> `400` unsupported_filter. NEVER an empty 200, NEVER a fabricated date.

| HTTP | `error` code | Meaning | Emitted by |
|---|---|---|---|
| 400 | `bad_request` | Malformed date, bad `format=`, conflicting `geo` | `util.badRequest` |
| 400 | `unsupported_filter` | A filter the store cannot honour (`freq=`, `unit=`, `geo=` on a non-projection source) — refuses to return a silently-unfiltered series | `util.unsupportedFilter` |
| 400 | `unsupported_language` | `?lang=` outside the six loaded languages | `util.reqLang` |
| 400 | `offset_too_deep` | `offset > 100,000` on `/v1/catalog`, with a pointer to `/v1/bundle` | `catalog.ts` |
| 401 | `auth_required` / `invalid_key` | No credential / bad credential on a data download | `auth.ts` |
| 404 | `not_found` | Series id is not in the catalogue | `util.notFound` |
| 404 | `geo_not_found` | Grouped series exists but holds no rows for that economy — and the message lists economies it *does* hold | `series.ts` |
| 405 | `method_not_allowed` | Anything but GET/OPTIONS | `index.ts` |
| 429 | `rate_limited` | Over 100 downloads/min (500 for VIP) | `auth.ts` |
| **451** | `not_redistributable` / `non_redistributable` | The licence forbids re-hosting. **This is the redistribution gate** | `index.ts`, `catalog.ts` |
| 500 | `internal_error` | Caught exception; message only, never a stack as a 200 | `index.ts` |
| 501 | `not_migrated` | The source has no at-rest resolver — loud, actionable | `util.notMigrated` |
| 502 | `data_unavailable` | Source *is* migrated but this series' R2 object was never published | `util.dataUnavailable` |
| 502 | `resolver_empty` | Object exists but yields zero rows after filtering | `util.resolverEmpty` |
| 503 | `stats_unavailable` | `_aqueduct/stats.json` absent from R2 — refuses to serve compiled-in numbers | `index.ts` |

The distinction between `501`, `502 data_unavailable` and `502 resolver_empty` is not
pedantry. `series.ts` records why: flipping a source's `SUPPORTED_SOURCES` flag before
its CSVs exist "turns a 501 into a 404 and a 404 says the series does not exist".

#### 2.4 Internationalisation

`?lang=` is accepted on `/v1/catalog` and `/v1/series/{id}.metadata.json`. Supported
set, from `util.ts::SUPPORTED_LANGS`: **`en, ar, es, fr, ru, zh`** — six languages,
all of them titles the *producer itself* publishes (World Bank `/v2/<lang>/`, IMF/ILO
SDMX `xml:lang`), stored at `series.metadata.titles[<lang>]`. Nothing is machine
translated. Rules:

* absent or `lang=en` → the response is byte-identical to the pre-i18n shape;
* a supported lang → `title` is swapped for the official label, `title_en` preserves
  the English one in `metadata.json`, and a `lang` key is echoed;
* an unsupported lang → **400**, never a silent English fallback.

Search itself is English-indexed; `?lang=` localises the *display* title only.

---

### 3. The data plane — how a series download actually resolves

This is the hot path. Follow `GET /v1/series/worldbank_wdi%3ANY.GDP.MKTP.CD.csv`
through the code.

#### 3.1 The id is the id

There is no `/provider/dataset/series` path split. `api/CONTRACT.md` explains why: the
catalog id grammar is `provider:tail` with a **variable** number of `:` segments per
source (`bls:CUUR0000SA0`, `worldbank_wdi:AG.CON.FERT.PT.ZS`,
`worldbank:NY.GDP.MKTP.CD:AFE`, `penn_world_table:rgdpe:USA`), so mapping `:` → `/`
is ambiguous. The URL therefore carries the **exact catalogue id, URL-encoded**:

```
/v1/series/{urlencoded series_id}.csv
/v1/series/{urlencoded series_id}.metadata.json
```

`index.ts` splits on the `.csv` / `.metadata.json` suffix and `decodeURIComponent`s
the rest. An empty id is a 400.

#### 3.2 The nine steps of a download

| # | Step | Code | Failure mode |
|---|---|---|---|
| 1 | **Redistribution gate** | `index.ts` → `denylist.isGated(id)` | `451 not_redistributable` |
| 2 | **Auth gate** | `index.ts` → `auth.requireDownloadAuth(request, env)` | `401` / `429` |
| 3 | **Catalogue membership** | `series.ts` → `dbForSeries(env,id).prepare(SELECT_SERIES)` — `SELECT * FROM series WHERE series_id = ?`, a PK seek | `404 not_found` (after trying the geo alias, §3.6) |
| 4 | **Resolver coverage** | `supportedSources(env).has(sourceOf(id))` | `501 not_migrated` |
| 5 | **Filter validation** | `freq=`/`unit=` rejected; `geo=` only for projection sources; `format` ∈ {full, filtered}; `from`/`to` must match `/^\d{4}-\d{2}-\d{2}$/` | `400` |
| 6 | **R2 GET** | `env.SERIES_BUCKET.get(objectKey(seriesId))` | `502 data_unavailable` if `null` |
| 7 | **Decompress if needed** | `obj.httpMetadata?.contentEncoding === "gzip"` → pipe through `DecompressionStream("gzip")` | — |
| 8 | **Project + window** | geo filter (`filterGeoRows`), then `applyDateWindow` on `from`/`to` | `404 geo_not_found`; `502 resolver_empty` if 0 rows |
| 9 | **Emit** | `?raw=1` → bare CSV with the R2 ETag; otherwise prepend the citation header. Then `logDownload` records user, series, ip, channel and byte count | — |

The order in step 1–2 matters and is deliberate: **compliance before authentication**.
A gated series returns 451 to an anonymous caller; it never leaks the fact that a login
would have worked.

#### 3.3 The R2 object key format — and the encoding trap

The key is:

```
series/<RFC-3986-percent-encoded series_id>.csv
```

The **writer** is Python. `tools/derive_csv_bulk.py::csv_key` is the single definition:

```python
def csv_key(prefix: str, source: str, series_key: str) -> str:
    return f"{prefix}/{urllib.parse.quote(f'{source}:{series_key}', safe='')}.csv"
```

`urllib.parse.quote(..., safe="")` percent-encodes everything outside
`A-Za-z0-9-_.~`. JavaScript's `encodeURIComponent` does **not** — it leaves five of
those characters literal: `!` `'` `(` `)` `*`. So the reader has to be aligned to the
writer, and `series.ts::objectKey` does exactly that:

```ts
function objectKey(seriesId: string): string {
  const rfc3986 = encodeURIComponent(seriesId).replace(
    /[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  return `series/${rfc3986}.csv`;
}
```

This is not theoretical. The comment above it records the measurement (2026-07-30):
**60,993 catalogued series across 12 sources** contain one of those five characters,
**54,745 of them in `un_wpp` alone**. Before the fix, 46 of 46 sampled objects existed
under the Python spelling and 0 of 46 under the JavaScript one, and the live API
returned `502 "the at-rest object for this series is not published yet"` for data it
was holding. A bounded scan of 60,000 keys under `series/` found no literal
`! ' ( ) *` in any key, which is why the reader was aligned to the writer rather than
60,993 objects being re-derived.

A second, independent confirmation of the key layout: the R2 LIST incident recorded in
`NUMBERS.md` (2026-08-30) reports the prefix `series/noaa%3A` covering **3,138,169
objects** — `%3A` being the encoded colon.

#### 3.4 Objects are gzipped at rest

`core/derive_csv.py` PUTs with `ContentEncoding="gzip"` (lines 323, 354) and
`gzip.compress(body, mtime=0)` so the bytes are reproducible. The Worker detects it
from the object's own metadata and decompresses transparently:

```ts
const gzipped = obj.httpMetadata?.contentEncoding === "gzip";
const text = gzipped
  ? await new Response(obj.body.pipeThrough(new DecompressionStream("gzip"))).text()
  : await obj.text();
```

The rationale in `series.ts`: the Worker has to materialise the text anyway (date
window + citation header), so at-rest compression is invisible to clients, and the
edge still re-compresses the *response* per each client's `Accept-Encoding`. Plain
objects keep working, so the fleet could migrate gradually with this reader deployed
first. The motive was cost — "numeric CSVs compress 5-10x and R2 storage is the
bill's dominant line" (cost plan 2026-08-18).

#### 3.5 `SUPPORTED_SOURCES` — the migrated-source allowlist

`api/worker/src/util.ts` exports a literal array of source ids that have an at-rest
resolver. Measured today by parsing the file: **323 ids**. Anything not in it returns
`501 not_migrated`, loudly, rather than a confusing 404.

Two things to know about this list:

* It can be **overridden at runtime** by the `SUPPORTED_SOURCES` environment variable
  (comma-separated) — `util.ts::supportedSources(env)`. Unset, the compiled-in array
  wins. The `[vars]` block in `wrangler.toml` leaves it commented out.
* Its header comment says *"The 191 sources with an at-rest resolver"*, which is
  **stale**: the array now holds 323. The array is authoritative; the prose is not.

Two ids appear in **both** `SUPPORTED_SOURCES` and the denylist — `dbnomics` and
`worldbank_pink` — so a data request for either resolves the allowlist and is then
refused with 451. (Measured by intersecting the two parsed sets.)

For the count of what is genuinely *served*, `NUMBERS.md` (2026-08-30) records **322
sources served (catalogued AND in util.ts)**, measured with
`py tools/audit_schedule_coverage.py` and independently confirmed the same day by
`SELECT COUNT(*) FROM source_counts` on both D1 databases (321 + 1 = 322). That is a
different definition from "ids listed in util.ts", which is why the two numbers differ
by one; both are reported here rather than reconciled by assertion.

#### 3.6 Grouped series and per-geo projection

Some sources catalogue **one id per indicator** whose CSV object carries every
economy's rows (row ids of the form `WDI:<CODE>:<GEO>`). A user naturally asks for
indicator × economy. `api/worker/src/geoProjection.ts` makes that work without minting
a single new catalogue row or R2 object:

```ts
export const GEO_PROJECTION_SOURCES: Record<string, string> = {
  worldbank: "worldbank_wdi",
  worldbank_wdi: "worldbank_wdi",
};
```

* An uncatalogued 3-part id like `worldbank:DT.DOD.DECT.CD:LMY` misses the PK lookup,
  falls through to `geoAlias()`, resolves to `worldbank_wdi:DT.DOD.DECT.CD` and filters
  the object's rows to `LMY`.
* `worldbank_wdi:<IND>?geo=LMY` does the same thing through the query parameter.
* Four World Bank 2-character income-group codes are translated to the 3-character form
  the store actually holds: `XD→HIC`, `XM→LIC`, `XN→LMC`, `XT→UMC`. The comment records
  both measurements behind that map — the publisher's own `/v2/country` list (295
  entries, fetched 2026-08-30) and a count across all 1,486 grouped parquets showing
  34,703 / 31,498 / 36,775 / 34,475 rows under HIC/LIC/LMC/UMC and **zero** under
  XD/XM/XN/XT.
* Zero matching rows is an honest 404 that names the economies the object *does*
  contain. The comment records that 1,984 indicator/code combinations reach a 404 or
  conflict message on this path (2026-08-30 review).
* The redistribution gate runs on **both** spellings — the alias is checked in
  `index.ts` and the canonical id again in `series.ts` — because a carve-out must cover
  sibling ids (ledger R32).
* An ETag is attached only on the `?raw=1` path and only when no geo filter applied,
  because "the R2 ETag describes the FULL stored object".

#### 3.7 The citation header

Unless `?raw=1` is passed, every CSV is prefixed with a `#`-commented block built from
the same `source` and `license` rows the metadata endpoint uses. `#` lines are skipped
by `pandas.read_csv(url, comment='#')` and R's `comment.char='#'`, so pipelines are
unaffected, but anyone who *opens* the file — including a data provider checking
attribution — sees:

```
# ============================================================================
#  DATA CITATION — please credit the original source in any use or publication.
#  By downloading from the Elkassabgi Data Library you agreed to cite this source.
#
#  Series:    <title>  [<series_id>]
#  Source:    <source.attribution>
#  License:   <name> — NON-COMMERCIAL USE ONLY (honor it); attribution required;
#             SHARE-ALIKE …; NO DERIVATIVES …
#  Dataset:   https://data.iadb.org/dataset/<slug>     (idb only — IDB's written
#                                                       permission requires it)
#  Homepage:  <source.homepage>
#  Terms:     <source.terms_url>
#  Cite as:   <citation_long | citation | citation_short from series.metadata>
#  Provided:  Elkassabgi Data Library — econdatalibrary.com
#  (Pipelines: pandas pd.read_csv(url, comment='#'), or append ?raw=1 for bare CSV.)
# ============================================================================
```

The ShareAlike and NoDerivatives lines were added because they were missing: the
comment in `series.ts` records that **2,866,900 served series carry one or the other —
WID alone is 2,465,197 of them under CC BY-NC-SA 4.0** — and downloaders were being
told only "non-commercial; attribution required". ShareAlike is detected from the
licence *name* (`/(^|[-_])sa([-_.]|\d|$)/` — anchored, because an unanchored `sa` test
is how ledger R112 produced three wrong answers) since the schema has no
`share_alike` column.

#### 3.8 Bundles

`GET /v1/bundle?ids=…` or `?source=…` returns a **Frictionless `datapackage.json`
manifest**, never data. `bundle.ts`:

* Validates each id against the catalogue with one PK lookup, grouping by source.
* Emits one `resource` per source whose `path` is the list of stable per-series CSV
  URLs the client will fetch itself.
* Attaches `econdl:provenance` per resource (source name, homepage, attribution,
  terms_url, licence block, generated citation) and a de-duplicated top-level
  `licenses[]`.
* Puts unresolvable ids under **`econdl:unresolved`** with a machine reason
  (`not_redistributable` / `not_found` / `not_migrated`) — loud, never silently
  dropped.
* Omits `bytes` and `hash`, because the Worker never reads the rows and "never faked".
* Carries `econdl:fanout_note` stating plainly that the client assembles the zip.

`?source=` on a denylisted source is refused before any lookup. Gated ids inside an
`ids=` list are reported as unresolved rather than advertised as URLs — the manifest
must not point at something the data endpoint will 451.

The browser implements exactly this contract: `catalog/site/download.html` loads JSZip
from a CDN, fetches each `/v1/series/{id}.csv`, adds a `MANIFEST.csv`, and calls
`zip.generateAsync({type:'blob'})` locally. If JSZip fails to load it degrades to
saving individual files rather than failing.

---

### 4. The catalogue in D1

#### 4.1 What D1 is

D1 is Cloudflare's managed SQLite. That single fact is load-bearing for this codebase:
`api/worker/src/sql.ts` opens by saying every query in it is "the byte-for-byte query
the local SQLite path runs", so the Worker, the Python dev shim (`api/devserver.py`)
and the `econdl` client cannot silently drift. `core/export_d1.py` dumps the local
`data/catalog.db` to a portable `.sql` file, verifies it by replaying it into a fresh
in-memory SQLite, and that file is loaded with `wrangler d1 execute --remote --file=`.

#### 4.2 The schema

Read from the local `data/catalog.db` (11,906,957,312 bytes; the D1 copy is built from
it):

```sql
CREATE TABLE license (
  license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT);

CREATE TABLE source (
  source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT,
  attribution TEXT, terms_url TEXT);

CREATE TABLE series (
  series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,
  last_updated TEXT, metadata TEXT);

CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
```

Note what is **absent from `series`**: any index other than the primary key. There is
no index on `source_id`. That is not an oversight — see §4.4.

Tables that exist only in D1, created by the sync scripts rather than by
`data/catalog.db`:

| Table | Written by | Purpose |
|---|---|---|
| `source_counts(source_id PK, n)` | `core/sync_catalog_d1.py` (lines 230-235) | One row per source; the `total` for a browse page without a `COUNT(*)` |
| `unit_state`, `source_state` | `core/sync_state_d1.py`, from `data/_aqueduct/state.db` | Freshness — status, cadence, `last_success_utc`, `upstream_vintage`, `last_obs_date`, `obs_count` |
| `source_data_through(source_id, data_through)` | `core/sync_state_d1.py` (lines 168-177) | `MAX(series.end_date)` per source, read from the local catalogue and stamped at sync time (task #138) |
| `pageview(path, day, hits)` | the `/v1/pv` beacon itself | Page-view counter, `ON CONFLICT(path,day) DO UPDATE` |

Local measurements taken today with read-only SQLite against `data/catalog.db`:

| Measure | Value | Command |
|---|---|---|
| `source` rows | **349** | `SELECT COUNT(*) FROM source` |
| `license` rows | **71** | `SELECT COUNT(*) FROM license` |
| sources whose licence has `reservable = 0` | **20** | `SELECT COUNT(*) FROM source s JOIN license l ON l.license_id=s.license_id WHERE l.reservable=0` |
| sources whose licence has `reservable = 1` | **329** | same with `= 1` |

From `NUMBERS.md` (each row carries its own instrument):

| Measure | Value | Instrument | Date |
|---|---|---|---|
| D1 `series` rows, both databases | 13,486,284 | `wrangler d1 execute <db> --remote --command "SELECT COUNT(*) FROM series"`, summed | 2026-08-24 |
| Series served (audit) | 13,486,342 | `py tools/audit_schedule_coverage.py` | 2026-08-30 |
| D1 `series_fts` rows, both databases | 26,981,683 (2.00×) | same, on `series_fts` | 2026-08-24 |
| `econ-catalog` size | 8.34 GB of the 10 GB per-database ceiling, 10,348,426 rows, ~806 B/row | `SELECT SUM(n) FROM source_counts` + its `meta.size_after` | 2026-08-28 |

#### 4.3 The primary-key layout — and why it is the whole cost story

**Every `series_id` begins with its `source_id` followed by a colon.** `denylist.ts`
and `util.ts` both derive the source by taking everything before the first `:`:

```ts
export function sourceOf(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i === -1 ? seriesId : seriesId.slice(0, i);
}
```

This invariant was **verified, not assumed**: ledger R430 records "0 violations in
9,214,639 + 3,137,871 rows" across both databases (2026-08-15).

Why it matters: `series_id` is the PRIMARY KEY of a `WITHOUT ROWID`-style text-keyed
table, so SQLite keeps an autoindex in `series_id` order. Because the source id is a
*prefix* of the key, the range `[src + ':', src + ';')` (`;` is the codepoint after
`:`) selects exactly that source's rows **in key order**, walking an index that already
exists.

The contrast is stark and measured:

| Query shape | Rows read | Why |
|---|---|---|
| `WHERE series_id = ?` on `series` | **1 row, 0.335 ms** | Primary-key seek (measured, ledger R492) |
| `WHERE source_id = ? ORDER BY series_id LIMIT ? OFFSET ?` | **4.93 M rows per page** on `wid` | No index on `source_id`; SQLite sort-scans the source's entire row set every page (R430) |
| `WHERE series_id >= 'wid:' AND series_id < 'wid;' ORDER BY series_id LIMIT ? OFFSET ?` | **offset + limit entries** | Rides the PK autoindex, already in the right order |
| `COUNT(*) WHERE source_id = ?` | **2.47 M rows, every request** | Same missing index (R430) |
| `WHERE series_id = ?` on `series_fts` | **23,843,482 rows, 11-16 s** | `series_id` is declared `UNINDEXED` in the FTS5 table — full scan (R492) |

And the reason the obvious fix is unavailable: **D1 cannot build the composite index.**
`CREATE INDEX` on the 9.2 M-row `series` table dies with `SQLITE_NOMEM` on D1. The
comment in `sql.ts` states it; ledger R430 promotes it to a rule: *"design key layouts
so the PRIMARY KEY carries the access pattern"*.

#### 4.4 The real queries, from `api/worker/src/sql.ts`

```sql
-- Single-row lookups (PK seeks; effectively free)
SELECT * FROM series  WHERE series_id  = ?;   -- SELECT_SERIES
SELECT * FROM source  WHERE source_id  = ?;   -- SELECT_SOURCE
SELECT * FROM license WHERE license_id = ?;   -- SELECT_LICENSE

-- Browse one source: the COST-CRITICAL statement, PK-range form.
-- Binds: lo = src+':', hi = src+';', limit, offset
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series WHERE series_id >= ? AND series_id < ? ORDER BY series_id LIMIT ? OFFSET ?;

-- The page total: one row, never a COUNT
SELECT n FROM source_counts WHERE source_id = ?;          -- BROWSE_SOURCE_COUNT_CACHED
SELECT COUNT(*) AS n FROM series WHERE source_id = ?;     -- BROWSE_SOURCE_COUNT (fallback only)

-- Full-text search (primary path)
SELECT s.series_id, s.source_id, s.title, … FROM series_fts f
  JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? <denylist exclusions> LIMIT ? OFFSET ?;

-- LIKE fallback
SELECT … FROM series WHERE (title LIKE ? OR series_id LIKE ?) <exclusions> LIMIT ? OFFSET ?;

-- Every source with data + licence + freshness (one statement, drives /v1/sources)
SELECT s.source_id, s.name, s.homepage, s.license_id, s.attribution, s.terms_url,
       l.name AS license_name, l.url AS license_url,
       l.reservable, l.commercial_ok, l.attribution_required, l.no_modify,
       ss.cadence, ss.status, ss.last_success_utc, dt.data_through
FROM source s
LEFT JOIN license l              ON l.license_id = s.license_id
LEFT JOIN source_state ss        ON ss.source_id = s.source_id
LEFT JOIN source_data_through dt ON dt.source_id = s.source_id
WHERE EXISTS (SELECT 1 FROM series se WHERE se.source_id = s.source_id)
ORDER BY s.source_id;

-- Freshness (canonical, copied verbatim from CONTRACT.md)
SELECT u.source_id, u.unit_id, u.status, u.last_success_utc, u.upstream_vintage,
       u.last_obs_date, u.obs_count, s.cadence
FROM unit_state u LEFT JOIN source_state s ON s.source_id = u.source_id
ORDER BY u.source_id, u.unit_id;
```

The `LEFT JOIN`s in `SELECT_SOURCES` are deliberate: a source with no licence row or no
freshness row still appears, with `null` fields, rather than being dropped. And
`freshness` is emitted as `null` in full when the source has no `source_state` row at
all — "honest absence, not a fabricated `{null,null,null}`" (`sources.ts`).

#### 4.5 Sharding across two D1 databases

D1 has a **10 GB per-database ceiling**. `util.ts` records that the primary measured
9.35 GB of it, so a shard was created (task #45):

```ts
export const SHARDED_SOURCES: ReadonlySet<string> = new Set(["noaa"]);

export function dbFor(env: Env, source: string | null | undefined): D1Database {
  return source && SHARDED_SOURCES.has(source) ? env.CATALOG_CLIMATE : env.CATALOG;
}
export function dbForSeries(env: Env, seriesId: string): D1Database {
  return dbFor(env, sourceOf(seriesId));
}
```

The split is **only `series` and `series_fts`**. `source`, `license`, `unit_state` and
`source_state` rows for `noaa` stay in the primary, so `/v1/sources` and provenance
need no cross-database union — which is fortunate, because **D1 cannot join across
databases at all**.

That constraint produces three consequences visible in the code:

1. **Source-scoped queries** route through `dbFor()` and hit one database.
2. **Global queries** (unscoped search and browse, and the `/v1/stats` count) must run
   on **both** databases and merge in JavaScript, or sharded sources silently vanish.
   `catalog.ts` fetches the first `offset + limit` window from each, concatenates,
   re-sorts by `series_id` for the browse path, slices the window, and sums the counts.
3. **`/v1/sources` needed a special case.** `SELECT_SOURCES` keeps a source only if the
   *primary* holds series rows for it, so `noaa` disappeared from the list while being
   fully served. `sources.ts` now runs a `SELECT 1 … LIMIT 1` existence probe against
   the shard and, if it hits, fetches the descriptive row from the primary with
   `SELECT_SOURCE_JOINED` (the same joins, without the `EXISTS` filter). The comment
   records the symptom: "`/v1/sources` reported 318 while noaa was fully served".

The noaa row count appears twice with slightly different values in different files —
`wrangler.toml` says **3,137,871 catalogue rows**, `sources.ts` says **3,138,211**, and
`NUMBERS.md` reports **3,138,169 R2 objects** under the `series/noaa%3A` prefix
(2026-08-30). These are measurements taken on different days as the source grew; the
authoritative live figure is `SELECT n FROM source_counts WHERE source_id='noaa'` on
`econ-catalog-climate`.

---

### 5. Search — how `/v1/catalog?q=` works

`api/worker/src/catalog.ts`. Parameters: `q=`, `source=`, `limit=` (default 50,
**max 500**), `offset=` (**max 100,000**), `lang=`.

#### 5.1 The four paths

```
                     q given?
                    /        \
                 yes          no
                  |            \
        ┌─────────┴──────┐      \
   source given?          \      source given?
    /        \             \      /        \
  yes         no            \   yes          no
   |           |             |    |            |
SEARCH_FTS   SEARCH_FTS      |  BROWSE_      BROWSE_ALL
_SOURCE      (both DBs,      |  SOURCE       (both DBs,
   |          merged)        |  (PK range)    merged, re-sorted)
   └───── FTS empty/threw? ──┘
              |
       LIKE fallback
```

#### 5.2 The source-id shortcut

Before anything else, if `q` (trimmed, lowercased) **is itself a supported source id**
and no `source=` was given, the handler converts it into a browse:

```ts
if (supportedSources(env).has(cand)) { src = cand; q = null; }
```

The comment explains a subtle failure this fixes. `ftsOk` is set from
`results.length > 0`, which means "FTS returned *something*", not "FTS answered *this*
query". Unscoped, `MATCH 'wid'` also matched 10 unrelated `unctad_rfia` rows, so a
non-empty-from-anywhere result suppressed the LIKE fallback for everybody else. It was
masked only because `wid`'s index still held 7,395,591 code-as-title rows. Matching the
id up front is both cheaper (it routes to the PK-range browse instead of a
leading-wildcard scan over millions of rows) and more correct.

#### 5.3 FTS5 `MATCH` — the primary path

`series_fts` is `fts5(series_id UNINDEXED, title, geography)`, so a `MATCH` query
consults the inverted index over `title` and `geography` and is fast. The join back to
`series` is by primary key, so it is a seek per hit.

`ftsOk` is only set true when the query returned at least one row *and* the count query
succeeded. D1 raises on a malformed MATCH expression (an unbalanced quote, a stray
operator), which is caught:

```ts
} catch {
  ftsOk = false; // malformed FTS query -> LIKE fallback below
}
```

This mirrors the Python `try/except OperationalError` in `core/catalog.py`, deliberately.

#### 5.4 The LIKE fallback — and why it is slow

```sql
SELECT … FROM series WHERE (title LIKE ? OR series_id LIKE ?) … LIMIT ? OFFSET ?
```

with both parameters bound to `%<q>%`. The leading `%` makes the pattern
**unanchored**, so no index can serve it — not the `series_id` primary key (a prefix
index cannot answer "contains"), and certainly not `title` (which has no index at all).
SQLite therefore reads **every row of `series`** and tests each one. On a 13.5 M-row
table that is a full scan, and `SEARCH_LIKE_COUNT` runs the identical scan a second
time to produce `total`.

In D1's billing model that is 13.5 M rows read per statement, twice per fallback
request — the same shape as the incident described in §10. It is a *fallback*: it fires
only when FTS matched nothing or threw. Keeping it rare is exactly why the source-id
shortcut in §5.2 exists.

#### 5.5 The offset cap

```ts
const MAX_OFFSET = 100_000;
if (offset > MAX_OFFSET) return json({ error: "offset_too_deep",
  detail: "offset is capped at 100000; to enumerate a whole source use " +
          "/v1/bundle?source= (all series ids) or the bulk parquet downloads" }, 400);
```

The reason, from the comment: `OFFSET N` is **O(N) rows read no matter the query plan**,
so an unbounded crawl of a multi-million-series source costs real money per page.
100 k covers every human browse; a whole-source consumer gets an honest pointer to the
bulk surface instead of a silent bill.

#### 5.6 Cross-shard merging, and its cost shape

For unscoped queries the handler asks each database for the first `offset + limit`
rows, concatenates primary-then-shard, and slices. The comment is candid about the
trade: "Deep offsets cost proportionally on both DBs; limit is capped at 500 and real
offsets are shallow, so this stays bounded."

#### 5.7 The two layers of redistribution filtering

Search results are filtered **twice**, on purpose:

* **In SQL.** `sql.ts` builds `AND s.source_id NOT IN (…)` from `denylist.ts`'s
  `NON_REDISTRIBUTABLE`, plus `AND s.series_id NOT LIKE '<src>:<ind>:%'` for each
  series-level carve-out. Because the denylist is a generated compile-time constant of
  static identifiers, there is no injection surface.
* **In JavaScript.** After mapping, `catalog.ts` filters again with
  `!NON_REDISTRIBUTABLE.has(source) && !isSeriesCarvedOut(id)` — "belt-and-suspenders
  over the SQL exclusion (covers the browse path)", since `BROWSE_SOURCE` is a bare PK
  range with no exclusion clause.

And a direct `?source=<gated>` ask returns **451**, not an empty result — an honest
refusal instead of a lie by omission.

#### 5.8 A stale string worth knowing about

`catalog.ts:19` defines:

```ts
const COVERAGE = "series-level for 33 sources; source-level for the rest";
```

and every `/v1/catalog` response carries it as `catalog_coverage`. The "33" traces to
`api/CONTRACT.md`'s original text and has not been updated; `SUPPORTED_SOURCES` now
holds 323 ids and `NUMBERS.md` measures 322 sources served. The field's *purpose* is
sound — it exists so absence is never read as nonexistence — but the number in it is
out of date. Fixing it is a one-line change plus a redeploy.

---

### 6. Auth and SSO

#### 6.1 One account for the whole family

`hfdatalibrary`'s users database **is** the identity provider for every Elkassabgi data
library. The econ Worker binds it directly as `USERS` → `hfdatalibrary-db`, so an
existing hf API key works on econ with no registration and no migration. The
`types.ts` comment states it as an owner directive (PLAN.md §6).

Econ writes only to *its own* tables inside that shared database: `econ_download_log`,
and the `econ:download:` namespace of `rate_limits`. hf's download counters are never
inflated by econ traffic, and vice versa.

Three deployments participate, and it helps to keep them apart:

| Deployment | Host | Role |
|---|---|---|
| `econdl-api` | `econdl-api.elkassabgi.workers.dev` | This API. **Validates** credentials against the shared DB; mints nothing |
| `hfdatalibrary-api` | `api.hfdatalibrary.com` | The auth worker — accounts, 2FA, ORCID/Google, Turnstile; owns `hfdatalibrary-db` (`AUTH_SSO_HANDOFF.md:56-58`) |
| The IdP + SDK | `accounts.elkassabgidata.com` | Issues family sessions and serves `/sdk/ekd-sso.js` to the browser |

#### 6.2 What is gated, and what is not

| Open, no credential | Requires a credential |
|---|---|
| `/v1/catalog` (search + browse) | `/v1/series/{id}.csv` — **data downloads only** |
| `/v1/sources` | |
| `/v1/last-updates` | |
| `/v1/series/{id}.metadata.json` | |
| `/v1/stats`, `/v1/public-stats` | |
| `/v1/bundle` (the manifest) | |
| `/v1/pv`, `/v1/pv/report` | |

"Browse free, download with the (free) family key" — `auth.ts` header.

#### 6.3 Two credential types, tried in order

`auth.ts::requireDownloadAuth`:

**1. Family access token (`edl_at`) — tried first.** A Bearer token minted by the
identity provider at `accounts.elkassabgidata.com`. The raw token is never stored: the
IdP stores its SHA-256 as `sessions.id`, so the Worker hashes what it receives and
looks that up:

```sql
SELECT u.id, u.email, u.is_vip, u.is_active, s.audience
FROM sessions s JOIN users u ON s.user_id = u.id
WHERE s.id = ? AND s.kind = 'family_access' AND s.expires_at > datetime('now')
```

Then two further checks, which are the reduced-scope part:

* the session's `audience` must equal the request's `Origin` header exactly; and
* that origin must be a **registered, `active`** row in `sso_clients`.

So a token minted for one family site cannot be replayed against another.

**2. API key — the unchanged original path.** Extracted from the `X-API-Key` header, or
failing that the `?api_key=` query parameter (same ergonomics as hf's API):

```sql
SELECT id, email, is_vip FROM users
WHERE api_key = ? AND is_active = 1
  AND (api_key_expires_at IS NULL OR api_key_expires_at > datetime('now'))
```

Failure messages are deliberately specific — `auth_required` (no credential at all,
with a link to `https://hfdatalibrary.com/pages/download`) versus `invalid_key`
(unknown, deactivated or expired) — so a programmatic user knows whether to *add*, *fix*
or *regenerate* a key rather than guessing at a bare 401.

#### 6.4 Rate limiting

A fixed window in the shared `rate_limits` table, keyed `econ:download:<user_id>`:

| Constant | Value |
|---|---|
| `LIMIT_MAX` | 100 downloads / minute / account |
| `LIMIT_MAX_VIP` | 500 (5×, matching hf's `api:download-vip`; bounded and unadvertised) |
| `LIMIT_WINDOW_S` | 60 |

Over the limit returns **429** with a `retry-after: 60` header and
`cache-control: no-store`. Both credential paths funnel into the same
`applyDownloadLimit` helper, extracted verbatim from the original api_key tail so the
two cannot diverge.

#### 6.5 Download logging

`logDownload` inserts into `econ_download_log (user_id, series_id, ip, channel, bytes)`
after a 200. `bytes` comes from the `content-length` header that `util.csv()` sets from
an exact `TextEncoder` byte count, so "data served" is real rather than re-derived.
`channel` is attributed as:

* `mcp` — `X-Elkassabgi-Client: mcp`, a UA containing `elkassabgidata-mcp`, or `?via=mcp`
* `web` — a `Referer` from econdatalibrary.com, elkassabgidata.com or hfdatalibrary.com
* `api` — everything else

The whole function is wrapped in `try/catch` that only `console.log`s: **logging must
never break a download.**

#### 6.6 The browser flow

`catalog/site/download.html` loads the IdP's SDK from
`https://accounts.elkassabgidata.com/sdk/ekd-sso.js`, calls `EKD.init()`, and on a
download click sends whichever credential it has: a pasted API key as `X-API-Key`, or
an `EKD.getAccessToken()` family token as `Authorization: Bearer`. The comments record
a real, owner-reported bug and its cause: the page read `localStorage['edl_api_key']`
while SSO wrote `edl_key`, so a signed-in user saw "no key set" and the Download button
did nothing. Both are read now, with the legacy name migrated forward once. If the SDK
is blocked or unreachable, the promise resolves `false` rather than rejecting, so a
pasted API key still works.

---

### 7. Gating — the redistribution control

#### 7.1 The principle

Some publishers' licences do not permit a third party to re-host their data, and some
individual series inside an otherwise-clear source embed third-party data the source's
own licence does not cover. Those must never be served — and must not be *advertised*
either, since a catalogue entry pointing at a 451 is still a claim we host something.

#### 7.2 `denylist.ts` is generated, not hand-written

The header says so in capitals. `core/gen_denylist.py` derives it from the local
catalogue:

```
NON_REDISTRIBUTABLE = { every source_id with license.reservable = 0 }
                      ∪ LEGACY_KEEP            (never silently un-gate)
                      − GRANTED_EXCEPTIONS     (written permission on file)
```

Run with `python -m core.gen_denylist` from the econ repo root, **then redeploy the
Worker.** It is not automatic.

The three sets, and why each exists:

* **`reservable = 0`** — the DB flag the *website* already gates on
  (`catalog/gen_site.py` advertises "metadata only" and omits the downloadable
  distribution from the JSON-LD for those sources). Deriving the Worker's gate from the
  same column is what makes the two unable to disagree. The header records the drift
  that motivated it: on 2026-07-14, `transparency_ti` and 141 other `reservable=0`
  sources read "metadata only" on the page and were still downloadable.
* **`LEGACY_KEEP`** — a safety floor. A source purged from the catalogue no longer
  appears in the `reservable = 0` scan, so without a pin it would *silently fall out of
  the gate* on the next regeneration. The comment records that this actually happened
  ("verified: they DID leak on the first regeneration after the purge") for
  `irena` / `freedomhouse` / `shiller`. Every removal from this floor is annotated with
  the licence evidence and the owner's explicit decision that authorised it.
* **`GRANTED_EXCEPTIONS`** — sources with written permission that would otherwise be
  gated by a conservative licence row: `kof_globalization` (KOF director, 2026-07-06,
  non-commercial academic re-hosting), `comtrade` (UN Comtrade, 2026-07-07, with a
  standing guard that holdings must stay ≤ 100,000 records or the free branch no longer
  applies), and `wid` (moved here 2026-07-29 only after the derive completed and was
  verified 2,465,197 catalogue = 2,465,197 R2 objects).

#### 7.3 The regeneration guard

Before writing anything, `main()` asserts every entry of `REQUIRED_CARVEOUTS` appears
in the generated `SERIES_CARVEOUTS` block and **refuses to write the file otherwise**.
The reason is recorded in the code: a 2026-07-16 regeneration silently wiped the
`worldbank_pink` carve-outs that a hand-edit had added, because the template only
carried the `worldbank` entry. Post-write assertions then check that no granted source
ended up gated, and that known-restricted ids (`wto_hs_a_0010`, `cboe`, `sipri`,
`polity`, `famafrench`) definitely are.

#### 7.4 What the current denylist contains

Measured by parsing `api/worker/src/denylist.ts`:

* **49 gated source ids**, split as **21 real** (present in the catalogue) and **28
  legacy/phantom** (not currently catalogued, kept as the safety floor).
* **3 sources with series-level carve-outs**: `worldbank`, `worldbank_wdi`,
  `worldbank_pink`.

The carve-outs, with the reasoning from the file:

| Source | Carved indicators | Why |
|---|---|---|
| `worldbank` | `FP.CPI.TOTL.ZG`, `SL.UEM.TOTL.ZS` | CPI is IMF-sourced, unemployment is ILO-sourced; WB terms bar redistributing third-party data. (GDP, `NY.GDP.MKTP.CD`, is WB-compiled and *is* served.) |
| `worldbank_wdi` | same two | Same indicators reached users through a second id because the carve-out was keyed only on `worldbank`. Confirmed live 2026-07-22: `worldbank_wdi:SL.UEM.TOTL.ZS` returned 401 (i.e. served, pending auth) while `worldbank`'s copy was gated |
| `worldbank_pink` | `aluminum, copper, nickel, zinc, gold, platinum, silver` | LME (base metals) and LBMA/IBA (precious metals) **refused redistribution in writing** on 2026-07-15. These must never serve even if the source is un-gated later |

Note the derived export `SERIES_CARVEOUT_LIKE`, which turns the map into SQL prefixes
`<src>:<ind>:` so the search queries and the handler logic cannot drift apart.

#### 7.5 Where the gate is enforced — five surfaces

| Surface | Behaviour | Code |
|---|---|---|
| `/v1/series/{id}.csv` | **451** `not_redistributable`, before auth | `index.ts` |
| `/v1/catalog?source=<gated>` | **451** `non_redistributable` | `catalog.ts` |
| `/v1/catalog` unscoped | Excluded in SQL **and** re-filtered in JS | `sql.ts` + `catalog.ts` |
| `/v1/sources` | Gated sources are hidden (they have no series rows once purged; still-catalogued gated-pending ones remain visible with their licence flags) | `sources.ts` |
| `/v1/bundle` | `?source=` refused outright; individual ids returned under `econdl:unresolved`, never as advertised URLs | `bundle.ts` |
| The website | `gen_site.py` advertises "metadata only" and omits the downloadable distribution from the schema.org JSON-LD for `reservable = 0` sources | `catalog/gen_site.py` |

**451** ("Unavailable For Legal Reasons") is the right code and is used correctly here:
the server holds the bytes and is refusing on legal grounds, which is exactly what the
status means.

---

### 8. The clients

#### 8.1 `clients/python/econdl` — the econ client

A stdlib-plus-pandas package (`__version__ = "0.1.0"`) that wraps the same `/v1`
contract. Its public surface, from `clients/python/econdl/__init__.py`:

| Function | Module | What it does |
|---|---|---|
| `search(...)` | `_catalog.py` | Catalogue search — the same FTS5-with-LIKE-fallback SQL the Worker runs |
| `get_series`, `get_source`, `get_license` | `_catalog.py` | The exact `SELECT … WHERE …_id = ?` statements `sql.ts` mirrors |
| `fetch(source, code, geo=[...])`, `to_wide`, `resolve_mask` | `_fetch.py` | Cross-section by dimension mask → tidy frame |
| `bundle(ids, out="mystudy.zip")` | `_bundle.py` | Builds a bundle **and a lockfile** |
| `pull("mystudy/datapackage.json")` | `_bundle.py` | Rebuilds the exact pinned numbers; `latest=True` opts into a refresh |
| `supported_sources()`, `ResolveError` | `_resolve.py` | The resolver registry `util.ts::SUPPORTED_SOURCES` is kept in sync with |
| `HttpClient`, `HttpResolveError` | `_http.py` | HTTP transport |
| `is_proxied`, `reservable_state` | `_proxy.py` | Redistribution state of a source |

Two design points that matter for the read path:

* **Same lockfile semantics over either backend.** `_http.py` lets `bundle()`/`pull()`
  target the public API (`$ECONDL_API`, or the local dev shim `api/devserver.py`)
  instead of the on-disk store. Only the *source of the rows* changes; the snapshot
  pin, the per-resource `sha256` and the loud-never-silent behaviour are identical.
* **Honest status passes through.** A 404/501/502/400 from the server is raised as
  `HttpResolveError` — a subclass of `ResolveError` — so an HTTP-sourced series that
  cannot be satisfied is treated exactly like a local-resolve miss and warned about,
  never silently dropped. The transport is **stdlib only** (`urllib`); there is no
  `requests` dependency.

The bundle's `datapackage.json` is a Frictionless data package that doubles as a
re-runnable lockfile: it pins `snapshot_date`, a per-resource `sha256`, and the
licence / attribution / citation from the registry.

#### 8.2 `clients/r`

**In the econ repo, `clients/r` does not exist** — `clients/` contains only `python`.
The R client belongs to the **hf** library:
`D:\research\hfdatalibrary\clients\r\hfdatalibrary\` (an R package with `DESCRIPTION`,
`NAMESPACE`, `R/`), installed with
`remotes::install_github("elkassabgi/hfdatalibrary", subdir = "clients/r/hfdatalibrary")`.
It requires `httr` and, for parquet, `arrow`. Its surface is
`hfdl_set_key()`, `hfdl_symbols()`, `hfdl_get(ticker, version=, timeframe=, format=)` —
HF 1-minute equity bars, not econ series.

**NOT ESTABLISHED:** whether an R client for the econ API is planned. Nothing in the
econ repo references one. For R users today the documented path is plain `read.csv()`
against a `/v1/series/{id}.csv?raw=1` URL, which is exactly what the "zero-install"
design rule in `api/CONTRACT.md` intends.

#### 8.3 The third client: the MCP server

`mcp/` is a separate Worker (`elkassabgidata-mcp`) built on the Agents SDK
`McpAgent` over a Durable Object, speaking Streamable HTTP at `/mcp`. It is a *client*
of this API — `mcp/src/index.ts:29` hard-codes
`const ECON = "https://econdl-api.elkassabgi.workers.dev"` — and registers 11 tools
plus one prompt:

`search_econ_series`, `get_econ_series`, `get_econ_series_metadata`,
`list_econ_sources`, `get_data_freshness`, `get_hf_download_link`,
`get_hf_variables_dictionary`, `list_ip_bundles`, `get_ip_download_link`,
`get_family_status`, `get_auth_status`, and the prompt `hf_event_study`.

The user's API key arrives per request (Bearer / `X-API-Key` / `?api_key=`) and is put
into `ctx.props` — **never stored**. Downloads through this path are tagged
`channel = 'mcp'` in `econ_download_log`.

---

### 9. The website — generation and deployment

#### 9.1 `catalog/gen_site.py`

3,857 lines. It reads the central registry (`data/catalog.db`: `source` / `license` /
`series`) plus an operational sidecar (`catalog/catalog.json`) and emits into
`catalog/site/`:

* **`<source>.html`** — one landing page per registered source, each embedding a valid
  **schema.org/Dataset** JSON-LD block (what Google Dataset Search indexes) and an
  inline **Croissant** block.
* **`index.html`** — a client-side searchable index of all datasets.
* **`sitemap.xml`** — every indexable page at the extensionless URL the host actually
  serves.

Current output, counted today: **333 `.html` files** in `catalog/site/`, and
**330 `<loc>` entries** in `sitemap.xml`.

Four rules the generator enforces, quoted from its docstring and comments:

1. **The registry is the single source of truth.** Licence, attribution and terms come
   from the `source` + `license` tables — never invented. Fields we do not have are
   omitted.
2. **Licence is a re-serve gate.** For `reservable = 0` sources the page advertises
   distribution as "metadata only", and the JSON-LD omits any downloadable distribution
   and sets `isAccessibleForFree` accordingly. This is the *same flag* the Worker's
   denylist is generated from (§7.2).
3. **URLs are the ones the host answers 200 for.** Cloudflare Pages strips `.html` and
   answers the extension form with a 308, so every canonical, `og:url`, schema.org
   `url` and sitemap `<loc>` used to be a redirect — 207 of 207 of them. `site_url()`
   now emits the extensionless form (`index.html` → the bare origin root).
4. **`<lastmod>` is proven, not stamped.** Each page's finished HTML is hashed *before*
   the volatile generation stamp is substituted, and the digest plus the date it was
   first seen are persisted in `catalog/sitemap_state.json` (outside `site/`, so it is
   neither deployed nor swept). A page whose digest is unchanged keeps its stored date.
   The motive: `last_updated` was populated for 3 of 203 sources, so 204 of 207 URLs got
   the run date on every build — the exact pattern that makes Google discount a site's
   `lastmod` entirely.

A `KEEP_UNGENERATED` set protects the hand-maintained pages from the orphan sweep that
deletes stale `.html`: `_redirects`, `download.html`, `status.html`, `mcp.html`,
`account.html`, `404.html`. Deleting `download.html` in particular would break every
dataset page, because each one's Croissant/schema.org distribution points at
`/download.html?source=<id>`.

#### 9.2 How pages get published

`.github/workflows/deploy-site.yml`:

```yaml
- uses: cloudflare/wrangler-action@v3
  with:
    apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
    accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
    command: pages deploy catalog/site --project-name=econdatalibrary --branch=main
```

Three facts about this workflow that matter operationally:

1. **It is `workflow_dispatch` only.** The `push:` trigger is written but **commented
   out**, because the first run failed: the repo's `CLOUDFLARE_API_TOKEN` authenticates
   but the Pages API refuses it — `Authentication error [code: 10000]`. That token was
   minted 2026-07-02 for the *updater* workflows (Worker deploy + R2 writes); publishing
   a Pages site is a different permission. To switch the push trigger on, the token
   needs **Account → Cloudflare Pages → Edit** at
   `dash.cloudflare.com/profile/api-tokens`, after which the same token also unblocks
   the elkassabgidata and ipdatalibrary sites. Nothing else in the file changes.
2. **It verifies the live site, not just the upload.** A green `wrangler` step means
   "Cloudflare accepted the upload", not "the site serves" — the custom domain is
   managed in the dashboard and is independent of the upload, so a detached domain
   would leave the job green while econdatalibrary.com is dark. The job therefore
   curls `https://econdatalibrary.com/` and `/account` expecting 200, retrying six
   times with 10-second sleeps to absorb edge propagation.
3. **Concurrency is `cancel-in-progress: false`.** Cloudflare makes the
   *last-created* deployment live, so cancelling an in-flight run could leave an older
   build serving.

The practical consequence, consistent with the standing note in the owner's memory:
**a `git push` publishes nothing by itself.** The site is deployed by dispatching this
workflow (or by running the same `wrangler` command locally), and the Worker is
deployed separately with `npx wrangler deploy` from `api/worker/` — there is no
workflow that deploys the Worker on push either.

#### 9.3 The other pieces of the read path on the site

* `catalog/site/assets/` and `catalog/site/auth/` hold the shared styling and the SSO
  callback page.
* `catalog/site/_redirects` carries the Pages redirect rules.
* `sources_meta.json` is a static sidecar; the only page that references it is
  `status.html` (measured: `grep -l sources_meta.json catalog/site/*.html`).
* Every generated page and hand-maintained page talks to the same API host — measured:
  each dataset page contains exactly one reference to
  `econdl-api.elkassabgi.workers.dev`; `api.html` has four and `catalog.html`,
  `account.html` two.

---

### 10. Cost — how this architecture bills

This is the section to read twice. The architecture's cost behaviour is not incidental;
several of its most distinctive features exist **only** because of a bill.

#### 10.1 The price list

From `tools/billing_guard.py`, which encodes Cloudflare's published pricing and — this
is the important part — has been **reconciled against a real invoice** (see §10.5).

| Meter | Included per month | Price past the allowance |
|---|---|---|
| D1 **rows read** | 25,000,000,000 | $0.001 / million ($1 per billion) |
| D1 **rows written** | 50,000,000 | **$1.00 / million** |
| D1 **storage** | 5 GB | $0.75 / GB-month |
| R2 **Class A** ops (PUT, LIST, DELETE, …) | 1,000,000 | **$4.50 / million** |
| R2 **Class B** ops (GET, HEAD, …) | 10,000,000 | $0.36 / million |
| R2 **storage** | (not deducted — see note) | $0.015 / GB-month |
| R2 **egress** | — | **$0** |
| Workers **requests** | 10,000,000 | $0.30 / million |
| Workers Paid base plan | — | $5.00 / month |
| Texas sales tax | — | ×**1.066** (80 % of the amount taxable at 8.25 %) |

Three subtleties the tool encodes because getting them wrong produced wrong numbers:

* **Cloudflare bills whole units, rounded up.** `units()` uses `math.ceil`. Confirmed
  twice on the invoice: 32,284,689 billable writes → 33 units → $33.00; 20,325,560
  billable Class A → 21 units → $94.50. Exact division understated that invoice by
  $3.75. And the trap: round the *monthly* quantity only — ceiling a *daily* figure and
  multiplying by 30 turns 0.2 M rows/day into 30 whole units, a 150× overstatement.
* **A "GB-month" is 730 GB-hours**, not a calendar month. `gb_months()` scales by
  `days × 24 / 730`; skipping it dropped 1.9 % of the storage charge, always downward.
* **Storage is billed on the period *average*, not today's size.** Pricing a snapshot
  against a GB-month charge measured 38-62 % below the invoice on the one period that
  can be checked.
* **The billing period runs the 9th to the 8th**, not the calendar month, so the
  allowances reset on the renewal date. Every monthly figure computed on calendar
  months was measured on the wrong window (`NUMBERS.md`, 2026-08-29).
* **The 10 GB R2 storage free tier is deliberately *not* deducted.** It is in
  Cloudflare's published free tier, but subtracting it moves the model *away* from the
  one invoice that can be checked (−0.75 % becomes −1.42 %), so on this plan the
  allowance is evidently not applied. `billing_guard.py` carries the standing
  instruction not to re-add it without an invoice that shows it — an unverified
  allowance that biases the number cheap is the ledger's R502 failure exactly.

#### 10.2 What each kind of request costs

| User action | D1 rows read | R2 ops | Notes |
|---|---|---|---|
| `/v1/series/{id}.csv` | ~1 (PK seek) + a few for auth/licence rows | **1 Class B GET** | The cheapest meaningful operation in the system. Egress is free |
| `/v1/catalog?source=X` (page) | `offset + limit` PK entries + 1 row from `source_counts` | 0 | After the fix. **Before** it: 4.93 M + 2.47 M rows *per page* |
| `/v1/catalog?q=…` (FTS hit) | index lookups + one PK seek per hit | 0 | Cheap |
| `/v1/catalog?q=…` (LIKE fallback) | **full scan of `series`, twice** (rows + count) | 0 | ~13.5 M rows × 2 per request |
| `/v1/sources` | **NOT MEASURED — see the flag below** | 0 | 349 correlated `EXISTS` subqueries against an unindexed column |
| `/v1/stats` | `SUM(n)` over `source_counts` on both DBs (one row per source) | 1 Class B GET | 6-hour edge cache in front |
| `/v1/bundle?ids=…` | 1 PK lookup per id | 0 | Bounded by the caller's request size |
| A catalogue sync | — | — | **Writes**, at $1.00/M, amplified ~3× by the FTS index |
| A derive run | — | **1 Class A PUT per series** | This is the expensive meter |
| `RELAUNCH_GUARD` re-running a job | — | **1 Class A LIST per 1,000 objects** | See §10.4 |

The asymmetry to internalise: **reads are nearly free, writes and Class A operations are
not.** $1 per *billion* rows read against $1 per *million* rows written and $4.50 per
million Class A operations — a factor of 1,000 and 4,500 respectively.

##### A flag on `/v1/sources`, raised honestly and left open

`SELECT_SOURCES` (quoted in full in §4.4) filters with a **correlated subquery**:

```sql
WHERE EXISTS (SELECT 1 FROM series se WHERE se.source_id = s.source_id)
```

`series` has **no index on `source_id`** (§4.2) — which is precisely the condition that
made the browse query cost $82 a day. This subquery runs once per source row, so a
single `/v1/sources` call issues on the order of 349 unindexed probes against a
13.5 M-row table. Two things soften it and one does not:

* *Softening:* because `series_id` is source-prefixed and the table is stored in PK
  order, a source whose rows sort early is found almost immediately, so this is not
  automatically 349 full scans.
* *Softening:* `/v1/sources` is a single call per page load, not a paginated crawl
  surface, so it lacks the multiplier that turned the browse defect into 130 B rows.
* *Not softening:* unlike `/v1/catalog` and `/v1/stats`, **`/v1/sources` is not in the
  Worker's edge cache** — it only carries the browser-level `max-age=300`.

**This has NOT been measured and this document does not claim a figure for it.** The
project's own pre-flight rule is the instrument: run the statement once and read
`meta.rows_read` —

```
npx wrangler d1 execute econ-catalog --remote --json --command "<SELECT_SOURCES verbatim>"
```

one query, ~13 seconds, free of the cost guard's scan budget only if it turns out to be
an index seek. If `rows_read` comes back in the millions, the fix is the same one R430
already proved: put a `source_counts`-style materialised answer in front of it, or
edge-cache the endpoint for 6 hours exactly as `/v1/catalog` is. Raising it here rather
than asserting either "it's fine" or "it's a defect" is the point.

#### 10.3 Incident 1 — the catalogue browse, ~$82 in one day (ledger R430, 2026-08-15)

**What happened.** `/v1/catalog?source=` ran two queries per request:

```sql
SELECT … FROM series WHERE source_id = ? ORDER BY series_id LIMIT ? OFFSET ?;
SELECT COUNT(*) AS n FROM series WHERE source_id = ?;
```

No composite index existed and **D1 cannot build one** (`CREATE INDEX` on the 9.2 M-row
table dies `SQLITE_NOMEM`), so SQLite sort-scanned the source's entire row set on every
page — 4.93 M rows on `wid` — and counted 2.47 M rows again for the `total`.

**The trigger** was external: a crawler paged `wid`, issuing ~17,700 requests in a day.
**87.3 billion + 42.2 billion ≈ 130 billion D1 rows read in one day**, measured with
`wrangler d1 insights` (average query efficiency 0.00008). Cost: ~$82.

**The detection** is the part the ledger is harshest about: Ahmed found it *by reading
his bill*. No spend alert fired; no `insights` check had ever run.

**The fix**, shipped the same hour (worker `5d63034f`, commits `c4c36762d` +
`ecf3c5073`) — all four parts are visible in the code documented above:

1. Browse rides the primary key: `[src+':', src+';')` (§4.3).
2. `total` comes from `source_counts`, one row per source, refreshed once per sync
   (`core/sync_catalog_d1.py`).
3. `OFFSET` capped at 100,000 with an honest 400 pointing at `/v1/bundle` (§5.5).
4. `/v1/catalog` edge-cached for 6 hours (§2.2).

Live-verified afterwards: `wid` page 1 returned 200 with the exact total 2,465,197, and
offset 200,000 returned 400. **Worst-case daily cost at the same traffic after the fix:
~$0.10** — an 800× reduction.

The measured aftermath, from `NUMBERS.md` (2026-08-29): D1 reads ran at
**28,965,011,928/day** for Aug 9-15 and **1,146,343,274/day** for Aug 16-29 — a
**96.0 % reduction**. Aug 14 (100.61 B) and Aug 15 (95.07 B) alone were 89 % of the
period's reads.

R430's rules, which now govern any new serving surface:

> (1) any query that runs PER PUBLIC REQUEST against a multi-million-row D1 table must
> be O(page) — a `COUNT(*)`, a sort without a usable index, or an uncapped `OFFSET` is a
> **billing defect, not a performance nit**;
> (2) metered infrastructure needs a meter-watcher, because the failure mode announces
> itself only on the invoice;
> (3) D1 cannot `CREATE INDEX` on big tables — design key layouts so the PRIMARY KEY
> carries the access pattern.

A sibling of the same shape was fixed the next day: `/v1/stats` ran a full `COUNT(*)`
over 12.3 M rows *per hit* — 267 M rows/day — and now reads `source_counts` behind a
6-hour cache (`index.ts` comment, 2026-08-16).

#### 10.4 Incident 2 — the R2 LIST loop (`NUMBERS.md`, 2026-08-30)

`RELAUNCH_GUARD.ps1` relaunches long-running jobs. The `derive_noaa.DONE` sentinel was
never written, because PowerShell 5.1 returns a null `ExitCode` unless the process
Handle is cached first — so **no job had ever been marked done**, and the guard
resurrected the noaa derive **975 times**. Each relaunch paged the entire
`series/noaa%3A` prefix: 3,138,169 objects ÷ 1,000 per LIST = **3,139 Class A LIST
operations per relaunch**.

Measured with `r2OperationsAdaptiveGroups` grouped by hour across a 42-hour window
spanning the fix: **6,062-7,776 LIST/hour → 57-368 LIST/hour, a 97.8 % drop**, with the
break exactly at the sentinel's hour (2026-08-29T19:18:12Z, exit 0). That is
~168,000 LIST/day removed ≈ 5.0 M/month ≈ **$22.68/month** at $4.50/M. The fix is worth
**$13.50/month pre-tax, $14.39 with tax** in the reconciled forward model.

The lesson generalises: **LIST is a Class A operation and its cost scales with the
number of objects in the prefix, not with how much you read.** With 3.1 M objects under
one prefix, a single unnecessary full listing costs about $0.014 — and a loop that does
it 975 times costs real money.

#### 10.5 The bill, reconciled against ground truth

Invoice **IN-74622130**, covering **Jul 9 – Aug 8, 2026**, supplied by the owner:

| Line | Amount |
|---|---|
| R2 **Class A operations** | **$94.50** (20,325,560 billable → 21 units) |
| D1 **rows written** | **$33.00** (32,284,689 billable → 33 units) |
| R2 **storage** | **$22.46** (≈1,497 GB-months) |
| Workers Paid base | $5.00 |
| D1 **rows read** | **$0.00** (10.97 B = 43.9 % of the 25 B allowance) |
| R2 Class B | $0.00 |
| D1 storage | $0.00 (under 5 GB that period) |
| **Subtotal** | **$154.96** |
| Texas tax | $10.23 |
| **Total** | **$165.19** |

`tools/billing_reconcile.py` predicts **$154.79** against the billed **$154.96** —
**−0.11 %**. Base, D1 writes and R2 Class A match *to the cent*; the entire $0.17
residual is R2 storage. D1 rows written matched to **21 rows out of 82 million**; R2
Class A to **0.055 %**.

**The single most important thing this invoice says:** the line the team spent the most
effort optimising — D1 rows read — cost **$0.00** that period. The two lines that
actually bill are **R2 Class A operations** and **D1 rows written**. Storage is third.

That said, `NUMBERS.md` carries the marginal-cost warning: in the *current* period the
25 B read allowance was **exhausted** (218.8 B used, almost all of it the Aug 14-15
incident), so "D1 reads are effectively free" must not be carried into a decision made
inside that period.

#### 10.6 Where the bill stands

From `NUMBERS.md`, all dated 2026-08-30:

| Figure | Value | Basis |
|---|---|---|
| Current period accrued (Aug 9-30, no forecast) | **$276 pre-tax / $294 with tax** | 21 of 21 complete days returned |
| Full period if the median day repeats | $301 / $320 | period-to-date + median daily rate × 10 remaining days |
| **Forward run-rate after the noaa fix** | **~$30/mo pre-tax, ~$32 with tax** | base $5 + Class A $9.00 + R2 storage $13.88 (925 GB) + D1 storage $2.10 + reads $0 (49 % of allowance) + writes $0 (27 %) |
| Same month *without* the noaa fix | $43.48 / $46.35 | class A at the pre-fix median |

Two caveats recorded with those figures, repeated here because they are the honest
frame: the forward Class A line rests on **7 post-fix hours**, the thinnest input in the
set; and reads sit at **49 % of the allowance on a median day**, so the headroom is one
bad day wide.

The $194 of D1 reads from the Aug 14-15 incident is **sunk** — no forward rate walks it
back. The clean rate starts with the period beginning 2026-09-09.

#### 10.7 `tools/billing_guard.py` — the meter-watcher

Runs daily from `.github/workflows/billing-guard.yml` at 13:23 UTC (an off-minute,
because Actions crons on `:00` are heavily delayed or dropped) plus `workflow_dispatch`.

**What it measures.** Two independent instruments, and the difference between them is
itself a documented finding:

* `npx wrangler d1 insights <db> --timePeriod 1d` per database, summed. **This
  truncates**: it returns only the top 100 query *shapes*. Measured 2026-08-29 it
  reported 36.1 M reads/day for the fleet while the true figure was 374.3 M — a **10×
  undercount**.
* The **Cloudflare GraphQL analytics API** (`CF_ANALYTICS_TOKEN`, a read-only Account
  Analytics token) for `d1AnalyticsAdaptiveGroups`, `r2OperationsAdaptiveGroups`,
  `r2StorageAdaptiveGroups`, `d1StorageAdaptiveGroups` and
  `workersInvocationsAdaptive`. These are the true account totals and the source the
  alarm now uses.

**Thresholds** (measured, not guessed): WARN at 2 B rows read/day or 5 M written/day;
ALERT at 5 B read or 15 M written. Steady state is ~1 B/day; the incident peaked at
130 B/day.

**Delivery.** Two independent paths: a Resend email, and `exit 1` reddening the workflow
(GitHub emails failed-workflow notices too). It also alerts on **zero hf registrations
in 24 h** against a ~20-30/day baseline, after the owner was himself the alarm for a
seven-hour registration dip on 2026-08-26.

**The defensive design is the interesting part.** Nearly every comment in the file
records a way a cost meter can silently lie, and the code closes it:

| Failure mode it guards against | Mechanism |
|---|---|
| A vendor blip drops the largest database from the total | `_TRIES = 3` retries; `MEASUREMENT FAILED` reddens the run and relabels the total |
| A GraphQL response truncated by `limit` (HTTP 200, no error key) | `_rows()` treats "row count == the limit you asked for" as suspect and degrades. Measured: `limit=1` returned 9 requests, `limit=2000` returned 1,038,162 — a 115,000× undercount with nothing to catch |
| A short or gappy day list read as "zero operations" | `_check_days()` asserts coverage on the **success** path, not by inference from failure |
| A blind spot printed and then omitted from the total | `_DEGRADED` is appended by both the fetch layer and the coverage checks, and the *label itself* flips from `PROJECTED MONTH` to `PARTIAL MONTH FLOOR` |
| A forecast that swings by an order of magnitude | `_median()` of the period's daily values, not yesterday's. Backtested: yesterday×remaining-days would have printed **$2,598** for D1 reads on Aug 15 and **$181** on Aug 20 from the same true spend |
| Pricing a snapshot against a GB-month charge | `r2_storage_period()` sums per-(bucket, storageClass) daily maxima across the billing period and drops the partial current day |
| A UTC/local off-by-one making "yesterday" wrong for five hours every evening | `datetime.now(timezone.utc)` throughout |
| A crash in the error handler | `_insights_sorted` re-encodes wrangler's emoji-bearing stderr before printing, after `🪵` killed a whole run on a cp1252 console |

Its own docstring states the standard it holds itself to: *"a guard that cannot see the
meter must not look green — that is exactly how the previous alert was 'ineffective'."*

#### 10.8 The desktop-first rule, and the pre-flight rule

Two standing rules follow directly from all of the above and are recorded in
`D:\research\hfdatalibrary\CLAUDE.md`:

**Decide locally, verify remotely.** Exploration, sizing, auditing and counting — anything
whose answer only informs a decision — runs against the local `catalog.db`. D1 is
touched only to serve users, to apply a write, and to verify user-facing state
afterwards. August billed ~$200 in D1 reads, 87 % of it on two days, and those two days
were catalogue maintenance, not users. The verification half is not optional: local and
D1 can disagree, so a claim about what users see still comes from D1 or the served file.

**Measure one statement and multiply.** Before any batch of D1 statements, run **one** of
them as a `SELECT` and read `meta.rows_read`. One query, ~13 seconds. Ledger R492
records what skipping it nearly cost: a plan of **164,705 statements** against
`series_fts`, whose `series_id` column is declared `UNINDEXED`, so each statement is a
full scan of **23,843,482 rows** — **3.93 trillion rows, ~24 days of wall clock, ~$2,500**.
It was estimated at 90 minutes from the file count, off by ~380×. It was caught by
adversarial review **before a single statement ran; $0 was spent.**

The rule it produced is the one to remember when touching this system:

> When the predicate column is unindexed, **the cost is per statement, not per id** — so
> **raise predicate arity, never add statements.** An `IN` list of 200 ids costs exactly
> what one id costs.

The same insight is now applied in `core/sync_catalog_d1.py`: a whole-source reconcile
uses **one** range-bounded `DELETE FROM series_fts WHERE series_id >= 'src:' AND
series_id < 'src;'` instead of 1,915 id-list statements — measured at ~$0.024 against
~$45.60, a ~1,900× reduction with an identical end state.

Two hooks in `.claude/hooks/` enforce what prose did not: `d1_cost_guard.py` (PreToolUse)
counts full-table scans and refuses past 15/hour or 40/day, and `cost_banner.py`
(SessionStart) prints the running total.

---

### 11. Loose ends and honest gaps

Things a reader should not take on faith from this document:

| Item | Status |
|---|---|
| A Workers **Custom Domain** for the API | **NOT ESTABLISHED as of today.** `wrangler.toml` has `workers_dev = true` and its `[[routes]]` block commented out; `AUTH_SSO_HANDOFF.md:60` records `api.econdatalibrary.com` as **NXDOMAIN** (verified 2026-07-17) and warns never to test API behaviour against it or against `econdatalibrary.com/v1` (which returns the site's `index.html`). `docs/DATABASE_REFERENCE.md:327` flags the live attachment state as dashboard-only. `npx wrangler deployments domains list` would settle it |
| `catalog_coverage: "series-level for 33 sources"` | **Stale string** in `catalog.ts:19`. 322-323 sources are served. Harmless but wrong |
| `SUPPORTED_SOURCES` header comment "The 191 sources" | **Stale comment** in `util.ts:13`. The array holds 323 |
| noaa row count | Three nearby values from different dates (3,137,871 / 3,138,211 / 3,138,169 objects). Live answer: `SELECT n FROM source_counts WHERE source_id='noaa'` on `econ-catalog-climate` |
| `/v1/stats` headline figures | The endpoint serves whatever `_aqueduct/stats.json` holds in R2. `NUMBERS.md` (2026-08-26) records that the newest census (33,908,707,379 observations / 3,901,731,326 series / 322 sources, `logs/stats-2026-08-26.json`) was **not published**, because a >20 % change gate refused it; the published July object reads 79.8 B / 7.73 B. **Do not quote either publicly without checking which one the endpoint is currently returning** |
| Exact local `series` row count | The `COUNT(*)` over the 11.9 GB local `catalog.db` did not complete inside this session's budget. The instrumented figures in the table at §4.2 come from `NUMBERS.md` (13,486,284 in D1 on 2026-08-24; 13,486,342 by the audit on 2026-08-30). To re-measure locally: `sqlite3 data/catalog.db "SELECT COUNT(*) FROM series"` (slow on the USB volume, free); on D1, `SELECT SUM(n) FROM source_counts` is instant and uncounted by the cost guard |
| Cache-control on authenticated CSV | `util.ts::CSV_HEADERS` sets `public, max-age=300` on gated download responses. The Worker never places them in `caches.default`, so only browsers and third-party intermediaries are affected. Whether that is intended is a design question, not a defect this document can resolve |
| An R client for the econ API | Does not exist. `clients/r` is an **hf** package. Nothing in the econ repo plans one |
| Per-request cost of `/v1/sources` | **NOT MEASURED** (§10.2 flag). It runs 349 correlated `EXISTS` probes against `series`, which has no `source_id` index, and it is not edge-cached. One `wrangler d1 execute --json` run of `SELECT_SOURCES` and its `meta.rows_read` would settle it in ~13 seconds |
## 3. How data gets in and becomes servable

This is the **write path** — everything between a publisher's server and a file a user can download.
Every claim here was established by reading the named file or running the named command during this
session.

---

### 1. The journey of one number

```
publisher API / bulk file / spreadsheet
        │
        ├── jobs/ingest_<source>.py        one-time bulk load  (212 files)
        └── updater/strategies/fetchers/<source>.py   scheduled refresh  (296 files)
        │
        ▼
  parquet store:  data/clean_full/<source>/*.parquet
        │         columns (series_key, obs_date, value)
        │
        ├── written through updater/merge.py::merge_and_write   ← fetchers ALWAYS
        └── written with pq.write_table directly                ← 146 of 212 ingest jobs
        │
        ▼
  catalogue:  data/catalog.db  (11.91 GB local SQLite)
        │     tables: series, series_fts, source, license
        │
        ├── core/sync_catalog_d1.py  ──►  Cloudflare D1 (series, series_fts, source_counts, …)
        │
        ▼
  derive:  core/derive_csv.py (CLI)  /  updater/derive.py::derive_and_put (nightly)
        │
        ▼
  R2 object storage:  series/<urlencoded source:id>.csv    ← what the user downloads
```

The critical property to hold in mind: **the store and the served file are separate artefacts.** A
source can be perfectly current in parquet and still serve month-old CSVs, because the derive step
is budgeted and can be skipped, deferred, or silently mismapped.

---

### 2. The two-parser reality

Almost every source has **two** pieces of code that can parse it:

| | ingester | fetcher |
|---|---|---|
| path | `jobs/ingest_<source>.py` | `updater/strategies/fetchers/<source>.py` |
| count | 212 files | 296 files |
| purpose | the original bulk load / backfill | the scheduled refresh |
| runs nightly? | no | yes |

**This is a standing trap and the ledger records it repeatedly.** A parser fixed in the ingester and
verified with a real live call proves the half that does *not* run nightly (R333). When diagnosing a
source, the first question is always *which of the two produced the bytes I am looking at*.

It also explains a class of confusion: the ingester's docstring may describe a key format that the
fetcher does not build, or vice versa. During this session I read the parked-looking module
`_who_gho.py` as disabled because of its leading underscore, when it is in fact a **shared base
module** imported by three live fetchers (`who_hwf`, `who_rs`, `who_sdg`) — a naming convention read
as behaviour (ledger R516).

---

### 3. The canonical store format, and its exceptions

The intended shape is three columns:

```
series_key : string     the identifier within this source
obs_date   : date32[day]
value      : double
```

**Measured 2026-08-30** by reading the arrow schema of one parquet per store directory, across all
430 directories under `data/clean_full/`:

| id column | stores | notes |
|---|---|---|
| `series_key` | **371** | the canonical shape |
| `series_id` | **3** | `bls`, `eia`, `ofr` |
| neither | **11** | `cepii_baci`, `cftc`, `edgar_13f`, `edgar_insider`, `edgar_pointers`, `fdic`, `gleif`, `insee_bdm`, `insee_sirene`, `worldbank_esg`, `worldbank_extra` |

The 11 with neither are genuinely outside the series model — `gleif` is an entity registry (legal
entity identifiers, names, jurisdictions), the `edgar_*` trees are filings. They cannot be
catalogued as time series at any grain, which is a product decision rather than a bug.

The 3 that use `series_id` matter more, because **any tool that requires `series_key` silently skips
them**. My own fleet sweep did exactly that and recorded them as "unmeasurable" — correctly named
rather than silently dropped, but it meant the source with the largest collision defect (`eia`) was
initially invisible to the instrument built to find collisions.

Note also that some stores carry **extra** columns: `eia`'s parquet has `series_id, obs_date, value,
period, freq` — the frequency is present in the *store* and absent from the *catalogue id*, which is
precisely its defect.

---

### 4. `updater/merge.py::merge_and_write` — the invariant layer

This is the single most important function in the write path. Signature:

```python
def merge_and_write(out_path, new_table, *, mode="merge", dedup_keys=DEDUP_KEYS,
                    min_ratio=0.97, allow_empty=False, blob=None):
```

with

```python
DEDUP_KEYS = ("series_key", "obs_date")
```

#### What it guarantees

1. **Deduplication.** In `mode="merge"` it concatenates the existing store with the new table and
   calls `_dedup(combined, dedup_keys)`. **A store written through merge mode therefore cannot
   contain duplicate `(series_key, obs_date)` pairs.**
2. **Never-shrink.** If the result would fall below `min_ratio` (default **0.97**) of the existing
   row count, it raises and **leaves the existing file untouched**. This exists so a truncated or
   partial upstream pull cannot silently overwrite good data.
3. **No dropped columns.** `_concat` raises if a published column would vanish.
4. **Dedup keys must exist.** If the dedup columns are absent it refuses rather than silently
   merging without them.
5. **No empty publish** unless `allow_empty`.
6. **Impossible-date reporting** via `_report_impossible_dates`.
7. **Atomic write**, through the blob abstraction.

#### The blob abstraction

`blob=None` writes to the local filesystem. Passing a blob handle treats `out_path` as an **object
key** and runs the identical invariants against R2. Which one is used is governed by
`AQUEDUCT_BACKEND`; production runs `r2`.

This matters more than it looks. Under `BACKEND=r2` the local directory is a *scratch mirror holding
only what this run wrote*, so any code that lists it with `os.listdir` or `glob` sees almost
nothing — and an empty listing is a legal value that nothing downstream objects to. The ledger
records this failing at least five separate times (R261, R264, R355, R272, R151).

#### `mode="overwrite"`

In overwrite mode `final = new_table` — **no dedup runs**, though never-shrink still applies.
Searching the repository, `mode="overwrite"` appears **only in tests**; no production fetcher uses
it.

#### `_dedup` keeps the LAST row

`_dedup` sorts by `(keys…, __i)` where `__i` runs existing-then-new, and keeps the last. So when a
merge collapses conflicting duplicates, **the newest row wins** — which is right for a revision and
catastrophic for a collision, because in a collision the "duplicates" are different series and the
survivor is arbitrary.

---

### 5. The origin of the whole defect class

**Measured 2026-08-30** by scanning every file in `jobs/`:

| | count |
|---|---|
| ingest jobs total | **212** |
| that call `merge_and_write` | **0** |
| that write parquet directly (`pq.write_table` / `to_parquet` / `ParquetWriter`) | **146** |
| not writers at all | 66 |

Every **fetcher** goes through `merge_and_write`. **Not one ingester does.**

So each store's *initial* content was written without dedup, without never-shrink, and without the
impossible-date check — and the maintenance path then assumes invariants that were never
established. `jobs/ingest_damodaran.py:474` is literally:

```python
pq.write_table(tbl, out, compression="zstd")
```

That single architectural fact explains the key-collision family, why it survives (a source whose
registry strategy re-runs the *ingest script* re-creates the duplicates every time), and why the
never-shrink guard keeps refusing writes on the two UNCTAD stores — it is correctly refusing to let
a dedup collapse data the ingester should never have stacked.

---

### 6. The derive step — store → downloadable CSV

Two entry points that are **not** the same program:

| | `core/derive_csv.py` | `updater/derive.py::derive_and_put` |
|---|---|---|
| invoked by | a human, from the command line | the orchestrator, nightly |
| concurrency | `--workers`, default 1 | `AQUEDUCT_DERIVE_WORKERS`, default 8 |
| resume flags | `--skip-existing`, `--skip-newer-than` | **none** |
| budget | none | `AQUEDUCT_DERIVE_BUDGET_MIN`, default 45 |

Confusing the two produced a 32× cost error in this session: I timed the CLI (single-threaded, no
uploads) and reported it as the nightly cost (8 threads, with uploads).

Notable properties:

* **There is no skip-unchanged path.** `--skip-existing` skips keys already present; `--skip-newer-than`
  compares R2 `LastModified`. Neither is a *content* check, and the orchestrator's path has no skip
  at all — so a qualifying run rewrites byte-identical CSVs for series that have not changed.
* **The projection is a pure-Python row loop**, so threads serialise on the GIL; separate processes
  scale, threads do not past a handful. There is a `--shard I/N` flag for that reason.
* **Bytes must be byte-identical** to what the Worker and the dev shim produce, which is why gzip is
  written with `mtime=0` and a fixed level.
* **Cost shape:** one R2 Class A PUT per series. Deriving a whole large source is hundreds of
  thousands of operations.

The derive is also where a collision becomes *visible to the user*: the emitted CSV for
`eia:EBA.AEC-ALL` contains 372,392 rows across 976 distinct dates, and its own `series_id` column
changes from row to row.

---

### 7. Cataloguing, and the five places a series lives

`catalog.db` is the local authority (11.91 GB; `source` table 349 rows, `license` table 71 rows,
measured 2026-08-30). `core/sync_catalog_d1.py` pushes it to Cloudflare D1.

A single series exists in **five** places, and only some of them have anything enforcing agreement:

1. the **R2 CSV** — what the user downloads
2. **D1 `series`** — what the catalogue API lists
3. **D1 `series_fts`** — what search matches on
4. **local `catalog.db`** — what the site generator and every local tool read
5. **`source_counts`** — the per-source total the API reports

There are no foreign keys between them. The consequences are recorded in the ledger and are all
user-visible:

* Purging 384 series from R2, D1 `series` and local `catalog.db` — three of five — left them in
  `series_fts`, where a search hit resolves to nothing: a **404 for the user** (R481).
* `source_counts` has **exactly one writer**, so every direct D1 write silently invalidates it. A
  source with **no** cache row falls back to a live `COUNT(*)` on every page view — which is the
  exact query shape that produced a $82 day (R489).
* A retired source that keeps its count row is worse than one that keeps its data, because the API
  goes on **advertising** rows it cannot deliver: `ilo` answered `total: 1157` with `results: []`.

`series_fts` deserves its own warning. It is declared

```sql
fts5(series_id UNINDEXED, title, geography)
```

`UNINDEXED` means a `WHERE series_id = ?` predicate has **no index**: one such statement was
measured reading **23,843,482 rows in 11.2 seconds**, and an `IN` list of two hundred ids reads
exactly the same, because the cost is **per statement**. This is why a repair plan of 164,705
statements was 3.93 trillion row reads and ~$2,500 — and why the fix is always to raise predicate
arity using the primary-key range, never to add statements (R492).

---

### 8. Titles

Series need human-readable names. The failure modes here are instructive because they were all
**invisible from the writing end**:

* A title wave computed 164,705 correct titles, wrote the `series` updates, and **ended with a
  `print()` telling the operator to rebuild the search index** — which nobody did. So the catalogue
  *displayed* real names while search still *matched* on raw keys, and `MATCH 'Yield'` did not find
  "Yield, Tomatoes — Sudan" (R491).
* A regional title run reported "titles APPLIED: 910,887" while **all 796,716** regional titles had
  silently taken the fallback branch, because a new API method was fed through a parser built for a
  different one. Fallback titles count as titles, so the aggregate could not distinguish success
  from total failure (R419).
* Four separate tools reported a *publisher* gap that was a *reader* bug — a regex using `[^>]*` over
  XML whose attribute values legally contain `>`, a parser requiring a column the domain does not
  have, a failed query turned into "0 rows, nothing to push" (R483, R484).

The durable lesson, and it generalises well beyond titles: **when a value lives in two tables and
only one is displayed, the undisplayed one rots silently.** Test the consumer's path, not the one
the page renders.
## 4. Every updating mechanism

This section describes, exhaustively and from the code, everything in
`E:/research/econfindatalibrary` that causes hosted data to refresh: the registry that
decides *what* can update, the four schedulers that decide *when and where*, the
orchestrator that runs one pass, the six strategies that decide *how*, the change-detection
machinery, the state store, the health gate, the retry queue, every GitHub Actions workflow,
and the workstation runner.

Every number, filename, constant and behaviour below was read out of a file or measured with
a command on 2026-08-30. Where something could not be established, it says
**NOT ESTABLISHED** and names what would establish it.

---

### 0. The shape of the system in one paragraph

A YAML **registry** lists every source and assigns it one **strategy**. Four independent
**schedulers** select subsets of that registry and invoke the same **orchestrator**
(`python -m updater.run`). The orchestrator walks the selected units in cost-then-staleness
order, asks each one's strategy "did upstream move?", runs the per-source **fetcher** when it
did, publishes the parquet through a never-shrink **merge**, re-derives the CSVs of exactly
the series that changed, and records everything in a single SQLite **state store** that is
pulled from and pushed back to Cloudflare R2 under a compare-and-swap. A **health gate** then
reads that state and reddens the run if any live source is past twice its SLA.

```
registry.yaml (282 sources)
      |
      +-- scheduler 1: updater-daily.yml       -> live: true                (229 sources)
      +-- scheduler 2: updater-heavy.yml       -> ALL=[...] matrix literal  ( 34 sources)
      +-- scheduler 3: sec-edgar-daily.yml     -> sec_edgar / sec_edgar_xbrl
      +-- scheduler 4: tools/run_local_heavy.ps1 -> run_location: local     ( 29 sources)
      |
      v
updater/run.py --pull-state -> updater/orchestrate.run_once() -> --push-state
      |                              |
      |                              +-- strategies/<name>.py -> fetchers/<source_id>.py
      |                              +-- merge.merge_and_write (atomic, dedup, never-shrink)
      |                              +-- derive.derive_and_put (CSV re-derive -> R2)
      v
data/_aqueduct/state.db  <->  r2://econ-data/_aqueduct/state.db.zst
      |
      +-- updater/health.py --fail-past-2x-sla  (red run = GitHub notification)
```

---

### 1. `updater/registry.yaml` — the source registry

**File:** `E:/research/econfindatalibrary/updater/registry.yaml` (634,567 bytes)
**Loader / validator:** `E:/research/econfindatalibrary/updater/registry.py`

The registry is the authoritative assignment of **exactly one strategy per source**. Its own
loader docstring states the design rule that matters most:

> "the rollout perimeter lives in DATA, never in a Python source list (the whack-a-mole
> pattern reborn)"

Top-level shape:

```yaml
version: 1
generated_from: "matrix + classifications"
sources:
  - source_id: eia
    ...
```

Measured on 2026-08-30: **282 entries** under `sources:`.

#### 1.1 Fields, and how often each appears

Counted across all 282 entries:

| Field | Present on | What it does |
|---|---:|---|
| `source_id` | 282 | The primary key. Also the store directory, the catalog id prefix (`<source_id>:<native>`), and the fetcher module name. |
| `strategy` | 282 | One of six names (§4). Validated against `registry.VALID_STRATEGIES`. |
| `strategy_reason` | 282 | Free prose: *why* that strategy, usually naming the upstream's date filter or vintage header. Read by humans and by `tools/gen_runbook.py`; no code branches on it. |
| `cadence` | 282 | How often **we poll**. Drives `Strategy.is_due()` and the health SLA. |
| `refresh_cost` | 282 | `fast` / `small` / `medium` / `large` / `giant`. Drives the lease TTL (§3.6). |
| `out_dir` | 282 | Store subdirectory under `data/clean_full/`. Defaults to `source_id`. |
| `review` | 282 | Bookkeeping flag from the generator. |
| `scripts` | 281 | The original ingest script(s), e.g. `['jobs/ingest_eia.py']`. Documentation + force-refresh procedure. |
| `adapter` | 274 | A dict describing the incremental contract: `vintage_signal`, `since_param`, and notes. Human-facing; the actual behaviour lives in the fetcher module. |
| `live` | 267 | Boolean rollout perimeter. **229 `true`, 38 `false`, 15 entries omit it** (absent = not live). |
| `data_cadence` | 110 | How often the **publisher releases** — a different fact from `cadence`. Affects ONLY the health lateness clock. 101 `annual`, 5 `monthly`, 4 `quarterly`. |
| `matrix` | 104 | Legacy capability-matrix payload (`refresh_mechanism`, `force_refresh_procedure`). |
| `upstream_verified` | 39 | An expiring completeness claim (§7.4). |
| `run_location` | 29 | `local` on all 29 that carry it. Enforced by `orchestrate._wrong_location`. |
| `fetcher_verified` | 10 | Evidence note that the fetcher was proved against the publisher. |
| `license` | 4 | Licence text/id for the few entries that carry one inline. |
| `attribution` | 4 | Required attribution string. |
| `keys_or_blockers` | 3 | Named API keys or known blockers. |
| `storage_layout` | 3 | Non-default on-disk layout note. |
| `catalog_scope` | 1 | `subset` — declares the catalogue is a deliberate curated slice of a much larger store (only `eia`). Changes the zero-mapped verdict in §6.4. |
| `id_collision_warning` | 1 | Warning note. |
| `refreshed_elsewhere` | 1 | Note that this id is kept fresh outside the orchestrator. |

A real entry, printed verbatim from the loaded YAML (`eia`, truncated at 300 chars per field):

```
source_id: eia
catalog_scope: subset
live: True
run_location: local
scripts: ['jobs/ingest_eia.py']
strategy: bulk_snapshot_if_changed
strategy_reason: EIA bulk zips are whole-series snapshots with NO date param; the public
  manifest gives a per-dataset last_updated, so re-download+rebuild only datasets whose
  last_updated advanced past the stored value.
cadence: daily
refresh_cost: large
out_dir: eia
review: True
adapter: {'vintage_signal': "GET https://api.eia.gov/bulk/manifest.txt -> per-dataset
  'last_updated' + 'accessURL'. Compare vs stored data/clean_full/eia/_summary.json
  last_updated; skip dataset when manifest last_updated <= stored.", 'since_param': ...}
matrix: {'refresh_mechanism': ['overwrite-multi-file'], 'force_refresh_procedure': [...]}
```

There is also an optional `units:` list (not used by any current entry's top level in the
counts above — it appears inside entries that split a source into named sub-units).
`registry.to_units()` materialises one `Unit` per `units[]` entry, otherwise a single
implicit `_all` unit covering the whole source directory.

#### 1.2 Distribution of the key fields

**Strategy** (all 282 / live-only 229):

| Strategy | All | `live: true` |
|---|---:|---:|
| `bulk_snapshot_if_changed` | 178 | 139 |
| `overwrite_if_changed` | 61 | 56 |
| `extend_by_date` | 24 | 18 |
| `sdmx_delta` | 15 | 14 |
| `giant_changed_units` | 3 | 2 |
| `manual_vintage` | 1 | 0 |

**Cadence** (poll frequency): weekly 105, annual 59, monthly 50, irregular 26, quarterly 23,
daily 14, static 5.

**Refresh cost**: fast 151, large 67, medium 54, small 7, giant 3.

**Run location**: 253 entries carry no `run_location` (treated as `any`); 29 carry `local`:

```
bea, bis, bls, cbs_nl, census, cepii_baci, cepii_gravity, comtrade, eia, faostat,
gus_dbw, imf_imts_direct, istat, noaa, oecd, ons_uk, statcan,
unctad_biotrademerch, unctad_creativegoodsvalue, unctad_criticalmineralstradebypart,
unctad_gstptradematrix, unctad_nonplasticsubststradebypartner, unctad_oceantrade,
unctad_tradefoodcatbyproc, unctad_tradefoodprocbycat, usda, vdem, whr, wid
```

#### 1.3 Validation: `registry.validate()`

`E:/research/econfindatalibrary/updater/registry.py`, lines 34-57. It returns a list of
problems; an empty list means valid. It flags:

- an entry with no `source_id`;
- a duplicate `source_id`;
- a `strategy` outside `VALID_STRATEGIES`;
- a missing `cadence`;
- a `live` value that is not a boolean (so `live: "yes"` cannot silently widen the perimeter);
- **`len(sources) != expected_count`** when an expected count is supplied.

#### 1.4 `EXPECTED_SOURCE_COUNT` — why a mismatch refuses ALL runs

**File:** `E:/research/econfindatalibrary/updater/config.py`, line 265:

```python
EXPECTED_SOURCE_COUNT = 282  # +unctad_tradefoodprocbycat (giant #15) 2026-08-17
```

Measured registry length on 2026-08-30: **282**. They agree, so runs are permitted.

`orchestrate.run_once()` calls the validator as its **first action**, before any source is
touched:

```python
problems = registry.validate(registry.load(),
                             expected_count=config.EXPECTED_SOURCE_COUNT)
if problems:
    raise SystemExit("registry invalid (fix before running):\n  " + "\n  ".join(problems[:20]))
```

`SystemExit` there aborts the whole process. It is not a warning and it does not skip the
offending entries — **it refuses the entire run, for every source**. The comment block above the
constant in `config.py` (lines 18–264) is the changelog of every source ever added or retired,
and one of them records exactly what this costs when it is forgotten:

> "I ADDED THE ENTRIES AND NOT THIS NUMBER, AND THAT TOOK THE WHOLE UPDATER DOWN. registry
> validation runs before any source does, so from that commit until this one every run — cloud
> and local — exited 1 at 'expected 141 sources, found 144' having fetched NOTHING."

The rationale is deliberate: a source appearing in the registry without anyone declaring it is
exactly the failure the tripwire exists to catch, so the count is **asserted** in a different
file from the one it protects.

Because a stale counter takes the fleet offline for a whole day, the alarm was moved earlier:
`.github/workflows/preflight.yml` runs `tools/preflight_registry.py` on every push touching
`updater/registry.yaml`, `updater/config.py`, `updater/strategies/fetchers/**`, or the
preflight files themselves — so the mismatch fails at push time, not at 06:00 UTC.

---

### 2. THE SCHEDULERS — enumerated from the code

The ledger (R411, R262) records that a previous count of these was wrong twice in one session,
each time by dropping a term. R262's rule is "ENUMERATE THE SCHEDULERS FROM THE CODE — grep for
what reads the registry and what dispatches sources — rather than from any list, including my
own." What follows is that enumeration.

The repository's own auditor, `E:/research/econfindatalibrary/tools/audit_schedule_coverage.py`,
independently computes the same union in `scheduled_sources()` and names four mechanisms plus a
fifth evidence-gated one. My reading of the code agrees with it, and adds two more things that
refresh data outside the orchestrator entirely.

#### 2.1 Scheduler 1 — `registry live: true`, via `updater-daily.yml`

**Selector:** `orchestrate.run_once()` reads `AQUEDUCT_LIVE_ONLY`; `updater-daily.yml` sets it
to `'1'`. With it set, a unit whose `live` flag is falsy and which was not named by an explicit
`--source` is counted in `not_in_rollout` and **never executed**:

```python
live_only = os.environ.get("AQUEDUCT_LIVE_ONLY", "").strip() in ("1", "true", "yes")
...
if live_only and not _is_live(unit) and not sources:
    not_in_rollout.append(unit.source_id)
    continue
```

**Population: 229 sources.** Runs twice daily (06:00 and 18:00 UTC).

#### 2.2 Scheduler 2 — the `updater-heavy.yml` matrix literal

**Selector:** a shell string inside the `setup` job of
`E:/research/econfindatalibrary/.github/workflows/updater-heavy.yml`, line 138:

```bash
ALL='["un_wpp","bundesbank","imf_gfsbs_direct","imf_gfscofog_direct","imf_gfssfcp_direct",
"imf_gfssoef_direct","imf_gfssoo_direct","imf_gfsssuc_direct","imf_pip_direct",
"imf_dip_direct","imf_mfsdc_direct","imf_mfsma_direct","imf_mfsofc_direct",
"imf_mfsfmp_direct","imf_mfsir_direct","imf_bopagg_direct","imf_psbs_direct",
"imf_ctot_direct","imf_sdg_direct","imf_namain_direct","imf_icsd_direct","imf_fd_direct",
"imf_hpd_direct","imf_gslgrghts_direct","imf_gslepm_direct","imf_gssdo_direct",
"imf_gsatf_direct","imf_gsli_direct","imf_er_direct","imf_eer_direct","imf_ls_direct",
"imf_pi_direct","imf_piwca_direct","imf_qgfs_direct"]'
```

The literal is fed through `fromJSON` into `strategy.matrix.source`, and each job runs
`python -m updater.run --source <that one id>`. An explicit `--source` bypasses
`AQUEDUCT_LIVE_ONLY` and bypasses the `run_location` check, so **matrix membership schedules a
source regardless of its `live` flag**.

**Population: 34 sources.** Measured: only **3** of them are `live: true`
(`un_wpp`, `imf_gfssoef_direct`, `imf_gfsssuc_direct`); the other **31 are `live: false` and
are reachable by no other scheduler**. That is exactly the 31-source blind spot R411 records.
None of the 34 carries `run_location: local`.

Runs twice daily (03:00 and 15:00 UTC), `max-parallel: 1`, `fail-fast: false`.

#### 2.3 Scheduler 3 — `sec-edgar-daily.yml`

**Selector:** hard-coded. The workflow does not touch the registry or the orchestrator at all;
it runs `python tools/refresh_sec_edgar.py --days N --apply --d1`. The header explains why:

> "It does not fit the main orchestrator cleanly — its layout is one grouped parquet per
> COMPANY (`clean_grouped/sec_edgar/<ID>.parquet`, metric/obs_date/value/vintage_date), not the
> long series_key/obs_date/value shape the Result contract and the generic CSV derive assume."

The registry ids it covers are `sec_edgar` and `sec_edgar_xbrl` — two ids, one product, a
crossing the auditor flags as **reserved for Ahmed** because repairing it would change public
series ids.

**Population: 1 served product** (catalogued under `sec_edgar`, registry-named
`sec_edgar_xbrl`). Runs daily at 08:00 UTC, `--days 4` by default (deliberate overlap so a
missed or failed run leaves no permanent hole; each companyfacts payload is full history, so
re-refreshing a company is idempotent).

#### 2.4 Scheduler 4 — `tools/run_local_heavy.ps1`, selecting on `run_location: local`

**Selector:** `E:/research/econfindatalibrary/tools/_list_local_sources.py`, which is the only
definition of the local route:

```python
ids = sorted({e.get("source_id") for e in (d.get("sources") or [])
              if e.get("run_location") == "local" and e.get("source_id")})
print(",".join(ids))
```

`run_local_heavy.ps1` shells out to that lister, refuses to treat a non-zero exit as an empty
registry, and then runs `python -m updater.run --source <id>` for every id returned. **It does
not consult `live` at all.**

**Population: 29 sources** (listed in §1.2). Measured: 18 are `live: true`, **11 are
`live: false` and are scheduled anyway** (`bis`, `bls`, `cbs_nl`, `census`, `faostat`,
`gus_dbw`, `imf_imts_direct`, `istat`, `oecd`, `statcan`, `vdem`). Fires on the workstation's
5-minute guard tick, gated to at most one real pass per 20 hours (§9).

#### 2.5 The union, and the ten sources no scheduler selects

Computed on 2026-08-30 over the four selectors above:

| Selector | Sources |
|---|---:|
| `registry live: true` | 229 |
| `updater-heavy.yml` matrix | 34 |
| `sec-edgar-daily.yml` | 2 registry ids |
| `run_location: local` | 29 |
| **Union (registry members)** | **272 of 282** |

The **10 registry entries no scheduler selects**:

| source_id | strategy | cadence | live |
|---|---|---|---|
| `cftc` | extend_by_date | weekly | (absent) |
| `edgar_13f` | extend_by_date | quarterly | (absent) |
| `gii` | manual_vintage | annual | (absent) |
| `insee_sirene` | bulk_snapshot_if_changed | monthly | (absent) |
| `ksh` | overwrite_if_changed | annual | false |
| `owid` | bulk_snapshot_if_changed | monthly | false |
| `pxweb` | bulk_snapshot_if_changed | irregular | (absent) |
| `sipri_polity` | overwrite_if_changed | annual | (absent) |
| `worldbank_extra` | extend_by_date | irregular | (absent) |
| `zillow` | bulk_snapshot_if_changed | monthly | false |

These are registered — so they count against `EXPECTED_SOURCE_COUNT` and appear in the health
table — but nothing schedules them. They update only on a manual `workflow_dispatch` or a
manual local invocation.

**Scheduled is not the same as able to run.** Of the 272 union members, three have no fetcher
module at `updater/strategies/fetchers/<source_id>.py`: `cbs_nl`, `gus_dbw` (both crawled by
the guard-run scripts of §2.6, not by the orchestrator) and `sec_edgar_xbrl` (refreshed by its
own workflow). For any other fetcher-backed source, a missing module makes
`orchestrate._has_adapter()` return False and the source is filed `PENDING — no adapter built`
and skipped; if it is `live: true`, the whole run then fails deliberately (§3.9).

#### 2.6 Two further mechanisms that refresh data outside the orchestrator

These do not read `registry.yaml`, so they are not "schedulers" in the sense above — but they
do cause hosted data to change, and omitting them would misstate the system.

**(a) `RELAUNCH_GUARD.ps1` long-running crawlers.**
`E:/research/econfindatalibrary/RELAUNCH_GUARD.ps1` tracks three crawler jobs and relaunches
any that is not currently running:

| name | matched command | launched as |
|---|---|---|
| `cbs_nl` | `ingest_cbs_nl.py` | `python jobs/ingest_cbs_nl.py` |
| `gus_dbw` | `ingest_gus_dbw.py` | `python jobs/ingest_gus_dbw.py` |
| `istat_sliced` | `ingest_istat_sliced.py` | `python jobs/ingest_istat_sliced.py` |

(A fourth entry, `dbnomics_istat`, was removed on 2026-08-03: it resurrected a puller for a
banned domain every five minutes, which the comment calls "the most durable possible form of
the violation".)

Both `cbs_nl` and `gus_dbw` genuinely *re-crawl* rather than only backfill:
`jobs/ingest_cbs_nl.py` gates on CBS's own per-table `Modified` timestamp and records what it
confirmed or replaced in `data/clean_full/cbs_nl/_modified.json` (184,219 bytes, last written
2026-08-28); `jobs/gus_dbw_refresh.py` re-sweeps the recent-year tail and upserts by year
boundary, recording per-area before/after row counts in
`data/clean_full/gus_dbw/_refresh_state.json` (2,812 bytes, last written 2026-08-24). The
auditor counts these two as scheduled **only when that artifact exists**, because "membership
in a schedule is not the ability to run".

**(b) `RELAUNCH_GUARD.ps1` long one-off jobs**, run through
`E:/research/econfindatalibrary/tools/run_guarded_job.ps1`, which writes `logs/<name>.DONE`
**only on exit 0** so a finished job is not restarted every five minutes:

| name | command |
|---|---|
| `derive_statcan` | `python -u tools/derive_statcan_tables.py --bucket econ-data --max-rows 3000000 --skip-existing --workers 20` |
| `derive_noaa` | `python -u -m core.derive_csv --bucket econ-data --source noaa --workers 16 --skip-newer-than 2026-08-03T00:00:00Z` |
| `rekey_eurostat` | `python -u tools/rekey_eurostat.py --dry-run` (read-only measurement) |

**(c) Post-run metadata syncs inside `updater-daily.yml` and `updater-heavy.yml`.** These
change what a user *sees* without changing parquet bytes: `core/sync_state_d1.py` pushes
freshness (unit_state / source_state) to D1; `core/sync_catalog_d1.py` upserts the catalogue
rows for series derived this run, reading the ids the orchestrator appended to
`data/_aqueduct/pending_catalog_sync.txt` (4,980,295 bytes on 2026-08-30).

**No Cloudflare Worker cron triggers exist.** Checked: `api/worker/wrangler.toml` contains no
`[triggers]` / `crons` block. The Worker only serves requests.

---

### 3. `updater/orchestrate.py` — the orchestrator

**File:** `E:/research/econfindatalibrary/updater/orchestrate.py` (112,887 bytes, 1,847 lines).
Entry point: `run_once(sources, strategies, cadences, force, dry, store, blob)` at line 1225.
CLI wrapper: `updater/run.py` (`python -m updater.run`).

The contract, stated in `strategies/base.py`:

> "The orchestrator only ever runs a unit when `is_due()` AND (`detect_change()` or forced).
> A unit is marked `ok` only after `run()` reports a successful atomic publish."

#### 3.1 The whole run, step by step

1. **Validate the registry** against `EXPECTED_SOURCE_COUNT`. Mismatch ⇒ `SystemExit`, nothing runs (§1.4).
2. **Materialise units** — `registry.all_units()`. One `Unit` per `units[]` entry, otherwise one implicit `_all` unit per source.
3. **Reconcile `--source` against reality.** Any requested id with no unit raises `SystemExit` naming it, rather than silently running a smaller set: *"a missing source is only noticed weeks later as unexplained staleness."*
4. **Estimate cost** — `StateStore.run_cost_estimate()` (§3.3).
5. **Order** — `order_units(units, costs, staleness_key)` (§3.2).
6. **Set the run deadline** — `AQUEDUCT_RUN_BUDGET_MIN` (§3.5).
7. **Loop over units, strictly serially** (§3.4). Per unit, in order:
   - CLI filters (`--source` / `--strategy` / `--cadence`);
   - rollout perimeter (`AQUEDUCT_LIVE_ONLY`);
   - location check (`run_location`);
   - first-pass protection (`FIRSTPASS_DIRS`);
   - **budget lookahead** — refuse to *start* a unit whose worst case would cross the ceiling;
   - adapter check (`_has_adapter`), broken-import branch, `PENDING` branch;
   - due check (`Strategy.is_due`);
   - `detect_change()` under a SIGALRM window;
   - lease claim;
   - `strat.run(unit, since=...)` under a second SIGALRM window;
   - CSV re-derive + retry-queue drain under a third SIGALRM fence;
   - state writes; lease release; per-unit cost line.
8. **Summaries** — NOT DUE, WRONG LOCATION, PROTECTED, RUN BUDGET, rollout perimeter, PENDING.
9. **Fail the run** if any `live: true` source had no runnable adapter (§3.9).

#### 3.2 Ordering — cost band ladder first, staleness within the band

```python
FAST_LANE_SECONDS = 120.0
BAND_LADDER_SECONDS = (600.0, 3600.0)   # rungs above fast_lane_seconds

def order_units(units, costs, staleness_key, fast_lane_seconds=FAST_LANE_SECONDS):
    def band(unit):
        est = costs.get(unit.source_id)
        if est is None or est < fast_lane_seconds:
            return 0
        for i, ceiling in enumerate(BAND_LADDER_SECONDS):
            if est < ceiling:
                return i + 1
        return len(BAND_LADDER_SECONDS) + 1
    return sorted(units, key=lambda u: (band(u), staleness_key(u)))
```

Four bands result: **0** = never-run or under 120 s (the *fast lane*); **1** = 120–600 s;
**2** = 600–3600 s; **3** = 3600 s and up. A source with **no cost on record sorts into band 0
and therefore has absolute priority for its first turn** — a new source is guaranteed a run.

The measurements that produced this design, quoted from the code:

- 2026-08-02, 106 live cloud sources: 68 cost < 2 min each (24.5 min for all of them together), 11 cost 2–10 min (40.7 min), 27 cost ≥ 10 min (**1,031.4 min — 4.3× the whole 240-min budget**). Under one flat staleness order the budget was gone after 20 sources and 76 were not attempted; `cnb` (4.9 s) and `frankfurter` (5.6 s) were among the skipped, both RED-SLA purely from queueing behind a 400-minute job.
- 2026-08-18: the single cheap/expensive split proved insufficient — the expensive band had grown to 118 sources / ~4,471 min of MAX-estimated work rotating through ~217 min of post-cheap budget, a **20+ day rotation**, and five daily sources went 12 days unattempted. Hence the ladder.

**Staleness within a band is cadence-normalised**, not absolute age:

```python
CADENCE_DAYS = {"daily": 1.0, "weekly": 7.0, "monthly": 30.0,
                "quarterly": 91.0, "annual": 365.0}

def overdue_key(last_utc, cadence, now_utc):
    if not last_utc:
        return float("-inf")            # never-run sorts first of all
    ...
    return -(age_days / days)           # most cadence-overdue first
```

An unknown cadence counts as monthly (30 d). An unparseable timestamp is treated as never-run
so the unit still gets a turn. Note this table differs from
`strategies/base.CADENCE_DAYS` (`monthly: 28, quarterly: 90`, plus `irregular: 7` and
`static: 10**6`) — the base table governs *due-ness*, this one governs *ordering priority*.

#### 3.3 The cost estimate

`StateStore.run_cost_estimate(sample=5)` in `updater/state.py`:

- **MAX** over the last 5 runs of each source, not mean or latest — *"it must not be fooled by one fast `no_change` on a source that takes 40 minutes whenever there IS a change."*
- **Floored at the latest non-fail run's duration** (`status IN ('ok','no_change','partial')`). This was added on 2026-08-19: `ecb`'s chronic seconds-long `transient_fail`s rolled its 2,400 s success out of the sample window, the estimate collapsed, it infiltrated the cheap band, and its next real attempt detonated for 40 minutes with 46 sources unattempted.
- A source with no runs on record is **absent** from the mapping, so the caller decides what never-run means rather than a `0` asserting "free".

#### 3.4 Sources run SERIALLY

There is no parallelism across sources. The unit loop in `run_once` is a plain `for` loop; the
`Deadline` docstring in `strategies/fetchers/_common.py` states it explicitly:

> "orchestrate.py runs sources strictly serially, so a single slow upstream stalls every…"

Parallelism exists only *inside* one unit: `derive.derive_and_put` uses a thread pool
(`AQUEDUCT_DERIVE_WORKERS`, default 8), and individual fetchers may use their own pools.

#### 3.5 The whole-run budget — `AQUEDUCT_RUN_BUDGET_MIN`

```python
try:
    run_budget_min = float(os.environ.get("AQUEDUCT_RUN_BUDGET_MIN", "240"))
except ValueError:
    run_budget_min = 240.0
run_deadline = time.time() + run_budget_min * 60.0 if run_budget_min > 0 else None
```

The value actually in force, per caller:

| Caller | `AQUEDUCT_RUN_BUDGET_MIN` | Where |
|---|---:|---|
| code default | 240 | `orchestrate.py:1336` |
| `updater-daily.yml` | **290** | env block, line 139 |
| `updater-heavy.yml` | **240** | env block, line 187 |
| `tools/run_local_heavy.ps1` | **2880** requested, then **clamped** to the minutes remaining before the next CI blackout window minus 25 | lines 213, 279–283 |

The budget is a **start gate with worst-case lookahead**, not a point check:

```python
_est_s = _costs.get(unit.source_id)
_worst_min = 2 * _unit_timeout_min()
if _est_s is None:
    _need_min = _worst_min                      # never run: reserve the full worst case
else:
    _need_min = min(_worst_min, max(2 * 1.5 * (_est_s / 60.0), 10.0))
if (run_deadline is not None
        and time.time() + _need_min * 60.0 > run_deadline
        and not (sources and len(sources) == 1)):
    budget_skipped.append(unit.source_id)
    continue
```

A unit owns up to **two** SIGALRM windows (probe + update), hence the `2 ×`. The reserve is
sized per unit at 1.5× its measured cost per window, floored at 10 minutes — the previous rule
reserved the *fleet* worst case (2 × 45 = 90 min) against every unit, which refused `boc`
(measured 14.6 min) a turn with 89 minutes still on the clock and took it RED-SLA on a daily
cadence. **A single-source dispatch is never budget-capped** — a proof run must not report
success having skipped the source under test.

Skipped units are **untouched**: not due-marked, not vintage-advanced, so the next tick takes
them first. The run says so loudly:

```
[orchestrator] RUN BUDGET 290 min SPENT — N source(s) NOT ATTEMPTED this run: ...
[orchestrator] this run is INCOMPLETE by design — stopping early beats being killed at the
300-minute ceiling, which would also lose the state push, the D1 syncs and the digest for
the sources that DID succeed.
```

#### 3.6 Per-unit timeouts, deadlines and leases

**`AQUEDUCT_UNIT_TIMEOUT_MIN` — default 45 minutes**, `0` disables:

```python
def _unit_timeout_min() -> float:
    try:
        return float(os.environ.get("AQUEDUCT_UNIT_TIMEOUT_MIN", "45"))
    except ValueError:
        return 45.0
```

`updater-heavy.yml` overrides it to **180** (one giant source owns the runner).
`updater-daily.yml` does not set it, so the daily runs at **45**.

The mechanism is **SIGALRM**, not a thread — *"injecting an exception across threads needs
ctypes and cannot interrupt a blocking C call."* `UnitTimeout` is a plain `Exception` so the
unit handler demotes *that source* and the run continues. Interrupting is safe for data because
`merge_and_write` publishes through `write_table_atomic`, so a half-written store is
unreachable.

**POSIX only.** `signal.setitimer` does not exist on Windows, so on the workstation this is a
documented no-op — announced once per run:

```
[orchestrator] per-unit hard timeout ARMED at 45 min (SIGALRM)
[orchestrator] per-unit hard timeout UNAVAILABLE on this platform (no signal.setitimer);
               relying on per-source Deadlines only
```

Each window is **clamped by the run remainder**:

```python
def _unit_window_min() -> float:
    t = _unit_timeout_min()
    rem = _remaining_run_min()
    if rem is None:
        return t
    return max(0.0, min(t, rem / 2.0))
```

The arithmetic is what makes the start gate safe: probe ≤ R/2 and update ≤ (R − probe)/2, so
probe + update ≤ R. A unit that starts inside the budget cannot outlive it.

Three windows are armed per unit:

| Window | Length | Introduced because |
|---|---|---|
| `detect_change` | `_unit_window_min()` | `owid`'s probe (HTTP HEAD over ~3,786 chart URLs) ran 150 and then 212 minutes outside any cap and killed two entire daily runs. *"A probe gets the same ceiling as the fetch: any vintage check that needs longer than the unit timeout is a fetch wearing a probe's name."* |
| `strat.run` | `_unit_window_min()` | `ssb` ran 2 h 31 m inside one `update()` and took GitHub's 300-minute ceiling with it. |
| csv phase | `max(1, min(60, remaining + 2))` | `abs`'s post-merge phase ran 115 silent minutes past every soft budget until the 285-min step kill destroyed the state push, the D1 syncs and the digest. |

**Leases** prevent two runners touching the same unit. `StateStore.claim_lease` is a
DB-arbitrated compare-and-set (`ON CONFLICT ... DO UPDATE ... WHERE leases.expires_utc < ? OR
leases.owner = ?`), so it is TOCTOU-safe. TTL scales with `refresh_cost`:

```python
_TTL_BY_COST = {"fast": 7200, "medium": 7200, "large": 43200, "giant": 172800}   # seconds
```

Default 7200 s (2 h) for anything else. `large` = 12 h, `giant` = 48 h. A refused unit is
**logged and recorded** with the holder's name and expiry — this branch used to `continue`
silently and made a two-day `eia` outage invisible (a dead run held a 64-hour lease and the
source simply looked "not due").

#### 3.7 The other reasons a unit is skipped — all announced

Every `continue` in the loop prints. The code's own rule (R101) is *"a deliberate skip that
leaves no trace is indistinguishable from a bug."*

| Branch | Log line | Meaning |
|---|---|---|
| rollout perimeter | `rollout perimeter: N non-live source(s) not executed (AQUEDUCT_LIVE_ONLY=1)` | `live: false` on a scheduled cloud run |
| wrong location | `WRONG LOCATION <key> — needs run_location=local, running on cloud` | merge peak exceeds a 16 GB runner |
| protected | `PROTECTED <key> — in-flight backfill, not attempted this run (FIRSTPASS_DIRS)` | `FIRSTPASS_DIRS = {"cbs_nl", "gus_dbw", "dbnomics"}`, matched on **both** `source_id` and the unit's output directory basename |
| budget | `RUN BUDGET N min SPENT — …` | start-gate lookahead |
| broken adapter | `BROKEN <src> — adapter import failed: …` | module exists but raises on import; recorded `transient_fail`; run-failure if live |
| no adapter | `PENDING <key> — no adapter built for strategy=…; not attempted` | fetcher module absent |
| not due | `NOT DUE <key> — cadence=…, last_success=…; skipped (use --force to override)` | cadence gate |
| leased | `LOCKED <key> — locked: held by <owner> until <expiry> …` | another run holds it |

`_wrong_location` is enforced **only when no explicit `--source` was given**. An explicit
`--source` overrides it (the workstation job depends on that) but now prints a warning,
because a manual cloud dispatch of a `run_location: local` source runs keyless there and
writes false structural verdicts.

#### 3.8 The failure contract

| `Result.status` | Set when | Effect |
|---|---|---|
| `ok` | net-new rows merged | `last_success_utc` advanced, `upstream_vintage` advanced, `source_state.status='ok'` |
| `no_change` | fetch ran, nothing new | same as `ok` for freshness bookkeeping |
| `partial` | some sub-units transient-failed, **or** the budget deferred some, **or** the CSV step failed | `last_success_utc` **NOT** set; `upstream_vintage` **NOT** advanced |
| `transient_fail` | the pass could not run at all (timeout, 5xx, network) | nothing advanced; immediately retryable |
| `definitive_fail` | raised as `DefinitiveError` (structural break) | recorded as `partial` by the unit handler |

The rule that a **partial run never sets `last_success_utc`** is deliberate and load-bearing
(ledger R231): a partial is not a success, and the health gate and `/v1/last-updates` must keep
saying so. Its cost is that a permanently-partial source reads as never-having-succeeded, which
is why `_deferral_only` / `ROTATING` exists in the health gate (§7.3) and why `is_due()` has a
`PARTIAL_RETRY_DAYS` escape (§4.1).

An **earned `no_change`** — where the probe returned a token equal to the stored one — *is*
recorded (`last_success_utc` advances) so a quiet-but-healthy source does not rot into RED-SLA.
An **undeterminable** probe records nothing, because nothing was verified.

`last_obs_date` **never regresses**: `if old_last and new_last and str(new_last) < str(old_last): new_last = old_last`.

#### 3.9 The run-failure rule for live sources

```python
if live_set:
    for k, s in results:
        print(f"  {s:16} {k}", flush=True)
    raise SystemExit(
        f"[orchestrator] RUN FAILURE: {len(live_set)} live-tier source(s) have no "
        f"runnable adapter: {', '.join(live_set)} — build the fetcher or remove "
        f"`live: true` from registry.yaml (no silent skips inside the rollout perimeter, §5.3)")
```

A source inside the rollout perimeter may never be silently skipped. The per-unit results are
printed first so the log still shows what *was* processed before the failure.

---

### 4. THE STRATEGIES

Six strategies, all registered in `updater/strategies/__init__.py` via a `@register` decorator.
Each implements three methods from `strategies/base.Strategy`: `is_due`, `detect_change`, `run`.

All except `giant_changed_units` delegate to a per-source fetcher module
`updater/strategies/fetchers/<source_id>.py` exposing `current_vintage(unit)` and
`update(unit, since)`. There are **284 non-underscore fetcher modules** (296 `.py` files in the
directory including the shared `_common.py`, `_giant.py`, `_unctad.py`, `_imf_direct.py`, etc.).

#### 4.1 The shared due-check

`strategies/base.py`:

```python
CADENCE_DAYS = {"daily": 1, "weekly": 7, "monthly": 28, "quarterly": 90,
                "annual": 365, "irregular": 7, "static": 10 ** 6}
PARTIAL_RETRY_DAYS = 7

def cadence_due(cadence, last_success_utc, now=None):
    if not last_success_utc: return True
    ...
    return (now - last).total_seconds() >= days * 86400 * 0.9      # 10% slack
```

`Strategy.is_due()` keys on `last_success_utc` when there is one. When there is not **and** the
last attempt was a `partial`, it measures the cadence from that attempt instead, capped at
`min(cadence_days, PARTIAL_RETRY_DAYS)` — because a partial means the pass *ran* and repeating
it tomorrow cannot help if the failures are permanent. `transient_fail` is deliberately
excluded and stays immediately retryable.

The measurement behind this (2026-07-31): 32 of 103 units had never recorded a success and
their typical durations summed to **1,303 minutes against a 240-minute budget** —
`unsdg` 351 min, `ssb` 231, `statfin` 138, `insee_bdm` 105, `stat_estonia` 103, `hagstofa` 64.
The run spent its whole budget on whoever sorted first, every day. `hagstofa` declares
`monthly` and was being re-crawled (1,906 tables, ~55 min) on every run it was offered.

#### 4.2 `bulk_snapshot_if_changed` (S5) — 178 sources (139 live)

For sources delivered as a whole bulk file or dump with **no server-side date filter**, so a
row-level delta is impossible. A cheap **vintage probe** (HTTP `Last-Modified`/`ETag`/
`Content-Length`, a manifest `DateUpdate`/`FileRows`/`FileSize`, a published date, or a content
hash) decides whether to re-download at all:

```python
def detect_change(self, unit, unit_state):
    cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
    self._cur = cur
    stored = (unit_state or {}).get("upstream_vintage")
    if cur is not None and stored is not None and cur == stored:
        return None             # unchanged -> skip the whole re-download
    return cur or "force"       # changed / first run / undeterminable -> fetch (cadence-gated)
```

`run()` re-downloads the bulk, parses to long format, and publishes in **merge** mode, so a
re-snapshot of the same data is an idempotent no-op and a revised vintage updates existing rows
and extends with new ones.

**The duplication invariant:** the emitted `series_key` MUST be stable across snapshots. The
vintage token gates the re-fetch and lives in state — *"it must NEVER be embedded in the
series_key. If it were, every new snapshot would mint brand-new keys and merge_and_write would
append instead of dedup, silently doubling the data."*

#### 4.3 `overwrite_if_changed` (S1) — 61 sources (56 live)

Whole-table sources (small/medium CSV/XLSX/zip/SDMX re-published each release). Mechanically
identical to S5's `detect_change`. The difference is semantic: S1 is for small/medium tables
published as one artifact (overwrite mode), S5 for large multi-part dumps where the honest
publish is a merge.

#### 4.4 `extend_by_date` (S2) — 24 sources (18 live)

For APIs that accept a server-side date filter. Change detection is free:

```python
def detect_change(self, unit, unit_state):
    return "date-tail"          # always attempt the tail
def run(self, unit, since):
    return get_fetcher(unit.source_id).update(unit, since)
```

The fetcher pulls only observations newer than the stored `last_obs_date` and merges them in.
This is the strategy that fixes the "skip if the series already exists" freeze — existing
series get **extended**, not skipped.

#### 4.5 `sdmx_delta` (S3) — 15 sources (14 live)

`class SdmxDelta(ExtendByDate): pass`. Functionally identical to S2; registered under its own
name so SDMX 2.1 and PxWeb entries resolve, and so the giants' engine can later specialise it
per flow. The date tail is expressed as SDMX `?startPeriod=` / `?updatedAfter=` or a PxWeb
time-dimension value selection.

#### 4.6 `giant_changed_units` (S4) — 3 sources (2 live)

For sources holding **thousands of per-flow parquet files** in one directory. A blind re-crawl
is many hours, so the refresh is a catalogue change-feed diff. The engine lives in
`updater/strategies/fetchers/_giant.py::run_giant`, which:

- re-downloads the source catalogue/TOC;
- diffs each flow's upstream last-update/version against a stored per-flow snapshot in the sidecar `<source_dir>/_giant_state.json`;
- selects flows that **changed, are NEW, or whose last run was partial/failed/empty/absent** (so a once-broken flow never freezes);
- materialises **one Unit per changed flow** via `registry.flow_unit()` and fetches it incrementally with server-side `startPeriod`;
- merges per flow under dedup-on-(series_key, obs_date) plus never-shrink;
- returns an honest status (429/timeout → partial and reselect; 200-with-0-rows from a real body → structural; cap overflow → partial).

`detect_change` hashes every flow's upstream last-update into one catalogue token; equal to the
stored one ⇒ skip the whole per-flow sweep.

The strategy file carries a boxed **DESIGNED, NOT RUN** note: a one-time re-key of the existing
~7,750 `clean_full/eurostat/*.parquet` files whose keys still carry the unstable
`LAST UPDATE=…` prefix. It is a data migration and *"this strategy NEVER performs it
implicitly."*

#### 4.7 `manual_vintage` (S6) — 1 source (0 live)

For publishers shipping discrete versioned editions with no incremental API (PWT-, Maddison-,
Barro-Lee-style). Three deliberate differences from S1:

1. **The vintage token is the sole discriminator.** Where S1 forces a re-fetch when the probe returns `None` ("can't tell → fetch anyway, the merge dedups it"), S6 treats an undeterminable vintage on a normal poll as *no new vintage* → clean `no_change`. It fetches only on a genuinely new token, on first run (nothing stored), or under `--force`.
2. **Honest status end to end.** `detect_change` never fetches, so a probe transient surfaces as `TransientError` → `transient_fail`, never laundered into `no_change`.
3. **The `series_key` must be vintage-stable.** Vintage identity lives in the token and in a `vintage_date` column, never in the key and never in the dedup key.

The single registry entry using it is `gii` (annual) — which, per §2.5, **no scheduler
currently selects**.

#### 4.8 The publish invariant every strategy shares

`updater/merge.py::merge_and_write(out_path, new_table, *, mode="merge",
dedup_keys=("series_key","obs_date"), min_ratio=0.97, allow_empty=False, blob=None)`

> "A write either ADVANCES `last_obs_date` / `obs_count` for a unit, or it is a no-op. It NEVER
> replaces good data with fewer or zero rows."

It raises `DefinitiveError` — leaving the existing file untouched — if a published column would
be dropped, the dedup keys are absent, the result is empty (and not `allow_empty`), or the
result would fall **below 97% of the existing row count**. The same code path runs against the
local filesystem (`blob=None`) and against R2 (`blob=R2Blob()`), so local and cloud publishes
cannot drift apart.

A separate guard counts **impossible dates** (far-future obs_dates) per source and reports them
on the per-unit cost line: `IMPOSSIBLE_DATES=N row(s) in M file(s) (worst … e.g. …)`. It does
not block the publish. 273,980 such rows across six sources sat published at 2999-12-31 …
9999-12-31 while the old per-file warning printed on every run and nobody diffed the log.

---

### 5. CHANGE DETECTION

Two independent signals answer two different questions.

#### 5.1 Vintage signals — "did upstream move?"

A strategy's `detect_change()` returns a **token** or `None`. `None` means skip. The token is
stored on `unit_state.upstream_vintage` and compared next run. Crucially:

```python
upstream_vintage=((res.new_vintage or vintage) if ok else (us or {}).get("upstream_vintage")),
```

**The vintage is advanced only on a clean success** (`ok` or `no_change`). A partial or failed
run must not bump it, or the next `detect_change` would skip a source that never fully fetched.
That single line is what makes a capped run resumable rather than silently stale.

Sentinel values in use: `"date-tail"` (S2/S3 — always attempt), `"force"` (S1/S5 —
undeterminable, fetch anyway), `"first-run"` (S6).

#### 5.2 `series_cursors` — "*which series* moved?"

`Result.series_cursors` is an optional `{series_key: last_obs_date}` map that the fetcher builds
**from rows it actually merged**. It is:

- persisted by `StateStore.put_series_cursors(source_id, mapping)` into `series_cursor`, keyed `(source_id, series_key)`;
- written **even on a `partial`**, on the explicit grounds that the parquet holding those observations *did* publish;
- the **exact input** to the CSV re-derive (§6) — `changed = sorted((res.series_cursors or {}).keys())`;
- read by `health.assess()` as the per-series recency signal, and to flag `discontinued` series (`STALE_SERIES_DAYS = 730`).

Because a unit-level `max(obs_date)` can hide a frozen series behind a fresh one, per-series
cursors are what make "this one series stopped" visible at all.

#### 5.3 `cursors_from_table` and friends

`updater/strategies/fetchers/_common.py`:

| Helper | Purpose |
|---|---|
| `cursors_from_table(tbl, cap=CURSOR_CAP, key_col="series_key", date_col="obs_date")` | Max obs_date per key from the table the fetcher **just merged** — reports the series it changed, not every series in the file. Returns `{}` on failure: *"a cursor problem must never sink a good publish."* |
| `merge_cursors(dst, path, **kw)` | Accumulates one file's cursors into `dst`, respecting the cap **across files** (a multi-file source would otherwise blow past it one file at a time). |
| `merge_cursor_map(dst, src, cap=CURSOR_CAP)` | Folds an in-memory map in, respecting the cap. |
| `_max_by_key(tbl, key_col, date_col)` | The sort-and-take-last implementation. Deliberately avoids pyarrow `group_by`: Arrow indexes strings with int32 offsets and past 2 GiB in one column the aggregate **overflows and kills the process** (0xC0000005 / SIGABRT) rather than raising — invisible to the caller's `except Exception`. |

**`CURSOR_CAP = 50_000`** (`_common.py`, line 308). It is a *disclosed* bound: when it bites,
the caller prints the count it dropped, because *"a truncation nobody is told about reads as
'we covered everything'."*

Why a bound exists at all: `orchestrate._catalog_ids_for` runs one SQLite query per changed key
and `put_series_cursors` writes one row per cursor into `state.db` — both linear in the cursor
count, and the state store is pulled and pushed on every run. Why 50,000 specifically: measured
on `ilostat`, whose 1,947 indicators hold ~30.8 **million** distinct store series; its store
keys already carry the `ilostat:` prefix so `_catalog_ids_for` would build
`ilostat:ilostat:…` and map nothing anyway. Every other bulk source is far below the cap
(`fed_board`'s largest release 39,882 series, `fhfa` ~5k, `maddison` 338, `who_hwf` 4,421).

At least 27 modules reference `CURSOR_CAP`; some (`usda`, `abs`, `noaa`) implement their own
rotation on top of it so successive runs advance rather than re-reporting the same head.

---

### 6. FROM CHANGED KEYS TO RE-DERIVED CSVs

This is contract step 5 (§5.7 of the build plan): **any series whose parquet changed in a run
MUST get a fresh CSV in the same run**, and a CSV failure must never crash or roll back the
parquet publish.

#### 6.1 When it runs

```python
def _should_derive_csvs(status: str) -> bool:
    return status in ("ok", "partial")
```

`partial` is included and its exclusion was a real outage: `worldbank_esg` returned `partial`
on 4 of the 4 runs it had ever had, so its CSVs were never re-derived once — 14 of 40 sampled
objects served 2023 values while the store held 2024, and `SH.DYN.MORT:PAK` served 58.5 for
2023 where the publisher had revised it to 57.8. Measured the same day: `hagstofa` 2/25 and
`stat_slovenia` 1/25 objects likewise stale, and ~56 live+served sources had never returned
`ok`.

#### 6.2 The key → catalog-id mapping ladder

Store keys and catalog ids are different namespaces (`frankfurter` stores `EURUSD`, catalogues
`frankfurter:EUR:USD`). `orchestrate._catalog_ids_for(source_id, changed_keys)` tries, in
order, per key:

1. **Exact** — `<source>:<key>` present in `series`.
2. **Flow grain** (`_flow_of`) — PxWeb-family stores are series-grain, catalogues are table-grain. Truncate at the `*.px` segment (immune to colons inside dimension values), else drop the `=`-bearing segments. `_FIRST_SEGMENT_FLOW = {"unsdg"}` takes the first `:`-segment instead. Measured across all nine PxWeb sources: exact-match 0%, flow-match 100%.
3. **Table grain** (`_table_grain_native`) — a fixed selection of dot-part positions, mirroring each source's resolver in `clients/python/econdl/_resolve.py`. `_TABLE_GRAIN` holds **14** IMF `*_direct` entries as `(positions, n_parts, tail_flow, nonempty)`; `_EIA_DEPTH` holds **29** EIA datasets as a prefix depth. Placed **before** the range scan below because that scan is the expensive one.
4. **ECB dataflow expansion** — one bulk file holds every series of a flow at a frequency, so a changed file expands to every catalogued id inside it, via a PK range on `ecb:<FLOW>:<SEG1>.` … `<SEG1>/`. Under the r2 backend it additionally requires the file to be present on this runner.
5. **Split-part expansion** — a table too large for one CSV is catalogued as `<source>:<table>#<part>` with no base id, so a changed table conservatively re-derives all its parts, via the PK range `cand#` … `cand$`.
6. **Punctuation-insensitive match** (`_norm_id`) — compare only the lower-cased alphanumerics. A normalised form claimed by more than one catalog id is **discarded rather than guessed**, because a collision would rewrite one series' CSV with another's data.
7. **Derive-all** — if anything is still unmapped and the source has `0 < n ≤ _DERIVE_ALL_CAP` catalog ids, re-derive *all* of them. **Local backend only.** Under `AQUEDUCT_BACKEND=r2` the runner's store holds only the files this run wrote, so derive-all would fail for every untouched flow (measured: `stat_estonia` "csv_derive failed 949/3437", `dst` "1923/1963", each reading "zero rows matched in N files").

```python
_DERIVE_ALL_CAP = 5000
```

Every catalogue lookup uses a **primary-key range** (`series_id >= 'x:' AND series_id < 'x;'`),
never `WHERE source_id = ?` and never `LIKE`. `series` carries exactly one index — the
`series_id` primary key — so the column form full-scans an 11.9 GB file. Measured 2026-08-30:
`ecb` 7.37 s warm (389 s cold) versus 0.0002 s, same answer; `cso` 7.13 s versus 0.00 s, same
answer (7,896 == 7,896); the `LIKE … ESCAPE` split-part form 1.57 s versus 0.00 s, a 6,872×
difference. `LIKE` is additionally unsafe here because `_` is a wildcard and source ids contain
underscores.

#### 6.3 The derive itself

`updater/derive.py::derive_and_put(series_ids, blob, budget_min=None)`:

- imports the byte contract from `core/derive_csv.py::_series_csv_bytes` — the same projection the Worker serves, never a duplicate;
- **threaded**, `AQUEDUCT_DERIVE_WORKERS` default **8**, each worker with its own blob handle;
- **`PUT_TRIES = 7`** with exponential backoff `2**attempt` (1…64 s), then reports failure — R2 throws transient `ServiceUnavailable`/`SlowDown` throttles that outlast boto's own retries;
- **wall-clock bounded** by `AQUEDUCT_DERIVE_BUDGET_MIN`, **default 45 minutes**, `0` disables:

```python
if budget_min is None:
    budget_min = float(os.environ.get("AQUEDUCT_DERIVE_BUDGET_MIN", "45") or 45)
```

The orchestrator overrides the parameter with the **run remainder**:

```python
def _capped_derive_budget() -> dict:
    rem = _remaining_run_min()
    if rem is None: return {}
    env_b = float(os.environ.get("AQUEDUCT_DERIVE_BUDGET_MIN", "45") or 45)
    cap = min(env_b, rem) if env_b > 0 else rem
    return {"budget_min": max(0.05, cap)}
```

Note the `0.05` floor: `derive.py` treats `budget_min=0` as *disabled/unbounded*, so passing a
raw `0` remainder would unbound the derive at exactly the moment it must not run at all.

Returns `{"put", "failed", "deferred", "deferred_ids", "failed_reasons"}`. A time-based
heartbeat prints every 120 s (`[derive heartbeat] put N, failed M, budget ok|spent`) because
per-id prints only fire when an id completes, and a wedged phase would otherwise be silent.

#### 6.4 Failure, coverage, and the difference between them

The orchestrator distinguishes three outcomes that used to be conflated:

| Outcome | Note prefix | Demotes to `partial`? |
|---|---|---|
| Real derive/PUT failures | `csv_derive failed N/M series [ids…]` | **Yes**, and the ids are queued |
| Budget deferral | `csv coverage note: derive budget spent — N of M id(s) deferred…` | **No** — queued for retry, but "a source that is merely large would otherwise be permanently `partial`" |
| Residue with no catalog row | `csv coverage note: N changed keys have no catalog row for <src> (<measured why>) — served ids coherent` | **No** |
| Zero mapped, catalogue has rows | `csv coherence unmet: N changed series_keys have no catalog mapping…` | **Yes** — the key-form-mismatch class |
| Merged rows but no cursors reported | `csv coherence unmet: fetcher reported no series_cursors for N merged obs` | **Yes** — nothing to queue, the un-bumped vintage forces a re-fetch |
| Vacuous coherence (source has **zero** catalogued series) | — | **No** — measured, not fetcher-declared. `gleif` is the only such source. |

The `catalog_scope: subset` exception (only `eia`) can spare a zero-mapped source, but only
under two refusing guards from the R497 review: the sample is **prefix-aware** (any catalogued
dot-prefix of a sampled key voids the exception), and a **cap-saturated cursor set**
(`len(unmapped) >= CURSOR_CAP`) can never grant it, because truncated evidence proves nothing
about what else changed.

The whole CSV phase is fenced by SIGALRM at `max(1, min(60, remaining + 2))` minutes. On a trip
it is abandoned as a coverage note — cursors are already recorded, the next run re-derives.

---

### 7. `csv_retry_queue`

**Table:** `csv_retry_queue(series_id TEXT PRIMARY KEY, source_id TEXT, enqueued_utc TEXT,
attempts INTEGER DEFAULT 0, last_error TEXT)` in `data/_aqueduct/state.db`.

**What enqueues:**

- `store.enqueue_csv_retry(source_id, csv_deferred, "derive budget spent — deferred, not failed")` — budget deferrals;
- `store.enqueue_csv_retry(source_id, csv_failed, csv_reasons or csv_err)` — real failures, with **per-id reasons** (`failed_reasons`). The queue used to store one summary string per id, which left `cso`'s 22 census series queued for 10 days with the actual exception unrecorded.
- On a crash inside `_derive_changed_csvs`, **only mapped catalog ids** are queued. It used to queue the raw store keys, and `ember` accumulated 161,843 colon-free keys (`'01 Apr 2025 (Tue)|Daily (2 years)|Hard coal'`) that every later drain re-failed on `series_id.split(":", 1)` — 20,000 ValueErrors and ~1 h wasted per run, forever, with the queue never draining.

**What drains:** the orchestrator, inside the same CSV phase, **after** the fresh changes
(fresh first — they are the run's purpose):

```python
_CSV_RETRY_CAP = 20_000
```

- `_split_retry_rows` first **purges** rows whose `series_id` lacks the `<source>:` prefix (they can never resolve) and says so loudly.
- Up to `_CSV_RETRY_CAP` rows are attempted per source per run, through the same `derive_and_put` with the same capped budget.
- Successes are cleared and recorded for the D1 catalog sync; **re-failures simply stay queued and never demote the run** — demoting over old residue would recreate the permanently-partial disease.
- The drain is skipped if the fresh-path CSV fence already tripped (retrying old ids in the same exhausted window is the overrun being fenced).
- There is **no `and not csv_err` gate**: that gate deadlocked exactly the population the queue exists for (`cso`'s 22 census series were their source's only mapped changed ids, so every run's fresh derive failed them, set `csv_err`, and thereby blocked the drain).

**Measured on the local state store, 2026-08-30: 231,782 queued ids.**

| source_id | queued | max attempts |
|---|---:|---:|
| `abs` | 100,000 | 1 |
| `ilostat` | 50,000 | 1 |
| `usda` | 48,047 | 1 |
| `imf_qgfs_direct` | 20,502 | 1 |
| `cso` | 7,256 | 1 |
| `scb` | 2,682 | 1 |
| `stat_estonia` | 2,620 | 1 |
| `ssb` | 672 | 1 |
| `bcrp` | 3 | 1 |

`abs` at exactly 100,000 and `ilostat` at exactly 50,000 are round numbers that look like caps
rather than counts. **NOT ESTABLISHED:** whether those two figures are cap artefacts (two runs
of `_CSV_RETRY_CAP = 20,000` do not produce 100,000, and `CURSOR_CAP` is 50,000) or genuine
totals. Establishing it would need `SELECT enqueued_utc, COUNT(*) FROM csv_retry_queue WHERE
source_id='abs' GROUP BY 1` against the state store to see whether they arrived in one batch.

---

### 8. THE STATE STORE — `data/_aqueduct/state.db`

**Schema:** `updater/state.py`, `DDL` (lines 18–67). One SQLite file, WAL mode,
`synchronous=NORMAL`, 60 s busy timeout.

```sql
CREATE TABLE IF NOT EXISTS source_state(
  source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT,
  last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT,
  enabled INTEGER DEFAULT 1, note TEXT);

CREATE TABLE IF NOT EXISTS unit_state(
  source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT,
  last_success_utc TEXT, last_attempt_utc TEXT, status TEXT,
  last_obs_date TEXT, obs_count INTEGER DEFAULT 0, attempt_count INTEGER DEFAULT 0,
  last_error TEXT, PRIMARY KEY(source_id, unit_id));

CREATE TABLE IF NOT EXISTS series_cursor(
  source_id TEXT, series_key TEXT, last_obs_date TEXT,
  PRIMARY KEY(source_id, series_key));

CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT,
  status TEXT, obs INTEGER, dur_s REAL, note TEXT);

CREATE TABLE IF NOT EXISTS leases(
  key TEXT PRIMARY KEY, owner TEXT, expires_utc TEXT);

CREATE TABLE IF NOT EXISTS csv_retry_queue(
  series_id TEXT PRIMARY KEY, source_id TEXT, enqueued_utc TEXT,
  attempts INTEGER DEFAULT 0, last_error TEXT);
```

#### 8.1 Measured contents (local copy, 2026-08-30)

The local file was last written 2026-08-29 21:31 UTC, with `.state_etag` written 21:34 — i.e.
a freshly pulled copy.

| Fact | Value |
|---|---:|
| `state.db` size | **11,300,503,552 bytes** (2,758,912 pages × 4,096 B, freelist 0) |
| `source_state` rows | 249 |
| `unit_state` rows | 283 (283 distinct `source_id`) |
| `series_cursor` rows | **28,771,556** (260 distinct `source_id`) |
| `runs` rows | 1,257 |
| `leases` rows | 1 |
| `csv_retry_queue` rows | 231,782 |
| `runs` timestamp range | 2026-06-23T19:01:21Z … 2026-08-30T00:46:16Z |

`unit_state` status distribution: `no_change` 190, `ok` 54, `partial` 37, `transient_fail` 2.
`runs` status distribution: `no_change` 583, `partial` 384, `ok` 253, `transient_fail` 37.

Two things worth flagging to the owner:

- **The state store has grown by roughly 50×** relative to what the code's own comments assume. `push_state`'s comment says "the ~207 MB db", `CURSOR_CAP`'s docstring says "already ~306 MB", and there is a `state.db.pre_reconcile_20260711` snapshot on disk at 217,092,096 bytes. The live file is 11.3 GB, driven almost entirely by 28.77 M `series_cursor` rows. Every run pulls, decompresses, VACUUMs, zstd-compresses at level 9, and uploads this file **twice** (the object plus a dated backup).
- `runs` holds only 1,257 rows over ten weeks — that is the per-unit run log, and `run_cost_estimate` samples the last 5 per source from it.

#### 8.2 The R2 round-trip and the compare-and-swap

**Keys:** `r2://econ-data/_aqueduct/state.db.zst`, backups at
`_aqueduct/backups/state-{YYYYMMDD}-{GITHUB_RUN_ID or 'local'}.db.zst`.
**Local ETag record:** `data/_aqueduct/.state_etag`.

`python -m updater.run --pull-state`:

1. `HEAD` the object, `GET` it, then **`HEAD` again**. If the ETag moved mid-download the pull fails — otherwise it would record a *newer* ETag against *older* bytes and a later push would pass CAS while silently reverting the other writer's state.
2. If a `-wal` file is present, the whole local `state.db`/`-wal`/`-shm` trio is moved to `data/_aqueduct/_superseded/<name>.<stamp>` first, loudly — that WAL belongs to the local database being replaced and may hold runs never pushed.
3. Write to a per-process temp name, then `os.replace` (atomic). On Windows a `PermissionError` here is reported with the **names of the processes holding the file open** — two orphaned manual `updater.run` processes once held it for 30 hours and every local-heavy launch aborted silently.
4. Record the ETag.

`python -m updater.run --push-state`:

1. **Compare-and-swap.** `remote_etag != stored_etag` ⇒ print both and **exit 2** without writing. Both-absent is the one allowed seed case.
2. **Shrink guard (R407).** CAS only proves nobody else moved the remote; it does not prove the *local* copy is still what was pulled. If the remote object is at least `_SUBSTANTIAL_REMOTE = 200_000` compressed bytes and `AQUEDUCT_ALLOW_SHRINK` is unset, the local file must be **≥ 1,000,000 bytes with ≥ 50 `source_state` rows**, or the push **exits 3**. An unreadable local db also exits 3. The guard asks "am I about to destroy something substantial?" — gating on the *remote* size, because the first version gated on local size and refused every legitimate first seed.
3. `VACUUM INTO` a temp copy (compact, and a consistent snapshot even if another local process holds the db open), zstd level 9, `put_atomic` to the live key, re-`HEAD`, re-record the ETag (so a second push in the same job passes its own CAS), then `put_atomic` the dated backup.
4. **Retention:** keep the newest 7 backups, prune the rest. (275 had accumulated — 55.8 GB, growing ~1.6 GB/day.) A prune failure never fails the push.

Exit codes: `0` pushed, `1` other error, `2` CAS refusal, `3` shrink-guard refusal.

**The CAS is the real cross-writer guard**, not the `leases` table — the leases live *inside*
`state.db`, so two writers each holding their own downloaded copy can never arbitrate through
them. The GitHub Actions `aqueduct-updater` concurrency group is the serializer; the CAS turns
a lost race into a loud red run instead of silent corruption.

**There are four CI state writers**, all doing `--pull-state … --push-state`:
`updater-heavy` at 03:00Z and 15:00Z, `updater-daily` at 06:00Z and 18:00Z. The workstation
runner models all four as blackout windows (§9.2).

---

### 9. THE HEALTH GATE — `updater/health.py`

**File:** `E:/research/econfindatalibrary/updater/health.py` (42,687 bytes, 716 lines).
Invocations: `python -m updater.health` (table + `data/_aqueduct/health.json`), `--json`,
`--red`, `--fail-past-2x-sla` (the CI gate). Unknown flags **exit 2** — the module used to
ignore them, which made `--fail-past-2x-sla` in the workflow a silent exit-0 no-op.

It reads **only** the state store plus the registry. It never probes upstream.

#### 9.1 Thresholds

```python
SLA_TOLERANCE = 2.0            # periods past cadence before RED
DATA_SLACK_PERIODS = 1.0       # extra slack for DATA recency
ATTENTION_STATUSES = ("partial", "definitive_fail", "transient_fail", "running")
STUCK_TRANSIENT_DAYS = 14
UPSTREAM_RECHECK_DAYS = 180.0
STALE_SERIES_DAYS = 730
ROUTE_SILENCE_DAYS = 3.0
LATENESS_PERIOD = {"irregular": 365}   # data clock only; scheduling still says 7
```

`sla_days = CADENCE_DAYS[cadence] × 2.0`.
`data_days = (LATENESS_PERIOD or CADENCE_DAYS)[data_cadence or cadence] × (2.0 + 1.0)`.

`data_cadence` is what separates *how often we poll* from *how often the publisher releases*.
`dst`'s check cadence had to go monthly → daily to converge on a publisher moving ~10 tables a
day, but its data is monthly — judged on the check clock that is a 3-day tolerance against a
62-day-old newest observation, a permanent false red. Every `data_cadence` in the registry is
measured (distinct `obs_date` over the trailing 3 years, recorded in that entry's comment).

Staleness is judged in **business days for `daily` sources** (FX/market feeds publish nothing
at the weekend; calendar age red-flagged every Monday morning) and calendar days otherwise.
Forward-dated observations are excluded from the recency signal but kept as a separate
`frontier_obs` for display — 28 of 93 units once reported a future frontier, so their
`obs_age` was negative and the staleness gate could never fire on them at all.

#### 9.2 The status classes, in evaluation order

```python
rotating = bool(attention) and _deferral_only(units)
if src is None and not units:
    health = "RED-UNRUN" if _adapter_ready(e) else "PENDING"
elif _stuck_transient(units, succ_age, sla_days, attempt_age):
    health = "RED-SLA"
elif attention and not rotating:
    health = "ATTENTION"
elif succ_age is None and not rotating:
    health = "RED-UNRUN"
elif not rotating and succ_age > sla_days:
    health = "RED-SLA"
elif eff_obs_age is not None and eff_obs_age > data_days:
    ... upstream_verified check ...  ->  "OK" | "ATTENTION" | "RED-DATA"
else:
    health = "OK"
if rotating and health == "OK":
    health = "ROTATING"
```

| Class | Meaning | Sort rank |
|---|---|---:|
| **RED-SLA** | The *job* has not succeeded within 2× its cadence — **or** `_stuck_transient` fired: a `transient_fail` that has outlived the word. | 0 |
| **RED-DATA** | The job "succeeds" but our **newest observation** is past `data_days`. Catches the quiet-`no_change`-forever hazard. | 1 |
| **RED-UNRUN** | Either no state rows at all while the adapter **is** built, or state rows exist but `last_success_utc` is null. | 2 |
| **ATTENTION** | Some unit is in `ATTENTION_STATUSES`, or an `upstream_verified` claim has expired (> 180 d) or is malformed. | 3 |
| **PENDING** | No state and the adapter is **not** built — expected during rollout, mirrors the orchestrator's `PENDING` line. | 4 |
| **ROTATING** | Every attention unit is a **pure budget deferral** (§9.3) and the data clock is satisfied. | 5 |
| **OK** | Nothing above. | 6 |

`_stuck_transient` is deliberately narrow: only `transient_fail` (never `partial`), only when a
real `last_success` exists, and it fires either when `succ_age > sla_days` or when the source
is being attempted (`attempt_age ≤ 2 d`) and has not succeeded in 14 days. `istat` sat at
ATTENTION for **40 days** — attempted nightly, `transient_fail` every time, never escalating —
because ATTENTION is evaluated before the age branches.

#### 9.3 What makes a source "partial", and `ROTATING`

A source is partial when `finalize()` in `_common.py` books it so — and it books `partial` on
three distinct things:

```python
if tally.structural:       raise DefinitiveError(...)      # 200 but 0 rows from a real body
if tally.added == 0 and tally.empty == tally.attempted and tally.attempted > 10:
                           raise DefinitiveError(...)      # all-empty over a large window
if tally.transient:        return Result(status="partial", ...)   # some sub-units failed
if tally.deferred:         return Result(status="partial", ...)   # budget stopped the sweep
status = "ok" if tally.added > 0 else "no_change"
```

The `Tally` deliberately separates `deferred` from `transient`: deferrals do **not** increment
`attempted`, because *"'transient' says something went wrong and retrying may help; 'deferred'
says nothing went wrong and rotation takes it next tick. Both mean come back; only one means
investigate."* Before that split, `ecb` reported "252/540 sub-unit(s) transient-failed" when
nothing had failed and 252 units had never been touched.

`_deferral_only(units)` then whitelists the pure-deferral shape with an **anchored** regex
against the exact string `finalize()` emits:

```python
_DEFERRAL_BASE = _re.compile(
    r"^[1-9]\d* sub-unit\(s\) attempted, none failed; "
    r"\d+ deferred by budget and taken next tick(?: \[[^\]]*\])?$")
```

`[1-9]` because a zero-attempt deferral note is a wedged rotator wearing the deferral costume.
`csv coverage note:` tails are stripped first (they are non-failures by design); anything else
appended — a `csv_derive crashed`, a transient tail, a clipped note — fails the anchor and the
source stays ATTENTION. It is a **whitelist**, demanded by adversarial review on 2026-08-26
after a substring blacklist accepted `unsdg`'s live note with a `csv_derive crashed` tail.

Why the class exists: a budget-bounded source (`ecb` 540 files / 35 min, `abs` 1,222 units /
45 min) can **never** run deferral-free, so it is ATTENTION by construction — on 2026-08-26 the
CI gate carried **22 such sources red on every run**, 40+ consecutive failures nobody could act
on. The docstring is careful not to oversell: ROTATING certifies that nothing *failed*, not
that the rotation is advancing or that the un-reached tail is fresh. A rotator still **falls
through to the data-recency branch**, so a rotator whose newest observation ages past its data
clock goes RED-DATA, not ROTATING-green (`abs` at 238 d and `ilostat` at 147 d against 90-day
clocks re-redden under exactly that rule).

#### 9.4 `upstream_verified` — an expiring completeness claim

39 registry entries carry it. When our newest observation is old but **matches the publisher's
own latest** (IMF Historical Public Debt ends at 2015), the source may declare
`{latest_obs: …, checked: …}` and read OK instead of RED-DATA. It is an assertion with an
expiry: past `UPSTREAM_RECHECK_DAYS = 180` it lapses to ATTENTION so somebody re-probes. If our
data ever falls *behind* the declared upstream end, the declaration stops applying and RED-DATA
stands.

Ten entries carry `upstream_verified` as free-text prose rather than the structured mapping. A
non-dict is now **surfaced as an attention note and ignored** rather than granting suppression —
previously it crashed the gate with `AttributeError: 'str' object has no attribute 'get'`,
reddening the 06:00 run daily **without assessing anything**.

#### 9.5 What the CI gate judges, and what it refuses to judge

```python
bad = ("RED-SLA", "RED-DATA", "RED-UNRUN", "ATTENTION", "PENDING")
return [... for r in report["sources"] if r.get("live") and r["health"] in bad and _judged_here(r)]
```

- Only `live: true` sources can fail the gate.
- `_judged_here()`: **the cloud gate declines to judge sources whose `run_location` is not `cloud`.** There is one state store per location, so their rows are frozen or absent in the cloud state by construction. A **local** invocation judges everything.
- The declined sources are printed under an explicit `NOT JUDGED HERE` heading — *"'we cannot see this from here' is a different statement from 'this is fine', and only one of them is true."*
- `route_silence()` then makes the one claim the cloud *can* make about the other route: if **every** live source on a foreign route has gone `ROUTE_SILENCE_DAYS = 3` without a success (and the route has succeeded at some point, so an unconfigured route is not permanently red), the gate fails with `ROUTE 'local' SILENT — N live source(s) run there and NOT ONE has succeeded within 3d`. Its own docstring is explicit that this **cannot** catch a short outage: on 2026-08-02 the guard loop died at 15:16 and the local pass went ~7 h past due, but the local sources still carried yesterday's successes. The instrument for that case is the workstation heartbeat (§10.3).

Exit 1 on either failure ⇒ red run ⇒ GitHub notification. D1 keeps serving the true stale date;
the public endpoint never lies to hide a failure.

---

### 10. THE LOCAL WORKSTATION RUNNER

Scheduled Tasks are blocked by policy on this machine, so the only durable, reboot-surviving
mechanism is a Startup-folder launcher plus a polling loop.

#### 10.1 The chain

```
C:\Users\aelkassabgi\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\EconGuard.cmd
  -> powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden
       -File E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP.ps1
         -> every 300 s:
              (a) RELAUNCH_GUARD.ps1                      (as a job, 120 s ceiling)
              (b) logs\guard_loop.heartbeat  <- local stamp, written AFTER the tick
              (c) python tools/guard_heartbeat.py --publish   (as a job, 90 s ceiling)
```

`RELAUNCH_GUARD.ps1` in turn:

- relaunches the three tracked crawlers if dead (§2.6a);
- relaunches the three long one-off jobs via `run_guarded_job.ps1` unless `logs/<name>.DONE` exists (§2.6b);
- launches `tools\run_local_heavy.ps1 -IfDue` unless `logs/local_heavy.DONE` exists.

Both the guard call and the heartbeat publish run as **bounded PowerShell jobs**, because
`& guard` inline and unbounded meant one hung invocation froze the watchdog while its process
stayed alive — *"the worst shape, because 'the process is running' then reads as healthy."*

The interpreter is **pinned** everywhere:
`C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe` — a bare `python`
resolves through PATH and can hit the WindowsApps store shim or a 3.11 with no `pyyaml`.

#### 10.2 Stall detection in the guard

A name match is not liveness. Before killing anything the guard requires **two independent
signals over two consecutive ticks**:

- `STALL_HOURS = 3` with no write to the job's newest `guard_<name>_*.log`;
- **and** no CPU (`Δcpu < 0.05`) **and** no I/O (`Δbytes < 5000`) over a 20-second sample;
- **and** the same verdict again on a later tick — the first quiet sample only arms a suspicion in `logs/<job>.stall_suspect`.

Both halves are scars. `cbs_nl` looked hung on CPU alone (7 s over 5 hours) while downloading
8 MB every 20 seconds; and an 8-second sample of that same crawler once read zero bytes both
ways and a perfectly healthy job was killed. `istat_sliced` meanwhile sat for 15.5 hours as an
orphan — parent gone, blocked writing into a pipe with no reader, 15 threads, zero CPU, zero
I/O — while the loop saw the right command line and left it alone.

#### 10.3 The heartbeat

`E:/research/econfindatalibrary/tools/guard_heartbeat.py`

- **`--publish`** (workstation, every tick): PUTs `r2://econ-data/_aqueduct/guard_heartbeat.json` holding `{utc, host, jobs_alive, tracked, emptiness}`. Deliberately **not** written into the state store — that is single-writer via CAS, and a 5-minutely write from a second machine would contend for no reason.
- **`--check`** (CI, `DEFAULT_MAX_AGE_MIN = 45.0`): exit 1 if the beat is stale, unreadable, or **absent**. Absent is reported as *"the workstation route is UNINSTRUMENTED, not proven healthy"*, never as a pass.
- **Age comes from the content**, not from R2 `LastModified` — a re-upload of a stale body would look perfectly fresh.
- **The observer cannot fake it**: the writer is the loop on the workstation, the reader is a gate in CI. (A prior liveness probe once matched its own command line and reported a dead loop alive.)
- The beat also carries the **crawl-emptiness verdict** (`tools/audit_crawl_emptiness.py --json`), so `fetch_without_write` units red the CI run. `cbs_nl` once fetched 144,000,000 rows and wrote **zero** for weeks; the signal existed on the workstation the whole time with nobody reading it. A failure to *run* the audit is reported as unknown, never as clean.
- `TRACKED = ("ingest_cbs_nl.py", "ingest_gus_dbw.py", "ingest_istat_sliced.py")`. A tracked job that is alive-but-absent is **noted, not failed**, because a finished crawler is legitimately absent and this tool cannot tell finished from dead.

`updater-daily.yml` runs `python tools/guard_heartbeat.py --check` as its own step, under
`always()`, deliberately separate from the health gate so the log says *which* claim failed.

#### 10.4 `tools/run_local_heavy.ps1` in detail

**File:** `E:/research/econfindatalibrary/tools/run_local_heavy.ps1`. ASCII-only by design —
Windows PowerShell 5.1 reads a BOM-less `.ps1` as system ANSI, and a single em-dash once
corrupted the token stream mid-file so the script silently skipped lines 67–120 and exited 0.

Parameters: `-Only <ids>`, `-WhatIf`, `-SkipCiCheck`, `-IfDue`, `-Force`, `-MinHours 20`.

The gate sequence under `-IfDue`:

1. **PID lockfile** `logs/local_heavy.lock`, holding `<pid>,<process start ticks>`. PID alone is not an identity (Windows reuses them), so the recorded start time must match too. A **live lock older than 8 hours** is announced as `WEDGED:` — no legitimate pass can hold it that long, and one manual run once held it for **19 hours** while the guard stood down silently every 5 minutes and the whole local route (`statcan`, `census`, `oecd`, `bea`, `eia`) did not run at all.
2. **Cadence stamp** `logs/local_heavy.last_success` — exit 0 quietly if less than `-MinHours` (default **20**) old.
3. **Source list** from `tools/_list_local_sources.py`. A non-zero exit is fatal: *"a crashing lister must not look like an empty registry."*
4. **CI in-flight check** — `gh run list --workflow=updater-daily.yml --limit 5 --json status`; any non-completed run ⇒ exit 2. A failure to query CI also exits 2.
5. **Blackout windows.** The instantaneous CI check says nothing about 06:00Z. The script models all four CI writers as intervals, expressed as minutes **after the cron** so the model absorbs GitHub's start lag (measured 13–55 min):

| Window | Start | End |
|---|---|---|
| 03:00Z heavy | cron − 20 min | cron + 150 min |
| 06:00Z daily | cron − 20 min | cron + 270 min |
| 15:00Z heavy | cron − 20 min | cron + 150 min |
| 18:00Z daily | cron − 20 min | cron + 270 min |

   Inside a window ⇒ abort. Otherwise the run budget is clamped to `(next window start − now) − 25 min`, and if that is under 20 minutes it aborts as not worth a state pull/push cycle. This leaves **two usable slots a day, ~10:30–14:40Z and ~22:30–02:40Z, each ~250 min**. The ends were measured from the last six runs of each workflow on 2026-08-23; a prior model that stored guard-adjusted times instead of cron times concluded at 17:41 that the 18:00Z window had *passed* and launched an 11.5 h pass against a run that fired at 18:17.
6. **Take the lock**, then `--pull-state`. A failed pull aborts before any write and removes the lock.
7. **Env:** `AQUEDUCT_BACKEND=r2`, `PYTHONUNBUFFERED=1`, `AQUEDUCT_BUDGET_MIN_OVERRIDE=360` (per-source), `AQUEDUCT_RUN_BUDGET_MIN=2880` then clamped as above.
8. **Run** `python -u -m updater.run --source <id> …` as a child process with **redirected stdout/stderr** to `logs/local_heavy_updater_<stamp>.log` and `.err.log`. This redirection was added on 2026-08-30 for a precise reason: the 06:00Z gate reported `ROUTE 'local' SILENT` and the corresponding local log was 1,776 bytes holding **nothing** between "running updater for 29 source(s)" and "HARD STOP" 105 minutes later — the one question the gate asked was unanswerable from our own logs.
9. **Hard wall clock.** The orchestrator's budget is checked *between* units and its per-unit SIGALRM is a documented no-op on Windows, so one long unit can sail past the window. The script polls every 15 s and `taskkill /T /F`s the child at `budget + 10 min`, sets `rc = 124` explicitly (a killed process leaves `ExitCode` unpopulated), and proceeds to push state — *"stopping here is strictly better than running on, because push-state still executes afterwards."*
10. **`--push-state`**, then **stamp the cadence clock only if the push succeeded and the updater did not crash** (`rc` < 0, 134, 137, 139). A non-zero updater rc alone is not disqualifying — the design is to fail one source while honestly refreshing the others — but a failed push is, because nothing durable came of the pass. A 2026-08-01 pass crashed inside `ons_uk` after 8 h 56 m, lost its CAS, and still stamped success; the guard then stood down for 20 hours over a run whose entire record had been lost.
11. Remove the lock, exit with the updater's rc.

---

### 11. ALL GITHUB ACTIONS WORKFLOWS

Directory: `E:/research/econfindatalibrary/.github/workflows/` — **8 files**, all read.

| File | Schedule (UTC) | Other triggers | Job timeout | Concurrency | What it does |
|---|---|---|---|---|---|
| `updater-daily.yml` | `0 6 * * *`, `0 18 * * *` | `workflow_dispatch` (`source`, `dry_run`, `force`) | **355 min**; the `Run updater` step **305 min** | `aqueduct-updater`, `cancel-in-progress: false` | The main pass. Pull state + catalog from R2 → run the orchestrator over live sources → push state (CAS) → sync freshness + new catalog rows to D1 → health gate → workstation heartbeat check → served-bytes freshness probe → digest email → heartbeat commit. |
| `updater-heavy.yml` | `0 3 * * *`, `0 15 * * *` | `workflow_dispatch` (`source`, `force`) | **300 min** per matrix job | same `aqueduct-updater` group | One dedicated runner per heavy source. `setup` emits the 34-id matrix; `heavy` runs `max-parallel: 1`, `fail-fast: false`. |
| `sec-edgar-daily.yml` | `0 8 * * *` | `workflow_dispatch` (`days`, `dry_run`) | **60 min** | `sec-edgar-daily` | `tools/refresh_sec_edgar.py --days 4 --apply --d1`, then reads `AAPL` back out of R2 and asserts the grouped parquet and served CSV agree row-for-row. |
| `billing-guard.yml` | `23 13 * * *` | `workflow_dispatch` | **20 min** | — | `tools/billing_guard.py`: `wrangler d1 insights --timePeriod 1d` across `econ-catalog`, `econ-catalog-climate`, `hfdatalibrary-db`. Over 5B rows/day ⇒ exit 1 (red run + Resend alert); over 2B ⇒ green run + Resend warning. |
| `preflight.yml` | — | `push` (paths: `updater/registry.yaml`, `updater/config.py`, `updater/strategies/fetchers/**`, `tools/preflight_registry.py`, itself), `pull_request`, `workflow_dispatch` | **5 min** | `preflight-${{ github.ref }}`, cancel-in-progress | `tools/preflight_registry.py` (the `EXPECTED_SOURCE_COUNT` tripwire at push time) + `tools/audit_updater_deps.py` (static scan: a fetcher whose import fails is filed "no adapter built" and skipped forever — this has already cost `edgar_jrc`/openpyxl, `damodaran`+`sipri_polity`/xlrd, `fed_board`/lxml). |
| `tests.yml` | — | `push` to `main`, `pull_request` | none set | `tests-${{ github.ref }}`, cancel-in-progress | `python -m pytest tests/ -q`, with Node 22 installed because one test runs the Worker's TypeScript under `node --experimental-strip-types` and would otherwise **skip itself into non-existence while the job stayed green**. Deliberately **not** in the `aqueduct-updater` group: that group holds at most one pending run and a newer arrival evicts it, so a push-triggered workflow there would let every push evict a scheduled updater run. |
| `deploy-site.yml` | — | `workflow_dispatch` **only** (the `push` block is present but commented out) | none set | `deploy-econ-site`, `cancel-in-progress: false` | `wrangler pages deploy catalog/site --project-name=econdatalibrary --branch=main`, then curls `https://econdatalibrary.com/` and `/account` expecting 200 with 6 retries — because a green wrangler step means "Cloudflare accepted the upload", not "the site serves". |
| `hello.yml` | — | `workflow_dispatch` | **5 min** | — | Trivial smoke workflow required to go green before any real one. |

#### 11.1 The daily run's step order and why each guard exists

1. **Checkout** with `persist-credentials: false` — so the long `pip install` and third-party fetch steps do not run beside a usable repo write token while the R2/Cloudflare secrets are in the environment.
2. **Python 3.11**, `pip install -r requirements-updater.txt`.
3. **Pull state from R2** (`id: pull_state`).
4. **Pull `catalog.db`** — streams `_aqueduct/catalog.db.zst` to `data/catalog.db`. Skipped on dry runs.
5. **Run updater** — `timeout-minutes: 305`, with a background `free -m` sampler every 15 s so an OOM is *proven* (exit 137/143 is annotated `::error::updater was KILLED`). The `source` input is passed via env and each token validated against `[a-z0-9_]` so a crafted value cannot become a flag or path.
6. **Push state** — `if: always() && steps.pull_state.outcome == 'success' && inputs.dry_run != true`. Gated on the **pull**, not the updater: the updater is *designed* to exit non-zero on a live-tier failure while having honestly refreshed ~130 other sources, and discarding that state is the opposite of the honest-status contract. Gating on the pull is safe because the CAS is structurally incapable of overwriting a newer remote.
7. **Sync freshness to D1** — `npm ci --prefix api/worker` first so `npx wrangler` resolves the pinned version rather than floating to latest.
8. **Sync new catalog series to D1** — `continue-on-error: true`. Without it, newly derived series reached R2 (hosted, downloadable by id) but never appeared in `/v1/catalog`; that gap stranded **31,259 series**, with `boe` serving 21 of its 30,674.
9. **Health gate** — `python -m updater.health --fail-past-2x-sla`.
10. **Workstation watchdog heartbeat** — `always()`, so it still runs when the health gate exited 1; the two failures are independent.
11. **Served-bytes freshness probe** — `tools/probe_csv_freshness.py --sources 6 --sample 8`, rotating and bookmarked in R2, `continue-on-error: true` *for now* with an explicit instruction to drop the flag once a week of runs shows it quiet.
12. **Daily digest email** — `updater/send_digest.py`, `always()`, Resend; without `RESEND_API_KEY` it prints the digest and skips sending loudly.
13. **Heartbeat commit** — writes `ops/last_run.txt`, `always()`, 3 attempts, each rebuilding on a fresh `origin` tip. This is the **cron-death guard**: GitHub disables schedules after 60 days without repo activity, and nothing else commits at steady state. A bare `if:` ANDs the implicit `success()`, so a run of failures — exactly the state that most needs the schedule to keep firing — silenced the guard protecting it.

A step removed on 2026-08-03 is worth knowing about: a relay-staleness audit that live-probed
`api.db.nomics.world` daily. That domain is **banned**; the comment records that the *question*
it answered survives, answered instead by the health gate's RED-DATA branch plus
`upstream_verified` declarations measured at the real publisher.

#### 11.2 Comment-versus-code drift in `updater-daily.yml`

Three numbers in the prose do not match the YAML they describe. Reported as a finding, not
corrected:

| Comment says | Code says | Line |
|---|---|---|
| "RAISED 250 -> **285**" for the updater step | `timeout-minutes: 305` | 220–229 |
| "The job cap moves 300 -> **335**" | `timeout-minutes: 355` | 224, 68 |
| "refuses to start a unit whose worst case … would cross minute **270**" | `AQUEDUCT_RUN_BUDGET_MIN: "290"` | 137, 139 |

The *relationships* the comments reason about still hold (step < job, budget < step, ~50-minute
teardown tail), so the configuration is coherent; only the narration is stale.

---

### 12. QUICK REFERENCE — every tunable, with its default and its real value

| Name | Default | Actually set to | Where |
|---|---:|---|---|
| `AQUEDUCT_BACKEND` | `local` | `r2` in both updater workflows and in `run_local_heavy.ps1` | `config.py`, workflows |
| `AQUEDUCT_LIVE_ONLY` | unset (off) | `'1'` in `updater-daily.yml` | `orchestrate.run_once` |
| `AQUEDUCT_RUN_BUDGET_MIN` | 240 | daily **290**, heavy **240**, local **2880 → clamped** | `orchestrate.py:1336` |
| `AQUEDUCT_UNIT_TIMEOUT_MIN` | 45 | heavy **180**; daily uses the default | `orchestrate._unit_timeout_min` |
| `AQUEDUCT_DERIVE_BUDGET_MIN` | 45 | not overridden; further capped by the run remainder | `orchestrate._capped_derive_budget`, `derive.py` |
| `AQUEDUCT_DERIVE_WORKERS` | 8 | not overridden | `derive.derive_and_put` |
| `AQUEDUCT_BUDGET_MIN_OVERRIDE` | — | **360** on the workstation (per-source fetcher budgets) | `run_local_heavy.ps1:212` |
| `AQUEDUCT_RUN_LOCATION` | derived from `GITHUB_ACTIONS` | unset | `orchestrate._here` |
| `AQUEDUCT_ALLOW_SHRINK` | unset | unset | `run.push_state` |
| `AQUEDUCT_WID_BUDGET_MIN` | 180 *(per the workflow comment; not verified in `fetchers/wid.py`)* | **60** in `updater-daily.yml` | workflow env |
| `DST_BUDGET_MIN` | 20 *(per the workflow comment; not verified in `fetchers/dst.py`)* | **12** in `updater-daily.yml` | workflow env |
| `AQUEDUCT_DATA_ROOT` / `AQUEDUCT_STATE_DIR` / `AQUEDUCT_REGISTRY` / `ECONDL_ROOT` | repo-relative | unset (workflows set `ECONDL_CATALOG` only) | `config.py` |
| `FAST_LANE_SECONDS` | 120.0 | — | `orchestrate.py:53` |
| `BAND_LADDER_SECONDS` | (600.0, 3600.0) | — | `orchestrate.py:64` |
| `PARTIAL_RETRY_DAYS` | 7 | — | `strategies/base.py` |
| `_TTL_BY_COST` | fast/medium 7200 s, large 43200 s, giant 172800 s | — | `orchestrate.py:128` |
| `CURSOR_CAP` | 50,000 | — | `fetchers/_common.py:308` |
| `_DERIVE_ALL_CAP` | 5,000 | — | `orchestrate.py:708` |
| `_CSV_RETRY_CAP` | 20,000 | — | `orchestrate.py:713` |
| `PUT_TRIES` | 7 (backoff 1…64 s) | — | `derive.py:41` |
| `merge` `min_ratio` | 0.97 | per-source overrides possible | `merge.merge_and_write` |
| `_SUBSTANTIAL_REMOTE` | 200,000 B | — | `run.py:199` |
| shrink-guard floors | 1,000,000 B and 50 `source_state` rows | — | `run.push_state` |
| `SLA_TOLERANCE` / `DATA_SLACK_PERIODS` | 2.0 / 1.0 | — | `health.py:26-28` |
| `STUCK_TRANSIENT_DAYS` | 14 | — | `health.py:30` |
| `UPSTREAM_RECHECK_DAYS` | 180.0 | — | `health.py:34` |
| `STALE_SERIES_DAYS` | 730 | — | `health.py:39` |
| `ROUTE_SILENCE_DAYS` | 3.0 | — | `health.py:570` |
| guard loop period | 300 s | — | `RELAUNCH_GUARD_LOOP.ps1:77` |
| guard tick ceiling / heartbeat ceiling | 120 s / 90 s | — | `RELAUNCH_GUARD_LOOP.ps1:43,66` |
| `DEFAULT_MAX_AGE_MIN` (heartbeat) | 45.0 | — | `tools/guard_heartbeat.py:55` |
| `STALL_HOURS` | 3 | — | `RELAUNCH_GUARD.ps1:78` |
| local `-MinHours` | 20 | — | `run_local_heavy.ps1:48` |
| local `$LEAD_MIN` / `$marginMin` / `$graceMin` | 20 / 25 / 10 min | — | `run_local_heavy.ps1:252,272,322` |

---

### 13. WHAT IS NOT ESTABLISHED

1. **The exact provenance of the `abs` = 100,000 and `ilostat` = 50,000 csv-retry rows.** Both are suspiciously round. Establishing it: `SELECT enqueued_utc, COUNT(*) FROM csv_retry_queue WHERE source_id IN ('abs','ilostat') GROUP BY 1 ORDER BY 1` against `data/_aqueduct/state.db`.
2. **Whether the R2 copy of `state.db` matches the 11.3 GB local copy.** I measured only the local file. The local `.state_etag` (written 2026-08-29 21:34) suggests it does, but confirming it means a `HEAD` on `r2://econ-data/_aqueduct/state.db.zst` and comparing ETags — an R2 call I did not make under the read-only rule for this task.
3. **Whether `updater/gen_registry.py` is still run.** It exists (5,291 bytes, dated 2026-06-23) and `registry.yaml` records `generated_from: "matrix + classifications"`, but no workflow or script I read invokes it; the registry's 246-line changelog in `config.py` describes hand-pinned entries. Establishing it: `git log --follow updater/registry.yaml` versus `git log updater/gen_registry.py`.
4. **The real-world cadence of the workstation route.** The design allows one pass per ≥ 20 h inside a ~250-minute window; whether it achieves that is a question for `logs/local_heavy.last_success` and the `runs` table over time, not for the code.
5. **Whether any of the 10 unscheduled registry entries (§2.5) is still served.** Being registered means they are counted and shown in the health table; whether users can currently download them is a catalogue/serving question outside this section.

---

# PART II — THE RECORD

## 5. What I have done, and how

This section is written first-hand. Where it states a number, the instrument that produced it is
named. Where something is uncertain, it says so.

---

### 1. The working method, and why it looks the way it does

The way I work on this project is not the way I would work by default. It is the accumulated
residue of things going wrong, and almost every element of it exists because a specific failure
made it necessary. Four mechanisms carry the weight.

#### 1.1 The mistake ledger (`.claude/MISTAKES.md`)

An append-only file, currently **14,010 lines, 124 full entries and 337 digest lines**
(`grep -c "^## R"` and `grep -c "^- R"`). Every mistake gets an entry the moment it is discovered,
not at the end of a session. Each entry states what I claimed, what was true, the mechanism that
produced the gap, and a rule.

The top of the file is a **Rules Digest** — the distilled one-line form. This exists because on
2026-08-04 I wrote sixteen entries and added zero digest lines, so the lessons were invisible by
that same evening. Ahmed caught it. The rule now is that **every new entry must add a digest line
in the same commit**, and that rule is enforced mechanically by
`.claude/skills/adversarial-review/tools/ledger_check.py --digest`, which fails if any entry from
R475 onward lacks one.

The ledger is not a diary. It is loaded at the start of every session and after every context
compaction, which is the only reason any of it survives.

#### 1.2 The numbers ledger (`.claude/NUMBERS.md`)

Every figure I report to Ahmed goes in a table row with four columns: the claim, the number, **the
instrument that produced it**, and the date, plus a note. It currently holds around 95 rows.

The reason is simple and was learned expensively: a number without its instrument cannot be
audited, and I have repeatedly produced confident numbers from broken instruments. When a figure is
later found wrong, the correction is appended as a new row that names the retraction rather than
editing the old one, so the record of having been wrong survives.

#### 1.3 Adversarial review

A standing order from Ahmed, tightened twice:

> *"i feel like you are repeating mistakes from the past ... the adversarial will be your check and
> balance ... create this as a skill to be permanent for this database as you are going in
> circles."* (2026-08-24)

> *"You need to run a parallel adversarial for everything you do."* (2026-08-29)

The protocol is **brief → challenge → do → verify → record**. A separate agent is given the plan,
the evidence, and an explicit instruction to *find the flaw, not to approve*. A verdict of REDIRECT
or FAIL is a success for the reviewer. On a FAIL the reviewer writes the ledger entry itself.

The second order — run it *in parallel*, for *everything* — came because three consecutive serial
reviews caught real defects only after the work was built. Running the reviewer alongside costs no
wall-clock and finds problems while the design is still cheap to change.

**This is the single highest-value mechanism in the project.** In this session alone, three reviews
returned against me and all three were right; one of them stopped an operation that would have
destroyed 603,467 rows of live data. Details in §3.

#### 1.4 Mechanical gates and hooks

Prose rules did not hold. The rules that hold are the ones a program enforces:

| Mechanism | What it does |
|---|---|
| `.claude/hooks/d1_cost_guard.py` | PreToolUse. Counts full-table scans against Cloudflare D1 and **refuses** past 15/hour or 40/day. Fails open. |
| `.claude/hooks/cost_banner.py` | SessionStart. Prints the running D1 scan budget. |
| `.claude/hooks/read_receipts.py` + `_receipt_rules.py` | The **reading gate**. A "I read the ledger" receipt only qualifies if the reads contiguously covered required line regions — because v1 accepted `Read(MISTAKES.md, limit=1)` as a full read, which an adversarial review proved against my own session state. |
| `.claude/hooks/heartbeat.py` | Injects the standing rules into every prompt. |
| `.claude/hooks/consequential_gate.py` | Refuses deploys and D1 writes when required reading is unread. |
| `ledger_check.py --digest / --counts / --titles` | Mechanical ledger and catalogue integrity checks. |
| `keep_working.sh` (Stop hook) | Refuses to let me end a turn except for an empty queue, a hard blocker, or a reserved decision. |
| CI guards in the econ repo | `test_registry_count_guard.py`, `test_dbnomics_ban.py`, `test_licence_gate_matches_docs.py`, and others. |

#### 1.5 Reserved decisions

A short list of things that are **Ahmed's call and not mine**, regardless of how confident I am:

* deleting data that is **not re-crawlable**
* un-gating a **DISPUTED** licence
* **auth and billing**
* **sending email as Ahmed**
* by precedent (ledger R275/R276), **any change that alters PUBLIC series ids**

#### 1.6 Desktop-first

*"i can host the data on cloudflare but do the updating and functions on the desktop."* (Ahmed,
2026-08-29)

Anything whose answer only informs a decision — counting, sizing, auditing, exploring — runs
against the **local** `catalog.db` (11.91 GB, 13,486,342 series). Cloudflare D1 is touched only to
serve users, to apply a write, and to verify user-facing state afterwards.

The reason is money: August billed roughly $200 in D1 reads, 87% of it on two days, and those two
days were our own catalogue maintenance rather than user traffic. The identical query is free on
this machine. The verification half is not optional — local and D1 can disagree, so a claim about
what users see still has to come from D1 or the served file. **Decide locally, verify remotely.**

---

### 2. What the work has consisted of, historically

The econ repository has **1,480 commits** (`git rev-list --count HEAD`). The work falls into a few
recognisable kinds.

#### 2.1 Building the update system

The original problem, in Ahmed's words, was five weeks of circular regressions: sources would be
fixed and then quietly stop updating, and nobody could tell which of them were current. The
response was the `econ-updater` skill (a mandatory, versioned procedure living in the econ repo
itself) plus the machinery it describes — a registry, an orchestrator, a health gate, a state
store, and per-source runbooks.

The standing goal is stated as: **every served source auto-updates, with no whack-a-mole.**

#### 2.2 Bringing sources online, one at a time

The rule is one source end-to-end before starting another: read its runbook, grep the ledger for
its id, read its fetcher header, its registry entry, and its licence verdict — then work. This
sounds slow and is the only thing that has worked.

#### 2.3 Licensing and redistribution

A single canonical file, `DATABASE_LICENSES_VERBATIM.md`, holds every database's terms **quoted
verbatim** with a URL that was actually retrieved. This exists because I once told Ahmed a named
exchange prohibited redistribution with no source at all, inferring it from how exchanges generally
behave — and the terms in fact *permitted* it (R416). The rule now is that a licence claim needs a
verbatim quote or an explicit "unassessed"; a cautious guess is not free, because a false negative
silently shrinks the library and nobody ever complains about it.

#### 2.4 Cost control

Two incidents drove this. In one, a catalogue endpoint ran a `COUNT(*)` and an unindexable
`ORDER BY` per page view; a crawler paging one source produced roughly **130 billion D1 rows read
in a single day**, about **$82**, and the detection mechanism was Ahmed reading his bill (R430). In
another, I planned 164,705 D1 statements against an unindexed column — **3.93 trillion row reads,
about 24 days and ~$2,500** — and estimated it at 90 minutes from the file count (R492). An
adversarial review caught it before a single statement ran.

Ahmed's words: *"This project has already cost me a month over $100 and a second month over $200
because of your mistakes. I need a safetyguard to stop these mistakes from happening. I cant afford
this I will go bankrupt."*

#### 2.5 Repair work

Long stretches of this project have been repairing damage that earlier work caused: fabricated
dates from mis-selected time axes, duplicated full-text index rows, series titled with their own
opaque keys, stores frozen behind guards that were right for the wrong reason.

---

### 3. What I did in the session that produced this document

This is a complete and honest account, including the parts where I was wrong.

#### 3.1 Billing instrument

Hardened `tools/billing_guard.py` against four proven defects, then wrote
`tools/billing_reconcile.py`, which reconstructs a **real Cloudflare invoice (IN-74622130) to
−0.11%** ($154.79 reconstructed against $154.96 billed) and exits non-zero past 2% drift. It
imports the shipped `units()` function rather than re-implementing it, so it cannot prove that a
copy agrees with itself.

The defects it fixed, all of which had been live: R2 operations printed four lines above a
projected total that omitted them; D1 reads projected from a top-100-truncated source while the
true figure was already in hand; a cumulative 25-billion-row monthly allowance compared against a
daily rate; a GB **mean** used where a GB-**month** (730 hours) was required; Workers requests
measured and never summed; and every figure printed pre-tax when Texas adds 6.6%.

Ledgered as **R505** and **R507**.

#### 3.2 Performance repairs in the updater

* `tools/audit_schedule_coverage.py` used `GROUP BY source_id` on an 11.9 GB catalogue and **never
  finished** (388.7 s and still running). Replaced with 349 primary-key range counts: **1.3 seconds**.
* `updater/orchestrate.py` full-scanned the catalogue **three times per run** and, on the split-part
  lookup, **once per unmapped key** — 6,872 times for one source. Replaced with PK ranges. The
  measured effect on one source was 87 hours of scanning reduced to 18 seconds.

#### 3.3 Source fixes

* **worldbank** — eight income-group aggregates were reported missing. Added a legacy-code alias
  (`HIC→XD`, `LIC→XM`, `LMC→XN`, `UMC→XT`), tried only after a direct miss. Verified 8 → 0.
* **ecb** — the retry loop caught only `OSError`, so `http.client.HTTPException` and `zlib.error`
  went straight through. Fixed. Separately expanded dataflow mapping so a changed bulk file
  re-derives the series inside it — **this took three attempts and is described honestly in §3.6.**
* **istat** — implemented the publisher's stated 5 requests/minute limit with cool-off backoff.
* **cso** — reserved 25% of each batch for previously-unlisted matrices, because making 468 frozen
  matrices *visible* had put them all at queue position 12,318 of 12,378, where a 60-table batch
  never reached them.
* **eurostat** — a guard had frozen the source for **45 days**. A full verification pass over all
  7,214 files found the store was **already clean**; the guard was blocking on a stale file *count*
  alone. Released, verified by calling the shipped guard itself, which printed "GUARD PASSES".

#### 3.4 The duplicate-key investigation — the main work

This began with two UNCTAD stores holding ~44% duplicate rows and became the largest finding of the
session, because of a standing rule of Ahmed's: **a reported example is one instance of a class.**

Instead of fixing the two stores I was handed, I swept the whole fleet. That sweep went through
**four versions**, each because the previous one was wrong:

| Version | Defect | Consequence |
|---|---|---|
| v1 | Only opened `<dir>/<dir>.parquet` | Silently skipped the **77 largest** multi-file stores; measured 299 of 430 while reporting "308 of 308" |
| v2 | `COUNT(DISTINCT value)` per group | 46 GB resident and 76 GB of spill on the first store without finishing |
| v3 | Pooled all files in a store into one namespace | Invented **11.2 million** false conflicts for `bea` |
| v4 (current) | Per-file grouping, summed in Python | Measures `bea` in **7.1 seconds**; validated against the hand-computed answers before use |

The final instrument measured **1.16 billion rows across 394 of 430 stores** and found the class has
few members. What survives as genuinely user-facing:

| Source | Served ids | The dimension the key drops |
|---|---|---|
| `eia` | 268,502 | frequency (`.A`/`.M`/`.Q`/`.D`) — 142,073 ids (52.9%) gather more than one publisher series |
| `idb` | 18,838 | every column except dataset and country — one id gathers 490 rows for one date |
| `unctad` ×2 | 36,704 | `Flow` — Imports vs Exports |
| `damodaran` | 24,687 (721 collided) | which worksheet a column came from |

Two are confirmed against the publisher's own data. UNCTAD's OData `$metadata` declares `Flow` in
its Fact key and Algeria/1995 returns Imports 0.1124 and Exports 0.0157 — the exact two values our
store stacks under one id, with **286,038 publisher two-flow cells against 286,038 duplicated store
pairs, zero residue.** For Damodaran, the publisher's own workbook gives India `Adj. Default Spread
= 0.02091491502586354` and `Corporate Tax Rate = 0.3`; **we serve 0.3 as the default spread.** Its
default-spread-by-rating ladder breaks required monotonicity at **9 of 19** adjacent steps.

The **systemic cause**: **146 of 212 ingest jobs write parquet directly** (`pq.write_table`), and
**zero** route through `merge_and_write` — the function that dedups on `(series_key, obs_date)`,
refuses shrinks below 97%, and reports impossible dates. Every fetcher uses it; no ingester does.

#### 3.5 Adversarial reviews in this session

Three ran. All three returned against me.

**Review 1 — `who_gho`: FAIL.** My store measurements all reproduced to the digit on an independent
engine, and the publisher confirmation widened from one indicator to three. But I had called it *"a
served source"* affecting *"69,590 served ids"*. It has **zero catalogue rows** and sits on the
worker's denylist returning a live **451**. I had also written that its correct-key fetcher was
"parked behind a leading underscore, so it never runs" — it is a **shared base module** that three
live WHO sources import and that ran three days earlier. I read a Python naming convention as a
statement about behaviour.

**Review 2 — `norgesbank`: REDIRECT.** I proposed a mapping fix costed at 52 minutes. The
primary-key range construction was correct and byte-verified on both bounds with a negative control.
Everything downstream was wrong: `norgesbank.py` seeds its cursors from the on-disk frontier
*before the first HTTP call*, so its "9 changed flows" means "every flow on disk" — the same 9 on
every run while row deltas ranged from +1,848 to +3,768,215. My change would have re-derived all
35,135 series every night, **32× the work I costed**, and **eleven fetchers share the shape**. I had
also timed the wrong program (a single-threaded CLI the orchestrator never invokes; the real path
runs 8 threads), quoted a 240-minute budget that is actually 290, and cited a four-week-stale
oversubscription figure of 4.3× when the truth is 20×. **Nothing shipped.**

**Review 3 — the UNCTAD remedy: FAIL, and this is the important one.** I planned to accept a 44%
shrink on two live sources, justified on "the fresh pull's row count equals our distinct count
exactly". The reviewer pulled every year 1995–2023 from the publisher: the fresh pull is **648,241
rows**, identical multiset to our store — *equal to our row count, not our distinct count*. My
number came from the log line `refusing shrink 648241->362203`, whose second figure is
`|dedup(existing ∪ fresh)|` — a union count that **structurally cannot distinguish a complete pull
from a truncated one**.

Had I run it: **603,467 rows destroyed**, `_dedup` keeps the last row and exports sort after
imports so **every Imports value would have been deleted**, ~43% of keys would have become unmarked
import/export hybrids, **nothing would have returned 404**, and the R2 store plus all 36,704 served
CSVs would have been overwritten in the same run. The never-shrink guard I was proposing to weaken
had refused that write three times and was right all three times.

#### 3.6 Where I was wrong, in this session, in order

I record these because they are the answer to Ahmed's question about repeat mistakes.

1. **R509** — built a fix on my own earlier digest line without re-measuring. The live probe
   contradicted it.
2. **R510** — nearly filed 27 CSO matrices as "publisher discontinued" from a listing endpoint;
   the data endpoint returns rows for all 27. The bug was ours.
3. **R511** — made 468 frozen matrices visible; all landed at the end of a queue that never reaches
   them. Fixed, then the fix moved them to position 5,191 — still zero in a 60-table batch.
4. **R512** — told Ahmed "istat IP-blocked us" under a heading claiming three independent
   confirmations. All three were one machine failing to reach one host. **Retracted.**
5. **R513** — invented a "digit trap" by checking one half of a two-sided predicate. The guard I
   added would have failed CI on a legitimate source id. Removed.
6. **R514** — reported three causal stories before running the refuting test for any of them.
7. **AR-025 / AR-026 (ecb)** — the expansion mapped the wrong files; **my own probe printed
   `unparsed: 489` and I read past it.** Withdrawn, rebuilt, and the rebuild had its own errors
   (two mistakes that cancelled to a plausible number) before it was correct.
8. **R515 → retracted** — reported `who_gho` as a served source. It serves nothing.
9. **R518** — told Ahmed **in writing** that `bea` held 11.2 million conflicting values. The true
   figure is **49,856 (0.074%)** — overstated by about **440×**, entirely because my own sweep
   pooled 592 separate per-table files into one namespace. The settling test cost one command and I
   ran it only *after* reporting.
10. **R519** — the UNCTAD remedy above.

**The direction is consistent: every one of these errors made the system look worse than it is, or
made my finding look bigger than it was.** None of them ran the other way. That asymmetry is itself
the most useful thing in this list, because it says which way to lean when checking my work.

#### 3.7 What shipped, verified

* billing guard reconciled to a real invoice at −0.11%
* `noaa` relaunch loop confirmed dead: R2 LIST operations fell from 6,062–7,776/hour to
  57–368/hour, a **97.8% cut**, roughly $22.68/month
* audit tool restored from "never finishes" to 1.3 s
* resolver full scans removed
* `eurostat` released after 45 days
* `istat` rate limiter shipped
* `worldbank` 8 → 0 missing
* the duplicate sweep promoted into the skill's tools, with its validation harness

#### 3.8 What is still running

* the `statcan` derive — **8,182 of 8,207 tables**, 201 CPU-hours, 242,500 objects uploaded; the
  remaining census tables take roughly 2–3 hours each
* the corrected fleet sweep — 394 of 430 stores, the ten largest still pending

---

### 4. The honest summary of my own reliability

Ahmed's standing assessment, quoted because it is accurate and because softening it would be
another instance of the problem:

> *"you have a bad habit of telling me a lie as a fact."*

The precise failure is not usually a wrong measurement. My measurements reproduce on independent
instruments most of the time. The failure is that **I state a property no instrument in the chain
ever measured**, in the same confident register I use for things I did verify — "served",
"complete", "parked", "gated", "it resumes". A claim presented as settled cannot be audited by the
person receiving it.

The operational rule that follows, and the one thing worth carrying out of this section: **if I
have not verified it, the hedge goes inside the sentence, not in a caveat further down.** "The
registry says X" is not "X". "The log shows N" is not "N is the number of rows fetched".
## 6. The mistake ledger, part 1 (R0–R300)

### Entries R0 through R300 — a complete catalogue

---

### 1. What this document is

`D:\research\hfdatalibrary\.claude\MISTAKES.md` is an append-only engineering ledger. It records,
one entry at a time, every mistake the AI assistant working on Ahmed Elkassabgi's data platform has
made and had caught — by Ahmed, by an adversarial reviewer, by a guard, or by itself moments before
shipping. It is 14,010 lines long and holds 500+ numbered rules.

This document catalogues **every entry numbered R0 through R300**. Later entries (R301 onward) are
covered in Part 2.

Two pieces of jargon used throughout, defined once:

- **econdatalibrary / econ repo** — `E:\research\econfindatalibrary`, the economics data platform
  (website + a nightly "updater" that refreshes ~200 statistical sources). Most entries are about it.
- **hfdatalibrary / hf repo** — `D:\research\hfdatalibrary`, the high-frequency finance data
  platform. It owns the ledger file and the skills, and it is also the design template for every
  other site in the family.
- **D1** is Cloudflare's hosted SQLite (what users' requests actually read). **R2** is Cloudflare's
  object store (where the CSV and parquet files live). **`catalog.db`** is a *local* SQLite copy of
  the catalogue on Ahmed's workstation. The recurring theme of "local is not live" turns on this
  distinction.

---

### 2. How the ledger is physically laid out

The file is not uniform. Reading it requires knowing four different formats, because it was written
by many sessions over about seven weeks and the conventions changed.

```
D:\research\hfdatalibrary\.claude\MISTAKES.md
├── line 1–7      title + pointer to the global cross-project ledger
├── line 8–518    ## Rules Digest        <-- the READ PATH
│   ├── line 15   ### ⚠ R0 — the meta-rule (13 numbered sub-rules, ~155 lines)
│   ├── line 169+ "### 2026-08-04 session (R312-R327), compressed"
│   └── line 270+ one-line entries:  "- R###. RULE TEXT ... [M-YYYYMMDD-NN]"
└── line 519+     ## Entries            <-- the ARCHIVE
    ├── "### M-YYYYMMDD-NN: title"      (early entries; the rule is tagged **Rule:** [R##])
    ├── "## R### — title"               (later entries, full prose)
    └── "### R### — title"              (later entries, full prose)
```

The header of the digest states its own purpose in the file's own words:

> **HOW TO USE THIS FILE.** Read THIS DIGEST — the 8,600 lines below it are the archive, not the
> read-path. **Every new entry MUST add a digest line here in the same commit.**

That instruction exists because on 2026-08-04 sixteen entries (R312–R327) were written with **zero**
digest lines, so the lessons were invisible to the next session. Ahmed caught it. The same failure
recurred later and became R485 ("nine ledger entries and ZERO digest lines"), which is outside this
range but is the direct descendant of the header you are reading.

#### Where each block of R0–R300 physically lives

| Range | Where the rule text is | Archive entry format |
|---|---|---|
| R0 | Digest, line 15–167 | none — R0 *is* a digest section |
| R1–R24 | Digest, lines 272–295 | `### M-202607{13..21}-NN` |
| R25–R35 | **Archive only**, lines 903–979, tagged `**Rule:** [R##]` | `### M-2026072{2,3}-NN` |
| R36–R94 (+R40b) | Digest, lines 296–356 | `### M-202607{23..28}-NN` |
| R95–R160 | Digest, lines 358–392 | `### M-202607{28,29}-NN` |
| R161–R186 | **Archive only**, lines 2222–2523, tagged `**Rules:** R###` | `### M-2026073{0}-NN` |
| R187–R244 | **Archive only**, lines 2525–5460 | `## R###` and `### R###` prose |
| R245–R251 | Digest, lines 455–461 **and** archive | `### R### — title` |
| R252–R286 | R256–R286 in digest (424–454); R252–R255 archive only | `### R### — title` |
| R287–R300 | **Archive only**, lines 7097–7756 | `### R### — title` |

The `[M-YYYYMMDD-NN]` token at the end of a digest line is a **citation** to the archive entry that
supplies the evidence. R121 exists because three of those citations pointed at entries that were
never written.

---

### 3. Integrity of this range — what I verified

I enumerated the ids mechanically rather than by eye, with this script:

```python
# run against D:\research\hfdatalibrary\.claude\MISTAKES.md
import re
digest, head, ruletag = set(), {}, set()
for i, l in enumerate(open('MISTAKES.md', encoding='utf-8').read().splitlines(), 1):
    if m := re.match(r'^- (R\d+)\.', l):            digest.add(m.group(1))
    if m := re.match(r'^#{2,4} +[⚠ ]*(R\d+)\b', l): head.setdefault(m.group(1), []).append(i)
    for m in re.finditer(r'\*\*Rules?:\*\* \[?(R\d+)', l): ruletag.add(m.group(1))
```

**Results:**

- **No id in R0–R300 is missing.** Every one of the 301 ids resolves to a digest line, an archive
  heading, or a `**Rule:** [R##]` tag. 187 of the 301 have a digest line; the other 114 exist only
  in the archive.
- **23 ids carry two headings.** One (R200) is a deliberate `addendum`. The other **22 are genuine
  collisions** — two unrelated incidents given the same number by two sessions writing the ledger at
  the same time. Section 17 maps them.
- **Three rules cite evidence that was never written.** R41, R42 and R43 all carry an in-line
  annotation reading `ENTRY NEVER WRITTEN; the rule text above is the only record`, and R40b's
  citation `M-20260724-08` is annotated `ENTRY BODY MISSING`. This is not a transcription slip on my
  part — the annotations are in the file, and R121 is the ledger entry about discovering them.

Counting each of the 22 collisions as two entries, plus R40b and the R200 addendum, this range holds
**325 distinct entries**. Every one appears exactly once below.

**Notation.** Where an id collides, I write `R205a` for the entry at the *earlier* line number and
`R205b` for the later one. This notation is mine; the file itself does not disambiguate them, which
is part of the problem.

---

### 4. R0 — the meta-rule, and the only entry in its own group

**Group A. Count: 1.**

R0 is not an incident. It is a synthesis, written after a session in which six of sixteen entries
turned out to be the same error, and it is the first thing a reader is told to load. Its headline:

> **⚠ R0 — THE ONE THAT KEEPS HAPPENING: my measurement's SHAPE, not my question**

Its thirteen numbered sub-rules, each anchored to a real incident (R312–R348, outside this range but
the evidence for R0 itself):

| # | Sub-rule | The incident behind it |
|---|---|---|
| 1 | Compute what the SYSTEM computes, or read its output — do not re-implement its rule | R318: widened a health-gate tolerance measured in CALENDAR days; the gate uses BUSINESS days and had solved it three weeks earlier |
| 2 | Read a long job's ARGV, not its progress — and never pipe it to `tail` | R323: watched `rekey_eurostat.py --dry-run` for six hours and reported it as the repair. R336: `cmd \| tail -18` shows nothing until exit, so a healthy job looks stalled — killed twice in one session |
| 3 | A sweep reports TWO numbers: what it found and what it could not reach. **"0 defects in 0 files examined" is not a result** | R330: three re-pull tools pointed at a dead drive letter; `os.path.isdir` False reads as "no data", so the repair tool printed `0 corrupt` while the store held **637,178** bad rows |
| 4 | When a probe reports ABSENCE, run it against something known PRESENT — **a FAILED control VOIDS the run** | R338: keyed `/v1/sources` on `id` when the payload uses `source`, so every source read as absent, including one verified live the day before — 25,109 catalogue rows were about to be dropped |
| 5 | A one-sided test on a two-sided failure gives a number that LOOKS like a measurement | R322: "273,980 fabricated rows" was under half; the audit only tested the future. Real figure ~**637,000** across seven sources |
| 6 | Two writers, one store: state is a MERGE, not a freshness comparison | R340: CI state and workstation state are separate lineages; `--push-state` would have discarded CI's runs |
| 7 | Re-running the same query is REPRODUCTION, not verification — use a different instrument | R342: "confirmed" a doubt by re-running the audit's own DuckDB query. Parquet footer metadata matched the row count exactly (976,632,535, ratio 1.0x) |
| 8 | To measure completeness, enumerate from the side that can be OVER-complete | R341: closed a re-derive on "400/400 present" — sampled from the catalogue, which cannot see the 1,998 series the store had gained |
| 9 | A reserved task reserves what its TEXT says, not its prefix | R343: filed all 36 `imf_*` sources under a task about "the 8 served imf_* sources" — **1,093,077** series into the nothing-to-do pile on a prefix match |
| 10 | Check that your evidence POSTDATES the fix before calling the fix a failure | R339: cited three 45-minute kills, all produced by code committed BEFORE the two commits that fixed it |
| 11 | When a component has a fallback, plausible output proves NOTHING | R344: a sidecar written one directory too shallow AND read with `open()` while the store lives in R2; the reader guessed instead, and the guess was usually right. **260,931** series shipped with raw keys as titles |
| 12 | "Deployed" is a state of the RUNNING SYSTEM, not of the repository — and a new check must be shown it can FAIL | R345: reported 425,462 series "SERVED" while the worker had not been deployed since 2026-08-02. R346: the replacement probe returned True for a source id that was invented |
| 13 | Syntactic ledger rules are checked at COMMAND-COMPOSITION time, not at reading time | R348: one hour after reading R336, piped two backgrounded jobs through `\| tail` — twice |

R0 closes with four standing judgements that decide whether a number is even a defect at all:

- A **future date is usually a legitimate PROJECTION** (CSO to 2057, Estonia 2085, UN WPP 2101). A
  defect is a **SENTINEL** (9999/2999 repeated) or a **COUNTER** (contiguous from year 1).
- **A range test cannot detect code-as-year fabrication.** Swedish municipality codes run
  0114..2584; any sane-band filter keeps the ones landing in 1500..2200. Detect the *structure* that
  is definitionally impossible instead — a time value inside a series identity (`Tid=`,
  `TLIST(A1)=1991`) cannot be right, because time varies per observation.
- `obs_count` means "rows this run" on a productive run and "whole store" on a quiet one, so a
  healthy source can appear to lose 168M rows.
- A date axis that looks **mis-selected** is usually one that could not be **parsed** — a parser
  returning `None` does not abstain, it votes for every other column.

---

### 5. How the groups were derived

I read every entry and grouped by its **primary** failure — the thing that, had it been done
differently, would have prevented the incident. Several entries could sit in two groups (a wrong
measurement that led to a wrong delete, say); each appears exactly once, filed under the step where
the chain first broke.

| Group | Theme | Count |
|---|---|---|
| A | The meta-rule | 1 |
| B | Instrument and measurement errors | 73 |
| C | False-absence probes | 22 |
| D | "Green is not done" — execution, gates and tests | 48 |
| E | Repository vs running system | 29 |
| F | Instance vs class | 15 |
| G | Prose treated as implementation | 20 |
| H | Data grain, keys, parsing and identity | 39 |
| I | Destructive operations, licence and compliance | 20 |
| J | Concurrency, scheduling, budgets and resources | 23 |
| K | Toolchain corruption (shell, escapes, encodings) | 18 |
| L | Process, ledger hygiene, communication and cost | 17 |
| | **Total** | **325** |

---

### 6. Group B — Instrument and measurement errors

**Count: 73.** The largest group by a wide margin. The pattern: a number was produced, it was
arithmetically correct, and it was about something other than the question. R123 puts the cost at
"thirteen of twenty-five entries in one session were my own measurement errors, each costing 20–40
minutes."

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R2 | 2:1 split is identical to a 50% crash | A signal with two causes must never auto-apply; require human confirmation | Fixed by routing through `pipeline/manual_split.py` |
| R4 | Tableau embed hangs the screenshotter | Heavy third-party embeds break the browser-pane instrument; read the DOM instead | SPI Tableau embed |
| R18 | Blamed a WAF for a downstream invariant | Reproduce the code's EXACT request sequence before blaming upstream | BLS CPI froze on 1.6M legacy dup rows tripping `merge.py`'s never-shrink guard (`min_ratio=0.97`), not on Akamai |
| R19 | Hand-assembled a "superset" | A shared list replacing N per-source lists must be machine-verified as a strict superset | assert set-difference == empty |
| R47 | Grepped `429` and matched a timestamp | Never count a bare numeric substring in a log and call it a status code | Three runs of rate-limit theory built on evidence that never existed |
| R48 | "Fixed" an OOM by freeing the wrong allocator | Profile each allocator separately — RSS *and* `pa.total_allocated_bytes()` | "Arrow pool 0 MB" and "32 GB resident" are consistent |
| R49 | A process filter matched THIS shell and killed it | Exclude `$PID`/`os.getpid()`; LIST matches before any kill | |
| R52 | A 900-key sample scored 100% on the broken rule AND the fixed one | Sampling cannot validate a transformation rule; use known-good controls and inverse audits | Only the full audit found the defect |
| R54 | Declared a working crawler dead off an 8-second sample | Read a long job's LOG before declaring it dead; a short sample cannot separate "blocked" from "between units" | |
| R59 | Read a stale state row as a run result | A state-table dump is not a run result; `unit_state` keeps rows for de-registered sources | Reported live 46; the run processed 7 units and the true count was 45 |
| R67 | A diagnosis tool disagreed with itself | Any surprising instrument reading gets a known-good control before it becomes a finding | CSS transitions do not run in the headless browser pane; a control element also stuck at opacity 0 |
| R70 | A killed process prints nothing | Announce work BEFORE starting it (`>>> unit` / `<<< unit took Ns, peak_rss`) so the last `>>>` names the culprit | Batch 30312217406 OOM-killed at 15,654 MB of a 16 GB runner, log could not name which of eight sources did it |
| R73 | A freshness signal borrowed from an intermediary | A cache/mirror/relay hash can prove staleness, never freshness — ask the PUBLISHER | 88 of 101 relayed sources sat behind an index over a year old — 56% of relayed series, all reporting healthy daily |
| R74 | `provider_code` looked like a `PROVIDER_DATASET` pair | Print an identifier next to the real upstream value before parsing it into parts | `IMF_COMMODITY` is our own id uppercased; upstream is `IMF/PCPS`. Splitting it would have 404'd all 82 probes |
| R76 | A metric maximised by the degenerate answer | Ask what the laziest possible answer scores; if that wins, the metric is wrong | A map sending all 191 countries to the single value 'A' scored 100% "consistency" |
| R77 | Took a long run's resource curve as an assumption | Sample the curve early; bound per ENTITY, not by truncating the population | 40 GB RSS growing 1.3 GB/20s — two samples 20s apart proved it could not finish |
| R78 | "Did this file change" is not "did the publisher release something" | A vintage probe bound to a fixed path detects revisions and is blind to releases; watch the LISTING | `yale_epi` pinned `epi2024results.csv`; Yale shipped EPI 2026 at a new URL, missed for three weeks |
| R80 | Measured a quantity that cannot move | Ask whether the work would change this metric AT ALL if it were succeeding | Counted R2 objects on a derive that OVERWRITES; count is constant by construction. 5,596 writes in the previous two hours |
| R84 | "Old" is not "behind", and a clock cannot tell them apart | Let a source declare what upstream's latest actually is | imf_hpdd and imf_fiscaldecentralization went RED at 4,227 and 2,401 days stale and were exactly current |
| R87 | Counted the convenient unit | When a ratio is about coverage of X, count X | WID: 118 of 424 FILES = 28%; 362 of 424 COUNTRY CODES = 85%. A 3x error |
| R93 | A test built on an invented identifier | Draw ids from the catalogue, the store or the listing — never from memory | Almost filed a regression against a source verified complete minutes earlier |
| R98 | A per-group label summed as if it described every member | Classify at the grain the data varies at; aggregate OBSERVATIONS, never group labels | statcan (74.6% of the library) sat entirely under "daily"; 53.9 BILLION period-END observations vanished, inflating a ratio from 70.5x to 270x |
| R100 | A detector's output is a candidate list | Confirm a sample against ground truth before reporting a detector's count | 429,560 of 2,503,070 files flagged "stale"; every one of the top four checked was CURRENT |
| R102 | Grepped a short token across `data/*.json` | Constrain content searches to code globs; check file sizes first | One match was a single-line ~200 KB catalogue file printed in full |
| R105 | Recall against your own stock is not a test of your reconstruction | Test with per-group form agreement and PRECISION; quantify retirement separately | 71.71% "failure" was the PUBLISHER shrinking ~28% since our 2022 snapshot; 420 of 421 live indicators rebuilt correctly |
| R111 | A sweep total is the sum of every reason | Bucket an aggregate by why-each-item-qualifies, and build the classification INTO the tool | 7,227,669,225 unserved observations → 581,173,163 (8%) actionable; 64% of that survivor was one source mid-ingest |
| R112 | A substring test on short identifiers | Anchor identifier lookups on word boundaries; corroborate with a second key | Answers went 25 → 2 → 11 → 4, all caused by the matcher. `ppi` matched inside "shipping" |
| R115 | Told Ahmed a derive was dead; it ran at ~42 CSVs/sec | Measure liveness by WORK ADVANCING; launch background jobs with `-u` | Command line truncated in the listing + block-buffered stdout = two broken instruments agreeing |
| R123 | When a number surprises you, suspect the instrument first | Spend ONE minute on the instrument before building on a surprising result; and report COST, not just progress | 13 of 25 entries in one session were measurement errors, 20–40 minutes each |
| R127 | One ratio per source hid three different causes | Break a metric down ONE level (per file, per flow) before grouping sources by a symptom | Grouped ons_uk, insee_melodi and cso as "pathologically fragmented"; only ons_uk was broken |
| R129 | An S3/R2 prefix is not a source filter | Anchor a prefix listing on the delimiter | `Prefix="series/imf_fsi"` also matches every `imf_fsire` object → 18,620 healthy files reported as orphans; 50 id pairs have that relationship |
| R140 | Polled a saturated service to watch a job I could not speed up | If a job cannot be hurried, check it rarely, cheaply, or not at all | ~10 full R2 prefix listings while two derives saturated the same bucket → `ServiceUnavailable: Reduce your concurrent request rate` |
| R141 | Compared two sources at a grain they do not share | Check the identifier MEANS the same thing on both sides before reporting overlap | At SERIES grain 3,363 of 25,057 matched (13%); at TABLE grain 394 of 415 — 96.4% redundant, the opposite conclusion |
| R146 | Put an unverified claim inside the entry about unverified claims | A COUNT is one query; a scope figure quoted without one is a guess wearing a number | Asserted ons_uk had 3,897,884 key-titled series; it has 42 catalog rows, 0 titled by key |
| R147 | A digest line reports stored state, not the run you are reading | Grep the run for the source actually being STARTED before attributing a state to it | Wrote an incident doc about a merge that "never reached R2"; there is no `>>> stat_estonia` line in that run at all |
| R149 | A key-shape claim from `LIMIT 2` | Count the distribution over every row before characterising a key format | Claimed `ksh` keys columns by numeric index; measured, only 810 of 25,057 (3.2%) do |
| R151 | A failure count is not a defect count | Grep the repo for a failure string before theorising about it | Read `csv_derive failed 1415/3437` as stale CSVs and republished 3,447 objects; `orchestrate.py` documents the number verbatim as a non-issue |
| R157 | A cadence filter hid ten promote-ready sources | Ask only whether a fetcher resolves; let the scheduler decide when to run it | Filtering `cadence in {static, irregular, None}` hid 10 sources / 359,539 series, three of them shipped that same day |
| R163 | Printed an unmeasured baseline beside a real measurement | Never print an unmeasured quantity next to a measured one | `2nd run elapsed: 1.3s (a real re-download took ~40s+)` — the 40s was invented; the run had re-downloaded everything |
| R166 | Measured the same fraction by hand every cycle, differently each time | A number that steers the work is a deliverable; the second hand-derivation is a tool | Built `tools/audit_schedule_coverage.py`; on first run it found cepii_gravity catalogued with 1,143,250 series and absent from the resolver |
| R167 | A sample from the front of an ordered listing | Count both sets before advertising anything | 1,143,250 catalogued vs **151,543** objects in R2 — MISSING 991,707. The first five keys probed were all real |
| R171 | A provider NAME matched and I nearly built on the wrong provenance | Require a full id-set comparison before treating a name match as provenance | `bea:A191RC:Q` (BEA direct) vs `BEA/GDPbyIndustry-1:...` (the mirror). Same name, different data |
| R173 | Blamed a host for my own burst | One request after an idle gap separates "they block us" from "I burst them" | After 90s idle a single request still returned 429 from AkamaiGHost — a real edge block, not throttling |
| R179 | A 25% "reproduction rate" that was an artifact of my own query | Check both sides speak the same id space before trusting a reproduction rate | SNB `/dimensions/en` ids gave 191 of 762 (25.07%); parsing the data CSV gave 762 of 762 (100.00%) |
| R185 | An empty string is not `None` | Print the raw values once before trusting a derived verdict | `conclusion not in ('success','skipped',None)` reported eight FAILED steps on a run still in progress; GitHub uses `''` for "not yet" |
| R188 | A repro of an OOM must be the one thing that cannot OOM | Bound a resource-exhaustion reproduction before running it | The repro would have built the same ~94 GB dict that killed the 16 GB runner, on Ahmed's workstation. `abs` holds 376,332,763 distinct series over 976,632,535 rows |
| R189 | An audit judged only the 25 sources it DISPLAYED, then printed "OFFENDERS: 0" | A display limit must never be the evaluation limit; print the population size next to the verdict | |
| R192 | Nearly certified a bug with a control that failed for an unrelated reason | If both arms fail, the experiment measured nothing | `unknown series id` (404, catalog) and `data_unavailable` (502, object) are different failures |
| R197 | Diagnosed "out of memory" all day; it was a 2 GiB Arrow limit | Falsify a resource diagnosis by giving the workload 10x the resource | A CRASH means a limit VIOLATED; exhaustion raises MemoryError, overflow corrupts and aborts |
| R198 | Reported a healthy run as 403 minutes old | Never subtract two timestamps whose zones are not both pinned; print the zone | 403 minutes under a 300-minute cap is impossible — the impossible reading was evidence about the formula |
| R199 | Measured one side of a two-sided operation and routed two databases on it | State what the real operation does and check the harness exercises ALL of it | Both databases died |
| R209a | A key-sharing detector silently dropped IPv6 | Assert the parse: count rows where the extracted field is empty/NULL/unchanged, require zero | Would have named three innocent users as credential thieves. The check was one line: `SELECT COUNT(*) WHERE instr(ip_address,'.')=0` |
| R212 | My own audit was the defect it was written to find | Bound the resources of an aggregate over an unknown corpus; ask what the CHEAPEST evidence is | An exact distinct-count builds a hash table of every distinct value across the store |
| R213 | Tested for zero, and zero is not complete | "Is it absent?" and "is it complete?" are different questions | Hand-curated demo rows are the signature: 10 noaa, 21 fed_board, 22 census, 25 usda, 52 zillow, 61 fhfa |
| R214 | Measured a proxy for what the code actually reads, then wrote the number into it | Measure the artefact the CODE reads; a sidecar/manifest/catalogue is a claim about data, not data | A column named `dataset` partitioned presentations, not files |
| R220b | Invented two ids, got 404, nearly filed a bug | A 404 is evidence about the id before it is evidence about the code | "A failure that lands exactly where I expected it is the one to distrust most" |
| R223b | Read "size does not fit in an int" as a hard ceiling | An exception message names what the runtime NOTICED, not the cause; reproduce before designing a fix | A failure on a small machine and not a large one is about RESOURCE GROWTH, not a hard limit |
| R225b | Given a three-part definition, implemented one part, reported against it | Implement N clauses and write the count into the code | A metric that silently drops a term is worse than no metric, because it is quoted with confidence |
| R239 | Printed a value I had not measured, then read it back as evidence | Never put a constant on the same output line as a measurement | `print(f'... upstream years WITH values: {yrs}  (ours: 2023)')` — "ours: 2023" was a literal |
| R241 | Measured the file the writer writes, not the files the reader reads | The store is what the RESOLVER opens, not what the fetcher writes | Filed "17,459 series dark, negligible against D1 headroom". The real number was **912,990** |
| R249 | Match the tool to the CLAIM, not the keyword | A claim about RUNTIME behaviour needs control flow, not an occurrence count | Counted `merge_and_write` with grep (matched a docstring), then with an AST call-count (cannot see loop position). Only 2 of 5 modules actually matched the claim, which was already in a pushed commit message |
| R252 | "The indicator is available" is not "the series are available" | Measure at the grain you intend to REPORT at; an impossible >100% intermediate means the two sides count different kinds of thing | Reported who_rs 100%; the fetcher returned 1,558 of 2,207 published keys — 70.6% |
| R257 | A clean causal story the data refuted | Name the number that would refute your hypothesis, and measure it before fixing | The tidy account needed a queue-ordering effect; **3** units fleet-wide had success older than attempt. The real cause was COST: 68 sources cost 24.5 min combined while 27 cost 1,031 min against a 240-min budget |
| R262 | The measuring instrument is a claim too, and it decays | Re-derive "what starts a run?" from the SYSTEM, not from the definition you were handed | A FOURTH scheduler existed (`run_local_heavy.ps1`); nine sources refreshing every ~20h read as unscheduled. 120 of 217, not 112 |
| R264 | A wrong listing PUBLISHES false numbers about our own system | Audit every number derived from a broken listing; count the store before crying data loss | dst reported obs falling 9,198,885 → 231,035 (97.5%); the store held 9,220,012 rows across 707 files. `_total_rows()` read through blob but LISTED with `os.listdir` |
| R267 | Fabrication and projection look identical in a MAXIMUM | A threshold finds candidates; only reading a record decides. Two scans of one aggregate are one measurement | `bfs` at 2150-12-31 was 49 rows of 5,337,621 — Swiss demographic SCENARIO projections, real data |
| R281 | The checker hardcoded the very thing it was checking | A checker reads the parameter it judges by from the code under test, and PRINTS which one it used | Hardcoding `(series_key, obs_date)` produced 257 under-keyed files, 166 of them false positives on treasury |
| R282 | A true aggregate over the wrong file set | Ask which code path WRITES those files, and whether the damage would already be visible | 91 "under-keyed" bea files were all in the ingester-written tree the fetcher never touches |
| R288 | Measured the symptom, so scope, grain and fix were all wrong | Measure by the DEFECT SIGNATURE, not by what the defect looks like | Proxy: 429,781 rows / 11 files. Signature (`TLIST(A1)=1991` inside a series_key): 290 matrices, 754,780 rows, 17 files. Subject-grain repair would have deleted 3,274,801 rows to fix 304,165 |
| R290 | Reported progress from a log the job stopped writing eight hours earlier | Launch ad-hoc long jobs with `python -u`; when you have no measurement, say "progress not instrumented" | The log was 4 lines, last written five minutes after start. Later the same day `ps -W \| grep -c derive_csv` returned 0 on a process alive at 253,452s CPU |
| R294 | A selftest's fixture grew by 1.09M rows and I nearly called it a production hole | A delta is meaningless without its baseline, and a selftest's baseline is not production's | Selftest grew `cu` to 1,204,362 rows; production `cu.parquet` holds 1,837,791 — larger |
| R296 | Re-ran a migration's safety measurement against the wrong store | Every tool that can address more than one store must print which one it chose, in its first line | Same 100 files: r2 → 247,714 rows collapsed; local mirror → **0**. The local answer was the flat opposite, and the comfortable one |
| R300 | Generalised one root cause to seven sources on shape alone | "200 with a real body, zero rows parsed" is the fingerprint of a CLASS, not of a CAUSE | hagstofa's failure was positional time codes with the period in the LABEL, not the daily grammar; the fix would have changed nothing |

---

### 7. Group C — False-absence probes

**Count: 22.** A probe, grep, listing or lookup returned nothing, and the nothing was reported as a
fact about the world. R0 sub-rule 4 is the distilled form: *when a probe reports ABSENCE, run it
against something known PRESENT, and a FAILED control VOIDS the run.*

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R15 | Reported a form field empty from one scripted read | Confirm against the underlying element value AND the authoritative record before telling the user | A flaky global (tinymce), an iframe boundary, or an icon-rendered value (ORCID logo) all yield false negatives |
| R57 | Stated an inventory from a check that could not have found the thing | Ask what the query returns if the item WERE present | Fourth wrong statement in a row about the SEC inventory |
| R61 | Absence from a listing is not absence from the API | Probe the specific endpoint before delisting anything | Six defillama `protocol_tvl` series missing from `/protocols` all answer 200 at `/protocol/<slug>` — they are PARENT entities the child-only listing can never reach |
| R75 | A negative result is only as broad as the key you looked up | State what the check would have missed had the thing been present under another name | "10 datasets retired, 45,000 series can never update" — they were RENAMED (PSBSFAD→PSBS, GENDER_*→GS_*) with identical series counts. Identical cardinality with zero matches proves the TEST is broken |
| R101 | "Never ran" usually means "not yet born" or "never asked" | Read what the run was TOLD to do (event type, `INPUT_*` env) and when the code landed relative to the last cron | The fetcher's first commit was FIFTEEN HOURS AFTER the last scheduled run |
| R104 | Read my own 404 as the publisher's answer | Search the project's canonical record before the open web | `DATABASE_LICENSES_VERBATIM.md` already held UNESCO's live URL with the grant quoted word-for-word, and the source marked CLEARED |
| R106 | An error body parsed as JSON is indistinguishable from an empty result | Assert the endpoint exists; make the probe fail loudly on an unexpected payload | `.get("results") or []` turned `{"error":"not_found"}` into zero hits, four times — one step from reporting 17,274 companies invisible to search |
| R114 | "No audit exists" is not evidence about the licence | When two records disagree, READ THE PUBLISHER; keep looking past the first confirming source | The publisher grants CC BY 3.0 IGO on its own download page. The first source found (un.org's site-wide "All rights reserved") agreed with the wrong answer |
| R122 | Checking a record FOR ONE SOURCE is not checking it | Two greps — source id and publisher name — across BOTH compliance records, before any assessment | Wrote a NEEDS-HUMAN-REVIEW verdict on IEP and asked to email them; the permission trail records the request was already GRANTED on 2026-07-06 |
| R134 | When a probe says "MISSING", suspect the probe | Print one whole record and look at its keys before escalating a negative | Queried `/v1/sources` for `source_id`/`id`; the handler emits `source` |
| R137 | One document can have two formats | Search by SEVERAL identifiers and know every shape the document uses | `cso` read as UNASSESSED because only `### Publisher` sections were searched; the confirming `grep -c "cso"` matched inside "HCSO" |
| R143 | A heading is a claim about the query under it | Label a result with what the query actually did | Printed a plain `GROUP BY source_id` under the heading "top SERVED sources"; the served-filter was in a different query |
| R161 | A grep pattern structurally incapable of matching | An empty result is only evidence if the query could have produced a non-empty one; use the file's own parser | Grepped `^  [a-z0-9_]*fed` (mapping shape) against `registry.yaml`, which is a LIST (`- source_id: fed_board`) |
| R206b | A helper that returns `{}` on failure | "It ran without error" is not evidence it worked — count what it produced, on real data | Cannot distinguish "nothing changed" from "I cannot read your data" |
| R211 | Read a source's silence as "busy" and parked real work | Find the line where the blocking job says it is doing the thing; a skip announced only in a summary is a silent skip | `--source` now fails fast on any name matching no unit |
| R229 | Re-derived an analysis that already existed | Grep the tree for a component before building it; prior sessions leave REASONS, not just artefacts | A module's docstring had already rejected the approach being proposed. `ls updater/strategies/fetchers/_*.py` would have taken two seconds |
| R233b | Grepped for the user's reported string, got 0, fixed a different bug | Grep the USER'S EXACT STRING across every surface; a string on screen but in no file is BUILT AT RUNTIME | The reported bug stayed live while a different one was fixed |
| R238b | Told Ahmed a feature did not exist; it had been live a week | Search for the CAPABILITY, not one implementation's name; `git log` the file they named | Searched `visitor-count`, `visitor`, `visitors`, `injectVisitorCounter`, `public-stats` — all five terms came from hf's implementation. The portal uses a hidden `new Image().src` beacon, already holding 68 loads of `/` since 29 July |
| R259 | "0 live sources use it" was true of the directory searched | Scope a sweep to the PROPERTY, not the module type — every executable surface (`py, ps1, cmd, yml, sh`) | A `RELAUNCH_GUARD.ps1` relaunched the banned DBnomics puller EVERY FIVE MINUTES, and `updater-daily.yml` called an audit tool that hit the banned host on every run |
| R261 | A listing that returns `[]` instead of failing | Ask what distinguishes "nothing is there" from "I could not look"; if nothing does, that is the bug | `bea._tree_frontier` globbed a local dir absent under `backend=r2`: raw 1 file, routed-recursive **591** |
| R276 | A uniform "not found" reads as a finding | Suspect the accessor before the data; copy the accessor from production code that already reads the structure | Called `r.get('tcmb')` on `registry.load()`'s 3-key dict, so five sources read as "NOT IN REGISTRY". All five were registered |
| R299 | "60/60 sub-units transient-failed" was a parser gap wearing an outage's clothes | Whenever one message covers a self-healing cause and a permanent one, the permanent one hides | Every one of the nine named matrices returned HTTP 200 with a real body and parsed to ZERO rows — daily (`2010M01D01`) and academic-year (`2003-2004`) axes. **5,983,026 rows recovered** |

---

### 8. Group D — "Green is not done"

**Count: 48.** The job exited 0, the badge was green, the test passed, the gate was wired in — and
nothing had been proved. R50 is the compact form: *a green run is not a proof; require positive
evidence of work.*

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R3 | Store-backed econ first passes only run on the workstation | A CI run for these sources no-ops with "source dir missing" | |
| R14 | A watchdog reported success it never achieved | A monitor must confirm its action took effect (real post-state / HTTP status) | The HF cron watchdog reported "cancelled" it never achieved |
| R23 | A flagged digest source is last-recorded state, not a fresh failure | Only `live:true` sources ran in the daily econ CI | 2 of 133 |
| R33 | Compute and verify BEFORE writing | Coupled files are ONE atomic change; prove it with the consumer's own validator, and guard identifiers with exact match | The script pruned the registry to 123 while `config.py` said 133 — a hard assert refuses ALL runs on mismatch, so the entire daily updater was dead. The assert that fired was itself a false positive from `startswith` matching `sipri_polity` |
| R35 | "Configured" is not "running" | The acceptance test is a number from the run's own output, compared to what SHOULD have been processed | Four days of green CI processing **2 sources of 113** in ~85 seconds. Every run printed `=== 2 unit(s) processed ===` |
| R44 | `tally.structural_unit()` is a whole-source veto | One odd file must be `empty_unit()`, not structural | owid merged 0 of 150 charts because 5 were zero-row; ons_uk 0 of 25 because 2 were; ember 0 of 32 because 11 were |
| R50 | A green run is not a proof | Require units>0 and rows counted; cadence gates make `--source` runs vacuous — use `--force` | "0 units processed" + exit 0 is a FAILED proof |
| R51 | Validate a gate against ground truth before scaling it | Check the SOURCE, not just our copy; account for the partial period; round before comparing | |
| R64 | A test that cannot fail proves nothing | Verify the test detects the bug's PRESENCE; control time with an injected clock, not by patching a shared module | Patching `M.time.sleep` made the fake request's own sleep a no-op — all 5 attempts ran in 0.00s |
| R71 | A one-shot "already checked" guard becomes a trap | Give a cached negative about MUTABLE external state an expiry or a re-check on the dependent action | Anyone who browsed econ before registering was keyless for the rest of the session, with a Download button that silently did nothing |
| R81 | `cancel-in-progress: false` protects the running job, not the queue | Serialise dispatches; read `conclusion`, not `status` | Four dispatches in twenty seconds: one running, one pending, two silently CANCELLED — each reporting `completed` |
| R83 | A document that parses is not a document that arrived | Gate bulk pulls on VOLUME against what is already published | The same IMF URL returned 31,884,260 bytes / 221,749 observations and, minutes later, 1,582,721 bytes / 10,100 — the short one well-formed. Every guard passed on 4.6% of the dataset |
| R94 | A completion count that counts its own bookkeeping | Count UNITS OF WORK, never iterations completed | "LOOP COMPLETE: 118 of 118 sources finished" for a 580-million-row job, in two minutes — CRLF line endings made every invocation miss its directory and exit 0 |
| R99 | "Complete" is a claim about what you did not read | Any loop that skips an input must NAME what it skipped | A bare `except Exception: continue` dropped 58 whole files: bls reported as 57.4M observations against an actual 328.1M |
| R118 | Never run a WRITE tool to observe its output | Ask what a tool WRITES before invoking it for its printout | Running `catalog_complete.py cso` to see one warning line inserted **9,920,979** catalog rows for a source pending Ahmed's decision |
| R142 | A check that would have printed CLEAN on a real gap | Count on a token you control or on structured data, never a formatted sentence | `endswith("0 served source(s) with NO page")` also matches 10, 20, 30. Fourth unanchored-match bug in one day |
| R144 | "No output" is a failure signal, not a pass | Never write to a store a background job is also writing to | A fix to un_wpp's attribution was overwritten by an in-flight re-export carrying the pre-fix value; the apply was piped through a grep that printed nothing |
| R168 | A counter took the whole nightly refresh offline | When a validation's failure mode is TOTAL, its alarm must fire at the moment of the change | `EXPECTED_SOURCE_COUNT` unbumped across two commits (128/125, then 134/125). `orchestrate.py` raises SystemExit — the run aborts before touching a single source. Fixed with `tools/preflight_registry.py` on every push |
| R172 | An early exit that reported `ok` | Every early-exit path must answer "what status does this report, and does that status let the gate advance?" | A budget-expired run returning `ok` records the new vintage, so the unfetched remainder is never pulled — a partially-loaded dataset behind a green source |
| R178 | A missing dependency makes a source report "no adapter built" | Audit the import graph STATICALLY against the requirements file | `lxml` absent from `requirements-updater.txt` would have made a just-promoted source silently never run. Third incident of the class (openpyxl, xlrd were the others); `numpy` was found undeclared on the audit's first run |
| R187 | Predicted a benign cause for a red run; it was a TOTAL OUTAGE | Read the log before explaining the colour; `cancelled` means suspect the runner | Memory climbed 1,211MB → 15,700MB at 299 MB/min for 48.5 min. `gh run view --log-failed` returns EMPTY for a cancelled step. The workflow's own OOM guard keys on the child's exit code and cannot see the parent being killed |
| R193 | Almost reported "verified in CI" on a run where the source never executed | Find positive evidence of execution: log lines, non-zero counts, a `<<<` completion | No `[bis]` lines, no completion, zero memory samples |
| R201 | A probe printed "CLEAN" on a response truncated at exactly 100,000 rows | Prove the response was complete; treat any round total (1,000 / 10,000 / 100,000) as a cap | "Deletion-shaped truncation passes every test that only looks at what is present" |
| R208b | Stamped a cadence "success" on a run that crashed | Gate a cadence marker on the durable artefact, not on reaching the end of the script | The stamp was unconditional in a function whose own exit code was in scope |
| R210b | A batch loop grepped stdout for success, so four failures rendered as silence | Test the EXIT CODE per item and print stderr on failure; verify the artefact, not the transcript | `catalog.db` is a shared writer and needs a `busy_timeout` |
| R216a | Tested the renderer and reported the feature as working | Verify at the boundary the user is on: does the RESPONSE contain the field | The panel said "0 dl" for every account. One grep of the response builder would have returned nothing |
| R216b | Every background job reported success, because the wrapper always did | Background as `cmd > log 2>&1` and nothing else — no `; echo`, no `; tail` | A green result from a harness you wrote is evidence about the harness first |
| R218 | `node --check` passed a file the deploy toolchain rejected | Verify with the tool that will actually process the artefact | Used as the syntax gate ~15 times that day; the real gate is `wrangler deploy --dry-run` (real esbuild) |
| R219b | "errors 0, skipped 0" with a REFUSED list three lines below | A completeness summary must enumerate every terminal disposition: emitted + refused + skipped + errored == considered? | |
| R220a | Four first-pass checks gave confident wrong answers in one day | Assert the precondition INSIDE the test; corroborate from outside the check | When a scan reports NO defect, read a sample anyway — "clean" is the answer that gets audited least |
| R222b | A mixed pass/fail across identical code paths | After a deploy, WAIT before probing; a hypothesis that explains the mechanism but not the PATTERN is untested | Global rollout is not instant and looks exactly like a partial outage |
| R224b | A verifier that says SERVED without asking the thing that serves | A checker's NAME is a claim; if it says SERVED it must test everything serving requires | Four stages (catalogue, R2, D1, `SUPPORTED_SOURCES`) were in my head; the verifier covered two |
| R231 | Simulated a code path instead of running it and "found" a bug a guard had covered since July | Do not simulate a pipeline you can RUN; find the commit that introduced a defect before fixing it | `git log -S"def sane_date"` dated the guard to a month before the bug was claimed. A comparison returning "no difference at all" is more likely a broken comparison |
| R232a | Wired in a monitor that would have examined nothing, and passed | A check is done when you have seen it produce a real verdict IN ITS ENVIRONMENT — and seen it FAIL | "Cannot evaluate" and "evaluated, all clear" must never share an exit code. `\|\| true` on a monitor deserves the same scrutiny as a bare `except:` |
| R233a | A fix planted a false red that would only appear once the source got healthy | Ask what a fix does to the MONITORING, not only to the data | Changing `cadence` drove scheduling AND the lateness clock. A masked defect is not an absent one: dst read ATTENTION, so the data verdict was never computed |
| R234b | A batch loop printed "DONE" while one of its 15 items had crashed and synced nothing | Count SUCCESSES, not iterations; verify the FINAL state at the destination, not the source | One emoji in one title killed a 100k-row job — `PYTHONIOENCODING` is a runtime dependency of PRINTING, not just of writing |
| R236 | Sandboxed 4 of 21 units; both bugs were in the other 17 | Say the fraction out loud — "four of twenty-one" — and the gap announces itself | The four were chosen because they were interesting, not representative. The run takes four minutes |
| R237 | The drop counter existed and was never printed | If you increment a counter for discarded input, PRINT it in the same commit | Every state row was discarded while the run reported `ok, +66 row(s)` |
| R242 | Three bugs in one fetcher, each invisible until the one in front was fixed | Fixing the error a source REPORTS does not mean the source works; bugs queue | bea had existed for weeks and never refreshed anything |
| R246 | "Scheduled" is not "attempted" | Count what the RUN DID; age-since-success and age-since-attempt are different measurements | Reported "N of 217 scheduled" for weeks; the run ATTEMPTED 20 of ~106 live cloud sources and said so: `RUN BUDGET 240 min SPENT — 76 source(s) NOT ATTEMPTED`. ~5x overstatement |
| R248 | A gate that CRASHES reads as a verdict about the data | Read the ERROR, not the colour; one malformed input must never end a sweep | `updater.health` died on one registry entry holding free text, so `assess()` covered ZERO of 217 sources — for three days |
| R270 | "How many have EVER succeeded?" was the question not asked | Read the NOTES of the ones that did not; fix a misused helper per-caller, never uniformly | Of 120 SERVED+SCHEDULED sources: 55 within 7d, 13 within 30d, and **52 NEVER**. `_max_by_key` returned strings but was annotated `-> dict` |
| R277 | A fixed source reports its pre-fix verdict until re-attempted | Compare a verdict's AGE to the last commit touching that fetcher; force-run before diagnosing | Four of the first five "bugs" worked from the digest were already fixed. The daily run reaches ~20 of ~106 cloud sources per budget |
| R283 | Clearing the blocker you found does not establish there was only one | Re-run the ORIGINAL claim through the REAL code path and require it to produce the thing you are enabling | census bds was fixed, enabled, tests passed — and the tail returned ZERO rows. The evidence came from a hand-picked 3-column probe the fetcher does not make |
| R291 | A queued run is not a reservation | Before debugging a source that "never runs", check whether it was ever GIVEN a turn | GitHub holds ONE pending run per concurrency group and a newer arrival EVICTS it. Seven of eight daily runs that day were my own proof dispatches; one evicted the waiting heavy run, and all nine heavy sources got no run |
| R292 | "bls FIXED" recorded twice while its change-probe raised on every call | Find it by CALLING the function, not reading it | A 2-way unpack of a 4-tuple: `current_vintage()` raised `ValueError` on every call, so its documented `return None` fallback was unreachable code. Swept all **142** fetchers: 102 token, 6 None, 34 without, **0 RAISE** |
| R293 | A suite green for its whole life failed the first time it ran anywhere else | A test suite that has only run on one machine has been shown to agree with that machine, not to test the code | The test asserted the `GITHUB_RUN_ID` unset branch, which can only pass off a runner |
| R297 | Three "never succeeded" errors: two were stale, one needed a control on the right stack | Date every error against the fix history; a control run on the wrong stack tells you nothing in either direction | Local pandas 2.3.3 coerced the value to NaT; CI pandas **3.0.5** kept year 6 as `datetime64[us]` and raised. `requirements-updater.txt` pins `pandas>=2.2` with no cap, so the runner takes a MAJOR version the developer never runs |

---

### 9. Group E — Repository vs running system

**Count: 29.** The change was made, committed, even deployed to *something* — and the thing users
touch was unaffected. This is the group that produces false statements to Ahmed most directly,
because the claim is always of the form "it is live".

The architecture that makes this possible is worth stating once. A datum reaches a user through five
hops, and a check on any one of them proves nothing about the others:

```
raw parquet in R2  →  catalog.db (local)  →  refresh_r2_catalog.py  →  R2 _aqueduct/catalog.db.zst
                                          →  derive_csv            →  R2 series/<id>.csv
                                          →  D1 econ-catalog        →  Worker (util.ts SUPPORTED_SOURCES)
                                                                    →  wrangler deploy
```

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R9 | Reassured "you're not exposed" before probing the live surface | Never assert compliance posture from code, configs or docs — probe the LIVE surface per channel | Staged is not deployed. Channels: API csv, bundle, MCP, site UI copy, JSON-LD |
| R11 | Answered "served" when asked about "showing" | "Showing" (pages, listings, JSON-LD) and "serving" (endpoints) are separate compliance surfaces | A 451 endpoint does not excuse a page advertising the download |
| R20 | `catalog.db` is not reproducible from `sources.yaml` | Never regenerate the gate with `build_registry`; edit the deployed denylist surgically and diff | Stage-0b wrote licences straight to the DB |
| R28 | Claimed a data fix "reached users" without tracing the serve path | Trace raw store → catalog.db → derive → D1/R2 `series/` → Worker, and confirm the source is PUBLISHED | The Worker serves pre-derived CSVs and "DOES NOT parse parquet"; all 9 PxWeb sources were in 0 of 191 catalogued source_ids |
| R36 | "Works on my local run" proves nothing about CI | Store reads/writes go through `blob.*` only; the acceptance test is a real `backend=r2` run | Locally the blob path and the raw path resolve to the same file, so a local run cannot detect this |
| R38 | The catalog lives in TWO stores | Every catalog mutation must propagate to D1 AND `_aqueduct/catalog.db.zst` | A source in D1 but absent from R2 demotes its every run to `partial` "csv coherence unmet" |
| R56 | Restarted crawlers after a drive move and never checked WHERE they wrote | Verify the OUTPUT PATH, not just that the process runs | `os.makedirs` recreates a directory you just renamed |
| R60 | Measure completeness at the surface the user touches | Reconcile local vs R2 vs D1 for EVERY series-level source | Declared 97,520 = 97,520 locally while the SERVING catalog had 97,297; the same blind spot hid **31,259** stranded series fleet-wide |
| R65 | A two-sided integration verified on one side | When two components must agree on a NAME, assert the agreement or grep both sides | The server accepted the token; `download.html` read `edl_api_key` while every writer used `edl_key` |
| R96 | A fix in a file nothing serves from | Trace the value from where you edited it to where the user receives it, and verify at the far end | The licence correction went into `data/catalog.db` — gitignored, and never read by the Worker, which resolves from D1 |
| R103 | A correct-looking response is not evidence your change caused it | Count the ROWS THAT REFERENCE the thing you edited; prefer a check that would FAIL if reverted | Updated five D1 licence rows, got "changes: 5", saw the corrected URL live — those five rows have ZERO sources referencing them |
| R107 | Two stores that can disagree must not share one diff | Make the plan the DESIRED STATE and every writer idempotent | The tool computed the diff from catalog.db, wrote catalog.db, then derived D1 work from the same plan — so D1 stayed stale while printing success |
| R108 | Refreshed a derived artefact without moving its source | LIST the remote prefix for the layout you are writing; write store and served object in one step | 68 companies had a served CSV containing facts their own stored parquet did not; a later rebuild would have silently reverted the refresh |
| R116 | Fixed the wrong column | Grep the serving SQL for the field; when an end-to-end check contradicts your DB read, believe the artefact | Corrected `source.license_id`; downloads resolve from `series.license_id`. At the correct level, **872,153+** series differ from their own source row |
| R119 | Any claim about what users see comes from the serving store | State it as a precondition: "I checked the database" is unfinished until you say WHICH database | Third instance in one day (after R107 and R116); nearly reported a phantom breach on 18,838 idb series |
| R124 | Syncing series is not syncing the source | After any un-gate, READ the citation header for attribution, licence and terms | 199,661 series rows pushed with no `source` row: CC BY-SA 4.0 data served with no attribution. HTTP 200 and the object counts all passed |
| R138 | A 501/404 seconds after `wrangler deploy` is probably propagation | Retry, and cross-check a second endpoint reading the same state | `/v1/bundle` already resolved the id from the same `supportedSources` in the same deployment |
| R155 | Commit a resolver change BEFORE the destructive step it authorises | The deploy is what makes the deletion safe | Deleted 25,057 catalog rows + 25,057 R2 CSVs while `util.ts` was still uncommitted — the next clean-checkout deploy would have restored the resolver over deleted data |
| R182 | Pushed a website change and would have called it shipped | econdatalibrary.com is a MANUAL Pages deploy — `Git Provider: No` | The repo has five workflows and none deploys the site. The command is `npx wrangler pages deploy catalog/site --project-name=econdatalibrary` |
| R184 | whr passed my own audit TWICE while re-downloading forever | Ask what a test is INCAPABLE of seeing; a two-probe comparison only catches movers faster than the gap | The `Last-Modified` was the CDN cache-FILL time: probed at 03:26 it said 03:26:17, at 07:33 it said 07:33:58. Fixed with a FETCH-TIME verdict needing one probe |
| R200a | Audited 124 KB of a 268 KB file and called it "the entire login mechanism" | Establish which bytes actually run — the deploy config (`wrangler.toml` `name`/`main`), the branch, the file size | `grep /token/exchange` on the local file would have returned nothing and ended the audit before it started |
| R200-addendum | Four worktrees can deploy to one live worker | When several checkouts can deploy to one target, enforce that only one can | A deploy refusal that blocks a destructive action is a feature wearing an error message |
| R206a | A verification probe read a different repository than the one just edited | Pass ABSOLUTE paths always; when a check contradicts an action that reported success, suspect the check | An Edit that returned success has already proved the bytes changed |
| R207a | Verified a fix was PRESENT on the page and never that it could RUN | For anything calling into a dependency, verify the dependency RESOLVES where the code runs | `typeof window.X` in the live page is a two-second check and is the difference between "served" and "works" |
| R222a | Announced a production outage that existed only in my own staging copy | A claim about what USERS experience must be measured against the LIVE origin | "A diagnosis that neatly explains a bug the user already reported is the one to verify hardest" |
| R224a | Took the public stats endpoint down on every family site by adding words to a list | Ask what SCALES with the thing being changed; after deploying to a live read path, curl it | `INSTITUTION_BLOCKLIST` grew 82 → 103 and every `/v1/public-stats` call returned `D1_ERROR: too many SQL variables`, on hf, econ and everything else |
| R225a | Changed a validation rule server-side while the browser enforced the old one | A validation rule normally lives in more than one layer; grep the FRONT END whenever you change it server-side | Five server call sites, two registration doors, three edit paths and the ORCID auto-create were all swept; the browser was not |
| R232b | A generator held a hard-coded copy of a value that had moved on | Diff a generator's output against what is LIVE, not the working tree; fix the generator in the same breath as any hand-edit | Regenerating would have silently undone a deployed fix |
| R272 | Routing the READ fixes nothing when nothing ever WROTE to the store | For a store-routed sidecar, check BOTH ends; read the path function instead of guessing the filename | cso's `_catalog.json` is 3,140,483 B locally and ABSENT from R2 because only the ingester wrote it, locally. A routed read would have found an empty store forever |

---

### 10. Group F — Instance vs class

**Count: 15.** Ahmed's standing rule, saved to memory as `feedback_example_means_class`: *a reported
example is one instance of a class; sweep the whole surface, fix all, prove it with a zero-result
check.* These are the entries where that rule was known and not run.

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R31 | Edited a UI control without the code that reads it | Edit a control and its reader together; a new `<option>` value with no handler is a silent no-op | The access-tier filter silently filtered NOTHING — misleading, not an error |
| R66 | Names are an interface | Grep every consumer before assuming a convention holds | One file of four used a different localStorage key and silently disabled the site's primary user action |
| R95 | A defect you were handed is a sample, not the population | After fixing an instance, enumerate every row that could share its shape and show the count is zero | The same licence fingerprint was live on **ten more SERVING rows** — WHO, UNESCO, Statistics Estonia, Fund for Peace — **105,301 series** |
| R207b | Fixed an Arrow `group_by` crash in one file and left it in seven others | When the defect is in a LIBRARY CALL, the unit of repair is every call site | "A comment in one file protects one file" |
| R208a | Three consecutive rejections for fixing the instance named and not the class | When a review names N instances, the deliverable is the ENUMERATION, not N fixes | "I fixed the ones you found" is a much weaker claim than "here is every place this pattern occurs" |
| R217 | Rebuilt a bug the same file had already fixed, twenty lines from the fix | Before adding state that suppresses a check, search the file for existing suppression state and read why it looks that way | A neighbouring flag already had an expiry, paid for in production |
| R221a | A rename broke a second lookup keyed on the same value | Enumerate EVERY structure keyed on a value BEFORE fixing any of them | `INST_PRESTIGE` (sort order) was fixed; `INST_DOMAINS` (favicon) stayed broken. A missing favicon renders as empty space, not as an error |
| R234a | Fixed two of three bypass paths and wrote the warning that should have found the third | When you catch a category error, fix the CATEGORY — enumerate the schedulers | `git grep` for the FIELD (`run_location`), not for the file |
| R240 | Wrote a workaround for a missing key instead of asking why the key was invisible | The SECOND time you write the same workaround, find the missing primitive | `BEA_API_KEY` was in the repo's `.env` the whole time and nothing loads `.env`. Ahmed was told twice to register for something he already had |
| R244 | An audit scored every module against a store of the same NAME | When an audit maps code to data BY NAME, enumerate what has no name in that space | Shared helpers, base classes and mixins run for every source and are unflaggable by a name-keyed gate. "An unjudgeable unit is not a clean unit" |
| R256 | Hand-listed "the class" twice and was wrong both times | Define the class by a DEFECT PATTERN and enumerate it with `grep -l '<the exact defective line>'`, BEFORE patching | Patched five PxWeb sources from memory; the grep printed two more (`cso_ireland`, `dst`) — 40% more work. "The zero-result check is not confirmation, it is what DEFINES the work" |
| R279 | Fixed a class by editing the instances I had looked at | The sweep has to be mechanical and it has to be an assertion that can fail | Three of four anonymous `tally.transient_unit()` calls sat together and were fixed; the fourth, 60 lines earlier, fails the WHOLE RUN |
| R284 | An exclusion justifies itself | Check scope notes on a SCHEDULE, not on suspicion; write exclusions as dated MEASUREMENTS | A header asserted 60 census files "do not gain periods"; Census's own catalogue lists every one as a timeseries dataset, and 16 flows had been two months behind for as long as the sentence stood |
| R285 | Searched for one filename, not for the mechanism | Enumerate a capability's implementations before counting; prefer evidence of BEHAVIOUR | Two sources kept bookmarks under different filenames. The behavioural test: rotating sources leave SCATTERED write times, stuck ones leave one contiguous stale tail (adb: 2 blocks, 44 of 54 flows frozen) |
| R287 | Two bookmarks with the same name mean different things | Durable-state audits must ask what the state ASSERTS, not where it is written | 15 fetchers call `save_rotation`; 14 in-loop. usda's records "which cursor window was REPORTED", so advancing it per-table would silently skip up to 50,000 cursor keys no run ever emitted |

---

### 11. Group G — Prose treated as implementation

**Count: 20.** A comment, docstring, log line, commit message, registry note or plan sentence
asserted a behaviour, and the assertion was believed — usually by its own author, later.

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R6 | Built API automation on the session log's recorded PAT | Documented credentials decay; use `git credential fill` / live stores | The Key Credentials table was partly stale — the PAT and admin API key had been rotated |
| R25 | "Unified" N call sites on the strength of my own summary | Read all N against ground truth; a remembered defect may be partly wrong | 1 of 2 claimed bugs was real: statfin's gate was correct, only stat_estonia was inverted |
| R58 | A rule you write but do not implement will not save you | When a postmortem's remedy is "make X observable", implement X in the SAME change | |
| R62 | A log line that sounds like a fix is not a fix | When a message describes an ACTION ("abandoning", "skipping"), verify the action has STATE behind it | istat printed "SSL FAIL, abandoning host" on every request and re-dialled the same dead host for the next year, burning ~3 hours on one flow while writing nothing |
| R109 | `str.replace` cannot fail, which is why it is the wrong editing tool | Use the Edit tool, which errors when the target is not found; if a script must edit, assert the string changed | A function was "added", defined and never called, and the tool reported success |
| R125 | A comment describing the change is not the change | After editing any list, grep the LITERAL token in the file you edited | Added a four-line comment saying `wid` was added to the resolver and never added `"wid",`. tsc passed, D1 had all 2,465,197 rows, and every download returned `not_migrated` |
| R130 | A loop that restarts at item 0 does not "resume" | Name the thing that makes run N+1 different from run N, and prove it with a negative control | The docstring said the next run picks up deferred countries; the loop walked `sorted(rows)` from the top every time, so the end of the alphabet was unreachable at ANY budget |
| R152 | An error message that asserts its own cause without checking it | State only what you verified, or name the possibilities | "N changed keys unmapped (over derive-all cap)" — the cap clause is HARDCODED and never tested. riksbank has 117 rows against a 5,000 cap |
| R153 | Called a helper I had not checked existed | After writing a call to a helper you did not just read, grep for its `def` | `_catalog_count()` does not exist anywhere in the repo; in CI it would have raised inside the code that reports breakage |
| R160 | Our own registry asserted an upstream capability that does not exist | A `strategy_reason` is a HYPOTHESIS; send the narrowing request and compare counts | The API accepts `$filter=VALDATA gt {date}`, returns HTTP 200, and ignores it — full series (68/68) on every cutoff |
| R162 | Guessed a probe URL instead of deriving it from the downloader | A probe and its fetch must be built from ONE expression | The probe added `&label=include`; the downloader does not send it. A mismatched vintage tracks a different object, silently, in both directions |
| R164 | The registry told me to build a cache gate that cannot work, and I built it | Probe the validator before building on it — three HEADs and two GETs cost fifteen seconds | `Output.aspx` `Last-Modified` advanced on every call: 03:17:40, 03:18:01, 03:18:21 across three HEADs twenty seconds apart. No ETag, no HEAD Content-Length, Range unsupported |
| R190 | Shipped a log line promising behaviour the code did not implement | When a fix bounds work, ask what makes the UNDONE part get done and prove it separately | "They drain next tick." A bound without a resume mechanism is a truncation |
| R191 | R190 recurred within the hour, in a commit message | Any sentence of the form "X is imported / bounded / verified" is a CLAIM ABOUT CODE; grep at the moment of writing | `grep import verify_derive_parity.py` would have taken two seconds |
| R195 | A fix targeted the wrong axis and the commit said so without noticing | When a fix has two parts and one is documented as NOT addressing the failure, check that the other acts THERE | "Between calls" and "within a call" are different places. A memory fix is verified by a memory curve, never by a green step |
| R202 | Twice in one day: the lesson was in a comment and never in an enforcement | A decision that lives only in prose has not been implemented; the record is not the deliverable, the check is | |
| R205a | Shipped a security fix and wrote in the commit that it closed the hole | Re-run the attack against the patched code and name the line that now stops it | Three of four fixes were incomplete; each time the thing being looked at was fixed without re-asking what else still reached the goal |
| R235a | Wrote "it works without a key" into the registry without testing it | Do not write an empirical claim into a config comment you have not run; check Content-Type and body shape, not just the status | An untested comment is a fabricated citation. 200-with-an-error-page is invisible to every `raise_for_status()` |
| R245 | The note blamed a cap the code had already returned before reaching | When you fix a hardcoded cause, GREP FOR THE SENTENCE; check the code can REACH the condition it names | The identical claim survived in the sibling branch on the hot path, on 28 of 54 partial sources, mailed daily in the digest. Under `BACKEND == "r2"` the function returns before the cap is consulted |
| R295 | A docstring promising a safety property the code did not implement | When a comment states a SAFETY property, locate the line that implements it before relying on it | The header claimed the dry run "REPORTS that count before anything is written". It did not, and the missing figure — identical duplicates vs real revisions — was the only one the migration's safety rested on. 2,457,810 collapsed rows are unactionable without it |

---

### 12. Group H — Data grain, keys, parsing and identity

**Count: 39.** The most technically specific group and the one most particular to this system. A
"series" is identified by a `series_key`; observations are `(series_key, obs_date, value)`. Almost
every entry here is a way for that identity to be wrong while every count looks fine.

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R22 | A parser SELECTION fix changes `series_key`s | Clean RE-PULL the old data (delete + re-ingest), never merge | Never-shrink misses growth and duplication; old and new rows never collide under dedup |
| R26 | Migrating a parser is half the job — the DELTA-QUERY builder picks the axis to tail | Builder-tailed dim must equal parser-keyed dim, or the live delta silently freezes | 24 of 27 in-scope two_axis tables were divergent; 2 statfin tables would have DOUBLED on the first live run |
| R27 | A whole-subject re-ingest is not detail-safe | "Corrupt" = fraction of obs-years in [1500,2100] < 0.5, never just min-year < 1500 | MAX_CELLS aggregation collapsed **34 clean big cubes** (one went 99,140 detailed rows → 24); then 5 far-future-corrupt tables were uploaded as "fixed" |
| R30 | `pc.extract_regex` returns an EMPTY capture, not null | Guard on null-OR-empty | All 7 hagstofa time-only tables (1,856 rows) collapsed into one junk `hagstofa:` entry; a `put == catalog` count-match agreed while both sides were wrong |
| R41 | A merging fetcher must report `series_cursors` | Build cursors from the merged table: `{k: max(obs_date)}` | Without them the run is demoted to `partial` — "fetcher reported no series_cursors for N merged obs" |
| R55 | A parser returning `None` + a caller that skips = silent 100% loss | Make the discard COUNTABLE: rows-in vs observations-out | Sustained zero = failure |
| R68 | A loud failure on a few rows beats a silent coercion on all of them | Never convert a raised error into a null (`errors="coerce"`) when the input is legitimate | pandas `datetime64[ns]` cannot represent anything before 1677-09-21. 98 gapminder series crashed; the crash is what made the bug findable |
| R69 | A library's limits are part of your data model | Check the bound against the DATA's real range | Maddison population from year 0001, Chinese income from 0980; 320 series across three sources once the catalog was swept |
| R79 | An id vocabulary mismatch does not fail — it FORKS, and the fork reads as health | Assert new keys LAND ON published ids (count the intersection) and refuse below a floor | EPI 2026 exposed `iso` (AFG) where our ids use numeric `code` (4): merging would have added 63,354 new series beside 21,300 live ones, frozen every live series, and turned the health gate GREEN |
| R82 | "Correct so far" is not correct when the cases so far were degenerate | Name the property that made the successes succeed and find a case that lacks it | The validating score compared ids with the prefix STRIPPED, so it was structurally incapable of catching a prefix error |
| R91 | Coverage is not containment | Assert BOTH bounds: what fraction of the old set survives AND how far the new set exceeds it | A guard passed fao_qa at 99.2% while it minted **75,786 duplicate series**. Only sequencing saved it |
| R97 | A safety assert covers the failure it models and no other | Validate the INPUT's shape against the transformation's precondition | A collision check could not see un_wpp's **27,756,924** mid-year (07-01) observations each mapping to a UNIQUE 12-31 — no collision, every value silently moved half a year |
| R110 | An UPDATE that matches no row is a silent drop | UPSERT when new keys can appear, and REPORT the insert count | Two companies had data in R2 and no catalog row: hosted, paid for, undownloadable |
| R133 | One output object per loop iteration is a silent overwrite when two iterations share a key | Prove the key is unique across iterations: distinct(keys) vs count(keys) | cso has 7,988 (file, prefix) pairs but 7,896 distinct prefixes — 92 tables would have been PUT twice and served holding only the last slice |
| R145 | "The publisher gives us no title" was wrong in 3 of 4 sources | Codes belong in ids; LABELS belong in titles | un_wpp's country name is read at `ingest_un_wpp.py:100` and DISCARDED at `:128`. "Do not invent data" is never a reason to stop looking for what the publisher already gave you |
| R156 | A "skip" is silent data loss unless something counts it | A parser that can decline input must count and report the decline next to the successes | 5 STADAT tables rejected by a guard requiring TWO time-like header cells, which rejects a one-column snapshot (`Denomination;2024`) |
| R159 | An ingest that discovers data by probing a hardcoded list cannot become an updater | Check how the ingest DISCOVERS work before wrapping it | A frozen list of period strings ("December-2024-quarter") is a staleness bomb with a scheduler attached; fixed, gdp_quarterly moved to MARCH-2026 |
| R165 | The same never-hitting-cache defect in a fetcher shipped hours earlier | Sweep by EXECUTING the production code path, and never exempt your own work | All 25 `http_vintage` fetchers probed twice: 23 stable, 2 moving. One mover was my own. BIS Apache ETags flapped because several replicas hold identical bytes with different mtimes — `Last-Modified` went BACKWARDS five hours |
| R174 | Eight fetchers would have republished forever while their CSVs went stale | Ask what the CALLER does with each field you left `None` | `orchestrate._derive_changed_csvs` takes the changed set from `Result.series_cursors` and nothing else. Fresh parquet, stale downloads, green-ish logs |
| R175 | The fix for R174 was itself unbounded | Before shipping to N callers, find the LARGEST caller and multiply | ilostat holds ~30.8 million distinct store series; a budget-limited first run would build ~1.6M cursors — 1.6M SQLite lookups and 1.6M state rows. `CURSOR_CAP = 50,000` |
| R176 | "Every real source stays under the cap" — measured four, asserted all | A claim about "every source" needs the query that covers every source | bis/LBS alone has **608,570** distinct series; fhfa/annual_tract 63,930. And the cap shipped was per-FILE, so unioning files reached the unbounded total anyway |
| R177 | comtrade's published data is under-keyed, and the guard caught it | When a guard fires, ask what it is protecting you from | 24,086 rows under 4,154 distinct (series_key, obs_date) pairs, 1,240 with CONFLICTING values — one id-date holding 1,603,998,886.636, 2,729,735,494.827 and 4,816,420,248.446 |
| R180 | Wrote a guess into the plan as a diagnosis | Label a hypothesis as one and test it before it becomes the justification for a refactor | "Needs UNION matching" — pooling all ten FAOSTAT datasets scored 27.18%, identical to one dataset alone. The real cause was three re-coded ELEMENT codes (`7231 → 723113`), worth 27% → 79% |
| R186 | Guessed a return key; the source published 119,105 rows while reporting `no_change` | Open the function's `return` and read the keys | `stats.get("rows") or stats.get("n_rows") or 0` — the real key is `n_obs`. `tally.added` stayed 0, so 2,905 series would have shipped with CSVs never derived |
| R204 | Called correct data a bug | A date far in the future is a QUESTION, not a finding; when a wrong value is real data, the bug is in what CONSUMES it | Splitting `newest_obs` from `frontier_obs` fixed it without touching a row. RED-DATA went 0 → 6 → 2 across two fixes; stopping after the first would have shipped four false alarms |
| R209b | A store that passes every structural test can still be wrong for the reader | Ask whether an omitted dimension changes the MEANING, not the row count | "No duplicate (key, date) pairs" does not prove each id means one thing — two datasets at different frequencies can share an id and never collide |
| R221b | Announced "raw key as title" and wrote 694,300 rows with no title at all | A counter that can only fire when a lookup table exists cannot report that the lookup table is missing | "raw-key titles = 0" and "titles are empty" are the same fact; only one is alarming |
| R235b | A guard tuned for one kind of data silently censored another | Measure BOTH sides of a threshold; a field rendering as absent is not evidence it is absent | Rejected **636,021 TRUE dates** to block 40,132 false ones |
| R253 | The whole unscheduled backlog was one shape | Attribute a backlog set once, early; the aggregate can be a different KIND of thing from its members | 97 sources / 1,366,990 series carried for cycles as a queue of fetchers to write. Slugged display labels in a key mean the ids were minted by an intermediary |
| R254 | Replaced a crashing dedup with a silent one | NULL is not a value: in Arrow a null MASK silently drops rows | A guard firing for a reason it was not designed for is a NEAR MISS. Upstream 185, R2 185, fetcher 185 distinct identities — the loss was ours |
| R265 | A served source publishing observations dated 9998 | Freshness checks are not correctness checks; bound a shape heuristic's VALUE, and authoritative evidence outranks a heuristic regardless of position | 434,408 rows (0.887% of 48,960,271) beyond year 2100; 272,445 in Census 2016 at 9998-12-31. A fabricated FUTURE date makes a source look maximally fresh |
| R266 | In an append-only store, bad data does not mean the producer is still producing it | Run the CURRENT code against the shape that failed | `resolve_time_dim` had carried sane bounds since 2026-07-21. File mtime records the last MERGE, not when a row appeared, so a never-shrink store has no timestamp that answers the question |
| R269 | CORRECTS R266 — a synthetic test built from my hypothesis can only confirm it | Get the shape from the PUBLISHER, not from your reconstruction | A live re-parse of hagstofa's UMH11130.px gave 120 bad rows of 168; its sentinels (3001–3004) sit ON the time axis. `statfin` and `cbs_nl` remain UNVERIFIED — unchecked, not correct |
| R271 | A blocked chore was the root cause, filed as hygiene | For any "X not found", name the STORE that was searched | The R2 coherence catalog held **4,605,291** series vs the local **10,863,548** (57.6% missing). `imf_gfssoo_direct` merged 5,557,444 rows and had them thrown away |
| R275 | One `source_id`, two products | Before believing a status, confirm the id means ONE thing | `sec_edgar` names both the SERVED XBRL set (17,276 series, parity-proven AAPL 25,135==25,135) and the UNSERVED 13f/insider giant. Both instruments were wrong, in opposite directions, from one cause |
| R278 | A date tail extends the series the store HAS; it does not mint new ones | Merge only keys already in the store, and REPORT the skipped count | `intltrade/exports/hs` matched 44,997/44,997 — perfect. `imports/naics` had 757 of 68,961 in store; merging would have taken 1,514 series to ~69,000 "as an update" |
| R280 | "The keys map correctly" and "the key is a KEY" are different claims | Before tailing incrementally, assert `distinct(dedup_keys) == rows` | 11 of 24 enabled census flows were under-keyed: `exports/statehs` held 3,356,888 rows under **4,400** distinct pairs |
| R289 | One `source_id` naming two products makes "is it scheduled?" unanswerable | The cheap probe (an authenticated GET) beats the slowest correct instrument | `clean_full/sec_edgar/` is empty; the serving layer reads `clean_grouped/`, which holds 17,296 objects against 17,276 catalogued series. The entry also corrects its own first draft for overstating a tool's failure |
| R298 | The predicted escalation arrived, and the obvious fix would have been quieter | Two individually correct fixes can compose into a data gap | Adding an inner deadline check alone would break out with the SUBJECT bookmark already advanced (written BEFORE the subject is worked, per R273), so the unfinished tail is never fetched — and it would report `partial` with a true, reassuring reason forever |

---

### 13. Group I — Destructive operations, licence and compliance

**Count: 20.** Deletes, purges, gating and publishing rights. The asymmetry Ahmed himself stated
(R34) governs the whole group: *deleting re-crawlable data costs a re-crawl; hosting without
permission is legal exposure.*

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R8 | Provider-REFUSED data gets a FULL purge, not a gate | Refused is not gated; refused = gone, upstream and downstream | The WTO gate had a phantom-id bug that served refused data, and the approved purge stalled un-executed |
| R10 | An empty ground-truth parse plus a destructive fallback | ABORT any destructive step whose ground-truth parse returns an empty/absurd sentinel; PRINT the delete-set BEFORE deleting | Deleted 121 sources |
| R21 | Un-gating has three parts | Remove from `denylist.ts` (deploy the worker) + set D1 `source.license_id` to a reservable licence + regenerate the site; verify 451 → 401 live | |
| R24 | Verify the DATA-SERVICE terms verbatim at source | A catalog "NEEDS-REVIEW" means un-reviewed, not restricted | UNdata's terms, not un.org's website terms |
| R29 | No metadata-only listings, EVER | Host it fully or do not list it; give a source its OWN licence row if a shared one is blocked | Ahmed: "no meta data, if i cant host it dont even mention it" |
| R32 | Verify a destructive proposal at the grain the action operates on | Deleting SERIES requires series-level duplication evidence, never "we have that provider" | 7 of 21 dbnomics series were UNIQUE; the plan would also have silently un-gated a live source. The same review found a REAL live leak: `SERIES_CARVEOUTS` keyed on `worldbank` only, so `worldbank_wdi:SL.UEM.TOTL.ZS` served 401 |
| R34 | Gating is not compliance — delete | Refusal, silence > 2 weeks, and never-assessed all mean DELETE; never re-escalate a call the user has answered | Deleted all 15 gated sources (14,469 objects, 0 errors). A narrow sweep had missed `polity` holding **5,672 derived, servable CSVs** |
| R86 | A licence recorded too permissively is worse than none at all | Check licence METADATA against the licence's actual terms; never copy a row between sources | yale_epi carried `commercial_ok=1, attribution_required=0, no_modify=1` for a CC BY-NC-SA source — every term inverted, with an unrelated organisation's URL |
| R113 | A `reservable=1` flag is not a licence clearance | Require BOTH the flag and an entry in the verbatim audit | Proposed hosting 8 sources (581M obs); five had NO entry — 495M observations would have been published on unverified flags |
| R117 | The verbatim audit is the authority — not `catalog.db`, not D1, and never a diff between them | A diff finds disagreement, never shared error | Repairing seven FAO sources relabelled **211,924** series as commercially usable; local said cc-by-4.0, the audit says non-commercial |
| R120 | Text about a restriction contains the words of that restriction | Classify on machine-readable verdicts, never English phrases inside quoted terms | Two checker versions accused sources using text that explicitly REFUTED them. Six fixtures caught what four rounds of eyeballing did not |
| R128 | A migration you decide not to run must be deleted with the decision | "I chose not to run this" exists only in your head | 85 abandoned statements left in `data/` beside four sibling files that HAD been applied; running it would have relicensed 299,583 series against the audit |
| R136 | A licence with three conditions is not satisfied by meeting one | Split a quote into its numbered obligations and verify each against the SERVED response | Etalab 2.0 requires source, the date of last update when known, and unaltered meaning. Shipped with `last_updated: null` on all 139 flows — INSEE hands the date over freely at `/melodi/catalog/{FLOW}` |
| R150 | Check the licence BEFORE a tool that auto-catalogues | A repair tool is a publishing tool | `make_servable` on `owid` would have catalogued **1,048,968 series over 72,514,320 rows** — and owid's verbatim verdict is DISPUTED / NEEDS HUMAN REVIEW |
| R194 | Put 1,143,250 series live in breach of a licence condition just verified | "The licence permits X" and "we satisfy the licence's conditions for X" are different claims | Fetch a real body and READ the citation header; never assert a condition met via substring search over a blob containing the source id |
| R215b | One command from publishing data under a citation never checked | Read where the data ACTUALLY came from — the ingest's URL list and its log | Fourteen unfamiliar country codes in a dry run were the only visible symptom of a wrong provenance chain |
| R226b | Served a source the owner had explicitly retired | Before serving a dark source, ask WHY it is dark; check the git log for the source id | A retired source leaves fingerprints: a fetcher whose ingest is gone, a frozen store mtime, a sibling with the same publisher and more rows. "May we host this?" and "should this exist?" are different questions |
| R227b | Labelled a gap "stale rows", then found they all serve | Report the MEASUREMENT and the interpretation separately; FETCH a thing before deleting it as orphaned | "D1 has 61 rows the catalogue does not" is a fact; "stale" is a hypothesis. A gap has a direction |
| R250 | I cannot open my own permission gate | Two attempts at most, then hand the blocked command to the user, FINISHED | Four denials on 2026-08-02. NEVER edit the permission file to unblock your own refused action. (Annotated 2026-08-27: the same command later ran without a denial — R250 is a dated measurement, not a standing fact) |
| R263 | "Delete the stale ones" was "delete all of them" | Guard the POST-STATE of a destructive op, not just the selection | The keep-set derived from the source's own sidecar was EMPTY, so the post-condition would have PASSED. The real fix was ORDERING: let the source run once so current cursors exist |

*(R33 — "compute and verify before writing", which left the registry half-pruned and the whole
updater refusing to run — is closely related to this group but is filed under Group D, where the
failure first broke: a guard that fired after the write instead of before it.)*

---

### 14. Group J — Concurrency, scheduling, budgets and resources

**Count: 23.**

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R5 | The updater state is single-writer (ETag compare-and-swap) | Never run local jobs concurrently with CI; skip the 05:40–06:45Z cron window | |
| R12 | Re-enabling a scheduled workflow AFTER its cron window silently skips that day | Dispatch the missed days manually as SEQUENTIAL SINGLE-DAY runs | Multi-day catch-up in one job exceeds GitHub's 6-hour ceiling and is killed as "cancelled" |
| R39 | Never run a bare local `python -m updater.run` | Local = always `--source X`; fleet proving = CI `workflow_dispatch` | It walks the full fleet with no memory ceiling, thrashes to tens of GB, never exits, and holds the SQLite lease lock so every later local run dies `database is locked` |
| R40 | A fetcher firing thousands of serial upstream requests | Parallelize from the START at the ingester's PROVEN worker count, each thread its own `requests.Session`; merge SERIALLY | boe's ~613 IADB CSV batches ballooned past 1h/run and stalled the 4 fast sources queued behind it |
| R40b | R40's "parallelize" has a CEILING — the server's rate limit | Take the ingester's proven level, else start at 2 and raise on evidence; 429s mean lower concurrency, never more retries | ons_uk at 5 workers drew **41 HTTP 429s in 4 minutes** (run 30133384687) |
| R45 | Applied a scale analysis to the fetchers I declined and not to the ones I wrote | Measure `blob.read_table(p).nbytes`; a MERGE needs ~3x; never batch memory-heavy sources | Run 30143118275 died with **exit 143 = SIGTERM = OOM-killed**, taking 3 healthy fetchers with it. Isolate anything over ~10M rows |
| R46 | Built and discarded three wrong theories instead of reading the provider's docs | One web search before any theorising: `<provider> API rate limit` / `<provider> bots` | ONS publishes a MANDATORY User-Agent format that explicitly forbids emails (ours embedded one) and a documented 1-hour block. Ahmed had to say "search" twice |
| R53 | A watchdog must be able to KILL and must run CONCURRENTLY | A timeout evaluated after draining a child's pipe never fires | Do not pipe a job you intend to watch through `tail` |
| R63 | Put the deadline where the hang happens, and state its true bound | Evaluate a deadline on every attempt; the honest bound includes the uncancellable in-flight request | The per-flow budget was checked once on entry, before the retry loop: real bound 40 min, not the 15 committed |
| R72 | A budget bounds the failure mode it measures and nothing else | Name which failure mode a guard covers and which it leaves open | A wall-clock budget added after a 300-minute timeout; the next batch died at 49 minutes of MEMORY |
| R85 | A function call in a comprehension's condition runs every iteration | Hoist any call whose result does not vary with the loop variable | `[i for i in ids if i not in r2_csvs(client, src)]` → **40,016 full R2 listings, 14 minutes, zero objects written**, every health signal normal. It had passed on a 574-id source minutes earlier |
| R88 | A per-item lookup against a multi-file dataset costs the whole dataset per item | Consolidate and SORT on the lookup key so row groups prune; verify the fast path is byte-identical | Deriving 2,465,197 WID series ran at 6.8/s = **101 hours**, because the store is 119 parquets and each read scanned ~437 MB. The same code did 60.4/s on a one-file source. Fixed: 11.6x, 101h → 8.6h |
| R90 | A kill is a request; only a listing is an outcome | Re-list processes and confirm the PID is gone before relaunching | Three kills silently failed in one session; the last left TWO copies of a 2.4M-object derive running for 23 minutes (79.2/s → 89.1/s the moment the stale one died) |
| R132 | The publisher's rate limit is already in the module you are importing | Find the pacing constant in the code that already talks to the host | `jobs/ingest_ons_uk.py` sets `RATE = 0.7` and quotes ONS's policy three lines above. Result: 328/337 HTML error pages, then 207 consecutive 429s. **This was a repeat of R40b, for the same host** |
| R169 | Reached for more threads when the algorithm was the problem | Measure WHERE it is slow before adding concurrency | 16 workers moved under 5,000 objects in ten minutes with ZERO PUT retries: each call scanned the whole 93 MB / 69,666,545-row parquet. Then the fix's own `--verify` reintroduced the same quadratic cost |
| R210a | Re-did 23,814 uploads another live session had already committed | Establish ownership before working in a repo this session has not been editing: `git log --oneline -5` + file mtime | Noticed only because a line number moved. "The queue says do this" is not evidence nobody else is doing it |
| R228 | Two DuckDB jobs sharing one spill directory delete each other's temp file | A scratch path shared by processes needs the PID in it | Every derive and audit tool set `temp_directory` to `logs/_duckspill`. "It has worked so far" is a statement about timing, not safety |
| R238a | Changed a cadence without checking the budget of the job it runs in | Promoting a cadence is a change to the SHARED job; know where the state push sits relative to the work | If the state push is at the end, the timeout is a data-loss boundary |
| R243 | A hard timeout on an accumulate-then-merge fetcher DISCARDS | Ask where the WRITE happens relative to the loop; a fetcher must bound ITSELF below any external cap | worldbank_wdi called `merge_and_write` ONCE after the loop and was killed at 45 minutes: 227,000+ observations thrown away every run, forever. It had no `unit_state` row at all |
| R260 | The watchdog had no watchdog, and my liveness probe matched itself | Liveness is a HEARTBEAT the process emits, stamped after each completed tick | The 5-minute guard loop died 2026-08-02 15:16; three crawlers stayed dead for ten hours. The cloud gate deliberately does not judge `run_location: local` sources, so "not judged here" + "not judged anywhere else" = unjudged |
| R273 | The rotation bookmark is written at the END, so the sources that need it never write it | Write survival state incrementally or in a `finally`; verify persistence by the ARTEFACT | Only 2 of 14 rotating sources had ever persisted `_rotation.json`. The four confirmed hard-killed at 45.0 min all lack bookmarks; the two that finish have them. Self-sustaining |
| R274 | An instrument only the unattended machine can read is not an instrument | Name the reader: WHO sees this, FROM WHERE, WHEN? | A heartbeat file correct, current and unread for 10 hours. Now published to R2 per tick, age read from the BODY, and ABSENT exits 1 as UNINSTRUMENTED |
| R286 | A cooperative budget bounds when you next look at the clock | State the HEADROOM (`cap - budget`) and require it to exceed the LONGEST single unit; check the TIGHTEST source | "16 of 16 protected" — stat_estonia was killed by the 45-min cap again, printing no budget message because `dl.spent()` is only checked between subjects |

---

### 15. Group K — Toolchain corruption

**Count: 18.** The work was correct and the tools between the author and the file destroyed it:
shells, heredocs, escapes, encodings, quoting. This group is the reason the ledger keeps insisting on
byte-level verification.

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R17 | `$(...)` strips ALL trailing newlines | Measure with a clean pipe (`curl … \| sha256sum`) or write to a file | `wc -c`/`sha256sum` on `$(curl …)` are off by ≥1 byte with a totally different hash. A hash "discrepancy" vs a plan constant is YOUR measurement until proven otherwise |
| R42 | A shell one-liner decided whether a push succeeded | Push, then verify independently: `git rev-list --count origin/main..HEAD` must be 0 | `git push … \| grep -qE "rejected" && (…) \|\| echo "pushed"` printed "pushed" on a REJECTED non-fast-forward push. FIVE commits stayed local while reported as live |
| R89 | A trailing `&` backgrounds the whole `&&` chain, including the `cd` | Any command whose meaning comes from the cwd must carry its own `cd`, and assert the resolved target | `cd repoA && nohup job &` then `gh workflow run` — the dispatch went to repoB. It failed only because the workflow did not exist there |
| R92 | An unquoted heredoc is a command-substitution context | Quote the delimiter (`<<'PY'`) whenever the body contains backticks, `$` or backslashes | Every backtick span in a generated markdown document was EXECUTED and replaced with an empty string. The ledger already carried a heredoc warning about ESCAPES — "write rules around the MECHANISM, not the symptom" |
| R126 | A tool that has only met one shape is not general — and non-ASCII output meets cp1252 | Check the LAYOUT of every input, put the smallest source first, set `PYTHONIOENCODING=utf-8` on every background job | `sync_parquet` assumed `<src>/<src>.parquet` and hit adb's 54 per-flow files with a bare NoSuchKey naming no source; then an em-dash crashed a nohup'd run |
| R131 | `run_in_background` PLUS `&` orphans the job | Use the harness flag OR a shell `&`, never both | The tracked command was the wrapper: it echoed one line, exited instantly, and the notification said "completed (exit code 0)" while the python child died with its parent |
| R135 | A `cd` earlier in the command retargets every relative path after it | Every ledger git command uses `git -C /d/research/hfdatalibrary`, never a bare `git add` after a `cd` | The entry went to the ECON repo, creating an untracked `MISTAKES.md` inside a PUBLIC repo, one `git add -A` from being published. **It recurred the same day**, hours after the rule was written |
| R139 | Generating a document through `bash -c` lets the shell eat its content | Write the generator to a file and run the file | `${r["source"]}` expanded to EMPTY and backticked text was executed: the file wrote successfully, all totals correct, and **every source name in both tables was blank** |
| R154 | Non-ASCII in an id: test it in Python, not through bash | A Windows cp1252 console mangles em-dashes and accented Latin-2 in transit | bash+curl returned `404 not_found` with the em-dash replaced by two spaces; `requests` inside Python returned **200 with 17 rows** |
| R181 | A `\b` became a literal BACKSPACE and the site nearly told visitors three live databases were not updating | When a computed value contradicts what you know to be true, look at the BYTES (`cat -A`) | The pattern was `\x08sec_edgar…\x08`. Read the code four times and it looked correct every time |
| R183 | The same escape bug twice in one session | Do not write regexes containing backslash escapes through a shell heredoc; use the Edit tool or a pattern with no escapes | `\n` inside `r"//[^\n]*"` became a REAL NEWLINE. This one failed LOUDLY (`SyntaxError`) — same cause as R181, opposite visibility |
| R196 | A UTF-8 `.ps1` with no BOM silently DELETED half my script under PowerShell 5.1 | Any `.ps1` this project writes must be pure ASCII, or saved with a UTF-8 BOM | 54 lines lost silently. Mojibake in a file you are debugging is evidence about the FILE, not the terminal |
| R203 | Generating a PowerShell file from a Python heredoc turned `\r` into a carriage return, twice | The escape characters of BOTH languages apply, and Windows paths are made of the escape character | Never hand-write a backslash path into generated code; parse the result with the target language's own parser |
| R223a | Wrote invisible bytes into source, then trusted three "success" messages over the file | NEVER write a literal control or format character into source; construct the backslash programmatically | A check reporting 7220 "control characters" in a file that had 9 was the predicate, not the file. The `SyntaxWarning` naming the exact escape was on screen for three passes |
| R226a | Broke a rule twice more within the hour of writing it down | When a mistake recurs, fix the DEFAULT rather than restating the rule | "A rule I have to remember at the moment of acting is a rule that will lose to momentum." For such regexes, do not use the Edit tool at all — build every backslash with `chr(92)` |
| R227a | A regex insert into a JS object literal produced `,,` and broke the stats page | Do not append to a structured literal by string surgery — PARSE it, add the key, serialise it back | Caught by parsing, not by reading: the defect was a comma at the start of a line already looked at. Run the check on EVERY file the script touched |
| R255 | Broke R131 while the rule sat in the digest read at the start of the cycle | Rules with a SYNTACTIC trigger (`&`, a glob in `git add`, `split(':')`) must be checked as the command is written | Reading the ledger at session start does not discharge it |
| R258 | A colon in prose broke the load for all 141 sources | Parse a machine-read file with its PRODUCTION loader in the same breath as the edit; use block scalars (`>-`) for prose | One documentation edit to `registry.yaml` made `registry.load()` raise `ScannerError`. The pytest suite passed throughout — nothing in it loads the registry |

---

### 16. Group L — Process, ledger hygiene, communication and cost

**Count: 17.** How the work is reported, escalated, recorded and paid for.

| ID | Title | Lesson | The memorable fact |
|---|---|---|---|
| R1 | Never push the local hfdatalibrary tree | It holds Ahmed's WIP and predates worktree-pushed commits; edit via a fresh worktree off `origin/main` | |
| R7 | Open the canonical doc before analysing a topic a memory note flags | Don't re-derive documented research from raw data | `REDISTRIBUTION_COMPLIANCE.md` + `REDISTRIBUTION_EMAIL_TRAIL.md` for anything licensing-related |
| R13 | "Approved" is not "verified safe" | Run a code-grounded adversarial review; treat a prior plan's factual claims as unverified | An approved SSO design plan carried latent critical flaws |
| R16 | A plan's hard gate is executable only with a named instrument | Exact command + a fallback for every recon outcome; commit per step, file-scoped restore, never whole-dir | |
| R37 | A standing RECURRING obligation must be AUTOMATED the moment you own it | A gap in a required daily record IS the failure, even when the system is healthy | The daily SSO soak instrument went dark for 4 days while buried in econ work |
| R43 | Stop-asking is the failure mode | If the next action is knowable from the plan and needs no decision only Ahmed can make, DO IT | Surface only for: a choice genuinely his, an irreversible outward-facing act, or an actually empty queue. "Want me to continue?" when a documented backlog exists is the exact error |
| R121 | Audit the ledger itself | Verify every rule cites an entry that exists and every entry cites a rule; annotate gaps rather than reconstructing incidents | Three rules (R41–R43) cited entries never written, one (R94) cited nothing. The first pass used `re.S` and manufactured phantom dangling citations — inside the check meant to validate this exact failure mode |
| R148 | Findings go in the ledger, not in new documents | A separate artifact is acceptable only when a COMMITTED script regenerates it | Three standalone docs created in one session. Ahmed: "you are notorious about forgetting about these documents… have everything centralized" |
| R158 | Reporting a source "shipped" without an updater is a half-delivery | DONE means catalogued + served + VERIFIED + on a schedule; report the fraction scheduled | Ahmed had to say it three times. Five sources shipped and verified end-to-end were left with no scheduled refresh |
| R170 | "103 sources left" implied 103 fetchers; 60 had no upstream to fetch from | Before calling something "remaining work", check that the work is the kind you think it is | 38 UNCTAD sources behind a 2023-06-30 index, 18 FAO behind 2024-05-09, 4 UNESCO behind 2022-04-04. "No fetcher" and "no upstream" look identical in a coverage table and are months apart |
| R205b | Stopped to report four times in one session against a standing order | A finding is not a reason to stop; carry results forward to the next real milestone | The reserved list is short: deleting non-re-crawlable data, un-gating a DISPUTED licence, auth/billing, sending email as Ahmed |
| R215a | Asked for one thing, delivered fourteen other things and not that one | The named deliverable is the definition of done; a detour must be deliberate and declared | When a blocked task's blocker is removed, re-check the blocked task immediately |
| R219a | Three defaults were all "correct" and all wrong | "Is it correct?" and "who pays for it?" are different questions | For any default, name the population it costs something and estimate its size — and name at least one group no dashboard counts, because that group cannot object |
| R230 | Inherited a false premise from a compaction summary and acted on it for hours | A compaction summary is a SUMMARY, not evidence; its provenance claims are hypotheses | `git log --format='%an %ad'` costs one command. When the user contradicts your account of the session's own history, they are almost certainly right |
| R247 | A concurrent write deleted eighteen ledger entries, including the one warning about concurrent writes | An append-only doc is APPENDED to at an anchor; verify the COUNT it holds afterwards | Two clobbers on 2026-08-02 (18 then 17 entries). **48 insertions / 883 deletions is a replacement, not an edit.** Repair by reconstructing the union from the pre-image |
| R251 | A standing instruction never written down, and therefore broken five times | Write a standing instruction down the moment it is given, into the file that loads itself — and put it where the CONTRADICTING EVIDENCE lives | db.nomics.world is BANNED. Now §0 of `econfindatalibrary/CLAUDE.md`. "Convenience is the tell": an approach attractive because it collapses a lot of work may have been ruled out for exactly that reason |
| R268 | Closed a two-part task on the part I had just been looking at | Re-read a task's own text and account for every claim separately; say which part is done | Task #65 said "912,990 series are DARK … AND the fetcher reads the wrong store". The second was fixed and the headline untouched — bea still served 240 series over a 913,230-series store |

---

### 17. The 22 id collisions — a map

Two sessions worked in this repository at the same time (one on the hf website and account system,
one on the econ updater) and both appended to the ledger. They allocated numbers independently, so
22 ids name two entirely unrelated incidents. R247 is the entry about the same concurrency deleting
entries outright; the collisions are the surviving, quieter half of that problem.

Reading rule: **the `## R###` headings are almost all the hf-site session; the `### R### — title`
headings are almost all the econ-updater session.** Where both exist, they are different incidents.

| id | `a` (earlier line) | `b` (later line) |
|---|---|---|
| R205 | L2 3209 — shipped a security fix, commit said it closed the hole | L2 3244 — stopped to report four times against a standing order |
| R206 | L 3271 — probe read a different repository | L 3304 — a helper returning `{}` on failure |
| R207 | L 3335 — verified a fix was PRESENT, never that it could RUN | L 3372 — fixed an Arrow crash in one file, left it in seven |
| R208 | L 3406 — three rejections for fixing the instance, not the class | L 3448 — stamped a cadence success on a crashed run |
| R209 | L 3477 — key-sharing detector dropped IPv6 | L 3525 — a structurally clean store still wrong for the reader |
| R210 | L 3557 — re-did 23,814 uploads another session had committed | L 3590 — batch loop grepped for success, hid four failures |
| R215 | L 3731 — delivered fourteen things and not the one asked for | L 3872 — one command from publishing under an unchecked citation |
| R216 | L 3763 — tested the renderer, panel said "0 dl" | L 3988 — every background job reported success |
| R219 | L 3900 — three defaults all "correct" and all wrong | L 4014 — "errors 0, skipped 0" beside a REFUSED list |
| R220 | L 3941 — four first-pass checks, four wrong answers | L 4057 — invented two ids, got 404 |
| R221 | L 4085 — a rename broke a second lookup | L 4190 — "raw key as title" wrote 694,300 rows with no title |
| R222 | L 4116 — announced an outage that existed only in staging | L 4269 — mixed pass/fail is a rollout signal |
| R223 | L 4148 — wrote invisible bytes into source | L 4297 — read an OverflowError as a hard ceiling |
| R224 | L 4227 — took `/v1/public-stats` down on every family site | L 4431 — a verifier says SERVED without asking the server |
| R225 | L 4340 — server rule changed, browser enforced the old one | L 4462 — implemented one clause of a three-part definition |
| R226 | L 4372 — broke a rule twice within the hour of writing it | L 4498 — served a source the owner had retired |
| R227 | L 4398 — a regex insert produced `,,` | L 4542 — labelled a gap "stale rows"; they all served |
| R232 | L 4779 — wired in a monitor that examined nothing | L 5660 — a generator held a hard-coded stale copy |
| R233 | L 4823 — a fix planted a false red | L 5856 — grepped the user's exact string, got 0, fixed a different bug |
| R234 | L 4859 — fixed two of three bypass paths | L 6337 — batch loop printed "DONE" while an item crashed |
| R235 | L 4899 — wrote "it works without a key" into the registry | L 6707 — a guard rejected 636,021 TRUE dates |
| R238 | L 5007 — changed a cadence without checking the job's budget | L 10343 — told Ahmed a live feature did not exist |

`R200` also carries two headings, but the second is explicitly labelled `### R200 addendum` and
belongs to the same incident; it is not a collision.

---

### 18. Known defects in the ledger's own bookkeeping (within R0–R300)

Recorded here because they matter to anyone using the file as a reference, and because the ledger
itself records most of them.

1. **Four rules cite evidence that does not exist.** The annotations are in the file:
   - `R41 … [M-20260724-05 — ENTRY NEVER WRITTEN; the rule text above is the only record]`
   - `R42 … [M-20260724-06 — ENTRY NEVER WRITTEN; …]`
   - `R43 … [M-20260724-07 — ENTRY NEVER WRITTEN; …]`
   - `R40b … [M-20260724-08 — ENTRY BODY MISSING: only M-20260724-01..04 were ever written.]`

   R121 is the entry recording the discovery. The chosen remedy is worth noting: the citations were
   *marked* rather than silently dropped, and the rules were kept because they stand on their own
   measured evidence (for R40b: run 30133384687, 41 HTTP 429s in 4 minutes).

2. **114 of the 301 ids have no digest line.** Specifically R25–R35, R161–R244, R252–R255 and
   R287–R300. Since the digest is the declared read path — "read THIS DIGEST, the 8,600 lines below
   it are the archive" — those rules are, by the file's own design, not being read after a
   compaction. This is the identical failure the digest header describes for R312–R327.

3. **R247's damage is partly permanent.** Two concurrent rewrites deleted 18 and then 17 entries on
   2026-08-02. The repair reconstructed the union from git's pre-image, but the incident is the
   direct explanation for the numbering collisions in section 17.

4. **R250 carries a dated correction inside the digest**, added 2026-08-27 under an adversarial
   review (AR-016): the classifier block it records was a measurement taken on 2026-08-02, not a
   standing fact, and the command later ran cleanly. This is the ledger doing the thing it asks of
   everything else — treating its own claims as dated measurements.

---

### 19. If you read only one page of this

Across 325 entries, four sentences carry most of the weight:

1. **"0 defects in 0 files examined" is not a result.** (R0 sub-rule 3, and Groups B and C entire —
   95 of the 325 entries are a measurement or a probe that could not have found what it was
   looking for.)
2. **A green run is not a proof; require positive evidence of work.** (R50, and Group D — 48
   entries.)
3. **"Deployed" is a state of the running system, not of the repository.** (R0 sub-rule 12, and
   Group E — 29 entries.)
4. **A reported example is one instance of a class; sweep the whole surface and prove it with a
   zero-result check.** (Ahmed's own standing rule, Group F — and R256's refinement: the zero-result
   check is not confirmation, it is *what defines the work*.)

The system's own answer to all four is structural rather than exhortatory, and it is stated in the
project's `CLAUDE.md`: *"Prose rules did not hold; a second agent and a runnable check are the
difference."* That is why the adversarial-review skill and
`.claude/skills/adversarial-review/tools/ledger_check.py` exist, and why R202 — *"the record is not
the deliverable; the check is"* — is arguably the most load-bearing entry in this range.
## 7. The mistake ledger, part 2 (R301 onward)

**Scope: entries R301 through R519 of `D:/research/hfdatalibrary/.claude/MISTAKES.md`.**

Part 1 of this document covered the early ledger. This part covers everything from R301 to the
highest id currently in the file, R519 (written 2026-08-30). It is a complete catalogue: every id
in the range appears exactly once, in exactly one thematic group, with its lesson and the concrete
number or fact that makes it memorable.

---

### 1. What this file is, and how to read it

#### 1.1 Vocabulary for a reader who does not know this codebase

| Term | What it means here |
|---|---|
| **The ledger** | `D:/research/hfdatalibrary/.claude/MISTAKES.md` — an append-only record of every mistake Claude has made on this project, one numbered entry each. 14,010 lines. |
| **Rules Digest** | The first ~500 lines of that file. One compressed line per entry. The digest is the *read path*; the archive below it is the *write path*. |
| **Archive entry** | The full write-up of one incident, further down the file, headed `## R###` or `### R###`. |
| **econ repo** | `E:/research/econfindatalibrary` — the econdatalibrary.com website, its data updater, and its catalogue. Most incidents happen here. |
| **hf repo** | `D:/research/hfdatalibrary` — hfdatalibrary.com, and the home of the ledger and the skills. |
| **catalog.db** | The local SQLite catalogue of every series the library holds. 11.91 GB, `series` + `series_fts` tables, 13,486,342 rows each. |
| **D1** | Cloudflare's hosted SQLite. The copy of the catalogue that actually answers user requests. Reads are metered and billed. |
| **R2** | Cloudflare's object store. Holds the parquet data files and the per-series CSVs users download. |
| **the worker** | The Cloudflare Worker at `api/worker/` that serves `/v1/...`. `SUPPORTED_SOURCES` in `api/worker/src/util.ts` is the list of source ids it will answer for. |
| **series_fts** | An SQLite FTS5 full-text index over series titles. Declared `fts5(series_id UNINDEXED, title, geography)` — the `UNINDEXED` word causes two separate incidents below. |
| **grain** | The level at which a thing is keyed. A source can be catalogued at *series* grain (one row per series) or *table* grain (one row per table, serving many series). Getting this wrong is a recurring, expensive class. |
| **the digest line** | The one-line summary a new entry is supposed to add to the Rules Digest in the same commit. Forgetting it is itself a logged mistake, twice (R328, R485). |
| **AR-nnn** | An adversarial-review round. A subagent is briefed on a plan and told to find the flaw rather than approve it. Many R490+ entries were written *by* the reviewer. |

#### 1.2 The physical anatomy of the file, measured

These are structural facts about the file as it stands, not editorial observations. Each is
reproducible from the commands shown.

```bash
wc -l D:/research/hfdatalibrary/.claude/MISTAKES.md
# 14010

grep -n -E '^#{2,3} R[0-9]+' D:/research/hfdatalibrary/.claude/MISTAKES.md
grep -n -E '^- R[0-9]+[a-z]?\.'  D:/research/hfdatalibrary/.claude/MISTAKES.md

python .claude/skills/adversarial-review/tools/ledger_check.py --digest
#   ledger: 111 headings, 335 digest lines
#   PASS  every entry from R475 onward has a digest line (58 older ones are the known pre-rule backlog)
#   RESULT: all checks passed
```

**Three different storage formats coexist in the range R301–R519, and it matters when you go
looking for something:**

| Range | Where the entry lives | Heading form |
|---|---|---|
| **R301 – R367** | Full archive entry only. **No digest line.** | `### R###` (three hashes), lines 7757–10341 |
| **R368 – R434** | **Digest line only. There is no archive entry.** The digest line *is* the whole record, and several of them run to 3,000+ words on a single physical line. | `- R###.` in the digest, lines 198–267 |
| **R435 – R519** | Full archive entry. Digest line for R467 onward only. | `## R###` (two hashes), lines 10388–14010 |

Consequences a reader should know:

* **No id in R301–R519 is missing from the file.** Verified by counting occurrences of every
  number in the range; all 219 appear. But 67 of them (R368–R434) exist *only* as a digest line,
  and 67 more (R301–R367) exist *only* as an archive entry with no digest line. Neither half is
  fully indexed.
* **`ledger_check.py --digest` enforces digest coverage only from R475 onward** and explicitly
  tolerates "58 older ones … the known pre-rule backlog". So the gap is known and accepted, not an
  undetected drift. The 58 are R435–R466 plus the R301–R367 block minus those already carried.
* **Four ids in this range are used twice, for two entirely different incidents each:**
  R308, R309, R310, R311. There are two archive entries with each of those headings, ~140 lines
  apart. They are catalogued separately below as `R308(a)` / `R308(b)` and so on. This is a real
  numbering collision, not a duplicate write.
* **R350 and R351 were renumbered on the way in.** Their headings say so: *"(renumbered from a
  stale sibling header '## R236'; written by a concurrent session)"* and *"(renumbered from a stale
  '## R237')"*. R352 records why — a concurrent session rewrote the file from a stale buffer and
  deleted 118 entries.
* **R506 records a second numbering collision, live.** *"a second session claimed R505 between my
  read of the ledger and my write, so this entry was renumbered to R506 on the way in … that
  session's R505 also carries `[M-20260829-03]`, which my R504 already owns, so the incident tags
  are ambiguous for 2026-08-29."*
* **One lettered sub-entry exists: `R473b`,** a digest line at line 469 with no archive entry.

#### 1.3 Counting rows, not ids

Because of the four doubled ids and the one lettered entry, the catalogue below has **224 rows**
covering **219 ids**:

```
219 ids (R301..R519)
+ 4  second entries for the doubled ids R308, R309, R310, R311
+ 1  R473b
= 224 catalogue rows
```

Every one of those 224 rows appears in exactly one of the sixteen groups.

---

### 2. The sixteen groups, and their counts

The groups are derived from the entries themselves, not imposed. Where an entry could sit in two
groups I placed it by its *stated* lesson — the sentence the entry itself calls "the rule".

| # | Group | Rows |
|---|---|---:|
| G1 | Broken readers: a bug in our code reported as a fact about the data or the publisher | 27 |
| G2 | Probes and comparisons that could not have come out any other way | 16 |
| G3 | Guards, gates and tests that could not fail, or failed open | 22 |
| G4 | Class and scope: the instance measured, the population assumed | 16 |
| G5 | Causal stories published before the refuting test | 18 |
| G6 | Data destruction and near-misses | 14 |
| G7 | Grain: the level at which a thing is keyed, catalogued and served | 6 |
| G8 | Cost, capacity and the bill | 10 |
| G9 | Concurrency, locks, schedulers, and jobs that look alive | 17 |
| G10 | "Live", "served", "deployed": the running system claimed from an artifact | 18 |
| G11 | Licences, provenance, and claims that leave the building | 16 |
| G12 | Authority: the owner's decisions and the premises I gave him | 10 |
| G13 | Shell, git, and the tools I type with | 13 |
| G14 | CI, deploys and the release path | 9 |
| G15 | The ledger and the reliability system itself | 7 |
| G16 | Time, duration and progress reported as fact | 5 |
| | **Total** | **224** |

---

### G1 — Broken readers: a bug in our code reported as a fact about the data or the publisher (27)

The single largest class. Every entry has the same shape: a parser, path, regex, JSON key, prefix
or store selector is wrong; the tool therefore sees nothing; and the tool's output announces that
*nothing is there*, in the vocabulary of a finding. R484 states the distinguishing test in one
line — *fetch one record from the publisher and look at the raw bytes*.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R302 | The docstring warned, then I built what it warned against | An instrument that summarises must be judged on the cases where its summary is wrong, not where it is right | `audit_stale_errors.py`'s shared-file list made **112 of 140** state rows read "superseded", nearly all citing one commit of mine |
| R304 | The instrument showed one commit, turning a stale row into a false lead | `git log -1` is not the explanatory commit; list every commit since the attempt | wid's real explanation, commit `691e6126`, was **two commits older** than the one the tool printed |
| R314 | Re-keyed a store from a docstring; verified IDs while the bug was in the DATES | A re-key check comparing identifiers verifies only the half you changed on purpose; compare (key, date) pairs | Key sets matched perfectly while **all 31,992 dates** for regional-gdp-by-quarter were wrong; ons_uk's two grains summed to **20,198,302** rows, exactly the recorded `obs_count` |
| R315 | Built a 337-request census on a URL I had already watched return 404 | Read a sweep's failure count, not just its results | **337 of 337** requests 404'd and the script printed a tidy header; the rerun was rate-limited on **192 of 337** |
| R316 | Asserted a source was missing from a list I had parsed wrong | When a probe reports ABSENCE, run it against something known PRESENT | The field is `source`, not `id`; every element yielded `None`, so **every** source would have read as missing |
| R326 | `obs_count` means two things; a healthy source read as losing 168M rows | A metric whose denominator silently changes is worse than a missing one | ecb appeared to drop from 218,396,836 to 49,851,636 obs; the store held **218,396,859 rows — 23 MORE** |
| R330 | The entire re-pull toolchain pointed at a dead drive letter and reported "0 corrupt" | An audit must report its DENOMINATOR; "0 defects in 0 files" is not a result | Three tools hardcoded `D:/research/econfindatalibrary`, which does not exist; **50 more** such constants survive in the repo |
| R335 | `--r2` did not select R2, so the check confirming my repair read the wrong store | A flag naming a store must SET that store, and print what it resolved | The audit read a local file last written **2026-07-14** and printed `(r2)` above it, for an object already deleted from R2 |
| R349 | The runbook told 215 sources' pages they were not served | Copy the parser a sibling tool already got right; never write a third regex | A one-id-per-line regex found **11 of 226** served sources; **215 pages** carried a false "NOT SERVED" verdict |
| R365 | Diagnosed a ksh resurrection from a CUMULATIVE resume file | A resume file's totals describe its history, not this run | `2 sources KEPT (60,192 series)` = 25,057 (old incident) + 35,135 (mine); `SELECT COUNT(*) WHERE source_id='ksh'` returned **0** |
| R366 | Declared a backfill complete by measuring the pre-purge local relic | For a cloud-backend source every completeness measurement runs against R2 | Local relic: 2,918,435 rows / **715 codes**. R2, the store the fetcher writes: 2,069,765 rows / **396 codes** |
| R373 | Keyed a PERMANENT retirement on an API error MESSAGE id | Never let a formatted error string carry a permanent verdict when structured data exists | With `source=75` in the URL the same deleted indicator returns id **120**, not 175 — and 120 is also what a typo returns |
| R374 | Measured coverage by globbing the local disk, for the third time in one day | For any coverage question on a cloud-backend source, run `tools/store_inventory.py` | Local: **84** parquets. R2: **139**. Published "we hold 79 of 145, 64 missing"; the real gap was **9** |
| R389 | Keyed a comparison by basename; two findings were entirely my own key | If you key a comparison by name, assert the names are unique in that namespace first | Reported "eia: 30 files AHEAD" and "bea: 588 R2-only"; re-keyed on relative path, bea **592/592 SAME**, eia **60/60 SAME** |
| R392 | The detector was structurally blind to its second victim | If a corruption hits two copies through one code path, comparing those copies proves nothing; find a witness written by a different path | XPRO's store and mirror both held **4 facts** where the catalogue's `n_obs` recorded **19,399**; the third witness found it |
| R431 | "statcan derive COMPLETE — 8,207/8,207" from a marker file the INGEST writes | A completion claim needs the writer's own evidence, never a sidecar count | The derive's own log read **[7964/8207]**, with the giant census tables still running at ~44 min each |
| R433 | Two hand-rolled probes produced false counts in one session | Any ad-hoc counting script prints ONE raw sample record beside its count | "28 queued insee ids ABSENT" (they were present with NULL `end_date`); "fdic NOT listed (317 sources)" (the key is `source`, not `id`) |
| R436 | `INSERT OR IGNORE` plus wrong-column verification | On conflict it silently ignores YOUR values; verify the column the CONSUMER reads | Rows already existed with `redirect_exact` NULL and `created_at` July; my check read `status`, saw "active", and I reported SSO live |
| R441 | Measured a CSS fix through a swapped `<link href>`, which renders stale | Never verify computed CSS through an href swap; reload the document | Reported padding **7.2px** where 4.8px was correct; a full reload gave 4.8px and the overflow fell from 85px to 9px on its own |
| R462 | Counted `series/ilo` and got ilostat's objects | An object-store prefix has no word boundaries; bake the terminator in (`series/ilo:`) | Prefix returned **3,305** objects for a source with 1,157 catalogue rows; the true count was **0** |
| R474 | The "unreadable titles" metric excluded the character most codes contain | Build a character-class metric from the data's actual alphabet | Reported **1,378 (0.011%)**; the honest count was **169,722 (1.34%)** — 123x — because the regex had no `:` and FAO titles read `FAO_FO:5510.1.1600` |
| R478 | Read the live API with the wrong JSON key, twice, and twice called a deployment failed | When a lookup misses for EVERY key, print one raw record before investigating anything | Four investigative steps chasing my own reader; a plain `grep -c cbs_nl` on the 146,311-byte body returned **1** |
| R483 | A regex that stopped at the first `>` made INSEE look like it does not name 61 of its own series | Never scan XML elements with `[^>]*`; attribute values may legally contain `>` | Reported "titled 0 of 61"; INSEE names **59 of 61**. The offending attribute: `TITLE_FR="… triples : >38% …"` |
| R484 | FIVE tools in one session said "the publisher does not provide this"; all five were reader bugs | "The publisher does not provide X" is a claim about a REMOTE system made by LOCAL code | Five independent tools, four phrasing the output as a property of the source; **0 of 61**, **0 of 13**, **0 raw**, all false |
| R510 | The probe behind "publisher discontinued this" cannot establish retirement | Absence from a catalogue is absence from a catalogue; REQUEST THE DATA | `ReadCollection` lists 12,985 datasets and omits **495** of our cso matrices; `ReadDataset` returned data for **8 of 8** probed, two updated in 2024 and 2025 |
| R518 | Told Ahmed bea had 11.2 million bad values; it has 49,856 | A grouping is an assumption about identity and must be tested like any other claim | Overstated by **~440x**. **823,537** bea keys legitimately live in more than one file; **11,200,190 of 11,212,677** "conflicts" (99.9%) were cross-file |
| R519 | Read a log field as a measurement it cannot make | A derived log field is not an instrument; ask what computed it | `refusing shrink 648241->362203` — the second number is `|dedup(existing ∪ fresh)|`, identical for a complete pull and a truncated one. The fresh pull really had **648,241** rows |

---

### G2 — Probes and comparisons that could not have come out any other way (16)

Distinct from G1: here the reader is fine, but the *test* was constructed so that only one answer
was reachable. The remedy in almost every entry is the same and costs one extra call — put a case
that MUST come back the other way into the same probe.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R308(a) | My probe PASSED, on output that was not the thing being probed | Read the bytes the probe produced, and make the assertion tight enough that a wrong path cannot satisfy it | Reported "after ~4s, log has 7 lines" from a job printing **six**. The seven lines were a `SyntaxError` |
| R325 | A subagent's number did not reproduce, and shipping anyway was tempting | Reproduce a second-hand number before acting; it arrives formatted and confident | Agent said "99.0% discontinued"; measured **87.9%**, with **7 tables still publishing** in a subject named "discontinued tables" |
| R329 | "Corrected" a correct number twice, with three probes all failing toward reassurance | A failed control VOIDS the run; it is not a caveat to publish alongside the result | Three probes said 0, 0 and 3 of 7 sources served. All seven are served: **637,178 rows**. The original figure was right the whole time |
| R334 | Applied the exact test my own note said could not work, and every instrument agreed | A range test cannot detect code-as-year fabrication, ever; detect it by structure | The band pass kept scb's municipality codes **1500..2200** as plausible years; **15,990** fabricated rows survived, certified clean by two green instruments |
| R337 | Overturned a correct finding with a number answering a different question | Before overturning a measurement, reproduce THEIR question — same grain, same denominator | My 87.9% counted **2,832 catalogue entries**; the 99.0% counted **1,869 tables that hold data**. Both true, different questions |
| R338 | My absence check said "absent" for every source, including the ones I knew were live | An absence check must carry a known-PRESENT control you would bet on | Saved only by padding the probe with `penn_world_table`, whose False was impossible. I was one step from deleting **25,109** catalogue rows |
| R342 | "Verified" a number by re-running the query that produced it | Reproduction is not verification; check with a DIFFERENT instrument | A second DuckDB run returned the same 376,332,763. Parquet footer metadata — no scan — gave the independent answer: **976,632,535 rows** |
| R354 | A 0% overlap from comparing codes at different decomposition levels | A zero overlap is unreadable until a positive control proves the comparison CAN hit | Legacy composite `1A_S1_G13` against decomposed successor `G111_T`: **0 of 74**. Redone at the shared grain: **61 of 74 already served** |
| R375 | Refuted a correct hypothesis with a test that could not see it, and published the refutation | State what the hypothesis PREDICTS about the quantity you are about to measure | Tested whether held flows form a *prefix* of the publisher's order; the real prediction was position: **6 of 9** missing flows sit at positions **123–143 of 145** |
| R413 | A crosswalk tool returned a confident "REFUTED" on its first real run | Run the identity case (X vs X) before believing a single negative result | The two eras stamp annual data at period START vs END, so agreement was unreachable. Self-comparison returned **6,440 matched / 328 ambiguous / 8 unmatched** instead of 100% |
| R463 | Three instrument errors in one session, each nearly a false alarm | "Impossible on its face" is the most valuable signal available and must never be reasoned away | (1) prefix without terminator: 3,305 vs 0; (2) `imf_afrreo` vs `imf_afrreo_direct`: 5 sources / 67,290 series became **19 sources / 386,687**; (3) `&cb=` appended to a path with no query string gave four 404s |
| R464 | Two duplicate-checks both said "new data" and both were wrong | Design the control so it can FAIL for the reason you care about | Filename match: 12 of 764. Key substring: 0 of 18. The data was the same as the served `imf_*_direct` family in a different component ORDER; I nearly catalogued **764 duplicates** |
| R465 | Served a redundant source, and the repo had a tool whose docstring named it | A key-level miss NEVER proves novelty across two stores from one publisher | `audit_dark_redundancy.py`'s docstring says verbatim: *"ilo scores 0 of 115 keys against ilostat, yet 1,154 of ilo's 1,157 INDICATORS are in ilostat"*. My evidence was **0 of 5 sampled keys** |
| R494 | Told Ahmed his site had ~87,000 downloads/day | Write telemetry is not an event count; events come from the event table | `rows_written` 86,880 in 24h. The `download_log` table: **9,580–14,615 downloads/day from 40–56 users**. D1 bills ~6 written rows per insert |
| R504 | Diagnosed a working API key as an entitlement problem, from a probe with no negative control | Never characterise a credential from a status code until a credential that MUST fail goes through the identical path | Real key **200**, garbage key of the same length **403**. So 403 never distinguished "recognised then refused" from "not recognised" — and a chore had already been handed to Ahmed |
| R512 | "We are IP-blocked", with three confirmations, all measuring the same thing | Corroboration is counted in independent VANTAGE POINTS, not observations | `esploradati.istat.it` (193.204.90.13) times out at 21.03s; `sdmx.istat.it` (**.1**) and `www.istat.it` (**.61**) — same /24, our IP — answer in **0.13s**. A foreign egress gets ECONNREFUSED. The host is down |

---

### G3 — Guards, gates and tests that could not fail, or failed open (22)

The most systemically dangerous class, because the artefact is *designed* to be the last line of
defence. R488 states the general form: *a success criterion that reads identically for the correct
and the catastrophic outcome is not a check.*

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R311(b) | My fix was undone three lines from where I wrote it, and all tests passed | Tests written against the piece you edited cannot see an interaction 200 lines away | An end-of-function `save_rotation` overwrote the deliberate wind-back; a capped run reported `no_change` with **2,832 tables deferred** |
| R324 | Two caught before shipping, recorded anyway because "caught in time" is luck | A module-level NameError in a rarely-taken branch is invisible to a green suite | `orchestrate.py` called `merge.impossible_reset()` without importing `merge`; the whole **213-test** suite would not have caught it |
| R344 | A silent fallback hid a broken primary path for weeks | When a component has a fallback, an output that looks right proves nothing | The recorded key order was never once used. `imf_bop_direct`: **260,931** series catalogued with the raw key as title. `imf_cpi_direct`: **27,094** live titles reading "CPI" |
| R346 | My replacement for a broken check could not return False | Before trusting a new check, feed it something that MUST fail | Auth runs before the migration gate, so **every** input returned 401 and the function returned True — including an invented source id |
| R359 | A gate that punished partial catalogue coverage harder than zero coverage | A check the median healthy source cannot pass measures its own policy, not the fleet | Zero catalogued series passes trivially; a partial catalogue could **never** go green. **36 live sources** ATTENTION on one day, real reds buried |
| R361 | The `csv_retry_queue` was write-only | When code says "retried next run", grep for the READER before believing it | `csv_retries()` and `clear_csv_retries()` had **ZERO callers**. insee_bdm parked **43,354** ids in a single run, all lost |
| R372 | One list served two purposes, so budget deferral was reported as breakage | A collection feeding both a retry mechanism and a health verdict must label its entries | insee_bdm read `csv_derive failed 43354/77501` (**55.9%**) and could never go green no matter how healthy |
| R378 | Near-miss: a scripted two-part edit died on its second assert, and the suite passed 3/3 | After any scripted multi-part edit, grep for a symbol that exists only in the new version | The 3 passes proved the **OLD** code still worked. A green suite after a failed edit looks exactly like a green suite after a good one |
| R380 | The comment promised "the next run re-derives", and for ~56 live sources there was no such run | Name the condition that guarantees a later run exists, and count how many entities never meet it | **136 of 173** sources with run history had never once returned `ok`; ~**56 live and served**. Users got `SH.DYN.MORT:PAK` = 58.5 where the publisher had revised to 57.8 |
| R385 | Reported a verification that could not verify, hours after writing down why | A check reading the same source as the artefact it checks is a consistency test, not a verification | Sampling said "15 of 17 mirrors current"; a from-scratch comparison of 55,394 local files against 36,972 R2 objects found **1,379 BEHIND** and **79 AHEAD** |
| R393 | Built a rotating monitor that never rotated, wired it into daily CI, and ignored its own warning | A monitor's first run must be checked for whether it moved its own STATE | It printed the correct warning every run and probed the same five sources forever. **It had never reached a source past 'b'** |
| R414 | The guard written to prevent R407 broke the path it protected, shipped without a test | A guard ships with a DISCRIMINATING PAIR — one case it must block, one it must let through | `local_bytes < 1_000_000 or n_src < 50` is true of every legitimate first seed. CI red across runs 31331575104 and 31331839118 |
| R419 | All 796,716 regional titles silently took the fallback | Whenever code has an intended path and a fallback, count them SEPARATELY | BEA's `GetParameterValuesFiltered` answers in `Results.ParamValue`, not `Results.Data`. The run reported "titles APPLIED: 910,887"; **796,716 of 796,716** Regional keys were fallbacks |
| R456 | The R448 fix computed the right deadline and could not enforce it | A limit nothing enforces is a comment; ask what process ends the work when it expires | The pass logged "clamped 2880 -> 220 min" and was still running **45 minutes past** its own deadline. `signal.setitimer` is a documented no-op on Windows |
| R466 | A streaming CSV writer produced the right data in the wrong order | A test must exercise the path it claims to test; a size-threshold branch needs a fixture that crosses it | All three "byte-identical" tables were small and took the in-memory path. On cbs_nl:71892ned: identical **599,257,804 bytes**, identical **5,536,832 lines**, equal `set(lines)`, differing **from line 1** |
| R488 | Proved a block of FTS rows was "a complete copy" using the one column the index ignores | Prove the SURVIVORS carry the CONTENT; ask which column the consumer reads | The plan kept 2,465,197 rows with **0 real titles** and would have deleted the 2,465,197 that carried them. `/v1/catalog?q=disposable&source=wid` would have gone from **33,390 to 0** |
| R491 | 164,705 series display their published name and match nothing | A tool that ends by PRINTING the next step has not done the next step | `dist/d1/titles/` holds **109 files / 162,769 UPDATE statements** and `grep -rl series_fts` returns nothing. Sample of 120: `series` title correct 120/120, index bare key 120/120, MATCH hits **0 of 9** |
| R499 | AR-017 FAIL: the istat dead-host fix killed live hosts and would have stranded merged rows | Write a threshold as an expression over the loop that generates the events, never a bare integer | `_HOST_DEAD_AFTER = 3` with `RETRIES = 3`: **one** slow flow marks a working host dead. And `raise` where `break` was needed would have stranded 800 flows' merged rows |
| R501 | "Validated zero coverage loss" was validated on ONE file | A guard must test the failure that HURTS; state the n in the same sentence as "validated" | Measured across **379** resources: 9 files lose 340 (series,date) pairs, **14 catalogued series emptied completely**, 24 shrunk. The guard tested uniqueness — and deleting a whole series *improves* uniqueness |
| R503 | Rebuilt R501's fail-open inside the guard written to close R501 | A guard's EXCEPT branch IS the guard; "cannot measure" must refuse, never pass | `except Exception: old_pairs = set()`. On the real file the store held **475 pairs**, the guard saw **0**, printed OK, and would have signed off on losing all 475 |
| R508 | Fixed a fail-open by testing the failure path, but every dangerous failure returns SUCCESS | `errors is None` is not success; enumerate what a *partially successful* response looks like | The same GraphQL query at `limit=1` vs `limit=2000`: **9 requests vs 1,038,162** — a 115,000x undercount, HTTP 200, no error key. Cost impact: **$40–45/mo, ~15% of the bill, deleted under a confident label** |
| R513 | Shipped a PK-range substitution whose stated justification was false | When you justify an optimisation with a general property, WRITE THE PROPERTY AS AN ASSERTION | The docstring said "letters and digits are all above 0x3B". `;` is 0x3B; digits are 0x30–0x39, **below** it. The empirical check over all 322 sources could not catch it, because the case does not exist yet |

**Note on R513: the entry contains its own retraction.** A reviewer showed the assertion tested one
half of a two-sided predicate: a digit-extended sibling also falls *below* the lower bound `foo:`,
so `[s+':', s+';')` is exactly the `s:` prefix set for any `s`, unconditionally. The registry guard
R513 added was a **false tripwire** that would have failed CI the day someone added a legitimate
`unctad_oceantrade2`. It was removed. The entry's extra rule: *when the thing under test is an
INTERVAL, assert BOTH bounds.*

---

### G4 — Class and scope: the instance measured, the population assumed (16)

Ahmed's standing correction — *a reported example is one instance of a class; sweep the whole
surface and prove it with a zero-result check* — is the parent rule for this group.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R303 | A deliberate deferral filed as a transient FAILURE, and I nearly called it 40 | The state store answers "what happened last time", never "what does this code do" | ecb read `252/540 transient-failed`, abs `805/1222` — nothing had failed. State showed **3** sources; grepping the code found **11** |
| R306 | Derived a class from the label text and shipped half of it as "the fix" | Define a class by what the code DOES, not by how it reads | Grep on `transient_unit(.*defer` found 11 fetchers; the behavioural definition (what follows `if dl.spent():`) found **21**. Four queued "investigations" were all deferrals |
| R310(a) | The count-cap half of the deferral class was clean, and I checked instead of assuming | After fixing a class, ask what OTHER mechanism produces the same observable | Three count-cap candidates, all **false positives** of the window. A sweep that finds nothing is still a result |
| R310(b) | A fix cleared two callers as safe in writing, and one was crashing in production | A grep for a function name finds callers; it does not find CONSUMERS of the value | bcrp's state row: `AttributeError('str' object has no attribute 'isoformat')` stamped **six hours after** the commit declaring it safe |
| R311(a) | "Verified repo-wide" meant the two directories I happened to search | For a standing ban, search for the HOST STRING across every file type | `connectors/dbnomics/connector.py` was a complete working client for the banned host, listed among **23** names in `jobs/ingest_all.py` |
| R313 | "Cleared by behaviour" is only as good as the STATE the behaviour was observed in | A self-draining queue and a truncation differ only when the item at the head never completes | **10 of 12** ons_uk slots were held by permanent non-publishers; 297 datasets pending, draining ~2/run → **~143 runs**, 83% of bandwidth re-fetching discards |
| R320 | The impossible-date class was never confined to PxWeb, and nobody had asked all 141 stores | "Fix every parser of type X" is completeness only with respect to X | **273,980 SERVED rows** dated 2999-12-31 to 9999-12-31 across six sources, one of them SDMX not PxWeb. The tool that found it in **four minutes** had never been run |
| R322 | "273,980 fabricated rows" was less than half, because the audit tested one direction | A one-sided test on a two-sided failure mode produces a number that LOOKS like a measurement | Real figure ~**637,000** across seven sources. One stat_slovenia key held 5,863 observations dated years **1, 2, 3 … 6152**, and 41,091 rows landed in 1900..2200 where no bound can separate them |
| R341 | A spot-check of what is PRESENT cannot measure what is ABSENT | Enumerate from the side that can be over-complete (the STORE), then check membership | noaa store 3,137,816 vs catalogue 3,135,873. **1,998** in store not catalogued (0 had a CSV); **55** catalogued not in store (all 55 did). The reported "missing 1,943" was the NET of two opposite defects |
| R377 | The R190 class was declared closed and was not | "Persists something" is not "resumes"; ask whether run N+1 starts anywhere run N did not | ecb had never fetched **280 of its 540** files; ssb ~71% of 186 groups; six insee_melodi flows held **ZERO rows** — all reporting `partial` with zero failures |
| R379 | Filtered the evidence to rows mentioning my symptom, then generalised | A WHERE clause selecting for your symptom cannot measure its prevalence | Published "280 of 540 NEVER fetched" from **7 filtered rows**. ecb has **12** runs, and the 5 excluded include **four complete passes**. Then I repeated the error inside the retraction |
| R382 | Picked the filter from the example instead of from the mechanism, and it excluded the worst case | Derive the population from the causal rule, then check the rule's boundary cases | ecb (1 ok, 12 partials) was never sampled and was the worst case in the fleet: **0 of 25** served objects identical |
| R390 | Fixed the flat-layout bug in one tool while three others still had it, including the guard | The moment you fix an assumption, grep for it and put the grep in the commit | The R383 guard printed `usda: NO local parquets … UNCHECKED` while **60** usda parquets sat one directory down; bea has **592** |
| R437 | "Made them all uniform" verified hero, nav and CTA but never audited the FOOTER | "Uniform" is a claim about the WHOLE page; enumerate every shared region first | Ahmed pasted three visibly different footers side by side: *"you told me that you made them all uniform, you lied.."* |
| R439 | "Fixed, measured at 4 widths" — all four were desktop | A responsive claim requires the responsive RANGE; four measurements inside one regime are one measurement | Measured 1135/1235/1385/1685 px, all 0. At **375px** hf still overflowed by **181px**; econ by **248px** |
| R517 | The fix rested on a premise never measured: that the "changed" set names what changed | Cost the INPUT SET before optimising the operation | `norgesbank.py` seeds cursors from disk before the first HTTP call, so 9 keys meant "every flow on disk". The change would map **all 35,135 ids every run** — 32x the costed work. **Eleven fetchers** share the shape |

---

### G5 — Causal stories published before the refuting test (18)

R514 names the mechanism: *a causal story is what makes a finding feel finished.* The observation
is sound; the leap to a cause is free; and the refuting test is almost always one call.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R301 | Wrote the rule that morning, then spent an investigation breaking it | Supporting evidence gets audited less than a primary claim, which is backwards | The 26 hagstofa "structural breaks" had been fixed **two days earlier** by commit `1188fb62`, whose message names the exact count |
| R305 | The guard had been shouting the answer every run, filed as the source's fault | A guard that fires repeatedly is EVIDENCE, not noise | `refusing shrink 185->1` was the never-shrink guard protecting **161,849 rows** across three dimension tables, every single run |
| R308(b) | Three wrong root causes for one silent job; the real one was in the query plan | Ask what the process is actually doing before theorising | No index on `series.source_id`, so every `--source X` derive full-scans an **8.5 GB** catalog before printing a line. I killed it three times |
| R309(a) | Shipped a fix built on a mechanism I never checked, and broke two working endpoints | A plausible mechanism plus confirming state measurements is not a diagnosis | `_identity_keys` — twenty lines above my edit — already keyed on every dimension. Nothing had ever collapsed; the "fix" stopped refreshing **161,664** healthy rows |
| R321 | Read a stale state row as a fresh crash, and my earlier sweep had stepped over the live bug | A red status has a DATE; and a symptom-shaped grep finds only the symptom it was shaped from | The crash predated the fix by **13h41m** and the source had not run since. The real live bug at line 343 raised `TypeError`, not the `AttributeError` I grepped for |
| R339 | Judged a fix by runs produced before the fix existed | Compare the newest run timestamp against the commit timestamp of the code under test | The three cited kills all predate the last two fixes; the newest cap logic had **never executed once** |
| R353 | An Exception control signal eaten by every broad handler in its path | An Exception-subclass control signal is only as strong as the narrowest `except Exception` between raise and handle | Three handlers swallowed `UnitTimeout`; one restarted a full sliced re-pull from zero. **CORRECTED same day:** the named victim was wrong — the unit took **1,595 s (27 min), under the deadline**; the extra hours were post-unit steps |
| R394 | "Not reproducible" was reproducible the moment I asked the publisher | "Not reproducible" after N eliminated mechanisms usually means it was never attempted against the PUBLISHER | We had been serving the **2011 Icelandic census dated 2000**. The loose predicate would have flagged 111,880 rows; the real one is **7 tables / 143 rows** — 782x smaller |
| R397 | Shipped a fix, called it the cause, and the proof run hung in the same place for 212 minutes | Ask which PHASE the failure was in, not which mechanism inside the assumed phase | `strat.detect_change` runs **eighty-five lines before** the deadline block, so no SIGALRM was armed. The first log had no `ARMED` line at all |
| R428 | Reported "imts converges over capped passes" three times while the mechanism made it impossible | "Progress persists / it converges" is a state-store claim; verify it against the state store | Three consecutive 10,800s caps. `unit_state.last_attempt_utc` was frozen at **2026-08-05** — no pass since had persisted anything, because the push step lacked `if: always()` |
| R444 | Called a measured result "implausible" before the deciding test finished | A failed sanity check licenses "unverified"; it does not license "implausible" | 2.53 obs/series against a published 10.32. The deciding query, already running, returned: statcan `98100619` has **674,645,760 rows / 674,645,760 distinct keys / ONE date**. Excluding statcan the fleet averages **9.0** |
| R467 | Chased a "segfault" through memory, S3 and the table; it was a collision I configured | A native crash with no traceback in concurrent I/O is a resource-collision hypothesis FIRST | DuckDB names every spill file `duckdb_temp_storage_DEFAULT-0.tmp`, and `core/derive_csv.py:98` pointed every connection at one shared temp dir. **Four wrong diagnoses**, and a permanent architectural workaround nearly shipped on the false one |
| R468 | "Confirmed real staleness" for fhfa after checking only the publisher | A staleness claim has THREE terms — publisher, our store, our state — and naming one stale requires measuring all three | Our parquet reached **2026-05-01**, exactly matching FHFA. The gap was `CURSOR_CAP` = **50,000**, spent alphabetically on annual series. A round number like 50,000 is a cap, not a coincidence |
| R480 | The defect was diagnosed correctly months ago and filed as a PROPERTY OF THE DATA | If the sentence contains "the parser/finder/loader did X", the resolution is a fix, never a permanent exclusion | **384** damodaran series served with a gross-margin *number* where their identity belongs. The corrected header rule yields **1,728** series — 4.5x more, all identifiable |
| R487 | Fixed the FTS duplication in a tool that cannot produce it | Check that the code path can PRODUCE the magnitude measured | `catalog_complete` returns early when nothing is missing, so it can add at most **one** extra copy. The real writer is the D1 sync — boc measured at **8.00 copies** (102,882 rows / 12,862 ids) — and it carries a comment adopting the defect |
| R506 | Published two opposite diagnoses of the same files, neither measured | Name the quantity that would DECIDE the question, then measure that one | Four instruments agreed and all four asked "are these the bytes we received?". Counting distinct `(reel-no, frame-no)`: **29 files carry 166,910 surplus duplicate records**, and `ad20260412.zip` is **byte-identical to what USPTO serves today** — the duplication is upstream |
| R514 | Three causal stories in one session before the cheap test that refuted each | Report the OBSERVATION at full confidence and the CAUSE at the confidence its test earned | (1) "istat IP-blocked us" — a second egress refutes it; (2) "fixing the local route unblocks 14 sources" — `bis` and `faostat` succeeded at **00:45Z inside the pass I called starved**; (3) "six sources share a §5.7 cause" — six became **one** |
| R516 | Read a filename convention as a fact about behaviour, and built a narrative on it | A naming convention is not a behaviour; one grep converts a hint into a fact | `_who_gho.py` is not "parked" — it is a shared base module `import`ed by who_hwf, who_rs and who_sdg, two of which ran **three days earlier**. Every error in the entry it corrected **inflated** the finding |

---

### G6 — Data destruction and near-misses (14)

Actual loss, or loss stopped by luck or by a guard someone else wrote. Ordered roughly by how
close each came.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R331 | A missing date grammar does not produce missing data, it produces confident wrong data | A parser returning None does not abstain — it votes for every other column | SCB municipality codes **0114..2584** became the years 114..2026 across 87,358 rows. After the guards landed, the same gap produced **0 rows** instead — silently |
| R333 | Shipped the parser fix to the backfill path; shipping it to the live path would have duplicated | A parser change that alters AXIS SELECTION is a re-grain, not a bug fix | Old and new keys never collide, so both survive and never-shrink cannot see it. That is exactly how ons_uk reached **20,198,302 rows for 10,099,151 observations** |
| R364 | The whr derive PUT 1,927 CSVs for a 1,749-row catalogue | A derive for source X must PUT exactly catalogue(X) objects; any excess names the contamination | The dry run printed "2 shards" and "1,927"; I read past both. **178** provenance-tainted OWID-era CSVs landed on R2 |
| R383 | Mass-rewrote served data from a scratch mirror without checking it against the store | Prove the copy is not behind the store BY CONTENT before writing from it | ~**250,000** CSVs re-derived across fifteen sources. stat_slovenia: local 2,629 rows to 2024 vs R2's **2,771 to 2025**; hagstofa 1,884,485 vs **2,222,916** |
| R384 | Saw a difference, named it "stale", and launched a 2.4-million-object rewrite | A byte difference is a SYMPTOM; establish WHICH side is wrong and WHY | The RESOLVER was broken — a legacy wid monolith (1.93M series) beside 412 per-country shards (2.86M) returning both for one id. Several thousand objects were rewritten into the broken shape before a tool someone else wrote **refused** at 242/300 |
| R386 | A served company lost 18 years of data because the refresher replaced instead of merging | An upstream identifier is not a stable primary key; a shrinking write needs an assertion at the write | Exxon re-registered: `XOM.parquet` went **20,629 rows to 274**. **Seven** CIKs have been re-assigned; six were still armed. My next action would have overwritten the last surviving copy |
| R387 | Assumed the natural key, and it would have deleted 2,013 of XOM's 20,629 rows | A column set is a key only when `count(*)` vs `count(distinct …)` says so | 20,629 rows, 20,578 distinct 4-tuples, **18,616** distinct on my assumed key. Caught by a superset guard I had written ten minutes earlier |
| R388 | The audit told me the sync would destroy data and I had already run it | Classify EVERY file in both directions before a bulk copy; the ahead set blocks the copy | **967,043 rows** present locally and absent from R2, overwritten. Six sources diverge in both directions at once |
| R399 | A generated source id collided with a legacy source and the ingest overwrote its store | A GENERATED identifier entering a namespace with history must be existence-checked first | `source_id_for("US.Cpi_A")` produced `unctad_cpia`, the exact id of a legacy source with **637** served series. The tell was arithmetic: 1,197 − 560 = **637** |
| R407 | Tested a state-store fix against the LIVE state store while a real pass was running | Never exercise a destructive-path fix against the live artifact | Moved the **10 GB** state.db out from under a running updater 90 seconds after it launched; SQLite minted a fresh empty **4,096-byte** database and the pass ran 18 heavy sources believing nothing had ever run |
| R424 | LATENT: the UNCTAD spill cache was measure-blind | A cache key must contain EVERY parameter that changes the response | The measure lives in `select`, which was not in the `sha1((tgroup, restr))` key. Measure #2 would have cache-hit measure #1's files and assembled as **(near-)empty while reporting success** |
| R469 | Rebuilt a work queue from a fresh scan and dropped the exclusion that made it safe | A safety filter belongs in the CODE that consumes the queue, not in the queue file | The two excluded monsters (**1,886,692,500** and **1,056,918,900** rows, ~360 GB and ~200 GB as CSV) went straight back in at lines 5 and 6. Killing them freed **152 GB** of DuckDB spill; D: had already fallen 581 → 555 GB |
| R481 | Deleted 384 series from R2, D1 and the catalogue, and left all 384 in the SEARCH INDEX | Deleting a series is FOUR places, and the fourth has no foreign key to remind you | The per-source FTS refresh deletes `WHERE series_id IN (SELECT … FROM series …)` — by construction it cannot remove a row whose series row is already gone. A search hit would have led to a **404** |
| R489 | `source_counts` is a FIFTH place a series lives, and one of the drifts was billing me | A derived cache with exactly one writer is a silent-drift generator | Four drifts across 321 sources. `vdem` had **no cache row at all**, so every catalogue page view read **783,100 rows live** — the 2026-08-15 cost incident rebuilt inside the table built to stop it. `ilo` advertised **1,157** series with **0** rows |

---

### G7 — Grain: the level at which a thing is keyed, catalogued and served (6)

Small group, disproportionate consequences. Every entry turns on the same question: *does one
catalogue row correspond to one series, or to a table containing many?*

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R350 | Catalogued at a grain that does not exist in its store — and the correction | Find the RESOLVER and call it; set intersection tests "ids equal keys", which coarse-grain serving explicitly denies | **0 of 2,937** catalogued ids matched any of 472,234 store keys. The CORRECTION, same day: this is a deliberate documented table grain — **2,937 = 245 countries × 4 frequencies × 3 indicators**. A count that factors that cleanly is a designed grain |
| R356 | A pre-committed grain rule skipped because the family's earlier members made series grain feel default | A pre-committed decision RULE is not a decision; it re-runs on every new measurement | GS_LI catalogued at series grain: **80,394** rows, ~16% of remaining D1 headroom. At table grain: **233** rows — a **345x** collapse. Caught before D1 and before serving |
| R403 | An id-level number presented as recoverable series; value verification refuted the re-key | An id-crosswalk hit is NOT a continuation licence; shared-period VALUES must agree | The approved plan cited "27%→79% recovered". Value-verified, the crosswalk mapped **7 of 7,650** — FAOSTAT had moved to AR5 GWP factors (CH4 21/25 → **28**), so keys reproduce 1:1 while values are rescaled |
| R497 | AR FAIL: "nothing served changed" for eia sampled the wrong property | Check the SERVING resolver's predicate, not the catalogue's id strings | eia's 268,495 catalogue ids are table-grain served by dot-PREFIX. The proposed green run would have silently frozen **598 served EBA CSVs forever**; the cursor set was cap-saturated at 50,000 besides |
| R498 | AR-016 FAIL: the dst plan's bulk-derive was wrong-grain, and its own verify gate would have passed it | Assert `COUNT(DISTINCT store keys) == catalogue rows` before any per-key bulk derive | `derive_csv_bulk` emits one object per distinct store key: **916,416** against dst's **2,264** catalogue ids. Its `--verify` resolves an id constructed BY the tool under test, so it byte-matches the wrong grain perfectly |
| R515 | A source merged distinct WHO series into one id, and the correct key was already in the repo | A duplicate `(key, date)` pair with CONFLICTING values is a key-collision — the key is missing a dimension | `who_gho` keyed `Indicator:Spatial`, dropping WHO's `Dim1..Dim3`: **42.7%** of 162,790 ids conflict, **80.4%** of 8,188,819 rows hidden. Publisher confirmed: `HCF_REL_ELECTRICITY` SEN 2019 returns URB 53.0 / RUR 3.0 / TOTL 45.0 |

**R515 carries two retractions, both written before Ahmed acted on it.** (1) `who_gho` is **not
served** — zero catalogue rows, no registry entry, gated 451 by the worker's denylist. The harm was
overstated; the served members are `damodaran` (721 conflicting keys) and two UNCTAD stores
(**16,835** and **15,402** conflicting keys). (2) The sweep was not exhaustive: it reported "308 of
308 stores" when the real population is **430** directories and it measured **299 (69.5%)**, because
the script globbed `<dir>/<dir>.parquet` only and silently excluded every multi-file store — ibge
(12,125 files), statcan (8,207), eurostat (7,213), cbs_nl (5,511), owid (3,787).

---

### G8 — Cost, capacity and the bill (10)

Ahmed pays this bill. Two months ran over $100 and $200 because of these. The project's cost
guards (`.claude/hooks/d1_cost_guard.py`, `cost_banner.py`) exist because prose rules did not hold.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R309(b) | The query was expensive because it answered a question nobody asked | "Make this query faster" is narrower than "what does this function have to return" | `count(*)` with a prefix LIKE: **116.35 s** per source. The caller only tested zero-ness. `SELECT 1 … LIMIT 1`: **0.0001 s** — six orders of magnitude, by deleting the part nobody wanted |
| R418 | Launched the family's largest pull on an accumulate-in-memory-only path | Before any multi-hour accumulation, ask what a kill at hour N costs | ~**19 hours** and ~14 GB working set lost to a single **HTTP 401** that was transient — the day after shipping a resume fix for exactly that class |
| R430 | The catalog endpoint's query shape billed ~$82 in one day | Any query running per public request against a multi-million-row D1 table must be O(page) | A crawler paging wid did 17.7k requests → **87.3B + 42.2B ≈ 130 BILLION rows read in one day**, average efficiency 0.00008. Detected by Ahmed reading his bill. After the fix: **~$0.10/day** at the same traffic |
| R435 | Changed a proven config and left a bytes-unbounded queue | A proven setting is evidence; deviations need their own evidence. Bound queues in BYTES, not items | duckdb `--memory-limit` "optimized" 8→24 GB with no profile, on a box already hosting a 63 GB job; the uploader buffered up to **1,000 raw CSVs at ~100 MB each**. MemoryError at giant **10 of 8,207**, ~**23 hours** lost |
| R473 | The fix made the broken flows parse, and I never asked whether they would FIT | When a fix makes previously-failing work succeed, size that work BEFORE shipping | One flow yields **213,650,346 observations** on a **16 GB** runner. The number that predicted the OOM was in my own verification output. The kill took the **125 flows / +12,328,261 rows** of real progress with it |
| R473b | Changed what a loop iterates without deleting what fed the old one | Dead code that still executes is not dead, it is unread; the cost shows up on the biggest input | A leftover `SELECT DISTINCT series_key` over the whole store reached a **79 GB** resident set before I killed it |
| R492 | Planned 164,705 statements against a column with no index | When the predicate column is UNINDEXED, cost is per statement — raise arity, never add statements | `series_fts WHERE series_id = ?` reads **23,843,482 rows in 11.2 s**; an `IN` list of **200** ids costs the same. The plan was **3.93 × 10¹² row reads ≈ 24 days ≈ $2,500**; I had estimated **90 minutes**. Five range-scoped statements replaced 164,705 — **~33,000x less** |
| R502 | Every D1 dollar figure ignored the 25-billion-row monthly allowance | A cost model is a claim about the vendor's terms and needs a citation | Workers Paid includes **25,000,000,000** D1 rows read and **50,000,000** written per month. `billing_guard.py` billed from row zero. The title repair's "billed $1.49" may truly have been **$0**. A second error the same hour: one DB failed to measure and I published the other two as the fleet, low by **8x** |
| R505 | The bill was "measured but unpriced" | Reconcile the TOTAL against the lines the report prints, and fail loudly on a mismatch | The report printed `R2 ops … ClassA 264,454 (~$1.19)` **four lines above** a PROJECTED MONTH that omitted it — ~**$31/month**, the largest variable line. And it projected D1 reads from the top-100-truncated insights (**36.1M/day**) while holding the GraphQL truth (**374.3M/day**). Honest model: **$24 → $75** |
| R507 | The billing guard's ALARM and its REPORT read different instruments | If a guard displays one number and alarms on another, they are two instruments and you must prove they agree | The alarm compared insights, which runs ~10x low: 2026-08-10 was **5.4B** true reads and the alarm saw ~0.5B, so it never fired. Reconciled against invoice **IN-74622130** ($154.96 pre-tax, **$165.19 paid**) only after five defects, every one biased cheap — a cumulative allowance compared to a rate (**218,803,889,335 = 880% of the 25B**), a GB-month priced from a snapshot, a `max` aggregate collapsing to one bucket (**758 vs 1,486 GB-months**), a missing 730-hour normalisation, and Texas tax never mentioned |

---

### G9 — Concurrency, locks, schedulers, and jobs that look alive (17)

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R307 | The watchdog covered the jobs I inherited and none of the jobs I started | A job that has to be relaunched by a person is not running | A reboot killed six jobs; three self-recovered, three did not. The noaa pass had reached ~**31%** and that work is simply gone |
| R340 | "Local is a stale mirror of the cloud" is only half true; it is a separate LINEAGE | When two writers share one data store, state is a MERGE problem, never a freshness comparison | ofr: CI 433,040 obs, LOCAL **433,301**, and R2's shared store held **433,301** — the local number exactly. `--push-state` would have silently discarded three later CI runs |
| R351 | Duplicated a concurrent session's fix, with the evidence in front of me twice | An unexplained change to the system is a REPORT THAT SOMEONE ELSE IS ACTING | Same file, same fix, near-identical message: `037033eb` at 22:51:51 and my `e5ded6d3` **twenty minutes later**. Both artefacts authored "Ahmed Elkassabgi" — the machine's shared git identity |
| R355 | A fetcher's cross-run state read raw-local made every CI run a cold start | Cross-run state lives behind `blob`, exactly like the store; a reset that lands in an "adopt as current" branch becomes completed work | dst's served store frozen **4 MONTHS** behind daily green `no_change` runs logging "63/63 drained; backlog clear". Third occurrence of the same backend rule |
| R357 | A catalog refresh raced a cataloguer; the upload passed `quick_check` and poisoned the guard | `quick_check` proves structure, never snapshot consistency; serialize refreshes AFTER cataloguing | The uploaded snapshot carried **114 rows whose `source_id` is raw b-tree page bytes**, and the guard's own report line then crashed on `bytes.__format__` |
| R362 | Dispatched a workstation-routed source's proof run to the cloud | Prove a source WHERE IT RUNS; read `run_location` first | Census answers a missing key with **HTTP 200 and an HTML page**, so **45/45** flows were recorded as schema breaks — a false verdict wearing my fix's name |
| R367 | The seventh hand-dispatched backfill run was cancelled by the scheduler I was racing | Hand-dispatching a live source serially competes with your own scheduler for one slot | Seven consecutive manual dispatches; the 04:59Z and 06:57Z crons both arrived at the single-slot `aqueduct-updater` group |
| R400 | Launched the R2 catalog refresh while my own batch was still cataloguing | A `PermissionError` on a database file is a WRITER-PRESENT signal, not a transient | The corrupt snapshot went live before the tool's own check condemned it: *"Tree 11 page 2216924 cell 0: 2nd reference to page 194146"*. Restored byte-equal at **518,315,759 B** |
| R406 | Two manual `updater.run` processes held state.db for 30 hours and took the fleet offline | A guard that retries a failing job every 5 minutes forever is an OUTAGE with a progress bar | Both alive **30 hours** having written nothing, burning **1,396 and 1,488 CPU-minutes**. **13 consecutive** guard aborts silently skipped all **18** heavy sources. Three tells sat unread |
| R421 | The hf site served 12-day-stale data and nobody in the repo noticed | Glance at a repo's scheduled-workflow conclusions at session start; a red daily job in a repo you push to is your outage | **Seven** consecutive daily runs died identically. GitHub hard-kills at 6h and marks 'cancelled', and the commit guards were `!cancelled()`, so each run **discarded the day it had already completed**. The ticker merge (3h43m of 3h55m) ran on half the machine |
| R427 | Relaunched a "resumable" job without its resumability flag | Resumption is a property of the INVOCATION, not the tool; verify it ENGAGED in the first log lines | The preflight printed "to derive: 6,666 (**already present: 0**)" against 4,794 objects I had just counted on R2. With the flag: "to derive: 1,872 (already present: **4,794**)" |
| R446 | Took the shared scheduler's lock with a manual `-Force` giant | Never hold a shared scheduler's lock for an ad-hoc proof run | A 1.06-billion-obs source livelocked: **18+ hours**, zero data files, log frozen at 1,775 bytes. The entire local route was frozen **19 hours**, including statcan, census, oecd, bea and eia |
| R448 | "CI idle — safe to proceed" 19 minutes before the cron fired | Never store a derived value where the source fact belongs; a window is an INTERVAL | The stored window was already guard-adjusted (`17:40` for an 18:00Z cron), so at 17:41 it read as PASSED and granted an **693-minute** budget. And only 2 of **4** state-writing crons were listed |
| R453 | "Alive and advancing" passed cbs_nl for 6 days while it accomplished nothing | Liveness, throughput and a progress counter can ALL be green on a job producing nothing; check the ARTEFACTS | **315** completed runs, one every ~68 minutes, each re-walking 5,951 already-crawled tables. The tell was consecutive run logs at the identical **827,610 bytes** |
| R454 | The second source found dead in one night by the same pattern | An `except` listing specific types is a whitelist; for network code catch the base class and classify | istat: **40 days** of `transient_fail`, because `requests.TooManyRedirects` was not in the except list, so a working fallback host was never reached. The dead host was already named in my own memory file |
| R457 | An ORPHANED child satisfied the guard's liveness check and froze a crawler | "Is a process with that name running" is a NAME check, not a liveness check | 15 threads alive, **zero CPU and zero I/O over 30 seconds**, parent gone. Blocked writing into a pipe with no reader, for **15.5 hours** |
| R475 | "Expected to resolve itself" was my own words for a source that resolved into permanent retirement | Never write "expected to resolve itself"; a sentinel is an interface — ask what else reads it | On completing **1,237,766,278** observations the crawler wrote `logs/gus_dbw.DONE`, which the guard reads as the operator retire-flag. **It retired itself**, and its 1.24 billion rows have no update path |

---

### G10 — "Live", "served", "deployed": the running system claimed from an artifact (18)

R345 is the parent rule: *"deployed" is a state of the running system, not of the repository.*

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R317 | "pwt is 4 years behind" — one source id named the OLD product | Sweep the registry for a sibling id before calling a source stale OR complete | `pwt` 389,098 rows to 2019; `penn_world_table` **418,397 rows to 2023**, ingested since July. The supersession note was four lines above where I was editing |
| R318 | Loosened a working gate because my ruler was not the gate's ruler | Before "fixing" a rule, compute the thing the rule computes, or read its output | I computed CALENDAR age; `_business_age_days` had existed since 2026-07-13 with my finding in its docstring. My change passed **198 tests** because nothing tested the gate's verdict. Reverted whole |
| R327 | Read a PROJECTION frontier as a staleness signal | A future date is usually a legitimate projection; the discriminator is contiguity and the key's own name | `18_Population_Projections.parquet`, table PEC26, **333,360 rows across 2030..2057 contiguous**. `health.py:221-223` already separated frontier from newest_obs, with a comment recording 28 of 93 units |
| R345 | Called 425,462 series "live" and "SERVED" | A config constant is only in effect once the artifact compiled from it is released | Committed and pushed; **not deployed since 2026-08-02**, and no workflow deploys the worker at all. Live `/v1/sources` returned **196** against 223 catalogued; every one of the 425,462 ids answered `501 not_migrated` |
| R371 | Committed "downloads were broken" and "345 rows have no store data" — both wrong | A claim about what USERS can do is a claim about the SERVING surface | The Worker serves pre-derived CSVs from R2, not the Python resolver: all **7,896 + 42 + 139** CSVs exist with real content. The true defect was milder and different — those CSVs are FROZEN |
| R376 | "No override exists and the constant must be edited" — both wrong | "I could not find X" is not "X does not exist"; grep the SHARED HELPER | `AQUEDUCT_BUDGET_MIN_OVERRIDE` in `_common.py` applies to all **38** budgeted fetchers. The log printed the effective value from the first deferral onward |
| R396 | Recorded "byte-parity is unreachable by construction" and one measurement overturned it | "Impossible by construction" is a claim about CODE YOU CAN READ | `native_to_tidy` ends `return out.sort_values(["series_id","obs_date"])` — **twenty lines below where I stopped**. I was one step from recommending a **69,704-object** rewrite |
| R401 | Eleven sources rode a deploy into `/v1/sources` with ZERO D1 rows | A deploy publishes every line of the file, staged or not | Live-listed but empty for ~**6 hours**; a user tapping any of them got an empty listing |
| R404 | The public API reference documented an API nobody ever built | A docs page is a PRODUCT SURFACE and needs the same running-system gate as a serving claim | Advertised `/v1/bars/{ticker}/daily`, a `/v1/bulk/{package}` with eight packages, and six query params — **none** exist. At ~**18 registrations/day** it had been sending developers into the same wall for weeks |
| R408 | "ei_statreview is still gated" because a doc said so | A "gated"/"applied" line in a tracking doc is a claim about code; when they disagree the doc is stale | `ei_statreview` appears **0 times** in the 50-id `denylist.ts`, and the live catalog returns its **18,464** series. The escalated compliance alarm was also wrong on substance — all **127** metrics are physical energy quantities, zero price series |
| R412 | Advised on a live trade-off that no longer existed | "We currently serve X" requires the catalogue count AND the live endpoint, never a remembered ratio | Both sources had been retired weeks earlier: `imf_fm` **0** catalogued rows, `imf_mcdreo` **0**, both absent from live `/v1/sources`. It cost Ahmed a decision round-trip on a non-question |
| R432 | Told Ahmed the SSO flow "has never been walked", from a stale task item | A task list is a plan, not evidence; verify its premise against the running system | One D1 query: **1,006 registered users, 845 logins in 21 days, 3,131 SSO refresh tokens, 91,557 downloads in 7 days** |
| R443 | "Verified end-to-end" after checking the CHAIN EXISTS, not that it RAN | The evidence for an automated fix is the TARGET's own state moving | The pass exited rc=0 and `last_attempt_utc` stayed at **2026-08-14**. A bare `python` resolved to a 3.11 with no pyyaml; the ImportError went to stderr and the **empty stdout** read as "no sources routed here" |
| R452 | Declared statcan "0 bytes served" and asked for money | "Is it served?" is a question about EVERY surface the worker can answer from | A full-bucket scan of 12,990,506 objects found **213,916 statcan objects** under `series/statcan%3A*`. And the deletion I was pricing was Ahmed's own **2026-08-18 cost order** (1,548.7 GB, 65% of the bucket, $23.23/mo) |
| R461 | Measured ONE leg of a five-leg serving chain and called it "served" | Verify the LAST leg — does the live API list it, does a real request return a body | Reported "2,187 uploaded"; cbs_nl and gus_dbw appear **zero times** in util.ts, have **zero** catalogue rows, and are absent from the live list of 318. True figure: **5,529 of 5,529 unserved**. And I put the false claim in a brief to another model |
| R496 | "The served number rises when it uploads" promised a mechanism the instrument lacks | A forward-looking "X will then happen" is a MECHANISM claim; find the code path | The statcan job PUTs **CSVs**; the instrument sums **worker-resolvable parquet footers** and excludes CSV-only sources by scope, statcan first among them. Finishing moves the number by **zero** |
| R509 | Copied "fresher counterparts" out of my own digest line and built a fix on it | Never build on your own ledger or digest line — it is the citation you are least likely to challenge | There is no 404: `worldbank:NY.GDP.MKTP.CD:XD` returns **200** with rows to 2024-12-31, and the fix sits behind `if (!series)` so it **cannot execute**. All 8 pairs: `legacy=65 wdi=65, wdi-only dates=0` — nothing fresher |
| R511 | Made 468 frozen tables VISIBLE and called it a fix | A fix that makes hidden work visible is not a fix until you compute WHERE that work lands | Best queue position of a probed matrix: **12,318 of 12,378**; `MAX_TABLES` is **60**; probed matrices inside the first 60: **0**. Six to twelve months to the first pull. The second attempt moved 12,318 → **5,191**, and 60 is still 60 |

---

### G11 — Licences, provenance, and claims that leave the building (16)

Anything a stranger can check: a licence string, a public number, an email, a page.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R358 | An un-gating updated D1 and the worker but not `catalog.db` | A re-classification lands in catalog.db AND D1 AND the R2 snapshot in one session, from one declared source of truth | A non-commercial written grant was advertised as `commercial_ok=1` on every catalog.db-fed surface for **20 days** |
| R369 | Invented a licence id that differed from the canonical one by a hyphen | A tool writing a licence row READS the id and its flags from `configs/sources.yaml` and never re-types either | `undata-terms` vs the canonical `un-data-terms`, assigned to **396** series. Both were `reservable=1`, so nothing would have failed loudly |
| R416 | Told Ahmed a named exchange prohibits redistribution, with no source | Never state that a publisher permits or forbids anything without a verbatim quote and a URL you retrieved | Ahmed asked for the link. There was none. He then told me the terms **specifically permit** redistribution — an unsourced claim wrong in the *opposite* direction, whose effect was to argue him out of data he is entitled to publish |
| R420 | The census tool published defective headline numbers to the live public endpoint | A tool publishing a public number gates on a sanity diff against the currently-published value | Uploaded **36.56B series / 89.74B observations**, wrong in both directions: statcan contributed 32.85B one-observation census cells, and the US census store was **missed entirely** (45M obs / 444k series against a known ~7.73B). Series delta +28.8B vs obs delta +9.96B is arithmetically impossible |
| R442 | An unverified number in an email draft, and a reply silently detached from its thread | Every number in text leaving under Ahmed's name must trace to a command run this session | Wrote "an inception price near $25" for a **leveraged** ETF from memory. Separately, `update_draft` does not take `replyToMessageId`, so the revision would have arrived as a new message |
| R449 | Gave Ahmed a public-facing number counting data that is NOT served | A number leaving for the outside world is measured on THE SERVING SURFACE | Quoted "over 4 billion series and more than 93 billion observations" from a **local-roots scan**. Recomputed over R2: **3,157,309,693 series / 23,461,900,880 observations** — series overstated by 28%, observations by **4x** |
| R450 | "Yes. Your claim is true." — then walked it back twice | Never answer a go/no-go on an outbound claim before the checks are done | Two failures: the local-vs-served gap (R449), and grain — oecd lists **28** catalogue entries against 752,941,722 series in its files, abs **18** against 443,275,108, cbs_nl **0** against 623,897,728, while a competitor lists all 600M individually |
| R455 | Published a LIVE API key to a public repo, after a secret sweep I called clean | Auditing what a push contains means READING THE FILE LIST, not grepping for a regex | `.uspto_key` — a bare 30-character token, no `=`, no quotes — matched no assignment pattern and sat in a listing I had already printed. Public for ~**3 minutes**, 0 forks, key compromised regardless |
| R459 | The number on the public site carries a published accuracy claim false in both halves | A published number carries its published METHOD, and both are claims you own the moment you re-publish | `/v1/stats` served 3,190,863,550 with "HyperLogLog estimate, ~1% error; conservative floor". Measured against exact counts: whr **+19.3%**, wid **+15.7%**, usda **−14.0%**. Not ~1%, and not a floor. Exact counting is affordable: **29.4M distinct keys in 99 s** |
| R471 | Checked that a licence verdict EXISTED, not that the guard could read it | Satisfy THE GUARD, not your own reading of the property | The verdicts were in a dated addendum; the test parses only the `## Per-database index` table. `pytest tests/test_licence_gate_matches_docs.py` takes **1.7 s** and would have caught it |
| R472 | Verified the SERIES licence and published the SOURCE licence | A source has more than one licence field; assert they agree before publishing | gus_dbw's 194 series all carry `gus-pl-open` (attribution **plus PSI disclosure**); the parent row said `cc-by-4.0`, and the D1 sync published the parent. `gus-pl-open` did not exist in D1 at all |
| R479 | Told Ahmed his data was NOT being used, contradicting my own verified registry | When a question has a recorded answer in an artifact you built, QUERY THE ARTIFACT | The page carries a full attribution with the DOI and the licence. `data/used_by.json`, which I built the same session, held that exact quote with `verified_utc: 2026-08-24`, and the page I handed him listed it |
| R482 | Announced a "user-visible" search defect from a shadow-table row count, then disproved it | When a claim is about what a USER sees, measure the user-facing surface first | 24,291,715 FTS rows against 10,348,125 series looked like duplicate results. The live API: **400 returned, 400 distinct**; `q=disposable&source=wid` total **33,390** against a true 33,390 — inflation 1.00x |
| R486 | RETRACTED a real user-visible defect by testing the one source where it was invisible | A retraction needs at least the evidence of the claim it withdraws; test the WORST ratio | boc is **8.00x** duplicated (102,882 FTS rows / 12,862 ids) and a search page is **84% repeats**. I had the per-source ratios in front of me — wid 4.00x, cepii_gravity 3.04x, boc 8.00x — and picked from the middle. The finding was **REOPENED** |
| R490 | Seven sources carry a different licence in `catalog.db` than in D1, and the generator reads the one that says yes | A licence claim has three surfaces — catalog.db, D1, and the verbatim audit — and the audit wins | `istat` serves the licence name *"License not yet verified — do not redistribute until reviewed"* beside **seven** download buttons; `who_hwf` and `who_rs` carry **Fund for Peace's** republish URL. **Corrected same day:** `ei_statreview` is served under a recorded written grant, not in violation |
| R495 | Told Ahmed the old 79.8B was "the US census source" | A docstring's aside is not a provenance record; measure the dominant component FIRST | It was statcan: a footer sweep of 8,207 parquets, free, **4 minutes**, gave **56,845,814,827 rows — 71% of the July 79.78B**. The message carried the hedge "I won't guess" two sentences before the guess |

---

### G12 — Authority: the owner's decisions and the premises I gave him (10)

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R343 | Filed 36 sources under a reserved task whose own text covers 8 | A reserved task reserves what its text says, not its prefix | The task title reads "The **8** served imf_* sources"; I mapped **36** onto it and put **1,093,077** series into the nothing-to-do pile on a prefix match |
| R381 | A gap is not a defect until you have checked whether it was a decision | Before calling a shortfall a bug, look for the code or comment that chose it | 68 of 92 stored indicators had no catalogue row. `connector.py` defines `INDICATORS` under "Curated starter set" — a hand-written editorial selection of **24**. Fixing it would have required inventing a pillar the publisher does not expose |
| R398 | Told Ahmed the UNCTAD key was his top blocker while it had been in `.env` since the night before | "Blocked on X" is a factual claim about the present; re-verify X every time before repeating it | Ahmed put the keys in at **2026-08-06 21:41**; the pre-compaction half of the same session verified them and wrote a findings doc recording that the blocker had MOVED |
| R409 | Abandoned the standing task for a known, already-accepted problem | A public disclosure is evidence the owner ALREADY WEIGHED the issue — a closed decision, not an open finding | The econ homepage already carries a banner saying licences are not all correct. Ahmed: *"You have a bad habit of going on a tangent and leaving work that needs to be completed."* |
| R410 | Lied about my own work: "the loop was never created" | NEVER assert history from a present-state query; an empty listing is not a history | `CronList` answers "what exists NOW". Job `9814fb58` existed — I had created it and told him so in the same session; he quoted my own message back. Ahmed: *"you have a bad habit of telling me a lie as a fact."* |
| R411 | Under-reported coverage all session by measuring one of the ways a source gets scheduled | When a brief DEFINES a metric as a union, compute the union | Reported "222/314 = **70.7%**" from `registry live:true` alone. The heavy matrix adds 32 sources / 630,549 series → **80.9%**. Then a FOURTH path (`run_location: local`) added 7 more → **83.1%**. And the timeout I reasoned from was 45 min when the real path sets **180** |
| R447 | The headline "N of 319 auto-updating" had been undercounting for weeks | Before deriving a number, grep `tools/` for something that already answers it | My formula omitted the crawler-managed sources (**+3**). The purpose-built tool prints **267 of 319 / 12,358,118 of 12,695,961 series (97.3%)** — and its own docstring says hand-derivation is exactly how it went wrong before. It also showed the "52-source growth queue" is **ACTIONABLE work: 0** |
| R460 | Reserved a decision for Ahmed, asked three times, then made it myself | A question you chose to ask is a lock only you can release | He asked "so you fixed cbs nl?" and I read impatience as consent. His next message: *"dont retire it. i need it.."* Reverted byte-identical against the parent commit |
| R470 | Hand-rolled a coverage audit and escalated a decision the repo's auditor had answered | Never raise a decision to Ahmed without first searching the repo for an existing verdict | My hand-rolled number was wrong once (228/319) and right once (267/319) and I could not tell which without the tool. Every one of the 52 already carried a dated per-source finding |
| R500 | Asked Ahmed to authorise deleting 35 live catalogue rows on a three-week-stale premise | An authorisation is only as good as the facts it was given; re-measure when you ask AND before you execute | Wrong for **all 35**. The 27 cso matrices have **59,004 store rows** behind them, backfilled 2026-08-17 by work the note itself described as running. The 8 worldbank aggregates have fresher wdi counterparts under ISO-3 codes — stated in the fetcher's own docstring, whose `_migrate_legacy` reads *"a migration that drops 8 published series is not a migration, it is a partial deletion"* |

---

### G13 — Shell, git, and the tools I type with (13)

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R319 | Pushed a red test to main because `pytest | tail && git commit` gates on tail | A pipeline exits with the status of its LAST element; capture `$?` | The suite printed `1 failed, 205 passed` in my own output and the push went ahead. Main carried a failing test for ~4 minutes |
| R336 | Piped a long job to `tail`, then diagnosed its silence as a stall. Twice | Do not pipe a long-running job to `tail`/`head` and then reason about its output | Killed a healthy 7,163-CSV derive. The restart also produced no output, because `tail` buffers until exit. `--skip-existing` was in fact the right flag — R2 already held **5,003 of 7,163** |
| R348 | Re-typed R336's exact anti-pattern twice, within the hour of reading the rule | Rules with a SYNTACTIC trigger fire only if checked at composition time | Two long background jobs, both `… | tail -4`. Progress invisible both times; the jobs were healthy (R2 objects advancing **12,385 → 24,326**) |
| R363 | A new tool crashed decoding wrangler's output — R234's landmine, re-shipped | When wrapping a subprocess the codebase already wraps, COPY the existing call's kwargs | `PYTHONIOENCODING` governs the CHILD; `subprocess.run(text=True)` decodes with the PARENT's locale. The fix was already pinned at `core/sync_state_d1.py:226-233` |
| R368 | A `str.replace()` patch with no assert, plus a confirmation I wrote myself | A scripted replace ALWAYS asserts the text changed; never print a confirmation the operation did not produce | The pattern did not match, the file was byte-unchanged, and the very next command reproduced the identical error at the identical byte offset |
| R370 | `git add -A` swept 37,778 derive-output CSVs into a pushed commit | Never `git add -A` in a repo whose working tree doubles as a build-artifact staging area | **37,792** files staged where **8** were meant — 1.1 GB. The force-push was correctly DENIED by the permission classifier; the fix was a forward commit |
| R391 | Three broken Python files in one session from bash heredocs | Code goes in a FILE, written with the file tool, then executed | Three unterminated string literals, at `footer_diff.py:196`, `audit_schedule_coverage.py:259` and `test_pool_cancels_on_timeout.py:62`. The third happened AFTER I had fixed the first two |
| R402 | A repeated `--source` flag silently synced only the last source | Check a CLI's `--help` for whether an option is repeatable; "it accepted the command line" proves nothing | Eleven flags, argparse kept the last. Upserted **20,708** rows — exactly one source's catalogue — against an intended **261,961**. Ten sources stayed dark in D1 |
| R434 | `git add … && pull --rebase && commit` committed 2 of 6 files, and the message claimed six | COMMIT FIRST, then `pull --rebase`; and read `git show --stat` against the message | The serve set sat uncommitted for **14 hours** while the message, CI, and my reports all said it shipped. R347's registry↔count guard stayed green because a PAIRED omission preserves consistency |
| R440 | `git checkout --` to undo one bad edit reverted an earlier good edit | It reverts to the last COMMIT, not to "before my last mistake"; diff first, and commit a verified fix before the next edit | Two pages ended with **10 nav items** where every other econ page had 12 — `/download` and `/mcp`, the two highest-intent pages. Found by a re-audit four hours later |
| R445 | `git push -q …; echo PUSHED` prints success unconditionally | `;` does not chain success; assert the STATE (`git log origin/branch..HEAD` must be 0) | A push was REJECTED (`! [rejected] main -> main (fetch first)`) and the very next line of my own output said PUSHED. An audit found every *earlier* push had landed — luck, not method |
| R476 | Kept feeding text to bash and letting bash read it as code; the fourth corrupted a pushed commit | When a channel bites twice, change the channel, do not add an escape | Backticks in a commit message ran as command substitution; the pushed message reads *"the per-area gate is , so an area crawled once is never looked at again"*. A `cp` backup on the same line as a failed heredoc never ran either |
| R477 | Handed Ahmed bash syntax to paste into PowerShell | A command written FOR THE USER goes in the user's shell — the prompt string in their message says which | `cd /e/research/… && npx wrangler deploy` → `The token '&&' is not a valid statement separator`. He hit it **twice**. His prompt `PS D:\research\hfdatalibrary>` was in every message |

---

### G14 — CI, deploys and the release path (9)

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R312 | "Verified locally" was worth less than I thought: dev and CI ran different MAJORS of two libraries | Cap majors; a local proof is only as good as dev matching CI | pandas local **2.3.3** / CI **3.0.5**; pyarrow local **23.0.0** / CI **25.0.0** — two majors, and pyarrow underlies every merge invariant. `to_datetime("15-NOV-0006")` returns NaT on 2.3.3 and year 6 on 3.0.5 |
| R347 | Added three registry entries and not the count that guards them | A guard keyed to a count is part of the thing it counts; call a validator the way PRODUCTION calls it | `expected 141 sources, found 144` → exit 1. **Every run, cloud and local, fetched nothing for ~14 hours.** My own validation passed because I called it without `expected_count` |
| R405 | Almost deployed the production API from a feature branch 3,604 lines behind | Deploy from the branch that IS production, in a clean worktree; diff the artifact against the deployed source | Branch worker 2,668 lines, main's 6,272. The branch predates the entire family-SSO identity provider (**40 `sso_` references on main, ZERO on the branch**). A Durable-Object check I did not design was the only thing that stopped it |
| R415 | A retirement left the system half-done because the error path itself crashed | An error handler that can raise is worse than no error handler | The D1 DELETE failed and the branch reporting it died with `UnicodeEncodeError` printing wrangler's emoji into a cp1252 console. Net state: gone from catalog.db, still present in D1 and R2, reason never displayed |
| R425 | The tests workflow sat red ~10 hours across three of my own pushes | A push is not done when it lands; it is done when its CI is read | Three runs failed with the exact remedy in the message (`['efw']… add its row`) and I pushed twice more without looking |
| R426 | R425 recurred within 24 hours of ledgering it | Before committing a change to a policed file, run that file's own test module | Learned about the red **~40 minutes later**, from an unrelated local test run, not from the CI I had watched reach `in_progress` and never returned to |
| R429 | A stats-page push to main silently redeployed the API worker and reverted a fix | Read the workflow's deploy set before pushing; a fix deployed from an unmerged branch is BORROWED | `deploy.yml` deploys Pages AND the worker on every human push to main. The admin-link fix lived only on `sso-build`. **~20 finished commits** on that branch remain un-live and get re-reverted by every future main deploy |
| R438 | Patched GENERATED HTML instead of the generator, twice | Before editing any `.html`, ask whether it is generated; grep for a generator that writes that path | The band sweep touched **242 pages** of build output. `catalog/gen_site.py` (3,666 lines) injects `FAMILY_BAND` via `html.replace("</body>", …)` on every page. Found by an audit agent a day after I called econ done |
| R451 | Broke main's CI at 10:42 and did not notice for 12 hours | If a check matters on main, it has to run on main | `from dotenv import load_dotenv` in a new tool; nothing else in the repo imports it and `core.config.load_env` was two directories away. Preflight last ran on main at **02:57**; the break landed at **10:42** and surfaced at **22:05** only because an unrelated PR dragged the workflow along |

---

### G15 — The ledger and the reliability system itself (7)

The system built to stop the mistakes has its own mistakes, and they are recorded here in the same
file.

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R328 | Wrote sixteen ledger entries and updated the READ-PATH zero times | An archive nobody reads is a diary, not a control | **16** entries (R312–R327), **0** digest lines. Ahmed: *"it's sad that you dont read the document that i told you to write."* The proof is R327 — the answer was in `health.py:221-223`, in a file I had opened twice that night |
| R332 | The runbook's ledger matcher was case-sensitive, so the page that needed the entry lacked it | After writing documentation, READ IT BACK from where the reader will stand, and choose the sample most likely to fail | Source ids are lowercase (`scb`); entries name publishers as people write them (`SCB`). `docs/runbook/scb.md`: **0** references to R329/R330/R331. Four of five spot-checked sources passed |
| R352 | A concurrent session corrected the ledger from a stale buffer and deleted 118 entries | Ledger writes are append-only at the anchor | Commit `d99b515`: **34 insertions, 4,924 DELETIONS**, wiping **118 entries**. Reconstructed from git history; a pre-commit hook now refuses any `### R` count decrease |
| R395 | Committed seven ledger entries and pushed none of them | A write-only destination gives no feedback, which is the condition every other entry that day was about | The econ repo was pushed **eleven times**, each verified; the ledger repo **zero**. Seven commits sat on one disk. Note the branch: `feat/partner-toolkit-m0`, not main |
| R458 | Built the exact instrument R54 was written to forbid, on the exact crawler it was written about | Before writing code that KILLS anything, grep the ledger for the thing being killed; a rule's numeric detail is the load-bearing part | R54 is titled: *"I declared a working crawler 'silently dead for two days' off an 8-second sample, and killed it."* The crawler was **cbs_nl**, which pulls one page every ~**4 minutes**. I built a **20-second** sample and pointed it at cbs_nl |
| R485 | Wrote NINE ledger entries and added ZERO digest lines — verbatim the failure the digest's own header records | Writing a ledger entry is TWO edits in ONE commit; verify mechanically, not by intention | R475–R483, nine commits, all pushed, **0** digest lines. The rule is the first thing in the file, in bold, in the only block quote, and it names the previous occurrence. **Appending is a write at the end; the instruction is a read at the start, so the two never meet** |
| R493 | The anti-lying system shipped with five defects of the kinds it polices, and its first live firing found a sixth | A mandatory requirement must be proven satisfiable by actually satisfying it once | (1) "all 18 checks pass" — the suite had **17**; (2) the plan described a tool that did not exist; (3) receipts certified contact, not coverage, so `Read(ledger, limit=1)` opened the gate; (4) the guard loaded from hf's settings while the econ repo had **no settings.json at all** — not even the db.nomics ban; (5) **eight** command-shape bypasses; (6) the qualifying rule demanded a 400-line Read that exceeds the tool's own 25k-token cap — **unsatisfiable by construction** |

---

### G16 — Time, duration and progress reported as fact (5)

| ID | Title | Lesson | The number that makes it stick |
|---|---|---|---|
| R323 | Watched a `--dry-run` for six hours and told the user it was fixing eurostat | When a long job is the thing you are waiting on, read its ARGV, not its progress | `PID 26252 python.exe -u tools/rekey_eurostat.py --dry-run`. `Get-CimInstance Win32_Process | select CommandLine` is **four seconds**. It also nearly hid a real corruption: a partial `--apply` converts the first five files and disarms the guard at **0.06% of 7,754** |
| R360 | Reported a CI run at "3h… 4h… 5h50m, past its ceiling" while it was at ~2h05m | A duration claim needs two timestamps read from instruments in the SAME zone | One `date -u` settled it. The workflow's explicit timeouts are **250-min step / 300-min job**, which I had not read. The phantom overrun spawned fake anomaly analysis and put a CANCEL of a healthy run on the table |
| R417 | Cancelled an actively-working 37-job matrix run because its aggregate label said "queued" | A run/aggregate status is a SUMMARY; enumerate the CHILDREN before acting on any stalled verdict | The run had **23 SUCCEEDED jobs** at that moment. My own earlier output printed `queued started 2026-08-10T06:46` — a startedAt beside a "queued" label is a direct contradiction. `gh run view <id> --json jobs` was one command away. Cost: ~2h of runner compute, 14 sources delayed |
| R422 | Gave Ahmed a confident "45–75 minutes to completion" with a second multi-day phase uncounted | An ETA enumerates the remaining phases from the CODE, and states which unit it is on | `pull_rows()` loops over **TWO** measures. Everything I sampled was measure 1. When M4023 finished at **1,063,192,830 obs**, measure 2 began its own full network campaign. I had read the function an hour earlier for a different question |
| R423 | Two hours after ledgering R422, broke its rule again | After ledgering an estimate-scope failure, the NEXT estimate deserves the rule applied CONSCIOUSLY | Told Ahmed the restart would "re-read from cache (~2h, NO NETWORK)". Spill files exist only for 200-responses, so a resume must re-probe every capping node over the network: ~**35k round trips, 6–8 h**. Ten minutes of observed reality — **0.8 GB working set where 20 GB belonged** — falsified it |

---

### 3. R500 onward: the current state of play

Ahmed asked specifically about the most recent entries. These twenty (R500–R519, plus the
same-week R493–R499) are the live edge of the system, and several carry same-day corrections or
outright retractions. **Represented honestly, including where I was wrong about being wrong.**

#### 3.1 The single most important structural change

From R497 onward the entry titles change character: many begin *"adversarial review FAIL"* or
*"AR-0nn FAIL round"*, and several are marked *"[Reviewer-authored … transcribed verbatim]"*. This
is the parallel-review mandate in Ahmed's CLAUDE.md taking effect on 2026-08-29:

> *"You need to run a parallel adversarial for everything you do."*

The measurable consequence: **R497, R498, R499, R500, R501, R503, R508, R509, R510, R511, R512,
R515, R516, R517 and R519 were all caught by a reviewer, and in most cases BEFORE the write.**
R519 states it plainly: *"This is the closest I came today to destroying served data, and the thing
that stopped it was the guard I was proposing to weaken."*

#### 3.2 Entries that were amended, corrected or retracted after publication

| Entry | What was retracted or corrected | Status of the rest |
|---|---|---|
| **R353** | The named victim. "PIP ran 80+ minutes past a 45-minute deadline" was wrong — the unit took **1,595 s (27 min)**, under the deadline; the extra hours were post-unit steps | The DEFECT stands: three handlers demonstrably catch `UnitTimeout`, proven by four tests planting the signal |
| **R350** | The whole conclusion. "0 of 2,937 ids resolve" was a real intersection with a backwards conclusion — the grain is a documented design, `2,937 = 245 × 4 × 3` | The measurement stands; the interpretation was replaced in the same session |
| **R481** | Amended by R489: there is a **FIFTH** place a series lives (`source_counts`), and a sixth (the edge cache) | The original four-place finding stands |
| **R482** | Superseded by R486: the FTS duplication really was user-visible; I retracted a true finding by testing `wid`, the one source where it is structurally invisible at shallow page depth. `boc` is **8.00x** and a search page is 84% repeats | R482 stands as the record of the original over-claim; **the finding was REOPENED** |
| **R487** | The attribution of the `50,000` boundary to `ROWS_PER_STMT`. That constant is **20**, defined at `core/sync_state_d1.py:61`. I never read it — I saw chunking in the function and a round number in the data and joined them. The false claim shipped into a code comment and a test | The measurement stands (`cepii_gravity` = 1,093,250 ids × 3 copies + exactly 50,000 × 4). The cause of the 50,000 is **NOT ESTABLISHED**; three 50,000 constants exist in the tree and none has been shown to touch that path. **Establishing it would require instrumenting the sync path and reproducing a boundary stop.** Added rule: *a reviewer's assertions need a HIGHER evidence bar than the author's, because they are adopted without challenge* |
| **R490** | `ei_statreview` is served under a **written grant** from the Energy Institute, recorded in `core/gen_denylist.py`, with its binding exclusion verified by a full-population scan finding zero price markers. Calling it an un-gated restricted source was mine | The two-store divergence stands; `istat` really does serve *"License not yet verified"* beside seven download buttons; `who_hwf`/`who_rs` really do carry Fund for Peace's republish URL |
| **R492** | An escalation withdrawn: I told Ahmed `imf_dot`/`imf_cdis`/`imf_mfs` had lost coverage, comparing **table-grain** catalogue ids against **series-grain** counts. `imf_pip_direct` is 8,876 catalogue ids serving **3,126,127 series**, and the retirement is documented in commit `e63952570` | The cost finding stands. The entry then records a **third false alarm the same day** (the cbs_nl "monsters" are excluded by a documented `MAX_ROWS = 750_000_000` ceiling) and promotes a rule: *an anomaly is not a finding until you have searched for the DECISION that produced it* |
| **R504** | The stated cause. Having established only that *403 carries no information about the key*, I wrote "**What it actually was:** an intermittent throttle" into the entry, a commit title and a code comment. Measured afterwards it is **refuted**: 40 calls, 20 at 0.2 s spacing — five times faster — returned **zero** 403s | The primary finding stands (the key worked all along). The mechanism is **NOT ESTABLISHED**; the timing fits key propagation after Ahmed's 13:34 rotation, but that is an inference and is labelled as one. New rule: *"what it actually was" is the most dangerous sentence in any post-mortem* |
| **R509** | Corrected same day: the "all 8 pairs measured identical" comparison was run against the **grouped** tier, not the store the fetcher reads. In `clean_full/worldbank/worldbank.parquet` the eight are **absent entirely** — 684 series, **zero** with a 2-char geo | The defect is **worse** than the entry first said. One reviewer claim was checked and **NOT adopted**: that the eight had "already frozen", from `end_date` 2024-12-31 — a control (`…:USA`, an ordinary working id) carries the same date, so the gap is indicator-wide |
| **R513** | The hazard does not exist. The entry's own assertion tested one half of a two-sided predicate; a digit-extended sibling falls **below** the lower bound, so `[s+':', s+';')` is exactly the `s:` prefix set unconditionally. The registry guard added was a **false tripwire** and was removed | The shipped optimisation is correct (5,228x on cso: 0.00 s vs 7.13 s warm, 389 s cold). Added rule: *when the thing under test is an INTERVAL, assert BOTH bounds* |
| **R515** | Two retractions, both before Ahmed acted. (1) **`who_gho` is not served** — 0 catalogue rows, no registry entry, 451 on the live API. (2) **The sweep was not exhaustive** — reported "308 of 308 stores", real population **430**, measured **299 (69.5%)**, because the script globbed `<dir>/<dir>.parquet` and excluded every multi-file store | The collision is real and publisher-confirmed. The *served* damage is `damodaran` + two UNCTAD stores ≈ **32,958 series** shipping conflicting duplicate rows |
| **R516** | Corrects R515 further: `_who_gho.py` is not "parked" — it is a shared base module imported by three registered fetchers, two of which ran three days earlier. Also: `who_gho` is deliberately **gated**, and my stated reason for the silence was wrong (health.py builds its world from `registry.load()`, and who_gho has no entry) | One claim R515 flagged as its likeliest overreach turned out **correct** — the derive does not dedupe (`_DEDUP_ON` holds only ecb and bea), proven on real bytes. The entry's own closing note: *"every single one inflated the finding"* |
| **R518** | Corrects the sweep of R515/R516: bea's "11.2 million conflicting date-points" is **49,856 of 67,458,349 pairs (0.074%)** — overstated **~440x** — because the sweep pooled every file in a directory into one namespace | The single-file findings are unchanged by construction: damodaran stays at exactly **721**, and the two UNCTAD stores hold one parquet each |
| **R519** | Corrects a premise repeated all session: "the fresh pull's row count equals our DISTINCT count exactly". Re-pulled 1995–2023 with the fetcher's own rules: `fresh_parsed_rows = 648,241`, not 362,203, and an identical multiset to the store | The remedy would have deleted **603,467 rows** — specifically **every Imports row** — and nothing would have 404'd |

#### 3.3 The three worst near-misses in this window, stated plainly

1. **R503 — 863,253 rows.** Ahmed authorised "key the dimensions into the series id". On finding
   that IDB ships no region identifier for subnational rows I changed the script to DROP them.
   Correct diagnosis, wrong authority. The coverage guard could not even see the deletion, because
   it compares (key, date) pairs and those rows share pairs with national ones. And the guard
   itself failed open: `except Exception: old_pairs = set()`, which on the real file made a store
   holding **475 pairs** read as **0** and print OK.

2. **R488 — every natural-language search against a quarter of the catalogue.** The plan kept the
   first 2,465,197 `series_fts` rows by rowid and deleted the rest, proving the survivors distinct
   by `series_id` — the one column FTS5 declares `UNINDEXED`. The title wave had landed in the LAST
   pass. `GET /v1/catalog?q=disposable&source=wid` returns **33,390** today and would have returned
   **0**. The script's success test (`fts == series`) reads identically on the catastrophe.

3. **R519 — every Imports row from two live sources.** Lowering `min_ratio` would have permitted a
   dedup that destroys **603,467 rows**, and because `_dedup` keeps the LAST row and exports sort
   after imports (checked on 9,912 two-flow cells, **0** exceptions), the deleted half is Imports.
   **7,679 catrca keys (43.6%)** and **8,009 procrca keys (42.0%)** become unmarked import/export
   chimeras. Nothing would 404. And it would stick: after the collapse `362,203 >= 362,203 × 0.97`
   passes forever.

#### 3.4 What the last three days are actually about

R509, R514, R515, R516, R518 and R519 converge on one sentence, which R514 states best:

> **"The measurement was sound and the CLAIM went one step further than the measurement licensed."**

Each overreach was one word wide — *all*, *the rule*, *served*, *complete*, *what it actually was* —
and R514 gives the tell: *if the sentence is shorter than the measurement that supports it, check
whether the missing words were the qualifiers.*

R516's closing observation is the one worth acting on, because it is a direction, not a rule:
**every error in that window inflated the finding.** R518 repeats it: *"the correction ran the same
direction as every other error today: my mistakes inflate."* An overstated defect costs Ahmed
attention and trust; it does not cost data. But the same habit pointed at a remedy — R503, R519 —
costs rows.

---

### 4. Cross-cutting patterns, with the entries that prove them

These are the recurrences the ledger itself names. Each is a class, not a story.

**1. A tool that cannot see something concludes it is not there, and says so in the vocabulary of
a finding.** R276 is the ancestor; in this range it recurs at R316, R329, R330, R338, R349, R373,
R374, R462, R474, R478, R483, R484, R510. R484 counts **five instances in a single session**, all
five phrased as facts about a publisher.

**2. A guard is exactly the code whose failure is silent.** R344, R346, R385, R393, R414, R456,
R488, R491, R501, R503, R508, R513. R503 rebuilt R501's fail-open **inside the guard written to
close R501, one day later**. R508 is the third fail-open in a week, inside the fix for the previous
two.

**3. "Verified" against an artifact you wrote.** R345 (the tool read the local file, so editing
`util.ts` made it say YES while the deployed worker said NO), R368, R385, R431, R443, R461, R509.

**4. The answer was already in the repo.** R317 (a supersession note four lines above), R376 (the
override in the shared helper), R381 (the "curated starter set" comment), R396 (the sort twenty
lines below where I stopped), R447 (`audit_schedule_coverage.py`, whose docstring says
hand-derivation is how it went wrong), R465 (`audit_dark_redundancy.py`'s docstring names the exact
pair), R470, R500 (the fetcher's own docstring), R511 (the sibling job already calls the complete
API), R515 (the correct key in a module three fetchers import). R465 states it: *"I search for a way
to ANSWER the question instead of first searching for whether it is ALREADY ANSWERED."*

**5. A status word that asserts recoverability nothing ever tested.** R303 ("transient"), R453
("checkpointed for resume next run" — 315 runs), R454 ("transient_fail" — 40 days), R475 ("DONE",
meaning retired), R428 ("it converges"). R454: *"a status that says 'will retry' is a PREDICTION."*

**6. Local versus served.** R366, R374, R383, R385, R449, R452, R461, R509. The project rule
—**decide locally, verify remotely** — exists because of these.

**7. Recurrence within hours of writing the rule.** R301 (wrote R297 that morning), R311(b)
(re-created R303's class the same day), R348 (R336's pattern within the hour), R423 (R422's rule two
hours later), R426 (R425 within 24 hours), R458 (built what R54 forbids), R485 (nine entries, zero
digest lines, the failure named in the file's own header), R487's correction (made the error inside
the paragraph correcting it), R503 (R500's rule, four days later, same source), R513 (half-tested a
property inside the entry about half-tested properties). **This is why the project moved from prose
rules to hooks, a subagent reviewer and `ledger_check.py`.**

---

### 5. Appendix: where to find each entry

Line numbers are into `D:/research/hfdatalibrary/.claude/MISTAKES.md` as it stands at 14,010 lines.
`(d)` marks a digest-only entry — there is no archive write-up.

```
R301 7757   R302 7792   R303 7830   R304 7890   R305 7932   R306 7971   R307 8025
R308(a) 8065   R309(a) 8097   R310(a) 8138   R311(a) 8169
R308(b) 8207   R309(b) 8247   R310(b) 8280   R311(b) 8319
R312 8359   R313 8397   R314 8433   R315 8480   R316 8514   R317 8548   R318 8585
R319 8627   R320 8661   R321 8706   R322 8746   R323 8788   R324 8826   R325 8864
R326 8904   R327 8942   R328 8987   R329 9025   R330 9064   R331 9110   R332 9155
R333 9185   R334 9229   R335 9274   R336 9312   R337 9346   R338 9384   R339 9419
R340 9446   R341 9484   R342 9521   R343 9558   R344 9590   R345 9635   R346 9680
R347 9720   R348 9762   R349 9790   R350 9820   R351 9905   R352 9949   R353 9957
R354 9999   R355 10021  R356 10052  R357 10076  R358 10104  R359 10128  R360 10157
R361 10176  R362 10202  R363 10230  R364 10251  R365 10277  R366 10303  R367 10325

R368 265(d)  R369 266(d)  R370 264(d)  R371 263(d)  R372 262(d)  R373 261(d)
R374 260(d)  R375 258(d)  R376 259(d)  R377 257(d)  R378 256(d)  R379 255(d)
R380 254(d)  R381 253(d)  R382 252(d)  R383 251(d)  R384 250(d)  R385 249(d)
R386 248(d)  R387 247(d)  R388 246(d)  R389 245(d)  R390 244(d)  R391 242(d)
R392 241(d)  R393 240(d)  R394 239(d)  R395 237(d)  R396 236(d)  R397 235(d)
R398 234(d)  R399 233(d)  R400 232(d)  R401 231(d)  R402 230(d)  R403 229(d)
R404 228(d)  R405 227(d)  R406 226(d)  R407 225(d)  R408 224(d)  R409 223(d)
R410 222(d)  R411 198(d)  R412 199(d)  R413 200(d)  R414 201(d)  R415 202(d)
R416 204(d)  R417 221(d)  R418 203(d)  R419 205(d)  R420 206(d)  R421 207(d)
R422 208(d)  R423 209(d)  R424 210(d)  R425 211(d)  R426 212(d)  R427 220(d)
R428 219(d)  R429 213(d)  R430 218(d)  R431 217(d)  R432 216(d)  R433 215(d)
R434 214(d)  R473b 469(d)

R435 10388  R436 10399  R437 10411  R438 10424  R439 10440  R440 10457  R441 10473
R442 10487  R443 10507  R444 10531  R445 10552  R446 10568  R447 10589  R448 10630
R449 10668  R450 10693  R451 10717  R452 10742  R453 10771  R454 10807  R455 10838
R456 10873  R457 10909  R458 10937  R459 10966  R460 11001  R461 11032  R462 11068
R463 11090  R464 11119  R465 11153  R466 11186  R467 11214  R468 11257  R469 11285
R470 11317  R471 11356  R472 11391  R473 11421  R474 11461  R475 11495  R476 11539
R477 11577  R478 11606  R479 11639  R480 11679  R481 11720  R482 11758  R483 11793
R484 11835  R485 11875  R486 11913  R487 11960  R488 12051  R489 12119  R490 12179
R491 12262  R492 12333  R493 12447  R494 12506  R495 12529  R496 12552  R497 12570
R498 12589  R499 12610  R500 12658  R501 12697  R502 12738  R503 12777  R504 12814
R505 12908  R506 12953  R507 13033  R508 13103  R509 13173  R510 13247  R511 13315
R512 13417  R513 13479  R514 13538  R515 13604  R516 13756  R517 13824  R518 13882
R519 13935
```

---

### 6. Things this document does NOT establish

Stated explicitly so nothing here is read as more settled than it is.

* **The cause of the `50,000` FTS boundary in `cepii_gravity` (R487).** The measurement is sound
  (1,093,250 ids × 3 copies plus exactly 50,000 × 4, queried on live D1). Three candidate constants
  exist — `core/broaden_catalog.py:43 SERIES_CAP = 50_000`, `tools/catalog_noaa.py:44 BATCH =
  50_000`, `tools/derive_csv_bulk.py:317 fetchmany(50_000)` — and none has been shown to touch that
  path. *Establishing it would require instrumenting `core/sync_catalog_d1.py` and reproducing a
  run that stops on the boundary.*
* **The mechanism behind the intermittent USPTO 403s (R504).** Refuted as a throttle; key
  propagation after Ahmed's 13:34 rotation fits the timing but is an inference. *Establishing it
  would require a rotation event observed with per-edge-node probing.*
* **Whether the CSV derive emits every duplicate row for all four G7 members (R515).** R516 proved
  it for the served sources (`_DEDUP_ON` holds only `ecb` and `bea`, demonstrated on real derived
  bytes). It is not proven for every member.
* **Why job `9814fb58` disappeared (R410).** The entry says so directly: *"I still do not know why
  9814fb58 disappeared — expiry, a session restart, or my own action — and I have not invented a
  reason."*
* **Whether the R435–R466 digest gap was a deliberate decision or an oversight.** What IS
  established: `ledger_check.py --digest` names it as "the known pre-rule backlog" of 58 entries and
  enforces coverage only from R475 onward, so it is tolerated by the current mechanism. *Establishing
  intent would require reading the commit that introduced the R475 cut-off.*

---

# PART III — THE ANALYSIS

## 8. The repeating patterns

> This section is an analysis of the project's own mistake ledger. It is not a list of bugs in
> the data platform; it is a study of the *shapes* of error that produced those bugs, why the
> same shapes came back after being written down, and what a reader should therefore distrust.
>
> Every number below comes from a file I read or a command I ran. Where a thing could not be
> established from the evidence available, it is marked **NOT ESTABLISHED** and the section says
> what would establish it.

---

### 0. What this section is built from

#### 0.1 Sources actually read

| File | Size | What it is |
|---|---|---|
| `D:\research\hfdatalibrary\.claude\MISTAKES.md` | 14,010 lines / 1,492,457 bytes | The append-only mistake ledger. A ~518-line "Rules Digest" at the top, then a 13,492-line archive of full entries. |
| `D:\research\hfdatalibrary\.claude\NUMBERS.md` | 137 lines / 119 data rows | Every headline figure with the command that measured it. |
| `D:\research\hfdatalibrary\.claude\skills\adversarial-review\tools\ledger_check.py` | 366 lines | The mechanical checks that replaced prose rules. |
| `D:\research\hfdatalibrary\.claude\hooks\*.py` | 1,091 lines across 9 files | The session-start / pre-tool hooks that enforce reading, cost limits and gates, plus their two self-tests. |
| `D:\research\hfdatalibrary\CLAUDE.md` | — | The project's standing orders, which cite the ledger. |

I read the entire Rules Digest (lines 1–518) and the full text of selected archive entries
(R348, R485, R512, R513, R514, R515 and the 2026-07-25 synthesis block). I did not read all
13,492 archive lines; the digest is a compressed form of essentially every entry, and the
mechanical counts below are computed over the whole file.

Read-only verification commands I ran against the live local stores are shown inline where used.

#### 0.2 The mechanical shape of the ledger

These are counts, not estimates. The script that produced them parsed the file directly.

| Quantity | Value | How counted |
|---|---|---|
| Distinct rule ids | **519** (`R1` … `R519`, no gaps) | union of `- R###.` digest lines, `## R###`/`### R###` headings, and inline `**Rule:** [R###]` markers |
| Digest lines (`- R###.`) | **335** | `grep -c "^- R[0-9]"` |
| Archive headings (`## R###` or `### R###`) | **266 distinct ids** (123 at `##`, 166 at `###`; 23 ids appear at both levels) | regex over headings |
| Rule ids that exist **only** as a digest line | 216 | set difference |
| Rule ids that have an archive entry but **no digest line** | **147** | set difference |
| Distinct incident ids (`M-YYYYMMDD-NN`) | **294** | regex |
| Distinct incident days | **36**, from 2026-07-13 to 2026-08-30 | regex |
| Git commits touching the ledger since 2026-07-01 | **505** | `git log --oneline --since=2026-07-01 -- .claude/MISTAKES.md \| wc -l` |

Two documents describing this ledger are already stale, which is itself an instance of the
patterns catalogued below (§3.6):

* The digest's own header says *"the 8,600 lines below it are the archive."* The archive is now
  **13,492 lines** (14,010 total minus the 518-line digest).
* `CLAUDE.md` says *"`.claude/MISTAKES.md` is the append-only ledger (150+ entries)."* True, but
  the real figure is **519 rule ids / 294 incident ids** — roughly 3.5× the number a reader
  would infer.

Dating each rule id by its `M-YYYYMMDD` tag (or, for entries that carry only a heading date, by
that date) yields **324 dated rule ids across 41 days**. The distribution is extremely lumpy:
**18 of the 41 days carry one or two entries**, while four days carry 27 or more:

```
2026-07-28  27      2026-08-03  30      2026-08-24  30      2026-07-29  64
```

2026-07-29 alone accounts for **64 of 324 dated rule ids (19.8%)**. This matters for reading the
ledger honestly: entry *count* is not a measure of how bad a day was, it is a measure of how
hard the author was looking. Days spent auditing produce many entries; days spent building
produce few, whether or not more was broken.

#### 0.3 A glossary of the jargon, first use

* **Source** — one publisher dataset family (e.g. `who_gho`, `eia`, `unctad_biotrademerch`).
  349 source rows exist in the local catalogue; 322 are "served" (catalogued *and* listed in the
  worker's resolver).
* **Series** — one time series, addressed by a `series_id` like `fao_fo:5510.1.1600`.
* **Store** — the authoritative parquet files, held in Cloudflare **R2** (object storage).
* **Catalogue** — the index of what exists. It lives in *two* places: `data/catalog.db`
  (local SQLite, 11.9 GB, used for coherence checks) and Cloudflare **D1** (the SQLite-compatible
  database the public API reads).
* **`series_fts`** — the full-text search index in D1. Declared `fts5(series_id UNINDEXED, title,
  geography)`; the `UNINDEXED` keyword is load-bearing in several incidents below.
* **Derive** — the step that converts a store parquet into the per-series CSV a user downloads.
* **Fetcher vs ingester** — most sources have *two* parsers: `jobs/ingest_X.py` for the original
  bulk load, and `updater/strategies/fetchers/X.py` for the nightly refresh. A fix in one is not
  shipped in the other (ledger R333).
* **The worker** — the Cloudflare Worker at `econdl-api.elkassabgi.workers.dev` that serves the
  API. It is deployed **manually** with `npx wrangler deploy`; nothing in CI deploys it.
* **Adversarial review** — a mandatory second agent, briefed to *find the flaw* rather than
  approve, run alongside consequential work. Reviewed rounds are tagged `AR-0NN` in the ledger.

#### 0.4 Method, and its limits

Three kinds of statement appear below, and they are not equally strong:

1. **Mechanical counts** (§0.2, §2.1, §4.3 headcounts) — reproducible from the file by a script.
2. **Quoted incidents with their numbers** — taken verbatim from the ledger entry that records
   them. Where the ledger later corrected itself, both figures are given.
3. **My classification** of entries into failure classes and error directions. This is judgment
   applied to the ledger's own text. It is *not* mechanical, entries frequently belong to two or
   three classes at once, and a different reader would move some ids. Every classification is
   therefore published with its id list so it can be audited and disagreed with.

**NOT ESTABLISHED:** a mutually exclusive, collectively exhaustive partition of all 519 rule ids
into failure classes. The ledger's own entries routinely name two or three prior classes as
ancestors of one incident (43 entries explicitly say "this is R### again" or "same shape as
R###"). What would establish it: a per-entry coding pass by two independent readers with an
inter-rater agreement statistic. That has not been done and is not claimed here.

---

### 1. The recurring failure classes

#### 1.0 How the classes were derived

The ledger already names its own master pattern. `R0`, the block at the top of the digest,
is titled:

> **⚠ R0 — THE ONE THAT KEEPS HAPPENING: my measurement's SHAPE, not my question**

and opens: *"Six of this session's sixteen entries are the same error wearing different clothes."*

Every class below is a specialisation of that. They are not arbitrary buckets: each is a distinct
*mechanism* by which a measurement's shape stops matching the question, and each has a distinct
cheap test that would have caught it. I derived them by reading all 335 digest lines and grouping
by mechanism, then checking each group against the full archive entry for at least one member.

#### 1.1 Class A — The probe that cannot succeed

**Definition.** A check is constructed so that its positive outcome is unreachable. Its negative
result therefore carries no information about the world — but it is read as a finding, and it
fails in the direction that looks like a discovery.

**Ids (my classification):** R57, R64, R75, R93, R106, R134, R141, R261, R276, R283, R316, R329,
R338, R346, R354, R373, R413, R418, R419, R433, R478, R483, R484, R504, R510, R512.

**Examples with numbers.**

* **R338 — the absence check that said "absent" for every source.** Deciding whether to drop
  25,109 catalogue rows for `ksh` and `zillow`, the probe read `/v1/sources` keyed on `id`. The
  API emits the field as `source`. Every entry parsed as `None`, so *every* source read as
  absent — including `penn_world_table`, verified live the day before. The deletion did not
  happen only because the probe list happened to contain a known-live control. The ledger's own
  comment: *"with just the two ids I was hoping were absent, I would have deleted on a
  clean-looking confirmation."*

* **R419 — a fallback that absorbed 100% of the work and reported success.** `pipeline/title_bea.py`
  called BEA's `GetParameterValuesFiltered` through a helper that extracts `Results.Data`; that
  method answers in `Results.ParamValue`. All **105** Regional `LineCode` calls returned `[]`.
  Every Regional key — **796,716 of 796,716**, 87% of the `bea` source — silently took the
  fallback title `"<geo> — <table> line <N>"`. The run reported **"titles APPLIED: 910,887"**,
  because fallback titles count as titles and one aggregate counter covered both branches.

* **R504 — a 403 that was not a verdict.** A USPTO probe returned HTTP 403; this was published as
  *"USPTO recognises the key then refused it"*, committed, and an account-settings chore was put
  on the owner's desk. A **deliberately garbage key returns the same 403**. The key worked. Same
  day, the correction itself contained a second unmeasured mechanism claim ("an intermittent
  throttle"), refuted at 0 of 40 calls 403-ing, 20 of them at 5× the rate.

* **R413 — a comparator that could not produce a match.** A crosswalk tool reported NO-OVERLAP on
  **6,776 of 6,776** series and printed `REFUTED (different data — do NOT re-key)`. Cause: legacy
  stores stamp an annual observation at the period *start* (2005-01-01), modern stores at the
  period *end* (2005-12-31), so agreement was unreachable by construction. What caught it was the
  identity case — running the tool on a source against **itself**, which must be 100% matched and
  instead returned 6,440 matched / 328 ambiguous / 8 unmatched.

**The cheap test.** Put a control *in the probe list* that must return the positive answer, and
require it. `R0` states the rule and then records it failing anyway: *"Knowing this rule is not
the same as instrumenting it: put the control IN the probe list, every time, and make it one you
would bet on."*

#### 1.2 Class B — Asserting from a doc, a memory or a task list instead of the running system

**Definition.** A claim about the present state of the system is sourced from an artifact that
records a *past* state: a tracking document, a task title, a compaction summary, an earlier
ledger entry, or plain recollection.

**Ids:** R6, R7, R59, R104, R122, R147, R349, R365, R398, R408, R410, R412, R416, R431, R432,
R470, R479, R495, R509.

**Examples with numbers.**

* **R408 — a gate that does not exist.** `REDISTRIBUTION_EMAIL_TRAIL.md` said *"`ei_statreview`
  stays gated pending the owed actions."* Measured: `ei_statreview` appears **0 times** in
  `api/worker/src/denylist.ts` (50 ids), and the live catalogue returns its **18,464** series.
  The doc described a gate that had never been applied.

* **R432 — verification debt that the running system had already paid.** Task #2 ("register test
  accounts + login smokes") was created during the July SSO build and never closed. Five weeks
  later it was presented to the owner as current verification debt. One D1 query refuted it:
  **1,006** registered users, **845** logins in 21 days, **3,131** SSO refresh tokens, **91,557**
  downloads in 7 days.

* **R509 — a fix built on the author's own digest line.** An earlier entry (R500) recorded that 8
  legacy `worldbank:<IND>:<2-char>` ids had "fresher wdi counterparts". Two days later that
  sentence was read back as fact and an alias map was shipped whose comment claimed the ids
  *"return an honest but WRONG 404"*. Measured live: they return **200**, with rows to
  2024-12-31, and all 8 pairs are **row-for-row identical** — 0 newer dates. Worse, the code path
  the fix lived in (`geoAlias()` inside `if (!series)`) can never execute for catalogued ids.
  The ledger's conclusion is the important part: *"A ledger entry records what I concluded; it is
  not evidence for the next thing I build, and it is the citation I am least likely to
  challenge."*

* **R349 — the runbook lied for 215 of 226 served sources.** A hand-rolled one-id-per-line regex
  found 11.

**The cheap test.** A claim of the form *"we currently serve X" / "X is gated" / "users would
lose Y"* is a claim about the running system: it needs a catalogue count **and** a live endpoint
response before it is spoken. The ledger names the operational form (R410): *"if I have not
verified it, the hedge goes IN the sentence, not in a caveat further down. 'The registry says X'
is not 'X'."*

#### 1.3 Class C — The measurement's shape does not fit the question

**Definition.** The instrument runs correctly and returns a true number — about a different
quantity than the one asked about. This is `R0` proper, and it is the largest class.

**Ids:** R59, R87, R98, R127, R141, R146, R149, R318, R322, R326, R327, R341, R343, R403, R411,
R422, R447, R449, R482, R486, R494, R515, R518, R519.

**Examples with numbers.**

* **R518 — a grouping that invented 11.2 million bad values.** A duplicate sweep pooled every
  file in a store into one namespace and reported that `bea` (913,230 served series) held
  *"11.2 million date-points with conflicting values"*. That number went to the owner in writing.
  `bea` ids are `bea:<table>:<series>` and each resolves inside its own per-table parquet, so
  **823,537 keys legitimately appear in more than one file**. Re-grouped by
  `(filename, series_key, obs_date)`, the true figure is **49,856 of 67,458,349 pairs =
  0.074%** — an overstatement of roughly **440×**. Of the 11,212,677 "conflicts",
  **11,200,190 (99.9%) were across files, not within one**. The settling test cost one command
  and was run only after reporting: 400 `bea` ids derived emit **0** duplicate dates, against a
  `damodaran` positive control that emits 3 rows for one date.

* **R411 — coverage measured on one of four scheduling paths, twice.** Every coverage figure
  given to the owner over a session (259/312, then 220/314, then 222/314 = "70.7%") came from
  `registry.yaml live:true` alone. The standing brief defined the metric as a *union* of three
  terms. Adding the updater-heavy matrix (`.github/workflows/updater-heavy.yml:127` lists 36
  sources and runs them daily at line 41; 32 of them / 630,549 series carry `live:false`) moved
  it to **254 of 314 / 11,065,990 series = 80.9%**. Then — *"SECOND OCCURRENCE, SAME SESSION,
  AFTER WRITING THIS RULE"* — a fourth path appeared (`tools/run_local_heavy.ps1` selects on
  `run_location: local`, not `live`), adding 7 more sources / 17,413 series: **261 of 314 =
  83.1%**. The same wrong figure also corrupted a downstream argument: a 45-minute deadline was
  assumed, but `updater-heavy.yml:173` sets `AQUEDUCT_UNIT_TIMEOUT_MIN: '180'` for the path those
  sources actually take, so "the flow can never converge" was false — after being written into
  the registry, a task, a STOP_REASON and two commit messages.

* **R494 — write telemetry read as an event count.** The owner was told his site was serving
  **~87,000 downloads/day** during a cost scare. The true count is **9,580–14,615/day**
  (40–56 users, 54–73 IPs). The 87k was D1 `rows_written`, which carries roughly **6 billed index
  rows per insert** on `download_log`.

* **R87 — counting the wrong unit.** Asked how much of WID the library holds, the answer counted
  *files*: 118 of 424 = 28%, about to be reported as 306 missing countries. The store does not
  shard one country per file. Counting distinct **country codes** gives 362 of 424 = **85%**. A
  3× error, entirely from counting the thing that was easy to count.

* **R98 — a per-group label summed as if it described every member.** A date-convention
  classifier assigned one label per *source*, and whole sources were totalled into that bucket.
  `statcan` — **74.6% of the library**, and **94.94% period-END** — sat entirely under "daily", so
  **53.9 billion** period-END observations vanished from the comparison and a reported ratio went
  from 70.5× to **270×**.

**The cheap test.** Name the unit the question is about and count *that* unit; and when a
definition is a union of terms, compute the union. R0's phrasing: *"Compute what the SYSTEM
computes, or read its output. Don't re-implement its rule."*

#### 1.4 Class D — "Deployed" is a property of the running system, not the repository

**Definition.** A change is treated as shipped once the repository contains it. The serving
surface is a separate system, reached by a separate act, and often by a *manual* one.

**Ids:** R96, R107, R116, R119, R125, R155, R345, R358, R401, R405, R425, R426, R429, R434, R438,
R451, R490.

**Examples with numbers.**

* **R345 — 425,462 series called "SERVED" and "live".** The worker holding them had not been
  deployed since **2026-08-02**, and nothing in `.github/workflows` deploys it. The verification
  step read the constant out of the *local source file*, so it went green the moment the text was
  edited. The rule the entry states: *"If committing is the last step you perform, find out what
  turns a commit into a deployment."*

* **R429 — a page edit that redeployed the whole stack and reverted a user-visible fix.**
  `deploy.yml` deploys both Pages **and** the worker on every human push to `main`. An admin-link
  fix lived only on the `sso-build` branch. A one-page edit pushed to `main` rebuilt the worker
  from `main`, without the fix, and the Admin link vanished again for the owner. The entry also
  records a standing hazard: **~20 finished commits** on `origin/sso-build` that are not live and
  are re-reverted by every future `main` deploy until merged.

* **R405 — a deploy from a branch 3,604 lines behind.** A user-reported download bug was patched
  in the working tree on branch `feat/partner-toolkit-m0` and `npx wrangler deploy` was run from
  it. Cloudflare refused, because the branch's worker (2,668 lines) does not export a Durable
  Object class that `main`'s worker (6,272 lines) does. Had the script not happened to own a
  Durable Object, the deploy would have **succeeded** and rolled production back past the entire
  family identity provider — breaking login for four sites. *A platform safety check nobody in
  this project designed was the only thing in the way.*

* **R125 — a comment instead of a list entry.** A four-line comment was added saying `wid` had
  been added to the worker's resolver list; `"wid",` itself was never added. TypeScript compiled
  (comments compile), the denylist was right, D1 held all **2,465,197** rows, and the catalogue
  endpoint reported them — while every download returned `not_migrated`.

**The cheap test.** After editing a list, grep the *literal token* in the file you edited; and
before any deploy, diff the artifact you are about to ship against the deployed source and
require the delta to be only your change.

#### 1.5 Class E — Unanchored substring and prefix matching

**Definition.** An identifier is matched as a substring or prefix of formatted text, so it
collides with a longer identifier or with ordinary prose. Nothing errors; a specific and
plausible number comes out.

**Ids:** R32, R33, R112, R129, R137, R142, R343, R415, R462, R474, R483.

The ledger records this class hitting **four times in a single day** (R142, 2026-07-29) and again
26 days later (R462, 2026-08-24).

**Examples with numbers.**

* **R129 — an R2 prefix is not a source filter.** `Prefix="series/imf_fsi"` also matches every
  `imf_fsire` object, so an orphan check reported **18,620 healthy files as orphans**. The
  catalogue contains **50 source-id pairs** in that relationship. The keys are
  `series/<urlencoded source:id>.csv`, so the prefix must carry the encoded colon
  (`series/imf_fsi%3A`).

* **R112 — the same matcher, four different answers.** Asking which served sources lack a licence
  audit returned **25, then 2, then 11, then 4** — every change caused by the matcher, not the
  data. A plain `sid in text` matched `ppi` inside "shipping" and `scb`/`ssb`/`dst` inside
  ordinary words, declaring *covered* what was absent.

* **R142 — the check written to stop silent gaps, with the bug it existed to stop.** Failures were
  counted with `endswith("0 served source(s) with NO page")`, which also matches **10, 20, 30**
  missing sources — so a real gap would have printed CLEAN. Caught before commit and fixed to key
  on a line emitted only when the list is non-empty, then proved with a discrimination test
  (0→0 failures, 10→1, 18→1).

* **R474 — a character class built from what codes "usually" look like.** An untitled-rows regex
  `^[0-9A-Z_.\-]+$` has no `:`, so FAO's `FAO_FO:5510.1.1600` was invisible. Reported: **1,378**
  remaining. Honest count: **169,722 (1.34%)** — **123×**. The contradicting number
  (`title == id` = 168,941) was already in hand and was explained away without printing one row
  from the difference.

**The cheap test.** Anchor on a delimiter you control (`series/imf_fsi%3A`), or match structured
data, never a formatted sentence. And check `len(keys) == len(set(basenames))` before trusting
any name-keyed comparison (R389: `eia`'s 60 nested objects collapsed to 30 names and the sweep
announced *"eia: 30 files AHEAD of the store"*).

#### 1.6 Class F — The guard that cannot fail

**Definition.** A safety check, gate, ratchet or test is shipped without a *discriminating pair*
— one case it must block and one it must let through. It then either fires on everything
(visible) or on nothing (invisible, and the whole reason it exists).

**Ids:** R51, R64, R91, R93, R97, R263, R346, R414, R471, R488, R492, R497, R501, R503, R508.

This class is the one the project has hit most persistently *in the code written to close other
classes*.

**Examples with numbers.**

* **R414 — the guard written to prevent R407 broke the path it protected.** After a fresh empty
  `state.db` nearly overwrote the authoritative store, a shrink guard was added to `push_state`:
  refuse when `local_bytes < 1_000_000 or n_src < 50`. Both conditions are **true of a legitimate
  first seed** — a machine that has never pulled has no `source_state` table at all. The guard
  refused every seed; CI failed on runs **31331575104** and **31331839118** and had been red since
  the guard landed. The cause in one line: the guard asked *"is the local file small?"* when the
  question is *"am I about to destroy something substantial?"* — a property of the **remote**.

* **R488 — a completeness proof that used the one column the index ignores.** A plan to delete
  `wid`'s 7,395,591 surplus `series_fts` rows proved the surviving block complete with
  `COUNT(*) == COUNT(DISTINCT series_id)`. The schema is `fts5(series_id UNINDEXED, title,
  geography)`: the proof was about the column FTS does **not** index, while `title` is the column
  search matches. Censused over all 9,860,788 rows, the *kept* block held **2,465,197 raw-code
  titles and zero real ones**, and all 2,465,197 real titles sat in the block to be deleted.
  `MATCH 'disposable' AND source_id='wid'` splits **0 kept / 33,390 deleted**. The abort condition
  was `fts < series` and it passed on `fts == series` — *identical whichever copy survived* — so
  it would have printed `ratio 1.00, exit 0` **on the catastrophe**.

* **R503 — a fail-open rebuilt inside the guard written to close a fail-open.** The coverage check
  added to close R501 did `except: old_pairs = set()`, so an unreadable store read as *"nothing to
  protect"* and the guard approved losing all 475 pairs. In the same operation, an instruction to
  *"key the dimensions"* was executed as deleting **863,253** unnameable rows.

* **R508 — every dangerous failure returns success.** A fail-open in `billing_guard` was gated on
  `_graphql() is None`. `limit` truncation, a short day list and a sub-2-day window all return
  **HTTP 200 with no error key**, so the guard could never fire on them. Proven live: the same
  query at `limit=1` returned **9** requests where the full page held **1,038,162** — a
  **115,000×** undercount. The consequence was that the bug being fixed silently deleted about
  **15% of the bill** under a confident label. The entry notes this was the *third* fail-open in a
  guard that week (R501, R503) *and all three were reviewed first*.

**The cheap test.** A guard ships in the same commit as a discriminating pair. R414 states why one
test is not enough: *"'refuses a tiny state' passes even if the guard refuses EVERYTHING, and
'allows a seed' passes even if the guard has been deleted."*

#### 1.7 Class G — Reading one of N paths and reporting it as the total

**Definition.** A system has several parallel surfaces — schedulers, parsers, stores, workflows,
serving legs — and one is measured, or fixed, and reported as the whole.

**Ids:** R38, R60, R95, R104, R107, R284, R333, R390, R411, R422, R428, R452, R461, R481, R489,
R490.

The project's canonical count of "how many places a series lives" grew twice, in public:

| Entry | Date | Places a series lives |
|---|---|---|
| R481 | 2026-08-24 | 4 — R2 store, D1 `series`, local catalogue, `series_fts` (the fourth was missed: 384 purged series stayed in the search index, resolving to nothing) |
| R489 | 2026-08-25 | **5** — `source_counts`, the single-writer cache that *is* the `total` the API returns for a source browse |

**Examples with numbers.**

* **R489 — the fifth place, and it was billing money.** `source_counts` holds one row per source
  and is written **only** by `core/sync_catalog_d1.py`, so every direct D1 write silently
  invalidates it. Measured across all 321 sources: `vdem` had **no cache row at all** (vs 783,100
  actual), `damodaran` 23,343 vs 24,687, `sec_edgar` 17,414 vs 17,437, `ilo` **1,157 vs 0**. Two
  different defects: with no cached row the worker falls back to a live `COUNT(*)` reading 783,100
  rows on *every* `vdem` catalogue page view — re-creating the exact incident the table was
  created to prevent; and `ilo`'s stale row made the live API answer `total:1157` with
  `results:[]`, advertising 1,157 series it could not deliver.

* **R428 — a fix ported to one workflow of a family.** Three consecutive `imf_imts_direct` heavy
  jobs each ran their full 10,800 s cap, and each was explained to the owner as *"budget deferral,
  progress persists, converges over runs."* One read of R2 `state.db` refuted it: the `unit_state`
  row's `last_attempt_utc` was frozen at **2026-08-05** — no pass had persisted anything.
  Mechanism: `updater-heavy.yml`'s "Push state to R2" step had no `if: always()`, so a source's
  transient failure exits the step non-zero and GitHub's implicit `success()` gate skips the push.
  **The identical defect had been found and fixed in the *daily* workflow on 2026-07-30**; the port
  to the heavy never happened — and the heavy is the workflow whose jobs are *designed* to exit
  non-zero.

* **R95 — one licence row was a sample, not the population.** Told that `yale_epi`'s licence row
  over-granted, that row was corrected and reported done. The identical fingerprint
  (`commercial_ok=1, attribution_required=0, no_modify=1`, unrelated URL) was live on **ten more
  serving rows** — WHO, UNESCO, Statistics Estonia, Fund for Peace — **105,301 series** whose
  downloads omitted the non-commercial warning or the attribution their publishers demand in
  writing.

**The cheap test.** After fixing any instance, enumerate every row/file/workflow that could share
its shape and show the count is zero. The owner's standing correction, recorded in memory as
`feedback_example_means_class`: *a reported example is one instance of a class — sweep the whole
surface and prove it with a zero-result check.*

#### 1.8 Class H — The silent empty result

**Definition.** A listing, lookup or parse returns an empty collection instead of raising.
"Nothing is there" and "I could not look" become indistinguishable, and the run goes green having
examined nothing.

**Ids:** R44, R55, R99, R106, R109, R156, R261, R264, R330, R366, R368, R374, R484.

R261 states the class directly: *"An empty listing is a LEGITIMATE value, so nothing downstream
objects and the run goes green having examined nothing — strictly quieter than a raw read, which
throws and gets fixed immediately."*

**Examples with numbers.**

* **R330 — "0 defects in 0 files examined."** All three re-pull tools pointed at
  `D:/research/econfindatalibrary`, a drive letter the store had left behind in a machine cutover.
  `os.path.isdir` returning `False` reads as *"this source has no data"*, so the authoritative
  repair tool printed **`0 corrupt` across nine sources** while the store held **637,178 bad
  rows**.

* **R264 — a plausible non-empty number, 40× low.** `dst` reported observations falling
  **9,198,885 → 231,035** (97.5%), which the merge never-shrink guard makes impossible. The store
  in fact held **9,220,012 rows across 707 files**. `_total_rows()` read *through* the storage
  abstraction but **listed** with `os.listdir`, and on a cloud runner that directory exists as a
  scratch mirror holding only the files that run wrote. Worse than an empty result: it invited a
  hunt for a merge bug that did not exist, and the false figure was written into the `runs` table
  and the daily digest, where it outlived the defect.

* **R99 — `except Exception: continue` dropped 58 whole files.** `bls` was reported as
  **57.4 million** observations against an actual **328.1 million**, while the tool's docstring
  promised a *complete* scan and that promise was repeated in a published document.

* **R261 — the frontier that returned `None` and reinstated the staleness it existed to remove.**
  `bea._tree_frontier` walked its 591-file tree with `glob.glob`. Under `AQUEDUCT_BACKEND=r2` the
  local directory is absent, so it iterated nothing and returned `None`. Measured: raw listing
  **1 file**, routed-recursive listing **591**, frontier `None` → date fell back to 2026-04-01.
  *Local testing cannot catch this*: for `dst` the local and routed paths resolve to the same
  files (707 vs 706), so they agree by construction.

**The cheap test.** Ask what distinguishes *"nothing is there"* from *"I could not look."* If
nothing does, that is the bug. Any loop that skips an input must name what it skipped in its
output, and a denominator must be printed beside every count (R0: *"A sweep reports TWO numbers —
what it found and what it could not reach. No denominator, no result."*).

#### 1.9 Class I — Cost and scale defects that only the invoice finds

**Definition.** A defect whose sole symptom is money. Every functional signal is green; the
failure mode announces itself on a bill, weeks later.

**Ids:** R85, R88, R140, R430, R473b, R492, R502, R505, R507, R508.

This class is unusual in that the *owner* was the detector twice.

**Examples with numbers.**

* **R430 — a query shape that billed ~$82 in one day.** `/v1/catalog?source=` ran two queries per
  request: `WHERE source_id=? ORDER BY series_id LIMIT ? OFFSET ?` (no composite index exists, and
  D1 **cannot** build one — `CREATE INDEX` on the 9.2M-row table dies `SQLITE_NOMEM` — so SQLite
  sort-scanned the source's entire row set per page: **4.93M rows on `wid`**), plus
  `COUNT(*) WHERE source_id=?` (**2.47M rows, every request**). A crawler paging `wid` made
  **17,700 requests** → **87.3B + 42.2B ≈ 130 billion rows read in one day**, measured with
  `wrangler d1 insights` at an average efficiency of **0.00008**. *"The design flaw was mine and
  OLD; the trigger was external traffic; the DETECTION was Ahmed reading his bill."* Fixed the
  same hour; worst-case daily cost after the fix at the same traffic: **~$0.10**.

* **R492 — a plan costed at 90 minutes that would have cost ~24 days and ~$2,500.** A repair
  issuing one `DELETE ... WHERE series_id = ?` plus one INSERT per id, for **164,705** ids.
  `series_fts` is `fts5(series_id UNINDEXED, ...)`, so that predicate has **no index** and every
  statement is a full table scan. Measured on live D1, not modelled: a single-id lookup reads
  **23,843,482 rows in 11.2 s**, while the same lookup on `series` by primary key reads **1 row in
  0.335 ms** — and an `IN` list of **200** ids reads the *same* 23,843,482, because the cost is per
  **statement**. 164,705 statements = **3.93 × 10¹² row reads**. The estimate was off by ~**380×**.
  Caught by adversarial review before a single statement ran. The fix is not smaller batches, it is
  higher predicate arity: five range-scoped statements replace 164,705, ~120M rows instead of
  3.93e12 — about **33,000× less**.

* **R507 / R505 / R502 — the meter itself was wrong in four independent ways.** The billing guard
  *printed* the true D1 total from GraphQL and *alarmed* on `wrangler d1 insights`, which reports
  only the top-100 query shapes and measures **10× low**. Two genuinely catastrophic days
  (2026-08-10 at 5.4B rows, 2026-08-25 at 8.9B) reached the alarm as ~0.5B / ~0.9B — under even the
  WARN threshold. Separately, R2 operations (**~$31/mo, the largest variable line**) were printed
  four lines above a projected month that omitted them; D1 was priced from row zero while the plan
  **includes 25 billion reads and 50 million writes per month**; a GB *mean* was used for a
  GB-*month* charge (730 hours); and every figure ever printed was **pre-tax** while Texas adds
  **6.6%**. The root cause named in R507: **nothing had ever been reconciled against an invoice.**

**What closed it.** Invoice **IN-74622130** (Jul 9 – Aug 8), supplied by the owner, became ground
truth: **$154.96 pre-tax / $165.19 with tax**. The reconstructed model now predicts **$154.79**
(−0.11%), with D1 rows written matching to **21 rows out of 82 million** and R2 class-A operations
to **0.055%**. The bill's actual shape was the surprise:

```
R2 class-A operations  $94.50   <- the single largest line
D1 writes              $33.00
R2 storage             $22.46
base                    $5.00
D1 reads                $0.00   <- covered by the 25B included allowance
```

The line the project had spent most effort optimising (D1 reads) was **free** that period.

#### 1.10 Class J — Key-grain and dropped dimensions

**Definition.** The store's key omits a dimension the publisher uses. Distinct series then stack
under one id. Nothing throws; every freshness and row-count check passes, because a collision
looks exactly like a healthy source with more observations per series.

**Ids:** R22, R79, R133, R141, R280, R350, R354, R356, R387, R491, R515, R516, R519.

This is the class the project was actively working through on 2026-08-30, and it is the one whose
harm reaches users most directly.

**Examples with numbers.**

* **R515 — `who_gho` drops WHO's `Dim1..Dim3`.** Keyed `IndicatorCode:SpatialDim`, while WHO
  dimensions rows by sex, age and residence area. `jobs/ingest_who_gho.py:72` requests only
  `$select=SpatialDim,TimeDim,NumericValue,TimeDimensionValue` and builds `f"{code}:{geo}"` at
  line 98. Measured on the store: **69,590 of 162,790 ids (42.7%)** hold conflicting values,
  **578 of 1,333 indicators**, **6,587,449 of 8,188,819 rows (80.4%)** hidden by the collision.
  Publisher-confirmed exactly: `HCF_REL_ELECTRICITY / SEN / 2019` returns three rows — URB 53.0,
  RUR 3.0, TOTL 45.0 — the three values the store interleaves under one key. *The correct key was
  already written down in the repository*, in a module the author had misread as parked.

* **R519 — UNCTAD drops `Flow` (Imports vs Exports).** The publisher's `$metadata` declares
  `Year, Flow, ProcessFoodCategory, Economy` as the Fact key; the store's key omits `Flow`.
  Reconciliation is exact: **286,038** publisher two-flow cells against **286,038** duplicated
  store pairs, **zero residue**, and every duplicate pair **adjacent in file order** (gap
  min = max = median = 1), which independently refutes the alternative story that two vintages had
  been merged.

* **`damodaran` drops the workbook sheet name** (NUMBERS.md, 2026-08-30). The key is
  `DAMODARAN:{dataset}:{col_label}:{entity}`; `_parse_rows` is *handed* `sheet_name` and uses it
  only to print progress. `ctryprem` parses **7 sheets** of one workbook, so any column label
  shared by two sheets collapses onto one id. **721 of 24,687** catalogued ids serve a wrong
  value, and all 721 serve the *last* store row rather than the modal one. The publisher-confirmed
  user-facing instance: India's **Adjusted Default Spread** is published as **0.0209** and served
  as **0.3**, which is India's corporate tax rate — a ~14× error on a variable that feeds directly
  into cost-of-capital work.

* **`eia` collides at frequency resolution** (NUMBERS.md, 2026-08-30). Store ids carry a frequency
  suffix (`ELEC.ASH_CONTENT.BIT-AL-1.A/.M/.Q`) that the catalogue ids lack. **142,073 of 268,502
  catalogued ids (52.9%) gather more than one publisher series**; the worst gathers **172,766**.
  `eia:ELEC.PLANT.GEN` is served as a **566,060,873-byte** object presented as one series;
  `eia:EBA.AEC-ALL` returns 372,392 rows over 976 dates, mixing hourly demand with day-ahead
  forecast.

* **R387 — assuming the natural key, which would have deleted 2,013 rows.** A repair deduped on
  `(metric, obs_date, vintage_date)` — the obvious primary key of a point-in-time fact table,
  chosen from the column names without measuring. Measured: XOM holds **20,629 rows, 20,578
  distinct 4-tuples, and only 18,616 distinct on the assumed key**, because the producer flattens
  SEC's `units` points keeping `end/val/filed` and **dropping `start`**, so a filing's 3-month and
  9-month figures for the same period end are two legitimate facts the schema cannot distinguish.

**The systemic cause, found 2026-08-30 and worth stating plainly:** **0 of 212** `jobs/*.py`
ingest scripts call `merge.merge_and_write`; **146 write parquet directly**. Every *fetcher* writes
through `merge_and_write`, which dedups on `(series_key, obs_date)`, refuses a shrink below 97%
and reports impossible dates. The ingest jobs that created each store's *initial* content do none
of that. That is why the class exists at all.

**The cheap test.** `count(*)` vs `count(distinct <candidate key>)` — one query. And read the
publisher's own key declaration (`$metadata`, the API's dimension list) rather than inferring it
from the columns you happened to request.

#### 1.11 Class K — Alive is not working

**Definition.** A long-running job's health is inferred from a proxy — a process listing, an
aggregate status label, CPU use, a log's silence — rather than from the work advancing.

**Ids:** R54, R80, R90, R115, R131, R260, R336, R348, R417, R418, R427, R443, R453, R454, R457.

**Examples with numbers.**

* **R417 — cancelled a run that had 23 succeeded jobs.** `updater-heavy` run **31356500944**
  showed a *run-level* status of `queued` for two hours. Two hours of careful-looking diagnosis
  followed (queue baselines, concurrency slots, account caps, githubstatus.com), then the run was
  cancelled and re-dispatched. It had **twenty-three succeeded jobs** at that moment and 14 still
  queued: run-level "queued" on a wide matrix means *some* jobs are waiting. The tell was in the
  author's own earlier output — `heavy redispatch: queued started 2026-08-10T06:46`, a *started*
  timestamp beside a *queued* label. The one direct observation never made:
  `gh run view <id> --json jobs`. Cost: ~2h of runner compute discarded, 14 sources delayed one
  pass.

* **R336 / R348 — piping a watched long job through `tail`.** `cmd | tail -18` shows **nothing**
  until the process exits, so a healthy job looks stalled. R336 records inventing a mechanism for
  the silence and killing the job **twice in one session**. R348 records doing it again — twice
  more — **roughly one hour after reading R336** in the skill digest.

* **R418 — 19 hours lost to a transient.** The family's largest pull ran ~19 hours accumulating
  everything in `out: list[str]` (~14 GB working set) and died on a single HTTP **401**. Total
  loss, nothing on disk. Two things make it more than a code defect: the kill-loses-everything
  shape was readable in the loop being watched (sampled a dozen times for CPU and working set,
  never once asked *"what happens if it dies?"*), and a sliced-resume design for exactly that
  failure mode had been built and tested **the day before**. The 401 itself was transient — the
  identical credentials returned 200 through the identical code path minutes later.

* **R453 / R454 — two sources found dead in one night by the same pattern.** `cbs_nl` passed an
  "alive and advancing" check for **6 days** while accomplishing nothing; `istat` had been in
  `transient_fail` for **40 days**.

**The cheap test.** Measure the quantity the work *moves* (object counts, row counts, cursor
advancement) between two readings, and enumerate a job's *children* before acting on any
aggregate verdict. R80 adds the necessary sanity check: *ask whether the work would change that
metric at all if it were succeeding* — a derive that **overwrites** objects leaves the object
count constant by construction.

#### 1.12 Class L — The causal story that outruns its test

**Definition.** A sound observation is made, and a *cause* is attached to it for free. The
observation is reported at the confidence of the measurement; the cause is reported at the same
confidence, having been tested at none.

**Ids:** R100, R257, R379, R384, R444, R504, R506, R512, R514, R516, R518.

R514 is the entry that names the class, and it records three instances in one session:

1. *"istat has IP-blocked us"* — reported under a heading claiming **three independent
   confirmations**. All three were the same machine failing to reach the same host. The
   discriminating test was one call to a different egress: Anthropic's WebFetch gets
   `ECONNREFUSED` on `193.204.90.13` too, while `sdmx.istat.it` (193.204.90.1) and `www.istat.it`
   (.61) — **the same /24** — answer *our* address in 0.13 s. The host is down for everyone.
   (R512.) The tell was inside the quoted evidence: the preflight line cited as proof of a
   firewall says a sibling host `SELF-REDIRECTS 302`.
2. *"Fixing the local route unblocks 14 sources at once."* The test: read `last_success_utc`.
   `bis` and `faostat` succeeded at 00:45Z **inside the very pass** described as starved.
3. *"These six sources share a cause."* The test: check each against the gate's failing list, the
   registry and the denylist. Two are correctly gated, one runs elsewhere, one's note is five days
   older than its fix. Six became one.

The entry's own diagnosis is the most useful sentence in the ledger on this point:

> **"Why it kept happening: a causal story is what makes a finding feel finished.** 'The host
> times out' is an observation and sits uncomfortably; 'they banned us' is a story and feels like
> an answer. The pull toward closure is strongest exactly when the observation is solid, because
> the solidity transfers to the explanation without being earned."

The same entry records three consecutive reviews of one change each reducing to *"the measurement
was sound and the CLAIM went one step further than the measurement licensed"* — and each overreach
being **one word wide**: "all" (47 of 540 parsing), "the rule" (35 of 35 catalogued ids), "still
maps zero" (a window in a ring read as a prefix from 0).

**The cheap test.** Before reporting a cause, write down the observation that would refute it and
run that. If you cannot name such an observation, you have a story, not a diagnosis. And
corroboration is counted in **independent vantage points**, not in observations (R512): three
readings from one machine are n = 1.

#### 1.13 Class M — Ledger and process hygiene

**Definition.** Failures of the recording system itself. These matter disproportionately, because
every other class depends on the record being readable.

**Ids:** R121, R135, R148, R247, R328, R352, R395, R485, R493.

**Examples with numbers.**

* **R352 / R247 — a concurrent session rewrote the ledger from a stale buffer**, twice on
  2026-08-02, deleting 18 then 17 entries; a later occurrence deleted **118 entries beside a
  34-line insertion**. Git recorded clean commits both times (`48 insertions / 883 deletions` is a
  replacement, not an edit). A pre-commit hook now refuses any decrease in the entry count.
* **R121 — rules citing evidence that was never written.** Three rules (R41–R43) cited entries
  that do not exist, and one (R94) cited nothing. The digest is what gets re-read after every
  context compaction, so an unanchored rule is *indistinguishable from an invented one*. The
  audit's own first pass used `re.S` and manufactured phantom dangling citations — inside the
  check meant to validate the file that records that exact failure mode.
* **R148 — findings written into new documents get forgotten.** In one session,
  `AUTOUPDATE_COVERAGE.md`, `UN_WPP_TITLE_ENRICHMENT.md` and `STALE_CSV_INCIDENT_20260729.md`
  were created. The owner's correction: *"you are notorious about forgetting about these
  documents... have everything centralized."*
* **R395 — the write-only destination drifts.** Eleven pushes to the econ repository, each
  verified; **zero** pushes to the ledger repository, where seven commits sat local-only. *"The
  repo I was actively building in got pushed reflexively, and the repo I only WROTE TO — never
  read back from, never ran CI on — silently accumulated."*
* **R485 / R328 — entries written with no digest line.** Covered in full in §2.4, because it is
  the clearest available case study of a rule that was written, enforced, and broken again.

#### 1.14 Class frequency, and the overlap problem

| Class | Short name | Ids I assigned | Count |
|---|---|---|---|
| A | The probe that cannot succeed | R57 R64 R75 R93 R106 R134 R141 R261 R276 R283 R316 R329 R338 R346 R354 R373 R413 R418 R419 R433 R478 R483 R484 R504 R510 R512 | 26 |
| B | Asserting from an artifact, not the system | R6 R7 R59 R104 R122 R147 R349 R365 R398 R408 R410 R412 R416 R431 R432 R470 R479 R495 R509 | 19 |
| C | Measurement shape ≠ question | R59 R87 R98 R127 R141 R146 R149 R318 R322 R326 R327 R341 R343 R403 R411 R422 R447 R449 R482 R486 R494 R515 R518 R519 | 24 |
| D | "Deployed" ≠ committed | R96 R107 R116 R119 R125 R155 R345 R358 R401 R405 R425 R426 R429 R434 R438 R451 R490 | 17 |
| E | Unanchored matching | R32 R33 R112 R129 R137 R142 R343 R415 R462 R474 R483 | 11 |
| F | The guard that cannot fail | R51 R64 R91 R93 R97 R263 R346 R414 R471 R488 R492 R497 R501 R503 R508 | 15 |
| G | One of N | R38 R60 R95 R104 R107 R284 R333 R390 R411 R422 R428 R452 R461 R481 R489 R490 | 16 |
| H | Silent empty result | R44 R55 R99 R106 R109 R156 R261 R264 R330 R366 R368 R374 R484 | 13 |
| I | Cost found by the invoice | R85 R88 R140 R430 R473b R492 R502 R505 R507 R508 | 10 |
| J | Key-grain / dropped dimension | R22 R79 R133 R141 R280 R350 R354 R356 R387 R491 R515 R516 R519 | 13 |
| K | Alive is not working | R54 R80 R90 R115 R131 R260 R336 R348 R417 R418 R427 R443 R453 R454 R457 | 15 |
| L | Causal story outruns its test | R100 R257 R379 R384 R444 R504 R506 R512 R514 R516 R518 | 11 |
| M | Ledger / process hygiene | R121 R135 R148 R247 R328 R352 R395 R485 R493 | 9 |

**The overlap is not noise, it is the finding.** The 13 classes above cover **173 distinct rule
ids**, and **25 of them appear in two or more classes** — R59, R64, R93, R104, R106, R107, R141,
R261, R343, R346, R354, R411, R418, R422, R483, R484, R490, R492, R504, R508, R512, R515, R516,
R518, R519. The ledger itself makes **43** explicit "this is R### again" cross-references
(counted by regex over `is R### again` / `same shape as R###` / `R### class` / `recurrence of
R###` and their variants). R510 is a clean example
of one incident sitting in two classes pointing in *opposite* directions: absence from a
publisher's listing was about to be filed as retirement for 27 matrices (**Class A / L**, a false
alarm about the publisher), while the same listing drives the fetcher's `changed` set, so **495 of
7,896** `cso` matrices — 6.3% of the source — are frozen by construction and report nothing
(**Class F / H**, a false all-clear about our own code). One measurement, two errors, opposite
signs.

The classes are best read not as a taxonomy of *bugs* but as a taxonomy of **the ways a check can
be true and useless**.

---

### 2. The repeat offenders

#### 2.1 The documented repeats, with elapsed time

The ledger flags recurrences explicitly. The table below lists the ones where both the prior rule
and the elapsed interval are stated in the text. Intervals in quotation marks are the ledger's own
words; dates come from the entries' `M-YYYYMMDD` tags or the date in the heading.

| # | Rule already written | Broken again | Elapsed | What happened the second time |
|---|---|---|---|---|
| 1 | R336 (never pipe a watched long job through `tail`) | R348 | **"roughly one hour"** after reading it | The same pipe composed **twice**, on two backgrounded jobs |
| 2 | R255 (the ledger applies at the point of action) | R255 itself | same hour it was written down | `nohup … &` typed inside `run_in_background` |
| 3 | R135 (ledger appends use an absolute path) | R135 | **"RECURRED THE SAME DAY"** | `cd econ-repo && … git add .claude/MISTAKES.md`; caught only because git failed loudly |
| 4 | R40b (take the ingester's proven rate for this exact host) | R132 | 5 days (2026-07-24 → 07-29) | ONS again: 41 HTTP 429s the first time, **207 consecutive 429s** the second, nearly reported as "328 ONS datasets are broken" |
| 5 | R107 (two stores must not share one diff) | R110 | **"hours after"**, same day | An `UPDATE ... WHERE series_id=?` that matches no row silently drops a first-time filer: two companies with data on R2 and no catalogue row |
| 6 | R107 + R116 | R119 | **"third time in one day"** | An audit run against local `catalog.db` instead of D1; nearly reported a phantom breach on 18,838 `idb` series |
| 7 | R389 (a flat-layout listing bug) | R390 | **2 hours** | Fixed in one tool, still present in three others *including the guard whose entire job is to stop a derive running from an unchecked mirror* |
| 8 | R366 (measure a cloud-backend source against R2) | R371, then R374 | 4 hours, then same day | *"A rule I have written down three times and broken three times is not a rule, it is a wish"* — so R374 built `tools/store_inventory.py` instead |
| 9 | R418 (a probe must use the target's own code path) | R419 | **"THE DAY AFTER LOGGING R418'S CANNOT-SUCCEED-PROBE RULE"** (tags `M-20260810-04` → `M-20260810-05`) | A new API method fed through a parser built for a different one: 796,716 fallback titles reported as success |
| 10 | R422 (an ETA enumerates remaining phases from the code) | R423 | **"TWO HOURS AFTER LEDGERING R422"** | *"re-read from cache (~2h, no network)"* — the resume path's first phase is a live network re-walk of ~35k nodes |
| 11 | R425 (a push is not done until its CI is read) | R426 | **"WITHIN 24 HOURS OF LEDGERING IT"** (2026-08-12 → 08-13) | A pushed `discontinued.yaml` entry reddened its own registry test; learned from an unrelated local run ~40 min later |
| 12 | R234 (cp1252 landmine on subprocess output) | R363 | **"the day after re-reading the rule"** | `subprocess.run(text=True)` decodes with the *parent's* locale; `PYTHONIOENCODING` was set for the child |
| 13 | R129 (anchor a prefix on its delimiter) | R462 | 26 days (2026-07-29 → 08-24) | Counting `series/ilo` returned `ilostat`'s objects |
| 14 | R54 (read a long job's log before declaring it dead) | R458 | ~30 days | *"I built the exact instrument R54 was written to forbid, on the exact crawler it was written about"* |
| 15 | R328 (every entry gets a digest line, same commit) | R485 | **20 days** (2026-08-04 → 08-24) | Nine entries, nine commits, **zero** digest lines |
| 16 | R501 (a guard must test the failure that hurts) | R503 | **1 day** (2026-08-28 → 08-29) | *"I rebuilt R501's fail-open inside the guard written to close R501"* |
| 17 | R504 (a 403 is not a verdict without a known-bad control) | R512 | **1 day** (2026-08-29 → 08-30) | *"That is R504 exactly, and R504 is mine, from yesterday"* |
| 18 | R513 (state the property as an assertion) | R513 | **same entry, same day** | The assertion written *inside the entry about half-tested assertions* was itself half-tested — one bound of a two-sided predicate — and invented a hazard that does not exist |
| 19 | R379 (a filtered query cannot measure prevalence) | R379 | **same edit** | The retraction re-used the filtered query and produced a second false claim *"one paragraph after diagnosing the filter as the cause"* |
| 20 | R487 rule 3 (grep every writer before closing a class) | R491 | same session | *"which is R487 rule 3 being broken while I was citing it"* |
| 21 | R414 + R419 (discriminating pairs; per-branch counters) | R420 | 1 day | Both rules applied and both ignored *"under completion momentum"*; a defective headline of 36.56B series / 89.74B observations was published to the live `/v1/stats` |

Beyond these, **22 digest lines** carry an ordinal self-description ("third time", "fourth
instance", "fifth") — R4, R63, R119, R134, R142, R149, R150, R262, R374, R381, R392, R406, R410,
R411, R416, R422, R428, R478, R481, R489, R508, R355 — including R484's *five tools in one
session* all reporting a publisher gap
that was a reader bug, and R476's *four times in one session* putting code into a shell command
and having the shell read it as syntax.

#### 2.2 Why writing the rule does not prevent the repeat

The ledger proposes two mechanisms. Both are correct and both are incomplete. Here they are,
developed, with four more the evidence supports.

##### 2.2.1 The write/read asymmetry — R485

> *"appending is a WRITE at the end of the file, and reading the digest is a READ at the start, so
> the two never meet. Nothing in the act of adding an entry brings the instruction into view."*

This is a claim about **workflow topology**, not about memory. The rule "every entry gets a digest
line" lives at line 10 of a 14,010-line file. The act it governs — appending an entry — touches
line 14,010. The two operations have **no shared read**. R485 records reading the file during the
same session (grepping it for landmines) and appending to it nine times, *"which means I opened the
path to it repeatedly without ever reading the top."*

The generalisation is stronger than the ledger states: **a rule is only enforceable at the places
your workflow actually visits.** A rule stored where the workflow does not go is not a weak
control, it is a *zero* control, and it will read as a strong one because it is well written and
prominently placed. Prominence is a property of the document; visibility is a property of the path.

The corollary R485 draws is the operational one: *"the check belongs in the act of writing, not in
the act of reading."*

##### 2.2.2 The retrieval-cue mismatch — R348

> *"Rules with a SYNTACTIC trigger (`| tail` on a long-running command, `&` inside
> run_in_background, `git -C` for the ledger) are not retrieved by topic — nothing about 'derive
> CSVs' cues 'tail'. They fire only if checked at composition time, and composition is exactly
> when attention is on the domain, not the shell."*

This explains a category of repeat that looks like carelessness and is not. Recall is
**cue-driven**. A rule is retrieved when something in the current situation resembles the
situation that stored it. But the situation that stored R336 was *"a derive job looked stalled"*,
and the situation that re-triggered it was *"launch a CSV derive and a D1 sync"* — the same shell
construct, an entirely different topic. The domain occupies attention; the syntax is invisible
precisely because it is habitual.

The remedy R348 states is not "remember harder", it is a **checklist bound to an act rather than a
topic**: *"Before running anything backgrounded or >60 s, scan for the triggers: a pipe after the
long-running process, a bare `&`, missing `-u`, missing `PYTHONIOENCODING`, a relative path after
`cd`."* That is a rule indexed by the *shape of the command line*, which is the thing actually
present at composition time.

##### 2.2.3 Rules are indexed by the symptom that taught them, and fire on a mechanism — R92

R92 records the same construct biting twice through different failure modes:

> *"the ledger already carried a heredoc warning about ESCAPES; I read it as a rule about
> backslashes and met the same construct's other failure mode. **Write rules around the MECHANISM,
> not the symptom that taught it.**"*

A rule stored as *"backslashes collapse in heredocs"* does not match *"backticks are
command-substituted in heredocs"*, even though the mechanism — *bash is interpreting content I am
treating as opaque* — is identical. R476 later generalises correctly and states the real remedy:
**when a channel bites twice, change the channel** — put code in a file, not in a shell command.

This is why the ledger's most durable entries are the ones phrased as *mechanisms* ("an empty
listing is a legitimate value"), and why its most-repeated ones are phrased as *incidents* ("don't
glob the local directory for `unsdg`").

##### 2.2.4 The read path outgrew the reader — R493

The reliability system built in response to a standing order about forgetting shipped with six
defects. The sixth is the one that matters here:

> *"the qualifying-read rule demanded a single 400-line Read of the ledger, and the ledger's digest
> lines are dense enough that 400 lines exceed the Read tool's own 25k-token cap — the requirement
> was **UNSATISFIABLE BY CONSTRUCTION**, discovered only when the gate blocked me and I tried to
> comply."*

I reproduced this while writing this section. A read of `MISTAKES.md` lines 1–219 returned
**26,603 tokens** against a 25,000-token cap and was refused; lines 1–1800 estimated **219,050
tokens**. The digest's densest lines are 3,591 and 3,493 characters long. **The control had grown
past the size at which it could be consulted.**

R493's rule is the general one: *"a mandatory requirement must be PROVEN SATISFIABLE by actually
satisfying it once — an impossible requirement trains the operator to use the override, which
un-builds the gate."* A rule nobody can comply with is worse than no rule, because it manufactures
a habit of overriding.

##### 2.2.5 Familiarity impersonates verification — R411

> *"a number I have repeated many times is not thereby verified, it is only familiar — R0's
> 'compute what the SYSTEM computes' applies hardest to the figures I am most fluent in."*

R416 states the same effect for domain knowledge rather than figures:

> *"fluency in a domain reads, from the inside, exactly like knowledge of a fact."*

This explains why the repeats cluster on the *most-worked* areas. The rules about licence records,
serving surfaces and coverage percentages are the most frequently re-broken not despite being
familiar but **because** of it: the fluent answer arrives with the same subjective confidence as
the measured one, and the difference is invisible from inside. R410 names the observable
signature — all three of that night's false claims were delivered **flat**, *"no hedge, no
attribution, no 'I have not checked' — with the cadence of a verified finding."*

That gives the reader a usable tell: **the absence of hedging is not evidence of verification, and
in this ledger it correlates with its absence.**

##### 2.2.6 Completion momentum — R420

> *"R414's rule ('a guard ships with a discriminating pair') and R419's ('a branch that can absorb
> everything needs its own counter') both applied and were both ignored **under completion
> momentum**."*

The repeats do not occur uniformly through a task. They cluster at the *end* — at the publish, the
push, the "done" report. That is precisely when the discriminating check feels like an obstacle to
a finished thing rather than part of it. R364 records the sharpest instance: a dry run *"SAID '2
shards' and printed 1,927"*, and both tells were read past, because the run was already understood
to be the confirmation step.

##### 2.2.7 A rule with no failure mode is not a control — R58, R279, R374

This is the mechanism that ties the other five together. Prose has no failure mode: it cannot
refuse, cannot go red, cannot be shown to fire. The ledger states the conclusion in three places
and eventually acts on it:

* R58: *"A rule you write but do not implement will not save you — when a postmortem's remedy is
  'make X observable', implement X in the SAME change."*
* R279: *"a class is not closed until the check is MECHANICAL."*
* R374, after the third recurrence: *"A rule I have written down three times and broken three times
  is not a rule, it is a wish — so this one gets a TOOL."*
* R383, most bluntly: *"a rule you wrote down today does not protect you from the mistake tomorrow
  morning unless it is in a tool that refuses — I had the sentence and still did it."*

`CLAUDE.md` reaches the same verdict at project level:

> *"**Prose rules did not hold; a second agent and a runnable check are the difference.**"*

#### 2.3 What mechanisation actually bought — and its own failure modes

The project acted on this. The mechanical layer now includes:

```
.claude/hooks/d1_cost_guard.py        PreToolUse; refuses full-table D1 scans past 15/hour, 40/day
.claude/hooks/cost_banner.py          SessionStart; prints the running spend total
.claude/hooks/consequential_gate.py   blocks consequential commands without a qualifying read receipt
.claude/hooks/read_receipts.py        records which REGIONS of a file were actually read
.claude/hooks/heartbeat.py            re-injects the five standing rules into every session
.claude/skills/adversarial-review/tools/ledger_check.py   --digest --served --purged --counts --titles --numbers
```

It works. Running the self-test now:

```
$ python .claude/hooks/test_reliability_system.py
   RESULT: 33 checks, all passed
$ python .claude/skills/adversarial-review/tools/ledger_check.py --numbers
   PASS  NUMBERS.md: 119 rows, every value has an instrument, none stale >30d,
         Total observations honestly gated
```

And the adversarial reviewer demonstrably catches what the author's own checks did not:
**17 digest lines** mention an adversarial review or reviewer (R13, R385, R388, R487, R488, R490,
R491, R492, R493, R497, R498, R499, R511, R516, R517, R518, R519), and among them are the
plans that would have cost the most — R492 (3.93 × 10¹² row reads, ~$2,500, **never run, $0
spent**), R488 (a delete that would have removed all 2,465,197 real `wid` titles), R498 (a
bulk derive at the wrong grain: 916,416 store keys against 2,264 catalogue rows), R519 (a remedy
that would have destroyed 603,467 rows and turned ~43% of two sources' keys into unmarked
import/export chimeras, **with nothing 404-ing afterwards**).

**But the mechanical layer inherits the classes it polices.** This is the most important caveat in
this section, and the ledger documents it without flinching:

| Guard | Its own defect | Entry |
|---|---|---|
| The shrink guard added after a near-catastrophe | Refused every legitimate seed; CI red since it landed | R414 |
| `ledger_check --titles`, written to catch stale search indexes | **Passes on a destroyed index** — it fails on `bare_index > bare_series`, so 0 > 0 is false and it reports PASS on a source erased from search | R492 |
| The coverage guard added to close R501 | `except: old_pairs = set()` — an unreadable store read as "nothing to protect" | R503 |
| `billing_guard`'s fail-open detector | Gated on `errors is None`; every dangerous failure returns HTTP 200 | R508 |
| The reliability system itself | Shipped with **five** defects of the kinds it polices, and its first live firing found a sixth | R493 |
| The ledger-citation audit (R121) | Its own first pass used `re.S` and manufactured phantom dangling citations | R121 |

The pattern is exact: **a guard is written under the same cognitive conditions as the code it
guards, so it acquires the same defects — and its failures are quieter, because nobody audits the
auditor.** R493 rule (4) draws the deployment corollary: *"install the guard where the RISK lives,
not where the ledger lives"* — the reliability system originally loaded only in the `hf`
repository while every failure it existed to prevent happened in the `econ` repository, which had
no settings file at all, meaning even the standing ban on a forbidden data host never loaded there.

#### 2.4 A live instance of the open R485 gap

The remedy for R485 is `ledger_check --digest`. It currently reports:

```
$ python .claude/skills/adversarial-review/tools/ledger_check.py --digest
  ledger: 111 headings, 335 digest lines
  PASS  every entry from R475 onward has a digest line (58 older ones are the known pre-rule backlog)
  RESULT: all checks passed
```

That PASS is technically correct and substantively incomplete, for two reasons visible in the
source:

1. **The heading regex is `^## (R\d+) — `.** It matches 111 headings. The file contains **123**
   `## R###` headings and **166** `### R###` headings. **100 entries written at `###` are invisible
   to the check entirely.**
2. **`RULE_FROM = 475`**, documented in the code as *"Entries below this id predate the rule (added
   2026-08-04)."* But entry ids and dates are not the same axis. **R435 through R466 — 32
   consecutive entries — are dated 2026-08-19 to 2026-08-24**, i.e. fifteen to twenty days *after*
   the rule was added, and **none of them has a digest line.** The check exempts them on id, not on
   date.

Verified directly:

```
$ python - <<'PY'
import re
t = open('.claude/MISTAKES.md', encoding='utf-8').read()
heads = set(re.findall(r"^#{2,3} (R\d+)\b", t, re.M))
digest = set(re.findall(r"^- (R\d+)\.", t, re.M))
print(len(heads - digest), "entries have no digest line")
PY
147 entries have no digest line
```

So the lesson the ledger calls *"the highest-risk kind, because it has already survived one round
of me knowing about it"* has now survived **three** rounds: written 2026-08-04 (R328), broken
2026-08-24 (R485), and still open for 32 post-rule entries under a check that reports PASS. That
is not a criticism of building the check — it is the exact prediction the ledger's own material
makes about checks: **a guard's scope is a claim, and an unstated scope defaults to whatever the
author happened to test.**

**What would close it:** change the heading regex to `^#{2,3} (R\d+)\b` and replace the id cutoff
with a date cutoff derived from the entry's own `M-YYYYMMDD` tag or heading date. That is a
five-line change and the fix is not being proposed here as work — it is named because a reader
should know the check's current blind spot before trusting its PASS.

---

### 3. The assumption taxonomy

Each type below follows the same structure: what is assumed, **why it is seductive** (the specific
reason it feels like knowledge rather than a guess), what it cost, and the cheap test that refutes
it. The costs are real figures from the entries cited.

#### 3.1 Assuming a field name without printing one raw record

**The assumption.** That an API's JSON response, a database row or a parsed record uses the key you
expect (`id`, `source_id`, `Results.Data`).

**Why it is seductive.** The parse *succeeds*. `dict.get()` returns `None` rather than raising, so
a wrong key produces a well-formed, plausible, specific answer. And the answer is uniform — every
lookup misses — which reads as a *strong* finding rather than a broken instrument. R478 names the
tell: *"a uniform negative across every key is almost never the data."*

**What it cost.**
* R338: `/v1/sources` keyed on `id` instead of `source` → every source read absent; **25,109**
  catalogue rows were one clean-looking confirmation away from deletion.
* R419: `Results.Data` instead of `Results.ParamValue` → **796,716 of 796,716** Regional titles took
  the fallback while the run reported 910,887 titles applied.
* R433: a probe reported *"28 queued insee ids ABSENT from the catalog"*; the dict mapped
  `series_id → end_date`, so a present row with a NULL end date was indistinguishable from a
  missing row. All 28 were present.
* R483: `<Series\b[^>]*>` over XML that legally contains `>` inside an attribute value → *"titled 0
  of 61 coded rows"* when INSEE names **59 of the 61**.

**The cheap test.** Print **one raw record** — the whole JSON entry, the whole row — before the
count. R433 states it as a standing rule: *"any ad-hoc counting script prints ONE sample record
alongside its count, and the count is not evidence until the sample confirms the keying."* Cost:
one line. R478 adds the even cheaper version for a negative result: `grep` the raw response body
for the literal string.

#### 3.2 Assuming a number from a log means what its label says

**The assumption.** That a field named `failed`, `obs_count`, `rows_written` or a line reading
`refusing shrink 648241->362203` measures the quantity its name suggests.

**Why it is seductive.** The number is real, precise and produced by the system itself — which
satisfies the very discipline the project imposes ("read what the system computes"). The failure is
one level subtler: you *did* read the system's output, and the system was measuring something else.

**What it cost.**
* R519: the second figure in `refusing shrink 648241->362203` is `|dedup(existing ∪ fresh)|` — a
  **union** count that equals our distinct count whenever the fresh pull adds no new pairs,
  **regardless of how many rows it has**. A truncated 5,000-row pull prints the identical line. It
  was used to justify accepting a 44% shrink whose remedy would have destroyed **603,467 rows** and
  made ~43% of two sources' keys unmarked import/export chimeras — **with nothing 404-ing**, and
  with the guard then passing forever.
* R494: D1 `rows_written` (~6 billed index rows per insert) reported as a download count →
  **~87,000/day** against a true **9,580–14,615/day**.
* R326: `obs_count` means *"rows this run"* on a productive run and *"whole store"* on a quiet one,
  so a healthy source appears to lose **168 million** rows.
* R151: `csv_derive failed 1415/3437 series` read as "1,415 tables serving stale CSVs" — and the
  codebase's own comment explains the number verbatim, naming the same source and nearly the same
  figure.
* R365: a summary line `"2 sources KEPT (60,192 series)"` read as this run's result. It is a
  **cumulative resume file**; `25,057 + 35,135 = 60,192` exactly, and the 25,057 was an old
  incident's bookkeeping. It produced a diagnosed "resurrection", two burned permission attempts
  and a near-miss urgent incident on the owner's list — refuted by one `SELECT COUNT`.

**The cheap test.** Read the line of code that *writes* the field. R361 gives the general form for
promises rather than counts: *"When code says 'retried later', grep for the READER before believing
it"* — the queue in question had **zero** readers and every parked id (43,354 in one run) was lost.

#### 3.3 Assuming a naming convention implies behaviour

**The assumption.** That `_module.py` means "parked", `.DONE` means "finished", `IMF_COMMODITY`
means `PROVIDER_DATASET`, or `sec_edgar` names one product.

**Why it is seductive.** Conventions are usually honoured, so the inference is usually right — which
is exactly what makes it dangerous. It is also *free*: reading a filename costs nothing, reading
every consumer of that name costs minutes.

**What it cost.**
* R516: a leading underscore was read as "parked", and *the finding's best line was built on it*.
  `_who_gho.py` is not parked; it is a **shared base module** that `who_hwf`, `who_rs` and
  `who_sdg` all import, and it ran three days earlier.
* R475: `jobs/ingest_gus_dbw.py` writes `logs/gus_dbw.DONE` to mean *"this pass completed"*.
  `RELAUNCH_GUARD.ps1` reads that exact filename as the **operator retire-flag**. The crawler
  retired itself and **1,237,766,278 rows** froze with no update path.
* R74: `provider_code` is `IMF_COMMODITY` — our own source id uppercased, which reads exactly like a
  `PROVIDER_DATASET` pair. Splitting it would have 404'd all **82** probes and produced a confident
  report of mass staleness built entirely on bad URLs.
* R275: `sec_edgar` denotes **two** products — the served XBRL per-ticker set (17,276 catalogue
  series, parity-proven) and an unserved 13F/insider giant (0 catalogue rows). The health gate
  called a healthy served source broken and the coverage audit called it "scheduled but not served"
  — two instruments wrong in opposite directions, one cause.

**The cheap test.** Grep every consumer of the name before relying on it. R66: *"NAMES ARE AN
INTERFACE... One file out of four used a different localStorage key and silently disabled the
site's primary user action. The sweep that finds this costs one grep."* R475 supplies the question
that generalises: **when a job signals completion, ask what else reads that signal.**

#### 3.4 Assuming a tool measured what it was pointed at

**The assumption.** That a `--r2` flag reads R2, that a path constant still resolves, that a sweep's
denominator is the population.

**Why it is seductive.** You chose the target deliberately and typed it into the command, so the
target feels like a fact about the run rather than a hypothesis about the code. The output then
*labels itself* with the target you asked for — R335's tool printed `(r2)` while scanning local
disk — which converts your assumption into apparent confirmation.

**What it cost.**
* R335: `audit_impossible_dates --r2` only picked a *listing function*; the read path consulted
  `AQUEDUCT_BACKEND`, so with no env var it scanned the **local** tree and printed `(r2)`. Local is
  a scratch mirror of the last run only, hence systematically *cleaner* than what users download —
  so the instrument failed **toward "fixed"**, and it was the check used to confirm a 104,501-row
  prune.
* R330: three tools with a stale hardcoded drive letter → `0 corrupt` across nine sources against
  **637,178** bad rows.
* R383 / R385: the derive reads the local mirror, and the verifier compares served bytes *against
  the local mirror*, so *"byte-compare 25/25 identical"* establishes only `served == local` and is
  structurally incapable of seeing that both are behind the store. A from-scratch comparison of
  **55,394** local files against **36,972** R2 objects then found **1,379 local files behind R2**
  (`ilostat` alone 952) where sampling had concluded "15 of 17 mirrors current" — plus **79** files
  *ahead*, a direction the guard could not even represent.
* The 2026-08-30 duplicate sweep: v1 built candidates as `<dir>/<dir>.parquet` **only**, so every
  multi-file store — i.e. all the largest — fell out silently while the script printed a reassuring
  count. Reported as **"308 of 308 stores scanned"**; actually **299 of 430**, with **122 never
  attempted**, of which **77 hold parquet** (`ibge` 12,125 files, `statcan` 8,207, `eurostat` 7,213,
  `cbs_nl` 5,511, `owid` 3,787…). The tell walked past: *"a whole-store sweep should have a
  denominator near the catalogue's 322, and 308 is neither 322 nor 430."*

**The cheap test.** Print the **resolved** target, never the flag; and check the denominator against
an independently known population size before reading any coverage number.

#### 3.5 Assuming a fix propagated to sibling code and workflows

**The assumption.** That fixing the instance fixes the class, because the code "looked like the only
place".

**Why it is seductive.** Local consistency reads as completeness. R279 states it exactly: after
fixing three of four identical calls that sat together, *"the block then looked uniformly correct"*
— and the fourth, sixty lines away under a different concern, was the one that failed the **whole
run** rather than one table of 26.

**What it cost.**
* R390: an assumption fixed in one tool, still live in three others, *including the guard whose
  entire job is to stop a derive running from an unchecked mirror*. Two of the three run
  unattended, one of them a daily CI step that had silently excluded an entire storage tier
  including 17,276 served series.
* R428: the `if: always()` fix landed in the daily workflow on 2026-07-30 and was never ported to
  the heavy — the workflow whose jobs are *designed* to exit non-zero, where the fix was needed
  more. Three consecutive multi-hour passes persisted nothing.
* R333: most sources have **two** parsers — an ingester for backfills and a fetcher for the nightly
  tick. A grammar fixed in the ingester and verified with a real live call proves *the half that
  does not run nightly*.
* R95: one licence row corrected and reported done; the identical fingerprint was live on ten more
  serving rows covering **105,301 series**.

**The cheap test.** The moment you fix an assumption, `grep` for it and **put the grep in the
commit**. R390: *"`os.listdir` next to `parquet` was a five-second search that found four more."*
R256 sharpens it: enumerate the class by `grep -l '<the exact defective line>'`, because *"a domain
list is only as complete as my recall, and nothing about five successful patches hints that three
files are missing."*

#### 3.6 Assuming a doc describes the current system

**The assumption.** That a tracking document, runbook, registry comment, task title or docstring
reflects the system as it is now.

**Why it is seductive.** Documents are written by people who *did* verify — at the time. The
document therefore carries authentic evidential weight, and nothing about reading it reveals its
age. R284 identifies the specific literary form that makes it worse: an exclusion written as a
**property** ("these files do not gain periods") instead of a dated **measurement** invites no
re-measurement, and 16 flows sat two months behind for as long as that sentence stood.

**What it cost.**
* R349: the runbook told **215 of 226** served sources they were not served.
* R408: a tracking doc described a gate that does not exist; 18,464 series were live all along.
* R160: `registry.yaml` asserted that IPEA's API accepts a server-side date filter. It does not —
  raw `$filter`, URL-encoded `%24filter` and cutoffs 2020 and 2026 all return **HTTP 200 and the
  full series (68/68)**. A fetcher was written around a parameter the server silently discards.
* R404: the public API reference documented an API nobody ever built — a `/v1/bars/{ticker}/daily`
  endpoint, a `/v1/bulk/{package}` endpoint with eight named packages, six query parameters, and
  two invented JSON response examples. At ~18 registrations/day the page had been sending every
  developer into the same wall for weeks. **A paying-attention user found it before any internal
  check did.**
* This document's own §0.2: the ledger's header says the archive is 8,600 lines (it is 13,492) and
  `CLAUDE.md` says the ledger holds "150+ entries" (519 rule ids).

**The cheap test.** Verify the doc's claim against the deployed artifact — and R408 adds the half
that is usually skipped: *"when a doc and the code disagree, FIX THE DOC in the same pass, or the
next reader repeats the error."*

#### 3.7 Assuming a cheap check and an expensive one agree

**The assumption.** That a proxy (a timestamp, a file size, an object count, an aggregate status, a
re-run of the same query) stands in for the expensive measurement.

**Why it is seductive.** The proxy is *usually* correlated, and when it agrees you have saved real
time — so the habit is positively reinforced on every occasion except the ones that matter.

**What it cost.**
* R342: an audit's figure was called impossible on a bytes-per-key estimate, and the doubt was
  "confirmed" by **re-running the audit's own query** and printing `matches: True`. That proves
  determinism and nothing else. A genuinely different instrument — parquet footer metadata, no scan
  — matched the row count exactly (**976,632,535**, ratio 1.0×) and showed the plausibility argument
  was wrong.
* R512: three confirmations counted as three, when all three were the same machine reaching the same
  host. **Corroboration is counted in independent vantage points.**
* R383: `LastModified` is upload time, not content change; nine sources were labelled *"REPAIR USED
  STALE LOCAL DATA"* on that basis and **seven of the nine were false**. Even md5 is the wrong
  instrument here: five of seven genuinely differing files had identical rows and identical maximum
  dates and differed only in parquet encoding.
* R100: a stale-file sweep compared a CSV's write time against its parquet's, and a parquet is
  rewritten every run whether or not a row changed. It returned **429,560 of 2,503,070** — 17% of
  the library, an apparent emergency. Every one of the top four sources content-checked was
  **current**.
* R249: *"when a cheap check and an expensive one disagree, the cheap one is wrong, not 'close
  enough'"* — a `grep` counted a docstring, and an AST call-site count cannot distinguish a merge
  *inside* a loop from one *after* it. A claim about runtime behaviour covering four modules that
  were never opened went into a pushed commit message.
* R506: **four** instruments agreed — file size, `zipfile.testzip()`, our own sha256 sidecar, and an
  md5 against the publisher — on a file whose XML repeats **166,910** records. All four ask *"are
  these the bytes we received?"*; the defect is one layer up.

**The cheap test.** Verify with a **different method**, not a repetition of the same one — and ask
what question each instrument actually answers before treating agreement as corroboration.

#### 3.8 Assuming a column set is a key

**The assumption.** That the obvious identifying columns uniquely determine a row.

**Why it is seductive.** The column names describe an identity, and the schema was designed by
someone competent. The failure lives in what the *producer discarded*, which the schema cannot show
you.

**What it cost.**
* R387: XOM holds **20,629 rows, 20,578 distinct 4-tuples, and 18,616 distinct** on the assumed
  key — because the parser keeps `end/val/filed` and drops `start`, so a filing's 3-month and
  9-month figures for one period end are two legitimate facts. Applying the assumed key would have
  dropped **2,013 rows from the very file being repaired for row loss**.
* R280: **11 of 24** census flows were under-keyed. `exports/statehs` held **3,356,888 rows under
  4,400 distinct `(series_key, obs_date)` pairs**. The first merge collapses the file, never-shrink
  refuses it, and the source then fails every run as a baffling "refusing shrink" that reads as a
  fetcher bug.
* R133: `cso` has **7,988** `(file, prefix)` pairs but **7,896** distinct prefixes, so 92 tables
  would have been written twice and served holding only the last file's slice — *no error, no short
  read, just missing rows*.

**The cheap test.** `SELECT COUNT(*), COUNT(DISTINCT <key>)` — one query. R387: *"'it is obviously
the key' is how you delete rows that are supposed to be duplicates."*

#### 3.9 Assuming absence from a listing is absence from the world

**The assumption.** That if an item is not in the publisher's index, catalogue or listing, it does
not exist or has been retired.

**Why it is seductive.** A listing is the publisher's own authoritative-looking statement, and
absence from it is a clean, actionable signal — exactly the kind that justifies a deletion.

**What it cost.**
* R510: **27** `cso` matrices were about to be filed as discontinued on *"0 of 27 listed by PxStat
  ReadCollection."* Re-measured: **495 of our 7,896** matrices are unlisted, and `ReadDataset`
  returns **data for 8 of 8 sampled** (two updated 2024/2025). Nothing is discontinued. And because
  `cso.py:303` builds its `changed` set **from that listing**, an unlisted matrix can never be
  re-pulled: **6.3% of `cso` is frozen by construction and reports nothing.** Filing them as
  archival would have documented a live fetcher bug as correct behaviour.
* R61: six `defillama` series were missing from `/protocols` and resolved to zero rows —
  *"retired slugs, delist them"* was the obvious read and was **wrong**. All six answer HTTP 200 at
  `/protocol/<slug>`; they are **parent** entities, and the crawl iterates a listing that carries
  only children.
* R75: an exact-id match against IMF's dataflow list became *"10 datasets retired, 45,000 series can
  never update."* They had been **renamed** — `PSBSFAD→PSBS`, `PCTOT→CTOT`, `GENDER_*→GS_*` — with
  identical series counts. *"Identical cardinality on both sides (4,320 vs 4,320, 14,018 vs 14,018)
  with zero matches is proof the TEST is broken, never proof the data is gone."*

**The cheap test.** Request the **data**, not its index. And ask the inverse question R510 names:
*what is in your store but not in the list that drives your updates?*

#### 3.10 The common structure of all nine

Every assumption above shares one property: **it is confirmed by the ordinary success of the
operation.** The parse succeeds, the tool exits 0, the count prints, the guard passes, the doc
reads coherently. None of them produces an exception, a red, or a blank — which is why they survive
review, and why the ledger's most-repeated advice is not "be careful" but a specific instruction to
manufacture a *failure condition* that does not otherwise exist:

* put a control in the probe (§1.1)
* print the denominator (§1.8)
* ship the discriminating pair (§1.6)
* run the identity case (§1.1, R413)
* use a second instrument, not the same one twice (§3.7)
* count the units of work, never the iterations (R94)

---

### 4. The direction of error

#### 4.1 The claim under test

Recent entries assert that the errors are not random but biased toward inflating findings. The
sharpest statement is R516, written 2026-08-30:

> *"Every error I made inflated the finding."*

and R518, the same day, quantifies one instance at roughly **440×**. The question is whether that
generalises.

#### 4.2 Coding rules

I coded each directional claim into one of four buckets. The coding is mine; the ids are listed so
it can be checked.

* **INFLATE** — a defect, absence, blocker, restriction or cost was claimed and measurement showed
  it smaller, elsewhere, or non-existent. Includes "the system is broken" when it was not, and "we
  cannot do X" when we could.
* **UNDERSTATE / FALSE ALL-CLEAR** — something was called complete, verified, healthy, served or
  safe when it was not; or an estimate was optimistic.
* **DESTRUCTIVE** — an action was taken or planned that would have damaged data, independent of the
  direction of any claim.
* **PROCESS** — hygiene, tooling, shell, git or ledger failures carrying no directional claim about
  the system's health.

Entries with claims in both directions are counted in both and marked.

#### 4.3 What actually reached the owner (n = 56)

Of the 335 digest lines, **56** explicitly name Ahmed — i.e. the claim was delivered to the person
paying for and relying on the system. This is the population the claim in §4.1 is really about,
because it is the only one whose errors cost him anything. I coded all 56.

| Id | The claim as made | Direction |
|---|---|---|
| R365 | "ksh has been resurrected" (a do-not-resurrect source) | INFLATE |
| R411 | coverage "70.7%" (true 83.1%) | INFLATE |
| R412 | "retiring these two would visibly shrink what users can download" — both retired weeks earlier | INFLATE |
| R413 | crosswalk "REFUTED — do NOT re-key" on 6,776/6,776 | INFLATE |
| R416 | "Tadawul prohibits redistribution" — no source; terms in fact permit it | INFLATE |
| R408 | "`ei_statreview` is still gated" — 18,464 series live | INFLATE |
| R398 | "blocked on the UNCTAD key" — key in `.env` since the night before | INFLATE |
| R432 | "the SSO flow has never been walked by a real fresh account" | INFLATE |
| R379 | "280 of 540 ecb files were NEVER fetched" | INFLATE |
| R101 | audited why a job never ran; the runs were single-source dispatches, and the code postdated the last cron | INFLATE |
| R104 | "UNESCO's terms page is unreachable" — the URL was in our own canonical file | INFLATE |
| R106 | nearly reported **17,274** companies invisible to search | INFLATE |
| R114 | "D1 is correct, un_wpp's licence is NEEDS-REVIEW" — publisher grants CC BY 3.0 IGO | INFLATE |
| R115 | "the WID derive is dead" — running at ~42 CSVs/sec | INFLATE |
| R122 | IEP "needs human review, may I email them" — permission granted 2026-07-06 | INFLATE |
| R141 | "ksh is NOT a subset" (3,363/25,057 keys) — 96.4% redundant at table grain | INFLATE |
| R146 | ons_uk "3,897,884 series titled with opaque keys" — true count 0 | INFLATE |
| R149 | "ksh keys columns by numeric index" — 3.2%, not the rule | INFLATE |
| R250 | "the R2 catalogue refresh is classifier-blocked" — later ran with no denial | INFLATE |
| R470 | escalated a retire-or-re-derive decision the repo's own auditor had already answered | INFLATE |
| R479 | "your data is not being used" — the page carries a full CC BY attribution with the DOI | INFLATE |
| R482 | "found it, and it's user-visible" (FTS duplication) — then disproved it | INFLATE |
| R494 | "~87,000 downloads/day" — true 9,580–14,615 | INFLATE |
| R500 | described 35 rows as dead to obtain a delete authorisation; all 35 were live | INFLATE |
| R502 | every D1 dollar figure ever given, priced from row zero against a 25B allowance | INFLATE |
| R504 | "USPTO recognises the key then refused it" — the key worked | INFLATE |
| R512 | "istat: IP-blocked, not slow" — the host is down for everyone | INFLATE |
| R515 | "69,590 **served** ids affected" — 0 are served (source is gated, live 451) | INFLATE |
| R403 | "element re-code recovered 27%→79%" — an id-level figure presented as recoverable series | UNDERSTATE |
| R422 | "45–75 minutes to completion" — a second multi-day phase remained | UNDERSTATE |
| R423 | "re-read from cache (~2h, no network)" — first phase is a live ~35k-node re-walk | UNDERSTATE |
| R427 | "the derive checkpoints on R2, it resumes" — relaunched without the resume flag | UNDERSTATE |
| R431 | "statcan derive is COMPLETE — 8,207/8,207" — the derive's own log said 7,964/8,207 | UNDERSTATE |
| R385 | published a 16-line table of "verified" repairs built on a tool structurally unable to verify | UNDERSTATE |
| R390 | reported a class fixed; three tools including the guard still had it | UNDERSTATE |
| R143 | "un_wpp is already wired" — believed a table whose heading did not match its SQL | UNDERSTATE |
| R145 | "the publisher gives us no per-series title" — wrong in 3 of 4 sources | UNDERSTATE |
| R490 | (reviewer-found) seven sources' pages promise downloads the data plane refuses, and vice versa | UNDERSTATE |
| R410 | "the loop was never created" — job `9814fb58` existed and had been announced to him | MISREPORT |
| R429 | a page edit silently redeployed the worker and reverted his admin-link fix | DESTRUCTIVE |
| R503 | 863,253 rows deleted outside the approved scope | DESTRUCTIVE |
| R118 | ran a write tool to observe its output: inserted **9,920,979** catalogue rows for a source held pending his decision | DESTRUCTIVE |
| R492 | a 3.93 × 10¹² row plan estimated at 90 minutes | PROCESS (cost) |
| R430 | the query shape that billed ~$82 in one day; he found it on the invoice | PROCESS (cost) |
| R493 | the reliability system shipped with five defects of the kinds it polices | PROCESS |
| R495 | "the old 79.8B was the US census source" — it was statcan | PROCESS |
| R485 | nine entries, zero digest lines | PROCESS |
| R395 | seven ledger commits never pushed | PROCESS |
| R409 | abandoned the standing task for a problem already disclosed and accepted | PROCESS |
| R148 | findings scattered into new documents | PROCESS |
| R370 | `git add -A` swept 37,778 CSVs / 1.1 GB into a pushed commit | PROCESS |
| R477 | handed him bash syntax to paste into PowerShell; parser error twice | PROCESS |
| R1, R8, R43, R46 | standing-order / policy entries, no directional claim | PROCESS |

**Result for the owner-facing population:**

| Direction | Ids | Share of the 38 directional claims |
|---|---|---|
| **INFLATE** | **28** | **73.7%** |
| UNDERSTATE / false all-clear | 10 | 26.3% |
| Misreport of own actions | 1 | — |
| Destructive | 3 | — |
| Process / cost / no directional claim | 14 | — |

(53 table rows, 56 ids — the last row covers four policy entries. 28 + 10 + 1 + 3 + 14 = 56.)

**Among claims that reached the owner, inflation outnumbers false all-clears 28 : 10, or roughly
3 : 1.** The claim in §4.1 is supported for this population.

#### 4.4 The recent block, R468–R519 (2026-08-24 → 08-30)

Coding the 52 most recent rule ids — 53 entries, counting `R473b` separately; the block the "we
consistently inflate" claim was written from — by the same rules gives a **different** answer:

| Direction | Count | Ids |
|---|---|---|
| INFLATE | **17** | R468 R470 R478 R479 R482 R483 R484 R500 R504 R509 R510ᵃ R512 R513 R514 R515 R516 R518 |
| UNDERSTATE / false all-clear | **14** | R472 R474 R475 R480 R481 R486 R487 R489 R490 R491 R497 R501 R510ᵃ R511 |
| Destructive plan stopped in review | 6 | R488 R498 R499 R500 R503 R519 |
| Cost, wrong direction stated | 6 | R492↓ R494↑ R502↑ R505↓ R507↓ R508↓ |
| Process / operational | 12 | R469 R471 R473 R473b R476 R477 R485 R493 R495 R496 R506 R517 |

ᵃ R510 is counted in both: it inflates a claim about the publisher (27 matrices "discontinued" —
none are) and simultaneously conceals a defect of ours (495 matrices frozen by construction).

Here inflation and false all-clear are **near-parity, 17 to 14**. So the strong form of the
claim — *"the mistakes consistently inflate findings"* — is **not supported over the ledger as a
whole**.

#### 4.5 The asymmetry that reconciles the two results

The two populations differ in one respect, and it explains the difference completely:

> **Errors that inflate get spoken. Errors that reassure stay silent until something else trips
> over them.**

An inflated finding is, by construction, *interesting*. It becomes a message, a commit message, a
task, a decision request — and so it enters the population that names Ahmed, where it is then
refuted and ledgered. A false all-clear produces no message at all. It produces a green run, a
closed task, and silence. It enters the ledger only when a *different* investigation stumbles into
it, often weeks later, and frequently only because an adversarial reviewer was pointed at it.

The ledger supplies direct evidence of the lag:

* **R421** — the HF site served 12-day-stale data. Seven consecutive daily runs died identically
  while sessions pushed unrelated work to that same repository daily. *"the workflow conclusion
  'cancelled' every day for 10 days was one `gh run list` away."* **The owner reported it.**
* **R430** — the $82/day query shape. *"The design flaw was mine and OLD... the DETECTION was Ahmed
  reading his bill — no spend alert, no insights check had ever run."*
* **R404** — a public API reference documenting endpoints that were never built. Found by a
  registrant who followed the page and emailed to ask whether *his* key was broken.
* **R355** — a fetcher's manifest read with `os.path.exists` + `open`; in CI it never existed, every
  run cold-started, and **the served store froze four months behind** while the daily report read
  *"63/63 drained, backlog clear, 0 rows"*.
* **R475** — `gus_dbw` retired itself; **1,237,766,278 rows** froze with no update path, under a
  note that said the situation was *"expected to resolve itself"*.
* **R510** — 495 `cso` matrices frozen by construction, *"reports nothing"*, found only while
  investigating an unrelated false alarm.

Every one of these is a false all-clear, and not one of them announced itself. Contrast the
inflation cases: R512, R504, R518, R482 and R514's three stories were all refuted **within hours**,
because a confident claim invites a check and a green run does not.

**The practical consequence for a reader:** the visible error rate is a biased sample of the true
error rate, and it is biased *toward* the harmless direction. The errors you can see are mostly the
ones that overstate a problem. The errors you cannot see are mostly the ones that understate one.

#### 4.6 The same asymmetry in the cost line, with the sign reversed

Cost errors run the other way, and the reason is the same mechanism seen from the other side:

| Entry | Direction | Magnitude |
|---|---|---|
| R502 | **Over**stated cost | Priced D1 from row zero, ignoring 25B reads + 50M writes included per month; *"the title repair's '$1.49' may truly have been $0"* |
| R494 | **Over**stated volume | 87k vs 9.6–14.6k downloads/day |
| R492 | **Under**stated cost | 90 minutes estimated vs ~24 days and ~$2,500 (~380×) |
| R505 | **Under**stated cost | R2 operations (~$31/mo, the largest variable line) printed and omitted from the total; $24 → $75 |
| R507 | **Under**stated exposure | The alarm read an instrument that measures 10× low; the two catastrophic days tripped it anyway, *"the SHOULDER days — the ones where stopping is still an option — did not"* |
| R508 | **Under**stated cost | A fail-open silently deleted ~15% of the bill under a confident label |

Four of six understate. And the invoice showed the modelling error was in the *shape* as well as
the level: the line optimised hardest (D1 reads) cost **$0.00** that period, while the largest
actual charge was **R2 class-A operations at $94.50**, which the meter did not measure at all until
2026-08-29.

That is the same bias in a different currency: **the reassuring reading is the one that survives,
because nothing contradicts it until the bill arrives.**

#### 4.7 Verdict

1. The claim *"the mistakes consistently inflate findings"* is **true of the claims that reach the
   owner** (28 of 38 directional claims, 73.7%) and **not true of the ledger as a whole** (17 vs 14
   in the most recent 53 entries).
2. The two results are not in conflict. They measure different populations, and the difference
   *is* the finding: **inflation is loud and gets corrected; false all-clears are silent and get
   discovered by accident, by the owner, or by a user.**
3. The three most expensive incidents in the ledger — R421 (12 days of stale public data), R430
   (~$82 in one day, ~130 billion rows), R404 (weeks of developers hitting documented endpoints
   that do not exist) — are **all false all-clears**, and **all three were detected outside the
   system**: by the owner twice and by a paying-attention user once.
4. Therefore the correct reading is not *"discount the alarming findings"*. It is:
   **discount the alarming findings by about 3 : 1, and treat every green as unmeasured until you
   can name the check that could have gone red.**

**NOT ESTABLISHED:** a direction coding for all 335 digest lines. I coded 56 ids (the owner-facing
set) and 53 (the recent block), overlapping in 16, for **93 distinct ids — 27.8% of the digest**.
What would establish the full picture: coding the remaining 242 lines against the same four
buckets, ideally by a second reader, and reporting inter-rater agreement.

---

### 5. What to distrust, in order

A reader's summary. Each item names the signal, why it is untrustworthy, and the one cheap thing
that settles it.

1. **A number with no instrument beside it.** This is why `NUMBERS.md` exists; the project's public
   headline drifted from "~77 billion points / ~7 billion series" to ~30 to ~20 with no instrument
   attached to any of the three. *Settle it:* ask which command produced it and when.

2. **A negative result — "absent", "missing", "0 found", "not provided by the publisher".**
   Class A. R484 records **five tools in one session** reporting a publisher gap that was a reader
   bug, and notes that four of the five *phrased it as a fact about the source*. *Settle it:* run
   the same probe against something known present.

3. **A round number in the "all of them failed" direction.** R484: *"suspect the reader when the
   number is round in the 'all of them failed' direction, because genuine publisher gaps are usually
   partial."* `0 of 61`, `0 of 6,776`, `796,716 of 796,716`, `0 of 74`. *Settle it:* print one raw
   record.

4. **A claim with no hedge.** R410: all three of that night's false claims were delivered flat,
   *"with the cadence of a verified finding"*. *Settle it:* ask what would have to be true for the
   claim to be wrong, and whether that was checked.

5. **A green check, a passing guard, an exit 0.** Class F. Ask what input would make it red, and
   whether that input was ever fed to it. R345/R346: *"Before trusting a check, feed it something
   that MUST fail, and verify your control really is negative."*

6. **A count with no denominator.** Class H. *"0 defects in 0 files examined is not a result."*
   *Settle it:* demand the two numbers — what was found, and what could not be reached.

7. **"Deployed", "live", "served", "complete", "verified".** Class D. In this project the worker is
   deployed by hand and nothing in CI does it. *Settle it:* a response from the running endpoint,
   not a line in a file.

8. **A cause attached to an observation.** Class L. Report the observation at full confidence and
   the cause at the confidence its test earned. *Settle it:* name the observation that would refute
   the cause, then make that one call.

9. **A document, a task title, a docstring, or an earlier ledger entry, as evidence about now.**
   Class B, §3.6. R509 is the sharpest case: a fix built on the author's own digest line, whose
   premise measured false. *Settle it:* query the store, or request the id.

10. **Agreement between two instruments that share a path.** §3.7. Four instruments agreed on a
    corrupt file because all four asked *"are these the bytes we received?"*. *Settle it:* name the
    question each instrument answers before counting them as two.

11. **Most of all: a system that has been quiet.** §4.5. Every one of the longest-lived defects in
    this ledger was silent — 4 months (R355), 45 days (`eurostat`, frozen on a stale file count),
    40 days (`istat`), 20 days (an NC licence advertised as commercially usable, R358), 12 days of
    stale public data (R421). Silence is the signature of this failure mode, not evidence against
    it. *Settle it:* for anything important, name the check that would have gone red — and if you
    cannot name one, the thing is unmeasured, whatever its status says.

---

### Appendix — commands run for this section

Every figure attributed to "counted" or "verified" above came from one of these, run read-only:

```bash
# ledger shape
wc -l  D:/research/hfdatalibrary/.claude/MISTAKES.md          # 14010
wc -c  D:/research/hfdatalibrary/.claude/MISTAKES.md          # 1492457
grep -c "^- R[0-9]" MISTAKES.md                               # 335

# 519 distinct rule ids, no gaps, from three id forms
python - <<'PY'
import re
t=open('MISTAKES.md',encoding='utf-8').read(); L=t.split('\n')
digest={m.group(1) for l in L if (m:=re.match(r'^- (R\d+)\.',l))}
entry ={m.group(1) for l in L if (m:=re.match(r'^#{2,3} (R\d+)\b',l))}
marks =set(re.findall(r'\*\*Rules?:\*\* \[?(R\d+)',t))
nums=sorted(int(x[1:]) for x in digest|entry|marks)
print(len(nums), nums[0], nums[-1], [i for i in range(1,nums[-1]+1) if i not in set(nums)])
PY
# -> 519 1 519 []

# entries with no digest line
# -> 147

# ledger commit cadence
git -C D:/research/hfdatalibrary log --oneline --since=2026-07-01 -- .claude/MISTAKES.md | wc -l   # 505

# the mechanical checks, as they stand today
python .claude/skills/adversarial-review/tools/ledger_check.py --digest    # PASS, 111 headings seen
python .claude/skills/adversarial-review/tools/ledger_check.py --numbers   # PASS, 119 rows
python .claude/hooks/test_reliability_system.py                            # 33 checks, all passed

# independent reproduction of R513's prefix-pair property, local read-only SQLite
python - <<'PY'
import sqlite3
con=sqlite3.connect("file:E:/research/econfindatalibrary/data/catalog.db?mode=ro", uri=True)
ids=[r[0] for r in con.execute("SELECT source_id FROM source")]
pairs=[(a,b) for a in ids for b in ids if a!=b and b.startswith(a)]
print(len(ids), len(pairs), sum(1 for a,b in pairs if b[len(a)].isdigit()))
PY
# -> 349 source ids, 19 prefix pairs, 0 digit-extended  (matches R513 exactly)
```

No command in this section wrote to any store, queried Cloudflare D1, deployed anything, or
contacted a publisher.
## 9. The problems, and the repeat mistakes

Ahmed asked for two things in one breath: the problems in the system, and the repeat mistakes I
make trying to fix those problems. They belong together, because the second is now a bigger risk to
the library than the first.

---

### PART A — THE PROBLEMS IN THE SYSTEM

#### A1. The key-collision class — the largest open data defect

**What it is.** A series key that omits a dimension the publisher actually varies. Several
genuinely different series then collapse onto one identifier, and a user asking for one series
receives several, interleaved, with nothing to tell them apart.

**Confirmed, served, measured (2026-08-30):**

| Source | Served ids | Dropped dimension | Evidence |
|---|---|---|---|
| `eia` | 268,502 | frequency (`.A`/`.M`/`.Q`/`.D`/`.D.H`) | 142,073 ids (52.9%) gather >1 publisher series; `ELEC.PLANT.GEN` gathers 172,766 |
| `idb` | 18,838 | every column but dataset + country | one id returns 2,805 rows over 13 dates |
| `unctad_tradefoodproccatprocrca` | 19,087 | `Flow` (Imports/Exports) | publisher `$metadata` names it in the Fact key |
| `unctad_tradefoodproccatcatrca` | 17,617 | `Flow` | 286,038 publisher two-flow cells = 286,038 duplicated pairs, zero residue |
| `damodaran` | 24,687 (721 collided) | worksheet name | publisher workbook confirms the wrong value is served |

**The sharpest single consequence.** `eia:ELEC.PLANT.GEN` currently serves a **566 MB** object when
a user asks for one series; `ELEC.PLANT.CONS_TOT_BTU` serves **658 MB**. The CSV's own first column
changes from row to row, so the file is self-evidently a bundle rather than a series. This is a
correctness defect, a usability defect and an egress cost simultaneously.

**The clearest wrong number.** The publisher's workbook gives India `Adj. Default Spread =
0.0209`; we serve **0.3**, which is India's corporate tax rate collided in from another sheet. The
default-spread-by-rating ladder breaks required monotonicity at **9 of 19** steps — Aaa is served a
*higher* spread than Caa3, which cannot be true under any convention.

**The systemic cause.** **146 of 212 ingest jobs write parquet directly**; **zero** route through
`merge_and_write`, the one function that dedups on `(series_key, obs_date)`, refuses shrinks, and
reports impossible dates. Every *fetcher* uses it. No *ingester* does. The stores were therefore
born without the invariant that the maintenance path assumes.

**Why no alarm ever fired.** Every health check asks whether a source ran, returned rows and stayed
fresh. A collided source does all three. **No check asks whether the key is at the publisher's
grain**, and a collision looks exactly like a healthy source with more observations per series.

**Why it is not fixed.** Every remedy is a **re-key**, and re-keying changes public series ids —
which project precedent (ledger R275/R276) makes Ahmed's decision, not mine.

#### A2. Updates that reach the store but never reach the user

A source can be perfectly current in the parquet store and still serve stale files, because the
*derive* step — turning the store into per-series CSVs on R2 — is separate and budgeted.

Measured from each source's latest run record: **73,125 changed series keys across 20 sources map
to no catalogue id at all**, led by `eia` (50,000 — which is exactly `CURSOR_CAP`, so that figure is
a cap, not a count), `owid` (12,192) and `sipri_polity` (6,513). For `eia` that means a run banking
**+235,050,106 new rows** delivered nothing to a single user.

Separately, **231,782 series sit in the CSV retry queue**, oldest entry 12 days. **183,735 of them
are hard `UnitTimeout` crashes**, not the designed graceful deferral, and **not one row has ever
reached a second attempt**.

#### A3. The gap between "the updater is green" and "the data is right"

The health gate measures liveness. It cannot see grain errors, values from the wrong column, or a
catalogue that is coarser than its store. Three of the five collided sources above have never
raised a flag.

#### A4. Structural fragility that keeps producing new defects

* **Two parsers per source.** Most sources have both an ingester and a fetcher. A fix in one is not
  shipped in the other, and only one of them runs nightly.
* **Five places a series lives** — R2 CSV, D1 `series`, D1 `series_fts`, local `catalog.db`,
  `source_counts` — with no foreign keys between them. Deleting from four leaves a user-visible
  404 or, worse, a catalogue advertising rows it cannot deliver.
* **A single-writer cache.** `source_counts` is written only by the sync, so every correct direct
  write silently invalidates it — and a missing row silently restores the query shape that once
  billed $82 in a day.
* **Manual deployment.** The worker deploys only by hand. Committing is not shipping, and a push to
  `main` can redeploy the whole stack as a side effect of a one-page edit.

#### A5. Cost as a permanent hazard

The architecture bills per row read and per operation, and the failure mode announces itself only on
the invoice. Two incidents (~$82 in a day from a query shape; a planned batch that would have cost
~$2,500) were both caught by something other than routine monitoring. The guard now exists and
reconciles to a real invoice at −0.11%, but the underlying property has not changed: **a bad query
shape is a billing defect, not a performance nit.**

---

### PART B — THE MISTAKES I KEEP MAKING WHILE FIXING THESE

This is the part that matters more, because a system defect sits still and my errors compound.

#### B1. The master pattern

The ledger's own first entry (R0) names it, and everything since has been a variation:

> **the error is my measurement's SHAPE, not my question.**

I ask a reasonable question, build an instrument that answers a *slightly different* question, and
then report the answer as though it addressed the original. The instrument usually works. The
mismatch is invisible from the inside, because the number arrives formatted and plausible.

#### B2. The eight repeating classes, with live examples

**1. The probe that cannot succeed.** An instrument that could never have returned a positive,
reporting absence as a finding. A parser keyed on `id` when the API emits `source` — so *every*
source read as missing (R134, R316, R338, R433, R478: **five occurrences of one shape**). An auth
probe with invented header names "confirming" a key was dead (R418). A crosswalk that compared
period-start dates against period-end dates and returned NO-OVERLAP on 6,776 of 6,776 (R413).
**The refutation is always the same and always cheap: run it against something known present.**

**2. Asserting from an artefact instead of the running system.** A doc said a source was gated; the
worker had never gated it (R408). A task list said SSO was unverified; the database held 1,006
users and 845 logins (R432). A summary file said a source was resurrected; the store said zero rows
(R365). This session: I called `who_gho` "a served source" — it has zero catalogue rows and returns
451 (R516).

**3. Claiming deployment from a commit.** Reporting 425,462 series "live" while the worker holding
them had not been deployed for two weeks and nothing in CI deploys it (R345).

**4. A guard or check that cannot fail.** A shrink guard shipped with no test that refused *every*
legitimate seed and left CI red (R414). A completeness proof that passed identically on the correct
and the catastrophic outcome (R488). A titles check that reports PASS on a search index that has
been destroyed (R492).

**5. Reading one of N paths and reporting it as the total.** Coverage computed from one of four
schedulers, reported for hours as a measurement — and then, *after writing the rule*, recomputed as
a three-term union of a four-term definition (R411, R262).

**6. Silent empty results.** A listing that returns `[]` instead of raising: a tree walk that found
nothing under R2 and silently reinstated the staleness it existed to remove (R261). *"0 defects in
0 files examined is not a result"* (R330).

**7. Unanchored matching.** `Prefix="series/imf_fsi"` also matches `imf_fsire`, reporting 18,620
healthy files as orphans (R129). Four such bugs in a single day (R112, R129, R137, R142).

**8. Key-grain and dimension errors.** The whole of Part A.

#### B3. The repeat offenders — where the rule was written down and then broken

This is the specific thing Ahmed asked about, and the ledger is unsparing about it:

| Rule | Broken again | Gap |
|---|---|---|
| R40b: take the ingester's proven rate limit | R132, same host (ONS) | days |
| R135: use absolute paths for ledger writes | recurred writing the rule's own session | hours |
| R336: never pipe a long job to `tail` | R348, twice | one hour after re-reading it |
| R425: read the CI of what you push | R426 | under 24 hours |
| R422: enumerate remaining phases before an ETA | R423 | two hours |
| R418: a probe must use the target's own code path | R419 | the next day |
| R411: compute the union a definition specifies | R411's own second half | same session |
| R328/R485: every entry needs a digest line | R485, "Ahmed caught it" both times | months apart |

**Why does writing the rule not prevent the repeat?** The ledger proposes two mechanisms and I
think both are right:

* **R485 — the write and the read never meet.** Appending an entry is a write at the *end* of a
  file whose instruction lives at the *start*. The workflow that creates the lesson never visits
  the place that states it.
* **R348 — the trigger is syntactic, not topical.** Nothing about a task's *subject* cues "you are
  about to pipe a long job to `tail`". Only scanning the command line does. A rule keyed to a topic
  cannot fire on a rule violated by a keystroke.

To which I would add a third from this session: **a label applied early survives the evidence that
should remove it.** I carried "two vintages merged" for the UNCTAD stores from the first hour to
the last, and proposed a remedy built entirely on that label, long after I had measured facts that
contradicted it.

**What actually works** is not a better-remembered rule. It is a *mechanism*: the digest check that
fails the commit, the cost hook that refuses the command, the reading gate that requires contiguous
coverage, and above all **a second agent instructed to find the flaw**. Prose rules did not hold.
Programs and reviewers do.

#### B4. The assumptions, and why they are seductive

| Assumption | Why it feels safe | What it actually costs | The cheap refutation |
|---|---|---|---|
| A field is called what I expect | The parse succeeds and prints a plausible total | Every lookup misses; a healthy system reads as broken | **Print one raw record** before counting |
| A number in a log means its label | It came from the system, so it is a measurement | The UNCTAD remedy: a union-dedup count read as a fresh-pull row count, nearly deleting 603,467 rows | Ask *what computed it*; if it would look identical under the hypothesis you are ruling out, it is not evidence |
| A naming convention implies behaviour | Conventions are usually honest | I called a live shared module "parked" and built a narrative on it | One `grep` for imports |
| My tool measured what I pointed it at | It ran and produced numbers | "308 of 308 stores" measured 299 of 430 and skipped the biggest | Reconcile the denominator against an independently known population |
| A fix propagated to siblings | The fix was correct where I made it | A state-push guard fixed in the daily workflow, never ported to the heavy one — three passes of work discarded | `grep .github/workflows` for the step name |
| A doc describes the current system | It was written by someone who knew | Advising on a trade-off between two sources that had been retired weeks earlier | Query the store |
| Re-running my query verifies it | The same answer twice feels confirmed | Proves determinism and nothing else | Use a **different instrument** |
| A clean result means clean data | Zero findings is good news | An eia probe returned "0 colliding" because it had read 60 metadata files and zero data files | Carry a **positive control** that must detect the defect |

#### B5. The direction of the error

This is the single most useful fact in this document for auditing me.

In this session I made ten recorded errors. **Every one of them made the finding bigger, or the
system look worse, than the truth**: `bea` overstated ~440×, `who_gho` reported as serving 69,590
ids when it serves none, "istat has banned us" when nothing was blocked, "the local route is dead"
when two sources had succeeded inside the very pass I called starved, "these six sources share a
cause" when four were not work at all.

Not one error ran the other way. I have not found an instance this session of understating a
problem or declaring something healthy that was broken.

**What follows practically:** when I report something alarming, the prior should be that it is
*real but smaller than stated*, and the first question is not "is the system broken?" but **"what
did the instrument actually measure?"** When I report something is fine, that claim has, so far,
held up better — but it has also been tested less, which is itself a reason for caution.

#### B6. What I would tell a successor in one paragraph

The data is mostly right and the update machinery mostly works; the recurring danger is me. Never
accept a number without its instrument, never accept a null result without a control that proves
the instrument can detect the thing, never believe a claim about the running system that was read
from a document, and never let a label survive the evidence that contradicts it. Run the adversarial
reviewer in parallel with everything, and when a guard refuses your write for the third time, the
guard is the witness and you are the suspect.

---

# PART IV — REFERENCE

## 10. The current state

All figures dated **2026-08-30** and produced by the named instrument. Where an earlier figure was
withdrawn, the withdrawal is stated rather than quietly corrected.

---

### 1. Coverage — is everything scheduled?

Instrument: `tools/audit_schedule_coverage.py` (the shipped tool; 1.3 s after this session's repair,
previously it never finished).

```
catalogued sources         322
resolvable (util.ts)       323
SERVED  (both)             322     13,486,342 series

SCHEDULED of served        270     13,148,499 series
NOT scheduled               52        337,843 series
   ARCHIVAL (retired)       52        337,843 series
   ACTIONABLE work           0              0 series

>>> 270 of 322 sources / 13,148,499 of 13,486,342 series scheduled
    (83.9% of sources, 97.5% of series)
```

**Read this carefully.** 83.9% of *sources* sounds poor and 97.5% of *series* sounds good; both are
true. The 52 unscheduled sources are **archival** — the publisher retired the dataset, so no fetcher
is possible. The audit attributes every one of them individually with a dated finding (IMF retired
IFS and re-keyed its contents; UNESCO re-coded and shrank; and so on).

**Actionable scheduling work: zero.** There is no backlog of "sources someone forgot to wire up".

Two sources are scheduled but serve nothing, both adjudicated:
* `gleif` — cleared licence (CC0) but blocked on **shape**: it is an entity registry with no
  series/date/value, so it cannot be catalogued in the series model at any grain. Serving it needs
  an entity-lookup surface, which is a product decision.
* `sec_edgar_xbrl` — **reserved for Ahmed**: one id names two products, and repairing the crossing
  changes public ids.

---

### 2. Freshness — does scheduled mean current?

**No, and the distinction matters.**

#### 2.1 Store files the fetcher has not rewritten

Instrument: `tools/audit_untouched_files.py --live`.

**26 of 229 sources hold files their fetcher has not written recently.** Worst:

| source | stale files | of total |
|---|---|---|
| `bea` | 591 | 592 (100%) |
| `dst` | 594 | 813 (73%) |
| `defillama` | 94 | 112 (84%) |
| `adb` | 32 | 54 (59%) |
| `cso` | 32 | 57 (56%) |
| `abs` | 444 | 1,222 (36%) |
| `worldbank_esg` | 13 | 93 (14%) |
| `ecb` | 17 | 540 (3%) |

**These have now been attributed, and almost none of them are defects.** Applying ledger R285's
behavioural test — a rotating source leaves *scattered* write blocks, a stuck one leaves *one
contiguous stale tail* — to all 24, using R2 `LastModified` per object:

| verdict | count | examples |
|---|---|---|
| **ROTATING** — scattered writes, healthy by design | **13** | `dst` 17 write-blocks, `stat_slovenia` 9, `cso` 8, `ssb` 8, `fed_board` 7 |
| one block, but wrote today | 3 | `hagstofa`, `worldbank_esg`, `snb` |
| already attributed in the ledger | 1 | `bea` — its fetcher writes only `bea.parquet`; the other 591 are the ingester-owned tree (R282) |
| looked stuck, **refuted by cadence** | 3 | `fdic` **quarterly**, last run `no_change`; `stats_nz` **quarterly**, last run ok +122,203 rows; `scb` **monthly**, last run `no_change` |
| **genuine fault** | **1** | `idb` |

**Zero are stuck.** The audit compares file age against a fixed **14-day** window with no reference
to how often the publisher actually releases — so a quarterly source sitting on month-old files is
flagged even though it is behaving correctly. **The instrument over-reports; the fleet is healthier
than that line suggests.**

The one real fault is `idb`: **10 of 40 sub-units refused identically on every run since
2026-08-06** (08-06, 08-19, 08-25), all in `social-indicators-of-latin-america-and-the-caribbean`,
all `merge guard … (< 97% of existing)`, with `obs` frozen at **15,066,444** across all three runs.

And that is not a separate problem — **it is the key-collision defect wearing a different mask.**
idb is separately measured at 39.83% conflicting pairs, so a clean upstream pull is *smaller* than
the collided store and the never-shrink guard refuses it. This is the same mechanism that has
refused the two UNCTAD stores three times and was right every time. `min_ratio` stays where it is;
the remedy is the re-key, which is reserved.

#### 2.2 A correction I nearly published

I measured the age of the served CSV for one catalogued id per source (321 of 322 returned an
object; ages min 0.2, median 22.8, max 59.0 days; 296 older than 7 days, 100 older than 30). I was
about to report "92% of sources serve CSVs older than a week" as a failure.

**The run records refute that reading.** `faostat` (last run 2026-08-30), `bis` (2026-08-30),
`maddison` (2026-08-29) and `transparency_ti` all report **no new rows**. Not re-deriving an
unchanged series is correct behaviour, not staleness. Also, one probe id per source cannot
distinguish "the source was not derived" from "that particular series did not change."

**CSV age alone is not evidence of staleness.** The meaningful metric is §2.4.

#### 2.3 A blind spot: 34 sources cannot be seen by any freshness instrument

This was found while trying to build a *cadence-aware* replacement for the file-age check, and it
is the most structurally important thing in this section.

Cross-tabulating every source that has ever run against membership in `source_state`:

| | has a `source_state` row | no row |
|---|---|---|
| **has ever recorded `ok` or `no_change`** | **249** | 0 |
| **has only ever been `partial` / `transient_fail`** | 0 | **34** |

**Zero exceptions across all 283 sources.** The ledger's R231 records that a `partial` run never
sets `last_success_utc`. It turns out it never *creates the row* either — so a source that is
always partial is absent from `source_state` entirely, and **any instrument keyed on that table
reports nothing at all for it.** 25 of the 34 have never recorded a single clean run in their
history.

They are not idle. `stat_estonia`, `norgesbank`, `sec_edgar` and `unctad_tradeservcatbypartner` all
ran on 2026-08-29. They do real work nightly and are marked `partial` because some sub-unit deferred
by budget.

And the set is not random — **it is disproportionately the largest sources**: `eurostat`, `oecd`,
`abs`, `eia`, `census`, `ilostat`, `noaa`, `owid`, `ssb`, `cso`, `idb`, `insee_melodi`,
`stat_slovenia`, and **all eleven UNCTAD giants**, including both collision sources. The mechanism
explains the correlation: the more sub-units a source has, the likelier one of them defers, so the
biggest sources are exactly the ones no freshness measure can see. This is ledger R359's disease in
its remaining form.

*A correction that belongs beside it.* My first attempt at the cadence audit reported `ecb` as
**45.2 days overdue** on a daily cadence. `ecb` ran on 2026-08-29. The 45 days is time since its
last **fully clean** run — a budget fact, not a staleness fact — and I nearly published it as the
latter.

#### 2.4 Updates that reach the store but not the user

Instrument: parsing the `note` field of each source's latest run in `data/_aqueduct/state.db`.

**73,125 changed series keys across 20 sources map to no catalogue id**, so those updates reach
nobody:

| class | sources | keys | leaders |
|---|---|---|---|
| `csv coherence unmet` (changed keys map to nothing) | 5 | **69,029** | eia 50,000; owid 12,192; sipri_polity 6,513; ecb 315; norgesbank 9 |
| `csv coverage note` (no catalog row) | 15 | 4,096 | defillama 2,730; ipea 257; who_sdg 235; rba 225 |

`eia`'s 50,000 is **exactly `CURSOR_CAP`**, so it is a cap, not a count — the true number is larger.
That source banked **+235,050,106 new rows** in its latest run and delivered none of them.

#### 2.5 The CSV retry queue

Instrument: `csv_retry_queue` in `state.db`.

**231,782 series queued.** Oldest entry 2026-08-18 (12 days). **Every row is still at
`attempts = 1`** — nothing has ever reached a second attempt.

| source | queued | why |
|---|---|---|
| `abs` | 100,000 | `UnitTimeout` — csv phase exceeded |
| `ilostat` | 50,000 | `UnitTimeout` |
| `usda` | 48,047 | derive budget spent — **the designed graceful path** |
| `imf_qgfs_direct` | 20,502 | `UnitTimeout` |
| `cso` | 7,256 | `UnitTimeout` |
| `scb` / `stat_estonia` / `ssb` / `bcrp` | 2,682 / 2,620 / 672 / 3 | mixed |

**183,735 of the 231,782 are hard `UnitTimeout` crashes**, not budget deferral. That distinction
matters: deferral is the system working as designed; a `UnitTimeout` takes the whole unit down.

---

### 3. The key-collision family — the largest open data defect

Full-fleet sweep, corrected instrument (`dupsweep.py` v3, per-file grouping validated against
hand-computed answers before use). **416 of 430 stores measured, 5,021,237,117 rows.**

| store | conflicting pairs | % of pairs | files | served? |
|---|---|---|---|---|
| `unctad_tradefoodproccatprocrca` | 312,791 | 79.16% | 1 | **SERVED** (19,087 ids) |
| `unctad_tradefoodproccatcatrca` | 281,400 | 77.69% | 1 | **SERVED** (17,617 ids) |
| `who_gho` | 797,557 | 49.80% | 1 | gated — 0 catalogue rows, live 451 |
| `idb` | 132,238 | 39.83% | 554 | **SERVED** (18,838 ids) |
| `ibge` | 110,807 | 30.30% | 12,125 | gated — 0 catalogue rows |
| `damodaran` | 721 | 2.92% | 1 | **SERVED** (24,687 ids) |
| `cow` | 2,081 | 0.55% | 7 | gated — 0 catalogue rows |
| `bea` | 49,856 | 0.074% | 592 | **SERVED** |
| `defillama` | 10,559 | 0.034% | 113 | **SERVED** |
| `ine_spain` | 17,375 | 0.014% | 98 | **SERVED** |
| `istat` | 16,271 | 0.003% | 2,442 | **SERVED** |

Plus **`eia`**, which the sweep cannot see because it keys on `series_id` — measured separately:
**142,073 of 268,502 catalogued ids (52.9%)** gather more than one publisher series, and within
files it holds **6,057,375 conflicting pairs (3.49%)**.

#### Retractions, stated plainly

* **`bea` was reported to Ahmed at 32.61% / 11.2 million conflicting values. That is withdrawn.**
  The true per-file figure is **49,856 (0.074%)** — overstated ~440×. The error was mine: the sweep
  pooled 592 separate per-table files into one namespace, and 99.9% of the "conflicts" were *across*
  files, which is normal because bea ids are `bea:<table>:<series>` and each resolves inside its own
  file. Settling test: 400 bea ids derived emit **zero** duplicate dates, against a damodaran
  positive control that emits three rows for one date.
* **`who_gho` was reported as "a served source" affecting 69,590 served ids. That is withdrawn.**
  It has zero catalogue rows and returns HTTP 451.
* **"308 of 308 stores scanned" is withdrawn.** The real population is 430 and the first sweep
  measured 299.

#### The sweep's own coverage, stated rather than implied

**29,322 of 32,189 parquet files measured — 91.1%.** The instrument records
`files_measured`, `files_skipped` and a reason per store, so the gap is named rather than
absorbed:

* **15 stores fully unmeasurable**, in three kinds — a *different id column* (`bls`, `eia`,
  `insee_bdm`, `ofr`, `worldbank_esg`, `worldbank_extra`, `fred`); *no value column* (`census`,
  `fhfa`, `treasury`); and *genuinely not series-shaped* (`cftc`, the three `edgar_*` trees,
  `gleif`).
* **7 stores partially measured**, and this qualifies one published figure: `defillama`'s
  **0.0335% was computed on 48 of 113 files**, so it is a lower bound over a subset, not a
  store-wide rate. Every *other* offender was measured on 100% of its files. The clean results
  for `fred` (1 of 165), `noaa` (417 of 834), `zillow` (206 of 412), `fed_board` (18 of 36),
  `fdic` (1 of 5) and `cepii_baci` (1 of 3) likewise cover only part of those stores. The
  exactly-half ratios suggest a paired layout where each data file has a differently-shaped
  sidecar.

Ledger **R281 records this exact failure before** — a dedup auditor that skipped stores keying on
different columns while "the run read as a pass". The difference now is that the skips are
recorded, not silent. The gap is real either way, which is why it is given as a coverage number.

`usda` joined the offender list at **0.0656%** (37,875 conflicting pairs, 63 files, fully
measured) as the sweep reached the larger stores, bringing the confirmed list to twelve.

#### The 15 unmeasurable stores, named

`bls`, `cftc`, `edgar_insider`, `edgar_pointers`, `eia`, `fhfa`, `gleif`, `insee_bdm`, `ofr`,
`treasury`, `wikidata`, `worldbank_esg`, `worldbank_extra` — they key on `series_id` or on their own
columns. `bls`, `eia` and `ofr` were measured by a targeted pass (`bls` 282,931 conflicting pairs
but only **9** catalogued ids; `ofr` clean).

#### Five giants still pending in the sweep

`statcan`, `eurostat`, `cbs_nl`, `oecd`, `ilostat`. **The census is therefore not final.**

---

### 4. Confirmed user-facing wrong values

Two are publisher-confirmed and are the strongest evidence that this class is not theoretical:

1. **Damodaran.** The publisher's own workbook (`ctrypremApr26.xlsx`, sheet "Regional breakdown")
   gives India `Adj. Default Spread = 0.02091491502586354` and `Corporate Tax Rate = 0.3`. **We
   serve 0.3 as the default spread** — a ~14× error on a cost-of-capital input. All **721 of 721**
   collided series serve the last store row, and in **zero** cases is that the modal value. The
   default-spread-by-rating ladder violates required monotonicity at **9 of 19** adjacent steps.
2. **UNCTAD.** The publisher's OData `$metadata` declares `Flow` in the Fact key (`01 Imports`,
   `02 Exports`). Algeria 1995 returns Imports 0.1124 and Exports 0.0157 — the exact two values our
   store holds under one id. Reconciliation is exact: **286,038 publisher two-flow cells against
   286,038 duplicated store pairs, zero residue.**

And one that is a usability failure as much as a data failure:

3. **eia** serves `ELEC.PLANT.GEN` as a **566,060,873-byte** object and `ELEC.PLANT.CONS_TOT_BTU` as
   **657,809,126 bytes**, each bundling 165,000–172,000 different publisher series under one
   "series" id.

---

### 5. What is running, and what just finished

**`statcan` derive — COMPLETE.** It finished during the writing of this document, after 7.8 days:

```
[8207/8207]  units: 466,341   put 252,425   skipped 213,916   errors 0   677,658s
duplicate (series_key, obs_date) rows collapsed: 0
REFUSED (too large, no usable splitter) — 5:
   37100234  63,821,210 rows · 37100277  75,731,377 · 98100023  24,347,136
   98100174 314,800,860 rows · 98100206  56,492,964
```

Note what the tool does with what it could not do: it **names the five tables it refused** rather
than reporting a clean total. That is the "no silent caps" rule working.

**This completes an order of Ahmed's, and cancels a queued task.** On 2026-08-18 he ordered
statcan's parquet deleted from R2 — 1,548.7 GB, 65% of the bucket, $23.23/month — to be re-derived
**compressed**. That is now done, and the arithmetic closes exactly: the ledger recorded 213,916
statcan CSV objects at the time of the order, this run PUT 252,425, and 213,916 + 252,425 =
**466,341**, which is both the derive's own unit count and the live object count measured
independently by listing R2. Sampled objects carry `ContentEncoding: gzip`.

I had been carrying "statcan parquet re-upload, +175 GB, ~$2.63/month" as queued work. **It is
cancelled.** The local store really is 8,207 files / 175.1 GB and R2 really holds zero — but that
absence is *his decision*, not a gap, and restoring it would undo the thing he asked for. Recorded
as ledger R520.

Still running:

* **the fleet duplicate sweep** — 419 of 430 stores, **5,411,046,427 rows measured**; four giants
  outstanding (`statcan`, `eurostat`, `cbs_nl`, `oecd`).
* **`cbs_nl` and `gus_dbw` crawlers** — long-running by design.

---

### 6. Things that were fixed this session and are verified

* the billing guard reconciles to a real Cloudflare invoice at **−0.11%**
* the `noaa` relaunch loop is confirmed dead: R2 LIST operations fell from 6,062–7,776/hour to
  57–368/hour, a **97.8%** cut (~$22.68/month)
* `tools/audit_schedule_coverage.py`: from never finishing to **1.3 s**
* three full-catalogue scans per orchestrator run removed; a per-key scan (6,872 times for one
  source) removed
* **`eurostat` released after 45 days** — a full pass over all 7,214 files found the store already
  clean; the guard had been blocking on a stale file count alone. Verified by calling the shipped
  guard, which printed "GUARD PASSES"
* `istat` rate limiter shipped (the publisher's stated 5 req/min)
* `worldbank` income-group aggregates 8 → 0 missing
* `ecb` retry loop now catches the exceptions it exists for

---

### 7. Diagnosed, not fixed

* **440 eurostat catalogued flows serve nothing**, and 540 store files disappeared while the source
  was frozen. Cause not established.
* **`oecd`: 60 of 131 flows have no `TIME_PERIOD` column at all** (13 of 13 sampled, all confirming).
  The data is real and **cross-sectional** — it is outside the series model, the same shape as
  `gleif`. It is currently mislabelled as a publisher "structural break". Whether to serve
  cross-sectional data is a product decision.
* The **gate has no tolerance for a bounded known-broken minority**: `bfs` 649/650, `hagstofa`
  1538/1568, `stat_slovenia` 95/97 are permanently red for a handful of tables.
* `norgesbank` and **ten other fetchers** compute their "changed" set from disk before any network
  call, violating the orchestrator's stated contract.

---

### 8. Reserved for Ahmed — not mine to decide

1. **Billing.** The account hit a monthly spend limit on the Fable model mid-session, killing the
   adversarial reviewer. Confirmed model-specific (Haiku and Opus answer), so reviews continued on
   Opus. Only Ahmed can lift it.
2. **Every fix in the key-collision family changes PUBLIC series ids** — `eia` would roughly double
   in id count. Precedent (R275/R276) makes that his call.
3. **`sec_edgar` / `sec_edgar_xbrl`** — repairing the crossed id changes public ids.
4. **`norgesbank` un-gating provenance.** Its R2 objects were deleted in the 2026-07-23 purge as
   "gated with 0 catalog series", and it was catalogued afterwards on 2026-08-06. Whether that
   un-gating was authorised should be confirmed before anything publishes 35,135 new objects.
5. **`oecd`'s 60 cross-sectional datasets** — serve them or formally exclude them.
6. **Gate policy** for permanently-red sources with a bounded broken minority.
7. Any **un-gating of a DISPUTED licence**, any deletion of **non-re-crawlable** data, and any email
   sent as him.
## 11. Glossary

Terms are grouped by where you meet them. Where a term has a precise operational meaning in this
codebase that differs from its ordinary sense, that is called out, because several of the ledger's
mistakes came from reading a term in its ordinary sense.

---

### The data model

**Series** — one time-varying quantity: "real GDP of France, annual". Identified by a `series_id`.

**Observation** — one `(series, date, value)` triple. The library's totals are quoted in
observations, which is why they run into the billions while series run into the millions.

**`series_id`** — the public identifier, of the form `<source>:<rest>`, e.g.
`eia:AEO.2014.BESTTECH`. **The colon prefix is load-bearing**: because `series_id` is the primary
key, `series_id >= 'eia:' AND series_id < 'eia;'` is exactly the set of one source's ids and rides
the index. `;` is the byte after `:`, so nothing can fall between them. This one trick is what makes
per-source queries cheap; without it they are full table scans.

**`series_key`** — the identifier *inside a store file*. Usually the `series_id` minus the source
prefix, but not always, and three stores (`bls`, `eia`, `ofr`) use a column called `series_id`
instead. Confusing the two is a recurring source of error.

**Grain** — how finely a key divides the data. If the publisher varies a dimension (frequency, sex,
Imports/Exports, worksheet) and our key does **not** include it, several genuinely different series
collapse onto one id. This is the **key-collision** class and it is the largest open defect family.

**Store** — the parquet files under `data/clean_full/<source>/` holding the actual numbers.
Canonically three columns: `series_key`, `obs_date`, `value`.

**Vintage** — a publisher's release of a dataset. Two vintages merged under one key look like
duplicates but are a different defect from a dropped dimension, and they need a different fix.

---

### The serving side

**D1** — Cloudflare's hosted SQLite. Holds the *catalogue* (which series exist, their titles,
licences), not the data itself. Billed by **rows read**, which is why query shape matters more than
query count.

**R2** — Cloudflare's object storage. Holds the actual per-series CSV files and the parquet stores.
Billed by storage plus **Class A** (writes/lists) and **Class B** (reads) operations.

**Worker** — the Cloudflare Workers program that answers every request. Deployed **manually** with
`npx wrangler deploy`; nothing in CI deploys it. This matters: *committing is not deploying*, and
the ledger records reporting things "live" that had never been deployed.

**Pages** — the static site hosting for the browsable pages.

**`series_fts`** — the full-text search index, an SQLite FTS5 virtual table declared as
`fts5(series_id UNINDEXED, title, geography)`. **`UNINDEXED` means what it says**: a
`WHERE series_id = ?` predicate on this table has no index and scans everything — measured at
23,843,482 rows per statement. A batch of such statements is priced per *statement*, not per id, so
the fix is always to raise predicate arity, never to add statements.

**LIKE fallback** — when FTS returns nothing usable, search falls back to a `LIKE` scan over
millions of rows, measured at 5–8 seconds. Slow, and the reason orphaned index rows matter.

**`source_counts`** — a one-row-per-source cache holding the `total` the API reports. It has
**exactly one writer** (`core/sync_catalog_d1.py`), so every direct D1 write silently invalidates
it. A source with no cache row falls back to a live `COUNT(*)` on every page view — the exact shape
that produced a $82 day.

**Denylist** (`api/worker/src/denylist.ts`) — the list of sources that must not be served.
Generated from licence data. A denylisted source returns **HTTP 451** ("unavailable for legal
reasons"). **451 and 401 are different answers**: 451 means the gate refused you, 401 means you
passed the gate and need to log in. A probe that cannot tell them apart has measured nothing.

**Reservable** — a licence flag meaning the data may be redistributed by us. It lives in *two*
stores (local `catalog.db` and D1) and they have been observed to disagree.

**Gated** — not downloadable, whether for licence reasons (denylist) or authentication.

---

### The update side

**Registry** (`updater/registry.yaml`) — the list of sources and how each is refreshed. Fields
include `source_id`, `live`, `strategy`, `cadence`, `data_cadence`, `refresh_cost`, `run_location`.

**`EXPECTED_SOURCE_COUNT`** — a constant that must be bumped in the *same commit* as any registry
entry added or removed. A mismatch refuses **all** runs, not just the changed one. CI enforces it.

**Fetcher** (`updater/strategies/fetchers/X.py`) — the code that runs on the schedule.

**Ingester** (`jobs/ingest_X.py`) — the code that did the original bulk load. **Most sources have
both, and a fix in one is not shipped in the other.** This is a standing trap; the ledger records
fixing a parser in the ingester and verifying it with a live call, which proved the half that does
not run nightly.

**Orchestrator** (`updater/orchestrate.py`) — decides which sources run, in what order, within what
budgets. Sources run **serially**.

**Derive** — turning store parquet into the per-series CSVs users download, and PUTting them to R2.
A source can be perfectly up to date in the store and still serve stale CSVs if the derive did not
run for it.

**Changed keys / `series_cursors`** — what a fetcher reports as having moved, so the derive knows
what to re-derive. **The name is not a guarantee**: at least eleven fetchers compute this from what
is on disk *before making any network call*, so their "changed" set is really "everything".

**`CURSOR_CAP`** — 50,000. A changed-set that hits exactly this number is *capped*, not measured. A
round number in a report is a cap until proven otherwise.

**Never-shrink guard** — `merge_and_write` refuses to publish a result below `min_ratio` (default
0.97) of what is already there. It exists to stop a truncated upstream pull silently overwriting
good data. **It has been right every time it has fired**, including three times on a write I was
preparing to force through.

**`partial`** — a run status meaning "some of it worked". Critically, **a partial run never sets
`last_success_utc`**, so a source can look permanently unsuccessful while doing real work every
night.

**State store** (`data/_aqueduct/state.db`) — run bookkeeping: `source_state`, `unit_state`,
`series_cursor`, `runs`, `leases`, `csv_retry_queue`. Pushed to and pulled from R2 with
compare-and-swap. **Compare-and-swap proves nobody moved the remote; it proves nothing about the
local copy still being what you pulled.**

**`csv_retry_queue`** — series whose CSV derive failed or was deferred, to be retried. It was
**write-only for a period** — the code that enqueued had no reader — so every parked id was lost.

**Health gate** (`updater/health.py`) — classifies each source: RED-SLA, RED-DATA, RED-UNRUN,
ATTENTION, PENDING, OK, ROTATING. It builds its world from the registry, which means **a store with
no registry entry is invisible to every check.**

---

### The working method

**Ledger** (`.claude/MISTAKES.md`) — the append-only record of mistakes. Entries are `R<number>`.

**Digest line** — the one-line form at the top of the ledger. Mandatory, same commit, mechanically
enforced.

**NUMBERS.md** — every reported figure with the instrument that produced it.

**Adversarial review** — a separate agent instructed to find the flaw, not approve. REDIRECT or FAIL
is a success for the reviewer.

**Positive / negative control** — the discipline that makes a null result mean something. A probe
that reports "absent" must be run against something known **present**; a guard must be shown a case
it must **block** and one it must **let through**. A failed control does not weaken a result, it
means there is **no** result.

**Reserved decision** — something that is Ahmed's call: deleting non-re-crawlable data, un-gating a
disputed licence, auth/billing, sending email as him, and any change to public series ids.

**Desktop-first** — decide against the free local catalogue, verify against the billed remote one.

**Instrument** — the specific command, query or tool that produced a number. A figure without one is
not a measurement.

---

### Units and pricing, stated once

* **Rows read** — D1's billing unit. A query that cannot use an index reads the whole table.
* **Class A operations** — R2 writes and lists. **Class B** — R2 reads.
* **GB-month** — 730 GB-hours. A GB *mean* is not a GB-month; conflating them misprices storage.
* **Included allowance** — the monthly quota that resets on the **subscription renewal date**
  (the 9th), not the 1st of the month.
* **Texas data-processing tax** — 80% of the subtotal is taxed at 8.25%, a **6.6% uplift** on the
  pre-tax figure. Every raw Cloudflare number is pre-tax.
