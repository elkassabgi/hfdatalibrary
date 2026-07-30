# WORK QUEUE — econ updater: make every served source auto-update

Contract: while ANY `- [ ]` remains, keep working. Do not end a turn to ask "want me to
continue?" — the queue is the answer. Append newly discovered work.

**Do not hand-derive the progress number.** Run `python tools/audit_schedule_coverage.py` in
`E:\research\econfindatalibrary`. It parses every input from the file that owns it
(`SUPPORTED_SOURCES` from `util.ts`, `live: true` from `registry.yaml`, the heavy matrix from
its `ALL='[...]'` literal, sec_edgar from its own workflow). Hand-derivation drifted three
different ways in a single day — R166.

**2026-07-30: 106 of 202 sources / 4,375,807 of 5,050,206 series scheduled** (52.5% / 86.6%).
Registry: 139 sources, 113 live.

NOTE the tool now requires an ADAPTER before counting a source as scheduled. updater-heavy ran
green at 05:50 with all four matrix jobs reporting "0 unit(s) processed" — two of them printed
`PENDING <src> — no adapter built`. Matrix membership is not a schedule.

---

## The remaining 98 are NOT 98 fetchers — establish which KIND of work first (R170)

`python tools/audit_upstream_liveness.py` buckets them by whether the upstream still moves:

| provider | sources | series | newest DBnomics index | what it means |
|---|---|---|---|---|
| IMF | 28 | 398,777 | 2025-08-31 | use the `_imf_direct` route, not DBnomics |
| UNCTAD | 38 | 127,413 | **2023-06-30** | mirror dead — re-derivation, not a fetcher |
| FAO | 18 | 87,579 | **2024-05-09** | mirror dead |
| UNESCO | 4 | 57,530 | **2022-04-04** | mirror dead; UIS API lacks their codes |
| WHO | 3 | 34,788 | 2026-07-24 | **DONE 2026-07-30** |
| BOC | 1 | 12,862 | 2025-02-15 | judgement call |
| 8 others | 8 | 3,953 | not DBnomics | ordinary fetcher work |

A source whose mirror is frozen cannot be fixed by writing an updater against that mirror — it
would run nightly, succeed, and transfer nothing. Fixing it means re-deriving from the real
publisher AND reproducing our published ids exactly, and that step fails SILENTLY: a wrong key
template mints a parallel id space beside the live series and reports success.

## Open

- [ ] **cepii_gravity** — 1,143,250 series, licence-cleared (Etalab 2.0, 100% dated), already
      scheduled, and reachable by nobody: absent from `util.ts`, 0 rows in live D1. Bulk derive
      running (`tools/derive_csv_bulk.py`; was 991,707 MISSING of 1,143,250, ORPHANED 0). Then
      re-verify to MISSING 0 → D1 export → `SUPPORTED_SOURCES` → deploy → confirm a real body
      with the key. **Do not do the last three before MISSING is 0** — that is how the IEP
      sources went live with zero objects behind them.
- [ ] **nine `imf_*_direct`** — live with ZERO catalog rows: they refresh sibling ids nobody can
      reach, while the served originals (`imf_gfsssuc` 36,901, `imf_gfscofog` 34,731, `imf_gfse`
      48,750, …) stay frozen. Either catalogue+serve the new ids, or point the fetchers at the
      originals. Refreshing data nobody can download is motion without delivery.
- [ ] **18 `fao_*`** (~87,000) — needs UNION matching across several FAOSTAT datasets. Measured:
      `fao_ae` vs AE reproduces **0.0%** of our ids (AE now exposes Indicator/Cost-Category/
      Institution while our ids are Element.Area.Item); `fao_gt` vs GT **27.2%** (FAOSTAT split
      the old emissions data across GT/GCE/GLE/GN/GF/GI/GV/GPP). The prover refused both,
      correctly. **Do not lower its acceptance threshold to make them pass.**
- [ ] **38 `unctad_*`** (127,413) — the biggest single block. DBnomics-frozen; ids are DBnomics
      slugs (`UNCTAD_RFIA:A.number-of-exporters.<slug>`), so UNCTADstat re-derivation needs a
      crosswalk that reproduces them exactly.
- [ ] **4 `unesco_*`** (clte/cltt/film/inno, 57,530) — deliberately deferred: the UIS API exposes
      5.1% / 0% / 1.3% / 0% of their indicator codes. Needs a different route.
- [ ] **worldbank** (692) — the last pre-migration source: legacy `data/clean/` tree, one parquet
      per series, identity in the FILENAME, no key column. Migrate to `clean_full` first.
- [ ] **imf_fsi** (73,288) — blocked, legacy host 403s.
- [ ] Small non-DBnomics remaining: usda 25, census 22, noaa 10, bea 240 (BEA's own API — its
      DBnomics namesake is a NAME COLLISION, R171). comtrade is BLOCKED (see above); snb, fhfa,
      maddison and boc are DONE.
- [ ] `worldbank_extra` — BLOCKED on a data repair: no series_key column and `country` is BLANK
      for all ~120k GEM + ~134k aggregate rows, so (indicator, country, obs_date) is not unique.
      Needs a re-key into a WDI-style layout BEFORE any fetcher can merge safely. Do not paper over.
- [ ] `ksh_stadat` — 3 unparsed table shapes (multi-label-column + single time column).
- [ ] `owid` — 24 missing + 32 stale CSVs (parked while gated).
- [ ] `stats_nz` — 10 of 12 datasets unreachable upstream (pre-existing, not our regression).
- [ ] Read each 06:00 UTC run and fix what it reddens.

## Done 2026-07-30

- [x] **The nightly refresh was DEAD and nothing said so.** `EXPECTED_SOURCE_COUNT` 125 against
      134 sources makes `registry.validate()` fail and `orchestrate.py` raise SystemExit, so
      every run aborted before touching a single source. Fixed; `tools/preflight_registry.py` +
      `.github/workflows/preflight.yml` now catch it on push (it has already caught a second
      break). A dry run now enumerates **51 due units**, including 13 that had never run.
- [x] **fed_board** live — content-hash gate. The registry PRESCRIBED an ETag/Last-Modified gate
      that cannot work: Output.aspx mints a new Last-Modified every request (03:17:40 / 03:18:01
      / 03:18:21 on three HEADs 20s apart), no ETag, no Content-Length, no Range support.
- [x] **bis** fixed — same defect class, found by sweeping rather than assuming: data.bis.org
      serves the same bytes from replicas with different mtimes, so ETag/Last-Modified FLAP
      (Last-Modified moved BACKWARDS five hours). Now gates on Content-Length + a 30-day re-pull.
- [x] **zillow** live — `cache_raw=False` is load-bearing: the ingest returns the cached raw copy
      when one exists and would have re-parsed the same bytes forever, reporting success.
- [x] **ilostat** live — gates on ILO's own TOC `last.update` (1,956/1,956 populated), not the
      n.records growth the registry proposed (blind to a revision that keeps the row count). The
      forced fresh TOC listed 9 indicators the cached copy hid.
- [x] **who_hwf / who_rs / who_sdg** live — the one DBnomics family still being indexed. Full id
      comparison, not a sample: 4,421 upstream codes vs 4,421 published, 4,421 exact, 0 either way.
- [x] **maddison** live — parse EXTRACTED from `main()` into `parse_xlsx` so ingest and fetcher
      share ONE parser; it reproduces the store exactly (36,905 obs / 338 series).
- [x] **boc** live — 12,862 series / 2.73M obs with NO ingest script and NO registry entry.
      Valet's series names ARE our keys: 12,862 of 12,862 exact. Live run +8,843 rows.
- [x] **snb** live — 762 series across 12 cubes, gated on each cube's own PublishingDate.
      Keys 762/762 and rows 303,358/303,358 verified against the store. +1,091 rows.
- [x] **fhfa** live — 18 parquets / 3,227,580 obs; its ingest was already safe to re-run.
- [x] **series_cursors** added to every bulk fetcher (R174) and then BOUNDED across files
      (R175/R176) — without cursors the orchestrator withholds the vintage and the source
      republishes forever with stale CSVs; unbounded, ilostat's 30.8M series would have OOM'd.
- [x] **Dependency preflight** — `lxml` was undeclared, so freshly-promoted fed_board would have
      reported "no adapter built" and silently never run (third repeat of that class).
      `tools/audit_updater_deps.py` now walks the import graph on every push.
- [x] Tools: `audit_schedule_coverage.py`, `audit_upstream_liveness.py`,
      `audit_vintage_stability.py`, `audit_updater_deps.py`, `derive_csv_bulk.py`,
      `preflight_registry.py`.

## Build rules (hard-won — see `.claude/MISTAKES.md`)

Reuse each source's own parser so `series_key` matches disk byte-for-byte (R33) · all store I/O
via `blob.*` (R36) · **MEASURE a validator before gating on it** — two HEADs seconds apart
(R164/R165) · a publisher's own stamp (ILOSTAT `last.update`, DBnomics `indexed_at`) beats any
HTTP header · read how an ingest DISCOVERS work: a frozen list or a skip-if-exists is a
staleness bomb (R159) · every early exit must answer "does this status let the gate advance?" —
a budget is a deferral, never a completion (R172) · bump `EXPECTED_SOURCE_COUNT` with a dated
changelog line (R168) · prove a gate FAILS, not just that it passes (R142) · never publish an id
space you have not reproduced exactly, and a matching provider NAME is not provenance (R171).
