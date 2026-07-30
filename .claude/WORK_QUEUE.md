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

## The remaining 96 are NOT 96 fetchers — establish which KIND of work first (R170)

`python tools/audit_upstream_liveness.py` — re-measured 2026-07-30 08:07 UTC,
**96 sources / 674,399 series** still unscheduled:

| provider | sources | series | newest DBnomics index | route |
|---|---|---|---|---|
| IMF | 28 | 398,777 | 2025-08-31 | **NOT recoverable by the `_direct` route** — see below |
| UNCTAD | 38 | 127,413 | **2023-06-30** | mirror dead — re-derivation + id crosswalk |
| FAO | 18 | 87,579 | **2024-05-09** | element crosswalk reaches 79%; rest restructured — Ahmed's call |
| UNESCO | 4 | 57,530 | **2022-04-04** | mirror dead; UIS API exposes too few codes |
| 8 others | 8 | 3,100 | mostly not DBnomics | hf_equities 1,391 · comtrade 713 · worldbank 692 · bea 240 · usda 25 · census 22 · noaa 10 · eia 7 |

**The IMF row needs care — I had it wrong once.** All 28 are LEGACY ids (imf_fsi 73,288,
imf_gfse 48,750, imf_gfsmab 43,179, imf_gfsssuc 36,901, imf_gfscofog 34,731, imf_gfsibs 29,390,
imf_cpi 28,420, …); NOT ONE `_direct` source is among them. The `_imf_direct` family is
ADDITIONAL coverage under NEW ids, not a rescue of these — `jobs/ingest_imf_direct.py` records
the crosswalk as uneven (FDI 95.3%, APDREO 100%, WHDREO 56%, FAS/WORLD/COFER ~0%), which is
exactly why new ids were minted rather than the old ones overwritten. So recovering these 398,777
series under their PUBLISHED ids needs a per-flow crosswalk proven flow by flow, the same bar the
FAO prover applies. Do not treat this row as "one fetcher away" (R170).

88 of the 96 sit in those four families. A source whose mirror is frozen cannot be fixed by
writing an updater against that mirror — it would run nightly, succeed, and transfer nothing.
Fixing it means re-deriving from the real publisher AND reproducing our published ids exactly,
and that step fails SILENTLY: a wrong key template mints a parallel id space beside the live
series and reports success.

DONE since the last table: WHO (3 / 34,788), BOC (1 / 12,862), SNB (1 / 762).

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
- [ ] **imf_fsi** (73,288) — NOT simply blocked; re-measured 2026-07-30 and the framing was
      stale. The LEGACY host is genuinely dead (`dataservices.imf.org` -> ConnectionError), but
      **api.imf.org is alive** (dataflow list 200, 444,501 B, 222 flows) and the FSI data is
      there, SPLIT ACROSS THREE RENAMED FLOWS: `FSIC` (Core and Additional Indicators),
      `FSIBSIS` (Balance Sheet, Income Statement) and `FSICDM` (Concentration and Distribution
      Measures). `/data/FSI` 404s because the old flow name is gone, not the data.
      Those are EXACTLY the three wrappers built this morning — imf_fsic_direct,
      imf_fsibsis_direct, imf_fsicdm_direct — which have never run yet (0 catalog rows).
      NEXT: check whether the legacy key shape (`imf_fsi:FSI:A.5Y.FSANL_PT`) is reproducible
      from those flows, the same way tools/prove_faostat_repair.py scores id reproduction, and
      REFUSE a partial template — a wrong one mints a parallel id space and reports success.
      73,288 series makes this the largest single recoverable source outside the frozen families.
- [ ] Small non-DBnomics remaining: usda 25, census 22, noaa 10, bea 240 (BEA's own API — its
      DBnomics namesake is a NAME COLLISION, R171). comtrade is BLOCKED (see above); snb, fhfa,
      maddison and boc are DONE.
- [ ] `worldbank_extra` — NOT a coverage gap; it is UNSERVED data. Re-measured 2026-07-30:
      **0 catalogued series, absent from util.ts**, 9 parquets on disk. So it never appears in
      the served-but-unscheduled count and no fetcher would help. The underlying defect is
      confirmed though — schema is [indicator, country, obs_date, value] with `country` BLANK
      (verified on doing_biz.parquet), so (indicator, country, obs_date) is not unique and there
      is no series_key at all. Re-key into a WDI-style layout FIRST, then catalogue, then serve,
      then schedule. Belongs with tools/audit_unserved.py's bucket, not this queue's numerator.
- [ ] `ksh_stadat` — 3 unparsed table shapes (multi-label-column + single time column).
- [ ] `owid` — 24 missing + 32 stale CSVs (parked while gated).
- [ ] `stats_nz` — 10 of 12 datasets unreachable upstream (pre-existing, not our regression).
- [ ] Read each 06:00 UTC run and fix what it reddens. **EXPECT THE NEXT RUN'S HEALTH GATE TO
      FAIL, AND DO NOT MISREAD IT.** Snapshot 2026-07-30 06:27 UTC: `RED-UNRUN 36`. RED-UNRUN
      means "adapter built, no state at all" — and 12 of those are sources promoted TODAY that
      have simply never executed in CI yet (bis, boc, fed_board, fhfa, ilostat, maddison, snb,
      who_hwf/rs/sdg, zillow) plus the 9 imf_*_direct registered this morning. The gate judges
      live-tier sources, so it will go red until each has run ONCE. That is the gate working, not
      the fetchers failing. Judge each source by its own line in the run log — `ok`, `no_change`,
      `partial`, or a named error — never by the gate's exit code alone. Other pre-existing reds:
      `ofr` RED-DATA (newest obs 2026-07-27, 3d, daily) and `bls` ATTENTION at 59d, which is the
      Akamai edge block (R173).

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
