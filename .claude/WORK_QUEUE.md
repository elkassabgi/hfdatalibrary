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


## P0 — THE DAILY UPDATER WAS A TOTAL OUTAGE (found + fixed 2026-07-30)

- [x] **abs OOMed the runner on source #1, so NOTHING ever ran.** Run 30523814247: one
      orchestrator banner (abs/_all), zero completions, memory 1,211MB -> 15,700MB at
      299 MB/min for 48.5 min, runner destroyed with 288 MB free. A recurrence —
      orchestrate.py:451 records batch 30312217406 doing the same (49 min, 15,654 MB),
      after which the ">>>" banner was added so a future OOM could NAME its culprit. That
      is what identified abs; the memory fix was never done. Now done: CURSOR_CAP on the
      run-global fold, a stream fold replacing the per-flow dict, Arrow pool release per
      flow, and an AQUEDUCT_ABS_BUDGET_MIN self-budget (the orchestrator's run budget is
      only checked BETWEEN sources, so a source that never RETURNS is unbounded).
      MEASURED: abs holds 976,632,535 rows / 376,332,763 distinct series -> ~94 GB of
      cursors. PROVEN: 800,000 synthetic series -> exactly 50,000 cursors, RSS +49 MB.

- [x] **Swept the class, not the instance** (tools/audit_cursor_blowup.py, new). Two more
      genuine per-series folds found and bounded: **vdem** (1,465,759 series) and **owid**
      (1,048,968) — not runner-killers at ~0.3 GB each, but each cursor is a state.db row
      and a _catalog_ids_for query, both linear. Added `merge_cursor_map` to _common for
      in-memory folds (merge_cursors only bounded parquet-derived sets).
      The audit's first cut FALSE-POSITIVED on statcan (56.8 BILLION rows), istat and ecb:
      all three key cursors per PID/flow/file, so they are bounded by the FILE count. The
      trigger is now the FOLD SHAPE, not the store size.

- [ ] **statcan would have been next.** It sorts after abs and holds 56,845,456,057 rows;
      it never ran only because abs died first. Its per-file fold means cursors are fine,
      but nothing has ever exercised it end-to-end — watch it on the first clean run.

- [ ] **abs cataloguing gap (NOT a memory issue).** The store holds 376,332,763 distinct
      series; the catalog credits abs with **18**. Whatever abs serves, it is not what it
      holds. Decide whether to catalogue at flow grain (like the 9 PxWeb sources) or leave
      it — but do not leave the discrepancy undocumented.

### Do not chase these until ONE clean full run has happened

The outage means several queue items may be describing symptoms of the outage, not real
defects. `_direct` wrappers with "0 catalog rows" have never executed once — not because
they are unreachable, but because nothing downstream of abs ever ran. Re-measure before
building anything:

  - #18 nine imf_*_direct (0 catalog rows each) — including the three FSI wrappers the
    imf_fsi unlock depends on. "Unreachable" and "never attempted" look identical here.
  - #24 census (22) / usda (25) / noaa (10) — all small, all downstream of abs.
  - Any source promoted to live on 2026-07-29/30: NONE has executed in CI even once.

The gate is a full run that reaches past abs. statcan (56,845,456,057 rows) is the first
untested giant behind it — watch its memory specifically, since the cursor audit clears its
per-PID fold but nothing has ever exercised the rest of that fetcher end-to-end.

### The OOM class beyond cursors: whole-file reads on giant stores

abs was a cursor blowup; statcan was about to be a READ blowup, found by looking ahead
rather than by another dead runner. Fixed 2026-07-30 (commit f66d299):
  - `_disk_vector_map` read every column of every changed cube and `.to_pydict()`-ed it.
    statcan's largest cube 98100435.parquet is 962,150,400 rows -> ~67 GB of Arrow at
    ~70 B/row before to_pydict() even starts. Now 4 columns, folded per record batch.
  - `merge._max_obs_date(blob.read_table(path))` -> projected to `columns=["obs_date"]`
    (~3.8 GB worst case instead of ~67 GB).

- [ ] **statcan vector-map cardinality is UNMEASURED.** The map holds one entry per
      distinct vector per cube. Far below the row count, but unknown for the census
      giants; if it runs to tens of millions it needs its own cap. Measure distinct
      series_key on 98100435 before assuming it is fine.

- [ ] **Sweep the whole-file-read class properly.** tools/audit_cursor_blowup.py covers
      cursor folds only. The analogous audit is: which fetchers call `blob.read_table(p)`
      with NO `columns=` against a store whose largest file is huge? statcan was found by
      hand; there may be others. oecd (6,979,047,823 rows / 1,413 files), cbs_nl
      (4,581,749,467 / 3,844) and eurostat (2,430,929,754 / 7,754) are the next largest
      stores and none has been checked.

## Budget is now the binding constraint, and three sources waste most of it (2026-07-31)

The first SUCCESSFUL cloud run (30577997654, exit 0, peak 3,575 MB) processed 15 sources and
left 69 unattempted when the 240-min budget ran out. Three sources took 226 of those 240 min.
Tested the "it is only catching up after weeks of no updates" hypothesis against the `runs`
table — it does NOT hold for these:

- [ ] **hagstofa — 55 min per run, no new data, for over two weeks.** Runs on 07-14, 07-25
      (x2), 07-30 all report the SAME note: `26/1906 sub-unit(s) returned 200 but parsed 0
      rows from a non-trivial body (schema/structural break)`. Durations 4,347s / 3,843s /
      3,088s / 3,275s — flat, not decaying, so it is not backlog.
      CORRECTIONS TO MY OWN FIRST READ: the store is NOT empty (7,207,289 rows / 1,775,507
      series); `obs=0` is the FETCHER reporting zero, which is a different claim. And of the
      82,655 "far-future" rows, 81,535 (2028-2100) are LEGITIMATE — thjodhagsspa is Iceland's
      national economic forecast. Only 1,120 rows beyond 2100 are genuinely corrupt, all from
      one climate table (UMH11140.px, dates like 3005-12-31). sane_since already guards that
      max, so it is NOT the cause of the runtime. The 26 structural sub-units are.

- [ ] **insee_bdm — 11 min on 07-16 became 105 min on 07-30, with 201/201 sub-units
      transient-failing.** That is 105 minutes of retries and timeouts, not data. Something
      changed upstream or in credentials; diagnose before it burns another budget.

- [x] **ecb / adb "re-fetch far more than they keep" — WITHDRAWN, this was my misreading.**
      The observation was right (ecb fetched 5,849,110 rows for a net +19,255) but the
      conclusion was wrong. ecb's startPeriod is the boundary period INCLUSIVE — a minimal
      one-period window, not a wide re-fetch. It returns millions of rows because ECB has
      millions of SERIES, and one period across all of them is millions of rows however few
      actually moved. Its cadence is genuinely daily and its store is 218M obs, so ~66 min is
      the inherent cost of a date-tail at that width, not waste. Narrowing the window would
      LOSE the in-place revisions the inclusive boundary exists to capture.
      Same shape as hagstofa: the per-run cost is inherent; what was fixable was the
      FREQUENCY, and that is what the is_due change addressed.

Recovering hagstofa's 55 min and insee_bdm's 105 min alone would return ~66% of the daily
budget, which is worth more than any per-source tuning.

## Routing sources to the workstation one-at-a-time IS whack-a-mole (2026-07-31)

Thirteen sources now carry run_location: local, and every one was added AFTER it destroyed a
runner. Four distinct causes, none predicted by row count:

    abs            unbounded cursor fold (376M series -> ~94 GB)
    bis, bls       dedup hash overflow past Arrow's 2 GiB string ceiling
    cepii_gravity  the COMBINED existing+new merge peak, which my harness never measured
    ons_uk         349-character series keys - 8.9 GB of key text over just 25.4M rows

They share ONE cause: merge_and_write holds whole tables in memory. #29's chunked merge would
retire the entire class, and most of these 13 could return to the cloud. Until then each new
source that gets far enough down the alphabet to run is a fresh casualty — the cron run only
reached ons_uk because the is_due fix stopped it repeating yesterday's work.

- [ ] **ons_uk key bloat (deferred, needs a decision).** Its keys carry the code AND the
      label — `sex=female:Sex=Female` — averaging 349 chars, max 525. Codes alone would cut
      the dominant memory term ~3.5x and might make it cloud-capable. BUT the key IS the
      published series id, so changing it breaks every existing download URL and requires
      re-deriving the source's CSVs. Not a unilateral change; costed here so the option is
      on the table rather than rediscovered.

WHAT THE CRON RUN PROVED, positively: with the is_due fix it reached insee_melodi, ipea,
maddison, nyfed and ofr — sources it had NEVER processed, because it was no longer repeating
work already done. It died further down the alphabet than any previous run.
