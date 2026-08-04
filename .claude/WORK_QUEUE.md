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

---

# 2026-08-03 → 08-04 UPDATE (Opus 5)

**Run the tool, do not trust the number above.** `python tools/audit_schedule_coverage.py`.
Measured 2026-08-04: **120 of 217 sources / 9,496,558 of 10,863,548 series scheduled**
(55.3% / 87.4%). The 2026-07-30 headline near the top of this file (106 of 202 / 4,375,807 of
5,050,206) is superseded — both the numerator and the DENOMINATOR moved, which is exactly why
that warning is there.

Cloud: 106 live — 64 clean inside their SLA, 36 ingesting but reporting `partial`, 3 past SLA,
3 stalled. Local: 6 live + 3 crawlers backfilling.

## The 21 sources that cannot be fully fetched — ROOT-CAUSED, not guessed

14-target investigation, 29 agents, every finding adversarially challenged (9 upheld, 4 refuted
and corrected). **Not one source fails on a credential or a dead endpoint.** The recurring guess
— "the API key expired" — was wrong for all 21.

| root cause | sources | meaning |
|---|---|---|
| budget_deferral | abs, ecb, ssb, ilostat, insee_melodi, ipea | **NOT BROKEN** — ran out of their 35-min slice |
| code_bug | bcrp, cso, hagstofa, stat_estonia, stat_slovenia, wid | ours |
| rate_limited | insee_bdm | INSEE refusing us; its "201/201" is a circuit breaker, not 201 attempts |
| gated_by_design | eurostat | deliberately refusing to update on corrupt keys |

**abs "805/1222 transient-failed", ecb "290/540", ssb "135/1515" are ZERO real failures.** Every
named unit reads "deferred (budget 35 min)". Stale strings predating the R303 fix; they overwrite
on the next successful tick. Do not re-fix that mislabel — it is already fixed and guarded.

## Fixed this session

- [x] **ons_uk — FIRST SUCCESS IN ITS HISTORY** (`last_success_utc` had been NULL forever). Four
      defects: time grammars that killed 10 of 12 datasets (0 rows from 0.5–22 MB bodies); wrong
      key grain (225,368 keys → 8,668 for the same rows); an R190 fixed-prefix truncation; and
      **287 of its 337 "datasets" were never time series** (ONS labels them `cantabular_*` —
      Census cross-tabulations with no time dimension, downloaded and discarded every run, and
      since `empty` holds a source at `partial` they alone guaranteed it could never succeed).
      Also found the fetcher had been UNDOING the approved 2026-07-29 re-key.
      d142c978, 0404baa2, 9f3af5a3, a5bdf4e1.
- [x] **cso — 222 unroutable matrices recovered**, proven live: cached map 13,364 → forced
      rebuild 13,586, ADDS 222 REMOVES 0, 5.8s. The subject cache could never be refreshed
      (`_matrix_subject_map` wrote it only when ABSENT; `build_catalog()` short-circuited on the
      file existing), so a stale cache froze permanently while those matrices ate ~45% of every
      run's 60-table budget without publishing. 9a228a1f.
- [x] **wid — the next run would have RAISED.** Its 12 header-only entities (47 bytes of CSV
      header) were never stamped, so once every real country was stamped they became the entire
      work list and the empty-window guard would read 12-of-12-empty as "the source went dark".
      ce08652c.
- [x] **bcrp — a THIRD string-vs-date site** my own R310 sweep stepped over, because it fixed the
      lines either side and this one raises `TypeError` where those raise `AttributeError`. Root
      enabler was a type hint promising `dict[str, dt.date]` while returning strings. ac3fcfa1.
- [x] **pxweb `resolve_time_dim` — stopped NEW date fabrication for four sources at once.** Step
      3 fell back to a name match with no value check; step 2 already refused those axes. 02944c86.
- [x] **eurostat re-key guard could be disarmed by a PARTIAL re-key** — it sampled
      `list_parquets()[:5]`, the first five of a SORTED list the migration walks in the same
      order, so a partial `--apply` released the guard at 0.06% of 7,754 files. The interrupt is
      observed (a pass died at file 4,403/7,754 after ~4h). Now a completion marker + an
      evenly-spaced content sample. 1ed5b4bb.
- [x] **Offender lists were being deleted by a 300-char clip** — `finalize()` names the failing
      sub-units precisely so a finding is actionable, and the orchestrator cut the list mid-token
      at 4 of 7 with nothing saying the rest were dropped. Now 1400 chars, ends on a whole
      element, announces truncation; `_named` cap 6 → 20. ac1f86f9, 12c62faa, c7e7f3fe.
- [x] **Impossible dates now AGGREGATE** into the orchestrator's per-source line instead of one
      print per file into a log nobody diffs. aab2f441.
- [x] **`audit_impossible_dates.py` made two-sided** — it read only the footer MAX, and a counter
      read as a year starts at 1. Lower bound calibrated against the data (1850 flagged 25
      sources, nearly all genuine); deep-history sources allowlisted by name AND reason. 19d4d201.
- [x] **requirements-updater.txt** — pandas AND pyarrow both diverged dev-vs-CI; all three majors
      capped to what the runner already resolves. 84ee688a.
- [x] Health gate: ofr proven healthy (upstream's newest was exactly what we held); four `static`
      sources declared complete on PROBED publisher evidence; worldbank given
      `data_cadence: annual`.

## Open — data damage, NOT yet repaired

- [ ] **~637,000 SERVED rows carry fabricated dates across 7 sources.** New fabrication is
      stopped; the existing rows are not repaired.
      stat_slovenia 05W **506,605 — the whole file** (one key holds 5,863 obs dated year
      1,2,3…6152, all at 12-31) | scb HE+BE 71,368 | statfin tyonv 32,013 (77%) |
      oecd 25,160 | hagstofa 1,120 | eurostat 912 (different cause — see below) | cbs_nl local.
      Every key names its own cause once translated: `DodaVeckaRegionCKM` deaths by WEEK,
      `DRUŽINE` families, `Alue`/`Ikäryhmitys` region/age, `Mælistöð`/`Mánuður` station/month.
      **Repair is a clean RE-PULL per table, never a date patch** — 41,091 of stat_slovenia's
      fake dates land in 1900..2200 and are indistinguishable from real data (R288: a naive date
      fix on cso would have destroyed 11 of 12 rows; only a negative control caught it).
      Template: `tools/cso_repull_matrix.py`.
- [ ] **ons_uk's 4 ashe files hold BOTH key grains** at 1.8 rows/key, summing to exactly the
      20,198,302 on the old state row. They cannot self-heal: repair means writing FEWER rows
      than are on disk and never-shrink refuses it by design. The #42 prune tool's precondition
      (ons_uk completes one run) is NOW MET.
- [ ] **The catalogue serves 7,159 rows of the SUPERSEDED PWT 10.0 against 60 of current 11.0.**
      PWT 11.0 has been ingested since July as `penn_world_table` — a browsing user mostly finds
      the old vintage. Product decision, not a bug.

## Open — needs Ahmed (both fully prepared)

- [ ] **`python tools/refresh_r2_catalog.py 20260803 --allow-shrink zillow,ksh`** — DENIED by the
      permission classifier. **36 of 57 partial units are blocked by nothing else**: 1,285,487
      uncatalogued keys, across runs that already fetched 250,418,112 rows successfully. Those
      sources are not broken — eia +150,676,326, vdem +79,590,111, fhfa +3,227,580 — they merge
      fine and then report `partial` solely because changed keys have no catalog entry, and since
      a partial never sets `last_success_utc` they also read as "never succeeded" everywhere.
      `--allow-shrink` is scoped to two sources with verified reasons (zillow out of the serving
      surface; ksh withdrawn upstream); every other source keeps never-shrink protection.
- [ ] **`python tools/rekey_eurostat.py --apply`** — overwrites 7,754 production files. The guard
      hole is fixed, so it is now safe to attempt. **The run I watched for six hours was
      `--dry-run` and writes NOTHING** (R323) — `--apply` has never been run. If the dry run's
      FINAL conflicting-revisions line reads 0 (it was 0 through 5,400/7,754), every collapse was
      an exact duplicate, no served value changes, and it is mechanical rather than a judgement
      call. Read that number from the END of the log (R246).

## Open — diagnosed, not yet fixed

- [ ] **stat_estonia** — DELIBERATELY HELD. An agent recommended a long-cadence gate on the
      `Lepetatud_tabelid` subject citing "99.0% have max(obs_date) <= 2024". **I measure 87.9%,
      and 7 tables carry 2025+ data.** Both can be true (2,832 catalog entries vs 58 that landed
      data) but the gate would silently delay live tables. Note the subject-level max is
      2080-12-31 (population projections), so any staleness gate here must be PER-TABLE — a
      subject-level test reads it as fresh. See R325 for why I stopped rather than adjusting the
      threshold.
- [ ] **insee_bdm** — INSEE is rate-limiting; needs a pacing change, possibly a conversation with
      the publisher.
- [ ] **ecb's real coverage is DECLINING** — attempted fell 297 → 288 → 250 over 2026-08-01/02/03
      at a fixed 540 denominator and a fixed ~2,105 s budget. The 35-minute slice buys less each
      day and the rotation may not be draining the tail. Cause NOT established.
- [ ] The remaining 97 unscheduled sources are dominated by four organisational decisions, not 97
      integrations: unctad (38), the imf_* family (~24), fao_*, unesco_*. 217 source ids come from
      only **90 distinct publishing organisations**; 82 of those contribute one id each.

## Where the records live (checked 2026-08-04, because it was asked)

- `.claude/MISTAKES.md` — **128 entries**, git-tracked and pushed. 14 this session (R312–R325).
- `.claude/WORK_QUEUE.md` — this file. Tracked. **Was 4 days stale before this update.**
- Commit messages + code comments — pushed; the most detailed record of WHY each fix looks the
  way it does.
- `session_log.md` (hf repo) — full session narrative, but **gitignored: this disk only**.
- The in-session task list (92 items) — **harness state, no file on disk.** That is why this
  section exists: anything that matters has to reach one of the tracked files above.

---

# 2026-08-04 (later) — the fabricated-date repair, and three instruments that lied

**Progress: 120 of 217 sources / 9,496,558 of 10,863,548 series scheduled.** Unchanged by this
block of work, which was data CORRECTNESS, not coverage.

## The headline number was right; I talked myself out of it twice

`~637,000 SERVED rows carry fabricated dates` was correct. Re-measured two-sided against R2:
**637,178 rows, and all seven sources ARE in SUPPORTED_SOURCES** (219 ids). Three probes said
otherwise and all three were wrong in the reassuring direction — a `series_id LIKE` missing an
`SI:` segment, a line-anchored regex that matched 10 ids, and quote-pairing thrown off by an
apostrophe inside a `//` comment. On the third I ran a control, watched it FAIL, and published the
numbers anyway. **A failed control voids the run.** R329.

## It is TWO defect classes, not one

| class | what it is | sources | repair |
|---|---|---|---|
| COUNTER | a non-time dimension read as years — OURS | stat_slovenia, scb, statfin | clean RE-PULL (R22) |
| SENTINEL | the PUBLISHER's own placeholder, faithfully recorded | oecd 2999, eurostat 9999 | parser guard / hosting decision |

Eurostat PROBED directly: `TEN00001` and `ENV_WAT_LTAA` return `time: ['9999']` with `freq='NAP'`
— genuinely time-invariant "long-term annual average" tables. We did not fabricate those.

## Done

- [x] **stat_slovenia 05W — 506,605 rows retired, backed up, re-pull dispatched** (run
      30879564906). The negative control is the whole story: a whole-file retire looked obviously
      right at 81.4% out-of-range, and would have **destroyed 1,463 real observations**. Of 33
      tables, 23 are settlement counters and **10 are real** — they carry a `LETO` (= YEAR)
      dimension with n=6 and n=2 values matching the measured distinct-year counts exactly, at
      1991/2002, Slovenia's census years. SURS reports `time=False` on EVERY dimension in BOTH
      kinds, so the presence of `LETO` is the only discriminator. Both controls pass on the fixed
      parser: the 10 re-parse to exactly their on-disk row counts, the counters return 0.
      Backup VERIFIED at `r2://_backup/repull/stat_slovenia/20260804T050351Z/05W.parquet`.
- [x] **scb — the real bug was a MISSING GRAMMAR, not a bad axis.** All five bad tables have a
      proper `Tid` flagged `time=true`; `parse_date` simply could not read `2011-2012` (multi-year
      windows) or `2025V01` (weeks — only `W` was handled). Zero parse-rate on the right axis, so
      the resolver fell through to `Region`, whose municipality codes 0114..2584 became years
      114..2026. Added both grammars; verified live: 91/91/91/74/14,974 rows, zero out-of-range,
      control unchanged. **Without this, a re-pull would have written 0 rows and looked like a
      success.** 98a57131, R331.
- [x] **oecd — 25,160 rows at 2999 dropped at parse time**, in BOTH the fetcher and the ingester
      (kept in sync deliberately). Safe because every 2999 key ALSO has real dated observations —
      checked, not assumed — so it removes an appendix row per series, never a series. 88f5f4f9.
- [x] **The whole re-pull toolchain pointed at a dead drive.** `repull_worklist.py` printed
      `GRAND: clean=0 corrupt=0` across nine sources against a store holding 637,178 bad rows,
      because `DATA = D:/research/econfindatalibrary/...` and the store moved to E:. `isdir()==False`
      reads as "this source has no data". Now derived + exits 2 when everything is skipped; the
      same scan reports **clean=23,174 two_axis=94**. 78ecb30e, R330.
- [x] **0 hardcoded `D:` constants remain in runtime code** (was ~50). Five were not cosmetic —
      most importantly `jobs/ingest_edgar_13f.py`, where a stale `PROJ` makes the licence-gate
      import raise into an `except` that silently substitutes a PERMISSIVE stub. Also disabled
      `pipeline/title_unctad_cioiuibbicoeair4a.py` outright: it fetches api.db.nomics.world,
      banned by §0. 537644b6.
- [x] `repull_file.py` now tests BOTH date bounds and no longer calls in-range rows "FINE" — that
      line said 291,830 for 05W when the true figure was 1,463. c868fd0e.

## Still open on this defect — 103,381 rows, all COUNTER class

The damage is **9 tables**, not scattered rows:

    scb HE   TABIRH3/4/5                61,152 rows   grammar SHIPPED, needs re-pull
    scb BE   DodaVeckaRegionCKM,
             Medellivsl                 26,206 rows   grammar SHIPPED, needs re-pull
    statfin  tyonv 12tc.px              32,884 rows   parser ALREADY correct (Koulutus, 522
                                                      education codes 0011..9999); rows are legacy
    hagstofa Umhverfi UMH11130/40/50    22,994 rows   parser ALREADY correct — VERIFIED, all three
                                                      re-parse to 1949..2024 with zero bad dates

**Do NOT retire scb BE.parquet as a whole**: 1,553,817 rows of which 26,206 are bad. A whole-file
retire re-pulls 264 tables through MAX_CELLS-limited tailing queries and leaves the database thin
for many runs. Precedent is `tools/cso_repull_matrix.py`, built for exactly this (subject-grain
would have deleted 742 matrices to fix 60). statfin tyonv (41,810 rows) and hagstofa Umhverfi are
small enough to retire whole.

**Blocked on the single-writer guard (R5) while run 30879564906 is in flight** — that guard is
correct and stopped me mid-repair; resume the retires when it lands.

## Verify when the run lands

    python tools/audit_impossible_dates.py --r2 --source stat_slovenia    # expect ZERO
    # AND 05W.parquet back at ~1,463 rows. NOT 0 — 0 means the ten LETO tables were lost too.

## CORRECTION to the block above — the band prune was NOT sufficient for scb (R334)

The section above says the prune was applied and verified. It was applied; the verification was
worth less than it looked. **`audit_impossible_dates --r2` reported 0 affected for scb, and 15,990
rows in those five tables were still fabricated.**

scb's fake dates are Swedish MUNICIPALITY CODES, 0114..2584. A sane-band prune removes the codes
below 1500 and above 2200 and KEEPS everything between, so codes 1500..2200 survive as entirely
plausible years:

    BE:...:Medellivsl:Kon=1:ContentsCode=000000NH:Tid=1998-2002   obs_date 1715-12-31
                                                   ^^^^^^^^^^^^   ^^^^ code 1715, not a year

Measured after the band prune, every surviving row still carried the OLD grain: 3,731 / 3,731 /
3,731 / 2,829 / 1,968. I had written the warning myself, earlier the same night, about
stat_slovenia — and filed it under stat_slovenia instead of under "code-as-year fabrication".

**A RANGE TEST CANNOT DETECT CODE-AS-YEAR FABRICATION, EVER.** Codes and years are the same
integers. Use the structure that is definitionally impossible instead: `Tid=` inside a series_key
(time varies per observation, so it cannot be part of a series identity) — the same signature cso
used (`TLIST(A1)=1991`) and ons_uk used (`calendar-years=`). And when a repair reports success,
**inspect what SURVIVED, not what was removed** — that question, not any instrument, is what
caught this.

### State of the four sources, corrected

    statfin  tyonv    CLEAN   32,013 dropped, audit 0 affected
    hagstofa Umhverfi CLEAN    1,120 dropped, audit 0 affected. NB those codes are Hagstofa's
                              CLIMATE NORMALS on the `Ár` axis (3000..3005, labelled "Average
                              1951-1980" … "Meðaltal 1991-2020") — NOT station codes as I first
                              guessed. parse_date already rejects them, so the prune is durable.
    scb      HE/BE    PARTIAL 71,368 band-dropped, but 15,990 old-grain rows REMAIN

### The one command left on this defect (needs the store writer free)

    python tools/prune_bad_grain_rows.py --apply --only scb     # GRAIN pass, drops 15,990
    # then delete the five entries from _REGRAIN_QUARANTINE in
    # updater/strategies/fetchers/scb.py — ORDER IS LOAD-BEARING, rows first, quarantine second.

Dry-run verified: HE 167,633 -> 156,440, BE 1,532,408 -> 1,527,611. The five tables EMPTY (100% of
their content is fabricated) and are restored by the publisher on the next tick once the
quarantine lifts — which is why lifting it is not optional.

**Blocked, and it is ordinary contention, not a fault:** `updater-heavy` is `queued` for a runner
and my dispatched `updater-daily` is `pending` behind it on the shared `aqueduct-updater`
concurrency group. 05W's re-pull is in that pending run.

## FINAL STATE of the fabricated-date defect — every figure R2-VERIFIED

The two corrections above were themselves measured with a broken instrument. `--r2` did not select
R2 (R335, fixed in 31f1346a): the flag only chose a listing function while the store came from
`AQUEDUCT_BACKEND`. The local tree is a scratch mirror of the LAST RUN, hence systematically
cleaner than what users download — so the audit failed toward "fixed", and it was the check
confirming this very repair. **Every number below was re-measured with the backend actually set.**

    scb            0 affected     87,358 rows removed (71,368 band + 15,990 grain); quarantine lifted
    statfin        0 affected     36,933 rows (32,013 tyonv + 4,920 mkan/tkker dual grain)
    hagstofa       0 affected      1,120 rows; parser already rejects the codes, so durable
    stat_slovenia  3 rows         05L1027S — LIVE bug, task #95
    oecd          25,160 rows     @2999, the PUBLISHER's placeholder; parse guard shipped, legacy stays
    eurostat         912 rows     @9999 + freq=NAP — time-invariant tables; hosting decision
    cbs_nl             0          its flagged files are ABSENT FROM R2 — local only, never served

**125,411 fabricated rows removed from the served store**, plus 505,142 retired with 05W.

### The one still fabricating, and why it is not a quick fix (#95)

SURS sets `time: true` on the AGE axis of 05L1027S ("Deaths by COMPLETED YEAR / YEAR OF BIRTH").
Code `'1000'` is labelled *"Deaths - TOTAL"* and parses to year 1000. `resolve_time_dim` returns a
flagged axis unconditionally.

I built the obvious guard — require the flagged axis to yield one sane date, else None — and **the
test suite killed it**: 7 failures across ssb/hagstofa/statfin/dst. Some PxWeb tables index time
POSITIONALLY (`category.index` = {"0":0,...}) and carry the period only in `category.label`; the
parsers already fall back to labels, and that fallback is what fixed hagstofa's 26 false structural
breaks. A codes-only check makes those axes look unreadable and kills it.

Nor may it fall through to the value scan — that is the 87,358-row scb failure, where the
publisher was RIGHT about which axis was time and we simply could not read it.

The real fix judges the flagged axis on CODES **or LABELS**, and returns None (never another
dimension) when neither works. It needs a signature change to `resolve_time_dim` and every caller
checked. Deliberately not rushed: the mechanism it touches protects four sources, and R318 is the
entry about loosening a working gate.

### Two rules earned here, both now in the digest

- **A range test cannot detect code-as-year fabrication.** Codes and years are the same integers.
  Select on the structure that is definitionally impossible — a time value inside a series
  identity (`Tid=`, `TLIST(A1)=1991`, `calendar-years=`). R334.
- **A flag that names a store must SET it**, and print what it RESOLVED. R335.

## PWT 11.0 fully served — 60 -> 7,163 series live (2026-08-04)

7,103 series of the CURRENT Penn World Table went from invisible to live. Not new data — data
already in R2 that nothing pointed at.

**The gap.** The store held 7,163 series (newest obs 2023-12-31); the catalogue held 60, because
`connectors/penn_world_table/connector.py` hard-codes VARIABLES(6) x ECONOMIES(10). The ingester
outgrew that years ago — its own docstring says it publishes "the 42 NUMERIC variables". Meanwhile
the SUPERSEDED id `pwt` was fully catalogued at 7,159 series ending 2019-12-31, with 7,159 CSVs in
R2. Both ids served. So a browsing user found the four-year-stale vintage in full and the current
one at 0.8%.

**Verified at every layer, each gating the next:**

    catalog.db   60 -> 7,163      tools/catalog_penn_world_table.py (259f7593)
    derive dry   7,163 derived, 0 unresolvable
    R2           7,163 objects; MISSING 0 / ORPHANED 0; byte-compare 40/40 identical
    D1           60 -> 7,163      core/sync_catalog_d1.py, replay-verified before wrangler
    LIVE API     penn_world_table:avh:AGO    -> "Average annual hours worked … Angola"  2005..2023
                 penn_world_table:rtfpna:ZWE -> "TFP at constant national prices … Zimbabwe" 1958..2023

**Nothing invented.** VAR_DEFS (42, "verbatim from the workbook's Legend sheet"), 185
countrycode/country pairs from pwt110.xlsx, per-series dates measured from the parquet, licence
matching the existing rows. Zero series skipped for a missing definition or country.

**Two endpoint facts, so nobody later reads them as breakage:** `/v1/series/{id}` needs a suffix
(`.metadata.json` / `.csv`) — the bare path 404s for every series including long-served ones. And
`.csv` returns 401 for new and pre-existing alike; downloads are auth-gated by design. Both probes
carried a known-present control, which is the only reason those were read correctly.

**Reserved — Ahmed's call:** what becomes of `pwt` (10.0). Both vintages are now fully derived and
catalogued under different ids, so 2019 and 2023 sit side by side. Retiring or re-labelling
existing series ids breaks saved links, notebooks and MCP configs (§2). Leave-and-label, retire,
or alias.
