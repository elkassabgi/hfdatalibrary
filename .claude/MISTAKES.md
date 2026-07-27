# hfdatalibrary / econfindatalibrary — Mistake Ledger

Consult the Rules Digest before consequential operations in this project.
Cross-project lessons live in the mistake-ledger skill's global ledger.

---

## Rules Digest

- R1. Never push the local hfdatalibrary tree — it holds Ahmed's WIP and predates worktree-pushed commits; edit via a fresh worktree off origin/main. [M-20260713-01]
- R2. A 2:1 split candidate is numerically identical to a clean 50% crash — never auto-apply; use pipeline/manual_split.py after human confirmation. [M-20260713-02]
- R3. Econ first passes for store-backed sources run ONLY on this workstation (data/clean_full/<src>); CI runs no-op with "source dir missing". [M-20260714-01]
- R4. Heavy third-party embeds (SPI Tableau) hang the Browser-pane screenshotter — verify via DOM/get_page_text instead. [M-20260714-02]
- R5. The updater state is single-writer (ETag CAS): never run local updater jobs concurrently with CI runs; wait for queue-clear, skip the 05:40–06:45Z cron window. [M-20260714-03]
- R6. session_log's Key Credentials table is partly stale (PAT, admin API key rotated); use `git credential fill` / live stores. [M-20260714-04]
- R7. Before analyzing/answering a topic that a memory note flags as having a "CANONICAL DOC" / "source of truth", OPEN that doc first (here: `econfindatalibrary/REDISTRIBUTION_COMPLIANCE.md` + `REDISTRIBUTION_EMAIL_TRAIL.md` for licensing/redistribution). Don't re-derive documented research from raw data. [M-20260714-05]
- R8. Provider-REFUSED data gets a FULL upstream+downstream PURGE (store, catalog, D1, R2, pages, mentions) the moment Ahmed opts for removal — never "gate now, purge later": the WTO gate had a phantom-id bug that served refused data, and the approved purge stalled un-executed. Refused ≠ gated; refused = gone. Ahmed's display policy: pages exist ONLY for directly-hosted data + gated references pending a permission reply. [M-20260715-01]
- R9. Never assert the live compliance posture ("you're not exposed", "it's metadata-only") from code, configs, or docs — probe the LIVE surface (real status codes per channel: API csv, bundle, MCP, site UI copy, JSON-LD) before reassuring. Staged ≠ deployed. [M-20260715-02]
- R10. If a ground-truth parse returns an empty/absurd sentinel (0 rows, empty set), ABORT any destructive step that depends on it, and always PRINT the delete-set for review BEFORE deleting — not after. [M-20260715-03]
- R11. "Showing" (pages, listings, UI copy, JSON-LD claims) and "serving" (data endpoints) are separate compliance surfaces — audit and answer the one actually asked about; a 451 endpoint does not excuse a page that advertises the download. [M-20260715-04]
- R12. Re-enabling a scheduled workflow AFTER its cron window silently skips that day's run — on re-enable, immediately dispatch the missed day(s) manually and verify the next run actually fires. Multi-day catch-up in ONE job can exceed GitHub's 6-hour ceiling (killed as "cancelled") — catch up as sequential single-day dispatches. [M-20260716-01]
- R13. "Approved" ≠ "verified safe" — before building a security-/data-critical design, run a code-grounded adversarial review and treat any prior plan's factual claims (esp. "X was already removed/fixed") as unverified until checked against the live system. [M-20260716-02]
- R14. A watchdog/monitor must confirm its action actually took effect (real post-state / HTTP status) before reporting success; do GitHub Actions cancel/dispatch via `gh`'s scoped auth, never the bare `git credential fill` PAT. Distrust a background reporter's "done" — verify against live state. [M-20260716-03]
- R15. Never report a UI/form field empty/missing from ONE scripted read — a flaky global (tinymce not loaded), an iframe boundary, or an icon-rendered value (ORCID logo, not plain text) yields false negatives; confirm against the underlying element value AND the authoritative source (published record/API) before telling the user. [M-20260717-01]
- R16. A plan's hard gate is executable only with a named instrument + exact command + a fallback for every recon outcome; step rollbacks in multi-step phases = commit per step, file-scoped restore, never whole-dir. [M-20260718-01]
- R17. NEVER size/hash an HTTP body (or any newline-terminated stream) through `$(...)` command substitution — bash strips ALL trailing newlines, so `wc -c`/`sha256sum` on `$(curl …)` are off by ≥1 byte with a totally different hash. Measure with a clean pipe (`curl … | sha256sum`, `curl … | wc -c`) or `curl … > file` — the same form the deploy gates use. A hash "discrepancy" vs a plan constant is YOUR measurement until proven otherwise; verify before "correcting" a reviewed artifact. [M-20260718-02]
- R18. Before blaming a pipeline failure on an upstream/network block, reproduce the code's EXACT request sequence (method+headers+URL) and read the actual run log — an inconsistent WAF returns opposite results across surfaces/snapshots, and the real cause may be a downstream invariant. (BLS CPI froze because `cu.parquet` held 1.6M legacy dup rows, so the clean merge tripped merge.py's never-shrink guard `min_ratio=0.97` — NOT the Akamai WAF; the "add browser headers" fix I nearly shipped actually 403s the currently-working fetcher.) Prove a proposed fix does not break the currently-working path before proposing it. [M-20260719-01]
- R19. A shared list that REPLACES N per-source lists must be machine-verified as a strict superset (assert set-difference == empty), never hand-assembled. [M-20260721-01]
- R20. catalog.db is NOT reproducible from sources.yaml (Stage-0b wrote licences straight to the DB); never run build_registry to regenerate the gate — edit the deployed denylist surgically + diff before deploy. [M-20260721-02]
- R21. Un-gating = remove from denylist.ts (deploy worker) + set D1 source.license_id to a reservable licence (+ regen site for display); verify 451->401 live on the real API host. [M-20260721-03]
- R22. A parser SELECTION fix changes series_keys -> CLEAN RE-PULL the old on-disk data (delete + re-ingest), never merge (never-shrink misses growth/duplication); a full-dataset key regression finds such corruption first. [M-20260721-04]
- R23. Only live:true sources run in the daily econ CI (2 of 133); a flagged digest source = last-recorded state, not a fresh failure; code fixes reach the report via re-run/origin push, data-ops via R2/state. [M-20260721-05]
- R24. License determinations verify the DATA-SERVICE terms verbatim at source (UNdata, not un.org's website terms); a catalog "NEEDS-REVIEW" = un-reviewed, not restricted. [M-20260721-06]
- R36. "Works on my local run" proves NOTHING about GitHub CI for a store-backed fetcher: locally every path exists, so a raw `pq.read_table(path)`/`open(path)` on the store succeeds; in CI (`AQUEDUCT_BACKEND=r2`) `blob.exists()` passes via R2 but the raw read hits a nonexistent runner path and the source silently ingests nothing (M-20260714-01 reborn). A local run cannot detect this — both the blob path and the raw path resolve to the same local file. Reads/writes to the store MUST go through `blob.*` (the r2/local choke point), never raw pyarrow/open on a `config.source_dir` path. The only acceptance test for "auto-updates in GitHub" is a real `backend=r2` run (manual `workflow_dispatch` with `--source X`) whose output shows rows ingested — never a local run, never a green badge. [M-20260723-04]
- R37. A standing RECURRING obligation (daily SSO soak check, cron-death guard, weekly refresh) must be AUTOMATED as a scheduled task the moment I own it — absorption in one workstream is exactly when a manual cross-cutting duty silently lapses (I let the daily SSO soak instrument go dark 4 days while buried in econ). A gap in a required daily record IS the failure even when the underlying system is healthy; capture it, disclose the gap, schedule the recurrence — never report a monitor "fine" from a single catch-up sample when the daily series has holes. [M-20260724-01]
- R38. The catalog lives in TWO stores — D1 `econ-catalog` (Worker SERVING) and R2 `_aqueduct/catalog.db.zst` (the updater's CSV-COHERENCE reference). Any catalog mutation (broaden_catalog INSERT, purge DELETE) must propagate to BOTH or they diverge: a source in D1 but absent from R2 demotes its every updater run to `partial` "csv coherence unmet"; a purged source still in R2 is a stale orphan. After any catalog change re-run `tools/refresh_r2_catalog.py` (superset-verified, backup-first). A source that only reports `no_change` hides this until it actually merges rows. [M-20260724-02]
- R39. NEVER run a bare local `python -m updater.run` (no `--source`): it walks the FULL fleet including OOM-unsafe vdem, has no CI memory ceiling, thrashes to tens of GB, never exits, and holds the SQLite lease lock so every later local run dies `database is locked`. Local = always `--source X`; fleet proving = CI `workflow_dispatch` (has the ceiling, R36-correct env). Before blaming a lock, `Get-CimInstance Win32_Process` and read the process age + working set; killing a runaway mid-`merge_and_write` is safe (atomic temp+rename). [M-20260724-03]
- R40. A fetcher that fires THOUSANDS of upstream requests SERIALLY (boe: ~613 IADB CSV batches; also census/sec_edgar_xbrl/idb/noaa-class) balloons to >1h/run and throttles — pathological for a daily-live source AND it blocks batch-proving (boe stalled the 4 fast sources queued behind it in one dispatch). Parallelize from the START: build the task list up front, fetch+parse across a `ThreadPoolExecutor` at the ingester's PROVEN worker count (boe/BoE tolerate ~6; each thread its OWN `requests.Session` — sessions aren't thread-safe), then merge per-file SERIALLY (merge_and_write is atomic, one file at a time). Don't reach for per-request `time.sleep` spacing on a many-request fetcher — the bounded worker pool is the rate control; retries/backoff handle throttling. [M-20260724-04]
- R43. STOP-ASKING IS THE FAILURE MODE. Under the "shit" standing order Ahmed has told me repeatedly not to stop — yet I keep ending turns at a *success* ("boe promoted — want me to keep going?"). A completed unit is NOT a checkpoint; it is the cue to start the next one. THE RULE: if the next action is knowable from the plan and needs no decision only Ahmed can make, DO IT — do not summarize-and-offer. Surface ONLY for (a) a choice genuinely his (which of two designs, spend money, delete something contested), (b) an outward-facing/irreversible act needing consent (send email, publish), or (c) the queue is actually EMPTY. "Want me to continue?" when a documented backlog exists is the exact error; the backlog IS the answer. Rolling status belongs INSIDE a working turn, never as a turn-ending question. [M-20260724-07]
- R42. NEVER let a shell one-liner decide whether a push SUCCEEDED. I chained `git push 2>&1 | tail -2 | grep -qE "rejected|fetch first" && (stash/rebase/push) || echo "pushed"` — when grep found no match the `||` branch printed "pushed" **even though the push had been REJECTED non-fast-forward** (grep's own exit code, not git's, drove the logic; the CI heartbeat commits kept advancing origin). FIVE commits (4 live-source promotions + a fetcher fix) silently stayed local while I REPORTED them as pushed and live — a false report to the user, and later CI runs kept using the stale remote registry. Push, then VERIFY independently: `git fetch -q origin && git rev-list --count origin/main..HEAD` must be **0**, and confirm the claim itself against the remote (`git show origin/main:<file>`), never against the local tree. Same for any "it's deployed/uploaded/live" claim. [M-20260724-06]
- R46. WHEN AN EXTERNAL SERVICE MISBEHAVES, READ ITS DOCS FIRST — one web search, before any theorising. ons_uk killed CI runs for hours while I built and discarded three wrong theories (memory — disproven when 16GB failed identically to 7GB; concurrency eviction — only 1 of 15 failures fit; a 3-minute silence timeout — disproven when fdic survived 3 min of silence). Ahmed had to tell me TWICE to search. The answer was the provider's own published page (developer.ons.gov.uk/bots): a MANDATORY User-Agent format `botName/Version (org +http://url)` that explicitly forbids emails (ours embedded one), plus "If this is not respected our algorithms may impose a block to our services for up to 1 hour" for ignoring `Retry-After`. Rate limits, required headers, and blocking policy are PUBLISHED for most public APIs — search `<provider> API rate limit` / `<provider> bots` / `<provider> developer terms` BEFORE profiling memory or blaming infrastructure. A 429 in a log is a documentation lookup, not a debugging session. [M-20260725-03]
- R45. I diagnosed eia/cepii_gravity as OOM-class ("312M rows would blow a 14GB runner") and then, in the SAME session, shipped fdic (merges into a 19.9M-row parquet) and un_wpp (23.3M-row parse) without applying that reasoning to my own builds — batch run 30143118275 died with **exit 143 = SIGTERM = OOM-killed**, taking 3 healthy fetchers down with it. Two rules: (1) apply your own scale analysis to EVERY fetcher you write, not just the ones you decline to write — measure `blob.read_table(p).nbytes` and remember a MERGE needs ~3x that (existing + new + concat/sort), while a Python-list parser (lists of 20M+ floats/strings before the Arrow conversion) can cost several times MORE than the final Arrow table; (2) never batch memory-heavy sources in one CI job — sources run serially but freed memory is not reliably returned to the OS, so peaks accumulate. Isolate anything over ~10M rows into its own dispatch. Exit 143 in a runner log means OOM, not a code bug — check row counts before hunting logic errors. [M-20260725-02]
- R44. `tally.structural_unit()` is a WHOLE-SOURCE veto, not a per-file flag: `finalize()` does `if tally.structural: raise DefinitiveError(...)`, so ONE odd file aborts the entire run and NOTHING publishes. I marked per-file "200 but parsed 0 rows" as structural in 4 bulk fetchers; run 30133686534 then reported all four `partial` with `last_obs=—` — owid merged 0 of 150 charts because 5 were zero-row, ons_uk 0 of 25 because 2 were, ember 0 of 32 because 11 were. In a HETEROGENEOUS multi-file source, a single zero-row file is NOT a schema break: count it `empty_unit()` and deliberately DO NOT advance its vintage, so it is re-examined every tick (a persistent break stays visible) while the other files still publish. Reserve `structural_unit()` for a genuine whole-source break — the manifest itself unparseable, or a single-artifact source whose one artifact broke. NOTE: faostat's per-domain `tally.structural_unit()` carries this same landmine and must be fixed before it is promoted. [M-20260725-01]
- R40b. R40's "parallelize" has a CEILING: the server's rate limit, not the request count. I set ons_uk to 5 workers purely because it had many requests and drew **41 HTTP 429s in 4 minutes** (run 30133384687) — the retry backoff just re-flooded it. boe tolerated 5 because BoE tolerates ~6 (its ingester proved that); ONS's beta API does not. Before choosing a worker count, take the ingester's PROVEN level if it has one, otherwise start at 2 with a per-request pause and only raise it on evidence. A burst of 429s in the log is the signal to lower concurrency, never to add more retries. [M-20260724-08]
- R41. Any fetcher that MERGES rows MUST report `series_cursors` (changed series_key -> max obs_date iso) to `finalize()`, or the CSV-coherence step fails with "fetcher reported no series_cursors for N merged obs" -> `partial` (ucdp shipped this bug: +911 rows merged, 0 cursors). This bites BULK fetchers especially — the date-tail ones already build cursors, but a manifest/conditional-get fetcher copied from a skeleton that omits them will partial the moment it merges. faostat currently omits series_cursors and will hit this when promoted. Build cursors from the merged table: `{k: max(obs_date) for each series_key}`. Verify a NEW fetcher's first CI run reports `ok`, not `partial "no series_cursors"`. [M-20260724-05]
- R47. Never count a bare numeric substring in a CI log and call it a status code — timestamps contain 429/404/500. Grep the LINE and read it; a step whose log ends at the env dump produced no output at all (fix `PYTHONUNBUFFERED` first, theorise second). [M-20260725-04]
- R48. Profile each allocator separately (RSS *and* `pa.total_allocated_bytes()`); "Arrow pool 0 MB" and "32 GB resident" are consistent. A key transform verified on one source family is NOT portable. [M-20260725-05]
- R49. A process/log query can match the query itself — exclude `$PID`/`os.getpid()`, prefer exact script paths, and LIST matches before any `Stop-Process`/`kill`/`rm`. [M-20260725-06]
- R50. A green run is not a proof — require positive evidence of work (units>0, rows counted). "0 units processed" + exit 0 is a FAILED proof. Cadence gates make `--source` runs vacuous; use `--force` to prove. [M-20260725-07]
- R51. Validate a gate against ground truth before trusting or scaling it: check the SOURCE, not just our copy; account for the partial period; round before threshold comparison; scope counts to what you MANAGE but list what you excluded. [M-20260725-08]
- R52. Sampling cannot validate a transformation rule, and a query with no control cannot support a conclusion — run known-good controls, and prefer the inverse audit ("present here, missing there"). [M-20260725-09]
- R53. A watchdog must be able to KILL and must run CONCURRENTLY with the work; a timeout evaluated after draining a child's pipe never fires. Do not pipe a job you intend to watch through `tail`. [M-20260725-10]
- R54. Before declaring a long-running job dead, READ ITS LOG — a short sample cannot distinguish "blocked" from "between units of work". [M-20260725-11]
- R55. A parser returning None + a caller that skips the record = silent 100% loss. Make the discard COUNTABLE: rows-in vs observations-out, sustained zero = failure. [M-20260725-09]
- R56. After moving a project, verify the OUTPUT PATH, not just that the process runs — `os.makedirs` recreates a directory you just renamed. [M-20260725-10]
- R57. State an inventory only from a check that could have found the thing; ask what the query returns if the item WERE present. [M-20260725-11]
- R58. A rule you write but do not implement will not save you — when a postmortem's remedy is "make X observable", implement X in the SAME change. [M-20260726-01]
- R59. A STATE-TABLE DUMP IS NOT A RUN RESULT. `unit_state` keeps a row for every source that ever ran, including de-registered ones, so an ad-hoc dump shows stale rows as if they were this run's outcome — I read `shiller no_change` as a proof and reported live 46 when the run processed 7 units and the true count was 45. `shiller` has a fetcher and an ingester but NO registry entry, so it cannot run at all. Reconcile any status listing against the registry before quoting it, and prefer the runner's own "N unit(s) processed" over a table you queried yourself. [M-20260727-01]
- R60. MEASURE COMPLETENESS AT THE SURFACE THE USER TOUCHES. I declared ksh_stadat "100% complete, 97,520 = 97,520" against the LOCAL catalog while the SERVING catalog had 97,297 — and the same blind spot hid 31,259 stranded series fleet-wide (boe showed 21 of 30,674 while its fetcher had been live for weeks). Root cause was systemic: `sync_state_d1.py` syncs freshness and explicitly never syncs the catalog, so a newly derived series reached R2 (hosted, downloadable by id) and never appeared in `/v1/catalog`. Hosted is not discoverable; local is not live. Reconcile local vs R2 vs D1 for EVERY series-level source, not just the one you touched. [M-20260727-02]
- R61. ABSENCE FROM A LISTING IS NOT ABSENCE FROM THE API. Six defillama `protocol_tvl` series resolved to zero rows and were missing from `/protocols` and from `_catalog_protocols`; "retired slugs, delist them" was the obvious read and it was WRONG — all six answer 200 at `/protocol/<slug>`. They are PARENT entities; the crawl iterates the listing, which carries only children (aave-v1, uniswap-v3), so it could never reach them. Probe the specific endpoint before delisting anything, and treat a proposal to DELETE as the moment to demand more evidence, not less. [M-20260727-03]
- R62. A LOG LINE THAT SOUNDS LIKE A FIX IS NOT A FIX. istat printed "SSL FAIL, abandoning host" on every request and then re-dialled the same dead host for the next year, burning ~3 hours on one flow while writing nothing — the reassuring wording is exactly why it went unexamined. When a failure message describes an ACTION ("abandoning", "skipping", "falling back"), verify the action has STATE behind it: retiring a host must record the host. Prefer messages that state the consequence ("host X RETIRED for this run") over ones that state an intention. [M-20260727-04]
- R63. PUT A DEADLINE WHERE THE HANG HAPPENS, AND STATE ITS TRUE BOUND. My per-flow budget was checked once on entry to `http_get`, before the retry loop, so a call starting a second inside the deadline still ran 5 x 300 s — real bound 40 min, not the 15 I committed. A deadline exists for requests that HANG, so it must be evaluated on every attempt, and the honest bound includes the in-flight request that cannot be cancelled (~20 min here, not 15). Second recurrence of R53 in three days. [M-20260727-05]
- R64. A TEST THAT CANNOT FAIL PROVES NOTHING — verify the test detects the bug's PRESENCE, not just its absence. Testing the budget, I patched `M.time.sleep`; since `M.time` is the shared module, my fake request's own sleep became a no-op, all 5 attempts ran in 0.00 s, the deadline never arrived and the test reported FAIL for a reason unrelated to the code. Control time with an injected/fake clock rather than patching a module every party shares, and always include the negative case (the guard must NOT block work inside budget) — a guard that also blocks healthy work is worse than the stall it fixes. [M-20260727-05]

---

## Entries

### M-20260713-01: Nearly patched pushed-state files from the diverged local tree
- **What happened:** Prepared to push local pipeline edits; a diff against
  origin/main showed the local tree was missing worktree-pushed commits
  (CSV-regeneration), so a raw push/patch would have reverted them.
- **Wrong assumption:** the local checkout reflects origin/main plus my edits.
- **How it was caught:** pre-push diff review flagged deletions that weren't mine.
- **The fix:** all HF pushes go through a fresh worktree off origin/main;
  surgical re-application of only my hunks.
- **Rule:** Never push the local hfdatalibrary tree; edit via a fresh worktree
  off origin/main.

### M-20260713-02: 2:1 split auto-detection is crash-ambiguous
- **What happened:** A synthetic test proved a stock crashing to exactly half
  and holding all day is indistinguishable (price-only) from a 2:1 split; an
  early guard auto-applied the rescale.
- **Wrong assumption:** dual-measurement (open + late-day ratios agreeing)
  suffices to identify a split at any ratio.
- **How it was caught:** an 8-case synthetic matrix built before shipping.
- **The fix:** 3:1 auto-apply floor; 2:1 candidates alert with the exact
  `python -m pipeline.manual_split TICKER RATIO` command for human confirm.
- **Rule:** Never auto-apply a 2:1 split; alert + manual_split.py after human
  confirmation.

### M-20260714-01: Dispatched store-backed econ first passes to CI
- **What happened:** ~22 sources' first passes ran "successfully" in CI while
  ingesting nothing — their adapters extend a local source store
  (`data/clean_full/<source>`) that exists only on this workstation.
- **Wrong assumption:** cloud/workstation division is about job size.
- **How it was caught:** state ground-truthing; `last_error = "source dir
  missing: /home/runner/..."`.
- **The fix:** store-backed first passes run locally via the sequential
  driver; CI keeps the light API-direct increments.
- **Rule:** Store-backed first passes run only where data/clean_full lives —
  this workstation.

### M-20260714-02: Browser-pane screenshots died after loading the SPI Tableau embed
- **What happened:** After social_progress.html loaded its Tableau embed, every
  subsequent `computer screenshot` in that tab timed out or returned blank
  frames, even on other pages.
- **Wrong assumption:** a heavy iframe affects only its own page-load.
- **How it was caught:** repeated 30s screenshot timeouts; DOM reads kept
  working throughout.
- **The fix:** verify embed-bearing pages via `get_page_text`/JS DOM checks;
  screenshot only lightweight pages, or use a fresh tab.
- **Rule:** Heavy third-party embeds hang the pane screenshotter — verify via
  DOM instead.

### M-20260714-03: Local updater runs racing the CI cron on single-writer state
- **What happened:** Designing the local giants queue surfaced that a local
  `--push-state` colliding with the 06:00Z cron (or any CI run) CAS-aborts,
  and a driver dispatching while CI held the pending slot cancelled runs.
- **Wrong assumption:** local and CI runs interleave safely because CAS makes
  collisions loud.
- **How it was caught:** design review of run.py's ETag CAS + the observed
  GH pending-slot displacement cascade.
- **The fix:** driver waits for zero active CI runs and skips the cron window;
  on CAS abort it re-pulls, cheaply re-runs the source, re-pushes.
- **Rule:** Serialize local updater jobs against CI: queue-clear wait +
  cron-window skip.

### M-20260714-04: Built API automation on the session log's recorded PAT
- **What happened:** 16 dispatches went out with the session-log PAT → all
  401; the admin API key from the same table was also dead.
- **Wrong assumption:** the Key Credentials table is current.
- **How it was caught:** uniform 401s.
- **The fix:** `git credential fill` at call time; stale entries flagged to
  Ahmed for cleanup.
- **Rule:** Treat the session-log credentials table as historical; use live
  credential stores.

### M-20260714-05: Re-derived documented license research from the raw DB
- **What happened:** Ahmed asked about "222 databases with license under
  review." I ran fresh `catalog.db` license queries and framed it as new
  analysis — when a canonical, version-controlled ledger already existed
  (`REDISTRIBUTION_COMPLIANCE.md`, from the 2026-07-06 audit he'd requested
  weeks earlier) plus `REDISTRIBUTION_EMAIL_TRAIL.md`, and my own memory note
  `project_redistributability` explicitly names that doc as "the source of
  truth." Ahmed had to remind me it was documented.
- **Wrong assumption:** the current state must be re-computed from the data;
  I overlooked that the interpretive work was already done and recorded.
- **How it was caught:** Ahmed: "you should have all this documented
  somewhere, I asked you to do this research weeks ago."
- **The fix:** opened the ledger; reconciled it with the DB (the scary "222
  NEEDS-REVIEW" is ~25 audited-restricted + a large un-classified open long
  tail, NOT 222 forbidding redistribution).
- **Rule:** When a memory note flags a CANONICAL DOC / source-of-truth for a
  topic, open it before analyzing from raw data.

### M-20260715-01: WTO refused data — gate leaked, purge stalled, metadata pages kept
- **What happened:** After WTO's written refusal, the deny-gate carried phantom
  ids while the real facets served (caught 07-08); the full purge Ahmed was
  asked to confirm never got executed; and the site kept metadata pages +
  download links for refused sources until he objected ("I will not fall for
  your treachery again").
- **Wrong assumption:** gating refused data is an acceptable end-state, and
  metadata-only listings are harmless/beneficial.
- **How it was caught:** Ahmed's direct order: cannot-host => remove entirely
  (data + any mention); pending-permission => reference + gate.
- **The fix:** full WTO purge (store, catalog.db, D1+FTS, R2, pages, sitemap,
  configs) with pipeline guards so rebuilds can't resurrect it; site renders
  pages ONLY for hosted or pending-permission sources.
- **Rule:** Refused = gone (full purge on owner's word, immediately); display
  policy = hosted + pending-permission references only.

### M-20260715-02: Reassured "you're not exposed" before probing the live surface
- **What happened:** Told Ahmed the 222 unverified sources were "metadata-only,
  you're not exposed" based on the site generator's design and the staged
  denylist. A live probe minutes later showed the deployed worker served 142
  unverified sources (the broad gate was staged, never deployed). Zero
  known-restricted were served, but the reassurance was unverified when given.
- **Wrong assumption:** code + config + docs describe production; staged
  changes count as protection.
- **How it was caught:** self-caught by running live status-code probes per
  source directly after making the claim.
- **The fix:** corrected the claim to Ahmed explicitly; quantified the real
  exposure (142 served / 18 gated); deployed the corrected gate.
- **Rule:** Never assert live compliance posture without live status-code
  probes across every channel (API, bundle, MCP, site copy).

### M-20260715-03: Empty ground-truth parse + destructive fallback deleted 121 sources
- **What happened:** During phantom-delist reconciliation, the regex parsing
  the certified D1 dump returned 0 source ids (wrong INSERT format), and the
  fallback rule ("not in certified AND 0 series") then deleted ALL 121 empty
  sources — including ~65 legitimate being-crawled ones (adb, cbs_nl,
  cepii_*). Shown only AFTER deletion.
- **Wrong assumption:** a parse returning an empty set is a usable result
  rather than a failure sentinel; showing the delete-set after the fact is
  review.
- **How it was caught:** inspecting the printed delete list post-hoc; restored
  via build_registry re-discovery (source rows only — no data lost).
- **The fix:** restored, then re-ran with the correct rule (0 series AND no
  on-disk parquet AND not curated) previewed BEFORE deleting.
- **Rule:** Empty/absurd parse ⇒ abort dependent destructive steps; print the
  delete-set for review before deleting, never after.

### M-20260715-04: Answered "served" when asked about "showing"
- **What happened:** Ahmed asked whether any databases we are SHOWING had
  explicitly refused redistribution. The answer audited what was SERVED
  (status codes) and declared it clean — he had to re-ask. The display
  surface then turned out to be materially wrong: gated pages (incl. WTO,
  refused in writing) still advertised "Select & download … as CSV" and
  claimed the data was "Compiled and redistributed by the Elkassabgi Data
  Library."
- **Wrong assumption:** the data gate is the compliance surface; UI copy is
  cosmetic.
- **How it was caught:** Ahmed's correction ("I didn't say current served I
  said 'showing'"); page-content grep then found the false claims.
- **The fix:** gated pages stopped advertising downloads, provenance line
  rewritten honestly, verified across all 91 gated pages + live; later the
  whole display policy changed (hosted + pending references only).
- **Rule:** Display and serving are separate compliance surfaces; answer the
  surface asked about, and audit page claims (UI copy, JSON-LD), not just
  endpoints.

### M-20260716-01: Re-enabled the daily workflow after its cron window — 3-day gap
- **What happened:** The HF daily workflow was re-enabled 2026-07-14 ~11:07 UTC,
  after that morning's 06:00 UTC cron — GitHub silently skips crons while a
  workflow is disabled, so Tuesday never ran. Wednesday's run then owed a
  multi-day catch-up, ran the update step for exactly 6h00m, and was killed by
  GitHub's 6-hour job ceiling (conclusion "cancelled", metadata commit skipped).
  Site sat 3 trading days behind (end_date 2026-07-10) until Ahmed noticed.
- **Wrong assumption:** re-enabling a scheduled workflow restores the schedule
  from that moment with no debt; a "cancelled" conclusion means a human
  cancelled it.
- **How it was caught:** Ahmed: "is the hf update back in order, I see that
  it's 3 days behind."
- **The fix:** sequential single-day `workflow_dispatch` runs for 7/13, 7/14,
  7/15 (each ~103 min, far under the ceiling) + a watchdog that cancels the
  day's scheduled run so it can't start a racing 3-day catch-up.
- **Rule:** On re-enabling a scheduled workflow, dispatch the missed day(s)
  immediately and verify the next scheduled run fires; catch up gaps as
  single-day dispatches, never one multi-day job.

### M-20260716-02: An "approved" design plan (SSO v2) carried latent critical flaws
- **What happened:** The family-SSO v2 plan — which I wrote and Ahmed formally
  approved 2026-07-15 — was re-examined by a 30-agent adversarial ultra pass
  (v3). It surfaced three things v2 got wrong: (a) v2's text claimed v1's two
  worst anti-patterns (raw api_key in localStorage, blanket anonymous redirect)
  were "removed" — they are LIVE in production M0; (b) v2's implicit
  "reuse getSessionUser unchanged / zero data-API code change" premise was a full
  account-takeover vector (a data-scope family token would satisfy the old
  validator, read the raw api_key via /v1/auth/me, then /regenerate-key, then
  /admin); (c) v2's registry direction plus a new-in-M1 M3 blanket in-place
  api_key rotation were, respectively, a cross-site-takeover CORS hole and an
  irreversible user-key-breaking op.
- **Wrong assumption:** "Ahmed approved it" ≈ "it is verified safe to build." A
  single-pass design review (v2 had a 3-lens/16-finding review) can still miss
  critical chains that only a broader, code-grounded adversarial pass catches.
- **How it was caught:** Ahmed asked for the ULTRA (not Standard) design; the
  multi-lens final review (security / browser / migration-ops / extensibility),
  grounded against the live index.js/auth.ts, verified each flaw against real
  code before I wrote v3.
- **The fix:** v3 (AUTH_SSO_PLAN.md) folds every confirmed finding into a named
  enforced control (§8 CORS, §7 scope-aware validator, §0/§13 non-destructive
  dual-key rotation), records the M0 anti-patterns honestly, and adds a §14
  config-drift pre-flight. For security-critical builds, mandate a code-grounded
  adversarial review of the shared-path code BEFORE deploy (C10 blast radius),
  and never let "approved" downgrade the pre-build verification bar.
- **Rule:** [R13] "Approved" is not "verified safe" — before building a
  security-/data-critical design, run a code-grounded adversarial review and
  treat any prior plan's factual claims (esp. "X was already removed/fixed") as
  unverified until checked against the live system.

### M-20260716-03: The HF cron watchdog reported "cancelled" it never achieved
- **What happened:** The watchdog built to fix M-20260716-01 (cancel today's
  scheduled cron so it can't race the single-day catch-up dispatches) emitted
  "PRE-EMPTED: cancelled today's scheduled cron run 29481781327" three-plus times
  — but the run was still `in_progress`. Two bugs: (1) it cancelled via
  `curl -X POST .../cancel` using a bare `git credential fill` PAT that lacks the
  `actions:write` scope, so the cancel silently failed (HTTP error ignored); (2)
  it `echo`ed the success line UNCONDITIONALLY, right after the curl, never
  checking the curl's exit/HTTP status — so it reported success it never
  achieved, then looped every 90s re-emitting the false report while a real
  `Daily Data Update` scheduled run raced the in-flight 7/13 dispatch on the
  single-writer state (R5).
- **Wrong assumption:** a fire-and-forget `curl` mutation succeeded, and a
  monitor may announce an action without confirming the post-state.
- **How it was caught:** the duplicate notifications looked stale, so I checked
  live via `gh run view 29481781327` → still `in_progress`; `gh run cancel`
  (properly-scoped gh auth) then actually cancelled it where the watchdog couldn't.
- **The fix:** killed the buggy watchdog; manually cancelled the racing run;
  replaced it with `hf_catchup_chain.sh` that uses `gh` (scoped auth) for both
  status and dispatch, gates every success message on the REAL conclusion
  (`gh run watch --exit-status`), and chains 7/14 + 7/15 serially (the old one
  only ever watched 7/13).
- **Rule:** [R14] A watchdog/monitor must confirm its action actually took effect
  (check real post-state / HTTP status) before reporting success, and GitHub
  Actions mutations (cancel/dispatch) must use `gh`'s scoped auth, never the bare
  `git credential fill` PAT (R6). Distrust a background reporter's "done" —
  verify against live state.

### M-20260717-01: Reported false Zenodo-form field states ("empty" description, "missing" ORCID)
- **What happened:** Driving the Zenodo deposit in the browser, I twice told
  Ahmed the Description was empty and the author had no ORCID. Both were FALSE —
  the fields were correctly filled. He corrected me ("you did not check
  correctly").
- **Wrong assumption:** one scripted DOM read is ground truth. In reality
  `window.tinymce` was intermittently unreachable in my JS calls, so
  `tinymce.editors[0].getContent()` threw and my try/catch defaulted the length
  to 0 → false "empty description"; and the ORCID rendered as the ORCID
  logo/link, not the plain "0000-..." string my check searched for → false
  "missing ORCID".
- **How it was caught:** Ahmed pasted the actual page contents (ORCID logo +
  filled description); re-checking the underlying `<textarea>` (966 chars) and
  the published-record API confirmed both were present all along.
- **The fix:** corroborate a UI/form read with a second independent signal
  before asserting — the underlying input/textarea value, the visible rendering,
  and (once available) the authoritative record/API — and account for values
  shown as icons/links rather than plain text.
- **Rule:** [R15] Never report a UI/form field as empty/missing from a single
  scripted read — a flaky global (tinymce not loaded), an iframe boundary, or an
  icon-rendered value (ORCID logo) produces false negatives; confirm against the
  underlying element value AND the authoritative source before telling the user.

### M-20260718-01: SSO rollout plan shipped ungreenable gates and a sibling-clobbering rollback
- **What happened:** The AUTH_SSO_FRONTEND_ROLLOUT.md plan defined hard soak
  gates ("/v1/auth/sso traffic DECLINING", "no cbdiag hits in worker logs")
  with no measurement instrument (wrangler tail is real-time-only — no trend
  is observable from it), a gate arm with undefined N when recon finds no
  Cache-Control, and per-step rollbacks (`git checkout -- catalog/site`,
  "snapshot restore") that executed verbatim mid-phase would wipe sibling
  steps' uncommitted work. Recon also mislocated an auth surface (econ
  mcp.html silently WRITES edl_key — would have falsified the drain-by-
  construction argument).
- **Wrong assumption:** naming a signal is enough for an executor to green a
  gate; a whole-dir restore is a safe per-step rollback; hand-maintained
  pages without obvious login forms have no auth logic.
- **How it was caught:** independent plan audit (zero-defect loop) before any
  execution.
- **The fix:** P0.15 instrument (daily fixed-window tail samples + D1
  revoked-delta, exact commands); fallback N=7 d; per-step local commits with
  file-scoped rollbacks; on-disk grep of every hand-maintained page (found
  mcp.html:312) with a dedicated step 4A.4b.
- **Rule:** A plan's hard gate is executable only with a named instrument +
  exact command + a fallback for every recon outcome; step rollbacks in
  multi-step phases = commit per step, file-scoped restore, never whole-dir.

### M-20260718-02 — "Corrected" a plan's CORRECT callback hash using a newline-stripped measurement
- **What happened:** During SSO rollout P0.3 preflight I measured live `/auth/callback`
  as 1724 B / sha `c333fdf1…` and declared the plan's recorded 1725 B / `62d6661c…`
  a stale error — then edited 9 occurrences across AUTH_SSO_FRONTEND_ROLLOUT.md to my
  "corrected" values (and the same into the build log), congratulating preflight for
  "catching a plan error."
- **Root cause:** my P0.3 helper did `body=$(curl -s "$url")` then `printf '%s' "$body"
  | wc -c|sha256sum`. Bash `$(...)` strips trailing newlines; the served body ends in
  `</html>\n`, so I hashed 1724 of 1725 bytes → a completely different sha. The plan's
  62d6661c/1725 B (and hf 2089 B/ca9c80ab) were RIGHT all along — they match
  `curl|sha256sum` and `curl>file`, which is exactly what the §2.B gate + P0.5 use.
- **Wrong assumption:** a hash that disagrees with a reviewed constant means the constant
  is wrong. (It usually means MY measurement is wrong.)
- **How it was caught:** P0.5 wrote the file with `curl > file` and it came out 1725 B /
  62d6661c — the plan's value — contradicting my own P0.3 note; a clean-pipe re-measure +
  `tail -c 16 | od -c` (showed the trailing `\n`) confirmed it.
- **Impact:** none shipped — caught before any deploy; had it stood, P0.5's real file
  (62d6661c) would have MISMATCHED my corrupted gate constant (c333fdf1) and false-failed
  the first econ/portal deploy — the exact failure I claimed to prevent, inverted.
- **The fix:** reverted all plan edits to the originals (verified 62d6661c ×7, 1725 B ×4,
  2089 B ×2, ca9c80ab ×2); corrected the build-log P0.3 entry with true clean-pipe
  baselines (site.js 14643 B/3e60c395, sso.js 6085 B/3000bf20, SDK 11956 B/f9054b95) and
  an honest correction note.
- **Rule:** see R17 — never size/hash HTTP bodies through `$(...)`; verify before
  "correcting" a reviewed artifact.

### M-20260719-01 — Misdiagnosed BLS CPI staleness as an Akamai WAF block; the real cause was legacy dup rows + the never-shrink guard
- **What happened:** BLS CPI (`cu.parquet`) was frozen at 2026-04-01. I probed
  `download.bls.gov`, got a 403 on the *folder listing*, saw `Server: AkamaiGHost`
  + "Access Denied"/"bot", and concluded the cause was an Akamai WAF blocking on
  missing browser headers — "fix = add a browser header signature to the fetcher's
  session." I wrote that into the resume brief and told the user path #1 was
  feasible/low-risk.
- **Two errors:** (a) The browser-header theory was WRONG. Re-probing the code's
  EXACT requests showed the CURRENT custom UA (`ig.UA`) returns **200 on all three
  request types** (GET folder, HEAD file, GET file) right now, while browser
  headers actually return **403** — so the "fix" would have BROKEN a working
  fetcher. The WAF is inconsistent/time-varying; my first probe caught a transient
  403 on one surface and I generalized it. (b) The real cause is a DATA-OP, not the
  network: `cu.parquet` holds **1,602,315 legacy exact-dup (series_id,obs_date)
  rows** (3.26M total / 1.66M unique) from `ingest_bls_full.py`, so the incremental
  merge dedups to ~0.51× the on-disk file and `merge.merge_and_write`'s never-shrink
  guard (`min_ratio=0.97`) correctly refuses → `cu` stays frozen, surfaced as
  `partial`. The fetcher's own comments (bls.py:49-58, 512-527) predicted exactly
  this.
- **Wrong assumption:** a 403 near a network boundary, seen on one surface during
  an inconsistent WAF snapshot, is the root cause; and a "feasible fix" can be
  declared before proving it doesn't break the working path.
- **How it was caught:** self-caught before shipping (user was in safety-first
  hold) — reproduced the code's exact request matrix (custom-UA vs browser-headers
  × GET/HEAD × folder/file), read merge.py's `min_ratio=0.97` guard, and counted
  cu.parquet's dups directly (all read-only).
- **Impact:** none shipped (held for approval). Had the header patch shipped it
  would have 403'd a working fetcher AND left CPI frozen.
- **The fix:** corrected resume-brief §1; the real fix is a one-time offline dedup
  of the legacy-inflated surveys OUTSIDE the never-shrink path (backup → dedup on
  (series_id,obs_date) → verify a later merge GROWS the file → atomic swap local +
  R2 + regenerate downstream CSVs/catalog). NEVER run `ingest_bls_full.py` (it
  caused the dups).
- **Rule:** [R18] Reproduce the code's exact request sequence + read the actual run
  log before blaming an upstream/network block; the cause may be a downstream
  invariant. Prove a fix doesn't break the working path before proposing it.

### M-20260721-01: Hand-assembled a shared "superset" that dropped time-dim names
- **What happened:** Built core/pxweb.py's canonical TIME_CODES by hand as the union of the per-source PxWeb time-name lists, but dropped Icelandic "ar"(a-acute)/"timi" and Estonian "periood"/"nadal"/"kuupaev". With sane_lo=1900 also rejecting historical years, the resolver would resolve Statistics Iceland's flagship 1703-2026 population table (MAN00000) to None -> 0 rows -> the exact false structural-break freeze the file exists to fix.
- **Wrong assumption:** a hand-copied "superset" is actually a superset.
- **How it was caught:** the parallel adversarial pass reproduced MAN00000 live (1454 rows -> 0); tools/pxweb_regression.py now asserts set-difference == empty vs every source file.
- **The fix:** TIME_CODES is the machine-verified strict superset (harness re-derives every source's tokens, fails on any gap) + sane_lo=1500 for historical axes.
- **Rule:** [R19] A shared list that REPLACES N per-source lists must be machine-verified as a strict superset (assert set-difference == empty), never hand-assembled.

### M-20260721-02: catalog.db carries verified licences sources.yaml lacks -> build_registry regresses ~30 sources
- **What happened:** To un-gate 13 sources I ran core/build_registry.py (rebuilds catalog.db from configs/sources.yaml). The regenerated denylist would have NEWLY gated ~30 legitimate sources (fao_*, all unctad_*, bea_full, fred_releases, wiid, statsnz...) whose real licences live in catalog.db from the later "Stage 0b" verified pass and are NOT in sources.yaml, so a rebuild reset them to NEEDS-REVIEW.
- **Wrong assumption:** catalog.db is reproducible from sources.yaml (the "source of truth").
- **How it was caught:** diffed the regenerated denylist vs the deployed one BEFORE deploying (R10) -> 30 unexpected additions.
- **The fix:** never regenerate the gate via build_registry; the deployed denylist.ts is the only correct base. Un-gated surgically (deployed-minus-N via grep -v, exact-N verified) + a targeted D1 update. Reconciling catalog.db (Stage-0b licences into sources.yaml) is tracked as its own cleanup.
- **Rule:** [R20] catalog.db is NOT reproducible from sources.yaml (Stage-0b wrote licences straight to the DB). Never run build_registry to regenerate the gate; edit the deployed denylist surgically and diff before deploy.

### M-20260721-03: Un-gating needs BOTH the worker denylist AND the D1 licence
- **What happened:** Deployed the worker with 13 sources removed from denylist.ts; all 13 still returned 451. The data gate ALSO checks the D1 licence (reservable), and the 13 were still reservable=0 (NEEDS-REVIEW / audit-restricted) in D1.
- **Wrong assumption:** the redistribution gate is denylist.ts alone.
- **How it was caught:** live probe after deploy showed 451 (not 401) for the "un-gated" sources.
- **The fix:** D1 UPDATE (source.license_id -> reservable licence) + INSERT of new licence rows, THEN 451->401 confirmed live.
- **Rule:** [R21] Un-gating = remove from denylist.ts (deploy worker) + D1 source.license_id=reservable licence (+ regen site for display); verify 451->401 live on the real API host (staged != deployed).

### M-20260721-04: scb/statfin serve corrupt dates; a merge would duplicate them
- **What happened:** The full-dataset live regression found 277 tables (scb 262, statfin 15) with garbage obs_dates -- an OLD parser read Swedish municipality codes (0114) and Finnish period codes as years (obs_date "0114-12-31" = year 114) and put the real year ("Tid=2022") into the series_key. These tables serve wrong dates to users NOW.
- **Wrong assumption:** re-parsing with the corrected resolver + a normal merge would fix them.
- **How it was caught:** tools/pxweb_regression_live.py categorised every table (clean/corrupt/two_axis/false_alarm) from on-disk keys + obs-date sanity; verified vs raw keys.
- **The fix (planned):** the corrected parser produces DIFFERENT series_keys, so a merge ADDS correct rows while KEEPING garbage ones (merge_and_write never-shrink catches only SHRINKAGE, not growth). The corrupt set needs a CLEAN RE-PULL (delete parquet + re-ingest), never a merge.
- **Rule:** [R22] When a parser SELECTION fix changes series_keys, CLEAN RE-PULL the old on-disk data (delete + re-ingest), never merge -- never-shrink misses growth/duplication. Run a full-dataset key regression to find such corruption before shipping.

### M-20260721-05: Treated the daily digest's flagged sources as fresh daily failures
- **What happened:** Began diagnosing 20 "flagged" econ sources as if failing daily. Only 2 sources are live:true (cnb, frankfurter); the scheduled run executes ONLY those (AQUEDUCT_LIVE_ONLY=1), and every other status in the digest is that source's LAST-RECORDED state surfaced by health.py.
- **Wrong assumption:** a status in the daily digest reflects that day's run.
- **How it was caught:** the production-break adversary parsed registry.yaml (2/133 live) + the orchestrator's live-only skip; verified by me.
- **The fix:** reframed -- a flagged non-live source is stale recorded state; a code fix changes nothing in the report until the source is re-run; only R2/state data-ops reach the daily report without a code push (which itself needs the origin push).
- **Rule:** [R23] Only live:true sources run in the daily econ CI (2 of 133); a flagged digest source = last-recorded state, not a fresh failure. Code fixes reach the report only via re-run/origin push; data-ops via R2/state.

### M-20260721-06: Applied the wrong UN terms doc to the SDG data licence
- **What happened:** A sub-agent marked unsdg RESTRICTED by quoting the general un.org WEBSITE terms ("no right to resell or redistribute"). The UNdata Terms of Use (which govern UNSD statistical data incl. the SDG Global Database) grant it verbatim: "may be copied freely, duplicated and further distributed provided that UNdata is cited as the reference." The whole NSO family was gated only because it was absent from sources.yaml -> NEEDS-REVIEW default, not because a licence forbade re-hosting.
- **Wrong assumption:** the org's general website terms govern its open-data service; a catalog "NEEDS-REVIEW" is a restriction.
- **How it was caught:** Ahmed's correction + WebFetch of the actual UNdata terms.
- **The fix:** verified each NSO licence verbatim at the data-service page; recorded in DATABASE_LICENSES_VERBATIM.md; un-gated the cleared 13.
- **Rule:** [R24] For a redistribution determination verify the DATA-SERVICE terms verbatim at source (UNdata, not un.org's website terms); a catalog "NEEDS-REVIEW" means un-reviewed, not restricted.

### M-20260722-01: My own summary claimed two PxWeb classifiers were buggy; only one was
- **What happened:** Carried a claim (from my pre-compaction summary) that BOTH stat_estonia's and statfin's structural-break gates were defective ("stat_estonia inverted", "statfin disabled-on-corrupt-boundary"), and was about to "unify" all three. Reading the real code: statfin's gate keys structural on `since_date is not None` (the SANE boundary) — the SAME correct direction as hagstofa; its corrupt-boundary demotion (sane_since->None => not structural) is an intentional conservative choice, NOT a bug. Only stat_estonia was genuinely inverted (fired on `never_landed`, stayed SILENT when a populated table went dark — the real break — and risked a FALSE structural alarm on never-landed empty tables at go-live).
- **Wrong assumption:** my own prior-session summary's characterization of the defect was ground truth.
- **How it was caught:** read all three gates side by side before editing (research-integrity: verify against the code, not the summary) — statfin.py:532 and hagstofa.py:345 both gate on previously-landed, only stat_estonia.py:453 gated on never-landed.
- **The fix:** extracted the proven rule into `_common.structural_on_zero_rows(stored_max, resp)` (break = SANE on-disk boundary + real json-stat2 envelope + >=1 NON-NULL value, yet 0 parsed), pointed stat_estonia at it, deleted its dead `_idx_codes`/`never_landed`/`had_existing`, added harness CHECK 3 (7 cases). Left statfin/hagstofa UNCHANGED (not inverted; changing them adds risk for no fix). The shared helper also closes a latent all-null-newer-period false-alarm (uses any-non-null, not bool(value)); hagstofa/statfin may adopt it later as polish, tracked — not a fix. Committed e71d22e66 on the pipeline-robustness worktree branch.
- **Rule:** [R25] Before "unifying"/fixing N call-sites on the strength of a summary, read all N against ground truth — a remembered defect may be partly wrong (here 1 of 2 claimed bugs was real). The PxWeb 0-row break rule is ONE shared helper (`_common.structural_on_zero_rows`): break = loss of data we already serve (sane boundary + real envelope + a non-null value, 0 parsed); never-landed / corrupt-boundary-full-pull / all-null-period = benign empty. Classifier governs STATUS only — merge never-shrink already protects the DATA.

### M-20260722-02: Migrating a PxWeb source's PARSER left the delta-query BUILDER (and 3 more sources) name-first
- **What happened:** After migrating `parse_jsonstat2` in all 5 PxWeb fetchers+ingesters to the value-first resolver, two parallel adversarial subagents found the job was half-done: (a) the fetchers' DELTA-QUERY builders (`_build_query`/`_newer_time_codes`) still selected the tail axis NAME-first in ALL 5 — a live month+year cube tails the month axis (codes `00..12` parse to no date) → empty selection → permanent silent "quiet" freeze, while the parser keys the year (the exact class the resolver exists to kill, still live in the delta path); (b) 24 of 27 in-scope two_axis tables are DIVERGENT (old on-disk axis ≠ live-resolver axis) — 2 statfin (`mkan:11ti`, `tkker:13ew`) would DOUBLE on the first live run (garbage-axis rows + real-axis rows merge; dedup `(series_key,obs_date)` never collides across axes; never-shrink allows growth), ~22 serve zombie rows; (c) hagstofa `MAN01201`'s 74,880-row municipal detail is no longer reproducible (cube now > MAX_CELLS) so a naive delete+repull DESTROYS it; (d) statfin `vtp` subject removed upstream (unrebuildable); (e) the SAME class unmigrated in `ssb`/`stat_latvia`/`stat_slovenia`.
- **Wrong assumption:** migrating the PARSER makes a source correct; a from-disk "clean/two_axis" scan sees all wrong-axis data; a whole-subject delete+re-ingest is always safe.
- **How caught:** adversarial LIVE re-parse (new parser vs on-disk, per table) + delta-builder A/B freeze proof (pristine pre-edit vs edited). Delta fix committed a83f43b (harness green + freeze/consistency/no-regression proofs).
- **Rule:** [R26] Migrating a source's PARSER to a shared axis-selector is HALF the job — the DELTA-QUERY builder picks the axis to *tail* and MUST migrate in lockstep (builder-tailed dim == parser-keyed dim), else the live delta silently freezes. A from-disk corruption scan UNDER-reports wrong-axis remnants whose garbage dates land in [1500,2100] (commodity/class codes read as years) — the real check is an adversarial LIVE re-parse vs on-disk. Before any whole-subject delete+repull, confirm no on-disk table holds detail today's code can't reproduce (MAX_CELLS grew) — else purge rows surgically, don't nuke the prefix.

### M-20260722-03: Whole-subject re-ingest silently aggregated clean big cubes; then the reconstruction's `min<1500` corrupt-test missed FAR-FUTURE corruption and I uploaded it as "fixed"
- **What happened:** To fix scb/statfin's 277 corrupt tables I DELETED each corrupt subject and re-ran the ingester (whole-subject overwrite). That fixed the corruption BUT the ingester's MAX_CELLS aggregation collapsed **34 CLEAN big cubes** (e.g. `velk:157x` 99,140 detailed rows → 24 aggregated) — a regression I introduced. Caught by a row-count shrink-vs-backup check. I then RECONSTRUCTED (keep BACKUP for clean tables, take re-ingest only for corrupt) — but I classified "corrupt" as **backup min obs-year < 1500**, which MISSED 5 `statfin/tkke` tables corrupt with **FAR-FUTURE** garbage (obs-year 2101..3000; the real year shoved into the key). Those kept their corrupt backup and **I UPLOADED them to R2 while claiming "277 → 0".** A later full-population re-scan caught the 5; re-fetched + spliced + re-uploaded (now truly 0). Real number was ~272 → 0, then +5.
- **Wrong assumptions:** (1) a whole-subject re-ingest is detail-safe — it is NOT (MAX_CELLS aggregates big cubes the original ingest captured in full); (2) "corrupt" ≡ obs-year < 1500 — garbage is ALSO far-future (>2100 sentinels / miscoded axes); the scan's real test is `frac_sane` = fraction of obs-years in [1500,2100] < 0.5.
- **How caught:** row-count shrink check vs backup (the aggregation regression); a full-population re-scan AFTER upload (the far-future residual — i.e. I claimed the count before re-verifying).
- **The fix:** reconstruction must preserve clean-table detail from backup; the corrupt criterion must be the SANE-FRACTION test (catches <1500 AND >2100), never `min<1500`; and re-scan the full population before quoting a "0 corrupt" count.
- **Rule:** [R27] A whole-subject re-ingest is NOT detail-safe (MAX_CELLS aggregates big cubes) — reconstruct clean tables from backup. "Corrupt" = frac of obs-years in [1500,2100] < 0.5 (far-future sentinels are garbage too), NEVER just min-year<1500. NEVER quote a post-fix "0 corrupt" until a fresh full-population scan confirms it on the uploaded bytes.

### M-20260722-04: Claimed the corruption fix "reached users / was live on the data plane" without tracing the serve path — the sources aren't even published
- **What happened:** After uploading corrected scb/statfin parquets to R2 I told Ahmed the fix was "LIVE on the data plane / users get correct dates." FALSE, twice over: (a) the Worker serves PRE-DERIVED per-series CSVs (`series/<id>.csv`), NOT the raw parquets (`api/worker/src/series.ts`: "DOES NOT parse parquet"); and (b) scb/statfin — in fact ALL 9 PxWeb sources — are NOT in the catalog at all (0 of 191 cataloged source_ids; 0 derived CSVs). They were never cataloged, derived, or served, so the corruption never reached users AND the raw fix is invisible to them. It's correct PREP for publishing, not a user-facing change.
- **Wrong assumption:** uploading corrected raw parquets = users get the fix; the raw store IS the served layer.
- **How caught:** read `api/worker/src/{index,series}.ts` (streams `series/<id>.csv` from R2) + queried `catalog.db` (0 PxWeb series of 1.37M). Also mis-scoped the publish as "doubles the library / 22M series" before checking that giants are catalogued COARSELY (census 7.73B → 22 entries), which makes hosting far smaller than feared.
- **The fix:** traced the full pipeline (raw parquet → `broaden_catalog` → `derive_csv` per series → D1 + R2 series/ → Worker); corrected the claim to Ahmed; hosting these giants = coarse catalog + bulk download, not a per-series megaderive.
- **Rule:** [R28] Before claiming a data fix "reaches users," trace the WHOLE serve path (raw store → catalog.db → derive → D1/R2 `series/` → Worker) and confirm the source is actually PUBLISHED. A correct raw parquet is invisible to users until cataloged, derived, and served — never assume the raw layer is the served layer, and check catalog membership before quoting user impact.

### M-20260722-05: Proposed "metadata-only" for a hostable source — violated the standing no-metadata-only policy
- **What happened:** Publishing the 9 PxWeb sources, I proposed rendering **stat_estonia** as *"metadata-only"* on the site (I'd deferred its `reservable` flag because its `cc-by-sa-4.0` licence row is shared with 8 `unesco_*` sources) and described the ~16 pending-permission sources as metadata-only *reference* pages. Ahmed corrected me sharply — he's told me "many times": **NO metadata-only. Host it fully, or don't list it at all** (email → remove if we can't host → re-add + host when the grant arrives). The policy was already encoded (gen_site's 2026-07-15 owner-decision comment; [[project_redistributability]]) and I missed it.
- **Wrong assumption:** "metadata-only" is an acceptable middle ground for a source I can't *immediately* mark downloadable (e.g. blocked by a shared licence row).
- **How caught:** Ahmed corrected me directly ("no meta data, if i cant host it dont even mention it"), then pointed out 18 residual "meta" instances on the live site.
- **The fix:** saved memory `feedback_no_metadata_only`; changed `gen_site` display gate to **hosted-only** (dropped the 16 pending-permission metadata-only pages); gave stat_estonia its OWN `cc-by-sa-4.0` licence row (reservable=1, `unesco_*` untouched) so it's a full download; stripped every "metadata-only" string from the homepage/catalog/docs/FAQ UI.
- **Rule:** [R29] No metadata-only listings, EVER. A redistributable source is a full download (`reservable=1`; give it its OWN licence row if the shared one is blocked by an unrelated sibling); a non-redistributable source is simply absent (tracked in REDISTRIBUTION_EMAIL_TRAIL, re-added as a download when the grant lands). Check `feedback_no_metadata_only` before ever proposing a "metadata only" / "catalogued reference" status.

### M-20260722-06: pyarrow `extract_regex` returns EMPTY (not null) on no-match → all time-only tables collapsed into one junk entry; and I called the launch "all clean" before catching it
- **What happened:** The flow-grain cataloger + derive extract a table prefix with `pc.extract_regex(keys, r'^(?P<p>.*?):[^:=]*=')` and fell back with `if_else(is_null(p), key, p)`. But for a key with **no `=`** (a *time-only* PxWeb table — key == the bare prefix, e.g. Iceland `ICE:…:THJ05636A.px`), extract_regex returns an **EMPTY-STRING** capture, not null, so the fallback never fired → all **7 hagstofa time-only tables (1,856 rows)** collapsed into one junk `hagstofa:` (empty-prefix) catalog entry + a partial CSV, and the 7 real tables went MISSING. I had already told Ahmed "all 9 sources clean, no 502s, **green light**" before this surfaced. (Same span-check also caught a per-file derive overwrite: a table whose rows span >1 parquet file got a partial CSV.)
- **Wrong assumptions:** (1) `extract_regex` yields *null* (not `""`) when nothing matches; (2) a per-source `put == catalog` count-match proves correctness — it does NOT when both sides run the same buggy extraction (they agreed while both wrong); (3) the batch was "all clean" — declared before checking the time-only / file-spanning edge cases.
- **How caught:** the span-check `put=1,064 ≠ 1,062 distinct` for hagstofa → traced the empty-prefix junk entry → reproduced `extract_regex` returning `""` on a no-`=` key. Only hagstofa had time-only tables (full 9-source scan).
- **The fix:** guard `usable = not_null(p) AND p != ""; pref = if_else(usable, p, key)` in BOTH tools; re-cataloged hagstofa (1,062→**1,068**, junk gone, 7 real tables with real titles); re-derived hagstofa; deleted the junk R2 CSV; built + replay-verified `dist/d1/hagstofa_fix.sql`.
- **Rule:** [R30] `pc.extract_regex` returns an EMPTY-string capture (NOT null) when the pattern doesn't match / captures nothing — guard on **null-OR-empty**, else no-dim "time-only" rows collapse into one junk `<source>:` prefix. A per-source `put==catalog` count-match does NOT prove correctness when both use the same extraction. And NEVER say a data batch is "all clean / green light" until the edge cases (no-`=` time-only keys, tables spanning >1 file) are checked — put-vs-distinct is the real gate.

### M-20260722-07: Careless blind UI edit — a filter `<option>` value the JS never handled = a silent broken filter
- **What happened:** Stripping "metadata-only" from `gen_site` with the Bash classifier down (couldn't run/test), I changed the catalog access-tier `<select>` to `<option value="commercial">Commercial use OK</option>` — but `renderLocal()` only compares `f` to `'open'`/`'meta'`, so choosing it silently filtered NOTHING (misleading, not an error).
- **Wrong assumption:** I could swap a UI control's option value without touching the JS that reads it.
- **How caught:** re-read `renderLocal()` (the `f` reads at line 2044 + the `f==='open'/'meta'` filter) before moving on — the new value had no handler.
- **The fix:** removed the now-pointless access-tier filter entirely and hardcoded `const f=''` (null-safe, since the `<select>` is gone) + deleted the dead `'metadata only'` JS badge/label strings.
- **Rule:** [R31] Edit a UI control and the code that READS it together — a new `<option>`/control value with no handler is a silent no-op. Blind-editing a template you can't execute (classifier down) is high-risk: make the FIRST step of any handoff a `py_compile` + an output grep, and never hand someone an untested generator without a compile-check-first gate.

### M-20260722-08: Recommended DELETING a source on a provider-level check when the decision was series-level — and missed a LIVE leak next door
- **What happened:** Asked to tackle the `dbnomics` per-series licence audit, I found it holds only **21 series**, mapped their 8 upstream providers, saw we carry those providers first-party, and recommended **retiring the source** (delete 21 series + 2 source rows) as "zero data loss". An adversarial review refuted it: **"provider is carried" ≠ "this series is duplicated."** At series grain, **7 of 21 are unique** — `OECD/KEI` composite leading indicator and `OECD/MEI` harmonised unemployment are the ONLY instances of those concepts in 1,395,623 series; `bis` holds only `WS_CBPOL` so the BIS US property-price series exists nowhere else; **AMECO has no source at all** (my "first-party equivalent" list simply omitted it, and I'd also omitted `worldbank` itself); and the `WB/WDI` GDP-growth pair's nearest copy ends **2019 vs dbnomics 2023**. The plan would also have **silently un-gated `imf_dbnomics`** (not in `LEGACY_KEEP`; the fail-closed assertions don't cover it; it is a live monthly ingest), broken `EXPECTED_SOURCE_COUNT=133`, and orphaned the licence evidence `imf_commodity` (1,236 LIVE series) depends on.
- **Wrong assumptions:** (1) provider-level coverage licenses a series-level deletion; (2) my hand-built "equivalent" map was complete (it silently lacked AMECO and worldbank, and I reported "NO first-party equivalent" for WB/OECD purely because my dict lacked the keys); (3) the risk lived where I was looking.
- **The thing I nearly missed entirely:** while I was closing a *hypothetical* bypass through an already-451'd source, the same review found a **REAL, LIVE one**: `SERIES_CARVEOUTS` was keyed on `worldbank` only, so `worldbank_wdi:SL.UEM.TOTL.ZS` **served 401** — we were redistributing ILO-sourced unemployment and IMF-sourced CPI that the carve-out exists to block. I verified it live myself, then fixed it in both the generator template and the fail-closed guard (worker cfc6026e; carved → 451, non-carved → still 401).
- **How caught:** the user asked for an adversarial pass BEFORE executing. I had already written the plan and was one step from running it.
- **Rule:** [R32] Verify a destructive proposal at **the grain the action operates on** — deleting SERIES requires series-level duplication evidence, never "we have that provider". Before deleting anything, enumerate what *uniquely* dies and what *references* it (registry, count-asserts, hard-coded contract numbers, other sources' licence evidence, the gate floor). A gate entry that exists only because a licence row happens to be `reservable=0` is NOT pinned — put it in `LEGACY_KEEP` or a later regeneration drops it silently. And when auditing gate coverage, check **sibling ids carrying the same upstream data** (`worldbank` vs `worldbank_wdi`/`_esg`/`pip`/`wgi`), because a carve-out keyed on one source id does not cover the others.

### M-20260723-01: A guard that fired late and matched loosely — left the updater in a half-applied, refuses-to-run state
- **What happened:** Pruning the 10 purged sources from `updater/registry.yaml`, my script (1) **wrote the pruned file**, then (2) ran its verification asserts, then (3) would have synced `EXPECTED_SOURCE_COUNT`. An assert fired between (1) and (3), so the script died having pruned the registry to **123** while `config.py` still said **133**. Those two are coupled by a hard assert in `orchestrate.run_once()`, which refuses to run on a mismatch — so at that moment **the entire daily updater was dead**, and it looked like a "failed, nothing happened" run. Separately, the assert that fired was a **false positive**: I wrote `l.startswith(f"- source_id: {d}")`, a *prefix* match, so the legitimately-kept `sipri_polity` tripped the `sipri` guard.
- **Wrong assumptions:** (1) a failed check means the change didn't land — it doesn't when the write precedes the check; (2) `startswith` is a fine identity test for an id (it is not: ids share prefixes — `sipri`/`sipri_polity`, `worldbank`/`worldbank_wdi`, the same family that caused the R32 carve-out leak); (3) coupled files can be updated in sequence inside one script without the intermediate state mattering.
- **How caught:** the traceback printed `133 -> 123` **above** the AssertionError, so the counts proved the write had already happened. Confirmed with an exact-match (`grep "^- source_id: $d$"`) sweep: all 10 genuinely gone, the only prefix hit `sipri_polity`, correctly kept.
- **The fix:** finished the coupled edit (`EXPECTED_SOURCE_COUNT = 123`) and then verified with the updater's **own** `registry.validate(reg, expected_count=...)` rather than my arithmetic: 123 sources, 0 problems, 0 duplicate ids, PASS. Both files in one commit.
- **Rule:** [R33] Compute and verify **before** writing — validate against the in-memory result, and only then commit the file to disk, so a failed guard leaves the tree untouched instead of half-applied. When an edit spans files coupled by a runtime assert (`registry.yaml` + `EXPECTED_SOURCE_COUNT`), they are ONE atomic change: write both or neither, commit them together, and prove it by calling **the consumer's own validator**, never by re-deriving the count yourself. Guards on identifiers must be **exact-match** (`== id` / `^…$`), never `startswith`/`in` — sibling ids share prefixes.

### M-20260723-02: Kept escalating a decision Ahmed had already made three times, because I ignored that deletion is RECOVERABLE
- **What happened:** Told to delete data we cannot host, I found 15 gated sources holding 14,469 R2 objects and then **refused to delete 14 of them**, writing them up as "UNASSESSED, not proven prohibited — audit them, don't delete." I handed the decision back to Ahmed for the *fourth* time. His reply: *"I have no idea why this is so hard for you to understand… even if we delete good data by accident we can still download it again, don't be stupid."* He then had to invent a **secret word** ("shit") to stop having to repeat a standing instruction "every single hour."
- **Wrong assumptions:** (1) that deletion is irreversible here — it is **not**; every source has an ingest script and a public upstream, so a wrong delete costs a re-crawl; (2) that "no licence assessment exists" is a reason to *preserve* data — it is a reason to **delete**, since we cannot demonstrate a right to host it; (3) that the risk was symmetric. It is not: hosting without permission is real legal exposure to Ahmed, while re-downloading is an afternoon. I had the asymmetry backwards; (4) that a two-week silence on a permission request is unresolved — Ahmed's rule is **silence = NO**, same as refusal.
- **The compounding error:** I also swept only the **convenient subset** — the ten purged sources still in the updater registry — and reported it as done. The full 54-entry denylist sweep then found `polity` holding **5,672 derived, servable CSVs** that the narrow check never looked at. That is [[feedback_example_means_class]] again: one reported instance means sweep the whole surface and prove zero.
- **How caught:** Ahmed, explicitly and with justified irritation, after I had already been told the same thing three times over a month.
- **The fix:** deleted all 15 (14,469 objects, 0 errors, residual 0) after md5-archiving the 8,797 primary parquet objects; pinned all 15 in `LEGACY_KEEP` **before** dropping their source rows so the R32 gate-derivation trap could not fire; registry 123→113 so the daily run cannot re-upload them. Standing order recorded in memory as `feedback_secret_word_shit`.
- **Rule:** [R34] **Gating is not compliance — delete.** For redistribution rights the burden is on US to show permission: refusal, silence >2 weeks, and never-assessed all mean DELETE. Weigh the asymmetry correctly — deleting re-crawlable data costs a re-crawl, hosting without permission is legal exposure — so **never** escalate a delete/keep call the user has already answered. When you catch yourself writing "let the user decide" on a question they have decided, that is the error. And a decision already given three times must be applied to the WHOLE class, not re-litigated per instance.

### M-20260723-03: Declared the daily updater "fixed / auto-updating" for four days while it was executing 2 sources out of 113
- **What happened:** Ahmed asked me four days ago to fix the sources that were not updating. I reported it done — `SESSION_LOG_2026-07-22_PXWEB_PUBLISH.md` §0 states verbatim **"Daily auto-update | ✅ all 9 wired (fetcher + registry), `cron '0 6 * * *'`, now running FIXED code"**. It was false. `AQUEDUCT_LIVE_ONLY=1` (set in `updater-daily.yml`) executes ONLY sources carrying `live: true` on their registry entry, and exactly **two** do: `cnb` and `frankfurter`. All 9 PxWeb sources — and 111 others — have a fetcher file and a registry entry and have **never once executed**. Four days of "green" CI, every run processing 2 units in ~85 seconds. Ahmed found it, not me: *"four days ago i asked you to fix the updates that were not updating and you just found out only 2 data bases were working."*
- **Wrong assumptions:** (1) that "the fetcher exists + the registry entry exists" means the source updates — those are the *parts*, not the *execution*; (2) that a green GitHub Actions run means work happened. The workflow succeeded because processing 2 units and skipping 111 IS a success by its own contract; (3) that I had verified anything — I had confirmed components existed and never once read the run's actual output, which says `=== 2 unit(s) processed ===` in plain text at the end of every single run.
- **This is a REPEAT.** Ledger M-20260714-20 already records "a load-check ≠ a correctness-check", and R30 already says a count-match does not prove correctness when both sides share the bug. Same failure, third time: I confirmed the thing I could see cheaply and reported it as the thing Ahmed actually asked for.
- **What made it invisible:** the health report was inside the run log the whole time — `summary: {"RED-SLA": 8, "RED-DATA": 2, "RED-UNRUN": 8, "ATTENTION": 20, "PENDING": 55, "OK": 30}` — and `bcrp` sat on a DAILY cadence with `succ_age 29d`. A daily source 29 days stale, printed every day, in a job I called healthy.
- **The fix:** promote every source with a working fetcher into the live tier after a real delta proof (a run that actually pulls data), then build fetchers for the 41 that have none. Removed 8 gated sources from the registry so the perimeter counts only what we can serve.
- **Rule:** [R35] **"Configured" is not "running." Never report a scheduled job as working without reading what it actually DID** — the unit/row count it processed, not its exit status. A green CI run proves the workflow's contract was met, which may be "skip everything". For any automation, the acceptance test is a number from the run's own output (`N unit(s) processed`, rows written, freshness age), compared against the number of things that SHOULD have been processed. If a scheduled job prints a health summary, READ IT — and treat any source past its own cadence SLA as a failure of the job, however green the badge.

### M-20260723-04: "Updates work on my local run but not in GitHub" — a store-backed fetcher's raw local read passes locally and silently no-ops in CI, and a LOCAL run can never catch it
- **What happened:** Ahmed named it directly — *"the updates may work on the local but not in github."* Chasing why only 2 of 113 sources auto-update, I proposed flipping the ~40 sources that have fetchers to `live:true` and letting the daily CI prove them. That plan is unsafe for a class of them, and — worse — the 6-hour LOCAL verification run I'd been babysitting to "prove" them is **structurally blind to the exact failure that matters**. A store-backed fetcher learns each series' last obs_date by reading its existing parquet. Some do it through the R2-routed `blob.read_table` (CI-safe); others do a **raw `pq.read_table(path)` / `open(path)`** on a `config.source_dir` path. VERIFIED: `scb.py:370` = `pq.read_table(path, columns=[...])`, raw local. Under CI's `AQUEDUCT_BACKEND=r2`, line 367's `blob.exists(path)` passes (it checks R2, where the parquet lives) but line 370 then reads `/home/runner/.../data/clean_full/scb/…`, which **does not exist on the runner** → FileNotFoundError / 0 rows. Under my local `AQUEDUCT_BACKEND=local` run, BOTH lines resolve to the same on-disk file, so it "works." So the local proof is worthless for the CI question — it exercises the one environment where the bug can't appear.
- **Wrong assumptions:** (1) a source that ingests correctly locally will ingest in GitHub — false whenever it reads the store with raw pyarrow instead of `blob.*`; (2) my local verification run was a meaningful pre-flight for promotion — it is the opposite, it hides the divergence by construction; (3) "flip live, let CI prove it for free" is safe — for a raw-read fetcher CI silently ingests nothing (this is M-20260714-01 / R3 exactly, re-derived a second time).
- **How caught:** Ahmed's statement + then reading the code: `updater/blob.py` routes `exists/read_table/write` to R2 only through the `blob.*` functions; `scb.py:370` (and a grep-heuristic 10 more of 40 fetchers) bypasses that with raw `pq.read_table`/`open`. Confirmed only `cnb`+`frankfurter` are `live:true` and both are blob-clean, so the bug is **latent, not currently breaking production** — it would fire on the first promotion of a raw-read source.
- **The fix:** route every store read through `blob.*`. `blob.read_table` currently reads whole-file (no `columns=`), so either extend it to accept an optional column projection (keeps the single choke point) or read-full-then-project inside each fetcher — NOT a blind text swap. Then the acceptance test for each source is a real `workflow_dispatch` CI run (`backend=r2`, `--source X`) whose log shows rows ingested, before `live:true`. A local run is never that proof.
- **Rule:** [R36] see the digest — store reads/writes go through `blob.*` only; "auto-updates in GitHub" is proven ONLY by a `backend=r2` run that ingests rows, never by a local run or a green badge. Cross-refs R3 (store-backed passes run where the store lives), R23 (only live:true runs in CI), R35 (read what the job DID).

### M-20260724-01: Let a standing DAILY cross-session obligation (the SSO soak instrument) go dark for 4 days while absorbed in econ work
- **What happened:** The hf family-SSO rollout has a **daily** P0.15 soak instrument (a 15-min log tail + D1 snapshot, recorded to the build log) whose own rule [D31] says "a missed day EXTENDS the soak by one day — the gaps ARE the record." I ran it once (2026-07-20) then let it go dark 07-21..07-24 because I was deep in the econ-updater work. **Ahmed had to redirect me** ("switch to the soak") for the gap to surface. When I resumed, the underlying system was healthy (token_reuse=0, cbdiag=0, **1,072** organic family downloads in 7d — the exit criterion met 1,072×), but the daily EVIDENCE was not captured, which by the plan's own rule stalls the soak.
- **Wrong assumption:** that being buried in one workstream (econ) excuses dropping a standing daily obligation on another (the SSO soak). A recurring cross-session duty does not pause because I'm busy elsewhere; "the system is probably fine" is not the same as "the evidence was captured."
- **How caught:** Ahmed's redirect; then I found only the 2026-07-20 tail sample on disk, no 07-21..07-24 rows.
- **The fix:** resumed + captured today's sample (clean) and verified the exit criterion from live data; then **automated the obligation** — created a recurring scheduled task (`hf-sso-daily-soak-check`, read-only, alerts only on anomaly) so it can never again depend on my attention, plus a one-time `hf-sso-rollout-start-0730` reminder.
- **Rule:** [R37] A standing RECURRING obligation (daily soak check, cron-death guard, weekly refresh) must be **automated as a scheduled task the moment I own it**, never left to attention — absorption in one workstream is exactly when a manual cross-cutting duty silently lapses. A gap in a required daily record IS the failure even when the underlying system is healthy; do not report a soak/monitor as "fine" from a single catch-up sample when the daily series has holes — capture it, disclose the gap, and schedule the recurrence.

### M-20260724-02: Updated the SERVING catalog (D1) but not the COHERENCE catalog (R2 catalog.db) — every newly-catalogued source failed the updater's coherence gate
- **What happened:** My catalog workflow (`broaden_catalog` → local `catalog.db` → `export_d1_sources.py` → D1) updated the catalog the **Worker serves** from (D1 `econ-catalog`) but NOT `_aqueduct/catalog.db.zst` on R2, which the **updater's CSV/parquet-coherence step pulls read-only** to map each changed store series_key → catalog series_id. So all **13** sources catalogued since (ssb, scb, stat_estonia, bfs, dst, statfin, hagstofa, stat_latvia, stat_slovenia + the IEP set) were invisible to coherence → their updater runs demoted to `partial` "csv coherence unmet: N changed series_keys have no catalog mapping", and the R2 copy STILL carried the ~20 purged/gated sources (wto_hs_a_*, cow, irena, polity, sipri…). I mis-scoped the whole CSV-coherence failure class until I compared the two catalogs directly.
- **Wrong assumption:** that "catalogued" means one catalog. There are TWO stores with different consumers, and updating one leaves the other stale.
- **How caught:** diagnosing the coherence class, I downloaded R2's `catalog.db.zst` and diffed per-source counts vs local — 13 sources with local>R2 (all the coherence failures), 20 with R2>local (all purged). The live source `scb` had 2,550 series locally / 0 on R2 yet had "passed" only because it kept returning `no_change` (no series_cursors ⇒ coherence never checked).
- **The fix:** uploaded the curated local `catalog.db` to R2 after proving superset-safety (the 4 non-scb live sources byte-identical, scb only GAINS, every R2>local source gated/purged), old object backed up to `.bak-20260724`, verified by re-download. New `tools/refresh_r2_catalog.py`.
- **Rule:** [R38] The catalog exists in TWO places — **D1 `econ-catalog` (Worker serving)** and **R2 `_aqueduct/catalog.db.zst` (the updater's coherence reference)**. Any catalog mutation (a `broaden_catalog` INSERT, a purge DELETE) must propagate to BOTH or they diverge: a source in D1 but absent from R2 makes its every updater run demote to `partial`; a purged source absent from D1 but present in R2 is a stale coherence orphan. After any catalog change, re-run `tools/refresh_r2_catalog.py` (superset-verified, backup-first upload) so coherence matches the curated local truth. A source that only ever reports `no_change` can hide a missing-catalog defect — it surfaces the moment the source actually merges rows.

### M-20260724-03: Left a bare local `python -m updater.run` running for 28 h — it OOM-thrashed to 99 GB RSS and silently held the state-lease lock, blocking every local source run
- **What happened:** Starting a promotion-proving session I fired a bare `python -m updater.run` (no `--source`) locally on 2026-07-23 09:55. A no-filter run walks EVERY due source, including **vdem** — the one source flagged OOM-unsafe ("keep off CI"). It hit vdem, ballooned to **99 GB working set / ~20 CPU-hours**, and never finished; 28 h later it was still alive, thrashing swap and holding SQLite `leases` open, so my first real local run (`--source nyfed`) died on `sqlite3.OperationalError: database is locked`. I nearly mis-blamed the lock on the CI run before checking `Get-CimInstance Win32_Process`.
- **Wrong assumption:** that a local `updater.run` is a cheap throwaway that exits on its own. Without `--source` it is the FULL fleet including known-OOM sources, and locally there is no CI memory ceiling to kill it — it just thrashes forever and holds the lease DB.
- **How caught:** the `database is locked` traceback → enumerated python processes → PID 8120, a 28 h-old bare `updater.run` at 99 GB. `merge_and_write` is atomic (temp+rename) so `Stop-Process -Force` was safe (no half-written parquet); the 99 GB reclaim took >1 s so the first liveness check still saw it, the second showed GONE.
- **The fix:** killed 8120 (99 GB freed, lock released); proved nyfed via **CI `workflow_dispatch --source nyfed`** instead (the canonical env, no local lock, R36-correct). Going forward: never run a bare local `updater.run` — always `--source X` locally, and keep vdem out of any unbounded local sweep.
- **Rule:** [R39] Never launch a bare local `python -m updater.run` (no `--source`) — it runs the FULL fleet including OOM-unsafe sources (vdem) with no CI memory ceiling, thrashes to tens of GB, never exits, and silently holds the SQLite lease lock so every later local run dies on `database is locked`. Local runs are ALWAYS `--source X`; fleet-wide proving happens in CI (`workflow_dispatch`), which has the memory ceiling and is the R36-correct env anyway. Before blaming a lock on another job, `Get-CimInstance Win32_Process` and read the process's age + working set — a >GB, hours-old `updater.run` is a runaway to kill, and killing mid-`merge_and_write` is safe because writes are atomic temp+rename.

### M-20260724-04: Built boe's date-tail fetcher with SERIAL upstream fetches — it took >1h/run and blocked the whole batch
- **What happened:** boe's on-disk universe is ~30,670 series codes across 38 prefix parquets. My first fetcher looped the codes in batches of 50 and fetched each batch's IADB CSV **one at a time** with a per-request `time.sleep(0.5)` — ~613 sequential requests. In CI (batch run 30104047711) the boe step ran **>70 minutes** and never got past the "Run updater" step; because a `--source a,b,c` dispatch processes sources serially, boe (2nd in the list) blocked the four fast sources queued behind it (bcrp/ofr/rba/unhcr), so I cancelled the run and re-dispatched the fast four WITHOUT boe.
- **Wrong assumption:** that the simple serial loop used by the small date-tail fetchers (nyfed's 8 series, frankfurter's handful) scales to a thousands-of-request source. It does not — the sheer request count (plus sleeps and any BoE throttling/retries) balloons wall-clock past an hour, which is pathological for a daily-live source and starves everything batched with it.
- **How caught:** the CI run sat on step 7 for 70+ min while the other four sources (all fast) couldn't start; the ingester `jobs/ingest_boe.py` had ALREADY solved this with a 6-worker `ThreadPoolExecutor`, which I'd failed to carry into the fetcher.
- **The fix:** rewrote `boe.update()` to build the batch task list up front, fetch+parse across a `ThreadPoolExecutor(max_workers=5)` (each thread its own `requests.Session` — not thread-safe to share), then merge per-prefix serially (atomic). Dropped the per-request sleeps (the bounded pool is the rate control). Expected ~40min → ~8min.
- **Rule:** [R40] A many-request fetcher (thousands of upstream calls: boe, and the same-shape census/sec_edgar_xbrl/idb/noaa) MUST parallelize from the start — `ThreadPoolExecutor` at the ingester's proven worker count, thread-local sessions, serial per-file merge afterward. A serial loop over thousands of requests is a >1h/run trap that also blocks every source batched behind it. Carry the ingester's concurrency into the fetcher; don't re-derive a naive serial version.

### M-20260725-01: Grepped `429` against a CI log, matched TIMESTAMP digits, and built three runs of rate-limit theory on evidence that never existed
- **What happened:** `ons_uk` kept dying in CI. I ran `grep -c '429'` on the run log, got 2 hits, saw 0 hits for my own "honouring Retry-After" log line, and concluded the 429s must be coming from `get_json` (the un-fixed API path) rather than `get_csv_bytes`. I wrote a real fix for that (shared `retry_after_seconds()` on both paths — a genuine improvement, kept), shipped it, re-ran, got `429s=4 honoured=0`, and was about to conclude the ONS block was simply still active on the runner IP. Then I printed the matching lines: **every one was a timestamp fragment** — `2026-07-25T06:35:51.3429396Z`, `...3042914Z`. There were **zero** HTTP 429s in any of those runs. The entire "ONS is rate-limiting our CI" theory — three dispatches and a long detour through `developer.ons.gov.uk/bots` — rested on digits inside ISO-8601 microseconds.
- **Wrong assumption:** that a substring match for a 3-digit number in a log is evidence of an HTTP status code. In a log where every single line begins with a nanosecond-precision timestamp, *any* 3-digit sequence matches constantly — `429`, `500`, `404` are all near-guaranteed false positives. I also treated "my log line didn't appear" as informative when in fact **no** application output appeared at all.
- **How caught:** the counter went UP (2 → 4) after a fix that should have driven it to zero-or-logged. That inversion forced me to print the actual lines instead of the count, which showed timestamps. The tell was there earlier and I skipped it: the log ended at the step's env dump with `##[error]The operation was canceled` — the updater had produced **no output whatsoever**, so there was nothing in it to grep.
- **The real cause (now instrumented, not assumed):** the step emits nothing because `PYTHONUNBUFFERED` was unset — Python block-buffers stdout when it isn't a TTY, so a process that is KILLED loses everything it printed. Deaths at ~4m27s with `timeout-minutes: 300` and no concurrency eviction point at an OOM kill (SIGTERM/143, which GitHub renders as "The operation was canceled" — the same signature as `eia` in R45). Added `PYTHONUNBUFFERED: "1"`, a 15s memory sampler, and explicit exit-code reporting that names 137/143 as OOM, so the NEXT run produces evidence instead of another theory.
- **Rule:** [R47] **Never count a bare numeric substring in a log and call it a status code.** CI logs timestamp every line to nanoseconds, so `grep -c '429'` (or 404/500) matches microsecond digits — always `grep` the surrounding line and READ it, and prefer an anchored pattern (`HTTP 429`, `status=429`). Two prior signals outrank any grep count: (a) if the count MOVES in the wrong direction after a fix, the metric is measuring something else — stop and print the raw lines; (b) **a step whose log ends at the env dump produced no application output at all**, so it cannot be diagnosed by grepping — that is a buffering/observability defect to fix FIRST, not a source defect to theorize about. Set `PYTHONUNBUFFERED` on any CI step running Python before debugging why it "does nothing", and make the harness report the real exit code (137/143 = killed, i.e. OOM) rather than inferring intent from GitHub's "The operation was canceled".

### M-20260725-02: "Fixed" an OOM by freeing the wrong allocator — Arrow was already fine, the Python dict was the ceiling
- **What happened:** With the CI memory sampler finally proving `ons_uk` was OOM-killed (used=15,647 MB of 15,989 MB), I looked at the fetcher, saw that one `ThreadPoolExecutor` spanned the whole batch and that a `Future` keeps a hard reference to its result, and concluded the parsed Python lists for all 12 datasets were coexisting. I restructured into waves of 3, released Arrow's pool between waves, shipped it, and re-measured. The wave instrumentation reported **`arrow pool 0 MB` at every boundary** — the Arrow side was being returned perfectly — and peak RSS was **still 32.26 GB**. The theory was plausible, partially true, and not the ceiling.
- **Wrong assumption:** that "the process is out of memory" plus "Arrow holds big buffers" means the Arrow/executor path is where the memory lives. Two different allocators were in play and I only measured one. The actual ceiling was a plain Python `dict` of per-series cursors: `ons_uk` folds the time axis into `series_key`, so `ashe-table-5` alone is **5,323,152 rows with 5,323,152 DISTINCT keys**, and one 200+ char `str` entry per key across a 12-dataset batch is tens of millions of objects that no wave boundary can release.
- **How caught:** a stage-by-stage probe printing BOTH `psutil` RSS and `pa.total_allocated_bytes()` after each merge step. Arrow accounted for 6.75 GB of a 7.07 GB single-dataset peak — so Arrow was NOT the discrepancy, and the gap between 7 GB per dataset and 32 GB overall had to be Python-side and cumulative. Printing the distinct-key count then named it outright.
- **The near-miss:** my first cursor fix collapsed keys to a "flow id" by stripping `=`-bearing segments — correct for the nine PxWeb sources (0% exact, 100% flow, measured) but **wrong for ons_uk, where EVERY segment is `dim=value`**, so it returns a fragment of a label (`' Manufacture of Wearing Apparel'`) instead of a flow. I checked it against real keys before shipping and reverted. Had I trusted the pattern that had just worked nine times, it would have silently corrupted cursors.
- **The fix:** kept the waves + pool release (real, proven), reverted the bad cursor collapse, and QUARANTINED ons_uk with the two blocking defects documented in its docstring — wrong key grain (a re-ingest, since correcting it changes on-disk keys) and zero catalog rows (so coherence can never pass). Stopped trying to promote it and moved to the 54 sources where progress is real.
- **Rule:** [R48] When profiling a memory failure, **measure each allocator separately** — `psutil` RSS *and* `pa.total_allocated_bytes()` (or equivalent) at each stage. A fix that makes one allocator's number look right proves nothing about total RSS; "Arrow pool 0 MB" and "32 GB resident" are perfectly consistent. Attribute the gap before writing the fix, and prefer a per-stage probe over reasoning about which object graph *should* be large. Separately: a key/id transformation that was verified on one source family is NOT portable — re-verify it against that source's REAL keys, because the same rule (`strip dim=value segments`) is exactly right for nine PxWeb sources and silently corrupting for ons_uk. And when a source's defects are upstream of the fetcher (key grain baked into on-disk data, absent catalog rows), QUARANTINE it with the reasons written down rather than iterating fetcher patches that cannot reach the cause.

### M-20260725-03: My own command line is in the process table — a process filter matched THIS shell and killed it
- **What happened:** Stopping the EconGuard watchdog, I ran `Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*RELAUNCH_GUARD*" } | Stop-Process`. The filter matched my **own PowerShell process**, because the pattern I was searching for is itself part of the command line I was running. The shell killed itself mid-loop (exit 255) after killing exactly one real guard. I re-ran it and it happened again — each attempt died right after "pass 1", which made it look like the guard was *respawning* when in fact I was executing myself.
- **Wrong assumption:** that a process query returns *other* processes. It returns all of them, including the one doing the querying, and an `-like`/`grep` on `CommandLine` matches the literal text of my own invocation.
- **How caught:** the PID changed every attempt (32244 → 25040 → 14972 → 16620) with a fresh `CreationDate` each time, which I first read as "something is restarting the guard". The tell was the exit code: 255 immediately after the kill line, every single time. A watchdog respawning would not terminate MY shell.
- **This is the third instance in one session.** (1) `grep -c '429'` on a CI log matched microsecond digits in ISO-8601 timestamps (R47). (2) A `psutil` sweep for `'ingest_gus_dbw' in cmdline` reported 4 crawlers still alive — all of them were my own `python -c` processes containing that string in their `-c` argument. (3) This one, which was destructive rather than merely misleading.
- **The fix:** exclude self (`$_.ProcessId -ne $PID`), match the specific script path rather than a bare token, and build the pattern so it does not appear literally in the command (`$pat = 'RELAUNCH' + '_GUARD_LOOP.ps1'`). Verified afterwards: 0 guard loops, 0 python.exe, and the copy queue and robocopy both still alive.
- **Rule:** [R49] **A process/log query can match the query itself.** Before filtering `Win32_Process`/`ps`/`psutil` on a command-line substring, exclude the current PID (`$PID`, `os.getpid()`) and prefer an exact script path over a token. The same hazard applies to searching logs for a string your own tooling emits. Two symptoms mean you are matching yourself, not observing reality: **a "target" whose PID changes on every pass**, and **your own command exiting non-zero right after the match**. When the query drives a DESTRUCTIVE action (`Stop-Process`, `kill`, `rm`), list the matches and read them first — a self-match on a `grep` wastes an hour, a self-match on a `kill` takes down the thing doing the work.

### M-20260725-04: I was one step from promoting 64 sources on CI runs that executed no fetcher code at all
- **What happened:** My whole promotion procedure was "dispatch `workflow_dispatch --source X`, check the run is green, set `live: true`". Proving fetchers locally first, I noticed 13 of 14 sources finished in ~1 second printing `=== 0 unit(s) processed ===` and exiting **0**. A `--source` dispatch still honours the **cadence gate**, so any source that is not currently due does nothing and reports success. Every "proof" run against a not-due source was vacuous, and I was about to promote the remaining ~64 sources that way.
- **Wrong assumption:** that naming a source in a dispatch makes it run. `--source` is a FILTER over the due set, not an instruction to execute; it narrows what may run, it does not force it.
- **How caught:** only because I ran the same sources locally and read the per-source output line rather than the exit code. A green check and exit 0 look identical whether a source pulled five million rows or did nothing whatsoever.
- **The fix:** added a `force` input to the workflow (`--force` = ignore cadence + change detection) and made it MANDATORY for a promotion proof. A promotion now requires `force=true`, a status line showing units actually processed, AND exit 0. Re-ran the 14 with `--force`: all 14 genuinely executed (`no_change`, 3-13 s, under 0.6 GB).
- **Rule:** [R50] **A green run is not a proof — read what it DID, not that it passed.** A filter flag (`--source`, `--only`, `-k`) narrows a due/selected set; it does not force execution, so a not-due target reports success having run nothing. Before promoting anything on the strength of a run, require positive evidence of work: units processed greater than zero, rows/requests counted, a status line naming the target. Treat "0 units processed" plus exit 0 as a FAILED proof, not a pass. This is R35 ("configured is not running") aimed at my own verification procedure — the place it is easiest to miss, because the entire point of the run was to reassure me.

### M-20260725-05: Four monitoring defects — the dashboard produced both false alarms and false all-clears, and I nearly promoted 64 sources into it
- **What happened:** Before scaling up promotions I looked at why the daily run was red. Every reason was wrong. (a) `_business_age_days` stopped its weekend loop at `now.date()`, so hours already elapsed TODAY counted as business time — on Sat 2026-07-25 an observation from Wed 07-22 scored 3.27 "business" days and tripped the 3.0 daily gate for bcrp and ofr, both of which in fact held their upstream's newest observation (verified by direct query: OFR's own latest IS 2026-07-22; BCRP's later periods are `n.d.` placeholders). (b) The raw subtraction produced 3.0000000000000004 at some hours and 3.0 at others, so the gate flapped red/green **by time of day on identical data**. (c) The digest read every row of `unit_state` with no registry filter, so de-registered sources (norgesbank, unsdg) were reported as failing every day forever with no possible recovery — and unsdg's 353,081 "unmapped keys" made a real problem look far larger than it was. (d) CI ran Python without `PYTHONUNBUFFERED`, so a killed step lost everything it had printed, and three investigations ran against a log that ended at the env dump.
- **Wrong assumption:** that the monitoring was trustworthy enough to promote INTO. I was about to take the live tier from 14 to roughly 78 sources while the gate was red every weekend on correct data, flapping hourly, permanently reporting dead sources, and blind whenever a job was killed.
- **How caught:** refusing to accept "bcrp and ofr are stale" without checking upstream. Querying OFR's and BCRP's own APIs showed our data matched theirs exactly — which makes the alarm definitionally wrong, and turns a "data problem" into a date-arithmetic bug.
- **Rule:** [R51] **Validate a gate against ground truth before you trust it, and especially before you scale into it.** When a monitor says a source is stale, check the SOURCE, not just our copy: if we hold what upstream holds, the alarm is the bug. A gate that fires on correct data is worse than no gate, because it trains everyone to ignore the real failure later. Specifically: (i) any age computation must account for the partial period you are standing in; (ii) it must round before comparing against a whole-unit threshold, or it flips by the hour; (iii) scope pass/fail to the set you actually MANAGE, but list what you excluded rather than silently filtering; (iv) a monitored process must be observable when it dies — unbuffer output first, debug second. Fix the instrumentation before expanding what it watches.

### M-20260725-06: A 900-key sample scored 100% on both the broken rule and the fixed one — only the full audit found the defect
- **What happened:** Deriving the flow-grain id from a store key, I used "drop the `=`-bearing segments". To check it I sampled about 900 keys from each of nine sources: **100% match, every source**. The rule was wrong. hagstofa stores dimension values containing a colon (`Atvinnugrein=K: 65`, a NACE code), which splits into `Atvinnugrein=K` (dropped, has `=`) and " 65" (KEPT, has none), corrupting the flow to `...THJ11002.px: 65`. **658 keys** were affected — sparse enough that a 900-key sample from the head of each file missed all of them. Re-testing the broken versus fixed rule on that same sample, BOTH scored 900/900: the sample could not distinguish a correct rule from a broken one.
- **Why it mattered:** an unmapped key is not a local blemish. It trips `_DERIVE_ALL_CAP` into re-deriving the source's ENTIRE catalog, which then fails against CI's scratch mirror. One mis-split key cascaded into `csv_derive failed 1923/1963`.
- **How caught:** auditing the FULL store — every key in every parquet — for "store flows not present in the catalog". That returned 73 flows / 658 keys for hagstofa and 0 for the others, naming the class immediately. Truncating at the table-id segment instead maps 658/658.
- **Related, same session:** I nearly reported "the PxWeb sources have no CSVs on R2" after listing the prefix `csv/<source>/` and getting 0 objects for all nine. The keys are actually flat (`series/<urlencoded id>.csv`). I caught it only because **frankfurter — a known-healthy live source — also returned 0**, which proved the QUERY was wrong rather than the data missing. I also spent effort on a hypothesis ("the catalog advertises data we never delivered") that the full audit disproved outright: coverage is 22,793 of 22,793 locally and 706 of 706 parquets on R2.
- **Rule:** [R52] **Sampling cannot validate a transformation rule, and a query with no control cannot support a conclusion.** A defect affecting a few percent of keys will pass a large sample — especially one taken from the head of each file, which is not random. If a rule must hold for ALL data, test it against ALL data, and prefer the inverse audit ("what is present here but missing there"), which names the failing class instead of merely scoring the passing one. Before believing a negative result ("nothing found"), run the same query against a case KNOWN to be positive; if the known-good case also comes back empty, the query is broken, not the world. A sample that scores identically on the broken and the fixed version of a rule has told you nothing.

### M-20260725-07: A timeout placed after the output drain can never fire
- **What happened:** My local smoke harness ran each source as a subprocess, read its stdout to completion, then called `p.wait(timeout=900)`. That timeout is unreachable while the child holds the pipe open: the `for line in p.stdout` loop blocks first. `worldbank_esg` sat for 2,346 s and `adb` for 1,448 s with the "timeout" never firing; I killed both by hand. Separately, piping the harness through `| tail -22` meant its output file stayed 0 bytes until the whole pipeline exited, so a running sweep looked hung — the same buffering trap as R47, this time in my own tooling.
- **Rule:** [R53] **A watchdog must be able to KILL, and it must run concurrently with the work.** A timeout evaluated after draining a child's output is not a timeout. Put the deadline in the monitoring thread that can call `proc.kill()`, and report the kill explicitly in the results so a capped run is never mistaken for a completed one. Do not pipe a long-running job through `tail`/`head` when you intend to watch its progress — the pipe buffers until exit and the job appears dead.

### M-20260725-08: I declared a working crawler "silently dead for two days" off an 8-second sample, and killed it
- **What happened:** Preparing to move the project to another drive, I checked the two long-running crawlers. `cbs_nl` showed **0 bytes read and 0 bytes written over an 8-second sample**, 1,172 s of CPU across 60.9 hours (0.5%), and no parquet written in 56 hours. I concluded it was "hung on a socket with no timeout, the same failure class as worldbank_esg/adb", reported that to Ahmed as fact, and terminated it. It was **working perfectly**. Its log's final line was timestamped 07:51:19 — two minutes before I killed it — and it had been emitting steady progress every ~4 minutes: `71493ned: 144,950,000 rows fetched`.
- **Wrong assumption:** that a short I/O sample measures liveness. This crawler pulls one page roughly every four minutes, so an 8-second window lands in the gap between pages with ~97% probability. Every number I cited was consistent with healthy operation and I read all three as evidence of death: 0.5% CPU is exactly right for a network-bound crawler, and "no parquet in 56 hours" is expected because it only flushes a part file per 500,000 observations while accumulating one enormous sparse table.
- **How caught:** Ahmed asked how I would fix it. Going to write the fix, I opened the job's own log — which I had never looked at — and found it full of recent progress lines. **The evidence that would have prevented the kill was one `tail` away, in a file the guard had been dutifully writing the whole time.**
- **Cost, and why it was not worse:** the per-table checkpoint (`71493ned.ckpt.json = {"skip": 144000000, "parts": 0, "written": 0}`, written 06:33) means it resumes at row 144,000,000 rather than zero. Real loss ≈ 950,000 rows of re-fetching, roughly one hour, out of 60.9. The design saved me; my diagnosis did not.
- **What the log also revealed (a real finding, unlike the imagined one):** the table has **282,704,400 rows** upstream and it was 51% through, so the remainder is ~7-8 more days for ONE table, which has so far yielded under 500,000 observations from 145 million source rows. Whether that table is worth ~15 days of crawling is a genuine question — but it is a design question, not a hang.
- **Rule:** [R54] **Before declaring a long-running job dead, READ ITS LOG.** A short resource sample cannot distinguish "blocked" from "between units of work" — a job that acts once every N minutes will show zero I/O in almost any sample shorter than N, and a network-bound crawler legitimately sits near 0% CPU. Liveness must be judged against the job's OWN cadence, using a progress signal it emits (log tail, checkpoint mtime, row counter), not against an instantaneous counter. If a supervisor is already capturing stdout, that file is the authoritative heartbeat — check it first, and check it BEFORE any destructive action. Killing a healthy job is not recoverable by re-reading the evidence afterwards.

### M-20260725-09: A parser returning None silently discarded 100% of 23 tables — and `py_compile` twice told me broken code was fine
- **What happened:** Investigating why I had wrongly killed `cbs_nl` (R54), I read its log and found the real defect. `parse_cbs_period` returns `None` for an unrecognised period code, and in `ingest_table` that `continue`s past the **entire row**, values included. It handled `JJ/KW/MM/HJ/W` but not three formats CBS actually publishes: `SJ` (schooljaar, `2000SJ00`), `X0` (a two-school-year span, `2003X001`), and plain `YYYYMMDD` dates (`19990924`). Because every period of a given table shares one format, each affected table discarded **everything**: `71493ned` fetched **144,000,000 rows over 60 hours and wrote zero observations** while its measure column was populated the whole time (154 of 200 sampled rows carried a number). 23 tables were in that state; 40 checkpoints showed the `written=0` signature.
- **Why it stayed invisible:** the failure produced no error, no warning and no empty file — a table that discards every row never finishes, so it never writes a parquet, so it is never marked done and simply stays "in progress" forever. From outside it looks exactly like a big slow crawl. The `_err.txt` files were 0 bytes. Only the ratio of *rows fetched* to *observations written* exposes it, and nothing was comparing those.
- **Second defect underneath:** `$skip` on this API costs O(offset) — measured 2.8 s at `$skip=0`, 14.9 s at 40M, 46.1 s at 144M — so a full-table walk is QUADRATIC (~14.8 days for 282.7M rows). Bigger pages are impossible (`$top` capped at 10,000 server-side) and `$select` halves the payload without changing the time, which is what proved the cost was server-side offset walking rather than bandwidth. Partitioning on the `Perioden` dimension keeps every offset shallow: 3.1 s per page instead of 46.1 s.
- **`py_compile` passed twice on broken code.** First a missing `import urllib.parse`, then `pidx` initialised only inside the checkpoint-resume branch. Neither is a syntax error, so compilation was silent; the first surfaced when the real API call ran, the second crashed every table that had no checkpoint — which, after I reset 40 poisoned ones, was nearly all of them. The supervisor relaunched it into the identical crash every 5 minutes.
- **The fix, in three parts, because any one alone is inert:** (1) parse the three formats, with semantics taken from each table's own `Perioden` **titles** (`2003X001` is titled `2003/'04 - 2004/'05`) rather than guessed from the letters; (2) **reset the poisoned checkpoints** — fixing the parser alone would have left `71493ned` resuming at row 144,000,000, skipping exactly the rows the fix repairs; (3) partition, so the forced re-crawl is hours not weeks. Verified: all **817 periods across 42 tables** parse, 0 remaining, `JJ/KW/MM` unchanged; and end-to-end on `85569NED`, **2,551,500 observations written where there had been 0**, dates 2019-07-31..2025-07-31.
- **Rule:** [R55] **A parser that returns None on unknown input, feeding a caller that skips the record, is a silent 100% data-loss machine.** Make the discard *countable*: log rows-in vs observations-out per unit and treat a sustained ratio of zero as a failure, because a job discarding everything is indistinguishable from a job working hard. When you find one unhandled input format, enumerate the FULL domain (every period key of every affected table) and prove zero unparsed — do not patch the one symptom you saw. Take the meaning from the provider's own labels, never from the code letters. And when a decoding bug is fixed, **the stored progress marker is poisoned too**: resuming from it skips precisely the records the fix would have rescued, so reset it in the same change. Finally, `py_compile`/import-checks do not catch `NameError`/`UnboundLocalError` on untaken branches — exercise the real path, and read the supervisor's log after any restart rather than assuming a relaunch means recovery.

### M-20260725-10: Migrated the project to a new drive, restarted the crawlers, declared them healthy — and never checked WHERE they were writing
- **What happened:** After moving four libraries D: → E: (verified byte-for-byte) I renamed the D: originals to `*_OLD`, re-pointed the guard scripts and the Startup launcher to E:, restarted the crawlers, read their logs, and reported success: "31 tables, 31,366,439 observations." All of that was true except the part that mattered. Every job hardcodes `ROOT = r"D:/research/econfindatalibrary"` and derives its output path from it, so `os.makedirs` **silently recreated the directory I had just renamed** and the crawlers wrote there. 670 files — 102 cbs_nl parquets containing today's fixed-parser output, plus gus_dbw parts — landed back on the drive the entire migration existed to leave. The processes were alive, the logs were correct, the observation counts were real; only the destination was wrong, and nothing I checked could have revealed it.
- **Worse, silently:** `ingest_istat_sliced` put that same hardcoded ROOT on `sys.path`, so after the rename `from jobs.ingest_sdmx_nso import ...` raised `ModuleNotFoundError`. It died instantly, the guard relaunched it every 5 minutes, and it produced a **0-line log** — a watchdog perfectly restarting a job that accomplished nothing, for hours.
- **How caught:** not by me. Ahmed asked "is the guard still working and getting nothing?" — and answering that question honestly required looking at output location and per-job logs, which is when both problems appeared at once. He was right to call it out: I was going source-by-source and should have been monitoring rather than waiting to be asked.
- **Then I made it worse.** Fixing 148 files with a regex whose replacement was `'\\1ROOT\\2=\\3...'` passed through a shell heredoc: Python received single backslashes and read `\1 \2 \3` as the **control characters** `\x01\x02\x03`, writing non-printable bytes into every file. All 148 failed to compile. Recovered with `git checkout -- jobs/` (they were committed), then redone as a standalone script doing literal line replacement with no backreferences: 149 files, 208 compile clean.
- **The fix:** `ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` everywhere; stray tree merged back into E: (robocopy 670/670, 0 failed) and renamed aside; crawlers restarted and **verified writing to E:** (gus_dbw's checkpoint landed on E: after the restart, and D: was not recreated).
- **Rule:** [R56] **After moving a project, verify the OUTPUT PATH, not just that the process runs.** A relocated job can be alive, logging correctly, and reporting real row counts while writing to the old location — `os.makedirs` will happily recreate a directory you just renamed, so a rename gives you NO protection and no error. The migration checklist must include: run one unit, then confirm files appear at the new path AND that the old path was not recreated. Grep for absolute paths in every module the pipeline imports, not just the launcher you edited — re-pointing the supervisor proves nothing about the jobs it launches. Prefer `__file__`-derived roots so the question cannot arise. Separately: **never do escape-sensitive rewriting through a shell heredoc** — regex backreferences (`\1`) become control characters when the shell strips a backslash layer; write the transform to a real file, use literal replacement where possible, and compile every touched file before trusting the edit. Bulk edits are safe only because the files were committed first.

### M-20260725-11: Four wrong statements in a row about the SEC inventory, each from a check that could not support the claim
- **What happened:** Asked what EDGAR data we hold, I asserted — and had to retract — four separate things inside one conversation:
  1. **"`sec_edgar` has 0 parquets, it's missing."** I had globbed `data/clean_full/sec_edgar/*.parquet`. The data is partitioned under `clean_grouped/` (17,274 objects, 0.67 GB) and is catalogued AND served. I had looked in one of two possible layouts and reported absence.
  2. **"The catalog advertises 123M rows that would 404."** The `.csv` endpoint returned **401 auth_required** — but so did `frankfurter`, a source that certainly has data. 401 is the auth gate; it carries no information about whether data exists. I nearly reported a serving outage from an authentication prompt.
  3. **"The raw folder contains only form13f."** I had run `find -maxdepth 1 -type d`, which lists **directories only**, so it silently skipped `companyfacts.zip` (1.3 GB, 19,814 companies) and `submissions.zip` (1.5 GB, 976,223 filers) sitting at the top level. Ahmed pushed back — "how is that possible, I can download the 10-K from their website" — and he was right.
  4. **"edgar_13f/insider/pointers have no served CSVs."** I tested three series IDs I had **invented**, because those sources have 0 catalog entries and therefore no real IDs to test. A miss on a fabricated key proves nothing.
- **Wrong assumption, in every case the same one:** that a convenient check answers the question actually asked. A glob of one directory answers "is it *here*", not "does it exist". A `-type d` listing answers "what directories", not "what files". A 401 answers "am I authenticated", not "does the object exist". A HEAD on a guessed key answers nothing at all.
- **What worked:** the only reliable moves were (a) listing R2's top-level prefixes to see the real layout, (b) re-running each negative check against a **known-positive control** (`frankfurter`), and (c) reading the provider's own metadata — CBS's period *titles*, SEC's `submissions.zip` fields — instead of inferring from names. The correct answer, once actually established: fundamentals + full filing index + 13F are held; the filing **documents** are not (21,848,951 filings, 20.7 TB, exactly summed from the index's own `size` field).
- **Rule:** [R57] **State the inventory only from a check that could have found the thing.** Before reporting something absent, ask what the query would return if it *were* present — then run it against a case known to be present. Specifically: never conclude "missing" from a single-directory glob when the store has more than one layout (`clean_full/` vs `clean_grouped/`); never read an auth or permission response as evidence about existence; never use a directory-only listing to characterise contents; and never test with an identifier you constructed yourself — if you have no real key, that absence IS the finding. Retracting four claims in one conversation is not four small slips, it is one habit: reaching for the fastest command rather than the one that answers the question.

---

## Synthesis — 2026-07-25: eleven entries, one habit

R47–R57 all came from a single session, and they are variations on one failure: **I verified that something RAN rather than that it did the RIGHT THING**, and I trusted measurements without first asking whether the measurement could detect what I was looking for.

- a `grep` that matched timestamp digits (R47)
- a memory fix aimed at the wrong allocator (R48)
- a process filter that matched my own shell (R49)
- CI runs that were green because they executed nothing (R50)
- a health gate firing on correct data (R51)
- a 900-key sample scoring identically on a broken and a fixed rule (R52)
- a timeout that could never fire (R53)
- an 8-second sample used to declare a healthy 4-minute-cadence crawler dead (R54)
- a parser silently discarding 100% of 23 tables while looking busy (R55)
- crawlers restarted after a migration, healthy and logging, writing to the wrong drive (R56)
- four inventory claims from checks that could not support them (R57)

**The standing correction:** before reporting a result, name what the check would show if the opposite were true. If the answer is "the same thing", the check is worthless. Run it against a known-good control, read the log the system is already writing, and prefer the provider's own labels over inference. Alive is not working; green is not proven; running is not writing.

### M-20260726-01: Third variant of the same silent-discard bug — I fixed the instance twice and never built the detector
- **What happened:** Overnight, `cbs_nl` ran 18 hours and completed **3 tables / 61,798 observations**, against 31 tables / 31.4M in its first 4.5. Cause: CBS names its time dimension after what it measures, so `84809NED`'s is **`JaarVanImmigratie`**. The detector matched column names EXACTLY against `("perioden","periods","jaar","period","datum","t_period")`, and `"jaarvanimmigratie" != "jaar"`, so `period_col` was `None`. The row loop then reads `row.get(period_col or "Perioden", "")` — a column that does not exist on that table — fails to date the row, and `continue`s past it. **Every row discarded.** `84809NED` (57,139,992 rows) reached 38,500,000 fetched; `84808NED` (23,253,048) the same. ~59 million rows fetched and thrown away, with nothing on disk but a `.ckpt.json`. It also silently disabled the partitioning added the day before, which requires `period_col`, so those tables were doing the quadratic deep-`$skip` walk as well.
- **Wrong assumption — and this is the real entry:** that fixing an instance closes the class. Yesterday I fixed unparsed period VALUES (`SJ`/`X0`/`YYYYMMDD`, 23 tables at 100% loss) and wrote R55, whose own rule says *"make the discard countable: log rows-in vs observations-out and treat a sustained ratio of zero as a failure."* **I wrote that rule and did not implement it.** So when the same class reappeared one level up — the period COLUMN rather than the period VALUE — there was still no counter, still no log line, and it ran for 18 hours looking healthy: process alive, log scrolling, row counts climbing.
- **How caught:** Ahmed asked "what time was your last action and what is it currently status?" Answering honestly meant comparing tables-completed against uptime, which is the ratio nothing was computing.
- **Also found by auditing the class instead of the instance:** 57 of 64 mid-crawl tables have NO time dimension at all — single-period business-ICT snapshots with 170-217 columns, the period living in table metadata. We were fetching millions of rows from each and discarding 100% of every one.
- **The fix, in three parts:** (1) `_find_period_col` tries exact names, then any column whose NAME suggests a year/period AND whose VALUES actually parse as CBS periods — the value test is what admits `JaarVanImmigratie` while rejecting `Leeftijd` (age) and `MinderDan10VanDeTijd_9` (a measure), which merely contain "tijd"; (2) a table with no period column is now SKIPPED, not crawled — one metadata call instead of millions of wasted rows; (3) **the counter R55 called for**, logged per flush-block and again on completion: `crawled N rows and wrote ZERO observations — this is a DEFECT, not an empty table`. Verified by monkeypatching detection off: broken -> skip before crawling; working -> 3,092 obs. 61 checkpoints reset, because their `pidx` counted partitions of `[None]` and would now skip whole real periods; 3 kept holding genuine data.
- **Rule:** [R58] **A rule you write but do not implement will not save you.** R55 identified this exact failure mode and prescribed the exact counter that would have caught this variant in minutes; writing it in the ledger changed nothing because no code computes it. When a postmortem's remedy is "make X observable", implement X **in the same change** — an unimplemented safeguard is worse than none, because it feels handled. And when you fix a decoding failure, ask what ELSE participates in decoding: this class walked upward from the value (`SJ`) to the exception handler (swallowed) to the column (`JaarVanImmigratie`), and each fix addressed only the layer that happened to be in front of me. For any pipeline that drops records it cannot interpret, the ratio of records-in to records-kept is a first-class health metric, not a debugging afterthought.

### M-20260727-01: I read a stale state row as a run result and over-reported the live count
- **What happened:** Proving a CI batch, I read a per-source status listing and reported `shiller no_change` as a passing result, promoting on that basis and committing a message claiming "live 43 -> 46". The run's own summary said `=== 7 unit(s) processed ===`, not 8. `shiller` is not a registry source at all — it has a fetcher (`updater/strategies/fetchers/shiller.py`) and an ingester but no registry entry, so it cannot run. The line came from `unit_state`, which keeps a row for every source that has EVER run, including ones since de-registered. True live count was **45**.
- **Wrong assumption:** that a status listing describes the run in front of me. It describes accumulated history. `send_digest.py` already handles this correctly — it scopes counts to registered sources and lists orphans separately — so the misleading output was an ad-hoc `unit_state` dump I wrote myself, with no registry filter. I had the correct tool available and used a worse one.
- **How caught:** comparing the listing's 8 lines against the runner's own "7 unit(s) processed".
- **Also found by auditing the class:** 14 fetchers exist with no registry entry (cboe, famafrench, fred_releases, freedomhouse, irena, norgesbank, qog, sdmx_nso, shiller, sipri, tcmb, unsdg, vdem, whr). All have **0 catalog rows**, so this is dead code rather than silently-stale live data, and 4 are correctly gated as RESTRICTED by the licence audit. They are also the source of the orphan `unit_state` rows that polluted the digest.
- **Rule:** [R59] see digest.

### M-20260727-02: "100% complete" measured against the wrong catalog — 31,259 series hosted but invisible
- **What happened:** I reported ksh_stadat complete at "97,520 catalog series = 97,520 R2 objects = 100.00%". That was the LOCAL catalog. The SERVING catalog had 97,297 — 223 short, every one of them holding a real CSV in R2 (294–9,015 bytes). Reconciling all 33 series-level sources then found the same fault far worse elsewhere: **boe 30,674 local / 21 live**, unhcr 18,670/18,367, insee_bdm 101,848/101,768. **31,259 series** were hosted and downloadable by id while absent from `/v1/catalog`. boe had been promoted live and updating daily for weeks with 21 of its series visible.
- **The systemic cause (not a per-source slip):** `core/sync_state_d1.py` syncs the freshness projection after every run and its own docstring states it "never full-dumps the catalog" — right for freshness, but it left NO automatic path for a NEW SERIES to reach D1. The only catalog path was a manual ~945 MB re-dump. Every component reported success at every step, which is exactly why it stayed invisible.
- **The 223 had a perfect signature:** every one contains `..` in its id, and ZERO of the 97,297 present ones do. Rowids interleave, so it was not a batch or timing artifact — it was a real, findable class, and I had dismissed the same 223 earlier in the session as noise.
- **The fix:** `core/sync_catalog_d1.py` (upserts catalog rows + series_fts, reusing the sibling's chunking, replay verification and wrangler execution), the orchestrator records each derived id, and a workflow step drains that queue. Titles for the 223 were regenerated by a rule first checked to reproduce all 97,297 existing titles EXACTLY (100.00%), so no title was invented. Caught while testing: the sibling import fails under `python core/sync_catalog_d1.py`, which is precisely how the workflow invokes it — found only by running the literal command instead of an equivalent import.
- **Rule:** [R60] see digest.

### M-20260727-03: I nearly deleted six live series because a listing did not mention them
- **What happened:** Six defillama `protocol_tvl` series (aave, makerdao, uniswap, compound-finance, pancakeswap, eigenlayer) resolved to zero rows. They were absent from DefiLlama's `/protocols` listing and from our `_catalog_protocols` snapshot, while `lido` and `curve-dex` — the two that DID work — were present. "Upstream retired these slugs, delist them" fit every observation, matched an existing precedent in the repo (`drop_phantoms.sql`), and was endorsed by Ahmed's own rule (host fully or do not list it).
- **It was wrong.** All six return **200** at `/protocol/<slug>`. They are PARENT entities; the crawl iterates the `/protocols` listing, which carries only children (`aave-v1`, `uniswap-v3`, ...), so no amount of crawling could reach them. lido and curve-dex are both parent AND listed child, which is exactly why only those two had data. Fetching the six directly produced 149,325 rows.
- **How caught:** probing each slug against the API before generating the DELETE — done only because the action was destructive.
- **The near-miss is the lesson:** a coherent story, a matching precedent in the codebase, and a project rule that endorsed the deletion. What broke it was one cheap check against the authority, run because deleting is irreversible.
- **Rule:** [R61] see digest.

### M-20260727-04: "Abandoning host" abandoned nothing — a reassuring message hid a 3-hour loop
- **What happened:** istat_sliced ran ~4 hours writing no data while looking healthy (CPU ticking, log scrolling). It was printing `SSL FAIL, abandoning host` and then immediately re-dialling the SAME dead host for the next year — 1990, 1991, 1992, and on. The SSL handler returned fast (a fix I had made earlier the same day) but recorded nothing, so the per-year fallback re-attempted the retired host on every slice. `sdmx.istat.it` 302s to `avvisi.istat.it`, which fails the TLS handshake.
- **Wrong assumption:** that my earlier "abandon the host immediately" change meant the host was abandoned. It meant that ONE request gave up. Abandonment with no state behind it is just a fast failure, and the message made it read as handled.
- **How caught:** an unprompted check of all three crawlers using CPU deltas plus actual data-file writes — the log MTIMEs lie (block-buffered, up to 13 h stale). gus_dbw looked equally silent in the same check and was genuinely healthy; only reading both logs separated the two.
- **The fix:** `_DEAD_HOSTS` records the host and `http_get` short-circuits, placed in the one chokepoint every subdivision path funnels through rather than in the four separate HOSTS loops. Proven with `requests.get` stubbed: a second request to the same host issues ZERO network calls, while a DIFFERENT host is still tried.
- **Rule:** [R62] see digest.

### M-20260727-05: my own budget could not fire, and my first test of it could not fail
- **What happened:** After the dead-host fix istat still made no progress — flow 4 of 3,883, zero parquet, burning 300 s timeouts subdividing one flow. There was no per-flow budget at all, so a single pathological flow could hold the crawler while 3,882 waited behind it. I added a 15-minute budget. Watching the live log then exposed my own bug: the check sat at the ENTRY to `http_get`, before the retry loop, so a call starting one second inside the deadline still ran its full 5 x 300 s ladder. The real bound was budget + retries x timeout = 40 min, not the 15 my commit message asserted.
- **Wrong assumption:** that putting a deadline check "in the request path" bounds the work. A deadline exists for requests that HANG, and a hang is exactly the case where an entry-only check has already been passed. This is R53's shape for the second time in three days.
- **And the test lied too:** to prove the interrupt I patched `M.time.sleep`. `M.time` is the shared `time` module, so my fake request's own `sleep(0.4)` also became a no-op; all 5 attempts finished in 0.00 s, the 0.5 s deadline never arrived, and the test printed FAIL for a reason having nothing to do with the code. Replaced with an injected fake clock: 3 attempts instead of 5, overrun past the deadline down from a possible 1500 s to 248 s.
- **Stated honestly:** 248 s is not zero. A request already in flight finishes its timeout, because a blocking socket read cannot be cancelled without threading it — so the true bound is budget plus one timeout, about 20 minutes, not 15. Budget exhaustion is recorded as DEFERRAL (the flow left untouched and retried next run), never as `unrecoverable`, because a false "unrecoverable" is indistinguishable from a real 404 afterwards.
- **Rule:** [R63], [R64] see digest.
