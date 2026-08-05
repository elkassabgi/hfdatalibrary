# hfdatalibrary / econfindatalibrary — Mistake Ledger

Consult the Rules Digest before consequential operations in this project.
Cross-project lessons live in the mistake-ledger skill's global ledger.

---

## Rules Digest

> **HOW TO USE THIS FILE.** Read THIS DIGEST — the 8,600 lines below it are the archive, not the
> read-path. **Every new entry MUST add a digest line here in the same commit.** On 2026-08-04 I
> wrote 16 entries (R312-R327) and added ZERO digest lines, so the lessons I had just learned were
> invisible to the next session. Ahmed caught it. An archive nobody reads is a diary, not a control.

### ⚠ R0 — THE ONE THAT KEEPS HAPPENING: my measurement's SHAPE, not my question

Six of this session's sixteen entries are the same error wearing different clothes. Before
trusting ANY number I produced, check these five things — each one cost real time:

1. **Compute what the SYSTEM computes, or read its output.** Don't re-implement its rule.
   R318: I widened a health-gate tolerance measured in CALENDAR days; the gate uses BUSINESS days
   and had solved it three weeks earlier. I loosened a working gate on a class that did not exist.
   R327: I called a PROJECTION frontier "staleness" when `health.py:221-223` already separates
   `observed` from `frontier` — in a file I had read twice that night.
2. **Read a long job's ARGV, not its progress — and never pipe it to `tail`.** R323: I watched
   `rekey_eurostat.py --dry-run` for six hours and reported it as the repair; a dry run prints the
   same numbers as a real one. R336: `cmd | tail -18` shows NOTHING until the process exits, so a
   healthy job looks stalled — I invented a mechanism for the silence and killed the job, twice in
   one session. If a job is silent, suspect the plumbing before the program.
3. **A sweep reports TWO numbers — what it found and what it could not reach. No denominator, no
   result.** R315: a 337-request census on a URL I had already watched 404; then a rerun where 192
   of 337 were 429s, making its table a lower bound, not a census. Read the failure count first.
   R330: all three re-pull tools pointed at `D:/research/econfindatalibrary`, a drive letter the
   store left in the cutover; `os.path.isdir` False reads as "this source has no data", so the
   authoritative repair tool printed `0 corrupt` across nine sources while the store held 637,178
   bad rows. **"0 defects in 0 files examined" is not a result** — and a hardcoded path is the
   most common way to get one.
4. **When a probe reports ABSENCE, run it against something known PRESENT — and a FAILED control
   VOIDS the run.** R316: I said a source was missing from a list I had parsed with the wrong
   field name — every element was `None`, so EVERY source would have read as missing. R329: three
   probes on one question, each reporting the damage as smaller or unreachable, each wrong; on the
   third I *did* run a control, watched it fail, and published the numbers anyway. A failed control
   is not a caveat to report alongside the result — it means there is no result. Note the tell:
   all three errors pointed the same way, because a probe built hoping for absence gets believed
   the moment it reports absence. **Never regex a language whose comments can contain the
   delimiter** — a quoted phrase inside a `//` comment flips quote-pairing parity and silently
   drops real entries. Strip comments first, or ask the system for its own answer.
   **R338 is R316 again, with this rule already written here.** Checking whether ksh/zillow were
   still served before dropping 25,109 catalogue rows, I keyed `/v1/sources` on `id` when the
   payload uses `source` — so every source read as absent, including `penn_world_table`, verified
   live the day before. The rule held only because I happened to pad the probe with a known-live
   control; with just the two ids I was hoping were absent, I would have deleted on a clean-looking
   confirmation. Knowing this rule is not the same as instrumenting it: **put the control IN the
   probe list, every time, and make it one you would bet on.**
5. **A one-sided test on a two-sided failure gives a number that LOOKS like a measurement.**
   R322: "273,980 fabricated rows" was under half — the audit only tested the future, and a
   counter-as-year starts at 1. The real figure was ~637,000 across seven sources.
6. **Two writers, one data store: state is a MERGE, not a freshness comparison.** R340: CI state
   and workstation state are separate LINEAGES, not a stale copy of each other — local was ahead
   on ofr (proved by the R2 row count matching local's obs_count exactly), CI was ahead on three
   later runs local has never seen. `--push-state` would have "fixed" it by discarding CI's runs,
   causing the lost update I was hunting. Diff BOTH directions with counts before reconciling, and
   always name WHICH database you are holding.
7. **Re-running the same query is REPRODUCTION, not verification — check with a different
   instrument.** R342: I called an audit's `abs: 376,332,763` impossible on a bytes-per-key
   estimate, then "confirmed" my doubt by re-running the audit's own DuckDB query and printing
   "matches: True". That proves determinism and nothing else. The real check was a different
   method — parquet footer metadata, no scan — which matched the row count exactly (976,632,535,
   ratio 1.0x) and showed my plausibility argument was wrong. Say which of the two you actually
   established: I verified the ROWS, never the distinct count. A plausibility argument is a reason
   to measure, never a result.
8. **To measure completeness, enumerate from the side that can be OVER-complete.** R341: I closed
   noaa's re-derive on "400/400 present" — a sample drawn from the catalogue, which cannot see the
   1,998 series the store had gained since. The sampling frame WAS the thing with the hole in it.
   The reported "missing 1,943" was the NET of 1,998 uncatalogued and 55 sidecar-omitted: two
   defects with opposite fixes, cancelled into one figure matching neither. Split every difference
   into both directions before acting, and note the obvious remedy here (re-run the cataloguer)
   would have created 1,998 listed-but-404 rows.
9. **A reserved task reserves what its TEXT says, not its prefix — and "the queue is empty" is a
   claim that needs the same proof as any other.** R343: I filed all 36 not-scheduled `imf_*`
   sources under #46, whose title reads "the 8 served imf_* sources"; the reservation is about
   RE-KEYING old ids, not about building `_direct` successors, which I had finished for the FSI
   trio an hour earlier. 1,093,077 series went into the nothing-to-do pile on a prefix match.
   Checked against IMF's own catalogue: 6 exact id matches, 3 built and pushed the same session.
   Corollary: a category you invent ("frozen" = no _direct counterpart) is a hypothesis — test it
   against the system's list, or you will nearly duplicate a source that already auto-updates.
10. **Check that your evidence POSTDATES the fix before calling the fix a failure.** R339: I said
   stat_estonia's 18-minute deadline "is NOT working" and cited three 45-minute kills — all of
   them produced by code committed BEFORE the two commits that fixed it. The current cap had never
   run once. "Still broken" and "never tried" look identical in a table of past runs, and only one
   is a reason to write code. Compare the newest run timestamp to `git log` on the file under
   test, and state both.

11. **When a component has a fallback, plausible output proves NOTHING — it may be the fallback's.**
   R344: an ingest recorded the authoritative key order in a sidecar, but wrote it one directory
   too shallow AND read it with `open()` while the store lives in R2. Two breaks, zero symptoms:
   the reader silently guessed the order instead, and the guess was usually right. It was never
   once used. The failure only surfaced on the one source where guessing is impossible — after
   260,931 series had shipped with raw keys for titles and 27,094 live ones carried a
   mislabelled dimension. Prove the PRIMARY path by its own evidence — assert the reader RETURNS
