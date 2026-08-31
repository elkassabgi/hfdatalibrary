# hfdatalibrary — project instructions

## FIRST: the adversarial-review skill is MANDATORY for every consequential action

Standing order from Ahmed (2026-08-24): *"i feel like you are repeating mistakes from the past
... the adversarial will be your check and balance ... create this as a skill to be permanent for
this database as you are going in circles."*

### PARALLEL, AND FOR EVERYTHING — Ahmed, 2026-08-29

*"You need to run a parallel adversarial for everything you do."*

This SUPERSEDES the narrower trigger list below. The review is no longer a gate you reach at
the end of a piece of work; it runs ALONGSIDE it. Launch the reviewer (background subagent) as
soon as the shape of the work is known — with the brief, the plan and the evidence so far — and
keep building while it hunts. Do not wait for it to finish before continuing, and do not ship
before reading its verdict.

Why he asked for it, from this week alone: three consecutive reviews caught things that had
already passed my own checks — R500 (an authorisation spent on a population I had not
re-measured), R501 (a guard that tested uniqueness while the risk was coverage), R503 (that
guard's own except-branch failing open, plus 863,253 rows deleted outside what was approved).
Serial review found each of them LATE, after the work was built. Parallel review costs nothing
extra in wall-clock and finds them while the design is still cheap to change.

What "everything" means in practice: any change to code, data, catalogue, serving surface or
configuration; any measurement whose number will be reported; any claim about the running
system. Reading, exploring and answering a question do not need one. When in doubt, launch it —
an unnecessary reviewer costs a few minutes; a missed one costs what R500-R503 cost.

Invoke the **adversarial-review** skill (`.claude/skills/adversarial-review/`) BEFORE and AFTER
any delete, purge, deploy, un-gate, licence decision, bulk write, or any claim that something is
*live*, *served*, *complete*, *verified*, or *not provided by the publisher*.

The protocol is brief → challenge → do → verify → record. The reviewer is a SUBAGENT, it is told
to find the flaw rather than approve, and on a FAIL it writes the MISTAKES.md entry itself.
Mechanical checks live in `.claude/skills/adversarial-review/tools/ledger_check.py` — run them,
do not read them and assume.

Why this outranks everything below it: in a single session I hit the same reader-bug class five
times (R478, R483, R484), purged 384 series from three of the four places they live (R481), and
wrote nine ledger entries with zero digest lines (R485) — which is verbatim the failure the
digest's own header already records, where it says "Ahmed caught it." Every one of those had a
written rule. **Prose rules did not hold; a second agent and a runnable check are the difference.**

## CLOUDFLARE HOSTS, THE DESKTOP COMPUTES — Ahmed, 2026-08-29

*"i can host the data on cloudflare but do the updating and functions on the desktop."*

Exploration, sizing, auditing, counting — anything whose answer only informs a DECISION —
runs against the LOCAL `catalog.db` (11.91 GB; it carries `series` AND the full `series_fts`
index, 13,486,342 rows each). D1 is touched only to serve users, to apply a write, and to
VERIFY the user-facing state afterwards — small, targeted, never a scan.

Why: August billed ~$200 in D1 reads, 87% of it on two days, and those two days were OUR
catalogue maintenance, not users. The identical query runs free on this machine.

The verification half is not optional (R60/R107/R116): local and D1 can disagree, so a claim
about what users see still comes from D1 or the served file. DECIDE LOCALLY, VERIFY REMOTELY.

Full decision record with the measurements: `.claude/DESKTOP_FIRST.md`.

## COST: Ahmed pays this bill, and twice it has been my fault

Ahmed, 2026-08-25: *"This project has already cost me a month over $100 and a second month over
$200 because of your mistakes. I need a safetyguard to stop these mistakes from happening. I
cant afford this I will go bankrupt."*

**Before ANY batch of D1 statements, run ONE of them as a `SELECT` and read `meta.rows_read`,
then multiply by the statement count.** One query, ~13 seconds. Without it I planned 164,705
statements that would have read **3.93 trillion rows** — ~24 days and ~$2,500 (R492) — and
estimated it at 90 minutes from the file count.

The trap is that `series_fts` is `fts5(series_id UNINDEXED, …)`, so **`WHERE series_id = ?` has
no index and every statement is a full scan of ~23.8M rows**. An `IN` list of 200 ids costs the
same as one id: **the cost is per statement, so raise predicate arity, never add statements.**
`series` by its primary key is an index seek (1 row) and is free by comparison.

Two mechanisms enforce this, because prose did not:

- `.claude/hooks/d1_cost_guard.py` — PreToolUse. Counts full-table scans and **refuses** past
  15/hour or 40/day (~$0.95/day). `MATCH`, `series`-by-PK, `source_counts` and `d1 insights`
  are free and uncounted. Fails OPEN. Override deliberately with `ALLOW_D1_SCAN_BUDGET=<n>`.
- `.claude/hooks/cost_banner.py` — SessionStart. Shows the running total, free and instant.

Run `python tools/billing_guard.py` in the econ repo for the real meter, and remember it
extrapolates ONE 24-hour window ×30 — a day of diagnostics reads as a monthly cost.

## econdatalibrary work: the econ-updater skill is MANDATORY

Standing order from Ahmed (2026-08-04, after 5 weeks of circular regressions on the update
system): **any** work touching econdatalibrary (`E:\research\econfindatalibrary` — its updater,
fetchers, catalog, D1, worker, or serving surface) MUST begin by invoking the **econ-updater**
skill and following it. No exceptions, no "quick fixes" outside it.

The skill's canonical content lives in the econ repo itself
(`E:\research\econfindatalibrary\.claude\skills\econ-updater\`) so it is versioned with the code
it describes. Load it COMPLETELY (SKILL.md + every references/*.md) — a skim is how five weeks
were lost:

```bash
node "C:/Users/aelkassabgi/.claude/skills/read-session-log/driver.mjs" skill "E:/research/econfindatalibrary/.claude/skills/econ-updater"
```

and do not proceed until `skill-verify` prints ALL PASS.

Non-negotiables enforced by hooks and CI (do not rely on memory — they are mechanical):

- **db.nomics.world is BANNED** (econ CLAUDE.md §0, ledger R251). A PreToolUse hook in
  `.claude/settings.json` denies any command reaching the domain; a CI test in the econ repo
  fails any push that reintroduces it.
- **One source at a time, end-to-end.** Before touching source X: read
  `E:\...\docs\runbook\X.md`, grep `.claude/MISTAKES.md` here for `X`, read X's fetcher header,
  its registry entry, and its licence verdict in `DATABASE_LICENSES_VERBATIM.md`.
- **A registry.yaml entry change bumps `config.EXPECTED_SOURCE_COUNT` in the same commit**
  (R347 — the updater refuses ALL runs on mismatch; `tests/test_registry_count_guard.py`
  enforces it in CI).
- **"Live"/"served" claims require the running system as evidence** — `npx wrangler deploy` is
  manual, nothing auto-deploys the worker, and a util.ts edit alone changes nothing a user can
  reach (R345).
- **Record every mistake in `.claude/MISTAKES.md` immediately** (append at the anchor, verify
  the entry count — R247). Never a separate incident doc.

## Mistake ledger

`.claude/MISTAKES.md` is the append-only ledger (**483 distinct rule ids**, R1–R520 with 37 gaps,
measured 2026-08-30 — "R520" is the highest id, not the count). Its Rules Digest (top of file)
is the distilled checklist — read it at session start and before any consequential operation.
