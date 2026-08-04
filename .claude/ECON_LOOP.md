# The econ-updater systematic loop (v2 — created 2026-08-04 at Ahmed's direction)

This file is the CANONICAL loop prompt. The cron job carries a copy; if the cron has expired
(session crons auto-expire after 7 days), re-arm by pasting the block below into `/loop 15m`,
or ask Claude to re-create the cron from this file.

---

ECON-UPDATER SYSTEMATIC LOOP. Work econdatalibrary (E:\research\econfindatalibrary)
database-by-database until 100% of served sources auto-update and stay green — WITHOUT
breaking anything that works.

MANDATORY EVERY CYCLE, IN ORDER:
1. Invoke the econ-updater skill (Skill tool). If its content is not fully in context, load it
   completely and prove it:
   node "C:/Users/aelkassabgi/.claude/skills/read-session-log/driver.mjs" skill "E:/research/econfindatalibrary/.claude/skills/econ-updater"
   and do not proceed until skill-verify prints ALL PASS.
2. Choose exactly ONE source: the in-progress source if one exists (FINISH IT FIRST), else the
   top ACTIONABLE item in the skill's references/50-queue.md, else the reddest genuinely-failing
   source in the latest updater-daily run (stale verdicts are not failures — check the evidence
   postdates the last fix).
3. BEFORE any edit for that source, do the 5 reads: docs/runbook/<source>.md · grep
   D:\research\hfdatalibrary\.claude\MISTAKES.md for the source id · the fetcher header · the
   registry.yaml entry · the licence verdict in DATABASE_LICENSES_VERBATIM.md.
4. Work that source END-TO-END per the skill's per-source procedure. It is DONE only when the
   Definition of Done table in SKILL.md is fully proven — including `npx wrangler deploy` and a
   LIVE /v1/sources check whenever the serving surface changed.
5. Regenerate its runbook page (tools/gen_runbook.py), commit AND push, and only then pick the
   next source.

HARD RULES (each has burned days; hooks and CI enforce the first two mechanically):
- db.nomics.world is BANNED — no fetching, probing, relays, mirrors (R251).
- Any registry.yaml entry add/remove bumps config.EXPECTED_SOURCE_COUNT in the SAME commit (R347).
- One source at a time. Shared infrastructure (fetchers/_common.py, _giant.py, orchestrate.py,
  merge, core/, api/worker/) changes ONLY when the current source strictly requires it, the full
  suite passes, and the change ships with a test.
- "Live"/"served" claims require the RUNNING system as evidence, never a file just edited (R345).
- Record every mistake in .claude/MISTAKES.md immediately; append at the anchor, verify the
  count (R247).

REPORT progress only as "N of M sources / X of Y series scheduled" (from
tools/audit_schedule_coverage.py) plus the one source completed this cycle.

RESERVED for Ahmed (stop, write .claude/STOP_REASON, ask): deleting data that is not
re-crawlable · un-gating a DISPUTED licence · re-keying/retiring SERVED series ids · switching
a source to a feed serving LESS · auth/security/billing · sending email as Ahmed.
