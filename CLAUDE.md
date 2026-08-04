# hfdatalibrary — project instructions

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

`.claude/MISTAKES.md` is the append-only ledger (150+ entries). Its Rules Digest (top of file)
is the distilled checklist — read it at session start and before any consequential operation.
