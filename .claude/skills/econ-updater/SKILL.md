---
name: econ-updater
description: MANDATORY before ALL econdatalibrary updater/serving work. This is a loader — the canonical skill is versioned inside the econ repo itself; load it COMPLETELY, then follow it.
---

# econ-updater (loader)

The canonical skill lives **inside the econ repo**, versioned with the code it describes:

    E:\research\econfindatalibrary\.claude\skills\econ-updater\

This loader exists because sessions run from `D:\research\hfdatalibrary` (where skills are
discovered) while the work happens in E:. Do NOT work from this stub alone — it contains none
of the rules. Load the canonical skill completely, references included, and prove it:

```bash
node "C:/Users/aelkassabgi/.claude/skills/read-session-log/driver.mjs" skill "E:/research/econfindatalibrary/.claude/skills/econ-updater"
```

Then fill the reading ledger it writes and run:

```bash
node "C:/Users/aelkassabgi/.claude/skills/read-session-log/driver.mjs" skill-verify "E:/research/econfindatalibrary/.claude/skills/econ-updater"
```

**Only when it prints ALL PASS** may econdatalibrary work begin. A skim of SKILL.md without
the references is how five weeks were lost — the references hold the rulebook, the per-source
procedure, the serving pipeline commands, the per-source landmines, and the work queue.

Fast orientation (not a substitute): the ten non-negotiables are in the canonical SKILL.md;
the ban on db.nomics.world is enforced by a PreToolUse hook here and by CI tests in the econ
repo; `partial` never sets last_success; nothing auto-deploys the worker; one source at a
time, end-to-end.
