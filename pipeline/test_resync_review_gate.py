"""The --apply gate and the driver's sibling/recode logic, as real pytest tests.

R757 #6: the previous version of this file had a `main()` and no `test_` function, so `pytest pipeline/`
collected six items, none of them from here — "6 passed" said nothing about the gate, and there was no
test at all over `_sibling_drift` or the exit-code recode. That absence is exactly why R757 #1 shipped:
a scoping change silently un-did R754 #5 and only an adversarial reviewer caught it.

Every shape that defeated an earlier version of the gate is kept here. A return to inference lights
them all up at once.
"""
from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import resync_variables as rv          # noqa: E402
import seam_rebase_batch as srb        # noqa: E402

SHA = rv._SOURCE_SHA[:12]
ID = "AR-038"
GOOD = f"APPROVE-APPLY resync_variables.py {SHA} {ID}"


@pytest.fixture
def passed(tmp_path):
    """Write PASSED.md content and ask the gate about it."""
    p = tmp_path / "PASSED.md"

    def ask(content, review_id=ID):
        p.write_text(content, encoding="utf-8")
        return rv.reviewed_ok(review_id, str(p))

    return ask


# ---------------------------------------------------------------- the token itself

def test_the_exact_token_authorises(passed):
    assert passed(GOOD) is True


def test_a_harmless_trailing_comment_is_fine(passed):
    assert passed(GOOD + "   # cleared after the 14th review") is True


@pytest.mark.parametrize("line,why", [
    (f"APPROVE-APPLY resync_variables.py deadbeefdead {ID}", "wrong hash"),
    (f"APPROVE-APPLY seam_rebase.py {SHA} {ID}", "wrong tool"),
    (f"APPROVE-APPLY resync_variables.py {SHA} AR-999", "wrong id"),
    (f"APPROVE_APPLY resync_variables.py {SHA} {ID}", "token misspelled"),
    (f"# APPROVE-APPLY resync_variables.py {SHA} {ID}", "commented out"),
    (f"NOT APPROVE-APPLY resync_variables.py {SHA} {ID}", "negated on the line"),
    (f"the reviewer wrote APPROVE-APPLY resync_variables.py {SHA} {ID} then withdrew it", "in prose"),
    ("", "empty"),
])
def test_near_misses_refuse(passed, line, why):
    assert passed(line) is False, why


@pytest.mark.parametrize("indent", ["  ", "\t", "    "])
def test_an_indented_token_refuses(passed, indent):
    """R757 #3: the refusal message printed the required line indented, so pasting the tool's own
    transcript into PASSED.md authorised it. A real approval sits at column 0."""
    assert passed(indent + GOOD) is False


def test_the_refusal_message_cannot_itself_authorise(passed, capsys):
    """Print the refusal, paste every line of it into PASSED.md, and it must still refuse."""
    assert passed("nothing here") is False
    out = capsys.readouterr().out
    assert passed(out) is False, "the tool's own refusal transcript authorised it"


# ---------------------------------------------------------------- what defeated the inference gate

@pytest.mark.parametrize("row,why", [
    (f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASS |", "R754: a PASS verdict row"),
    (f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | FAIL | PASS |", "R755: FAIL beside PASS"),
    (f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASS - with changes |", "R755: repunctuated"),
    (f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASSED |", "R754: PASSED contains PASS"),
    (f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} (SUPERSEDED) | PASS |", "R755: superseded"),
])
def test_no_prose_row_authorises(passed, row, why):
    assert passed(row) is False, why


# ---------------------------------------------------------------- revocation

def test_a_revoke_line_withdraws_an_approval(passed):
    assert passed(GOOD + "\n" + f"REVOKE-APPLY resync_variables.py {SHA} {ID}") is False


def test_a_wildcard_revoke_withdraws_it(passed):
    assert passed(GOOD + "\n" + "REVOKE-APPLY resync_variables.py * " + ID) is False


def test_a_revoking_comment_on_the_token_withdraws_it(passed):
    assert passed(GOOD + "  # REVOKED, do not use") is False


# ---------------------------------------------------------------- ids and the file

@pytest.mark.parametrize("bad", ["", "   ", "PASS", "APPROVE-APPLY", "resync_variables", "-", "A", "AR-0", "*"])
def test_malformed_ids_refuse(passed, bad):
    assert passed(GOOD, review_id=bad) is False


@pytest.mark.parametrize("ok_id", [ID + " ", " " + ID, f"  {ID}  "])
def test_the_same_id_with_stray_whitespace_still_works(passed, ok_id):
    assert passed(GOOD, review_id=ok_id) is True


def test_a_missing_file_refuses_without_raising(tmp_path):
    assert rv.reviewed_ok(ID, str(tmp_path / "nope.md")) is False


# ---------------------------------------------------------------- the driver's sibling check

def _reading(**over):
    """A complete, clean sibling line for the guarded set, with overrides."""
    vals = {n[:-3]: f"{i:012x}" for i, n in enumerate(srb.GUARDED, start=1)}
    vals.update(over)
    return "  imported module sha256: " + ", ".join(f"{k} {v}" for k, v in vals.items())


def _shas():
    return {n: f"{i:012x}" + "0" * 52 for i, n in enumerate(srb.GUARDED, start=1)}


def test_a_clean_reading_shows_no_drift():
    assert srb._sibling_drift(_reading(), _shas()) is None


def test_a_missing_sibling_line_is_a_breach():
    assert srb._sibling_drift("no such line here", _shas())


def test_a_reading_that_identifies_one_module_is_a_breach():
    """R757 #2: refusing only an EMPTY parse let 'seam_rebase <hash>, HASHES-UNAVAILABLE ...' pass."""
    out = "  imported module sha256: seam_rebase 000000000001, HASHES-UNAVAILABLE the child could not hash the rest"
    assert srb._sibling_drift(out, _shas())


def test_all_not_imported_is_a_breach():
    assert srb._sibling_drift(_reading(**{n[:-3]: "not-imported" for n in srb.GUARDED}), _shas())


def test_an_unreadable_module_is_a_breach():
    assert srb._sibling_drift(_reading(aggregate="unreadable"), _shas())


def test_header_and_trailer_disagreement_is_a_breach():
    out = _reading() + "\n" + _reading(aggregate="999999999999").replace("sha256:", "sha256 at exit:")
    assert srb._sibling_drift(out, _shas())


def test_a_drifted_middle_reading_is_a_breach():
    """R755 #5: taking only the first and last readings missed a drift between them."""
    out = "\n".join([_reading(), _reading(aggregate="999999999999"), _reading()])
    assert srb._sibling_drift(out, _shas())


@pytest.mark.parametrize("order", ["bad_first", "good_first"])
def test_naming_a_module_twice_is_a_breach_in_either_order(order):
    """R760 #1, the worst defeat found so far: `parse` built a dict, so a child could print a guarded
    module TWICE - the drifted value and then the driver's value - and the last write won. The drift
    vanished, the ticker was logged 0, and the next start skipped it forever. A dict cannot represent
    "said twice", so the parser now returns the ordered names too."""
    good, bad = _shas()["aggregate.py"][:12], "999999999999"
    pair = (bad, good) if order == "bad_first" else (good, bad)
    line = _reading().replace(f"aggregate {good}", f"aggregate {pair[0]}, aggregate {pair[1]}")
    assert srb._sibling_drift(line, _shas()), f"a duplicate name passed with {order}"


def test_a_one_character_hash_is_a_breach():
    """R760 #2: the compare was `parent.startswith(short)`, so ANY prefix matched - a child printing
    a single character as its hash was clean every time."""
    assert srb._sibling_drift(_reading(aggregate="0"), _shas())


def test_a_prefix_of_the_real_hash_is_still_a_breach():
    """The same defect's sharpest form: a genuine prefix of the driver's own hash."""
    real = _shas()["aggregate.py"][:12]
    assert srb._sibling_drift(_reading(aggregate=real[:6]), _shas())


def test_a_part_naming_no_guarded_module_is_a_breach():
    """R760 #1's rider: `malformed` tested arity only, so a well-formed pair naming something outside
    the guarded set passed - the check must know WHICH modules it verified."""
    assert srb._sibling_drift(_reading() + ", something_else 0123456789ab", _shas())


def test_a_non_hex_value_is_a_breach():
    assert srb._sibling_drift(_reading(aggregate="zzzzzzzzzzzz"), _shas())


# ---------------------------------------------------------------- revocation must fail OPPOSITE to approval

@pytest.mark.parametrize("line", [
    "    REVOKE-APPLY resync_variables.py {sha} {rid}",
    "\tREVOKE-APPLY resync_variables.py {sha} {rid}",
    "REVOKE-APPLY  resync_variables.py  {sha}  {rid}",
    "see below: REVOKE-APPLY resync_variables.py {sha} {rid}",
    "    REVOKE-APPLY resync_variables.py * {rid}",
    "REVOKE-APPLY resync_variables.py {sha} *",
])
def test_a_revocation_is_honoured_however_it_is_written(passed, line):
    """R760 #2: approval is anchored at column 0 and fails CLOSED, which is right. The revoke was
    anchored the same way and so failed OPEN - an indented one, or one with a double space, was
    silently ignored and the approval it withdrew still stood."""
    assert passed(GOOD + "\n" + line.format(sha=SHA, rid=ID)) is False


@pytest.mark.parametrize("word", [
    "REVOKED", "revoking this approval", "supersede this", "supersedes AR-037", "superseded",
    "withdrawn", "rescinded", "cancelled", "obsolete", "expired", "invalid", "NOT VALID",
    "no longer applies", "do  not  use", "don't use",
])
def test_a_withdrawing_comment_on_the_token_refuses(passed, word):
    assert passed(f"{GOOD}  # {word}") is False, word


def test_an_innocent_comment_still_authorises(passed):
    """The word list must not swallow ordinary notes - a gate that refuses everything is not a gate."""
    assert passed(GOOD + "  # cleared; avoid rerunning during the daily window") is True


@pytest.mark.parametrize("shape,line", [
    ("path prefix",       f"REVOKE-APPLY pipeline/resync_variables.py {SHA} {ID}"),
    ("dot-slash prefix",  f"REVOKE-APPLY ./resync_variables.py {SHA} {ID}"),
    ("absolute path",     f"REVOKE-APPLY D:/x/pipeline/resync_variables.py {SHA} {ID}"),
    ("bold filename",     f"REVOKE-APPLY **resync_variables.py** {SHA} {ID}"),
    ("backticked file",   f"REVOKE-APPLY `resync_variables.py` {SHA} {ID}"),
    ("colon after token", f"REVOKE-APPLY: resync_variables.py {SHA} {ID}"),
    ("word between",      f"REVOKE-APPLY for resync_variables.py {SHA} {ID}"),
    ("no .py",            f"REVOKE-APPLY resync_variables {SHA} {ID}"),
    ("bolded token",      f"**REVOKE-APPLY** resync_variables.py {SHA} {ID}"),
    ("wrapped over two",  f"REVOKE-APPLY\n    resync_variables.py {SHA} {ID}"),
    ("misspelled file",   f"REVOKE-APPLY resync_varibles.py {SHA} {ID}"),
])
def test_no_shape_of_revocation_is_silently_ignored(passed, shape, line):
    """R765 #1, and this test REPLACES one that pinned the hole as intended.

    The previous version asserted `is True` for the misspelled-filename shape and called it a stated
    limit. It was not a limit, it was a fail-open with eleven members: narrowing the mention scan to
    `REVOKE-APPLY\\s+resync_variables\\.py` dropped every shape below with no diagnostic, including
    `REVOKE-APPLY pipeline/resync_variables.py ...` - the exact string PASSED.md used to name the
    tool. Pinning a fail-open as intended is worse than the fail-open, because the next author who
    fixes it breaks a test.

    A revocation fails OPEN by design: anything that mentions one and does not parse must refuse."""
    assert passed(GOOD + "\n" + line) is False, shape


def test_a_revocation_for_a_DIFFERENT_id_does_not_block_this_one(passed):
    """The other side of the asymmetry: it parses, names another review, and must be ignored."""
    assert passed(GOOD + f"\nREVOKE-APPLY resync_variables.py {SHA} AR-999") is True


# ---------------------------------------------------------------- documentation is not a decision

def test_a_fenced_approve_example_neither_authorises_nor_refuses(passed):
    """The fix for R760 #2 made the revoke matcher permissive, and the same commit documented the
    syntax IN PASSED.md - so the file's own examples made the tool refuse every run (measured
    2026-09-05T20:51Z). The APPROVE path skips fences, so its example is inert. The revoke example
    cannot rely on that, because the revoke path reads fences too - it uses a placeholder tool name
    instead, which is what `test_the_documentation_placeholder_is_inert` covers."""
    doc = ("Some prose.\n```\nAPPROVE-APPLY resync_variables.py <sha12> <id>\n```\n" + GOOD)
    assert passed(doc) is True, "a fenced approve example must not refuse a genuine approval"


def test_a_fenced_real_token_does_not_authorise(passed):
    """The other half, and the last of R757 #3's class: a column-0 token INSIDE a fence is a quotation."""
    assert passed("```\n" + GOOD + "\n```") is False


def test_an_inline_code_token_does_not_authorise(passed):
    assert passed(f"`{GOOD}`") is False


@pytest.mark.parametrize("wrap", [
    "```\n{r}\n```",                       # a plain fence
    "```python\n{r}\n```",                 # a fence with an info string
    "~~~\n{r}\n~~~",                       # the tilde form
    "````\n{r}\n````",                     # four backticks
    "```\nunclosed fence, then:\n{r}",     # an unclosed fence
    "```\na\n```\n```\nb\n```\n```\n{r}",  # an odd number of fence markers
    "`{r}`",                               # inline code
    "text before `x` then {r}",            # after an inline span on the same line
])
def test_a_revocation_is_never_hidden_by_quoting(passed, wrap):
    """R763 #2, and it was MY regression. The first fence fix skipped fenced and inline-code regions
    before ANY matching, which blinded the revoke path in eight shapes: a genuine revocation inside a
    fence was silently ignored and the approval it withdrew still stood. Approval fails CLOSED and so
    ignores quotation; revocation fails OPEN and so reads every line."""
    assert passed(GOOD + "\n" + wrap.format(r=f"REVOKE-APPLY resync_variables.py {SHA} {ID}")) is False


def test_even_a_PLACEHOLDER_revoke_example_now_refuses(passed):
    """R765 #1's resolution, and it reverses this test's previous assertion on purpose.

    The placeholder `<tool>.py` was the second attempt at letting PASSED.md document the syntax. It
    only worked because the mention scan had been narrowed to lines naming this tool - and that
    narrowing dropped eleven shapes of GENUINE revocation. Given a choice between "documentation can
    live in the decision file" and "no withdrawal is ever silently ignored", the second wins.

    So the scan is broad again and PASSED.md carries NO example tokens at all; the syntax moved to
    the skill's SKILL.md, which this tool never opens. A revoke example written into PASSED.md now
    refuses, loudly, which is the correct answer to someone putting an example in the decision file."""
    doc = "```\nREVOKE-APPLY <tool>.py <sha12> <review id>\n```\n" + GOOD
    assert passed(doc) is False


def test_a_mistyped_revocation_naming_the_tool_still_refuses_loudly(passed, capsys):
    assert passed(GOOD + "\nREVOKE-APPLY resync_variables.py nonsense") is False
    assert "REVOKE-APPLY" in capsys.readouterr().out


def test_the_projects_own_passed_file_does_not_refuse(tmp_path):
    """The live PASSED.md must not, by documenting the syntax, refuse every run. Hermetic: R763 #5
    noted the previous version hard-coded an absolute path to a gitignored file, so it skipped on
    every other machine and in CI. This rebuilds that file's SHAPE from its own documented examples."""
    p = tmp_path / "PASSED.md"
    p.write_text(
        "# Adversarial review - pass log\n\n"
        "To authorise, write at column 0:\n\n"
        "```\nAPPROVE-APPLY resync_variables.py <sha12> <review id>\n```\n\n"
        "The syntax for granting and withdrawing lives in SKILL.md, not here, because this file's\n"
        "withdrawal scanner reads every line and would read an example as a real attempt.\n\n"
        "| date | id | verdict |\n|---|---|---|\n| 2026-09-05 | AR-001 | PASS |\n",
        encoding="utf-8")
    import io
    buf = io.StringIO()
    real, rv._say = rv._say, lambda s: buf.write(s + "\n")
    try:
        assert rv.reviewed_ok(ID, str(p)) is False
    finally:
        rv._say = real
    out = buf.getvalue()
    assert "REVOKE" not in out.upper(), "the file's own documentation is making the gate refuse:\n" + out
    assert "carries no approval line" in out, out


# ---------------------------------------------------------------- the LOGGED code, end to end (R767 #3)
#
# Everything above tests the driver's parts. The number that actually matters is the one written to
# the batch log, because the next start reads it: a 0 marks the ticker done forever. Those assertions
# lived ONLY in drv58/run_harness.sh, a bash script outside the repo, so `pytest pipeline/` could and
# did stay green at 127 passed while a one-line change collapsed 17 of 20 breach scenarios from 4 to
# 0 (R767 #1). These tests drive the REAL driver against generated stand-ins and assert the logged
# code, so that failure mode is now visible to the runner everyone actually runs.

_SIB = ", ".join(f"{n[:-3]} {{{n[:-3]}}}" for n in srb.GUARDED)


def _standin(body: str) -> str:
    """A stand-in seam_rebase.py. It prints the real tool's header and sibling grammar, then does
    whatever `body` says. Hashes are computed at run time from the sibling modules beside it."""
    return (
        "import hashlib, os, sys\n"
        "SIBLINGS = " + repr(tuple(n[:-3] for n in srb.GUARDED)) + "\n"
        "HERE = os.path.dirname(os.path.abspath(__file__))\n"
        "def h(n):\n"
        "    with open(os.path.join(HERE, n + '.py'), 'rb') as f:\n"
        "        return hashlib.sha256(f.read()).hexdigest()[:12]\n"
        "def _v(n, over):\n"
        "    return over.get(n, 'not-imported' if n == 'seam_rebase' else h(n))\n"
        "def sibs(drop=(), dup=(), extra='', **over):\n"
        "    parts = [f'{n} ' + _v(n, over) for n in SIBLINGS if n not in drop]\n"
        "    parts += [f'{n} ' + _v(n, over) for n in dup]\n"
        "    if extra:\n"
        "        parts.append(extra)\n"
        "    return ', '.join(parts)\n"
        "own = hashlib.sha256(open(os.path.abspath(__file__),'rb').read()).hexdigest()\n"
        "snap = sys.argv[sys.argv.index('--snapshot-dir') + 1]\n"
        + body)


_SCENARIOS = {
    # name: (stand-in body, the code the DRIVER must write to the log)
    "clean": ("print(f'  tool source sha256 {own} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n"
              "print('  imported module sha256: ' + sibs(), flush=True)\n"
              "os.makedirs(snap, exist_ok=True)\n"
              "open(os.path.join(snap,'_RESULT.txt'),'a').write(f'x\\tpid={os.getpid()}\\tEXIT 0 DONE rebased (split)\\n')\n"
              "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
              "print('  DONE: rebased (split)', flush=True)\n"
              "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
              "sys.exit(0)\n", 0),
    "drift": ("print(f'  tool source sha256 {own} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n"
              "print('  imported module sha256: ' + sibs(), flush=True)\n"
              "os.makedirs(snap, exist_ok=True)\n"
              "open(os.path.join(snap,'_RESULT.txt'),'a').write(f'x\\tpid={os.getpid()}\\tEXIT 0 DONE rebased (split)\\n')\n"
              "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
              "print('  DONE: rebased (split)', flush=True)\n"
              "print('  imported module sha256 at exit: ' + sibs(aggregate='9'*12), flush=True)\n"
              "sys.exit(0)\n", 4),
    "no_header": ("print('  nothing to rebase in --mode split (P_int=1)', flush=True)\n"
                  "sys.exit(0)\n", 4),
    "clean_abort": ("print(f'  tool source sha256 {own} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n"
                    "print('  imported module sha256: ' + sibs(), flush=True)\n"
                    "os.makedirs(snap, exist_ok=True)\n"
                    "open(os.path.join(snap,'_RESULT.txt'),'a').write(f'x\\tpid={os.getpid()}\\tEXIT 5 aborted before any write\\n')\n"
                    "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
                    "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                    "sys.exit(5)\n", 5),
}

# R770 #1. The four scenarios above notice ONE of nine deliberate re-openings of defects earlier
# reviews already found: three clauses are LIVE, turn a breach's logged 4 into 0 - which records the
# ticker done and skips it forever, R760 #1's exact harm - and are invisible to every test. Each
# scenario below is aimed at exactly one of them.
#
# HOW THE AIM IS PROVEN, and where (R779 #5 - the previous version of this comment cited
# `test_each_new_scenario_needs_its_own_clause`, which does not exist; `grep -rn` over the repo
# returns exactly one hit, the citation itself). The aim is proven OUT OF TREE, by
# scratchpad/scenario_control.py, which copies the driver, deletes one clause, drives the REAL
# driver end to end and requires the LOGGED code to fall 4 -> 0. It is not a pytest test because
# it mutates the driver's source, which a test running beside the real one must not do. Latest
# receipt, on driver sha256 fed3af7f7a0b:
#   `missing` -> sib_missing 4->0 | directional not-imported -> sib_hash_then_notimported 4->0
#   write-path header hash -> hdr_wrong_sha 4->0 | `malformed` -> sib_malformed 4->0
#   no-write header hash -> nw_hash_wrong 4->0 | no-write missing header -> nw_no_header 4->0
#   the MERGE clause -> no change, and it is declared untested rather than claimed (see below)
_HDR = ("print(f'  tool source sha256 {own} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n")
_WROTE = ("os.makedirs(snap, exist_ok=True)\n"
          "open(os.path.join(snap,'_RESULT.txt'),'a').write(f'x\\tpid={os.getpid()}\\tEXIT 0 DONE rebased (split)\\n')\n"
          "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
          "print('  DONE: rebased (split)', flush=True)\n")

_SCENARIOS.update({
    # (a) the COVERAGE clause: a sibling line that simply OMITS a guarded module. Every value it
    # does print is honest, both readings agree, and nothing else in _sibling_drift objects - only
    # `missing` does. A child that stops naming a module is a child whose imports are unverified.
    "sib_missing": (_HDR
                    + "print('  imported module sha256: ' + sibs(drop=('aggregate',)), flush=True)\n"
                    + _WROTE
                    + "print('  imported module sha256 at exit: ' + sibs(drop=('aggregate',)), flush=True)\n"
                    "sys.exit(0)\n", 4),
    # (b) header DRIFTED, trailer `not-imported` - the original R760 #1 shape, where `child.update()`
    # gave the trailer the final say and erased the drift.
    #
    # HONEST SCOPE, because the mutation control refused to let me claim more. I added this to test
    # the MERGE clause ("a real hash beats not-imported"). It does not: with the merge re-opened to
    # plain last-wins the code still comes out 4, because the DIRECTIONAL rule refuses this input
    # first - a drifted hash followed by not-imported is the illegal direction. Given that loop, the
    # merge is not independently reachable through the driver at all; every input that would need it
    # is already a disagreement. So the merge is defence in depth, deliberately kept and deliberately
    # NOT claimed as tested. This scenario earns its place as the R760 #1 regression itself, caught
    # by the directional rule - which is exactly what (c) below proves is load-bearing.
    "sib_drift_then_notimported": (_HDR
                                   + "print('  imported module sha256: ' + sibs(aggregate='9'*12), flush=True)\n"
                                   + _WROTE
                                   + "print('  imported module sha256 at exit: ' + sibs(aggregate='not-imported'), flush=True)\n"
                                   "sys.exit(0)\n", 4),
    # (c) the DIRECTIONAL rule, ISOLATED. Here the header hash is CORRECT, so the merge sees nothing
    # wrong and every value matches the driver's reading - only the direction is impossible. A module
    # goes not-imported -> hash; hash -> not-imported is a module un-importing itself mid-run, which
    # cannot happen, so the child is misreporting and the served state is unknown. My first attempt
    # at this scenario used a DRIFTED header and was caught by the merge instead, proving nothing
    # about this clause (R346: a control that passes for the wrong reason is not a control).
    "sib_hash_then_notimported": (_HDR
                                  + "print('  imported module sha256: ' + sibs(), flush=True)\n"
                                  + _WROTE
                                  + "print('  imported module sha256 at exit: ' + sibs(aggregate='not-imported'), flush=True)\n"
                                  "sys.exit(0)\n", 4),
    # (c) the WRITE-PATH header-hash compare. This is a DIFFERENT comparison from the one the drift
    # scenario exercises: here the child's own tool hash disagrees with what the driver hashed at
    # launch, on the path where a snapshot and a result line were already written. The tool running
    # is not the tool that was checked, so the served state is unknown however cleanly it exited.
    "hdr_wrong_sha": ("print(f'  tool source sha256 {chr(48)*64} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n"
                      "print('  imported module sha256: ' + sibs(), flush=True)\n"
                      + _WROTE
                      + "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                      "sys.exit(0)\n", 4),
    # ---- R779 #1: three MORE clauses whose removal takes a logged breach from 4 to 0 with all
    # 160 tests green, and two of them invisible to the 25-scenario harness as well. R770's own
    # text claimed RONEPAIR exercised `malformed`; it does not - with `malformed = []` every
    # harness scenario is identical to shipped, because RONEPAIR is refused by `missing`.
    #
    # (d) the MALFORMED clause: every guarded module named exactly once with a good value, plus one
    # trailing segment that is not a `name value` pair. missing/dupes/unknown/bad all stay quiet.
    "sib_malformed": (_HDR
                      + "print('  imported module sha256: ' + sibs() + ', bogus', flush=True)\n"
                      + _WROTE
                      + "print('  imported module sha256 at exit: ' + sibs() + ', bogus', flush=True)\n"
                      "sys.exit(0)\n", 4),
    # (e) and (f) the NO-WRITE path - a child that wrote nothing at all. Both differ from the
    # write-path comparisons above, and the pre-existing `no_header` scenario reaches NEITHER,
    # because a child printing no sibling line is refused earlier by _sibling_drift. To isolate
    # these the child must print a VALID sibling line and still write nothing.
    "nw_no_header": ("print('  imported module sha256: ' + sibs(), flush=True)\n"
                     "print('  nothing to rebase in --mode split (P_int=1)', flush=True)\n"
                     "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                     "sys.exit(0)\n", 4),
    "nw_hash_wrong": ("print(f'  tool source sha256 {chr(48)*64} ({os.path.abspath(__file__)}) pid {os.getpid()}', flush=True)\n"
                      "print('  imported module sha256: ' + sibs(), flush=True)\n"
                      "print('  nothing to rebase in --mode split (P_int=1)', flush=True)\n"
                      "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                      "sys.exit(0)\n", 4),
    # ---- R787 #1: three more clauses whose removal takes a logged breach from 4 to 0 with all 175
    # tests green. The first is the one that matters most.
    #
    # (g) THE FINAL CHILD-HASH vs DRIVER-HASH COMPARE — the comparison the whole sibling mechanism
    # exists for, and it had never been exercised. Every earlier drift scenario drifts the TRAILER
    # only, so the reading-vs-reading loop answers first and this `if` is never reached. A sibling
    # replaced BEFORE the child imported it reports the SAME drifted hash in header and trailer:
    # the readings agree, the merge is consistent, nothing upstream objects, and only the final
    # comparison against the driver's own start-time reading can see it.
    "sib_consistent_drift": (_HDR
                             + "print('  imported module sha256: ' + sibs(aggregate='9'*12), flush=True)\n"
                             + _WROTE
                             + "print('  imported module sha256 at exit: ' + sibs(aggregate='9'*12), flush=True)\n"
                             "sys.exit(0)\n", 4),
    # (h) the write-path RECORD/HEADER/CHILD PID TIE. The record line carries a pid that is not the
    # child's, i.e. another actor wrote it (R741 #5's "another actor's EXIT 1 RESTORED after t0 was
    # believed"). Everything else about the run is clean.
    "rec_foreign_pid": (_HDR
                        + "print('  imported module sha256: ' + sibs(), flush=True)\n"
                        + "os.makedirs(snap, exist_ok=True)\n"
                        "open(os.path.join(snap,'_RESULT.txt'),'a').write('x\\tpid=999999\\tEXIT 0 DONE rebased (split)\\n')\n"
                        "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
                        "print('  DONE: rebased (split)', flush=True)\n"
                        + "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                        "sys.exit(0)\n", 4),
    # (i) the write-path RECORD FRESHNESS STAMP. `rec_stamp >= stamp` is why the other scenarios use
    # a literal 'x' — it sorts above any ISO timestamp. A stamp from 2020 is a record from BEFORE
    # this run started, i.e. a leftover being read as this run's outcome.
    # ---- R787 #4 / R770 REQUIRED #2 AT LAST. Rather than close the clauses a reviewer named, I ran
    # the sweep the requirement always asked for: neutralise EVERY clause, run EVERY scenario, and
    # a clause is caught only if a LOGGED code moves. First run: 8 of 15, with a no-op control
    # correctly detected by nothing. These five close the gaps it found by name — which is the whole
    # point of a sweep, that it does not depend on anyone noticing the right clause.
    "sib_no_line_but_header": (_HDR + _WROTE
                               + "sys.exit(0)\n", 4),          # header fine, NO sibling line at all
    "sib_dupe": (_HDR
                 + "print('  imported module sha256: ' + sibs(dup=('aggregate',)), flush=True)\n"
                 + _WROTE
                 + "print('  imported module sha256 at exit: ' + sibs(dup=('aggregate',)), flush=True)\n"
                 "sys.exit(0)\n", 4),
    "sib_unknown": (_HDR
                    + "print('  imported module sha256: ' + sibs(extra='notamodule abc123def456'), flush=True)\n"
                    + _WROTE
                    + "print('  imported module sha256 at exit: ' + sibs(extra='notamodule abc123def456'), flush=True)\n"
                    "sys.exit(0)\n", 4),
    "sib_bad_value": (_HDR
                      + "print('  imported module sha256: ' + sibs(aggregate='zz'), flush=True)\n"
                      + _WROTE
                      + "print('  imported module sha256 at exit: ' + sibs(aggregate='zz'), flush=True)\n"
                      "sys.exit(0)\n", 4),
    "sib_all_not_imported": (_HDR
                             + "print('  imported module sha256: ' + sibs(**{n: 'not-imported' for n in SIBLINGS}), flush=True)\n"
                             + _WROTE
                             + "print('  imported module sha256 at exit: ' + sibs(**{n: 'not-imported' for n in SIBLINGS}), flush=True)\n"
                             "sys.exit(0)\n", 4),
    "rec_stale_stamp": (_HDR
                        + "print('  imported module sha256: ' + sibs(), flush=True)\n"
                        + "os.makedirs(snap, exist_ok=True)\n"
                        "open(os.path.join(snap,'_RESULT.txt'),'a').write(f'2020-01-01T00:00:00Z\\tpid={os.getpid()}\\tEXIT 0 DONE rebased (split)\\n')\n"
                        "print(f'  snapshot: 4 objects -> {snap} (size + MD5/ETag verified)', flush=True)\n"
                        "print('  DONE: rebased (split)', flush=True)\n"
                        + "print('  imported module sha256 at exit: ' + sibs(), flush=True)\n"
                        "sys.exit(0)\n", 4),
})


def _logged_code(tmp_path, body):
    """Run the REAL driver over one stand-in ticker and return the code it wrote to the log."""
    import shutil
    import subprocess
    d = tmp_path
    for f in ("seam_rebase_batch.py",) + srb.GUARDED[1:]:
        shutil.copyfile(os.path.join(HERE, f), str(d / f))
    (d / "seam_rebase.py").write_text(_standin(body), encoding="utf-8")
    (d / "tick.txt").write_text("TEST\n", encoding="utf-8")
    log = d / "b.log"
    subprocess.run(
        [sys.executable, str(d / "seam_rebase_batch.py"), "--apply",
         "--tickers", str(d / "tick.txt"), "--log", str(log),
         "--snapshot-root", str(d / "snaps")],
        capture_output=True, text=True, cwd=str(d))
    if not log.exists():
        return None
    lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
    return int(lines[-1].split("\t")[2]) if lines else None


@pytest.mark.parametrize("name", sorted(_SCENARIOS))
def test_the_driver_logs_the_right_code(tmp_path, name):
    """The end-to-end assertion pytest was missing. A breach must log 4; only a CLEAN child keeps its
    own code. R767 #1 shipped because this lived in bash and nothing in CI read it."""
    body, want = _SCENARIOS[name]
    assert _logged_code(tmp_path, body) == want, name


# ---------------------------------------------------------------- the SIBLINGS cross-check (R765 #3)

def _guard_verdict(tmp_path, sibling_src):
    """Run the driver's own _assert_guarded_matches_tool against a stand-in seam_rebase.py.

    Returns True if it ACCEPTED. The guard raises SystemExit to refuse, which is the shape the real
    driver relies on, so it is caught here rather than mocked."""
    import shutil
    import subprocess
    for f in ("seam_rebase_batch.py", "aggregate.py", "r2_client.py", "variables_sync.py",
              "compute_variables.py", "symbol_map.py"):
        shutil.copyfile(os.path.join(HERE, f), str(tmp_path / f))
    (tmp_path / "seam_rebase.py").write_text(sibling_src + "\n", encoding="utf-8")
    runner = tmp_path / "_run.py"
    runner.write_text(
        "import sys\nsys.path.insert(0, sys.argv[1])\n"
        "import seam_rebase_batch as s\ns._assert_guarded_matches_tool()\nprint('ACCEPTED')\n",
        encoding="utf-8")
    r = subprocess.run([sys.executable, str(runner), str(tmp_path)], capture_output=True, text=True)
    return "ACCEPTED" in r.stdout


_REAL = tuple(n[:-3] for n in srb.GUARDED)
_FULL = "SIBLINGS = " + repr(_REAL)
_SHORT = "SIBLINGS = " + repr(_REAL[:4])
_W = repr(_REAL[:4])          # the shrunk value, for the walrus-disguise cases below


@pytest.mark.parametrize("shape,src", [
    ("if True",        _FULL + "\nif True:\n    " + _SHORT),
    ("try",            _FULL + "\ntry:\n    " + _SHORT + "\nexcept Exception:\n    pass"),
    ("for block",      _FULL + "\nfor _ in (1,):\n    " + _SHORT),
    ("TYPE_CHECKING",  _FULL + "\nimport typing\nif not typing.TYPE_CHECKING:\n    " + _SHORT),
    ("globals()",      _FULL + "\nglobals()['SIBLINGS'] = " + repr(_REAL[:4])),
    ("duplicate name", "SIBLINGS = " + repr(_REAL[:5] + (_REAL[0],))),
    # R767 #4 - seven more shapes that bound at runtime while the guard read the honest declaration
    ("for target",     _FULL + "\nfor SIBLINGS in (" + repr(_REAL[:4]) + ",): pass"),
    ("with-as",        _FULL + "\nimport contextlib\nwith contextlib.nullcontext("
                       + repr(_REAL[:4]) + ") as SIBLINGS: pass"),
    ("walrus",         _FULL + "\n_ = (SIBLINGS := " + repr(_REAL[:4]) + ")"),
    ("import star",    _FULL + "\nfrom os.path import *"),
    ("list + pop",     "SIBLINGS = " + repr(list(_REAL)) + "\nSIBLINGS.pop()"),
    ("del",            _FULL + "\ndel SIBLINGS\n" + _FULL),
    ("exec",           _FULL + "\nexec('SIB' + 'LINGS = ()')"),
    # R770 #4 - binding through the namespace MAPPING. No assignment target, no setattr, so every
    # check above misses it; the reviewer's runtime control bound 4 of 6 modules while the guard
    # said nothing. The computed-key forms are R767 #4's split string wearing a different hat.
    ("globals().update",     _FULL + "\nglobals().update(SIBLINGS=" + repr(_REAL[:4]) + ")"),
    ("globals().__setitem__", _FULL + "\nglobals().__setitem__('SIBLINGS', " + repr(_REAL[:4]) + ")"),
    ("globals()[computed]",  _FULL + "\n_k = 'SIB' + 'LINGS'\nglobals()[_k] = " + repr(_REAL[:4])),
    ("vars().update",        _FULL + "\nvars().update(SIBLINGS=" + repr(_REAL[:4]) + ")"),
    # R770 #3's other direction: scoping the walk to module level must NOT open the `global` door.
    ("global in a function", _FULL + "\ndef f():\n    global SIBLINGS\n    SIBLINGS = "
                             + repr(_REAL[:4]) + "\nf()"),
    # R779 #2 - EIGHT module-scope walrus disguises my first scope fix accepted while the runtime
    # bound 4 of 6. A def's decorators, its argument DEFAULTS and a class's BASES are AST children
    # of that def but execute in the ENCLOSING scope; and PEP 572 puts a comprehension's walrus in
    # the CONTAINING scope, so only the comprehension's `for` target is genuinely comp-local.
    ("walrus in a def decorator",
     _FULL + "\ndef _d(x):\n    return lambda f: f\n@_d((SIBLINGS := " + _W + "))\ndef g(): pass"),
    ("walrus in a class decorator",
     _FULL + "\ndef _d(x):\n    return lambda c: c\n@_d((SIBLINGS := " + _W + "))\nclass C: pass"),
    ("walrus in an argument default",
     _FULL + "\ndef g(a=(SIBLINGS := " + _W + ")):\n    return a"),
    ("walrus in a lambda default",
     _FULL + "\n_l = lambda a=(SIBLINGS := " + _W + "): a"),
    ("walrus in a class base",
     _FULL + "\nclass C(dict if (SIBLINGS := " + _W + ") else dict): pass"),
    ("walrus in a list comprehension",
     _FULL + "\n_x = [1 for _ in (1,) if (SIBLINGS := " + _W + ")]"),
    ("walrus in a comprehension inside if",
     _FULL + "\nif True:\n    _x = [1 for _ in (1,) if (SIBLINGS := " + _W + ")]"),
    ("walrus in a generator expression",
     _FULL + "\n_g = list(1 for _ in (1,) if (SIBLINGS := " + _W + "))"),
    # R779 #2 / R770 REQUIRED #5's second named shape: the module dict reached without globals().
    ("sys.modules __dict__.update",
     _FULL + "\nimport sys\nsys.modules[__name__].__dict__.update(SIBLINGS=" + _W + ")"),
    ("sys.modules __dict__[...] =",
     _FULL + "\nimport sys\nsys.modules[__name__].__dict__['SIBLINGS'] = " + _W),
    # R787 #2 - three module-scope shrinks a54a632 refused and my R779 #4 fix let back in. BOTH
    # causes were that fix: `_declares_global`'s walk never broke on ClassDef, and the zero-argument
    # restriction let `vars(<the module>)` through while the `__dict__` clause saw no `__dict__`.
    ("global in a CLASS BODY",
     _FULL + "\nclass C:\n    global SIBLINGS\n    SIBLINGS = " + _W),
    ("global in a class body inside a function",
     _FULL + "\ndef f():\n    class C:\n        global SIBLINGS\n        SIBLINGS = " + _W + "\nf()"),
    ("vars(sys.modules[__name__]).update",
     _FULL + "\nimport sys\nvars(sys.modules[__name__]).update(SIBLINGS=" + _W + ")"),
    ("vars(sys.modules[__name__])[...] =",
     _FULL + "\nimport sys\nvars(sys.modules[__name__])['SIBLINGS'] = " + _W),
    # a computed key on the module namespace - R767 #4's split string, third costume
    ("globals()[split key] =",
     _FULL + "\n_k = 'SIB' 'LINGS'\nglobals()[_k] = " + _W),
])
def test_a_shrunk_SIBLINGS_is_refused_however_it_is_bound(tmp_path, shape, src):
    """R765 #3 then R767 #4: the guard walked `tree.body` only, so a shrink inside ANY module-level
    block bound at runtime while the guard read the honest declaration above it. `globals()[...]`
    escaped even the first fix, because the name sits in the assignment's subscript rather than
    inside the `globals()` call. R767 then found seven more that a target list of Assign/AnnAssign
    cannot see at all."""
    assert _guard_verdict(tmp_path, src) is False, shape


@pytest.mark.parametrize("shape,src", [
    ("tuple unpack read", _FULL + "\na, b, c, d, e, f = SIBLINGS"),
    ("dict value read",   _FULL + "\nD = {}\nD['x'] = SIBLINGS"),
    ("attribute read",    _FULL + "\nimport types\nns = types.SimpleNamespace()\nns.m = SIBLINGS"),
    # R770 #3: ast.walk reaches into function and class bodies, so an ordinary LOCAL that cannot
    # touch the module global refused every batch. These are the shapes that were wrongly refused.
    ("local in a function",  _FULL + "\ndef f():\n    SIBLINGS = 3\n    return SIBLINGS"),
    ("local in a class",     _FULL + "\nclass C:\n    SIBLINGS = 3"),
    ("for target in a func", _FULL + "\ndef f():\n    for SIBLINGS in (1, 2):\n        pass"),
    ("comprehension target", _FULL + "\n_x = [SIBLINGS for SIBLINGS in (1, 2)]"),
    ("setattr in a function", _FULL + "\ndef f(o):\n    setattr(o, 'x', 1)"),
    ("globals() READ",       _FULL + "\n_g = globals().get('X')"),
    # R779 #4: I removed one false refusal and added another. `vars(obj)` on an UNRELATED object is
    # not a namespace write; only the ZERO-argument form names this module.
    ("vars(obj).update",     _FULL + "\nimport types\n_o = types.SimpleNamespace()\n"
                             "vars(_o).update(x=1)"),
    ("a comprehension FOR target is genuinely comp-local",
     _FULL + "\n_x = [SIBLINGS for SIBLINGS in (1, 2)]"),
    # R787 #5: `vars(_o).update()` was accepted while the IDENTICAL `_o.__dict__.update()` was
    # refused — the same operation on the same object judged two ways. Both are ordinary objects
    # and both are accepted now; only THIS module's namespace is refused.
    ("obj.__dict__.update",  _FULL + "\nimport types\n_o = types.SimpleNamespace()\n"
                             "_o.__dict__.update(x=1)"),
    ("obj.__dict__[...] =",  _FULL + "\nimport types\n_o = types.SimpleNamespace()\n"
                             "_o.__dict__['x'] = 1"),
])
def test_innocent_READS_of_SIBLINGS_are_not_refused(tmp_path, shape, src):
    """The other half of R767 #4, and the half that matters for whether the guard survives contact
    with a maintainer. My first version matched the name ANYWHERE in an indirect assignment, so
    merely READING SIBLINGS into a tuple, a dict or an attribute refused the whole batch. It removed
    one false refusal and added three. A guard that cries wolf is the one that gets commented out."""
    assert _guard_verdict(tmp_path, src) is True, shape


def test_an_honest_declaration_is_accepted(tmp_path):
    assert _guard_verdict(tmp_path, _FULL) is True


def test_the_guard_ACCEPTS_THE_REAL_SHIPPED_TOOL():
    """The check that stands between this guard and a production outage.

    Every other test here drives a stand-in. But `_assert_guarded_matches_tool` runs against the REAL
    seam_rebase.py on every batch start, and the guard now refuses `exec`, `eval` and `setattr`
    outright - deliberately, because a name built from parts cannot be read statically (R767 #4). The
    cost of that choice is that adding any of them to seam_rebase.py for an unrelated reason would
    refuse EVERY run, and no stand-in test would notice. This one would."""
    srb._assert_guarded_matches_tool()          # raises SystemExit to refuse


def test_a_REORDERED_tuple_is_accepted(tmp_path):
    """R765's rider, and it reverses the previous behaviour. The property the guard exists for is
    WHICH modules are covered, not the order they are declared in. Refusing a reorder is a false
    refusal, and a guard that cries wolf is the one that gets commented out."""
    assert _guard_verdict(tmp_path, "SIBLINGS = " + repr(tuple(reversed(_REAL)))) is True


LIVE_PASSED = r"D:\research\hfdatalibrary\.claude\skills\adversarial-review\PASSED.md"


def test_the_live_PASSED_file_holds_ZERO_example_tokens():
    """R767 #5: this is the invariant the whole PASSED.md/SKILL.md split rests on, and until now
    nothing asserted it.

    The gate reads exactly one file and its revoke scan reads every line of that file, fences
    included, so a withdrawal cannot be hidden by quoting it. The price is that ANY example token
    written into PASSED.md is read as a real attempt. The design holds only while that file carries
    none. R767 also observed that `.claude/` is gitignored (`.gitignore:56`), so no commit, test or
    CI run can see this file - which is precisely why the check must be machine-local and must SKIP
    rather than fail when the file is absent."""
    if not os.path.exists(LIVE_PASSED):
        pytest.skip("live PASSED.md not on this machine (it is gitignored, so CI never sees it)")
    text = open(LIVE_PASSED, encoding="utf-8-sig", errors="replace").read()
    for tok in ("APPROVE-APPLY", "REVOKE-APPLY"):
        n = text.count(tok)
        assert n == 0, (
            f"PASSED.md contains {n} occurrence(s) of {tok}. The gate reads this file and treats a "
            f"{tok} line as a real attempt, so an EXAMPLE here refuses every run. THE FIX IS TO "
            f"DELETE THE LINE - never to narrow the matcher, which is what R763 #2 and R765 #1 both "
            f"did and both re-opened the revoke path. The syntax belongs in the skill's SKILL.md, "
            f"which this tool never opens.")


def test_the_live_PASSED_file_refuses_for_the_RIGHT_reason():
    """Machine-local companion to the above (R767 #5). It must refuse because there is no approval,
    not because its own text tripped the revoke scan."""
    if not os.path.exists(LIVE_PASSED):
        pytest.skip("live PASSED.md not on this machine")
    import io as _io
    buf = _io.StringIO()
    real, rv._say = rv._say, lambda s: buf.write(s + "\n")
    try:
        verdict = rv.reviewed_ok(ID, LIVE_PASSED)
    finally:
        rv._say = real
    out = buf.getvalue()
    assert verdict is False, "the live PASSED.md authorised a run it should not have"
    assert "REVOKE" not in out.upper(), (
        "the live PASSED.md is refusing because of its own text, not because an approval is absent. "
        "That is a self-inflicted denial of service - delete the offending line:\n" + out)
    assert "carries no approval line" in out, out


def test_a_bom_does_not_hide_the_first_line(passed):
    """A UTF-8 BOM made line 1 unmatchable; harmless for an approval, but it would also swallow a
    revocation written there."""
    assert passed("﻿" + GOOD) is True


# ---------------------------------------------------------------- the recode rule

@pytest.mark.parametrize("rc", [0, 1, 2, 3, 5, 6, 7])
def test_no_child_code_survives_a_drift(rc):
    """THE REAL FUNCTION the driver calls, not a copy of the rule written here.

    R760 #3: the previous version of these tests defined its own `_recode` and asserted that. It
    passed while the driver did anything at all, including a complete revert to the child's own code.
    Covers R754 #5 (the conclusion codes 0/2/3/6/7), R755 #4 (the write claims 1/5) and R759 (no
    snapshot is still UNKNOWN, the deliberate deviation from R757 #6) in one rule, because the rule is
    now one rule."""
    assert srb.recode_on_drift(rc) == 4


def _is_detail_write(stmt) -> bool:
    """The pin's END anchor, matched STRUCTURALLY (R770 #2).

    Every earlier version asked whether the TEXT `detail = os.path.join(` appeared in the
    statement - first by splitting source lines, then, after R765 #5, by searching
    `ast.unparse(stmt)`. The ast rewrite closed the COMMENT forge (comments are not nodes) and
    left the near-identical DOCSTRING one wide open: a bare string expression carrying that text
    unparses to itself, so it matched, truncated the region early, and hid a later `rc = raw_rc`
    - the exact defeat R763 #9 and R767 #1 were each written about, arriving a third time by a
    third route. A string constant is not an assignment.

    R779 #3: but matching "an Attribute called `join`" was STILL forgeable, now by an ordinary
    line - `detail = ', '.join(())`, and equally `''.join`, `sep.join`, `os.sep.join`. That
    collapsed the region from 2 statements to 1 while `found_end` stayed True, so the fail-closed
    assertion never fired and a revert after the block was invisible again. The anchor is
    `os.path.join` specifically: Attribute(join) of Attribute(path) of Name(os)."""
    import ast as _a
    f = stmt.value.func if (isinstance(stmt, _a.Assign) and isinstance(stmt.value, _a.Call)) else None
    return (isinstance(stmt, _a.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], _a.Name)
            and stmt.targets[0].id == "detail"
            and isinstance(f, _a.Attribute) and f.attr == "join"
            and isinstance(f.value, _a.Attribute) and f.value.attr == "path"
            and isinstance(f.value.value, _a.Name) and f.value.value.id == "os")


def _drift_region(src: str):
    """(region, found_end) for the statements from `if drift:` up to the detail write.

    Shared by the pin and by the forgery test below, so the property the pin rests on is
    exercised against a source I control as well as against the shipped one."""
    import ast as _a
    tree = _a.parse(src)
    blocks = [n for n in _a.walk(tree)
              if isinstance(n, _a.If) and isinstance(n.test, _a.Name) and n.test.id == "drift"]
    assert len(blocks) == 1, f"expected exactly one `if drift:` block, found {len(blocks)}"
    drift = blocks[0]
    parent = None
    for n in _a.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            seq = getattr(n, field, None)
            if isinstance(seq, list) and any(s is drift for s in seq):
                parent = seq
    assert parent is not None, "could not locate the statement list holding `if drift:`"
    start = parent.index(drift)
    end = next((i for i in range(start + 1, len(parent)) if _is_detail_write(parent[i])),
               len(parent))
    return parent[start:end], end < len(parent)


def test_the_end_anchor_cannot_be_FORGED_by_a_docstring():
    """R770 #2, shown able to fail. A module whose drift block is followed by a string expression
    carrying the anchor text, then a revert, then the REAL detail write: the region must still
    reach the revert. Under the text rule the region stopped at the string and `rc = 99` was
    invisible - which is how a 4 becomes a 0 and a ticker is skipped forever."""
    forged = (
        "import os\n"
        "def f(drift, raw_rc):\n"
        "    rc = 1\n"
        "    if drift:\n"
        "        rc = recode_on_drift(rc)\n"
        '    """detail = os.path.join(a, b)"""\n'
        "    rc = 99\n"
        "    detail = os.path.join('a', 'b')\n"
        "    return rc, detail\n")
    region, found_end = _drift_region(forged)
    assert found_end, "the real detail write must still be the anchor, not the string"
    import ast as _a
    rebinds = [_a.unparse(n) for stmt in region for n in _a.walk(stmt)
               if isinstance(n, _a.Assign) and len(n.targets) == 1
               and isinstance(n.targets[0], _a.Name) and n.targets[0].id == "rc"]
    assert "rc = 99" in rebinds, (
        f"the forged docstring truncated the region: the pin would not see the revert. got {rebinds}")


@pytest.mark.parametrize("forge", [
    "detail = ', '.join(())",
    "detail = ''.join(())",
    "detail = os.sep.join(())",
    "detail = sep.join(())",
])
def test_the_end_anchor_cannot_be_FORGED_by_an_ordinary_join(forge):
    """R787 #3 — the RED case that was missing, and its absence is measurable: widening
    `_is_detail_write` back to "any Attribute called `join`" leaves all 175 tests green. The
    docstring forge was closed and this one, an ORDINARY LINE, was not: it collapsed the region
    from 2 statements to 1 while `found_end` stayed True, so the fail-closed assertion never
    fired and a revert after the block went unseen."""
    src = ("import os\n"
           "def f(drift, raw_rc):\n"
           "    rc = 1\n"
           "    if drift:\n"
           "        rc = recode_on_drift(rc)\n"
           f"    {forge}\n"
           "    rc = 99\n"
           "    detail = os.path.join('a', 'b')\n"
           "    return rc, detail\n")
    region, found_end = _drift_region(src)
    assert found_end, f"{forge} became the anchor instead of the real os.path.join write"
    import ast as _a
    rebinds = [_a.unparse(n) for stmt in region for n in _a.walk(stmt)
               if isinstance(n, _a.Assign) and len(n.targets) == 1
               and isinstance(n.targets[0], _a.Name) and n.targets[0].id == "rc"]
    assert "rc = 99" in rebinds, (
        f"{forge} truncated the region: the pin would not see the revert. got {rebinds}")


def test_the_call_site_uses_that_function_and_nothing_else_touches_rc():
    """A source pin over the CALL SITE, hardened after R760 #3 showed the old one passing on 5 of 6
    mutations - including `rc = 4` followed by `rc = raw_rc`, a total revert.

    PARSED, NOT SPLIT (R765 #5). Every string-splitting version of this pin was defeated by moving
    text across its anchors: a COMMENT containing `detail = os.path.join(` truncated the block early
    and hid a later rebinding, and `globals()`, `(rc) =` and a backslash continuation each slipped
    past the line-oriented regex. ast sees the block the way Python does, so the anchors cannot be
    forged by text that is not code."""
    import ast as _ast
    src = open(os.path.join(HERE, "seam_rebase_batch.py"), encoding="utf-8").read()
    tree = _ast.parse(src)

    # find `if drift:` - the guard is a bare Name test, so it cannot be confused with `if drift and x`
    blocks = [n for n in _ast.walk(tree)
              if isinstance(n, _ast.If) and isinstance(n.test, _ast.Name) and n.test.id == "drift"]
    assert len(blocks) == 1, f"expected exactly one `if drift:` block, found {len(blocks)}"
    drift = blocks[0]

    # THE REGION IS THE BLOCK **PLUS EVERY STATEMENT UP TO THE DETAIL WRITE** (R767 #1).
    # My ast rewrite guarded the If node alone and so SHRANK the window the string version had -
    # re-opening R763 #9 exactly. `rc = raw_rc` placed after the block and before
    # `detail = os.path.join(` was missed, and it collapses 17 of 20 harness breach scenarios from
    # 4 to 0 while pytest stays green. The string version's wider anchor was the point of R763 #9,
    # and I deleted the comment that said so along with the code.
    parent = None
    for n in _ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            seq = getattr(n, field, None)
            # `body` is a plain expression on Lambda/IfExp, so check it really is a statement list
            if isinstance(seq, list) and any(s is drift for s in seq):
                parent = seq
    assert parent is not None, "could not locate the statement list holding `if drift:`"
    start = parent.index(drift)
    # STRUCTURAL end anchor (R770 #2) - see _is_detail_write: the text test was forgeable by a
    # docstring, which is the comment forge R765 #5 closed, returning by another door.
    end = next((i for i in range(start + 1, len(parent)) if _is_detail_write(parent[i])),
               len(parent))
    region = parent[start:end]
    # The detail write must actually be FOUND. If it were not, `end` falls back to the end of the
    # function and the region silently widens to everything after the block - which would over-fire
    # rather than under-fire, but either way the pin would no longer mean what it says.
    assert end < len(parent), (
        "could not find the `detail = os.path.join(` statement after the drift block; the pin's "
        "region is only meaningful between those two anchors")
    assert len(region) >= 1

    def _rebinds_rc(node):
        """Every way a statement can bind the name rc, not only `rc = ...`."""
        out = []
        tgts = []
        if isinstance(node, _ast.Assign):
            tgts = list(node.targets)
        elif isinstance(node, (_ast.AugAssign, _ast.AnnAssign)):
            tgts = [node.target]
        elif isinstance(node, (_ast.For, _ast.AsyncFor)):
            tgts = [node.target]                                  # `for rc in ...`
        elif isinstance(node, (_ast.With, _ast.AsyncWith)):
            tgts = [i.optional_vars for i in node.items if i.optional_vars]   # `with ... as rc`
        elif isinstance(node, _ast.NamedExpr):
            tgts = [node.target]                                  # `(rc := ...)`
        elif isinstance(node, _ast.comprehension):
            tgts = [node.target]
        for t in tgts:
            names = list(t.elts) if isinstance(t, (_ast.Tuple, _ast.List)) else [t]
            if any(isinstance(e, _ast.Name) and e.id == "rc" for e in names):
                out.append(_ast.unparse(node).splitlines()[0][:90])
        # and the forms no static target list can see
        if isinstance(node, _ast.Call) and isinstance(node.func, _ast.Name) \
                and node.func.id in ("exec", "eval", "setattr") and "rc" in _ast.dump(node):
            out.append(_ast.unparse(node)[:90])
        if isinstance(node, _ast.Subscript) and isinstance(node.value, _ast.Call) \
                and isinstance(node.value.func, _ast.Name) and node.value.func.id in ("globals", "vars"):
            out.append(_ast.unparse(node)[:90])
        return out

    binds = [b for stmt in region for n in _ast.walk(stmt) for b in _rebinds_rc(n)]
    assert binds == ["rc = recode_on_drift(rc)"], (
        f"from `if drift:` up to the detail write, rc must be rebound exactly ONCE and through the "
        f"shipped function; found {binds}. Defeats already seen: 'rc = 4' then 'rc = raw_rc'; "
        f"'rc, _ = raw_rc, 0'; 'rc -= (rc - raw_rc)'; a comment carrying the text anchor; and - the "
        f"one this wider region exists for - a revert placed AFTER the block and BEFORE the detail "
        f"write, which pytest missed while the driver harness went 4 -> 0 on 17 of 20 scenarios")
    assert not [n for stmt in region for n in _ast.walk(stmt)
                if isinstance(n, _ast.Name) and n.id == "snapshot_ok"], (
        "the recode must not branch on snapshot_ok - that scoping un-did R754 #5 once already")


# ---------------------------------------------------------------- GUARDED vs the tool's own SIBLINGS

OURS = "(" + ", ".join(repr(n[:-3]) for n in srb.GUARDED) + ")"


def _write_tool(tmp_path, body):
    p = tmp_path / "seam_rebase.py"
    p.write_text("import os" + chr(10) + body + chr(10) * 2 + "def main():" + chr(10) + "    return 0" + chr(10),
                 encoding="utf-8")
    return str(p)


def test_a_matching_siblings_tuple_passes(tmp_path):
    srb._assert_guarded_matches_tool(_write_tool(tmp_path, "SIBLINGS = " + OURS))


@pytest.mark.parametrize("body", [
    "PLACEHOLDER = 1",                          # no SIBLINGS at all
    "SIBLINGS = ('aggregate',)",                # a short tuple: modules nobody hashes
    "SIBLINGS = tuple(sorted(['a', 'b']))",     # built by an expression, unreadable statically
])
def test_a_bad_siblings_declaration_refuses(tmp_path, body):
    with pytest.raises(SystemExit):
        srb._assert_guarded_matches_tool(_write_tool(tmp_path, body))


def test_a_second_shrinking_assignment_refuses(tmp_path):
    """R763 #6: the regex read only the FIRST assignment, so a later shrinking one passed while the
    runtime bound the shrunk tuple - modules unguarded, the first declaration still looking right."""
    with pytest.raises(SystemExit):
        srb._assert_guarded_matches_tool(
            _write_tool(tmp_path, "SIBLINGS = " + OURS + chr(10) + "SIBLINGS = ('aggregate',)"))


def test_an_annotated_declaration_is_accepted(tmp_path):
    """The other direction of R763 #6: the regex REFUSED correct code. A guard that refuses correct
    behaviour is a guard the next author deletes."""
    srb._assert_guarded_matches_tool(_write_tool(tmp_path, "SIBLINGS: tuple = " + OURS))


def test_a_comment_inside_the_tuple_is_accepted(tmp_path):
    body = "SIBLINGS = (" + chr(10) + "".join(
        "    " + repr(n[:-3]) + ",   # guarded" + chr(10) for n in srb.GUARDED) + ")"
    srb._assert_guarded_matches_tool(_write_tool(tmp_path, body))


def test_an_unparseable_tool_refuses(tmp_path):
    p = tmp_path / "seam_rebase.py"
    p.write_text("SIBLINGS = (", encoding="utf-8")
    with pytest.raises(SystemExit):
        srb._assert_guarded_matches_tool(str(p))
