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


def test_an_unparseable_revoke_mention_refuses_loudly(passed, capsys):
    """We cannot tell which id it was meant for, so the safe reading of 'somebody tried to withdraw
    this' is to stop - never to pass over it in silence."""
    assert passed(GOOD + "\nREVOKE-APPLY resync_varibles.py oops") is False
    assert "REVOKE-APPLY" in capsys.readouterr().out


# ---------------------------------------------------------------- documentation is not a decision

def test_a_fenced_example_neither_authorises_nor_refuses(passed):
    """The fix for R760 #2 made the revoke matcher permissive, and the same commit documented the
    syntax IN PASSED.md - so the file's own examples made the tool refuse every run (measured
    2026-09-05T20:51Z). Fenced blocks are documentation and are skipped entirely."""
    doc = ("Some prose.\n```\nAPPROVE-APPLY resync_variables.py <sha12> <id>\n"
           "REVOKE-APPLY resync_variables.py <sha12> <id>\n```\n" + GOOD)
    assert passed(doc) is True, "a fenced example must not refuse a genuine approval"


def test_a_fenced_real_token_does_not_authorise(passed):
    """The other half, and the last of R757 #3's class: a column-0 token INSIDE a fence is a quotation."""
    assert passed("```\n" + GOOD + "\n```") is False


def test_an_inline_code_token_does_not_authorise(passed):
    assert passed(f"`{GOOD}`") is False


def test_inline_code_mentioning_revoke_does_not_refuse(passed):
    """Prose like: a line that mentions `REVOKE-APPLY` but does not parse makes the tool refuse."""
    assert passed(GOOD + "\nprose that mentions `REVOKE-APPLY` in passing") is True


def test_a_real_revoke_outside_a_fence_still_wins(passed):
    """Skipping fences must not become a way to hide a revocation from the gate."""
    doc = ("```\nREVOKE-APPLY resync_variables.py <sha12> <id>\n```\n"
           + GOOD + f"\n  REVOKE-APPLY resync_variables.py {SHA} {ID}")
    assert passed(doc) is False


def test_the_projects_own_passed_file_does_not_refuse():
    """The live 74 KB PASSED.md must not, by documenting the syntax, refuse every run. It carries no
    approval for this hash, so the answer is False - but it must be False for THAT reason."""
    p = r"D:\research\hfdatalibrary\.claude\skills\adversarial-review\PASSED.md"
    if not os.path.exists(p):
        pytest.skip("PASSED.md not present in this checkout")
    import io
    buf = io.StringIO()
    real, rv._say = rv._say, lambda s: buf.write(s + "\n")
    try:
        assert rv.reviewed_ok(ID, p) is False
    finally:
        rv._say = real
    assert "REVOKE" not in buf.getvalue().upper(), (
        "PASSED.md's own documentation of the revoke syntax is making the gate refuse:\n" + buf.getvalue())
    assert "carries no approval line" in buf.getvalue()


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


def test_the_call_site_uses_that_function_and_nothing_else_touches_rc():
    """A source pin over the CALL SITE, hardened after R760 #3 showed the old one passing on 5 of 6
    mutations - including `rc = 4` followed by `rc = raw_rc`, a total revert."""
    src = open(os.path.join(HERE, "seam_rebase_batch.py"), encoding="utf-8").read()
    block = src.split("if drift:", 1)[1].split("hash_breach = hash_breach or", 1)[0]
    code = [l.strip() for l in block.splitlines() if l.strip() and not l.strip().startswith("#")]
    assigns = [l for l in code if l.startswith("rc =") or l.startswith("rc=")]
    assert assigns == ["rc = recode_on_drift(rc)"], (
        f"the drift block must assign rc exactly once, through the shipped function; found {assigns}. "
        "A second assignment is how a revert hides - the old pin accepted 'rc = 4' followed by "
        "'rc = raw_rc'")
    assert "snapshot_ok" not in " ".join(code), (
        "the recode must not branch on snapshot_ok - that scoping un-did R754 #5 once already")
