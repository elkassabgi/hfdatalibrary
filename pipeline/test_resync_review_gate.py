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


def test_a_revocation_misspelling_the_TOOL_escapes_the_warning(passed):
    """A stated, deliberate limit rather than a hidden one (R763 #3). The mention check is narrowed to
    lines that NAME THIS TOOL, so PASSED.md can document the syntax with a `<tool>.py` placeholder
    without refusing every run. The cost is that a revocation which misspells the filename is not
    warned about. A mistyped hash or id, far likelier, still is - see the test above."""
    assert passed(GOOD + "\nREVOKE-APPLY resync_varibles.py oops") is True


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


def test_the_documentation_placeholder_is_inert(passed):
    """The other side of the same coin: PASSED.md documents the syntax, and since the revoke scanner
    reads fences too, the example must not name a real tool. It uses `<tool>.py`."""
    doc = "```\nREVOKE-APPLY <tool>.py <sha12> <review id>\n```\n" + GOOD
    assert passed(doc) is True


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
        "To withdraw one, write anywhere on a line:\n\n"
        "```\nREVOKE-APPLY <tool>.py <sha12> <review id>\n```\n\n"
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
    import re as _re
    src = open(os.path.join(HERE, "seam_rebase_batch.py"), encoding="utf-8").read()
    # R763 #9: the block used to end at "hash_breach = hash_breach or", so a revert placed one line
    # AFTER that terminator was outside the pin. Run to the next real statement instead.
    block = src.split("if drift:", 1)[1].split("detail = os.path.join(", 1)[0]
    code = [l.strip() for l in block.splitlines() if l.strip() and not l.strip().startswith("#")]
    # any rebinding of rc, not just "rc =": tuple targets and augmented assignment both defeated it
    binds = [l for l in code if _re.match(r"^rc\b\s*(,|=|\+=|-=|\*=|/=|//=|%=|:)", l)]
    assert binds == ["rc = recode_on_drift(rc)"], (
        f"between 'if drift:' and the detail-file write, rc must be rebound exactly once and through "
        f"the shipped function; found {binds}. Defeats already seen: 'rc = 4' then 'rc = raw_rc', "
        f"'rc, _ = raw_rc, 0', 'rc -= (rc - raw_rc)', and a revert placed after the block terminator")
    assert "snapshot_ok" not in " ".join(code), (
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
