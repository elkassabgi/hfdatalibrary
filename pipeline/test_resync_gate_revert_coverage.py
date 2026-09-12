"""Revert coverage for the mention rule: one test per shipped fix that goes GREEN when the fix is
removed (R878 #5, R883).

WHY THIS FILE EXISTS. A revert harness (`ar47_revert.py`, scratchpad) patched each fix out of
`resync_variables.py` one at a time and re-ran the suite. Seven of the mention rule's fixes left it
at 438 passed with the fix REMOVED - the three late Unicode separators in `_REVOKE_SHAPE` and in
`_TOKEN_SPELLED`, four widenings of `OPERAND_RE` (a leading backtick/quote, a URL, a Windows drive
with any short extension, `AR046` / `#046` / `#12`), and the cell-end disjunct. Part of that figure
was a stale copy of the tests (R883), but the shapes below were chosen by reading the rule rather
than by trusting either number: each line is decided by EXACTLY the disjunct or branch its test is
for, so no neighbouring rule can catch it when that one is reverted.

WHAT MASKS WHAT, because it dictates the shapes and the next author will otherwise "simplify"
them back into masked ones:

  * `CELL_END_RE` is `_REVOKE_SHAPE` + a lookahead for "not followed by a word", compiled
    case-SENSITIVELY. Any CAPS shape followed by punctuation, a quote, a `#`, a `|` or the end of
    the line is caught by it whatever the other rules say. So the leading-quote branch of
    `OPERAND_RE` and its `#046` / `#12` branch can only be seen with a lower- or mixed-case verb,
    which is why several lines below are not shouted.
  * `TOKEN_SPELLED_RE` catches a CAPS token-spelled shape ANYWHERE, so a separator test for it
    needs the shape followed by an ordinary word (else the cell-end rule catches the line first),
    and a separator test for `_REVOKE_SHAPE` can use a bare token (with no shape at all, nothing
    downstream runs).
  * `TOOL_MENTION_RE` catches any line naming `resync_variables`, so no operand line may name it.

THE CODEPOINTS, spelled out so nobody has to trust an editor's rendering of them:

    U+2212  MINUS SIGN                 (the three separators the fix added; R878 #1 measured
    U+00AD  SOFT HYPHEN                 every one of them silently ignored by an earlier version)
    U+FF0D  FULLWIDTH HYPHEN-MINUS
    U+2010..U+2015, U+2043, U+FE63     also in both classes; not under test here
    U+00A0  NO-BREAK SPACE             in `_TOKEN_SPELLED` only; U+0020 is deliberately NOT

EVERY REFUSING LINE HAS AN INNOCENT NEIGHBOUR in the same parametrize list. A test that only
proves refusal passes against a rule that refuses everything - R876 and R878 were exactly that -
so each list carries prose that must still be GRANTED, including a line that contains the very
codepoint or construct under test without any revoke shape.

Direction, once more: approval fails CLOSED, withdrawal fails OPEN. A mistyped withdrawal must
REFUSE (`expect=False`); ordinary prose must be GRANTED (`expect=True`).
"""
from __future__ import annotations

import importlib.util
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(_HERE, "resync_variables.py")

spec = importlib.util.spec_from_file_location("_rv_revcov", _TOOL)
rv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rv)

ID = "AR-040"
SHA12 = rv._SOURCE_SHA[:12]
GOOD = f"APPROVE-APPLY resync_variables.py {SHA12} {ID}"

MINUS = "\u2212"        # U+2212 MINUS SIGN
SHY = "\u00ad"          # U+00AD SOFT HYPHEN
FWHYPHEN = "\uff0d"     # U+FF0D FULLWIDTH HYPHEN-MINUS


@pytest.fixture
def gate(tmp_path):
    """The gate over a temp file holding a genuine approval plus ONE more line. Never the live
    PASSED.md - the tool is gated on a machine-checked token there and this file must not touch
    it (R874/R877: hermetic on tmp_path, or it is a local-machine artefact)."""
    def run(line: str, review_id: str = ID):
        p = tmp_path / "PASSED.md"
        p.write_text(GOOD + "\n" + line + "\n", encoding="utf-8")
        return rv.reviewed_ok(review_id, str(p))
    return run


# ------------------------------------------------------- F1: the separators in _REVOKE_SHAPE

@pytest.mark.parametrize("line,expect", [
    ("REVOKE" + MINUS + "APPLY", False),
    ("REVOKE" + SHY + "APPLY", False),
    ("REVOKE" + FWHYPHEN + "APPLY", False),
    # a real withdrawal of THIS tool, minus-sign spelled: the parser cannot read it (it wants the
    # ASCII hyphen) and the mention rule must therefore refuse rather than let the approval stand
    ("REVOKE" + MINUS + "APPLY resync_variables.py * *", False),
    ("| AR-047 | 2026-09-07 | WITHDRAW" + SHY + "APPLY |", False),
    ("<!-- 2026-09-07: CANCEL" + FWHYPHEN + "APPLY -->", False),
    # innocent neighbours: the codepoint alone, and the prose the suite already pins as granted
    ("the minus sign " + MINUS + " is not a hyphen and this line withdraws nothing", True),
    ("A MISMATCH MUST CANCEL APPLY AND RESTORE.", True),
    ("The cancel-apply path is exit 1.", True),
], ids=["U+2212 bare", "U+00AD bare", "U+FF0D bare", "U+2212 real withdrawal", "U+00AD table cell",
        "U+FF0D html note", "U+2212 in prose", "caps prose", "lowercase hyphen prose"])
def test_the_three_late_separators_are_revoke_SHAPES(gate, line, expect):
    """`_REVOKE_SHAPE` is the gate to the whole mention scan: with no shape on the line nothing
    downstream runs, so a separator missing from ITS class is a withdrawal that is ignored outright,
    whatever `_TOKEN_SPELLED` or the cell rule would have said. Reverting U+2212, U+00AD and U+FF0D
    from that class leaves every refusing line here GRANTED."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F2: the separators in _TOKEN_SPELLED

@pytest.mark.parametrize("line,expect", [
    # CAPS, token-spelled with a late separator, FOLLOWED BY A WORD - so the cell-end rule cannot
    # catch it, it names no tool and carries no operand: only the spelling test can see it
    ("the verdict is REVOKE" + MINUS + "APPLY until the pool denominator is fixed", False),
    ("| AR-047 | 2026-09-07 | resync gate | WITHDRAW" + SHY + "APPLY as of today |", False),
    ("<!-- 2026-09-07: this tool's approval is CANCEL" + FWHYPHEN + "APPLY per the reviewer -->", False),
    # the ASCII-hyphen twins refuse in every version of the rule: the class is the only variable
    ("the verdict is REVOKE-APPLY until the pool denominator is fixed", False),
    ("<!-- 2026-09-07: this tool's approval is CANCEL-APPLY per the reviewer -->", False),
    # innocent neighbours
    ("prices fell " + MINUS + "4.2 % after the split and nobody withdrew anything", True),
    ("The cancel-apply path is exit 1.", True),
    ("we cancel apply", True),
], ids=["U+2212 mid-line", "U+00AD table cell mid-line", "U+FF0D html note mid-line",
        "ascii twin 1", "ascii twin 2", "U+2212 in prose", "lowercase hyphen prose", "lowercase space prose"])
def test_a_late_separator_token_is_spelled_wherever_it_sits(gate, line, expect):
    """`_TOKEN_SPELLED` is what makes `REVOKE-APPLY` a token mid-sentence. The bare-token form of
    each late separator is ALSO caught by the cell-end rule, so a test made of bare tokens stays
    green with U+2212, U+00AD and U+FF0D reverted from THIS class - which is exactly what happened.
    The shapes here are followed by a word, so the spelling test is the only way in."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F3: OPERAND_RE - a leading backtick or quote

@pytest.mark.parametrize("line,expect", [
    # NOT shouted, on purpose: a CAPS verb followed by a quote is caught by the cell-end rule
    # regardless, so only a lower- or mixed-case verb can show whether the operand branch itself
    # accepts a quoted operand
    ("Revoke apply `<tool>.py <sha12> <review id>`", False),
    ('Revoke apply "AR-046" and re-review the seam job', False),
    ("Withdraw apply 'https://github.com/elkassabgi/hfdatalibrary/pull/8'", False),
    ("revoke apply `notes.txt`", False),
    # the unquoted twin refuses in every version: the quote is the only variable
    ("Revoke apply AR-046 and re-review the seam job", False),
    # innocent neighbours: a backtick right after the verb with no operand behind it, and prose
    ("The reviewer wrote `cancel apply` in quotes but meant restore.", True),
    ("we cancel apply", True),
    ("Cancel apply and restore whenever the two hashes disagree.", True),
], ids=["backtick placeholder", "double-quoted id", "single-quoted url", "backtick file",
        "unquoted twin", "backtick with no operand", "lowercase space prose", "sentence prose"])
def test_an_operand_may_open_with_a_backtick_or_a_quote(gate, line, expect):
    """R876 #1 measured a backticked operand ignored; the fix lets `OPERAND_RE` skip one leading
    backtick or quote. Reverting it leaves every refusing line here GRANTED."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F4: OPERAND_RE - the URL branch

@pytest.mark.parametrize("line,expect", [
    ("REVOKE APPLY https://github.com/elkassabgi/hfdatalibrary/pull/8", False),
    ("revoke apply https://github.com/elkassabgi/hfdatalibrary/pull/8 (the review thread)", False),
    ("| AR-047 | 2026-09-07 | WITHDRAW APPLY http://example.org/ar-047 |", False),
    # innocent neighbours: a URL on the line does not by itself make it a withdrawal
    ("see https://github.com/elkassabgi/hfdatalibrary/pull/8 before you cancel apply and restore", True),
    ("The cancel-apply path is exit 1.", True),
], ids=["caps https", "lowercase https", "table cell http", "url in prose", "lowercase hyphen prose"])
def test_a_URL_is_an_operand(gate, line, expect):
    """A space-spelled CAPS verb followed by a URL: `https` starts with a letter, so the cell-end
    rule does not fire, the spelling test does not fire, and no path branch matches `https://...`
    (the drive-letter group wants ONE letter before the colon). Only the URL branch refuses it."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F5: OPERAND_RE - Windows drive + any extension

@pytest.mark.parametrize("line,expect", [
    ("REVOKE APPLY notes.txt", False),                                          # any extension
    ("revoke apply run_momentum.log", False),
    ("REVOKE APPLY D:/temp/claude/hf_wt_main/pipeline/notes.py", False),       # drive letter
    ("WITHDRAW APPLY C:\\Users\\ae\\PASSED.md", False),
    # `.md` with no drive is accepted by the OLD branch too: refuses in every version
    ("REVOKE APPLY notes.md", False),
    # innocent neighbours: a file name and a drive letter on the line, verb followed by prose
    ("Read notes.txt before you cancel apply and restore.", True),
    ("the C:\\ drive was detached, so we cancel apply and restore", True),
    ("we cancel apply", True),
], ids=["txt", "log lowercase", "drive forward slashes", "drive backslashes", "md twin",
        "file in prose", "drive in prose", "lowercase space prose"])
def test_a_windows_path_or_any_short_extension_is_an_operand(gate, line, expect):
    """The old path branch accepted `.py` and `.md` only and had no room for `D:` - so
    `D:/.../notes.py` was NOT an operand even with its allowed extension (the drive letter breaks
    the match at the colon). Both halves are exercised; the single revert removes both."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F6: OPERAND_RE - AR046 / #046 / #12

@pytest.mark.parametrize("line,expect", [
    ("REVOKE APPLY AR046", False),
    ("REVOKE APPLY AR_046 - the approval is pulled", False),
    # `#` is not alphanumeric, so a CAPS verb before `#046` is caught by the cell-end rule anyway;
    # the `#nnn` branch is only visible behind a lower- or mixed-case verb
    ("revoke apply #046 (see the row below)", False),
    ("Withdraw apply #12 before Monday", False),
    # the hyphenated id is accepted by the OLD branch too: refuses in every version
    ("REVOKE APPLY AR-046", False),
    # innocent neighbours
    ("AR-046 and AR046 are the same review; we cancel apply and restore.", True),
    ("issue #12 asks why the cancel-apply path is exit 1", True),
    ("we cancel apply", True),
], ids=["AR046", "AR_046", "#046 lowercase", "#12 mixed case", "AR-046 twin",
        "ids in prose", "#12 in prose", "lowercase space prose"])
def test_AR046_hash046_and_hash12_are_review_id_operands(gate, line, expect):
    """The review-id branch used to be `AR-\\d+` alone. `AR046`, `AR_046`, `#046` and `#12` are
    how the same id is written in a hurry, and each was an ignored withdrawal (R876 #1)."""
    assert gate(line) is expect, repr(line)


# ------------------------------------------------------- F8: the cell-end disjunct

@pytest.mark.parametrize("line,expect", [
    # CAPS, SPACE-spelled (so the spelling test cannot see it), no tool name, no operand, and
    # NOT followed by a word: only the cell-end rule refuses these
    ("REVOKE APPLY", False),
    ("| AR-047 | 2026-09-07 | REVOKE APPLY |", False),
    ("<!-- 2026-09-07: the approval above is REVOKE APPLY -->", False),
    ("2026-09-07: WITHDRAW APPLY, per the reviewer.", False),
    ("the reviewer asks you to REVOKE APPLY.", False),
    # innocent neighbours: the same verbs followed by a word, or not in capitals
    ("A MISMATCH MUST CANCEL APPLY AND RESTORE.", True),
    ("THE GUARD MUST REVOKE APPLY PERMISSION ONLY ON A REAL TOKEN.", True),
    ("we cancel apply", True),
    ("(we cancel apply)", True),
    ("- Cancel apply and restore on any mismatch (exit 1).", True),
], ids=["bare column 0", "table cell", "html note", "before comma", "before full stop",
        "caps then AND", "caps then PERMISSION", "lowercase end of line", "lowercase in parens", "sentence prose"])
def test_a_CAPS_shape_not_followed_by_a_word_is_a_token_whatever_its_separator(gate, line, expect):
    """R878 #2's rule as shipped: prose runs on into another word, a written token does not.
    Reverting `or _cell_end(ln)` leaves every refusing line here GRANTED, and the innocent lines
    are the ones the earlier case-insensitive version bricked on a one-character boundary."""
    assert gate(line) is expect, repr(line)
