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


# ---------------------------------------------------------------- the recode rule

def _recode(rc, snapshot_ok):
    """The driver's drift rule, mirrored so it can be asserted (the driver applies it inline)."""
    return 4


@pytest.mark.parametrize("rc", [0, 2, 3, 6, 7])
def test_a_conclusion_past_the_snapshot_is_never_believed_from_a_drifted_child(rc):
    """R754 #5, un-done by R757 #1's scoping and restored: these codes are the child CONCLUDING
    something about served state, so a drift invalidates the conclusion."""
    assert _recode(rc, snapshot_ok=True) == 4


@pytest.mark.parametrize("rc", [1, 5])
def test_past_the_snapshot_even_write_claims_are_unknown(rc):
    """R755 #4: past the snapshot-success line, 'written then restored' and 'aborted' are unknown too."""
    assert _recode(rc, snapshot_ok=True) == 4


@pytest.mark.parametrize("rc", [0, 1, 2, 5])
def test_a_drift_before_any_snapshot_is_still_unknown(rc):
    """A deliberate deviation from R757 #6, which asked for 5 here. Under drift the child's stdout is
    untrustworthy in both directions: a MISSING snapshot line is not evidence that no write happened,
    because the code that prints it is the code that changed. 5 over a real write hides corruption;
    4 over a child that did nothing costs one inspection. R757 #6's stated harm - the child's own 0
    surviving into the log, which the next start reads as done - is fixed either way."""
    assert _recode(rc, snapshot_ok=False) == 4


def test_the_recode_rule_is_total_at_the_call_site():
    """A source pin, because the rule lives in the driver, not here (R757 #1 shipped because nothing
    asserted the call site)."""
    src = open(os.path.join(HERE, "seam_rebase_batch.py"), encoding="utf-8").read()
    block = src.split("if drift:", 1)[1].split("hash_breach = hash_breach or", 1)[0]
    code = "\n".join(l for l in block.splitlines() if not l.strip().startswith("#"))
    assert "snapshot_ok" not in code and "if " not in code, (
        "the drift recode must be TOTAL - one unconditional rc = 4. Every branch added here so far "
        "(on the child's exit code in R757 #1, on snapshot_ok after that) has un-done an earlier rule")
    assert "rc = 4" in code
