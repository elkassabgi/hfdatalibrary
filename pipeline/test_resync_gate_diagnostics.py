"""A gate this brittle owes the reviewer a REASON, and three more quoting shapes still authorised.

R866 #1: the revoke path prints `line 95:` and quotes the line; the approve path printed only
"carries no approval line. It must read, at column 0: APPROVE-APPLY ..." - which is verbatim the
line a reviewer had just written. When a token is swallowed by an unclosed block the message
therefore reads "you wrote nothing" while the truth is "you wrote it inside a <code> block that
opened at line 65". That is not hypothetical: it is R858, where the gate was bricked against the
only file it reads and the tool said nothing useful about it for a whole round.

R866 #5: CommonMark raw-HTML block types 3, 4 and 5 - `<?...?>`, `<!DECLARATION>` and
`<![CDATA[...]]>` - are not tags and not comments, so neither the tag stack nor the comment handler
saw them and a token quoted inside one AUTHORISED. YAML front matter was the same.

R866 #6: six token-shaped revocation spellings were silently ignored, in the one direction the
design says must fail OPEN.

The tests below are in both directions on purpose: every new refusal is paired with a case that
must still authorise, because a gate that refuses everything passes any one-sided suite.
"""
from __future__ import annotations

import importlib.util
import io
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(_HERE, "resync_variables.py")

spec = importlib.util.spec_from_file_location("_rv_diag", _TOOL)
rv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rv)

ID = "AR-040"
SHA12 = rv._SOURCE_SHA[:12]
GOOD = f"APPROVE-APPLY resync_variables.py {SHA12} {ID}"


@pytest.fixture
def gate(tmp_path):
    def run(body: str, review_id: str = ID):
        p = tmp_path / "PASSED.md"
        p.write_text(body if body.endswith("\n") else body + "\n", encoding="utf-8")
        return rv.reviewed_ok(review_id, str(p))
    return run


# ------------------------------------------------------------------ the diagnostic

@pytest.mark.parametrize("body,expect", [
    ("<code>\nnote\n" + GOOD, "raw-HTML <code> block opened at line 1"),
    ("```\nnote\n" + GOOD, "fenced code block opened at line 1"),
    ("<!--\nnote\n" + GOOD, "HTML comment opened at line 1"),
    ("---\n" + GOOD, "YAML front matter opened at line 1"),
    ("<?php\nnote\n" + GOOD, "<?...?> processing instruction opened at line 1"),
])
def test_a_swallowed_token_is_named_with_its_line_and_its_cause(gate, capsys, body, expect):
    assert gate(body) is False
    out = capsys.readouterr().out
    assert "WOULD authorise" in out, out
    assert expect in out, out


def test_a_backticked_token_refuses_and_is_NOT_claimed_to_have_would_authorised(gate, capsys):
    """An inline-code span must OPEN with a backtick, so a span covering a column-0 token puts that
    backtick at column 0 - and then the token is not at column 0 and never would have authorised.
    Reporting it as "WOULD authorise, but..." would send a reviewer to un-quote a line that was
    never going to work."""
    assert gate("`" + GOOD + "`") is False
    assert "WOULD authorise" not in capsys.readouterr().out


def test_an_unclosed_region_with_no_token_is_still_reported(gate, capsys):
    """R858's exact signature: everything after the unclosed opener is quoted, INCLUDING an
    approval appended at the end of the file, which is the documented procedure."""
    assert gate("<code>\nprose that never closes it\n") is False
    out = capsys.readouterr().out
    assert "never closed" in out and "line 1" in out, out


def test_an_approval_for_another_review_id_says_so(gate, capsys):
    assert gate(GOOD.replace(ID, "AR-001")) is False
    out = capsys.readouterr().out
    assert "AR-001" in out and "not" in out, out


def test_the_diagnostic_does_not_appear_when_the_token_is_simply_absent(gate, capsys):
    assert gate("| AR-040 | resync_variables.py | PASS |") is False
    out = capsys.readouterr().out
    assert "WOULD authorise" not in out, out
    assert "never closed" not in out, out


# ------------------------------------------------------- raw-HTML block types 3, 4, 5

@pytest.mark.parametrize("name,body", [
    ("processing instruction", "<?php\n{r}\n?>"),
    ("declaration",            "<!DOCTYPE html\n{r}\n>"),
    ("CDATA",                  "<![CDATA[\n{r}\n]]>"),
    ("unclosed PI",            "<?php\n{r}"),
    ("unclosed CDATA",         "<![CDATA[\n{r}"),
    ("YAML front matter",      "---\n{r}\n---"),
])
def test_a_token_quoted_in_a_raw_block_does_not_authorise(gate, name, body):
    assert gate(body.format(r=GOOD)) is False, name


def test_a_closed_raw_block_does_not_quote_what_follows_it(gate):
    """The other direction: these must not become a new way to brick the gate."""
    assert gate("<?php echo 1; ?>\n" + GOOD) is True
    assert gate("<![CDATA[x]]>\n" + GOOD) is True
    assert gate("---\ntitle: notes\n---\n" + GOOD) is True


def test_a_bare_less_than_is_not_a_raw_block(gate):
    assert gate("2 < 3 and 4 > 1\n" + GOOD) is True


# ------------------------------------------------------------------ revocation spellings

@pytest.mark.parametrize("line", [
    "REVOKE-APPLY resync_variables.py " + SHA12 + " " + ID,
    "REVOKE" + chr(0x2011) + "APPLY resync_variables.py " + SHA12 + " " + ID,
    "REVOKE APPLY resync_variables.py " + SHA12 + " " + ID,
    "REVOKE_APPLY resync_variables.py " + SHA12 + " " + ID,
    "WITHDRAW-APPLY resync_variables.py " + SHA12 + " " + ID,
    "UNAPPROVE-APPLY resync_variables.py " + SHA12 + " " + ID,
    "RESCIND-APPLY resync_variables.py " + SHA12 + " " + ID,
    "CANCEL-APPLY resync_variables.py " + SHA12 + " " + ID,
])
def test_no_token_shaped_revocation_is_silently_ignored(gate, line):
    """Revocation fails OPEN. A spelling the parser cannot read must REFUSE, not pass in silence -
    we cannot tell who it was for, and the safe reading of 'somebody tried to withdraw this' is to
    stop."""
    assert gate(GOOD + "\n" + line) is False, line


@pytest.mark.parametrize("prose", [
    "I am not withdrawing this approval.",
    "The applying reviewer revoked nothing.",
    "Re-apply the patch before reading this row.",
    "This supersedes nothing; the earlier verdict stands.",
])
def test_innocent_prose_does_not_brick_the_gate(gate, prose):
    """The mirror of the test above, and the more dangerous direction. A mention pattern wide
    enough to catch free English would refuse on ordinary review prose - R858's failure with the
    sign flipped, and it would be found only when a legitimate approval could not be granted."""
    assert gate(prose + "\n" + GOOD) is True, prose


# ------------------------------------------------------------------ the live file, both ways

_LIVE = os.path.join(r"D:\research\hfdatalibrary", ".claude", "skills", "adversarial-review", "PASSED.md")


@pytest.mark.skipif(not os.path.exists(_LIVE), reason=f"needs the live {_LIVE}")
def test_the_live_file_still_grants_and_still_refuses(tmp_path):
    """R866 #2 reported this test as missing; it exists in test_resync_gate_quoting.py and passes.
    It is repeated here against the NEW bytes because every change to the parser can re-brick the
    gate against the one file it actually reads, and that is how R858 shipped."""
    live = io.open(_LIVE, encoding="utf-8").read()
    granted = tmp_path / "granted.md"
    granted.write_text(live.rstrip("\n") + "\n" + GOOD + "\n", encoding="utf-8")
    assert rv.reviewed_ok(ID, str(granted)) is True, "the live file quotes a genuine appended approval"
    plain = tmp_path / "plain.md"
    plain.write_text(live, encoding="utf-8")
    assert rv.reviewed_ok(ID, str(plain)) is False
