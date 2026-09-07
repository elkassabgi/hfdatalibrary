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


def test_a_QUOTED_token_for_another_id_is_not_reported_as_present(gate, capsys):
    """R868 #6. `other_id` was read off the RAW line, so a token the parser had itself discarded
    inside a fence was announced as "line 2 carries an approval for review id 'AR-999'". A
    diagnostic that states a falsehood is worse than none, because a human acts on it."""
    assert gate("```\n" + GOOD.replace(ID, "AR-999") + "\n```") is False
    assert "AR-999" not in capsys.readouterr().out


@pytest.mark.parametrize("line,expect", [
    ("  " + GOOD, "indented"),
    (">> " + GOOD, "indented"),
    (GOOD.replace(SHA12, "0" * 12), "hash"),
    (GOOD.replace("resync_variables.py", "pipeline/resync_variables.py"), "column 0"),
])
def test_a_near_miss_is_named_rather_than_reported_as_absent(gate, capsys, line, expect):
    """R868 #5. The strict pattern is anchored at column 0 with an exact hash, so an indented
    token - including the one produced by pasting the tool's own `>> ` transcript - a stale sha,
    and a path-qualified filename all printed the identical "carries no approval line". That is
    the message R866 already failed this gate for."""
    assert gate(line) is False
    out = capsys.readouterr().out
    assert "nearly the token" in out, out
    assert expect in out, out


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


@pytest.mark.parametrize("shape", [
    "<!NOTE\n<script>\n{r}",
    "<!NOTE\n<pre>\n{r}",
    "<!NOTE\n<details>\n{r}",
    "<?php\n<pre> ?>\n{r}",
    "<![CDATA[\n<pre> ]]>\n{r}",
])
def test_a_raw_block_closer_cannot_swallow_the_tag_that_quotes_what_follows(gate, shape):
    """R868 #1, the catastrophic direction: these five REFUSED before the raw-block handler was
    added and AUTHORISED after it. A declaration runs to its first `>`, so `<!NOTE` on one line and
    `<script>` on the next made the closer consume the whole `<script>` TEXT - the tag never
    entered the stack, and the token below it was read as live. The two families are read
    independently now and their quoted regions union."""
    assert gate(shape.format(r=GOOD)) is False, shape


@pytest.mark.parametrize("shape", [
    "<?note see <![CDATA[x]]>\n{r}",      # earliest opener `<?` is UNCLOSED; CDATA after it closes
    "<![CDATA[x]]> then <!NOTE\n{r}",     # first-in-tuple closes, a LATER opener is left unclosed
    "<![CDATA[a]]> and <?php\n{r}",
])
def test_more_than_one_raw_block_on_a_line_is_read_in_full(gate, shape):
    """R868 #2. The scan took the first type in TUPLE order and stopped after ONE block per line,
    so a block that closed on the line could hide an unclosed opener beside it - either an earlier
    one it jumped over, or a later one it never looked for - and the next line's token authorised."""
    assert gate(shape.format(r=GOOD)) is False, shape


def test_a_line_whose_raw_blocks_all_close_does_not_quote_what_follows(gate):
    """The other direction. `<!DECL see <?php x ?>` is ONE declaration that ends at the first `>`
    - which is the `>` of `?>` - so nothing is left open and the next line is live. A rule that
    refuses here would be quoting on suspicion, and that is how R858 bricked the gate."""
    assert gate("<!DECL see <?php x ?>\n" + GOOD) is True
    assert gate("<![CDATA[a]]> <?php b ?> <!DECL c>\n" + GOOD) is True


@pytest.mark.parametrize("line", [
    # The CLAIMED id's own PASS row is the one row guaranteed to discuss it, and a rule that read
    # prose withdrew the gate from itself: measured, 2 of the live file's 36 ids (AR-025, AR-026)
    # could not be granted at all. R870 #1 - the plain-English branch is DELETED, not narrowed.
    ID + " is REVOKED - superseded by AR-044, do not use",
    "The " + ID + " approval is withdrawn.",
    "| " + ID + " | resync_variables.py | **FAIL** | ... the earlier verdict was superseded |",
    "<!-- " + ID + " (2026-09-07): the ecb expansion was WITHDRAWN after the review. -->",
    "AR-025 was withdrawn as unsafe; see the row above.",
    "AR-026 supersedes it and AR-025 is no longer valid.",
    # R870 #4: prose in this register is often shouted - 12 of the live file's 92 lines carry a
    # run of three or more ALL-CAPS words - so CAPS cannot be the token marker either.
    "A MISMATCH MUST CANCEL APPLY AND RESTORE.",
    "THE GUARD MUST REVOKE APPLY PERMISSION ONLY ON A REAL TOKEN.",
])
def test_prose_about_a_revocation_never_bricks_the_gate(gate, line):
    """A gate that cannot be GRANTED is R858, and it is worse than one that misses a prose
    revocation: no token means no approval anyway, so the default is already refusal."""
    assert gate(GOOD + "\n" + line) is True, line


@pytest.mark.parametrize("line", [
    "REVOKE-APPLY <tool>.py <sha12> <review id>",
    "REVOKE-APPLY",
    "Revoke-Apply resync_variables.py " + SHA12 + " " + ID,
    "> REVOKE-APPLY something",
    "  WITHDRAW_APPLY resync_variables.py",
    # R871 #1 - MID-LINE, and the anchor that used to hold this test dropped every one of them.
    # This file writes its records in TABLE ROWS and in `<!-- -->` notes, 21 of which exist, so a
    # withdrawal will be written mid-line far more often than at column 0. Measured against the
    # position anchor: 14 of 24 token-shaped mentions and 7 of 11 realistic withdrawals moved
    # REFUSE -> IGNORED, in the one direction the design says must never happen.
    "| " + ID + " | 2026-09-07 | REVOKE-APPLY | the approval is pulled |",
    "| " + ID + " | 2026-09-07 | REVOKE-APPLY resync_variabes.py * * |",   # one letter missing
    "<!-- " + ID + " (2026-09-07): REVOKE-APPLY resync_variables.py " + SHA12 + " " + ID + " -->",
    "  1. REVOKE-APPLY <tool>.py <sha12> <review id>",
    "2026-09-07 ae: REVOKE-APPLY resync_variables.py " + SHA12 + " " + ID,
    "the reviewer withdrew it in the row below - REVOKE-APPLY",           # hard wrap, token at EOL
    # ...and the documented unresolvable tie: spelled and placed exactly like `> REVOKE-APPLY
    # something` above, so it refuses. The remedy is to rephrase the line, never to narrow the
    # matcher - R763 #2 and R765 #1 both narrowed it and both re-opened the revoke path.
    "Withdraw-apply semantics are documented in SKILL.md.",
])
def test_a_revocation_MENTION_refuses_wherever_it_is_written(gate, line):
    """THE OPERAND is the discriminator, not position, not case and not only the tool name. A
    revocation is a statement about a file, a sha and a review id, so its verb is followed by one
    of those; prose puts an ordinary word there. A token-spelled verb that opens or closes its
    CELL counts too - that is the placeholder line and the hard-wrapped withdrawal, and a markdown
    cell is the unit because a table row is how this file writes its records."""
    assert gate(GOOD + "\n" + line) is False, line


@pytest.mark.parametrize("line", [
    "Cancel apply and restore whenever the two hashes disagree.",
    "- Cancel apply and restore on any mismatch (exit 1).",
    "Revoke apply is not the same as restore.",
    "> Cancel apply and restore, said the reviewer.",
])
def test_ordinary_English_at_the_start_of_a_line_does_not_brick_the_gate(gate, line):
    """R871 #3, the mirror direction: the position anchor refused these five ordinary sentences,
    which is R858's failure with the sign flipped - a gate that cannot be GRANTED. The spaced
    spelling is what saves them: only a hyphenated or underscored verb counts as a token when it
    carries no operand at all."""
    assert gate(GOOD + "\n" + line) is True, line


def test_a_parsed_revocation_for_another_id_does_not_silence_the_rest_of_its_line(gate):
    """R870 #3: `not revoke.search(ln)` let one well-formed revocation suppress every other shape
    beside it - R858 #4's defect, fixed on the parsing half and left standing on the mention half."""
    other = "REVOKE-APPLY resync_variables.py " + SHA12 + " AR-001"
    assert gate(GOOD + "\n" + other + " and REVOKE APPLY resync_variables.py") is False


@pytest.mark.parametrize("prose", [
    "I am not withdrawing this approval.",
    "The applying reviewer revoked nothing.",
    "Re-apply the patch before reading this row.",
    "This supersedes nothing; the earlier verdict stands.",
    "we cancel apply and restore.",
    "The cancel-apply path is exit 1.",
    "Do not revoke apply here without reading the note.",
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
