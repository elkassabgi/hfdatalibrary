"""The approval gate must ignore EVERY form of quotation, and the revoke gate must ignore none.

R855 #1 measured the asymmetry that had gone unnoticed for ten rounds: the shipped 8-shape
quoting list (`test_resync_review_gate.py`) is parametrised over the REVOKE direction only. Run
against the GRANT direction, six shapes it does not contain AUTHORISE:

    ```-open with ~~~ inside                       AUTHORISED
    ~~~-open with ``` inside                       AUTHORISED
    ````-open with ``` inside                      AUTHORISED
    ```-open with an indented ~~~ inside           AUTHORISED
    a multi-line <!-- ... TOKEN ... -->            AUTHORISED
    PASSED.md's own FAIL-row footnote idiom        AUTHORISED

The cause was one boolean for two fence markers - `^\\s*(```|~~~)` flipped the same flag whichever
character it saw, at any indent, at any length - and no knowledge of HTML comments at all. The
live `PASSED.md` carries 21 HTML-comment footnotes as its established reviewer-note idiom, and
R757 #3 named the HTML-comment shape ten rounds ago; the fence half was fixed and this half was
not mentioned again.

THE PAYMENT IS AT THE SOURCE (R763 #2, R765 #1 - both re-opened this path by narrowing a matcher
instead). A fence now has a CHARACTER, a LENGTH and an INDENT, and only a fence of the same
character, at least as long, carrying no info string, closes one. HTML comments get the same
open/closed state a fence does.

THIS FILE RUNS ONE LIST IN BOTH DIRECTIONS, which is the thing nobody had done.
"""
from __future__ import annotations

import importlib.util
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(_HERE, "resync_variables.py")

spec = importlib.util.spec_from_file_location("_rv_quoting", _TOOL)
rv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rv)

ID = "AR-040"
SHA12 = rv._SOURCE_SHA[:12]
GOOD = f"APPROVE-APPLY resync_variables.py {SHA12} {ID}"
REVOKE = f"REVOKE-APPLY resync_variables.py {SHA12} {ID}"

# ONE list, exercised in BOTH directions. The first eight are the shipped list; the rest are the
# shapes R855 measured, including the file's own footnote idiom.
QUOTINGS = [
    ("plain fence",                 "```\n{r}\n```"),
    ("fence with info string",      "```python\n{r}\n```"),
    ("tilde fence",                 "~~~\n{r}\n~~~"),
    ("four backticks",              "````\n{r}\n````"),
    ("unclosed fence",              "```\nunclosed fence, then:\n{r}"),
    ("odd number of markers",       "```\na\n```\n```\nb\n```\n```\n{r}"),
    ("inline code",                 "`{r}`"),
    ("after an inline span",        "text before `x` then {r}"),
    # --- R855 #1: the six the shipped list does not contain -----------------------------
    ("backtick open, tilde inside", "```\n~~~\n{r}\n```"),
    ("tilde open, backtick inside", "~~~\n```\n{r}\n~~~"),
    ("longer open, shorter inside", "````\n```\n{r}\n````"),
    ("indented closer inside",      "```\n  ~~~\n{r}\n```"),
    ("multi-line html comment",     "<!--\nnote:\n{r}\n-->"),
    ("PASSED.md footnote idiom",
     "| AR-039 | resync_variables.py | **FAIL** |\n"
     "<!-- the line that WOULD have cleared it is\n{r}\nand it was not written. -->"),
    # --- R856: my own fence fix WIDENED the grant path, and one HTML construct is not the family.
    # Modelling CommonMark's 0-3 space rule stopped an indented fence from opening a quotation
    # region, so a column-0 token between two of them started authorising where it had refused.
    # And PASSED.md holds ZERO code fences and 21 HTML comments - raw HTML IS its quoting idiom,
    # so <pre>, <code> and <details> belong to the same family as the comment.
    ("fence indented 4 spaces",     "    ```\n{r}\n    ```"),
    ("fence indented 8 spaces",     "        ~~~\n{r}\n        ~~~"),
    ("fence indented by a tab",     "\t```\n{r}\n\t```"),
    ("pre block",                   "<pre>\n{r}\n</pre>"),
    ("code block",                  "<code>\n{r}\n</code>"),
    ("details/summary",             "<details><summary>why</summary>\n{r}\n</details>"),
    ("blockquote tag",              "<blockquote>\n{r}\n</blockquote>"),
    ("details wrapping a pre",      "<details>\n<pre>\n{r}\n</pre>\n</details>"),
]


@pytest.fixture
def gate(tmp_path):
    def go(body: str, review_id: str = ID) -> bool:
        p = tmp_path / "PASSED.md"
        p.write_text(body, encoding="utf-8")
        return rv.reviewed_ok(review_id, str(p))
    return go


@pytest.mark.parametrize("name,wrap", QUOTINGS, ids=[n for n, _ in QUOTINGS])
def test_a_quoted_approval_never_authorises(gate, name, wrap):
    """GRANT fails CLOSED: quotation is an example, not a decision."""
    assert gate(wrap.format(r=GOOD)) is False, (
        f"{name}: a quoted approval token authorised a live run")


@pytest.mark.parametrize("name,wrap", QUOTINGS, ids=[n for n, _ in QUOTINGS])
def test_a_quoted_revocation_is_never_hidden(gate, name, wrap):
    """REVOKE fails OPEN: a withdrawal is honoured wherever it is written. The two directions
    must not share a skip, which is exactly the regression R763 #2 records."""
    body = wrap.format(r=REVOKE)
    assert gate(f"{GOOD}\n{body}") is False, (
        f"{name}: a revocation was hidden by quoting and the approval still stood")


def test_the_control_still_authorises(gate):
    """A guard that refuses everything is not a guard. One unquoted line must work."""
    assert gate(GOOD) is True


def test_a_fence_that_does_not_close_stays_open(gate):
    """The defect in miniature: a `~~~` inside a ```-fence used to close it, so everything after
    was read as live text."""
    assert gate("```\n~~~\n" + GOOD + "\nstill inside\n```") is False


def test_a_closer_needs_the_same_character_and_length(gate):
    assert gate("````\n```\n" + GOOD + "\n````") is False
    assert gate("~~~~\n~~~\n" + GOOD + "\n~~~~") is False


def test_an_info_string_cannot_close_a_fence(gate):
    """CommonMark: a closing fence carries no info string. ```` ```python ```` opens, never closes."""
    assert gate("```\n```python\n" + GOOD + "\n```") is False


def test_a_comment_beside_live_text_blanks_only_the_comment(gate):
    """Blanked, not dropped: the column-0 anchor must still see the right columns."""
    assert gate(GOOD + " <!-- approved after the fourth review -->") is True
    assert gate("<!-- " + GOOD + " -->") is False


def test_an_unclosed_html_comment_swallows_the_rest(gate):
    """Failing closed: if a comment is opened and never closed, everything after it is quotation."""
    assert gate("<!-- note\n" + GOOD) is False


def test_an_indented_fence_still_opens_a_quotation_region(gate):
    """R856 #1, and it was MY regression. Modelling CommonMark's 0-3 space rule dropped the
    indented case that `^\\s*` had been covering by accident, so a fence indented four spaces
    stopped opening anything and the column-0 token between two of them AUTHORISED where it had
    refused. A guard is not a renderer: anything that looks like a quoted block is quotation."""
    for indent in ("    ", "        ", "\t", "  \t "):
        assert gate(f"{indent}```\n{GOOD}\n{indent}```") is False, repr(indent)


def test_the_raw_html_family_is_treated_as_one(gate):
    """R856 #2. The comment fix modelled ONE construct in a file whose only quoting idiom is raw
    HTML - zero code fences, 21 HTML comments."""
    for tag in ("pre", "code", "details", "blockquote", "xmp"):
        assert gate(f"<{tag}>\n{GOOD}\n</{tag}>") is False, tag


def test_an_unclosed_html_block_swallows_the_rest(gate):
    """Fail closed, like the unclosed comment and the unclosed fence."""
    assert gate("<pre>\n" + GOOD) is False


def test_nested_html_blocks_do_not_close_early(gate):
    """Counted, not a boolean: </pre> must not release a still-open <details>."""
    assert gate("<details>\n<pre>\nx\n</pre>\n" + GOOD + "\n</details>") is False
