"""resync_variables.py v2 - make the served variables/quality objects agree with the served bars, one ticker at a
time, with a scoped snapshot, a measurement before, a full recompute, and an EXACT verification after.

WHY (2026-09-05, handoff section 15, ledger R741/R743/R745). The served clean variables and quality objects
disagree with the served clean 1-minute bars fleet-wide: on ten tickers outside the seam work they differ from a
fresh recompute with the pipeline's own compute_recent_days on 61-81 % of all sessions (AAPL 3,662 of 5,959) - the
daily path computes only the newest sessions (max_new=5) and never revisits older ones, so a re-clean that changed
the bars left the variables describing bars that no longer exist. The 47+ tickers rebased on 2026-09-05 are
consistent by construction (the seam tool re-syncs with force_full). This tool does the same for any ticker.

WHAT IT DOES for each ticker:
  1. downloads the served raw and clean 1-minute files;
  2. MEASURES: served variables vs compute_recent_days(bars, ticker, existing_dates=None, max_new=1e9) - the exact
     call sync_ticker_variables(force_full=True) makes - for raw and clean: sessions differing (relative tolerance
     1e-9, NO absolute tolerance: R745 - amihud_illiquidity is ~1e-11 and an absolute 1e-9 hid it), columns, dates.
     The dry run (default) stops here and prints it.
  3. with --apply --reviewed <PASSED.md line id> (validated against PASSED.md; never implied): a content-checked
     snapshot of THE FOUR OBJECTS THIS TOOL WRITES ({raw,clean}/{variables,quality}/<T>.parquet; R745: the seam
     tool's 22-object snapshot would restore bars this tool never touched), then the measured frames - the same
     frames force_full would write, prepared the same way - are uploaded directly (no second compute after the
     write flag: R745), then VERIFY: each object read back must equal its frame EXACTLY (every finite value equal,
     NaN where NaN, identical column lists, no extra sessions on either side) and the quality object must equal the
     variables' QUALITY_COLS subset; a mismatch restores the snapshot (exit 1), a failed restore exits 4, a read-back
     failure exits 3 (data live, nothing restored). Every outcome after the measurement is recorded in
     <snap_dir>/_RESULT.txt BEFORE it is printed (R735/R738), with this file's sha256 and the review id.
Exit codes (one meaning each, R731/R735): 0 done and verified | 1 written then restored | 2 nothing to do (already
consistent) | 3 unverifiable (read-back failed; data live) | 4 restore failed, or an escape after the first upload |
5 aborted before any write (missing input, snapshot check, unvalidated --reviewed, crash) | 7 deferred: the Daily
Data Update workflow is running/queued/unknown (nothing written; run again outside its window).

    python resync_variables.py TICKER [--apply --reviewed AR-0NN] [--snapshot-dir DIR] [--passed-file PATH]
Run from inside pipeline/ of a MAIN-based tree, under seam_rebase_batch.py --tool resync_variables.py for a list.
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import io
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _own_sha256() -> str:
    with open(os.path.abspath(__file__), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


_SOURCE_SHA = _own_sha256()          # taken at import, before anything else runs (R741)

from r2_client import get_client, download_parquet, download_to_buffer, upload_parquet   # noqa: E402
from compute_variables import compute_recent_days                                        # noqa: E402
from variables_sync import QUALITY_COLS                                                  # noqa: E402
import seam_rebase                                                                       # noqa: E402
from seam_rebase import _say, _record, md5_of, BUCKET                                    # noqa: E402

PASSED_DEFAULT = r"D:\research\hfdatalibrary\.claude\skills\adversarial-review\PASSED.md"


def keys_of(t: str) -> list[str]:
    return [f"{v}/{k}/{t}.parquet" for v in ("raw", "clean") for k in ("variables", "quality")]


def served_obj(client, key: str):
    data = download_to_buffer(client, key)
    return pd.read_parquet(io.BytesIO(data)) if data else None


def prepare(fresh: pd.DataFrame) -> pd.DataFrame:
    """Exactly what sync_ticker_variables(force_full=True) does to the computed frame before uploading it."""
    f = fresh.copy()
    f["trade_date"] = pd.to_datetime(f["trade_date"]).dt.normalize()
    return f.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)


def compare(served: pd.DataFrame | None, fresh: pd.DataFrame, exact: bool):
    """(sessions differing, {col: n}, common, only_served, only_fresh, first, last, columns_identical)
    exact=False (MEASURE): |a-b| <= 1e-9 |b|, NaN == NaN. exact=True (VERIFY): a == b, NaN == NaN, nothing else."""
    if served is None or served.empty:
        return None
    s = served.copy(); f = fresh.copy()
    s["trade_date"] = pd.to_datetime(s["trade_date"]); f["trade_date"] = pd.to_datetime(f["trade_date"])
    s = s.set_index("trade_date").sort_index(); f = f.set_index("trade_date").sort_index()
    common = s.index.intersection(f.index)
    cols_identical = list(s.columns) == list(f.columns)
    cols = [c for c in s.columns if c in f.columns]
    diff_days, colset = set(), {}
    for c in cols:
        x = s.loc[common, c]; y = f.loc[common, c]
        if np.issubdtype(x.dtype, np.number) and np.issubdtype(y.dtype, np.number):
            xv = x.astype(float).to_numpy(); yv = y.astype(float).to_numpy()
            both_nan = np.isnan(xv) & np.isnan(yv)
            ne = ~((xv == yv) | both_nan) if exact else ~np.isclose(xv, yv, rtol=1e-9, atol=0, equal_nan=True)
        else:
            ne = (x.astype(str).to_numpy() != y.astype(str).to_numpy())
        if ne.any():
            colset[c] = int(ne.sum()); diff_days.update(common[ne])
    return (len(diff_days), colset, len(common), int(len(s.index.difference(f.index))), int(len(f.index.difference(s.index))),
            min(diff_days).date().isoformat() if diff_days else None, max(diff_days).date().isoformat() if diff_days else None,
            cols_identical)


def verify_ok(r) -> bool:
    return bool(r) and r[0] == 0 and r[3] == 0 and r[4] == 0 and r[7]


def snapshot4(client, t: str, out_dir: str) -> int:
    """The four objects this tool writes, content-checked (size, MD5 vs ETag), manifest in seam_rebase's format so
    seam_rebase.restore() restores it. Never overwrites an earlier manifest (R731). Any failure raises SystemExit(5):
    nothing has been uploaded yet."""
    if os.path.exists(os.path.join(out_dir, "_MANIFEST.txt")):
        _say(f"  snapshot: {out_dir} already holds a _MANIFEST.txt from an earlier run - refusing to overwrite it (R731); "
             f"if that run ended with exit 4 restore it first, otherwise pass a fresh --snapshot-dir")
        raise SystemExit(5)
    os.makedirs(out_dir, exist_ok=True)
    keys = []
    for k in keys_of(t):
        try:
            h = client.head_object(Bucket=BUCKET, Key=k)
        except Exception as ex:                                       # noqa: BLE001
            _say(f"  snapshot: {k} is not served ({type(ex).__name__}) - aborting before any write"); raise SystemExit(5)
        keys.append((k, int(h["ContentLength"]), h["ETag"].strip('"')))
    for k, size, etag in keys:
        dest = os.path.join(out_dir, k.replace("/", "__"))
        client.download_file(BUCKET, k, dest)
        if os.path.getsize(dest) != size:
            _say(f"  snapshot: {k} size {size} on R2 vs {os.path.getsize(dest)} on disk - aborting before any write"); raise SystemExit(5)
        if "-" not in etag and md5_of(dest) != etag:
            _say(f"  snapshot: {k} MD5 {md5_of(dest)} != ETag {etag} - aborting before any write"); raise SystemExit(5)
    with open(os.path.join(out_dir, "_MANIFEST.txt"), "w", encoding="utf-8") as f:
        for k, size, etag in keys:
            f.write(f"{size}\t{etag}\t{k}\n")
    return len(keys)


import re as _re

# THE APPROVAL TOKEN. Four reviews (R745, R750, R752, R754) narrowed a gate that tried to INFER
# approval from a prose table, and the fifth (R755) found that my own fix for the fourth had deleted
# a guard while adding one. Every defeat came from the same place: inference over free text, where
# "PASSED" contains "PASS", a note can negate a verdict, and a FAIL cell can sit beside a PASS cell.
# So the gate no longer infers. A reviewer who clears this tool writes ONE line, exactly:
#
#     APPROVE-APPLY resync_variables.py <first 12 hex of this file's sha256> <review id>
#
# optionally followed by "#" and any comment. Nothing else authorises: not a verdict column, not the
# word PASS, not the file being called PASSED.md. The hash binds the approval to the reviewed bytes,
# so any edit to this file invalidates it, which is the point.
# AT COLUMN 0, no leading whitespace (R757 #3): the refusal message printed the required line indented,
# and the pattern allowed leading space, so pasting the tool's own refusal transcript into PASSED.md
# authorised it - and so did the token quoted as an indented example inside a FAIL row, a fenced code
# block, or an HTML comment. A real approval is written flush left and nowhere else.
APPROVE_RE_FMT = r"^APPROVE-APPLY resync_variables\.py {sha} (\S+)[ \t]*(?:#(.*))?$"
# AN APPROVAL AND A REVOCATION MUST FAIL IN OPPOSITE DIRECTIONS (R760 #2). The approve token is
# anchored at column 0 and fails CLOSED - a mis-typed approval simply does not authorise, which is
# safe. The revoke was anchored the same way and therefore failed OPEN: an indented `REVOKE-APPLY`,
# or one with a double space, was silently ignored and the approval it meant to withdraw still stood,
# with no diagnostic. A revocation is now honoured ANYWHERE on a line, at any indentation, with any
# run of whitespace between its fields and any text around it.
# The id is captured as an ID SHAPE or `*`, not as `\S+`. With `\S+` a revocation written inside
# inline code captured the trailing backtick into the id (`AR-038\`` != `AR-038`) and so was ignored -
# the exact hiding-by-quoting this pattern exists to prevent (R763 #2).
REVOKE_ANY_FMT = r"REVOKE-APPLY\s+resync_variables\.py\s+(?:{sha}|\*)\s+([A-Za-z]{{1,6}}-\d{{1,4}}|\*)"
# ...and a line that MENTIONS a revocation we could not parse refuses outright rather than passing in
# silence: we cannot tell who it was for, and the safe reading of "someone tried to withdraw this" is
# to stop.
# BROAD ON PURPOSE, and the third attempt at this line (R760 #2, R763 #3, R765 #1).
#
# Attempt 2 narrowed it to `REVOKE-APPLY\s+resync_variables\.py` so PASSED.md's own documentation of
# the syntax would not refuse every run. That let NINE shapes of genuine revocation through with no
# diagnostic at all - `REVOKE-APPLY pipeline/resync_variables.py ...` among them, which is the exact
# string PASSED.md used to name the tool. A revocation must fail OPEN; narrowing this is the same act
# as skipping a region, and both make a decision written in the file invisible.
#
# R765 prescribed paying at the SOURCE instead - writing the doc example with a look-alike hyphen so
# it cannot match. I did not do that, because the REVOKE example exists to be COPIED: a reviewer
# pasting a U+2011 token would write a revocation that matches neither the parser nor this warning,
# which is the silent failure this whole line exists to prevent, handed to the one person actually
# trying to withdraw an approval.
#
# The root fix is that the gate reads ONE file, so that file carries no example tokens: the syntax is
# documented in the skill's SKILL.md, which this tool never opens, and PASSED.md holds only a pointer.
#
# WHAT THAT COSTS, stated rather than hidden (R767 #5/#6). The design's invariant is "PASSED.md
# contains this token only as a real attempt", and it is NOT enforceable by CI: `.claude/` is
# gitignored (`.gitignore:56`), so PASSED.md and SKILL.md are untracked and no commit, test run or CI
# job can see them. `test_the_live_PASSED_file_holds_ZERO_example_tokens` checks it where the file
# actually lives and SKIPS elsewhere, which is the honest best available.
#
# So a stale revocation, a revocation naming ANOTHER tool, this token quoted in a PASS row, or the
# tool's own refusal transcript pasted back in will each refuse EVERY run. That is a denial of
# service - but a loud, diagnosable one: the tool names the file and the line number it objected to.
# THE FIX IS ALWAYS TO DELETE THE LINE, NEVER TO NARROW THIS MATCHER. Narrowing it is what R763 #2
# and R765 #1 both did, and both silently re-opened the revoke path - eleven shapes the second time.
REVOKE_MENTION_RE = _re.compile(r"REVOKE-APPLY", _re.I)
# PASSED.md is a MARKDOWN file that now documents this very syntax, so the tokens appear in it as
# EXAMPLES. Fenced blocks and inline-code spans are therefore stripped before anything is matched.
# This is structural, not another inference: an example lives in a fence or in backticks, a decision
# is written as live text. It also closes the last of R757 #3's class - a column-0 token inside a
# fenced block used to authorise - and it is what stops the documentation added for R760 #3 from
# making this tool refuse every run, which it did (measured 2026-09-05T20:51Z, before this fix).
# A FENCE HAS A CHARACTER, A LENGTH AND AN INDENT, and this used to be one boolean for two
# markers (R855 #1). Because a ``` line and a ~~~ line both flipped the same flag, a ~~~ INSIDE
# a ```-fence closed it and the token after was read as live text; four more shapes fell out of
# the same defect, including a longer fence opened and a shorter one nested. CommonMark: an
# opening fence is 3+ of ` or ~ indented 0-3 spaces; only a fence of the SAME character, at
# least as long, with no info string, closes it. Modelled, not special-cased - R763 #2 and
# R765 #1 both re-opened this path by narrowing a matcher instead of paying at the source.
# INDENTATION IS UNRESTRICTED, DELIBERATELY (R856 #1). CommonMark says a fence is indented
# 0-3 spaces and that 4+ is an indented code block instead - and modelling that faithfully
# WIDENED this guard: a fence indented four spaces stopped opening a quotation region, so a
# column-0 token between two of them started authorising where it had refused. A guard is not
# a renderer. Anything that LOOKS like a quoted block is quotation, so any leading whitespace
# opens a fence; what the previous version got wrong was the MARKER, not the indent.
FENCE_RE = _re.compile(r"^(?P<indent>\s*)(?P<marker>`{3,}|~{3,})(?P<info>.*)$")
# HTML COMMENTS ARE QUOTATION TOO, and PASSED.md uses them as its reviewer-note idiom - 21 of
# them today. A FAIL row whose footnote quoted the approval token AUTHORISED (R757 #3 named
# this shape ten rounds ago; the fence half was fixed and this half never was). Multi-line,
# so it needs the same open/closed state a fence does.
HTML_OPEN_RE = _re.compile(r"<!--")
HTML_CLOSE_RE = _re.compile(r"-->")
HTML_ONELINE_RE = _re.compile(r"<!--.*?-->", _re.S)
# ONE CONSTRUCT IS NOT THE FAMILY (R856 #2). PASSED.md holds zero code fences and 21 HTML
# comments - raw HTML IS its quoting idiom - and `<pre>`, `<code>` and `<details>` all still
# authorised after the comment fix. Same treatment: an opening tag starts a quotation region,
# its closing tag ends it, and an unclosed one swallows the rest of the file, which is the
# fail-closed direction.
HTML_BLOCK_OPEN_RE = _re.compile(r"<\s*(pre|code|details|summary|blockquote|xmp)\b", _re.I)
HTML_BLOCK_CLOSE_RE = _re.compile(r"<\s*/\s*(pre|code|details|summary|blockquote|xmp)\s*>",
                                  _re.I)
INLINE_CODE_RE = _re.compile(r"`[^`]*`")
REVOKING_COMMENT_RE = _re.compile(
    r"(\brevok\w*|\bsupersed\w*|\bwithdraw\w*|\brescind\w*|\bcancel\w*|\bvoid\b|\bobsolete\b"
    r"|\bexpired\b|\binvalid\b|\bnot\s+valid\b|\bno\s+longer\b|\bdo\s+not\s+use\b|\bdon'?t\s+use\b)",
    _re.I)
ID_RE = _re.compile(r"^[A-Za-z]{1,6}-\d{1,4}$")


def reviewed_ok(review_id: str, passed_file: str) -> bool:
    """Does PASSED.md carry an exact approval token for THESE bytes and THIS review id?

    This gate used to infer approval from a prose table, and four reviews in a row defeated the
    inference in a new way each time (R745, R750, R752, R754), with the fifth finding that my own fix
    for the fourth had deleted a guard while adding one (R755). The defeats were all the same shape:
    free text is not a decision. "PASSED" contains "PASS"; a note can negate a verdict; a FAIL cell
    can sit beside a PASS cell; a row clearing another tool can mention this one in passing.

    So nothing is inferred. A reviewer writes one line, exactly:

        APPROVE-APPLY resync_variables.py <sha12> <review id>          [# any comment]

    and only that authorises. The hash is this file's own, so editing the tool invalidates every
    approval of it - which is the property the whole gate exists for. A line for a different hash, a
    different tool, or a different id does not match, and no amount of surrounding prose can make it.
    """
    rid = (review_id or "").strip()
    if not ID_RE.match(rid):
        _say(f"  --reviewed {review_id!r} is not shaped like a review id (e.g. AR-037) - refused")
        return False
    sha12 = _SOURCE_SHA[:12]
    want = _re.compile(APPROVE_RE_FMT.format(sha=_re.escape(sha12)))
    revoke = _re.compile(REVOKE_ANY_FMT.format(sha=_re.escape(sha12)), _re.I)
    approved = False
    unparsed_revokes: list = []
    try:
        # utf-8-sig, because a BOM makes the FIRST line unmatchable and a first-line approval would
        # then be silently ignored - harmless for an approval, but it would also swallow a revocation.
        with open(passed_file, encoding="utf-8-sig", errors="replace") as fh:
            fence_char = None        # the marker CHARACTER of the open fence, or None
            fence_len = 0            # and its length; a closer must be at least this long
            in_html = False          # inside a multi-line <!-- ... -->
            html_block = 0           # depth of open <pre>/<code>/<details>/...
            for n, ln in enumerate(fh, 1):
                ln = ln.rstrip("\n")
                fm = FENCE_RE.match(ln)
                fence_marker = False
                if fm:
                    mk = fm.group("marker")
                    ch, ln_len = mk[0], len(mk)
                    info = fm.group("info").strip()
                    if fence_char is None:
                        fence_char, fence_len = ch, ln_len
                        fence_marker = True
                    elif ch == fence_char and ln_len >= fence_len and not info:
                        fence_char, fence_len = None, 0
                        fence_marker = True
                    # else: a different marker, a shorter one, or one carrying an info string is
                    # CONTENT inside the open fence - it does not close anything.
                in_fence = fence_char is not None

                # THE TWO PATHS SKIP DIFFERENT THINGS, AND THAT ASYMMETRY IS THE WHOLE POINT (R763 #2).
                # My first fence fix skipped fenced and inline-code regions before ANY matching, which
                # blinded the REVOKE path too: a genuine revocation inside a fence, after an unclosed
                # fence, or in backticks was silently ignored and the approval it withdrew still
                # stood - eight new shapes, worse than the defect it fixed. Approval must fail CLOSED,
                # so it ignores quoted text. Revocation must fail OPEN, so it reads EVERY line, fenced
                # or not, exactly as PASSED.md promises.
                r = revoke.search(ln)
                if r:
                    if r.group(1) in (rid, "*"):
                        _say(f"  --reviewed {rid}: {passed_file}:{n} carries a REVOKE-APPLY line for "
                             f"it - refused")
                        return False
                elif REVOKE_MENTION_RE.search(ln):
                    unparsed_revokes.append((n, ln.strip()[:90]))

                # ...and only now, for the APPROVE path, drop quotation.
                if in_fence or fence_marker:
                    continue
                # HTML comments, single- and multi-line. Blanked rather than dropped so the
                # column-0 approve anchor still sees the right columns on a line that carries a
                # comment beside live text.
                approve_ln = HTML_ONELINE_RE.sub(lambda m: " " * len(m.group(0)), ln)
                if in_html:
                    c = HTML_CLOSE_RE.search(approve_ln)
                    if c:
                        approve_ln = " " * c.end() + approve_ln[c.end():]
                        in_html = False
                    else:
                        continue                      # wholly inside a comment
                o = HTML_OPEN_RE.search(approve_ln)
                if o:
                    approve_ln = approve_ln[:o.start()]
                    in_html = True
                # RAW-HTML BLOCKS, same family as the comment. Counted rather than a boolean
                # so nesting (<details><pre>...) cannot close early, and an unclosed one
                # keeps the rest of the file quoted - fail closed.
                _opens = len(HTML_BLOCK_OPEN_RE.findall(approve_ln))
                _closes = len(HTML_BLOCK_CLOSE_RE.findall(approve_ln))
                if html_block or _opens:
                    html_block = max(0, html_block + _opens - _closes)
                    continue
                if _closes:
                    html_block = 0
                    continue
                ln = approve_ln
                # inline code is quotation, not decision; blanking it preserves column positions for
                # the column-0 approve anchor, so a token written as `APPROVE-APPLY ...` cannot pass
                ln = INLINE_CODE_RE.sub(lambda m: " " * len(m.group(0)), ln)
                m = want.match(ln)
                if m and m.group(1) == rid:
                    comment = (m.group(2) or "")
                    if REVOKING_COMMENT_RE.search(comment):
                        _say(f"  --reviewed {rid}: the approval line's own comment withdraws it "
                             f"({comment.strip()[:60]!r}) - refused")
                        return False
                    approved = True          # keep reading: a later REVOKE-APPLY still wins
    except OSError as ex:
        _say(f"  --reviewed {rid}: cannot read {passed_file} ({type(ex).__name__}) - refused")
        return False
    if unparsed_revokes:
        # Never pass over one of these in silence. We cannot tell which id it was meant for, and the
        # safe reading of "somebody tried to withdraw an approval here" is to stop.
        _say(f"  --reviewed {rid}: {passed_file} mentions REVOKE-APPLY on "
             f"{len(unparsed_revokes)} line(s) that do not parse as a revocation for this tool and "
             f"hash - refusing rather than guessing who they were for:")
        for n, text in unparsed_revokes[:5]:
            _say(f"      line {n}: {text}")
        _say(f"      a revocation reads: REVOKE-APPLY resync_variables.py {sha12} <id>   "
             f"(or * for the hash, or * for the id)")
        return False
    if approved:
        return True
    # NOTE the leading marker: the required line must sit at column 0, so this message cannot itself
    # be pasted into PASSED.md and authorise the tool (R757 #3).
    _say(f"  --reviewed {rid}: {passed_file} carries no approval line. It must read, at column 0:")
    _say(f"  >> APPROVE-APPLY resync_variables.py {sha12} {rid}")
    _say(f"  (the '>> ' above is not part of it). The hash is of the tool as it stands; if it was "
         f"edited after the review, it needs reviewing again.")
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewed", default=None, help="the PASSED.md line id of the adversarial review that cleared this tool for --apply")
    ap.add_argument("--snapshot-dir", default=None)
    ap.add_argument("--passed-file", default=PASSED_DEFAULT)
    try:
        a = ap.parse_args()
    except SystemExit as ex:                          # argparse exits 2, which here means "already consistent"
        raise SystemExit(5 if ex.code else ex.code)   # R750 finding 4: a usage error is an abort before any write
    t = a.ticker.upper()
    _say(f"  tool source sha256 {_SOURCE_SHA} ({os.path.abspath(__file__)}) pid {os.getpid()}; seam_rebase.py sha256 {seam_rebase._source_sha256()}")
    # the same sibling-hash contract the seam tool prints (R752 #3). The driver compares these against its
    # own start-time read and now treats their ABSENCE as a breach, so this line is not optional.
    seam_rebase._say_module_hashes("imported module sha256")
    if a.apply and not a.reviewed:
        _say("--apply needs --reviewed <PASSED.md id>: a rewrite of served objects runs only after its review (exit 5)"); return 5
    if a.apply and not reviewed_ok(a.reviewed, a.passed_file):
        _say(f"--reviewed {a.reviewed!r} is not on a line of {a.passed_file} that names resync_variables - refused (exit 5)"); return 5
    # the daily window is checked BEFORE the downloads and the two full computes (R750 finding 7): asking
    # afterwards spent ~50 s per ticker to learn the run should not have started
    if a.apply:
        st = seam_rebase.daily_run_state()
        if st in ("in_progress", "queued", "unknown"):
            _say(f"  DEFERRED (exit 7) before any read: Daily Data Update workflow is {st}; a resync inside its "
                 f"window is overwritten - run again outside it"); return 7
    client = get_client()
    bars = {}
    for version in ("raw", "clean"):
        b = download_parquet(client, version, t)
        if b is None or b.empty:
            _say(f"{t}: served {version} 1-minute file missing - aborted before any write (exit 5)"); return 5
        b["datetime"] = pd.to_datetime(b["datetime"]); bars[version] = b
    # MEASURE - the frames computed here are the ones uploaded under --apply (no second compute: R745)
    fresh, before, before_q = {}, {}, {}
    for version in ("raw", "clean"):
        fresh[version] = prepare(compute_recent_days(bars[version], t, existing_dates=None, max_new=10 ** 9))
        r = compare(served_obj(client, f"{version}/variables/{t}.parquet"), fresh[version], exact=False)
        before[version] = r
        # ALL FOUR OBJECTS THIS TOOL WRITES, NOT TWO (R856 #4). `keys_of()` is
        # {raw,clean}/{variables,quality}, and measuring only the variables meant a ticker with a
        # broken QUALITY object exited 2 - "already consistent, nothing to do" - and was never
        # repaired. Four of AAPL clean's 25 differing columns ARE QUALITY_COLS. The write path
        # already compares this object exactly (see VERIFY below); the measurement now uses the
        # same comparison at the same tolerance it uses for variables.
        _qc = [c for c in QUALITY_COLS if c in fresh[version].columns]
        _rq = compare(served_obj(client, f"{version}/quality/{t}.parquet"),
                      fresh[version][_qc], exact=False)
        before_q[version] = (_rq, _qc)
        if _rq is None:
            _say(f"  {version}/quality: served object missing")
        else:
            _qcols = sorted(_rq[1].items(), key=lambda kv: (-kv[1], kv[0]))
            _qshown = ", ".join(f"{c}({n:,})" for c, n in _qcols[:10]) or "none"
            _say(f"  {version}/quality:   {_rq[0]:,} of {_rq[2]:,} sessions differ; "
                 f"only-served {_rq[3]}, only-fresh {_rq[4]}; columns identical {_rq[7]}; "
                 f"{len(_qcols)} differing column(s), worst first: {_qshown}")
        if r is None:
            _say(f"  {version}/variables: served object missing")
        else:
            # THE COUNT AND THE CAP, and ordered by SEVERITY (R855 #2). This printed
            # `sorted(r[1])[:10]` - ten of twenty-five, alphabetically, with nothing
            # to say it had truncated - so the five worst columns (vr5, vr10,
            # dollar_volume, share_volume, num_trades) were exactly the ones dropped
            # and every list came out exactly ten long. `compare()` already returns
            # the per-column session count in r[1]; the caller was discarding it.
            _cols = sorted(r[1].items(), key=lambda kv: (-kv[1], kv[0]))
            _shown = ", ".join(f"{c}({n:,})" for c, n in _cols[:10]) or "none"
            _more = f" ... and {len(_cols) - 10} more" if len(_cols) > 10 else ""
            _say(f"  {version}/variables: {r[0]:,} of {r[2]:,} sessions differ from a fresh recompute ({r[5]}..{r[6]}); "
                 f"only-served {r[3]}, only-fresh {r[4]}; columns identical {r[7]}; {len(_cols)} differing column(s), "
                 f"worst first: {_shown}{_more}")
    # STALE AND CONSISTENT ARE DECIDED FROM ALL FOUR OBJECTS (R856 #4). A quality object
    # that is missing, short a column, or carrying stale sessions makes the ticker stale
    # even when its variables agree - otherwise the tool reports "nothing to do" about
    # two objects it is on the point of overwriting.
    stale = (sum((r[0] + r[3] + r[4]) if r else 0 for r in before.values())
             + sum((rq[0] + rq[3] + rq[4]) if rq else 0 for rq, _qc in before_q.values()))
    consistent = (stale == 0
                  and all(r is not None and r[7] for r in before.values())
                  and all(rq is not None and rq[7] for rq, _qc in before_q.values()))
    if not a.apply:
        _say("  already consistent - nothing to do" if consistent else
             "(dry run - the measurement above is the finding; pass --apply --reviewed <id> to rewrite)")
        return 2 if consistent else 0
    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_vars_{dt.datetime.now(dt.timezone.utc):%Y%m%d}", t)
    written = []
    # the try opens with the makedirs (R750 finding 5): the window between "the directory exists" and
    # "the guarded block starts" produced rc 5 with a directory and NO _RESULT.txt in it
    try:
        os.makedirs(snap_dir, exist_ok=True)           # every outcome from here on is recorded
        if consistent:
            _record(snap_dir, "EXIT 2 nothing to do: served variables equal a fresh recompute on every session and column")
            _say(f"  {t}: already consistent - nothing to do (exit 2)"); return 2
        st = seam_rebase.daily_run_state()
        if st in ("in_progress", "queued", "unknown"):
            _record(snap_dir, f"EXIT 7 DEFERRED: Daily Data Update workflow is {st}; nothing written")
            _say(f"  DEFERRED (exit 7): Daily Data Update workflow is {st}; a resync inside its window is overwritten - run again later"); return 7
        n_snap = snapshot4(client, t, snap_dir)
        _say(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")
        seam_rebase._STATE["wrote"] = True             # the first upload starts now
        for version in ("raw", "clean"):
            f = fresh[version]; qcols = [c for c in QUALITY_COLS if c in f.columns]
            upload_parquet(client, f, version, t, timeframe="variables"); written.append(f"{version}/variables/{t}.parquet")
            upload_parquet(client, f[qcols], version, t, timeframe="quality"); written.append(f"{version}/quality/{t}.parquet")
        # VERIFY: read back, EXACT equality with the frames just written (R745), every session and column, both objects
        bad = []
        for version in ("raw", "clean"):
            f = fresh[version]; qcols = [c for c in QUALITY_COLS if c in f.columns]
            try:
                srv = served_obj(client, f"{version}/variables/{t}.parquet")
                q = served_obj(client, f"{version}/quality/{t}.parquet")
            except Exception as ex:                                   # noqa: BLE001
                _record(snap_dir, f"EXIT 3 UNVERIFIABLE read-back {version}: {type(ex).__name__}: {str(ex)[:160]}; written {written}")
                _say(f"  UNVERIFIABLE, DATA LIVE: read-back of {version} failed ({type(ex).__name__}); nothing restored (exit 3)"); return 3
            r = compare(srv, f, exact=True); rq = compare(q, f[qcols], exact=True)
            ok_v = verify_ok(r); ok_q = verify_ok(rq) and q is not None and list(q.columns) == qcols
            _say(f"  VERIFY {version}/variables: {'OK' if ok_v else 'MISMATCH'} ({r[0] if r else 'missing'} differing of {r[2] if r else 0}; "
                 f"columns identical {r[7] if r else False})")
            _say(f"  VERIFY {version}/quality: {'OK' if ok_q else 'MISMATCH'} (columns {list(q.columns) if q is not None else 'missing'})")
            if not ok_v:
                bad.append((f"{version}/variables", r))
            if not ok_q:
                bad.append((f"{version}/quality", rq))
        if bad:
            raise RuntimeError("verify mismatch: " + "; ".join(f"{k} differing {r[0] if r else 'missing'} columns-identical {r[7] if r else False}" for k, r in bad))
    except BaseException as ex:                                       # noqa: BLE001
        why = f"{type(ex).__name__}: {str(ex)[:200]}"
        if not seam_rebase._STATE["wrote"]:
            _record(snap_dir, f"EXIT 5 aborted before any write: {why}")
            _say(f"  ABORTED before any write (exit 5): {why}"); return 5
        try:
            n_back = seam_rebase.restore(client, snap_dir)
        except BaseException as ex2:                                  # noqa: BLE001
            _record(snap_dir, f"EXIT 4 RESTORE FAILED after writing {written}; cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            _say(f"  FAILED ({why}) and the RESTORE FAILED ({type(ex2).__name__}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        _record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after writing {written}; cause {why}")
        _say(f"  FAILED ({why}) - restored {n_back} objects from {snap_dir}; served state is the pre-resync state (exit 1)"); return 1
    _record(snap_dir, f"EXIT 0 DONE variables+quality resynced raw+clean and verified exact; wrote {written}; reviewed {a.reviewed}; "
                      f"tool sha256 {_SOURCE_SHA}; seam_rebase.py sha256 {seam_rebase._source_sha256()}; before: "
                      + "; ".join(f"{k} {v[0] if v else 'missing'} stale" for k, v in before.items()))
    _say(f"  DONE: {t} variables and quality resynced from the served bars and verified exact; snapshot kept at {snap_dir}")
    return 0


def _guarded_main() -> int:
    st = seam_rebase._STATE
    try:
        return main()
    except SystemExit as ex:
        if isinstance(ex.code, int) or ex.code is None:
            raise
        _say(f"  {ex.code}"); return 4 if st["wrote"] else 5
    except BaseException as ex:                                       # noqa: BLE001
        _say(f"  {'ESCAPED AFTER WRITES - served state UNKNOWN (exit 4)' if st['wrote'] else 'ABORTED before any write (exit 5)'}: "
             f"{type(ex).__name__}: {str(ex)[:300]}"); return 4 if st["wrote"] else 5


if __name__ == "__main__":
    _rc = _guarded_main()
    # the trailer, after every lazy import has happened (R748 #5 / R752 #3): the driver reads the LAST
    # occurrence, so the detail file names the sibling bytes that actually ran
    seam_rebase._say_module_hashes("imported module sha256 at exit")
    sys.exit(_rc)
