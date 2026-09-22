"""Revert coverage for repair_reassigned.py: one test per shipped fix that goes GREEN when the fix
is removed (R878 #5, R883). No R2, no E:, no F:, no network - every root is stubbed or pointed
at tmp_path, the way `test_repair_apply_path._install` does it (R874/R877).

The five fixes, and what each test pins that the earlier tests did not:

  F9   the CUT-3 "too EARLY" sentence is printed ONLY when CUT-3 is among the blockers. Pinned in
       both directions: absent for a CUT-1/CUT-2 co-failure, present for a CUT-3 one, and the
       whole `ALSO FAILING` clause absent when CUT-4 fails alone.
  F10  the basis-gate line names its denominator. Pinned on the NUMBERS, with the real
       `basis_gate` over a tmp print store: 4 checks drawn from a pool of 6 (two sessions have no
       trades file), 2 of 6, and the R878 #6 case - a detached store that compared NOTHING must
       print `0 drawn`, not `1`.
  F12  the VERIFY (b) pre-flight asks Yahoo about the rebuild's own range, `cut..until`. Pinned on
       the arguments the probe receives with an `--until` that is NOT the window end, so both
       endpoints separate the fix from the old constant `WINDOW[1]-45d..WINDOW[1]`.
  G2   the two apply-only flags carry their help text VERBATIM. The earlier test asserted one
       word from each, and a revert that rewrote the first fragment of `--snapshot-dir`'s help
       left `_MANIFEST.txt` in place and the test green (R878 #5's own correction).
  G3   `yfinance` is a declared requirement, not a comment. `"yfinance" in text` is true of
       `# yfinance>=0.2`; a requirement parser is not.

Run from inside `pipeline/`, as CI does (`.github/workflows/tests.yml`): the sibling test module
is imported by basename.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import repair_reassigned as R                       # noqa: E402
import test_repair_apply_path as tap                # noqa: E402
from test_repair_apply_path import _install, T, CUT  # noqa: E402


def _run(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py"] + argv)
    try:
        return R._guarded_main()
    except SystemExit as ex:
        return ex.code


# ---------------------------------------------------------------- F9: the CUT-3 clause is conditional

EARLY = "a failing CUT-3 means the cut is too EARLY"


@pytest.mark.parametrize("rows,also,early", [
    # CUT-1 and CUT-2 both fail (neither carries the other), CUT-3 is OK: name the two, and say
    # NOTHING about the cut being too early - nothing measured that
    ([("CUT-1", "served discontinuity: 0 weekday(s)", "FAIL"),
      ("CUT-2", "12 silent sessions, floor 20", "FAIL"),
      ("CUT-3", "the first dropped session printed in the main pass", "OK"),
      ("CUT-4", "the last kept session is anchored by the MAIN pass", "FAIL")], "CUT-1, CUT-2", False),
    # CUT-2 and CUT-3 are n/a (no print store for those sessions): CUT-1 is named, CUT-3 is not
    ([("CUT-1", "served discontinuity: 0 weekday(s)", "FAIL"),
      ("CUT-2", "n/a - no print store", "n/a"),
      ("CUT-3", "n/a - outside the retained window", "n/a"),
      ("CUT-4", "the last kept session is anchored by the MAIN pass", "FAIL")], "CUT-1", False),
    # the positive control: CUT-3 really failed, so the sentence that explains it must be there
    ([("CUT-3", "the first dropped session printed in the main pass", "FAIL"),
      ("CUT-4", "the last kept session is anchored by the MAIN pass", "FAIL")], "CUT-3", True),
    # CUT-4 alone: no ALSO FAILING clause at all, and no CUT-3 sentence
    ([("CUT-1", "served discontinuity: 61 weekday(s)", "OK"),
      ("CUT-2", "142 silent sessions", "OK"),
      ("CUT-3", "the first dropped session printed in the main pass", "OK"),
      ("CUT-4", "the last kept session is anchored by the MAIN pass", "FAIL")], None, False),
], ids=["CUT-1+CUT-2 also fail", "CUT-1 also fails, CUT-3 n/a", "CUT-3 also fails", "CUT-4 alone"])
def test_the_too_EARLY_sentence_appears_only_when_CUT_3_is_among_the_blockers(monkeypatch, tmp_path, capsys, rows, also, early):
    """R876 #3: across the 38 reachable verdict maps the unconditional clause explained CUT-3 on
    four where CUT-3 had not failed. Reverting the condition makes the first two cases here print
    the sentence again; the last two are the controls that hold in every version."""
    _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "cut_gate", lambda *a, **k: (list(rows), False))
    code = _run(monkeypatch, [T, "--cut", CUT.isoformat()])
    out = capsys.readouterr().out
    assert code == 2, out
    tail = out.split("REFUSED:")[-1]
    assert "--rebuild-from-cs" in tail, f"not the CUT-4 branch: {tail}"
    if also is None:
        assert "ALSO FAILING" not in tail, tail
    else:
        assert f"ALSO FAILING: {also} - read those rows above as well" in tail, tail
    assert (EARLY in tail) is early, tail


# ---------------------------------------------------------------- F10: the basis-gate pool denominator

WIN_DAYS = [dt.date(2023, 6, 5) + dt.timedelta(days=k) for k in (0, 1, 2, 3, 4, 7)]   # six weekday sessions, all < CUT


def _flat_frame(days):
    """Three bars a session, close 10.0 and 100 shares each, so a session reads (10.0, 300) - the
    same shape `test_repair_reassigned.py` gives the real basis gate."""
    rows = []
    for d in days:
        for m in range(3):
            rows.append({"datetime": pd.Timestamp(d) + pd.Timedelta(hours=10, minutes=m),
                         "Open": 10.0, "High": 10.0, "Low": 10.0, "Close": 10.0, "Volume": 100,
                         "source": "iex"})
    return pd.DataFrame(rows)


@pytest.mark.parametrize("files_for,samples,line,code", [
    (4, 4, "basis gate (4 check(s), 4 drawn from a pool of 6 kept window session(s)) -> OK", 0),
    (6, 2, "basis gate (2 check(s), 2 drawn from a pool of 6 kept window session(s)) -> OK", 0),
    # R878 #6: the detached-store refusal row is a "prints" row with no date; it is a check, not a
    # comparison, so the numerator must be 0 - and the gate FAILS, exit 2
    (0, 4, "basis gate (1 check(s), 0 drawn from a pool of 6 kept window session(s)) -> FAIL", 2),
], ids=["4 of 6", "2 of 6", "0 of 6"])
def test_the_basis_gate_line_counts_checks_against_the_kept_window_pool(monkeypatch, tmp_path, capsys, files_for, samples, line, code):
    """R876 #4: "7 check(s) -> OK" read identically whether the window rows were drawn from 688
    candidates or from 3. The REAL `basis_gate` runs here over a tmp print store holding trades
    files for only some of the six kept window sessions, so the pool (6) and the checks drawn from
    it differ, and the line has to say both. Reverting the denominator leaves the old
    `basis gate (N check(s)) -> ...` and every case here fails."""
    real_basis_gate = R.basis_gate                     # captured BEFORE _install stubs it
    monkeypatch.setattr(tap, "SERVED", _flat_frame(WIN_DAYS + tap.FOREIGN))
    _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "basis_gate", real_basis_gate)
    store = tmp_path / "prints"
    for d in WIN_DAYS[:files_for]:
        ymd = d.strftime("%Y%m%d")
        (store / ymd).mkdir(parents=True)
        (store / ymd / f"trades_{ymd}.csv").write_text("", encoding="utf-8")
    monkeypatch.setattr(R, "CS_ROOT", str(store))
    monkeypatch.setattr(R, "SNAP_0713", str(tmp_path / "no_snapshot"))       # never F:
    monkeypatch.setattr(R, "_print_anchor", lambda d, t, cs=None: (10.0, 300, 3, {}, "main"))
    got = _run(monkeypatch, [T, "--cut", CUT.isoformat(), "--basis-samples", str(samples)])
    out = capsys.readouterr().out
    assert got == code, out
    assert line in out, out


# ---------------------------------------------------------------- F12: the pre-flight range is cut..until

@pytest.mark.parametrize("until", [dt.date(2026, 3, 20), R.WINDOW[1]], ids=["until inside the window", "until at the window end"])
def test_the_oracle_preflight_probes_exactly_cut_to_until(monkeypatch, tmp_path, capsys, until):
    """R876 #2, the range half. The old probe asked about a constant `WINDOW[1]-45d..WINDOW[1]`
    (2026-02-10..2026-03-27), a period the run may never touch; the fix asks about `cut..until`.
    The first case moves BOTH endpoints away from the constant. The probe returns no sessions, so
    the run refuses at exit 5 before any write and names the range it asked about."""
    calls = _install(monkeypatch, "healthy", 22)
    monkeypatch.setattr(R, "last_cs_session", lambda sym, tkr, limit=15: until)   # the --until gate, off E:
    seen = []

    def probe(symbol, start, end):
        seen.append((symbol, start, end))
        return pd.Series(dtype=float)
    monkeypatch.setattr(R, "_yahoo_close", probe)
    code = _run(monkeypatch, [T, "--cut", CUT.isoformat(), "--apply",
                              "--verify-against", "B", "--rebuild-from-cs", "B",
                              "--until", until.isoformat(),
                              "--snapshot-dir", str(tmp_path / "range")])
    out = capsys.readouterr().out
    assert code == 5, out
    assert calls["uploads"] == [], "nothing may be written when the oracle answers nothing"
    assert seen == [("B", CUT, until)], seen
    assert f"Yahoo returned no sessions for B between {CUT} and {until}" in out, out
    assert "2026-02-10" not in out, f"the old constant range is still in use: {out}"


# ---------------------------------------------------------------- G2: the two --help strings, verbatim

SNAPSHOT_DIR_HELP = (
    "where --apply keeps the pre-write snapshot (default F:/hf_r2_snapshot_reassigned_<utc-ymd>/<TICKER>). "
    "A directory that already holds a _MANIFEST.txt is REFUSED by snapshot()'s R731 guard, so a "
    "collision cannot silently overwrite a kept snapshot - do not point two runs at one directory.")
ALLOW_QUEUED_HELP = (
    "proceed when the Daily Data Update workflow is QUEUED (not in_progress, not unknown - those "
    "are never overridable). The workflow can then start mid-repair and overwrite the write. Use "
    "only when the queue is known to be waiting on an unrelated job; the run prints and records "
    "that the override fired.")


def _one_line(text: str) -> str:
    """argparse re-wraps help at the terminal width and may break a hyphenated word across lines;
    fold the whitespace and re-join `word-<newline>word` so the comparison is on the words."""
    text = " ".join(text.split())
    return re.sub(r"(?<=\w)- (?=\w)", "-", text)


def test_the_two_apply_only_flags_print_their_help_verbatim(monkeypatch, capsys):
    """R872 #4 / R876 #6: `--help` is the only place an operator meets the flag that relaxes the
    write-window gate. The earlier test asserted `QUEUED` and `_MANIFEST.txt`, one word from each
    string, and a revert that rewrote the opening fragment of `--snapshot-dir`'s help stayed green
    (R878 #5). The whole of both strings is asserted here, in-process rather than by subprocess."""
    monkeypatch.setenv("COLUMNS", "1000")       # widen argparse so it does not wrap these at all
    monkeypatch.setattr(sys, "argv", ["repair_reassigned.py", "--help"])
    with pytest.raises(SystemExit) as ex:
        R.main()
    assert ex.value.code == 0
    out = _one_line(capsys.readouterr().out)
    assert "--snapshot-dir" in out and "--allow-queued" in out, out
    assert _one_line(SNAPSHOT_DIR_HELP) in out, out
    assert _one_line(ALLOW_QUEUED_HELP) in out, out


# ---------------------------------------------------------------- G3: yfinance is a requirement, not a comment

def _requirement_names(text: str) -> set:
    """The project names of a requirements file: comments and blank lines dropped, option lines
    (`-r`, `--index-url`) skipped, the name normalised the way pip does (`_` -> `-`, lower)."""
    names = set()
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        m = re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", line)
        if m:
            names.add(m.group(0).lower().replace("_", "-"))
    return names


def test_the_requirement_parser_does_not_count_a_commented_line():
    """The control for the test below: a parser that counted comments would pass either way."""
    assert _requirement_names("pandas>=2.0\n# yfinance>=0.2\n  # numpy\n-r other.txt\n") == {"pandas"}


def test_yfinance_is_a_declared_requirement_not_a_comment():
    """R876 #6: VERIFY (b) is mandatory with --verify-against and imports yfinance lazily, so the
    dependency is invisible until the run is past its gates. `"yfinance" in text` is still true of
    `# yfinance>=0.2`, which is exactly the revert that left the suite green (R878 #5)."""
    req = open(os.path.join(HERE, "requirements.txt"), encoding="utf-8").read()
    names = _requirement_names(req)
    assert {"pandas", "pyarrow"} <= names, names          # the parser sees the file at all
    assert "yfinance" in names, f"yfinance is not a live requirement line: {names}"
    src = open(os.path.join(HERE, "repair_reassigned.py"), encoding="utf-8").read()
    assert "import yfinance" in src, "the requirement is load-bearing only while the tool imports it"
