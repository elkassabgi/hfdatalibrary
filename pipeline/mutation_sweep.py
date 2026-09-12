"""AST-DERIVED mutation sweep — the answer to R793 #1.

full_sweep.py neutralised a list of clauses I TYPED, so it omitted 12 of 27 — including the final
child-hash compare, the clause the entire round was about. A sweep whose population is hand-written
inherits exactly the blindness it exists to remove; the reviewer's 19-of-27 against my 13-of-15 is
that gap measured.

So enumerate the population from the SOURCE. Every boolean test in the guard-bearing functions is a
candidate: each `if`/`elif` test, each operand of an `and`/`or`, each comparison assigned to a name
that a test later reads. For each, emit two mutants — the condition forced True and forced False —
run the scenarios against the REAL driver, and call the clause CAUGHT if either direction moves a
LOGGED code. Short-circuits on the first scenario that notices, so caught clauses cost seconds and
only genuine misses pay for the full sweep.

Copies only; the working tree is byte-verified unchanged (R758).
"""
import ast
import hashlib
import io
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))   # derived, never hardcoded, so the
                                                    # sweep travels with the code it measures
DRIVER = os.path.join(HERE, "seam_rebase_batch.py")
TESTS = os.path.join(HERE, "test_resync_review_gate.py")
BEFORE = hashlib.sha256(open(DRIVER, "rb").read()).hexdigest()

sys.path.insert(0, HERE)
import seam_rebase_batch as srb                                   # noqa: E402
import importlib.util                                             # noqa: E402
spec = importlib.util.spec_from_file_location("t_gate", TESTS)
tg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tg)

src = io.open(DRIVER, encoding="utf-8", newline="").read()
tree = ast.parse(src)
lines = src.splitlines(keepends=True)

# The functions that decide what gets LOGGED. `main` carries the write-path and no-write blocks.
# `_assert_guarded_matches_tool` is deliberately OUT: it raises SystemExit before any child runs, so
# its tests cannot move a logged code and including it buried the real population in 160 inert
# candidates. It has its own 58-case refuse/accept suite; this sweep is about the logged verdict.
TARGETS = {"_sibling_drift", "recode_on_drift", "main"}


def _seg(node):
    """Exact source slice for a node, using end positions (py3.8+)."""
    if node.lineno == node.end_lineno:
        return lines[node.lineno - 1][node.col_offset:node.end_col_offset]
    out = [lines[node.lineno - 1][node.col_offset:]]
    out += lines[node.lineno: node.end_lineno - 1]
    out.append(lines[node.end_lineno - 1][:node.end_col_offset])
    return "".join(out)


def _replace(node, text):
    """The whole file with `node`'s source replaced by `text`."""
    before = "".join(lines[:node.lineno - 1]) + lines[node.lineno - 1][:node.col_offset]
    after = lines[node.end_lineno - 1][node.end_col_offset:] + "".join(lines[node.end_lineno:])
    return before + text + after


candidates = []
for fn in ast.walk(tree):
    if not (isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and fn.name in TARGETS):
        continue
    # CONTROL-FLOW TESTS ONLY. The first run enumerated `ast.IfExp` too and reported `bad`,
    # `malformed` and `not real` as NOT CAUGHT at lines 448-450 - which are the `... if malformed
    # else ""` conditionals INSIDE the f-string that names the offender. Those are cosmetic by
    # design: neutralising one changes the message and nothing else, so counting them as guard
    # clauses understates the ratio exactly as R793 showed a hand-typed list overstates it. An
    # enumeration has to be derived AND scoped; deriving it is not sufficient on its own.
    for n in ast.walk(fn):
        tests = []
        if isinstance(n, (ast.If, ast.While)):
            tests.append(n.test)
        elif isinstance(n, ast.Assign) and isinstance(n.value, (ast.Compare, ast.BoolOp)):
            tests.append(n.value)                       # hash_ok = ..., fresh = ...
        for t in tests:
            parts = t.values if isinstance(t, ast.BoolOp) else [t]
            for p in parts:
                if isinstance(p, ast.Constant):
                    continue                            # already a literal
                candidates.append((fn.name, p))

# de-duplicate by source position
seen, uniq = set(), []
for name, node in candidates:
    key = (node.lineno, node.col_offset, node.end_lineno, node.end_col_offset)
    if key not in seen:
        seen.add(key)
        uniq.append((name, node))

print(f"driver sha256 {BEFORE[:12]}   {len(uniq)} boolean tests enumerated FROM THE AST "
      f"across {sorted(TARGETS)}")

names = sorted(tg._SCENARIOS)
baseline = {}


def logged(driver_src, scenario):
    d = pathlib.Path(tempfile.mkdtemp(prefix="astsw_"))
    io.open(d / "seam_rebase_batch.py", "w", encoding="utf-8", newline="").write(driver_src)
    for f in srb.GUARDED[1:]:
        shutil.copyfile(os.path.join(HERE, f), str(d / f))
    (d / "seam_rebase.py").write_text(tg._standin(tg._SCENARIOS[scenario][0]), encoding="utf-8")
    (d / "tick.txt").write_text("TEST\n", encoding="utf-8")
    log = d / "b.log"
    subprocess.run([sys.executable, str(d / "seam_rebase_batch.py"), "--apply",
                    "--tickers", str(d / "tick.txt"), "--log", str(log),
                    "--snapshot-root", str(d / "snaps")],
                   capture_output=True, text=True, cwd=str(d))
    code = None
    if log.exists():
        ls = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        code = int(ls[-1].split("\t")[2]) if ls else None
    shutil.rmtree(d, ignore_errors=True)     # AFTER reading it, not before
    return code


for s in names:
    baseline[s] = logged(src, s)
    assert baseline[s] == tg._SCENARIOS[s][1], f"baseline {s}: {baseline[s]} != {tg._SCENARIOS[s][1]}"
print(f"baseline agrees with the suite on all {len(names)} scenarios\n")

caught, misses, unmutatable = 0, [], []
for i, (fnname, node) in enumerate(uniq, 1):
    text = _seg(node).strip().replace("\n", " ")[:72]
    # CAUGHT MEANS **FORCED FALSE** MOVED SOMETHING (R798 #1, and it invalidated my published
    # ratio). Forcing a test True makes the guard fire MORE; a scenario noticing that proves the
    # clause is REACHABLE, not that its absence is detected. The fail-open I am hunting is the guard
    # not firing, which is the False direction alone. 13 of my previous 39 "CAUGHT" rows were
    # `CAUGHT (True -> ...)`, so the honest fail-open ratio was 26 of 82 - and two of those thirteen
    # were the very items the round was supposed to close. Both directions are still run, and
    # reported separately, because "reachable but undefended" is a different and useful state from
    # "never executed at all".
    hit_false = None
    mutated_false = _replace(node, "False")
    try:
        ast.parse(mutated_false)
        for s in names:
            if logged(mutated_false, s) != baseline[s]:
                hit_false = s
                break
    except SyntaxError:
        pass
    reachable = None
    if hit_false is None:
        mutated_true = _replace(node, "True")
        try:
            ast.parse(mutated_true)
            for s in names:
                if logged(mutated_true, s) != baseline[s]:
                    reachable = s
                    break
        except SyntaxError:
            pass
    if hit_false:
        caught += 1
        print(f"  [{i:2}/{len(uniq)}] {fnname}:{node.lineno} CAUGHT (False -> {hit_false})  {text}")
    else:
        misses.append((fnname, node.lineno, text, reachable))
        state = f"reachable via {reachable}, but its ABSENCE is undetected" if reachable else \
                "not exercised at all"
        print(f"  [{i:2}/{len(uniq)}] {fnname}:{node.lineno} ** NOT CAUGHT ** ({state})  {text}")

print(f"\nRATIO: {caught} of {len(uniq)} AST-enumerated boolean tests are CAUGHT — meaning forcing "
      f"the test FALSE moves a logged code, i.e. the guard's absence is detected.")
if misses:
    reach = [m for m in misses if m[3]]
    dead = [m for m in misses if not m[3]]
    print(f"\nNOT CAUGHT: {len(misses)} ({len(reach)} reachable but undefended, "
          f"{len(dead)} not exercised at all)")
    print("  REACHABLE BUT UNDEFENDED — a scenario reaches the line, nothing notices it going away:")
    for fnname, ln, text, r in reach:
        print(f"   * {fnname}:{ln}  {text}   (reached by {r})")
    print("  NOT EXERCISED — inert, unreachable, or needing a scenario shape that does not exist:")
    for fnname, ln, text, _r in dead:
        print(f"   * {fnname}:{ln}  {text}")

after = hashlib.sha256(open(DRIVER, "rb").read()).hexdigest()
print(f"\nworking tree UNCHANGED: {BEFORE == after}")
