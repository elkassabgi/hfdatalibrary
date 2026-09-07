"""seam_rebase_batch.py — drive seam_rebase.py (v5, self-measuring) across the served population,
one ticker at a time. The tool decides per ticker; this driver only sequences and records.

    python seam_rebase_batch.py [--apply] [--limit N] [--tickers A,B,...|FILE] [--candidates CSV] [--events-file CSV]

Candidates: every ticker with a PiTrading/IEX seam window — from seam_measure_K.py's CSV (rows
whose flag does not say no_seam_window), or the explicit --tickers list / one-per-line file. The
tool re-measures each one, so a stale factor in the CSV cannot reach the data.

Exit codes from seam_rebase.py and what the driver does with each (one meaning per code, R731/R735):
    0  rebased and verified, or nothing to rebase / already on target -> record, continue
    2  refused before any write (measurement says no)                  -> record, continue (manual list)
    3  unmeasurable (no market reference / reassigned symbol)          -> record, continue (disclose list)
    5  aborted before any write (missing object, snapshot check, crash) -> record, continue (retry list)
    6  prices verified, variables/quality sync FAILED                  -> record, continue, LIST at the end
    1  written then RESTORED from its snapshot; served = pre-rebase    -> record, STOP (read why)
    4  served state UNKNOWN: restore failed, an inconsistent set, or a
       hash/sibling breach (unidentified code ran, so "no write" is not
       a fact)                                                        -> record, STOP; --restore first
    7  deferred: the daily window (resync_variables.py only)            -> record, continue, LIST at the end
--tool resync_variables.py drives that tool instead (same header, same record grammar, its own snapshot);
--mode / --convention-decided / --events-file are seam_rebase.py's; --reviewed is resync_variables.py's.
The driver refuses to START while the log's last line is an exit 4. THE CHILD IS NOT KILLED BY AN
INTERRUPT (R735): it runs in its own process group, and a Ctrl-C on the driver waits for the running
ticker to finish (the tool restores itself if it must), logs it, and then stops the batch. One line
per ticker is appended to the log AFTER the tool exits — never before — with a UTC-labelled stamp
(ledger R591/R729), and the ticker's FULL stdout+stderr goes to a detail file next to the log
(seam_detail_<UTC>_<TICKER>.txt), so the VERIFY lines exist somewhere. Re-runs skip tickers already
logged with exit 0. --mode full needs --convention-decided on the driver too; it is never implied.
THE SOURCE GUARD (R742/R743/R744): the tool AND the five modules it imports live (aggregate, r2_client,
variables_sync, compute_variables, symbol_map) are hashed at start and before every launch; any change
stops the batch before the next child. The child must also print that hash and its own reading of the
same six modules, as a header and again at exit, and all of those readings must agree with the driver's.
ANY BREACH - a wrong header hash, no header, a missing or incomplete sibling line, or two readings that
disagree - IS LOGGED 4 (served state unknown) and stops the batch, whatever the child's own exit code
said and whether or not it printed a snapshot line. The reason is that under a breach the child's stdout
is untrustworthy in both directions: a missing snapshot line is not evidence that no write happened,
because the code that prints it is the code that changed. Exit 5 in the LOG therefore has exactly one
producer: a CLEAN child's own honest "aborted before any write". (The driver's own pre-child refusals
- an unreadable guarded module, a trailing exit 4, a bad argument - return 1 and write no log line at
all; R760 corrected this sentence, which used to claim they were the other producer of 5.)
"""
from __future__ import annotations
import argparse, datetime as dt, os, re, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
# CONDITION-NEUTRAL ON PURPOSE (R765 #6). This string is appended LAST to STOP_TEXT[4], so whatever
# it says has the final word. It used to say "with code 0 if the restore succeeded", which flatly
# contradicts the branch immediately above it for the case where no snapshot exists and there is
# nothing to restore from - and the contradiction was the last thing the operator read. It now names
# the condition that is true in BOTH cases: you established that served state is correct.
RELEASE = ("To release the batch: append ONE line to the log in its own format - "
           "<UTC stamp>\\t<TICKER>\\t<code>\\t0s\\t<note> - with code 0 ONLY if you have ESTABLISHED "
           "THAT SERVED STATE IS CORRECT, by whichever route applies above (a restore that succeeded, "
           "or your own comparison against the store when there was nothing to restore). Say which in "
           "the note, e.g. `2026-09-05T13:00:00Z\\tXYZ\\t0\\t0s\\trestored by hand from <snap dir>` or "
           "`...\\t0\\t0s\\tno snapshot; served == store, verified by hand`. Any other code keeps the refusal.")

STOP_TEXT = {
    1: ("STOPPING: the last ticker was written and then RESTORED from its snapshot (a failure between the first "
        "upload and the last VERIFY line, or a VERIFY mismatch); served state is the pre-rebase state. Read why "
        "before continuing."),
    4: ("STOPPING - SERVED STATE UNKNOWN or INCONSISTENT. One of: the last ticker was written AND its automatic restore "
        "failed; the tool found a partial earlier write; or a HASH/SIBLING BREACH means unidentified code ran to completion, "
        "so nothing it printed about writing - including 'nothing to rebase' and a missing snapshot line - can be believed. "
        "Check the snapshot directory first: if it holds objects, run the printed --restore command and then release with "
        "RELEASE below. IF IT IS ABSENT OR EMPTY there is nothing to restore from, so RELEASE's wording does not apply: "
        "compare the ticker's served objects against the store yourself, and append code 0 only if you established that "
        "served state is correct, not that a restore succeeded (R763 #8). " + RELEASE),
}


# EXIT 3 MEANS OPPOSITE THINGS IN THE TWO TOOLS (R855 #3). For seam_rebase.py it is a benign
# refusal - no market reference - and the batch should carry on. For resync_variables.py it is
# "UNVERIFIABLE, DATA LIVE ... nothing restored": all four objects were uploaded and then could
# not be verified. Filing that under `unmeasurable` and continuing marches the run through the
# remaining tickers while one ticker's served state is unknown. R750 finding 3 named exactly this
# class for exit 2 and split it per tool; exit 3 was left behind.
STOP_TEXT_RESYNC = {
    3: ("STOPPING - THE LAST TICKER'S FOUR OBJECTS ARE WRITTEN AND UNVERIFIED. resync_variables.py "
        "exit 3 is 'UNVERIFIABLE, DATA LIVE - nothing restored': the uploads succeeded and the "
        "read-back comparison did not complete, so served state for that ticker is UNKNOWN and it "
        "is NOT the pre-resync state. Do not treat this as a skip.\n"
        "  WHAT A PLAIN RE-RUN PROVES, AND WHAT IT DOES NOT (R856 #3): a re-run reaches MEASURE "
        "first, which compares at rtol=1e-9 - NOT the exact equality the failed VERIFY used - and "
        "on agreement returns 2, printing 'already consistent - nothing to do' directly beneath "
        "this message. That is a weaker test answering a different question, and reading it as "
        "reassurance is how an unknown served state becomes a closed one.\n"
        "  WHAT SETTLES IT: (a) restore from the snapshot directory named in the ticker's "
        "_RESULT.txt, which puts back objects whose size and ETag are verified against the "
        "manifest from R2; or (b) compare the four served objects against the store yourself, at "
        "exact equality, the way VERIFY does. Only then continue the batch."),
}


def stop_text_for(rc: int, seam: bool):
    """The stop text for this code IN THIS TOOL, or None to carry on."""
    if not seam and rc in STOP_TEXT_RESYNC:
        return STOP_TEXT_RESYNC[rc]
    return STOP_TEXT.get(rc)


def _utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


GUARDED = ("seam_rebase.py", "aggregate.py", "r2_client.py", "variables_sync.py", "compute_variables.py", "symbol_map.py")
# How many hex characters the child prints per module. MUST match the `[:12]` in seam_rebase.py's and
# resync_variables.py's `_imported_module_hashes`. The sibling check now compares for EQUALITY at this
# width (R760 #2); it used to accept any prefix, so a one-character "hash" matched everything.
SHORT = 12


def _source_sha256s(names=GUARDED) -> dict:
    """sha256 of the tool AND of every module it imports live (R744 finding 4): a guard that watched
    seam_rebase.py alone let an edited aggregate.py run under the batch."""
    import hashlib
    out = {}
    for name in names:
        try:
            with open(os.path.join(HERE, name), "rb") as f:
                out[name] = hashlib.sha256(f.read()).hexdigest()
        except OSError as ex:
            # R748 finding 6: this returned "missing" and the batch ran on with five of six modules
            # unguarded and no warning - the R503 fail-open shape. A guard that cannot read what it
            # guards is not a guard.
            raise SystemExit(f"source guard: cannot read {os.path.join(HERE, name)} ({type(ex).__name__}: {ex}) - "
                             f"refusing to run a batch whose guarded set cannot be hashed")
    return out


def _assert_guarded_matches_tool(path: str | None = None) -> None:
    """The driver's GUARDED and seam_rebase.py's SIBLINGS are separate literals in separate files.
    Three reviews recorded that nothing compared them (R757, R760 lesser): drop a name from GUARDED and
    the driver stops hashing a module the child still imports, silently, with no test failing. Both
    tools print through seam_rebase._say_module_hashes, so there is exactly one literal to check.

    Refuses rather than warns: a guard that notices a mismatch and continues is the R503 shape.
    """
    path = path or os.path.join(HERE, "seam_rebase.py")   # a path only so the check is testable
    try:
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
    except OSError as ex:
        raise SystemExit(f"source guard: cannot read {path} to cross-check SIBLINGS ({type(ex).__name__}: {ex})")
    # PARSED, not regexed (R763 #6). The regex read `^SIBLINGS\s*=\s*\(([^)]*)\)`, which took only the
    # FIRST assignment - so a second, shrinking `SIBLINGS = (...)` later in the file passed while the
    # runtime bound the shrunk tuple - and refused wrongly on an annotated form or a comment inside
    # the tuple. ast sees all of them and sees them the way Python does.
    import ast
    try:
        tree = ast.parse(src, filename=path)
    except SyntaxError as ex:
        raise SystemExit(f"source guard: cannot parse {path} to cross-check SIBLINGS ({ex}) - refusing")
    # WALK THE WHOLE TREE, not just tree.body (R765 #3). "Module level only; that is where it binds"
    # was wrong: a module-level `if True:`, `try:`, `for`, or `if not TYPE_CHECKING:` block also binds
    # at module scope, and a shrinking SIBLINGS inside one was invisible to a tree.body scan while the
    # runtime bound the shrunk tuple - confirmed end to end in a dry run. ast.walk sees every nesting.
    def _binds_siblings(t) -> bool:
        """Is this assignment TARGET the name SIBLINGS? Tuple/list unpacking counts, and so does a
        STARRED target: `for *SIBLINGS, x in ...` and `with cm as (*SIBLINGS, x)` bind it just as
        surely, and both were accepted by every revision until R793 #7 enumerated them."""
        elts = list(t.elts) if isinstance(t, (ast.Tuple, ast.List)) else [t]
        elts = [e.value if isinstance(e, ast.Starred) else e for e in elts]
        return any(isinstance(e, ast.Name) and e.id == "SIBLINGS" for e in elts)

    # ---- THE MODULE NAMESPACE: FAIL CLOSED, because enumerating its spellings does not work
    # R798's judgement, and it is the R791 lesson applied to this guard. Five consecutive rounds
    # wrote a rule for 'the module namespace however it is spelled' - globals()/vars(),
    # sys.modules[...], an alias, __dict__, vars(sys.modules[name]) - and each round a reviewer
    # found spellings it missed, four of those rounds on the PREVIOUS round's fix. Nine open
    # spellings from five rules. Enumeration is the wrong shape here: the set of ways to reach a
    # module's own namespace is not closed, so a matcher over it can never be either.
    #
    # So refuse the whole FAMILY at module scope, whatever it is used for. The price is measured,
    # not assumed: in pipeline/seam_rebase.py today, globals( = 0, vars( = 0, __dict__ = 0,
    # importlib = 0, exec( = 0, eval( = 0, setattr( = 0, and the single `sys.modules` is
    # `sys.modules.get(name)` at line 158 INSIDE a function body, which the module-scope walk
    # already skips. Zero false refusals today, and the cost of a future one is a loud, named
    # refusal that says exactly which token to move into a function - not a silent unguarded
    # module. `test_the_guard_ACCEPTS_THE_REAL_SHIPPED_TOOL` is what keeps that price honest.
    NAMESPACE_CALLS = ('globals', 'vars', 'exec', 'eval', 'setattr', 'compile')
    NAMESPACE_ATTRS = ('__dict__', 'modules', '__globals__', 'import_module')

    # WHICH BINDINGS CAN REACH MODULE SCOPE (R770 #3). ast.walk sees every node, including bodies of
    # functions and classes - so `def f(): SIBLINGS = 3`, an ordinary LOCAL that cannot touch the
    # module global, refused every batch. That is the false-refusal shape R767 rule 5 warns about, and
    # it is how a guard gets disabled. A binding matters here only if it executes at module scope, or
    # if its enclosing function declares `global SIBLINGS`. Nothing below is weakened for module-level
    # code; the walk simply stops crediting a local with power it does not have.
    for _p in ast.walk(tree):
        for _f, _v in ast.iter_fields(_p):
            for _c in (_v if isinstance(_v, list) else [_v]):
                if isinstance(_c, ast.AST):
                    _c._guard_parent = _p                        # ast gives no parent links
                    _c._guard_field = _f                         # ...and the FIELD decides scope

    def _declares_global(fn) -> bool:
        """`global SIBLINGS` in THIS function's own body - not in a nested one, whose declaration
        rebinds the module name only when that inner function runs and says nothing about this
        one (R779 #4: the old walk reported an inner `global` as a second assignment out here)."""
        for node in ast.walk(fn):
            if isinstance(node, ast.Global) and "SIBLINGS" in node.names:
                p = node
                while getattr(p, "_guard_parent", None) is not None:
                    p = p._guard_parent
                    # ClassDef MUST break too (R787 #2). Without it, `global SIBLINGS` written in a
                    # CLASS BODY walked straight past the ClassDef this call was asked about, never
                    # matched `fn`, and was dismissed as a local — so `class C: global SIBLINGS;
                    # SIBLINGS = (...)` at module level rebound 4 of 6 modules with the guard
                    # silent. My own R779 #4 narrowing introduced it: the walk was added to ignore a
                    # NESTED function's `global`, and a class body is not a nested function.
                    if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                                      ast.ClassDef)):
                        break
                if p is fn:
                    return True
        return False

    def _reaches_module_scope(node) -> bool:
        """FAILS CLOSED: everything is checked unless it is PROVABLY a local.

        R779 #2 - my previous version asked only "is any ancestor a def/class?", which accepted
        EIGHT module-scope walrus disguises the predecessor refused (decorator on a def, decorator
        on a class, a default argument, a lambda default, a class base, and three comprehension
        forms), each binding 4 of 6 modules at runtime. Two facts it had wrong:
          * a def's decorator_list, argument DEFAULTS and a class's bases/keywords are AST children
            of that def but EXECUTE in the enclosing scope - only its `body` is the new scope;
          * PEP 572 puts a comprehension's walrus in the CONTAINING scope. Only the comprehension's
            own `for` TARGET is comprehension-local, which is the single case that may be skipped.
        """
        n = node
        while getattr(n, "_guard_parent", None) is not None:
            p, fld = n._guard_parent, getattr(n, "_guard_field", None)
            if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)) \
                    and fld == "body":
                return _declares_global(p)                       # `global SIBLINGS` puts it back
            n = p
        return True

    def _is_comprehension_target(node) -> bool:
        """The one provable local: `[... for SIBLINGS in xs]` binds only inside the comprehension."""
        return (isinstance(node, ast.comprehension)
                and isinstance(getattr(node, "_guard_parent", None),
                               (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)))

    # EVERY BINDING FORM, not only `=` (R767 #4). A target list that knows about Assign and AnnAssign
    # alone accepted seven shapes that rebind at runtime: a `for` target, a walrus, `with ... as`,
    # `import *`, a list `.pop()`, `del`, and setattr with a split string.
    found, dynamic = [], []
    for node in ast.walk(tree):
        if _is_comprehension_target(node) or not _reaches_module_scope(node):
            continue                                             # a local cannot rebind the global
        tgts = []
        if isinstance(node, ast.Assign):
            tgts = list(node.targets)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            tgts = [node.target]
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            tgts = [node.target]
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            tgts = [i.optional_vars for i in node.items if i.optional_vars]
        elif isinstance(node, ast.NamedExpr):
            tgts = [node.target]
        elif isinstance(node, ast.comprehension):
            tgts = [node.target]
        if any(_binds_siblings(t) for t in tgts):
            # Only a plain `SIBLINGS = <literal>` or its ANNOTATED form `SIBLINGS: t = <literal>` is
            # statically READABLE; every other binding form binds something this guard cannot read.
            # (The annotated form is accepted deliberately - R763 #6 records that an earlier regex
            # refused it wrongly, and a guard that refuses correct code gets disabled.)
            readable = (isinstance(node, ast.Assign) and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name)) or \
                       (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                        and node.value is not None)
            if readable:
                found.append(node.value)
            else:
                dynamic.append(type(node).__name__)

        # forms that rebind or mutate without any target the walk above can see
        if isinstance(node, ast.ImportFrom) and any(a.name == "*" for a in node.names):
            dynamic.append("import *")
        if isinstance(node, ast.Delete) and any(_binds_siblings(t) for t in node.targets):
            dynamic.append("del")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "SIBLINGS" \
                and node.func.attr in ("pop", "append", "remove", "clear", "extend", "insert", "__setitem__"):
            dynamic.append(f"SIBLINGS.{node.func.attr}()")
        # `globals()["SIBLINGS"] = x` puts the name in the assignment's SUBSCRIPT, not in the call, so
        # it has to be matched on the target. Matching it anywhere in the node was my previous version
        # and it produced three FALSE refusals on innocent READS - `a, b = SIBLINGS`,
        # `d['x'] = SIBLINGS`, `ns.m = SIBLINGS` (R767 #4). Only the target side may refuse.
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if not isinstance(t, ast.Name) and "SIBLINGS" in ast.dump(t):
                    dynamic.append("indirect assignment target")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in ("exec", "eval"):
            dynamic.append(node.func.id)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == "setattr" and len(node.args) >= 2:
            dynamic.append("setattr")
        # BINDING THROUGH THE NAMESPACE MAPPING (R770 #4). `globals().update(SIBLINGS=...)` rebinds the
        # module global with no assignment target and no setattr, so every check above misses it - the
        # runtime bound 4 of 6 modules in the reviewer's control while this guard said nothing. The
        # receiver, not the name, is what makes it dangerous: a computed key (`globals()[k] = v`, or
        # `.update({"SIBL"+"INGS": v})`) is R767 #4's split-string escape in another costume, so any
        # MUTATING call on globals()/vars() is refused. Reads - `globals().get(...)`, `vars(obj)` -
        # are untouched, because refusing a read is the false refusal that gets a guard deleted.
        # WHAT MAKES A MUTATING CALL DANGEROUS IS THAT ITS RECEIVER IS *THIS MODULE'S* NAMESPACE
        # (R787 #2 and #5). Two rounds of narrowing left this inconsistent and holed:
        #   * `vars(obj).update(...)` accepted while `obj.__dict__.update(...)` refused — the same
        #     operation on the same object, judged two ways;
        #   * `vars(sys.modules[__name__]).update(SIBLINGS=...)` accepted by BOTH, because my R779
        #     zero-argument restriction let an argument through and the `__dict__` clause saw no
        #     `__dict__`. The runtime bound 4 of 6.
        # FAIL CLOSED on the whole namespace family (see NAMESPACE_CALLS above). No receiver
        # analysis, no alias tracking, no 'is this really the module' judgement - those are the
        # five rules that produced nine holes. If it appears at module scope, it is refused.
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in NAMESPACE_CALLS:
            dynamic.append(f"{node.func.id}() at module scope")
        if isinstance(node, ast.Attribute) and node.attr in NAMESPACE_ATTRS:
            dynamic.append(f".{node.attr} at module scope")
        # and the subscript form, whether or not the key is a readable literal
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Subscript) and isinstance(t.value, ast.Call) \
                        and isinstance(t.value.func, ast.Name) \
                        and t.value.func.id in ("globals", "vars"):
                    dynamic.append(f"{t.value.func.id}()[...] = ...")

    # NOTE ON exec/eval/setattr/globals(): these are refused whether or not SIBLINGS visibly appears
    # in them, because R767 #4's escape was `setattr` with the name SPLIT ACROSS STRINGS - which no
    # static match can see. The cost is stated rather than discovered: adding any of them to
    # seam_rebase.py AT MODULE SCOPE for an UNRELATED reason will refuse every batch. That is
    # deliberate and fails closed, and `test_the_guard_ACCEPTS_THE_REAL_SHIPPED_TOOL` is what stops it
    # reaching production unnoticed - the real tool contains none of them today.
    #
    # WHAT THIS GUARD DOES NOT CLAIM (R770 #3, said plainly rather than left to the next reviewer):
    # it reads the DECLARED set at module scope. Bindings inside a function body are skipped unless
    # that function declares `global SIBLINGS`, so a rebind performed at RUNTIME by calling such a
    # function is outside what a static read can settle. That hole is covered elsewhere and by a
    # different instrument - the child prints its own sibling hashes and `_sibling_drift` compares
    # them against the driver's reading, which sees a module swapped DURING the child. Static guard =
    # what the file declares; drift check = what the child actually imported. Neither alone suffices.
    if dynamic:
        raise SystemExit(f"source guard: {path} binds SIBLINGS in a way no static read can follow "
                         f"({', '.join(sorted(set(dynamic)))}) - refusing to run")
    if not found:
        raise SystemExit(f"source guard: {path} declares no SIBLINGS tuple, so the driver cannot confirm "
                         f"it guards the same modules the child imports - refusing to run")
    if len(found) > 1:
        raise SystemExit(f"source guard: {path} assigns SIBLINGS {len(found)} times. The last one wins at "
                         f"runtime, so a later shrinking assignment would leave modules unguarded while "
                         f"the first still looked right - refusing to run")
    value = found[0]
    # A TUPLE, not a list (R767 #4). A list literal is mutable, so a later `.pop()` shrinks the set
    # the runtime binds while this static read still sees six names. The mutation calls are refused
    # above as well; requiring an immutable literal removes the shape rather than chasing its methods.
    if isinstance(value, ast.List):
        raise SystemExit(f"source guard: {path}'s SIBLINGS is a LIST literal. A list can be shrunk in "
                         f"place after this guard reads it - declare it as a tuple.")
    if not isinstance(value, ast.Tuple) or not all(
            isinstance(e, ast.Constant) and isinstance(e.value, str) for e in value.elts):
        raise SystemExit(f"source guard: {path}'s SIBLINGS is not a literal tuple of strings, so the "
                         f"driver cannot read what the child guards - refusing to run")
    theirs = tuple(e.value for e in value.elts)
    ours = tuple(n[:-3] for n in GUARDED)
    # COMPARE AS SETS (R765 #3's rider). The property this guard exists for is WHICH modules are
    # covered; the declaration order is not part of it, and refusing on a reorder is a false refusal
    # that teaches the next author to disable the guard. A duplicate is still refused, because a
    # tuple naming a module twice hides a missing one behind an equal length.
    if len(set(theirs)) != len(theirs):
        dupes = sorted({n for n in theirs if theirs.count(n) > 1})
        raise SystemExit(f"source guard: seam_rebase.py's SIBLINGS names {dupes} more than once, which "
                         f"can hide a missing module behind an equal count - refusing to run")
    if set(theirs) != set(ours):
        missing, extra = sorted(set(ours) - set(theirs)), sorted(set(theirs) - set(ours))
        raise SystemExit(f"source guard: the guarded set and the tool's SIBLINGS disagree - "
                         + (f"the tool does not name {missing}; " if missing else "")
                         + (f"the tool names {extra} which the driver does not hash; " if extra else "")
                         + f"driver {ours} vs seam_rebase.py {theirs}. One was edited without the "
                         f"other; whichever is short is a module nobody is hashing.")


HEADER_RE = re.compile(r"tool source sha256 (\S+) \(.*\) pid (\d+)")
# the child names the siblings it actually imported, at exit, when the set is complete (R748 finding 5)
SIBLING_RE = re.compile(r"imported module sha256(?: at exit)?: (.+)$", re.M)


def _sibling_drift(out: str, shas: dict) -> str | None:
    """The child's own reading of its siblings vs the driver's start-time reading. A sibling saved during
    a child is imported by that child; comparing the two readings is the only thing that sees it.

    A MISSING line is a breach, not a pass (R752 #3): both tools print it, so its absence means either an
    unknown child or a tool built before the contract - and the check silently returned None, which the
    caller read as "no drift". That is the same fail-open shape as the missing header before R748 #4."""
    hits = SIBLING_RE.findall(out)
    if not hits:
        return "child printed no sibling-hash line - the modules it imported are unidentified"

    def parse(line):
        """name -> value, and the ORDERED list of names so a duplicate cannot hide behind the dict.

        R760 #1: `d[name] = value` let a child print a guarded module TWICE - the drifted value first,
        the driver's value second - and the last write won, so a real drift was logged 0 and the next
        start skipped the ticker forever. A dict cannot represent "said twice", so the names come back
        separately and the caller refuses on any repeat."""
        d, names = {}, []
        for part in line.split(", "):
            bits = part.strip().split(" ")
            if len(bits) == 2:
                names.append(bits[0])
                d[bits[0]] = bits[1]
        return d, names

    # THE HEADER AND THE TRAILER MUST AGREE (R754 #3). Taking only the last reading threw away the
    # header - the one that can see a module swapped and restored inside a single child, which is
    # exactly the ~100 ms launch race the header check was added for. A disagreement is a breach.
    parsed = [parse(h) for h in hits]
    readings = [p[0] for p in parsed]
    # COVERAGE, and the STRENGTH of each entry (R757 #2, then R760 #1 and #2). Three fail-opens have
    # been found here in three consecutive reviews, each because the check asked a WEAKER question
    # than the guarantee needs. The guarantee is: this line names every guarded module exactly once,
    # and each value is something that can actually be compared. So every clause below is required.
    for i, (raw, (r, names)) in enumerate(zip(hits, parsed)):
        missing = [n for n in GUARDED if n[:-3] not in r]        # GUARDED holds "<name>.py"
        dupes = sorted({n for n in names if names.count(n) > 1})
        unknown = [n for n in names if f"{n}.py" not in GUARDED]
        # a value is comparable only if it is EXACTLY "not-imported" or EXACTLY a SHORT-length hash.
        # "unreadable" is a breach by name; a 1-char "hash" used to pass because the comparison was
        # startswith(), so any prefix matched - now both the shape and the compare are exact.
        bad = sorted(k for k, v in r.items()
                     if v != "not-imported" and not (len(v) == SHORT and all(c in "0123456789abcdef" for c in v)))
        real = [v for v in r.values() if v != "not-imported"]
        malformed = [p.strip() for p in raw.split(", ")
                     if len(p.strip().split(" ")) != 2 and p.strip()]
        if missing or dupes or unknown or bad or malformed or not real:
            return (f"sibling reading {i} does not identify the guarded set"
                    + (f"; missing {missing}" if missing else "")
                    + (f"; named more than once {dupes}" if dupes else "")
                    + (f"; not a guarded module {unknown}" if unknown else "")
                    + (f"; value is neither 'not-imported' nor a {SHORT}-hex hash {bad}" if bad else "")
                    + (f"; malformed {malformed[:3]}" if malformed else "")
                    + ("; every module says not-imported, so nothing is verified" if not real else ""))
    # R755 #5: compare EVERY reading, not just the first and last. The blind spot had moved from
    # "last only" to "first and last only", so three lines with a drifted middle read as clean.
    # `not-imported` IS NOT A WILDCARD, AND IT IS DIRECTIONAL (R763 #1). This loop used to skip any
    # pair where either side said `not-imported`, and `child.update()` then let the trailer win, so a
    # child could print a DRIFTED hash in its header and `not-imported` in its trailer and the drift
    # was thrown away - logged 0, ticker recorded done, skipped forever. That is R760 #1's defeat
    # moved from one line to two. An import only ever goes not-imported -> hash; hash -> not-imported
    # is a module UN-importing itself mid-run, which cannot happen, so it is a contradiction and a
    # breach. Only the legal direction is tolerated.
    disagree = []
    for i in range(len(readings) - 1):
        a, b = readings[i], readings[i + 1]
        for k in set(a) & set(b):
            if a[k] == b[k]:
                continue
            if a[k] == "not-imported":
                continue                                         # legal: a lazy import bound later
            disagree.append(f"{k} reading{i} {a[k]} vs reading{i+1} {b[k]}")
    if disagree:
        return "the child's own sibling readings disagree: " + "; ".join(disagree)
    # Build the merged view so a REAL HASH always beats `not-imported`, whichever reading carried it.
    # `child.update()` gave the last reading the final say, which is how the trailer erased the header.
    child = {}
    for r in readings:
        for k, v in r.items():
            if child.get(k) in (None, "not-imported") or v != "not-imported":
                child[k] = v
    drift = []
    for name, short in child.items():
        parent = shas.get(f"{name}.py")
        if short == "not-imported" or parent is None:
            continue
        # EQUALITY on a fixed width, not startswith (R760 #2): `parent.startswith(short)` accepted any
        # prefix, so a child printing a single character as its "hash" matched every time and a real
        # drift was logged 0. The coverage check above has already refused any value that is not
        # exactly SHORT hex characters, so this compare is total.
        if short != parent[:SHORT]:
            drift.append(f"{name} child {short} vs driver {parent[:SHORT]}")
    return "; ".join(drift) if drift else None


def recode_on_drift(child_rc: int) -> int:
    """What the driver logs when a hash/sibling breach means UNIDENTIFIED CODE RAN TO COMPLETION.

    Always 4 - "served state UNKNOWN" - regardless of what the child claimed and regardless of whether
    it printed a snapshot line. "No write reached" is not a fact the driver establishes; it is the
    ABSENCE of a line in the child's own stdout, and under a breach that stdout is untrustworthy in
    both directions, because the code that prints the line is the code that changed. Logging a calm 5
    over a write that did happen hides corruption; logging 4 over a child that did nothing costs one
    inspection. See R759 for the full argument and the deliberate deviation from R757 #6.

    This lives in a function, not inline, because R760 #3 found the test that was supposed to guard it
    had reimplemented the rule in the test file and so asserted nothing about the shipped code.
    """
    return 4


RECORD_RE = re.compile(r"^(\S+)\t(?:pid=(\d+)\t)?(EXIT \d+.*)$")


def _run_child(cmd: list[str]):
    """Run the tool in its own process group and wait for it even through a Ctrl-C on the driver:
    a TerminateProcess mid-restore would leave a half-restored set (R735 finding 1). Returns
    (returncode, stdout, stderr, interrupted)."""
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8",
                         errors="replace", cwd=HERE, creationflags=flags)
    interrupted = False
    while True:
        try:
            out, err = p.communicate()
            return p.returncode, out or "", err or "", interrupted, p.pid
        except KeyboardInterrupt:
            interrupted = True
            try:
                print("  interrupt received - waiting for the running ticker to finish (the tool restores itself if it must); "
                      "the batch stops after it", flush=True)
            except Exception:                                    # noqa: BLE001
                pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--tickers", default=None); ap.add_argument("--candidates", default=r"D:\temp\claude\seam_K.csv")
    ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--convention-decided", action="store_true", help="required with --mode full; passed through to the tool")
    ap.add_argument("--log", default=r"D:\temp\claude\seam_rebase_batch.log")
    # the snapshot root is per tool too (R750 finding 1): a shared default made a resync run abort 68
    # tickers on seam's kept manifests, which is the R731 refusal firing for a spurious reason
    ap.add_argument("--snapshot-root", default=None)
    ap.add_argument("--events-file", default=None, help="passed through to seam_rebase.py (issuer-recorded split events, see its docstring)")
    ap.add_argument("--tool", default="seam_rebase.py", choices=("seam_rebase.py", "resync_variables.py"),
                    help="the per-ticker tool to drive; both print the same header and write the same record grammar")
    ap.add_argument("--reviewed", default=None, help="resync_variables.py only: the PASSED.md id passed through as --reviewed")
    a = ap.parse_args()
    seam = a.tool == "seam_rebase.py"
    if a.snapshot_root is None:
        kind = "seam" if seam else "vars"
        a.snapshot_root = os.path.join("F:\\", f"hf_r2_snapshot_{kind}_{dt.datetime.now(dt.timezone.utc):%Y%m%d}")
    if not seam and not a.reviewed:
        print(f"--tool {a.tool} writes served objects and needs --reviewed <PASSED.md id>; the tool refuses "
              f"without it, so every ticker would exit 5. Pass it on the driver."); return 1
    if a.mode == "full" and not a.convention_decided:
        print("--mode full folds dividends into the pre-2022 half; that is the convention decision. Pass --convention-decided "
              "on the driver only once it is recorded (every ticker would exit 5 otherwise)."); return 1
    if a.tickers and os.path.isfile(a.tickers):
        cands = [l.strip().upper() for l in open(a.tickers, encoding="utf-8") if l.strip()]   # one ticker per line
    elif a.tickers:
        cands = [t.strip().upper() for t in a.tickers.split(",") if t.strip()]
    else:
        d = pd.read_csv(a.candidates); d["flag"] = d.flag.fillna("")
        cands = sorted(d[~d.flag.str.contains("no_seam_window")].ticker)
    # THE LOG IS PER TOOL FOR "ALREADY DONE", AND CROSS-TOOL FOR THE EXIT-4 REFUSAL (R750 #1, R752 #1).
    # `done` is a property of THIS tool's work, so it is filtered by the tool column. An exit 4 is a
    # property of the DATA - both tools write the same four {version}/{variables,quality}/<T>.parquet
    # objects - so a ticker whose served state is UNKNOWN must block BOTH tools until it is released.
    # Lines from before the column existed (pass 1) have no tool field and belong to seam_rebase.py.
    def line_tool(p):
        return p[5] if len(p) >= 6 and p[5].endswith(".py") else "seam_rebase.py"

    done = set(); last_line = None; last_line_tool = None
    # "NO EXIT 4 FOUND" AND "NO LOG READ" ARE DIFFERENT ANSWERS (R866 #8). The cross-tool exit-4
    # block below is only as wide as the ONE file --log names, and the default
    # (D:\temp\claude\seam_rebase_batch.log) does not exist - the real pass-1 log is
    # seam_rebase_batch_pass1.log, and it holds an exit 4. Running with the default therefore
    # skipped the block in silence, which reads as "the log says it is safe to start".
    if not os.path.exists(a.log):
        import glob as _glob
        _sib = sorted(_glob.glob(os.path.join(os.path.dirname(os.path.abspath(a.log)),
                                              "seam_rebase_batch*.log")))
        print(f"NOTE: {a.log} does not exist, so no prior run was read: neither the skip list nor the "
              f"cross-tool exit-4 block ran. That is not the same as 'no breach recorded'."
              + (f" Sibling logs beside it: {', '.join(os.path.basename(x) for x in _sib)} - pass --log "
                 f"if one of those is this batch's history." if _sib else ""))
    if os.path.exists(a.log):
        for line in open(a.log, encoding="utf-8"):
            p = line.rstrip("\n").split("\t")
            if len(p) < 3:
                continue
            last_line = p; last_line_tool = line_tool(p)          # the log's last line, whichever tool wrote it
            # EXIT 2 AND 7 ARE TERMINAL SUCCESSES FOR resync_variables.py (R856 #6). 2 is
            # "already consistent - nothing to do" and 7 is "deferred by the daily window";
            # neither leaves anything to redo. Counting only 0 made every restart re-measure
            # every already-consistent ticker at 50-77 s each, and 1,324 of those is most of a
            # day of work that changes nothing. For seam_rebase.py exit 2 is a REFUSAL, so it
            # stays out - the same per-tool asymmetry as R750 #3 and R856 #3.
            # EXIT 7 IS NOT TERMINAL (R858 #2). It returns BEFORE get_client(), having written
            # and read nothing - "deferred by the daily window", not "done". Counting it retired
            # tickers for ever: a 45-minute window across a 22-33 h run would silently drop
            # ~700-900 of 1,324 and then report a clean finish.
            #
            # Exit 2 stays, and stays flagged: the log line records the tool NAME, never its
            # sha, so an "already consistent" written by the 2-of-4-object MEASURE is honoured by
            # the 4-of-4 bytes. Fix that by putting the sha in the log line, not by trusting it.
            _terminal_ok = {"0"} if a.tool == "seam_rebase.py" else {"0", "2"}
            if line_tool(p) == a.tool and p[2] in _terminal_ok:
                done.add(p[1])
    if last_line is not None and last_line[2] == "4":
        who = f" (written by {last_line_tool})" if last_line_tool != a.tool else ""
        # the restore is ALWAYS seam_rebase.py's: resync_variables.py has no --restore, and printing
        # it there gave the operator a command that exits 5, which in this grammar reads as "aborted
        # before any write" - i.e. a failure that looks like a safe decline (R754 #2). Both tools
        # write seam_rebase-format manifests, so one restore command covers both.
        # R760 #4: this used to say "UNKNOWN until --restore <its snapshot dir> has run" unconditionally.
        # Since R759, an exit 4 also covers breaches where NO SNAPSHOT WAS EVER TAKEN, and for those
        # the directory does not exist - so the only instruction the operator was given could not be
        # followed, and RELEASE told them to append code 0 "if the restore succeeded". Branch on
        # whether the directory is actually there, the same way STOP_TEXT[4] now does.
        snap_dir = os.path.join(a.snapshot_root, last_line[1]) if a.snapshot_root else None
        # A NON-EMPTY DIRECTORY IS NOT A SNAPSHOT (R866 #3). Exits 5 and 7 both makedirs and write
        # _RESULT.txt BEFORE any snapshot is taken, and both are retried by design - so a directory
        # holding one _RESULT.txt satisfied `os.listdir` and the operator was sent to a `--restore`
        # that dies on `FileNotFoundError: _MANIFEST.txt` and exits 4. That re-opened R760 #4, which
        # is the entry about giving an operator an instruction that cannot be followed. The manifest
        # is what restore() actually reads, so the manifest is what decides.
        try:
            have_snap = bool(snap_dir and os.path.isfile(os.path.join(snap_dir, "_MANIFEST.txt")))
        except OSError as ex:                                # R763 #7: an unreadable dir killed main()
            have_snap = False                                # with a traceback and no refusal message
            print(f"  (could not read {snap_dir}: {type(ex).__name__}: {ex} - treating it as absent)")
        if have_snap:
            how = (f"its served state is UNKNOWN until `python seam_rebase.py {last_line[1]} --restore "
                   f"{snap_dir}` has run (that is the restore for BOTH tools; resync_variables.py has "
                   f"none of its own). " + RELEASE)
        else:
            how = (f"its served state is UNKNOWN and THERE IS NO SNAPSHOT TO RESTORE FROM"
                   + (f" ({snap_dir} holds no _MANIFEST.txt, which is the only thing --restore reads)"
                      if snap_dir else " (no --snapshot-root given)")
                   + ". That means the breach happened before any snapshot was taken, so unidentified "
                   f"code ran and nothing it printed about writing can be believed. Compare "
                   f"{last_line[1]}'s served objects against the store yourself and decide; then "
                   "release the batch by appending ONE line to the log in its own format - "
                   "<UTC stamp>\\t<TICKER>\\t<code>\\t0s\\t<note> - with code 0 ONLY if you established "
                   "that served state is correct (e.g. `...\\t0\\t0s\\tinspected, served == store, no "
                   "write had happened`); any other code keeps the refusal.")
        print(f"REFUSING TO START: the log's last line is an exit 4 for {last_line[1]} ({last_line[0]}){who} - {how} "
              f"Both tools write the same objects, so this blocks {a.tool} too.")
        return 1
    # BEFORE the dry-run return (R763 #6). Sitting after it meant the one command an operator runs to
    # check the setup was the one command that never checked the guarded set.
    _assert_guarded_matches_tool()
    todo = [t for t in cands if t not in done]
    # R866 #7: the label said "already exit-0" while _terminal_ok admits exit 2 as well for the
    # resync tool, so the number a human reads did not name what it counted.
    _skip_word = "exit-0" if a.tool == "seam_rebase.py" else "exit-0 or exit-2 (already consistent)"
    print(f"candidates {len(cands)}; skipped, {_skip_word} in log: {len(done & set(cands))}; "
          f"to do {len(todo)}; mode {a.mode}; apply {a.apply}")
    if not a.apply:
        print("  first 20:", " ".join(todo[:20])); print("(dry run - pass --apply to run the batch)"); return 0
    detail_dir = os.path.dirname(os.path.abspath(a.log))
    # THE SOURCE GUARD (R743, after R742): the tool file is hashed at start and re-hashed before every
    # child; a batch never runs bytes that were not there when it started. An edit under the batch
    # stops it - the edit waits, or the batch is restarted deliberately on the new file.
    guarded = tuple(dict.fromkeys((a.tool,) + GUARDED))     # the launched tool first, then the live imports
    shas = _source_sha256s(guarded); tool_sha = shas[a.tool]
    print(f"tool {a.tool} source sha256 {tool_sha} at start; guarded modules: " + ", ".join(f"{k} {v[:12]}" for k, v in shas.items())
          + "; the batch refuses to launch a child if any of them changes", flush=True)
    n = 0; stopped = False; incomplete = []; aborted = []; refused = []; unmeasurable = []; deferred = []; already_ok = []
    for t in todo:
        if a.limit is not None and n >= a.limit:
            break
        now = _source_sha256s(guarded)
        changed = [k for k in guarded if now.get(k) != shas.get(k)]
        if changed:
            print(f"STOPPING before {t}: {', '.join(changed)} changed under the batch ("
                  + "; ".join(f"{k} {shas[k][:12]} -> {now.get(k, 'missing')[:12]}" for k in changed)
                  + "). Nothing launched on the new file(s); restart the batch deliberately if they are the ones to run.", flush=True)
            stopped = True; break
        hash_breach = None
        cmd = [sys.executable, "-u", os.path.join(HERE, a.tool), t] + (["--mode", a.mode] if seam else []) + \
              ["--apply", "--snapshot-dir", os.path.join(a.snapshot_root, t)]
        # PASS IT ONLY IF THE DRIVER WAS GIVEN IT (R798 #2). This used to append the flag whenever
        # the mode was full, so the child received the convention decision whether or not anyone had
        # made one - which disabled the very backstop the gate at the top of main() names in its own
        # message ("every ticker would exit 5 otherwise"). With the gate the only route, the driver
        # refuses AND the child would refuse: the header's "it is never implied" is now true of the
        # code as well as of the sentence.
        if seam and a.mode == "full" and a.convention_decided:
            cmd += ["--convention-decided"]
        if seam and a.events_file:
            cmd += ["--events-file", a.events_file]
        if not seam and a.reviewed:
            cmd += ["--reviewed", a.reviewed]
        t0 = dt.datetime.now(dt.timezone.utc)
        stamp = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
        rc, out, err, interrupted, child_pid = _run_child(cmd)
        raw_rc = rc                                          # the child's own code, kept in the detail header
        last = (out.strip().splitlines() or [""])[-1]
        # THE RECORD OUTRANKS THE EXIT CODE (R738 finding 1). A child killed from outside
        # (TerminateProcess) exits 1 - the code that means "written then RESTORED" - with nothing
        # restored and no record. Once the tool printed its "snapshot:" line, writes may have
        # happened, and the exit code is believed only when <snap_dir>/_RESULT.txt's last line says
        # the same thing; otherwise the ticker is logged as 4: served state UNKNOWN.
        # Keyed on the snapshot SUCCESS line, not the word "snapshot:" (R739): the four pre-write
        # abort lines ("snapshot: ... aborting before any write", exit 5) also carry the word, and
        # the first form of this check rewrote every one of them to 4 and stopped the batch.
        snapshot_ok = bool(re.search(r"^\s+snapshot: \d+ objects -> ", out, re.M))
        if snapshot_ok:
            rec_path = os.path.join(a.snapshot_root, t, "_RESULT.txt")
            rec_last = None
            try:
                lines = [ln.rstrip("\n") for ln in open(rec_path, encoding="utf-8") if ln.strip()]
                rec_last = lines[-1] if lines else None
            except OSError:
                pass
            m = RECORD_RE.match(rec_last) if rec_last else None
            rec_stamp = m.group(1) if m else ""
            rec_pid = m.group(2) if m else None
            said = m.group(3) if m else (rec_last or "")
            # the child prints "pid <n>" in its header (v5.4+): a record without a pid under such a child,
            # or a child without the header at all, is not believed (R743 finding 4)
            # every header the child printed, not just the first: a second header line with a different
            # hash would otherwise be hidden behind the first (found by the R748 finding-4 harness)
            hdrs = HEADER_RE.findall(out)
            hdr = HEADER_RE.search(out)
            hdr_sha = hdr.group(1) if hdr else None
            hdr_pid = hdr.group(2) if hdr else None
            if len({h[0] for h in hdrs}) > 1:
                hdr_sha = f"MULTIPLE ({', '.join(sorted({h[0][:12] for h in hdrs}))})"
            # the header's hash must be the launch hash (R744 finding 5): the ~100 ms between the guard's
            # read and the child's module-top read is seen by nothing else
            hash_ok = hdr_sha == tool_sha
            # the record must be THIS run's: a parseable line, stamped at/after the child's start, written
            # by the child itself (R741 finding 5: another actor's "EXIT 1 RESTORED" after t0 was believed)
            fresh = bool(m) and rec_stamp >= stamp and hdr is not None and hash_ok and rec_pid == hdr_pid == str(child_pid)
            if not fresh or not (said.startswith(f"EXIT {rc} ") or said == f"EXIT {rc}"):
                if not rec_last:
                    why = "died without its record"
                elif not m:
                    why = "record line not in the grammar"
                elif hdr is None:
                    why = "child printed no hash/pid header"
                elif not hash_ok:
                    why = f"child header hash {hdr_sha[:12]} differs from the launch hash {tool_sha[:12]}"
                elif hdr_pid != str(child_pid):
                    why = "header pid differs from the launched pid (a wrapper or launcher between driver and tool)"
                elif rec_pid is None:
                    why = "record without a pid under a child that printed one"
                elif rec_pid != str(child_pid):
                    why = "record from another run or actor"
                elif rec_stamp < stamp:
                    why = "stale record from an earlier run"
                else:
                    why = "record disagrees with the exit code"
                last = (f"{why} (exit {rc}, _RESULT.txt says {said[:80]!r} at {rec_stamp or 'no stamp'} pid {rec_pid or '?'} vs child "
                        f"{child_pid}) - served state UNKNOWN; inspect {os.path.join(a.snapshot_root, t)} and --restore if the objects "
                        f"there differ from R2")
                rc = 4
        else:
            # No write reached. The same evidence that is exit 4 on the write path must not be exit 0 here
            # (R748 finding 4): a hash breach or a missing header was printed but LOGGED WITH THE CHILD'S OWN
            # CODE, so a 0 went into the log and the next start skipped the ticker as already done.
            # "No write reached" is itself read off the child's stdout, so under a breach it is not a
            # fact - it is the untrustworthy code's own account of itself. One rule covers every breach
            # here and in the drift block below: A BREACH ALWAYS LOGS 4 (served state UNKNOWN). 5 is
            # reserved for refusals the DRIVER settles before a child runs, where no child existed to
            # write anything. This corrects an incoherence the harness surfaced: NWHASH (the worse
            # breach - the tool's own hash is wrong) and NWNOHDR both logged 5, while a sibling drift
            # on the same evidence logged 4. Measured on the predecessor f358795, not recalled.
            hdrs = HEADER_RE.findall(out)
            hdr = HEADER_RE.search(out)
            if hdr and (len({h[0] for h in hdrs}) > 1 or hdr.group(1) != tool_sha):
                shown = ", ".join(sorted({h[0][:12] for h in hdrs}))
                hash_breach = (f"child header hash {shown} differs from the launch hash {tool_sha[:12]} (child exit {rc}) - "
                               f"unidentified code ran to completion, so its 'no write' cannot be believed")
                last = hash_breach + " - " + last
                rc = 4                      # never 0, never skippable, and never a calm 5
            elif hdr is None:
                hash_breach = (f"child printed no hash/pid header (child exit {rc}) - the code that ran is unidentified, "
                               f"so its 'no write' cannot be believed")
                last = hash_breach + " - " + last
                rc = 4
        # the siblings the CHILD loaded, not the driver's start-time read (R748 finding 5)
        child_sibs = SIBLING_RE.findall(out)
        drift = _sibling_drift(out, shas)
        if drift:
            last = f"SIBLING DRIFT: {drift} - a guarded module changed while the child ran; " + last
            # R754 #5 recoded 0/2/3/6/7 (each is the child CONCLUDING something about served state);
            # R755 #4 widened it to 1 and 5 past a successful snapshot. Scoping the whole thing on
            # snapshot_ok (R757 #1) silently un-did the first rule - RC2DRIFT went back to logging 2.
            #
            # The rule is now TOTAL and has no snapshot_ok in it, which DEVIATES from R757 #6's
            # "no-snapshot drift logs 5". Reason: under drift the child's stdout is untrustworthy in
            # BOTH directions. A missing snapshot line is not evidence that no write happened, because
            # the code that would have printed it is exactly the code that changed. Logging 5 over a
            # write that did happen hides corruption in served data and nobody looks; logging 4 over a
            # child that truly did nothing costs one wasted inspection. The errors are not symmetric.
            # R757 #6's actual harm - "the log keeps a 0 the next start skips" - is fixed either way.
            rc = recode_on_drift(rc)
            hash_breach = hash_breach or f"sibling drift under exit {raw_rc}"
        detail = os.path.join(detail_dir, f"seam_detail_{t0:%Y%m%dT%H%M%SZ}_{t}.txt")
        try:
            with open(detail, "w", encoding="utf-8") as f:
                f.write(f"# {stamp} {' '.join(cmd)}\n# child exit {raw_rc}; logged as {rc}; child pid {child_pid}; tool sha256 at launch {tool_sha}\n"
                        f"# driver read at start : " + ", ".join(f"{k} {v[:12]}" for k, v in shas.items()) + "\n"
                        + "".join(f"# child reading {i}      : {s}\n" for i, s in enumerate(child_sibs))
                        + ("" if child_sibs else "# child readings       : (none printed)\n")
                        + (f"# SIBLING DRIFT       : {drift}\n" if drift else "")
                        + f"--- stdout ---\n{out}\n--- stderr ---\n{err}\n")
        except Exception as e:                                   # noqa: BLE001
            print(f"  (detail file not written: {e})", flush=True)
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(f"{stamp}\t{t}\t{rc}\t{(dt.datetime.now(dt.timezone.utc) - t0).total_seconds():.0f}s\t{last[:200]}\t{a.tool}\n")
        print(f"  {t:6} exit {rc}  {last[:130]}", flush=True)
        n += 1
        if hash_breach:
            print(f"STOPPING: {hash_breach} for {t} - the batch ran bytes other than the ones it hashed; restart deliberately", flush=True)
            # R755 #7: the drift stop used to REPLACE the served-state guidance, so on the very run
            # where a breach happened over a write the operator was told "restart deliberately" and
            # not how to restore - that text only arrived on the next start.
            _st = stop_text_for(rc, seam)
            if _st:
                print(_st, flush=True)
            stopped = True; break
        _st = stop_text_for(rc, seam)
        if _st:
            print(_st + "\n" + out[-2500:] + err[-800:])
            stopped = True; break
        if rc == 6:
            incomplete.append(t)
        elif rc == 5:
            aborted.append(t)
        elif rc == 2:
            # per tool (R750 finding 3): for seam_rebase.py exit 2 is a refusal to act; for
            # resync_variables.py it is "already consistent", which is a success, not a manual item
            (refused if seam else already_ok).append(t)
        elif rc == 3:
            # Only reachable for seam_rebase.py: stop_text_for() halts the batch on a resync 3.
            # Kept explicit so the asymmetry is visible at the dispatch, not only at the guard.
            unmeasurable.append(t)
        elif rc == 7:
            deferred.append(t)                                   # resync_variables.py: the daily window; run again later
        elif rc != 0:
            print(f"STOPPING: undefined exit code {rc} from seam_rebase.py for {t}\n" + out[-2500:] + err[-1200:])
            stopped = True; break
        if interrupted:
            print("batch stopped by the interrupt after the running ticker finished and was logged")
            stopped = True; break
    if incomplete:
        print(f"SERVING INCOMPLETE for {len(incomplete)} ticker(s) - prices rebased and verified, variables/quality sync failed: "
              f"{' '.join(incomplete)}. Run sync_ticker_variables(force_full=True) for each (a re-run of the tool will not).")
    if aborted:
        print(f"aborted before any write (exit 5) - read each one's line before re-running (a malformed events file or a kept "
              f"manifest is not fixed by a re-run): {' '.join(aborted)}")
    if refused:
        print(f"refused before any write (exit 2) - the manual list: {' '.join(refused)}")
    if already_ok:
        print(f"already consistent, nothing to do (exit 2) - {len(already_ok)} ticker(s): {' '.join(already_ok)}")
    if unmeasurable:
        print(f"unmeasurable (exit 3) - the disclose list: {' '.join(unmeasurable)}")
    if deferred:
        print(f"deferred by the daily window (exit 7) - run again outside it: {' '.join(deferred)}")
    # A RUN THAT ACHIEVED NOTHING IS NOT A RUN THAT SUCCEEDED (R866 #4, generalised by R868 #4).
    # The first version of this special-cased exit 7 alone - the deferral - and that was fixing the
    # INSTANCE, not the class. Every whole-batch no-op has the same shape and the same danger:
    #   * exit 7 for all: `daily_run_state()` answers "unknown" on any `gh` failure and every
    #     ticker defers without reading anything (2.45 s each, so 1,324 sweep in ~54 min);
    #   * exit 5 for all: a STALE --reviewed id - which EVERY edit to the tool creates, because the
    #     approval is bound to its hash - refuses each ticker before any write (3.86 s each, so
    #     the same list reports success in ~85 min);
    #   * exit 6 for all: prices verified, serving incomplete on every one.
    # In each case the driver printed "batch done" and returned 0, which an unattended caller
    # reads as the work being finished. So the outcome is derived from what was ACHIEVED.
    achieved = n - (len(deferred) + len(aborted) + len(incomplete) + len(refused) + len(unmeasurable))
    if stopped:
        outcome = "stopped"
    elif n and achieved == 0:
        outcome = "achieved nothing"
    else:
        outcome = "done"
    print(f"batch {outcome}: {n} processed this run, {achieved} completed; log {a.log}; "
          f"details in {detail_dir}/seam_detail_*.txt")
    if outcome == "achieved nothing":
        _why = {"deferred by the daily window (exit 7)": deferred,
                "aborted before any write (exit 5)": aborted,
                "serving incomplete (exit 6)": incomplete,
                "refused (exit 2)": refused,
                "unmeasurable (exit 3)": unmeasurable}
        _named = "; ".join(f"{len(v)} {k}" for k, v in _why.items() if v)
        print(f"  NOT ONE of the {n} ticker(s) attempted this run completed: {_named}. Nothing was written. "
              f"A whole-batch no-op is almost always ONE cause, not {n} coincidences - a stale --reviewed id "
              f"(every edit to the tool invalidates the approval bound to its hash), a `gh` that cannot answer "
              f"so daily_run_state() reads 'unknown', or an expired credential. Find that cause before re-running.")
    return 1 if stopped or (n and achieved == 0) else 0


if __name__ == "__main__":
    sys.exit(main())
