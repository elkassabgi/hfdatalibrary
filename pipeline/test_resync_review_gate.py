"""Try to defeat resync_variables.reviewed_ok, which is now an EXACT approval token rather than an
inference over a prose table.

Every row below is a candidate PASSED.md line. `want` says whether it MUST authorise. The list keeps
every shape that beat the four previous inference-based versions (R745, R750, R752, R754, R755), so a
future return to inference would light them all up again."""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import resync_variables as rv

SHA = rv._SOURCE_SHA[:12]
ID = "AR-038"
GOOD = f"APPROVE-APPLY resync_variables.py {SHA} {ID}"

CASES = [
    (True,  "the token, exactly", GOOD),
    (True,  "the token with a trailing comment", GOOD + "   # cleared after the 14th review"),
    (True,  "the token indented", "   " + GOOD),
    (False, "wrong hash", f"APPROVE-APPLY resync_variables.py deadbeefdead {ID}"),
    (False, "wrong tool", f"APPROVE-APPLY seam_rebase.py {SHA} {ID}"),
    (False, "wrong id", f"APPROVE-APPLY resync_variables.py {SHA} AR-999"),
    (False, "token misspelled", f"APPROVE_APPLY resync_variables.py {SHA} {ID}"),
    (False, "token embedded in prose",
     f"the reviewer wrote APPROVE-APPLY resync_variables.py {SHA} {ID} but then withdrew it"),
    (False, "commented out", f"# APPROVE-APPLY resync_variables.py {SHA} {ID}"),
    (False, "negated on the same line", f"NOT APPROVE-APPLY resync_variables.py {SHA} {ID}"),
    # everything that defeated the inference gate - none of it can produce the token
    (False, "R754: a PASS verdict row", f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASS |"),
    (False, "R755 #1: FAIL and PASS cells together",
     f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | FAIL | PASS |"),
    (False, "R755 #2: PASS - with changes",
     f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASS - with changes |"),
    (False, "R754: PASSED tokenises as PASS",
     f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} | PASSED |"),
    (False, "R754: the only PASS is inside 'nothing added to PASSED.md'",
     f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA}; nothing added to PASSED.md | |"),
    (False, "R755 #6: SUPERSEDED clearance",
     f"| 2026-09-05 | {ID} | resync_variables.py sha256 {SHA} (SUPERSEDED) | PASS |"),
    (False, "a bare mention of the id and tool", f"{ID} resync_variables.py looks fine to me"),
    (False, "empty file line", ""),
]

BAD_IDS = ["", "   ", "PASS", "APPROVE-APPLY", "resync_variables", "-", "A", "AR-0", "*"]
# the SAME id with stray whitespace is the same id: stripping it is intended, not a defeat
OK_IDS = [ID + " ", " " + ID, f"  {ID}  "]


def main():
    fd, path = tempfile.mkstemp(suffix=".md", text=True)
    os.close(fd)
    bad = 0
    for want, why, row in CASES:
        with open(path, "w", encoding="utf-8") as f:
            f.write("# PASSED.md\n\n" + row + "\n")
        got = rv.reviewed_ok(ID, path)
        ok = (got == want)
        bad += 0 if ok else 1
        print(f"  {'ok      ' if ok else 'DEFEATED'}  want={want!s:5} got={got!s:5}  {why}")

    # a good token present, but the caller asks with a different / malformed id
    for b in BAD_IDS:
        with open(path, "w", encoding="utf-8") as f:
            f.write(GOOD + "\n")
        if rv.reviewed_ok(b, path):
            bad += 1
            print(f"  DEFEATED by id {b!r}")

    for g in OK_IDS:
        with open(path, "w", encoding="utf-8") as f:
            f.write(GOOD + "\n")
        if not rv.reviewed_ok(g, path):
            bad += 1
            print(f"  WRONGLY REFUSED the same id with whitespace: {g!r}")

    # the file being missing must refuse, not crash
    os.unlink(path)
    if rv.reviewed_ok(ID, path):
        bad += 1
        print("  DEFEATED: a missing PASSED.md authorised")

    print(f"\n{len(CASES)} lines + {len(BAD_IDS)} ids + a missing file; {bad} defeat(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
