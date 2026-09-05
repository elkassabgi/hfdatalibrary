"""Render the 'applied on 2026-09-05' table for docs/VENDOR_SEAM_20260905.md from the pass-1 log and the
all-ticker verification JSON (D:/temp/claude/_verify_applied_seam.json). Read-only; prints markdown.

Tool label per row, BY EVIDENCE (R744), in this order:
  1. the detail file carries `tool source sha256 <hex>` (child header, from the v5.4 file on)  -> "sha256 <12 hex>"
  2. the record in <snap>/_RESULT.txt carries `pid=<n>` but the detail has no hash line          -> "uncommitted file:
     pid in record, no hash line (saved 14:31:58-14:33:56Z)" - a file version no commit holds (R744)
  3. otherwise the version is INFERRED from the time the file on disk changed, with two boundaries marked
     "commit time" because only the commit time is known for them (R744: 13:39:25Z and 14:36:42Z were commit
     times +4/+2 s relabelled as file times; the v5.1 file was measured on disk at 13:36:38Z by R739, and the
     14:34:12Z file is what 88584e2 committed, so there is no 14:36:42Z change). A row launched within 2 s of
     a boundary is marked undecidable.
  4. a release line (exit 0, "release:") is not a run: it releases a ticker logged 4 by an older driver whose
     child had exited 0 with a DONE record (R742); labelled as such, counted once with its DONE line.
Every row: UTC launch time (the first three log lines carry unlabelled local stamps, R729: shown as their UTC
equivalent, +5 h, and marked), exit, seconds, K, the verification's pre-seam bar count and mismatches, the tool
label and the outcome."""
import datetime as dt, glob, json, os, re, sys

LOG = "D:/temp/claude/seam_rebase_batch_pass1.log"
VER = "D:/temp/claude/_verify_applied_seam.json"
SNAP = "F:/hf_r2_snapshot_seam_20260905"
# (upper bound, label, how the bound is known)
VERSIONS = [("2026-09-05T13:13:35Z", "v5 file (later 9ba6a3f)", "COMMIT time of a38144c (13:13:32Z) + 3 s - no measured mtime exists for this bound (R748)"),
            ("2026-09-05T13:36:38Z", "v5.1 file (later a38144c)", "file time measured by R739"),
            ("2026-09-05T13:51:30Z", "v5.2 file (later daef642)", "file time"),
            ("2026-09-05T14:00:20Z", "v5.2.1 file (later fc9d33a)", "file time"),
            ("2026-09-05T14:31:58Z", "v5.3 file (later 14a0869)", "LAUNCH time of NOW, the last run whose record has no pid - not a file time"),
            ("2026-09-05T14:34:12Z", "uncommitted file: pid in record, no hash line", "file time; NVDA launched 14:33:56Z, before it - see the pid rule, which decides that row"),
            ("9999", "v5.4 file (88584e2, committed 14:36:40Z from the 14:34:12Z file)", "file time")]
UNDECIDABLE_S = 2


def parse_stamp(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def version_at(stamp):
    """(label, note) - the version whose window holds the launch; undecidable within 2 s of a boundary"""
    t = parse_stamp(stamp)
    for upto, name, how in VERSIONS:
        if upto == "9999" or stamp < upto:
            note = f"inferred from file times; bound {how}" if upto != "9999" else "inferred from file times"
            if upto != "9999" and abs((parse_stamp(upto) - t).total_seconds()) <= UNDECIDABLE_S:
                return name, f"UNDECIDABLE: launched within {UNDECIDABLE_S} s of the {upto[11:19]}Z change"
            return name, note
    return VERSIONS[-1][1], "inferred"


def record_has_pid(ticker):
    p = os.path.join(SNAP, ticker, "_RESULT.txt")
    try:
        lines = [l for l in open(p, encoding="utf-8", errors="replace").read().splitlines() if l.strip()]
    except OSError:
        return None
    return any(re.match(r"^\S+\tpid=\d+\t", l) for l in lines) if lines else None


def detail_sha(ticker):
    for p in sorted(glob.glob(f"D:/temp/claude/seam_detail_*_{ticker}.txt"), reverse=True):
        try:
            head = open(p, encoding="utf-8", errors="replace").read(4000)
        except OSError:
            continue
        m = re.search(r"tool source sha256 ([0-9a-f]{64})", head)
        if m:
            return m.group(1)[:12]
    return None


def tool_label(stamp, ticker, is_release):
    if is_release:
        return "release line (R742), not a run"
    sha = detail_sha(ticker)
    if sha:
        return f"sha256 {sha}"
    has_pid = record_has_pid(ticker)
    if has_pid:
        return "uncommitted file: pid in record, no hash line (saved 14:31:58-14:33:56Z)"
    name, note = version_at(stamp)
    if name.startswith("uncommitted file"):
        # inside the window but the record has no pid: the pid-writing file was saved AFTER this launch (NOW, 14:31:58Z)
        return "v5.3 file (later 14a0869) (pid-less record: launched before the pid-writing file was saved)"
    return f"{name} ({note})"


def main():
    ver = {}
    if os.path.exists(VER):
        for row in json.load(open(VER, encoding="utf-8")):
            ver[row["ticker"]] = row
    rows = []
    for line in open(LOG, encoding="utf-8"):
        p = line.rstrip("\n").split("\t")
        if len(p) < 5:
            continue
        stamp, t, rc, dur, last = p[0], p[1], p[2], p[3], p[4]
        local_note = ""
        if not stamp.endswith("Z"):
            # 'YYYY-MM-DD HH:MM:SS' local (UTC-5 on 2026-09-05): +5 h
            d = dt.datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=5)
            stamp = d.strftime("%Y-%m-%dT%H:%M:%SZ"); local_note = " (local stamp, +5 h)"
        is_release = rc == "0" and "release:" in last
        v = ver.get(t, {})
        m = v.get("raw_1min") if isinstance(v.get("raw_1min"), dict) else None
        K = f"{m['K']:g}" if m else "-"
        pre = f"{m['pre_bars']:,}" if m else "-"
        mis = f"{m['pre_price_mismatch']}/{m['pre_volume_mismatch']}/{m['post_mismatch']}" if m else "-"
        rows.append((stamp, t, rc, dur, K, pre, mis, tool_label(stamp, t, is_release), last[:60] + local_note, is_release))
    print("| UTC (launch) | ticker | exit | secs | K (price) | pre-seam 1-min bars | mismatches price/volume/post | tool | outcome |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        print("| " + " | ".join(str(x) for x in r[:9]) + " |")
    done_rows = [r for r in rows if r[2] == "0" and "DONE" in r[8] and not r[9]]
    release_rows = [r for r in rows if r[9]]
    done_tickers = {r[1] for r in done_rows} | {r[1] for r in release_rows}
    verified = {r[1] for r in rows if r[1] in done_tickers and r[6] != "-" and r[6].startswith("0/0/0")}
    by_label = {}
    for r in done_rows:
        lab = r[7]
        # R748: an UNDECIDABLE row and a pid-less row were folded into the plain version counts
        if "UNDECIDABLE" in lab:
            key = lab.split(" (")[0] + " [UNDECIDABLE]"
        elif "pid-less record" in lab:
            key = lab.split(" (")[0] + " [pid-less record]"
        else:
            key = lab.split(" (")[0]
        by_label[key] = by_label.get(key, 0) + 1
    print(f"\n{len(done_tickers)} tickers rebased (exit 0 DONE; {len(release_rows)} of them through a release line), {len(verified)} of them verified bar for bar "
          f"by verify_applied_seam.py at the time of this table; {len(rows) - len(done_rows) - len(release_rows)} other exit line(s). "
          f"Tool labels by evidence: " + "; ".join(f"{k}: {n}" for k, n in sorted(by_label.items())) + ". "
          f"Generated {dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} from {LOG}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
