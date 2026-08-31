#!/usr/bin/env python3
"""Find things that use the ElkassabgiData libraries, and report what is NOT yet recorded.

WHY A TOOL AND NOT A ONE-OFF SEARCH. Uses accumulate quietly: a paper cites the DOI, an app
ships with the attribution in its privacy policy, a repo starts pulling /v1/download-token.
None of that notifies anyone. This re-runs the searches that found the current entries so a
new one surfaces the week it appears rather than whenever somebody thinks to look.

WHAT IT DOES NOT DO: it does not add anything to data/used_by.json. Every entry in that file
carries a verbatim quote or a file path that was actually read, because a search-result
snippet is not evidence that someone used the data - it is evidence that a string matched.
This prints CANDIDATES; a human (or a careful agent) confirms and writes the entry.

GitHub code search needs `gh auth login`. Own repositories are filtered out by owner.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY = os.path.join(ROOT, "data", "used_by.json")
OWN_OWNERS = {"elkassabgi"}
QUERIES = [
    "api.hfdatalibrary.com",
    "hfdatalibrary.com",
    "api.econdatalibrary.com",
    "econdatalibrary.com",
    "elkassabgidata.com",
    "ipdatalibrary.com",
    "zenodo.19501605",
]


def _known() -> set:
    try:
        reg = json.load(open(REGISTRY, encoding="utf-8"))
    except (OSError, ValueError):
        return set()
    out = set()
    for u in reg.get("uses", []):
        url = str(u.get("url", ""))
        if "github.com/" in url:
            out.add(url.split("github.com/", 1)[1].strip("/").lower())
        out.add(url.lower())
        if u.get("evidence_repo"):
            out.add(str(u["evidence_repo"]).lower())
    return out


def _gh_code(query: str) -> list:
    try:
        r = subprocess.run(
            ["gh", "search", "code", query, "--limit", "50", "--json", "repository,path"],
            capture_output=True, text=True, timeout=180)
        if r.returncode != 0:
            return []
        return json.loads(r.stdout or "[]")
    except Exception:                                            # noqa: BLE001
        return []


def main() -> int:
    known = _known()
    found: dict = {}
    for q in QUERIES:
        for hit in _gh_code(q):
            repo = (hit.get("repository") or {}).get("nameWithOwner")
            if not repo:
                continue
            owner = repo.split("/", 1)[0].lower()
            if owner in OWN_OWNERS:
                continue
            entry = found.setdefault(repo, {"queries": set(), "paths": set()})
            entry["queries"].add(q)
            if hit.get("path"):
                entry["paths"].add(hit["path"])

    new = {r: v for r, v in found.items() if r.lower() not in known}
    print("third-party repositories matching: %d  (already recorded: %d, NEW: %d)"
          % (len(found), len(found) - len(new), len(new)))
    for repo, v in sorted(new.items()):
        print("\n  NEW  https://github.com/%s" % repo)
        print("       matched: %s" % ", ".join(sorted(v["queries"])))
        for p in sorted(v["paths"])[:6]:
            print("       %s" % p)
    if not new:
        print("\n  nothing new since the registry was last updated.")
    print("\nCandidates only. Confirm each by READING the file that matched, then add an entry "
          "to data/used_by.json with a verbatim quote or the path you read, and re-render the "
          "page with tools/gen_used_by_page.py.")
    _citation_indexes()
    _download_counts()
    print("")
    print("Still not covered by any API - check by hand periodically:")
    print("  - Google Scholar (no API; it indexes preprint bibliographies OpenAlex misses)")
    print("  - apps/sites carrying the CC BY attribution in a privacy policy or credits page")
    return 0


def _download_counts():
    """Aggregate usage: how many people took the data, from the two hosts that count it.

    Not a list of users - neither host discloses who - but it is the only measure of uptake
    that does not depend on someone choosing to cite. Reported beside the registry so a
    short registry is never mistaken for no usage.
    """
    import requests
    UA = "hfdatalibrary-usage-check (mailto:aelkassabgi@uca.edu)"
    print("")
    print("download counts (aggregate uptake; neither host names the downloader):")
    try:
        r = requests.get("https://zenodo.org/api/records/19501605", timeout=60,
                         headers={"User-Agent": UA})
        if r.status_code == 200:
            st = r.json().get("stats") or {}
            print("  Zenodo 19501605      views=%-6s unique=%-6s downloads=%-5s unique=%s"
                  % (st.get("views"), st.get("unique_views"),
                     st.get("downloads"), st.get("unique_downloads")))
        else:
            print("  Zenodo               HTTP %s" % r.status_code)
    except Exception as e:                                   # noqa: BLE001
        print("  Zenodo               unreachable (%s)" % type(e).__name__)
    try:
        r = requests.get("https://huggingface.co/api/datasets/elkassabgi/hfdatalibrary",
                         timeout=60, headers={"User-Agent": UA})
        if r.status_code == 200:
            j = r.json()
            print("  Hugging Face         downloads(30d)=%-5s likes=%s"
                  % (j.get("downloads"), j.get("likes")))
        else:
            print("  Hugging Face         HTTP %s" % r.status_code)
    except Exception as e:                                   # noqa: BLE001
        print("  Hugging Face         unreachable (%s)" % type(e).__name__)

def _citation_indexes() -> None:
    """What the citation databases believe, which is NOT the same as who cites you.

    Measured 2026-08-24: arXiv:2605.17705 uses the HF panel and lists the Zenodo DOI as its
    reference [13], yet OpenAlex reports the dataset at 0 citations. The reason is visible in
    the data - OpenAlex holds that preprint (twice: W7161723618, W7161914953) with
    `referenced_works: 0`, i.e. it never parsed the bibliography, and the dataset record is
    indexed_in ['datacite'] with no Crossref deposit. Semantic Scholar 404s on the DOI.

    A zero here therefore means "no index has connected them yet", never "nobody cites you".
    Printing it beside the registry is the point: the registry is ground truth, this is lag.
    """
    try:
        import requests
    except ImportError:
        return
    ua = {"User-Agent": "hfdatalibrary-citation-check (mailto:aelkassabgi@uca.edu)"}
    doi = "10.5281/zenodo.19501605"
    print("")
    print("citation indexes (they lag reality - compare against the registry):")
    try:
        r = requests.get("https://api.openalex.org/works/doi:%s" % doi, timeout=60, headers=ua)
        if r.status_code == 200:
            j = r.json()
            print("  OpenAlex          cited_by_count=%s  indexed_in=%s"
                  % (j.get("cited_by_count"), j.get("indexed_in")))
        else:
            print("  OpenAlex          HTTP %s" % r.status_code)
    except Exception as e:                                       # noqa: BLE001
        print("  OpenAlex          unreachable: %s" % str(e)[:60])
    try:
        r = requests.get("https://api.semanticscholar.org/graph/v1/paper/DOI:%s" % doi,
                         params={"fields": "citationCount"}, timeout=60)
        msg = ("citationCount=%s" % r.json().get("citationCount")) if r.status_code == 200             else "HTTP %s (dataset not indexed)" % r.status_code
        print("  Semantic Scholar  %s" % msg)
    except Exception as e:                                       # noqa: BLE001
        print("  Semantic Scholar  unreachable: %s" % str(e)[:60])


if __name__ == "__main__":
    sys.exit(main())
