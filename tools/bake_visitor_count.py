#!/usr/bin/env python3
"""Bake the current visitor count into every page's footer, for view-source.

WHY THIS EXISTS. js/site.js injects a live counter into the DOM, which covers DevTools' element
inspector and the select-to-reveal line. It does NOT cover "View Page Source": that shows the raw
HTML the server sent, before any JavaScript runs, so a JS-added node is simply absent from it.
The only way a number appears there is if it is in the file on disk.

So this stamps a snapshot into each page and is run before a deploy. The number is therefore
"as of the last deploy", which is why the comment says so explicitly rather than implying it is
live — a stale number presented as current is worse than an honest dated one.

Idempotent: an existing stamp is replaced, never duplicated.
"""
import glob, io, json, os, re, sys, urllib.request

API = "https://api.hfdatalibrary.com/v1/public-stats"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MARK_RE = re.compile(r"[ \t]*<!-- visitor-count:.*?-->\n?", re.S)

def main():
    # An explicit User-Agent is required: Cloudflare answers urllib's default
    # ("Python-urllib/3.x") with 403, while the same URL fetched by curl succeeds. Without this
    # the script cannot read the number at all.
    req = urllib.request.Request(API, headers={
        "User-Agent": "hfdatalibrary-build/1.0 (+https://hfdatalibrary.com)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print("could not fetch stats: %s" % e); return 1

    v, pv, c = d.get("total_visitors"), d.get("total_page_views"), d.get("visitor_country_count")
    asof = str(d.get("as_of") or "")[:10]
    if not isinstance(v, int):
        print("stats payload missing total_visitors; refusing to bake a wrong number"); return 1

    stamp = ("  <!-- visitor-count: %s visitors | %s page views | %s countries"
             " (snapshot taken at deploy time%s; the live figure is on /pages/stats) -->\n"
             % (format(v, ","), format(pv or 0, ","), format(c or 0, ","),
                (", data as of " + asof) if asof else ""))

    pages = sorted(glob.glob(os.path.join(ROOT, "*.html")) + glob.glob(os.path.join(ROOT, "pages", "*.html")))
    changed = 0
    for f in pages:
        s = io.open(f, encoding="utf-8").read()
        if "</footer>" not in s:
            continue
        s2 = MARK_RE.sub("", s)                       # drop any previous stamp
        s2 = s2.replace("</footer>", stamp + "</footer>", 1)
        if s2 != s:
            io.open(f, "w", encoding="utf-8", newline="").write(s2)
            changed += 1
    print("baked %s visitors into %d pages" % (format(v, ","), changed))
    return 0

if __name__ == "__main__":
    sys.exit(main())
