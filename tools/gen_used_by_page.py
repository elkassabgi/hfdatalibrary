#!/usr/bin/env python3
"""Render pages/used-by.html from data/used_by.json.

The page is GENERATED so the list cannot drift from the evidence: every card is built from a
registry entry that carries a verbatim quote or a file path someone actually read. Editing
the HTML by hand would let an unverified claim onto a public page, which is the one thing
this section must not do - it is a page about other people's work, published under their
names.

Markup follows pages/cite.html exactly (nav, hero, footer, fonts, stylesheet) because hf is
the design canon for the family.
"""
from __future__ import annotations

import html
import json
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY = os.path.join(ROOT, "data", "used_by.json")
OUT = os.path.join(ROOT, "pages", "used-by.html")

KIND_LABEL = {"paper": "Research paper", "product": "Product", "code": "Code",
              "directory": "Listing", "course": "Teaching", "post": "Article",
              "mention": "Mention"}
NAV = [("data", "Data"), ("tickers", "Tickers"), ("docs", "Documentation"), ("api", "API"),
       ("download", "Download"), ("code", "Code"), ("ai-prompts", "AI Prompts"),
       ("cite", "Cite"), ("stats", "Stats"), ("contact", "Contact")]


def card(u: dict) -> str:
    e = html.escape
    bits = []
    bits.append('<div style="border:1px solid var(--border,#e2e8f0); border-radius:8px; '
                'padding:1.25rem 1.5rem; margin-bottom:1.25rem;">')
    bits.append('<div style="font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; '
                'color:#64748b; margin-bottom:0.35rem;">%s &middot; %s</div>'
                % (e(KIND_LABEL.get(u.get("kind"), u.get("kind", ""))), e(str(u.get("date", "")))))
    bits.append('<h3 style="margin:0 0 0.25rem;"><a href="%s" rel="nofollow noopener">%s</a></h3>'
                % (e(u.get("url", "#")), e(u.get("title", ""))))
    if u.get("authors"):
        bits.append('<p style="margin:0 0 0.6rem; color:#475569;">%s%s</p>'
                    % (e(u["authors"]),
                       (" &middot; " + e(u["venue"])) if u.get("venue") else ""))
    if u.get("how"):
        bits.append('<p style="margin:0 0 0.6rem;">%s</p>' % e(u["how"]))
    if u.get("quote"):
        bits.append('<blockquote style="margin:0 0 0.6rem; padding:0.5rem 0 0.5rem 1rem; '
                    'border-left:3px solid var(--navy,#0f172a); color:#334155;">%s</blockquote>'
                    % e(u["quote"]))
    tags = []
    if u.get("cites_doi"):
        tags.append("cites the DOI")
    if u.get("library"):
        tags.append(e(u["library"]))
    if u.get("verified_utc"):
        tags.append("verified " + e(u["verified_utc"]))
    if tags:
        bits.append('<p style="margin:0; font-size:0.85rem; color:#64748b;">%s</p>'
                    % " &middot; ".join(tags))
    bits.append("</div>")
    return "\n      ".join(bits)


def main() -> int:
    reg = json.load(open(REGISTRY, encoding="utf-8"))
    uses = sorted(reg.get("uses", []), key=lambda u: str(u.get("date", "")), reverse=True)
    kinds = Counter(u.get("kind") for u in uses)
    summary = ", ".join("%d %s" % (n, KIND_LABEL.get(k, k).lower() + ("s" if n != 1 else ""))
                        for k, n in kinds.most_common())
    nav = "\n        ".join(
        '<li><a href="%s">%s</a></li>' % (h, t) for h, t in NAV)
    cards = "\n      ".join(card(u) for u in uses)

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Used by - Who Cites and Uses This Data - HF Data Library</title>
  <meta name="description" content="Papers, products and code that use the HF Data Library and the other ElkassabgiData libraries. Every entry is verified against the source.">
  <link rel="canonical" href="https://hfdatalibrary.com/pages/used-by">
  <meta name="author" content="Ahmed Elkassabgi">
  <meta property="og:title" content="Used by - Who Cites and Uses This Data">
  <meta property="og:description" content="Papers, products and code that use the HF Data Library. Every entry verified against the source.">
  <meta property="og:url" content="https://hfdatalibrary.com/pages/used-by">
  <meta property="og:type" content="article">
  <meta property="og:image" content="https://hfdatalibrary.com/assets/og-image.svg">
  <meta property="og:site_name" content="HF Data Library">
  <meta property="og:locale" content="en_US">
  <meta name="twitter:card" content="summary_large_image">
  <link rel="icon" type="image/svg+xml" href="../assets/favicon.svg">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="../css/style.css">
</head>
<body>

<nav class="navbar">
  <div class="container">
    <a href="../" class="navbar-brand">HF Data <span>Library</span></a>
    <button class="nav-toggle" onclick="document.querySelector('.nav-links').classList.toggle('open')" aria-label="Toggle navigation">&#9776;</button>
    <ul class="nav-links">
        {nav}
    </ul>
  </div>
</nav>

<section style="background: var(--navy); color: #fff; padding: 3rem 0;">
  <div class="container">
    <h1 style="color:#fff; margin-bottom:0.5rem;">Used by</h1>
    <p style="color: rgba(255,255,255,0.7); font-size:1.1rem; margin:0;">Work that uses these libraries: {summary}. Last checked {html.escape(str(reg.get("last_checked_utc", "")))}.</p>
  </div>
</section>

<section class="section">
  <div class="container" style="max-width:880px;">
    <p style="color:#475569;">Every entry below was checked against its source &mdash; a quote read from the paper itself, an attribution read from the product's own pages, or the file in the repository that makes the call. Nothing is listed on the strength of a search result. If your work belongs here, or you would rather it did not, <a href="contact">get in touch</a>.</p>
      {cards}
    <p style="color:#64748b; font-size:0.9rem;">Uses are found with <code>tools/track_uses.py</code>, which re-runs the searches that found these. It reports candidates only; each is confirmed by hand before it appears here.</p>
  </div>
</section>

<footer class="footer">
  <div class="container">
    <div class="footer-bottom">
      <p>&copy; 2026 Ahmed Elkassabgi. University of Central Arkansas.</p>
      <p class="orcid">ORCID: <a href="https://orcid.org/0000-0002-5926-7493">0000-0002-5926-7493</a></p>
    </div>
  </div>
</footer>

<script src="../js/site.js"></script>
</body>
</html>
"""
    open(OUT, "w", encoding="utf-8", newline="\n").write(doc)
    print("wrote %s (%d entries: %s)" % (OUT, len(uses), summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
