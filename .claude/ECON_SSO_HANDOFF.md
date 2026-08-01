# Econ sign-in: what is known, what is unverified, what I got wrong

Written 2026-08-01 after a long unsuccessful session. Read this before touching anything.
The point of this file is to stop the next person repeating my mistakes, not to hand over a
theory. Several "findings" below are marked UNVERIFIED because I never proved them and I
asserted several things tonight that turned out to be false.

## The symptom, in the owner's words

He tested repeatedly in a fresh incognito window. Two different reports, and BOTH are data:

1. "i opened incognito signed in at hf then went to econ and it was already signed in but
   showing account not ahmed" — the HF→Econ silent SSO WORKED. Key delivered, downloads
   would have worked. The only defect was the nav label.
2. After signing in on econ itself via the ElkassabgiData popup: signed in on /account, but
   /download says "not signed in" and demands a pasted API key.

I spent the session conflating these. They are different paths with different mechanisms.
Report 1 is a cosmetic bug on a working path. Report 2 is a broken path.

## Path A — HF → Econ silent SSO (WORKS, per the owner)

`catalog/site/assets/sso.js`, loaded by every Econ page. Step 6 redirects to
`api.hfdatalibrary.com/v1/auth/sso?return=<page>`; `handleSSO` (hf_wt_sso/api/src/index.js,
search `async function handleSSO`) reads HF's first-party cookie and 302s back with
`#sso_key=…&sso_name=…`. Step 1 stores both in localStorage (`edl_key`, `edl_name`).

VERIFIED by reading the code: handleSSO DOES send `sso_name`. My earlier claim that it
didn't was wrong — I asserted it before reading the function.

The nav label fix is deployed (commit 9b627a6): read `edl_name`, show the first name, fall
back to "Account". The owner says it still shows "Account", which is NOT yet explained.
Candidates, none tested:
  - `edl_name` genuinely empty because `user.name` is empty for his account in D1
  - the label is set, then something re-renders and overwrites it
  - `updateUI()` returns early, or its selector misses on that page
  - browser cache — see the CACHE TRAP below before assuming anything about what is served
NEXT STEP: in a signed-in browser, console: `localStorage.getItem('edl_name')`. That single
value splits "never stored" from "stored but not displayed". I never obtained it.

## Path B — the ElkassabgiData popup on Econ (BROKEN)

`catalog/site/account.html` loads `accounts.elkassabgidata.com/sdk/ekd-sso.js`, calls
`EKD.login()`, and shows "Signed in as …" on success. Navigating to /download shows signed
out.

VERIFIED by reading the SDK: it persists to localStorage as `ekd_rt` (refresh token) and
`ekd_at` (access token). localStorage is per-origin, and /account and /download are the same
origin, so they DO share storage. A session stored by one is visible to the other.

UNVERIFIED, and my best remaining guess: the popup's token exchange returns an access token
but no refresh token, so `setRt()` stores nothing, and the "Signed in as …" the owner sees
comes from the in-memory `at` variable, which dies on navigation. Check
`handleTokenExchange` in hf_wt_sso/api/src/index.js — does the popup flow return a refresh
token? Compare with `/token/refresh`, which demonstrably persists one (HF sessions survive).
NEXT STEP: sign in on /account, then console: `Object.keys(localStorage).filter(k=>k.startsWith('ekd'))`.
Empty ⇒ nothing was persisted ⇒ the bug is server-side in the exchange.

## THE CACHE TRAP — read this before verifying anything

Every Econ page loads `assets/sso.js?v=<version>`. The URL is the cache key. That version
string had not changed since 15 July, so hours of edits to that file were deployed and never
reached a browser: the pinned URL served 9,087 bytes while the file on disk was 7,199.

Worse, every check I ran appended my own `?cb=random` — a DIFFERENT URL that misses the cache
and returns the fresh file. So my verifications passed on bytes no visitor was served. If you
verify a cache-busted URL you have proved the origin is correct and NOTHING about what the
browser gets. Fetch the exact pinned URL the pages reference.

Bumped to `?v=20260801a` in commit f4a106e — but that commit was inside the range reverted by
410a7f4, so CHECK WHETHER THE BUMP IS STILL PRESENT:
  grep -o 'sso\.js?v=[0-9a-z]*' catalog/site/account.html
If it is back to 20260715c, the trap is live again and any sso.js change you make will appear
to do nothing.

FIX PROPERLY: derive the version from a content hash in `catalog/gen_site.py` instead of a
hand-edited constant. Otherwise this recurs every time the file changes.

## What I broke and reverted (do not repeat)

I widened `signedIn()` in sso.js to count a family session so the nav would show a name for
popup users. `signedIn()` has TWO consumers: `updateUI()` (the nav) and step 2, which returns
early when true and thereby SKIPS the bounce that fetches the key. So widening it disabled the
working Path A. Downloads stopped; the owner reported it as "back to sign-in and I can't
download". If you need a family-aware predicate, add a SECOND function; do not widen this one.

Reverted in 410a7f4. Econ is now at its pre-session state plus the nav label change (9b627a6).

## Verification rules earned the hard way

- Do not verify a fix by grepping the deployed HTML for your new code. That proves the bytes
  arrived, not that they can run. I shipped a fix calling `window.EKD` onto a page that never
  loaded the SDK; the guard `if (window.EKD && …)` failed silently into the old behaviour and
  I reported it fixed. (R207)
- Pass ABSOLUTE paths to .NET file APIs in PowerShell. `Set-Location` does not change the
  directory they resolve against, so a relative path silently reads a different worktree and
  reports your edits MISSING. (R206)
- When the owner says he saw something work, that is evidence. I contradicted his account
  three times by reconstructing timelines, and he was right every time. The simplest reading
  of a user's direct observation is usually correct.
- One question, one measurement, before any edit. Six of tonight's rounds were: infer a cause,
  ship a fix, ask him to test. Every inference was wrong and he paid for each one.

## Deploy mechanics

- Econ is a MANUAL Cloudflare Pages deploy. A git push publishes NOTHING.
    npx wrangler pages deploy catalog/site --project-name=econdatalibrary --branch=main
  Its CI workflow exists but is parked on workflow_dispatch: the repo's CLOUDFLARE_API_TOKEN
  lacks `Account → Cloudflare Pages → Edit`. Granting it unblocks econ, elkassabgidata and
  ipdatalibrary, all on one account.
- HF deploys Pages + Worker as two PARALLEL jobs on push to main. NOT atomic — a worker change
  must work against today's pages and vice versa.
- `catalog/site/*.html` is GENERATED by `catalog/gen_site.py`. account.html, download.html,
  mcp.html and status.html are on its protected list and are hand-maintained.

## Also in flight

Two background workflows were still running when this was written: a security-findings
cleanup on hf_wt_sso, and an Econ SEO job that regenerates catalog/site. Neither deploys
itself. Review both before shipping, and re-verify the Econ pages after the SEO job writes,
since it rewrites the same directory.
