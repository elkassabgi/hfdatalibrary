# ElkassabgiData family SSO — how it works, and how to add a new site

**Status:** live in production. **Last updated:** 2026-08-01.
**Authoritative source:** `api/src/index.js` in this worktree (branch `sso-build`). Every
constant and route below was read out of that file, not remembered. If this document and the
code disagree, the code is right and this document is stale — fix it.

---

## 1. What the system is

One ElkassabgiData account works on every site in the family. A person registers once and is
recognised on hfdatalibrary.com, econdatalibrary.com, ipdatalibrary.com and the portal, without
registering again and without a separate password per site.

`accounts.elkassabgidata.com` is the **identity provider (IdP)**. It is the only host that:

* stores the login session cookie (`ekd_session`),
* renders the login / register / consent screens,
* mints authorization codes and family tokens.

Every other site is a **client**. A client never sees a password and never stores a long-lived
credential of its own — it holds a refresh token it obtained from the IdP and can be cut off
centrally at any time.

**One worker serves both.** `api/src/index.js` is deployed once and answers on
`api.hfdatalibrary.com` *and* `accounts.elkassabgidata.com`. The accounts host is dispatched at
the very top of `fetch()` to `handleAccountsHost()`, a separate fail-closed router, so the IdP
can never reach the data/admin route table. Do not "simplify" that split.

---

## 2. The moving parts

| Piece | Where | What it is |
|---|---|---|
| IdP | `accounts.elkassabgidata.com` | login, register, consent, token endpoints |
| SDK | `https://accounts.elkassabgidata.com/sdk/ekd-sso.js` | the client-side library; served by the worker, not a CDN |
| Client registry | D1 table `sso_clients` | which origins may participate |
| Client callback | `<your-origin>/auth/callback` | a static page every client must serve |
| User store | D1 table `users` | shared by all sites — one row per person, family-wide |

`sso_clients` columns: `origin`, `brand_name`, `logo_url`, `theme_json`, `redirect_exact`,
`status`, `created_at`.

---

## 3. Two planes — keep them straight

This is the distinction that causes the most confusion, so it comes before the mechanics.

**Identity plane — "who is this person?"**
Answered by the IdP. Produces a short-lived **access token** (`ekd_at`) and a rotating
**refresh token** (`ekd_rt`). This is what makes the nav say a name instead of "Sign in".

**Data plane — "may this request download this file?"**
Answered by each site's own API. Accepts *either* the family access token as
`Authorization: Bearer <access_token>`, *or* a per-user `X-API-Key`.

A visitor can legitimately be signed in (identity plane) and still not hold an API key (data
plane). Treating those as one condition has broken downloads here more than once — see §9.

---

## 4. Token model (authoritative TTLs)

| Token | Lives in | TTL | Notes |
|---|---|---|---|
| `ekd_session` | cookie, **host-only** on `accounts.elkassabgidata.com` | 30 days (`SESSION_DAYS`) | the IdP login session. Never visible to any client. |
| authorization code | server-side (`sso_codes`), hashed | **60 s** (`CODE_TTL_SEC`) | single-use, PKCE-bound |
| access token | `localStorage.ekd_at` as `{t, e}` | **900 s** (`EDL_AT_TTL_SEC`) | sent as `Authorization: Bearer` |
| refresh token | `localStorage.ekd_rt` | rotating; **720 h absolute** (`EDL_RT_TTL_HOURS`) = 30 days | reuse is detected and kills the whole chain |
| `hfd_session` | cookie, host-only on `api.hfdatalibrary.com` | 30 days | **legacy** password/OAuth session, hf only |
| API key | `users.api_key` | 30 days (`API_KEY_DAYS`), renewed on session use | users are told to obtain a new one when it lapses |

`RT_GRACE_SEC = 10` exists so two tabs refreshing at the same moment is not treated as theft.

**`localStorage` is per-origin.** This single fact explains nearly every cross-site bug in this
system. econdatalibrary.com cannot read hfdatalibrary.com's tokens. Ever.

---

## 5. How a person signs in

**a. Pop-up (the normal path).** `EKD.login()` opens `accounts.*/authorize`, the user
authenticates, the IdP 303s to `<origin>/auth/callback#code=…&state=…`, the callback posts the
code to the opener, the SDK exchanges it at `/token/exchange`.

**b. Google / ORCID.** Buttons on the IdP's own login page. On the accounts host the Google
`id_token` is RS256-verified against Google's JWKS and **rejected outright unless
`email_verified` is true**.

**c. Password.** `POST /login` on the IdP. `/login`, `/login/2fa` and `/register` are
**POST-only** — a GET returns 404 by design. Do not treat that 404 as breakage.

**d. Silent cross-site resume (added 2026-08-01).** See §6.

---

## 6. Silent cross-site resume — `prompt=none`

**The problem it solves.** Sign in on econ, open hf: hf's `localStorage` is empty and
`ekd_session` is host-only on the IdP, so hf had no way to learn the session existed. It showed
"Sign in" to someone who had signed in a tab ago. The IdP knew; nobody asked it. And nothing
*could* ask silently — `/authorize` rendered a consent page needing a click, and `EKD.login()`
needs a click.

**The mechanism.** `GET /authorize?prompt=none&…` performs the identical validation
(registered client, `status = active`, exact redirect match, PKCE S256) and then answers with no
UI:

* live `ekd_session` → mints a code and 303s to the client's own registered callback;
* no session → 303s back with `#error=login_required`.

**Why a top-level redirect and not a hidden iframe.** `ekd_session` is `SameSite=Lax`, which a
top-level GET carries and a framed request does not — and an iframe is third-party, so Safari,
Firefox and Chrome-incognito would strip the cookie and report a signed-in user as signed out.
The redirect costs a visible flash; an iframe costs correctness.

**Why skipping consent is safe here.** The consent page and its gesture token defend the consent
*POST*. There is no POST on this path. The code is bound to
`(user, client_id, redirect_exact, state, code_challenge)` and is delivered to the client's own
pre-registered callback, and redeeming it requires the PKCE verifier that never left the
initiating page. A hostile site can start the flow but the code lands on *your* callback, not
theirs. The one thing a caller learns is whether the browser has a family session — which is why
it is restricted to registered, active clients.

**Loop safety is the dangerous part.** `auth/callback.html` writes `ekd_silent_done` to
`sessionStorage` **before anything can fail** — on `login_required`, on a state mismatch, on a
failed exchange — and the bouncer refuses to start when that flag is present. One attempt per
browser session; a broken IdP degrades to "signed out", never to an infinite redirect.

---

## 7. ADDING A NEW SITE — the checklist

Say the new site is `https://example.com`.

### 7.1 Register the client (D1)

```sql
INSERT INTO sso_clients (origin, brand_name, redirect_exact, status, created_at)
VALUES ('https://example.com', 'Example Library',
        'https://example.com/auth/callback', 'active', strftime('%s','now'));
```

Register the `www.` host too if it resolves, as a **separate row with its own
`redirect_exact`** — `redirect_exact` is an exact string match, not a prefix or pattern.

```bash
npx wrangler d1 execute hfdatalibrary-db --remote --command "SELECT origin, redirect_exact, status FROM sso_clients"
```

### 7.2 Serve `/auth/callback`

Copy `auth/callback.html` from this repo verbatim. It handles **both** arrivals:

* **popup** (`window.opener` present) → postMessage the code to the opener and close;
* **top-level** (no opener) → redeem the code itself, write `ekd_rt` / `ekd_at`, return the user
  to where they were.

Branch 2 is what makes silent resume work. A callback that only does branch 1 leaves the user
stranded on "Completing sign-in…" forever.

Confirm it is actually served at that exact path before going further:
```bash
curl -s -o /dev/null -w "%{http_code}\n" https://example.com/auth/callback
```

### 7.3 Load the SDK and initialise

```html
<script src="https://accounts.elkassabgidata.com/sdk/ekd-sso.js"></script>
<script>
  EKD.init();                                   // clientId defaults to location.origin
  document.querySelector('#signin').onclick = () => EKD.login();
  EKD.on('login',  () => paintNav());
  EKD.on('logout', () => paintNav());
</script>
```

`EKD.init()` also validates any stored session against the server on load, so a central
"log out everywhere" is reflected promptly.

### 7.4 Authenticate data requests

```js
const at = await EKD.getAccessToken();          // null when signed out; never throws
const headers = at ? { 'Authorization': 'Bearer ' + at } : {};
```

Server side, the token is validated by `validateFamilyToken`, which requires a live
`family_access` session, an active user, an active registered client, **and `audience` equal to
the requesting Origin**. A token minted for econ cannot be replayed against hf.

### 7.5 Add the silent resume (optional but recommended)

Copy the `§SILENT-RESUME` block from `js/site.js`. Preconditions, all required:

* this origin holds no credential of its own;
* `sessionStorage.ekd_silent_done` is unset;
* not already on `/auth/callback`;
* secure context with `crypto.subtle`;
* **not a crawler** — see §8.

### 7.6 Verify before declaring done

```bash
# signed-out browser must terminate with login_required, not an HTML page
curl -s -o /dev/null -w "%{http_code} %{redirect_url}\n" \
  "https://accounts.elkassabgidata.com/authorize?response_type=code&prompt=none\
&client_id=https%3A%2F%2Fexample.com\
&redirect_uri=https%3A%2F%2Fexample.com%2Fauth%2Fcallback\
&state=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\
&code_challenge=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB&code_challenge_method=S256"
```
Expect `303 https://example.com/auth/callback#error=login_required&state=…`.

Then confirm an **unregistered** `client_id` and a **mismatched** `redirect_uri` both return
`400` with no redirect. If either redirects, stop — you have an open redirect.

---

## 8. Invariants — do not break these

1. **`redirect_exact` is exact.** No prefixes, no wildcards. It is the only thing standing
   between `prompt=none` and an open redirect.
2. **PKCE S256 is mandatory.** `code_challenge` must be 43 chars base64url. No `plain`.
3. **Codes and errors ride in the URL *fragment*, never the query string.** A fragment is never
   sent to a server, so it stays out of access logs, browser history and `Referer`. The session
   id was moved out of the query string on 2026-08-01 for exactly this reason.
4. **Never widen the predicate that guards a bounce.** See §9.
5. **Never bounce a crawler.** Googlebot executes JavaScript; without a guard it follows the
   silent-resume redirect off your indexable page to a `noindex` auth host, on every page of
   every crawl. Guard on `navigator.webdriver` plus a UA test.
6. **Revocation goes through one helper.** `revokeAllUserCredentials()` is the only function
   that knows the full set (sessions, refresh chains, SSO codes, download tokens, pending-2FA,
   password resets). Hand-rolling a subset is how "log out everywhere" silently missed 384
   sessions.
7. **Prune what you mint.** Every short-lived credential table needs a sweep, or D1 fills and
   *writes* start failing. `pruneExpiredCredentials()` in the 02:00 cron covers eight tables.
   `sso_refresh_tokens` is swept on `absolute_expires_at`, **never** `expires_at` — a retained
   used row is what turns a replayed stolen token into *detected theft* rather than a silent
   rejection.

---

## 9. Gotchas that have actually bitten us

**`localStorage` is per-origin.** Not a bug. The reason cross-site resume needs the IdP.

**Two predicates, not one.** On econ, `hasKey()` ("is the API key here?") guards the bounce that
*fetches* the key, and `signedIn()` ("should the nav show a name?") drives the nav. They were
briefly one function; a signed-in visitor then short-circuited the fetch and downloads stopped
working. Keep them separate.

**Cache-pin your SDK/glue JS.** Econ's pages reference `assets/sso.js?v=<version>`. Change the
file *and* the version, or every returning browser runs the old file. A stale pin cost hours
once: the pinned URL served 9,087 B while disk held 7,199 B, and every verification used a
cache-busting query string — a *different URL* — so it looked fine.

**Worker and Pages deploy separately.** They are not atomic and Pages propagation lags. Any
change where the worker and the page must agree has to ship in two steps: make the page accept
**both** shapes, confirm it live, *then* move the worker. Doing both at once signs everyone out
with no error, in whichever direction lands first.

**`GET /login` returning 404 is correct.** `/login`, `/login/2fa` and `/register` are POST-only
on the IdP. `/authorize` is the GET entry point.

**Verify with the URL the code actually builds.** Not one you typed from memory. Extract the
builder from the deployed asset and run *that*.

**Present ≠ runnable.** A file can be served and still not execute — a missing dependency, a
guard that fails closed, a value the response never sends. Test the behaviour, not the bytes.

---

## 10. Where things live

| What | Path |
|---|---|
| Worker (IdP + hf API) | `api/src/index.js` (branch `sso-build`) |
| SDK source | `EKD_SDK_JS` inside `api/src/index.js` |
| hf callback | `auth/callback.html` |
| hf nav + silent resume | `js/site.js` (`§SILENT-RESUME`) |
| econ glue | `catalog/site/assets/sso.js` (`§SILENT-FAMILY-RESUME`) |
| econ callback | `catalog/site/auth/callback.html` |
| Build history | `AUTH_SSO_BUILD_LOG.md` (econ repo) |
| Mistake ledger | `.claude/MISTAKES.md` (hfdatalibrary repo) |

**Deploys**
```bash
npx wrangler deploy                                             # worker (from api/)
npx wrangler pages deploy .            --project-name=hfdatalibrary
npx wrangler pages deploy catalog/site --project-name=econdatalibrary
```
Both Pages projects are **manual** (Git integration off) — a `git push` publishes nothing.
Always check the live URL after deploying.

---

## 11. Locked out of 2FA — the owner's recovery path

**You cannot be permanently locked out of your own account.** Recovery codes exist for ordinary
users, who have nothing else. You own the database, which outranks every credential in this
document.

If you lose your authenticator *and* your recovery codes, run this and 2FA is off:

```bash
npx wrangler d1 execute hfdatalibrary-db --remote --command "UPDATE users SET totp_enabled = 0, totp_secret = NULL WHERE email = 'YOUR@EMAIL'"
```

Then sign in with your password as normal, and re-enrol if you want to. Verified 2026-08-01: the
statement is valid and is a no-op when 2FA is already off, so running it when you don't need it
costs nothing.

Clear the recovery codes too, so a set you no longer trust cannot be used later:

```bash
npx wrangler d1 execute hfdatalibrary-db --remote --command "DELETE FROM totp_backup_codes WHERE user_id = (SELECT id FROM users WHERE email = 'YOUR@EMAIL')"
```

**Why this is not a back door.** It needs Cloudflare account access — the same access that could
read the whole users table, rotate any API key, or delete the worker. Anyone holding it does not
need to bypass your second factor. The factor protects the account from people on the internet,
not from the person who owns the infrastructure, and pretending otherwise would only mean the
owner is the one person the system can permanently lock out.

**For everyone else**, the recovery codes are the only path: no admin reset exists, and
`/2fa/disable` requires either a working authenticator code or one of those codes. That is
deliberate — an admin-reset button is a social-engineering target, and this service has one
admin and no support desk to verify anybody's identity.

---

## 12. The rate limiter is still in shadow mode — how to turn it on

`rateLimit()` on `/authorize` and `/token/*` logs a would-be denial and **never blocks**. Five of
its six call sites pass `enforce = false`; only `oauth_start_ip` enforces, because that path
writes state before authentication. The soak it was waiting on concluded PASS on 2026-07-29.

**To enforce**, change the last argument from `false` to `true` at those five call sites
(`grep -n "await rateLimit(env," api/src/index.js`), deploy, and watch for `evt:"rate_limit"` in
`wrangler tail`. To roll back, flip them and deploy again — one command each way.

**The evidence that it is safe**, measured 2026-08-01 against production:

| bucket | cap (per 60s) | peak observed | headroom |
|---|---|---|---|
| `authz_ip`, `exch_ip` | 120 | 5 logins/IP/min | 24× |
| `rt_ip` | 240 | refreshes are ~4/hour/tab (15-min access-token TTL) | very large |
| `exch_acct` / `rt_acct` | 30 / 60 per ACCOUNT | one person cannot approach this | very large |

The 124 downloads/IP/min seen in the logs is **not** relevant here: downloads use
`checkRateLimit` with `api:download` (100/min per user), a different mechanism that never touches
these buckets.

**Why it was not flipped automatically.** Enforcement can only ever DENY traffic. It was left in
shadow while Ahmed was actively testing sign-in, because a stray 429 would be indistinguishable
from a real fault — and the security benefit is small (the caps are far above any human, so this
guards against automation, which is also bounded by the other limiter). It is a deliberate
change for a quiet moment, not something to slip in.
