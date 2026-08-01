/**
 * HF Data Library — API Worker v2
 * Full auth system with admin controls
 * Cloudflare Workers + R2 + D1
 * Author: Ahmed Elkassabgi, University of Central Arkansas
 */

const SESSION_DAYS = 30;
const API_KEY_DAYS = 30;
const ADMIN_EMAILS = ['elkassabgi@yahoo.com', 'elkassabgi@gmail.com'];
const ADMIN_NOTIFY = 'elkassabgi@gmail.com';
const FROM_EMAIL = 'noreply@hfdatalibrary.com';
const FROM_NAME = 'HF Data Library';
const NEWSLETTER_FROM = 'newsletter@hfdatalibrary.com';
const NEWSLETTER_FROM_NAME = 'HF Data Library Newsletter';
const SITE_URL = 'https://hfdatalibrary.com';

// Known disposable / temporary email domains — a starter set of common
// providers plus ones observed in abuse here. Used to flag likely throwaway
// signups in the admin user list (a review signal, not an auto-ban). Extend
// as new ones appear. NOTE: privacy providers (proton.me, tutanota) are NOT
// disposable and are deliberately excluded.
const DISPOSABLE_EMAIL_DOMAINS = new Set([
  'passmail.net', 'passinbox.com', 'passmail.com',
  'mailinator.com', 'guerrillamail.com', 'guerrillamail.info', 'grr.la', 'sharklasers.com',
  '10minutemail.com', '10minutemail.net', 'temp-mail.org', 'tempmail.com', 'tempmail.net',
  'tempr.email', 'throwawaymail.com', 'throwaway.email', 'yopmail.com', 'getnada.com', 'nada.email',
  'trashmail.com', 'trashmail.de', 'maildrop.cc', 'dispostable.com', 'fakeinbox.com', 'mailnesia.com',
  'mintemail.com', 'mohmal.com', 'emailondeck.com', 'spamgourmet.com', 'tempinbox.com', 'mailcatch.com',
  'moakt.com', 'discard.email', 'inboxkitten.com', 'harakirimail.com', 'fakemail.net', 'tmail.ws',
  'mailto.plus', 'fexbox.org', 'maileax.com', 'vmani.com', 'dropmail.me', 'minuteinbox.com',
  'burnermail.io', 'guerrillamailblock.com', 'spam4.me', 'mvrht.net', 'tafmail.com', 'cuvox.de',
]);

function isDisposableEmail(email) {
  if (!email || typeof email !== 'string') return false;
  const at = email.lastIndexOf('@');
  if (at < 0) return false;
  return DISPOSABLE_EMAIL_DOMAINS.has(email.slice(at + 1).toLowerCase().trim());
}

// Allowed CORS origins
const ALLOWED_ORIGINS = [
  'https://hfdatalibrary.com',
  'https://www.hfdatalibrary.com',
  'http://localhost:8080', // for local dev
];

// ── Family SSO M1 scaffolding (AUTH_SSO_PLAN v3) ────────────────────────────
// §6 fail-closed hostname gate. ONLY these hosts serve; anything else — most
// importantly the default *.workers.dev bypass — 404s before any routing.
// (accounts.elkassabgidata.com is added in M2.) Loopback is kept for `wrangler
// dev`; Cloudflare never routes a request with hostname 'localhost' to the
// deployed worker, so this is not a production bypass.
const HOSTNAME_ALLOW = new Set([
  'api.hfdatalibrary.com',
  'localhost',
  '127.0.0.1',
]);

// §6 The IdP host. accounts.elkassabgidata.com is dispatched to its OWN
// fail-closed explicit-allowlist router (handleAccountsHost) at the very top of
// fetch(), BEFORE the api.* gate — so it can never fall through to the data or
// admin path table. It is intentionally NOT in HOSTNAME_ALLOW. Only these paths
// (+ the /sdk/ and /.well-known/ prefixes) serve on it; everything else 404s.
const ACCOUNTS_HOST = 'accounts.elkassabgidata.com';
const IDP_ORIGIN = 'https://accounts.elkassabgidata.com';
const ACCOUNTS_ALLOW = new Set([
  '/authorize',
  '/login',
  '/login/2fa',
  '/register',
  '/token/exchange',
  '/token/refresh',
  '/logout',
  '/account',
  '/account/regenerate-key',
  '/account/logout',
  '/account/update-profile',
  '/account/change-password',
  '/account/export',
  '/account/resend-verification',
  '/account/delete',
  '/csp-report',
  '/v1/auth/google/start',
  '/v1/auth/orcid/start',
  '/v1/auth/google/callback',
  '/v1/auth/orcid/callback',
]);

// §5 M2 token TTLs. SHORT TTLs are written with SQLite datetime() arithmetic,
// NEVER toISOString() (the 'T' > ' ' lexical-compare bug makes a toISOString
// expiry validate for ~a full day).
//
// §EXPIRY-COMPARE (2026-07-31). That write rule was only ever honoured by the M2
// SSO code below. Everything older — web sessions, download tokens, password
// resets, 2FA pending rows, ORCID link state, api_key_expires_at — stores
// toISOString(), so `sessions` alone holds BOTH formats right now: createSession
// writes '2026-07-31T13:00:00.000Z', createIdpSession writes '2026-07-31 13:00:00'.
// SQLite compares TEXT byte by byte and 'T' (0x54) sorts above ' ' (0x20), so a
// bare `expires_at > datetime('now')` read every ISO row as live until the UTC
// date rolled over: a download link stamped 10 minutes stayed redeemable all day,
// a reset link stamped 1 hour stayed redeemable all day.
//
// Two fixes were rejected. Rewriting the writers to datetime() arithmetic leaves
// every row already in D1 with its old text, so it fixes nothing that is live and
// splits each table into two formats. Binding a JS ISO 'now' on the read side is
// worse: measured, a space-format idp_master row with ten hours of life left
// compares BELOW an ISO 'now', so the first request after deploy would log every
// family SSO user out. So every expiry comparison in this file reads
//     datetime(<column>) > datetime('now')
// which canonicalises '…T13:00:00.000Z' and '… 13:00:00' to the same UTC text
// before comparing — correct for the rows that exist today and for whichever
// format a future writer picks. Unparseable text yields NULL, so the predicate
// fails: an unreadable expiry is expired, never authenticated. Writers are left
// exactly as they are; this is a read-side change only.
const EKD_SESSION_DAYS = 30;   // idp_master (ekd_session), 30d sliding
const EDL_AT_TTL_SEC = 900;    // family access token, 15 min
const EDL_RT_TTL_HOURS = 720;  // refresh-token absolute cap = 30 days (G-H LOCKED 2026-07-18; was 24 h — restores the pre-SSO 30-day persistence)
const CODE_TTL_SEC = 60;       // one-time authorization code
const GESTURE_TTL_SEC = 300;   // consent gesture HMAC token, 5 min
const RT_GRACE_SEC = 10;       // benign multi-tab refresh race window
// §18 DO rate-limit ceilings (per minute); log-only shadow for the first soak.
const AUTHZ_IP_MAX = 120;
const EXCH_IP_MAX = 120;
const EXCH_ACCT_MAX = 30;
const RT_IP_MAX = 240;
const RT_ACCT_MAX = 60;

// hf-owned origins that legitimately use the first-party hfd_session cookie and
// may receive credentialed CORS. Every OTHER family origin uses Authorization:
// Bearer and must NEVER get Access-Control-Allow-Credentials (§8).
const HF_OWNED_ORIGINS = new Set([
  'https://hfdatalibrary.com',
  'https://www.hfdatalibrary.com',
  'http://localhost:8080',
]);

// §8 CSRF surface: account-mutation / admin routes that must reject cross-site
// browser requests regardless of CORS (a cross-site fetch still EXECUTES
// server-side even when the browser blocks reading the response). Matched by
// exact path or, for admin, prefix. Non-browser clients (no Sec-Fetch-Site
// header) and same-origin/same-site requests are allowed.
const MUTATION_GUARD_EXACT = new Set([
  '/v1/auth/regenerate-key',
  '/v1/auth/delete',
  '/v1/auth/update-profile',
  '/v1/auth/change-password',
  '/v1/auth/2fa/setup',
  '/v1/auth/2fa/enable',
  '/v1/auth/2fa/disable',
  // Other authenticated state-changing POSTs. Cross-site is already blocked
  // upstream (hfd_session is SameSite=Lax; the Bearer / X-API-Key path is
  // unforgeable cross-site), so these are uniform defense-in-depth — legit
  // same-site calls send Sec-Fetch-Site: same-site and pass.
  '/v1/auth/logout',
  '/v1/auth/orcid/link-init',
  '/v1/newsletter/subscribe',
  '/v1/newsletter/unsubscribe-toggle',
]);
function isMutationGuarded(path) {
  return MUTATION_GUARD_EXACT.has(path) || path.startsWith('/v1/admin/');
}
// A browser labels genuinely cross-site requests 'cross-site'. same-origin,
// same-site (hfdatalibrary.com → api.hfdatalibrary.com), and absent (non-browser
// API clients) are all allowed.
function isCrossSiteRequest(request) {
  return request.headers.get('Sec-Fetch-Site') === 'cross-site';
}

// §9 client registry, cached ~60s per isolate (branding/CORS-allowlist lookups
// are not urgency-critical; suspend/revocation gets a fast channel in M2).
let _registryCache = null;
let _registryCacheAt = 0;
const REGISTRY_TTL_MS = 60000;       // good-data cache window
const REGISTRY_NEG_TTL_MS = 5000;    // short negative-cache on D1 error
async function getRegistry(env) {
  const now = Date.now();
  if (_registryCache && (now - _registryCacheAt) < REGISTRY_TTL_MS) return _registryCache;
  try {
    const { results } = await env.DB.prepare(
      'SELECT origin, brand_name, logo_url, theme_json, redirect_exact, status FROM sso_clients'
    ).all();
    _registryCache = new Map((results || []).map((r) => [r.origin, r]));
    _registryCacheAt = now;
  } catch (e) {
    // Fail closed and NEGATIVE-CACHE briefly: on a D1 error, keep the
    // last-known-good map if we have one; otherwise serve an empty map (grants
    // nothing beyond the hardcoded hf-owned set). Stamp a SHORT window so an
    // ongoing outage isn't re-queried on every request, while still recovering
    // within a few seconds once D1 heals. Never throws into routing.
    if (!_registryCache) _registryCache = new Map();
    _registryCacheAt = now - (REGISTRY_TTL_MS - REGISTRY_NEG_TTL_MS);
  }
  return _registryCache;
}

// The registry-driven CORS decision (LIVE from the enforcement flip). Rules:
// hf-owned origins → allow + credentials; other registered active origins →
// allow, NEVER credentials; everything else → deny. Fails closed on D1 error
// (hf-owned still allowed via the hardcoded set; family origins denied until
// the registry read recovers).
async function corsDecision(origin, env) {
  if (!origin) return { allow: false, credentials: false };
  if (HF_OWNED_ORIGINS.has(origin)) return { allow: true, credentials: true };
  const reg = await getRegistry(env);
  const row = reg.get(origin);
  if (row && row.status === 'active') return { allow: true, credentials: false };
  return { allow: false, credentials: false };
}

// Country name -> ISO 3166-1 alpha-2 code. Users register by typing the
// country, but the world map (Google GeoChart) and flag CDN both want ISO-2.
// Keys are lowercased; lookup via normalizeCountry() handles both directions.
// Includes common variants (e.g. "USA", "U.S.", "America" all map to US).
const COUNTRY_TO_ISO = {
  // North America
  'united states': 'US', 'united states of america': 'US', 'usa': 'US', 'u.s.': 'US', 'u.s.a.': 'US', 'us': 'US', 'america': 'US',
  'canada': 'CA',
  'mexico': 'MX',
  // Europe
  'united kingdom': 'GB', 'uk': 'GB', 'great britain': 'GB', 'britain': 'GB', 'england': 'GB', 'scotland': 'GB', 'wales': 'GB', 'northern ireland': 'GB',
  'ireland': 'IE',
  'germany': 'DE', 'deutschland': 'DE',
  'france': 'FR',
  'spain': 'ES', 'españa': 'ES',
  'portugal': 'PT',
  'italy': 'IT', 'italia': 'IT',
  'netherlands': 'NL', 'holland': 'NL', 'the netherlands': 'NL',
  'belgium': 'BE',
  'switzerland': 'CH',
  'austria': 'AT',
  'sweden': 'SE',
  'norway': 'NO',
  'denmark': 'DK',
  'finland': 'FI',
  'iceland': 'IS',
  'poland': 'PL',
  'czech republic': 'CZ', 'czechia': 'CZ',
  'slovakia': 'SK',
  'hungary': 'HU',
  'romania': 'RO',
  'bulgaria': 'BG',
  'greece': 'GR',
  'turkey': 'TR', 'türkiye': 'TR', 'turkiye': 'TR',
  'russia': 'RU', 'russian federation': 'RU',
  'ukraine': 'UA',
  'belarus': 'BY',
  'lithuania': 'LT',
  'latvia': 'LV',
  'estonia': 'EE',
  'croatia': 'HR',
  'serbia': 'RS',
  'slovenia': 'SI',
  'luxembourg': 'LU',
  // Asia
  'china': 'CN', 'people\'s republic of china': 'CN', 'prc': 'CN', 'mainland china': 'CN',
  'hong kong': 'HK',
  'taiwan': 'TW', 'republic of china': 'TW', 'roc': 'TW',
  'japan': 'JP',
  'south korea': 'KR', 'korea': 'KR', 'republic of korea': 'KR', 'rok': 'KR',
  'north korea': 'KP', 'dprk': 'KP',
  'india': 'IN',
  'pakistan': 'PK',
  'bangladesh': 'BD',
  'sri lanka': 'LK',
  'nepal': 'NP',
  'singapore': 'SG',
  'malaysia': 'MY',
  'indonesia': 'ID',
  'philippines': 'PH', 'the philippines': 'PH',
  'thailand': 'TH',
  'vietnam': 'VN', 'viet nam': 'VN',
  'cambodia': 'KH',
  'laos': 'LA',
  'myanmar': 'MM', 'burma': 'MM',
  'mongolia': 'MN',
  'kazakhstan': 'KZ',
  'uzbekistan': 'UZ',
  'iran': 'IR',
  'iraq': 'IQ',
  'israel': 'IL',
  'palestine': 'PS',
  'lebanon': 'LB',
  'syria': 'SY',
  'jordan': 'JO',
  'saudi arabia': 'SA', 'ksa': 'SA',
  'united arab emirates': 'AE', 'uae': 'AE', 'u.a.e.': 'AE',
  'qatar': 'QA',
  'kuwait': 'KW',
  'bahrain': 'BH',
  'oman': 'OM',
  'yemen': 'YE',
  'afghanistan': 'AF',
  // Oceania
  'australia': 'AU',
  'new zealand': 'NZ',
  // South America
  'brazil': 'BR', 'brasil': 'BR',
  'argentina': 'AR',
  'chile': 'CL',
  'colombia': 'CO',
  'peru': 'PE',
  'venezuela': 'VE',
  'ecuador': 'EC',
  'uruguay': 'UY',
  'paraguay': 'PY',
  'bolivia': 'BO',
  // Africa
  'south africa': 'ZA',
  'egypt': 'EG',
  'nigeria': 'NG',
  'kenya': 'KE',
  'ethiopia': 'ET',
  'morocco': 'MA',
  'algeria': 'DZ',
  'tunisia': 'TN',
  'ghana': 'GH',
  'tanzania': 'TZ',
  'uganda': 'UG',
  'senegal': 'SN',
  'cameroon': 'CM',
  'zimbabwe': 'ZW',
  'angola': 'AO',
  // Caribbean / Central America
  'costa rica': 'CR',
  'panama': 'PA',
  'guatemala': 'GT',
  'honduras': 'HN',
  'el salvador': 'SV',
  'nicaragua': 'NI',
  'cuba': 'CU',
  'dominican republic': 'DO',
  'jamaica': 'JM',
  'haiti': 'HT',
  'puerto rico': 'PR',
  'trinidad and tobago': 'TT',
};

// Map a free-form country string to a 2-letter ISO code.
// - Already-shaped ISO-2 codes (case-insensitive) pass through, uppercased.
// - 3-letter codes like "USA" / "GBR" are treated as common shorthand and looked up.
// - Full names are looked up case-insensitive after trimming.
// - Returns null for anything we can't classify.
function normalizeCountry(input) {
  if (!input || typeof input !== 'string') return null;
  const s = input.trim();
  if (s.length === 0) return null;
  // Pure ISO-2 (e.g. "US", "cn") — accept directly.
  if (/^[A-Za-z]{2}$/.test(s)) return s.toUpperCase();
  // Full-name (or common abbreviation) lookup.
  const hit = COUNTRY_TO_ISO[s.toLowerCase()];
  return hit || null;
}

// Rate limits: key -> { max, window_seconds }
const RATE_LIMITS = {
  'api:login': { max: 5, window: 300 },         // 5 login attempts per 5 min per IP
  // REGISTRATION — split in two on 2026-07-31 after a real user was locked out for ~50 min.
  // The old rule was a single 3-per-hour-per-IP counter charged at the TOP of handleRegister,
  // before the body was parsed, before Turnstile and before any field was validated. So a
  // typo, a CAPTCHA hiccup or a rejected non-Latin name each burned one of three attempts,
  // and the third mistake locked the person out for the rest of the hour. Worse, the key is
  // the IP: a university NAT, a department or a conference share one, so three fumbled
  // sign-ups could lock out an entire institution — for a library whose users are academics,
  // exactly the wrong shape.
  //   api:register:burst  cheap flood guard, charged on ENTRY. Generous enough that no honest
  //                       person retrying a form ever meets it.
  //   api:register        the real "how many ACCOUNTS may come from here" cap, charged only
  //                       immediately before the INSERT — so only accounts that are actually
  //                       created count against it, never failures.
  // Both register rules — and every other IP-keyed rule here — are charged on rlIpKey(ip),
  // not the raw address. See rlIpKey: keyed on the full address, an IPv6 client had no cap
  // at all, because the last hextet is free to change.
  //
  // The account cap was 10/hour. One IP is one NAT, and a lab section of twenty signing up
  // during the same class hour is an ordinary Tuesday for this library — they used to hit
  // the cap halfway through and the rest were told to come back in an hour. There is no
  // key available at that point in the handler that tells a class apart from a script:
  // both arrive from one address with distinct fresh mailboxes, both have passed Turnstile.
  // So the cap is raised above a plausible cohort rather than split on a signal that does
  // not exist. NOTE: api:register:burst allows only 30 requests/hour from the same address,
  // so for a cohort larger than that the burst guard — not this cap — is what bites first,
  // and raising this rule past 30 would just make it dead code.
  'api:register:burst': { max: 30, window: 3600 },
  'api:register': { max: 25, window: 3600 },    // 25 CREATED accounts per hour per IP (per /64 on IPv6)
  // Raised from 3 and now charged only when a mail actually goes out (handleResetRequest peeks
  // first, exactly as handleLogin does). At 3-charged-on-arrival, three colleagues behind one
  // university egress exhausted the hour and the fourth was refused — the same lockout that had
  // 'api:register' raised from 3 to 25 on 2026-07-31, which this rule was left behind by. A
  // mistyped address or a double-click also each spent one.
  'api:reset': { max: 10, window: 3600 },       // 10 password-reset EMAILS per hour per IP
  // The per-IP budget bounds what one network can do and does nothing to stop one mailbox being
  // flooded from many addresses, which is the abuse that reaches a person. Keyed on user id, the
  // same unit 'api:resend' and 'api:2fa' use and for the same reason.
  'api:reset_acct': { max: 3, window: 3600 },   // 3 password-reset emails per hour per ACCOUNT
  // Resend-verification, keyed per ACCOUNT (the request is authenticated, so the IP is the
  // wrong unit — a shared campus address would otherwise exhaust everyone's allowance).
  'api:resend': { max: 3, window: 3600 },       // 3 verification emails per hour per account
  'api:download': { max: 100, window: 60 },     // 100 downloads per minute per user
  'api:general': { max: 300, window: 60 },      // 300 general API requests per minute
  'api:2fa': { max: 5, window: 600 },           // 5 TOTP guesses per ACCOUNT per 10 min.
  // Keyed on user_id, never on the pending token: the token is minted by whoever already
  // has the password, so a token-keyed counter caps a batch rather than the attack.
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const ip = request.headers.get('cf-connecting-ip') || 'unknown';
    const ua = request.headers.get('user-agent') || 'unknown';
    const country = request.headers.get('cf-ipcountry') || 'unknown';

    // §6 IdP host: dispatch accounts.elkassabgidata.com to its own fail-closed
    // router BEFORE the api.* gate/routing, so it can never reach the data/admin
    // table. The api.* path table below stays byte-for-byte unchanged.
    if (url.hostname === ACCOUNTS_HOST) {
      return await handleAccountsHost(request, env, url, path, ip, ua, country);
    }

    // §6 fail-closed hostname gate — before any routing. Only HOSTNAME_ALLOW
    // hosts serve; the *.workers.dev bypass and every other host 404.
    if (!HOSTNAME_ALLOW.has(url.hostname)) {
      return new Response('Not found', { status: 404 });
    }

    // robots.txt: the API host has no indexable content — its /v1/* endpoints
    // intentionally 401 without auth. Disallow all crawling so Search Console stops
    // reporting those expected 401s as an indexing problem (WNC-20237597). No CORS
    // needed (crawler top-level fetch). Cacheable.
    if (path === '/robots.txt') {
      return new Response('User-agent: *\nDisallow: /\n', {
        headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'public, max-age=86400' },
      });
    }

    // §8/§9 ENFORCEMENT: the registry now drives CORS. Allowed origins are the
    // hf-owned set (credentialed — they use the hfd_session cookie) plus every
    // active sso_clients row (family sites, allowed but NEVER credentialed — the
    // family flow is Authorization: Bearer). Unregistered origins get a safe
    // canonical fallback and no credentials. The soak proved this only EXPANDS
    // access (corsDecision allows a superset of ALLOWED_ORIGINS), so no origin
    // that works today loses CORS. corsDecision fails closed on a D1 error.
    const origin = request.headers.get('Origin') || '';
    const decision = await corsDecision(origin, env);
    const allowedOrigin = decision.allow ? origin : 'https://hfdatalibrary.com';
    const cors = {
      'Access-Control-Allow-Origin': allowedOrigin,
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
      'Access-Control-Max-Age': '86400',
      'Vary': 'Origin',
    };
    // §8: credentials ONLY for hf-owned origins. Never send
    // Access-Control-Allow-Credentials to a cross-registrable-domain family origin.
    if (decision.credentials) {
      cors['Access-Control-Allow-Credentials'] = 'true';
    }

    // §8 anti-CSRF: reject genuinely cross-site browser requests to
    // mutation/admin routes (they execute server-side even when CORS blocks the
    // response read). Same-origin, same-site, and non-browser clients pass.
    if (isMutationGuarded(path) && isCrossSiteRequest(request)) {
      return jsonRes({ error: 'Cross-site request blocked' }, 403, cors);
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    try {
      // ── Public endpoints ──
      if (path === '/' || path === '')
        return jsonRes({ name: 'HF Data Library API', version: '2.0', author: 'Ahmed Elkassabgi', docs: 'https://hfdatalibrary.com/pages/api' }, 200, cors);

      if (path === '/v1/status')
        return await handleStatus(env, cors);

      if (path === '/v1/public-stats')
        return await handlePublicStats(env, cors);

      // ── Auth endpoints ──
      if (path === '/v1/auth/register' && request.method === 'POST')
        return await handleRegister(request, env, cors, ip, ua, country);

      if (path === '/v1/auth/login' && request.method === 'POST')
        return await handleLogin(request, env, cors, ip, ua, country);

      if (path === '/v1/auth/logout' && request.method === 'POST')
        return await handleLogout(request, env, cors);

      if (path === '/v1/auth/me')
        return await handleMe(request, env, cors);

      // Cross-site SSO: a family site (econ / elkassabgidata) redirects the user
      // here; we read the first-party hfd_session cookie and bounce back to the
      // (allow-listed) return URL with the user's key in the fragment, so they
      // arrive already signed in. Works in every browser (no third-party cookie).
      if (path === '/v1/auth/sso')
        return await handleSSO(request, env);

      if (path === '/v1/auth/regenerate-key' && request.method === 'POST')
        return await handleRegenerateKey(request, env, cors);

      if (path === '/v1/auth/export' && request.method === 'GET')
        return await handleDataExport(request, env, cors);

      if (path === '/v1/auth/download-history')
        return await handleMyDownloadHistory(request, env, cors);

      if (path === '/v1/auth/delete' && request.method === 'POST')
        return await handleDeleteAccount(request, env, cors);

      if (path === '/v1/auth/update-profile' && request.method === 'POST')
        return await handleUpdateProfile(request, env, cors);

      if (path === '/v1/auth/change-password' && request.method === 'POST')
        return await handleChangePassword(request, env, cors);

      if (path === '/v1/auth/2fa/setup' && request.method === 'POST')
        return await handle2faSetup(request, env, cors);

      if (path === '/v1/auth/2fa/enable' && request.method === 'POST')
        return await handle2faEnable(request, env, cors);

      if (path === '/v1/auth/2fa/disable' && request.method === 'POST')
        return await handle2faDisable(request, env, cors);

      if (path === '/v1/auth/2fa/verify-login' && request.method === 'POST')
        return await handle2faVerifyLogin(request, env, cors, ip, ua, country);

      // ── OAuth ──
      if (path === '/v1/auth/orcid/link-init' && request.method === 'POST')
        return await handleOrcidLinkInit(request, env, cors);
      // Both /start handlers are async now — they write a single-use oauth_state
      // row and set the nonce cookie that binds the flow to this browser.
      if (path === '/v1/auth/orcid/start')
        return await handleOrcidStart(request, env, cors);
      if (path === '/v1/auth/orcid/callback')
        return await handleOrcidCallback(request, env, ip, ua, country);
      if (path === '/v1/auth/google/start')
        return await handleGoogleStart(env, cors);
      if (path === '/v1/auth/google/callback')
        return await handleGoogleCallback(request, env, ip, ua, country);

      if (path === '/v1/auth/verify' && request.method === 'POST')
        return await handleVerifyEmail(request, env, cors);

      if (path === '/v1/auth/resend-verification' && request.method === 'POST')
        return await handleResendVerification(request, env, cors);

      if (path === '/v1/auth/reset-request' && request.method === 'POST')
        return await handleResetRequest(request, env, cors);

      if (path === '/v1/auth/reset' && request.method === 'POST')
        return await handleReset(request, env, cors);

      // ── Data endpoints (require auth) ──
      if (path === '/v1/symbols')
        return await handleSymbols(env, cors);

      const symbolMatch = path.match(/^\/v1\/symbols\/([A-Z0-9.-]+)$/i);
      if (symbolMatch)
        return await handleSymbolInfo(symbolMatch[1].toUpperCase(), env, cors);

      const barsMatch = path.match(/^\/v1\/bars\/([A-Z0-9.-]+)$/i);
      if (barsMatch)
        return await handleBars(barsMatch[1].toUpperCase(), request, env, cors, ip);

      // Pre-computed academic variables (25 measures) and data-quality metrics
      const varsMatch = path.match(/^\/v1\/variables\/([A-Z0-9.-]+)$/i);
      if (varsMatch)
        return await handleDerived(varsMatch[1].toUpperCase(), 'variables', request, env, cors, ip);

      const qualMatch = path.match(/^\/v1\/quality\/([A-Z0-9.-]+)$/i);
      if (qualMatch)
        return await handleDerived(qualMatch[1].toUpperCase(), 'quality', request, env, cors, ip);

      // Request a signed download URL (short-lived token)
      const dlRequestMatch = path.match(/^\/v1\/download-token\/([A-Z0-9.-]+)$/i);
      if (dlRequestMatch)
        return await handleDownloadToken(dlRequestMatch[1].toUpperCase(), request, env, cors);

      // Use a signed download URL
      const dlMatch = path.match(/^\/v1\/download\/([A-Z0-9.-]+)$/i);
      if (dlMatch)
        return await handleDownload(dlMatch[1].toUpperCase(), request, env, cors, ip);

      // ── Newsletter ──
      if (path === '/v1/newsletter/unsubscribe' && request.method === 'POST')
        return await handleUnsubscribe(request, env, cors);

      if (path === '/v1/newsletter/subscribe' && request.method === 'POST')
        return await handleToggleSubscribe(request, env, cors, true);

      if (path === '/v1/newsletter/unsubscribe-toggle' && request.method === 'POST')
        return await handleToggleSubscribe(request, env, cors, false);

      // ── Resend webhooks (bounce/complaint list hygiene) ──
      if (path === '/v1/webhooks/resend' && request.method === 'POST')
        return await handleResendWebhook(request, env);

      // ── Admin endpoints ──
      if (path.startsWith('/v1/admin/'))
        return await handleAdmin(path, request, env, cors, ip);

      return jsonRes({ error: 'Not found' }, 404, cors);
    } catch (err) {
      return jsonRes({ error: 'Internal server error', detail: err.message }, 500, cors);
    }
  },

  // Cron trigger (see wrangler.toml [triggers]). Fires at 02:00 UTC daily =
  // 21:00 CDT (DST) / 20:00 CST. The daily activity digest goes to admin.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(sendDailyDigest(env));
    // Sweep expired rate_limits rows. This was the only place they could ever be removed and
    // it did not exist: the table grew one permanent row per key forever, and unauthenticated
    // callers supply part of those keys. A full D1 refuses writes, which is a site outage
    // reached without a single exploit.
    ctx.waitUntil(pruneRateLimits(env));
    // Same reasoning as pruneRateLimits, applied to the seven credential tables that have the
    // identical unbounded-growth defect. See PRUNE_SWEEPS for why sso_refresh_tokens is swept
    // on absolute_expires_at and why login_history / admin_audit_log are left alone.
    ctx.waitUntil(pruneExpiredCredentials(env));
  }
};

// ══════════════════════════════════════
// ── Rate Limiting ──
// ══════════════════════════════════════

// Rate-limit key for a client address. Every IP-keyed limiter used to pass the address
// verbatim, which is a cap on IPv4 and nothing at all on IPv6: a residential or mobile
// provider hands one subscriber a whole /64, so a single machine can source addresses that
// differ only in the last hextet and each one opens its own empty counter. "5 login attempts
// per 5 minutes" became unlimited login attempts for the cost of incrementing a number.
// Folding IPv6 to its /64 puts the budget on the subscriber, which is the unit the rule
// meant all along. IPv4 is returned unchanged on purpose — there one address really is one
// host, and folding it to a /24 would hang a whole campus off one counter, which is the
// lockout shape this file has already had to fix twice.
function rlIpKey(ip) {
  const raw = String(ip == null ? '' : ip).trim().toLowerCase();
  if (!raw) return 'unknown';
  if (raw.indexOf(':') === -1) return raw.slice(0, 45);   // IPv4, or the literal 'unknown'
  // ::ffff:a.b.c.d — an IPv4 address wearing an IPv6 shape. Its first four groups are all
  // zero, so folding it would collapse every IPv4 client on earth into one shared budget.
  if (raw.indexOf('.') !== -1) return raw.slice(0, 45);
  const bare = raw.split('%')[0];                          // drop any zone id
  const sides = bare.split('::');
  if (sides.length > 2) return bare.slice(0, 45);          // malformed — key it whole rather than guess
  const head = sides[0] ? sides[0].split(':') : [];
  const tail = (sides.length === 2 && sides[1]) ? sides[1].split(':') : [];
  let groups = head;
  if (sides.length === 2) {
    const gap = 8 - head.length - tail.length;
    if (gap < 0) return bare.slice(0, 45);
    groups = head.concat(new Array(gap).fill('0'), tail);
  }
  if (groups.length !== 8) return bare.slice(0, 45);
  return groups.slice(0, 4).map(g => g.replace(/^0+/, '') || '0').join(':') + '::/64';
}

// A rate-limit key becomes a PRIMARY KEY row in D1 and, until the prune below existed, stayed
// there for good. Parts of those keys come from unauthenticated request bodies, so the key
// has to be bounded or a caller can choose how many bytes we store per request. Every real
// key is short — an address, a user id, 'tfa:u<id>' — so anything longer is replaced by its
// SHA-256: still one distinct counter per distinct input, but a fixed 64 hex characters.
const RL_KEY_MAX = 96;

// Nothing in the worker ever deleted from rate_limits. Every key the site had ever seen kept
// a row permanently, in the same D1 that holds users, sessions and download_log — and D1
// fails WRITES when it fills, so an unbounded limiter table takes out logins, registrations
// and download logging with it. A row is dead once its window has passed; twice the longest
// window is a safe horizon and it follows RATE_LIMITS instead of drifting away from it.
const RL_PRUNE_AGE = 2 * Math.max(...Object.values(RATE_LIMITS).map(r => r.window));

async function pruneRateLimits(env) {
  try {
    return await env.DB.prepare(
      "DELETE FROM rate_limits WHERE (julianday('now') - julianday(window_start)) * 86400 > ?"
    ).bind(RL_PRUNE_AGE).run();
  } catch {
    return null;   // housekeeping — the daily digest must still go out if this sweep fails
  }
}

// rate_limits was never the only unbounded table — it was just the one that got noticed.
// EVERY short-lived credential table here has an expires_at and NONE of them were ever swept,
// so each one grew for the life of the service. Measured before this sweep existed
// (2026-08-01), expired rows as a share of each table:
//
//   download_tokens     149,203 rows   148,870 expired   99.8%   <- 10-minute tokens, ~15k/day
//   sessions              1,702          1,148           67%
//   password_resets         242            228           94%
//   sso_codes               331            331          100%
//   sso_oauth_state         164            162           99%
//   totp_pending              7              7          100%
//
// D1 refuses WRITES when full, so this ends as an outage — logins, registrations and download
// logging all stop — reached with no exploit at all, just time. The backlog above was cleared
// by hand; this keeps it cleared.
//
// TWO THINGS THIS DELIBERATELY DOES NOT DO.
//
// 1. sso_refresh_tokens is pruned on absolute_expires_at, NEVER on expires_at. expires_at is
//    the short access window; the row must outlive it. handleTokenRefresh looks the row up by
//    token_hash and treats a PRESENT row with used=1 as token REUSE — the signal that a
//    refresh token was stolen — and revokes the whole chain. Delete that row early and a
//    replayed stolen token stops being detected theft and becomes an ordinary unknown-token
//    rejection, silently disabling the defence. 1,001 of 1,016 rows are inside their absolute
//    window, so pruning on the wrong column would have destroyed almost all of it.
//
// 2. login_history and admin_audit_log are not touched. They have no expires_at because they
//    are the record of what happened, not credentials — they are what an investigation reads
//    after an account is compromised, and a retention policy for them is Ahmed's call, not a
//    side effect of a cleanup patch.
//
// The 24-hour grace exists so a row is never deleted out from under an in-flight request and
// so a just-expired credential is still visible while debugging a login someone is reporting
// right now. LIMIT-bounded because download_tokens turns over ~15k/day and an unbounded DELETE
// on a table that once held 149k rows is a statement that can time out; 50k per run is over
// three days of turnover, so it keeps up while staying bounded.
const PRUNE_BATCH = 50000;
const PRUNE_SWEEPS = [
  // [table, primary key, expiry column]
  ['download_tokens',    'token',      'expires_at'],
  ['sessions',           'id',         'expires_at'],
  // password_resets keys on id, not token — token is a UNIQUE column but the PK is id, and
  // the batching subquery should select the actual key. Verified against pragma_table_info
  // for all eight tables rather than inferred from column order.
  ['password_resets',    'id',         'expires_at'],
  ['sso_codes',          'code_hash',  'expires_at'],
  ['sso_oauth_state',    'state',      'expires_at'],
  ['oauth_state',        'state',      'expires_at'],
  ['totp_pending',       'token',      'expires_at'],
  ['sso_refresh_tokens', 'token_hash', 'absolute_expires_at'],   // see note 1 above
];

async function pruneExpiredCredentials(env) {
  const out = {};
  for (const [table, pk, col] of PRUNE_SWEEPS) {
    // Per-table try/catch: one bad table must not abort the sweep of the other seven, for the
    // same reason pruneRateLimits swallows its own error — housekeeping never takes the cron
    // down with it.
    try {
      const r = await env.DB.prepare(
        `DELETE FROM ${table} WHERE ${pk} IN (SELECT ${pk} FROM ${table} ` +
        `WHERE datetime(${col}) < datetime('now','-1 day') LIMIT ${PRUNE_BATCH})`
      ).run();
      out[table] = (r && r.meta && r.meta.changes) || 0;
    } catch (e) {
      out[table] = 'error: ' + (e && e.message ? e.message : 'unknown');
    }
  }
  return out;
}

// opts.charge === false asks "is this key already over the limit?" WITHOUT spending an
// attempt. handleLogin uses it so that only a WRONG password costs anything: charging every
// request meant five successful sign-ins from one university NAT locked out the sixth
// colleague, which is an availability bug wearing a security badge. Brute force still pays,
// because brute force is failures.
//
// The charge is now a single statement, and that statement is the decision. It used to be
// three — SELECT the count, compare it in JS, UPDATE — with nothing holding between them, so
// N requests sent at the same moment all read the same value, all concluded they were under
// the cap, and all passed: 200 attempts against a limit of 5. On a key with no row yet it was
// worse, because every one of them took the "insert 1" branch and the whole burst left the
// counter at 1, so the attacker did not even have to wait out the window. The peek/charge
// split above widened the gap from one database round-trip to tens of milliseconds, since a
// 100,000-iteration password hash now runs inside it. Decide from what the write did, never
// from a value read by an earlier SELECT.
async function checkRateLimit(env, key, ruleName, opts) {
  const rule = RATE_LIMITS[ruleName];
  if (!rule) return { ok: true };
  const charge = !opts || opts.charge !== false;

  const rawKey = String(key == null ? '' : key);
  const fullKey = ruleName + ':' + (rawKey.length <= RL_KEY_MAX ? rawKey : 'h:' + await sha256Hex(rawKey));

  // Window age is computed by SQLite, not by `new Date(window_start)`. window_start is stored
  // as 'YYYY-MM-DD HH:MM:SS' with no zone marker, and V8 reads that shape as LOCAL time — the
  // old JS arithmetic was correct only because Workers happens to run in UTC.
  if (!charge) {
    const peek = await env.DB.prepare(
      "SELECT count, CAST((julianday('now') - julianday(window_start)) * 86400 AS INTEGER) AS age FROM rate_limits WHERE key = ?"
    ).bind(fullKey).first();
    if (!peek || peek.age >= rule.window) return { ok: true, remaining: rule.max };
    if (peek.count >= rule.max) return { ok: false, retryAfter: Math.max(rule.window - peek.age, 1) };
    return { ok: true, remaining: rule.max - peek.count };
  }

  // Insert-or-increment, but only while the row is inside its window and under the cap. An
  // expired window resets the count to 1 and restamps; a full one matches no WHERE and writes
  // nothing at all. meta.changes is therefore 1 exactly when this request was entitled to
  // spend an attempt — the same single-use claim test consumeOauthState already relies on.
  const charged = await env.DB.prepare(
    "INSERT INTO rate_limits (key, count, window_start) VALUES (?, 1, datetime('now')) " +
    "ON CONFLICT(key) DO UPDATE SET " +
      "count = CASE WHEN (julianday('now') - julianday(window_start)) * 86400 >= ? THEN 1 ELSE count + 1 END, " +
      "window_start = CASE WHEN (julianday('now') - julianday(window_start)) * 86400 >= ? THEN datetime('now') ELSE window_start END " +
    "WHERE (julianday('now') - julianday(window_start)) * 86400 >= ? OR count < ?"
  ).bind(fullKey, rule.window, rule.window, rule.window, rule.max).run();

  if (charged.meta && charged.meta.changes === 0) {
    // Blocked. Only this branch needs to know how much of the window is left, so the extra
    // read costs nothing on the common path.
    const left = await env.DB.prepare(
      "SELECT CAST(? - (julianday('now') - julianday(window_start)) * 86400 AS INTEGER) AS retry FROM rate_limits WHERE key = ?"
    ).bind(rule.window, fullKey).first();
    return { ok: false, retryAfter: (left && left.retry > 0) ? left.retry : rule.window };
  }
  // No meta at all means we got no measurement, which is not the same as "over the limit".
  // Reading it as a rejection would turn one D1 hiccup into a site-wide 429 on login and
  // registration — a far worse failure than one attempt going uncharged.

  // Sweep dead rows about once every 500 charged calls. The cron prunes daily, but a burst
  // between two cron runs is precisely the case that grows the table, and the burst is the
  // thing rate limiting exists to meet.
  if (Math.random() < 0.002) await pruneRateLimits(env);

  // No `remaining` on this path: an atomic charge cannot report the resulting count without
  // RETURNING, and no caller has ever read the field — every one of them tests .ok and
  // .retryAfter only.
  return { ok: true };
}

// Profile fields are displayed publicly (stats page world map, institutions list,
// admin emails). Restrict them to Latin script + digits + common punctuation so a
// user submitting "中国" doesn't render Chinese characters under the world map.
// Allows accented Latin (é, ü, ñ, etc.) for European institutions/names.
function isLatinish(s) {
  if (typeof s !== 'string' || s.length === 0) return false;
  return /^[\p{Script=Latin}\p{N}\s\-'.,&()/]+$/u.test(s)
      && /[\p{Script=Latin}]/u.test(s); // require at least one actual letter
}

// Gate for the EDIT forms (as opposed to registration): a submitted profile value only has
// to clear isLatinish when the user is actually changing it.
//
// Why the distinction is necessary. Non-Latin text gets into these columns without ever
// passing this filter — Google auto-create copies users.name straight out of Google's
// profile (see handleGoogleCallback), and every row that predates the filter was never
// checked. Both edit forms then prefill from the stored row and re-post all four fields on
// every save (account.html loadAccount/saveProfile; renderAccountPage's fname/finst/
// fcountry/frole). So checking each submitted field unconditionally rejects the whole save
// over a value the user never touched, and because institution/country/role are what set
// profile_complete, the user is left permanently unable to complete their profile — which
// is what handleDownloadToken and handleDownload check before serving any data. A Google
// user with a CJK, Cyrillic or Arabic display name lost downloads entirely for one day
// under that rule; they could download the day before.
//
// Why letting it through is safe: the string is already in the column. Re-sending it stores
// nothing new, so it cannot be an injection — only a value the user has typed can be, and
// that still has to pass. An empty value is allowed for the same reason: it clears the
// field and there is nothing in it to inject.
//
// What this deliberately does NOT do is scrub a bad value that is already stored. Getting
// junk out of a column is a data cleanup plus output escaping at the render sites; an
// input validator only decides what may go in from here on.
function latinOkOrUnchanged(submitted, stored) {
  const v = (submitted == null ? '' : String(submitted)).trim();
  if (v.length === 0) return true;
  if (v === (stored == null ? '' : String(stored)).trim()) return true;
  return isLatinish(v);
}

function rateLimitResponse(retryAfter, cors) {
  return new Response(
    JSON.stringify({ error: `Rate limit exceeded. Try again in ${retryAfter} seconds.` }),
    {
      status: 429,
      headers: {
        'Content-Type': 'application/json',
        'Retry-After': retryAfter.toString(),
        ...cors
      }
    }
  );
}

// ══════════════════════════════════════
// ── Audit Log ──
// ══════════════════════════════════════

async function auditLog(env, adminUser, action, targetUserId, targetEmail, details, ip) {
  const adminId = adminUser.user_id || adminUser.id;
  await env.DB.prepare(
    'INSERT INTO admin_audit_log (admin_user_id, admin_email, action, target_user_id, target_email, details, ip_address) VALUES (?, ?, ?, ?, ?, ?, ?)'
  ).bind(adminId, adminUser.email, action, targetUserId, targetEmail, details || '', ip).run();
}

// ══════════════════════════════════════
// ── Password Strength ──
// ══════════════════════════════════════

function checkPasswordStrength(password) {
  if (password.length < 10) return { ok: false, error: 'Password must be at least 10 characters.' };
  if (!/[a-z]/.test(password)) return { ok: false, error: 'Password must include lowercase letters.' };
  if (!/[A-Z]/.test(password)) return { ok: false, error: 'Password must include uppercase letters.' };
  if (!/[0-9]/.test(password)) return { ok: false, error: 'Password must include numbers.' };
  // Check against very common passwords
  const common = ['password', 'password1', '12345678', 'qwerty123', 'letmein', 'admin1234'];
  if (common.some(c => password.toLowerCase().includes(c))) return { ok: false, error: 'Password is too common.' };
  return { ok: true };
}

// ══════════════════════════════════════
// ── OAuth (ORCID + Google) ──
// ══════════════════════════════════════

const OAUTH_REDIRECT_ORCID = 'https://api.hfdatalibrary.com/v1/auth/orcid/callback';
const OAUTH_REDIRECT_GOOGLE = 'https://api.hfdatalibrary.com/v1/auth/google/callback';
// M2b-2b — the centralized family broker's own callbacks on accounts.*. These
// URIs must be registered as Authorized redirect URIs on the Google OAuth client
// and the ORCID app (Ahmed console step) or the providers reject with
// redirect_uri_mismatch. Distinct from the api.* URIs above (M3: api.* untouched).
const OAUTH_REDIRECT_GOOGLE_ACCOUNTS = 'https://accounts.elkassabgidata.com/v1/auth/google/callback';
const OAUTH_REDIRECT_ORCID_ACCOUNTS  = 'https://accounts.elkassabgidata.com/v1/auth/orcid/callback';

// ── api.* OAuth CSRF state (2026-07-31) ──
// Until now neither api.* flow carried a state. handleGoogleStart never sent one
// and handleGoogleCallback never read one; handleOrcidStart merely echoed back
// whatever the caller typed into ?state=, and handleOrcidCallback treated an
// unknown state as "not a link request" and fell through to a plain login. So
// both callbacks accepted any authorization code that arrived. That is login
// CSRF: the attacker runs the flow in their own browser, stops before the last
// hop, keeps the unused code, and sends the victim
// api.hfdatalibrary.com/v1/auth/google/callback?code=<attacker's code>. The
// worker exchanges it, resolves the ATTACKER's account, and plants that session
// in the victim's browser (download.html even overwrites a victim who was
// already signed in as themselves). Everything the victim does afterwards —
// downloads, profile edits, an ORCID link — lands in a row whose password the
// attacker knows.
//
// A state alone would not close it, because the ORCID link state was a bearer
// value: the attacker called link-init, got S bound to THEIR user_id, and sent
// the victim the clean first-party link /v1/auth/orcid/start?state=S. So the
// state is bound to the browser that started the flow. /start mints a 256-bit
// nonce, keeps it in a host-only cookie, and puts only sha256(nonce) in the URL
// and in the oauth_state row — a callback that lands in a browser which never
// started the flow has no cookie and fails closed, and a state lifted from a URL
// or a Referer header is not the cookie. Same shape as the accounts.* broker
// (startFamilyOAuth / consumeOauthState), which already did this correctly.
// One exception, and only until the pages catch up: see §DEPLOY-ORDER below.
//
// This deliberately does NOT reuse sso_oauth_state. consumeOauthState matches on
// provider alone, so an api.* row parked in that table could be consumed by
// handleAccountsGoogleCallback and brokered into a family code with an empty
// PKCE challenge. Separate flows, separate tables. The legacy oauth_state has no
// verifier column, so the api.* Google flow still has no PKCE — that is code
// interception, a different hole from the one closed here.
//
// §DEPLOY-ORDER — one flow cannot demand the cookie yet. CI deploys the Worker and
// the Cloudflare Pages site as two PARALLEL jobs, so for a window the LIVE OLD page
// talks to the NEW worker. Both /start handlers set their cookie on their own 302,
// which the browser stores and replays on the provider's top-level GET back — those
// are page-independent and stay strict. The ORCID LINK flow is not: its cookie ships
// on the JSON response to account.html's fetch, and a fetch without
// credentials: 'include' (which only the new account.html sends) makes the browser
// DISCARD the Set-Cookie. Demanding the cookie there turns every "Link ORCID" click
// on the deployed page into oauth_error=state_invalid with no workaround. So the
// cookie is required when present and optional when absent for link rows only —
// full strength the moment the pages ship, and never weaker than the state-only
// check that is live today.
const OAUTH_STATE_COOKIE = { google: '__Host-hfd_oauth_g', orcid: '__Host-hfd_oauth_o' };
// §PER-FLOW-COOKIE. The cookie NAME carries the flow. Until this change there was one
// fixed name per provider, so every /start wrote its fresh nonce straight over whatever
// the browser was already holding. Two tabs — or Back, click "Sign in with Google"
// again, then finish the older tab — and the older callback arrived carrying the NEWER
// tab's nonce, failed the compare in consumeApiOauthState and was bounced to
// ?oauth_error=state_invalid, which download.html shows as
// alert('Sign-in failed: state_invalid'). Both tabs used to work, because before the
// login-CSRF fix both /start handlers were pure redirects that carried no state at all.
// So this was a NEW user-visible failure on the entry point 364 of 572 users take, and
// it had nothing to do with the pages being stale — it happened with any pages.
//
// Deriving the name from the state gives every flow its own slot, so nothing a second
// flow starts can reach the first one's nonce. The state is sha256(nonce) and is already
// public — it travels in the authorize URL — so putting 8 of its characters in a cookie
// name discloses nothing. The secret is the VALUE, and the value is still a 256-bit
// nonce that never left this browser.
//
// THE SECURITY PROPERTY IS UNCHANGED. The callback reads the cookie named for ITS OWN
// state and still requires sha256(nonce) === state (constantTimeEqual, below in
// consumeApiOauthState); a mismatch is still rejected. All that moved is WHICH cookie a
// given callback is permitted to look at — from "the one shared slot, whoever wrote it
// last" to "the slot belonging to this state". That is strictly narrower, never looser:
// a nonce that does not hash to the state cannot be accepted through any name.
//
// Eight hex characters, not all 64: these names ride on every request to
// api.hfdatalibrary.com and several can be live at once, so the name stays short. Two
// concurrent flows colliding needs the same 8 hex out of 2^32, and even then it degrades
// to exactly the old behaviour — the later flow overwrites the earlier one's nonce —
// never to accepting a nonce that does not hash to the state.
//
// The state on the read side arrives from the URL, i.e. from an attacker if they like,
// so the name is built only from a canonical 64-char lowercase sha256 hex string. That
// keeps it a legal cookie name and keeps the RegExp below free of any metacharacter
// somebody could inject. A malformed state yields no name at all, which reads as "no
// cookie" and fails closed exactly as an absent cookie already did.
//
// DEPLOY ORDER: nothing to coordinate. The worker live right now sends no state and no
// cookie whatsoever, so a flow started before the push already dies at the callback on
// the state check itself — the naming scheme makes that neither better nor worse, and it
// is over within one authorize round-trip. Do NOT "help" that window by falling back to
// the old fixed name when the per-flow cookie is missing: consumeApiOauthState enforces
// the nonce whenever one is present, so a leftover under a shared name is precisely what
// used to kill the next "Link ORCID" with state_invalid. Per-flow names retire that
// failure, and a fallback would bring it back.
function oauthCookieName(provider, state) {
  const base = OAUTH_STATE_COOKIE[provider];
  if (!base) return null;
  const s = state == null ? '' : String(state);
  if (!/^[0-9a-f]{64}$/.test(s)) return null;
  return `${base}_${s.slice(0, 8)}`;
}
// __Host- prefix: a browser refuses to store such a cookie if it carries Domain, so no
// sibling *.hfdatalibrary.com host can toss one at api.*. The prefix also REQUIRES
// Secure and Path=/. The per-flow suffix is appended to the END of the name, so this is
// still a __Host- cookie and all three requirements still hold. SameSite=Lax so it keeps
// riding the provider's top-level GET back into the callback. Max-Age stays at the same
// 600 seconds as the oauth_state row: with one cookie per flow they accumulate instead
// of overwriting each other, and a short life is half of what bounds them — the other
// half is expiredOauthStateCookie on every exit that consumes a state.
function oauthStateCookie(provider, state, nonce) {
  const name = oauthCookieName(provider, state);
  if (!name) return null;
  return `${name}=${nonce}; Path=/; Max-Age=600; HttpOnly; Secure; SameSite=Lax`;
}
function readOauthNonce(request, provider, state) {
  const name = oauthCookieName(provider, state);
  if (!name) return null;
  const m = (request.headers.get('cookie') || '')
    .match(new RegExp('(?:^|;\\s*)' + name + '=([A-Za-z0-9_-]+)'));
  return m ? m[1] : null;
}
// The spent-nonce counterpart of oauthStateCookie, and now the main thing that stops a
// browser filling up with orphans: with a name per flow, an abandoned sign-in leaves its
// own cookie sitting there instead of being overwritten by the next click. Same name,
// same Path, same flags on purpose — a browser only replaces a cookie when name, Path
// and Domain all match, so an expiry differing in any of them would sit alongside the
// live one and delete nothing. What a user who clicks sign-in repeatedly can therefore
// hold is: one ~72-byte cookie per flow they started and did not finish, each gone
// within ten minutes, and the finished ones cleared on the spot by the exits below.
function expiredOauthStateCookie(provider, state) {
  const name = oauthCookieName(provider, state);
  if (!name) return null;
  return `${name}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax`;
}
// §NONCE-CLEANUP. Both callbacks ended in Response.redirect() on every exit but one, and
// Response.redirect() freezes its headers — so the state nonce cookie outlived the flow
// that minted it for the rest of its 10-minute Max-Age. That is not cosmetic, because
// consumeApiOauthState enforces the nonce WHENEVER one is present: the leftover from an
// abandoned "Sign in with ORCID" cannot hash to the state of a later "Link ORCID", so the
// link died at ?oauth_error=state_invalid, which account.html renders as a bare error box.
// Worse, retrying did not help — the deployed account.html fetches link-init without
// credentials, so the browser discards the fresh Set-Cookie and the stale one keeps
// winning until it times out. These build the 302 by hand so the expiry can ride along.
//
// Which flow actually gets hurt is worth being precise about — and §PER-FLOW-COOKIE has
// since changed the answer, so read this in that light. When one name was shared, a
// leftover was harmless to the next SIGN-IN (each /start overwrote it on a top-level 302
// before any callback compared it) and fatal to the next ORCID LINK, the one flow whose
// Set-Cookie the deployed account.html throws away. Names are now per flow, so nothing
// overwrites anything and nothing collides either: a stale cookie is simply not the name
// the next callback reads. Cleanup still matters, and matters more than it did, because
// it is now the only thing that removes a spent cookie before its Max-Age — a leftover no
// longer gets replaced, it just sits there.
//
// Use this ONLY on exits reached AFTER consumeApiOauthState returned a row. That is what
// makes an unconditional expiry safe here: consume succeeds only when the cookie's nonce
// hashed to this state or when there was no cookie at all, so what is being cleared is
// always this flow's own nonce (or nothing — Max-Age=0 on a cookie that does not exist is
// a no-op). It takes the state for the same reason: the name it clears is derived from
// it, so this can only ever reach the cookie belonging to the flow that is ending. A
// second tab's live nonce lives under a different name and is now unreachable from here
// by construction, not merely by which exit the request happened to take.
function redirectExpiringOauthState(location, provider, state) {
  const expired = expiredOauthStateCookie(provider, state);
  // No name means the state was not a canonical sha256 hex string, which cannot happen on
  // an exit that reached here through consume (it matched a row keyed by that state). Fall
  // back to a plain redirect rather than emitting a malformed Set-Cookie: the destination
  // is what the user needs, and an uncleared cookie only ever times out.
  if (!expired) return Response.redirect(location, 302);
  return new Response(null, {
    status: 302,
    headers: {
      'Location': location,
      'Set-Cookie': expired,
      'Cache-Control': 'no-store'
    }
  });
}
// The provider-error exit — the user pressed Deny, or came back with no code — runs BEFORE
// the state is consumed, so unlike the exits above it holds no proof that the cookie in
// this request belongs to the flow that is ending. It gets that proof the same way consume
// does, from the state the provider echoes back with the error (RFC 6749 §4.1.2.1), and
// expires the cookie only when the two agree.
//
// Attributed rather than "clear whatever is there", and it stays attributed under
// §PER-FLOW-COOKIE even though the name now does half the work. Deriving the name from
// the echoed state already makes it impossible to touch a second tab's nonce — that was
// the first reason for attributing, and it is now structural. The second reason still
// needs the compare: the callback URL is reachable with no code and no state at all, and
// with a state somebody simply copied out of a URL, so without checking sha256(nonce)
// against it anyone who can cause a top-level navigation could delete a stranger's
// in-progress nonce for a flow they happen to know the state of. When the state is
// missing or does not match, this behaves exactly as the old code did: redirect, touch no
// cookie.
async function redirectOauthProviderError(request, provider, stateParam, location) {
  const nonce = readOauthNonce(request, provider, stateParam);
  if (nonce && stateParam && constantTimeEqual(await sha256Hex(nonce), String(stateParam))) {
    return redirectExpiringOauthState(location, provider, stateParam);
  }
  return Response.redirect(location, 302);
}
// 'YYYY-MM-DD HH:MM:SS' UTC — the exact spelling SQLite's datetime() produces, so
// it can be compared against a stored expires_at as plain text.
function sqliteNowUtc() {
  return new Date().toISOString().slice(0, 19).replace('T', ' ');
}
// The ONE sweep for oauth_state, shared by everything that writes the table: the
// api.* CSRF states minted just below and the accounts.* orcid_prefill ledger at
// mintOrcidPrefill. Expired rows are garbage in both cases and nothing else ever
// removes them, so without a sweep the table grows until D1 fills — and D1 refuses
// WRITES when it fills, which takes login, registration and download logging down.
//
// Three properties, each of which the first version of this sweep lacked:
//   SAMPLED — it ran on EVERY /start. oauth_state has no index on expires_at
//     (schema_live_20260717.sql:61 — `state` is the only key), so that was a
//     full-table scan on the click 364 of 572 users begin with, and anyone looping
//     the endpoint made their own scan more expensive with every request. Running
//     it on ~1 write in 20 deletes exactly the same rows; the table still cannot
//     hold more than a TTL window of starts plus a short tail. (An index on
//     expires_at would turn the scan into a seek and is worth adding next time the
//     schema is touched, but it is not what made this dangerous.)
//   SARGABLE — datetime(expires_at) called a function on every row, so no index
//     could ever be used even once one exists. The stored value comes from
//     datetime(), which is zero-padded fixed-width UTC and therefore sorts lexically
//     in time order, so comparing the bare column against the same spelling of "now"
//     is the identical predicate for far less work. (Rows written before 2026-07-31
//     by the old link-init hold an ISO string with a 'T', which sorts after a
//     same-day space-separated value; those get swept a day late. They are dead
//     rows either way.)
//   NON-FATAL — housekeeping must never decide whether someone can sign in. A D1
//     error here is logged and swallowed; the worst case is that expired rows
//     survive to the next sweep.
async function sweepOauthState(env) {
  if (Math.random() >= 0.05) return;
  try {
    await env.DB.prepare('DELETE FROM oauth_state WHERE expires_at <= ?')
      .bind(sqliteNowUtc()).run();
  } catch (e) {
    console.log(JSON.stringify({ evt: 'oauth_state_sweep_failed', msg: e && e.message }));
  }
}
// Start one flow. userId is the account being linked (ORCID link-init) or null
// for a plain sign-in. §EXPIRY-COMPARE: written with datetime() arithmetic and
// read with datetime() on both sides, so a 10-minute state lasts 10 minutes —
// the old link-init wrote toISOString() against a bare compare, which kept the
// state usable until midnight UTC. (The sweep above compares the column as text
// instead; same instant, no per-row function call.)
async function mintApiOauthState(env, provider, userId) {
  const nonce = generateToken() + generateToken();   // 256-bit; never leaves the browser
  const state = await sha256Hex(nonce);
  try {
    await env.DB.prepare(
      "INSERT INTO oauth_state (state, user_id, provider, expires_at) VALUES (?, ?, ?, datetime('now','+10 minutes'))"
    ).bind(state, userId || null, provider).run();
  } catch (e) {
    // §START-DEGRADES. Before this round both /start handlers were pure redirects
    // that touched no database, so no D1 trouble of any kind could stop a user
    // signing in with Google. Adding a write here quietly made "Sign in with
    // Google" — the entry point for 364 of 572 users — fail with a 500 during a D1
    // incident it used to survive. It must not.
    //   A plain sign-in degrades: the cookie, not the row, is what proves this
    //   browser started the flow, so consumeApiOauthState accepts a state that
    //   hashes from the cookie nonce even when the row is missing.
    //   A LINK cannot degrade and is deliberately still fatal: user_id is the only
    //   record of WHICH account the iD attaches to, and guessing is how someone
    //   else's ORCID ends up on your row. handleOrcidLinkInit answers a fetch, so
    //   account.html renders that failure as an error box rather than a blank page.
    if (userId) throw e;
    console.log(JSON.stringify({ evt: 'oauth_state_insert_failed', provider, msg: e && e.message }));
  }
  await sweepOauthState(env);
  // §PER-FLOW-COOKIE: the state names the cookie as well as travelling in the URL, so
  // this flow's nonce goes into a slot of its own and a sign-in started in another tab
  // a second later no longer lands on top of it.
  return { state, cookie: oauthStateCookie(provider, state, nonce) };
}
// Finish one flow. Returns the row (user_id = the link target, null for a plain
// sign-in) or null, and null must always abandon the callback. Called BEFORE the
// token exchange so a forged callback never burns a real code or hits the
// provider. oauth_state has no `used` column, so single use is the DELETE: the
// request whose DELETE reports changes === 1 owns the flow and a concurrent
// replay of the same state gets null.
async function consumeApiOauthState(request, env, provider, stateParam) {
  if (!stateParam) return null;
  // §PER-FLOW-COOKIE: read back the cookie named for THIS state, not the one shared slot
  // whose contents the most recent /start decided. That is what lets a second concurrent
  // sign-in exist without invalidating this one. It does not soften the check below — the
  // nonce found under that name must still hash to this exact state.
  const nonce = readOauthNonce(request, provider, stateParam);
  // The row is read before the cookie decision because the row is what says WHICH
  // flow this is: user_id set = ORCID link-init, null = a plain sign-in from /start.
  // Reading first leaks nothing — the state is sha256 of a 256-bit nonce, so there
  // is nothing to guess — and nothing is burned until every check below passes.
  const row = await env.DB.prepare(
    'SELECT * FROM oauth_state WHERE state = ? AND provider = ? AND datetime(expires_at) > datetime("now")'
  ).bind(stateParam, provider).first();
  if (!row) {
    // §START-DEGRADES (see mintApiOauthState). No row, but this browser is holding a
    // nonce that hashes to exactly this state — a pair only /start running in THIS
    // browser can produce, since the nonce is 256 bits in an HttpOnly __Host- cookie
    // and only sha256(nonce) was ever published. The CSRF property the row supports
    // is therefore already satisfied without it, so a D1 write that failed at /start
    // costs the user nothing instead of costing them the sign-in.
    // Always a plain sign-in, never a link: the link target lives only in the row.
    // What is given up is single use — the same state could be presented twice
    // inside the cookie's 10-minute life — which is worth nothing to an attacker who
    // would need the victim's HttpOnly cookie to present it at all, and the provider
    // refuses a second exchange of the same authorization code regardless. Expiry
    // still holds too: the cookie's Max-Age is the same 10 minutes as the row's TTL.
    if (nonce && constantTimeEqual(await sha256Hex(nonce), String(stateParam))) {
      console.log(JSON.stringify({ evt: 'oauth_state_row_missing', provider }));
      return { user_id: null };
    }
    return null;
  }
  if (nonce) {
    // Present → it must match. A state lifted from a URL and replayed in a browser
    // that holds a different (or stale) nonce fails here, which is the whole point.
    if (!constantTimeEqual(await sha256Hex(nonce), String(stateParam))) return null;
  } else if (!row.user_id) {
    return null;                                      // this browser never started a flow
  } else {
    // Link flow, no cookie: the deployed account.html fetches link-init without
    // credentials so the browser threw the Set-Cookie away (§DEPLOY-ORDER above).
    // Rejecting here would break "Link ORCID" outright for every user until Pages
    // redeploys, so fall back to the state-only check that is live today. This line
    // should stop appearing in the logs once the new account.html is out; when it
    // does, delete this branch and the flow is browser-bound in both directions.
    console.log(JSON.stringify({ evt: 'oauth_state_unbound_link', provider }));
  }
  const burn = await env.DB.prepare('DELETE FROM oauth_state WHERE state = ? AND provider = ?')
    .bind(stateParam, provider).run();
  if (!burn.meta || burn.meta.changes !== 1) return null;
  return row;
}

// Fetch ORCID public profile data (employment, current affiliation, etc.)
async function fetchOrcidProfile(orcidId) {
  try {
    const r = await fetch(`https://pub.orcid.org/v3.0/${orcidId}/record`, {
      headers: { 'Accept': 'application/json' }
    });
    if (!r.ok) return null;
    const data = await r.json();

    // Extract person info
    const person = data.person || {};
    const nameData = person.name || {};
    const givenNames = nameData['given-names']?.value || '';
    const familyName = nameData['family-name']?.value || '';
    const fullName = `${givenNames} ${familyName}`.trim();

    // Extract biography
    const biography = person.biography?.content || '';

    // Extract emails (often private but try)
    const emails = (person.emails?.email || [])
      .filter(e => e.verified)
      .map(e => e.email);

    // Extract researcher URLs
    const urls = (person['researcher-urls']?.['researcher-url'] || [])
      .map(u => ({ name: u['url-name'], url: u.url?.value }));

    // Extract country
    const addresses = person.addresses?.address || [];
    const country = addresses.length > 0 ? addresses[0].country?.value : null;

    // Extract current employment
    const activitiesSummary = data['activities-summary'] || {};
    const employments = activitiesSummary.employments?.['affiliation-group'] || [];
    const currentEmployment = employments
      .map(g => (g.summaries || []).map(s => s['employment-summary']))
      .flat()
      .filter(e => e && !e['end-date']) // current = no end date
      .map(e => ({
        organization: e.organization?.name || null,
        role: e['role-title'] || null,
        department: e['department-name'] || null,
        country: e.organization?.address?.country || null,
        start_date: e['start-date'] ? `${e['start-date'].year?.value || ''}-${e['start-date'].month?.value || ''}` : null
      }));

    // Also include education
    const educations = activitiesSummary.educations?.['affiliation-group'] || [];
    const educationList = educations
      .map(g => (g.summaries || []).map(s => s['education-summary']))
      .flat()
      .map(e => ({
        organization: e.organization?.name || null,
        role: e['role-title'] || null,
        country: e.organization?.address?.country || null
      }));

    // Works count
    const worksCount = (activitiesSummary.works?.group || []).length;

    return {
      fullName,
      biography,
      emails,
      urls,
      country: country || (currentEmployment[0]?.country || educationList[0]?.country || null),
      currentEmployment,
      educationList,
      worksCount
    };
  } catch (e) {
    console.error('ORCID fetch error:', e);
    return null;
  }
}

// The ?state= pass-through is gone. It let anyone launder an attacker's link
// state through a clean first-party URL — /v1/auth/orcid/start?state=<S from the
// attacker's link-init> — and the victim's real ORCID iD was written onto the
// attacker's row. No caller ever supplied one: account.html gets its entire
// authorize URL from link-init, and download.html links here with no query at
// all. This mints its own browser-bound state instead.
async function handleOrcidStart(request, env, cors) {
  const { state, cookie } = await mintApiOauthState(env, 'orcid', null);
  const url = new URL('https://orcid.org/oauth/authorize');
  url.searchParams.set('client_id', env.ORCID_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', '/authenticate');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_ORCID);
  url.searchParams.set('state', state);
  // Built by hand rather than Response.redirect(): that helper returns immutable
  // headers, and the nonce cookie has to ship with the 302 or there is nothing
  // for the callback to check.
  return new Response(null, {
    status: 302,
    headers: { 'Location': url.toString(), 'Set-Cookie': cookie, 'Cache-Control': 'no-store' }
  });
}

async function handleOrcidLinkInit(request, env, cors) {
  // Requires session auth (user must be logged in)
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  const userId = user.user_id || user.id;
  const { state, cookie } = await mintApiOauthState(env, 'orcid', userId);

  const url = new URL('https://orcid.org/oauth/authorize');
  url.searchParams.set('client_id', env.ORCID_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', '/authenticate');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_ORCID);
  url.searchParams.set('state', state);

  // The cookie, not the URL, is what ties this link request to this browser. The
  // state in the URL is only sha256(nonce), so forwarding the authorize URL to
  // someone else no longer links their iD to this account. account.html must send
  // this fetch with credentials: 'include' — hfdatalibrary.com and api.* are the
  // same site but different origins, and without it the browser discards the
  // Set-Cookie and every link attempt then dies at the callback.
  const res = jsonRes({ url: url.toString() }, 200, cors);
  res.headers.append('Set-Cookie', cookie);
  return res;
}

// A second factor has to guard EVERY door, not just the password one.
//
// handleLogin refuses to issue a session to a totp_enabled account until a code is verified.
// Both OAuth callbacks did not consult totp_enabled at all — they went straight from resolving
// the user to createSession — so an account whose owner had deliberately turned on 2FA was
// enterable through "Sign in with Google" or "Sign in with ORCID" with no code. The factor was
// enforced on the one door that already asks for a password, and skipped on the two that do not.
//
// Latent today (0 of 599 accounts have 2FA enabled, measured 2026-08-01) and it stops being
// latent the moment anyone turns it on — which became possible for any verified user earlier
// today. An unenforced second factor is worse than none: it is a promise of protection the
// system does not keep, and the person most likely to enable it here is the owner of the admin
// account.
//
// Mirrors handleLogin exactly: mint a 10-minute totp_pending row and hand the browser the
// pending token. The user finishes at /v1/auth/2fa/verify-login — the same endpoint the password
// flow uses, and the thing that actually mints the session. Nothing here writes last_login_at or
// a success row in login_history, because a challenge is not a completed login and handleLogin
// does not record one either.
//
// The pending token rides in the FRAGMENT, like every other credential-shaped value in this
// file, so it stays out of the Pages access log, browser history and Referer.
async function oauthTotpChallenge(env, user, ip, ua, provider, stateParam) {
  const pendingToken = generateId();
  const pendingExpires = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  await env.DB.prepare('INSERT INTO totp_pending (token, user_id, expires_at, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)')
    .bind(pendingToken, user.id, pendingExpires, ip, ua).run();
  // §NONCE-CLEANUP: state consumed, cookie spent — as in every other terminal branch here.
  return redirectExpiringOauthState(
    `${SITE_URL}/pages/download#totp_required=1&pending_token=${pendingToken}`,
    provider, stateParam
  );
}

async function handleOrcidCallback(request, env, ip, ua, country) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const error = url.searchParams.get('error');
  // Read once and carry it. Under §PER-FLOW-COOKIE the state names this flow's cookie, so
  // every exit that expires that cookie needs it — pulling it out of the query string
  // again at each exit is how one of them eventually gets missed and starts clearing
  // nothing, silently.
  const stateParam = url.searchParams.get('state');

  if (error || !code) {
    // §NONCE-CLEANUP. This is the abandoned-flow exit — ORCID's Deny button, or a bounce
    // back with no code. The state is never consumed on this path, so the cookie is
    // expired only when the echoed state proves it is this flow's own.
    return await redirectOauthProviderError(
      request, 'orcid', stateParam,
      `${SITE_URL}/pages/download?oauth_error=${encodeURIComponent(error || 'missing_code')}`
    );
  }

  // State first, before the token exchange, and required for BOTH branches. The
  // old code read the state only after exchanging the code and treated a missing
  // or unknown one as "not a link request", falling through to a plain login —
  // which is precisely the CSRF sign-in an attacker gets by mailing a victim a
  // callback URL holding the attacker's code. Checking here also means a forged
  // callback never burns a live authorization code at ORCID.
  const stateRec = await consumeApiOauthState(request, env, 'orcid', stateParam);
  if (!stateRec) {
    // Still deliberately expires nothing (§NONCE-CLEANUP). The reason used to be that a
    // shared cookie name made this the one exit reachable while holding a DIFFERENT,
    // still-live flow's nonce, so clearing would take a second tab's sign-in down with it.
    // §PER-FLOW-COOKIE removed that hazard — the only cookie this request could clear is
    // the one named for its own state. Leaving it alone anyway: reaching here means the
    // state matched no live row and the nonce under its name did not hash to it, i.e. the
    // cookie is either absent or something this browser did not get from /start, and
    // deleting it buys the user nothing. It times out inside ten minutes either way.
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=state_invalid`, 302);
  }
  // Set by handleOrcidLinkInit only; a plain sign-in state carries user_id NULL.
  const linkingUserId = stateRec.user_id || null;

  // Exchange code for token
  const tokenRes = await fetch('https://orcid.org/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json'
    },
    body: new URLSearchParams({
      client_id: env.ORCID_CLIENT_ID,
      client_secret: env.ORCID_CLIENT_SECRET,
      grant_type: 'authorization_code',
      code,
      redirect_uri: OAUTH_REDIRECT_ORCID
    }).toString()
  });

  if (!tokenRes.ok) {
    // §NONCE-CLEANUP: the state was consumed above, so the cookie is spent no matter what
    // ORCID's token endpoint said. Left live it would fail the user's next link attempt.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=token_exchange_failed`, 'orcid', stateParam);
  }

  const tokenData = await tokenRes.json();
  const orcidId = tokenData.orcid;
  const userName = tokenData.name || 'ORCID User';

  // (linkingUserId came from the state consumed above, before the exchange.)

  // Check if anyone already has this ORCID iD linked
  let user = await env.DB.prepare('SELECT * FROM users WHERE orcid_id = ?').bind(orcidId).first();

  if (linkingUserId) {
    // Linking mode: tie this ORCID to the specified user
    if (user && user.id !== linkingUserId) {
      // §NONCE-CLEANUP: state consumed, cookie spent. Whoever hits this sees an error and
      // will very likely click "Link ORCID" again straight away — with the old cookie still
      // in place that retry failed as state_invalid, i.e. a second, unrelated error.
      return redirectExpiringOauthState(`${SITE_URL}/pages/account?oauth_error=orcid_already_linked_to_another_account`, 'orcid', stateParam);
    }
    // Fetch and store ORCID profile data
    const profile = await fetchOrcidProfile(orcidId);
    const profileJson = profile ? JSON.stringify(profile) : null;
    await env.DB.prepare('UPDATE users SET orcid_id = ?, orcid_profile_json = ? WHERE id = ?')
      .bind(orcidId, profileJson, linkingUserId).run();
    // §NONCE-CLEANUP: the link succeeded, so this nonce has done its job and must not be
    // left to mismatch the next flow this browser starts.
    return redirectExpiringOauthState(`${SITE_URL}/pages/account?orcid_linked=1`, 'orcid', stateParam);
  }

  if (!user) {
    // No session and no existing link → fetch profile and redirect to registration
    const profile = await fetchOrcidProfile(orcidId);
    // The ORCID iD must reach /register as PROOF, not as a value the browser can retype.
    // oauth_id below stays for display only; orcid_prefill is an HMAC-signed 10-minute
    // token minted HERE — the one place that has actually completed ORCID's OAuth.
    // Audience 'api': this token authorizes a link on api.hfdatalibrary.com and nowhere
    // else, and it only works in THIS browser (the nonce cookie set on the redirect).
    const orcidProof = await mintOrcidPrefill(env, orcidId, profile?.fullName || userName, 'api');
    // mintOrcidPrefill returns null only when this worker has no CONSENT_HMAC_SECRET
    // bound (or the iD is not ORCID-shaped) — a configuration fault, not anything the
    // person in front of the browser did or can fix. Round 3 turned that into
    // oauth_error=orcid_link_unavailable, which is a dead end: ORCID sign-up becomes
    // 100% unavailable, with no path to an account at all, for as long as the secret
    // is missing. Carry on to the registration form without a token instead — an
    // account with no ORCID link is a working account, and the user can link ORCID
    // later from /pages/account. The flag below lets the page say so; the log line
    // is how we find out the secret is gone, since nobody will report "my ORCID
    // wasn't linked" as a fault.
    if (!orcidProof) {
      console.log(JSON.stringify({ evt: 'orcid_prefill_mint_failed', orcid: orcidId }));
    }
    const params = new URLSearchParams({
      oauth_provider: 'orcid',
      oauth_id: orcidId,
      oauth_name: profile?.fullName || userName
    });
    if (orcidProof) params.set('orcid_prefill', orcidProof.token);
    else params.set('orcid_link_unavailable', '1');
    if (profile?.currentEmployment?.[0]?.organization) {
      params.set('oauth_institution', profile.currentEmployment[0].organization);
    }
    if (profile?.country) {
      params.set('oauth_country', profile.country);
    }
    if (profile?.currentEmployment?.[0]?.role) {
      params.set('oauth_role', profile.currentEmployment[0].role);
    }
    if (profile?.emails?.[0]) {
      params.set('oauth_email', profile.emails[0]);
    }
    // Hand-built, not Response.redirect(): that helper freezes its headers, and
    // without the nonce cookie riding along the prefill token is only half bound —
    // verify has nothing to match sha256(nonce) against. The prefill cookie goes out
    // only when there is a token to bind: on the degraded path above there is no
    // nonce, and shipping a cookie that names no token would just sit in the browser
    // and mismatch the next real flow.
    const headers = new Headers({
      'Location': `${SITE_URL}/pages/download?${params.toString()}#register`,
      'Cache-Control': 'no-store'
    });
    // The sign-in state was consumed at the top of this handler, so its cookie is
    // spent — expire it rather than leaving it to sit for the rest of its 10 minutes.
    // Under a shared cookie name this was the cleanup that mattered most: a user who
    // tried ORCID sign-in, found they had no linked account, signed in with a password
    // and then clicked "Link ORCID" arrived at the callback still holding this leftover,
    // which could not match the link's state, and the link died at state_invalid for
    // reasons entirely invisible to them. §PER-FLOW-COOKIE ends that class of failure —
    // the leftover no longer occupies the name the link reads — so what is left here is
    // simple hygiene: a spent credential should not outlive the flow that minted it.
    // (The literal became expiredOauthStateCookie so the attributes cannot drift apart
    // between the exits that send it: an expiry whose Path or name differed from the live
    // cookie's would delete nothing at all.)
    // Prefill cookie first, expiry second: two Set-Cookie headers on one response is
    // ordinary (Headers.append keeps them separate), but if anything ever folded them
    // into one line a browser would keep only the first — and the prefill nonce is the
    // one that matters, while a stale state cookie merely times out on its own.
    if (orcidProof) headers.append('Set-Cookie', orcidProof.cookie);
    // Guarded because the name is derived from the state now: no name, nothing to send.
    // Unreachable on this path — consume matched a row keyed by this very state — but an
    // append of null would put a literal "null" in the header rather than skip it.
    const spentState = expiredOauthStateCookie('orcid', stateParam);
    if (spentState) headers.append('Set-Cookie', spentState);
    return new Response(null, { status: 302, headers });
  }

  // Existing user with linked ORCID — log them in
  if (!user.is_active) {
    // §NONCE-CLEANUP: state consumed, cookie spent. If the account is later reactivated
    // the same browser may still be inside the ten minutes, and the link it then tries is
    // the flow that cannot replace this cookie for itself.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=account_deactivated`, 'orcid', stateParam);
  }

  // A totp_enabled account must clear its second factor here too, not only on the
  // password path. Placed BEFORE the last_login/login_history writes on purpose: a
  // challenge is not a completed sign-in, and handleLogin does not record one either.
  if (user.totp_enabled) return await oauthTotpChallenge(env, user, ip, ua, 'orcid', stateParam);

  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId } = await createSession(env, user.id, ip, ua);

  // Two Set-Cookie headers now, so this has to be a Headers object: an object literal can
  // only carry one value per name, and writing the second as a comma-joined string would
  // produce a single malformed header that browsers reject wholesale — including the
  // session cookie. Session first, spent nonce second, for the same reason the
  // registration branch above orders its pair that way: if anything ever folds them
  // together, the one that must survive is the one that keeps the user signed in.
  const headers = new Headers({
    // §SESSION-IN-FRAGMENT — the session id rides in the fragment. Fixed 2026-08-01.
    //
    // hfdatalibrary.com genuinely needs this value: the hfd_session cookie below is
    // host-only to api.hfdatalibrary.com, and download.html authenticates purely from
    // localStorage, so the redirect is the only channel that gets the id to the page.
    // It used to travel as `?session=`, which wrote a 30-day, full-scope bearer credential
    // verbatim into the Cloudflare Pages access log and into browser history before the
    // page could scrub the address bar, and sent it out in the Referer of anything that
    // page loaded. A fragment is never transmitted to any server, so none of that happens
    // — the same reason mintSsoCode has always used one for the family SSO code.
    //
    // This could not be flipped alone, and was not. The Worker and the Pages site deploy as
    // two parallel jobs; a worker sending a fragment to a page that reads only the query
    // string signs every Google and ORCID user OUT silently, and the reverse is equally
    // dark. So it shipped in the documented order: download.html first, taught to read the
    // fragment AND fall back to the query string (a no-op while the worker still sent a
    // query string), confirmed live on hfdatalibrary.com, and only then this line.
    //
    // The page keeps its query-string fallback deliberately. It costs nothing, and it is
    // what makes a rollback of this worker safe.
    'Location': `${SITE_URL}/pages/download#oauth_success=1&session=${sessionId}`,
    'Referrer-Policy': 'no-referrer',
    'Cache-Control': 'no-store'
  });
  headers.append('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  // §NONCE-CLEANUP: the ordinary success path left the spent nonce behind too — the most
  // travelled exit in the handler. With a shared name it was also the biggest single
  // source of the leftover that broke the next "Link ORCID"; §PER-FLOW-COOKIE means the
  // link no longer reads this name, so this is now about not leaving a used credential in
  // the browser and not letting per-flow cookies pile up. Guarded for the same reason as
  // the registration branch: no state, no name, nothing to append.
  const spentState = expiredOauthStateCookie('orcid', stateParam);
  if (spentState) headers.append('Set-Cookie', spentState);
  return new Response(null, { status: 302, headers });
}

async function handleGoogleStart(env, cors) {
  const { state, cookie } = await mintApiOauthState(env, 'google', null);
  const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  url.searchParams.set('client_id', env.GOOGLE_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', 'openid email profile');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_GOOGLE);
  url.searchParams.set('access_type', 'online');
  url.searchParams.set('state', state);
  // Hand-built for the same reason as handleOrcidStart: Response.redirect()
  // freezes its headers, and without the nonce cookie the callback has nothing to
  // match the state against.
  return new Response(null, {
    status: 302,
    headers: { 'Location': url.toString(), 'Set-Cookie': cookie, 'Cache-Control': 'no-store' }
  });
}

async function handleGoogleCallback(request, env, ip, ua, country) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const error = url.searchParams.get('error');
  // Read once and carry it, as in handleOrcidCallback: §PER-FLOW-COOKIE derives this
  // flow's cookie name from the state, so every exit that clears the cookie needs it.
  const stateParam = url.searchParams.get('state');

  if (error || !code) {
    // §NONCE-CLEANUP, the same defect as handleOrcidCallback's error exit, fixed the same
    // way. A cancelled consent screen is the commonest way to abandon the flow 364 of 572
    // users take, so this is where the ten-minute orphans come from — and since names are
    // now per flow, an orphan is never overwritten by the next click, only cleared here or
    // by its own Max-Age. Expired only when the state Google echoes back with the error
    // proves the cookie belongs to this flow.
    return await redirectOauthProviderError(
      request, 'google', stateParam,
      `${SITE_URL}/pages/download?oauth_error=${encodeURIComponent(error || 'missing_code')}`
    );
  }

  // No state was ever sent or checked here, so this handler would exchange any
  // code anyone put in the URL and hand back a session cookie for whatever
  // account that code resolved to. Consume the state — and with it the proof that
  // this browser is the one that started the flow — before touching Google.
  const st = await consumeApiOauthState(request, env, 'google', stateParam);
  if (!st) {
    // Clears nothing, deliberately — see the matching exit in handleOrcidCallback. It used
    // to be the one exit reachable while holding a second tab's live nonce; §PER-FLOW-COOKIE
    // means a second tab's nonce is under a different name and cannot be touched from here
    // at all. Left as-is because clearing the state's own cookie on a failed compare gains
    // the user nothing and this exit is the one that must never make things worse.
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=state_invalid`, 302);
  }

  // Exchange code for token
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      code,
      grant_type: 'authorization_code',
      redirect_uri: OAUTH_REDIRECT_GOOGLE
    }).toString()
  });

  if (!tokenRes.ok) {
    // §NONCE-CLEANUP: the state was consumed above, so the cookie is spent regardless of
    // what Google's token endpoint returned, and nothing should still be holding it.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=token_exchange_failed`, 'google', stateParam);
  }

  const tokenData = await tokenRes.json();

  // Fetch user info
  const userRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
    headers: { 'Authorization': `Bearer ${tokenData.access_token}` }
  });
  if (!userRes.ok) {
    // §NONCE-CLEANUP: state consumed, cookie spent.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=userinfo_failed`, 'google', stateParam);
  }
  const profile = await userRes.json();
  const email = (profile.email || '').toLowerCase();
  const name = profile.name || email.split('@')[0];

  if (!email) {
    // §NONCE-CLEANUP: state consumed, cookie spent.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=no_email`, 'google', stateParam);
  }

  // Look up existing user by email
  let user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email).first();

  if (!user) {
    // "Google emails are already verified" was an assumption, not a check.
    //
    // email_verified was the literal 1 in the INSERT below while profile.verified_email — the
    // field that actually answers the question — was never read on this branch. It IS read 50
    // lines down on the link branch (§GOOGLE-VERIFIED), so the same handler trusted Google's
    // answer when attaching to an existing row and ignored it when creating one. Google does
    // return verified_email:false for some accounts (notably Workspace identities whose
    // primary address was never confirmed), and email_verified is a privilege flag here: the
    // download gate reads it, and so does the admin console gate. Minting it from an
    // assumption meant an unconfirmed mailbox could arrive pre-verified.
    //
    // Now bound from the profile. An account Google will not vouch for is created UNVERIFIED
    // and takes the ordinary email-verification path, which is exactly what a
    // password-registered account does.
    const googleSaysVerified = (profile.verified_email === true || profile.verified_email === 'true') ? 1 : 0;
    const apiKey = 'hfd_' + generateId();
    const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    const unsubscribeToken = generateId();
    const isAdmin = ADMIN_EMAILS.includes(email) ? 1 : 0;
    // Use a random strong password placeholder (user can reset later)
    const randomPassword = generateId() + generateId();
    const passwordHash = await hashPassword(randomPassword);

    await env.DB.prepare(
      'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, google_id, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0)'
    ).bind(
      name, email, passwordHash,
      '', country || '', '',
      apiKey, apiKeyExpires, isAdmin, googleSaysVerified,
      unsubscribeToken, ip, ua, profile.id
    ).run();

    user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email).first();

    // Admin notification
    try {
      await sendEmail(
        env, ADMIN_NOTIFY,
        `New registration via Google: ${name}`,
        adminNotificationEmail({ name, email, institution: '(via Google)', country, role: 'Not specified' }, ip, ua, country)
      );
    } catch (e) {}
  } else {
    // AN ACCOUNT WITH THIS EMAIL ALREADY EXISTS. Until 2026-07-31 we simply logged into it,
    // which is the account-takeover pattern known as pre-hijacking:
    //
    //   1. Anyone registers with a stranger's address. handleRegister creates the row
    //      immediately with email_verified = 0 — owning the mailbox is never required.
    //   2. The real owner later clicks "Sign in with Google".
    //   3. The lookup above finds that row and signs them straight into it.
    //   4. Whoever planted it still knows the password, so they keep access to the victim's
    //      account, its API key and its download history.
    //
    // Academic email addresses are printed on faculty pages, so step 1 is easy here. Found
    // when a real user hit the registration lockout, switched to Google, and landed in the
    // account he had half-created.
    //
    // Google's userinfo carries verified_email, which is genuine proof of mailbox control —
    // strictly better proof than an unverified local password. So when the local account has
    // NOT proved ownership and Google HAS, hand the account to the Google owner and
    // invalidate everything the planter could still be holding.
    //
    // THE API KEY MUST ROTATE TOO. handleRegister issues a live key at sign-up, before any
    // verification. It is inert only because every download route refuses an unverified
    // account — so setting email_verified = 1 below is precisely what would switch the
    // planter's key ON. Rotating the password while leaving the key is not remediation, it
    // is a downgrade from "harmless" to "armed".
    const googleVerified = profile.verified_email === true || profile.verified_email === 'true';

    if (!user.email_verified) {
      if (!googleVerified) {
        // Neither side has proved it owns this mailbox — refuse rather than guess.
        // §NONCE-CLEANUP: state consumed, cookie spent.
        return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=email_unverified`, 'google', stateParam);
      }
      const rotatedHash = await hashPassword(generateId() + generateId());
      await env.DB.prepare(
        'UPDATE users SET password_hash = ?, ' +
        'email_verified = 1, google_id = COALESCE(google_id, ?) WHERE id = ?'
      ).bind(rotatedHash, profile.id, user.id).run();
      // Hand over EVERYTHING, not just the password. The first version of this fix rotated
      // the password and the API key and deleted `sessions`, which left four other ways
      // back in: the family SSO refresh chain (30 more days of access tokens for every
      // site), any pending-2FA row (a 10-minute re-entry window), an ORCID iD the planter
      // had linked (which is itself a login credential), and a TOTP secret they enrolled
      // (which locks the rightful owner out of password login permanently).
      // The api_key must rotate here for a subtle reason: handleRegister issues a live key
      // at sign-up, inert only because download routes refuse unverified accounts — so the
      // `email_verified = 1` on the line above is exactly what would arm the planter's key.
      //
      // WHY THIS IS ALSO ANNOUNCED BY EMAIL. email_verified = 0 does not only mean "planted".
      // Accounts created THROUGH ORCID are born unverified by design, so for a user who is in
      // both groups — one of the 16 rows with an orcid_id and one of the 35 that never had
      // their verification mail clicked — this branch replaces their password, their API key
      // and their own ORCID link. Their next "Sign in with ORCID" then matches nothing and
      // sends them to a registration form that rejects their address as already taken.
      //
      // The database cannot tell that user apart from a squatter who linked their own ORCID
      // to a stranger's address: both are a proven ORCID iD sitting on an unverified row. So
      // the clearing stays — leaving it would hand a squatter a second, still-working way in,
      // which is the entire point of the handover. What does NOT have to stay is doing it
      // silently. Telling the owner turns an invisible breakage into a recoverable one, and
      // announcing a credential change is right even when nothing suspicious happened.
      const hadOrcid = !!user.orcid_id;
      const hadTotp = !!user.totp_enabled;
      await revokeAllUserCredentials(env, user.id, {
        rotateApiKey: true,
        clearForeignIdentities: true,
        clearTotp: true,
      });
      // Non-blocking: a mail failure must never break a sign-in that has already succeeded.
      try {
        const changed = ['your password', 'your API key']
          .concat(hadOrcid ? ['your linked ORCID iD'] : [])
          .concat(hadTotp ? ['your two-factor authentication'] : [])
          .join(', ');
        await sendEmail(
          env, email,
          'Your HF Data Library account was secured',
          '<p>Hello' + (user.name ? ' ' + escapeHtml(user.name) : '') + ',</p>' +
          '<p>You just signed in to HF Data Library with Google. This address already had an ' +
          'account that had never been confirmed by email, so we transferred it to you and ' +
          'reset ' + escapeHtml(changed) + '.</p>' +
          '<p>You can keep signing in with Google. To use a password instead, choose ' +
          '&ldquo;Forgot password&rdquo; on the sign-in page.' +
          (hadOrcid ? ' If you used ORCID with this account, please link it again from your ' +
            'account page.' : '') + '</p>' +
          '<p>If this was not you, reply to this message immediately.</p>',
          FROM_EMAIL, 'HF Data Library'
        );
      } catch (e) { /* non-fatal */ }
      user = await env.DB.prepare('SELECT * FROM users WHERE id = ?').bind(user.id).first();
    } else if (!user.google_id) {
      // Verified account signing in with Google for the first time: record the link so the
      // pairing is visible, but change nothing else.
      await env.DB.prepare('UPDATE users SET google_id = ? WHERE id = ?')
        .bind(profile.id, user.id).run();
    } else if (String(user.google_id) !== String(profile.id)) {
      // A DIFFERENT Google identity presenting a matching address. Refuse.
      //
      // This chain had no terminal else, so the case fell through to createSession below and
      // was handed a full 30-day session on an account bound to somebody else's Google `sub`.
      // Matching on the email address alone is matching on the one field Google does not
      // promise is stable: an address is reassignable, a `sub` is not. It happens without any
      // attacker at all — a Workspace domain lapses and is re-registered, or a deleted account
      // is recreated — and the new owner of the address then inherits the previous owner's
      // library account, its API key and its download history.
      //
      // The accounts.elkassabgidata.com twin already fails closed on precisely this, returning
      // account_link_conflict rather than auto-merging (see the "Unverified same-email row ...
      // OR one bound to a DIFFERENT google_id" branch). Two handlers, same question, opposite
      // answers — and the weaker one was reachable from the public download page. This makes
      // them agree.
      //
      // Deliberately NOT auto-relinking: overwriting google_id here would let anyone who can
      // obtain a Google account at a matching address silently take over a verified row, which
      // is the takeover this is meant to stop. The legitimate cases — a genuinely new owner of
      // a reassigned address — need a human decision, so they get a clear error and can
      // register or contact us.
      // §NONCE-CLEANUP: state consumed, cookie spent.
      return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=account_link_conflict`, 'google', stateParam);
    }
  }

  if (!user.is_active) {
    // §NONCE-CLEANUP: state consumed, cookie spent.
    return redirectExpiringOauthState(`${SITE_URL}/pages/download?oauth_error=account_deactivated`, 'google', stateParam);
  }

  // A totp_enabled account must clear its second factor here too, not only on the
  // password path. Placed BEFORE the last_login/login_history writes on purpose: a
  // challenge is not a completed sign-in, and handleLogin does not record one either.
  if (user.totp_enabled) return await oauthTotpChallenge(env, user, ip, ua, 'google', stateParam);

  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId } = await createSession(env, user.id, ip, ua);

  // A Headers object because there are two Set-Cookie values now and an object literal
  // holds only one per name; session first, spent nonce second, so the cookie that keeps
  // the user signed in is the one that survives if they are ever folded together.
  const headers = new Headers({
    // §SESSION-IN-FRAGMENT — see handleOrcidCallback for the full reasoning. Short form:
    // `?session=` leaked a 30-day full-scope credential into the Pages access log, browser
    // history and any outgoing Referer; a fragment is never sent to a server. Moved
    // 2026-08-01, after download.html had shipped a fragment reader with a query-string
    // fallback and that page was confirmed live — not in the same push.
    'Location': `${SITE_URL}/pages/download#oauth_success=1&session=${sessionId}`,
    'Referrer-Policy': 'no-referrer',
    'Cache-Control': 'no-store'
  });
  headers.append('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  // §NONCE-CLEANUP: success left the spent nonce behind as well, and this is the exit
  // almost every one of the 364 Google users reaches — so it is the single largest source
  // of ten-minute orphans, and since §PER-FLOW-COOKIE gave every flow its own name, the
  // next /start no longer overwrites them. Clearing here is what keeps a browser from
  // carrying one dead cookie per successful sign-in. Guarded: no state, no name.
  const spentState = expiredOauthStateCookie('google', stateParam);
  if (spentState) headers.append('Set-Cookie', spentState);
  return new Response(null, { status: 302, headers });
}

// ══════════════════════════════════════
// ── TOTP 2FA (Google Authenticator) ──
// ══════════════════════════════════════

const BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

function base32Encode(bytes) {
  let bits = 0, value = 0, output = '';
  for (let i = 0; i < bytes.length; i++) {
    value = (value << 8) | bytes[i];
    bits += 8;
    while (bits >= 5) {
      output += BASE32_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) output += BASE32_ALPHABET[(value << (5 - bits)) & 31];
  return output;
}

function base32Decode(str) {
  const clean = str.replace(/=+$/, '').toUpperCase();
  const bytes = [];
  let bits = 0, value = 0;
  for (let i = 0; i < clean.length; i++) {
    const idx = BASE32_ALPHABET.indexOf(clean[i]);
    if (idx === -1) continue;
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return new Uint8Array(bytes);
}

function generateTotpSecret() {
  const bytes = crypto.getRandomValues(new Uint8Array(20));
  return base32Encode(bytes);
}

async function generateTotp(secret, timestamp) {
  const time = Math.floor(timestamp / 30000);
  const timeBuffer = new ArrayBuffer(8);
  const timeView = new DataView(timeBuffer);
  timeView.setUint32(0, Math.floor(time / 0x100000000));
  timeView.setUint32(4, time & 0xffffffff);

  const keyBytes = base32Decode(secret);
  const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
  const hmac = await crypto.subtle.sign('HMAC', key, timeBuffer);
  const hmacBytes = new Uint8Array(hmac);

  const offset = hmacBytes[19] & 0xf;
  const code = ((hmacBytes[offset] & 0x7f) << 24) |
               ((hmacBytes[offset + 1] & 0xff) << 16) |
               ((hmacBytes[offset + 2] & 0xff) << 8) |
               (hmacBytes[offset + 3] & 0xff);
  return String(code % 1000000).padStart(6, '0');
}

async function verifyTotp(secret, userCode) {
  if (!userCode || !/^\d{6}$/.test(userCode)) return false;
  const now = Date.now();
  // Allow ±1 window (30s drift)
  for (let i = -1; i <= 1; i++) {
    const expected = await generateTotp(secret, now + i * 30000);
    if (expected === userCode) return true;
  }
  return false;
}

// ══════════════════════════════════════
// ── Turnstile CAPTCHA Verification ──
// ══════════════════════════════════════

async function verifyTurnstile(env, token, ip) {
  if (!token) return false;
  if (!env.TURNSTILE_SECRET) return true; // Skip if not configured
  try {
    const r = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `secret=${encodeURIComponent(env.TURNSTILE_SECRET)}&response=${encodeURIComponent(token)}&remoteip=${encodeURIComponent(ip)}`
    });
    const data = await r.json();
    return data.success === true;
  } catch (e) {
    return false;
  }
}

// ══════════════════════════════════════
// ── Email Sending (Resend) ──
// ══════════════════════════════════════

// Strip HTML tags in guaranteed-linear time. For each "<"-delimited segment, drop
// up to the first ">" (the tag) and keep the rest as text; an unterminated "<"
// keeps its text. No regex backtracking, so it stays O(n) even on ">"-free input
// (unlike /<[^>]+>/g, which is quadratic there). Behaviour matches the tag strip
// for all well-formed HTML we send.
function stripTagsLinear(s) {
  const parts = String(s).split('<');
  let out = parts[0];
  for (let i = 1; i < parts.length; i++) {
    const gt = parts[i].indexOf('>');
    out += gt === -1 ? parts[i] : parts[i].slice(gt + 1);
  }
  return out;
}

// Derive a readable plaintext alternative from an HTML body. Sending HTML-only
// mail (no text/plain part) is a well-known spam signal (e.g. SpamAssassin
// MIME_HTML_ONLY); a multipart/alternative message with a real plaintext part
// scores better and is a deliverability best practice. Pure string ops — never
// throws on the inputs we build, but callers guard anyway so email never breaks.
function htmlToText(html) {
  // Cap the input: no legitimate email body approaches this. The cap plus bounded
  // tag scans plus the linear strip below keep CPU provably small even on
  // malformed admin-pasted HTML — a Workers CPU-time kill would bypass the
  // callers' try/catch, so we must not rely on backtracking regexes over
  // attacker-shaped input. Clipping the PLAINTEXT of an oversized body is fine;
  // the HTML part is sent untouched.
  let s = String(html);
  if (s.length > 200000) s = s.slice(0, 200000);
  // Drop <style>/<head> blocks (bounded lazy so an unclosed one can't scan far).
  s = s.replace(/<style[\s\S]{0,50000}?<\/style>/gi, '').replace(/<head[\s\S]{0,50000}?<\/head>/gi, '');
  // Preserve link URLs: "<a href=URL>label</a>" -> "label (URL)". The [^>] runs
  // are BOUNDED so a run of unterminated "<a" can't drive quadratic CPU; real
  // tags close well under 2000 chars, so no functional change for real HTML.
  s = s.replace(/<a\b[^>]{0,2000}href="([^"]{0,2000})"[^>]{0,2000}>([\s\S]{0,20000}?)<\/a>/gi, (m, href, label) => {
    const t = stripTagsLinear(label).trim();
    return t && t !== href ? `${t} (${href})` : href;
  });
  // Block elements -> newlines.
  s = s.replace(/<br\s*\/?>/gi, '\n').replace(/<\/(p|div|h[1-6]|tr|li|table)>/gi, '\n');
  // Strip the remaining tags in guaranteed-linear time (split, not a backtracking
  // /<[^>]+>/ which is quadratic on ">"-free input).
  s = stripTagsLinear(s);
  return s
    .replace(/&mdash;/g, '—').replace(/&middot;/g, '·')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&#(\d+);/g, (m, n) => { try { return String.fromCodePoint(+n); } catch (e) { return ' '; } })
    .replace(/&[a-z]+;/gi, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

async function sendEmail(env, to, subject, htmlBody, fromEmail = FROM_EMAIL, fromName = FROM_NAME, textBody = null) {
  // Always include a plaintext part (explicit or derived) so the message is
  // multipart/alternative rather than HTML-only. Derivation is best-effort:
  // if it ever fails, fall back to HTML-only rather than break the send.
  let text = textBody;
  if (!text) { try { text = htmlToText(htmlBody); } catch (e) { text = null; } }
  const payload = { from: `${fromName} <${fromEmail}>`, to: [to], subject, html: htmlBody };
  if (text) payload.text = text;
  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    console.error('Resend error:', await response.text());
  }
  return response.ok;
}

async function sendEmailBatch(env, items) {
  // items: array of full email objects {from, to, subject, html} — Resend's
  // batch endpoint accepts up to 100 per call. One retry on 429/network error.
  // Add a derived plaintext part to any item that lacks one (multipart/alternative
  // beats HTML-only for deliverability — see htmlToText/sendEmail).
  const payloadItems = items.map((it) => {
    if (it && it.html && !it.text) {
      try { const t = htmlToText(it.html); if (t) return { ...it, text: t }; } catch (e) { /* html-only fallback */ }
    }
    return it;
  });
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const response = await fetch('https://api.resend.com/emails/batch', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${env.RESEND_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payloadItems)
      });
      if (response.ok) {
        const data = await response.json();
        const sent = Array.isArray(data?.data) ? data.data.length : items.length;
        return { success: sent, failed: items.length - sent };
      }
      if (response.status === 429 && attempt === 0) {
        await new Promise(res => setTimeout(res, 1100));
        continue;
      }
      console.error('Resend batch error:', response.status, await response.text());
      return { success: 0, failed: items.length };
    } catch (e) {
      if (attempt === 0) {
        await new Promise(res => setTimeout(res, 1100));
        continue;
      }
      console.error('Resend batch exception:', e);
      return { success: 0, failed: items.length };
    }
  }
  return { success: 0, failed: items.length };
}

// ══════════════════════════════════════
// ── Resend Webhook (list hygiene) ──
// ══════════════════════════════════════
// Auto-unsubscribes addresses that hard-bounce or file spam complaints, and
// emails the admin about each removal. Setup:
//   1. Resend dashboard → Webhooks → Add endpoint:
//        https://api.hfdatalibrary.com/v1/webhooks/resend
//      Events: email.bounced, email.complained, email.suppressed
//   2. Copy the signing secret (whsec_...) into the Worker secret
//      RESEND_WEBHOOK_SECRET (Cloudflare dashboard → Worker → Settings).

async function verifySvixSignature(secret, svixId, svixTimestamp, svixSignature, rawBody) {
  if (!secret || !svixId || !svixTimestamp || !svixSignature) return false;

  // Replay protection: reject timestamps more than 5 minutes off
  const ts = parseInt(svixTimestamp, 10);
  if (!ts || Math.abs(Date.now() / 1000 - ts) > 300) return false;

  const secretBytes = Uint8Array.from(atob(secret.replace(/^whsec_/, '')), c => c.charCodeAt(0));
  const key = await crypto.subtle.importKey('raw', secretBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const signedContent = `${svixId}.${svixTimestamp}.${rawBody}`;
  const sigBuf = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(signedContent));
  const expected = btoa(String.fromCharCode(...new Uint8Array(sigBuf)));

  // Header carries space-separated versioned signatures: "v1,<base64> v1,<base64>"
  return svixSignature.split(' ').some(part => {
    const [version, sig] = part.split(',');
    return version === 'v1' && sig === expected;
  });
}

async function handleResendWebhook(request, env) {
  const jsonHeaders = { 'Content-Type': 'application/json' };

  if (!env.RESEND_WEBHOOK_SECRET) {
    console.error('RESEND_WEBHOOK_SECRET not configured');
    return new Response(JSON.stringify({ error: 'Webhook secret not configured' }), { status: 503, headers: jsonHeaders });
  }

  const rawBody = await request.text();
  const valid = await verifySvixSignature(
    env.RESEND_WEBHOOK_SECRET,
    request.headers.get('svix-id'),
    request.headers.get('svix-timestamp'),
    request.headers.get('svix-signature'),
    rawBody
  );
  if (!valid) {
    return new Response(JSON.stringify({ error: 'Invalid signature' }), { status: 401, headers: jsonHeaders });
  }

  let event;
  try { event = JSON.parse(rawBody); } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON' }), { status: 400, headers: jsonHeaders });
  }

  const type = event?.type || '';
  if (!['email.bounced', 'email.complained', 'email.suppressed'].includes(type)) {
    // Acknowledge everything else so Resend doesn't retry
    return new Response(JSON.stringify({ received: true, ignored: type }), { status: 200, headers: jsonHeaders });
  }

  const addresses = Array.isArray(event?.data?.to) ? event.data.to : [event?.data?.to].filter(Boolean);
  const reason = type.replace('email.', '');
  const detail = event?.data?.bounce?.message || event?.data?.bounce?.subType || '';

  let removed = 0;
  for (const addr of addresses) {
    const email = String(addr).toLowerCase();
    const res = await env.DB.prepare(
      'UPDATE users SET newsletter_subscribed = 0 WHERE email = ? AND newsletter_subscribed = 1'
    ).bind(email).run();

    if (res.meta && res.meta.changes > 0) {
      removed++;
      try {
        await auditLog(env, { user_id: null, id: null, email: 'resend-webhook' },
          'newsletter_auto_unsubscribe', null, email, `${reason}${detail ? ': ' + detail : ''}`, 'webhook');
      } catch (e) {
        console.error('Webhook audit log failed:', e);
      }
      // The admin notification — so a dead address never goes unnoticed
      await sendEmail(
        env,
        ADMIN_EMAILS[0],
        `[HFDL] Subscriber auto-removed (${reason}): ${email}`,
        // Signature-verified, but that only proves Resend sent it — not that the contents are
        // safe. `detail` is the receiving mail server's own bounce text, and `email` is a
        // user-chosen address that the registration regex lets carry < > " and '. Both are
        // escaped; `type`/`reason` are already confined to the three-value allow-list above,
        // and are escaped too so nobody has to re-derive which of the four is which.
        `<p><strong>${escapeHtml(email)}</strong> was automatically unsubscribed from the newsletter.</p>` +
        `<p><strong>Reason:</strong> ${escapeHtml(reason)}${detail ? ' — ' + escapeHtml(detail) : ''}</p>` +
        `<p style="color:#6b7280;font-size:13px;">Triggered by Resend webhook event <code>${escapeHtml(type)}</code>. ` +
        `Resend has also added this address to its suppression list, so future sends skip it automatically.</p>`
      );
    }
  }

  return new Response(JSON.stringify({ received: true, type, removed }), { status: 200, headers: jsonHeaders });
}

// `name` is the registrant's own string. It is Latin-filtered on the api.* and accounts.*
// register paths, but not on the ORCID auto-create path, and rows predating the filter are
// still in the table — so the greeting is escaped rather than trusted. Same reasoning as
// adminNotificationEmail; the difference is only who receives the message.
function verificationEmail(name, token) {
  const link = SITE_URL + '/pages/verify?token=' + token;
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">Welcome to ElkassabgiData</h2>
      <p>Hi ${escapeHtml(name)},</p>
      <p>Thank you for creating your free <strong>ElkassabgiData</strong> account. One login and one API key work across the whole family &mdash; the <a href="https://hfdatalibrary.com" style="color: #2563eb;">HF Data Library</a> (1-minute U.S. equities) and the <a href="https://econdatalibrary.com" style="color: #2563eb;">Econ Data Library</a> (global economic &amp; financial data).</p>
      <p>Please verify your email address to activate your account and start downloading data.</p>
      <p style="text-align: center; margin: 2rem 0;">
        <a href="${link}" style="background: #2563eb; color: #fff; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: 600;">Verify Email</a>
      </p>
      <p style="font-size: 0.9rem; color: #6b7280;">Or copy this link: ${link}</p>
      <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 2rem 0;">
      <p style="font-size: 0.8rem; color: #9ca3af;">ElkassabgiData &mdash; Ahmed Elkassabgi, University of Central Arkansas<br>
      <a href="https://elkassabgidata.com" style="color: #2563eb;">elkassabgidata.com</a> &middot; <a href="https://hfdatalibrary.com" style="color: #2563eb;">hfdatalibrary.com</a> &middot; <a href="https://econdatalibrary.com" style="color: #2563eb;">econdatalibrary.com</a></p>
    </div>`;
}

// Every value below is written by the person registering, and this message arrives from
// noreply@ with the real "Open Admin Panel" button under it — a template the recipient has
// every reason to trust. Until 2026-07-31 all eight were interpolated raw. `ua` is the
// User-Agent header verbatim (never validated, never capped) and `email` only has to survive
// a regex that permits < > " and ', so an anonymous registration — or a Google/ORCID callback,
// which needs no CAPTCHA — could paste a lookalike sign-in button and a tracking pixel into
// the one inbox that can dump every user's API key. The name/institution/country/role fields
// arrive unfiltered on the ORCID auto-create path too (they come from the attacker's own
// editable ORCID profile). escapeHtml on all eight, the way dailyDigestEmail already does it;
// the UA is capped as well so a megabyte of junk cannot bury the rest of the table.
function adminNotificationEmail(user, ip, ua, country) {
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">New HF Data Library Registration</h2>
      <p>A new user has registered:</p>
      <table style="width: 100%; border-collapse: collapse; margin: 1rem 0;">
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Name</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(user.name)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Email</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(user.email)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Institution</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(user.institution)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Country</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(user.country)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Role</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(user.role)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>IP Address</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(ip)}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>CF Country</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${escapeHtml(country)}</td></tr>
        <tr><td style="padding: 6px 12px;"><strong>User Agent</strong></td><td style="padding: 6px 12px; font-size: 0.85rem; color: #6b7280;">${escapeHtml(String(ua ?? '').slice(0, 200))}</td></tr>
      </table>
      <p style="text-align: center; margin: 2rem 0;">
        <a href="https://hfdatalibrary.com/pages/admin" style="background: #1a2332; color: #d4a843; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: 600;">Open Admin Panel</a>
      </p>
      <p style="font-size: 0.8rem; color: #9ca3af;">HF Data Library — automatic notification</p>
    </div>`;
}

// ══════════════════════════════════════
// ── Daily Activity Digest (cron) ──
// ══════════════════════════════════════

// Fair-use alert threshold, GB of downloads in a trailing 30 days, decimal (1e9) to match the
// admin console's column and threshold chips. See the query below for why 50 and not 1 or 10.
const FAIRUSE_ALERT_GB = 50;

async function sendDailyDigest(env) {
  // Gather everything in a single Promise.all for speed.
  const [
    newUsers,
    loginSuccess,
    loginFail,
    uniqueLoggedIn,
    topCountries,
    downloadAgg,
    topTickers,
    topUsers,
    topInstitutions,
    fairUse,
  ] = await Promise.all([
    env.DB.prepare(
      "SELECT name, email, institution, country, role, created_at FROM users WHERE created_at > datetime('now', '-1 day') ORDER BY created_at DESC"
    ).all(),
    env.DB.prepare(
      "SELECT COUNT(*) as c FROM login_history WHERE timestamp > datetime('now', '-1 day') AND success = 1"
    ).first(),
    env.DB.prepare(
      "SELECT COUNT(*) as c FROM login_history WHERE timestamp > datetime('now', '-1 day') AND success = 0"
    ).first(),
    env.DB.prepare(
      "SELECT COUNT(DISTINCT user_id) as c FROM login_history WHERE timestamp > datetime('now', '-1 day') AND success = 1"
    ).first(),
    env.DB.prepare(
      "SELECT country, COUNT(*) as c FROM login_history WHERE timestamp > datetime('now', '-1 day') AND country IS NOT NULL AND country != '' AND country != 'unknown' GROUP BY country ORDER BY c DESC LIMIT 10"
    ).all(),
    env.DB.prepare(
      "SELECT COUNT(*) as c, COALESCE(SUM(bytes_served), 0) as bytes, COUNT(DISTINCT user_id) as users FROM download_log WHERE timestamp > datetime('now', '-1 day')"
    ).first(),
    env.DB.prepare(
      "SELECT ticker, COUNT(*) as c, COALESCE(SUM(bytes_served), 0) as bytes FROM download_log WHERE timestamp > datetime('now', '-1 day') GROUP BY ticker ORDER BY c DESC LIMIT 5"
    ).all(),
    env.DB.prepare(
      "SELECT u.name, u.email, u.institution, COUNT(*) as c, COALESCE(SUM(dl.bytes_served), 0) as bytes FROM download_log dl LEFT JOIN users u ON dl.user_id = u.id WHERE dl.timestamp > datetime('now', '-1 day') AND u.id IS NOT NULL GROUP BY dl.user_id ORDER BY c DESC LIMIT 5"
    ).all(),
    env.DB.prepare(
      "SELECT u.institution, COUNT(*) as c FROM download_log dl LEFT JOIN users u ON dl.user_id = u.id WHERE dl.timestamp > datetime('now', '-1 day') AND u.institution IS NOT NULL AND u.institution != '' GROUP BY u.institution ORDER BY c DESC LIMIT 5"
    ).all(),
    // Fair use. The digest's existing "top users" block is 24 hours ranked by COUNT, which is
    // the wrong shape for a fair-use breach twice over: a breach is about VOLUME, and it builds
    // over weeks, so a steady 40 GB/day never stands out on any single day. This is the same
    // trailing-30-day window and the same decimal-GB unit the admin console ranks on, so the
    // email and the console cannot disagree about who the heavy accounts are.
    //
    // FAIRUSE_ALERT_GB = 50 GB / 30 days. Chosen from the measured distribution on 2026-08-01:
    // 129 accounts exceed 1 GB and 64 exceed 10 GB, so alerting at those levels would mail a
    // list nobody reads; 15 exceed 50 GB and 9 exceed 100 GB. Ten rows is a list that gets
    // looked at. is_active = 1 because an account already revoked is not news every night.
    env.DB.prepare(
      "SELECT u.id, u.name, u.email, u.institution, COUNT(*) as c, COALESCE(SUM(dl.bytes_served), 0) as bytes " +
      "FROM download_log dl JOIN users u ON dl.user_id = u.id " +
      "WHERE dl.timestamp > datetime('now', '-30 days') AND u.is_active = 1 " +
      "GROUP BY dl.user_id HAVING bytes >= ? ORDER BY bytes DESC LIMIT 10"
    ).bind(FAIRUSE_ALERT_GB * 1e9).all(),
  ]);

  const stats = {
    date_ct: new Date().toLocaleDateString('en-US', { timeZone: 'America/Chicago', year: 'numeric', month: 'long', day: 'numeric' }),
    new_users: newUsers.results || [],
    logins_success: loginSuccess?.c || 0,
    logins_fail: loginFail?.c || 0,
    unique_users_logged_in: uniqueLoggedIn?.c || 0,
    countries: topCountries.results || [],
    downloads_count: downloadAgg?.c || 0,
    downloads_bytes: downloadAgg?.bytes || 0,
    downloads_users: downloadAgg?.users || 0,
    top_tickers: topTickers.results || [],
    top_users: topUsers.results || [],
    top_institutions: topInstitutions.results || [],
    fair_use: fairUse.results || [],
    fair_use_gb: FAIRUSE_ALERT_GB,
  };

  const subject = `HF Data Library — daily digest (${stats.date_ct})`;
  const html = dailyDigestEmail(stats);
  const ok = await sendEmail(env, ADMIN_NOTIFY, subject, html);
  console.log(`[daily-digest] sent=${ok} users=${stats.new_users.length} logins=${stats.logins_success} downloads=${stats.downloads_count}`);
  return ok;
}

function fmtBytes(n) {
  if (!n) return '0 B';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + ' GB';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + ' MB';
  if (n >= 1e3) return (n / 1e3).toFixed(0) + ' KB';
  return n + ' B';
}

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function dailyDigestEmail(s) {
  const cell = 'padding:6px 12px; border-bottom:1px solid #e5e7eb; font-size:0.9rem;';
  const cellHead = 'padding:6px 12px; border-bottom:2px solid #1a2332; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.05em; color:#6b7280; text-align:left;';
  const statCard = 'background:#f9fafb; border-radius:8px; padding:16px; text-align:center;';
  const bigNum = 'font-family:Menlo,Consolas,monospace; font-size:1.8rem; font-weight:700; color:#1a2332; line-height:1.1;';
  const bigLabel = 'font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; color:#6b7280; margin-top:4px;';

  const statsCards = `
    <table style="width:100%; border-collapse:separate; border-spacing:8px; margin:1rem 0;">
      <tr>
        <td style="${statCard}"><div style="${bigNum}">${s.new_users.length}</div><div style="${bigLabel}">New users</div></td>
        <td style="${statCard}"><div style="${bigNum}">${s.logins_success}</div><div style="${bigLabel}">Logins</div></td>
        <td style="${statCard}"><div style="${bigNum}">${s.downloads_count}</div><div style="${bigLabel}">Downloads</div></td>
        <td style="${statCard}"><div style="${bigNum}">${fmtBytes(s.downloads_bytes)}</div><div style="${bigLabel}">Served</div></td>
      </tr>
    </table>`;

  const newUsersSection = s.new_users.length === 0
    ? '<p style="color:#6b7280; font-style:italic;">No new registrations in the last 24 hours.</p>'
    : `<table style="width:100%; border-collapse:collapse; margin:0.5rem 0 1.5rem;">
        <tr><th style="${cellHead}">Name</th><th style="${cellHead}">Email</th><th style="${cellHead}">Institution</th><th style="${cellHead}">Country</th><th style="${cellHead}">Role</th></tr>
        ${s.new_users.map(u => `
          <tr><td style="${cell}">${escapeHtml(u.name)}</td><td style="${cell}">${escapeHtml(u.email)}</td><td style="${cell}">${escapeHtml(u.institution)}</td><td style="${cell}">${escapeHtml(u.country)}</td><td style="${cell}">${escapeHtml(u.role)}</td></tr>`).join('')}
      </table>`;

  const tickersSection = s.top_tickers.length === 0
    ? '<p style="color:#6b7280; font-style:italic;">No downloads in the last 24 hours.</p>'
    : `<table style="width:100%; border-collapse:collapse; margin:0.5rem 0 1.5rem;">
        <tr><th style="${cellHead}">Ticker</th><th style="${cellHead}">Downloads</th><th style="${cellHead}">Bytes</th></tr>
        ${s.top_tickers.map(t => `
          <tr><td style="${cell}"><strong>${escapeHtml(t.ticker)}</strong></td><td style="${cell}">${t.c}</td><td style="${cell}">${fmtBytes(t.bytes)}</td></tr>`).join('')}
      </table>`;

  const usersSection = s.top_users.length === 0
    ? ''
    : `<h3 style="margin-top:1.5rem;">Top users by downloads</h3>
       <table style="width:100%; border-collapse:collapse; margin:0.5rem 0 1.5rem;">
        <tr><th style="${cellHead}">User</th><th style="${cellHead}">Institution</th><th style="${cellHead}">Count</th><th style="${cellHead}">Bytes</th></tr>
        ${s.top_users.map(u => `
          <tr><td style="${cell}">${escapeHtml(u.name || '?')}<br><span style="font-size:0.8rem; color:#9ca3af;">${escapeHtml(u.email || '')}</span></td><td style="${cell}">${escapeHtml(u.institution || '-')}</td><td style="${cell}">${u.c}</td><td style="${cell}">${fmtBytes(u.bytes)}</td></tr>`).join('')}
      </table>`;

  // Fair use. Rendered ONLY when an active account is over the threshold, and omitted entirely
  // otherwise — a section that appears every night saying "nobody" is a section that stops being
  // read, and this one exists to be noticed on the night it is not empty. The row carries the
  // account id because the next action is opening that user in the console.
  const fairUseSection = (!s.fair_use || s.fair_use.length === 0)
    ? ''
    : `<div style="border:2px solid #f59e0b; background:#fffbeb; border-radius:6px; padding:0.75rem 1rem; margin:1.25rem 0;">
         <h3 style="margin:0 0 0.25rem; color:#b45309;">Fair use — ${s.fair_use.length} account${s.fair_use.length === 1 ? '' : 's'} over ${s.fair_use_gb} GB in 30 days</h3>
         <p style="margin:0 0 0.5rem; font-size:0.82rem; color:#78716c;">Trailing 30 days, ranked by volume — the same window and unit as the admin console.</p>
         <table style="width:100%; border-collapse:collapse; margin:0.25rem 0 0;">
          <tr><th style="${cellHead}">User</th><th style="${cellHead}">Institution</th><th style="${cellHead}">Downloads</th><th style="${cellHead}">30-day volume</th></tr>
          ${s.fair_use.map(u => `
            <tr><td style="${cell}">${escapeHtml(u.name || '?')}<br><span style="font-size:0.8rem; color:#9ca3af;">${escapeHtml(u.email || '')} &middot; id ${u.id}</span></td><td style="${cell}">${escapeHtml(u.institution || '-')}</td><td style="${cell}">${u.c}</td><td style="${cell}"><strong>${fmtBytes(u.bytes)}</strong></td></tr>`).join('')}
         </table>
       </div>`;

  const institutionsLine = s.top_institutions.length === 0
    ? ''
    : `<p><strong>Active institutions:</strong> ${s.top_institutions.map(i => `${escapeHtml(i.institution)} (${i.c})`).join(', ')}</p>`;

  const countriesLine = s.countries.length === 0
    ? ''
    : `<p><strong>Login countries:</strong> ${s.countries.map(c => `${escapeHtml(c.country)} (${c.c})`).join(', ')}</p>`;

  const failLine = s.logins_fail > 0
    ? `<p style="color:#b91c1c;"><strong>Failed login attempts:</strong> ${s.logins_fail}</p>`
    : '';

  return `
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 680px; margin: 0 auto; color: #1a2332;">
      <h2 style="color:#1a2332; margin-bottom:0.25rem;">Daily activity — ${s.date_ct}</h2>
      <p style="color:#6b7280; margin-top:0;">24-hour summary for HF Data Library.</p>
      ${statsCards}
      ${fairUseSection}
      <h3 style="margin-top:1.5rem;">New registrations (${s.new_users.length})</h3>
      ${newUsersSection}
      <h3 style="margin-top:1.5rem;">Top tickers downloaded</h3>
      ${tickersSection}
      ${usersSection}
      <h3 style="margin-top:1.5rem;">Logins &amp; reach</h3>
      <p><strong>${s.logins_success}</strong> successful logins from <strong>${s.unique_users_logged_in}</strong> unique users.</p>
      ${countriesLine}
      ${institutionsLine}
      ${failLine}
      <p style="text-align:center; margin:2rem 0;">
        <a href="${SITE_URL}/pages/admin" style="background:#1a2332; color:#d4a843; padding:12px 32px; border-radius:8px; text-decoration:none; font-weight:600;">Open Admin Panel</a>
      </p>
      <p style="font-size:0.75rem; color:#9ca3af; text-align:center;">HF Data Library — automatic daily digest, sent ~9 PM Central.</p>
    </div>`;
}

// Escaped for the same reason as verificationEmail — `name` is a user-written column, and a
// password-reset message is the highest-value place to hang a fake button.
function resetEmail(name, token) {
  const link = SITE_URL + '/pages/reset?token=' + token;
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">Password Reset</h2>
      <p>Hi ${escapeHtml(name)},</p>
      <p>You requested a password reset for your HF Data Library account. Click the button below to set a new password. This link expires in 1 hour.</p>
      <p style="text-align: center; margin: 2rem 0;">
        <a href="${link}" style="background: #2563eb; color: #fff; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: 600;">Reset Password</a>
      </p>
      <p style="font-size: 0.9rem; color: #6b7280;">Or copy this link: ${link}</p>
      <p style="font-size: 0.8rem; color: #9ca3af;">If you did not request this, ignore this email.</p>
    </div>`;
}

// ══════════════════════════════════════
// ── Password Hashing (PBKDF2) ──
// ══════════════════════════════════════

async function hashPassword(password) {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), 'PBKDF2', false, ['deriveBits']);
  const hash = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, key, 256);
  const saltB64 = btoa(String.fromCharCode(...salt));
  const hashB64 = btoa(String.fromCharCode(...new Uint8Array(hash)));
  return saltB64 + ':' + hashB64;
}

async function verifyPassword(password, stored) {
  const [saltB64, hashB64] = stored.split(':');
  const salt = Uint8Array.from(atob(saltB64), c => c.charCodeAt(0));
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), 'PBKDF2', false, ['deriveBits']);
  const hash = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, key, 256);
  const hashB64Check = btoa(String.fromCharCode(...new Uint8Array(hash)));
  return hashB64 === hashB64Check;
}

// ══════════════════════════════════════
// ── Session Management ──
// ══════════════════════════════════════

function generateId() {
  return crypto.randomUUID().replace(/-/g, '');
}

async function createSession(env, userId, ip, ua) {
  const sessionId = generateId();
  const expires = new Date(Date.now() + SESSION_DAYS * 86400000).toISOString();
  await env.DB.prepare('INSERT INTO sessions (id, user_id, ip_address, user_agent, expires_at) VALUES (?, ?, ?, ?, ?)')
    .bind(sessionId, userId, ip, ua, expires).run();
  return { sessionId, expires };
}

// Cookie lookup with a NAME BOUNDARY, shared by every session-cookie read.
// Until 2026-07-31 all seven call sites did `cookie.match(/hfd_session=(...)/)`
// (and the ekd_session equivalent), which matches the name ANYWHERE in the header.
// A cookie called `x_hfd_session` — which any sibling *.hfdatalibrary.com or
// *.elkassabgidata.com host can set on us — therefore matched first and shadowed
// the real one: session substitution on one side, and on the other a lockout we
// could not clear, because every `Max-Age=0` response we send names only the real
// cookie and can never delete a differently-named shadow. Anchoring to
// start-of-header or "; " means only the actual cookie can match.
// The value pattern must match the WHOLE value (hence the trailing `;`-or-end):
// a malformed cookie now yields null, so callers fall through to their
// Authorization-header path instead of being handed a truncated prefix that
// could never resolve to a session.
function readCookie(header, name, valuePattern) {
  const m = String(header || '').match(
    new RegExp('(?:^|;\\s*)' + name + '=(' + valuePattern + ')(?:;|$)')
  );
  return m ? m[1] : null;
}

async function getSessionUser(request, env) {
  // Check cookie first
  const cookie = request.headers.get('cookie') || '';
  let sessionId = readCookie(cookie, 'hfd_session', '[a-f0-9]+');

  // Check Authorization header as fallback
  if (!sessionId) {
    const auth = request.headers.get('authorization') || '';
    if (auth.startsWith('Bearer ')) sessionId = auth.slice(7);
  }

  if (!sessionId) return null;

  // §7 scope-aware, collision-free lookup. Explicit aliases — NEVER SELECT
  // s.*, u.* (which flattens so `session.id` becomes the USER id). u.* supplies
  // the user fields (user.id = the user's id); `user_id` is preserved for the
  // ~20 handlers that read it; the session's own id/kind/audience/expiry are
  // exposed under distinct session_* names. The `kind IS NULL OR kind = 'web'`
  // predicate means family_access / idp_master tokens (minted from M2) can never
  // authenticate a full/web session here — structural, not by convention.
  //
  // §EXPIRY-COMPARE: datetime() on both sides. createSession writes this column
  // with toISOString() and createIdpSession writes it with datetime() arithmetic,
  // so this one table is read in both formats and only the canonicalising compare
  // is right for both. Bare, a 30-day web session ran 30 days plus up to a day.
  const session = await env.DB.prepare(
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, " +
    "s.kind AS session_kind, s.audience AS session_audience, " +
    "s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND datetime(s.expires_at) > datetime('now') " +
    "AND (s.kind IS NULL OR s.kind = 'web')"
  ).bind(sessionId).first();

  if (!session || !session.is_active) return null;
  // Defense in depth: assert kind even though the query already filters it.
  if (session.session_kind && session.session_kind !== 'web') return null;
  await touchApiKeyExpiry(env, session);
  return session;
}

/** Slide api_key_expires_at forward for a user who is demonstrably still active.
 *
 *  WHY: API_KEY_DAYS is applied ONCE, at registration, and nothing renewed it —
 *  not login, not SSO, not /v1/auth/me. The only renewal was the user manually
 *  clicking "regenerate", which nobody does until something breaks. Measured
 *  2026-07-27: 180 of 493 accounts (36.5%) were already expired, and every one of
 *  them saw their key displayed on the download page and then got
 *  "invalid_key" — signed in, key visible, download refused. More lapsed daily.
 *
 *  Placed in getSessionUser because that is the single chokepoint every
 *  authenticated path funnels through (password login, Google, ORCID, the family
 *  SSO redirect, /v1/auth/me). Refreshing at each call site instead would mean
 *  finding them all, and missing one silently reinstates the lockout for that path.
 *
 *  The WHERE clause is what keeps this cheap: it only writes when the key is
 *  actually near or past expiry, so an active session costs a no-op UPDATE
 *  (0 rows) rather than a write per request.
 *
 *  Deliberately keeps the EXISTING key value. Rotating on login would invalidate
 *  every key already saved in a user's script, notebook or MCP config — a silent
 *  break far worse than the one being fixed.
 *
 *  Never throws: authentication must not fail because a housekeeping write did.
 */
async function touchApiKeyExpiry(env, session) {
  try {
    const userId = session.user_id || session.id;
    if (!userId) return;
    const next = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    // §EXPIRY-COMPARE: datetime() on the stored side. This column is ISO text, so
    // the bare compare made every key look up to a day further from expiry than it
    // was, and the "is it near expiry" gate opened a day late. Harmless here — the
    // only cost was a delayed no-op UPDATE — but it is the same defect, and leaving
    // one bare compare behind is how the next reader concludes the pattern is fine.
    await env.DB.prepare(
      'UPDATE users SET api_key_expires_at = ? WHERE id = ? AND ' +
      "(api_key_expires_at IS NULL OR datetime(api_key_expires_at) < datetime('now', ?))"
    ).bind(next, userId, '+' + Math.ceil(API_KEY_DAYS / 2) + ' days').run();
  } catch (_e) {
    // Non-fatal by design — a failed refresh must never log the user out.
  }
}

// Cross-site SSO: read the first-party hfd_session cookie and redirect back to an
// allow-listed family origin with the user's api_key in the URL fragment. The
// fragment is never sent to any server (and econ strips it on arrival); the key
// is the user's own download key already shown on the account page; and the
// return-origin allow-list stops the key ever reaching an untrusted site.
async function handleSSO(request, env) {
  const ALLOWED_RETURN = [
    'https://econdatalibrary.com', 'https://www.econdatalibrary.com',
    'https://elkassabgidata.com', 'https://www.elkassabgidata.com',
  ];
  const ret = new URL(request.url).searchParams.get('return') || '';
  let retUrl;
  try { retUrl = new URL(ret); } catch (e) { return new Response('bad return url', { status: 400 }); }
  if (!ALLOWED_RETURN.includes(retUrl.origin)) {
    return new Response('return origin not allowed', { status: 403 });
  }
  const user = await getSessionUser(request, env);
  const frag = (user && user.api_key)
    ? 'sso_key=' + encodeURIComponent(user.api_key) + '&sso_name=' + encodeURIComponent(user.name || '')
    : 'sso_key=none';
  const dest = retUrl.origin + retUrl.pathname + '#' + frag;
  return new Response(null, { status: 302, headers: { 'Location': dest, 'Cache-Control': 'no-store' } });
}

// opts.allowQueryKey — honour ?api_key= in the URL. Off unless the caller asks
// for it, and only the data routes ask, because a plain browser navigation to a
// download link has no way to set a header. Reading it unconditionally meant the
// key was accepted on every route requireAuth guards, including /v1/admin/*: a
// credential in a query string is written into browser history, bookmarks, the
// Referer header of anything the page then links to, and every proxy/CDN access
// log, so the routes it must never reach are exactly the ones that change state
// or read other people's rows.
async function getUserByApiKey(request, env, opts) {
  // Check header first
  let apiKey = request.headers.get('X-API-Key');
  // Fallback to query parameter (for direct browser downloads)
  if (!apiKey && opts && opts.allowQueryKey) {
    const url = new URL(request.url);
    apiKey = url.searchParams.get('api_key');
  }
  if (!apiKey) return null;
  // Check expiration. §EXPIRY-COMPARE: datetime() on the stored side — the column
  // is ISO text, so bare this accepted a lapsed key until the end of its UTC day.
  return await env.DB.prepare(
    'SELECT * FROM users WHERE api_key = ? AND is_active = 1 AND (api_key_expires_at IS NULL OR datetime(api_key_expires_at) > datetime("now"))'
  ).bind(apiKey).first();
}

// opts is forwarded verbatim to getUserByApiKey. Callers that pass nothing —
// which is every mutation route — get header-only key auth, never ?api_key=.
async function requireAuth(request, env, opts) {
  let user = await getSessionUser(request, env);
  if (!user) user = await getUserByApiKey(request, env, opts);
  return user;
}

// ══════════════════════════════════════
// ── Family SSO M2 — shared crypto + token helpers (single source of truth) ──
// ══════════════════════════════════════
// These are deterministic Web-Crypto helpers used ONLY for the family SSO
// (M2) credentials. Do NOT confuse with hashPassword (PBKDF2, salted, one-way,
// unusable as a lookup key) or generateId (UUID hex, web-session-only).

// SHA-256 hex — the at-rest hash for every M2 credential (ekd_session, edl_at,
// one-time code, oauth state). A raw token is stored ONLY as its sha256, so a
// replayed raw token misses the lookup (null → fail closed).
async function sha256Hex(s) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, '0')).join('');
}

function b64url(bytes) {
  let s = '';
  for (const x of bytes) s += String.fromCharCode(x);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

// 128-bit base64url — the raw factory for all M2 tokens. Its charset can never
// be captured by the '[a-f0-9]+' value pattern getSessionUser passes to
// readCookie, so an M2 token can't even be parsed as a web session id (defense
// in depth over the kind predicate).
function generateToken() {
  return b64url(crypto.getRandomValues(new Uint8Array(16)));
}

// PKCE S256: base64url(sha256(verifier)) — compared against the code_challenge.
async function pkceS256(verifier) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return b64url(new Uint8Array(b));
}

function constantTimeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

function extractBearer(request) {
  const auth = request.headers.get('Authorization') || request.headers.get('authorization') || '';
  return auth.startsWith('Bearer ') ? auth.slice(7).trim() : null;
}

// §7 scope-aware family-token validator (data plane). Accepts a family_access
// edl_at (Bearer, stored hashed) and returns a REDUCED-SCOPE user with api_key
// stripped. Rejects unless: the hashed token maps to a live family_access
// session, the user is active, the token's audience is an ACTIVE registered
// origin, AND the audience equals the request Origin (revocation/audit binding).
// Never authenticates a web/full session — that stays getSessionUser (kind
// predicate). Family tokens are ONLY ever passed to requireDataAuth (data
// routes); mutation/admin routes keep requireAuth, which rejects them.
async function validateFamilyToken(request, env) {
  const raw = extractBearer(request);
  if (!raw) return null;
  const idHash = await sha256Hex(raw);
  // §EXPIRY-COMPARE: datetime() on both sides. mintFamilyTokens writes this row in
  // space format, so it was already compared correctly — the wrap is what keeps it
  // correct now that the same column is also read for ISO-format web sessions, and
  // it is why the read side could not simply bind an ISO 'now' instead.
  const row = await env.DB.prepare(
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, s.kind AS session_kind, " +
    "s.audience AS session_audience, s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.kind = 'family_access' AND datetime(s.expires_at) > datetime('now')"
  ).bind(idHash).first();
  if (!row || !row.is_active) return null;
  const origin = request.headers.get('Origin') || '';
  if (!row.session_audience || row.session_audience !== origin) return null;
  const reg = await getRegistry(env);
  const client = reg.get(origin);
  if (!client || client.status !== 'active') return null;
  return { ...row, api_key: null, isFamilyToken: true };
}

// Data-route auth: a full session/api_key (full scope, keeps api_key) OR a
// family token (reduced scope). Used ONLY by data handlers — never mutation/admin.
// This is the ONLY place ?api_key= is still honoured: a download opened straight
// from the address bar or a wget/curl one-liner cannot send X-API-Key, and those
// URLs are already in users' scripts. Nothing here mutates state or reveals
// another account.
async function requireDataAuth(request, env) {
  return (await requireAuth(request, env, { allowQueryKey: true })) || (await validateFamilyToken(request, env));
}

// §6 fail-closed IdP router for accounts.elkassabgidata.com. Serves ONLY the
// explicit allowlist (+ /sdk/ and /.well-known/ prefixes); everything else 404s
// and can never reach the data/admin table. In M2a the allowlisted endpoints
// are not built yet → 501; M2b replaces these stubs with the real /authorize,
// token endpoints, OAuth callbacks, and SDK. This host never serves /v1/admin,
// /v1/download, /v1/bars, /v1/auth/me, or any mutation route.
async function handleAccountsHost(request, env, url, path, ip, ua, country) {
  const onAllowlist =
    ACCOUNTS_ALLOW.has(path) ||
    path.startsWith('/sdk/') ||
    path.startsWith('/.well-known/');
  if (!onAllowlist) return new Response('Not found', { status: 404 });

  const method = request.method;
  // Cookieless CORS for the token endpoints (called cross-origin by the SDK).
  // A registered active family origin gets ACAO=origin, NEVER credentials.
  const origin = request.headers.get('Origin') || '';
  const decision = await corsDecision(origin, env);
  const tokenCors = {
    'Access-Control-Allow-Origin': decision.allow ? origin : IDP_ORIGIN,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Vary': 'Origin',
  };
  if (method === 'OPTIONS' && (path === '/token/exchange' || path === '/token/refresh' || path === '/logout')) {
    return new Response(null, { headers: tokenCors });
  }

  try {
    if (path === '/authorize' && method === 'GET') return await handleAuthorizeGet(request, env, url);
    if (path === '/authorize' && method === 'POST') return await handleAuthorizePost(request, env, ip, ua);
    if (path === '/login' && method === 'POST') return await handleAccountsLogin(request, env, ip, ua, country);
    if (path === '/login/2fa' && method === 'POST') return await handleAccounts2faVerify(request, env, ip, ua, country);
    if (path === '/register' && method === 'POST') return await handleAccountsRegister(request, env, ip, ua, country);
    // GET on a form path (no body) → 404 (the allowlist is method-agnostic).
    if (path === '/login' || path === '/login/2fa' || path === '/register') return new Response('Not found', { status: 404 });
    if (path === '/v1/auth/google/start' && method === 'GET') return await startFamilyOAuth(request, env, 'google', ip, url);
    if (path === '/v1/auth/orcid/start'  && method === 'GET') return await startFamilyOAuth(request, env, 'orcid', ip, url);
    if (path === '/account' && method === 'GET') return await handleAccountGet(request, env);
    if (path === '/account/regenerate-key' && method === 'POST') return await handleAccountRegenerate(request, env, ip, ua);
    if (path === '/account/logout' && method === 'POST') return await handleAccountLogout(request, env);
    if (path === '/account/update-profile' && method === 'POST') return await handleAccountUpdateProfile(request, env);
    if (path === '/account/change-password' && method === 'POST') return await handleAccountChangePassword(request, env);
    if (path === '/account/export' && method === 'GET') return await handleAccountExport(request, env);
    if (path === '/account/resend-verification' && method === 'POST') return await handleAccountResendVerification(request, env);
    if (path === '/account/delete' && method === 'POST') return await handleAccountDelete(request, env);
    if (path === '/csp-report' && method === 'POST') return await handleCspReport(request, env);
    if (path === '/account' || path === '/account/regenerate-key' || path === '/account/logout' || path === '/account/update-profile' || path === '/account/change-password' || path === '/account/export' || path === '/account/resend-verification' || path === '/account/delete') return new Response('Not found', { status: 404 });
    if (path === '/token/exchange' && method === 'POST') return await handleTokenExchange(request, env, ip, ua, tokenCors);
    if (path === '/token/refresh' && method === 'POST') return await handleTokenRefresh(request, env, ip, ua, tokenCors);
    if (path === '/logout' && method === 'POST') return await handleAccountsLogout(request, env, tokenCors);
    if (path === '/v1/auth/google/callback') return await handleAccountsGoogleCallback(request, env, ip, ua, country);
    if (path === '/v1/auth/orcid/callback') return await handleAccountsOrcidCallback(request, env, ip, ua, country);
    if (path.startsWith('/sdk/')) return await handleSdkAsset(path);
    // Allowlisted but not built in this sub-stage (e.g. /v1/auth/*/start until
    // M2b-2b, /.well-known/*): fail closed.
    return new Response('Not implemented', { status: 501, headers: { 'Cache-Control': 'no-store' } });
  } catch (err) {
    // Generic error only — the token endpoints are CORS-readable by family
    // origins, so never reflect err.message (D1/SQL/schema fragments). Log it
    // server-side instead.
    console.error(JSON.stringify({ evt: 'idp_error', path, msg: err && err.message }));
    return new Response(JSON.stringify({ error: 'idp_error' }), {
      status: 500, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...tokenCors },
    });
  }
}

// Returns a specific, actionable 401 message explaining WHY auth failed —
// distinguishes missing key / invalid key / inactive / expired so users
// (esp. programmatic ones) know to regenerate rather than seeing a bare 401.
const ACCOUNT_URL = 'https://hfdatalibrary.com/pages/account';
async function explainAuthFailure(request, env) {
  let apiKey = request.headers.get('X-API-Key');
  if (!apiKey) {
    try { apiKey = new URL(request.url).searchParams.get('api_key'); } catch (e) { /* ignore */ }
  }
  if (!apiKey) {
    return `Authentication required. Provide your API key in the X-API-Key header (or log in). Get a key at ${ACCOUNT_URL}`;
  }
  const row = await env.DB.prepare(
    'SELECT is_active, api_key_expires_at FROM users WHERE api_key = ?'
  ).bind(apiKey).first();
  if (!row) {
    return `Invalid API key. Check the value or generate a new one at ${ACCOUNT_URL}`;
  }
  if (!row.is_active) {
    return 'This account is inactive. Contact admin@hfdatalibrary.com';
  }
  if (row.api_key_expires_at) {
    const day = String(row.api_key_expires_at).slice(0, 10);
    return `Your API key expired on ${day}. API keys are valid for 30 days — regenerate yours at ${ACCOUNT_URL} (regenerating issues a new key value, so update your scripts).`;
  }
  return `Authentication failed. Manage your key at ${ACCOUNT_URL}`;
}

// ══════════════════════════════════════
// ── Auth Handlers ──
// ══════════════════════════════════════

async function handleRegister(request, env, cors, ip, ua, country) {
  // Flood guard only — generous on purpose. The real "how many accounts may come from this
  // IP" cap is charged further down, immediately before the INSERT, so honest retries after
  // a typo or a CAPTCHA failure cost nothing. See the api:register notes in RATE_LIMITS.
  const rl = await checkRateLimit(env, rlIpKey(ip), 'api:register:burst');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  // Turnstile CAPTCHA
  const turnstileValid = await verifyTurnstile(env, body.turnstile_token, ip);
  if (!turnstileValid) {
    return jsonRes({ error: 'CAPTCHA verification failed. Please try again.' }, 400, cors);
  }

  const { name, email, password, institution, role } = body;
  const userCountry = body.country || country;
  const newsletter = body.newsletter ? 1 : 0;
  // ORCID iD AT SIGN-UP MUST BE PROVEN. Until 2026-07-31 this read body.orcid_id straight
  // from the request, and handleOrcidCallback resolves a login with
  //     SELECT * FROM users WHERE orcid_id = ?
  // so an unproven claim here is a login credential there. The attack needed no mailbox and
  // no interaction from the victim:
  //   1. POST /register with orcid_id set to a stranger's ORCID iD.
  //   2. That person later clicks "Sign in with ORCID".
  //   3. The lookup finds the ATTACKER's row and signs the victim into it.
  // ORCID iDs are published on every paper an academic writes, so step 1 costs nothing —
  // this is the Google pre-hijack again, but through an identifier that is public by design.
  //
  // fetchOrcidProfile is not a defence: it proves the iD EXISTS, never that the registrant
  // owns it. Only completing ORCID's OAuth proves that, and handleOrcidCallback now mints an
  // HMAC-signed 10-minute token when it does. Accept the token; ignore body.orcid_id.
  //
  // The signature alone was still not enough: it proved somebody had completed ORCID,
  // not that THIS browser had, so the token could be mailed to a stranger and spent on
  // their brand-new account. Audience 'api' plus the __Host- nonce cookie make it
  // unusable anywhere but the browser that earned it (download.html sends the fetch
  // with credentials so the cookie rides along), and the row is burned at the INSERT.
  const orcidPrefill = await verifyOrcidPrefill(env, request, body.orcid_prefill, 'api');
  // A prefill that was SENT but does not verify used to return 400 right here. That was
  // wrong: it turned a registration that has always succeeded into a failure. The worker
  // this replaces just created the account with orcid_id NULL, and two ordinary people
  // hit the 400. (1) The token lives ORCID_PREFILL_TTL_MS = 10 minutes, counted from the
  // ORCID callback — that is, from the moment the EMPTY form first appears — and the form
  // asks for name, email, a strength-checked password, institution, country, role and a
  // Turnstile solve. Anyone slower than ten minutes lost the account, and could not even
  // retry: download.html captures the token once at load and re-posts the same dead string,
  // so "Create Account" failed identically until they restarted the ORCID flow, discarding
  // everything they had typed. (2) The 2026-07-31 deploy window — pages and worker ship as
  // separate jobs, so tokens minted by the previous worker were briefly arriving in the new
  // format check. That second case is over (see §PF-LEGACY in verifyOrcidPrefill, removed
  // once the last old token expired); the first is permanent, and neither was ever a reason
  // to refuse somebody an account.
  //
  // So: create it, leave orcid_id NULL, and SAY SO in the 201. Staying silent is not the
  // alternative — the banner above the form has already promised "Your ORCID iD will be
  // linked to this account. You will be able to sign in with ORCID next time", and it will
  // not be, so the success response has to correct that promise. See the response body at
  // the end of this function: api_key and session keep their exact shape (the deployed
  // download.html keys its whole success branch off data.api_key), and the ORCID fields are
  // purely additive, which is the only thing that is safe when the page and the worker
  // deploy independently.
  const orcidPrefillFailed = !!body.orcid_prefill && !orcidPrefill;
  const orcidFromOauth = orcidPrefill ? orcidPrefill.orcidId : null;

  // If ORCID provided, fetch profile data too
  let orcidProfileJson = null;
  if (orcidFromOauth) {
    const profile = await fetchOrcidProfile(orcidFromOauth);
    if (profile) orcidProfileJson = JSON.stringify(profile);
  }

  if (!name || !email || !password || !institution || !role || !userCountry) {
    return jsonRes({ error: 'Required: name, email, password, institution, country, role' }, 400, cors);
  }
  // Email format check
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return jsonRes({ error: 'Invalid email address' }, 400, cors);
  }
  // Field length limits
  if (name.length > 100 || institution.length > 200 || role.length > 100 || userCountry.length > 100) {
    return jsonRes({ error: 'One or more fields exceed length limits' }, 400, cors);
  }
  // Latin/English characters only — these strings render publicly on the stats page
  // (world map, institutions list) and in admin emails. Reject CJK, Cyrillic, Arabic, etc.
  if (!isLatinish(name) || !isLatinish(institution) || !isLatinish(userCountry) || !isLatinish(role)) {
    return jsonRes({ error: 'Name, institution, country, and role must use English/Latin letters only.' }, 400, cors);
  }
  // Normalize country to ISO-2 if recognized — "United States" / "USA" / "us" all
  // become "US". Falls back to original (trimmed) for unrecognized free-text so we
  // don't reject countries we haven't enumerated.
  const normalizedCountry = normalizeCountry(userCountry) || userCountry.trim();
  // Password strength
  const pw = checkPasswordStrength(password);
  if (!pw.ok) return jsonRes({ error: pw.error }, 400, cors);

  // Check existing
  const existing = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();
  if (existing) {
    return jsonRes({ error: 'Email already registered. Please log in.' }, 409, cors);
  }

  // One ORCID iD, one account — otherwise a second row silently shadows the first and the
  // WHERE orcid_id = ? login above becomes ambiguous.
  if (orcidFromOauth) {
    const orcidTaken = await env.DB.prepare('SELECT id FROM users WHERE orcid_id = ?')
      .bind(orcidFromOauth).first();
    if (orcidTaken) {
      return jsonRes({ error: 'That ORCID iD is already linked to an account. Please log in.' }, 409, cors);
    }
  }

  // ACCOUNT CAP — charged here, not on entry. Everything above this line can reject a
  // request for reasons that are the person's honest mistake (typo, weak password, CAPTCHA
  // hiccup, a name we refuse for not being Latin script). Charging at the top made each of
  // those cost one of three hourly attempts and locked real users out; because the key is
  // the IP, one NAT'd department could be locked out by three fumbled sign-ups. Only
  // accounts that actually get created count against this.
  const acct = await checkRateLimit(env, rlIpKey(ip), 'api:register');
  if (!acct.ok) return rateLimitResponse(acct.retryAfter, cors);

  const passwordHash = await hashPassword(password);
  const apiKey = 'hfd_' + generateId();
  const unsubscribeToken = generateId();
  // NO SELF-SERVICE ADMIN. This used to be `ADMIN_EMAILS.includes(email) ? 1 : 0`, so
  // anyone who registered one of the two (published, guessable) owner addresses received
  // is_admin = 1 from a public unauthenticated endpoint. The only thing preventing it was
  // that rows for both addresses already exist and a duplicate email 409s first — one
  // deleted row from a full console takeover. Admin is granted out-of-band now: by the
  // authenticated admin PUT, or directly in D1. A registration form must never mint it.
  const isAdmin = 0;

  const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();

  // Burn the prefill here, not at verify time: everything above can reject for an
  // honest mistake (typo, weak password, CAPTCHA hiccup) and the retry must still
  // carry a live token. From this line on the token is spent, so a replay — the same
  // string posted twice — cannot mint a second account wearing the same ORCID iD.
  // burnOrcidPrefill rather than consumeOrcidPrefill directly: it treats "no prefill was
  // sent at all" as success, so this reads the same as the older
  // `pre && !(await consumeOrcidPrefill(...))`. It no longer excuses anything else — the
  // legacy-token exemption it used to carry went with §PF-LEGACY on 2026-07-31.
  if (!(await burnOrcidPrefill(env, orcidPrefill))) {
    return jsonRes({ error: 'That ORCID confirmation has already been used. Please sign in with ORCID again.' }, 409, cors);
  }

  await env.DB.prepare(
    'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)'
  // email_verified is 0 for EVERYONE, admins included. It used to be `isAdmin ? 1 : 0`, so
  // registering with an address listed in ADMIN_EMAILS minted an is_admin = 1 account that
  // was verified on the spot, without anyone ever proving they could read that mailbox.
  // Both admin addresses are guessable, and the only thing standing in the way was that
  // rows for them already exist (a duplicate email 409s above) — one deleted row and the
  // console was open to whoever registered first. Admins verify by email like everyone else.
  ).bind(name, email.toLowerCase(), passwordHash, institution, normalizedCountry, role, apiKey, apiKeyExpires, isAdmin, 0, newsletter, unsubscribeToken, ip, ua, orcidFromOauth, orcidProfileJson).run();

  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  // Log registration
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, userCountry).run();

  // Send verification email (skip for admins — auto-verified)
  {
    // Sent to EVERYONE now, admins included. Admin rows are no longer born verified, so an
    // admin who is never emailed a link is one nobody can prove owns the address it names.
    const verifyToken = generateId();
    const verifyExpires = new Date(Date.now() + 86400000).toISOString(); // 24 hours
    await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)')
      .bind(user.id, verifyToken, verifyExpires).run();
    await sendEmail(env, email.toLowerCase(), 'Verify your ElkassabgiData account', verificationEmail(name, verifyToken), FROM_EMAIL, 'ElkassabgiData');
  }

  // Send admin notification for every new registration
  try {
    await sendEmail(
      env,
      ADMIN_NOTIFY,
      `New registration: ${name} (${institution})`,
      adminNotificationEmail({ name, email: email.toLowerCase(), institution, country: userCountry, role }, ip, ua, country)
    );
  } catch (e) { /* notification failures shouldn't block registration */ }

  // Create session
  const { sessionId, expires } = await createSession(env, user.id, ip, ua);

  // The four original keys, unmoved and unrenamed. The download.html that is deployed
  // RIGHT NOW decides success on `data.api_key` being truthy and then reads `data.session`;
  // anything that shifted those would blank out the "Account created!" panel for every
  // registrant during the window where the old page is talking to this worker.
  const payload = {
    message: isAdmin ? 'Registration successful' : 'Registration successful. Please check your email to verify your account.',
    api_key: apiKey,
    session: sessionId,
    email_verified: isAdmin ? true : false
  };
  // Additive, and only when the caller actually tried to bring an ORCID iD. An older page
  // ignores unknown keys, so this can never break one; a page that knows them renders the
  // notice next to the API key. orcid_linked is the machine-readable half — false is the
  // honest answer to a form whose banner said the iD would be linked.
  if (body.orcid_prefill) payload.orcid_linked = !!orcidFromOauth;
  if (orcidPrefillFailed) {
    payload.orcid_notice = ORCID_NOT_LINKED_NOTICE;
    // Also folded into message, because a script POSTing /v1/auth/register directly prints
    // message and nothing else. Telling only the browser would leave API callers with an
    // account that silently lacks the ORCID link they asked for.
    payload.message = payload.message + ' ' + ORCID_NOT_LINKED_NOTICE;
  }
  const res = jsonRes(payload, 201, cors);

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  return res;
}

async function handleLogin(request, env, cors, ip, ua, country) {
  // Peek only — a correct password must not spend anyone's budget. The charge happens in
  // the failure branch below, so the counter measures wrong guesses rather than traffic.
  const rl = await checkRateLimit(env, rlIpKey(ip), 'api:login', { charge: false });
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { email, password } = body;
  if (!email || !password) {
    return jsonRes({ error: 'Required: email, password' }, 400, cors);
  }

  const user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  if (!user || !(await verifyPassword(password, user.password_hash))) {
    // THIS is the attempt worth counting. Charged here and nowhere else, so five wrong
    // guesses still lock the IP for five minutes while honest sign-ins cost nothing.
    await checkRateLimit(env, rlIpKey(ip), 'api:login');
    // Log failed attempt
    if (user) {
      await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)')
        .bind(user.id, ip, ua, country).run();
    }
    return jsonRes({ error: 'Invalid email or password' }, 401, cors);
  }

  if (!user.is_active) {
    return jsonRes({ error: 'Account has been deactivated. Contact admin@hfdatalibrary.com.' }, 403, cors);
  }

  // Check if 2FA is enabled
  if (user.totp_enabled) {
    const pendingToken = generateId();
    const pendingExpires = new Date(Date.now() + 10 * 60 * 1000).toISOString(); // 10 min to enter code
    await env.DB.prepare('INSERT INTO totp_pending (token, user_id, expires_at, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)')
      .bind(pendingToken, user.id, pendingExpires, ip, ua).run();

    return jsonRes({
      totp_required: true,
      pending_token: pendingToken,
      message: 'Enter your 2FA code from your authenticator app'
    }, 200, cors);
  }

  // Update login info
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();

  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId, expires } = await createSession(env, user.id, ip, ua);

  const res = jsonRes({
    message: 'Login successful',
    user: { name: user.name, email: user.email, institution: user.institution, api_key: user.api_key },
    session: sessionId
  }, 200, cors);

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  return res;
}

async function handleLogout(request, env, cors) {
  const cookie = request.headers.get('cookie') || '';
  let sessionId = readCookie(cookie, 'hfd_session', '[a-f0-9]+');
  // Accept the Bearer session too, mirroring getSessionUser.
  //
  // This read the cookie ONLY, while both callers in the repo authenticate by header:
  // pages/admin.html:375 sends authHeaders() (Authorization: Bearer <session id>) and
  // js/site.js:279 sends an explicit Bearer. admin.html is served from hfdatalibrary.com and
  // the cookie is scoped to api.hfdatalibrary.com, so it is not attached to that fetch at all.
  // The handler therefore found no session id, deleted nothing, cleared a cookie the browser
  // never had, and returned 200 "Logged out" — the console showed a successful sign-out while
  // the session stayed valid in D1 for its full 30 days. That session id is exactly what
  // /v1/auth/sso hands an api_key out for, so "log out" on the admin console left the highest
  // -privilege credential on the machine.
  //
  // Same precedence as getSessionUser (cookie first, then Bearer) so a browser carrying both
  // behaves identically on both paths.
  if (!sessionId) {
    const auth = request.headers.get('authorization') || '';
    if (auth.startsWith('Bearer ')) {
      const bearer = auth.slice(7).trim();
      // Same shape check the cookie gets. generateId() is lowercase hex, so anything else was
      // never a session id we issued and must not reach the DELETE as a bind value.
      if (/^[a-f0-9]+$/.test(bearer)) sessionId = bearer;
    }
  }
  if (sessionId) {
    await env.DB.prepare('DELETE FROM sessions WHERE id = ?').bind(sessionId).run();
  }
  const res = jsonRes({ message: 'Logged out' }, 200, cors);
  res.headers.set('Set-Cookie', 'hfd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0');
  return res;
}

async function handleMe(request, env, cors) {
  const user = await requireDataAuth(request, env);
  if (!user) return jsonRes({ error: 'Not authenticated' }, 401, cors);

  // §7 Family (edl_at) tokens get a SCRUBBED profile — never api_key, nor any
  // admin/VIP/2FA/counter field. Revealing api_key requires a full first-party
  // session (validateFamilyToken already set api_key=null; this omits it and the
  // sensitive fields entirely so nothing leaks even if that changed).
  if (user.isFamilyToken) {
    return jsonRes({
      id: user.user_id || user.id,
      name: user.name,
      email: user.email,
      institution: user.institution,
      country: user.country,
      role: user.role,
      // email_verified is what gates every download route, so the account page cannot tell
      // a user why their downloads are refused without it. It was absent here, which also
      // made `!user.email_verified` on the page read as true for EVERYONE — a banner shown
      // to people who are already verified is worse than no banner at all.
      email_verified: !!user.email_verified,
      profile_complete: !!user.profile_complete,
      orcid_id: user.orcid_id || null,
      created_at: user.created_at,
      isFamilyToken: true,
    }, 200, cors);
  }

  return jsonRes({
    id: user.user_id || user.id,
    name: user.name,
    email: user.email,
    institution: user.institution,
    country: user.country,
    role: user.role,
    api_key: user.api_key,
    api_key_expires_at: user.api_key_expires_at,
    is_admin: !!user.is_admin,
    is_vip: !!user.is_vip,
    totp_enabled: !!user.totp_enabled,
    orcid_id: user.orcid_id || null,
    google_id: user.google_id || null,
    email_verified: !!user.email_verified,   // gates downloads; the account page needs it
    profile_complete: !!user.profile_complete,
    orcid_profile: user.orcid_profile_json ? JSON.parse(user.orcid_profile_json) : null,
    newsletter_subscribed: !!user.newsletter_subscribed,
    created_at: user.created_at,
    download_count: user.download_count,
    total_bytes_downloaded: user.total_bytes_downloaded
  }, 200, cors);
}

// ── 2FA handlers ──

// Enrolling a second factor requires a VERIFIED mailbox.
//
// handleLogin does not check email_verified, so registering with an address you do not own
// still yields a working session. From there, enrolling TOTP set totp_enabled = 1, and
// handleLogin's `if (user.totp_enabled)` then demands a code from EVERY subsequent login —
// including the real owner's, once they verify or reset their password. There is no backup
// code, no admin reset, and disable itself requires a working code, so the mailbox owner was
// permanently locked out of their own address by someone who never proved they could read it.
//
// Requiring verification to enrol closes it at the only point where the squatter has to
// demonstrate something they cannot fake. It costs a legitimate user nothing: they verify by
// email anyway, and 2FA is opt-in and set up later.
function require2faEnrolmentEligible(user, cors) {
  if (!user.email_verified) {
    return jsonRes({
      error: 'Verify your email address before enabling two-factor authentication.',
    }, 403, cors);
  }
  return null;
}

async function handle2faSetup(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);
  const ineligible = require2faEnrolmentEligible(user, cors);
  if (ineligible) return ineligible;

  const userId = user.user_id || user.id;

  // Re-enrolment on an ALREADY-ENABLED account has to go through disable first.
  //
  // This handler's UPDATE sets totp_enabled = 0, and its only gate was "has a session". Two
  // doors down, handle2faDisable requires BOTH the password and a currently-valid TOTP code
  // before it will turn the second factor off. So the strong gate was bypassable by calling
  // the weak endpoint: anyone holding a session — a borrowed browser, a stolen cookie, the
  // session id that /v1/auth/sso hands out — could POST /v1/auth/2fa/setup and the victim's
  // second factor was off in one request, no password, no code. Whatever protection
  // handle2faDisable was providing, it was providing to nobody.
  //
  // Blocking only when totp_enabled = 1 keeps every legitimate flow: a user with no 2FA
  // enrols normally, and one who ran setup but never confirmed (secret present,
  // totp_enabled = 0) can re-run setup to get a fresh QR code. Changing authenticators means
  // disable-then-setup, which is the path that already asks for the password and a code.
  const existing = await env.DB.prepare('SELECT totp_enabled FROM users WHERE id = ?').bind(userId).first();
  if (existing && existing.totp_enabled) {
    return jsonRes({
      error: 'Two-factor authentication is already enabled. Disable it first (which requires your password and a current code) before setting up a new authenticator.',
    }, 409, cors);
  }

  const secret = generateTotpSecret();
  const otpauthUrl = `otpauth://totp/HF%20Data%20Library:${encodeURIComponent(user.email)}?secret=${secret}&issuer=HF%20Data%20Library`;

  // Store secret temporarily (not enabled yet — user must confirm with a valid code)
  await env.DB.prepare('UPDATE users SET totp_secret = ?, totp_enabled = 0 WHERE id = ?').bind(secret, userId).run();

  return jsonRes({
    secret,
    otpauth_url: otpauthUrl,
    qr_code_url: `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(otpauthUrl)}`
  }, 200, cors);
}

async function handle2faEnable(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);
  // Checked here as well as in setup, not only there. These are two independently routed
  // endpoints and enable is the one that actually writes totp_enabled = 1 — a guard that
  // lives only on the step before it is a guard on the wrong statement, and would be bypassed
  // by anyone posting straight to /2fa/enable with a secret from an earlier eligible moment.
  const ineligible = require2faEnrolmentEligible(user, cors);
  if (ineligible) return ineligible;

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { code } = body;
  if (!code) return jsonRes({ error: 'Required: code' }, 400, cors);

  const userId = user.user_id || user.id;
  const dbUser = await env.DB.prepare('SELECT totp_secret FROM users WHERE id = ?').bind(userId).first();
  if (!dbUser || !dbUser.totp_secret) return jsonRes({ error: 'Run setup first' }, 400, cors);

  const valid = await verifyTotp(dbUser.totp_secret, code);
  if (!valid) return jsonRes({ error: 'Invalid code. Check your authenticator app and try again.' }, 400, cors);

  await env.DB.prepare('UPDATE users SET totp_enabled = 1 WHERE id = ?').bind(userId).run();

  const ip = request.headers.get('cf-connecting-ip') || 'unknown';
  await auditLog(env, user, 'enable_2fa', userId, user.email, 'TOTP enabled', ip);

  return jsonRes({ message: '2FA enabled successfully' }, 200, cors);
}

async function handle2faDisable(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { password, code } = body;
  if (!password || !code) return jsonRes({ error: 'Required: password, code' }, 400, cors);

  const userId = user.user_id || user.id;
  const dbUser = await env.DB.prepare('SELECT password_hash, totp_secret FROM users WHERE id = ?').bind(userId).first();

  const passwordOk = await verifyPassword(password, dbUser.password_hash);
  if (!passwordOk) return jsonRes({ error: 'Invalid password' }, 401, cors);

  const codeOk = await verifyTotp(dbUser.totp_secret, code);
  if (!codeOk) return jsonRes({ error: 'Invalid 2FA code' }, 401, cors);

  await env.DB.prepare('UPDATE users SET totp_enabled = 0, totp_secret = NULL WHERE id = ?').bind(userId).run();

  const ip = request.headers.get('cf-connecting-ip') || 'unknown';
  await auditLog(env, user, 'disable_2fa', userId, user.email, 'TOTP disabled', ip);

  return jsonRes({ message: '2FA disabled' }, 200, cors);
}

async function handle2faVerifyLogin(request, env, cors, ip, ua, country) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { pending_token, code } = body;
  if (!pending_token || !code) return jsonRes({ error: 'Required: pending_token, code' }, 400, cors);

  // Cap TOTP guesses per pending token (IP-independent brute-force cap). Same
  // hardening as the accounts.* /login/2fa endpoint — kept in sync so the two
  // 2FA surfaces don't drift.
  // Resolve the pending row FIRST so the guess counter can be keyed on the ACCOUNT.
  // Keying it on the pending token was bypassable: whoever has the password mints a fresh
  // token whenever the counter fills, and each new token starts a new budget — so the
  // "5 guesses" cap never actually bounded anything, it just set the batch size. Since
  // 2026-07-31 the login limiter charges only WRONG passwords, so minting those tokens
  // became free too, taking a six-digit code from ~5 guesses/10 min to unlimited.
  // Resolving first also means an unknown token costs no budget, so it cannot be used to
  // lock a stranger out of their own second factor.
  // §EXPIRY-COMPARE: datetime() on both sides. The row is written with toISOString(),
  // so bare this gave a pending-2FA token stamped 10 minutes the rest of the UTC day —
  // a stolen password plus a captured pending token had hours, not minutes, of runway.
  const pending = await env.DB.prepare('SELECT * FROM totp_pending WHERE token = ? AND datetime(expires_at) > datetime("now")').bind(pending_token).first();
  if (!pending) return jsonRes({ error: 'Invalid or expired login attempt. Please log in again.' }, 401, cors);

  const rl2 = await checkRateLimit(env, 'tfa:u' + pending.user_id, 'api:2fa');
  if (!rl2.ok) {
    // Drop EVERY pending row for this account, not just this token: leaving the others
    // alive is exactly the minting loop this fix exists to close.
    await env.DB.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(pending.user_id).run();
    return jsonRes({ error: 'Too many attempts. Please log in again.' }, 429, cors);
  }

  const user = await env.DB.prepare('SELECT * FROM users WHERE id = ?').bind(pending.user_id).first();
  if (!user || !user.totp_secret) return jsonRes({ error: 'Invalid state' }, 400, cors);

  const valid = await verifyTotp(user.totp_secret, code);
  if (!valid) {
    await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)')
      .bind(user.id, ip, ua, country).run();
    return jsonRes({ error: 'Invalid 2FA code' }, 401, cors);
  }

  // Clean up pending
  await env.DB.prepare('DELETE FROM totp_pending WHERE token = ?').bind(pending_token).run();

  // Create real session
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId } = await createSession(env, user.id, ip, ua);

  const res = jsonRes({
    message: 'Login successful',
    user: { name: user.name, email: user.email, institution: user.institution, api_key: user.api_key },
    session: sessionId
  }, 200, cors);

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  return res;
}

async function handleDataExport(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  const userId = user.user_id || user.id;

  const profile = await env.DB.prepare('SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, is_vip, totp_enabled, newsletter_subscribed, created_at, last_login_at, login_count, download_count, total_bytes_downloaded FROM users WHERE id = ?').bind(userId).first();
  const logins = await env.DB.prepare('SELECT ip_address, user_agent, country, success, timestamp FROM login_history WHERE user_id = ? ORDER BY timestamp DESC').bind(userId).all();
  const downloads = await env.DB.prepare('SELECT ticker, version, endpoint, ip_address, bytes_served, timestamp FROM download_log WHERE user_id = ? ORDER BY timestamp DESC').bind(userId).all();

  return new Response(JSON.stringify({
    exported_at: new Date().toISOString(),
    profile,
    login_history: logins.results,
    download_history: downloads.results
  }, null, 2), {
    status: 200,
    headers: {
      ...cors,
      'Content-Type': 'application/json',
      'Content-Disposition': `attachment; filename="hfdatalibrary_data_${user.email}_${new Date().toISOString().slice(0,10)}.json"`
    }
  });
}

async function handleMyDownloadHistory(request, env, cors) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  const userId = user.user_id || user.id;
  const logs = await env.DB.prepare(
    'SELECT ticker, version, endpoint, bytes_served, timestamp FROM download_log WHERE user_id = ? ORDER BY timestamp DESC LIMIT 100'
  ).bind(userId).all();

  return jsonRes({ downloads: logs.results }, 200, cors);
}

async function handleDeleteAccount(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { password, confirm } = body;
  if (confirm !== 'DELETE') return jsonRes({ error: 'Type DELETE to confirm' }, 400, cors);
  if (!password) return jsonRes({ error: 'Password required' }, 400, cors);

  const userId = user.user_id || user.id;
  const dbUser = await env.DB.prepare('SELECT password_hash, email FROM users WHERE id = ?').bind(userId).first();
  const passwordOk = await verifyPassword(password, dbUser.password_hash);
  if (!passwordOk) return jsonRes({ error: 'Invalid password' }, 401, cors);

  // Delete all user data (personal info removed; anonymized counts remain in aggregated queries)
  // Same as the accounts.* twin: everything, not just sessions.
  await revokeAllUserCredentials(env, userId);
  await env.DB.prepare('DELETE FROM login_history WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM download_log WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM password_resets WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(userId).run();
  // sso_refresh_tokens, sso_codes and newsletter_prefs also FK->users; without clearing them the
  // users DELETE fails with a FOREIGN KEY constraint for any user who has logged in via the popup
  // (sso_codes/sso_refresh_tokens) or set newsletter prefs at registration. Surfaced live
  // 2026-07-20; the accounts.* handleAccountDelete was fixed the same way (commit e60005e).
  await env.DB.prepare('DELETE FROM sso_refresh_tokens WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM sso_codes WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM newsletter_prefs WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM users WHERE id = ?').bind(userId).run();

  // Notify admin
  try {
    await sendEmail(
      env,
      ADMIN_NOTIFY,
      `Account deleted: ${dbUser.email}`,
      // The address is user-chosen and the registration regex allows < > " and ' as long as
      // there is no whitespace, so the raw interpolation put markup into the owner's inbox.
      // The subject stays raw — Resend sends it as plain text, escaping it would print the
      // entities. The accounts.* twin already does this (htmlEncode in handleAccountDelete).
      `<p>User <strong>${escapeHtml(dbUser.email)}</strong> has self-deleted their account. All personal data has been removed from the database.</p>`
    );
  } catch (e) {}

  const res = jsonRes({ message: 'Account deleted. Goodbye.' }, 200, cors);
  res.headers.set('Set-Cookie', 'hfd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0');
  return res;
}

async function handleUpdateProfile(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const updates = [];
  const values = [];
  // Until 2026-07-31 these four fields were length-capped here and nothing else, which made
  // the Latin-only filter at registration decorative: register as "Harvard University", then
  // POST this endpoint once with markup in `institution`. handlePublicStats has no filter of
  // its own, so the string lands in the public /v1/stats JSON that stats.html concatenates
  // into innerHTML — arbitrary script on an unauthenticated page, planted by any account.
  // So: trim, cap the length, and require the charset. The typeof guard is not cosmetic
  // either — `{"name": 42}` threw on .length and surfaced as a 500.
  //
  // The charset check goes through latinOkOrUnchanged and NOT isLatinish directly. The
  // first version of this loop called isLatinish on every submitted field, and account.html
  // prefills all four inputs from the stored row and posts all four on every save, so a
  // Google user whose stored name is Chinese/Cyrillic/Arabic got
  // "Name must use English/Latin letters only." on the very save that was meant to fill in
  // their institution — profile_complete could never reach 1 and they could never download
  // again. Only a value that DIFFERS from what is already stored is a value the user is
  // introducing; the reasoning is written out at latinOkOrUnchanged. `user` comes from
  // getSessionUser, which SELECTs u.*, so user.name/institution/country/role are this
  // request's own read of the row — no extra query.
  //
  // Blank is written, not skipped. Skipping it (also from the first version) meant clearing
  // your institution answered "Saved." and kept the old value, and a save with all four
  // boxes empty produced nothing to update and fell through to the 400 below, where the
  // same request used to return 200. An empty string carries nothing to inject, so it never
  // needed the special case.
  for (const field of ['name', 'institution', 'country', 'role']) {
    if (body[field] === undefined) continue;
    const label = field.charAt(0).toUpperCase() + field.slice(1);
    if (typeof body[field] !== 'string') return jsonRes({ error: `${label} must be a string` }, 400, cors);
    const v = body[field].trim();
    if (v.length > (field === 'institution' ? 200 : 100)) return jsonRes({ error: `${label} too long` }, 400, cors);
    if (!latinOkOrUnchanged(v, user[field])) return jsonRes({ error: `${label} must use English/Latin letters only.` }, 400, cors);
    updates.push(`${field} = ?`); values.push(v);
  }
  if (body.newsletter_subscribed !== undefined) {
    updates.push('newsletter_subscribed = ?'); values.push(body.newsletter_subscribed ? 1 : 0);
  }

  if (updates.length === 0) return jsonRes({ error: 'No updates provided' }, 400, cors);

  const userId = user.user_id || user.id;
  values.push(userId);
  await env.DB.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`).bind(...values).run();

  // Check if profile is now complete
  const updatedUser = await env.DB.prepare('SELECT institution, country, role FROM users WHERE id = ?').bind(userId).first();
  if (updatedUser.institution && updatedUser.country && updatedUser.role) {
    await env.DB.prepare('UPDATE users SET profile_complete = 1 WHERE id = ?').bind(userId).run();
  }

  return jsonRes({ message: 'Profile updated' }, 200, cors);
}

async function handleChangePassword(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { current_password, new_password } = body;
  if (!current_password || !new_password) return jsonRes({ error: 'Required: current_password, new_password' }, 400, cors);

  const pw = checkPasswordStrength(new_password);
  if (!pw.ok) return jsonRes({ error: pw.error }, 400, cors);

  const userId = user.user_id || user.id;
  const dbUser = await env.DB.prepare('SELECT password_hash FROM users WHERE id = ?').bind(userId).first();

  const ok = await verifyPassword(current_password, dbUser.password_hash);
  if (!ok) return jsonRes({ error: 'Current password is incorrect' }, 401, cors);

  const newHash = await hashPassword(new_password);
  await env.DB.prepare('UPDATE users SET password_hash = ? WHERE id = ?').bind(newHash, userId).run();

  // Kill all OTHER sessions as a security measure, PRESERVING the current one.
  // user.session_id is the real session id (exposed by the §7 getSessionUser
  // rewrite); user.id is the USER id. This handler is session-only
  // (getSessionUser above returns 401 otherwise), so session_id is always
  // present. Previously this bound user.id here, and since sessions.id is a TEXT
  // uuid never equal to the integer user id, `id != <userId>` matched every row
  // and logged the user out on their current device too.
  const currentSession = user.session_id;

  // …and `sessions` was ALL this deleted until 2026-07-31, while the response below told
  // the user their other devices were logged out. They were not. The family SSO refresh
  // chain lives in `sso_refresh_tokens`, and handleTokenRefresh validates against that
  // table alone — so a stolen `ekd_rt` kept minting 15-minute access tokens for
  // hfdatalibrary and econdatalibrary for another 30 days, no matter how many times the
  // password changed. An unclicked reset link mailed earlier could also hand the password
  // straight back. Somebody who changes their password because they think they are
  // compromised was told the problem was solved while it wasn't.
  //
  // Opts, and why the other three are deliberately absent: this is the ordinary case. The
  // caller typed the current password correctly six lines up, so they are the account
  // owner, not someone taking the account off a squatter.
  //  - keepSessionId — preserves the behaviour above: their own tab stays signed in.
  //  - no rotateApiKey — that key is pasted into their notebooks and cron jobs. Killing it
  //    on a routine password change is a support ticket, not remediation. The account page
  //    has a deliberate regenerate button. handleReset does not rotate either, by the same
  //    reasoning plus an explicit decision — see the comment there; the only caller that
  //    passes rotateApiKey is the Google account-transfer.
  //  - no clearForeignIdentities — the Google/ORCID iD on this row is the owner's own. It
  //    is cleared only in the Google account-transfer, where the previous holder may have
  //    planted it. Clearing it here would break the sign-in button they normally use.
  //  - no clearTotp — it is their authenticator. Silently disabling someone's 2FA because
  //    they changed a password is a downgrade they never asked for.
  await revokeAllUserCredentials(env, userId, { keepSessionId: currentSession });

  return jsonRes({ message: 'Password changed. Other sessions have been logged out.' }, 200, cors);
}

async function handleRegenerateKey(request, env, cors) {
  // Require session auth (not API key, since user's current key may be expired)
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required. Please log in.' }, 401, cors);

  const userId = user.user_id || user.id;
  const newKey = 'hfd_' + generateId();
  const newExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();

  await env.DB.prepare('UPDATE users SET api_key = ?, api_key_expires_at = ? WHERE id = ?')
    .bind(newKey, newExpires, userId).run();

  return jsonRes({
    message: 'API key regenerated',
    api_key: newKey,
    api_key_expires_at: newExpires
  }, 200, cors);
}

async function handleVerifyEmail(request, env, cors) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { token } = body;
  if (!token) return jsonRes({ error: 'Required: token' }, 400, cors);

  // §EXPIRY-COMPARE: datetime() on both sides. Written with toISOString(), so a
  // verification link stamped 24 hours was honoured for up to 48.
  const reset = await env.DB.prepare(
    'SELECT * FROM password_resets WHERE token = ? AND used = 0 AND datetime(expires_at) > datetime("now")'
  ).bind(token).first();

  if (!reset) return jsonRes({ error: 'Invalid or expired verification link' }, 400, cors);

  await env.DB.prepare('UPDATE users SET email_verified = 1 WHERE id = ?').bind(reset.user_id).run();
  await env.DB.prepare('UPDATE password_resets SET used = 1 WHERE id = ?').bind(reset.id).run();

  return jsonRes({ message: 'Email verified! You can now download data.' }, 200, cors);
}

async function handleResendVerification(request, env, cors) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Not authenticated' }, 401, cors);
  if (user.email_verified) return jsonRes({ message: 'Email already verified' }, 200, cors);

  const userId = user.user_id || user.id;
  // Throttled per ACCOUNT, and only now that we know a mail is actually going to be sent.
  // Every call sends real email and writes a password_resets row, and until a button existed
  // nobody found it — so it had no limit at all. Keyed on the user rather than the IP because
  // the request is authenticated: a shared campus address must not exhaust anyone else's
  // allowance, and the thing worth bounding is one account's mail volume. Three an hour is
  // far more than a person who has lost one email needs, and far less than a useful relay.
  const rlv = await checkRateLimit(env, 'resend:u' + userId, 'api:resend');
  if (!rlv.ok) {
    return jsonRes({
      error: 'We have already sent several verification emails recently. Please check your inbox and spam folder, then try again a little later.'
    }, 429, cors);
  }
  const verifyToken = generateId();
  const verifyExpires = new Date(Date.now() + 86400000).toISOString();
  await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)')
    .bind(userId, verifyToken, verifyExpires).run();
  await sendEmail(env, user.email, 'Verify your ElkassabgiData account', verificationEmail(user.name, verifyToken), FROM_EMAIL, 'ElkassabgiData');

  return jsonRes({ message: 'Verification email sent. Check your inbox.' }, 200, cors);
}

async function handleResetRequest(request, env, cors) {
  const ip = request.headers.get('cf-connecting-ip') || 'unknown';
  // PEEK, do not charge yet — the same shape handleLogin uses so that only a real event costs.
  //
  // This charged an attempt at the top, BEFORE the body was parsed and before anyone knew
  // whether the address even exists. On a shared egress — a university, a department, a
  // conference wifi — three colleagues resetting in an hour exhausted the budget and the fourth
  // was refused, which is an availability bug wearing a security badge. It is the identical
  // lockout that had `api:register` raised from 3 to 25 on 2026-07-31; this rule was left
  // behind at 3, and a typo'd address or a double-click spent one of them.
  const peek = await checkRateLimit(env, rlIpKey(ip), 'api:reset', { charge: false });
  if (!peek.ok) return rateLimitResponse(peek.retryAfter, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { email } = body;
  if (!email) return jsonRes({ error: 'Required: email' }, 400, cors);

  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  // Always return success to avoid email enumeration
  if (user) {
    // A PER-ACCOUNT cap as well as the per-IP one. The IP budget bounds how much one network can
    // do; it does nothing to stop a mailbox being flooded from many addresses, which is the
    // abuse that actually reaches a person. Keyed on user id, mirroring 'api:2fa' — and checked
    // only inside the `user` branch, so an unknown address can never spend a real account's
    // budget, and the response stays identical either way so nothing is enumerable.
    const perAcct = await checkRateLimit(env, 'rst:u' + user.id, 'api:reset_acct');
    if (perAcct.ok) {
      // Charge the IP only now, when a mail is actually going out. A request that produced no
      // email cost nothing, which is what makes the raised cap safe.
      await checkRateLimit(env, rlIpKey(ip), 'api:reset');
      const token = generateId();
      const expires = new Date(Date.now() + 3600000).toISOString(); // 1 hour
      await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)')
        .bind(user.id, token, expires).run();
      const u = await env.DB.prepare('SELECT name FROM users WHERE id = ?').bind(user.id).first();
      await sendEmail(env, email.toLowerCase(), 'Reset your HF Data Library password', resetEmail(u.name, token));
    }
    // Over the per-account cap: silently skip the send. Saying so would confirm the address is
    // registered, which is exactly what the constant-response above exists to hide.
  }

  return jsonRes({ message: 'If that email is registered, a reset link has been sent.' }, 200, cors);
}

async function handleReset(request, env, cors) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { token, password } = body;
  if (!token || !password) return jsonRes({ error: 'Required: token, password' }, 400, cors);
  const pw = checkPasswordStrength(password);
  if (!pw.ok) return jsonRes({ error: pw.error }, 400, cors);

  // §EXPIRY-COMPARE: datetime() on both sides. Written with toISOString() one line
  // below the comment that says "1 hour", and the reset email says one hour too —
  // but bare, the link stayed redeemable until midnight UTC, up to a 24× widening of
  // the window an intercepted reset mail can be used to take the account over.
  const reset = await env.DB.prepare(
    'SELECT * FROM password_resets WHERE token = ? AND used = 0 AND datetime(expires_at) > datetime("now")'
  ).bind(token).first();

  if (!reset) return jsonRes({ error: 'Invalid or expired reset token' }, 400, cors);

  const passwordHash = await hashPassword(password);
  await env.DB.prepare('UPDATE users SET password_hash = ? WHERE id = ?').bind(passwordHash, reset.user_id).run();
  await env.DB.prepare('UPDATE password_resets SET used = 1 WHERE id = ?').bind(reset.id).run();

  // Account recovery must actually recover the account. Until 2026-07-31 this deleted
  // `sessions` and nothing else, so someone who had taken the account over kept it:
  // the family SSO refresh chain lived another 30 days and went on minting access tokens
  // for every site in the family. Everything that can act as the user dies here —
  // every session, the SSO refresh chain, unspent SSO codes, unspent download tokens,
  // any half-finished 2FA login, and every other reset link that was still outstanding.
  //
  // With ONE deliberate exception: no rotateApiKey. Ahmed's call, and it is a trade, not
  // an oversight. Researchers paste that key into notebooks and cron jobs; rotating it on
  // a password reset breaks running work silently, at the exact moment the user is already
  // struggling to get back in, and there is no error message anywhere that would tell them
  // their key changed. The cost is that a key stolen before the reset keeps working until
  // it expires (API_KEY_DAYS) — the remedy is the Regenerate key button on the account
  // page (POST /v1/auth/regenerate-key), which the user chooses knowingly and can schedule
  // around their scripts. The Google account-transfer path still rotates, because there the
  // account is being taken back off a squatter whose key must not survive the handover and
  // no legitimate script can exist on an account its rightful owner has never used.
  await revokeAllUserCredentials(env, reset.user_id);

  return jsonRes({ message: 'Password reset successful. Please log in.' }, 200, cors);
}

// ── Credential invalidation, in ONE place ─────────────────────────────────────
// Before 2026-07-31 each recovery path invalidated a DIFFERENT subset: password reset
// deleted `sessions` only, the Google account-transfer deleted sessions and rotated the API
// key, admin deactivation deleted nothing. "Signed out everywhere" was never true.
//
// The family SSO refresh chain was the worst of the gaps. It survives 30 days and mints
// fresh access tokens for hfdatalibrary, econdatalibrary and every future family site, so
// an attacker retained the victim's account for a month after the victim "recovered" it.
//
// Anything that can act AS the user is revoked here. Every table below was checked against
// api/migrations + schema*.sql for a user_id column before this was written.
async function revokeAllUserCredentials(env, userId, opts) {
  const o = opts || {};
  const db = env.DB;
  // `keepSessionId` exists for the two change-password handlers only. There the person
  // typing the new password has just proved they know the old one, so they are the owner —
  // throwing them out of the tab they are standing in turns routine hygiene into what looks
  // like a failed request. Every OTHER session still dies. Pass the value from
  // getSessionUser/getIdpSessionUser's `session_id`, which is sessions.id (a TEXT id, never
  // equal to the integer user id — binding the user id here matches every row and signs the
  // caller out too). The recovery paths — reset, hostile handover, admin deactivate — pass
  // nothing and lose every session, which is the point of them.
  if (o.keepSessionId) {
    await db.prepare('DELETE FROM sessions WHERE user_id = ? AND id != ?').bind(userId, o.keepSessionId).run();
  } else {
    await db.prepare('DELETE FROM sessions WHERE user_id = ?').bind(userId).run();
  }
  await db.prepare('UPDATE sso_refresh_tokens SET revoked = 1 WHERE user_id = ? AND revoked = 0').bind(userId).run();
  await db.prepare('UPDATE sso_codes SET used = 1 WHERE user_id = ? AND used = 0').bind(userId).run();
  await db.prepare('UPDATE download_tokens SET used = 1 WHERE user_id = ? AND used = 0').bind(userId).run();
  // A pending-2FA row is a half-finished login; leaving one alive hands over a 10-minute
  // re-entry window immediately after a takeover is supposedly remediated.
  await db.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(userId).run();
  // Burn OTHER outstanding reset/verification links. Only the row this caller consumed was
  // ever marked used, so a second link mailed earlier stayed live and could undo the reset.
  //
  // §VERIFY-SURVIVES. password_resets stores TWO kinds of token and has no column that says
  // which (schema_live_20260717.sql:62-70): password-reset links, written +1 h by
  // handleResetRequest — the only writer of that kind — and email-VERIFICATION links,
  // written +24 h by handleRegister, handleResendVerification and their accounts.* twins.
  // Burning the lot killed a pending verification link every time the same person reset
  // their password: register, the verification mail lands in spam, forget the password
  // weeks later, reset it, then find the old mail and click it — "Invalid or expired
  // verification link", and every download route above refuses an unverified account, so
  // downloads stay refused with no button anywhere on hfdatalibrary.com to mail a fresh
  // link (the only resend control lives on accounts.elkassabgidata.com/account). This
  // function does not exist on origin/main, so that would have been new in production.
  //
  // The carve-out is the narrowest one possible without a migration, and it only ever does
  // LESS than the line it replaces, so it cannot break a flow that works today:
  //   * Only for an account that is still UNVERIFIED. A verified account has no use for a
  //     pending verification link, so nothing is spared there and the burn is unchanged for
  //     537 of the 572 accounts.
  //   * Only for rows that cannot be a live reset link. A reset row is written +1 h, so at
  //     every instant of its life its expires_at is less than an hour away; a row expiring
  //     more than two hours out is a verification row, with an hour of margin for clock
  //     skew between the Worker's Date.now() (the writer) and D1's datetime('now') (the
  //     reader). datetime() on the column because these expiries are toISOString() strings
  //     with a 'T' and a 'Z', which do not text-compare against SQLite's spelling —
  //     see §EXPIRY-COMPARE at handleReset and handleVerifyEmail.
  //   * If the users row cannot be read at all we fall through to the old unconditional
  //     burn rather than guessing, so a lookup failure loses no security.
  //
  // A squatter who planted an account on someone else's address gains nothing: the
  // verification link is mailed to the address ON the account, i.e. to the victim, and the
  // squatter never sees it. Nor does the carve-out reach the hostile-handover path — the
  // Google account-transfer sets email_verified = 1 in the UPDATE immediately before it
  // calls this, so that caller still burns every outstanding row exactly as it did before.
  const verifiedRow = await db.prepare('SELECT email_verified FROM users WHERE id = ?').bind(userId).first();
  if (verifiedRow && !verifiedRow.email_verified) {
    await db.prepare(
      "UPDATE password_resets SET used = 1 WHERE user_id = ? AND used = 0 AND datetime(expires_at) <= datetime('now','+2 hours')"
    ).bind(userId).run();
  } else {
    await db.prepare('UPDATE password_resets SET used = 1 WHERE user_id = ? AND used = 0').bind(userId).run();
  }
  if (o.rotateApiKey) {
    const freshKey = 'hfd_' + generateId();
    const freshExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    await db.prepare('UPDATE users SET api_key = ?, api_key_expires_at = ? WHERE id = ?')
      .bind(freshKey, freshExpires, userId).run();
  }
  // Identity bindings that are themselves login credentials. Only for a hostile handover:
  // an ordinary password reset must NOT unlink the owner's own Google/ORCID.
  if (o.clearForeignIdentities) {
    await db.prepare('UPDATE users SET orcid_id = NULL, orcid_profile_json = NULL WHERE id = ?')
      .bind(userId).run();
  }
  // A TOTP secret enrolled by whoever held the account before the handover would lock the
  // rightful owner out of password login permanently, with no self-service recovery.
  if (o.clearTotp) {
    await db.prepare('UPDATE users SET totp_secret = NULL, totp_enabled = 0 WHERE id = ?')
      .bind(userId).run();
  }
}

// ══════════════════════════════════════
// ── Newsletter Handlers ──
// ══════════════════════════════════════

async function handleUnsubscribe(request, env, cors) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { token } = body;
  if (!token) return jsonRes({ error: 'Required: token' }, 400, cors);

  const user = await env.DB.prepare('SELECT id, name, email FROM users WHERE unsubscribe_token = ?').bind(token).first();
  if (!user) return jsonRes({ error: 'Invalid unsubscribe token' }, 400, cors);

  await env.DB.prepare('UPDATE users SET newsletter_subscribed = 0 WHERE id = ?').bind(user.id).run();
  return jsonRes({ message: `${user.email} has been unsubscribed from the newsletter.` }, 200, cors);
}

async function handleToggleSubscribe(request, env, cors, subscribe) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Authentication required' }, 401, cors);

  const userId = user.user_id || user.id;
  await env.DB.prepare('UPDATE users SET newsletter_subscribed = ? WHERE id = ?').bind(subscribe ? 1 : 0, userId).run();
  return jsonRes({ message: subscribe ? 'Subscribed to newsletter' : 'Unsubscribed from newsletter', newsletter_subscribed: subscribe }, 200, cors);
}

// `bodyHtml` is deliberately raw — it is the campaign the admin composed in the console, and
// escaping it would print the markup instead of rendering it. `userName` is not: it is the
// recipient's own profile string, so it gets the same escapeHtml treatment as every other
// user-written value that reaches an outbound template.
function buildNewsletterHtml(subject, bodyHtml, userName, unsubscribeUrl) {
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <div style="background: #1a2332; padding: 1.5rem; text-align: center;">
        <h1 style="color: #d4a843; margin: 0; font-size: 1.5rem;">HF Data Library</h1>
      </div>
      <div style="padding: 2rem 1.5rem;">
        <p>Hi ${escapeHtml(userName)},</p>
        ${bodyHtml}
      </div>
      <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 1rem 0;">
      <div style="padding: 1rem 1.5rem; text-align: center; font-size: 0.8rem; color: #9ca3af;">
        <p>HF Data Library — Ahmed Elkassabgi, University of Central Arkansas</p>
        <p><a href="https://hfdatalibrary.com" style="color: #2563eb;">hfdatalibrary.com</a> · <a href="${unsubscribeUrl}" style="color: #9ca3af;">Unsubscribe</a></p>
      </div>
    </div>`;
}

async function handleSendNewsletter(request, env, cors) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { subject, body_html, test_only } = body;
  if (!subject || !body_html) return jsonRes({ error: 'Required: subject, body_html' }, 400, cors);

  // Session only, matching handleAdmin — this handler's sole route is
  // POST /v1/admin/newsletter, so an is_admin check satisfied by a bare API key
  // would have been a second door into the admin plane (one that mails every
  // verified subscriber) if the dispatch above ever gained another entry point.
  const user = await getSessionUser(request, env);
  if (!user || !user.is_admin) return jsonRes({ error: 'Admin access required' }, 403, cors);

  // Test mode: send only to the admin
  if (test_only) {
    const unsubUrl = `${SITE_URL}/pages/unsubscribe?token=${user.unsubscribe_token || 'test'}`;
    const html = buildNewsletterHtml(subject, body_html, user.name, unsubUrl);
    const ok = await sendEmail(env, user.email, '[TEST] ' + subject, html, NEWSLETTER_FROM, NEWSLETTER_FROM_NAME);
    return jsonRes({ message: ok ? 'Test email sent to ' + user.email : 'Failed to send test email' }, ok ? 200 : 500, cors);
  }

  // Get all active subscribers
  const subscribers = await env.DB.prepare(
    'SELECT id, name, email, unsubscribe_token FROM users WHERE newsletter_subscribed = 1 AND is_active = 1 AND email_verified = 1'
  ).all();

  const total = subscribers.results.length;
  let success = 0, failed = 0;

  // Send via Resend's batch endpoint, 50 per call. Per-subscriber fetches
  // exceed the Workers subrequest cap and Resend's rate limit on real list
  // sizes — that combination is what 500'd the first campaign send.
  const BATCH_SIZE = 50;
  const payloads = subscribers.results.map(sub => ({
    from: `${NEWSLETTER_FROM_NAME} <${NEWSLETTER_FROM}>`,
    to: [sub.email],
    subject,
    html: buildNewsletterHtml(subject, body_html, sub.name,
      `${SITE_URL}/pages/unsubscribe?token=${sub.unsubscribe_token}`)
  }));

  try {
    for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
      const chunk = payloads.slice(i, i + BATCH_SIZE);
      const r = await sendEmailBatch(env, chunk);
      success += r.success;
      failed += r.failed;
      if (i + BATCH_SIZE < payloads.length) {
        await new Promise(res => setTimeout(res, 600));  // stay under Resend req/s limit
      }
    }
  } catch (e) {
    // Never let a mid-send exception produce an opaque 500 — record what we know
    console.error('Newsletter send aborted mid-stream:', e);
    failed = total - success;
  }

  // Emails are already delivered at this point — recording the campaign must
  // never turn a successful send into a 500.
  let historyRecorded = true;
  try {
    const userId = user.user_id || user.id || null;
    await env.DB.prepare(
      'INSERT INTO newsletter_campaigns (subject, body_html, sent_by_user_id, recipients_count, success_count, failed_count) VALUES (?, ?, ?, ?, ?, ?)'
    ).bind(subject, body_html, userId, total, success, failed).run();

    const ip = request.headers.get('cf-connecting-ip') || 'unknown';
    await auditLog(env, user, 'send_newsletter', null, null, `${total} recipients, subject: ${subject}`, ip);
  } catch (e) {
    console.error('Campaign history insert failed:', e);
    historyRecorded = false;
  }

  return jsonRes({ message: 'Newsletter sent', total, success, failed, history_recorded: historyRecorded }, 200, cors);
}

// ══════════════════════════════════════
// ── Data Handlers ──
// ══════════════════════════════════════

async function handleSymbols(env, cors) {
  let allObjects = [];
  let cursor = undefined;
  do {
    const opts = { prefix: 'clean/', limit: 1000 };
    if (cursor) opts.cursor = cursor;
    const list = await env.DATA_BUCKET.list(opts);
    allObjects = allObjects.concat(list.objects);
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  const symbols = allObjects
    .filter(o => o.key.endsWith('.parquet'))
    // Only top-level 1-minute files: clean/{ticker}.parquet. Skip nested
    // timeframe dirs (clean/5min/{ticker}.parquet, etc.) which would otherwise
    // appear as bogus "5min/AAPL" tickers and inflate the count 8x.
    .filter(o => !o.key.slice('clean/'.length).includes('/'))
    .map(o => ({ ticker: o.key.slice('clean/'.length).replace('.parquet', ''), size_bytes: o.size, last_modified: o.uploaded }))
    .sort((a, b) => a.ticker.localeCompare(b.ticker));
  return jsonRes({ count: symbols.length, symbols }, 200, cors);
}

async function handleSymbolInfo(ticker, env, cors) {
  const info = { ticker, versions: {} };
  for (const v of ['raw', 'clean']) {
    const obj = await env.DATA_BUCKET.head(`${v}/${ticker}.parquet`);
    if (obj) info.versions[v] = { size_bytes: obj.size, last_modified: obj.uploaded };
  }
  if (Object.keys(info.versions).length === 0) return jsonRes({ error: `Ticker '${ticker}' not found` }, 404, cors);
  return jsonRes(info, 200, cors);
}

async function handleBars(ticker, request, env, cors, ip) {
  const user = await requireDataAuth(request, env);
  if (!user) return jsonRes({ error: await explainAuthFailure(request, env) }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data. Check your inbox.' }, 403, cors);
  if (user.profile_complete === 0) return jsonRes({ error: 'Please complete your profile (institution, country, role) before downloading.' }, 403, cors);

  // Per-user rate limit
  const rl = await checkRateLimit(env, String(user.id), 'api:download');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  const version = new URL(request.url).searchParams.get('version') || 'clean';
  if (!['raw', 'clean'].includes(version)) return jsonRes({ error: 'Invalid version. Use: raw or clean' }, 400, cors);

  const obj = await env.DATA_BUCKET.get(`${version}/${ticker}.parquet`);
  if (!obj) return jsonRes({ error: `Ticker '${ticker}' not found in ${version}` }, 404, cors);

  const userId = user.user_id || user.id;
  await env.DB.prepare('UPDATE users SET download_count = download_count + 1, total_bytes_downloaded = total_bytes_downloaded + ? WHERE id = ?')
    .bind(obj.size, userId).run();
  // Best-effort logging — must never block the download itself.
  const channel = new URL(request.url).searchParams.get('via') === 'mcp' ? 'mcp' : 'api';
  try {
    await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, channel, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .bind(userId, user.api_key, ticker, version, '/v1/bars', channel, ip, obj.size).run();
  } catch (e) { console.error('download_log insert failed:', e.message); }

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${ticker}_${version}.parquet"`, 'Content-Length': obj.size, 'X-Data-Attribution': 'Data provided for free by IEX (post-March-2022 bars). Terms: https://www.iex.io/legal/hist-data-terms' }
  });
}

// Serve a derived per-ticker dataset: kind ∈ {'variables','quality'}.
// R2 key {version}/{kind}/{ticker}.parquet. Same auth/rate-limit/logging as /v1/bars.
async function handleDerived(ticker, kind, request, env, cors, ip) {
  const user = await requireDataAuth(request, env);
  if (!user) return jsonRes({ error: await explainAuthFailure(request, env) }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data.' }, 403, cors);
  if (user.profile_complete === 0) return jsonRes({ error: 'Please complete your profile (institution, country, role) before downloading.' }, 403, cors);

  const rl = await checkRateLimit(env, String(user.id), 'api:download');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  const version = new URL(request.url).searchParams.get('version') || 'clean';
  if (!['raw', 'clean'].includes(version)) return jsonRes({ error: 'Invalid version. Use: raw or clean' }, 400, cors);

  const obj = await env.DATA_BUCKET.get(`${version}/${kind}/${ticker}.parquet`);
  if (!obj) return jsonRes({ error: `${kind} for '${ticker}' (${version}) not available yet` }, 404, cors);

  const userId = user.user_id || user.id;
  await env.DB.prepare('UPDATE users SET download_count = download_count + 1, total_bytes_downloaded = total_bytes_downloaded + ? WHERE id = ?')
    .bind(obj.size, userId).run();
  // Best-effort logging — must never block the download itself.
  const channel = new URL(request.url).searchParams.get('via') === 'mcp' ? 'mcp' : 'api';
  try {
    await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, channel, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .bind(userId, user.api_key, ticker, version, `/v1/${kind}`, channel, ip, obj.size).run();
  } catch (e) { console.error('download_log insert failed:', e.message); }

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${ticker}_${version}_${kind}.parquet"`, 'Content-Length': obj.size, 'X-Data-Attribution': 'Data provided for free by IEX (post-March-2022 bars). Terms: https://www.iex.io/legal/hist-data-terms' }
  });
}

const VALID_TIMEFRAMES = ['1min', '5min', '15min', '30min', 'hourly', 'daily', 'weekly', 'monthly'];

async function handleDownloadToken(ticker, request, env, cors) {
  // Auth two-step (instead of requireAuth) so the CHANNEL the token was issued
  // through is known: a browser session -> 'web', an X-API-Key -> 'api'. The
  // signed-token flow is also the documented API path for non-1min timeframes
  // and CSV, so token presence alone does NOT imply a website download.
  let user = await getSessionUser(request, env);
  let issuedVia = 'web';
  // allowQueryKey: this is a data route and reached the same way downloads are,
  // so it keeps the ?api_key= form requireDataAuth keeps (see getUserByApiKey).
  if (!user) { user = await getUserByApiKey(request, env, { allowQueryKey: true }); issuedVia = 'api'; }
  if (!user) { user = await validateFamilyToken(request, env); if (user) issuedVia = 'family'; }
  if (!user) return jsonRes({ error: await explainAuthFailure(request, env) }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data.' }, 403, cors);
  if (user.profile_complete === 0) return jsonRes({ error: 'Please complete your profile (institution, country, role) before downloading.' }, 403, cors);

  const url = new URL(request.url);
  if (url.searchParams.get('via') === 'mcp') issuedVia = 'mcp';
  const version = url.searchParams.get('version') || 'clean';
  const format = (url.searchParams.get('format') || 'parquet').toLowerCase();
  const timeframe = url.searchParams.get('timeframe') || '1min';

  if (!['raw', 'clean'].includes(version)) return jsonRes({ error: 'Invalid version. Use: raw or clean' }, 400, cors);
  if (!['parquet', 'csv'].includes(format)) return jsonRes({ error: 'Invalid format. Use: parquet or csv' }, 400, cors);
  if (!VALID_TIMEFRAMES.includes(timeframe)) return jsonRes({ error: 'Invalid timeframe. Use: ' + VALID_TIMEFRAMES.join(', ') }, 400, cors);

  const token = generateId();
  const expires = new Date(Date.now() + 10 * 60 * 1000).toISOString(); // 10 minutes
  const userId = user.user_id || user.id;

  // Encode timeframe in version field (e.g. "clean|5min")
  const versionTf = `${version}|${timeframe}`;
  await env.DB.prepare(
    'INSERT INTO download_tokens (token, user_id, ticker, version, format, channel, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
  ).bind(token, userId, ticker, versionTf, format, issuedVia, expires).run();

  return jsonRes({
    url: `https://api.hfdatalibrary.com/v1/download/${ticker}?token=${token}`,
    expires_at: expires,
    version, timeframe, format
  }, 200, cors);
}

async function handleDownload(ticker, request, env, cors, ip) {
  const url = new URL(request.url);
  const downloadToken = url.searchParams.get('token');

  // Accept EITHER: signed download token OR regular auth
  let user = null;
  let tokenRecord = null;

  if (downloadToken) {
    // §EXPIRY-COMPARE: datetime() on both sides. This is the worst of the bare
    // compares: the token is stamped 10 minutes, travels inside the URL, and nothing
    // else re-authenticates here — the user is resolved from the token row. Bare, a
    // link sitting in browser history, a proxy log or a pasted chat message stayed
    // redeemable for the rest of the UTC day.
    tokenRecord = await env.DB.prepare(
      'SELECT * FROM download_tokens WHERE token = ? AND used = 0 AND datetime(expires_at) > datetime("now")'
    ).bind(downloadToken).first();
    if (!tokenRecord) return jsonRes({ error: 'Invalid or expired download link. Please request a new download.' }, 401, cors);
    if (tokenRecord.ticker !== ticker) return jsonRes({ error: 'Token does not match ticker' }, 400, cors);
    user = await env.DB.prepare('SELECT * FROM users WHERE id = ? AND is_active = 1').bind(tokenRecord.user_id).first();
  } else {
    // No signed token: a full session/api_key OR a family edl_at (data route).
    user = await requireDataAuth(request, env);
  }

  if (!user) return jsonRes({ error: await explainAuthFailure(request, env) }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data.' }, 403, cors);
  if (user.profile_complete === 0) return jsonRes({ error: 'Please complete your profile (institution, country, role) before downloading.' }, 403, cors);

  // Per-user download rate limit
  const rl = await checkRateLimit(env, String(user.id), 'api:download');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  // Use token's version/format if provided, otherwise query params
  // Token's version field may be encoded as "version|timeframe"
  let version, timeframe;
  if (tokenRecord) {
    const parts = tokenRecord.version.split('|');
    version = parts[0];
    timeframe = parts[1] || '1min';
  } else {
    version = url.searchParams.get('version') || 'clean';
    timeframe = url.searchParams.get('timeframe') || '1min';
    // Validate the DIRECT branch with the same whitelists the token branch was validated by.
    //
    // handleDownloadToken checks all three inputs before it will mint a link — version against
    // ['raw','clean'], format against ['parquet','csv'], timeframe against VALID_TIMEFRAMES —
    // so anything arriving via a token is already known-good. This branch took the same three
    // values straight from the query string and interpolated two of them into the R2 key
    // (`${version}/${ticker}.parquet`, `${version}/${timeframe}/${ticker}.parquet`, and the csv/
    // equivalents). Same bucket, same key template, one path checked and the other not.
    //
    // The consequence is not path traversal — R2 keys are opaque strings and `ticker` is already
    // constrained to [A-Z0-9.-] by the route regex, so no `/` can be injected through it — it is
    // that an authenticated caller chose an arbitrary key PREFIX and could address objects the
    // catalogue never offers, by asking for a version or timeframe that is not a real one.
    //
    // Rejecting rather than silently coercing: a caller who asks for something that does not
    // exist should be told, not quietly handed a different file than the one requested.
    if (!['raw', 'clean'].includes(version)) {
      return jsonRes({ error: "Invalid version. Use: raw, clean" }, 400, cors);
    }
    if (!VALID_TIMEFRAMES.includes(timeframe)) {
      return jsonRes({ error: 'Invalid timeframe. Use: ' + VALID_TIMEFRAMES.join(', ') }, 400, cors);
    }
  }
  const format = tokenRecord ? tokenRecord.format : ((url.searchParams.get('format') || 'parquet').toLowerCase());
  // format selects between two fixed key templates rather than being interpolated, so it cannot
  // shape a key — but an unknown value silently fell into the parquet branch and returned a
  // parquet file to someone who asked for something else. Same whitelist as the mint path.
  if (!tokenRecord && !['parquet', 'csv'].includes(format)) {
    return jsonRes({ error: "Invalid format. Use: parquet, csv" }, 400, cors);
  }

  // Mark token as used
  if (tokenRecord) {
    await env.DB.prepare('UPDATE download_tokens SET used = 1 WHERE token = ?').bind(downloadToken).run();
  }

  // R2 path layout:
  //   1-minute parquet:   {version}/{ticker}.parquet
  //   1-minute CSV:       csv/{version}/{ticker}.csv
  //   Aggregated parquet: {version}/{timeframe}/{ticker}.parquet  (e.g. clean/5min/AAPL.parquet)
  //   Aggregated CSV:     csv/{version}/{timeframe}/{ticker}.csv  (not yet generated)
  let key, filename, contentType;
  if (format === 'csv') {
    if (timeframe === '1min') {
      key = `csv/${version}/${ticker}.csv`;
    } else {
      key = `csv/${version}/${timeframe}/${ticker}.csv`;
    }
    filename = `${ticker}_${version}_${timeframe}.csv`;
    contentType = 'text/csv';
  } else {
    if (timeframe === '1min') {
      key = `${version}/${ticker}.parquet`;
    } else {
      key = `${version}/${timeframe}/${ticker}.parquet`;
    }
    filename = `${ticker}_${version}_${timeframe}.parquet`;
    contentType = 'application/octet-stream';
  }

  const obj = await env.DATA_BUCKET.get(key);
  if (!obj) {
    if (format === 'csv') return jsonRes({ error: `CSV for '${ticker}' (${version}) not yet available. Try format=parquet.` }, 404, cors);
    return jsonRes({ error: `Ticker '${ticker}' not found in ${version}` }, 404, cors);
  }

  const userId = user.user_id || user.id;
  await env.DB.prepare('UPDATE users SET download_count = download_count + 1, total_bytes_downloaded = total_bytes_downloaded + ? WHERE id = ?')
    .bind(obj.size, userId).run();
  // Channel: explicit via=mcp wins; otherwise the channel the token was ISSUED
  // through (web session vs API key — see handleDownloadToken); tokenless
  // authenticated calls are direct API. Legacy tokens (pre-channel column)
  // fall back to 'web'. Logging is best-effort: a schema/logging failure must
  // never block a download the user already earned (bytes are in hand).
  const channel = url.searchParams.get('via') === 'mcp' ? 'mcp'
    : (tokenRecord ? (tokenRecord.channel || 'web') : 'api');
  try {
    await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, channel, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .bind(userId, user.api_key, ticker, version, '/v1/download', channel, ip, obj.size).run();
  } catch (e) { console.error('download_log insert failed:', e.message); }

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': contentType, 'Content-Disposition': `attachment; filename="${filename}"`, 'Content-Length': obj.size, 'X-Data-Attribution': 'Data provided for free by IEX (post-March-2022 bars). Terms: https://www.iex.io/legal/hist-data-terms' }
  });
}

// ══════════════════════════════════════
// ── Admin Handlers ──
// ══════════════════════════════════════

async function handleAdmin(path, request, env, cors, ip) {
  // getSessionUser, deliberately NOT requireAuth. requireAuth falls through to
  // getUserByApiKey, so a bare data key was a complete console credential: no
  // password, no 2FA (handleLogin refuses to issue a session until a totp_enabled
  // user produces a code; the key path has no such step), and — until the change
  // above — accepted straight out of the query string. That key is not an
  // identity: handleSSO hands it to econdatalibrary.com and elkassabgidata.com in
  // a URL fragment, the account page tells people to paste it into scripts, and
  // it sits in plain text in every notebook that reads data. One copy of it read
  // every user's row including their own api_key (the list query below returns
  // it), set is_admin on a second account, or set is_active = 0 on the real
  // admin. The privilege ordering was inverted: renaming yourself, changing your
  // password, touching 2FA and regenerating your key all already demanded a
  // session and refused a key, while the highest-privilege surface accepted less.
  // Nothing legitimate breaks — admin.html has only ever sent
  // Authorization: Bearer <session id> (pages/admin.html, authHeaders()).
  const user = await getSessionUser(request, env);
  if (!user || !user.is_admin) {
    return jsonRes({ error: 'Admin access required' }, 403, cors);
  }
  // email_verified must gate PRIVILEGE, not just downloads. Setting email_verified = 0 for
  // admin registrations (2026-07-31) did not by itself close the console takeover it was
  // meant to close, because nothing on this path ever read the flag and handleLogin does
  // not read it either: an is_admin row could still simply log in with the password chosen
  // at registration and arrive here. Both halves are needed — the flag, and a check of it.
  if (!user.email_verified) {
    return jsonRes({ error: 'Verify your email address before using the admin console.' }, 403, cors);
  }

  // GET /v1/admin/audit — audit log
  if (path === '/v1/admin/audit') {
    const logs = await env.DB.prepare('SELECT * FROM admin_audit_log ORDER BY timestamp DESC LIMIT 200').all();
    return jsonRes({ audit_log: logs.results }, 200, cors);
  }

  // GET /v1/admin/users — list users (server-side search / sort / filter / paginate)
  if (path === '/v1/admin/users' && request.method === 'GET') {
    const url = new URL(request.url);
    const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '50') || 50, 500));
    const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0') || 0);
    const q = (url.searchParams.get('q') || '').trim();
    const filter = url.searchParams.get('filter') || '';   // vip|admin|revoked|active|flagged
    // Fair-use threshold in GB over the trailing 30 days. Parsed as a float so 0.5 works.
    //
    // The clamp is finite on BOTH ends, not just at zero. `Math.max(0, parseFloat(x) || 0)`
    // handles the obvious junk — "abc", "", "-10" and "NaN" all collapse to 0, i.e. no filter —
    // but it happily passes Infinity through: parseFloat('1e400') is Infinity, Infinity || 0 is
    // Infinity, and Math.max(0, Infinity) is Infinity. That then reaches D1 as a bound
    // parameter of Infinity, which is not a value SQLite has. `?min_gb30=1e400` is a one-word
    // query string, so the ceiling is not optional.
    //
    // MAX_GB30 is 9e6 GB (9 PB in a 30-day window — absurd by four orders of magnitude against
    // the real maximum of 1.1 TB, so it can never clip a genuine query). It is chosen so
    // MAX_GB30 * 1e9 = 9e15 stays under Number.MAX_SAFE_INTEGER (~9.007e15) and the bound value
    // is always an exact integer: without it, "9007199254740993" binds 9.007e24 as a float.
    // Unparseable input and a huge number are NOT the same request and must not get the same
    // answer. "abc" expresses no threshold, so it means no filter. "1e400" expresses a
    // threshold that simply nobody meets, so it must filter to NOTHING — rejecting it into
    // "no filter" would answer "who is over 10^400 GB?" with the entire user list, which is
    // the exact inversion of what was asked. Math.min clamps Infinity to the ceiling for free,
    // and `NaN > 0` is already false, so no isFinite test is needed to separate them.
    const MAX_GB30 = 9e6;
    const rawGb30 = parseFloat(url.searchParams.get('min_gb30') || '0');
    const minGb30 = rawGb30 > 0 ? Math.min(rawGb30, MAX_GB30) : 0;
    // Sort whitelist — never interpolate raw input into SQL.
    const SORT_COLS = {
      created_at: 'created_at', name: 'name COLLATE NOCASE', email: 'email COLLATE NOCASE',
      institution: 'institution COLLATE NOCASE', country: 'country COLLATE NOCASE',
      downloads: 'download_count', logins: 'login_count', last_login: 'last_login_at',
      // Fair use: trailing-30-day volume, the two columns this console is judged on.
      bytes_30d: 'bytes_30d', downloads_30d: 'downloads_30d',
    };
    // hasOwnProperty, not a bare lookup. SORT_COLS is an object literal, so it inherits from
    // Object.prototype and a bare `SORT_COLS[input] || default` is not actually a whitelist:
    // ?sort=constructor resolves to Object's constructor, ?sort=toString and ?sort=valueOf to
    // native functions, ?sort=__proto__ to an object — all truthy, so none fall through to the
    // default, and each is then string-interpolated straight into ORDER BY, producing e.g.
    // `ORDER BY function Object() { [native code] } DESC`. The comment one line above says
    // "never interpolate raw input into SQL", which is exactly what this did for four inputs.
    // Behind the admin gate, so the reachable damage is a 500 rather than injection — but a
    // whitelist that admits four keys nobody put in it is not a whitelist.
    const sortKey = url.searchParams.get('sort') || '';
    const sortCol = Object.prototype.hasOwnProperty.call(SORT_COLS, sortKey)
      ? SORT_COLS[sortKey]
      : SORT_COLS.created_at;
    const dir = (url.searchParams.get('dir') || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    // Abuse signals computed across ALL users (needed for row enrichment, the
    // 'flagged' filter, and the global alert-bar counts) — not just this page.
    const sharedRows = await env.DB.prepare(
      "SELECT last_login_ip AS ip, COUNT(*) AS c FROM users " +
      "WHERE last_login_ip IS NOT NULL AND last_login_ip != '' " +
      "GROUP BY last_login_ip HAVING c > 1"
    ).all();
    const sharedMap = {};
    for (const r of sharedRows.results) sharedMap[r.ip] = r.c;
    const allFlags = await env.DB.prepare('SELECT id, email, last_login_ip FROM users').all();
    const disposableIds = new Set(allFlags.results.filter(u => isDisposableEmail(u.email)).map(u => u.id));
    const sharedIds = new Set(allFlags.results.filter(u => u.last_login_ip && sharedMap[u.last_login_ip]).map(u => u.id));

    const where = [];
    const args = [];
    if (q) {
      where.push('(name LIKE ? OR email LIKE ? OR institution LIKE ? OR country LIKE ? OR role LIKE ?)');
      const like = '%' + q + '%';
      args.push(like, like, like, like, like);
    }
    if (filter === 'vip') where.push('is_vip = 1');
    else if (filter === 'admin') where.push('is_admin = 1');
    else if (filter === 'revoked') where.push('is_active = 0');
    else if (filter === 'active') where.push('is_active = 1');
    else if (filter === 'flagged') {
      // Inline the ids rather than binding them: D1 caps bound parameters at
      // ~100 per query, and the flagged set (shared university/VPN IPs +
      // disposable domains) can easily exceed that. Safe to inline — these are
      // DB-generated integer primary keys, coerced through Number().
      const ids = [...new Set([...disposableIds, ...sharedIds])]
        .map(Number).filter(Number.isInteger);
      where.push('id IN (' + (ids.length ? ids.join(',') : '-1') + ')');
    }
    // Fair-use filter. Pushed LAST so its bound parameter lands after the ones `q` pushed —
    // D1 binds positionally, and an arg appended out of order silently filters on the wrong
    // column rather than erroring. GB here is decimal (1e9), the convention for transfer
    // volume and the same base the console formats with, so the number typed into the box is
    // the number rendered in the column.
    if (minGb30 > 0) {
      where.push('COALESCE(d30.b, 0) >= ?');
      args.push(Math.round(minGb30 * 1e9));
    }
    const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';

    // Trailing-30-day download volume per user, as a CTE rather than a correlated subquery.
    // A correlated subquery would re-walk every row of the heaviest account (45,204 of them)
    // once per rendered row — up to 500 rows a page. This aggregates the window ONCE.
    //
    // Cost is measured, not assumed: ~368k rows read, ~82 ms, via a full scan on the existing
    // idx_download_log_user, which SQLite prefers because it returns rows already grouped by
    // user_id and so makes GROUP BY free. A purpose-built (timestamp, user_id, bytes_served)
    // index was built, forced with INDEXED BY, and measured SLOWER in rows read (381,577);
    // it was dropped by the 2026-08-01 migration, which records the numbers. The scan is
    // inherent to aggregating a window across every user — if this needs to get cheaper the
    // answer is a summary table on the existing 02:00 cron, not another index.
    //
    // LEFT JOIN because a user with no downloads in the window must still appear — at 0,
    // not missing.
    const D30_CTE =
      "WITH d30 AS (SELECT user_id, SUM(bytes_served) AS b, COUNT(*) AS c FROM download_log " +
      "WHERE timestamp > datetime('now','-30 days') GROUP BY user_id) ";
    const D30_JOIN = ' FROM users LEFT JOIN d30 ON d30.user_id = users.id ';

    // ip_country: geolocation (Cloudflare cf-ipcountry) of the user's last-login
    // IP, resolved from login_history — distinct from self-declared users.country.
    const users = await env.DB.prepare(
      D30_CTE +
      'SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, is_vip, newsletter_subscribed, created_at, last_login_at, last_login_ip, last_login_ua, login_count, download_count, total_bytes_downloaded, notes, ' +
      '(SELECT lh.country FROM login_history lh WHERE lh.ip_address = users.last_login_ip ' +
      'AND lh.country IS NOT NULL AND lh.country != "" AND lh.country != "unknown" ' +
      'ORDER BY lh.id DESC LIMIT 1) AS ip_country, ' +
      'COALESCE(d30.b, 0) AS bytes_30d, COALESCE(d30.c, 0) AS downloads_30d' +
      D30_JOIN + whereSql + ` ORDER BY ${sortCol} ${dir} LIMIT ? OFFSET ?`
    ).bind(...args, limit, offset).all();

    const totalAll = await env.DB.prepare('SELECT COUNT(*) as count FROM users').first();
    // The count carries the SAME cte + join as the page query. It has to: whereSql can now
    // contain a d30 predicate, and a count that quietly dropped the fair-use filter would
    // report "1 of 442 shown" while the pager believed there were 442 pages to walk.
    const totalMatch = whereSql
      ? await env.DB.prepare(D30_CTE + 'SELECT COUNT(*) as count' + D30_JOIN + whereSql).bind(...args).first()
      : totalAll;

    const usersOut = users.results.map(u => ({
      ...u,
      shared_ip: sharedIds.has(u.id),
      shared_ip_count: u.last_login_ip ? (sharedMap[u.last_login_ip] || 1) : 0,
      disposable_email: disposableIds.has(u.id),
    }));

    return jsonRes({
      total: totalAll.count,
      total_matching: totalMatch.count,
      limit, offset,
      users: usersOut,
      shared_ip_clusters: sharedRows.results.length,
      flagged_users: sharedIds.size,
      disposable_users: disposableIds.size,
      // Union of shared-IP + disposable — matches what the 'flagged' filter returns.
      flagged_total: new Set([...sharedIds, ...disposableIds]).size,
    }, 200, cors);
  }

  // GET /v1/admin/users/:id — single user detail
  const userDetailMatch = path.match(/^\/v1\/admin\/users\/(\d+)$/);
  if (userDetailMatch && request.method === 'GET') {
    const uid = parseInt(userDetailMatch[1]);
    const u = await env.DB.prepare('SELECT * FROM users WHERE id = ?').bind(uid).first();
    if (!u) return jsonRes({ error: 'User not found' }, 404, cors);

    // Same trailing-30-day window the list column ranks on. The detail panel is where a
    // fair-use case is actually decided, so it must show the figure that put the account on
    // the list — a panel showing only the all-time total would make a month-old 1 TB burst
    // and a steady 1 TB over two years look identical. Single user, so a direct aggregate is
    // correct here; the CTE above exists only because the list needs all of them at once.
    const vol30 = await env.DB.prepare(
      "SELECT COALESCE(SUM(bytes_served), 0) AS b, COUNT(*) AS c FROM download_log " +
      "WHERE user_id = ? AND timestamp > datetime('now','-30 days')"
    ).bind(uid).first();
    u.bytes_30d = vol30 ? vol30.b : 0;
    u.downloads_30d = vol30 ? vol30.c : 0;

    const logins = await env.DB.prepare(
      'SELECT * FROM login_history WHERE user_id = ? ORDER BY timestamp DESC LIMIT 20'
    ).bind(uid).all();

    const downloads = await env.DB.prepare(
      'SELECT * FROM download_log WHERE user_id = ? ORDER BY timestamp DESC LIMIT 50'
    ).bind(uid).all();

    return jsonRes({
      user: {
        id: u.id, name: u.name, email: u.email, institution: u.institution, country: u.country, role: u.role,
        api_key: u.api_key, is_active: u.is_active, is_admin: u.is_admin, is_vip: u.is_vip,
        newsletter_subscribed: u.newsletter_subscribed, totp_enabled: u.totp_enabled, profile_complete: u.profile_complete,
        orcid_id: u.orcid_id, google_id: u.google_id,
        orcid_profile: u.orcid_profile_json ? JSON.parse(u.orcid_profile_json) : null,
        created_at: u.created_at, last_login_at: u.last_login_at, last_login_ip: u.last_login_ip, last_login_ua: u.last_login_ua,
        login_count: u.login_count, download_count: u.download_count, total_bytes_downloaded: u.total_bytes_downloaded,
        notes: u.notes,
        hide_institution: u.hide_institution ? true : false,
        // Computed above, then dropped on the floor. This handler returns an explicit field
        // list rather than spreading `u`, so assigning u.bytes_30d / u.downloads_30d put them
        // on an object the response never serialises. The list branch DOES spread, which is
        // why the column worked and hid this: the panel rendered fmtVol30(undefined) as "-"
        // and "in 0 dl" for every account, including the 1.10 TB one. That is not a blank, it
        // is an affirmative statement of zero — on the one screen whose entire purpose is
        // deciding whether a volume warrants revoking, directly contradicting the row it was
        // opened from. The aggregate query was already running; only its result was lost.
        bytes_30d: u.bytes_30d,
        downloads_30d: u.downloads_30d
      },
      recent_logins: logins.results,
      recent_downloads: downloads.results
    }, 200, cors);
  }

  // PUT /v1/admin/users/:id — update user (activate/deactivate/notes)
  const userUpdateMatch = path.match(/^\/v1\/admin\/users\/(\d+)$/);
  if (userUpdateMatch && request.method === 'PUT') {
    const uid = parseInt(userUpdateMatch[1]);
    let body;
    try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

    const updates = [];
    const values = [];
    if (body.is_active !== undefined) { updates.push('is_active = ?'); values.push(body.is_active ? 1 : 0); }
    if (body.notes !== undefined) { updates.push('notes = ?'); values.push(body.notes); }
    if (body.is_admin !== undefined) { updates.push('is_admin = ?'); values.push(body.is_admin ? 1 : 0); }
    if (body.is_vip !== undefined) { updates.push('is_vip = ?'); values.push(body.is_vip ? 1 : 0); }
    if (body.hide_institution !== undefined) { updates.push('hide_institution = ?'); values.push(body.hide_institution ? 1 : 0); }
    // Profile fields — admin can correct typos / unify naming for stats display.
    // Each must pass the Latin-only check (same rule as /v1/auth/register).
    // Country is also normalized to ISO-2 if recognizable.
    for (const f of ['name', 'institution', 'country', 'role']) {
      if (typeof body[f] === 'string') {
        let v = body[f].trim();
        if (v.length === 0) continue; // skip empty (don't wipe field)
        if (!isLatinish(v)) {
          return jsonRes({ error: `${f} must use English/Latin letters only.` }, 400, cors);
        }
        if (f === 'country') v = normalizeCountry(v) || v;
        updates.push(`${f} = ?`);
        values.push(v);
      }
    }

    if (updates.length === 0) return jsonRes({ error: 'No updates provided' }, 400, cors);

    values.push(uid);
    await env.DB.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`).bind(...values).run();

    // If deactivating, cut off everything that can still act as them. Until 2026-07-31 this
    // deleted `sessions` and stopped, so deactivating an abusive or compromised account did
    // not deactivate it: the family SSO refresh chain in `sso_refresh_tokens` kept minting
    // access tokens for hfdatalibrary and econdatalibrary for up to 30 days, an unredeemed
    // sso_code was still exchangeable for a session, and any download token already issued
    // still resolved. The admin saw "Revoked" in the console and the user carried on.
    //
    // No opts. Deactivation is an admin judgement about the account and is reversible — it
    // is not a claim that the owner lost control of their credentials, so nothing that would
    // survive reactivation in a broken state is touched. The API key stays (and is inert
    // while it is off: every key lookup requires is_active = 1), the owner's own Google/ORCID
    // link stays, their authenticator stays. Reactivating gives back a working account.
    if (body.is_active === false || body.is_active === 0) {
      await revokeAllUserCredentials(env, uid);
    }

    // Audit log
    const target = await env.DB.prepare('SELECT email FROM users WHERE id = ?').bind(uid).first();
    const actions = Object.keys(body).filter(k => ['is_active','is_admin','is_vip','notes','hide_institution','name','institution','country','role'].includes(k));
    await auditLog(env, user, 'update_user:' + actions.join(','), uid, target?.email, JSON.stringify(body), ip);

    return jsonRes({ message: 'User updated' }, 200, cors);
  }

  // POST /v1/admin/digest/preview — fire daily activity digest on demand (testing)
  if (path === '/v1/admin/digest/preview' && request.method === 'POST') {
    const ok = await sendDailyDigest(env);
    await auditLog(env, user, 'digest:preview', null, null, null, ip);
    return jsonRes({ message: ok ? 'Daily digest sent.' : 'Digest failed — check worker logs.' }, ok ? 200 : 500, cors);
  }

  // GET /v1/admin/stats — dashboard stats
  if (path === '/v1/admin/stats') {
    const totalUsers = await env.DB.prepare('SELECT COUNT(*) as c FROM users').first();
    const activeUsers = await env.DB.prepare('SELECT COUNT(*) as c FROM users WHERE is_active = 1').first();
    const totalDownloads = await env.DB.prepare('SELECT COUNT(*) as c FROM download_log').first();
    const totalBytes = await env.DB.prepare('SELECT SUM(bytes_served) as s FROM download_log').first();
    const todayLogins = await env.DB.prepare("SELECT COUNT(*) as c FROM login_history WHERE timestamp > datetime('now', '-1 day') AND success = 1").first();
    const todayDownloads = await env.DB.prepare("SELECT COUNT(*) as c FROM download_log WHERE timestamp > datetime('now', '-1 day')").first();
    const topTickers = await env.DB.prepare('SELECT ticker, COUNT(*) as downloads FROM download_log GROUP BY ticker ORDER BY downloads DESC LIMIT 10').all();
    const recentUsers = await env.DB.prepare('SELECT name, email, institution, country, role, created_at FROM users ORDER BY created_at DESC LIMIT 10').all();

    // Download channel breakdown (api / web / mcp). `channel` is captured at
    // download time going forward; NULL (pre-tracking) rows are classified by
    // endpoint — the API-only endpoints resolve exactly, legacy /v1/download rows
    // fall back to 'web' (see migrate_download_channel.sql).
    const CHANNEL_EXPR =
      "COALESCE(channel, CASE WHEN endpoint IN ('/v1/bars','/v1/variables','/v1/quality') THEN 'api' ELSE 'web' END)";
    const chanAll = await env.DB.prepare(
      `SELECT ${CHANNEL_EXPR} AS channel, COUNT(*) AS downloads, COALESCE(SUM(bytes_served),0) AS bytes FROM download_log GROUP BY 1`
    ).all();
    const chan7d = await env.DB.prepare(
      `SELECT ${CHANNEL_EXPR} AS channel, COUNT(*) AS downloads FROM download_log WHERE timestamp > datetime('now','-7 days') GROUP BY 1`
    ).all();
    // Tracked-since must reflect when live capture began, NOT the migration
    // backfill (which stamps 'api' onto old rows with their original
    // timestamps). /v1/download rows are left NULL by the migration, so the
    // earliest non-NULL one marks the first genuinely-tracked download.
    const trackedSince = await env.DB.prepare(
      "SELECT MIN(timestamp) AS t FROM download_log WHERE endpoint = '/v1/download' AND channel IS NOT NULL"
    ).first();

    return jsonRes({
      total_users: totalUsers.c,
      active_users: activeUsers.c,
      total_downloads: totalDownloads.c,
      total_bytes_served: totalBytes.s || 0,
      today_logins: todayLogins.c,
      today_downloads: todayDownloads.c,
      top_tickers: topTickers.results,
      recent_registrations: recentUsers.results,
      channels: chanAll.results,
      channels_7d: chan7d.results,
      channel_tracked_since: trackedSince ? trackedSince.t : null
    }, 200, cors);
  }

  // GET /v1/admin/downloads — download log (optional ?channel=api|web|mcp filter)
  if (path === '/v1/admin/downloads') {
    const url = new URL(request.url);
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '100') || 100, 500);
    const channel = url.searchParams.get('channel');
    const CHANNEL_EXPR =
      "COALESCE(dl.channel, CASE WHEN dl.endpoint IN ('/v1/bars','/v1/variables','/v1/quality') THEN 'api' ELSE 'web' END)";
    const filtered = channel && ['api', 'web', 'mcp'].includes(channel);
    const stmt = env.DB.prepare(
      `SELECT dl.*, ${CHANNEL_EXPR} AS channel_display, u.name, u.email, u.institution ` +
      `FROM download_log dl LEFT JOIN users u ON dl.user_id = u.id ` +
      (filtered ? `WHERE ${CHANNEL_EXPR} = ? ` : '') +
      `ORDER BY dl.timestamp DESC LIMIT ?`
    );
    const logs = filtered ? await stmt.bind(channel, limit).all() : await stmt.bind(limit).all();
    return jsonRes({ downloads: logs.results }, 200, cors);
  }

  // GET /v1/admin/logins — login history
  if (path === '/v1/admin/logins') {
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '100');
    const logs = await env.DB.prepare(
      'SELECT lh.*, u.name, u.email FROM login_history lh LEFT JOIN users u ON lh.user_id = u.id ORDER BY lh.timestamp DESC LIMIT ?'
    ).bind(limit).all();
    return jsonRes({ logins: logs.results }, 200, cors);
  }

  // POST /v1/admin/newsletter — send newsletter
  if (path === '/v1/admin/newsletter' && request.method === 'POST') {
    return await handleSendNewsletter(request, env, cors);
  }

  // GET /v1/admin/newsletter/campaigns — history
  if (path === '/v1/admin/newsletter/campaigns') {
    const campaigns = await env.DB.prepare(
      'SELECT c.*, u.name as sent_by_name FROM newsletter_campaigns c LEFT JOIN users u ON c.sent_by_user_id = u.id ORDER BY c.sent_at DESC LIMIT 50'
    ).all();
    const subCount = await env.DB.prepare('SELECT COUNT(*) as c FROM users WHERE newsletter_subscribed = 1 AND is_active = 1 AND email_verified = 1').first();
    return jsonRes({ subscribers: subCount.c, campaigns: campaigns.results }, 200, cors);
  }

  return jsonRes({ error: 'Admin endpoint not found' }, 404, cors);
}

// ── Status ──

// Cached for PUBLIC_STATS_TTL. This route is unauthenticated, has no rate limit — it is not even
// passed `ip`, so it could not have one without a signature change — and every single call ran
// TEN full-table D1 aggregates (COUNT over users, COUNT and SUM over download_log, day and week
// windows, a DISTINCT-user country CTE, institutions, top tickers, by-version, registration
// trend) plus an outbound Cloudflare GraphQL request. Anyone with curl could hold the database
// at full scan indefinitely, and D1 refuses WRITES when it is saturated, so the failure lands on
// logins and download logging rather than on this endpoint.
//
// A cache is the right control here rather than a limiter: the answer is identical for every
// caller, so there is nothing to throttle per-client — the work simply should not be repeated.
// Five minutes is far fresher than the numbers need (they move by single-digit counts a day) and
// turns any volume of traffic into at most 12 computations an hour.
//
// ONLY THE BODY IS CACHED, never the Response. CORS headers are per-origin here (corsDecision
// returns a different Access-Control-Allow-Origin for each registered family site), so storing a
// whole Response would serve one origin's CORS headers to another the moment a second site asked
// — a caching bug that presents as an intermittent CORS failure and is miserable to diagnose.
// Re-wrapping the cached JSON with the CURRENT request's cors object makes that impossible.
const PUBLIC_STATS_TTL = 300;

async function handlePublicStats(env, cors) {
  let cache = null;
  const cacheKey = new Request('https://api.hfdatalibrary.com/__cache/public-stats', { method: 'GET' });
  try {
    cache = caches.default;
    const hit = await cache.match(cacheKey);
    if (hit) {
      const body = await hit.text();
      return new Response(body, {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'X-Content-Type-Options': 'nosniff',
          'X-Frame-Options': 'DENY',
          'Referrer-Policy': 'strict-origin-when-cross-origin',
          'Cache-Control': 'public, max-age=' + PUBLIC_STATS_TTL,
          'X-Cache': 'HIT',
          ...cors,
        },
      });
    }
  } catch (e) {
    cache = null;   // Cache API unavailable (e.g. workers.dev) → compute, exactly as before
  }

  // Public stats — no auth required. All data is aggregated, no PII exposed.
  // Total registered accounts (all rows, incl. deactivated) — matches the admin "Total Users" count.
  const totalUsers = await env.DB.prepare('SELECT COUNT(*) as c FROM users').first();
  const totalDownloads = await env.DB.prepare('SELECT COUNT(*) as c FROM download_log').first();
  const totalBytes = await env.DB.prepare('SELECT COALESCE(SUM(bytes_served),0) as s FROM download_log').first();
  const todayDownloads = await env.DB.prepare("SELECT COUNT(*) as c FROM download_log WHERE timestamp > datetime('now', '-1 day')").first();
  const weekDownloads = await env.DB.prepare("SELECT COUNT(*) as c FROM download_log WHERE timestamp > datetime('now', '-7 days')").first();

  // Per-(user, country) DISTINCT pairs across both signals an active user
  // contributes to: their self-declared profile country, and any country
  // they've actually logged in from (cf-ipcountry from login_history).
  // UNION dedupes — if the user typed "IL" AND logged in from IL, they
  // count once for IL, not twice. UNION ALL would double-count.
  // Wrapped as a CTE so the GROUP BY counts distinct users per country.
  const countries = await env.DB.prepare(
    'WITH user_countries AS ( ' +
    '  SELECT id AS user_id, UPPER(country) AS country FROM users ' +
    '    WHERE is_active = 1 AND country != "" ' +
    '  UNION ' +
    '  SELECT lh.user_id, UPPER(lh.country) FROM login_history lh ' +
    '    JOIN users u ON lh.user_id = u.id ' +
    '    WHERE u.is_active = 1 AND lh.country IS NOT NULL ' +
    '      AND lh.country != "" AND lh.country != "unknown" ' +
    ') ' +
    'SELECT country, COUNT(DISTINCT user_id) as users FROM user_countries ' +
    'GROUP BY country ORDER BY users DESC'
  ).all();

  // Distinct institutions (exclude hidden ones + placeholder junk).
  // Many users type "none", "n/a", "self", etc. instead of a real
  // institution. Filter these out server-side BEFORE the LIMIT so junk
  // (e.g. "None" currently ranks #1 by count) doesn't consume top slots
  // or push real schools off the list. Match is case-insensitive and
  // whitespace-trimmed. Real companies (NVIDIA, TeleAI, brokerages) are
  // intentionally NOT blocked — only non-institutional placeholders.
  // To exclude a newly-seen junk value, add its lowercase form here.
  const INSTITUTION_BLOCKLIST = [
    'none', 'n/a', 'na', 'n.a.', 'n.a', 'no', 'nil', 'null', 'nan',
    'self', 'myself', 'me', 'private', 'personal', 'home', 'individual',
    'individuals', 'independent', 'independent trader', 'unaffiliated',
    'unknown', 'student', 'retired', 'retail', 'retail trader',
    'retail investor', 'freelance', 'freelancer', 'trader', 'aleppo',
    '-', '--', '.', '..', '...', 'x', 'xx', 'test', 'asdf',
    // added 2026-06-29 (seen in live data): more placeholders / non-institutions.
    'non applicable', 'independent researcher', 'private trader', 'private use',
    'privat', 'perso', 'persoonlijk', 'full-time employee', 'company', 'exploring',
    'university', 'labs', 'new in fin', 'test university', 'rebel', 'myass',
    '1qaz2wsx', 'gz', 'berln',
  ];
  // Canonical names so the SAME school typed different ways (alias / typo /
  // locale / casing) merges into ONE row instead of splitting its count across
  // the list. Keyed by LOWER(TRIM(value)). Mirrors the normalizeCountry pass.
  // Add an entry only when you're confident two values are the same institution.
  const INSTITUTION_ALIASES = {
    'stanford': 'Stanford University',
    'havard': 'Harvard University',
    'hongkong university': 'University of Hong Kong',
    '中国人民大学': 'Renmin University of China', // 中国人民大学
    'erasmus universiteit rotterdam': 'Erasmus University Rotterdam',
    'michigan': 'University of Michigan',
    'illinois': 'University of Illinois',
    'cambridge': 'University of Cambridge',
    'oxford university': 'University of Oxford',
    'old dominion university': 'Old Dominion University',
    'fordham': 'Fordham University',
  };
  const instPlaceholders = INSTITUTION_BLOCKLIST.map(() => '?').join(',');
  // Fetch ALL non-junk institutions (no LIMIT) so aliases can merge BEFORE the
  // top-N cut, then canonicalize + re-aggregate in JS (same approach as the
  // country normalization below). ~150 distinct values, so no LIMIT is fine.
  const instRaw = await env.DB.prepare(
    'SELECT institution, COUNT(*) as users FROM users ' +
    'WHERE is_active = 1 AND TRIM(institution) != "" AND COALESCE(hide_institution, 0) = 0 ' +
    'AND LOWER(TRIM(institution)) NOT IN (' + instPlaceholders + ') ' +
    'GROUP BY institution'
  ).bind(...INSTITUTION_BLOCKLIST).all();
  const instMerged = {};
  for (const row of (instRaw.results || [])) {
    const name = (row.institution || '').trim();
    if (!name) continue;
    const canon = INSTITUTION_ALIASES[name.toLowerCase()] || name;
    instMerged[canon] = (instMerged[canon] || 0) + row.users;
  }
  const institutions = {
    results: Object.keys(instMerged)
      .map((institution) => ({ institution, users: instMerged[institution] }))
      .sort((a, b) => b.users - a.users)
      .slice(0, 50),
  };

  // Top downloaded tickers
  const topTickers = await env.DB.prepare('SELECT ticker, COUNT(*) as downloads, SUM(bytes_served) as bytes FROM download_log GROUP BY ticker ORDER BY downloads DESC LIMIT 25').all();

  // Downloads by version
  const byVersion = await env.DB.prepare('SELECT version, COUNT(*) as downloads FROM download_log GROUP BY version ORDER BY downloads DESC').all();

  // Registrations per week (last 12 weeks)
  const regTrend = await env.DB.prepare("SELECT strftime('%Y-W%W', created_at) as week, COUNT(*) as registrations FROM users WHERE created_at > datetime('now', '-84 days') GROUP BY week ORDER BY week").all();

  // Normalize each row's country to an ISO-2 code via normalizeCountry() so
  // "United States", "USA", "U.S." and "us" all collapse to "US". Anything
  // that fails normalization (CJK, corrupted bytes, free-text we don't
  // recognize) is dropped before reaching the world map renderer.
  const userCountryMap = {};
  for (const row of countries.results) {
    const code = normalizeCountry(row.country);
    if (code) userCountryMap[code] = (userCountryMap[code] || 0) + row.users;
  }

  // Cloudflare Analytics — cumulative visitor countries since site launch
  let visitorCountryMap = {};
  let totalVisitors = 0;
  let totalPageViews = 0;
  try {
    if (env.CF_API_TOKEN && env.CF_ZONE_ID) {
      const gqlQuery = `query {
        viewer {
          zones(filter: {zoneTag: "${env.CF_ZONE_ID}"}) {
            httpRequests1dGroups(limit: 10000, filter: {date_geq: "2026-04-09"}) {
              sum { requests pageViews countryMap { clientCountryName requests } }
              uniq { uniques }
            }
          }
        }
      }`;
      const cfRes = await fetch('https://api.cloudflare.com/client/v4/graphql', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + env.CF_API_TOKEN,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query: gqlQuery }),
      });
      const cfData = await cfRes.json();
      const zones = cfData?.data?.viewer?.zones;
      if (zones && zones.length > 0) {
        for (const g of zones[0].httpRequests1dGroups) {
          totalVisitors += g.uniq?.uniques || 0;
          totalPageViews += g.sum?.pageViews || 0;
          for (const c of (g.sum?.countryMap || [])) {
            const code = c.clientCountryName;
            if (code && code.length <= 3 && code !== 'XX' && code !== 'T1') {
              visitorCountryMap[code] = (visitorCountryMap[code] || 0) + c.requests;
            }
          }
        }
      }
    }
  } catch (e) {
    // Analytics fetch failed — return stats without visitor data
  }

  const payload = ({
    total_users: totalUsers?.c || 0,
    total_downloads: totalDownloads?.c || 0,
    total_bytes_served: totalBytes?.s || 0,
    downloads_today: todayDownloads?.c || 0,
    downloads_this_week: weekDownloads?.c || 0,
    countries: userCountryMap,
    country_count: Object.keys(userCountryMap).length,
    visitor_countries: visitorCountryMap,
    visitor_country_count: Object.keys(visitorCountryMap).length,
    total_visitors: totalVisitors,
    total_page_views: totalPageViews,
    institutions: institutions.results,
    institution_count: institutions.results.length,
    top_tickers: topTickers.results,
    by_version: byVersion.results,
    registration_trend: regTrend.results,
    generated_at: new Date().toISOString(),
  });
  const body = JSON.stringify(payload, null, 2);
  // Store the BODY only — see the note above on why a whole Response would leak per-origin CORS.
  if (cache) {
    try {
      await cache.put(cacheKey, new Response(body, {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=' + PUBLIC_STATS_TTL },
      }));
    } catch (e) { /* caching is an optimisation; never fail the request for it */ }
  }
  return new Response(body, {
    status: 200,
    headers: {
      'Content-Type': 'application/json',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      'Cache-Control': 'public, max-age=' + PUBLIC_STATS_TTL,
      'X-Cache': 'MISS',
      ...cors,
    },
  });
}

async function handleStatus(env, cors) {
  const list = await env.DATA_BUCKET.list({ prefix: 'clean/', limit: 1 });
  const userCount = await env.DB.prepare('SELECT COUNT(*) as c FROM users').first();
  return jsonRes({
    status: 'operational',
    api_version: '2.0',
    author: 'Ahmed Elkassabgi',
    r2_connected: list.objects.length > 0,
    registered_users: userCount?.c || 0,
    rate_limit: '300 requests per minute (downloads), 5 login attempts per 5 min',
    attribution: 'Post-March-2022 data: Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use. https://www.iex.io/legal/hist-data-terms',
    timestamp: new Date().toISOString()
  }, 200, cors);
}

// ── Helpers ──

function jsonRes(data, status, cors) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      ...cors
    }
  });
}

// ══════════════════════════════════════
// ── Durable Object rate limiter (§18) ──
// ══════════════════════════════════════
// One DO instance per bucket:key (idFromName). Fixed-window, in-memory (off D1),
// atomic within the instance. Fronts ONLY /authorize + /token/* on accounts.*.
export class RateLimiterDO {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    const { limit, windowSec } = await request.json();
    const now = Date.now();
    let d = await this.state.storage.get('w');
    if (!d || now >= d.reset) d = { count: 0, reset: now + windowSec * 1000 };
    d.count += 1;
    await this.state.storage.put('w', d);
    const ok = d.count <= limit;
    return new Response(
      JSON.stringify({ ok, retryAfter: Math.max(1, Math.ceil((d.reset - now) / 1000)) }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }
}

// Ask the DO whether this (bucket,key) is within `limit` per `windowSec`.
// SHADOW mode (default) logs a would-be denial but never blocks — flip `enforce`
// after a soak. Fails OPEN on any DO error (availability > strictness on the
// login path; abuse is still bounded by the other bucket).
async function rateLimit(env, bucket, key, limit, windowSec, enforce) {
  try {
    const id = env.RATE_LIMITER.idFromName(bucket + ':' + key);
    const stub = env.RATE_LIMITER.get(id);
    const res = await stub.fetch('https://ratelimit/', {
      method: 'POST',
      body: JSON.stringify({ limit, windowSec }),
    });
    const { ok, retryAfter } = await res.json();
    if (!ok) {
      console.log(JSON.stringify({ evt: 'rate_limit', bucket, enforce: !!enforce, key: key.slice(0, 24) }));
      if (enforce) return { ok: false, retryAfter };
    }
    return { ok: true };
  } catch (e) {
    return { ok: true };
  }
}

// ══════════════════════════════════════
// ── Family SSO M2b-1 — IdP issuer core ──
// ══════════════════════════════════════
// The IdP identity, tokens, and issuer endpoints on accounts.elkassabgidata.com.
// All credentials are stored HASHED at rest (sha256Hex). Short TTLs use SQLite
// datetime() arithmetic, never toISOString().

function htmlEncode(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

async function hmacSign(secret, msg) {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(msg));
  return b64url(new Uint8Array(sig));
}

// ── ekd_session (idp_master) — the family-wide login. Stored hashed; the raw
//    value lives only in a host-only HttpOnly cookie on accounts.* ──
async function getIdpSessionUser(request, env) {
  const cookie = request.headers.get('cookie') || '';
  const raw = readCookie(cookie, 'ekd_session', '[A-Za-z0-9_-]+');
  if (!raw) return null;
  const idHash = await sha256Hex(raw);
  // §EXPIRY-COMPARE: datetime() on both sides. This one was NOT broken —
  // createIdpSession writes space format — but it reads the same `sessions.expires_at`
  // that createSession fills with ISO text, and only the kind predicate keeps the two
  // apart. Wrapping costs nothing on a space-format row and means a future writer on
  // this column cannot silently reopen the hole.
  const row = await env.DB.prepare(
    "SELECT u.*, s.id AS session_id, s.kind AS session_kind, s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.kind = 'idp_master' AND datetime(s.expires_at) > datetime('now')"
  ).bind(idHash).first();
  if (!row || !row.is_active) return null;
  return row;
}

async function createIdpSession(env, userId, ip, ua) {
  const raw = generateToken();
  const idHash = await sha256Hex(raw);
  await env.DB.prepare(
    "INSERT INTO sessions (id, user_id, ip_address, user_agent, expires_at, kind) " +
    "VALUES (?, ?, ?, ?, datetime('now','+" + EKD_SESSION_DAYS + " days'), 'idp_master')"
  ).bind(idHash, userId, ip, ua).run();
  const cookie = 'ekd_session=' + raw + '; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=' + (EKD_SESSION_DAYS * 86400);
  return { raw, cookie };
}

// ── Consent gesture (stateless HMAC, defense-in-depth over SameSite=Lax) ──
async function mintGestureToken(env, idpSessionHash, clientId, state, codeChallenge) {
  if (!env.CONSENT_HMAC_SECRET) return '';
  const expMs = Date.now() + GESTURE_TTL_SEC * 1000;
  const sig = await hmacSign(env.CONSENT_HMAC_SECRET, idpSessionHash + '.' + clientId + '.' + state + '.' + codeChallenge + '.' + expMs);
  return sig + '.' + expMs;
}
async function verifyGestureToken(env, token, idpSessionHash, clientId, state, codeChallenge) {
  // No secret configured → gesture is explicitly skipped (relies on SameSite=Lax
  // + Origin + Sec-Fetch), never fail-open silently.
  if (!env.CONSENT_HMAC_SECRET) { console.log(JSON.stringify({ evt: 'gesture_skipped_no_secret' })); return true; }
  if (!token || token.indexOf('.') < 0) return false;
  const i = token.lastIndexOf('.');
  const sig = token.slice(0, i);
  const expMs = parseInt(token.slice(i + 1), 10);
  if (!expMs || Date.now() > expMs) return false;
  const expected = await hmacSign(env.CONSENT_HMAC_SECRET, idpSessionHash + '.' + clientId + '.' + state + '.' + codeChallenge + '.' + expMs);
  return constantTimeEqual(sig, expected);
}

// ── Family token minting + chain revocation ──
async function mintFamilyTokens(env, userId, clientOrigin, ip, ua, chain) {
  const rawAt = generateToken();
  const atHash = await sha256Hex(rawAt);
  await env.DB.prepare(
    "INSERT INTO sessions (id,user_id,ip_address,user_agent,expires_at,kind,audience) " +
    "VALUES (?,?,?,?,datetime('now','+" + EDL_AT_TTL_SEC + " seconds'),'family_access',?)"
  ).bind(atHash, userId, ip, ua, clientOrigin).run();

  const rawRt = generateToken();
  const rtHash = await sha256Hex(rawRt);
  const chainId = (chain && chain.chainId) || generateToken();
  const generation = (chain && chain.generation != null) ? chain.generation + 1 : 0;
  const parentHash = (chain && chain.parentHash) || null;
  if (chain && chain.absoluteExpiresAt) {
    await env.DB.prepare(
      "INSERT INTO sso_refresh_tokens (token_hash,user_id,audience,chain_id,parent_hash,access_hash,generation,used,revoked,absolute_expires_at,expires_at) " +
      "VALUES (?,?,?,?,?,?,?,0,0,?,?)"
    ).bind(rtHash, userId, clientOrigin, chainId, parentHash, atHash, generation, chain.absoluteExpiresAt, chain.absoluteExpiresAt).run();
  } else {
    await env.DB.prepare(
      "INSERT INTO sso_refresh_tokens (token_hash,user_id,audience,chain_id,parent_hash,access_hash,generation,used,revoked,absolute_expires_at,expires_at) " +
      "VALUES (?,?,?,?,?,?,?,0,0,datetime('now','+" + EDL_RT_TTL_HOURS + " hours'),datetime('now','+" + EDL_RT_TTL_HOURS + " hours'))"
    ).bind(rtHash, userId, clientOrigin, chainId, parentHash, atHash, generation).run();
  }
  if (parentHash) {
    await env.DB.prepare("UPDATE sso_refresh_tokens SET child_hash=? WHERE token_hash=?").bind(rtHash, parentHash).run();
  }
  return { access_token: rawAt, refresh_token: rawRt, expires_in: EDL_AT_TTL_SEC, chain_id: chainId, generation };
}

async function mintFamilyAccessOnly(env, userId, clientOrigin, ip, ua, chain) {
  const rawAt = generateToken();
  const atHash = await sha256Hex(rawAt);
  await env.DB.prepare(
    "INSERT INTO sessions (id,user_id,ip_address,user_agent,expires_at,kind,audience) " +
    "VALUES (?,?,?,?,datetime('now','+" + EDL_AT_TTL_SEC + " seconds'),'family_access',?)"
  ).bind(atHash, userId, ip, ua, clientOrigin).run();
  // Bookkeeping refresh row (used=1, never handed out) that LINKS this grace-minted
  // edl_at to the chain via access_hash, so revokeChain's access_hash sweep can
  // delete it on chain revocation — otherwise a grace edl_at would survive a
  // reuse-triggered revoke for its full 15-min TTL.
  if (chain && chain.chainId) {
    const bookHash = await sha256Hex(generateToken());
    if (chain.absoluteExpiresAt) {
      await env.DB.prepare(
        "INSERT INTO sso_refresh_tokens (token_hash,user_id,audience,chain_id,access_hash,generation,used,revoked,absolute_expires_at,expires_at) " +
        "VALUES (?,?,?,?,?,?,1,0,?,?)"
      ).bind(bookHash, userId, clientOrigin, chain.chainId, atHash, (chain.generation || 0), chain.absoluteExpiresAt, chain.absoluteExpiresAt).run();
    } else {
      await env.DB.prepare(
        "INSERT INTO sso_refresh_tokens (token_hash,user_id,audience,chain_id,access_hash,generation,used,revoked,absolute_expires_at,expires_at) " +
        "VALUES (?,?,?,?,?,?,1,0,datetime('now','+" + EDL_RT_TTL_HOURS + " hours'),datetime('now','+" + EDL_RT_TTL_HOURS + " hours'))"
      ).bind(bookHash, userId, clientOrigin, chain.chainId, atHash, (chain.generation || 0)).run();
    }
  }
  return { access_token: rawAt, expires_in: EDL_AT_TTL_SEC };
}

async function revokeChain(env, chainId) {
  // Delete the linked live family_access sessions (kills live edl_ats now), then
  // mark the whole refresh chain revoked.
  await env.DB.prepare(
    "DELETE FROM sessions WHERE id IN (SELECT access_hash FROM sso_refresh_tokens WHERE chain_id=? AND access_hash IS NOT NULL)"
  ).bind(chainId).run();
  await env.DB.prepare("UPDATE sso_refresh_tokens SET revoked=1 WHERE chain_id=?").bind(chainId).run();
}

function jsonNoStore(obj, status, cors) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...(cors || {}) },
  });
}

// ── GET /authorize — sign-in prompt (no session) or branded consent (session) ──
async function handleAuthorizeGet(request, env, url) {
  const q = url.searchParams;
  const clientId = q.get('client_id') || '';
  const redirectUri = q.get('redirect_uri') || '';
  const state = q.get('state') || '';
  const codeChallenge = q.get('code_challenge') || '';
  const method = q.get('code_challenge_method') || '';
  const responseType = q.get('response_type') || '';
  const secHeaders = {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-store',
    // 'same-origin' (NOT 'no-referrer'): this page's consent form does a
    // same-origin POST /authorize whose Origin header the server checks. Under
    // 'no-referrer' the browser sends Origin: null on that POST → cross_site_blocked.
    // 'same-origin' preserves Origin for same-origin submits; the code-carrying
    // 303 keeps its own 'no-referrer'.
    'Referrer-Policy': 'same-origin',
    'X-Frame-Options': 'DENY',
    'X-Content-Type-Options': 'nosniff',
    // form-action includes https: because the consent form's SUCCESS is a 303 to
    // the client's cross-origin callback (e.g. hfdatalibrary.com/auth/callback);
    // CSP enforces form-action on the REDIRECT target, so 'self' alone silently
    // blocks the redirect and the popup never reaches the callback. The real
    // control is the server-side redirect_exact validation, not this directive.
    'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; form-action 'self' https:; frame-ancestors 'none'; base-uri 'none'",
  };
  if (responseType !== 'code' || method !== 'S256' || !/^[A-Za-z0-9_-]{43}$/.test(codeChallenge)) {
    return new Response('<h1>Invalid request</h1>', { status: 400, headers: secHeaders });
  }
  const reg = await getRegistry(env);
  const row = reg.get(clientId);
  if (!row) return new Response('<h1>Unknown application</h1>', { status: 400, headers: secHeaders });
  if (row.status !== 'active') return new Response('<h1>Application suspended</h1>', { status: 403, headers: secHeaders });
  if (!row.redirect_exact || row.redirect_exact !== redirectUri) {
    return new Response('<h1>Redirect URI mismatch</h1>', { status: 400, headers: secHeaders });
  }
  const user = await getIdpSessionUser(request, env);

  // §PROMPT-NONE — silent cross-site resume.
  //
  // THE PROBLEM THIS SOLVES. ekd_session is host-only on accounts.elkassabgidata.com, and the
  // SDK's tokens live in localStorage, which is per-origin. So signing in on econdatalibrary
  // leaves hfdatalibrary with an empty localStorage: EKD.getAccessToken() returns null without
  // ever asking the IdP, js/site.js paints "Sign in", and the user who just signed in one tab
  // ago is told they are a stranger. The IdP knew all along — nobody asked it.
  //
  // Nothing could ask it silently. GET /authorize with a live session renders a CONSENT PAGE
  // that needs a click, and EKD.login() opens a popup that needs a click. prompt=none is the
  // missing third door: same validation, no UI, answer immediately either way.
  //
  // WHY SKIPPING CONSENT IS SOUND HERE. The consent page and its gesture token defend the POST
  // against cross-site form submission. There is no POST on this path — it mints a code bound
  // to (user, client_id, redirect_exact, state, code_challenge) and 303s it to the client's
  // OWN pre-registered callback. A hostile site can start this flow, but the code lands on
  // hfdatalibrary.com/auth/callback, not on the attacker, and redeeming it needs the PKCE
  // verifier that never left the initiating page. What an attacker does learn is one bit —
  // whether this browser has a family session — which is why it is confined to clients already
  // in the registry with status active and an exact redirect match, all checked above. These
  // are Ahmed's own sites sharing one account by design; asking a user to re-consent to
  // hfdatalibrary on every visit is friction that buys nothing.
  //
  // The error goes in the FRAGMENT, not the query string, for the same reason the success code
  // does: a fragment is not sent to the server, so it stays out of Cloudflare's access log,
  // out of Referer, and out of anything downstream of the callback.
  if (q.get('prompt') === 'none') {
    if (!user) {
      const dest = redirectUri + '#error=login_required&state=' + encodeURIComponent(state);
      return new Response(null, {
        status: 303,
        headers: { 'Location': dest, 'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer' },
      });
    }
    return await mintCodeAndRedirect(env, user.id, clientId, redirectUri, state, codeChallenge, 303);
  }

  if (!user) {
    // No IdP session → the real login/register auth page (M2b-2a). Uses the
    // Turnstile-permitting CSP; on submit, /login or /register sets ekd_session
    // and 303s back with the code (the auth submission is the consent). The SDK
    // popup passes hint=register to open straight to the sign-up tab.
    const hint = q.get('hint') === 'register' ? 'register' : 'login';
    // ORCID link-or-register bounce: a valid signed prefill carries the verified
    // orcid_id — default to the login tab (link to an existing account) with a
    // banner offering "or create a new account".
    const orcidTok = q.get('orcid_prefill') || '';
    // Read-only: renders the banner and pre-fills the name. Audience 'idp' plus the
    // nonce cookie mean a token from api.* — or from anyone else's browser — draws no
    // banner at all, so an attacker's iD can no longer even be shown to a victim here.
    const orcidPre = orcidTok ? await verifyOrcidPrefill(env, request, orcidTok, 'idp') : null;
    const p = { clientId, redirectUri, state, codeChallenge, method };
    if (orcidPre) p.orcidPrefill = orcidTok;
    return new Response(
      renderAuthPage(row, p, { tab: orcidPre ? 'login' : hint, error: '', loginEmail: '', orcid: orcidPre }),
      { status: 200, headers: authPageHeaders }
    );
  }
  // signed in → branded consent with a gesture-bound POST form
  const cookie = request.headers.get('cookie') || '';
  const rawEkd = readCookie(cookie, 'ekd_session', '[A-Za-z0-9_-]+');
  const idpSessionHash = rawEkd ? await sha256Hex(rawEkd) : '';
  const gesture = await mintGestureToken(env, idpSessionHash, clientId, state, codeChallenge);
  return new Response(renderConsentPage(user, row, { clientId, redirectUri, state, codeChallenge, method }, gesture), { status: 200, headers: secHeaders });
}

// ── POST /authorize — the ONLY code-minting path for password/cookie users ──
async function handleAuthorizePost(request, env, ip, ua) {
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response('login_required', { status: 401 });
  const origin = request.headers.get('Origin') || '';
  const sfs = request.headers.get('Sec-Fetch-Site');
  if (origin !== IDP_ORIGIN || (sfs && sfs !== 'same-origin')) {
    return new Response('cross_site_blocked', { status: 403 });
  }
  const rl = await rateLimit(env, 'authz_ip', ip, AUTHZ_IP_MAX, 60, false);
  if (!rl.ok) return new Response('rate_limited', { status: 429 });

  let body;
  try { body = await request.formData(); } catch { return new Response('Bad request', { status: 400, headers: { 'Cache-Control': 'no-store' } }); }
  const clientId = body.get('client_id') || '';
  const redirectUri = body.get('redirect_uri') || '';
  const state = body.get('state') || '';
  const codeChallenge = body.get('code_challenge') || '';
  const method = body.get('code_challenge_method') || '';
  const gesture = body.get('gesture') || '';
  if (method !== 'S256' || !/^[A-Za-z0-9_-]{43}$/.test(codeChallenge)) return new Response('invalid_request', { status: 400 });
  const reg = await getRegistry(env);
  const row = reg.get(clientId);
  if (!row || row.status !== 'active') return new Response('invalid_client', { status: 400 });
  if (!row.redirect_exact || row.redirect_exact !== redirectUri) return new Response('redirect_mismatch', { status: 400 });

  const cookie = request.headers.get('cookie') || '';
  const rawEkd = readCookie(cookie, 'ekd_session', '[A-Za-z0-9_-]+');
  const idpSessionHash = rawEkd ? await sha256Hex(rawEkd) : '';
  if (!(await verifyGestureToken(env, gesture, idpSessionHash, clientId, state, codeChallenge))) {
    return new Response('bad_gesture', { status: 403 });
  }
  return await mintCodeAndRedirect(env, user.id, clientId, redirectUri, state, codeChallenge, 303);
}

// Shared code-mint + redirect (used by consent POST and OAuth callbacks).
// Mint a single-use PKCE code bound to (user, client, state, challenge) and return
// the exact callback destination (code+state in the URL fragment). Shared by the
// 303 redirect path and the post-register verify-notice interstitial's Continue link.
async function mintSsoCode(env, userId, clientOrigin, redirectExact, state, codeChallenge, ttlSec) {
  // Default 60s (CODE_TTL_SEC) for the auto-followed 303 paths (popup navigates
  // sub-second). Callers that put a human-paced click between mint and consume
  // (the verify-notice interstitial) pass a longer, RFC-6749-compliant TTL.
  const ttl = Number.isFinite(ttlSec) ? Math.max(1, Math.floor(ttlSec)) : CODE_TTL_SEC;
  const rawCode = generateToken();
  const codeHash = await sha256Hex(rawCode);
  const consentToken = generateToken();
  await env.DB.prepare(
    "INSERT INTO sso_codes (code_hash,user_id,client_origin,state,code_challenge,consent_token,used,expires_at) " +
    "VALUES (?,?,?,?,?,?,0,datetime('now','+" + ttl + " seconds'))"
  ).bind(codeHash, userId, clientOrigin, state, codeChallenge, consentToken).run();
  return redirectExact + '#code=' + encodeURIComponent(rawCode) + '&state=' + encodeURIComponent(state);
}

async function mintCodeAndRedirect(env, userId, clientOrigin, redirectExact, state, codeChallenge, status) {
  const dest = await mintSsoCode(env, userId, clientOrigin, redirectExact, state, codeChallenge);
  return new Response(null, {
    status: status || 303,
    headers: { 'Location': dest, 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store' },
  });
}

// ── POST /token/exchange — cookieless, no-store ──
async function handleTokenExchange(request, env, ip, ua, cors) {
  const rlIp = await rateLimit(env, 'exch_ip', ip, EXCH_IP_MAX, 60, false);
  if (!rlIp.ok) return jsonNoStore({ error: 'rate_limited' }, 429, cors);
  let body;
  try { body = await request.json(); } catch { return jsonNoStore({ error: 'invalid_request' }, 400, cors); }
  const { code, code_verifier, client_origin } = body || {};
  if (!code || !code_verifier || !client_origin) return jsonNoStore({ error: 'invalid_request' }, 400, cors);
  const origin = request.headers.get('Origin') || '';
  if (origin !== client_origin) return jsonNoStore({ error: 'origin_mismatch' }, 403, cors);

  // atomic single-use consume — burns the code regardless of what follows
  const codeHash = await sha256Hex(code);
  // §EXPIRY-COMPARE: datetime() on both sides. Already correct — mintSsoCode is the
  // only writer and uses datetime() arithmetic — wrapped so every expiry compare in
  // the file reads the same way and none of them depends on its writer staying put.
  const claim = await env.DB.prepare(
    "UPDATE sso_codes SET used=1 WHERE code_hash=? AND used=0 AND datetime(expires_at)>datetime('now')"
  ).bind(codeHash).run();
  if (!claim.meta || claim.meta.changes !== 1) return jsonNoStore({ error: 'invalid_code' }, 400, cors);
  const row = await env.DB.prepare("SELECT * FROM sso_codes WHERE code_hash=?").bind(codeHash).first();
  if (!row || row.client_origin !== client_origin || !row.consent_token) return jsonNoStore({ error: 'invalid_code' }, 400, cors);
  if ((await pkceS256(code_verifier)) !== row.code_challenge) return jsonNoStore({ error: 'invalid_grant' }, 400, cors);
  const reg = await getRegistry(env);
  const client = reg.get(client_origin);
  if (!client || client.status !== 'active') return jsonNoStore({ error: 'invalid_client' }, 400, cors);
  const u = await env.DB.prepare("SELECT id,is_active FROM users WHERE id=?").bind(row.user_id).first();
  if (!u || !u.is_active) return jsonNoStore({ error: 'user_inactive' }, 401, cors);

  const rlAcct = await rateLimit(env, 'exch_acct', String(row.user_id), EXCH_ACCT_MAX, 60, false);
  if (!rlAcct.ok) return jsonNoStore({ error: 'rate_limited' }, 429, cors);
  const t = await mintFamilyTokens(env, row.user_id, client_origin, ip, ua, null);
  return jsonNoStore({ access_token: t.access_token, refresh_token: t.refresh_token, token_type: 'Bearer', expires_in: t.expires_in }, 200, cors);
}

// ── POST /token/refresh — rotating single-use, reuse→chain revoke ──
async function handleTokenRefresh(request, env, ip, ua, cors) {
  const rlIp = await rateLimit(env, 'rt_ip', ip, RT_IP_MAX, 60, false);
  if (!rlIp.ok) return jsonNoStore({ error: 'rate_limited' }, 429, cors);
  let body;
  try { body = await request.json(); } catch { return jsonNoStore({ error: 'invalid_request' }, 400, cors); }
  const { refresh_token, client_origin } = body || {};
  if (!refresh_token || !client_origin) return jsonNoStore({ error: 'invalid_request' }, 400, cors);
  const origin = request.headers.get('Origin') || '';
  if (origin !== client_origin) return jsonNoStore({ error: 'origin_mismatch' }, 403, cors);

  const rtHash = await sha256Hex(refresh_token);
  // Read + validate BEFORE any state mutation. audience is immutable (written only
  // at INSERT), so a pre-claim read is TOCTOU-safe and a wrong-audience/absent
  // token becomes a pure no-op — it can never BURN a valid token (forced-logout).
  const rt = await env.DB.prepare("SELECT * FROM sso_refresh_tokens WHERE token_hash=?").bind(rtHash).first();
  if (!rt) return jsonNoStore({ error: 'invalid_grant' }, 401, cors);
  if (rt.audience !== client_origin) return jsonNoStore({ error: 'origin_mismatch' }, 403, cors);

  // Atomic single-use rotation claim (the used=0 predicate is the sole atomicity point).
  // §EXPIRY-COMPARE: datetime() wraps the stored absolute_expires_at only. The two
  // datetime('now') calls in the SET clause are WRITES, not comparisons — they are what
  // keeps this table in space format and they stay exactly as they are.
  const claim = await env.DB.prepare(
    "UPDATE sso_refresh_tokens SET used=1, used_at=datetime('now'), grace_until=datetime('now','+" + RT_GRACE_SEC + " seconds') " +
    "WHERE token_hash=? AND used=0 AND revoked=0 AND datetime(absolute_expires_at)>datetime('now')"
  ).bind(rtHash).run();

  if (claim.meta && claim.meta.changes === 1) {
    const rlAcct = await rateLimit(env, 'rt_acct', String(rt.user_id), RT_ACCT_MAX, 60, false);
    if (!rlAcct.ok) return jsonNoStore({ error: 'rate_limited' }, 429, cors);
    const t = await mintFamilyTokens(env, rt.user_id, client_origin, ip, ua, {
      chainId: rt.chain_id, generation: rt.generation, parentHash: rtHash, absoluteExpiresAt: rt.absolute_expires_at,
    });
    return jsonNoStore({ access_token: t.access_token, refresh_token: t.refresh_token, token_type: 'Bearer', expires_in: t.expires_in }, 200, cors);
  }

  // Claim failed — re-read the CURRENT state to classify (a concurrent claim may
  // have won). Three cases: (a) still used=0/revoked=0 → the only failing predicate
  // was the absolute cap → benign idle expiry, NOT reuse (do not revoke). (b) inside
  // the grace window → benign multi-tab race → a fresh edl_at only (linked to the
  // chain so revokeChain can reach it). (c) otherwise → genuine reuse → revoke chain.
  const cur = await env.DB.prepare("SELECT used,revoked,grace_until FROM sso_refresh_tokens WHERE token_hash=?").bind(rtHash).first();
  if (!cur) return jsonNoStore({ error: 'invalid_grant' }, 401, cors);
  if (cur.used === 0 && cur.revoked === 0) {
    return jsonNoStore({ error: 'invalid_grant' }, 401, cors);
  }
  // §EXPIRY-COMPARE: datetime() on both sides. grace_until is written by the claim
  // above in space format, and is NULL until a claim sets it — datetime(NULL) is NULL,
  // so an unclaimed row still fails the predicate exactly as it did before.
  const inGrace = await env.DB.prepare(
    "SELECT 1 FROM sso_refresh_tokens WHERE token_hash=? AND revoked=0 AND datetime(grace_until)>datetime('now')"
  ).bind(rtHash).first();
  if (inGrace) {
    const a = await mintFamilyAccessOnly(env, rt.user_id, client_origin, ip, ua, {
      chainId: rt.chain_id, generation: rt.generation, absoluteExpiresAt: rt.absolute_expires_at,
    });
    return jsonNoStore({ access_token: a.access_token, token_type: 'Bearer', expires_in: a.expires_in }, 200, cors);
  }
  await revokeChain(env, rt.chain_id);
  return jsonNoStore({ error: 'token_reuse' }, 401, cors);
}

// ── POST /logout ──
async function handleAccountsLogout(request, env, cors) {
  let body = {};
  try { body = await request.json(); } catch { /* optional */ }
  const cookie = request.headers.get('cookie') || '';
  const rawEkd = readCookie(cookie, 'ekd_session', '[A-Za-z0-9_-]+');
  if (rawEkd) {
    const idHash = await sha256Hex(rawEkd);
    await env.DB.prepare("DELETE FROM sessions WHERE id=? AND kind='idp_master'").bind(idHash).run();
  }
  if (body && body.refresh_token) {
    const rtHash = await sha256Hex(body.refresh_token);
    const rt = await env.DB.prepare("SELECT chain_id, user_id FROM sso_refresh_tokens WHERE token_hash=?").bind(rtHash).first();
    if (rt) {
      await revokeChain(env, rt.chain_id);
      // END THE IdP SESSION TOO, resolved from the refresh token rather than from the cookie.
      //
      // The cookie branch above is dead code from this caller and always has been: the SDK's
      // logout() is a CROSS-ORIGIN fetch from the client site to accounts.elkassabgidata.com,
      // and postJson sets no `credentials`, so the browser sends no cookies at all. rawEkd is
      // therefore null on every real call, the idp_master DELETE never runs, and the Set-Cookie
      // that clears ekd_session is equally inert cross-origin. Signing out revoked the refresh
      // chain and left the 30-day family session untouched.
      //
      // That was a latent inconsistency until today. It is not latent now: the silent resume
      // added this morning bounces to /authorize?prompt=none precisely WHEN there is no local
      // credential — which is the state logout creates. A live ekd_session then mints a fresh
      // code and signs the user straight back in, so "log out" undid itself on the next page
      // load. The feature turned a dormant bug into a broken control.
      //
      // Resolving the user from the refresh token needs no cookie and no credentialed CORS
      // (which the token endpoints deliberately do not allow — they are non-credentialed by
      // design). Deleting by user_id also means this cannot miss a session whose kind or id we
      // failed to guess, the same reasoning that put every other revocation path through
      // revokeAllUserCredentials.
      await env.DB.prepare("DELETE FROM sessions WHERE user_id = ? AND kind = 'idp_master'").bind(rt.user_id).run();
    }
  }
  const clear = 'ekd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0';
  return new Response(JSON.stringify({ ok: true }), {
    status: 200, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Set-Cookie': clear, ...cors },
  });
}

// ── Branded pages (theme is an enumerated token; brand/name HTML-encoded) ──
function renderConsentPage(user, row, p, gesture) {
  const brand = htmlEncode(row.brand_name || 'ElkassabgiData');
  const name = htmlEncode(user.name || user.email || 'your account');
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Continue to ' + brand + '</title><style>body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}' +
    '.card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:2rem;max-width:380px;text-align:center}' +
    'h1{font-size:1.1rem;color:#d4a843}button{background:#d4a843;color:#0f1729;border:0;border-radius:8px;padding:.7rem 1.4rem;font-weight:700;font-size:1rem;cursor:pointer;width:100%}' +
    'p{color:#9ca3af;font-size:.9rem}</style></head><body><div class="card"><h1>Continue to ' + brand + '</h1>' +
    '<p>You are signed in to ElkassabgiData as<br><strong style="color:#e5e7eb">' + name + '</strong></p>' +
    '<form method="POST" action="/authorize">' +
    '<input type="hidden" name="client_id" value="' + htmlEncode(p.clientId) + '">' +
    '<input type="hidden" name="redirect_uri" value="' + htmlEncode(p.redirectUri) + '">' +
    '<input type="hidden" name="state" value="' + htmlEncode(p.state) + '">' +
    '<input type="hidden" name="code_challenge" value="' + htmlEncode(p.codeChallenge) + '">' +
    '<input type="hidden" name="code_challenge_method" value="S256">' +
    '<input type="hidden" name="gesture" value="' + htmlEncode(gesture) + '">' +
    '<button type="submit">Continue as ' + name + '</button></form></div></body></html>';
}

function renderSignInPrompt(row, p) {
  const brand = htmlEncode(row.brand_name || 'ElkassabgiData');
  // M2b-2 builds the real login/register surface here; for now, a prompt.
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Sign in to ElkassabgiData</title><style>body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}' +
    '.card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:2rem;max-width:380px;text-align:center}h1{font-size:1.1rem;color:#d4a843}p{color:#9ca3af}</style></head><body>' +
    '<div class="card"><h1>Sign in to continue to ' + brand + '</h1><p>One ElkassabgiData account works across every library. Sign-in and registration arrive here shortly.</p></div></body></html>';
}

// ══════════════════════════════════════════════════════════════════
// ── Family SSO — account surface on accounts.elkassabgidata.com ──
// ══════════════════════════════════════════════════════════════════
// The first-party home for a signed-in ElkassabgiData account: view/copy the API
// key, regenerate it, see the profile, log out. Authed by the ekd_session cookie
// (getIdpSessionUser, kind='idp_master') — SAME origin, so the api_key never
// crosses to a family site (the scope-split the whole design protects). NO client
// JS: the key sits in a readonly field, and regenerate/logout are same-origin form
// POSTs — zero XSS surface on a page that shows a secret. api.* handlers unchanged.
const accountPageHeaders = {
  'Content-Type': 'text/html; charset=utf-8',
  'Cache-Control': 'no-store',
  // 'same-origin' so the regenerate/logout same-origin form POSTs keep their
  // Origin header (no-referrer would send Origin: null → cross_site_blocked).
  'Referrer-Policy': 'same-origin',
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'Content-Security-Policy':
    "default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; " +
    "form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
};

function renderSignedOutPage() {
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>ElkassabgiData account</title><style>body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}' +
    '.card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:2rem;max-width:400px;text-align:center}h1{font-size:1.1rem;color:#d4a843}p{color:#9ca3af}</style></head><body>' +
    '<div class="card"><h1>You are not signed in</h1><p>Open any ElkassabgiData library (hfdatalibrary.com, econdatalibrary.com) and choose <strong>Log in</strong> to access your account and API key.</p></div></body></html>';
}

function renderAccountPage(user, opts) {
  opts = opts || {};
  const name = htmlEncode(user.name || user.email || 'your account');
  const email = htmlEncode(user.email || '');
  const key = htmlEncode(user.api_key || '');
  const exp = user.api_key_expires_at ? htmlEncode(String(user.api_key_expires_at).slice(0, 10)) : 'no expiry';
  const inst = htmlEncode(user.institution || '—');
  const country = htmlEncode(user.country || '—');
  const role = htmlEncode(user.role || '—');
  // Raw (un-dashed) values for the editable profile form fields.
  const fname = htmlEncode(user.name || '');
  const finst = htmlEncode(user.institution || '');
  const fcountry = htmlEncode(user.country || '');
  const frole = htmlEncode(user.role || '');
  const profileIncomplete = !(user.institution && user.country && user.role);
  const orcid = user.orcid_id ? '<span class="lk">ORCID linked</span>' : '';
  const google = user.google_id ? '<span class="lk">Google linked</span>' : '';
  const notice = opts.notice ? '<div class="ok">' + htmlEncode(opts.notice) + '</div>' : '';
  const S = "body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;margin:0;padding:2rem 1rem;display:flex;justify-content:center}" +
    ".card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:1.8rem;max-width:560px;width:100%}" +
    "h1{font-size:1.2rem;color:#d4a843;margin:.2rem 0 .3rem}.sub{color:#9ca3af;font-size:.9rem;margin-bottom:1.3rem}" +
    "h2{font-size:.95rem;color:#e5e7eb;margin:1.4rem 0 .5rem;border-top:1px solid #2a3550;padding-top:1.1rem}" +
    ".key{width:100%;box-sizing:border-box;padding:.6rem;border-radius:8px;border:1px solid #2a3550;background:#0f1729;color:#d4a843;font-family:ui-monospace,Consolas,monospace;font-size:.95rem}" +
    ".hint{color:#6b7280;font-size:.8rem;margin:.35rem 0 0}.exp{color:#9ca3af;font-size:.82rem;margin:.4rem 0 0}" +
    "dl{display:grid;grid-template-columns:auto 1fr;gap:.35rem 1rem;margin:.2rem 0;font-size:.9rem}dt{color:#9ca3af}dd{margin:0;color:#e5e7eb}" +
    "button{background:#d4a843;color:#0f1729;border:0;border-radius:8px;padding:.55rem 1.1rem;font-weight:700;cursor:pointer;font-size:.9rem}" +
    "button.ghost{background:transparent;color:#d4a843;border:1px solid rgba(212,168,67,.5)}" +
    ".row{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:.8rem}" +
    ".ok{background:rgba(52,211,153,.12);border:1px solid rgba(52,211,153,.4);color:#a7f3d0;border-radius:8px;padding:.6rem .8rem;font-size:.85rem;margin-bottom:1rem}" +
    ".lk{display:inline-block;background:#0f1729;border:1px solid #2a3550;color:#9ca3af;border-radius:6px;padding:.15rem .5rem;font-size:.78rem;margin-right:.4rem}" +
    ".fl{display:block;font-size:.78rem;color:#9ca3af;margin:.55rem 0 .15rem}.fld{width:100%;box-sizing:border-box;padding:.5rem;border-radius:8px;border:1px solid #2a3550;background:#0f1729;color:#e5e7eb;font-size:.9rem}" +
    ".warn{background:rgba(212,168,67,.12);border:1px solid rgba(212,168,67,.4);color:#f0d090;border-radius:8px;padding:.55rem .75rem;margin:.3rem 0 .1rem;font-size:.82rem}" +
    ".btnlink{display:inline-block;text-decoration:none;background:transparent;color:#d4a843;border:1px solid rgba(212,168,67,.5);border-radius:8px;padding:.55rem 1.1rem;font-weight:700;font-size:.9rem}" +
    ".danger h2{color:#f87171}.danger .warn{background:rgba(248,113,113,.1);border-color:rgba(248,113,113,.4);color:#fca5a5}.del{background:#dc2626;color:#fff}";
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<meta name="robots" content="noindex,nofollow"><title>Your ElkassabgiData account</title><style>' + S + '</style></head><body>' +
    '<div class="card">' + notice +
    '<h1>Your ElkassabgiData account</h1><div class="sub">Signed in as <strong style="color:#e5e7eb">' + name + '</strong> &middot; ' + email + '</div>' +
    '<h2>API key</h2>' +
    '<input class="key" readonly value="' + key + '" aria-label="Your API key" onfocus="this.select()">' +
    '<p class="hint">Use this in the <span style="font-family:ui-monospace,Consolas,monospace">X-API-Key</span> header (or <span style="font-family:ui-monospace,Consolas,monospace">?api_key=</span>) to download data from any ElkassabgiData library. Click the field to select it, then copy.</p>' +
    '<p class="exp">Valid until <strong>' + exp + '</strong>. One key works across every library.</p>' +
    '<form method="POST" action="/account/regenerate-key" class="row"><button type="submit" class="ghost">Regenerate key</button></form>' +
    '<p class="hint">Regenerating issues a new key value and invalidates the old one — update any scripts.</p>' +
    '<h2>Profile</h2>' +
    (profileIncomplete ? '<div class="warn">Complete your institution, country and role to download data.</div>' : '') +
    '<form method="POST" action="/account/update-profile">' +
    '<label class="fl">Name<input class="fld" name="name" value="' + fname + '" maxlength="100"></label>' +
    '<label class="fl">Institution<input class="fld" name="institution" value="' + finst + '" maxlength="200" required></label>' +
    '<label class="fl">Country<input class="fld" name="country" value="' + fcountry + '" maxlength="100" required></label>' +
    '<label class="fl">Role<input class="fld" name="role" value="' + frole + '" maxlength="100" required></label>' +
    '<div class="row"><button type="submit" class="ghost">Save profile</button></div></form>' +
    (orcid || google ? '<p style="margin-top:.6rem">' + orcid + google + '</p>' : '') +
    // resend verification — only when the email is unverified
    (user.email_verified ? '' :
      '<h2>Verify your email</h2><div class="warn">Your email isn\'t verified yet — verify it to download data.</div>' +
      '<form method="POST" action="/account/resend-verification" class="row"><button type="submit" class="ghost">Resend verification email</button></form>') +
    '<h2>Change password</h2>' +
    '<form method="POST" action="/account/change-password">' +
    '<label class="fl">Current password<input class="fld" type="password" name="current_password" autocomplete="current-password" required></label>' +
    '<label class="fl">New password<input class="fld" type="password" name="new_password" autocomplete="new-password" required></label>' +
    '<div class="row"><button type="submit" class="ghost">Change password</button></div></form>' +
    '<p class="hint">Changing your password signs out your other devices.</p>' +
    '<h2>Your data</h2><div class="row"><a href="/account/export" class="btnlink">Download my data (JSON)</a></div>' +
    '<p class="hint">Everything on file: profile, login history and download history.</p>' +
    '<h2>Session</h2><form method="POST" action="/account/logout" class="row"><button type="submit">Log out everywhere</button></form>' +
    '<div class="danger"><h2>Delete account</h2>' +
    '<div class="warn">This permanently removes your account and personal data across every ElkassabgiData library. It cannot be undone.</div>' +
    '<form method="POST" action="/account/delete">' +
    '<label class="fl">Confirm your password<input class="fld" type="password" name="password" autocomplete="current-password" required></label>' +
    '<label class="fl">Type DELETE to confirm<input class="fld" name="confirm" placeholder="DELETE" required></label>' +
    '<div class="row"><button type="submit" class="del">Delete my account</button></div></form></div>' +
    '<h2>About your ElkassabgiData account</h2>' +
    '<p class="hint" style="line-height:1.6">One free account works across every ElkassabgiData library (hfdatalibrary.com, econdatalibrary.com, and more) &mdash; sign in once per site, and sessions last 30 days. Your API key above works everywhere via the <span style="font-family:ui-monospace,Consolas,monospace">X-API-Key</span> header. &ldquo;Log out everywhere&rdquo; ends every session at once. Forgot your password? Reset it at <a href="https://hfdatalibrary.com/pages/reset" style="color:#d4a843">hfdatalibrary.com/pages/reset</a>.</p>' +
    '</div></body></html>';
}

// The `onfocus="this.select()"` above is a benign inline handler; CSP has no
// script-src (default-src 'none'), so it is inert if the browser blocks inline
// handlers — the field is still selectable manually. No secret depends on JS.
async function handleAccountGet(request, env) {
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 200, headers: accountPageHeaders });
  return new Response(renderAccountPage(user, {}), { status: 200, headers: accountPageHeaders });
}
async function handleAccountRegenerate(request, env, ip, ua) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  const newKey = 'hfd_' + generateId();
  const newExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
  await env.DB.prepare('UPDATE users SET api_key = ?, api_key_expires_at = ? WHERE id = ?')
    .bind(newKey, newExpires, user.id).run();
  const fresh = await getIdpSessionUser(request, env);
  return new Response(renderAccountPage(fresh || { ...user, api_key: newKey, api_key_expires_at: newExpires }, { notice: 'Your API key was regenerated. The previous key no longer works.' }), { status: 200, headers: accountPageHeaders });
}
async function handleAccountLogout(request, env) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (user) {
    // Log out EVERYWHERE means EVERY session, not an enumerated subset.
    //
    // This read `kind IN ('idp_master','family_access')`. Legacy web sessions carry
    // kind = NULL — the column was added by 2026-07-17-m1-sso.sql with no default and
    // createSession has never set it — and SQL `IN` cannot match NULL. So the control that
    // exists to end every session silently skipped the largest group of them. Measured
    // against production on 2026-08-01: 384 live NULL-kind sessions versus 164 idp_master
    // and 3 family_access. Both the Google callback and the ORCID callback create them and
    // set hfd_session on api.hfdatalibrary.com, so this is the ordinary state for most of
    // the 364 Google and 16 ORCID users.
    //
    // What that cost: someone who loses a laptop clicks "log out everywhere", is told it
    // worked, and their session lives its full 30 days. /v1/auth/sso authenticates from
    // exactly that cookie, so the next person to use that browser gets their api_key and
    // their name handed to econdatalibrary — and for an is_admin row, the admin console.
    //
    // Routed through the ONE helper rather than a hand-rolled pair of statements. Deleting
    // sessions and revoking chains still leaves live download tokens, pending-2FA rows,
    // unspent SSO codes and outstanding password-reset links — each of them a way back into
    // the account this button promises to close. revokeAllUserCredentials is the only place
    // that knows the full list, it deletes sessions by user_id with no kind predicate (so
    // the NULL-blindness above cannot recur), and five other call sites already use it.
    await revokeAllUserCredentials(env, user.id);
  } else {
    // No valid session — still clear whatever ekd_session cookie is present.
    const cookie = request.headers.get('cookie') || '';
    const rawEkd = readCookie(cookie, 'ekd_session', '[A-Za-z0-9_-]+');
    if (rawEkd) { const idHash = await sha256Hex(rawEkd); await env.DB.prepare("DELETE FROM sessions WHERE id = ?").bind(idHash).run(); }
  }
  const clear = 'ekd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0';
  return new Response(renderSignedOutPage(), { status: 200, headers: { ...accountPageHeaders, 'Set-Cookie': clear } });
}

// 1.3: update-profile on accounts.*/account. Mirrors the api.* handleUpdateProfile
// logic (length caps, profile_complete recompute) but authed via the ekd_session
// cookie (getIdpSessionUser) + CSRF via assertSameOriginForm, form input, HTML re-render.
async function handleAccountUpdateProfile(request, env) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  let form;
  try { form = await request.formData(); } catch { return new Response(renderAccountPage(user, { notice: 'Could not read the form — please try again.' }), { status: 400, headers: accountPageHeaders }); }
  const name = (form.get('name') || '').toString().trim().slice(0, 100);
  const institution = (form.get('institution') || '').toString().trim().slice(0, 200);
  const country = (form.get('country') || '').toString().trim().slice(0, 100);
  const role = (form.get('role') || '').toString().trim().slice(0, 100);
  if (!institution || !country || !role) {
    return new Response(renderAccountPage(user, { notice: 'Institution, country and role are all required.' }), { status: 200, headers: accountPageHeaders });
  }
  // The .slice() caps above bound the length but not the character set, so this form was the
  // second way past the Latin-only filter that registration applies: sign up with a clean
  // institution, then edit it here to anything at all. These four columns are exactly what the
  // public stats page renders (world map, institutions list) and what the admin notification
  // emails print, so whatever is stored here is displayed to strangers and to the owner. Same
  // check as handleRegister and the api.* handleUpdateProfile twin.
  //
  // Via latinOkOrUnchanged, for the same reason as that twin: renderAccountPage writes the
  // stored row back into these four inputs, so every save re-posts values the user did not
  // touch — including a name that Google supplied in a non-Latin script and that no filter
  // ever saw. Checking those unconditionally made the Save button impossible to satisfy for
  // those users, and this handler is the only thing on accounts.* that sets
  // profile_complete = 1, so it took their downloads with it. A value identical to the one
  // in the column is not something the user is introducing; a changed one still has to pass.
  // `name` stays optional — latinOkOrUnchanged treats blank as fine, and institution,
  // country and role are already required non-blank by the check above.
  if (!latinOkOrUnchanged(name, user.name) || !latinOkOrUnchanged(institution, user.institution) ||
      !latinOkOrUnchanged(country, user.country) || !latinOkOrUnchanged(role, user.role)) {
    return new Response(renderAccountPage(user, { notice: 'Name, institution, country and role must use English/Latin letters only.' }), { status: 200, headers: accountPageHeaders });
  }
  await env.DB.prepare('UPDATE users SET name = ?, institution = ?, country = ?, role = ?, profile_complete = 1 WHERE id = ?')
    .bind(name || user.name || '', institution, country, role, user.id).run();
  const fresh = await getIdpSessionUser(request, env);
  return new Response(renderAccountPage(fresh || user, { notice: 'Profile saved.' }), { status: 200, headers: accountPageHeaders });
}

// 1.3: change-password on accounts.* (mirrors api.* handleChangePassword; preserves THIS ekd_session).
async function handleAccountChangePassword(request, env) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  let form;
  try { form = await request.formData(); } catch { return new Response(renderAccountPage(user, { notice: 'Could not read the form — please try again.' }), { status: 400, headers: accountPageHeaders }); }
  const current = (form.get('current_password') || '').toString();
  const next = (form.get('new_password') || '').toString();
  if (!current || !next) return new Response(renderAccountPage(user, { notice: 'Both your current and new password are required.' }), { status: 200, headers: accountPageHeaders });
  const strength = checkPasswordStrength(next);
  if (!strength.ok) return new Response(renderAccountPage(user, { notice: strength.error }), { status: 200, headers: accountPageHeaders });
  const dbUser = await env.DB.prepare('SELECT password_hash FROM users WHERE id = ?').bind(user.id).first();
  if (!dbUser || !(await verifyPassword(current, dbUser.password_hash))) {
    return new Response(renderAccountPage(user, { notice: 'Your current password is incorrect.' }), { status: 200, headers: accountPageHeaders });
  }
  const newHash = await hashPassword(next);
  await env.DB.prepare('UPDATE users SET password_hash = ? WHERE id = ?').bind(newHash, user.id).run();
  // Sign out other devices; preserve THIS accounts.* session (user.session_id = the ekd_session hash).
  // Until 2026-07-31 that was one DELETE on `sessions`, which is exactly the gap the api.*
  // twin had: the notice below promises the other devices are gone, and the family SSO
  // refresh token in `sso_refresh_tokens` went on minting access tokens for every family
  // site for 30 more days. This host is the identity provider — it is the one place where
  // "signed out everywhere" has to be literally true. Same opts as handleChangePassword and
  // for the same reasons: the current password was verified two lines up, so this is the
  // owner doing hygiene — keep their tab, keep their API key, keep their Google/ORCID link,
  // keep their authenticator. See the reasoning written out at handleChangePassword.
  await revokeAllUserCredentials(env, user.id, { keepSessionId: user.session_id });
  return new Response(renderAccountPage(user, { notice: 'Password changed. Your other devices have been signed out.' }), { status: 200, headers: accountPageHeaders });
}

// 1.3: GDPR export on accounts.* (mirrors api.* handleDataExport; GET read, no state change so no CSRF).
async function handleAccountExport(request, env) {
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  const profile = await env.DB.prepare('SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, is_vip, totp_enabled, newsletter_subscribed, created_at, last_login_at, login_count, download_count, total_bytes_downloaded FROM users WHERE id = ?').bind(user.id).first();
  const logins = await env.DB.prepare('SELECT ip_address, user_agent, country, success, timestamp FROM login_history WHERE user_id = ? ORDER BY timestamp DESC').bind(user.id).all();
  const downloads = await env.DB.prepare('SELECT ticker, version, endpoint, ip_address, bytes_served, timestamp FROM download_log WHERE user_id = ? ORDER BY timestamp DESC').bind(user.id).all();
  const fname = 'elkassabgidata_' + (user.email || 'account') + '_' + new Date().toISOString().slice(0, 10) + '.json';
  return new Response(JSON.stringify({ exported_at: new Date().toISOString(), profile, login_history: logins.results, download_history: downloads.results }, null, 2), {
    status: 200,
    headers: { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="' + fname + '"', 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'no-store' }
  });
}

// 1.3: resend verification on accounts.* (mirrors api.* handleResendVerification).
async function handleAccountResendVerification(request, env) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  if (user.email_verified) return new Response(renderAccountPage(user, { notice: 'Your email is already verified.' }), { status: 200, headers: accountPageHeaders });
  // Same per-account throttle as the api.* twin. This surface already HAD a visible button,
  // so it was the one an abuser could find: every press sent real mail and wrote a
  // password_resets row, with nothing bounding either.
  const rlv = await checkRateLimit(env, 'resend:u' + user.id, 'api:resend');
  if (!rlv.ok) {
    return new Response(renderAccountPage(user, {
      notice: 'We have already sent several verification emails recently — please check your inbox and spam folder, then try again a little later.'
    }), { status: 429, headers: accountPageHeaders });
  }
  const verifyToken = generateId();
  const verifyExpires = new Date(Date.now() + 86400000).toISOString();
  await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)').bind(user.id, verifyToken, verifyExpires).run();
  try { await sendEmail(env, user.email, 'Verify your ElkassabgiData account', verificationEmail(user.name, verifyToken), FROM_EMAIL, 'ElkassabgiData'); } catch (e) {}
  return new Response(renderAccountPage(user, { notice: 'Verification email sent — check your inbox (and spam).' }), { status: 200, headers: accountPageHeaders });
}

// 1.3: delete account on accounts.* (mirrors api.* handleDeleteAccount + revokes family sessions/tokens).
async function handleAccountDelete(request, env) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const user = await getIdpSessionUser(request, env);
  if (!user) return new Response(renderSignedOutPage(), { status: 401, headers: accountPageHeaders });
  let form;
  try { form = await request.formData(); } catch { return new Response(renderAccountPage(user, { notice: 'Could not read the form — please try again.' }), { status: 400, headers: accountPageHeaders }); }
  const password = (form.get('password') || '').toString();
  const confirm = (form.get('confirm') || '').toString();
  if (confirm !== 'DELETE') return new Response(renderAccountPage(user, { notice: 'Type DELETE (all caps) in the confirm box to delete your account.' }), { status: 200, headers: accountPageHeaders });
  const dbUser = await env.DB.prepare('SELECT password_hash, email FROM users WHERE id = ?').bind(user.id).first();
  if (!dbUser || !(await verifyPassword(password, dbUser.password_hash))) {
    return new Response(renderAccountPage(user, { notice: 'Incorrect password — your account was NOT deleted.' }), { status: 200, headers: accountPageHeaders });
  }
  // Remove every trace: family sessions/tokens + api.* rows + the user.
  // Deleting the account must revoke everything it could still be reached by, not just its
  // sessions: a live download token or an unspent password-reset link outliving the account
  // is the same defect as a session doing so.
  await revokeAllUserCredentials(env, user.id);
  await env.DB.prepare('DELETE FROM sso_refresh_tokens WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM login_history WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM download_log WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM password_resets WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(user.id).run();
  // sso_codes (one per popup login) + newsletter_prefs (one per registration) also FK->users;
  // without clearing them the final users DELETE hits a FOREIGN KEY constraint and fails for any
  // real user (surfaced live 2026-07-20 during the throwaway delete test). The api.* handler
  // handleDeleteAccount has the same latent gap — noted for M3, not touched here (G-4/G-6).
  await env.DB.prepare('DELETE FROM sso_codes WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM newsletter_prefs WHERE user_id = ?').bind(user.id).run();
  await env.DB.prepare('DELETE FROM users WHERE id = ?').bind(user.id).run();
  try { await sendEmail(env, ADMIN_NOTIFY, 'Account deleted: ' + dbUser.email, '<p>User <strong>' + htmlEncode(dbUser.email) + '</strong> self-deleted via accounts.elkassabgidata.com/account. All personal data removed.</p>'); } catch (e) {}
  const clear = 'ekd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0';
  return new Response(renderSignedOutPage(), { status: 200, headers: { ...accountPageHeaders, 'Set-Cookie': clear } });
}

// Temporary CSP-violation sink (report-uri on the auth page) — logs exactly what a
// browser's CSP blocked (e.g. a Turnstile connection), visible in `wrangler tail`.
// Remove once the Turnstile connectivity is confirmed.
async function handleCspReport(request, env) {
  let body = '';
  try { body = await request.text(); } catch (e) {}
  console.log(JSON.stringify({ evt: 'csp_report', body: body.slice(0, 900) }));
  return new Response(null, { status: 204, headers: { 'Cache-Control': 'no-store' } });
}

// ══════════════════════════════════════════════════════════════════
// ── Family SSO M2b-2b — centralized Google + ORCID broker (accounts.*) ──
// ══════════════════════════════════════════════════════════════════
// One family broker terminates Google/ORCID for every site. The client's family
// PKCE (state + code_challenge) is preserved through the provider detour in a
// provider-bound, single-use sso_oauth_state row (10-min TTL, datetime() arith).
// On success we mint the SAME family code (mintCodeAndRedirect) + ekd_session as
// the password path, so the SDK exchange is identical regardless of login method.
// SECURITY: Google id_token is RS256-verified against Google JWKS (aud/iss/exp/
// email_verified) — not merely userinfo-trusted. ORCID accounts are linked ONLY
// by stored orcid_id, NEVER by ORCID-supplied email (account-takeover ban).
// The api.* handleGoogleCallback/handleOrcidCallback stay byte-for-byte (M3).

// Google JWKS, cached at module scope (~1h). Fail-closed: a fetch error throws,
// the callers treat a null verify as auth failure.
let _googleJwks = null, _googleJwksAt = 0;
const GOOGLE_JWKS_TTL_MS = 3600 * 1000;
async function getGoogleJwks(force) {
  const now = Date.now();
  if (!force && _googleJwks && (now - _googleJwksAt) < GOOGLE_JWKS_TTL_MS) return _googleJwks;
  const r = await fetch('https://www.googleapis.com/oauth2/v3/certs');
  if (!r.ok) throw new Error('google_jwks_fetch_' + r.status);
  const data = await r.json();
  _googleJwks = (data && data.keys) || [];
  _googleJwksAt = now;
  return _googleJwks;
}
function b64urlToBytes(s) {
  s = String(s || '').replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) s += '=';
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
// RS256-verify a Google id_token + full claim checks. Returns {sub,email} or null.
async function verifyGoogleIdToken(env, idToken) {
  try {
    const parts = String(idToken || '').split('.');
    if (parts.length !== 3) return null;
    const [h, p, s] = parts;
    const header = JSON.parse(new TextDecoder().decode(b64urlToBytes(h)));
    if (header.alg !== 'RS256') return null;
    let jwks = await getGoogleJwks();
    let jwk = jwks.find((k) => k.kid === header.kid && (k.alg === 'RS256' || !k.alg));
    if (!jwk) {
      // kid not in cache → Google likely rotated keys; force one refetch.
      jwks = await getGoogleJwks(true);
      jwk = jwks.find((k) => k.kid === header.kid && (k.alg === 'RS256' || !k.alg));
    }
    if (!jwk) return null;
    const key = await crypto.subtle.importKey(
      'jwk', { kty: jwk.kty, n: jwk.n, e: jwk.e, alg: 'RS256', ext: true },
      { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['verify']);
    const ok = await crypto.subtle.verify(
      'RSASSA-PKCS1-v1_5', key, b64urlToBytes(s), new TextEncoder().encode(h + '.' + p));
    if (!ok) return null;
    const c = JSON.parse(new TextDecoder().decode(b64urlToBytes(p)));
    if (c.iss !== 'accounts.google.com' && c.iss !== 'https://accounts.google.com') return null;
    if (c.aud !== env.GOOGLE_CLIENT_ID) return null;
    if (!c.exp || (c.exp * 1000) <= Date.now()) return null;
    if (c.email_verified !== true && c.email_verified !== 'true') return null;
    if (!c.sub || !c.email) return null;
    return { sub: String(c.sub), email: String(c.email).toLowerCase() };
  } catch (e) {
    console.error(JSON.stringify({ evt: 'google_idtoken_verify_error', msg: e && e.message }));
    return null;
  }
}
// Atomic single-use consume, provider-bound (mix-up defense) + TTL. Returns the
// row or null. Mirrors the sso_codes burn (changes===1 gate).
async function consumeOauthState(env, brokerState, expectedProvider) {
  if (!brokerState) return null;
  // §EXPIRY-COMPARE: datetime() on both sides. Already correct — the single writer
  // uses datetime('now','+10 minutes') — wrapped to keep the invariant uniform.
  const claim = await env.DB.prepare(
    "UPDATE sso_oauth_state SET used=1 WHERE state=? AND used=0 AND provider=? AND datetime(expires_at)>datetime('now')"
  ).bind(brokerState, expectedProvider).run();
  if (!claim.meta || claim.meta.changes !== 1) return null;
  return await env.DB.prepare('SELECT * FROM sso_oauth_state WHERE state=?').bind(brokerState).first();
}
// Generic broker error page. The internal reason is logged, NEVER shown (no
// leak); the user gets a clear next step. no-store, framed-denied, no-referrer.
function oauthErrorPage(reason) {
  console.log(JSON.stringify({ evt: 'broker_error', reason }));
  return new Response(
    '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Sign-in problem</title><div style="font-family:system-ui,sans-serif;max-width:420px;margin:3rem auto;padding:0 1rem;text-align:center;color:#e5e7eb">' +
    '<h1 style="color:#d4a843;font-size:1.15rem">We couldn’t complete sign-in</h1>' +
    '<p style="color:#9ca3af">Something went wrong. Please close this window and try again, or sign in with your email and password.</p></div>',
    { status: 400, headers: {
      'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store',
      'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY', 'X-Content-Type-Options': 'nosniff',
      'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
    } });
}
// Broker success tail: recover the (active) registry row for the stored client
// origin, then mint the family code + ekd_session — same 303 shape as the
// password path. family_state/family_code_challenge come from the client's /start.
async function brokerLoginRedirect(env, userId, st, ip, ua) {
  const reg = await getRegistry(env);
  const client = reg.get(st.client_origin);
  if (!client || client.status !== 'active' || !client.redirect_exact) return oauthErrorPage('client_unavailable');

  // THIRD instance of the same defect, found by enumerating rather than stopping at the two the
  // finding named. handleAccountsLogin (password) already refuses to mint an IdP session for a
  // totp_enabled account until a code is verified; this broker — the Google/ORCID path on the
  // accounts host — did not consult totp_enabled at all. So the second factor was enforced on
  // the password door and skipped on both provider doors, exactly as it was on api.*.
  //
  // No new UI is required: renderTwoFactorPage and POST /login/2fa already exist for the
  // password flow, and handleAccounts2faVerify reads the authorize params back out of the form
  // that page embeds and then performs the very redirect this function would have done.
  // Rebuilding `p` from the stashed OAuth state is the whole join — clientId, redirectUri,
  // state and codeChallenge are the same four values either flow carries.
  const brokerUser = await env.DB.prepare('SELECT totp_enabled FROM users WHERE id = ?').bind(userId).first();
  if (brokerUser && brokerUser.totp_enabled) {
    const pendingToken = generateId();
    const pendingExpires = new Date(Date.now() + 10 * 60 * 1000).toISOString();
    await env.DB.prepare('INSERT INTO totp_pending (token, user_id, expires_at, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)')
      .bind(pendingToken, userId, pendingExpires, ip, ua).run();
    return new Response(renderTwoFactorPage(pendingToken, {
      clientId: st.client_origin,
      redirectUri: client.redirect_exact,
      state: st.family_state,
      codeChallenge: st.family_code_challenge,
      method: 'S256',
    }, ''), { status: 200, headers: authPageHeaders });
  }

  const idp = await createIdpSession(env, userId, ip, ua);
  const resp = await mintCodeAndRedirect(env, userId, st.client_origin, client.redirect_exact, st.family_state, st.family_code_challenge, 303);
  resp.headers.append('Set-Cookie', idp.cookie);
  return resp;
}

// GET /v1/auth/{google,orcid}/start — validate the family authorize params (no
// provider hit on failure), stash provider-bound single-use state, 303 to the
// provider. Google uses PKCE S256; ORCID uses the proven /authenticate scope.
async function startFamilyOAuth(request, env, provider, ip, url) {
  const p = {
    clientId: url.searchParams.get('client_id') || '',
    redirectUri: url.searchParams.get('redirect_uri') || '',
    state: url.searchParams.get('state') || '',
    codeChallenge: url.searchParams.get('code_challenge') || '',
    method: url.searchParams.get('code_challenge_method') || '',
  };
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('Invalid request', { status: v.status, headers: { 'Cache-Control': 'no-store' } });
  // ENFORCED per-IP cap (distinct bucket) BEFORE the persistent sso_oauth_state
  // write + outbound provider token exchange this seeds. Unlike the password
  // authorize path, /start writes pre-authentication, so shadow mode is not safe.
  const rl = await rateLimit(env, 'oauth_start_ip', ip, AUTHZ_IP_MAX, 60, true);
  if (!rl.ok) return new Response('Too many requests', { status: 429, headers: { 'Cache-Control': 'no-store', 'Retry-After': String(rl.retryAfter || 60) } });
  const brokerState = generateToken() + generateToken(); // 256-bit opaque
  let verifier = null, nonce = null, providerUrl;
  if (provider === 'google') {
    verifier = generateToken() + generateToken();
    const challenge = await pkceS256(verifier);
    const g = new URL('https://accounts.google.com/o/oauth2/v2/auth');
    g.searchParams.set('client_id', env.GOOGLE_CLIENT_ID);
    g.searchParams.set('response_type', 'code');
    g.searchParams.set('scope', 'openid email profile');
    g.searchParams.set('redirect_uri', OAUTH_REDIRECT_GOOGLE_ACCOUNTS);
    g.searchParams.set('code_challenge', challenge);
    g.searchParams.set('code_challenge_method', 'S256');
    g.searchParams.set('state', brokerState);
    g.searchParams.set('prompt', 'select_account');
    providerUrl = g.toString();
  } else {
    const o = new URL('https://orcid.org/oauth/authorize');
    o.searchParams.set('client_id', env.ORCID_CLIENT_ID);
    o.searchParams.set('response_type', 'code');
    o.searchParams.set('scope', '/authenticate');
    o.searchParams.set('redirect_uri', OAUTH_REDIRECT_ORCID_ACCOUNTS);
    o.searchParams.set('state', brokerState);
    providerUrl = o.toString();
  }
  await env.DB.prepare(
    'INSERT INTO sso_oauth_state (state,provider,client_origin,family_state,family_code_challenge,provider_code_verifier,nonce,link_user_id,used,expires_at) ' +
    "VALUES (?,?,?,?,?,?,?,NULL,0,datetime('now','+10 minutes'))"
  ).bind(brokerState, provider, p.clientId, p.state, p.codeChallenge, verifier, nonce).run();
  return new Response(null, { status: 303, headers: { 'Location': providerUrl, 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store' } });
}

// GET /v1/auth/google/callback — consume state, exchange code+PKCE, RS256-verify
// id_token, link by google_id=sub (else pin to a verified-email match, else
// create a verified account), then broker success tail.
async function handleAccountsGoogleCallback(request, env, ip, ua, country) {
  const u = new URL(request.url);
  const code = u.searchParams.get('code');
  const brokerState = u.searchParams.get('state') || '';
  if (u.searchParams.get('error') || !code || !brokerState) return oauthErrorPage('provider_denied');
  const st = await consumeOauthState(env, brokerState, 'google');
  if (!st) return oauthErrorPage('state_invalid');
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      code,
      grant_type: 'authorization_code',
      redirect_uri: OAUTH_REDIRECT_GOOGLE_ACCOUNTS,
      code_verifier: st.provider_code_verifier || '',
    }).toString(),
  });
  if (!tokenRes.ok) return oauthErrorPage('token_exchange_failed');
  const tokenData = await tokenRes.json();
  const verified = await verifyGoogleIdToken(env, tokenData.id_token);
  if (!verified) return oauthErrorPage('idtoken_invalid');
  const sub = verified.sub, email = verified.email;

  let user = await env.DB.prepare('SELECT * FROM users WHERE google_id=?').bind(sub).first();
  if (!user) {
    // No google_id match yet. Google has PROVEN this email (email_verified===true),
    // but we adopt a pre-existing same-email row ONLY when it is safe to do so.
    // The email owner may claim their email — they must never silently inherit an
    // attacker-provisioned or foreign-identity account.
    const byEmail = await env.DB.prepare('SELECT * FROM users WHERE email=?').bind(email).first();
    if (byEmail) {
      if (byEmail.google_id === sub) {
        // Already ours (e.g. a concurrent callback pinned it first) — same identity.
        user = byEmail;
      } else if (Number(byEmail.email_verified) === 1 && !byEmail.google_id) {
        // A row whose OWNER already verified this email and that is not linked to
        // any Google identity: the verified email owner == the Google email owner.
        // Race-guarded pin: only adopt when THIS call actually set google_id.
        const link = await env.DB.prepare(
          "UPDATE users SET google_id=? WHERE id=? AND (google_id IS NULL OR google_id='')"
        ).bind(sub, byEmail.id).run();
        if (link.meta && link.meta.changes === 1) {
          user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(byEmail.id).first();
        } else {
          return oauthErrorPage('account_link_conflict'); // lost a concurrent link race
        }
      } else {
        // Unverified same-email row (possible pre-hijack squat) OR one bound to a
        // DIFFERENT google_id (email reassigned by the provider). Never auto-merge
        // or log in — fail closed. The owner can password-login or verify first.
        return oauthErrorPage('account_link_conflict');
      }
    }
  }
  if (!user) {
    // No same-email row at all → create. Google email is verified; family default
    // newsletter_subscribed=0.
    const apiKey = 'hfd_' + generateId();
    const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    const unsub = generateId();
    const isAdmin = ADMIN_EMAILS.includes(email) ? 1 : 0;
    const rndPw = generateId() + generateId();
    const pwHash = await hashPassword(rndPw);
    const name = email.split('@')[0];
    await env.DB.prepare(
      'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, google_id, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, 0)'
    ).bind(name, email, pwHash, '', country || '', '', apiKey, apiKeyExpires, isAdmin, unsub, ip, ua, sub).run();
    user = await env.DB.prepare('SELECT * FROM users WHERE google_id=?').bind(sub).first();
    try {
      await sendEmail(env, ADMIN_NOTIFY, 'New registration via Google (family SSO): ' + name,
        adminNotificationEmail({ name, email, institution: '(via Google / accounts)', country, role: 'Not specified' }, ip, ua, country));
    } catch (e) { /* non-fatal */ }
  }
  if (!user || !user.is_active) return oauthErrorPage('account_unavailable');
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await brokerLoginRedirect(env, user.id, st, ip, ua);
}

// GET /v1/auth/orcid/callback — consume state, exchange code (/authenticate),
// link ONLY by stored orcid_id (NEVER by ORCID-supplied email). New ORCID with a
// unique public email → create (email_verified=0); no/colliding email → honest
// error (register with email or Google). Then broker success tail.
// ── ORCID link-or-register seam ──
// ORCID's /authenticate scope never returns email, so a NEW ORCID (no orcid_id
// match, no unique public email) can't be auto-created. Instead of dead-ending,
// carry the VERIFIED orcid_id to the auth page in a short-lived HMAC-signed token
// (10 min): the user signs in (link the ORCID to their existing account) or
// registers with an email (create + set orcid_id). A valid token proves ORCID auth
// was completed, so it also stands in for the register CAPTCHA (ORCID = the human check).
// ── The prefill token is BOUND, AUDIENCED and SINGLE-USE (2026-07-31) ──
// The HMAC proves ORCID's OAuth was completed for this iD. Until now that was the
// ONLY thing it proved: the token named no browser, no host and no single use, so
// it was a transferable bearer credential. The attack that made possible:
//   1. Attacker completes ORCID with a throwaway iD at api.hfdatalibrary.com and
//      lifts the token out of their OWN address bar.
//   2. Attacker sends the victim the genuine accounts.* /authorize link carrying
//      it — right domain, right TLS, our branding.
//   3. Victim signs in with their own email and password. loginAndRedirect called
//      maybeLinkOrcid unconditionally, so the ATTACKER's orcid_id was written onto
//      the VICTIM's row, announced only by a decorative banner.
//   4. "Sign in with ORCID" resolves an account by orcid_id, so the attacker then
//      signed in AS the victim. An iD that is a login credential had been attached
//      by a request the victim never made.
// Three changes close it and all three are needed:
//   BINDING — mint puts a 256-bit nonce in a host-only HttpOnly SameSite=Lax
//     cookie and signs sha256(nonce) into the token. A token pasted into anyone
//     else's browser has no matching cookie and never verifies. §PF-DEPLOY-ORDER:
//     on audience 'api' the cookie is enforced when present and OPTIONAL when
//     absent, because the deployed download.html POSTs /register as a cross-origin
//     fetch WITHOUT credentials, so the cookie the ORCID callback set is never
//     sent. Requiring it there does not fail the registration — it silently drops
//     the ORCID from an otherwise successful 201, right after the page promised
//     the user their iD would be linked. The absent-cookie path is exactly the
//     signed, audienced, single-use check that is live today; the moment the new
//     page ships (credentials: 'include') every request carries the cookie and
//     BINDING is unconditional again. Audience 'idp' stays strict: those pages are
//     rendered by this worker, so they can never lag behind it.
//   AUDIENCE — the signed body names the host that minted it ('api' or 'idp'), so
//     an api.*-minted token can never authorize a link at accounts.*, or the
//     reverse. The cookie name differs per audience too, belt and braces.
//   SINGLE USE — mint writes sha256(token) into oauth_state; the statement that
//     actually attaches the iD burns that row (DELETE, changes === 1) so a replay
//     finds nothing. Same burn-on-consume shape as consumeApiOauthState.
// Verification that only DISPLAYS the token — the banner, the CAPTCHA stand-in, a
// re-render after a form error — must NOT burn it; only the write consumes.
//
// Field separator: the signing input joins fields with '.', so a field containing
// a '.' could shift the parse. None can. aud is a literal key of the map below;
// orcidId is regex-checked to the ORCID shape (digits, dashes, trailing X) at mint
// AND at verify; nameB64 and sig are base64url; nonceHash is lowercase hex; expMs
// is decimal. Verify additionally re-serialises the signing input from exactly the
// split parts and demands 6 of them, so an ambiguous split cannot survive the HMAC.
const ORCID_PREFILL_COOKIE = { api: '__Host-hfd_orcid_pf', idp: '__Host-ekd_orcid_pf' };
const ORCID_PREFILL_TTL_MS = 10 * 60 * 1000;
const ORCID_ID_RE = /^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/;
// What we tell someone whose ORCID confirmation did not survive the time it took them to
// fill the form. It has to name the outcome (not linked), reassure about the part that DID
// work (the account exists), and give the exact control that fixes it — the account page
// already has a "Link ORCID account" button, so this is one click, not a support ticket.
const ORCID_NOT_LINKED_NOTICE =
  'Your ORCID iD was not linked to this account: the ORCID confirmation could not be verified ' +
  '(it is only valid for 10 minutes, and filling the form often takes longer). Your account is ' +
  'created and works normally. To link your ORCID iD, open your account page and click ' +
  '"Link ORCID account".';
// __Host- prefix: a browser refuses to store such a cookie if it carries Domain,
// so no sibling host can plant one. SameSite=Lax so it still rides the top-level
// navigation back from the provider and the same-site form POST that follows.
function orcidPrefillCookie(aud, nonce) {
  return `${ORCID_PREFILL_COOKIE[aud]}=${nonce}; Path=/; Max-Age=600; HttpOnly; Secure; SameSite=Lax`;
}
function readOrcidPrefillNonce(request, aud) {
  const name = ORCID_PREFILL_COOKIE[aud];
  if (!name || !request) return null;
  const m = (request.headers.get('cookie') || '')
    .match(new RegExp('(?:^|;\\s*)' + name + '=([A-Za-z0-9_-]+)'));
  return m ? m[1] : null;
}
// Returns { token, cookie } — BOTH must reach the browser or the token is inert.
// null means "no token could be minted": a missing CONSENT_HMAC_SECRET, an
// audience we don't serve, or an iD that isn't ORCID-shaped. Callers must treat
// null as an honest error, never as "carry on without the link" — this is the
// fail-closed edge, and an unsigned prefill must never become an accepted one.
async function mintOrcidPrefill(env, orcidId, name, aud) {
  if (!env.CONSENT_HMAC_SECRET) return null;
  if (!ORCID_PREFILL_COOKIE[aud]) return null;
  if (!ORCID_ID_RE.test(String(orcidId || ''))) return null;
  const nonce = generateToken() + generateToken();     // 256-bit; never leaves the browser
  const nonceHash = await sha256Hex(nonce);
  const expMs = Date.now() + ORCID_PREFILL_TTL_MS;
  const nameB64 = b64url(new TextEncoder().encode((name || '').slice(0, 100)));
  const bodyStr = aud + '.' + orcidId + '.' + nameB64 + '.' + nonceHash + '.' + expMs;
  const sig = await hmacSign(env.CONSENT_HMAC_SECRET, bodyStr);
  const token = bodyStr + '.' + sig;
  // Single-use ledger. oauth_state already exists with exactly the columns needed
  // (state PK, provider, expires_at); provider 'orcid_prefill' is a value no
  // consumeApiOauthState call ever asks for, so these rows can't be mistaken for a
  // Google/ORCID CSRF state.
  await env.DB.prepare(
    "INSERT INTO oauth_state (state, user_id, provider, expires_at) VALUES (?, NULL, 'orcid_prefill', datetime('now','+10 minutes'))"
  ).bind(await sha256Hex(token)).run();
  // These rows had no cleanup at all. consumeOrcidPrefill deletes one only when a
  // link actually completes, and the note here used to claim mintApiOauthState's
  // sweep expired them "for free" — it does not: that sweep runs only when somebody
  // clicks Google/ORCID sign-in on api.hfdatalibrary.com, and this mint is reached
  // from accounts.elkassabgidata.com. So every abandoned link-or-register flow on
  // the identity provider left a permanent row, which is the unbounded growth that
  // fills D1 and stops it accepting writes — i.e. stops logins. Same sweep, same
  // table, same sampling, equally non-fatal.
  await sweepOauthState(env);
  return { token, cookie: orcidPrefillCookie(aud, nonce) };
}
// Read-only check. Returns { orcidId, name, bound, tokenHash } or null. tokenHash is
// what consumeOrcidPrefill burns; holding it does nothing on its own. bound === false
// means the nonce cookie was absent and the token was accepted on its signature alone
// (§PF-DEPLOY-ORDER) — true is the state every request reaches once the pages ship.
async function verifyOrcidPrefill(env, request, token, aud) {
  if (!env.CONSENT_HMAC_SECRET || !token) return null;
  if (!ORCID_PREFILL_COOKIE[aud]) return null;
  const parts = String(token).split('.');
  // §PF-LEGACY IS GONE, and this note records why it existed and why it had to be removed
  // on the same day. deploy.yml ships the Pages site and the Worker as two PARALLEL jobs,
  // so for the first ten minutes after a deploy some browsers still hold a token the
  // PREVIOUS worker minted. Those had FOUR parts (orcidId.nameB64.expMs.sig): no audience,
  // no nonce hash, no ledger row, because none of that existed when they were issued. For
  // that window they were accepted on their signature alone.
  //
  // Accepting them meant a token that skipped the audience check, the browser binding AND
  // the single-use ledger all at once — every protection this function exists to apply. A
  // compatibility shim is a hole with a good excuse, and the excuse expires: nothing has
  // minted the old format since the 2026-07-31 deploy and each one carried its own
  // 10-minute expMs, so the last possible legacy token died ten minutes after that deploy.
  // Keeping it any longer would have been keeping the weakest path in the whole auth
  // surface alive for no living user. Removed once the window provably closed.
  if (parts.length !== 6) return null;
  const tokAud = parts[0], orcidId = parts[1], nameB64 = parts[2],
        nonceHash = parts[3], expMsStr = parts[4], sig = parts[5];
  if (tokAud !== aud) return null;                     // minted for another surface
  if (!ORCID_ID_RE.test(orcidId)) return null;
  if (!/^[0-9a-f]{64}$/.test(nonceHash)) return null;
  const expMs = parseInt(expMsStr, 10);
  if (!expMs || Date.now() > expMs) return null;
  const expected = await hmacSign(env.CONSENT_HMAC_SECRET,
    tokAud + '.' + orcidId + '.' + nameB64 + '.' + nonceHash + '.' + expMsStr);
  if (!constantTimeEqual(sig, expected)) return null;
  // The binding: only the browser that completed ORCID holds the nonce. Present →
  // it must match, absent → see §PF-DEPLOY-ORDER above — enforced on 'idp' either
  // way, waived on 'api' only until the page that sends the cookie is live.
  const nonce = readOrcidPrefillNonce(request, aud);
  if (nonce) {
    if (!constantTimeEqual(await sha256Hex(nonce), nonceHash)) return null;
  } else if (aud !== 'api') {
    return null;
  } else {
    // Log it so the waiver is observable: once the new download.html/account.html
    // are deployed this stops appearing, and the `aud !== 'api'` branch above can
    // become an unconditional `return null`.
    console.log(JSON.stringify({ evt: 'orcid_prefill_unbound', aud }));
  }
  let name = '';
  try { name = new TextDecoder().decode(b64urlToBytes(nameB64)); } catch (e) {}
  return { orcidId, name, bound: !!nonce, tokenHash: await sha256Hex(String(token)) };
}
// (verifyLegacyOrcidPrefill removed with §PF-LEGACY — see verifyOrcidPrefill.)
// Burn the single-use row. Called ONLY by the code path that is about to write
// orcid_id. false means the token was already spent (or expired between the verify
// and here) — the caller must abandon the link rather than write anyway.
async function consumeOrcidPrefill(env, tokenHash) {
  const burn = await env.DB.prepare(
    "DELETE FROM oauth_state WHERE state = ? AND provider = 'orcid_prefill' AND datetime(expires_at) > datetime('now')"
  ).bind(tokenHash).run();
  return !!(burn.meta && burn.meta.changes === 1);
}
// The burn every caller should go through. Passing null (no prefill was sent at all)
// returns true, so `!(await burnOrcidPrefill(...))` reads the same as the older
// `pre && !(await consumeOrcidPrefill(...))`.
//
// The `if (pre.legacy) return true` line that used to sit here went with §PF-LEGACY. It
// existed because a legacy token had no ledger row, so consuming it answered false and the
// caller abandoned a link over a row that was never written. Every token now has a row, so
// there is nothing to except — and an unconditional consume is the point: a single-use
// token that some branch can skip burning is not single-use.
async function burnOrcidPrefill(env, pre) {
  if (!pre) return true;
  return await consumeOrcidPrefill(env, pre.tokenHash);
}
// Link a verified orcid_id to an account ONLY if that account has none AND no other
// account already owns it (atomic single statement → no cross-account takeover/dupe).
// Callers must additionally have proof the USER asked for this link — see the
// link_orcid checkbox in renderAuthPage. This function no longer runs on every login.
async function maybeLinkOrcid(env, request, userId, token, aud) {
  const pre = await verifyOrcidPrefill(env, request, token, aud);
  if (!pre) return;
  // Every prefill now has a ledger row, so this burn is unconditional: a token that some
  // branch could skip burning would not be single-use.
  if (!(await burnOrcidPrefill(env, pre))) return;                // replay / already spent
  await env.DB.prepare(
    "UPDATE users SET orcid_id = ? WHERE id = ? AND (orcid_id IS NULL OR orcid_id = '') " +
    "AND NOT EXISTS (SELECT 1 FROM users u2 WHERE u2.orcid_id = ? AND u2.id != ?)"
  ).bind(pre.orcidId, userId, pre.orcidId, userId).run();
}

async function handleAccountsOrcidCallback(request, env, ip, ua, country) {
  const u = new URL(request.url);
  const code = u.searchParams.get('code');
  const brokerState = u.searchParams.get('state') || '';
  if (u.searchParams.get('error') || !code || !brokerState) return oauthErrorPage('provider_denied');
  const st = await consumeOauthState(env, brokerState, 'orcid');
  if (!st) return oauthErrorPage('state_invalid');
  const tokenRes = await fetch('https://orcid.org/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
    body: new URLSearchParams({
      client_id: env.ORCID_CLIENT_ID,
      client_secret: env.ORCID_CLIENT_SECRET,
      grant_type: 'authorization_code',
      code,
      redirect_uri: OAUTH_REDIRECT_ORCID_ACCOUNTS,
    }).toString(),
  });
  if (!tokenRes.ok) return oauthErrorPage('token_exchange_failed');
  const tokenData = await tokenRes.json();
  const orcidId = tokenData.orcid;
  if (!orcidId) return oauthErrorPage('orcid_missing');

  // Link ONLY by stored orcid_id — the account-takeover ban (no email fallback).
  let user = await env.DB.prepare('SELECT * FROM users WHERE orcid_id=?').bind(orcidId).first();
  if (!user) {
    const profile = await fetchOrcidProfile(orcidId);
    const profEmail = (profile && profile.emails && profile.emails[0]) ? String(profile.emails[0]).toLowerCase() : null;
    const name = (profile && profile.fullName) || (tokenData.name || 'ORCID User');
    // Frictionless auto-create ONLY when ORCID gave a UNIQUE public email.
    if (profEmail && !(await env.DB.prepare('SELECT id FROM users WHERE email=?').bind(profEmail).first())) {
      // Everything ORCID hands back here is typed by the profile's owner, and it goes straight
      // into the four columns the public stats page renders and the admin emails print — the
      // one write path to those columns with no Latin-only check at all, so it reopened by
      // itself what the register and update-profile handlers filter. Sanitized rather than
      // rejected: a researcher whose ORCID record is in Cyrillic is legitimate, and refusing
      // their login would be the wrong answer to a display problem. A dropped institution or
      // role is the state ORCID already produces when it returns no employment record — the
      // account page prompts for them. The name falls back the way the Google path does.
      const rawInst = (profile && profile.currentEmployment && profile.currentEmployment[0] && profile.currentEmployment[0].organization) || '';
      const rawRole = (profile && profile.currentEmployment && profile.currentEmployment[0] && profile.currentEmployment[0].role) || '';
      const rawCtry = (profile && profile.country) || country || '';
      const inst = isLatinish(rawInst) ? rawInst.trim().slice(0, 200) : '';
      const role = isLatinish(rawRole) ? rawRole.trim().slice(0, 100) : '';
      const ctry = isLatinish(rawCtry) ? rawCtry.trim().slice(0, 100) : '';
      const emailLocal = profEmail.split('@')[0];
      const safeName = isLatinish(name) ? name.trim().slice(0, 100)
        : (isLatinish(emailLocal) ? emailLocal.slice(0, 100) : 'ORCID User');
      const profileJson = profile ? JSON.stringify(profile) : null;
      const apiKey = 'hfd_' + generateId();
      const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
      const unsub = generateId();
      const isAdmin = ADMIN_EMAILS.includes(profEmail) ? 1 : 0;
      const rndPw = generateId() + generateId();
      const pwHash = await hashPassword(rndPw);
      // ORCID email is UNVERIFIED by us (email_verified=0); family newsletter default 0.
      let created = false;
      try {
        await env.DB.prepare(
          'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, 1)'
        ).bind(safeName, profEmail, pwHash, inst, ctry, role, apiKey, apiKeyExpires, isAdmin, unsub, ip, ua, orcidId, profileJson).run();
        created = true;
      } catch (e) { /* concurrent same-orcid create won the UNIQUE index → use theirs */ }
      user = await env.DB.prepare('SELECT * FROM users WHERE orcid_id=?').bind(orcidId).first();
      if (created && user) {
        try {
          // safeName, not name: the notification should say what is actually in the row. The
          // untouched ORCID record is still on the row in orcid_profile_json.
          await sendEmail(env, ADMIN_NOTIFY, 'New registration via ORCID (family SSO): ' + safeName,
            adminNotificationEmail({ name: safeName, email: profEmail, institution: inst || '(via ORCID / accounts)', country: ctry, role: role || 'Not specified' }, ip, ua, country));
        } catch (e) { /* non-fatal */ }
      }
    } else {
      // No unique email (private or already taken) → link-or-register: bounce to the
      // auth page carrying a signed prefill of the VERIFIED orcid_id. The user signs
      // in to link it, or registers with an email.
      // Audience 'idp': valid on accounts.elkassabgidata.com only, and only in this
      // browser (the nonce cookie goes out with the 303 below). null → fail closed.
      const prefill = await mintOrcidPrefill(env, orcidId, name, 'idp');
      if (!prefill) return oauthErrorPage('orcid_link_unavailable');
      const reg = await getRegistry(env);
      const client = reg.get(st.client_origin);
      if (!client || client.status !== 'active' || !client.redirect_exact) return oauthErrorPage('client_unavailable');
      const dest = IDP_ORIGIN + '/authorize?response_type=code'
        + '&client_id=' + encodeURIComponent(st.client_origin)
        + '&redirect_uri=' + encodeURIComponent(client.redirect_exact)
        + '&state=' + encodeURIComponent(st.family_state || '')
        + '&code_challenge=' + encodeURIComponent(st.family_code_challenge || '')
        + '&code_challenge_method=S256&orcid_prefill=' + encodeURIComponent(prefill.token);
      // The nonce cookie is half the credential — without it the token verifies
      // nowhere, including here. Referrer-Policy still no-referrer so the token in
      // the Location doesn't leak onward.
      return new Response(null, { status: 303, headers: { 'Location': dest, 'Set-Cookie': prefill.cookie, 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store' } });
    }
  }
  if (!user || !user.is_active) return oauthErrorPage('account_unavailable');
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await brokerLoginRedirect(env, user.id, st, ip, ua);
}
const EKD_SDK_JS = "/* ElkassabgiData family SSO SDK \u2014 served from https://accounts.elkassabgidata.com/sdk/ekd-sso.js\n * One universal ElkassabgiData account across HF / Econ / IP / portal.\n * Popup + PKCE (S256) + opaque family tokens. No third-party deps.\n *\n * Usage on a site:\n *   <script src=\"https://accounts.elkassabgidata.com/sdk/ekd-sso.js\"></script>\n *   <script>\n *     EKD.init();                     // clientId defaults to location.origin\n *     document.querySelector('#login').onclick = () => EKD.login();\n *     EKD.on('login',  u => ...);     // signed in (has a fresh access token)\n *     EKD.on('logout', () => ...);\n *     const at = await EKD.getAccessToken();  // for Authorization: Bearer <at>; null if signed out\n *   </script>\n *\n * The site must also serve a callback page at <origin>/auth/callback (see the\n * per-site callback snippet) whose exact URL is registered as this client's\n * redirect_exact in the IdP registry.\n */\n(function () {\n  'use strict';\n  if (window.EKD && window.EKD.__ready) return;\n\n  var ACCOUNTS = 'https://accounts.elkassabgidata.com';\n  var CALLBACK_PATH = '/auth/callback';\n  var LS_RT = 'ekd_rt';                 // refresh token (localStorage, shared across tabs)\n  var LS_AT = 'ekd_at';                 // shared access token {t,e} \u2014 lets tabs reuse one refresh\n  var AT_SKEW_MS = 30000;               // refresh this many ms before expiry\n\n  var cfg = { clientId: null, accounts: ACCOUNTS, callbackPath: CALLBACK_PATH };\n  var at = null;                        // in-memory access token\n  var atExp = 0;                        // in-memory access-token expiry (ms epoch)\n  var listeners = { login: [], logout: [] };\n  var loginInFlight = null;             // single-flight login (coalesces concurrent calls)\n  var refreshInFlight = null;           // per-tab single-flight refresh\n\n  // \u2500\u2500 small helpers \u2500\u2500\n  function b64url(bytes) {\n    var s = '';\n    for (var i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);\n    return btoa(s).replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=+$/, '');\n  }\n  function randToken() { return b64url(crypto.getRandomValues(new Uint8Array(32))); } // 43 chars\n  async function s256(v) {\n    var d = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(v));\n    return b64url(new Uint8Array(d));\n  }\n  function emit(ev, arg) { (listeners[ev] || []).forEach(function (f) { try { f(arg); } catch (e) {} }); }\n  function getRt() { try { return localStorage.getItem(LS_RT) || null; } catch (e) { return null; } }\n  function setRt(v) { try { v ? localStorage.setItem(LS_RT, v) : localStorage.removeItem(LS_RT); } catch (e) {} }\n  function readSharedAt() { try { var j = JSON.parse(localStorage.getItem(LS_AT) || 'null'); if (j && j.t && j.e) return j; } catch (e) {} return null; }\n  function writeSharedAt(t, e) { try { localStorage.setItem(LS_AT, JSON.stringify({ t: t, e: e })); } catch (e) {} }\n  function clearSharedAt() { try { localStorage.removeItem(LS_AT); } catch (e) {} }\n  function adoptAt(t, e) { at = t; atExp = e; }\n\n  // Never throws \u2014 a network/CORS failure becomes a not-ok result, so callers get\n  // the documented \"token or null\" behaviour instead of an exception.\n  async function postJson(path, body) {\n    try {\n      var r = await fetch(cfg.accounts + path, {\n        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),\n      });\n      var data = null; try { data = await r.json(); } catch (e) {}\n      return { ok: r.ok, status: r.status, data: data };\n    } catch (e) {\n      return { ok: false, status: 0, data: null };\n    }\n  }\n\n  // \u2500\u2500 token lifecycle \u2500\u2500\n  function storeTokens(d) {\n    if (!d) return;\n    if (d.access_token) { var e = Date.now() + (Number(d.expires_in || 900) * 1000); adoptAt(d.access_token, e); writeSharedAt(d.access_token, e); }\n    if (d.refresh_token) setRt(d.refresh_token);   // grace responses omit refresh_token \u2014 keep the shared one\n  }\n  function clearLocal() { at = null; atExp = 0; setRt(null); clearSharedAt(); }\n\n  // Serialize refresh across tabs (Web Locks) so only one tab ever spends a given\n  // rt; other tabs, once inside the lock, adopt the just-rotated shared token.\n  function withLock(fn) {\n    try {\n      if (navigator.locks && navigator.locks.request) return navigator.locks.request('ekd_refresh', { mode: 'exclusive' }, fn);\n    } catch (e) {}\n    return fn(); // no Web Locks \u2192 rely on per-tab single-flight + server grace window\n  }\n\n  async function refreshOnce(force) {\n    if (!force) {                              // force=true skips the cache to VALIDATE against the server\n      var shared = readSharedAt();             // another tab may have refreshed while we waited for the lock\n      if (shared && Date.now() < shared.e - AT_SKEW_MS) { adoptAt(shared.t, shared.e); return at; }\n    }\n    var rt = getRt();\n    if (!rt) return null;\n    var res = await postJson('/token/refresh', { refresh_token: rt, client_origin: cfg.clientId });\n    if (res.ok && res.data && res.data.access_token) { storeTokens(res.data); return at; }\n    if (res.status === 401) { clearLocal(); emit('logout'); }  // revoked/reuse/invalid_grant \u2192 chain dead, fail closed\n    return null;                                                // transient (status 0) \u2192 no token, keep the rt\n  }\n\n  // Returns a valid access token or null. Never throws.\n  async function getAccessToken() {\n    if (at && Date.now() < atExp - AT_SKEW_MS) return at;\n    var shared = readSharedAt();\n    if (shared && Date.now() < shared.e - AT_SKEW_MS) { adoptAt(shared.t, shared.e); return at; }\n    if (refreshInFlight) return refreshInFlight;\n    refreshInFlight = Promise.resolve(withLock(function () { return refreshOnce(false); })).catch(function () { return null; }).finally(function () { refreshInFlight = null; });\n    return refreshInFlight;\n  }\n\n  // Page-load session validation: force a server refresh (ignore the cached token)\n  // so a server-side \"log out everywhere\" is detected promptly \u2014 the revoked rt\n  // returns 401 \u2192 refreshOnce clears local state + emits logout. A transient\n  // failure (status 0) keeps the session (returns null without clearing).\n  async function validateSession() {\n    if (!getRt()) return null;\n    if (refreshInFlight) return refreshInFlight;\n    refreshInFlight = Promise.resolve(withLock(function () { return refreshOnce(true); })).catch(function () { return null; }).finally(function () { refreshInFlight = null; });\n    return refreshInFlight;\n  }\n\n  // \u2500\u2500 popup login \u2500\u2500\n  function login(opts) {\n    if (loginInFlight) return loginInFlight;   // coalesce double-clicks / concurrent callers\n    opts = opts || {};\n    loginInFlight = new Promise(function (resolve, reject) {\n      (async function () {\n        if (!(window.isSecureContext !== false && typeof crypto !== 'undefined' && crypto.subtle)) throw new Error('insecure_context');\n        var verifier = randToken();\n        var challenge = await s256(verifier);\n        var state = randToken();\n\n        var redirectUri = cfg.clientId + cfg.callbackPath;\n        var url = cfg.accounts + '/authorize?response_type=code'\n          + '&client_id=' + encodeURIComponent(cfg.clientId)\n          + '&redirect_uri=' + encodeURIComponent(redirectUri)\n          + '&state=' + encodeURIComponent(state)\n          + '&code_challenge=' + encodeURIComponent(challenge)\n          + '&code_challenge_method=S256'\n          + (opts.tab === 'register' ? '&hint=register' : '');\n\n        var w = 480, h = 640, x = 0, y = 0;\n        try { // window.top can throw if framed cross-origin; fall back to screen center\n          var bw = window.outerWidth || screen.width, bh = window.outerHeight || screen.height;\n          x = (window.screenX || 0) + (bw - w) / 2;\n          y = (window.screenY || 0) + (bh - h) / 2;\n        } catch (e) {}\n        var popup = window.open(url, 'ekd_login_' + state.slice(0, 8),\n          'width=' + w + ',height=' + h + ',left=' + Math.max(0, x | 0) + ',top=' + Math.max(0, y | 0));\n        if (!popup) throw new Error('popup_blocked');\n\n        var done = false, accepted = false, poll = 0, bc = null;\n        function teardown() {\n          window.removeEventListener('message', onMsg);\n          if (poll) { clearInterval(poll); poll = 0; }\n          if (bc) { try { bc.close(); } catch (e) {} }\n        }\n        function settle(fn, arg) { if (done) return; done = true; teardown(); fn(arg); }\n\n        // Exactly-once handoff: the first valid, state-matched message wins; the\n        // popup-closed poll is disarmed BEFORE the exchange await so a poll tick\n        // during the network round-trip can't reject a login that is succeeding.\n        async function handleAuth(code, st) {\n          if (accepted) return;\n          if (!code || st !== state) return;         // wrong/missing state \u2192 keep listening\n          accepted = true;\n          if (poll) { clearInterval(poll); poll = 0; }\n          try { popup.close(); } catch (e) {}\n          try {\n            var res = await postJson('/token/exchange', { code: code, code_verifier: verifier, client_origin: cfg.clientId });\n            if (res.ok && res.data && res.data.access_token) {\n              storeTokens(res.data);\n              emit('login', { access_token: at });\n              settle(resolve, { access_token: at });\n            } else {\n              settle(reject, new Error((res.data && res.data.error) || 'exchange_failed'));\n            }\n          } catch (e) { settle(reject, e instanceof Error ? e : new Error('exchange_error')); }\n        }\n\n        function onMsg(ev) {\n          if (ev.origin !== cfg.clientId) return;    // only our own callback origin\n          if (ev.source && ev.source !== popup) return; // ...and only from our popup\n          var m = ev.data;\n          if (!m || m.type !== 'ekd_auth') return;\n          handleAuth(m.code, m.state);\n        }\n        window.addEventListener('message', onMsg);\n        // COOP fallback (opener severed): same-origin BroadcastChannel, state-guarded.\n        try { bc = new BroadcastChannel('ekd_auth'); bc.onmessage = function (ev) { var m = ev.data; if (m && m.type === 'ekd_auth') handleAuth(m.code, m.state); }; } catch (e) {}\n\n        poll = setInterval(function () { if (!accepted && popup.closed) settle(reject, new Error('popup_closed')); }, 500);\n      })().catch(function (e) { reject(e); });\n    }).finally(function () { loginInFlight = null; });\n    return loginInFlight;\n  }\n\n  async function logout() {\n    var rt = getRt();\n    await postJson('/logout', rt ? { refresh_token: rt } : {});\n    clearLocal();\n    emit('logout');\n  }\n\n  function on(ev, fn) { if (listeners[ev] && typeof fn === 'function') listeners[ev].push(fn); }\n  function isLoggedIn() { return !!getRt() || !!at; }\n\n  // Cross-tab: another tab cleared the refresh token (logout / dead chain).\n  window.addEventListener('storage', function (e) {\n    if (e.key === LS_RT && !e.newValue) { at = null; atExp = 0; emit('logout'); }\n  });\n\n  function init(options) {\n    options = options || {};\n    cfg.clientId = options.clientId || location.origin;\n    if (options.accounts) cfg.accounts = options.accounts;\n    if (options.callbackPath) cfg.callbackPath = options.callbackPath;\n    // On load, VALIDATE the stored session against the server (not the cached\n    // token) so a cross-origin \"log out everywhere\" is reflected here promptly:\n    // a still-valid session warms a token + fires on('login'); a revoked one 401s\n    // \u2192 clears local state + fires on('logout').\n    if (getRt() && !options.noAutoResume) {\n      validateSession().then(function (tok) { if (tok) emit('login', { access_token: tok }); }).catch(function () {});\n    }\n    return window.EKD;\n  }\n\n  window.EKD = {\n    __ready: true,\n    init: init,\n    login: login,\n    logout: logout,\n    getAccessToken: getAccessToken,\n    isLoggedIn: isLoggedIn,\n    on: on,\n    get clientId() { return cfg.clientId; },\n  };\n})();\n";
// GET /sdk/ekd-sso.js — the family SSO client SDK (M2b-3). Immutable per deploy;
// short cache so worker updates propagate. Loaded via <script src> (no CORS).
async function handleSdkAsset(path) {
  if (path === '/sdk/ekd-sso.js') {
    return new Response(EKD_SDK_JS, {
      status: 200,
      headers: {
        'Content-Type': 'application/javascript; charset=utf-8',
        'Cache-Control': 'public, max-age=300, must-revalidate',
        'X-Content-Type-Options': 'nosniff',
        'Referrer-Policy': 'no-referrer',
      },
    });
  }
  return new Response('Not found', { status: 404, headers: { 'Cache-Control': 'no-store' } });
}

// ══════════════════════════════════════
// ── Family SSO M2b-2a — IdP account surface (auth page + login + register) ──
// ══════════════════════════════════════
// Server-rendered, same-origin forms on accounts.elkassabgidata.com. On a
// successful login/register/2FA we set ekd_session (createIdpSession) and 303 to
// the family callback with the code — the auth submission IS the consent gesture
// (a RETURNING cookie user still gets the M2b-1 "Continue as X" gesture page).
// The api.* handleRegister/handleLogin stay byte-for-byte (M3 policy); these
// duplicate their sequences (see AUTH_SSO_BUILD_LOG.md duplication-drift note).

const NEWSLETTER_LISTS = [
  { key: 'hf',     label: 'HF Data Library — 1-minute U.S. equities' },
  { key: 'econ',   label: 'Econ Data Library — global economic & financial data' },
  { key: 'ip',     label: 'IP / Patent Data Library' },
  { key: 'family', label: 'ElkassabgiData family updates' },
];
const NEWSLETTER_KEYS = new Set(NEWSLETTER_LISTS.map((l) => l.key));

const authPageHeaders = {
  'Content-Type': 'text/html; charset=utf-8',
  'Cache-Control': 'no-store',
  // 'same-origin' so the login/register/2FA same-origin form POSTs keep their
  // Origin header (no-referrer would send Origin: null → cross_site_blocked).
  'Referrer-Policy': 'same-origin',
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'Content-Security-Policy':
    "default-src 'none'; " +
    "script-src https://challenges.cloudflare.com; " +
    "frame-src https://challenges.cloudflare.com; " +
    // 'self' in connect-src: the Turnstile widget can make a same-origin request as
    // part of verification; without it the widget shows "unable to connect to the
    // website" (the working hf download page allows connect-src 'self'). report-uri
    // captures any remaining violation server-side for diagnosis.
    "connect-src 'self' https://challenges.cloudflare.com; " +
    "style-src 'unsafe-inline'; img-src https: data:; " +
    // https: so the login/register form's SUCCESS 303 to the client's cross-origin
    // callback isn't blocked by form-action (enforced on redirect targets).
    "form-action 'self' https:; frame-ancestors 'none'; base-uri 'none'; report-uri /csp-report",
};

// Same-origin form guard (copied from handleAuthorizePost). The login/register
// forms are same-origin (form-action 'self'); a cross-site POST is rejected.
function assertSameOriginForm(request) {
  const origin = request.headers.get('Origin') || '';
  const sfs = request.headers.get('Sec-Fetch-Site');
  return !(origin !== IDP_ORIGIN || (sfs && sfs !== 'same-origin'));
}

// Re-validate the authorize params on EVERY POST (the GET-time check does not
// bind a tampered hidden field). Returns {ok, row, status}.
async function validateAuthorizeParams(env, p) {
  if (p.method !== 'S256' || !/^[A-Za-z0-9_-]{43}$/.test(p.codeChallenge || '')) return { ok: false, status: 400 };
  const reg = await getRegistry(env);
  const row = reg.get(p.clientId);
  if (!row) return { ok: false, status: 400 };
  if (row.status !== 'active') return { ok: false, status: 403 };
  if (!row.redirect_exact || row.redirect_exact !== p.redirectUri) return { ok: false, status: 400 };
  return { ok: true, row, status: 200 };
}

function paramsFromForm(body) {
  return {
    clientId: body.get('client_id') || '',
    redirectUri: body.get('redirect_uri') || '',
    state: body.get('state') || '',
    codeChallenge: body.get('code_challenge') || '',
    method: body.get('code_challenge_method') || '',
    orcidPrefill: body.get('orcid_prefill') || '',
    // The user's explicit "link this ORCID" tick. Absent = do not touch orcid_id.
    linkOrcid: body.get('link_orcid') === '1',
  };
}

function parseNewsletter(body) {
  const prefs = body.getAll('newsletter').filter((k) => NEWSLETTER_KEYS.has(k));
  return { prefs, hfSelected: prefs.includes('hf') };
}
async function applyNewsletterPrefs(env, userId, prefs) {
  for (const key of prefs) {
    await env.DB.prepare(
      "INSERT OR REPLACE INTO newsletter_prefs (user_id, list_key, subscribed, created_at) VALUES (?, ?, 1, datetime('now'))"
    ).bind(userId, key).run();
  }
}

// Shared success tail: set ekd_session + 303 to the family callback with the code.
async function loginAndRedirect(env, request, userId, ip, ua, p) {
  // ORCID is attached ONLY when the user ticked the box on the login form. This ran
  // on EVERY login that carried a prefill, which made "sign in with your password"
  // enough to have a stranger's ORCID iD written onto your account — and that iD is
  // itself a login credential, so the stranger could then sign in as you. An identity
  // that doubles as a credential must never be attached by a request the user did not
  // knowingly make; p.linkOrcid is that knowing act, and maybeLinkOrcid still checks
  // the token's browser binding, audience and single use on top.
  if (p && p.orcidPrefill && p.linkOrcid) await maybeLinkOrcid(env, request, userId, p.orcidPrefill, 'idp');
  const idp = await createIdpSession(env, userId, ip, ua);
  const resp = await mintCodeAndRedirect(env, userId, p.clientId, p.row.redirect_exact, p.state, p.codeChallenge, 303);
  resp.headers.append('Set-Cookie', idp.cookie);
  return resp;
}

// Post-registration interstitial for accounts that must verify their email before
// downloading (every non-admin sign-up). Signs the user in exactly as
// loginAndRedirect does — IdP session cookie + a fresh single-use SSO code — but,
// instead of the silent 303, renders a "check your inbox (and spam folder)" notice
// whose Continue link carries that same code to the site's callback. Closing the
// popup without continuing is harmless: the account exists, the email is sent, and
// the IdP session makes the next site login one click.
async function registerVerifyNotice(env, userId, ip, ua, p, email) {
  // No maybeLinkOrcid here any more. Its only caller, handleAccountsRegister, already
  // writes orcid_id in the INSERT itself (so the partial UNIQUE index arbitrates
  // concurrent sign-ups) and burns the prefill row there. Running it again could only
  // link an iD to an account this request did not create.
  const idp = await createIdpSession(env, userId, ip, ua);
  // 10-min code TTL: the user may detour to their inbox/spam (as the notice invites)
  // before clicking Continue, so the sub-second 60s default would expire the handoff.
  const dest = await mintSsoCode(env, userId, p.clientId, p.row.redirect_exact, p.state, p.codeChallenge, 600);
  const resp = new Response(renderVerifyNoticePage(p.row.brand_name, email, dest), { status: 200, headers: authPageHeaders });
  resp.headers.append('Set-Cookie', idp.cookie);
  return resp;
}

function renderVerifyNoticePage(brandName, email, dest) {
  const brand = htmlEncode(brandName || 'ElkassabgiData');
  const em = htmlEncode(email || '');
  const href = htmlEncode(dest);
  const S = "body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center}" +
    ".card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:1.6rem;max-width:440px;width:92%}" +
    "h1{font-size:1.15rem;color:#d4a843;text-align:center;margin:.2rem 0 1rem}" +
    "p{font-size:.92rem;line-height:1.5;color:#cbd5e1;margin:.6rem 0}" +
    ".ok{background:rgba(52,211,153,.12);border:1px solid rgba(52,211,153,.4);color:#a7f3d0;border-radius:8px;padding:.55rem .75rem;margin-bottom:.4rem;font-size:.9rem;text-align:center}" +
    ".warn{background:rgba(212,168,67,.12);border:1px solid rgba(212,168,67,.45);color:#f1d18b;border-radius:8px;padding:.6rem .8rem;margin:.9rem 0;font-size:.86rem;line-height:1.5}" +
    "a.btn{display:block;text-align:center;background:#d4a843;color:#0f1729;border-radius:8px;padding:.7rem;font-weight:700;font-size:1rem;text-decoration:none;margin-top:1rem}";
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Verify your email &middot; ' + brand + '</title><style>' + S + '</style></head><body><div class="card">' +
    '<h1>One more step &mdash; verify your email</h1>' +
    '<div class="ok">&#10003; Your ' + brand + ' account was created.</div>' +
    '<p>We just emailed a verification link to <strong>' + em + '</strong>. Click it to confirm your address &mdash; you’ll need to verify before you can download data.</p>' +
    '<div class="warn">&#9888;&#65039; Don’t see it within a minute? <strong>Check your spam or junk folder</strong> &mdash; the message sometimes lands there. Marking it &ldquo;not spam,&rdquo; or adding ' + htmlEncode(FROM_EMAIL) + ' to your contacts, keeps future emails in your inbox.</div>' +
    '<a class="btn" href="' + href + '">Continue to ' + brand + '</a>' +
    '</div></body></html>';
}

// ── Auth page (login/register tabs) + 2FA page ──
function hiddenAuthParams(p) {
  return '<input type="hidden" name="client_id" value="' + htmlEncode(p.clientId) + '">' +
    '<input type="hidden" name="redirect_uri" value="' + htmlEncode(p.redirectUri) + '">' +
    '<input type="hidden" name="state" value="' + htmlEncode(p.state) + '">' +
    '<input type="hidden" name="code_challenge" value="' + htmlEncode(p.codeChallenge) + '">' +
    '<input type="hidden" name="code_challenge_method" value="S256">' +
    (p.orcidPrefill ? '<input type="hidden" name="orcid_prefill" value="' + htmlEncode(p.orcidPrefill) + '">' : '');
}

function renderAuthPage(row, p, opts) {
  opts = opts || {};
  const brand = htmlEncode(row.brand_name || 'ElkassabgiData');
  const err = opts.error ? '<div class="err">' + htmlEncode(opts.error) + '</div>' : '';
  const em = htmlEncode(opts.loginEmail || '');
  const loginChecked = opts.tab === 'register' ? '' : 'checked';
  const regChecked = opts.tab === 'register' ? 'checked' : '';
  const oauthQ = 'client_id=' + encodeURIComponent(p.clientId) + '&redirect_uri=' + encodeURIComponent(p.redirectUri) +
    '&state=' + encodeURIComponent(p.state) + '&code_challenge=' + encodeURIComponent(p.codeChallenge) +
    '&code_challenge_method=S256&response_type=code';
  const news = NEWSLETTER_LISTS.map((l) =>
    '<label class="nl"><input type="checkbox" name="newsletter" value="' + l.key + '"' + (l.key === 'hf' ? ' checked' : '') + '> ' + htmlEncode(l.label) + '</label>'
  ).join('');
  // ORCID link-or-register: banner + prefilled name + NO CAPTCHA (ORCID auth is the
  // human check; the signed orcid_prefill token stands in for Turnstile).
  const orcid = opts.orcid || null;
  const orcidBanner = orcid ? '<div class="ok">&#10003; ORCID <strong>' + htmlEncode(orcid.orcidId) + '</strong> verified &mdash; <strong>Log in</strong> and tick the box to link it to your existing ElkassabgiData account, or <strong>Sign up</strong> to create a new one.</div>' : '';
  // Opt-in, never pre-ticked. Linking an ORCID iD hands that iD the power to sign in
  // as this account, so it takes a deliberate click — logging in must not do it by
  // itself. A failed password attempt re-renders with the user's own earlier tick
  // preserved (their choice, from their own same-origin POST), never invented.
  const orcidLinkOptIn = orcid
    ? '<label class="nl" style="margin:.6rem 0"><input type="checkbox" name="link_orcid" value="1"' + (p.linkOrcid ? ' checked' : '') +
      '> Link ORCID ' + htmlEncode(orcid.orcidId) + ' to this account (you will be able to sign in with ORCID)</label>'
    : '';
  const regNameVal = orcid ? htmlEncode(orcid.name || '') : '';
  const turnstileWidget = orcid ? '' : '<div class="cf-turnstile" data-sitekey="0x4AAAAAAC5ydfuRj9dEK0kY" data-response-field-name="turnstile_token" data-theme="auto"></div>';
  const S = "body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center}" +
    ".card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:1.6rem;max-width:420px;width:92%}" +
    "h1{font-size:1.15rem;color:#d4a843;text-align:center;margin:.2rem 0 1rem}" +
    ".tabs{display:flex;gap:.4rem;margin-bottom:1rem}.tabs label{flex:1;text-align:center;padding:.5rem;border-radius:8px;background:#0f1729;cursor:pointer;color:#9ca3af;font-weight:600}" +
    "input[name=authtab]{display:none}.panel{display:none}" +
    "#tl:checked~.tabs label[for=tl],#tr:checked~.tabs label[for=tr]{background:#d4a843;color:#0f1729}" +
    "#tl:checked~#pl{display:block}#tr:checked~#pr{display:block}" +
    "input[type=email],input[type=password],input[type=text]{width:100%;box-sizing:border-box;padding:.6rem;margin:.3rem 0;border-radius:8px;border:1px solid #2a3550;background:#0f1729;color:#e5e7eb}" +
    "button{width:100%;background:#d4a843;color:#0f1729;border:0;border-radius:8px;padding:.7rem;font-weight:700;font-size:1rem;cursor:pointer;margin-top:.5rem}" +
    ".oauth a{display:block;text-align:center;padding:.55rem;margin:.4rem 0;border:1px solid #2a3550;border-radius:8px;color:#e5e7eb;text-decoration:none}" +
    ".err{background:#7f1d1d;color:#fee;padding:.5rem .7rem;border-radius:8px;margin-bottom:.8rem;font-size:.9rem}" +
    ".nl{display:block;font-size:.82rem;color:#cbd5e1;margin:.25rem 0}.nl input{width:auto;margin-right:.4rem}fieldset{border:1px solid #2a3550;border-radius:8px;margin:.6rem 0;padding:.5rem}legend{font-size:.8rem;color:#9ca3af}" +
    ".muted{color:#9ca3af;font-size:.8rem;text-align:center;margin-top:.8rem}" +
    ".ok{background:rgba(52,211,153,.12);border:1px solid rgba(52,211,153,.4);color:#a7f3d0;border-radius:8px;padding:.55rem .75rem;margin-bottom:.9rem;font-size:.85rem;line-height:1.4}";
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Sign in to ElkassabgiData</title>' +
    (orcid ? '' : '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>') +
    '<style>' + S + '</style></head><body><div class="card">' +
    '<h1>Continue to ' + brand + '</h1>' + orcidBanner + err +
    '<input type="radio" name="authtab" id="tl" ' + loginChecked + '>' +
    '<input type="radio" name="authtab" id="tr" ' + regChecked + '>' +
    '<div class="tabs"><label for="tl">Log in</label><label for="tr">Sign up</label></div>' +
    // login panel
    '<div class="panel" id="pl"><form method="POST" action="/login">' + hiddenAuthParams(p) +
    '<input type="email" name="email" placeholder="Email" value="' + em + '" required autocomplete="email">' +
    '<input type="password" name="password" placeholder="Password" required autocomplete="current-password">' +
    orcidLinkOptIn +
    '<button type="submit">Log in</button></form>' +
    '<p style="text-align:center;margin:.5rem 0 0"><a href="https://hfdatalibrary.com/pages/reset" target="_blank" rel="noopener" style="color:#9ca3af;font-size:.8rem;text-decoration:none">Forgot password?</a></p>' +
    '<div class="oauth"><a href="/v1/auth/google/start?' + oauthQ + '">Continue with Google</a>' +
    '<a href="/v1/auth/orcid/start?' + oauthQ + '">Continue with ORCID</a></div></div>' +
    // register panel
    '<div class="panel" id="pr"><form method="POST" action="/register">' + hiddenAuthParams(p) +
    '<input type="text" name="name" placeholder="Full name" value="' + regNameVal + '" required maxlength="100">' +
    '<input type="email" name="email" placeholder="Email" required autocomplete="email">' +
    '<input type="password" name="password" placeholder="Password (min 10 chars)" required autocomplete="new-password">' +
    '<input type="text" name="institution" placeholder="Institution" required maxlength="200">' +
    '<input type="text" name="country" placeholder="Country" required maxlength="100">' +
    '<input type="text" name="role" placeholder="Role (e.g. Professor, Student)" required maxlength="100">' +
    '<fieldset><legend>Newsletters (optional)</legend>' + news + '</fieldset>' +
    turnstileWidget +
    '<button type="submit">Create ElkassabgiData account</button></form>' +
    '<div class="oauth"><a href="/v1/auth/google/start?' + oauthQ + '">Sign up with Google</a>' +
    '<a href="/v1/auth/orcid/start?' + oauthQ + '">Sign up with ORCID</a></div></div>' +
    '<p class="muted">One free account works across every ElkassabgiData library.</p>' +
    '</div></body></html>';
}

function renderTwoFactorPage(pendingToken, p, error) {
  const err = error ? '<div class="err">' + htmlEncode(error) + '</div>' : '';
  const S = "body{font-family:system-ui,sans-serif;background:#0f1729;color:#e5e7eb;margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center}.card{background:#141c2e;border:1px solid rgba(212,168,67,.3);border-radius:14px;padding:1.6rem;max-width:360px;width:92%;text-align:center}h1{font-size:1.1rem;color:#d4a843}input{width:100%;box-sizing:border-box;padding:.6rem;margin:.4rem 0;border-radius:8px;border:1px solid #2a3550;background:#0f1729;color:#e5e7eb;text-align:center;letter-spacing:.3em;font-size:1.2rem}button{width:100%;background:#d4a843;color:#0f1729;border:0;border-radius:8px;padding:.7rem;font-weight:700;cursor:pointer}.err{background:#7f1d1d;color:#fee;padding:.5rem;border-radius:8px;margin-bottom:.6rem;font-size:.9rem}";
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Two-factor</title><style>' + S + '</style></head><body><div class="card"><h1>Enter your 2FA code</h1>' + err +
    '<form method="POST" action="/login/2fa">' + hiddenAuthParams(p) +
    // Carry the login form's ORCID tick across the second factor. Without it the
    // user's explicit choice would be dropped on every 2FA account, and the link
    // would silently not happen — the opposite failure, but still a surprise.
    (p.linkOrcid ? '<input type="hidden" name="link_orcid" value="1">' : '') +
    '<input type="hidden" name="pending_token" value="' + htmlEncode(pendingToken) + '">' +
    '<input type="text" name="code" inputmode="numeric" pattern="[0-9]{6}" maxlength="6" placeholder="000000" required autofocus>' +
    '<button type="submit">Verify</button></form></div></body></html>';
}

// ── POST /login ──
async function handleAccountsLogin(request, env, ip, ua, country) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  // Peek only; the charge is in the wrong-password branch below (see handleLogin).
  const rl = await checkRateLimit(env, rlIpKey(ip), 'api:login', { charge: false });
  if (!rl.ok) return new Response('Too many attempts. Try again later.', { status: 429, headers: authPageHeaders });
  let body;
  try { body = await request.formData(); } catch { return new Response('Bad request', { status: 400, headers: { 'Cache-Control': 'no-store' } }); }
  const p = paramsFromForm(body);
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('<h1>Invalid request</h1>', { status: v.status, headers: authPageHeaders });
  p.row = v.row;
  // Read-only (banner + the link checkbox's label). The actual link happens in
  // loginAndRedirect, and only if the user ticked the box.
  const orcidPre = p.orcidPrefill ? await verifyOrcidPrefill(env, request, p.orcidPrefill, 'idp') : null;
  const email = (body.get('email') || '').toLowerCase();
  const password = body.get('password') || '';
  const user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email).first();
  if (!user || !(await verifyPassword(password, user.password_hash))) {
    await checkRateLimit(env, rlIpKey(ip), 'api:login');   // only wrong guesses cost anything
    if (user) await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)').bind(user.id, ip, ua, country).run();
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Invalid email or password', loginEmail: email, orcid: orcidPre }), { status: 200, headers: authPageHeaders });
  }
  if (!user.is_active) {
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Account has been deactivated.', loginEmail: email, orcid: orcidPre }), { status: 200, headers: authPageHeaders });
  }
  if (user.totp_enabled) {
    const pendingToken = generateId();
    const pendingExpires = new Date(Date.now() + 10 * 60 * 1000).toISOString();
    await env.DB.prepare('INSERT INTO totp_pending (token, user_id, expires_at, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)').bind(pendingToken, user.id, pendingExpires, ip, ua).run();
    return new Response(renderTwoFactorPage(pendingToken, p, ''), { status: 200, headers: authPageHeaders });
  }
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await loginAndRedirect(env, request, user.id, ip, ua, p);
}

// ── POST /login/2fa ──
async function handleAccounts2faVerify(request, env, ip, ua, country) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  let body;
  try { body = await request.formData(); } catch { return new Response('<h1>Invalid request</h1>', { status: 400, headers: authPageHeaders }); }
  const p = paramsFromForm(body);
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('<h1>Invalid request</h1>', { status: v.status, headers: authPageHeaders });
  p.row = v.row;
  const pendingToken = body.get('pending_token') || '';
  const code = body.get('code') || '';
  // Cap TOTP guesses per ACCOUNT, not per pending token — see the api.* twin for the full
  // reasoning: a token-keyed counter is only a batch size when whoever holds the password
  // can mint a fresh token for each new batch. Pending row resolved first so the counter
  // has a user to key on, and so an unknown token spends nobody's budget.
  // §EXPIRY-COMPARE: datetime() on both sides, same as the api.* twin — this row is
  // toISOString() too, so the 10-minute pending window really ran until midnight UTC.
  const pending = await env.DB.prepare('SELECT * FROM totp_pending WHERE token = ? AND datetime(expires_at) > datetime("now")').bind(pendingToken).first();
  if (!pending) return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Login expired — please sign in again.' }), { status: 200, headers: authPageHeaders });
  const rl2 = await checkRateLimit(env, 'tfa:u' + pending.user_id, 'api:2fa');
  if (!rl2.ok) {
    // Every pending row for the account, not just this token: leaving the rest alive is
    // precisely the minting loop this change exists to close.
    await env.DB.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(pending.user_id).run();
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Too many attempts — please sign in again.' }), { status: 200, headers: authPageHeaders });
  }
  const user = await env.DB.prepare('SELECT * FROM users WHERE id = ?').bind(pending.user_id).first();
  if (!user || !user.totp_secret || !(await verifyTotp(user.totp_secret, code))) {
    if (user) await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)').bind(user.id, ip, ua, country).run();
    return new Response(renderTwoFactorPage(pendingToken, p, 'Invalid 2FA code'), { status: 200, headers: authPageHeaders });
  }
  await env.DB.prepare('DELETE FROM totp_pending WHERE token = ?').bind(pendingToken).run();
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await loginAndRedirect(env, request, user.id, ip, ua, p);
}

// ── POST /register ──  (duplicates handleRegister's validation/creation; api.* untouched)
async function handleAccountsRegister(request, env, ip, ua, country) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  // Flood guard only; the account cap is charged just before the INSERT (see handleRegister).
  const rl = await checkRateLimit(env, rlIpKey(ip), 'api:register:burst');
  if (!rl.ok) return new Response('Too many attempts. Try again later.', { status: 429, headers: authPageHeaders });
  let body;
  try { body = await request.formData(); } catch { return new Response('Bad request', { status: 400, headers: { 'Cache-Control': 'no-store' } }); }
  const p = paramsFromForm(body);
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('<h1>Invalid request</h1>', { status: v.status, headers: authPageHeaders });
  p.row = v.row;
  // ORCID link-or-register: a valid signed prefill proves ORCID auth (the human
  // check), so it stands in for the CAPTCHA and re-renders the banner on error.
  // Verification is read-only — the CAPTCHA stand-in and the banner must survive a
  // rejected form so an honest retry still works; the row is burned at the INSERT.
  // Since the token is now bound to this browser and audienced to 'idp', a stranger's
  // token cannot skip the CAPTCHA here either.
  const orcidPre = p.orcidPrefill ? await verifyOrcidPrefill(env, request, p.orcidPrefill, 'idp') : null;
  const rerr = (msg, tab, extra) => new Response(renderAuthPage(v.row, p, Object.assign({ tab: tab || 'register', error: msg, orcid: orcidPre }, extra || {})), { status: 200, headers: authPageHeaders });

  if (!orcidPre && !(await verifyTurnstile(env, body.get('turnstile_token'), ip))) return rerr('CAPTCHA verification failed. Please try again.');
  const name = body.get('name') || '';
  const email = body.get('email') || '';
  const password = body.get('password') || '';
  const institution = body.get('institution') || '';
  const role = body.get('role') || '';
  const userCountry = body.get('country') || country;
  const { prefs, hfSelected } = parseNewsletter(body);

  if (!name || !email || !password || !institution || !role || !userCountry) return rerr('All fields are required.');
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return rerr('Invalid email address.');
  if (name.length > 100 || institution.length > 200 || role.length > 100 || userCountry.length > 100) return rerr('One or more fields exceed length limits.');
  if (!isLatinish(name) || !isLatinish(institution) || !isLatinish(userCountry) || !isLatinish(role)) return rerr('Name, institution, country, and role must use English/Latin letters only.');
  const normalizedCountry = normalizeCountry(userCountry) || userCountry.trim();
  const pw = checkPasswordStrength(password);
  if (!pw.ok) return rerr(pw.error);

  const existing = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();
  if (existing) return rerr('Email already registered — please log in.', 'login', { loginEmail: email.toLowerCase() });
  if (orcidPre) {
    const orcidTaken = await env.DB.prepare('SELECT id FROM users WHERE orcid_id = ?').bind(orcidPre.orcidId).first();
    if (orcidTaken) return rerr('This ORCID is already linked to an ElkassabgiData account — please log in instead.', 'login');
  }

  // Account cap, charged only now that the request is going to create something. The entry
  // check above is the flood guard; see the api:register notes in RATE_LIMITS.
  const acct = await checkRateLimit(env, rlIpKey(ip), 'api:register');
  if (!acct.ok) return new Response('Too many attempts. Try again later.', { status: 429, headers: authPageHeaders });

  const passwordHash = await hashPassword(password);
  const apiKey = 'hfd_' + generateId();
  const unsubscribeToken = generateId();
  // NO SELF-SERVICE ADMIN. This used to be `ADMIN_EMAILS.includes(email) ? 1 : 0`, so
  // anyone who registered one of the two (published, guessable) owner addresses received
  // is_admin = 1 from a public unauthenticated endpoint. The only thing preventing it was
  // that rows for both addresses already exist and a duplicate email 409s first — one
  // deleted row from a full console takeover. Admin is granted out-of-band now: by the
  // authenticated admin PUT, or directly in D1. A registration form must never mint it.
  const isAdmin = 0;
  const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
  // Burn the prefill now, after every rejection that a retry could fix, and before the
  // one statement that writes orcid_id. Past this line the token is spent: posting the
  // same string a second time creates no second account carrying that ORCID iD.
  // burnOrcidPrefill rather than consumeOrcidPrefill directly: it reads "no prefill sent"
  // as success. Its legacy-token exemption went with §PF-LEGACY, so the burn is now
  // unconditional for every token that exists.
  if (!(await burnOrcidPrefill(env, orcidPre))) {
    return rerr('That ORCID confirmation has already been used. Please sign in with ORCID again.', 'login');
  }
  // Set orcid_id in the INSERT itself (not deferred) so the partial UNIQUE index on
  // users.orcid_id makes concurrent same-token registrations COLLIDE — exactly one
  // account per ORCID login, no CAPTCHA-skip amplification. A UNIQUE collision (email
  // or orcid) fails closed to a login nudge instead of a 500.
  try {
    await env.DB.prepare(
      'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)'
    // email_verified is 0 for EVERYONE. It was `isAdmin ? 1 : 0`, which on THIS path meant
    // that registering an address from ADMIN_EMAILS produced a verified admin row, sent no
    // verification email at all, and handed the registrant an admin session on the spot
    // (see below) — a complete console takeover in one request, from a guessable address.
    // The only thing preventing it was that rows for both admin addresses already exist.
    ).bind(name, email.toLowerCase(), passwordHash, institution, normalizedCountry, role, apiKey, apiKeyExpires, isAdmin, 0, hfSelected ? 1 : 0, unsubscribeToken, ip, ua, orcidPre ? orcidPre.orcidId : null, null).run();
  } catch (e) {
    return rerr('That account could not be created — the email or ORCID may already be registered. Please log in.', 'login', { loginEmail: email.toLowerCase() });
  }
  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();
  await applyNewsletterPrefs(env, user.id, prefs);
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, userCountry).run();
  {
    // Sent to EVERYONE now, admins included. Admin rows are no longer born verified, so an
    // admin who is never emailed a link is one nobody can prove owns the address it names.
    const verifyToken = generateId();
    const verifyExpires = new Date(Date.now() + 86400000).toISOString();
    await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)').bind(user.id, verifyToken, verifyExpires).run();
    try { await sendEmail(env, email.toLowerCase(), 'Verify your ElkassabgiData account', verificationEmail(name, verifyToken), FROM_EMAIL, 'ElkassabgiData'); } catch (e) { /* non-blocking */ }
  }
  try { await sendEmail(env, ADMIN_NOTIFY, `New registration: ${name} (${institution})`, adminNotificationEmail({ name, email: email.toLowerCase(), institution, country: userCountry, role }, ip, ua, country)); } catch (e) { /* non-blocking */ }
  // EVERYONE must confirm their email before downloading, so instead of a silent 303 we
  // show a "check your inbox — and your spam folder" notice with a Continue link.
  // Admins used to be auto-verified and logged straight in from here. Both are gone:
  // together they turned "register with a guessable address" into an admin session.
  return await registerVerifyNotice(env, user.id, ip, ua, p, email.toLowerCase());
}
