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
  '/v1/auth/google/start',
  '/v1/auth/orcid/start',
  '/v1/auth/google/callback',
  '/v1/auth/orcid/callback',
]);

// §5 M2 token TTLs. SHORT TTLs are written with SQLite datetime() arithmetic,
// NEVER toISOString() (the 'T' > ' ' lexical-compare bug makes a toISOString
// expiry validate for ~a full day).
const EKD_SESSION_DAYS = 30;   // idp_master (ekd_session), 30d sliding
const EDL_AT_TTL_SEC = 900;    // family access token, 15 min
const EDL_RT_TTL_HOURS = 24;   // refresh token absolute cap (§18 decision)
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
  'api:register': { max: 3, window: 3600 },     // 3 registrations per hour per IP
  'api:reset': { max: 3, window: 3600 },        // 3 password resets per hour per IP
  'api:download': { max: 100, window: 60 },     // 100 downloads per minute per user
  'api:general': { max: 300, window: 60 },      // 300 general API requests per minute
  'api:2fa': { max: 5, window: 600 },           // 5 TOTP guesses per pending token (IP-independent brute-force cap)
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
      if (path === '/v1/auth/orcid/start')
        return handleOrcidStart(request, env, cors);
      if (path === '/v1/auth/orcid/callback')
        return await handleOrcidCallback(request, env, ip, ua, country);
      if (path === '/v1/auth/google/start')
        return handleGoogleStart(env, cors);
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

      const symbolMatch = path.match(/^\/v1\/symbols\/([A-Z0-9.]+)$/i);
      if (symbolMatch)
        return await handleSymbolInfo(symbolMatch[1].toUpperCase(), env, cors);

      const barsMatch = path.match(/^\/v1\/bars\/([A-Z0-9.]+)$/i);
      if (barsMatch)
        return await handleBars(barsMatch[1].toUpperCase(), request, env, cors, ip);

      // Pre-computed academic variables (25 measures) and data-quality metrics
      const varsMatch = path.match(/^\/v1\/variables\/([A-Z0-9.]+)$/i);
      if (varsMatch)
        return await handleDerived(varsMatch[1].toUpperCase(), 'variables', request, env, cors, ip);

      const qualMatch = path.match(/^\/v1\/quality\/([A-Z0-9.]+)$/i);
      if (qualMatch)
        return await handleDerived(qualMatch[1].toUpperCase(), 'quality', request, env, cors, ip);

      // Request a signed download URL (short-lived token)
      const dlRequestMatch = path.match(/^\/v1\/download-token\/([A-Z0-9.]+)$/i);
      if (dlRequestMatch)
        return await handleDownloadToken(dlRequestMatch[1].toUpperCase(), request, env, cors);

      // Use a signed download URL
      const dlMatch = path.match(/^\/v1\/download\/([A-Z0-9.]+)$/i);
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
  }
};

// ══════════════════════════════════════
// ── Rate Limiting ──
// ══════════════════════════════════════

async function checkRateLimit(env, key, ruleName) {
  const rule = RATE_LIMITS[ruleName];
  if (!rule) return { ok: true };

  const fullKey = `${ruleName}:${key}`;
  const now = Date.now();
  const windowMs = rule.window * 1000;

  const existing = await env.DB.prepare('SELECT count, window_start FROM rate_limits WHERE key = ?').bind(fullKey).first();

  if (!existing) {
    await env.DB.prepare('INSERT OR REPLACE INTO rate_limits (key, count, window_start) VALUES (?, 1, datetime("now"))').bind(fullKey).run();
    return { ok: true, remaining: rule.max - 1 };
  }

  const windowStart = new Date(existing.window_start).getTime();
  if (now - windowStart > windowMs) {
    // Window expired, reset
    await env.DB.prepare('UPDATE rate_limits SET count = 1, window_start = datetime("now") WHERE key = ?').bind(fullKey).run();
    return { ok: true, remaining: rule.max - 1 };
  }

  if (existing.count >= rule.max) {
    const retryAfter = Math.ceil((windowMs - (now - windowStart)) / 1000);
    return { ok: false, retryAfter };
  }

  await env.DB.prepare('UPDATE rate_limits SET count = count + 1 WHERE key = ?').bind(fullKey).run();
  return { ok: true, remaining: rule.max - existing.count - 1 };
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

function handleOrcidStart(request, env, cors) {
  const reqUrl = new URL(request.url);
  const state = reqUrl.searchParams.get('state') || '';

  const url = new URL('https://orcid.org/oauth/authorize');
  url.searchParams.set('client_id', env.ORCID_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', '/authenticate');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_ORCID);
  if (state) url.searchParams.set('state', state);
  return Response.redirect(url.toString(), 302);
}

async function handleOrcidLinkInit(request, env, cors) {
  // Requires session auth (user must be logged in)
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  const userId = user.user_id || user.id;
  const state = generateId();
  const expires = new Date(Date.now() + 10 * 60 * 1000).toISOString();

  await env.DB.prepare(
    'INSERT INTO oauth_state (state, user_id, provider, expires_at) VALUES (?, ?, ?, ?)'
  ).bind(state, userId, 'orcid', expires).run();

  const url = new URL('https://orcid.org/oauth/authorize');
  url.searchParams.set('client_id', env.ORCID_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', '/authenticate');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_ORCID);
  url.searchParams.set('state', state);

  return jsonRes({ url: url.toString() }, 200, cors);
}

async function handleOrcidCallback(request, env, ip, ua, country) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const error = url.searchParams.get('error');

  if (error || !code) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=${encodeURIComponent(error || 'missing_code')}`, 302);
  }

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
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=token_exchange_failed`, 302);
  }

  const tokenData = await tokenRes.json();
  const orcidId = tokenData.orcid;
  const userName = tokenData.name || 'ORCID User';

  // Check for link state (user is linking ORCID to existing account)
  const stateToken = url.searchParams.get('state');
  let linkingUserId = null;
  if (stateToken) {
    const stateRec = await env.DB.prepare(
      'SELECT * FROM oauth_state WHERE state = ? AND provider = ? AND expires_at > datetime("now")'
    ).bind(stateToken, 'orcid').first();
    if (stateRec) {
      linkingUserId = stateRec.user_id;
      await env.DB.prepare('DELETE FROM oauth_state WHERE state = ?').bind(stateToken).run();
    }
  }

  // Check if anyone already has this ORCID iD linked
  let user = await env.DB.prepare('SELECT * FROM users WHERE orcid_id = ?').bind(orcidId).first();

  if (linkingUserId) {
    // Linking mode: tie this ORCID to the specified user
    if (user && user.id !== linkingUserId) {
      return Response.redirect(`${SITE_URL}/pages/account?oauth_error=orcid_already_linked_to_another_account`, 302);
    }
    // Fetch and store ORCID profile data
    const profile = await fetchOrcidProfile(orcidId);
    const profileJson = profile ? JSON.stringify(profile) : null;
    await env.DB.prepare('UPDATE users SET orcid_id = ?, orcid_profile_json = ? WHERE id = ?')
      .bind(orcidId, profileJson, linkingUserId).run();
    return Response.redirect(`${SITE_URL}/pages/account?orcid_linked=1`, 302);
  }

  if (!user) {
    // No session and no existing link → fetch profile and redirect to registration
    const profile = await fetchOrcidProfile(orcidId);
    const params = new URLSearchParams({
      oauth_provider: 'orcid',
      oauth_id: orcidId,
      oauth_name: profile?.fullName || userName
    });
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
    return Response.redirect(`${SITE_URL}/pages/download?${params.toString()}#register`, 302);
  }

  // Existing user with linked ORCID — log them in
  if (!user.is_active) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=account_deactivated`, 302);
  }

  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId } = await createSession(env, user.id, ip, ua);

  return new Response(null, {
    status: 302,
    headers: {
      'Location': `${SITE_URL}/pages/download?oauth_success=1&session=${sessionId}`,
      'Set-Cookie': `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`
    }
  });
}

function handleGoogleStart(env, cors) {
  const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  url.searchParams.set('client_id', env.GOOGLE_CLIENT_ID);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', 'openid email profile');
  url.searchParams.set('redirect_uri', OAUTH_REDIRECT_GOOGLE);
  url.searchParams.set('access_type', 'online');
  return Response.redirect(url.toString(), 302);
}

async function handleGoogleCallback(request, env, ip, ua, country) {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const error = url.searchParams.get('error');

  if (error || !code) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=${encodeURIComponent(error || 'missing_code')}`, 302);
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
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=token_exchange_failed`, 302);
  }

  const tokenData = await tokenRes.json();

  // Fetch user info
  const userRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
    headers: { 'Authorization': `Bearer ${tokenData.access_token}` }
  });
  if (!userRes.ok) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=userinfo_failed`, 302);
  }
  const profile = await userRes.json();
  const email = (profile.email || '').toLowerCase();
  const name = profile.name || email.split('@')[0];

  if (!email) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=no_email`, 302);
  }

  // Look up existing user by email
  let user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email).first();

  if (!user) {
    // Auto-create account — Google emails are already verified
    const apiKey = 'hfd_' + generateId();
    const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    const unsubscribeToken = generateId();
    const isAdmin = ADMIN_EMAILS.includes(email) ? 1 : 0;
    // Use a random strong password placeholder (user can reset later)
    const randomPassword = generateId() + generateId();
    const passwordHash = await hashPassword(randomPassword);

    await env.DB.prepare(
      'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, google_id, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, 0)'
    ).bind(
      name, email, passwordHash,
      '', country || '', '',
      apiKey, apiKeyExpires, isAdmin,
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
  }

  if (!user.is_active) {
    return Response.redirect(`${SITE_URL}/pages/download?oauth_error=account_deactivated`, 302);
  }

  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?')
    .bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, country).run();

  const { sessionId } = await createSession(env, user.id, ip, ua);

  return new Response(null, {
    status: 302,
    headers: {
      'Location': `${SITE_URL}/pages/download?oauth_success=1&session=${sessionId}`,
      'Set-Cookie': `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`
    }
  });
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

async function sendEmail(env, to, subject, htmlBody, fromEmail = FROM_EMAIL, fromName = FROM_NAME) {
  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from: `${fromName} <${fromEmail}>`,
      to: [to],
      subject,
      html: htmlBody
    })
  });
  if (!response.ok) {
    console.error('Resend error:', await response.text());
  }
  return response.ok;
}

async function sendEmailBatch(env, items) {
  // items: array of full email objects {from, to, subject, html} — Resend's
  // batch endpoint accepts up to 100 per call. One retry on 429/network error.
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const response = await fetch('https://api.resend.com/emails/batch', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${env.RESEND_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(items)
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
        `<p><strong>${email}</strong> was automatically unsubscribed from the newsletter.</p>` +
        `<p><strong>Reason:</strong> ${reason}${detail ? ' — ' + detail : ''}</p>` +
        `<p style="color:#6b7280;font-size:13px;">Triggered by Resend webhook event <code>${type}</code>. ` +
        `Resend has also added this address to its suppression list, so future sends skip it automatically.</p>`
      );
    }
  }

  return new Response(JSON.stringify({ received: true, type, removed }), { status: 200, headers: jsonHeaders });
}

function verificationEmail(name, token) {
  const link = SITE_URL + '/pages/verify?token=' + token;
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">Welcome to ElkassabgiData</h2>
      <p>Hi ${name},</p>
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

function adminNotificationEmail(user, ip, ua, country) {
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">New HF Data Library Registration</h2>
      <p>A new user has registered:</p>
      <table style="width: 100%; border-collapse: collapse; margin: 1rem 0;">
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Name</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${user.name}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Email</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${user.email}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Institution</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${user.institution}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Country</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${user.country}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>Role</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${user.role}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>IP Address</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${ip}</td></tr>
        <tr><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;"><strong>CF Country</strong></td><td style="padding: 6px 12px; border-bottom: 1px solid #e5e7eb;">${country}</td></tr>
        <tr><td style="padding: 6px 12px;"><strong>User Agent</strong></td><td style="padding: 6px 12px; font-size: 0.85rem; color: #6b7280;">${ua}</td></tr>
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

function resetEmail(name, token) {
  const link = SITE_URL + '/pages/reset?token=' + token;
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">Password Reset</h2>
      <p>Hi ${name},</p>
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

async function getSessionUser(request, env) {
  // Check cookie first
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(/hfd_session=([a-f0-9]+)/);
  let sessionId = match ? match[1] : null;

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
  const session = await env.DB.prepare(
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, " +
    "s.kind AS session_kind, s.audience AS session_audience, " +
    "s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.expires_at > datetime('now') " +
    "AND (s.kind IS NULL OR s.kind = 'web')"
  ).bind(sessionId).first();

  if (!session || !session.is_active) return null;
  // Defense in depth: assert kind even though the query already filters it.
  if (session.session_kind && session.session_kind !== 'web') return null;
  return session;
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

async function getUserByApiKey(request, env) {
  // Check header first
  let apiKey = request.headers.get('X-API-Key');
  // Fallback to query parameter (for direct browser downloads)
  if (!apiKey) {
    const url = new URL(request.url);
    apiKey = url.searchParams.get('api_key');
  }
  if (!apiKey) return null;
  // Check expiration
  return await env.DB.prepare(
    'SELECT * FROM users WHERE api_key = ? AND is_active = 1 AND (api_key_expires_at IS NULL OR api_key_expires_at > datetime("now"))'
  ).bind(apiKey).first();
}

async function requireAuth(request, env) {
  let user = await getSessionUser(request, env);
  if (!user) user = await getUserByApiKey(request, env);
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
// be captured by getSessionUser's /hfd_session=([a-f0-9]+)/ cookie regex, so an
// M2 token can't even be parsed as a web session id (defense in depth over the
// kind predicate).
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
  const row = await env.DB.prepare(
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, s.kind AS session_kind, " +
    "s.audience AS session_audience, s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.kind = 'family_access' AND s.expires_at > datetime('now')"
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
async function requireDataAuth(request, env) {
  return (await requireAuth(request, env)) || (await validateFamilyToken(request, env));
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
    if (path === '/account' || path === '/account/regenerate-key' || path === '/account/logout') return new Response('Not found', { status: 404 });
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
  // Rate limit
  const rl = await checkRateLimit(env, ip, 'api:register');
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
  const orcidFromOauth = body.orcid_id || null;

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

  const passwordHash = await hashPassword(password);
  const apiKey = 'hfd_' + generateId();
  const unsubscribeToken = generateId();
  const isAdmin = ADMIN_EMAILS.includes(email.toLowerCase()) ? 1 : 0;

  const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();

  await env.DB.prepare(
    'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)'
  ).bind(name, email.toLowerCase(), passwordHash, institution, normalizedCountry, role, apiKey, apiKeyExpires, isAdmin, isAdmin ? 1 : 0, newsletter, unsubscribeToken, ip, ua, orcidFromOauth, orcidProfileJson).run();

  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  // Log registration
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)')
    .bind(user.id, ip, ua, userCountry).run();

  // Send verification email (skip for admins — auto-verified)
  if (!isAdmin) {
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

  const res = jsonRes({
    message: isAdmin ? 'Registration successful' : 'Registration successful. Please check your email to verify your account.',
    api_key: apiKey,
    session: sessionId,
    email_verified: isAdmin ? true : false
  }, 201, cors);

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 86400}`);
  return res;
}

async function handleLogin(request, env, cors, ip, ua, country) {
  // Rate limit per IP
  const rl = await checkRateLimit(env, ip, 'api:login');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { email, password } = body;
  if (!email || !password) {
    return jsonRes({ error: 'Required: email, password' }, 400, cors);
  }

  const user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  if (!user || !(await verifyPassword(password, user.password_hash))) {
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
  const match = cookie.match(/hfd_session=([a-f0-9]+)/);
  if (match) {
    await env.DB.prepare('DELETE FROM sessions WHERE id = ?').bind(match[1]).run();
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
    profile_complete: !!user.profile_complete,
    orcid_profile: user.orcid_profile_json ? JSON.parse(user.orcid_profile_json) : null,
    newsletter_subscribed: !!user.newsletter_subscribed,
    created_at: user.created_at,
    download_count: user.download_count,
    total_bytes_downloaded: user.total_bytes_downloaded
  }, 200, cors);
}

// ── 2FA handlers ──

async function handle2faSetup(request, env, cors) {
  const user = await getSessionUser(request, env);
  if (!user) return jsonRes({ error: 'Session required' }, 401, cors);

  const userId = user.user_id || user.id;
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
  const rl2 = await checkRateLimit(env, 'tfa:' + pending_token, 'api:2fa');
  if (!rl2.ok) {
    await env.DB.prepare('DELETE FROM totp_pending WHERE token = ?').bind(pending_token).run();
    return jsonRes({ error: 'Too many attempts. Please log in again.' }, 429, cors);
  }

  const pending = await env.DB.prepare('SELECT * FROM totp_pending WHERE token = ? AND expires_at > datetime("now")').bind(pending_token).first();
  if (!pending) return jsonRes({ error: 'Invalid or expired login attempt. Please log in again.' }, 401, cors);

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
  await env.DB.prepare('DELETE FROM sessions WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM login_history WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM download_log WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM password_resets WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM totp_pending WHERE user_id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM users WHERE id = ?').bind(userId).run();

  // Notify admin
  try {
    await sendEmail(
      env,
      ADMIN_NOTIFY,
      `Account deleted: ${dbUser.email}`,
      `<p>User <strong>${dbUser.email}</strong> has self-deleted their account. All personal data has been removed from the database.</p>`
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
  if (body.name !== undefined) {
    if (body.name.length > 100) return jsonRes({ error: 'Name too long' }, 400, cors);
    updates.push('name = ?'); values.push(body.name);
  }
  if (body.institution !== undefined) {
    if (body.institution.length > 200) return jsonRes({ error: 'Institution too long' }, 400, cors);
    updates.push('institution = ?'); values.push(body.institution);
  }
  if (body.country !== undefined) {
    if (body.country.length > 100) return jsonRes({ error: 'Country too long' }, 400, cors);
    updates.push('country = ?'); values.push(body.country);
  }
  if (body.role !== undefined) {
    if (body.role.length > 100) return jsonRes({ error: 'Role too long' }, 400, cors);
    updates.push('role = ?'); values.push(body.role);
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
  await env.DB.prepare('DELETE FROM sessions WHERE user_id = ? AND id != ?').bind(userId, currentSession).run();

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

  const reset = await env.DB.prepare(
    'SELECT * FROM password_resets WHERE token = ? AND used = 0 AND expires_at > datetime("now")'
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
  const verifyToken = generateId();
  const verifyExpires = new Date(Date.now() + 86400000).toISOString();
  await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)')
    .bind(userId, verifyToken, verifyExpires).run();
  await sendEmail(env, user.email, 'Verify your ElkassabgiData account', verificationEmail(user.name, verifyToken), FROM_EMAIL, 'ElkassabgiData');

  return jsonRes({ message: 'Verification email sent. Check your inbox.' }, 200, cors);
}

async function handleResetRequest(request, env, cors) {
  const ip = request.headers.get('cf-connecting-ip') || 'unknown';
  const rl = await checkRateLimit(env, ip, 'api:reset');
  if (!rl.ok) return rateLimitResponse(rl.retryAfter, cors);

  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { email } = body;
  if (!email) return jsonRes({ error: 'Required: email' }, 400, cors);

  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();

  // Always return success to avoid email enumeration
  if (user) {
    const token = generateId();
    const expires = new Date(Date.now() + 3600000).toISOString(); // 1 hour
    await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)')
      .bind(user.id, token, expires).run();
    const u = await env.DB.prepare('SELECT name FROM users WHERE id = ?').bind(user.id).first();
    await sendEmail(env, email.toLowerCase(), 'Reset your HF Data Library password', resetEmail(u.name, token));
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

  const reset = await env.DB.prepare(
    'SELECT * FROM password_resets WHERE token = ? AND used = 0 AND expires_at > datetime("now")'
  ).bind(token).first();

  if (!reset) return jsonRes({ error: 'Invalid or expired reset token' }, 400, cors);

  const passwordHash = await hashPassword(password);
  await env.DB.prepare('UPDATE users SET password_hash = ? WHERE id = ?').bind(passwordHash, reset.user_id).run();
  await env.DB.prepare('UPDATE password_resets SET used = 1 WHERE id = ?').bind(reset.id).run();

  // Invalidate all sessions
  await env.DB.prepare('DELETE FROM sessions WHERE user_id = ?').bind(reset.user_id).run();

  return jsonRes({ message: 'Password reset successful. Please log in.' }, 200, cors);
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

function buildNewsletterHtml(subject, bodyHtml, userName, unsubscribeUrl) {
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <div style="background: #1a2332; padding: 1.5rem; text-align: center;">
        <h1 style="color: #d4a843; margin: 0; font-size: 1.5rem;">HF Data Library</h1>
      </div>
      <div style="padding: 2rem 1.5rem;">
        <p>Hi ${userName},</p>
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

  const user = await requireAuth(request, env);
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
  if (!user) { user = await getUserByApiKey(request, env); issuedVia = 'api'; }
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
    tokenRecord = await env.DB.prepare(
      'SELECT * FROM download_tokens WHERE token = ? AND used = 0 AND expires_at > datetime("now")'
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
  }
  const format = tokenRecord ? tokenRecord.format : ((url.searchParams.get('format') || 'parquet').toLowerCase());

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
  const user = await requireAuth(request, env);
  if (!user || !user.is_admin) {
    return jsonRes({ error: 'Admin access required' }, 403, cors);
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
    // Sort whitelist — never interpolate raw input into SQL.
    const SORT_COLS = {
      created_at: 'created_at', name: 'name COLLATE NOCASE', email: 'email COLLATE NOCASE',
      institution: 'institution COLLATE NOCASE', country: 'country COLLATE NOCASE',
      downloads: 'download_count', logins: 'login_count', last_login: 'last_login_at',
    };
    const sortCol = SORT_COLS[url.searchParams.get('sort')] || SORT_COLS.created_at;
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
    const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';

    // ip_country: geolocation (Cloudflare cf-ipcountry) of the user's last-login
    // IP, resolved from login_history — distinct from self-declared users.country.
    const users = await env.DB.prepare(
      'SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, is_vip, newsletter_subscribed, created_at, last_login_at, last_login_ip, last_login_ua, login_count, download_count, total_bytes_downloaded, notes, ' +
      '(SELECT lh.country FROM login_history lh WHERE lh.ip_address = users.last_login_ip ' +
      'AND lh.country IS NOT NULL AND lh.country != "" AND lh.country != "unknown" ' +
      'ORDER BY lh.id DESC LIMIT 1) AS ip_country ' +
      'FROM users ' + whereSql + ` ORDER BY ${sortCol} ${dir} LIMIT ? OFFSET ?`
    ).bind(...args, limit, offset).all();

    const totalAll = await env.DB.prepare('SELECT COUNT(*) as count FROM users').first();
    const totalMatch = whereSql
      ? await env.DB.prepare('SELECT COUNT(*) as count FROM users ' + whereSql).bind(...args).first()
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
        hide_institution: u.hide_institution ? true : false
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

    // If deactivating, kill their sessions
    if (body.is_active === false || body.is_active === 0) {
      await env.DB.prepare('DELETE FROM sessions WHERE user_id = ?').bind(uid).run();
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

async function handlePublicStats(env, cors) {
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

  return jsonRes({
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
  }, 200, cors);
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
  const m = cookie.match(/ekd_session=([A-Za-z0-9_-]+)/);
  if (!m) return null;
  const idHash = await sha256Hex(m[1]);
  const row = await env.DB.prepare(
    "SELECT u.*, s.id AS session_id, s.kind AS session_kind, s.expires_at AS session_expires_at " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.kind = 'idp_master' AND s.expires_at > datetime('now')"
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
  if (!user) {
    // No IdP session → the real login/register auth page (M2b-2a). Uses the
    // Turnstile-permitting CSP; on submit, /login or /register sets ekd_session
    // and 303s back with the code (the auth submission is the consent). The SDK
    // popup passes hint=register to open straight to the sign-up tab.
    const hint = q.get('hint') === 'register' ? 'register' : 'login';
    return new Response(
      renderAuthPage(row, { clientId, redirectUri, state, codeChallenge, method }, { tab: hint, error: '', loginEmail: '' }),
      { status: 200, headers: authPageHeaders }
    );
  }
  // signed in → branded consent with a gesture-bound POST form
  const cookie = request.headers.get('cookie') || '';
  const cm = cookie.match(/ekd_session=([A-Za-z0-9_-]+)/);
  const idpSessionHash = cm ? await sha256Hex(cm[1]) : '';
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
  const cm = cookie.match(/ekd_session=([A-Za-z0-9_-]+)/);
  const idpSessionHash = cm ? await sha256Hex(cm[1]) : '';
  if (!(await verifyGestureToken(env, gesture, idpSessionHash, clientId, state, codeChallenge))) {
    return new Response('bad_gesture', { status: 403 });
  }
  return await mintCodeAndRedirect(env, user.id, clientId, redirectUri, state, codeChallenge, 303);
}

// Shared code-mint + redirect (used by consent POST and OAuth callbacks).
async function mintCodeAndRedirect(env, userId, clientOrigin, redirectExact, state, codeChallenge, status) {
  const rawCode = generateToken();
  const codeHash = await sha256Hex(rawCode);
  const consentToken = generateToken();
  await env.DB.prepare(
    "INSERT INTO sso_codes (code_hash,user_id,client_origin,state,code_challenge,consent_token,used,expires_at) " +
    "VALUES (?,?,?,?,?,?,0,datetime('now','+" + CODE_TTL_SEC + " seconds'))"
  ).bind(codeHash, userId, clientOrigin, state, codeChallenge, consentToken).run();
  const dest = redirectExact + '#code=' + encodeURIComponent(rawCode) + '&state=' + encodeURIComponent(state);
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
  const claim = await env.DB.prepare(
    "UPDATE sso_codes SET used=1 WHERE code_hash=? AND used=0 AND expires_at>datetime('now')"
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
  const claim = await env.DB.prepare(
    "UPDATE sso_refresh_tokens SET used=1, used_at=datetime('now'), grace_until=datetime('now','+" + RT_GRACE_SEC + " seconds') " +
    "WHERE token_hash=? AND used=0 AND revoked=0 AND absolute_expires_at>datetime('now')"
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
  const inGrace = await env.DB.prepare(
    "SELECT 1 FROM sso_refresh_tokens WHERE token_hash=? AND revoked=0 AND grace_until>datetime('now')"
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
  const cm = cookie.match(/ekd_session=([A-Za-z0-9_-]+)/);
  if (cm) {
    const idHash = await sha256Hex(cm[1]);
    await env.DB.prepare("DELETE FROM sessions WHERE id=? AND kind='idp_master'").bind(idHash).run();
  }
  if (body && body.refresh_token) {
    const rtHash = await sha256Hex(body.refresh_token);
    const rt = await env.DB.prepare("SELECT chain_id FROM sso_refresh_tokens WHERE token_hash=?").bind(rtHash).first();
    if (rt) await revokeChain(env, rt.chain_id);
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
    ".lk{display:inline-block;background:#0f1729;border:1px solid #2a3550;color:#9ca3af;border-radius:6px;padding:.15rem .5rem;font-size:.78rem;margin-right:.4rem}";
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
    '<h2>Profile</h2><dl><dt>Institution</dt><dd>' + inst + '</dd><dt>Country</dt><dd>' + country + '</dd><dt>Role</dt><dd>' + role + '</dd></dl>' +
    (orcid || google ? '<p style="margin-top:.6rem">' + orcid + google + '</p>' : '') +
    '<h2>Session</h2><form method="POST" action="/account/logout" class="row"><button type="submit">Log out everywhere</button></form>' +
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
    // Log out EVERYWHERE: drop the SSO session + every live family access token,
    // and revoke every refresh chain for this account.
    await env.DB.prepare("DELETE FROM sessions WHERE user_id = ? AND kind IN ('idp_master','family_access')").bind(user.id).run();
    await env.DB.prepare("UPDATE sso_refresh_tokens SET revoked = 1 WHERE user_id = ?").bind(user.id).run();
  } else {
    // No valid session — still clear whatever ekd_session cookie is present.
    const cookie = request.headers.get('cookie') || '';
    const cm = cookie.match(/ekd_session=([A-Za-z0-9_-]+)/);
    if (cm) { const idHash = await sha256Hex(cm[1]); await env.DB.prepare("DELETE FROM sessions WHERE id = ?").bind(idHash).run(); }
  }
  const clear = 'ekd_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0';
  return new Response(renderSignedOutPage(), { status: 200, headers: { ...accountPageHeaders, 'Set-Cookie': clear } });
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
  const claim = await env.DB.prepare(
    "UPDATE sso_oauth_state SET used=1 WHERE state=? AND used=0 AND provider=? AND expires_at>datetime('now')"
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
    // Create only with a public email that belongs to nobody else. Absent or
    // colliding email → honest degrade (no takeover, no placeholder, no dup).
    // The smooth orcid_prefill register-completion is deferred to M2b-3.
    if (!profEmail) return oauthErrorPage('orcid_no_email');
    const collision = await env.DB.prepare('SELECT id FROM users WHERE email=?').bind(profEmail).first();
    if (collision) return oauthErrorPage('orcid_email_taken');
    const name = (profile && profile.fullName) || (tokenData.name || 'ORCID User');
    const inst = (profile && profile.currentEmployment && profile.currentEmployment[0] && profile.currentEmployment[0].organization) || '';
    const role = (profile && profile.currentEmployment && profile.currentEmployment[0] && profile.currentEmployment[0].role) || '';
    const ctry = (profile && profile.country) || country || '';
    const profileJson = profile ? JSON.stringify(profile) : null;
    const apiKey = 'hfd_' + generateId();
    const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
    const unsub = generateId();
    const isAdmin = ADMIN_EMAILS.includes(profEmail) ? 1 : 0;
    const rndPw = generateId() + generateId();
    const pwHash = await hashPassword(rndPw);
    // ORCID email is UNVERIFIED by us (email_verified=0); family newsletter default 0.
    await env.DB.prepare(
      'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, 1)'
    ).bind(name, profEmail, pwHash, inst, ctry, role, apiKey, apiKeyExpires, isAdmin, unsub, ip, ua, orcidId, profileJson).run();
    user = await env.DB.prepare('SELECT * FROM users WHERE orcid_id=?').bind(orcidId).first();
    try {
      await sendEmail(env, ADMIN_NOTIFY, 'New registration via ORCID (family SSO): ' + name,
        adminNotificationEmail({ name, email: profEmail, institution: inst || '(via ORCID / accounts)', country: ctry, role: role || 'Not specified' }, ip, ua, country));
    } catch (e) { /* non-fatal */ }
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
    "connect-src https://challenges.cloudflare.com; " +
    "style-src 'unsafe-inline'; img-src https: data:; " +
    // https: so the login/register form's SUCCESS 303 to the client's cross-origin
    // callback isn't blocked by form-action (enforced on redirect targets).
    "form-action 'self' https:; frame-ancestors 'none'; base-uri 'none'",
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
async function loginAndRedirect(env, userId, ip, ua, p) {
  const idp = await createIdpSession(env, userId, ip, ua);
  const resp = await mintCodeAndRedirect(env, userId, p.clientId, p.row.redirect_exact, p.state, p.codeChallenge, 303);
  resp.headers.append('Set-Cookie', idp.cookie);
  return resp;
}

// ── Auth page (login/register tabs) + 2FA page ──
function hiddenAuthParams(p) {
  return '<input type="hidden" name="client_id" value="' + htmlEncode(p.clientId) + '">' +
    '<input type="hidden" name="redirect_uri" value="' + htmlEncode(p.redirectUri) + '">' +
    '<input type="hidden" name="state" value="' + htmlEncode(p.state) + '">' +
    '<input type="hidden" name="code_challenge" value="' + htmlEncode(p.codeChallenge) + '">' +
    '<input type="hidden" name="code_challenge_method" value="S256">';
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
    ".muted{color:#9ca3af;font-size:.8rem;text-align:center;margin-top:.8rem}";
  return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Sign in to ElkassabgiData</title>' +
    '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>' +
    '<style>' + S + '</style></head><body><div class="card">' +
    '<h1>Continue to ' + brand + '</h1>' + err +
    '<input type="radio" name="authtab" id="tl" ' + loginChecked + '>' +
    '<input type="radio" name="authtab" id="tr" ' + regChecked + '>' +
    '<div class="tabs"><label for="tl">Log in</label><label for="tr">Sign up</label></div>' +
    // login panel
    '<div class="panel" id="pl"><form method="POST" action="/login">' + hiddenAuthParams(p) +
    '<input type="email" name="email" placeholder="Email" value="' + em + '" required autocomplete="email">' +
    '<input type="password" name="password" placeholder="Password" required autocomplete="current-password">' +
    '<button type="submit">Log in</button></form>' +
    '<div class="oauth"><a href="/v1/auth/google/start?' + oauthQ + '">Continue with Google</a>' +
    '<a href="/v1/auth/orcid/start?' + oauthQ + '">Continue with ORCID</a></div></div>' +
    // register panel
    '<div class="panel" id="pr"><form method="POST" action="/register">' + hiddenAuthParams(p) +
    '<input type="text" name="name" placeholder="Full name" required maxlength="100">' +
    '<input type="email" name="email" placeholder="Email" required autocomplete="email">' +
    '<input type="password" name="password" placeholder="Password (min 10 chars)" required autocomplete="new-password">' +
    '<input type="text" name="institution" placeholder="Institution" required maxlength="200">' +
    '<input type="text" name="country" placeholder="Country" required maxlength="100">' +
    '<input type="text" name="role" placeholder="Role (e.g. Professor, Student)" required maxlength="100">' +
    '<fieldset><legend>Newsletters (optional)</legend>' + news + '</fieldset>' +
    '<div class="cf-turnstile" data-sitekey="0x4AAAAAAC5ydfuRj9dEK0kY" data-response-field-name="turnstile_token" data-theme="auto"></div>' +
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
    '<input type="hidden" name="pending_token" value="' + htmlEncode(pendingToken) + '">' +
    '<input type="text" name="code" inputmode="numeric" pattern="[0-9]{6}" maxlength="6" placeholder="000000" required autofocus>' +
    '<button type="submit">Verify</button></form></div></body></html>';
}

// ── POST /login ──
async function handleAccountsLogin(request, env, ip, ua, country) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const rl = await checkRateLimit(env, ip, 'api:login');
  if (!rl.ok) return new Response('Too many attempts. Try again later.', { status: 429, headers: authPageHeaders });
  let body;
  try { body = await request.formData(); } catch { return new Response('Bad request', { status: 400, headers: { 'Cache-Control': 'no-store' } }); }
  const p = paramsFromForm(body);
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('<h1>Invalid request</h1>', { status: v.status, headers: authPageHeaders });
  p.row = v.row;
  const email = (body.get('email') || '').toLowerCase();
  const password = body.get('password') || '';
  const user = await env.DB.prepare('SELECT * FROM users WHERE email = ?').bind(email).first();
  if (!user || !(await verifyPassword(password, user.password_hash))) {
    if (user) await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)').bind(user.id, ip, ua, country).run();
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Invalid email or password', loginEmail: email }), { status: 200, headers: authPageHeaders });
  }
  if (!user.is_active) {
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Account has been deactivated.', loginEmail: email }), { status: 200, headers: authPageHeaders });
  }
  if (user.totp_enabled) {
    const pendingToken = generateId();
    const pendingExpires = new Date(Date.now() + 10 * 60 * 1000).toISOString();
    await env.DB.prepare('INSERT INTO totp_pending (token, user_id, expires_at, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)').bind(pendingToken, user.id, pendingExpires, ip, ua).run();
    return new Response(renderTwoFactorPage(pendingToken, p, ''), { status: 200, headers: authPageHeaders });
  }
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await loginAndRedirect(env, user.id, ip, ua, p);
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
  // Cap TOTP guesses per pending token (IP-independent), so a compromised
  // password can't brute-force the 6-digit code within the 10-min pending window.
  const rl2 = await checkRateLimit(env, 'tfa:' + pendingToken, 'api:2fa');
  if (!rl2.ok) {
    await env.DB.prepare('DELETE FROM totp_pending WHERE token = ?').bind(pendingToken).run();
    return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Too many attempts — please sign in again.' }), { status: 200, headers: authPageHeaders });
  }
  const pending = await env.DB.prepare('SELECT * FROM totp_pending WHERE token = ? AND expires_at > datetime("now")').bind(pendingToken).first();
  if (!pending) return new Response(renderAuthPage(v.row, p, { tab: 'login', error: 'Login expired — please sign in again.' }), { status: 200, headers: authPageHeaders });
  const user = await env.DB.prepare('SELECT * FROM users WHERE id = ?').bind(pending.user_id).first();
  if (!user || !user.totp_secret || !(await verifyTotp(user.totp_secret, code))) {
    if (user) await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 0)').bind(user.id, ip, ua, country).run();
    return new Response(renderTwoFactorPage(pendingToken, p, 'Invalid 2FA code'), { status: 200, headers: authPageHeaders });
  }
  await env.DB.prepare('DELETE FROM totp_pending WHERE token = ?').bind(pendingToken).run();
  await env.DB.prepare('UPDATE users SET last_login_at = datetime("now"), last_login_ip = ?, last_login_ua = ?, login_count = login_count + 1 WHERE id = ?').bind(ip, ua, user.id).run();
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, country).run();
  return await loginAndRedirect(env, user.id, ip, ua, p);
}

// ── POST /register ──  (duplicates handleRegister's validation/creation; api.* untouched)
async function handleAccountsRegister(request, env, ip, ua, country) {
  if (!assertSameOriginForm(request)) return new Response('cross_site_blocked', { status: 403 });
  const rl = await checkRateLimit(env, ip, 'api:register');
  if (!rl.ok) return new Response('Too many attempts. Try again later.', { status: 429, headers: authPageHeaders });
  let body;
  try { body = await request.formData(); } catch { return new Response('Bad request', { status: 400, headers: { 'Cache-Control': 'no-store' } }); }
  const p = paramsFromForm(body);
  const v = await validateAuthorizeParams(env, p);
  if (!v.ok) return new Response('<h1>Invalid request</h1>', { status: v.status, headers: authPageHeaders });
  p.row = v.row;
  const rerr = (msg, tab, extra) => new Response(renderAuthPage(v.row, p, Object.assign({ tab: tab || 'register', error: msg }, extra || {})), { status: 200, headers: authPageHeaders });

  if (!(await verifyTurnstile(env, body.get('turnstile_token'), ip))) return rerr('CAPTCHA verification failed. Please try again.');
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

  const passwordHash = await hashPassword(password);
  const apiKey = 'hfd_' + generateId();
  const unsubscribeToken = generateId();
  const isAdmin = ADMIN_EMAILS.includes(email.toLowerCase()) ? 1 : 0;
  const apiKeyExpires = new Date(Date.now() + API_KEY_DAYS * 86400000).toISOString();
  await env.DB.prepare(
    'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, api_key_expires_at, is_admin, email_verified, newsletter_subscribed, unsubscribe_token, last_login_ip, last_login_ua, orcid_id, orcid_profile_json, profile_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)'
  ).bind(name, email.toLowerCase(), passwordHash, institution, normalizedCountry, role, apiKey, apiKeyExpires, isAdmin, isAdmin ? 1 : 0, hfSelected ? 1 : 0, unsubscribeToken, ip, ua, null, null).run();
  const user = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();
  await applyNewsletterPrefs(env, user.id, prefs);
  await env.DB.prepare('INSERT INTO login_history (user_id, ip_address, user_agent, country, success) VALUES (?, ?, ?, ?, 1)').bind(user.id, ip, ua, userCountry).run();
  if (!isAdmin) {
    const verifyToken = generateId();
    const verifyExpires = new Date(Date.now() + 86400000).toISOString();
    await env.DB.prepare('INSERT INTO password_resets (user_id, token, expires_at) VALUES (?, ?, ?)').bind(user.id, verifyToken, verifyExpires).run();
    try { await sendEmail(env, email.toLowerCase(), 'Verify your ElkassabgiData account', verificationEmail(name, verifyToken), FROM_EMAIL, 'ElkassabgiData'); } catch (e) { /* non-blocking */ }
  }
  try { await sendEmail(env, ADMIN_NOTIFY, `New registration: ${name} (${institution})`, adminNotificationEmail({ name, email: email.toLowerCase(), institution, country: userCountry, role }, ip, ua, country)); } catch (e) { /* non-blocking */ }
  return await loginAndRedirect(env, user.id, ip, ua, p);
}
