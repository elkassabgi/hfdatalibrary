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

// Allowed CORS origins
const ALLOWED_ORIGINS = [
  'https://hfdatalibrary.com',
  'https://www.hfdatalibrary.com',
  'http://localhost:8080', // for local dev
];

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
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const ip = request.headers.get('cf-connecting-ip') || 'unknown';
    const ua = request.headers.get('user-agent') || 'unknown';
    const country = request.headers.get('cf-ipcountry') || 'unknown';

    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = ALLOWED_ORIGINS.includes(origin) ? origin : 'https://hfdatalibrary.com';
    const cors = {
      'Access-Control-Allow-Origin': allowedOrigin,
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
      'Access-Control-Allow-Credentials': 'true',
      'Access-Control-Max-Age': '86400',
      'Vary': 'Origin',
    };

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
      'Set-Cookie': `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${SESSION_DAYS * 86400}`
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
      'Set-Cookie': `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${SESSION_DAYS * 86400}`
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

function verificationEmail(name, token) {
  const link = SITE_URL + '/pages/verify?token=' + token;
  return `
    <div style="font-family: Georgia, serif; max-width: 600px; margin: 0 auto; color: #1a2332;">
      <h2 style="color: #1a2332;">Welcome to the HF Data Library</h2>
      <p>Hi ${name},</p>
      <p>Thank you for registering. Please verify your email address to activate your account and start downloading data.</p>
      <p style="text-align: center; margin: 2rem 0;">
        <a href="${link}" style="background: #2563eb; color: #fff; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: 600;">Verify Email</a>
      </p>
      <p style="font-size: 0.9rem; color: #6b7280;">Or copy this link: ${link}</p>
      <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 2rem 0;">
      <p style="font-size: 0.8rem; color: #9ca3af;">HF Data Library — Ahmed Elkassabgi, University of Central Arkansas<br>
      <a href="https://hfdatalibrary.com" style="color: #2563eb;">hfdatalibrary.com</a></p>
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

  const session = await env.DB.prepare(
    'SELECT s.*, u.* FROM sessions s JOIN users u ON s.user_id = u.id WHERE s.id = ? AND s.expires_at > datetime("now")'
  ).bind(sessionId).first();

  if (!session || !session.is_active) return null;
  return session;
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
    await sendEmail(env, email.toLowerCase(), 'Verify your HF Data Library account', verificationEmail(name, verifyToken));
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

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${SESSION_DAYS * 86400}`);
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

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${SESSION_DAYS * 86400}`);
  return res;
}

async function handleLogout(request, env, cors) {
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(/hfd_session=([a-f0-9]+)/);
  if (match) {
    await env.DB.prepare('DELETE FROM sessions WHERE id = ?').bind(match[1]).run();
  }
  const res = jsonRes({ message: 'Logged out' }, 200, cors);
  res.headers.set('Set-Cookie', 'hfd_session=; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=0');
  return res;
}

async function handleMe(request, env, cors) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Not authenticated' }, 401, cors);

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

  res.headers.set('Set-Cookie', `hfd_session=${sessionId}; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=${SESSION_DAYS * 86400}`);
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
  res.headers.set('Set-Cookie', 'hfd_session=; Path=/; HttpOnly; Secure; SameSite=None; Max-Age=0');
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

  // Kill all other sessions as a security measure
  const currentSession = user.id; // session id
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
  await sendEmail(env, user.email, 'Verify your HF Data Library account', verificationEmail(user.name, verifyToken));

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

  // Send emails (Resend allows batch sends, but let's be safe and send individually for now)
  for (const sub of subscribers.results) {
    const unsubUrl = `${SITE_URL}/pages/unsubscribe?token=${sub.unsubscribe_token}`;
    const html = buildNewsletterHtml(subject, body_html, sub.name, unsubUrl);
    const ok = await sendEmail(env, sub.email, subject, html, NEWSLETTER_FROM, NEWSLETTER_FROM_NAME);
    if (ok) success++; else failed++;
  }

  const userId = user.user_id || user.id;
  await env.DB.prepare(
    'INSERT INTO newsletter_campaigns (subject, body_html, sent_by_user_id, recipients_count, success_count, failed_count) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(subject, body_html, userId, total, success, failed).run();

  // Audit log
  const ip = request.headers.get('cf-connecting-ip') || 'unknown';
  await auditLog(env, user, 'send_newsletter', null, null, `${total} recipients, subject: ${subject}`, ip);

  return jsonRes({ message: 'Newsletter sent', total, success, failed }, 200, cors);
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
    .map(o => ({ ticker: o.key.replace('clean/', '').replace('.parquet', ''), size_bytes: o.size, last_modified: o.uploaded }))
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
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Authentication required. Log in or provide X-API-Key header.' }, 401, cors);
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
  await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .bind(userId, user.api_key, ticker, version, '/v1/bars', ip, obj.size).run();

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${ticker}_${version}.parquet"`, 'Content-Length': obj.size }
  });
}

const VALID_TIMEFRAMES = ['1min', '5min', '15min', '30min', 'hourly', 'daily', 'weekly', 'monthly'];

async function handleDownloadToken(ticker, request, env, cors) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Authentication required' }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data.' }, 403, cors);
  if (user.profile_complete === 0) return jsonRes({ error: 'Please complete your profile (institution, country, role) before downloading.' }, 403, cors);

  const url = new URL(request.url);
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
    'INSERT INTO download_tokens (token, user_id, ticker, version, format, expires_at) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(token, userId, ticker, versionTf, format, expires).run();

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
    user = await requireAuth(request, env);
  }

  if (!user) return jsonRes({ error: 'Authentication required' }, 401, cors);
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
  await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .bind(userId, user.api_key, ticker, version, '/v1/download', ip, obj.size).run();

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': contentType, 'Content-Disposition': `attachment; filename="${filename}"`, 'Content-Length': obj.size }
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

  // GET /v1/admin/users — list all users
  if (path === '/v1/admin/users' && request.method === 'GET') {
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '100');
    const offset = parseInt(url.searchParams.get('offset') || '0');

    const users = await env.DB.prepare(
      'SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, is_vip, newsletter_subscribed, created_at, last_login_at, last_login_ip, last_login_ua, login_count, download_count, total_bytes_downloaded, notes FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?'
    ).bind(limit, offset).all();

    const total = await env.DB.prepare('SELECT COUNT(*) as count FROM users').first();

    return jsonRes({ total: total.count, users: users.results }, 200, cors);
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

    return jsonRes({
      total_users: totalUsers.c,
      active_users: activeUsers.c,
      total_downloads: totalDownloads.c,
      total_bytes_served: totalBytes.s || 0,
      today_logins: todayLogins.c,
      today_downloads: todayDownloads.c,
      top_tickers: topTickers.results,
      recent_registrations: recentUsers.results
    }, 200, cors);
  }

  // GET /v1/admin/downloads — download log
  if (path === '/v1/admin/downloads') {
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '100');
    const logs = await env.DB.prepare(
      'SELECT dl.*, u.name, u.email, u.institution FROM download_log dl LEFT JOIN users u ON dl.user_id = u.id ORDER BY dl.timestamp DESC LIMIT ?'
    ).bind(limit).all();
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
  const totalUsers = await env.DB.prepare('SELECT COUNT(*) as c FROM users WHERE is_active = 1').first();
  const totalDownloads = await env.DB.prepare('SELECT COUNT(*) as c FROM download_log').first();
  const totalBytes = await env.DB.prepare('SELECT COALESCE(SUM(bytes_served),0) as s FROM download_log').first();
  const todayDownloads = await env.DB.prepare("SELECT COUNT(*) as c FROM download_log WHERE timestamp > datetime('now', '-1 day')").first();
  const weekDownloads = await env.DB.prepare("SELECT COUNT(*) as c FROM download_log WHERE timestamp > datetime('now', '-7 days')").first();

  // Countries (from users table — registered users)
  const countries = await env.DB.prepare('SELECT UPPER(country) as country, COUNT(*) as users FROM users WHERE is_active = 1 AND country != "" GROUP BY UPPER(country) ORDER BY users DESC').all();

  // Distinct institutions (exclude hidden ones)
  const institutions = await env.DB.prepare('SELECT institution, COUNT(*) as users FROM users WHERE is_active = 1 AND institution != "" AND COALESCE(hide_institution, 0) = 0 GROUP BY institution ORDER BY users DESC LIMIT 50').all();

  // Top downloaded tickers
  const topTickers = await env.DB.prepare('SELECT ticker, COUNT(*) as downloads, SUM(bytes_served) as bytes FROM download_log GROUP BY ticker ORDER BY downloads DESC LIMIT 25').all();

  // Downloads by version
  const byVersion = await env.DB.prepare('SELECT version, COUNT(*) as downloads FROM download_log GROUP BY version ORDER BY downloads DESC').all();

  // Registrations per week (last 12 weeks)
  const regTrend = await env.DB.prepare("SELECT strftime('%Y-W%W', created_at) as week, COUNT(*) as registrations FROM users WHERE created_at > datetime('now', '-84 days') GROUP BY week ORDER BY week").all();

  // Country codes from login_history (Cloudflare cf-ipcountry).
  // Inner-join users so deactivated/revoked users stop contributing to the
  // registered-user country counts. Without this, an admin "deleting" the
  // only IL user via Revoke Access still left IL=2 in the country map
  // because their historical login_history rows kept being counted.
  // Visitor traffic from the same country (via Cloudflare Analytics) still
  // surfaces as a light-blue badge / map tile, so the country isn't lost
  // from the visualization — just demoted from registered-user to visitor.
  const accessCountries = await env.DB.prepare(
    'SELECT UPPER(lh.country) as country, COUNT(DISTINCT lh.user_id) as users ' +
    'FROM login_history lh JOIN users u ON lh.user_id = u.id ' +
    'WHERE u.is_active = 1 AND lh.country IS NOT NULL AND lh.country != "" AND lh.country != "unknown" ' +
    'GROUP BY UPPER(lh.country)'
  ).all();

  // Merge registered user country data. Normalize each row's country to an
  // ISO-2 code via normalizeCountry() so "United States", "USA", "U.S." and
  // "us" all collapse to "US". Anything that fails normalization (CJK,
  // corrupted bytes, free-text we don't recognize) is dropped before
  // reaching the world map renderer.
  const userCountryMap = {};
  const accumulate = (country, users) => {
    const code = normalizeCountry(country);
    if (code) userCountryMap[code] = (userCountryMap[code] || 0) + users;
  };
  for (const row of countries.results) accumulate(row.country, row.users);
  for (const row of accessCountries.results) accumulate(row.country, row.users);

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
