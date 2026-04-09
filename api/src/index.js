/**
 * HF Data Library — API Worker v2
 * Full auth system with admin controls
 * Cloudflare Workers + R2 + D1
 * Author: Ahmed Elkassabgi, University of Central Arkansas
 */

const RATE_LIMIT_MAX = 300;
const SESSION_DAYS = 30;
const ADMIN_EMAILS = ['aelkassabgi@uca.edu', 'elkassabgi@yahoo.com'];
const FROM_EMAIL = 'noreply@hfdatalibrary.com';
const FROM_NAME = 'HF Data Library';
const SITE_URL = 'https://hfdatalibrary.com';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const ip = request.headers.get('cf-connecting-ip') || 'unknown';
    const ua = request.headers.get('user-agent') || 'unknown';
    const country = request.headers.get('cf-ipcountry') || 'unknown';

    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization, Cookie',
      'Access-Control-Allow-Credentials': 'true',
      'Access-Control-Max-Age': '86400',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    try {
      // ── Public endpoints ──
      if (path === '/' || path === '')
        return jsonRes({ name: 'HF Data Library API', version: '2.0', author: 'Ahmed Elkassabgi', docs: 'https://hfdatalibrary.com/pages/api.html' }, 200, cors);

      if (path === '/v1/status')
        return await handleStatus(env, cors);

      // ── Auth endpoints ──
      if (path === '/v1/auth/register' && request.method === 'POST')
        return await handleRegister(request, env, cors, ip, ua, country);

      if (path === '/v1/auth/login' && request.method === 'POST')
        return await handleLogin(request, env, cors, ip, ua, country);

      if (path === '/v1/auth/logout' && request.method === 'POST')
        return await handleLogout(request, env, cors);

      if (path === '/v1/auth/me')
        return await handleMe(request, env, cors);

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

      const dlMatch = path.match(/^\/v1\/download\/([A-Z0-9.]+)$/i);
      if (dlMatch)
        return await handleDownload(dlMatch[1].toUpperCase(), request, env, cors, ip);

      // ── Admin endpoints ──
      if (path.startsWith('/v1/admin/'))
        return await handleAdmin(path, request, env, cors, ip);

      return jsonRes({ error: 'Not found' }, 404, cors);
    } catch (err) {
      return jsonRes({ error: 'Internal server error', detail: err.message }, 500, cors);
    }
  }
};

// ══════════════════════════════════════
// ── Email Sending (Mailchannels — free via Cloudflare) ──
// ══════════════════════════════════════

async function sendEmail(to, subject, htmlBody) {
  const response = await fetch('https://api.mailchannels.net/tx/v1/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      personalizations: [{ to: [{ email: to }] }],
      from: { email: FROM_EMAIL, name: FROM_NAME },
      subject,
      content: [{ type: 'text/html', value: htmlBody }]
    })
  });
  return response.ok;
}

function verificationEmail(name, token) {
  const link = SITE_URL + '/pages/verify.html?token=' + token;
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

function resetEmail(name, token) {
  const link = SITE_URL + '/pages/reset.html?token=' + token;
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
  const apiKey = request.headers.get('X-API-Key');
  if (!apiKey) return null;
  return await env.DB.prepare('SELECT * FROM users WHERE api_key = ? AND is_active = 1')
    .bind(apiKey).first();
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
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { name, email, password, institution, role } = body;
  const userCountry = body.country || country;

  if (!name || !email || !password || !institution || !role || !userCountry) {
    return jsonRes({ error: 'Required: name, email, password, institution, country, role' }, 400, cors);
  }
  if (password.length < 8) {
    return jsonRes({ error: 'Password must be at least 8 characters' }, 400, cors);
  }

  // Check existing
  const existing = await env.DB.prepare('SELECT id FROM users WHERE email = ?').bind(email.toLowerCase()).first();
  if (existing) {
    return jsonRes({ error: 'Email already registered. Please log in.' }, 409, cors);
  }

  const passwordHash = await hashPassword(password);
  const apiKey = 'hfd_' + generateId();
  const isAdmin = ADMIN_EMAILS.includes(email.toLowerCase()) ? 1 : 0;

  await env.DB.prepare(
    'INSERT INTO users (name, email, password_hash, institution, country, role, api_key, is_admin, email_verified, last_login_ip, last_login_ua) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(name, email.toLowerCase(), passwordHash, institution, userCountry, role, apiKey, isAdmin, isAdmin ? 1 : 0, ip, ua).run();

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
    await sendEmail(email.toLowerCase(), 'Verify your HF Data Library account', verificationEmail(name, verifyToken));
  }

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
    return jsonRes({ error: 'Account has been deactivated. Contact aelkassabgi@uca.edu.' }, 403, cors);
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
    created_at: user.created_at,
    download_count: user.download_count,
    total_bytes_downloaded: user.total_bytes_downloaded
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
  await sendEmail(user.email, 'Verify your HF Data Library account', verificationEmail(user.name, verifyToken));

  return jsonRes({ message: 'Verification email sent. Check your inbox.' }, 200, cors);
}

async function handleResetRequest(request, env, cors) {
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
    await sendEmail(email.toLowerCase(), 'Reset your HF Data Library password', resetEmail(u.name, token));
  }

  return jsonRes({ message: 'If that email is registered, a reset link has been sent.' }, 200, cors);
}

async function handleReset(request, env, cors) {
  let body;
  try { body = await request.json(); } catch { return jsonRes({ error: 'Invalid JSON' }, 400, cors); }

  const { token, password } = body;
  if (!token || !password) return jsonRes({ error: 'Required: token, password' }, 400, cors);
  if (password.length < 8) return jsonRes({ error: 'Password must be at least 8 characters' }, 400, cors);

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
// ── Data Handlers ──
// ══════════════════════════════════════

async function handleSymbols(env, cors) {
  const list = await env.DATA_BUCKET.list({ prefix: 'clean/', limit: 1500 });
  const symbols = list.objects
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

  const version = new URL(request.url).searchParams.get('version') || 'clean';
  if (!['raw', 'clean', 'filled'].includes(version)) return jsonRes({ error: 'Invalid version' }, 400, cors);

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

async function handleDownload(ticker, request, env, cors, ip) {
  const user = await requireAuth(request, env);
  if (!user) return jsonRes({ error: 'Authentication required. Log in or provide X-API-Key header.' }, 401, cors);
  if (!user.email_verified) return jsonRes({ error: 'Please verify your email before downloading data. Check your inbox.' }, 403, cors);

  const version = new URL(request.url).searchParams.get('version') || 'clean';
  const obj = await env.DATA_BUCKET.get(`${version}/${ticker}.parquet`);
  if (!obj) return jsonRes({ error: `Ticker '${ticker}' not found in ${version}` }, 404, cors);

  const userId = user.user_id || user.id;
  await env.DB.prepare('UPDATE users SET download_count = download_count + 1, total_bytes_downloaded = total_bytes_downloaded + ? WHERE id = ?')
    .bind(obj.size, userId).run();
  await env.DB.prepare('INSERT INTO download_log (user_id, api_key, ticker, version, endpoint, ip_address, bytes_served) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .bind(userId, user.api_key, ticker, version, '/v1/download', ip, obj.size).run();

  return new Response(obj.body, {
    headers: { ...cors, 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${ticker}_${version}.parquet"`, 'Content-Length': obj.size }
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

  // GET /v1/admin/users — list all users
  if (path === '/v1/admin/users' && request.method === 'GET') {
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '100');
    const offset = parseInt(url.searchParams.get('offset') || '0');

    const users = await env.DB.prepare(
      'SELECT id, name, email, institution, country, role, api_key, is_active, is_admin, created_at, last_login_at, last_login_ip, last_login_ua, login_count, download_count, total_bytes_downloaded, notes FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?'
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
      user: { id: u.id, name: u.name, email: u.email, institution: u.institution, country: u.country, role: u.role, api_key: u.api_key, is_active: u.is_active, is_admin: u.is_admin, created_at: u.created_at, last_login_at: u.last_login_at, last_login_ip: u.last_login_ip, last_login_ua: u.last_login_ua, login_count: u.login_count, download_count: u.download_count, total_bytes_downloaded: u.total_bytes_downloaded, notes: u.notes },
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

    if (updates.length === 0) return jsonRes({ error: 'No updates provided' }, 400, cors);

    values.push(uid);
    await env.DB.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`).bind(...values).run();

    // If deactivating, kill their sessions
    if (body.is_active === false || body.is_active === 0) {
      await env.DB.prepare('DELETE FROM sessions WHERE user_id = ?').bind(uid).run();
    }

    return jsonRes({ message: 'User updated' }, 200, cors);
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

  return jsonRes({ error: 'Admin endpoint not found' }, 404, cors);
}

// ── Status ──

async function handleStatus(env, cors) {
  const list = await env.DATA_BUCKET.list({ prefix: 'clean/', limit: 1 });
  const userCount = await env.DB.prepare('SELECT COUNT(*) as c FROM users').first();
  return jsonRes({
    status: 'operational',
    api_version: '2.0',
    author: 'Ahmed Elkassabgi',
    r2_connected: list.objects.length > 0,
    registered_users: userCount?.c || 0,
    rate_limit: RATE_LIMIT_MAX + ' requests per minute',
    timestamp: new Date().toISOString()
  }, 200, cors);
}

// ── Helpers ──

function jsonRes(data, status, cors) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...cors }
  });
}
