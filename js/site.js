/* HF Data Library — Dynamic site data
   Loads metadata.json and populates all dynamic elements.
   Author: Ahmed Elkassabgi */

(function () {
  'use strict';

  // ── Notice banner (auto-expires; adjust/remove MAINT_EXPIRES_UTC once the
  //    scheduled API upgrade is complete) ──
  var MAINT_EXPIRES_UTC = Date.UTC(2026, 7, 1, 0, 0, 0); // 2026-08-01 00:00Z
  var MAINT_MSG = 'API access will be temporarily unavailable during a scheduled upgrade.';
  function injectMaintenanceBanner() {
    try {
      if (Date.now() > MAINT_EXPIRES_UTC) return;
      if (sessionStorage.getItem('apinotice-dismissed') === '1') return;
      var bar = document.createElement('div');
      bar.id = 'maint-banner';
      bar.style.cssText = 'background:#1e3a5f;color:#fff;padding:0.6rem 2.2rem 0.6rem 1rem;' +
        'font-size:0.88rem;line-height:1.45;text-align:center;position:relative;z-index:1500;';
      bar.textContent = '\u2699\uFE0F ' + MAINT_MSG;
      var x = document.createElement('button');
      x.textContent = '\u00D7';
      x.setAttribute('aria-label', 'Dismiss');
      x.style.cssText = 'position:absolute;right:0.7rem;top:50%;transform:translateY(-50%);' +
        'background:none;border:none;color:#fff;font-size:1.1rem;cursor:pointer;';
      x.onclick = function () { bar.remove(); sessionStorage.setItem('apinotice-dismissed', '1'); };
      bar.appendChild(x);
      document.body.insertBefore(bar, document.body.firstChild);
    } catch (e) { /* banner must never break the page */ }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectMaintenanceBanner);
  } else {
    injectMaintenanceBanner();
  }

  // Determine path to data/metadata.json relative to current page
  const isSubpage = window.location.pathname.includes('/pages/');
  const basePath = isSubpage ? '../data/metadata.json' : 'data/metadata.json';

  // Format large numbers: 1533014567 → "1.53B" (floor, never round up —
  // reported counts should never overstate the data we actually have).
  function formatBars(n) {
    if (n >= 1e9) return (Math.floor(n / 1e7) / 100).toFixed(2) + 'B';
    if (n >= 1e6) return (Math.floor(n / 1e5) / 10).toFixed(1) + 'M';
    if (n >= 1e3) return Math.floor(n / 1e3) + 'K';
    return n.toLocaleString();
  }

  // Format with commas: 1533014567 → "1,533,014,567"
  function formatComma(n) {
    return Number(n).toLocaleString();
  }

  // Animated counter: counts up from 0 to target over ~2 seconds
  function animateCounter(el, target) {
    var duration = 2000;
    var start = 0;
    var startTime = null;
    // Ease-out for a satisfying slowdown at the end
    function step(timestamp) {
      if (!startTime) startTime = timestamp;
      var progress = Math.min((timestamp - startTime) / duration, 1);
      var eased = 1 - Math.pow(1 - progress, 3);
      var current = Math.floor(eased * target);
      el.textContent = current.toLocaleString();
      if (progress < 1) {
        requestAnimationFrame(step);
      } else {
        // Final value with written-out label below — floor to 2 decimals
        // so 1,538,207,376 reads as "1.53+ Billion" (not 1.54 from rounding).
        var billions = (Math.floor(target / 1e7) / 100).toFixed(2) + '+ Billion';
        el.style.lineHeight = '1.1';
        el.innerHTML = target.toLocaleString() + '<br><span style="font-size:0.45em; opacity:0.7; line-height:1;">(' + billions + ')</span>';
      }
    }
    requestAnimationFrame(step);
  }

  // Format date: "2026-04-09T06:00:00Z" → "April 9, 2026"
  function formatDate(iso) {
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
  }

  // Relative time: "2026-04-09T06:00:00Z" → "3 hours ago"
  function timeAgo(iso) {
    const now = new Date();
    const then = new Date(iso);
    const diff = Math.floor((now - then) / 1000);
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + ' minutes ago';
    if (diff < 86400) return Math.floor(diff / 3600) + ' hours ago';
    if (diff < 604800) return Math.floor(diff / 86400) + ' days ago';
    return formatDate(iso);
  }

  // Build the status bar HTML
  function buildStatusBar(meta) {
    const bar = document.createElement('div');
    bar.id = 'status-bar';
    bar.className = 'status-bar';

    const isOp = meta.status === 'operational';
    const statusColor = isOp ? '#059669' : '#dc2626';
    const statusText = isOp ? 'All systems operational' : 'Service disruption';
    const statusDot = isOp ? '&#9679;' : '&#9888;';

    bar.innerHTML =
      '<div class="container" style="display:flex; justify-content:space-between; align-items:center; height:100%; flex-wrap:wrap; gap:0.25rem;">' +
        '<span style="display:flex; align-items:center; gap:0.5rem;">' +
          '<span style="color:' + statusColor + '; font-size:0.7rem;">' + statusDot + '</span>' +
          '<span>' + statusText + '</span>' +
        '</span>' +
        '<span style="display:flex; gap:1.5rem;">' +
          '<span>Website updated: ' + formatDate(meta.website_updated) + '</span>' +
          '<span>Data updated: ' + formatDate(meta.data_updated) + ' (' + timeAgo(meta.data_updated) + ')</span>' +
        '</span>' +
      '</div>';

    // Insert before navbar
    const navbar = document.querySelector('.navbar');
    if (navbar) {
      navbar.parentNode.insertBefore(bar, navbar);
    } else {
      document.body.insertBefore(bar, document.body.firstChild);
    }
  }

  // ── User identity in the navbar — DUAL-MODE (legacy hfd_session ∪ EKD family SSO) ──
  // [Phase 3.2] Purely additive. A VALIDATED legacy hfd_session ALWAYS wins (existing users keep
  // their exact nav + in-site account link); otherwise the EKD popup provides family sign-in. ONE
  // precedence helper owns the nav (D42) and re-runs on SDK login/logout + bfcache (D34). Nothing
  // here removes the old login — the retained old form is the dark launch.
  const API_BASE = 'https://api.hfdatalibrary.com';
  const ACCOUNTS_BASE = 'https://accounts.elkassabgidata.com';
  const isSubpage2 = window.location.pathname.includes('/pages/');
  const downloadUrl = isSubpage2 ? 'download' : 'pages/download';
  const accountUrl = isSubpage2 ? 'account' : 'pages/account';
  const adminUrl = isSubpage2 ? 'admin' : 'pages/admin';

  // G-11a: storage may throw (private mode / blocked) — never let it break the page.
  function safeGet(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
  function safeSet(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
  function safeDel(k) { try { localStorage.removeItem(k); } catch (e) {} }
  // G-11b/D56: every profile-derived value is escaped before it touches innerHTML.
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  var EKD_READY = false, sdkSettled = false, paintGen = 0, paintedSignedIn = false, loggingOut = false;

  // Load the SDK for site.js's OWN use; feature-detect everywhere. onerror / a 4 s timeout still
  // "settles" so the optimistic chip can never hang if accounts.* is blocked or slow.
  function settleSdk() { if (!sdkSettled) { sdkSettled = true; paintUserWidget(); } }
  (function loadSdk() {
    try {
      var s = document.createElement('script');
      s.src = ACCOUNTS_BASE + '/sdk/ekd-sso.js';
      s.onload = function () {
        try {
          if (window.EKD) {
            EKD_READY = true;
            window.EKD.init();                                          // clientId = this origin, callback /auth/callback
            // D42: SDK events NEVER paint directly — they re-run the single nav owner.
            window.EKD.on('login',  function () { safeDel('ekd_notice_demoted'); paintUserWidget(); });
            window.EKD.on('logout', function () { paintUserWidget(); });
          }
        } catch (e) {}
        settleSdk();
      };
      s.onerror = function () { settleSdk(); };
      document.head.appendChild(s);
      setTimeout(settleSdk, 4000);
    } catch (e) { settleSdk(); }
  })();

  // Flash-fix: before the SDK settles, if a session token is stored show a neutral chip (not
  // "Sign in") so a returning user never flashes signed-out → signed-in.
  function optimisticPaint() {
    var navLinks = document.querySelector('.nav-links');
    if (!navLinks || document.getElementById('nav-user-widget')) return;
    if (!(safeGet('hfd_session') || safeGet('ekd_rt'))) return;         // truly signed-out → paintUserWidget draws "Sign in"
    var li = document.createElement('li');
    li.id = 'nav-user-widget';
    li.style.marginLeft = '0.75rem';
    li.innerHTML = '<span style="display:inline-flex; align-items:center; gap:0.4rem; background:rgba(255,255,255,0.1); border-radius:6px; padding:0.35rem 0.75rem; color:rgba(255,255,255,0.7); font-size:0.85rem;">&#8230;</span>';
    navLinks.appendChild(li);
  }

  // The single nav owner. Precedence: validated-legacy → EKD family → signed-out.
  async function paintUserWidget() {
    var navLinks = document.querySelector('.nav-links');
    if (!navLinks) return;
    var gen = ++paintGen;                                              // adversarial#1: only the newest paint renders
    var user = null, mode = null;

    // (1) VALIDATED-LEGACY wins — unchanged UX for existing users.
    var legacy = safeGet('hfd_session');
    if (legacy) {
      try {
        var r = await fetch(API_BASE + '/v1/auth/me', { headers: { 'Authorization': 'Bearer ' + legacy } });
        if (r.ok) { user = await r.json(); mode = 'legacy'; }
        else if (r.status === 401) { safeDel('hfd_session'); }         // D02/G-13: dead session → purge, fall through
        // D02: any 5xx / non-401 → KEEP hfd_session, fall through to signed-out THIS pageview only (never delete).
      } catch (e) { /* D02: network/timeout → KEEP, fall through (transient) */ }
    }

    // (2) else EKD family session.
    if (!user && EKD_READY && window.EKD) {
      try {
        var at = await window.EKD.getAccessToken();
        if (at) {
          var r2 = await fetch(API_BASE + '/v1/auth/me', { headers: { 'Authorization': 'Bearer ' + at } });
          if (r2.ok) { user = await r2.json(); mode = 'ekd'; }
        }
      } catch (e) {}
    }

    if (gen !== paintGen) return;                                      // superseded by a newer paint
    // EKD state still pending (SDK not settled) + a stored rt → keep the optimistic chip; the SDK
    // settle re-runs this and resolves the real name (avoids …→"Sign in"→name).
    if (!user && !sdkSettled && safeGet('ekd_rt')) return;
    renderWidget(navLinks, user, mode);
  }

  function renderWidget(navLinks, user, mode) {
    var existing = document.getElementById('nav-user-widget');
    if (existing) existing.remove();
    var li = document.createElement('li');
    li.id = 'nav-user-widget';
    li.style.marginLeft = '0.75rem';

    if (user) {
      paintedSignedIn = true;
      var vipBadge = user.is_vip
        ? '<span style="display:inline-block; background:linear-gradient(135deg,#d4a843,#f0d78c); color:#1a2332; font-size:0.6rem; font-weight:700; padding:0.1em 0.4em; border-radius:3px; margin-left:0.25rem; letter-spacing:0.05em; text-transform:uppercase;">&#9733;</span>'
        : '';
      var firstName = esc((user.name || '').split(' ')[0]);
      var initial = esc((user.name || 'U')[0].toUpperCase());
      var acctHref = mode === 'ekd' ? (ACCOUNTS_BASE + '/account') : accountUrl;
      var acctAttr = mode === 'ekd' ? ' target="_blank" rel="noopener"' : '';
      var logoutLabel = mode === 'ekd' ? 'Log out (this site)' : 'Log out';
      var logoutNote = mode === 'ekd'
        ? '<div style="padding:0 1rem 0.45rem; font-size:0.72rem; color:var(--gray-500); line-height:1.35;">To log out of every library, use &ldquo;Log out everywhere&rdquo; on your account page.</div>'
        : '';
      li.style.position = 'relative';
      li.innerHTML =
        '<div style="display:inline-flex; align-items:center; gap:0.4rem; background:rgba(255,255,255,0.1); border-radius:6px; padding:0.35rem 0.6rem; color:#fff; font-size:0.85rem; cursor:pointer; white-space:nowrap;" onclick="var d=document.getElementById(\'user-dropdown\'); d.style.display = d.style.display===\'block\'?\'none\':\'block\'">' +
          '<span style="display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; background:var(--gold); color:var(--navy); border-radius:50%; font-weight:700; font-size:0.7rem;">' + initial + '</span>' +
          '<span>' + firstName + '</span>' + vipBadge +
          '<span style="font-size:0.65rem; opacity:0.7;">&#9660;</span>' +
        '</div>' +
        '<div id="user-dropdown" style="display:none; position:absolute; top:calc(100% + 0.5rem); right:0; background:#fff; border:1px solid var(--gray-200); border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.15); padding:0.5rem 0; min-width:220px; z-index:101;">' +
          '<div style="padding:0.75rem 1rem; border-bottom:1px solid var(--gray-100);">' +
            '<div style="font-weight:600; color:var(--navy);">' + esc(user.name) + '</div>' +
            '<div style="font-size:0.8rem; color:var(--gray-500);">' + esc(user.email) + '</div>' +
          '</div>' +
          '<a href="' + acctHref + '"' + acctAttr + ' style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">My Account</a>' +
          '<a href="' + downloadUrl + '" style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">Downloads</a>' +
          (user.is_admin ? '<a href="' + adminUrl + '" style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">Admin Panel</a>' : '') +
          '<div onclick="window.__hfdLogout()" style="display:block; padding:0.5rem 1rem; color:var(--red); font-size:0.9rem; cursor:pointer; border-top:1px solid var(--gray-100); margin-top:0.25rem;">' + logoutLabel + '</div>' +
          logoutNote +
        '</div>';
    } else {
      // signed-out: EKD popup Sign-in (synchronous, G-12a) + a secondary "More sign-in options" link
      // keeping Google/ORCID/password reachable until G-C is a WITNESSED live popup OAuth login.
      li.innerHTML =
        '<span style="display:inline-flex; align-items:center; gap:0.55rem; white-space:nowrap;">' +
          '<a id="nav-signin" href="' + downloadUrl + '#register" style="background:var(--gold); color:var(--navy); padding:0.4rem 0.875rem; border-radius:6px; font-size:0.85rem; font-weight:600;">Sign in</a>' +
          '<a href="' + downloadUrl + '#register" style="color:rgba(255,255,255,0.72); font-size:0.72rem;">More sign-in options</a>' +
        '</span>';
    }

    navLinks.appendChild(li);

    // logout: clears BOTH the legacy session and the EKD family session.
    window.__hfdLogout = async function () {
      loggingOut = true;                                               // suppress the demotion notice on INTENTIONAL logout
      var t = safeGet('hfd_session');
      if (t) { try { await fetch(API_BASE + '/v1/auth/logout', { method: 'POST', headers: { 'Authorization': 'Bearer ' + t } }); } catch (e) {} }
      safeDel('hfd_session');
      try { if (window.EKD) await window.EKD.logout(); } catch (e) {}
      window.location.reload();
    };

    if (user) {
      if (mode === 'legacy') maybeShowTransitionNotice();             // D87 one-time upgrade notice
    } else if (paintedSignedIn && !loggingOut) {
      paintedSignedIn = false;
      showDemotionNotice();                                            // D36 one-time NEUTRAL "session ended" notice
    }

    // signed-out Sign-in click → popup (synchronous G-12a); rejection copy per §9/D40.
    if (!user) {
      var btn = document.getElementById('nav-signin');
      if (btn && EKD_READY && window.EKD) {
        btn.addEventListener('click', function (e) {
          e.preventDefault();
          window.EKD.login().catch(function (err) { showSigninError(err); });
        });
      }
    }

    // VIP site-wide banner (unchanged behavior).
    if (user && user.is_vip) {
      if (!document.getElementById('vip-banner')) {
        var banner = document.createElement('div');
        banner.id = 'vip-banner';
        banner.style.cssText = 'background:linear-gradient(90deg,#1a2332 0%,#2a3a5a 50%,#1a2332 100%); color:#d4a843; padding:0.4rem 0; text-align:center; font-size:0.8rem; font-weight:500; letter-spacing:0.05em; border-bottom:1px solid #d4a843;';
        banner.innerHTML = '&#9733; VIP MEMBER &#9733; &nbsp;&nbsp; You have access to premium features and priority support.';
        var navbar = document.querySelector('.navbar');
        if (navbar) navbar.parentNode.insertBefore(banner, navbar.nextSibling);
      }
    } else {
      var vb = document.getElementById('vip-banner'); if (vb) vb.remove();
    }
  }

  // ── §9 notices (exact copy pack) ──
  function showSigninError(err) {
    var m = (err && err.message) || 'exchange_failed';
    var msg = (m === 'popup_blocked')
      ? 'Your browser blocked the sign-in window. Allow popups for this site, then click Sign in again.'
      : (m === 'popup_closed')
        ? 'The sign-in window was closed. If you just registered, check your email (and spam) to verify your address, then click Sign in.'
        : 'Sign-in didn’t complete. Click Sign in to try again — if you just registered, one click is all it takes.';
    showToast(msg);
  }
  function showDemotionNotice() {
    var last = safeGet('ekd_notice_demoted');
    if (last && (Date.now() - Number(last)) < 7 * 864e5) return;       // 7-day suppression (D36)
    safeSet('ekd_notice_demoted', String(Date.now()));
    showToast('Your sign-in for this site expired or was ended — click Sign in to reconnect (same email and password).');
  }
  function maybeShowTransitionNotice() {
    if (safeGet('ekd_notice_transition') === '1') return;
    if (document.getElementById('ekd-transition-banner')) return;
    try {
      var bar = document.createElement('div');
      bar.id = 'ekd-transition-banner';
      bar.style.cssText = 'background:#243b53; color:#e8eef6; padding:0.6rem 2.4rem 0.6rem 1rem; font-size:0.84rem; line-height:1.5; text-align:center; position:relative; z-index:1400;';
      bar.innerHTML = 'Sign-in has been upgraded &mdash; one ElkassabgiData account now works across all our libraries. You&rsquo;re still signed in; nothing changes today. Next time, use the <strong>Sign in</strong> button (a quick popup) &mdash; same email and password.';
      var x = document.createElement('button');
      x.textContent = '×'; x.setAttribute('aria-label', 'Dismiss');
      x.style.cssText = 'position:absolute; right:0.7rem; top:50%; transform:translateY(-50%); background:none; border:none; color:#e8eef6; font-size:1.15rem; cursor:pointer;';
      x.onclick = function () { bar.remove(); safeSet('ekd_notice_transition', '1'); };
      bar.appendChild(x);
      document.body.insertBefore(bar, document.body.firstChild);
    } catch (e) {}
  }
  function showToast(text) {
    try {
      var t = document.createElement('div');
      t.setAttribute('role', 'status');
      t.style.cssText = 'position:fixed; top:80px; right:1.5rem; max-width:340px; background:#1e3a5f; color:#fff; padding:0.85rem 2.3rem 0.85rem 1rem; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.2); z-index:9999; font-size:0.86rem; line-height:1.45;';
      t.textContent = text;
      var x = document.createElement('button');
      x.textContent = '×'; x.setAttribute('aria-label', 'Dismiss');
      x.style.cssText = 'position:absolute; right:0.6rem; top:0.45rem; background:none; border:none; color:#fff; font-size:1.1rem; cursor:pointer;';
      x.onclick = function () { t.remove(); };
      t.appendChild(x);
      document.body.appendChild(t);
      setTimeout(function () { if (t.parentNode) { t.style.transition = 'opacity 0.4s'; t.style.opacity = '0'; setTimeout(function () { t.remove(); }, 400); } }, 8000);
    } catch (e) {}
  }

  // Initial paint: optimistic chip (sync, flash-fix) → real state (async) → bfcache re-run (D34/S21).
  optimisticPaint();
  paintUserWidget();
  window.addEventListener('pageshow', function (e) { if (e.persisted) paintUserWidget(); });

  // Populate all elements with data-meta attribute
  function populateData(meta) {
    // Map of data-meta values to their display values
    const values = {
      'tickers': formatComma(meta.tickers),
      'tickers-short': meta.tickers.toLocaleString(),
      'bars-raw': formatComma(meta.bars_raw),
      'bars-clean': formatComma(meta.bars_clean),
      'bars-filled': meta.bars_filled ? formatComma(meta.bars_filled) : '',
      'bars-raw-short': formatBars(meta.bars_raw),
      'bars-clean-short': formatBars(meta.bars_clean),
      'bars-filled-short': meta.bars_filled ? formatBars(meta.bars_filled) : '',
      'bars-counter': null,  // handled separately with animation
      'bars-removed': formatComma(meta.bars_removed),
      'bars-filled-count': meta.bars_filled_count ? formatComma(meta.bars_filled_count) : '',
      'trading-days': formatComma(meta.trading_days),
      'years': meta.years_of_data + '+',
      'variables': meta.academic_variables,
      'start-date': meta.start_date,
      'end-date': meta.end_date,
      'website-updated': formatDate(meta.website_updated),
      'data-updated': formatDate(meta.data_updated),
      'data-updated-ago': timeAgo(meta.data_updated),
      'next-update': formatDate(meta.next_update),
      'update-summary': meta.update_summary,
      'version': meta.version,
      'q5-gap': (meta.quintiles.q5.avg_gap_rate * 100).toFixed(1) + '%',
      'q4-gap': (meta.quintiles.q4.avg_gap_rate * 100).toFixed(1) + '%',
      'q3-gap': (meta.quintiles.q3.avg_gap_rate * 100).toFixed(1) + '%',
      'q2-gap': (meta.quintiles.q2.avg_gap_rate * 100).toFixed(1) + '%',
      'q1-gap': (meta.quintiles.q1.avg_gap_rate * 100).toFixed(1) + '%',
      'q5-completeness': ((1 - meta.quintiles.q5.avg_gap_rate) * 100).toFixed(1),
      'q4-completeness': ((1 - meta.quintiles.q4.avg_gap_rate) * 100).toFixed(1),
      'q3-completeness': ((1 - meta.quintiles.q3.avg_gap_rate) * 100).toFixed(1),
      'q2-completeness': ((1 - meta.quintiles.q2.avg_gap_rate) * 100).toFixed(1),
      'q1-completeness': ((1 - meta.quintiles.q1.avg_gap_rate) * 100).toFixed(1)
    };

    document.querySelectorAll('[data-meta]').forEach(function (el) {
      var key = el.getAttribute('data-meta');
      if (values[key] !== undefined) {
        el.textContent = values[key];
      }
    });

    // Animated bar counter on home page
    var counterEl = document.getElementById('bars-counter');
    if (counterEl && meta.bars_raw) {
      animateCounter(counterEl, meta.bars_raw);
    }

    // Update quality bar widths
    document.querySelectorAll('[data-bar]').forEach(function (el) {
      var key = el.getAttribute('data-bar');
      if (values[key] !== undefined) {
        el.style.width = values[key] + '%';
      }
    });
  }

  // Build the update notice for the data page
  function buildUpdateNotice(meta) {
    var container = document.getElementById('update-notice');
    if (!container) return;

    container.innerHTML =
      '<div style="background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 8px; padding: 1.25rem 1.5rem; margin-bottom: 2rem;">' +
        '<div style="display:flex; justify-content:space-between; align-items:start; flex-wrap:wrap; gap:0.5rem;">' +
          '<div>' +
            '<strong style="color: #065f46;">Latest Data Update — ' + formatDate(meta.data_updated) + '</strong>' +
            '<p style="margin: 0.25rem 0 0; color: #047857; font-size: 0.9rem;">' + meta.update_summary + '</p>' +
          '</div>' +
          '<div style="text-align:right; font-size:0.85rem; color:#6b7280;">' +
            '<div>Version ' + meta.version + '</div>' +
            '<div>Next update: ' + formatDate(meta.next_update) + '</div>' +
          '</div>' +
        '</div>' +
      '</div>';
  }

  // Fetch and apply (cache-bust so status bar always reflects latest data)
  fetch(basePath + '?t=' + Date.now())
    .then(function (r) { return r.json(); })
    .then(function (meta) {
      buildStatusBar(meta);
      populateData(meta);
      buildUpdateNotice(meta);
    })
    .catch(function (err) {
      console.warn('Could not load metadata.json:', err);
    });

})();
