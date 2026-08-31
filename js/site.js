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

  // ── Catch-up notice banner (data-driven, self-retiring) ──
  // Shows on every page whenever the dataset's end_date has fallen more than
  // CATCHUP_GAP_DAYS behind today (UTC). The worst NORMAL gap is ~5.5 days
  // (Friday session, Monday holiday, viewed Wednesday before that day's run
  // lands), so 6 fires only when sessions are genuinely missing — and the
  // banner disappears on its own once catch-up restores currency, no code
  // change needed to retire it. Called after metadata.json loads.
  var CATCHUP_GAP_DAYS = 6;
  function injectCatchupBanner(meta) {
    try {
      if (!meta || !meta.end_date) return;
      if (sessionStorage.getItem('catchup-dismissed') === '1') return;
      var gapDays = (Date.now() - Date.parse(meta.end_date + 'T00:00:00Z')) / 864e5;
      if (!(gapDays > CATCHUP_GAP_DAYS)) return;
      // end_date is a bare date — format it in UTC, or viewers west of UTC
      // see the previous day (formatDate renders in the viewer's timezone).
      var through = new Date(meta.end_date + 'T00:00:00Z').toLocaleDateString('en-US',
        { year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC' });
      var bar = document.createElement('div');
      bar.id = 'catchup-banner';
      bar.style.cssText = 'background:#fef3c7;color:#92400e;border-bottom:1px solid #f59e0b;' +
        'padding:0.6rem 2.2rem 0.6rem 1rem;font-size:0.88rem;line-height:1.45;' +
        'text-align:center;position:relative;z-index:1500;';
      bar.textContent = '⚠️ Service notice: a service error interrupted daily data ' +
        'updates. It has been fixed and the archive is catching up automatically — data ' +
        'currently runs through ' + through + ', and the remaining sessions are being restored ' +
        'with each catch-up run. Existing data is unaffected.';
      var x = document.createElement('button');
      x.textContent = '×';
      x.setAttribute('aria-label', 'Dismiss');
      x.style.cssText = 'position:absolute;right:0.7rem;top:50%;transform:translateY(-50%);' +
        'background:none;border:none;color:#92400e;font-size:1.1rem;cursor:pointer;';
      x.onclick = function () { bar.remove(); sessionStorage.setItem('catchup-dismissed', '1'); };
      bar.appendChild(x);
      document.body.insertBefore(bar, document.body.firstChild);
    } catch (e) { /* banner must never break the page */ }
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


  // ── Camouflaged visitor counter ──
  //
  // Ahmed wanted the number present but not on display: readable if you select the text, and
  // findable in the page source. Two mechanisms, because they are visible in different places:
  //
  //   1. A line in the footer coloured EXACTLY the footer background (#1a2332). Invisible while
  //      unselected; drag across it and the selection highlight makes it readable. Deliberately
  //      not `color:transparent` or `opacity:0` — several browsers keep transparent glyphs
  //      transparent when selected, so the reveal would not work.
  //   2. A comment node carrying the same numbers, so it shows in DevTools' element inspector.
  //
  // NOTE ON "VIEW SOURCE": view-source shows the raw HTML the server sent, and anything JS adds
  // is NOT in it — it only appears in Inspect. The static comment baked into each page's footer
  // at deploy time is what makes view-source work; this keeps the DOM copy current between
  // deploys.
  //
  // Cost is nil: /v1/public-stats has been edge-cached for 5 minutes since 2026-08-01, so this
  // is a cache hit for essentially every visitor and never touches D1.
  function injectVisitorCounter() {
    try {
      var foot = document.querySelector('footer.footer .container') || document.querySelector('footer.footer');
      if (!foot || document.getElementById('vc-line')) return;
      fetch(API_BASE + '/v1/public-stats')
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (d) {
          if (!d || !foot) return;
          var v = Number(d.total_visitors || 0).toLocaleString();
          var pv = Number(d.total_page_views || 0).toLocaleString();
          var c = Number(d.visitor_country_count || 0).toLocaleString();
          var txt = 'Visitors: ' + v + '  ·  Page views: ' + pv + '  ·  Countries: ' + c;
          var el = document.createElement('p');
          el.id = 'vc-line';
          // Same colour as the footer background = invisible until selected.
          el.style.cssText = 'color:#1a2332; font-size:0.72rem; margin:1.25rem 0 0; letter-spacing:0.02em; user-select:text;';
          el.textContent = txt;
          foot.appendChild(el);
          foot.appendChild(document.createComment(' ' + txt + ' '));
        })
        .catch(function () { /* cosmetic — never disturb the page */ });
    } catch (e) {}
  }
  if (document.readyState !== 'loading') injectVisitorCounter();
  else document.addEventListener('DOMContentLoaded', injectVisitorCounter);

  // ── §SILENT-RESUME — ask the IdP once whether this browser already has a family session ──
  //
  // Sign in on econdatalibrary, open hfdatalibrary, and you were told "Sign in". Not because
  // the session had ended, but because nothing here could see it: ekd_session is host-only on
  // accounts.elkassabgidata.com, and the SDK's tokens live in localStorage, which is
  // per-origin. EKD.getAccessToken() returns null off an empty ekd_rt WITHOUT ever contacting
  // the IdP, so hf concluded "signed out" from its own ignorance. The IdP knew; nobody asked.
  //
  // This asks, exactly once per browser session, by navigating to /authorize?prompt=none. A
  // TOP-LEVEL navigation is the point: ekd_session is SameSite=Lax, which a top-level GET
  // carries and a hidden iframe does not — and an iframe would additionally be third-party,
  // so Safari, Firefox and Chrome-incognito would strip the cookie and report a signed-in
  // user as signed out. The redirect costs a flash; an iframe costs correctness.
  //
  // The IdP answers immediately either way and comes straight back to /auth/callback, which
  // redeems the code and returns the user to this exact URL.
  //
  // LOOP SAFETY, which is the thing that makes this dangerous if done casually: the callback
  // sets ekd_silent_done BEFORE it can fail, on every path including login_required and a
  // failed exchange. This function refuses to start when that flag is present. So a
  // signed-out visitor pays one bounce per browser session and never a second, and a broken
  // IdP degrades to "signed out" rather than to an infinite redirect.
  (function silentResume() {
    try {
      // Only when this origin genuinely has nothing. A stored credential means the normal
      // paths already work and must not be disturbed.
      if (safeGet('hfd_session') || safeGet('ekd_rt')) return;
      // An explicit logout this browser session means "stay out" — never auto-resume over it.
      // Either store: sessionStorage is per-TAB, so a sign-out in one tab left every other
      // tab (and any tab opened afterwards) unprotected — open the site in a new tab and the
      // resume signed you straight back in. That only bites when server-side revocation did
      // not land, which is exactly the case this marker exists to cover. localStorage is the
      // durable copy; the sessionStorage read stays for tabs that predate this change.
      if (sessionStorage.getItem('ekd_signed_out') || localStorage.getItem('ekd_signed_out')) return;
      // Never bounce from the callback itself — that is the flow returning, not starting.
      if (location.pathname.indexOf('/auth/callback') === 0) return;

      // A "no session" ANSWER GOES STALE, so the flag has to re-arm.
      //
      // ekd_silent_done was a bare flag with no expiry: once a signed-out visit set it, this
      // browser session never asked again. That produced exactly the sequence Ahmed reported on
      // the econ side — log out of both, log back in to ONE, visit the OTHER, and be shown
      // "Sign in" because the stale answer from before the sign-in was still on file.
      //
      // Re-armed on the two signals that mean the answer may have changed: arriving from a
      // family site (the "I just signed in over there" case), and age (a bookmark, a typed
      // address or an already-open tab carries no referrer). Bounded by a try counter so it can
      // never run away — the clock decides responsiveness, the counter decides whether a loop is
      // possible. Same division econ's older check settled on after the same bug.
      // TEN minutes, not one. The time-based re-arm exists only for the case with NO referrer —
      // a bookmark or typed address after signing in on another family site. The referrer check
      // above handles the common case instantly and is unaffected by this number.
      //
      // At 60s it cost the majority of traffic real redirects: measured 2026-08-01, this library
      // has 21,692 visitors against 603 accounts, so ~97% of arrivals are signed out and can
      // never resume. Simulated over a 12-page, 10-minute visit, a 60s window produced THREE
      // redirects; 10 minutes produces ONE — the unavoidable first ask. Making 97% of visitors
      // pay three bounces to shorten a rare no-referrer case is the wrong trade.
      var RESUME_RECHECK_MS = 10 * 60 * 1000, RESUME_MAX_TRIES = 3, TRIES_K = 'ekd_silent_tries';
      var famRef = /^https:\/\/(www\.)?(econdatalibrary|elkassabgidata|ipdatalibrary)\.com(\/|$)/;
      var doneAt = parseInt(sessionStorage.getItem('ekd_silent_done') || '0', 10) || 0;
      var tries = parseInt(sessionStorage.getItem(TRIES_K) || '0', 10) || 0;
      // '1' is what the first build wrote — treat as "checked, time unknown" and expire at once.
      if (doneAt && tries < RESUME_MAX_TRIES &&
          ((document.referrer && famRef.test(document.referrer)) || doneAt === 1 || (Date.now() - doneAt) > RESUME_RECHECK_MS)) {
        sessionStorage.removeItem('ekd_silent_done');
        doneAt = 0;
      }
      if (doneAt) return;
      sessionStorage.setItem(TRIES_K, String(tries + 1));

      // NEVER bounce a crawler. Googlebot renders JavaScript, so without this it would execute
      // the redirect and be carried off hfdatalibrary.com to accounts.elkassabgidata.com on the
      // first view of every page it visits — turning every indexable URL into a redirect to a
      // noindex auth host. That is an SEO self-inflicted wound on a site whose whole purpose is
      // being found, and it would be invisible in testing because a human browser is signed in
      // or bounces once and forgets. A bot is never signed in, so it would pay it on every page,
      // every crawl. navigator.webdriver additionally covers headless/automation.
      var ua = (navigator.userAgent || '');
      if (navigator.webdriver) return;
      if (/bot|crawl|spider|slurp|bingpreview|duckduckbot|baiduspider|yandex|facebookexternalhit|embedly|quora link preview|showyoubot|outbrain|pinterest|slackbot|vkshare|w3c_validator|whatsapp|telegrambot|discordbot|googlebot|applebot|petalbot|semrush|ahrefs|mj12bot|dotbot|lighthouse|headless/i.test(ua)) return;
      // PKCE needs SubtleCrypto, which needs a secure context. Without it, stay signed-out
      // rather than start a flow that cannot be completed.
      if (!(window.isSecureContext !== false && window.crypto && crypto.subtle && crypto.getRandomValues)) return;

      var b64url = function (bytes) {
        var s = '';
        for (var i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
        return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
      };
      var rand = function () { return b64url(crypto.getRandomValues(new Uint8Array(32))); };

      var verifier = rand(), state = rand();
      crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)).then(function (d) {
        var challenge = b64url(new Uint8Array(d));
        // Stored for the callback: the verifier it must present, the state it must match, and
        // where to put the user back. sessionStorage (not localStorage) so a second tab cannot
        // consume this tab's flow and the values die with the tab.
        sessionStorage.setItem('ekd_silent_v', verifier);
        sessionStorage.setItem('ekd_silent_s', state);
        sessionStorage.setItem('ekd_silent_r', location.pathname + location.search + location.hash);

        var redirectUri = location.origin + '/auth/callback';
        var url = ACCOUNTS_BASE + '/authorize?response_type=code&prompt=none'
          + '&client_id=' + encodeURIComponent(location.origin)
          + '&redirect_uri=' + encodeURIComponent(redirectUri)
          + '&state=' + encodeURIComponent(state)
          + '&code_challenge=' + encodeURIComponent(challenge)
          + '&code_challenge_method=S256';
        // replace(), not assign(): the bounce must not become a history entry, or Back from
        // the restored page would land the user right back in the redirect.
        location.replace(url);
      }).catch(function () {});
    } catch (e) { /* storage blocked / crypto unavailable → stay signed-out, never throw */ }
  })();

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
            window.EKD.on('login',  function (detail) {
              // ONLY a DELIBERATE sign-in retires the "stay signed out" flag. This event ALSO
              // fires for the automatic resume init() runs on every page load, and clearing the
              // flag there defeated the whole suppression: sign out, reload, init() resumes,
              // this handler wipes the flag, signed back in. The old comment here claimed it
              // "suppresses only the AUTOMATIC path" - the code did exactly the opposite.
              if (detail && detail.deliberate) {
                try { sessionStorage.removeItem('ekd_signed_out'); } catch (e) {}
                try { localStorage.removeItem('ekd_signed_out'); } catch (e) {}   // BOTH, or the
                // durable copy outlives the sign-in and suppresses resume forever after.
              }
              safeDel('ekd_notice_demoted'); paintUserWidget();
            });
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
        '</span>';
    }

    navLinks.appendChild(li);

    // logout: clears BOTH the legacy session and the EKD family session.
    window.__hfdLogout = async function () {
      loggingOut = true;                                               // suppress the demotion notice on INTENTIONAL logout
      // Suppress the silent resume for the rest of this browser session.
      //
      // The resume fires exactly WHEN there is no local credential, and logout's whole job is
      // to create that state — so without this, signing out bounced to the IdP on the very next
      // page load and signed the user straight back in. The server ends the IdP session too, but
      // that is a network call that can be slow, fail, or be raced by the reload below, and
      // "did my logout work" must not depend on winning a race. Belt and braces, deliberately.
      //
      // Only a DELIBERATE sign-in clears this again — the SDK's 'login' event carries
      // `deliberate`, and the listener below gates on it. It used to clear on ANY 'login',
      // including the automatic resume init() runs on every page load, which is precisely the
      // path this flag exists to suppress. Ledger R228.
      try { sessionStorage.setItem('ekd_signed_out', '1'); } catch (e) {}
      try { localStorage.setItem('ekd_signed_out', '1'); } catch (e) {}   // survives into OTHER tabs
      var t = safeGet('hfd_session');
      // LOCAL CREDENTIALS GO FIRST, before any network call. safeDel used to sit AFTER the
      // await, so a revocation that was merely slow — or accepted and never answered — left
      // hfd_session in localStorage AND never reached the reload below: the visitor pressed
      // Sign out and simply stayed signed in, on a page still showing their account. Revoking
      // server-side is best effort and follows. Ledger R228.
      safeDel('hfd_session');
      var revocations = [];
      if (t) {
        try {
          revocations.push(fetch(API_BASE + '/v1/auth/logout', {
            method: 'POST', headers: { 'Authorization': 'Bearer ' + t }
          }).catch(function () {}));
        } catch (e) {}
      }
      if (window.EKD) {
        try { revocations.push(Promise.resolve(window.EKD.logout()).catch(function () {})); } catch (e) {}
      }
      // Bounded wait, matching econ's account page. Give the server a moment to hear about it,
      // but never let a hung connection strand the visitor on a signed-in view whose credentials
      // this function has already deleted.
      try {
        await Promise.race([
          Promise.all(revocations),
          new Promise(function (r) { setTimeout(r, 2500); })
        ]);
      } catch (e) {}
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

  // Let same-page login flows (e.g. download.html's old email/password form) repaint the nav WITHOUT
  // a reload — they call checkAuth() after setting hfd_session, which then invokes this.
  window.__hfdPaintNav = paintUserWidget;

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
      injectCatchupBanner(meta);
    })
    .catch(function (err) {
      console.warn('Could not load metadata.json:', err);
    });

})();

/* ──────────────────────────────────────────────────────────────────────────────
 * API key substitution — ONE implementation for the whole site.
 *
 * WHY THIS REPLACED THE PER-PAGE SCRIPTS. pages/mcp.html and pages/ai-prompts.html
 * each carried their own copy of "fetch /v1/auth/me, drop api_key into .ekey".
 * Both copies resolved the session the same wrong way:
 *
 *     var token = localStorage.getItem('hfd_session');
 *     if (!token) return;                     // ← silent no-op
 *
 * hfdatalibrary.com has had TWO sign-in modes since the family SSO shipped. A
 * visitor who arrives through accounts.elkassabgidata.com holds an EKD access
 * token and has no 'hfd_session' at all, so both injectors returned at that first
 * line — the navbar showed their name while every snippet on the page still read
 * YOUR_KEY. Ahmed reported exactly that on 2026-08-31. Two copies of a rule also
 * means two things to keep in step (ledger R65 is a storage-key mismatch between
 * these very two pages), so this is now the only copy and the pages call it.
 *
 * It resolves a key through three routes, cheapest and least privileged first:
 *   1. the legacy bearer in localStorage;
 *   2. the first-party session COOKIE — api.hfdatalibrary.com is same-SITE, so a
 *      SameSite=Lax cookie rides a credentialed fetch, and this alone recovers
 *      anyone whose localStorage was cleared while their 30-day cookie lives;
 *   3. the family token, via /v1/auth/api-key, which the worker answers only when
 *      that token's audience is an HF-owned origin.
 * Any route may be unavailable; the page simply keeps its placeholders and says
 * so, which is the pre-existing behaviour.
 * ────────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var API_BASE = 'https://api.hfdatalibrary.com';
  // Every spelling of the placeholder that appears in a snippet anywhere on the
  // site. The reported example was YOUR_KEY on pages/mcp.html; a sweep of all
  // pages found three more spellings and two more pages, so this list is the
  // whole class rather than the one instance:
  //   YOUR_API_KEY   pages/ai-prompts.html   (also inside .placeholder pills)
  //   YOUR_KEY       pages/mcp.html, pages/ai-prompts.html, pages/api.html
  //   your-key-here  pages/api.html:84, pages/code.html:81 (Python), :171 (R)
  // Longest first, so a longer token can never be partly eaten by a shorter one
  // that happens to be its prefix.
  var PLACEHOLDERS = ['YOUR_API_KEY', 'your-key-here', 'YOUR_KEY'];
  var keyPromise = null;

  function safeGet(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }

  function keyFrom(json) {
    return (json && typeof json.api_key === 'string' && json.api_key) ? json.api_key : null;
  }

  // The EKD SDK is injected dynamically by the block above (script.src set at
  // runtime, appended to <head>), and a dynamically inserted script does NOT hold
  // up DOMContentLoaded. So on the family sign-in path window.EKD is reliably
  // still undefined when this module first runs — the exact case the whole key
  // filler exists to serve. Wait for it, but only when there is a refresh token
  // to suggest the SDK is going to matter, and only for a bounded time.
  function waitForEkd(timeoutMs) {
    return new Promise(function (resolve) {
      if (window.EKD) { resolve(window.EKD); return; }
      if (!safeGet('ekd_rt')) { resolve(null); return; }
      var waited = 0, step = 150;
      var t = setInterval(function () {
        waited += step;
        if (window.EKD) { clearInterval(t); resolve(window.EKD); }
        else if (waited >= timeoutMs) { clearInterval(t); resolve(null); }
      }, step);
    });
  }

  // Resolve the signed-in user's API key, or null. Never throws, never rejects.
  //
  // A null is NOT memoised. Caching it was a real defect: the first call can lose
  // the race with the SDK above, and a cached null would then keep every snippet
  // on the page reading YOUR_KEY for the rest of its life with no way to retry.
  // Only a real key is worth remembering.
  function resolveApiKey() {
    if (keyPromise) return keyPromise;
    var pending = (async function () {
      // Nothing to resolve for a visitor with no session marker, and asking anyway
      // would spend a D1 read on every anonymous view of the four snippet pages.
      // These are the same two markers the navbar gates on (paintUserWidget), so a
      // page can never think it is signed in while this thinks otherwise.
      if (!safeGet('hfd_session') && !safeGet('ekd_rt')) return null;
      // (1) legacy bearer
      var legacy = safeGet('hfd_session');
      if (legacy) {
        try {
          var r = await fetch(API_BASE + '/v1/auth/me', { headers: { 'Authorization': 'Bearer ' + legacy } });
          if (r.ok) { var k = keyFrom(await r.json()); if (k) return k; }
        } catch (e) {}
      }
      // (2) first-party cookie (same-site subdomain; needs credentials)
      try {
        var r2 = await fetch(API_BASE + '/v1/auth/me', { credentials: 'include' });
        if (r2.ok) { var k2 = keyFrom(await r2.json()); if (k2) return k2; }
      } catch (e) {}
      // (3) family/EKD session — the worker decides whether to answer.
      try {
        var ekd = await waitForEkd(8000);
        if (ekd && typeof ekd.getAccessToken === 'function') {
          var at = await ekd.getAccessToken();
          if (at) {
            var r3 = await fetch(API_BASE + '/v1/auth/api-key', { headers: { 'Authorization': 'Bearer ' + at } });
            if (r3.ok) { var k3 = keyFrom(await r3.json()); if (k3) return k3; }
          }
        }
      } catch (e) {}
      return null;
    })();
    keyPromise = pending;
    pending.then(function (k) {
      if (!k && keyPromise === pending) keyPromise = null;   // never cache a miss
    });
    return pending;
  }

  // Replace placeholder TEXT inside snippet blocks with a marked span, so pages
  // that never adopted the .ekey convention (pages/api.html) are covered too.
  // Text nodes only, and only inside <pre>/<code>: prose that mentions YOUR_KEY
  // to explain the convention must keep saying YOUR_KEY, and no form field is
  // ever rewritten.
  // ONE pass over the document's text nodes, keeping those inside a snippet.
  //
  // Walking `pre, code` separately looked equivalent and was not: the site's
  // snippets are <pre><code>…</code></pre>, so the same text node is reached
  // twice — once as a descendant of the <pre> and once of the <code> — and the
  // second visit wrapped the span the first visit had just created. Verified in
  // the browser: pages/api.html produced 8 nested spans for its 4 placeholders.
  function wrapPlaceholderText(root) {
    var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
    var targets = [], n;
    while ((n = walker.nextNode())) {
      if (!n.nodeValue || !n.parentElement) continue;
      if (!n.parentElement.closest('pre, code')) continue;   // snippets only, never prose
      if (n.parentElement.classList.contains('ekey')) continue;  // already substituted
      for (var p = 0; p < PLACEHOLDERS.length; p++) {
        if (n.nodeValue.indexOf(PLACEHOLDERS[p]) !== -1) { targets.push(n); break; }
      }
    }
    for (var t = 0; t < targets.length; t++) splitTextNode(targets[t]);
  }

  function splitTextNode(node) {
    var text = node.nodeValue;
    var frag = document.createDocumentFragment();
    var i = 0;
    while (i < text.length) {
      var best = -1, bestTok = null;
      for (var p = 0; p < PLACEHOLDERS.length; p++) {
        var at = text.indexOf(PLACEHOLDERS[p], i);
        if (at !== -1 && (best === -1 || at < best)) { best = at; bestTok = PLACEHOLDERS[p]; }
      }
      if (best === -1) { frag.appendChild(document.createTextNode(text.slice(i))); break; }
      if (best > i) frag.appendChild(document.createTextNode(text.slice(i, best)));
      var span = document.createElement('span');
      span.className = 'ekey';
      span.textContent = bestTok;              // still the placeholder; fill() sets the real value
      frag.appendChild(span);
      i = best + bestTok.length;
    }
    node.parentNode.replaceChild(frag, node);
  }

  function announce(signedIn) {
    var bar = document.getElementById('keybar');
    if (bar) {
      if (signedIn) {
        bar.classList.add('signed');
        bar.textContent = '';
        var strong = document.createElement('strong');
        strong.textContent = 'Signed in.';
        bar.appendChild(strong);
        bar.appendChild(document.createTextNode(
          ' Every command below now contains your real API key — just copy and paste. ' +
          'Treat it like a password: do not share it or commit it to a repository.'));
      }
      // Not signed in → the page's own static wording already explains the placeholder.
    }
    var note = document.getElementById('key-note');
    if (note && signedIn) {
      note.textContent = 'You are signed in, so your real API key is already filled into every ' +
        'prompt below. Yellow-pill tokens like AAPL are still yours to edit.';
    }
  }

  async function fill() {
    // Normalise first so .ekey is the single thing we fill, whether the page
    // author wrote the span or wrote bare text in a code block.
    try { wrapPlaceholderText(document); } catch (e) {}
    var spans = document.querySelectorAll('.ekey, .placeholder');
    if (!spans.length) return;
    var key = await resolveApiKey();
    if (!key) return;
    for (var i = 0; i < spans.length; i++) {
      var el = spans[i];
      var txt = (el.textContent || '').trim();
      // .placeholder is also used for editable sample values such as AAPL — only
      // rewrite the ones that are actually a key placeholder.
      if (PLACEHOLDERS.indexOf(txt) === -1) continue;
      el.textContent = key;                    // textContent, never innerHTML
      el.setAttribute('data-real-key', '1');
    }
    announce(true);
  }

  window.HFDKeys = { get: resolveApiKey, fill: fill };

  // ── Saving a file without tripping Chrome's automatic-downloads block ───────
  // Every ordinary way a page starts a download — a programmatic <a download>
  // click, a navigation to a Content-Disposition: attachment response, and a
  // blob:/object-URL click alike — goes through Chrome's DownloadRequestLimiter.
  // A visitor who once answered Block to the "download multiple files" prompt has
  // all three refused silently, site-wide, and no script can detect or undo it.
  // The File System Access API is the one route the limiter never sees (its gate
  // is FILE_SYSTEM_WRITE_GUARD and it creates no DownloadItem), so it is tried
  // first wherever it exists and the classic route stays as the fallback.
  //
  // MUST be called while the user's transient activation is still live —
  // showSaveFilePicker() requires it — so call it from the click path, not after
  // a long await. A picker that throws for any reason other than the user
  // cancelling falls back rather than failing.
  var HFDSave = {
    // Ask the user where to put a file.
    //   → a FileSystemFileHandle when the picker is available and they chose;
    //   → 'cancelled' when they dismissed it (callers must NOT fall back — the
    //     user said no, and starting a download anyway is the opposite of that);
    //   → null when there is no picker, or it failed for any other reason
    //     (most often the transient activation having expired), meaning: use the
    //     classic route.
    // Shared so the two callers cannot drift apart the way the two API-key
    // injectors did.
    pickHandle: async function (filename) {
      if (typeof window.showSaveFilePicker !== 'function') return null;
      try {
        return await window.showSaveFilePicker({ suggestedName: filename });
      } catch (e) {
        return (e && e.name === 'AbortError') ? 'cancelled' : null;
      }
    },
    // Returns 'saved' | 'cancelled' | 'fallback'.
    blob: async function (blob, filename) {
      var handle = await HFDSave.pickHandle(filename);
      if (handle === 'cancelled') return 'cancelled';
      if (handle) {
        var w = await handle.createWritable();
        await blob.stream().pipeTo(w);
        return 'saved';
      }
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url; a.download = filename;
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(function () { URL.revokeObjectURL(url); }, 60000);
      return 'fallback';
    }
  };
  window.HFDSave = HFDSave;

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { fill(); });
  } else {
    fill();
  }
})();
