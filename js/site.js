/* HF Data Library — Dynamic site data
   Loads metadata.json and populates all dynamic elements.
   Author: Ahmed Elkassabgi */

(function () {
  'use strict';

  // Determine path to data/metadata.json relative to current page
  const isSubpage = window.location.pathname.includes('/pages/');
  const basePath = isSubpage ? '../data/metadata.json' : 'data/metadata.json';

  // Format large numbers: 1533014567 → "1.53B"
  function formatBars(n) {
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + 'K';
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
        // Final value with short label
        el.textContent = target.toLocaleString() + ' (' + formatBars(target) + ')';
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

  // ── User Widget in Navbar ──
  const API_BASE = 'https://api.hfdatalibrary.com';
  const isSubpage2 = window.location.pathname.includes('/pages/');
  const downloadUrl = isSubpage2 ? 'download.html' : 'pages/download.html';

  async function buildUserWidget() {
    const navLinks = document.querySelector('.nav-links');
    if (!navLinks) return;

    const sessionToken = localStorage.getItem('hfd_session');
    let user = null;

    if (sessionToken) {
      try {
        const r = await fetch(API_BASE + '/v1/auth/me', {
          headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        if (r.ok) user = await r.json();
      } catch (e) {}
    }

    // Remove existing widget if any
    const existing = document.getElementById('nav-user-widget');
    if (existing) existing.remove();

    const li = document.createElement('li');
    li.id = 'nav-user-widget';
    li.style.marginLeft = '0.5rem';

    if (user) {
      const vipBadge = user.is_vip
        ? '<span style="display:inline-block; background:linear-gradient(135deg,#d4a843,#f0d78c); color:#1a2332; font-size:0.6rem; font-weight:700; padding:0.1em 0.4em; border-radius:3px; margin-left:0.25rem; letter-spacing:0.05em; text-transform:uppercase;">&#9733;</span>'
        : '';
      const firstName = (user.name || '').split(' ')[0];
      li.style.position = 'relative';
      li.innerHTML =
        '<div style="display:inline-flex; align-items:center; gap:0.4rem; background:rgba(255,255,255,0.1); border-radius:6px; padding:0.35rem 0.6rem; color:#fff; font-size:0.85rem; cursor:pointer; white-space:nowrap;" onclick="document.getElementById(\'user-dropdown\').style.display = document.getElementById(\'user-dropdown\').style.display === \'block\' ? \'none\' : \'block\'">' +
          '<span style="display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; background:var(--gold); color:var(--navy); border-radius:50%; font-weight:700; font-size:0.7rem;">' + (user.name || 'U')[0].toUpperCase() + '</span>' +
          '<span>' + firstName + '</span>' + vipBadge +
          '<span style="font-size:0.65rem; opacity:0.7;">&#9660;</span>' +
        '</div>' +
        '<div id="user-dropdown" style="display:none; position:absolute; top:calc(100% + 0.5rem); right:0; background:#fff; border:1px solid var(--gray-200); border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.15); padding:0.5rem 0; min-width:220px; z-index:101;">' +
          '<div style="padding:0.75rem 1rem; border-bottom:1px solid var(--gray-100);">' +
            '<div style="font-weight:600; color:var(--navy);">' + user.name + '</div>' +
            '<div style="font-size:0.8rem; color:var(--gray-500);">' + user.email + '</div>' +
          '</div>' +
          '<a href="' + (isSubpage2 ? 'account.html' : 'pages/account.html') + '" style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">My Account</a>' +
          '<a href="' + downloadUrl + '" style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">Downloads</a>' +
          (user.is_admin ? '<a href="' + (isSubpage2 ? 'admin.html' : 'pages/admin.html') + '" style="display:block; padding:0.5rem 1rem; color:var(--gray-700); font-size:0.9rem;">Admin Panel</a>' : '') +
          '<div onclick="window.__hfdLogout()" style="display:block; padding:0.5rem 1rem; color:var(--red); font-size:0.9rem; cursor:pointer; border-top:1px solid var(--gray-100); margin-top:0.25rem;">Log out</div>' +
        '</div>';
    } else {
      li.innerHTML =
        '<a href="' + downloadUrl + '#register" style="background:var(--gold); color:var(--navy); padding:0.4rem 0.875rem; border-radius:6px; font-size:0.85rem; font-weight:600; white-space:nowrap;">Sign in</a>';
    }

    navLinks.appendChild(li);

    // Expose logout
    window.__hfdLogout = async function() {
      const t = localStorage.getItem('hfd_session');
      if (t) {
        try { await fetch(API_BASE + '/v1/auth/logout', { method: 'POST', headers: { 'Authorization': 'Bearer ' + t } }); } catch (e) {}
      }
      localStorage.removeItem('hfd_session');
      window.location.reload();
    };

    // VIP site-wide banner
    if (user && user.is_vip) {
      const existingBanner = document.getElementById('vip-banner');
      if (!existingBanner) {
        const banner = document.createElement('div');
        banner.id = 'vip-banner';
        banner.style.cssText = 'background:linear-gradient(90deg,#1a2332 0%,#2a3a5a 50%,#1a2332 100%); color:#d4a843; padding:0.4rem 0; text-align:center; font-size:0.8rem; font-weight:500; letter-spacing:0.05em; border-bottom:1px solid #d4a843;';
        banner.innerHTML = '&#9733; VIP MEMBER &#9733; &nbsp;&nbsp; You have access to premium features and priority support.';
        const navbar = document.querySelector('.navbar');
        if (navbar) navbar.parentNode.insertBefore(banner, navbar.nextSibling);
      }
    }
  }

  buildUserWidget();

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
    if (counterEl && meta.bars_clean) {
      animateCounter(counterEl, meta.bars_clean);
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

  // Fetch and apply
  fetch(basePath)
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
