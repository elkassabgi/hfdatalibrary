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

  // Populate all elements with data-meta attribute
  function populateData(meta) {
    // Map of data-meta values to their display values
    const values = {
      'tickers': formatComma(meta.tickers),
      'tickers-short': meta.tickers.toLocaleString(),
      'bars-raw': formatComma(meta.bars_raw),
      'bars-clean': formatComma(meta.bars_clean),
      'bars-filled': formatComma(meta.bars_filled),
      'bars-raw-short': formatBars(meta.bars_raw),
      'bars-clean-short': formatBars(meta.bars_clean),
      'bars-filled-short': formatBars(meta.bars_filled),
      'bars-removed': formatComma(meta.bars_removed),
      'bars-filled-count': formatComma(meta.bars_filled_count),
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
