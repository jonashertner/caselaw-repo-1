/**
 * OpenCaseLaw Word Add-in — Main Application
 *
 * All dynamic content is escaped via escHtml() before insertion.
 * innerHTML is used only with pre-escaped strings — no raw user input.
 * Views: search (default), detail, laws, verify, settings
 */

// State
var state = {
  view: 'search',
  lang: 'de',
  query: '',
  results: [],
  total: 0,
  offset: 0,
  loading: false,
  error: null,
  detail: null,
  caseBrief: null,
  verifyResult: null,
  verifyText: '',
  filters: {},
  courts: [],
};

// Track whether we're running inside Word or standalone browser
var _insideWord = false;

// Initialize — works both inside Word (Office.onReady) and in plain browser
function initApp() {
  document.getElementById('lang-select').addEventListener('change', function (e) {
    state.lang = e.target.value;
    // Update gear button title
    document.getElementById('btn-settings').title = t('settings_gear_title', state.lang);
    document.getElementById('btn-settings').setAttribute('aria-label', t('settings_gear_title', state.lang));
    if (_insideWord && Office.context && Office.context.roamingSettings) {
      try {
        Office.context.roamingSettings.set('ocl_lang', state.lang);
        Office.context.roamingSettings.saveAsync();
      } catch (err) { /* roamingSettings not available */ }
    } else {
      try { localStorage.setItem('ocl_lang', state.lang); } catch (e) {}
    }
    render();
  });

  document.getElementById('btn-settings').addEventListener('click', function () {
    state.view = 'settings';
    render();
  });

  // Load saved language
  if (!_insideWord) {
    try { state.lang = localStorage.getItem('ocl_lang') || 'de'; } catch (e) {}
  }
  document.getElementById('lang-select').value = state.lang;

  // Bind delegated click handler ONCE (never re-bind)
  document.getElementById('app').addEventListener('click', handleAppClick);

  // Render immediately, then fetch courts in background
  render();
  listCourts().then(function (c) { state.courts = c; }).catch(function () {});
}

// Try Office.js initialization (fires if inside Word)
if (typeof Office !== 'undefined' && Office.onReady) {
  Office.onReady(async function (info) {
    if (info.host === Office.HostType.Word) {
      _insideWord = true;
      try {
        var settings = Office.context.roamingSettings;
        state.lang = settings.get('ocl_lang') || 'de';
      } catch (e) {}
    }
    initApp();
  });
} else {
  // Standalone browser — init on DOMContentLoaded
  document.addEventListener('DOMContentLoaded', function () { initApp(); });
}

// Rendering — all values passed through escHtml() for XSS safety
// Safe: all dynamic content is escaped via escHtml before concatenation
function render() {
  var app = document.getElementById('app');
  var html;
  switch (state.view) {
    case 'search':   html = renderSearch(); break;
    case 'detail':   html = renderDetail(); break;
    case 'laws':     html = renderLaws(); break;
    case 'verify':   html = renderVerify(); break;
    case 'guide':    html = renderGuide(); break;
    case 'settings': html = renderSettings(); break;
    default:         html = ''; break;
  }
  app.innerHTML = html; // eslint-disable-line no-unsanitized/property -- all values pre-escaped via escHtml()
  bindEvents();
}

// Search View
function renderSearch() {
  var lang = state.lang;
  var html =
    '<div class="search-wrap">' +
    '<svg class="search-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>' +
    '<input class="search-bar" id="search-input" type="text" ' +
    'placeholder="' + escHtml(t('search_placeholder', lang)) + '" ' +
    'value="' + escHtml(state.query) + '">' +
    '</div>' +
    '<div class="filters">' +
    '<button class="filter-btn" data-action="show-laws">\uD83D\uDCDA ' + escHtml(t('btn_laws', lang)) + '</button>' +
    '<button class="filter-btn" data-action="verify-ref" title="' + escHtml(t('btn_verify_title', lang)) + '">\uD83D\uDD0D ' + escHtml(t('btn_verify', lang)) + '</button>' +
    '</div>';

  if (state.loading) {
    html += '<div class="results">' + renderSkeletonCards(3) + '</div>';
  } else if (state.error) {
    html += renderError();
  } else if (state.results.length === 0 && state.query) {
    html += '<div class="state-message">' + escHtml(t('no_results', lang)) +
      '<br><span style="font-size:12px;">' + escHtml(t('no_results_hint', lang)) + '</span></div>';
  } else if (state.results.length === 0 && !state.query) {
    html += renderWelcome(lang);
  } else {
    html += '<div class="results">';
    if (state.total > 0) {
      html += '<div class="results-count">' + escHtml(t('results_count', lang, { n: state.total })) + '</div>';
    }
    for (var i = 0; i < state.results.length; i++) {
      html += renderResultCard(state.results[i], i);
    }
    if (state.results.length < state.total) {
      html += '<button class="load-more" data-action="load-more">' + escHtml(t('load_more', lang)) + '</button>';
    }
    html += '</div>';
  }
  return html;
}

function renderWelcome(lang) {
  var isFirstRun = false;
  try { isFirstRun = !localStorage.getItem('ocl_seen'); } catch (e) {}

  var html = '<div class="welcome">';

  // Hero
  html += '<div class="welcome-hero">' +
    '<div class="welcome-count">' + escHtml(t('welcome_count', lang, { n: '963\u2009000+' })) + '</div>' +
    '<div class="welcome-hint">' + escHtml(t('welcome_hint', lang)) + '</div>' +
    '</div>';

  // Feature cards — always show on first run, collapsed after
  if (isFirstRun) {
    html += '<div class="feature-cards">' +
      '<div class="feature-card">' +
      '<div class="feature-icon">\uD83D\uDD0E</div>' +
      '<div class="feature-text"><strong>' + escHtml(t('feat_search_title', lang)) + '</strong><br>' +
      escHtml(t('feat_search_desc', lang)) + '</div></div>' +
      '<div class="feature-card">' +
      '<div class="feature-icon">\uD83D\uDCDD</div>' +
      '<div class="feature-text"><strong>' + escHtml(t('feat_cite_title', lang)) + '</strong><br>' +
      escHtml(t('feat_cite_desc', lang)) + '</div></div>' +
      '<div class="feature-card">' +
      '<div class="feature-icon">\u2705</div>' +
      '<div class="feature-text"><strong>' + escHtml(t('feat_verify_title', lang)) + '</strong><br>' +
      escHtml(t('feat_verify_desc', lang)) + '</div></div>' +
      '</div>';
    try { localStorage.setItem('ocl_seen', '1'); } catch (e) {}
  }

  // Quick search suggestions
  html += '<div class="quick-label">' + escHtml(t('quick_try', lang)) + '</div>';
  html += '<div class="quick-chips">';
  var examples = t('quick_examples', lang).split('|');
  for (var i = 0; i < examples.length; i++) {
    html += '<button class="quick-chip" data-action="quick-search" data-query="' + escHtml(examples[i]) + '">' + escHtml(examples[i]) + '</button>';
  }
  html += '</div>';

  // How it works link
  html += '<div class="welcome-footer">' +
    '<a class="welcome-link" data-action="show-guide">' + escHtml(t('how_it_works', lang)) + '</a>' +
    '</div>';

  html += '</div>';
  return html;
}

function renderResultCard(r, idx) {
  var lang = state.lang;
  var badges = '';
  if (r.is_leading_case) badges += '<span class="badge badge-leading">\u2605 ' + escHtml(t('badge_leading', lang)) + '</span> ';
  if (r.citation_count > 0) badges += '<span class="badge badge-citations">' + escHtml(t('badge_citations', lang, { n: r.citation_count })) + '</span> ';
  if (r.legal_area) badges += '<span class="badge badge-area">' + escHtml(r.legal_area) + '</span>';

  var regeste = r.regeste || r.snippet || '';
  if (regeste.length > 200) regeste = regeste.substring(0, 200) + '\u2026';
  var courtName = r.court ? getCourtName(r.court, lang) : (r.court_name || '');
  var date = r.date || '';

  return '<div class="result-card">' +
    '<div class="result-header"><div>' +
    '<div class="result-docket">' + escHtml(r.docket_number || r.decision_id) + '</div>' +
    '<div class="result-meta">' + escHtml(date) + ' \u00B7 ' + escHtml(courtName) + '</div>' +
    '</div><div>' + badges + '</div></div>' +
    '<div class="result-regeste">' + escHtml(regeste) + '</div>' +
    '<div class="result-actions">' +
    '<button class="btn btn-insert" data-action="insert" data-idx="' + idx + '">' + escHtml(t('btn_insert', lang)) + '</button>' +
    '<button class="btn btn-detail" data-action="detail" data-idx="' + idx + '">' + escHtml(t('btn_fulltext', lang)) + '</button>' +
    '</div></div>';
}

// Detail View
function renderDetail() {
  var lang = state.lang;
  var d = state.detail;
  var cb = state.caseBrief;

  if (!d) return renderDetailSkeleton();

  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', lang)) + '</a>';
  html += '<h2 class="detail-title">' + escHtml(d.docket_number || d.decision_id) + '</h2>';
  var courtName = d.court ? getCourtName(d.court, lang) : (d.court_name || '');
  html += '<div class="detail-meta">' + escHtml(d.date || '') + ' \u00B7 ' + escHtml(courtName) + '</div>';

  var badges = '';
  if (d.is_leading_case) badges += '<span class="badge badge-leading">\u2605 ' + escHtml(t('badge_leading', lang)) + '</span> ';
  if (d.citation_count) badges += '<span class="badge badge-citations">' + escHtml(String(d.citation_count)) + ' ' + escHtml(t('citations_label', lang)) + '</span> ';
  if (d.legal_area) badges += '<span class="badge badge-area">' + escHtml(d.legal_area) + '</span>';
  if (badges) html += '<div class="badges-row">' + badges + '</div>';

  if (d.regeste) {
    html += '<div class="section-card"><div class="section-label">' + escHtml(t('section_regeste', lang)) + '</div>' +
      '<div class="section-body">' + escHtml(d.regeste) + '</div></div>';
  }

  if (cb && cb.erwaegungen && cb.erwaegungen.length) {
    html += '<div class="section-card"><div class="section-label">' + escHtml(t('section_erwaegungen', lang)) + '</div>';
    for (var i = 0; i < cb.erwaegungen.length; i++) {
      var e = cb.erwaegungen[i];
      var num = e.number || '?';
      var preview = (e.text || '').substring(0, 80);
      html += '<div class="erwaegung-row">' +
        '<span class="erwaegung-num">E. ' + escHtml(num) + '</span>' +
        '<span class="erwaegung-text">' + escHtml(preview) + '\u2026</span>' +
        '<button class="btn btn-insert erwaegung-insert" data-action="insert-ew" data-ew="' + escHtml(num) + '">' + escHtml(t('btn_insert', lang)) + '</button>' +
        '</div>';
    }
    html += '</div>';
  } else if (state.loading) {
    html += '<div class="section-card">' + renderSkeletonLines(4) + '</div>';
  }

  if (cb && cb.statutes && cb.statutes.length) {
    html += '<div class="section-card"><div class="section-label">' + escHtml(t('section_statutes', lang)) + '</div><div class="pills">';
    for (var j = 0; j < cb.statutes.length; j++) {
      html += '<span class="pill">' + escHtml(cb.statutes[j]) + '</span>';
    }
    html += '</div></div>';
  }

  html += '<button class="btn btn-insert btn-full" data-action="insert-main">' + escHtml(t('btn_insert', lang)) + '</button>';
  return html;
}

// Laws View
function renderLaws() {
  var lang = state.lang;
  return '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back_short', lang)) + '</a>' +
    '<input class="search-bar" id="law-search-input" type="text" placeholder="' + escHtml(t('law_search_placeholder', lang)) + '">' +
    '<div id="law-results" class="results" style="margin-top:12px;"></div>';
}

// Guide View
function renderGuide() {
  var lang = state.lang;
  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back_short', lang)) + '</a>';
  html += '<h3 class="settings-heading">' + escHtml(t('guide_title', lang)) + '</h3>';

  // Step 1: Search
  html += '<div class="guide-step">' +
    '<div class="guide-step-num">1</div>' +
    '<div class="guide-step-body">' +
    '<div class="guide-step-title">' + escHtml(t('guide_step1_title', lang)) + '</div>' +
    '<div class="guide-step-desc">' + escHtml(t('guide_step1_desc', lang)) + '</div>' +
    '</div></div>';

  // Step 2: Insert
  html += '<div class="guide-step">' +
    '<div class="guide-step-num">2</div>' +
    '<div class="guide-step-body">' +
    '<div class="guide-step-title">' + escHtml(t('guide_step2_title', lang)) + '</div>' +
    '<div class="guide-step-desc">' + escHtml(t('guide_step2_desc', lang)) + '</div>' +
    '</div></div>';

  // Step 3: Verify
  html += '<div class="guide-step">' +
    '<div class="guide-step-num">3</div>' +
    '<div class="guide-step-body">' +
    '<div class="guide-step-title">' + escHtml(t('guide_step3_title', lang)) + '</div>' +
    '<div class="guide-step-desc">' + escHtml(t('guide_step3_desc', lang)) + '</div>' +
    '</div></div>';

  // Coverage info
  html += '<div class="section-card" style="margin-top:16px;">' +
    '<div class="section-label">' + escHtml(t('guide_coverage_title', lang)) + '</div>' +
    '<div class="section-body">' + escHtml(t('guide_coverage_desc', lang)) + '</div>' +
    '</div>';

  html += '<button class="btn btn-insert btn-full" data-action="back" style="margin-top:16px;">' + escHtml(t('guide_start', lang)) + '</button>';
  return html;
}

// Verify View
function renderVerify() {
  var lang = state.lang;
  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back_short', lang)) + '</a>';
  html += '<h3 class="verify-heading">' + escHtml(t('verify_title', lang)) + '</h3>';

  if (state.verifyText) {
    html += '<div class="verify-selected"><div class="section-label">' + escHtml(t('verify_selected', lang)) + '</div>' +
      '<div class="section-body">' + escHtml(state.verifyText) + '</div></div>';
  }

  if (state.loading) {
    html += '<div class="verify-loading"><div class="spinner"></div><span>' + escHtml(t('verify_checking', lang)) + '</span></div>';
  } else if (state.verifyResult) {
    var v = state.verifyResult;
    var verdictLabels = {
      supports: t('verdict_supports', lang),
      partial: t('verdict_partial', lang),
      contradicts: t('verdict_contradicts', lang),
    };
    var icons = { supports: '\u2713', partial: '\u26A0', contradicts: '\u2717' };

    html += '<div class="verdict-card ' + escHtml(v.verdict) + '">' +
      '<div class="verdict-header">' +
      '<div class="verdict-icon ' + escHtml(v.verdict) + '">' + (icons[v.verdict] || '?') + '</div>' +
      '<div class="verdict-label">' + escHtml(verdictLabels[v.verdict] || v.verdict) + '</div></div>' +
      '<div class="verdict-explanation">' + escHtml(v.explanation || '') + '</div></div>';

    if (v.quote) {
      html += '<div class="section-card"><div class="section-label">' + escHtml(t('relevant_ew', lang)) +
        (v.relevant_erwaegung ? ' (E. ' + escHtml(v.relevant_erwaegung) + ')' : '') +
        '</div><div class="verdict-quote">\u00AB' + escHtml(v.quote) + '\u00BB</div></div>';
    }

    var commentBtn = supportsComments()
      ? '<button class="btn btn-insert" style="flex:1;" data-action="insert-comment">' + escHtml(t('btn_insert_comment', lang)) + '</button>'
      : '<button class="btn btn-insert" style="flex:1;" data-action="insert-verdict-text">' + escHtml(t('btn_insert_result', lang)) + '</button>';

    html += '<div class="verdict-actions">' + commentBtn +
      '<button class="btn btn-detail" style="flex:1;" data-action="verify-fulltext">' + escHtml(t('btn_fulltext', lang)) + '</button></div>';
    var footerKey = localStorage.getItem('ocl_pro_key') ? 'verify_footer_pro' : 'verify_footer';
    html += '<div class="verdict-footer">' + escHtml(t(footerKey, lang)) + '</div>';
  } else if (state.error) {
    html += renderError();
  }

  return html;
}

// Settings View
function renderSettings() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key') || '';
  var apiKey = localStorage.getItem('ocl_anthropic_key') || '';
  var previewCitation = formatCitation({ court: 'bge', docket_number: 'BGE 125 III 231', date: '1999-01-01' }, lang, '3');

  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back_short', lang)) + '</a>';
  html += '<h3 class="settings-heading">' + escHtml(t('settings_title', lang)) + '</h3>';

  // Citation format preview
  html += '<div class="section-card"><div class="section-label">' + escHtml(t('settings_citation_format', lang)) + '</div>' +
    '<div class="settings-preview">' + escHtml(t('settings_preview', lang)) + ': <strong>' + escHtml(previewCitation) + '</strong></div></div>';

  // Pro subscription section
  html += '<div class="section-card">';
  if (proKey) {
    // Pro active
    html += '<div class="pro-badge-row"><span class="pro-badge">PRO</span> ' + escHtml(t('pro_active', lang)) + '</div>' +
      '<div class="settings-field"><label class="settings-label">' + escHtml(t('pro_license_key', lang)) + '</label>' +
      '<input type="text" class="settings-input" id="pro-key-input" value="' + escHtml(proKey) + '" readonly style="font-size:11px;color:var(--text-secondary);">' +
      '</div>' +
      '<div class="settings-actions">' +
      '<button class="btn btn-detail" data-action="remove-pro" style="color:var(--red);">' + escHtml(t('btn_remove_license', lang)) + '</button>' +
      '</div>';
  } else {
    // Upgrade CTA
    html += '<div class="section-label">' + escHtml(t('pro_section_title', lang)) + '</div>' +
      '<div class="pro-features">' +
      '<div class="pro-feature">\u2713 ' + escHtml(t('pro_feature_verify', lang)) + '</div>' +
      '<div class="pro-feature">\u2713 ' + escHtml(t('pro_feature_no_key', lang)) + '</div>' +
      '<div class="pro-feature">\u2713 ' + escHtml(t('pro_feature_limit', lang)) + '</div>' +
      '</div>' +
      '<div class="pro-price">CHF 5 / ' + escHtml(t('pro_month', lang)) + '</div>' +
      '<button class="btn btn-pro" data-action="upgrade-pro">' + escHtml(t('btn_upgrade', lang)) + '</button>' +
      '<div class="pro-divider"><span>' + escHtml(t('pro_or_key', lang)) + '</span></div>' +
      '<div class="settings-field"><label class="settings-label">' + escHtml(t('pro_license_key', lang)) + '</label>' +
      '<input type="text" class="settings-input" id="pro-key-input" value="" placeholder="ocl_pro_...">' +
      '</div>' +
      '<div class="settings-actions">' +
      '<button class="btn btn-insert" data-action="activate-pro">' + escHtml(t('btn_activate', lang)) + '</button>' +
      '</div>';
  }
  html += '</div>';

  // Own API key section (fallback for power users)
  html += '<div class="section-card"><div class="section-label">' + escHtml(t('settings_own_key', lang)) + '</div>' +
    '<div class="settings-hint" style="margin-bottom:6px;">' + escHtml(t('settings_own_key_hint', lang)) + '</div>' +
    '<div class="settings-field"><label class="settings-label">' + escHtml(t('settings_api_key', lang)) + '</label>' +
    '<input type="password" class="settings-input" id="api-key-input" value="' + escHtml(apiKey) + '" placeholder="sk-ant-...">' +
    '<div class="settings-hint">' + escHtml(t('settings_api_hint', lang)) + '</div></div>' +
    '<div class="settings-actions">' +
    '<button class="btn btn-insert" data-action="save-key">' + escHtml(t('btn_save', lang)) + '</button>' +
    '<button class="btn btn-detail" data-action="test-key">' + escHtml(t('btn_test', lang)) + '</button>';
  if (apiKey) html += '<button class="btn btn-detail" data-action="delete-key" style="color:var(--red);">' + escHtml(t('btn_delete', lang)) + '</button>';
  html += '</div></div>';

  html += '<div class="settings-footer">' +
    '<a href="https://opencaselaw.ch" target="_blank" style="color:var(--blue);">opencaselaw.ch</a> \u00B7 ' +
    '<a href="https://github.com/jonashertner/caselaw-repo-1" target="_blank" style="color:var(--blue);">GitHub</a><br>' +
    'Code: MIT \u00B7 Daten: CC0 1.0</div>';

  return html;
}

// Error rendering
function renderError() {
  var lang = state.lang;
  if (!state.error) return '';
  if (state.error.type === 'rate_limit') {
    return '<div class="state-message">' + escHtml(t('error_rate_limit', lang)) + '<br>' +
      escHtml(t('error_rate_wait', lang, { n: state.error.retryAfter })) +
      '<button class="retry-btn" data-action="retry">' + escHtml(t('btn_retry', lang)) + '</button></div>';
  }
  if (state.error.type === 'no_citation' || state.error.type === 'no_selection' || state.error.type === 'decision_not_found') {
    return '<div class="state-message">' + escHtml(state.error.message) + '</div>';
  }
  return '<div class="state-message">' + escHtml(t('error_connection', lang)) +
    '<button class="retry-btn" data-action="retry">' + escHtml(t('btn_retry', lang)) + '</button></div>';
}

// Skeleton helpers
function renderSkeletonCards(n) {
  var html = '';
  for (var i = 0; i < n; i++) {
    html += '<div class="skeleton-card"><div class="skeleton-line" style="width:40%;"></div>' +
      '<div class="skeleton-line short" style="width:25%;margin-top:4px;"></div>' +
      '<div class="skeleton-line" style="margin-top:8px;"></div>' +
      '<div class="skeleton-line" style="width:85%;"></div></div>';
  }
  return html;
}

function renderSkeletonLines(n) {
  var html = '';
  for (var i = 0; i < n; i++) {
    html += '<div class="skeleton-line" style="width:' + (70 + Math.round(Math.random() * 25)) + '%;"></div>';
  }
  return html;
}

function renderDetailSkeleton() {
  return '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', state.lang)) + '</a>' +
    '<div class="skeleton-card" style="margin-top:8px;">' +
    '<div class="skeleton-line" style="width:50%;height:18px;"></div>' +
    '<div class="skeleton-line short" style="margin-top:6px;"></div>' +
    '</div>' +
    '<div class="skeleton-card">' + renderSkeletonLines(5) + '</div>' +
    '<div class="skeleton-card">' + renderSkeletonLines(3) + '</div>';
}

// Post-render setup (input focus + keydown only — click is bound once in initApp)
function bindEvents() {
  var searchInput = document.getElementById('search-input');
  if (searchInput) {
    searchInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') doSearch(searchInput.value);
    });
    searchInput.focus();
  }

  var lawInput = document.getElementById('law-search-input');
  if (lawInput) {
    lawInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') doLawSearch(lawInput.value);
    });
    lawInput.focus();
  }
}

// Delegated click handler — bound ONCE in initApp, never re-added
async function handleAppClick(e) {
    var btn = e.target.closest('[data-action]');
    if (!btn) return;
    var action = btn.dataset.action;
    var idx = parseInt(btn.dataset.idx, 10);

    switch (action) {
      case 'insert':       await insertCitation(state.results[idx]); break;
      case 'detail':       await showDetail(state.results[idx]); break;
      case 'insert-main':  await insertCitation(state.detail); break;
      case 'insert-ew':    await insertCitation(state.detail, btn.dataset.ew); break;
      case 'back':
        state.view = 'search';
        state.detail = null;
        state.caseBrief = null;
        state.verifyResult = null;
        state.error = null;
        render();
        break;
      case 'load-more':    await loadMore(); break;
      case 'retry':
        state.error = null;
        if (state.view === 'search') doSearch(state.query);
        render();
        break;
      case 'show-laws':
        state.view = 'laws';
        render();
        break;
      case 'quick-search':
        doSearch(btn.dataset.query || '');
        break;
      case 'show-guide':
        state.view = 'guide';
        render();
        break;
      case 'verify-ref':   await startVerify(); break;
      case 'insert-comment': await doInsertComment(); break;
      case 'insert-verdict-text': await doInsertVerdictText(); break;
      case 'verify-fulltext':
        if (state.verifyResult && state.verifyResult._decision) {
          await showDetail(state.verifyResult._decision);
        }
        break;
      case 'upgrade-pro':  await doUpgradePro(); break;
      case 'activate-pro': await doActivatePro(); break;
      case 'remove-pro':
        localStorage.removeItem('ocl_pro_key');
        render();
        break;
      case 'save-key':
        localStorage.setItem('ocl_anthropic_key', document.getElementById('api-key-input').value);
        render();
        break;
      case 'test-key':     await testApiKey(); break;
      case 'delete-key':
        localStorage.removeItem('ocl_anthropic_key');
        render();
        break;
    }
}

// Actions
async function doSearch(query) {
  state.query = query;
  state.offset = 0;
  state.results = [];
  state.loading = true;
  state.error = null;
  render();
  try {
    var data = await searchDecisions(query, { limit: 20, offset: 0 });
    state.results = data.results || [];
    state.total = data.total || 0;
  } catch (e) {
    state.error = e;
  }
  state.loading = false;
  render();
}

async function loadMore() {
  var btn = document.querySelector('[data-action="load-more"]');
  if (btn) { btn.disabled = true; btn.textContent = '...'; }
  state.offset += 20;
  try {
    var data = await searchDecisions(state.query, { limit: 20, offset: state.offset });
    state.results = state.results.concat(data.results || []);
  } catch (e) {
    state.error = e;
    state.offset -= 20;
  }
  render();
}

async function showDetail(decision) {
  state.view = 'detail';
  state.detail = decision;
  state.caseBrief = null;
  state.loading = true;
  render();
  try {
    var id = decision.decision_id || decision.docket_number;
    var results = await Promise.all([
      getDecision(id).catch(function () { return null; }),
      getCaseBrief(id).catch(function () { return null; }),
    ]);
    if (results[0]) state.detail = Object.assign({}, decision, results[0]);
    state.caseBrief = results[1];
  } catch (e) {
    state.error = e;
  }
  state.loading = false;
  render();
}

async function insertCitation(decision, erwaegung) {
  if (!decision) return;
  var text = formatCitation(decision, state.lang, erwaegung);
  try {
    await insertTextAtCursor(text);
    // Brief flash on the button that was clicked
    var active = document.querySelector('[data-action].btn-insert:focus, [data-action].btn-insert:active');
    if (active) {
      var orig = active.textContent;
      active.textContent = '\u2713';
      active.style.background = 'var(--green)';
      setTimeout(function () { active.textContent = orig; active.style.background = ''; }, 800);
    }
  } catch (e) {
    console.error('Insert failed:', e);
  }
}

async function startVerify() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');
  var apiKey = localStorage.getItem('ocl_anthropic_key');

  if (!proKey && !apiKey) {
    state.view = 'settings';
    render();
    return;
  }
  try {
    var selected = await getSelectedText();
    if (!selected || selected.trim().length < 10) {
      state.error = { type: 'no_selection', message: t('no_selection', lang) };
      state.view = 'verify';
      render();
      return;
    }
    state.verifyText = selected;
    state.view = 'verify';
    state.loading = true;
    state.verifyResult = null;
    state.error = null;
    render();

    var result;
    if (proKey) {
      // Pro path: server-side verify (extract citation first)
      var refs = extractCitations(selected);
      var caseRef = refs.length > 0 ? refs[0] : selected.trim().substring(0, 50);
      result = await verifyReferencePro(proKey, selected, caseRef, lang);
    } else {
      // Own API key path: client-side verify
      result = await verifyReference(selected, apiKey, lang);
    }
    state.verifyResult = result;
  } catch (e) {
    if (e.type === 'invalid_license') {
      localStorage.removeItem('ocl_pro_key');
      state.error = { type: 'no_selection', message: t('pro_key_invalid', lang) };
    } else {
      state.error = e;
    }
  }
  state.loading = false;
  render();
}

async function doInsertComment() {
  if (!state.verifyResult) return;
  var v = state.verifyResult;
  var lang = state.lang;
  var verdictLabels = {
    supports: '\u2713 ' + t('verdict_supports', lang),
    partial: '\u26A0 ' + t('verdict_partial', lang),
    contradicts: '\u2717 ' + t('verdict_contradicts', lang),
  };
  var text = (verdictLabels[v.verdict] || v.verdict) + ': ' + (v.explanation || '');
  var inserted = await insertComment(text);
  if (!inserted) {
    await insertTextAtCursor(' [' + text + ']');
  }
}

async function doInsertVerdictText() {
  if (!state.verifyResult) return;
  var v = state.verifyResult;
  var icons = { supports: '\u2713', partial: '\u26A0', contradicts: '\u2717' };
  await insertTextAtCursor(' [' + (icons[v.verdict] || '?') + ' ' + (v.explanation || '') + ']');
}

async function doLawSearch(query) {
  var lang = state.lang;
  var resultsDiv = document.getElementById('law-results');
  if (!resultsDiv) return;
  resultsDiv.textContent = '';
  var skeleton = document.createElement('div');
  skeleton.className = 'skeleton';
  resultsDiv.appendChild(skeleton);
  try {
    var data = await searchLaws(query, { language: lang });
    var items = Array.isArray(data) ? data : (data.results || []);
    resultsDiv.textContent = '';
    if (items.length === 0) {
      resultsDiv.textContent = t('no_laws', lang);
      return;
    }
    items.forEach(function (r) {
      var card = document.createElement('div');
      card.className = 'result-card';
      var title = document.createElement('div');
      title.className = 'result-docket';
      title.textContent = r.article || r.title || '';
      card.appendChild(title);
      var text = document.createElement('div');
      text.className = 'result-regeste';
      var content = r.text || r.content || '';
      text.textContent = content.length > 300 ? content.substring(0, 300) + '\u2026' : content;
      card.appendChild(text);
      resultsDiv.appendChild(card);
    });
  } catch (e) {
    resultsDiv.textContent = t('law_load_error', lang);
  }
}

async function doUpgradePro() {
  try {
    var successUrl = 'https://word.opencaselaw.ch/pro-success.html?session_id={CHECKOUT_SESSION_ID}';
    var cancelUrl = 'https://word.opencaselaw.ch/';
    var data = await createCheckout(successUrl, cancelUrl);
    if (data.checkout_url) {
      window.open(data.checkout_url, '_blank');
    }
  } catch (e) {
    console.error('Checkout error:', e);
  }
}

async function doActivatePro() {
  var keyInput = document.getElementById('pro-key-input');
  if (!keyInput) return;
  var key = keyInput.value.trim();
  if (!key || !key.startsWith('ocl_pro_')) {
    return;
  }
  try {
    var data = await validateLicense(key);
    if (data.valid) {
      localStorage.setItem('ocl_pro_key', key);
      render();
    }
  } catch (e) {
    console.error('License validation error:', e);
  }
}

async function testApiKey() {
  var key = document.getElementById('api-key-input').value;
  if (!key) return;
  var btn = document.querySelector('[data-action="test-key"]');
  if (btn) { btn.disabled = true; btn.textContent = '...'; }
  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 10,
        messages: [{ role: 'user', content: 'Say OK' }],
      }),
    });
    if (btn) {
      btn.textContent = resp.ok ? '\u2713' : '\u2717';
      btn.style.color = resp.ok ? 'var(--green)' : 'var(--red)';
      setTimeout(function () { btn.textContent = t('btn_test', state.lang); btn.style.color = ''; btn.disabled = false; }, 2000);
    }
  } catch (e) {
    if (btn) {
      btn.textContent = '\u2717';
      btn.style.color = 'var(--red)';
      setTimeout(function () { btn.textContent = t('btn_test', state.lang); btn.style.color = ''; btn.disabled = false; }, 2000);
    }
  }
}

// Utilities — HTML entity escaping for XSS prevention
function escHtml(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
