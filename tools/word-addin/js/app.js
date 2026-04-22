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
  loading: false,
  error: null,
  detail: null,
  caseBrief: null,
  verifyResult: null,
  verifyText: '',
  courts: [],
  lawResult: null,
  supportText: '',
  supportResult: null,
  relatedRef: '',
  relatedResult: null,
  consent: false,
  previousView: null,
  recentLookups: [],
  scanResults: null,
  scanDocText: '',
};

// SVG icon library — single source of truth for chrome-free, line-icon glyphs.
// Each function returns a string (escape-safe; no user input concatenated).
var ICONS = {
  lock: '<svg class="icon" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg>',
  shieldCheck: '<svg class="icon icon-lg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="M9 12l2 2 4-4"/></svg>',
  link: '<svg class="icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1.5 1.5"/><path d="M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1.5-1.5"/></svg>',
  insert: '<svg class="icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"/><path d="M5 12h14"/></svg>',
  chevron: '<svg class="icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>',
  check: '<svg class="icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>',
};
function proLockSvg() { return localStorage.getItem('ocl_pro_key') ? '' : '<span class="pro-lock" title="Pro" aria-label="Pro">' + ICONS.lock + '</span>'; }

// Pattern to detect law article queries like "Art. 41 OR", "Art. 8 BV", "OR 41", "ZGB 8"
var LAW_ABBREVS = 'OR|ZGB|StGB|BV|StPO|ZPO|SchKG|AHVG|AIG|AHVV|KVV|BGG|VwVG|ATSG|UVG|BVG|UWG|KG|MSchG|URG|PatG|DSG|FINMAG|GwG|BankG|FusG|IPRG|Lug\u00DC|KKG|CO|CC|CP|Cst\\.?|Cost\\.?|CPP|CPC|LP|LTF|LCD|LEtr|LDIP|LFus|LCart|LDA|LPM|LBI|LPD|LAJ|LAI|LATF|LPA|LPGA|LAA|LPP';
// Matches: "Art. 41 OR", "Art 8 BV", "OR 41", "BV 8", "StGB 261bis", "art. 41 CO", "Cst. 8", "art. 2 al. 1 CC"
var LAW_PATTERN = new RegExp('(?:[Aa]rt\\.?\\s*)(\\d+[a-z]?(?:bis|ter|quater)?(?:\\s*(?:Abs|al|cpv|let|ch|lit)\\.?\\s*\\d*[a-z]?)?)\\s+(' + LAW_ABBREVS + ')', 'i');
var LAW_PATTERN_REVERSE = new RegExp('^(' + LAW_ABBREVS.replace(/\\\.\?/g, '\\.?') + ')\\s+(\\d+[a-z]?(?:bis|ter|quater)?)$', 'i');

// French/Italian abbreviation → German canonical form for API lookup
var LAW_ALIAS = {
  'co': 'OR', 'cc': 'ZGB', 'cp': 'StGB', 'cst': 'BV', 'cst.': 'BV', 'cost': 'BV', 'cost.': 'BV',
  'cpp': 'StPO', 'cpc': 'ZPO', 'lp': 'SchKG', 'ltf': 'BGG', 'lcd': 'UWG', 'lcart': 'KG',
  'letr': 'AIG', 'ldip': 'IPRG', 'lfus': 'FusG',
  'lda': 'URG', 'lpm': 'MSchG', 'lbi': 'PatG', 'lpd': 'DSG',
  'laj': 'AHVG', 'lai': 'AIG', 'latf': 'BGG', 'lpa': 'VwVG', 'lpga': 'ATSG',
  'laa': 'UVG', 'lpp': 'BVG'
};

// Track whether we're running inside Word or standalone browser
var _insideWord = false;
// Debounce timer for live search-as-you-type.
var _liveSearchTimer = null;

// True for inputs that should require Enter (reference / law / docket lookups).
function _isReferencePattern(s) {
  if (LAW_PATTERN.test(s)) return true;
  if (LAW_PATTERN_REVERSE.test(s.trim())) return true;
  if (/^(BGE|ATF|DTF)\s+\d/i.test(s)) return true;
  if (/^\d[A-Z]_\d+\/\d{4}$/.test(s)) return true;
  if (/^[A-Z]-\d+\/\d{4}$/.test(s)) return true;
  return false;
}

// Initialize — works both inside Word (Office.onReady) and in plain browser
function initApp() {
  document.getElementById('lang-select').addEventListener('change', function (e) {
    state.lang = e.target.value;
    // Update gear button title
    document.getElementById('btn-settings').title = t('settings_gear_title', state.lang);
    document.getElementById('btn-settings').setAttribute('aria-label', t('settings_gear_title', state.lang));
    var brandSub = document.getElementById('brand-sub');
    if (brandSub) brandSub.textContent = t('brand_sub', state.lang);
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
    state.view = (state.view === 'settings') ? 'search' : 'settings';
    render();
  });

  // Load saved language
  if (!_insideWord) {
    try { state.lang = localStorage.getItem('ocl_lang') || 'de'; } catch (e) {}
  }
  document.getElementById('lang-select').value = state.lang;
  var brandSub = document.getElementById('brand-sub');
  if (brandSub) brandSub.textContent = t('brand_sub', state.lang);

  // Bind delegated click handler ONCE (never re-bind)
  document.getElementById('app').addEventListener('click', handleAppClick);

  // Load recent lookups
  try { state.recentLookups = JSON.parse(localStorage.getItem('ocl_recent') || '[]'); } catch (e) { state.recentLookups = []; }

  // Check consent, then render
  try { state.consent = !!localStorage.getItem('ocl_consent'); } catch (e) {}
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

// Which tab should appear active for the current view? Sub-views (detail,
// verify, support, related) inherit the tab they were launched from.
function _activeTab() {
  if (state.view === 'scan') return 'audit';
  if (state.view === 'verify' || state.view === 'support' || state.view === 'related') {
    return state.previousView === 'scan' ? 'audit' : 'search';
  }
  return 'search';
}

function renderTabBar() {
  var lang = state.lang;
  var active = _activeTab();
  return '<div class="tab-bar">' +
    '<button class="tab' + (active === 'search' ? ' tab-active' : '') + '" data-action="switch-tab" data-tab="search">' +
    escHtml(t('tab_search', lang)) + '</button>' +
    '<button class="tab' + (active === 'audit' ? ' tab-active' : '') + '" data-action="switch-tab" data-tab="audit">' +
    escHtml(t('tab_audit', lang)) +
    proLockSvg() +
    '</button>' +
    '</div>';
}

// Rendering — all values passed through escHtml() for XSS safety
// Safe: all dynamic content is escaped via escHtml before concatenation
function render() {
  var app = document.getElementById('app');
  if (!state.consent) { app.innerHTML = renderConsent(); bindEvents(); return; }

  // Tab bar mounts above the main view; sub-views (detail/verify/support/
  // related) get a back-link instead, so we hide tabs there.
  var showTabs = (state.view === 'search' || state.view === 'scan');
  var html = showTabs ? renderTabBar() : '';
  var view = state.view;
  if (view === 'search') html += renderSearch();
  else if (view === 'detail') html += renderDetail();
  else if (view === 'verify') html += renderVerify();
  else if (view === 'support') html += renderSupport();
  else if (view === 'related') html += renderRelated();
  else if (view === 'scan') html += renderScan();
  else if (view === 'guide') html += renderGuide();
  else if (view === 'settings') html += renderSettings();

  app.innerHTML = html; // eslint-disable-line no-unsanitized/property -- all values pre-escaped via escHtml()
  bindEvents();
  if (state.lawResult) resolveAmendmentRefs();
}

// Format law article text into styled paragraphs with superscript numbers.
// Each paragraph carries TWO insert affordances:
//   - the small superscript number → inserts the reference only
//   - a hover-revealed "Wortlaut" button → inserts verbatim paragraph + ref
function formatLawParagraphs(text, artNum, abbrev) {
  if (!text) return '';
  var lines = text.split('\n');
  var html = '';
  var lang = state.lang;
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (!line) continue;
    var m = line.match(/^(\d+[a-z]?(?:bis|ter|quater)?)\s+(.*)$/);
    if (m) {
      var paraRef = 'Art. ' + artNum + ' Abs. ' + m[1] + ' ' + abbrev;
      var paraBody = formatLawFootnotes(m[2]);
      // Strip footnote markers from the verbatim text we insert (no inline HTML).
      var verbatim = m[2].replace(/\s*(Fassung gemäss|Eingef\u00fcgt durch|Aufgehoben durch|Berichtigung der|Bereinigt gemäss|Strafdrohungen?\s+gem\u00e4ss|SR\s+\d+[\.\d]*).*$/i, '').trim();
      html += '<div class="law-para">' +
        '<span class="law-para-num" data-action="insert-law-para" data-ref="' + escHtml(paraRef) + '" title="' + escHtml(t('btn_insert_ref', lang)) + ': ' + escHtml(paraRef) + '">' + escHtml(m[1]) + '</span> ' +
        paraBody +
        ' <button class="law-para-verbatim" data-action="insert-law-verbatim" data-ref="' + escHtml(paraRef) + '" data-text="' + escHtml(verbatim) + '" title="' + escHtml(t('btn_insert_verbatim', lang)) + '" aria-label="' + escHtml(t('btn_insert_verbatim', lang)) + '">' + ICONS.insert + '</button>' +
        '</div>';
    } else {
      // Check if entire line is a footnote/amendment note
      if (/^(Fassung|Eingef|Aufgehoben|Strafdrohung|Berichtigung|Version|Introdotto|Abrogato|Nuovo|Introd)/i.test(line)) {
        html += '<div class="law-footnote">' + escHtml(line) + '</div>';
      } else {
        html += '<div class="law-para">' + formatLawFootnotes(line) + '</div>';
      }
    }
  }
  return html;
}

function formatLawFootnotes(text) {
  // Split inline footnotes (e.g. "Fassung gemäss...", "SR 281.1") from the main text
  // Pattern: text ending with a period, followed by footnote-style content
  var footnotePatterns = [
    /(\.\s*)(Fassung gem\u00e4ss.+)$/,
    /(\.\s*)(Strafdrohungen?.+gem\u00e4ss.+)$/,
    /(\.\s*)(Eingef\u00fcgt durch.+)$/,
    /(\.\s*)(Aufgehoben durch.+)$/,
    /(\.\s*)(Berichtigung der.+)$/,
    /(\.\s*)(Bereinigt gem\u00e4ss.+)$/,
    /(\.\s*)(SR \d+[\.\d]*)$/,
  ];
  for (var i = 0; i < footnotePatterns.length; i++) {
    var fm = text.match(footnotePatterns[i]);
    if (fm) {
      return escHtml(text.substring(0, text.length - fm[2].length)) +
        '<span class="law-footnote-inline">' + escHtml(fm[2]) + '</span>';
    }
  }
  return escHtml(text);
}

// Async: resolve AS/BBl refs in footnotes to clickable Fedlex links (called after render)
function resolveAmendmentRefs() {
  var footnotes = document.querySelectorAll('.law-footnote, .law-footnote-inline');
  footnotes.forEach(function (el) {
    var text = el.textContent;
    var refs = text.match(/(AS|BBl|RO|RU|FF)\s+(\d{4})\s+(\d+)/g);
    if (!refs) return;
    refs.forEach(function (ref) {
      var m = ref.match(/(AS|BBl|RO|RU|FF)\s+(\d{4})\s+(\d+)/);
      if (!m) return;
      apiFetch('/amendment-ref', { ref_type: m[1], year: m[2], page: m[3] }).then(function (data) {
        if (data && data.url && data.url.indexOf('https://') === 0) {
          var link = '<a href="' + escHtml(data.url) + '" target="_blank" class="law-ref-link">' + escHtml(ref) + '</a>';
          el.innerHTML = el.innerHTML.replace(escHtml(ref), link); // eslint-disable-line no-unsanitized/property
        }
      }).catch(function () {});
    });
  });
}

function renderConsent() {
  var lang = state.lang;
  return '<div class="consent">' +
    '<div class="consent-title">' + escHtml(t('consent_title', lang)) + '</div>' +
    '<div class="consent-text">' + escHtml(t('consent_text', lang)) + '</div>' +
    '<div class="consent-links">' +
    '<a href="https://word.opencaselaw.ch/terms.html" target="_blank">' + escHtml(t('consent_terms', lang)) + '</a>' +
    '<span class="pro-sep">\u00B7</span>' +
    '<a href="https://word.opencaselaw.ch/privacy.html" target="_blank">' + escHtml(t('consent_privacy', lang)) + '</a>' +
    '</div>' +
    '<button class="btn btn-full" data-action="accept-consent">' + escHtml(t('consent_accept', lang)) + '</button>' +
    '</div>';
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
    (state.query ? '<button class="search-clear" data-action="clear-search" aria-label="Clear">\u00D7</button>' : '') +
    '</div>';

  // Show law article card if a law result exists
  if (state.lawResult && state.lawResult.articles && state.lawResult.articles.length > 0) {
    var lawArt = state.lawResult.articles[0];
    var abbrev = state.lawResult.abbreviation || '';
    var artNum = lawArt.article_num || lawArt.article || '';
    var heading = lawArt.heading ? ' \u2014 ' + lawArt.heading : '';
    var lawTitle = 'Art. ' + artNum + ' ' + abbrev + heading;
    var lawSubtitle = state.lawResult.title || '';
    var lawText = lawArt.text || lawArt.content || '';
    // Format paragraphs: split on lines starting with a number
    var lawParas = formatLawParagraphs(lawText, artNum, abbrev);
    html += '<div class="law-card">' +
      '<div class="law-card-header"><div>' +
      '<div class="law-card-title">' + escHtml(lawTitle) + '</div>' +
      '<div class="law-card-subtitle">' + escHtml(lawSubtitle) + '</div>' +
      '</div></div>' +
      '<div class="law-paras">' + lawParas + '</div>' +
      '<div class="law-card-actions">' +
      '<button class="btn btn-insert" data-action="insert-law-ref">' + escHtml(t('btn_insert_ref', lang)) + '</button>' +
      '</div></div>';
  }

  if (state.loading) {
    html += '<div class="loading-spinner"><div class="spinner"></div><span>' + escHtml(t('loading', lang)) + '</span></div>';
  } else if (state.error) {
    html += renderError();
  } else if (state.results.length === 0 && state.query && !state.lawResult) {
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
    html += '</div>';
  }
  return html;
}

function renderWelcome(lang) {
  var html = '<div class="welcome">';

  // First-run hint — single dismissible banner the very first time a user
  // sees the welcome screen. Dismissed forever via localStorage flag.
  var tourSeen = false;
  try { tourSeen = localStorage.getItem('ocl_hint_seen') === '1'; } catch (e) {}
  if (!tourSeen && state.recentLookups.length === 0) {
    html += '<div class="welcome-hint">' +
      '<div class="welcome-hint-text">' + escHtml(t('hint_welcome', lang)) + '</div>' +
      '<button class="btn btn-detail welcome-hint-dismiss" data-action="dismiss-hint">' +
      escHtml(t('hint_dismiss', lang)) + '</button>' +
      '</div>';
  }

  // Recent lookups (when user has history) — surfaces the user's own context first.
  if (state.recentLookups.length > 0) {
    html += '<div class="quick-label">' + escHtml(t('recent_label', lang)) + '</div>';
    html += '<div class="quick-chips">';
    for (var r = 0; r < state.recentLookups.length; r++) {
      html += '<button class="quick-chip" data-action="quick-search" data-query="' + escHtml(state.recentLookups[r]) + '">' + escHtml(state.recentLookups[r]) + '</button>';
    }
    html += '</div>';
  }

  // Quick search suggestions
  html += '<div class="quick-label">' + escHtml(t('quick_try', lang)) + '</div>';
  html += '<div class="quick-chips">';
  var examples = t('quick_examples', lang).split('|');
  for (var i = 0; i < examples.length; i++) {
    html += '<button class="quick-chip" data-action="quick-search" data-query="' + escHtml(examples[i]) + '">' + escHtml(examples[i]) + '</button>';
  }
  html += '</div>';

  // Footer — coverage stats + how-it-works link, kept whisper-quiet.
  html += '<div class="welcome-footer">' +
    '<a class="welcome-link" data-action="show-guide">' + escHtml(t('how_it_works', lang)) + '</a>' +
    '<span class="welcome-count-small">' + escHtml(t('welcome_count', lang, { n: '967\u2009000+' })) + '</span>' +
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

  // Sentence-aware truncation: prefer cutting at ". " over mid-word.
  var regeste = r.regeste || stripHtml(r.snippet) || r.title || '';
  regeste = _truncateSentence(regeste, 200);
  var courtName = r.court ? getCourtName(r.court, lang) : (r.court_name || '');
  var date = r.decision_date || r.date || '';
  var docket = r.citation_string_de || r.docket_number || r.decision_id;

  return '<div class="result-card" data-action="detail" data-idx="' + idx + '" tabindex="0" role="button">' +
    '<div class="result-header"><div>' +
    '<div class="result-docket">' + escHtml(docket) + '</div>' +
    '<div class="result-meta">' + escHtml(date) + ' \u00B7 ' + escHtml(courtName) + '</div>' +
    '</div><div>' + badges + '</div></div>' +
    (regeste ? '<div class="result-regeste">' + escHtml(regeste) + '</div>' : '') +
    '<div class="result-actions">' +
    '<button class="btn btn-insert btn-insert-card" data-action="insert" data-idx="' + idx + '" title="' + escHtml(t('btn_insert', lang)) + '">' +
    ICONS.insert + ' ' + escHtml(t('btn_insert', lang)) + '</button>' +
    '</div></div>';
}

// Truncate at the last sentence boundary within `max` chars; fall back to
// last word boundary; only resort to mid-word cut if no whitespace at all.
function _truncateSentence(s, max) {
  s = String(s || '').replace(/\s+/g, ' ').trim();
  if (s.length <= max) return s;
  var window = s.substring(0, max);
  var lastSentence = Math.max(window.lastIndexOf('. '), window.lastIndexOf('? '), window.lastIndexOf('! '));
  if (lastSentence > max * 0.5) return s.substring(0, lastSentence + 1).trim();
  var lastSpace = window.lastIndexOf(' ');
  if (lastSpace > max * 0.6) return s.substring(0, lastSpace).trim() + '\u2026';
  return window + '\u2026';
}

/**
 * Split an Erwägung into sub-sections based on numbering patterns (3.1, 3.2, 3.2.1).
 * Uses RegExp.prototype to find sub-section markers in the text.
 */
function splitErwaegung(mainNum, text) {
  if (!text) return [{ num: mainNum, text: '' }];
  var escaped = mainNum.replace(/\./g, '\\.');
  var re = new RegExp('(?:\\. |\\n)(' + escaped + '(?:\\.\\d+)+)\\s', 'g');
  var parts = [];
  var lastIdx = 0;
  var m;
  while ((m = re.exec(text)) !== null) {
    var prevText = text.substring(lastIdx, m.index).trim();
    if (parts.length === 0 && prevText) {
      parts.push({ num: mainNum, text: prevText });
    } else if (prevText && parts.length > 0) {
      parts[parts.length - 1].text += ' ' + prevText;
    }
    lastIdx = m.index + m[0].length;
    parts.push({ num: m[1], text: '' });
  }
  var remaining = text.substring(lastIdx).trim();
  if (parts.length === 0) {
    return [{ num: mainNum, text: text }];
  }
  if (remaining) {
    parts[parts.length - 1].text = remaining;
  }
  if (parts.every(function (p) { return !p.text; })) {
    return [{ num: mainNum, text: text }];
  }
  return parts;
}

// Detail View
function renderDetail() {
  var lang = state.lang;
  var d = state.detail;
  var cb = state.caseBrief;

  if (!d) return renderDetailSkeleton();

  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', lang)) + '</a>';

  // Sticky insert bar — primary "Insert" + secondary "Insert with link"
  var citationText = formatCitation(d, lang);
  html += '<div class="sticky-insert">' +
    '<span class="sticky-citation">' + escHtml(citationText) + '</span>' +
    '<div class="sticky-insert-actions">' +
    '<button class="btn btn-insert" data-action="insert-main" title="' + escHtml(t('btn_insert', lang)) + '">' +
    ICONS.insert + escHtml(t('btn_insert', lang)) + '</button>' +
    '<button class="btn btn-insert btn-icon-only" data-action="insert-main-link" title="' + escHtml(t('btn_insert_link', lang)) + '" aria-label="' + escHtml(t('btn_insert_link', lang)) + '">' +
    ICONS.link + '</button>' +
    '</div></div>';

  html += '<h2 class="detail-title">' + escHtml(d.docket_number || d.decision_id) + '</h2>';
  if (d.title) html += '<div style="font-size:12px;color:var(--text-secondary);margin-top:2px;">' + escHtml(d.title) + '</div>';
  var courtName = d.court ? getCourtName(d.court, lang) : (d.court_name || '');
  html += '<div class="detail-meta">' + escHtml(d.decision_date || d.date || '') + ' \u00B7 ' + escHtml(courtName) + '</div>';

  var badges = '';
  if (d.is_leading_case) badges += '<span class="badge badge-leading">\u2605 ' + escHtml(t('badge_leading', lang)) + '</span> ';
  if (d.citation_count) badges += '<span class="badge badge-citations">' + escHtml(String(d.citation_count)) + ' ' + escHtml(t('citations_label', lang)) + '</span> ';
  if (d.legal_area) badges += '<span class="badge badge-area">' + escHtml(d.legal_area) + '</span>';
  if (badges) html += '<div class="badges-row">' + badges + '</div>';

  if (d.source_url) {
    html += '<a class="source-link" href="' + escHtml(d.source_url) + '" target="_blank">' + escHtml(t('source_link', lang)) + ' \u2192</a>';
  }

  // ── Regeste ──
  if (d.regeste) {
    var needsCollapse = d.regeste.length > 300;
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_regeste', lang)) + '</div>' +
      '<div class="section-body' + (needsCollapse ? ' collapsible' : '') + '" id="regeste-body">' + escHtml(d.regeste) + '</div>' +
      (needsCollapse ? '<button class="expand-btn" data-action="toggle-expand" data-target="regeste-body">' + escHtml(t('btn_show_more', lang)) + '</button>' : '') +
      '</div>';
  }

  // ── Sachverhalt (from case brief) ──
  if (cb && cb.sachverhalt) {
    var svCollapse = cb.sachverhalt.length > 200;
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_sachverhalt', lang)) + '</div>' +
      '<div class="section-body' + (svCollapse ? ' collapsible' : '') + '" id="sv-body">' + escHtml(cb.sachverhalt) + '</div>' +
      (svCollapse ? '<button class="expand-btn" data-action="toggle-expand" data-target="sv-body">' + escHtml(t('btn_show_more', lang)) + '</button>' : '') +
      '</div>';
  }

  // ── Erwägungen (accordion with sub-sections) ──
  var ewList = cb && (cb.key_erwaegungen || cb.erwaegungen);
  if (ewList && ewList.length) {
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_erwaegungen', lang)) + '</div>';
    for (var i = 0; i < ewList.length; i++) {
      var e = ewList[i];
      var num = e.number || '?';
      var ewText = e.text || '';
      var subSections = splitErwaegung(num, ewText);
      html += '<div class="erwaegung-block">';
      for (var si = 0; si < subSections.length; si++) {
        var sub = subSections[si];
        var subNeedsCollapse = sub.text.length > 200;
        var subId = 'ew-' + i + '-' + si;
        var isMain = (si === 0);
        html += '<div class="ew-sub' + (isMain ? ' ew-main' : '') + '">' +
          '<div class="erwaegung-header">' +
          '<span class="erwaegung-num' + (isMain ? '' : ' ew-sub-num') + '">E. ' + escHtml(sub.num) + '</span>' +
          '<button class="btn btn-insert erwaegung-insert" data-action="insert-ew" data-ew="' + escHtml(sub.num) + '" title="' + escHtml(formatCitation(d, lang, sub.num)) + '">E. ' + escHtml(sub.num) + '</button>' +
          '</div>' +
          '<div class="erwaegung-body' + (subNeedsCollapse ? ' collapsible' : '') + '" id="' + subId + '">' + escHtml(sub.text) + '</div>' +
          (subNeedsCollapse ? '<button class="expand-btn" data-action="toggle-expand" data-target="' + subId + '">' + escHtml(t('btn_show_more', lang)) + '</button>' : '') +
          '</div>';
      }
      html += '</div>';
    }
    html += '</div>';
  } else if (state.loading) {
    html += '<div class="section-card">' + renderSkeletonLines(4) + '</div>';
  }

  // ── Dispositiv (from case brief) ──
  if (cb && cb.dispositiv) {
    var dpCollapse = cb.dispositiv.length > 200;
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_dispositiv', lang)) + '</div>' +
      '<div class="section-body' + (dpCollapse ? ' collapsible' : '') + '" id="dp-body">' + escHtml(cb.dispositiv) + '</div>' +
      (dpCollapse ? '<button class="expand-btn" data-action="toggle-expand" data-target="dp-body">' + escHtml(t('btn_show_more', lang)) + '</button>' : '') +
      '</div>';
  }

  // ── Statutes ──
  if (cb && cb.statutes && cb.statutes.length) {
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_statutes', lang)) + '</div><div class="pills">';
    for (var j = 0; j < cb.statutes.length; j++) {
      var st = cb.statutes[j];
      var stLabel = typeof st === 'string' ? st : ('Art. ' + (st.article || '?') + ' ' + (st.law_code || ''));
      html += '<span class="pill pill-static">' + escHtml(stLabel) + '</span>';
    }
    html += '</div></div>';
  }

  // ── Full text: fallback when no Erwägungen were extracted ──
  var hasErwaegungen = ewList && ewList.length;
  if (d.full_text && !hasErwaegungen) {
    // No structured sections — show truncated full text as fallback
    var ftLen = d.full_text.length;
    var ftCollapse = ftLen > 3000;
    var ftDisplay = ftCollapse ? d.full_text.substring(0, 3000) : d.full_text;
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_fulltext', lang)) + '</div>' +
      '<div class="fulltext-body collapsible" id="fulltext-body">' + escHtml(ftDisplay) + '</div>';
    if (ftCollapse) {
      html += '<button class="expand-btn" data-action="expand-fulltext">' + escHtml(t('btn_show_more', lang)) + '</button>';
    }
    html += '</div>';
  }

  // Link to full text on opencaselaw.ch
  var entscheidUrl = 'https://mcp.opencaselaw.ch/entscheid/' + encodeURIComponent(d.decision_id || '');
  html += '<a class="fulltext-link" data-action="open-external" data-url="' + escHtml(entscheidUrl) + '">' +
    escHtml(t('fulltext_link', lang)) + ' \u2192</a>';

  return html;
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
      html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('relevant_ew', lang)) +
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

// Support View
function renderSupport() {
  var lang = state.lang;
  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', lang)) + '</a>';
  html += '<h3 class="verify-heading">' + escHtml(t('tool_support', lang)) + '</h3>';

  if (state.supportText) {
    html += '<div class="verify-selected"><div class="section-label">' + escHtml(t('support_your_statement', lang)) + '</div>' +
      '<div class="section-body">' + escHtml(state.supportText) + '</div></div>';
  }

  if (state.loading) {
    html += '<div class="verify-loading"><div class="spinner"></div><span>' + escHtml(t('support_searching', lang)) + '</span></div>';
  } else if (state.supportResult) {
    var sr = state.supportResult;

    // Show parsed claim + legal area
    if (sr.claim || sr.legal_area) {
      html += '<div class="support-meta">';
      if (sr.legal_area) html += '<span class="badge badge-area">' + escHtml(sr.legal_area) + '</span> ';
      if (sr.statutes) {
        for (var s = 0; s < sr.statutes.length && s < 3; s++) {
          html += '<span class="pill" style="margin:2px;">' + escHtml(sr.statutes[s]) + '</span>';
        }
      }
      html += '</div>';
    }

    // Results
    var results = sr.results || [];
    if (results.length === 0) {
      html += '<div class="state-message">' + escHtml(t('support_no_results', lang)) + '</div>';
    } else {
      html += '<div class="results">';
      html += '<div class="results-count">' + escHtml(t('support_found', lang, { n: results.length })) + '</div>';
      for (var i = 0; i < results.length; i++) {
        html += renderSupportCard(results[i], i);
      }
      html += '</div>';
    }

    html += '<div class="verdict-footer">' + escHtml(t('verify_footer_pro', lang)) + '</div>';
  } else if (state.error) {
    html += renderError();
  }
  return html;
}

function renderSupportCard(r, idx) {
  var lang = state.lang;
  var docket = r.docket_number || r.decision_id || '';
  var courtName = r.court ? getCourtName(r.court, lang) : (r.court_name || '');
  var date = r.decision_date || r.date || '';
  var relevance = r._relevance || 0;
  var supports = r._supports;
  var explanation = r._explanation || '';
  var keyPassage = r._key_passage || '';
  var regeste = r.regeste || stripHtml(r.snippet) || r.title || '';
  if (regeste.length > 150) regeste = regeste.substring(0, 150) + '\u2026';

  var supportClass = supports ? 'support-yes' : 'support-maybe';

  var html = '<div class="result-card ' + supportClass + '">' +
    '<div class="result-header"><div>' +
    '<div class="result-docket">' + escHtml(docket) + '</div>' +
    '<div class="result-meta">' + escHtml(date) + ' \u00B7 ' + escHtml(courtName) + '</div>' +
    '</div>';

  // Relevance indicator
  if (relevance > 0) {
    html += '<div class="relevance-bar" title="' + relevance + '% relevant">' +
      '<div class="relevance-fill" style="width:' + Math.min(relevance, 100) + '%;"></div></div>';
  }
  html += '</div>';

  // AI explanation
  if (explanation) {
    html += '<div class="support-explanation">' + escHtml(explanation) + '</div>';
  }

  // Key passage quote
  if (keyPassage) {
    html += '<div class="support-quote">\u00AB' + escHtml(keyPassage) + '\u00BB</div>';
  }

  // Regeste fallback
  if (!explanation && regeste) {
    html += '<div class="result-regeste">' + escHtml(regeste) + '</div>';
  }

  html += '<div class="result-actions">' +
    '<button class="btn btn-insert" data-action="insert-support" data-idx="' + idx + '">' + escHtml(t('btn_insert', lang)) + '</button>' +
    '<button class="btn btn-detail" data-action="detail-support" data-idx="' + idx + '">' + escHtml(t('btn_fulltext', lang)) + '</button>' +
    '</div></div>';
  return html;
}

// Related View
function renderRelated() {
  var lang = state.lang;
  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', lang)) + '</a>';
  html += '<h3 class="verify-heading">' + escHtml(t('btn_find_related', lang)) + '</h3>';

  if (state.relatedRef) {
    html += '<div class="verify-selected"><div class="detail-label">' + escHtml(t('related_for', lang)) + '</div>' +
      '<div class="section-body" style="font-weight:600;">' + escHtml(state.relatedRef) + '</div></div>';
  }

  if (state.loading) {
    html += '<div class="verify-loading"><div class="spinner"></div><span>' + escHtml(t('loading', lang)) + '</span></div>';
  } else if (state.relatedResult) {
    var r = state.relatedResult;
    var incoming = r.incoming || [];
    var outgoing = r.outgoing || [];

    // Put all related results into state.results so insert/detail handlers work
    state.results = incoming.concat(outgoing);

    if (incoming.length > 0) {
      html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('related_cited_by', lang)) + '</div>';
      for (var i = 0; i < incoming.length; i++) {
        html += renderResultCard(incoming[i], i);
      }
      html += '</div>';
    }

    if (outgoing.length > 0) {
      html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('related_cites', lang)) + '</div>';
      for (var j = 0; j < outgoing.length; j++) {
        html += renderResultCard(outgoing[j], incoming.length + j);
      }
      html += '</div>';
    }

    if (incoming.length === 0 && outgoing.length === 0) {
      html += '<div class="state-message">' + escHtml(t('related_none', lang)) + '</div>';
    }

    html += '<div class="verdict-footer">' + escHtml(t('verify_footer_pro', lang)) + '</div>';
  } else if (state.error) {
    html += renderError();
  }
  return html;
}

// Settings View
function renderSettings() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key') || '';

  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back_short', lang)) + '</a>';
  html += '<h3 class="settings-heading">' + escHtml(t('settings_title', lang)) + '</h3>';

  // Pro subscription section
  html += '<div class="section-card">';
  if (proKey) {
    // Pro active
    html += '<div class="pro-badge-row"><span class="pro-badge">PRO</span> ' + escHtml(t('pro_active', lang)) + '</div>' +
      '<div class="settings-field"><label class="settings-label">' + escHtml(t('pro_license_key', lang)) + '</label>' +
      '<input type="text" class="settings-input" id="pro-key-input" value="' + escHtml(proKey) + '" readonly style="font-size:11px;color:var(--text-secondary);">' +
      '</div>' +
      '<div class="settings-actions">' +
      '<button class="btn btn-detail" data-action="manage-subscription">' + escHtml(t('btn_manage_sub', lang)) + '</button>' +
      '<button class="btn btn-detail" data-action="remove-pro" style="color:var(--red);">' + escHtml(t('btn_remove_license', lang)) + '</button>' +
      '</div>';
  } else {
    // Upgrade CTA — hero pitch on top, features as support, pricing on the button.
    html += '<div class="pro-hero">' +
      '<div class="pro-hero-icon">' + ICONS.shieldCheck + '</div>' +
      '<div class="pro-hero-title">' + escHtml(t('pro_hero_title', lang)) + '</div>' +
      '<div class="pro-hero-sub">' + escHtml(t('pro_hero_sub', lang)) + '</div>' +
      '</div>' +
      '<ul class="pro-features">' +
      '<li class="pro-feature"><span class="pro-feature-check">' + ICONS.check + '</span>' + escHtml(t('pro_feature_verify_new', lang)) + '</li>' +
      '<li class="pro-feature"><span class="pro-feature-check">' + ICONS.check + '</span>' + escHtml(t('pro_feature_support', lang)) + '</li>' +
      '<li class="pro-feature"><span class="pro-feature-check">' + ICONS.check + '</span>' + escHtml(t('pro_feature_related', lang)) + '</li>' +
      '<li class="pro-feature"><span class="pro-feature-check">' + ICONS.check + '</span>' + escHtml(t('pro_feature_limit', lang)) + '</li>' +
      '</ul>' +
      '<div class="pro-consent">' +
      '<label class="pro-consent-label"><input type="checkbox" id="pro-consent-check" class="pro-consent-check"> ' +
      escHtml(t('pro_consent_text', lang)) + ' ' +
      '<a href="https://word.opencaselaw.ch/terms.html" target="_blank">' + escHtml(t('consent_terms', lang)) + '</a> ' +
      escHtml(t('pro_consent_and', lang)) + ' ' +
      '<a href="https://word.opencaselaw.ch/privacy.html" target="_blank">' + escHtml(t('consent_privacy', lang)) + '</a>.' +
      '</label></div>' +
      '<button class="btn btn-pro" data-action="upgrade-pro" id="btn-upgrade-pro" disabled>' +
      escHtml(t('btn_upgrade_price', lang)) + '</button>' +
      '<div class="pro-cancel-note">' + escHtml(t('pro_cancel_note', lang)) + '</div>' +
      '<div class="pro-divider"><span>' + escHtml(t('pro_or_key', lang)) + '</span></div>' +
      '<div class="settings-field"><label class="settings-label">' + escHtml(t('pro_license_key', lang)) + '</label>' +
      '<input type="text" class="settings-input" id="pro-key-input" value="" placeholder="ocl_pro_...">' +
      '</div>' +
      '<div class="settings-actions">' +
      '<button class="btn btn-detail" data-action="activate-pro">' + escHtml(t('btn_activate', lang)) + '</button>' +
      '</div>';
  }
  html += '</div>';

  // Privacy / telemetry section — opt-out toggle for the install cohort header
  var optedOut = localStorage.getItem('ocl_usage_signal_optout') === '1';
  html += '<div class="section-card" style="margin-top:12px;">' +
    '<div class="section-label">' + escHtml(t('priv_signal_title', lang)) + '</div>' +
    '<label class="pro-consent-label" style="align-items:flex-start; gap:8px;">' +
    '<input type="checkbox" class="pro-consent-check" id="priv-signal-toggle"' +
    (optedOut ? '' : ' checked') + '> ' +
    '<span>' + escHtml(t('priv_signal_body', lang)) + '</span>' +
    '</label>' +
    '<div style="font-size:11px; color:var(--text-secondary); margin-top:8px; line-height:1.5;">' +
    escHtml(t('priv_signal_note', lang)) + ' ' +
    '<a href="https://opencaselaw.ch/datenschutz/" target="_blank" style="color:var(--blue);">' + escHtml(t('priv_signal_more', lang)) + '</a>' +
    '</div>' +
    '</div>';

  html += '<div class="settings-footer">' +
    '<a href="https://opencaselaw.ch" target="_blank" style="color:var(--blue);">opencaselaw.ch</a> \u00B7 ' +
    '<a href="https://opencaselaw.ch/datenschutz/" target="_blank" style="color:var(--blue);">' + escHtml(t('priv_signal_privacy_link', lang)) + '</a> \u00B7 ' +
    '<a href="https://github.com/jonashertner/caselaw-repo-1" target="_blank" style="color:var(--blue);">GitHub</a><br>' +
    'Code: MIT \u00B7 Daten: CC0 1.0</div>';

  return html;
}

// Error rendering
function renderError() {
  var lang = state.lang;
  if (!state.error) return '';
  if (state.error.type === 'daily_limit') {
    return '<div class="state-message">' + escHtml(t('error_daily_limit', lang)) + '</div>';
  }
  if (state.error.type === 'rate_limit') {
    return '<div class="state-message">' + escHtml(t('error_rate_limit', lang)) + '<br>' +
      escHtml(t('error_rate_wait', lang, { n: state.error.retryAfter })) +
      '<button class="retry-btn" data-action="retry">' + escHtml(t('btn_retry', lang)) + '</button></div>';
  }
  if (state.error.type === 'use_search') {
    return '<div class="state-message">' + escHtml(state.error.message) + '<br><a href="https://opencaselaw.ch" target="_blank" style="color:var(--text);font-weight:600;">opencaselaw.ch</a></div>';
  }
  if (state.error.type === 'not_found' || state.error.type === 'no_citation' || state.error.type === 'no_selection' || state.error.type === 'decision_not_found') {
    return '<div class="state-message">' + escHtml(state.error.message) + '</div>';
  }
  return '<div class="state-message">' + escHtml(t('error_connection', lang)) +
    '<button class="retry-btn" data-action="retry">' + escHtml(t('btn_retry', lang)) + '</button></div>';
}

// Skeleton helpers
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
      if (e.key === 'Enter') {
        if (e.shiftKey) {
          doQuickInsert(searchInput.value);
        } else {
          // Cancel any pending live-search and run immediately.
          if (_liveSearchTimer) clearTimeout(_liveSearchTimer);
          doSearch(searchInput.value);
        }
      }
    });
    // Debounced live search — 350ms after the last keystroke. Skips empty,
    // skips reference patterns (those need Enter to confirm intent and
    // jump to detail). Min 3 chars to avoid scanning the whole corpus.
    searchInput.addEventListener('input', function (e) {
      var value = e.target.value;
      if (_liveSearchTimer) clearTimeout(_liveSearchTimer);
      var trimmed = (value || '').trim();
      if (trimmed.length < 3) return;
      if (_isReferencePattern(trimmed)) return;
      _liveSearchTimer = setTimeout(function () { doSearch(value); }, 350);
    });
    searchInput.focus();
  }

  var proCheck = document.getElementById('pro-consent-check');
  var proBtn = document.getElementById('btn-upgrade-pro');
  if (proCheck && proBtn) {
    proCheck.addEventListener('change', function () {
      proBtn.disabled = !proCheck.checked;
    });
  }

  // Privacy / usage signal toggle — persist opt-out state in localStorage
  var privToggle = document.getElementById('priv-signal-toggle');
  if (privToggle) {
    privToggle.addEventListener('change', function () {
      if (privToggle.checked) {
        localStorage.removeItem('ocl_usage_signal_optout');
      } else {
        localStorage.setItem('ocl_usage_signal_optout', '1');
      }
    });
  }
}

// Delegated click handler — bound ONCE in initApp, never re-added
async function handleAppClick(e) {
    var btn = e.target.closest('[data-action]');
    if (!btn) return;
    _lastClickedBtn = btn;
    var action = btn.dataset.action;
    var idx = parseInt(btn.dataset.idx, 10);

    switch (action) {
      case 'insert':       await insertCitation(state.results[idx]); break;
      case 'detail':       await showDetail(state.results[idx]); break;
      case 'insert-main':  await insertCitation(state.detail); break;
      case 'insert-main-link': await insertCitation(state.detail, null, { asLink: true }); break;
      case 'insert-ew':    await insertCitation(state.detail, btn.dataset.ew); break;
      case 'back': {
        // Sub-views (detail/verify/support/related/guide) return to the tab
        // they were launched from. Falls back to the search tab.
        var subViews = { detail: 1, verify: 1, support: 1, related: 1, guide: 1 };
        if (subViews[state.view] && state.previousView) {
          state.view = state.previousView;
          state.detail = null;
          state.caseBrief = null;
          state.error = null;
        } else {
          state.view = 'search';
          state.detail = null;
          state.caseBrief = null;
          state.verifyResult = null;
          state.supportResult = null;
          state.supportText = '';
          state.relatedResult = null;
          state.relatedRef = '';
          state.error = null;
        }
        render();
        break;
      }
      case 'retry':
        state.error = null;
        if (state.view === 'search') doSearch(state.query);
        render();
        break;
      case 'find-related':  await findRelated(); break;
      case 'find-support': await findSupport(); break;
      case 'scan-doc':     await scanDocument(); break;
      case 'switch-tab': {
        var tab = btn.dataset.tab;
        // Switching tabs always returns to the tab's primary view.
        if (tab === 'search') {
          state.view = 'search';
        } else if (tab === 'audit') {
          state.view = 'scan';
        }
        state.detail = null;
        state.caseBrief = null;
        state.error = null;
        render();
        break;
      }
      case 'open-settings':
        state.view = 'settings';
        render();
        break;
      case 'audit-reset':
        state.scanResults = null;
        state.scanDocText = '';
        state.error = null;
        render();
        break;
      case 'audit-comment-all': await commentAllAuditIssues(); break;
      case 'audit-insert-bibliography': await insertBibliographyAtCursor(); break;
      case 'dismiss-hint':
        try { localStorage.setItem('ocl_hint_seen', '1'); } catch (_e) {}
        render();
        break;
      case 'audit-jump': {
        var jIss = state.scanResults && state.scanResults.issues && state.scanResults.issues[idx];
        if (jIss) {
          var jOk = await selectInDocument(jIss.citation, jIss._nth || 0);
          if (jOk && btn) {
            var jOrig = btn.textContent; btn.textContent = '\u2713';
            setTimeout(function () { btn.textContent = jOrig; }, 800);
          }
        }
        break;
      }
      case 'audit-comment': {
        var cIss = state.scanResults && state.scanResults.issues && state.scanResults.issues[idx];
        if (cIss) {
          var cText = _formatIssueComment(cIss, state.lang);
          var cOk = await commentOnSubstring(cIss.citation, cText, cIss._nth || 0);
          if (btn) {
            btn.textContent = cOk ? '\u2713' : '\u2717';
            setTimeout(function () { render(); }, 1000);
          }
        }
        break;
      }
      case 'audit-fix-pinpoint': {
        var fIss = state.scanResults && state.scanResults.issues && state.scanResults.issues[idx];
        var newPp = btn.dataset.pp;
        if (fIss && newPp) {
          // Strip old E./consid. suffix and re-attach the suggested one in document language.
          var stripped = fIss.citation.replace(/\s*(?:E\.|consid\.|para\.|cons\.)\s*[\d.]+\s*$/i, '').trim();
          var ewLabel = (state.lang === 'fr' || state.lang === 'it') ? 'consid.' : 'E.';
          var fixed = stripped + ' ' + ewLabel + ' ' + newPp;
          var fOk = await replaceInDocument(fIss.citation, fixed, fIss._nth || 0);
          if (btn) {
            btn.textContent = fOk ? '\u2713' : '\u2717';
            setTimeout(function () { render(); }, 1000);
          }
        }
        break;
      }
      case 'insert-support':
        if (state.supportResult && state.supportResult.results) {
          await insertCitation(state.supportResult.results[idx]);
        }
        break;
      case 'detail-support':
        if (state.supportResult && state.supportResult.results) {
          await showDetail(state.supportResult.results[idx]);
        }
        break;
      case 'insert-law-para':
        await insertTextAtCursor(btn.dataset.ref || '');
        if (btn) { var _o = btn.textContent; btn.textContent = '\u2713'; setTimeout(function () { btn.textContent = _o; }, 600); }
        break;
      case 'insert-law-verbatim': {
        var lvText = btn.dataset.text || '';
        var lvRef = btn.dataset.ref || '';
        if (lvText && lvRef) {
          // German guillemets for quote; legal style is «...» (Art. X Abs. N AB)
          await insertTextAtCursor('\u00AB' + lvText + '\u00BB (' + lvRef + ')');
          if (btn) { btn.classList.add('btn-success'); setTimeout(function () { btn.classList.remove('btn-success'); }, 900); }
        }
        break;
      }
      case 'insert-law-ref':
        if (state.lawResult && state.lawResult.articles && state.lawResult.articles.length > 0) {
          var la = state.lawResult.articles[0];
          var laRef = 'Art. ' + (la.article_num || la.article || '') + ' ' + (state.lawResult.abbreviation || '');
          await insertTextAtCursor(laRef);
        }
        break;
      case 'accept-consent':
        state.consent = true;
        try { localStorage.setItem('ocl_consent', '1'); } catch (e) {}
        render();
        break;
      case 'quick-search':
        doSearch(btn.dataset.query || '');
        break;
      case 'toggle-expand':
        var target = document.getElementById(btn.dataset.target);
        if (target) {
          var isExpanded = target.classList.toggle('expanded');
          btn.textContent = isExpanded ? t('btn_show_less', state.lang) : t('btn_show_more', state.lang);
        }
        break;
      case 'expand-fulltext':
        var ftEl = document.getElementById('fulltext-body');
        if (ftEl && state.detail && state.detail.full_text) {
          var MAX_DOM_CHARS = 50000;
          var ft = state.detail.full_text;
          if (ft.length > MAX_DOM_CHARS) {
            ftEl.textContent = ft.substring(0, MAX_DOM_CHARS);
            // Add "continue on opencaselaw.ch" link after the text
            var contLink = document.createElement('a');
            contLink.className = 'fulltext-link';
            contLink.dataset.action = 'open-external';
            contLink.dataset.url = 'https://mcp.opencaselaw.ch/entscheid/' + encodeURIComponent(state.detail.decision_id || '');
            contLink.textContent = t('fulltext_continue', state.lang);
            ftEl.parentNode.insertBefore(contLink, ftEl.nextSibling.nextSibling || null);
          } else {
            ftEl.textContent = ft;
          }
          ftEl.classList.add('expanded');
          btn.textContent = t('btn_show_less', state.lang);
          btn.dataset.action = 'collapse-fulltext';
        }
        break;
      case 'collapse-fulltext':
        var ftEl2 = document.getElementById('fulltext-body');
        if (ftEl2 && state.detail && state.detail.full_text) {
          ftEl2.textContent = state.detail.full_text.substring(0, 3000);
          ftEl2.classList.remove('expanded');
          btn.textContent = t('btn_show_more', state.lang);
          btn.dataset.action = 'expand-fulltext';
          // Remove "continue" link if added
          var contLinks = ftEl2.parentNode.querySelectorAll('.fulltext-link');
          contLinks.forEach(function(el) { el.remove(); });
        }
        break;
      case 'open-external':
        var extUrl = btn.dataset.url;
        if (extUrl) openExternal(extUrl);
        break;
      case 'clear-search':
        state.query = '';
        state.results = [];
        state.total = 0;
        state.lawResult = null;
        state.error = null;
        render();
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
      case 'manage-subscription':
        var mKey = localStorage.getItem('ocl_pro_key');
        if (mKey) {
          try {
            var portal = await apiPost('/billing/portal?key=' + encodeURIComponent(mKey));
            if (portal.portal_url) openExternal(portal.portal_url);
          } catch (e) { console.error('Portal error:', e); }
        }
        break;
      case 'upgrade-pro':  await doUpgradePro(); break;
      case 'activate-pro': await doActivatePro(); break;
      case 'remove-pro':
        localStorage.removeItem('ocl_pro_key');
        render();
        break;
    }
}

// Actions — Free tier: direct lookup only (no keyword search)
async function doSearch(query) {
  query = (query || '').trim();
  if (!query) return;

  state.query = query;
  state.results = [];
  state.lawResult = null;
  state.loading = true;
  state.error = null;
  render();

  // 1. Detect law article pattern → direct law lookup
  // Matches "Art. 41 OR" and "OR 41" / "BV 8" etc.
  var lawMatch = query.match(LAW_PATTERN);
  var lawReverse = !lawMatch && query.trim().match(LAW_PATTERN_REVERSE);
  var lawAbbrev = lawMatch ? lawMatch[2] : (lawReverse ? lawReverse[1] : null);
  var lawArticle = lawMatch ? lawMatch[1] : (lawReverse ? lawReverse[2] : null);
  if (lawAbbrev) {
    var normalized = LAW_ALIAS[lawAbbrev.toLowerCase()];
    if (normalized) lawAbbrev = normalized;
    try {
      var lawData = await getLaw(lawAbbrev, lawArticle, state.lang);
      if (lawData && lawData.articles && lawData.articles.length > 0) {
        state.lawResult = lawData;
        addRecentLookup(query);
      } else {
        state.error = { type: 'not_found', message: t('lookup_not_found', state.lang) };
      }
    } catch (e) {
      state.error = { type: 'not_found', message: t('lookup_not_found', state.lang) };
    }
    state.loading = false;
    render();
    return;
  }

  // 2. Detect decision reference → direct decision lookup
  var isDecisionRef = /^(BGE|ATF|DTF)\s+\d+\s+[IVX]+\s+\d+$/i.test(query) ||
                      /^\d[A-Z]_\d+\/\d{4}$/.test(query) ||
                      /^[A-Z]-\d+\/\d{4}$/.test(query) ||
                      /^[A-Z]{2,}\.\d{4}\.\d+$/.test(query);

  if (isDecisionRef) {
    try {
      var decision = await getDecision(query);
      if (decision && decision.decision_id) {
        state.loading = false;
        addRecentLookup(query);
        await showDetail(decision);
        return;
      }
    } catch (e) {
      // Not found by direct lookup — fall through to search
    }
  }

  // 3. Keyword search — full-text search across the corpus, all languages.
  // Result objects already include citation_string_{de,fr,it} + canonical_url.
  try {
    var data = await searchDecisions(query, { limit: 20 });
    state.results = (data && data.results) || [];
    state.total = (data && data.total) || 0;
    if (state.results.length > 0) addRecentLookup(query);
  } catch (e) {
    state.error = e && e.type ? e : { type: 'http_error' };
  }
  state.loading = false;
  render();
}

async function doQuickInsert(query) {
  query = (query || '').trim();
  if (!query) return;
  // Try to format and insert citation directly
  var lawMatch = query.match(LAW_PATTERN);
  var lawReverse = !lawMatch && query.trim().match(LAW_PATTERN_REVERSE);
  var lawAbbrev = lawMatch ? lawMatch[2] : (lawReverse ? lawReverse[1] : null);
  var lawArticle = lawMatch ? lawMatch[1] : (lawReverse ? lawReverse[2] : null);
  if (lawAbbrev) {
    await insertTextAtCursor('Art. ' + lawArticle + ' ' + lawAbbrev);
    return;
  }
  // Decision ref — build citation and insert
  var citation = '(' + query + ')';
  await insertTextAtCursor(citation);
}

async function showDetail(decision) {
  state.previousView = state.view;
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

var _lastClickedBtn = null;
async function insertCitation(decision, erwaegung, opts) {
  if (!decision) return;
  opts = opts || {};
  var text = formatCitation(decision, state.lang, erwaegung);
  var btn = _lastClickedBtn;

  // Default insert mode persists per-user; flip with the link button.
  var asLink = opts.asLink !== undefined ? opts.asLink : (localStorage.getItem('ocl_insert_as_link') === '1');
  var url = decision.canonical_url || (decision.decision_id ? 'https://mcp.opencaselaw.ch/entscheid/' + encodeURIComponent(decision.decision_id) : null);

  try {
    if (asLink && url) {
      await insertHyperlinkAtCursor(text, url);
    } else {
      await insertTextAtCursor(text);
    }
    if (btn) {
      var orig = btn.innerHTML;
      btn.innerHTML = ICONS.check;
      btn.classList.add('btn-success');
      setTimeout(function () {
        btn.innerHTML = orig;
        btn.classList.remove('btn-success');
      }, 900);
    }
  } catch (e) {
    if (btn) {
      btn.textContent = '\u2717';
      btn.style.background = 'var(--red)';
      setTimeout(function () { btn.textContent = t('btn_insert', state.lang); btn.style.background = ''; }, 2000);
    }
    console.error('Insert failed:', e);
  }
}

async function startVerify() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');

  if (!proKey) {
    state.view = 'settings';
    render();
    return;
  }
  state.previousView = state.view;
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

    var refs = extractCitations(selected);
    var caseRef = refs.length > 0 ? refs[0] : selected.trim().substring(0, 50);
    state.verifyResult = await verifyReferencePro(proKey, selected, caseRef, lang);
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

async function findSupport() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');
  if (!proKey) { state.view = 'settings'; render(); return; }
  state.previousView = state.view;
  try {
    var selected = await getSelectedText();
    if (!selected || selected.trim().length < 15) {
      state.error = { type: 'no_selection', message: t('no_selection_support', lang) };
      state.view = 'support';
      render();
      return;
    }
    state.supportText = selected;
    state.view = 'support';
    state.loading = true;
    state.supportResult = null;
    state.error = null;
    render();
    state.supportResult = await findSupportingDecisions(proKey, selected, lang);
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

async function findRelated() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');
  if (!proKey) { state.view = 'settings'; render(); return; }
  state.previousView = state.view;
  try {
    var selected = await getSelectedText();
    if (!selected || selected.trim().length < 3) {
      state.error = { type: 'no_selection', message: t('no_selection_related', lang) };
      render();
      return;
    }
    var refs = extractCitations(selected);
    if (refs.length === 0) {
      state.error = { type: 'no_citation', message: t('no_selection_related', lang) };
      render();
      return;
    }
    // Resolve the citation to a decision ID
    state.loading = true;
    state.view = 'related';
    state.relatedRef = refs[0];
    state.relatedResult = null;
    state.error = null;
    render();

    // Get the decision first to resolve the ID
    var decision = null;
    try { decision = await getDecision(refs[0]); } catch (e) {}
    var decisionId = decision ? decision.decision_id : refs[0];

    // Fetch citation graph
    var citations = await findCitations(decisionId, 'both', 10);

    // Enrich results with metadata by looking up each one
    var incoming = citations.incoming || [];
    var outgoing = citations.outgoing || [];

    // Fetch regeste for top results
    async function enrichResults(list) {
      var refs = list
        .map(function (c) { return c.docket_number || c.bge_ref || c.decision_id || ''; })
        .filter(function (r) { return r && r !== 'None'; })
        .slice(0, 5);
      // Sequential to avoid overwhelming the connection
      var enriched = [];
      for (var i = 0; i < refs.length && enriched.length < 3; i++) {
        try {
          var data = await searchDecisions(refs[i], { limit: 1 });
          if (data.results && data.results.length > 0) {
            var d = data.results[0];
            if (d.regeste || d.title || stripHtml(d.snippet)) enriched.push(d);
          }
        } catch (e) {}
      }
      return enriched;
    }

    var enrichedIncoming = await enrichResults(incoming);
    var enrichedOutgoing = await enrichResults(outgoing);

    state.relatedResult = {
      ref: refs[0],
      decision_id: decisionId,
      incoming: enrichedIncoming,
      outgoing: enrichedOutgoing,
    };
  } catch (e) {
    state.error = e;
  }
  state.loading = false;
  render();
}

async function doUpgradePro() {
  try {
    var successUrl = 'https://word.opencaselaw.ch/pro-success.html?session_id={CHECKOUT_SESSION_ID}';
    var cancelUrl = 'https://word.opencaselaw.ch/';
    var localeMap = { de: 'de', fr: 'fr', it: 'it', en: 'en' };
    var locale = localeMap[state.lang] || 'de';
    var data = await createCheckout(successUrl, cancelUrl, locale);
    if (data.checkout_url) {
      openExternal(data.checkout_url);
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

// Recent lookups helper
function addRecentLookup(query) {
  query = query.trim();
  if (!query) return;
  // Remove if already exists, add to front
  state.recentLookups = state.recentLookups.filter(function (r) { return r !== query; });
  state.recentLookups.unshift(query);
  if (state.recentLookups.length > 8) state.recentLookups = state.recentLookups.slice(0, 8);
  try { localStorage.setItem('ocl_recent', JSON.stringify(state.recentLookups)); } catch (e) {}
}

// Audit Document (Pro feature) — single /api/attest call validates every
// citation in the document for existence + pinpoint correctness, returning
// structured issues with positions for in-document jump + comment.
async function scanDocument() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');
  if (!proKey) { state.view = 'settings'; render(); return; }

  var docText = await getDocumentText();
  if (!docText) {
    state.error = { type: 'no_selection', message: t('audit_no_word', lang) };
    state.view = 'scan';
    render();
    return;
  }

  state.view = 'scan';
  state.loading = true;
  state.scanResults = null;
  state.scanDocText = docText;
  state.error = null;
  render();

  try {
    var report = await attestDocument(docText, lang);
    // Compute occurrence index per citation string so search-anchor lands
    // on the right hit when the same citation appears multiple times.
    var seen = {};
    var issues = (report.issues || []).map(function (issue) {
      var cite = issue.citation || '';
      var n = seen[cite] || 0; seen[cite] = n + 1;
      return Object.assign({}, issue, { _nth: n });
    });
    state.scanResults = {
      ok: !!report.ok,
      total: report.citations_found || 0,
      passed: report.citations_ok || 0,
      issues: issues,
      annotated: report.annotated_text || '',
    };
  } catch (e) {
    if (e && e.type === 'invalid_license') {
      try { localStorage.removeItem('ocl_pro_key'); } catch (_e) {}
      state.error = { type: 'no_selection', message: t('pro_key_invalid', lang) };
    } else {
      state.error = e;
    }
  }
  state.loading = false;
  render();
}

// Insert a Word comment for every issue, anchored on the original citation text.
async function commentAllAuditIssues() {
  var lang = state.lang;
  if (!state.scanResults || !state.scanResults.issues) return;
  var inserted = 0;
  for (var i = 0; i < state.scanResults.issues.length; i++) {
    var iss = state.scanResults.issues[i];
    var commentText = _formatIssueComment(iss, lang);
    var ok = await commentOnSubstring(iss.citation, commentText, iss._nth || 0);
    if (ok) inserted += 1;
  }
  if (_lastClickedBtn) {
    _lastClickedBtn.textContent = '\u2713 ' + inserted;
    setTimeout(function () { render(); }, 1200);
  }
}

// Build a deduped, alphabetically-sorted bibliography of validated citations
// from the current audit report and insert it at the cursor as a heading + list.
async function insertBibliographyAtCursor() {
  var lang = state.lang;
  if (!state.scanResults || !state.scanResults.annotated) return;
  var cites = _extractPassedCitations(state.scanResults.annotated);
  if (cites.length === 0) return;
  // Sort: BGE-style refs first (by volume), then docket-style alphabetical.
  cites.sort(function (a, b) { return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }); });
  var heading = t('bibliography_heading', lang);
  var body = '\n' + heading + '\n';
  for (var i = 0; i < cites.length; i++) body += '\u2014 ' + cites[i] + '\n';
  await insertTextAtCursor(body);
  if (_lastClickedBtn) {
    var orig = _lastClickedBtn.innerHTML;
    _lastClickedBtn.innerHTML = ICONS.check + ' ' + cites.length;
    _lastClickedBtn.classList.add('btn-success');
    setTimeout(function () {
      if (_lastClickedBtn) {
        _lastClickedBtn.innerHTML = orig;
        _lastClickedBtn.classList.remove('btn-success');
      }
    }, 1200);
  }
}

function _formatIssueComment(issue, lang) {
  var label;
  if (issue.problem === 'decision_not_in_corpus') label = t('audit_issue_not_found', lang);
  else if (issue.problem === 'pinpoint_not_in_decision') label = t('audit_issue_pinpoint', lang);
  else label = t('audit_issue_unknown', lang);
  var msg = '\u26a0 ' + label;
  if (issue.suggestion) msg += ' \u2014 ' + issue.suggestion;
  if (issue.valid_pinpoints && issue.valid_pinpoints.length) {
    msg += ' (' + t('audit_valid_pinpoints', lang) + ' ' + issue.valid_pinpoints.slice(0, 8).join(', ') + ')';
  }
  return msg;
}

// Audit View — render the structured /api/attest report.
function renderScan() {
  var lang = state.lang;
  var hasPro = !!localStorage.getItem('ocl_pro_key');
  var html = '';

  // Loading: just the spinner.
  if (state.loading) {
    html += '<div class="verify-loading"><div class="spinner"></div><span>' + escHtml(t('audit_running', lang)) + '</span></div>';
    return html;
  }

  // Entry state — no audit has been run yet on this view.
  if (!state.scanResults) {
    // Inline error (e.g., no text selected for verify/support/related)
    if (state.error) {
      html += '<div class="audit-inline-error">' + escHtml(state.error.message || t('error_connection', lang)) + '</div>';
    }
    html += '<div class="audit-intro">' +
      '<div class="audit-intro-icon">' + ICONS.shieldCheck + '</div>' +
      '<div class="audit-intro-title">' + escHtml(t('audit_intro_title', lang)) + '</div>' +
      '<div class="audit-intro-desc">' + escHtml(t('audit_intro_desc', lang)) + '</div>' +
      '<div class="audit-intro-preview">' +
      '<div class="audit-intro-preview-row"><span class="audit-pp-icon ok">' + ICONS.check + '</span> BGE 140 III 86 E. 2.3</div>' +
      '<div class="audit-intro-preview-row warn"><span class="audit-pp-icon warn">!</span> BGE 140 III 86 E. 3.2 — ' + escHtml(t('audit_intro_preview_pinpoint', lang)) + '</div>' +
      '</div>' +
      (hasPro
        ? '<button class="btn btn-pro-primary btn-full" data-action="scan-doc">' + escHtml(t('audit_start_button', lang)) + '</button>'
        : '<button class="btn btn-pro-primary btn-full" data-action="open-settings">' + escHtml(t('audit_unlock_button', lang)) + ' \u2014 CHF\u00A05/Mt.</button>' +
          '<div class="audit-pro-note">' + escHtml(t('audit_pro_required', lang)) + '</div>'
      ) +
      '</div>';

    // Secondary tools — verify selection, find support, find related.
    html += '<div class="audit-secondary">' +
      '<div class="audit-secondary-title">' + escHtml(t('audit_secondary_title', lang)) + '</div>' +
      '<button class="btn btn-detail audit-secondary-btn" data-action="verify-ref">' +
      '<span class="audit-secondary-label">' + escHtml(t('btn_verify_pro', lang)) + '</span>' +
      (hasPro ? '' : proLockSvg()) + '</button>' +
      '<button class="btn btn-detail audit-secondary-btn" data-action="find-support">' +
      '<span class="audit-secondary-label">' + escHtml(t('tool_support', lang)) + '</span>' +
      (hasPro ? '' : proLockSvg()) + '</button>' +
      '<button class="btn btn-detail audit-secondary-btn" data-action="find-related">' +
      '<span class="audit-secondary-label">' + escHtml(t('btn_find_related', lang)) + '</span>' +
      (hasPro ? '' : proLockSvg()) + '</button>' +
      '</div>';

    return html;
  }

  // Results header — show "audit again" affordance + the action to clear.
  html += '<div class="audit-result-header">' +
    '<button class="back-link" data-action="audit-reset">\u2190 ' + escHtml(t('back', lang)) + '</button>' +
    '<button class="btn btn-detail audit-rerun" data-action="scan-doc">\u21BB ' + escHtml(t('audit_start_button', lang)) + '</button>' +
    '</div>';

  var report = state.scanResults;
  var total = report.total || 0;
  var passed = report.passed || 0;
  var bad = (report.issues && report.issues.length) || 0;

  if (total === 0) {
    html += '<div class="state-message">' + escHtml(t('audit_summary_none', lang)) + '</div>';
    return html;
  }

  // Summary banner
  var summaryClass = bad === 0 ? 'audit-summary-ok' : 'audit-summary-mixed';
  var summaryText = bad === 0
    ? t('audit_summary_ok', lang, { n: total })
    : t('audit_summary_mixed', lang, { n: total, ok: passed, bad: bad });
  html += '<div class="audit-summary ' + summaryClass + '">' +
    '<span class="audit-summary-icon">' + (bad === 0 ? '\u2713' : '\u26a0') + '</span>' +
    '<span class="audit-summary-text">' + escHtml(summaryText) + '</span>' +
    '</div>';

  // Bulk actions: comment-all (issues) + insert-bibliography (passed)
  var bulkActions = '';
  if (bad > 0 && supportsComments()) {
    bulkActions += '<button class="btn btn-detail" data-action="audit-comment-all">' +
      escHtml(t('audit_comment_all', lang)) + '</button>';
  }
  if (passed > 0) {
    bulkActions += '<button class="btn btn-detail" data-action="audit-insert-bibliography">' +
      escHtml(t('audit_insert_bibliography', lang)) + '</button>';
  }
  if (bulkActions) {
    html += '<div class="audit-bulk-actions">' + bulkActions + '</div>';
  }

  // Issues list — each card shows what's wrong + suggested fix + jump-to + comment
  for (var i = 0; i < (report.issues || []).length; i++) {
    var iss = report.issues[i];
    html += renderAuditIssueCard(iss, i, lang);
  }

  // Passed citations — collapsible reveal so users see exactly what was validated.
  if (passed > 0) {
    var passedCites = _extractPassedCitations(report.annotated || '');
    html += '<details class="audit-passed">' +
      '<summary class="audit-passed-summary">' +
      '<span class="audit-pp-icon ok">' + ICONS.check + '</span>' +
      '<span>' + escHtml(t('audit_passed_label', lang, { n: passed })) + '</span>' +
      ICONS.chevron +
      '</summary>' +
      '<ul class="audit-passed-list">';
    for (var p = 0; p < passedCites.length; p++) {
      html += '<li>' + escHtml(passedCites[p]) + '</li>';
    }
    html += '</ul></details>';
  }

  return html;
}

// Walk the server-annotated text, pulling out citations marked with ✓.
// The /api/attest contract appends ✓ to each validated citation in
// document order; we capture the preceding token group as the citation.
function _extractPassedCitations(annotated) {
  if (!annotated) return [];
  var re = /([A-Z][A-Za-zÀ-ÿ.\-_/\d\s]{2,80}?)\s\u2713/g;
  var seen = {};
  var out = [];
  var match;
  while ((match = re.exec(annotated)) !== null) {
    var c = match[1].trim().replace(/\s+/g, ' ');
    if (!c || seen[c]) continue;
    seen[c] = true;
    out.push(c);
  }
  return out;
}

function renderAuditIssueCard(iss, idx, lang) {
  var problemLabel;
  var problemClass;
  if (iss.problem === 'decision_not_in_corpus') {
    problemLabel = t('audit_issue_not_found', lang);
    problemClass = 'audit-issue-missing';
  } else if (iss.problem === 'pinpoint_not_in_decision') {
    problemLabel = t('audit_issue_pinpoint', lang);
    problemClass = 'audit-issue-pinpoint';
  } else {
    problemLabel = t('audit_issue_unknown', lang);
    problemClass = 'audit-issue-other';
  }

  var html = '<div class="audit-issue ' + problemClass + '">' +
    '<div class="audit-issue-header">' +
    '<span class="audit-issue-icon">\u26a0</span>' +
    '<span class="audit-issue-citation">' + escHtml(iss.citation || '') + '</span>' +
    '</div>' +
    '<div class="audit-issue-label">' + escHtml(problemLabel) + '</div>';

  if (iss.suggestion) {
    html += '<div class="audit-issue-suggestion">' + escHtml(iss.suggestion) + '</div>';
  }

  if (iss.valid_pinpoints && iss.valid_pinpoints.length) {
    html += '<div class="audit-issue-pinpoints">' +
      '<span class="audit-pinpoints-label">' + escHtml(t('audit_valid_pinpoints', lang)) + '</span> ';
    for (var k = 0; k < Math.min(iss.valid_pinpoints.length, 8); k++) {
      var pp = iss.valid_pinpoints[k];
      html += '<button class="pill audit-pinpoint-pill" data-action="audit-fix-pinpoint" ' +
        'data-idx="' + idx + '" data-pp="' + escHtml(pp) + '">E. ' + escHtml(pp) + '</button>';
    }
    html += '</div>';
  }

  html += '<div class="audit-issue-actions">';
  html += '<button class="btn btn-detail" data-action="audit-jump" data-idx="' + idx + '">' +
    escHtml(t('audit_jump_to', lang)) + '</button>';
  if (supportsComments()) {
    html += '<button class="btn btn-detail" data-action="audit-comment" data-idx="' + idx + '">' +
      escHtml(t('audit_insert_comment', lang)) + '</button>';
  }
  html += '</div></div>';
  return html;
}

// Utilities — HTML entity escaping for XSS prevention
function escHtml(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function openExternal(url) {
  // Office add-ins: use a temporary <a> click (bypasses popup blocker)
  var a = document.createElement('a');
  a.href = url;
  a.target = '_blank';
  a.rel = 'noopener';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function stripHtml(s) {
  if (!s) return '';
  return s.replace(/<[^>]*>/g, '');
}
