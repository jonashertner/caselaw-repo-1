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

// Rendering — all values passed through escHtml() for XSS safety
// Safe: all dynamic content is escaped via escHtml before concatenation
function render() {
  var app = document.getElementById('app');
  var html;
  if (!state.consent) { app.innerHTML = renderConsent(); bindEvents(); return; }
  switch (state.view) {
    case 'search':   html = renderSearch(); break;
    case 'detail':   html = renderDetail(); break;
    case 'verify':   html = renderVerify(); break;
    case 'support':  html = renderSupport(); break;
    case 'related':  html = renderRelated(); break;
    case 'scan':     html = renderScan(); break;
    case 'guide':    html = renderGuide(); break;
    case 'settings': html = renderSettings(); break;
    default:         html = ''; break;
  }
  app.innerHTML = html; // eslint-disable-line no-unsanitized/property -- all values pre-escaped via escHtml()
  bindEvents();
  if (state.lawResult) resolveAmendmentRefs();
}

// Format law article text into styled paragraphs with superscript numbers
function formatLawParagraphs(text, artNum, abbrev) {
  if (!text) return '';
  var lines = text.split('\n');
  var html = '';
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (!line) continue;
    var m = line.match(/^(\d+[a-z]?(?:bis|ter|quater)?)\s+(.*)$/);
    if (m) {
      var paraRef = 'Art. ' + artNum + ' Abs. ' + m[1] + ' ' + abbrev;
      var paraBody = formatLawFootnotes(m[2]);
      html += '<div class="law-para">' +
        '<span class="law-para-num" data-action="insert-law-para" data-ref="' + escHtml(paraRef) + '" title="' + escHtml(paraRef) + '">' + escHtml(m[1]) + '</span> ' +
        paraBody + '</div>';
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
    '</div>' +
    '<div class="pro-tools-row">' +
    '<button class="pro-pill" data-action="find-related">' + escHtml(t('btn_find_related', lang)) + (!localStorage.getItem('ocl_pro_key') ? '<span class="pro-pip">PRO</span>' : '') + '</button>' +
    '<button class="pro-pill" data-action="verify-ref">' + escHtml(t('btn_verify_pro', lang)) + (!localStorage.getItem('ocl_pro_key') ? '<span class="pro-pip">PRO</span>' : '') + '</button>' +
    '<button class="pro-pill" data-action="find-support">' + escHtml(t('tool_support', lang)) + (!localStorage.getItem('ocl_pro_key') ? '<span class="pro-pip">PRO</span>' : '') + '</button>' +
    '<button class="pro-pill" data-action="scan-doc">' + escHtml(t('tool_scan', lang)) + (!localStorage.getItem('ocl_pro_key') ? '<span class="pro-pip">PRO</span>' : '') + '</button>' +
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

  html += '<div class="welcome-subtitle">' + escHtml(t('welcome_hint', lang)) + '</div>';

  // Recent lookups
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

  // How it works link
  html += '<div class="welcome-footer">' +
    '<a class="welcome-link" data-action="show-guide">' + escHtml(t('how_it_works', lang)) + '</a>' +
    '</div>';

  html += '<div class="welcome-count-small">' + escHtml(t('welcome_count', lang, { n: '963\u2009000+' })) + '</div>';

  html += '</div>';
  return html;
}

function renderResultCard(r, idx) {
  var lang = state.lang;
  var badges = '';
  if (r.is_leading_case) badges += '<span class="badge badge-leading">\u2605 ' + escHtml(t('badge_leading', lang)) + '</span> ';
  if (r.citation_count > 0) badges += '<span class="badge badge-citations">' + escHtml(t('badge_citations', lang, { n: r.citation_count })) + '</span> ';
  if (r.legal_area) badges += '<span class="badge badge-area">' + escHtml(r.legal_area) + '</span>';

  var regeste = r.regeste || stripHtml(r.snippet) || r.title || '';
  if (regeste.length > 200) regeste = regeste.substring(0, 200) + '\u2026';
  var courtName = r.court ? getCourtName(r.court, lang) : (r.court_name || '');
  var date = r.decision_date || r.date || '';

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

  // Sticky insert bar
  var citationText = formatCitation(d, lang);
  html += '<div class="sticky-insert">' +
    '<span class="sticky-citation">' + escHtml(citationText) + '</span>' +
    '<button class="btn btn-insert" data-action="insert-main">' + escHtml(t('btn_insert', lang)) + '</button>' +
    '</div>';

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

  if (d.regeste) {
    var needsCollapse = d.regeste.length > 300;
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_regeste', lang)) + '</div>' +
      '<div class="section-body' + (needsCollapse ? ' collapsible' : '') + '" id="regeste-body">' + escHtml(d.regeste) + '</div>' +
      (needsCollapse ? '<button class="expand-btn" data-action="toggle-expand" data-target="regeste-body">' + escHtml(t('btn_show_more', lang)) + '</button>' : '') +
      '</div>';
  }

  var ewList = cb && (cb.key_erwaegungen || cb.erwaegungen);
  if (ewList && ewList.length) {
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_erwaegungen', lang)) + '</div>';
    for (var i = 0; i < ewList.length; i++) {
      var e = ewList[i];
      var num = e.number || '?';
      var ewText = e.text || '';
      // Split into sub-sections (e.g. 3.1, 3.2, 3.2.1)
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

  // Full text — always show if available
  if (d.full_text) {
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_fulltext', lang)) + '</div>' +
      '<div class="fulltext-body">' + escHtml(d.full_text) + '</div></div>';
  }

  if (cb && cb.statutes && cb.statutes.length) {
    html += '<div class="detail-section"><div class="detail-label">' + escHtml(t('section_statutes', lang)) + '</div><div class="pills">';
    for (var j = 0; j < cb.statutes.length; j++) {
      var st = cb.statutes[j];
      var stLabel = typeof st === 'string' ? st : ('Art. ' + (st.article || '?') + ' ' + (st.law_code || ''));
      html += '<span class="pill pill-static">' + escHtml(stLabel) + '</span>';
    }
    html += '</div></div>';
  }

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
    // Upgrade CTA
    html += '<div class="section-label">' + escHtml(t('pro_section_title', lang)) + '</div>' +
      '<div class="pro-features">' +
      '<div class="pro-feature">\u2014 ' + escHtml(t('pro_feature_related', lang)) + '</div>' +
      '<div class="pro-feature">\u2014 ' + escHtml(t('pro_feature_verify_new', lang)) + '</div>' +
      '<div class="pro-feature">\u2014 ' + escHtml(t('pro_feature_support', lang)) + '</div>' +
      '<div class="pro-feature">\u2014 ' + escHtml(t('pro_feature_limit', lang)) + '</div>' +
      '</div>' +
      '<div class="pro-consent">' +
      '<label class="pro-consent-label"><input type="checkbox" id="pro-consent-check" class="pro-consent-check"> ' +
      escHtml(t('pro_consent_text', lang)) + ' ' +
      '<a href="https://word.opencaselaw.ch/terms.html" target="_blank">' + escHtml(t('consent_terms', lang)) + '</a> ' +
      escHtml(t('pro_consent_and', lang)) + ' ' +
      '<a href="https://word.opencaselaw.ch/privacy.html" target="_blank">' + escHtml(t('consent_privacy', lang)) + '</a>.' +
      '</label></div>' +
      '<button class="btn btn-pro" data-action="upgrade-pro" id="btn-upgrade-pro" disabled>' + escHtml(t('btn_upgrade', lang)) + '</button>' +
      '<div class="pro-divider"><span>' + escHtml(t('pro_or_key', lang)) + '</span></div>' +
      '<div class="settings-field"><label class="settings-label">' + escHtml(t('pro_license_key', lang)) + '</label>' +
      '<input type="text" class="settings-input" id="pro-key-input" value="" placeholder="ocl_pro_...">' +
      '</div>' +
      '<div class="settings-actions">' +
      '<button class="btn btn-insert" data-action="activate-pro">' + escHtml(t('btn_activate', lang)) + '</button>' +
      '</div>';
  }
  html += '</div>';

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
          doSearch(searchInput.value);
        }
      }
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
      case 'insert-ew':    await insertCitation(state.detail, btn.dataset.ew); break;
      case 'back':
        if (state.view === 'detail' && state.previousView) {
          // Return to the view that opened the detail
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
          state.scanResults = null;
          state.scanDocText = '';
          state.error = null;
        }
        render();
        break;
      case 'retry':
        state.error = null;
        if (state.view === 'search') doSearch(state.query);
        render();
        break;
      case 'find-related':  await findRelated(); break;
      case 'find-support': await findSupport(); break;
      case 'scan-doc':     await scanDocument(); break;
      case 'verify-all':   await verifyAllScanned(); break;
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

  // 3. Not a recognized reference — point to opencaselaw.ch for full search
  state.error = { type: 'use_search', message: t('lookup_use_search', state.lang) };
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
async function insertCitation(decision, erwaegung) {
  if (!decision) return;
  var text = formatCitation(decision, state.lang, erwaegung);
  var btn = _lastClickedBtn;
  try {
    await insertTextAtCursor(text);
    // Visual feedback: checkmark
    if (btn) {
      var orig = btn.textContent;
      btn.textContent = '\u2713';
      setTimeout(function () { btn.textContent = orig; }, 800);
    }
  } catch (e) {
    // Show error on button so user sees something
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
  try {
    var selected = await getSelectedText();
    if (!selected || selected.trim().length < 3) {
      state.error = { type: 'no_selection', message: t('no_selection_related', lang) };
      state.view = 'search';
      render();
      return;
    }
    var refs = extractCitations(selected);
    if (refs.length === 0) {
      state.error = { type: 'no_citation', message: t('no_selection_related', lang) };
      state.view = 'search';
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

// Scan Document (Pro feature)
async function scanDocument() {
  var lang = state.lang;
  var proKey = localStorage.getItem('ocl_pro_key');
  if (!proKey) { state.view = 'settings'; render(); return; }

  // Get entire document text
  var docText = '';
  if (_wordAvailable()) {
    docText = await Word.run(async function (context) {
      var body = context.document.body;
      body.load('text');
      await context.sync();
      return body.text;
    });
  }
  if (!docText) {
    state.error = { type: 'no_selection', message: t('scan_no_doc', lang) };
    state.view = 'scan';
    render();
    return;
  }

  state.view = 'scan';
  state.loading = true;
  state.scanResults = null;
  state.scanDocText = docText;
  render();

  // Extract all citations from document text
  var allRefs = extractCitations(docText);
  // Also find BGE references with different pattern
  var bgeRefs = docText.match(/(BGE|ATF|DTF)\s+\d+\s+[IVX]+\s+\d+/g) || [];
  var docketRefs = docText.match(/\d[A-Z]_\d+\/\d{4}/g) || [];

  // Deduplicate
  var seen = {};
  var refs = [];
  var allFound = allRefs.concat(bgeRefs).concat(docketRefs);
  for (var i = 0; i < allFound.length; i++) {
    var r = allFound[i].trim();
    if (!seen[r]) { seen[r] = true; refs.push(r); }
  }

  // Look up each citation — verify actual content exists
  function docketMatch(decision, ref) {
    if (!decision) return false;
    var dn = (decision.docket_number || '').trim();
    var r = ref.trim();
    return dn === r || dn.replace(/\s+/g, ' ') === r.replace(/\s+/g, ' ');
  }
  var results = await Promise.all(refs.map(function (ref) {
    return getDecision(ref).then(function (d) {
      if (!d || !docketMatch(d, ref)) return { ref: ref, found: false, hasText: false, decision: null };
      var hasText = d.regeste || d.full_text || d.title;
      return { ref: ref, found: true, hasText: !!hasText, decision: d };
    }).catch(function () {
      return searchDecisions(ref, { limit: 1 }).then(function (data) {
        if (data.results && data.results.length > 0 && docketMatch(data.results[0], ref)) {
          var d = data.results[0];
          var hasText = d.regeste || d.full_text || d.title;
          return { ref: ref, found: true, hasText: !!hasText, decision: d };
        }
        return { ref: ref, found: false, hasText: false, decision: null };
      }).catch(function () {
        return { ref: ref, found: false, hasText: false, decision: null };
      });
    });
  }));

  state.scanResults = results;
  state.loading = false;
  render();
}

// Scan View
async function verifyAllScanned() {
  var proKey = localStorage.getItem('ocl_pro_key');
  if (!proKey || !state.scanResults) return;
  var lang = state.lang;
  var docText = state.scanDocText || '';

  for (var i = 0; i < state.scanResults.length; i++) {
    var r = state.scanResults[i];
    if (!r.found || !r.hasText || r.verdict) continue;

    // Extract context: find the sentence(s) around this reference in the document
    var refIdx = docText.indexOf(r.ref);
    var context = '';
    if (refIdx >= 0) {
      var start = Math.max(0, refIdx - 200);
      var end = Math.min(docText.length, refIdx + r.ref.length + 200);
      context = docText.substring(start, end);
    } else {
      context = r.ref;
    }

    // Mark as verifying and re-render
    r.verifying = true;
    render();

    try {
      var result = await verifyReferencePro(proKey, context, r.ref, lang);
      r.verdict = result.verdict || 'unknown';
      r.explanation = result.explanation || '';
    } catch (e) {
      if (e.type === 'rate_limit' || (e.status && e.status === 429)) {
        r.verifying = false;
        r.verdict = 'limit';
        r.explanation = t('scan_limit_reached', lang);
        render();
        break;
      }
      r.verdict = 'error';
      r.explanation = '';
    }
    r.verifying = false;
    render();
  }
}

function renderScan() {
  var lang = state.lang;
  var html = '<a class="back-link" data-action="back">\u2190 ' + escHtml(t('back', lang)) + '</a>';
  html += '<h3 class="verify-heading">' + escHtml(t('tool_scan', lang)) + '</h3>';

  if (state.loading) {
    html += '<div class="verify-loading"><div class="spinner"></div><span>' + escHtml(t('scan_scanning', lang)) + '</span></div>';
  } else if (state.scanResults) {
    var results = state.scanResults;
    var found = results.filter(function (r) { return r.found; }).length;
    var notFound = results.length - found;

    var verifiable = results.filter(function (r) { return r.found && r.hasText; }).length;
    var verified = results.filter(function (r) { return r.verdict; }).length;

    html += '<div class="scan-summary">' +
      escHtml(t('scan_found', lang, { n: results.length })) +
      (notFound > 0 ? ' \u00B7 <span style="color:var(--red);">' + notFound + ' ' + escHtml(t('scan_not_found', lang)) + '</span>' : '') +
      '</div>';

    if (verifiable > 0 && verified < verifiable) {
      html += '<button class="btn btn-full" data-action="verify-all" style="margin-top:0;margin-bottom:12px;">' +
        escHtml(t('scan_verify_all', lang, { n: verifiable })) + '</button>';
    }

    // Store decisions in state.results so detail handler works
    state.results = results.map(function (r) { return r.decision; }).filter(Boolean);

    for (var i = 0; i < results.length; i++) {
      var r = results[i];
      var statusClass = r.found ? (r.hasText ? 'scan-ok' : 'scan-stub') : 'scan-missing';
      var statusIcon = r.found ? (r.hasText ? '\u2713' : '\u2014') : '\u2717';

      // Show verdict if verified
      if (r.verdict) {
        var vColors = { supports: 'var(--green)', partial: 'var(--yellow)', contradicts: 'var(--red)' };
        var vIcons = { supports: '\u2713', partial: '\u26A0', contradicts: '\u2717' };
        statusIcon = vIcons[r.verdict] || statusIcon;
        statusClass = 'scan-verified';
      }

      // Find index in state.results for detail navigation
      var resultIdx = r.decision ? state.results.indexOf(r.decision) : -1;

      html += '<div class="scan-item ' + statusClass + '">' +
        '<span class="scan-icon"' + (r.verdict ? ' style="color:' + (vColors[r.verdict] || '') + '"' : '') + '>' + statusIcon + '</span>' +
        '<div class="scan-ref">';
      if (r.found && resultIdx >= 0) {
        html += '<div class="scan-ref-name"><a class="scan-link" data-action="detail" data-idx="' + resultIdx + '">' + escHtml(r.ref) + '</a></div>';
      } else {
        html += '<div class="scan-ref-name">' + escHtml(r.ref) + '</div>';
      }
      if (r.found && r.decision) {
        var courtName = r.decision.court ? getCourtName(r.decision.court, lang) : '';
        var date = r.decision.decision_date || r.decision.date || '';
        html += '<div class="scan-ref-meta">' + escHtml(date) + (courtName ? ' \u00B7 ' + escHtml(courtName) : '') + '</div>';
      }
      if (r.verdict && r.explanation) {
        html += '<div class="scan-verdict-text">' + escHtml(r.explanation) + '</div>';
      }
      if (r.verifying) {
        html += '<div class="scan-ref-meta"><span class="spinner" style="width:12px;height:12px;border-width:1.5px;display:inline-block;vertical-align:middle;margin-right:4px;"></span>' + escHtml(t('verify_checking', lang)) + '</div>';
      }
      html += '</div></div>';
    }

    html += '<div class="verdict-footer">' + escHtml(t('verify_footer_pro', lang)) + '</div>';
  } else if (state.error) {
    html += renderError();
  }
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
