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
function render() {
  var app = document.getElementById('app');
  var html;
  switch (state.view) {
    case 'search':   html = renderSearch(); break;
    case 'detail':   html = renderDetail(); break;
    case 'laws':     html = renderLaws(); break;
    case 'verify':   html = renderVerify(); break;
    case 'settings': html = renderSettings(); break;
    default:         html = ''; break;
  }
  // Safe: all dynamic content is escaped via escHtml before concatenation
  app.innerHTML = html; // eslint-disable-line no-unsanitized/property
  bindEvents();
}

// Search View
function renderSearch() {
  var html =
    '<input class="search-bar" id="search-input" type="text" ' +
    'placeholder="Entscheide, Gesetze, Doktrin suchen..." ' +
    'value="' + escHtml(state.query) + '">' +
    '<div class="filters">' +
    '<button class="filter-btn" data-action="show-laws">\uD83D\uDCDA Gesetze</button>' +
    '<button class="filter-btn" data-action="verify-ref" title="Text markieren, dann pr\u00FCfen">\uD83D\uDD0D Referenz pr\u00FCfen</button>' +
    '</div>';

  if (state.loading) {
    html += '<div class="results"><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>';
  } else if (state.error) {
    html += renderError();
  } else if (state.results.length === 0 && state.query) {
    html += '<div class="state-message">Keine Treffer gefunden.<br>Versuchen Sie einen allgemeineren Suchbegriff.</div>';
  } else {
    html += '<div class="results">';
    if (state.total > 0) {
      html += '<div style="font-size:11px;color:var(--text-muted);margin-bottom:4px;">' + state.total + ' Entscheide gefunden</div>';
    }
    for (var i = 0; i < state.results.length; i++) {
      html += renderResultCard(state.results[i], i);
    }
    if (state.results.length < state.total) {
      html += '<button class="load-more" data-action="load-more">Weitere laden</button>';
    }
    html += '</div>';
  }
  return html;
}

function renderResultCard(r, idx) {
  var badges = '';
  if (r.is_leading_case) badges += '<span class="badge badge-leading">\u2605 Leitentscheid</span> ';
  if (r.citation_count > 0) badges += '<span class="badge badge-citations">' + escHtml(String(r.citation_count)) + ' Zit.</span> ';
  if (r.legal_area) badges += '<span class="badge badge-area">' + escHtml(r.legal_area) + '</span>';

  var regeste = r.regeste || r.snippet || '';
  if (regeste.length > 200) regeste = regeste.substring(0, 200) + '...';
  var courtName = r.court_name || r.court || '';
  var date = r.date || '';

  return '<div class="result-card">' +
    '<div class="result-header"><div>' +
    '<div class="result-docket">' + escHtml(r.docket_number || r.decision_id) + '</div>' +
    '<div class="result-meta">' + escHtml(date) + ' \u00B7 ' + escHtml(courtName) + '</div>' +
    '</div><div>' + badges + '</div></div>' +
    '<div class="result-regeste">' + escHtml(regeste) + '</div>' +
    '<div class="result-actions">' +
    '<button class="btn btn-insert" data-action="insert" data-idx="' + idx + '">Einf\u00FCgen</button>' +
    '<button class="btn btn-detail" data-action="detail" data-idx="' + idx + '">Volltext</button>' +
    '</div></div>';
}

// Detail View
function renderDetail() {
  var d = state.detail;
  var cb = state.caseBrief;
  if (!d) return '<div class="state-message">Laden...</div>';

  var html = '<a class="back-link" data-action="back">\u2190 Zur\u00FCck zur Suche</a>';
  html += '<h2 style="font-size:16px;font-weight:700;margin:8px 0 4px;">' + escHtml(d.docket_number || d.decision_id) + '</h2>';
  html += '<div style="font-size:11px;color:var(--text-muted);">' + escHtml(d.date || '') + ' \u00B7 ' + escHtml(d.court_name || d.court || '') + '</div>';

  var badges = '';
  if (d.is_leading_case) badges += '<span class="badge badge-leading">\u2605 Leitentscheid</span> ';
  if (d.citation_count) badges += '<span class="badge badge-citations">' + escHtml(String(d.citation_count)) + ' Zitierungen</span> ';
  if (d.legal_area) badges += '<span class="badge badge-area">' + escHtml(d.legal_area) + '</span>';
  if (badges) html += '<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:6px;">' + badges + '</div>';

  if (d.regeste) {
    html += '<div class="section-card"><div class="section-label">Regeste</div>' +
      '<div style="font-size:12px;line-height:1.5;">' + escHtml(d.regeste) + '</div></div>';
  }

  if (cb && cb.erwaegungen && cb.erwaegungen.length) {
    html += '<div class="section-card"><div class="section-label">Erw\u00E4gungen</div>';
    for (var i = 0; i < cb.erwaegungen.length; i++) {
      var e = cb.erwaegungen[i];
      var num = e.number || '?';
      var preview = (e.text || '').substring(0, 80);
      html += '<div class="erwaegung-row">' +
        '<span class="erwaegung-num">E. ' + escHtml(num) + '</span>' +
        '<span class="erwaegung-text">' + escHtml(preview) + '...</span>' +
        '<button class="btn btn-insert" style="font-size:9px;padding:2px 8px;" data-action="insert-ew" data-ew="' + escHtml(num) + '">Einf\u00FCgen</button>' +
        '</div>';
    }
    html += '</div>';
  }

  if (cb && cb.statutes && cb.statutes.length) {
    html += '<div class="section-card"><div class="section-label">Gesetzesartikel</div><div class="pills">';
    for (var j = 0; j < cb.statutes.length; j++) {
      html += '<span class="pill">' + escHtml(cb.statutes[j]) + '</span>';
    }
    html += '</div></div>';
  }

  html += '<button class="btn btn-insert btn-full" style="margin-top:12px;" data-action="insert-main">Einf\u00FCgen</button>';
  return html;
}

// Laws View
function renderLaws() {
  return '<a class="back-link" data-action="back">\u2190 Zur\u00FCck</a>' +
    '<input class="search-bar" id="law-search-input" type="text" placeholder="Gesetzesartikel suchen...">' +
    '<div id="law-results" class="results" style="margin-top:12px;"></div>';
}

// Verify View
function renderVerify() {
  var html = '<a class="back-link" data-action="back">\u2190 Zur\u00FCck</a>';
  html += '<h3 style="font-size:14px;font-weight:700;margin-bottom:8px;">Referenzpr\u00FCfung</h3>';

  if (state.verifyText) {
    html += '<div class="verify-selected"><div class="section-label">Markierter Text</div>' +
      '<div style="font-size:12px;line-height:1.5;">' + escHtml(state.verifyText) + '</div></div>';
  }

  if (state.loading) {
    html += '<div class="state-message">Referenz wird gepr\u00FCft...</div>';
  } else if (state.verifyResult) {
    var v = state.verifyResult;
    var labels = { supports: 'Zutreffend', partial: 'Teilweise zutreffend', contradicts: 'Nicht zutreffend' };
    var icons = { supports: '\u2713', partial: '\u26A0', contradicts: '\u2717' };

    html += '<div class="verdict-card ' + escHtml(v.verdict) + '">' +
      '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' +
      '<div class="verdict-icon ' + escHtml(v.verdict) + '">' + (icons[v.verdict] || '?') + '</div>' +
      '<div style="font-weight:700;font-size:13px;">' + escHtml(labels[v.verdict] || v.verdict) + '</div></div>' +
      '<div style="font-size:12px;line-height:1.5;">' + escHtml(v.explanation || '') + '</div></div>';

    if (v.quote) {
      html += '<div class="section-card"><div class="section-label">Relevante Erw\u00E4gung' +
        (v.relevant_erwaegung ? ' (E. ' + escHtml(v.relevant_erwaegung) + ')' : '') +
        '</div><div style="font-size:12px;line-height:1.5;font-style:italic;">\u00AB' + escHtml(v.quote) + '\u00BB</div></div>';
    }

    var commentBtn = supportsComments()
      ? '<button class="btn btn-insert" style="flex:1;" data-action="insert-comment">Kommentar einf\u00FCgen</button>'
      : '<button class="btn btn-insert" style="flex:1;" data-action="insert-verdict-text">Ergebnis einf\u00FCgen</button>';

    html += '<div style="display:flex;gap:6px;margin-top:10px;">' + commentBtn +
      '<button class="btn btn-detail" style="flex:1;" data-action="verify-fulltext">Volltext</button></div>';
    html += '<div style="text-align:center;font-size:10px;color:var(--text-muted);margin-top:8px;">Eigener API-Key \u00B7 Claude Haiku</div>';
  } else if (state.error) {
    html += renderError();
  }

  return html;
}

// Settings View
function renderSettings() {
  var apiKey = localStorage.getItem('ocl_anthropic_key') || '';
  var previewCitation = formatCitation({ court: 'bge', docket_number: 'BGE 125 III 231', date: '1999-01-01' }, state.lang, '3');

  var html = '<a class="back-link" data-action="back">\u2190 Zur\u00FCck</a>';
  html += '<h3 style="font-size:14px;font-weight:700;margin-bottom:12px;">Einstellungen</h3>';

  html += '<div class="section-card"><div class="section-label">Zitierformat</div>' +
    '<div style="font-size:12px;margin-top:4px;">Vorschau: <strong>' + escHtml(previewCitation) + '</strong></div></div>';

  html += '<div class="section-card"><div class="section-label">Referenzpr\u00FCfung (Tier B)</div>' +
    '<div class="settings-field"><label>Anthropic API Key</label>' +
    '<input type="password" id="api-key-input" value="' + escHtml(apiKey) + '" placeholder="sk-ant-...">' +
    '<div class="hint">Wird nur lokal gespeichert. Nie an unsere Server gesendet.</div></div>' +
    '<div style="display:flex;gap:6px;margin-top:8px;">' +
    '<button class="btn btn-insert" data-action="save-key">Speichern</button>' +
    '<button class="btn btn-detail" data-action="test-key">Testen</button>';
  if (apiKey) html += '<button class="btn btn-detail" data-action="delete-key" style="color:var(--red);">L\u00F6schen</button>';
  html += '</div></div>';

  html += '<div style="margin-top:20px;font-size:11px;color:var(--text-muted);text-align:center;">' +
    '<a href="https://opencaselaw.ch" target="_blank" style="color:var(--blue);">opencaselaw.ch</a> \u00B7 ' +
    '<a href="https://github.com/jonashertner/caselaw-repo-1" target="_blank" style="color:var(--blue);">GitHub</a><br>' +
    'Code: MIT \u00B7 Daten: CC0 1.0</div>';

  return html;
}

// Error rendering
function renderError() {
  if (!state.error) return '';
  if (state.error.type === 'rate_limit') {
    return '<div class="state-message">Zu viele Anfragen.<br>Bitte ' + escHtml(String(state.error.retryAfter)) + 's warten.' +
      '<button class="retry-btn" data-action="retry">Erneut versuchen</button></div>';
  }
  if (state.error.type === 'no_citation' || state.error.type === 'no_selection' || state.error.type === 'decision_not_found') {
    return '<div class="state-message">' + escHtml(state.error.message) + '</div>';
  }
  return '<div class="state-message">Verbindungsfehler.' +
    '<button class="retry-btn" data-action="retry">Erneut versuchen</button></div>';
}

// Event Binding (delegated)
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

  document.getElementById('app').addEventListener('click', async function (e) {
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
      case 'verify-ref':   await startVerify(); break;
      case 'insert-comment': await doInsertComment(); break;
      case 'insert-verdict-text': await doInsertVerdictText(); break;
      case 'verify-fulltext':
        if (state.verifyResult && state.verifyResult._decision) {
          await showDetail(state.verifyResult._decision);
        }
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
  });
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
  state.offset += 20;
  state.loading = true;
  render();
  try {
    var data = await searchDecisions(state.query, { limit: 20, offset: state.offset });
    state.results = state.results.concat(data.results || []);
  } catch (e) {
    state.error = e;
    state.offset -= 20;
  }
  state.loading = false;
  render();
}

async function showDetail(decision) {
  state.view = 'detail';
  state.detail = decision;
  state.caseBrief = null;
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
  render();
}

async function insertCitation(decision, erwaegung) {
  if (!decision) return;
  var text = formatCitation(decision, state.lang, erwaegung);
  try {
    await insertTextAtCursor(text);
  } catch (e) {
    console.error('Insert failed:', e);
  }
}

async function startVerify() {
  var apiKey = localStorage.getItem('ocl_anthropic_key');
  if (!apiKey) {
    state.view = 'settings';
    render();
    return;
  }
  try {
    var selected = await getSelectedText();
    if (!selected || selected.trim().length < 10) {
      state.error = { type: 'no_selection', message: 'Bitte markieren Sie einen Textabschnitt mit einer Entscheidreferenz.' };
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
    var result = await verifyReference(selected, apiKey, state.lang);
    state.verifyResult = result;
  } catch (e) {
    state.error = e;
  }
  state.loading = false;
  render();
}

async function doInsertComment() {
  if (!state.verifyResult) return;
  var v = state.verifyResult;
  var labels = { supports: '\u2713 Zutreffend', partial: '\u26A0 Teilweise', contradicts: '\u2717 Nicht zutreffend' };
  var text = (labels[v.verdict] || v.verdict) + ': ' + (v.explanation || '');
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
  var resultsDiv = document.getElementById('law-results');
  if (!resultsDiv) return;
  resultsDiv.textContent = '';
  var skeleton = document.createElement('div');
  skeleton.className = 'skeleton';
  resultsDiv.appendChild(skeleton);
  try {
    var data = await searchLaws(query, { language: state.lang });
    var items = Array.isArray(data) ? data : (data.results || []);
    resultsDiv.textContent = '';
    if (items.length === 0) {
      resultsDiv.textContent = 'Keine Gesetze gefunden.';
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
      text.textContent = content.length > 300 ? content.substring(0, 300) + '...' : content;
      card.appendChild(text);
      resultsDiv.appendChild(card);
    });
  } catch (e) {
    resultsDiv.textContent = 'Fehler beim Laden.';
  }
}

async function testApiKey() {
  var key = document.getElementById('api-key-input').value;
  if (!key) return;
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
    if (resp.ok) alert('\u2713 API-Key ist g\u00FCltig.');
    else alert('\u2717 API-Key ung\u00FCltig oder abgelaufen.');
  } catch (e) {
    alert('\u2717 Verbindungsfehler zu Anthropic.');
  }
}

// Utilities — HTML entity escaping for XSS prevention
function escHtml(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
