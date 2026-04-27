/**
 * Citation formatting module for Swiss court decisions.
 * Pure functions, no dependencies. Works in both Node.js and browser.
 */

var COURT_NAMES = {
  bger:   { de: 'BGer',   fr: 'TF',  it: 'TF',  en: 'BGer' },
  bge:    { de: 'BGE',    fr: 'ATF', it: 'DTF', en: 'BGE' },
  bvger:  { de: 'BVGer',  fr: 'TAF', it: 'TAF', en: 'BVGer' },
  bstger: { de: 'BStGer', fr: 'TPF', it: 'TPF', en: 'BStGer' },
  bpatger:{ de: 'BPatGer',fr: 'TFB', it: 'TFB', en: 'BPatGer' }
};

// Spelled-out (long-form) court names for the federal courts. Used by
// the "long" citation style — independent of i18n.js so citation.js
// stays a pure module the test suite can consume directly.
// For cantonal courts the i18n.js getCourtName() fallback covers the
// long version; this map only provides the federal long names.
var COURT_NAMES_LONG = {
  bger:    { de: 'Bundesgericht',         fr: 'Tribunal fédéral',                     it: 'Tribunale federale',                en: 'Federal Supreme Court' },
  bge:     { de: 'Bundesgericht',         fr: 'Tribunal fédéral',                     it: 'Tribunale federale',                en: 'Federal Supreme Court' },
  bvger:   { de: 'Bundesverwaltungsgericht', fr: 'Tribunal administratif fédéral',   it: 'Tribunale amministrativo federale', en: 'Federal Administrative Court' },
  bstger:  { de: 'Bundesstrafgericht',    fr: 'Tribunal pénal fédéral',               it: 'Tribunale penale federale',         en: 'Federal Criminal Court' },
  bpatger: { de: 'Bundespatentgericht',   fr: 'Tribunal fédéral des brevets',         it: 'Tribunale federale dei brevetti',   en: 'Federal Patent Court' }
};

var ERWAEGUNG_LABEL = {
  de: 'E.',
  fr: 'consid.',
  it: 'consid.',
  en: 'para.'
};

/**
 * Locale-correct pinpoint label for paragraph/Erwägung references.
 * DE → "E."  ·  FR/IT → "consid."  ·  EN → "para."
 *
 * Centralised so every UI surface that previously hard-coded "E. " (the
 * German abbreviation) gets the right label for FR/IT/EN users. This is
 * the *display* label only — the citation produced by formatCitation()
 * already localises correctly via ERWAEGUNG_LABEL.
 */
function pinpointLabel(lang) {
  return ERWAEGUNG_LABEL[lang] || ERWAEGUNG_LABEL.de;
}

var MONTH_NAMES = {
  de: ['Januar','Februar','März','April','Mai','Juni','Juli','August','September','Oktober','November','Dezember'],
  fr: ['janvier','février','mars','avril','mai','juin','juillet','août','septembre','octobre','novembre','décembre'],
  it: ['gennaio','febbraio','marzo','aprile','maggio','giugno','luglio','agosto','settembre','ottobre','novembre','dicembre'],
  en: ['January','February','March','April','May','June','July','August','September','October','November','December']
};

var DATE_PREFIX = {
  de: 'vom',
  fr: 'du',
  it: 'del',
  en: 'of'
};

/**
 * Format a date string (YYYY-MM-DD or ISO) into localized form.
 * DE: "vom 5. April 2013"
 * FR: "du 5 avril 2013"
 * IT: "del 5 aprile 2013"
 * EN: "of 5 April 2013"
 */
function formatDate(dateStr, lang) {
  if (!dateStr) return '';
  var d = new Date(dateStr);
  var day = d.getUTCDate();
  var month = MONTH_NAMES[lang][d.getUTCMonth()];
  var year = d.getUTCFullYear();
  var prefix = DATE_PREFIX[lang];

  if (lang === 'de') {
    return prefix + ' ' + day + '. ' + month + ' ' + year;
  }
  return prefix + ' ' + day + ' ' + month + ' ' + year;
}

/**
 * Detect if a decision is a BGE (published leading case).
 * Checks court_id or docket pattern.
 */
function isBge(decision) {
  if (decision.court_id === 'bge') return true;
  if (decision.court && decision.court.toLowerCase() === 'bge') return true;
  // Check docket pattern: "BGE 125 III 231" or "BGE_125_III_231"
  if (decision.docket && /^BGE[\s_]\d+[\s_][IVX]+[\s_]\d+/.test(decision.docket)) return true;
  return false;
}

/**
 * Extract the BGE reference from a docket string.
 * Input: "BGE 125 III 231" or "BGE_125_III_231"
 * Returns: "125 III 231"
 */
function extractBgeRef(docket) {
  if (!docket) return '';
  var m = docket.match(/^BGE[\s_](\d+)[\s_]([IVX]+)[\s_](\d+)/);
  if (m) return m[1] + ' ' + m[2] + ' ' + m[3];
  return docket.replace(/^BGE[\s_]?/, '');
}

/**
 * Citation style options. Swiss legal practice has several conventions
 * depending on the publication channel (court briefs vs. journal articles
 * vs. footnotes). The add-in lets the user pick once in settings; every
 * insert from then on follows that style.
 *
 *   parenthesised : "(BGer 4A_747/2012 vom 5. April 2013, E. 2)"  ← default
 *   footnote      : "BGer 4A_747/2012 vom 5. April 2013, E. 2"     (no parens)
 *   brief         : "(BGer 4A_747/2012, E. 2)"                     (no date)
 *   long          : "(Bundesgericht, Urteil vom 5. April 2013, 4A_747/2012, E. 2)"
 *
 * For BGE (leading decisions), the date is conventionally omitted
 * regardless of style — they're identified by volume + page, not date.
 */
var CITATION_STYLES = ['parenthesised', 'footnote', 'brief', 'long'];

function _wrap(style, body) {
  return style === 'footnote' ? body : '(' + body + ')';
}

/**
 * Format a citation for a Swiss court decision.
 *
 * Prefers the API-provided citation_string_{lang} if present (verbatim,
 * authoritative — built server-side with full canonical knowledge). For
 * the canonical form we still apply style transforms (parens / no
 * parens / strip date for `brief`).
 *
 * @param {Object} decision   Decision object: docket, date, court_id, ...
 * @param {string} lang       'de' | 'fr' | 'it' | 'en'
 * @param {string} [erwaegung]  Optional Erwägung number (e.g. "3" or "2.1")
 * @param {string} [style]    One of CITATION_STYLES. Defaults to
 *                            localStorage('ocl_citation_style') or
 *                            'parenthesised'.
 */
function formatCitation(decision, lang, erwaegung, style) {
  lang = lang || 'de';
  style = style || _readStylePref();
  if (CITATION_STYLES.indexOf(style) < 0) style = 'parenthesised';

  // Canonical path — API gave us a verbatim citation string.
  var apiKey = lang === 'en' ? 'citation_string_de' : 'citation_string_' + lang;
  var canonical = decision[apiKey] || decision.citation_string_de;
  if (canonical) {
    var c = canonical;
    if (style === 'brief') {
      // Drop the "vom 5. April 2013" / "du 5 avril 2013" / etc. tail.
      c = c.replace(/\s+(?:vom|du|del|of)\s+\d.*$/i, '');
    }
    if (erwaegung) c += ', ' + ERWAEGUNG_LABEL[lang] + ' ' + erwaegung;
    return _wrap(style, c);
  }

  // Fallback path — reconstruct from structured fields.
  var docket = decision.docket || decision.docket_number || '';
  var date = decision.date || decision.decision_date || '';
  var courtId = (decision.court_id || decision.court || '').toLowerCase();
  var d = { docket: docket, date: date, court_id: courtId, court: courtId };

  var parts = [];
  if (isBge(d)) {
    // BGE format never carries a date (volume+page is the identifier).
    var ref = extractBgeRef(docket);
    parts.push(COURT_NAMES.bge[lang] + ' ' + ref);
  } else {
    // Non-BGE: court name + docket [+ date]
    var courtName;
    if (style === 'long') {
      // Prefer the spelled-out federal court name; fall back to i18n.js
      // getCourtName() (covers cantonal courts in the browser);
      // last resort: the abbreviated COURT_NAMES entry.
      if (COURT_NAMES_LONG[courtId]) {
        courtName = COURT_NAMES_LONG[courtId][lang];
      } else if (typeof getCourtName === 'function') {
        courtName = getCourtName(courtId, lang);
      } else if (COURT_NAMES[courtId]) {
        courtName = COURT_NAMES[courtId][lang];
      } else {
        courtName = courtId || '';
      }
    } else if (COURT_NAMES[courtId]) {
      courtName = COURT_NAMES[courtId][lang];
    } else if (typeof getCourtName === 'function') {
      courtName = getCourtName(courtId, lang);
    } else {
      courtName = courtId || '';
    }

    if (style === 'long') {
      // "Bundesgericht, Urteil vom 5. April 2013, 4A_747/2012"
      var URTEIL = { de: 'Urteil', fr: 'Arrêt', it: 'Sentenza', en: 'Judgment' };
      var seg = courtName + ', ' + URTEIL[lang];
      if (date) seg += ' ' + formatDate(date, lang);
      seg += ', ' + docket;
      parts.push(seg);
    } else {
      var seg2 = courtName + ' ' + docket;
      if (date && style !== 'brief') seg2 += ' ' + formatDate(date, lang);
      parts.push(seg2);
    }
  }

  if (erwaegung) parts.push(ERWAEGUNG_LABEL[lang] + ' ' + erwaegung);
  return _wrap(style, parts.join(', '));
}

function _readStylePref() {
  try {
    var s = (typeof localStorage !== 'undefined') ? localStorage.getItem('ocl_citation_style') : null;
    return s || 'parenthesised';
  } catch (e) { return 'parenthesised'; }
}

// Export for both Node.js and browser
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    COURT_NAMES: COURT_NAMES,
    ERWAEGUNG_LABEL: ERWAEGUNG_LABEL,
    MONTH_NAMES: MONTH_NAMES,
    DATE_PREFIX: DATE_PREFIX,
    formatDate: formatDate,
    formatCitation: formatCitation,
    isBge: isBge,
    extractBgeRef: extractBgeRef,
    pinpointLabel: pinpointLabel,
    CITATION_STYLES: CITATION_STYLES
  };
}
