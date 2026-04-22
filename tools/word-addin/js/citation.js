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

var ERWAEGUNG_LABEL = {
  de: 'E.',
  fr: 'consid.',
  it: 'consid.',
  en: 'para.'
};

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
 * Format a citation for a Swiss court decision.
 *
 * Prefers the API-provided citation_string_{lang} if present (verbatim,
 * authoritative — built server-side with full canonical knowledge).
 * Falls back to local reconstruction only when the API field is missing.
 *
 * @param {Object} decision - Decision object with at least: docket, date, court_id
 * @param {string} lang - Language code: 'de', 'fr', 'it', 'en'
 * @param {string} [erwaegung] - Optional Erwägung number (e.g. "3" or "2.1")
 * @returns {string} Formatted citation, e.g. "(BGE 125 III 231, E. 3)"
 */
function formatCitation(decision, lang, erwaegung) {
  lang = lang || 'de';

  // Prefer canonical citation string from API (anti-hallucination contract).
  // EN falls back to DE since the API only emits de/fr/it.
  var apiKey = lang === 'en' ? 'citation_string_de' : 'citation_string_' + lang;
  var canonical = decision[apiKey] || decision.citation_string_de;
  if (canonical) {
    if (erwaegung) canonical += ', ' + ERWAEGUNG_LABEL[lang] + ' ' + erwaegung;
    return '(' + canonical + ')';
  }

  var parts = [];

  // Normalize field names (API uses docket_number/decision_date/court, module uses docket/date/court_id)
  var docket = decision.docket || decision.docket_number || '';
  var date = decision.date || decision.decision_date || '';
  var courtId = (decision.court_id || decision.court || '').toLowerCase();
  // Patch decision for isBge/extractBgeRef
  var d = { docket: docket, date: date, court_id: courtId, court: courtId };

  if (isBge(d)) {
    // BGE format: replace "BGE" with language-appropriate prefix, no date
    var ref = extractBgeRef(docket);
    var prefix = COURT_NAMES.bge[lang];
    parts.push(prefix + ' ' + ref);
  } else {
    // Non-BGE format: court name + docket + date
    var courtName;
    if (COURT_NAMES[courtId]) {
      courtName = COURT_NAMES[courtId][lang];
    } else if (typeof getCourtName === 'function') {
      courtName = getCourtName(courtId, lang);
    } else {
      courtName = courtId || '';
    }
    var citation = courtName + ' ' + docket;
    if (date) {
      citation += ' ' + formatDate(date, lang);
    }
    parts.push(citation);
  }

  // Append Erwägung if provided
  if (erwaegung) {
    parts.push(ERWAEGUNG_LABEL[lang] + ' ' + erwaegung);
  }

  return '(' + parts.join(', ') + ')';
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
    extractBgeRef: extractBgeRef
  };
}
