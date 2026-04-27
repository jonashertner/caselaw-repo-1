/**
 * Citation extraction helper for the Word add-in.
 *
 * The only export is extractCitations(text) — a regex-based detector
 * for Swiss case references (BGE / ATF / DTF and federal docket
 * patterns) inside a selected paragraph. All AI-assisted features
 * (Pro reference verification, argument search, document scan) are
 * called via api.js → POST /api/billing/verify on the server side.
 * The add-in iframe never makes LLM calls, never asks users to paste
 * an API key, and never contacts any LLM provider directly.
 *
 * (For the regression guard that asserts this invariant, see
 * tests/web/test_word_addin_no_browser_anthropic.py in the repo.)
 */

var CITATION_PATTERNS = [
  /(BGE|ATF|DTF)\s+(\d+)\s+([IVX]+)\s+(\d+)/g,
  /(\d[A-Z]_\d+\/\d{4})/g,
];

function extractCitations(text) {
  var refs = [];
  for (var i = 0; i < CITATION_PATTERNS.length; i++) {
    var regex = new RegExp(CITATION_PATTERNS[i].source, CITATION_PATTERNS[i].flags);
    var match;
    while ((match = regex.exec(text)) !== null) {
      refs.push(match[0]);
    }
  }
  // Deduplicate, preserve insertion order.
  return refs.filter(function (v, i, a) { return a.indexOf(v) === i; });
}
