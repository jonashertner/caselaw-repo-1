/**
 * Citation extraction helper for the Word add-in.
 *
 * Historical note: this file used to contain a "Tier B" verification path
 * that called the Anthropic Messages API directly from the browser using
 * the user's personal API key. That path was removed because:
 *   - it required users to paste their own LLM key into a browser
 *     (security liability — leaks via DevTools, extensions, screenshots),
 *   - it bypassed our billing / auth boundary,
 *   - the Pro flow (verifyReferencePro() in api.js, calling
 *     POST /api/billing/verify) is now the canonical, server-side,
 *     license-based verification path.
 *
 * What remains here is the lightweight citation regex used by app.js to
 * detect Swiss case references in selected paragraphs. The file keeps
 * its existing name so the <script src="js/verify.js"> tag in
 * index.html continues to load it without any markup change.
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
