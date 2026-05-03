/**
 * Client-side PII redaction for the Pro check-cites flow.
 *
 * The two Pro endpoints — POST /billing/verify and POST /attest —
 * forward selected text or the full document to the server-side LLM
 * for citation verification. Law-firm documents routinely contain
 * client names, AHV/AVS numbers, IBANs, addresses, etc. that must
 * never leave the lawyer's machine in the clear.
 *
 * `redactPII(text)` runs Swiss-aware regex matching, replaces every
 * detected PII span with a typed placeholder ([NAME_1], [AHV_1], …)
 * and returns the redacted text + a per-call replacement map. The
 * LLM only ever sees the placeholders; legal citations (BGE, BGer,
 * statute refs, dockets) are NOT touched, so citation-checking is
 * unaffected. `unredact(text, replacements)` reverses the mapping —
 * useful when displaying server-returned annotated text back to the
 * user with original PII restored.
 *
 * Privacy invariant: this module is the ONLY place that defines what
 * counts as PII. If you add a new field to a Pro-bound payload,
 * pipe it through redactPII first.
 *
 * Tests: tests/redact.test.js (run with `node tests/redact.test.js`).
 */

'use strict';

/* Patterns ordered by specificity. Earlier patterns "win" overlapping
   regions because we de-overlap left-to-right after sorting. Keep
   structured-ID patterns (AHV, IBAN, CHE) before free-text ones
   (NAME, ADDRESS) so an AHV inside a sentence doesn't get partially
   eaten by the address pattern. */
var PATTERNS = [
  {
    /* TLD constrained to LOWERCASE letters: the case-sensitivity of
       [a-z]{2,} makes "info@x.chHerr" stop at ".ch" because the next
       character (uppercase 'H') simply doesn't match the class. \b
       handles plain end-of-word/string for normal cases. */
    type: 'EMAIL',
    regex: /[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[a-z]{2,}\b/g,
  },
  {
    /* Swiss social-security number: 756.XXXX.XXXX.XX (or with spaces). */
    type: 'AHV',
    regex: /\b756[.\s]\d{4}[.\s]\d{4}[.\s]\d{2}\b/g,
  },
  {
    /* Swiss IBAN: CH + 2 check digits + 17 alphanumerics, formatted in
       4-digit groups. Always 21 chars total without spaces. */
    type: 'IBAN',
    regex: /\bCH\d{2}(?:\s?[A-Z0-9]{4}){4}\s?[A-Z0-9]\b/g,
  },
  {
    /* Swiss company UID: CHE-XXX.XXX.XXX (also accept hyphenated). */
    type: 'CHE',
    regex: /\bCHE[\-\s]?\d{3}[.\-\s]?\d{3}[.\-\s]?\d{3}\b/g,
  },
  {
    /* Swiss phone: +41 79 123 45 67  /  +41-79-123-45-67  /  079 123 45 67.
       Boundary keeps it from eating into adjacent digits. */
    type: 'PHONE',
    regex: /(?:\+41[\s\-]?\(?0?\)?[\s\-]?\d{2}|\b0\d{2})[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b/g,
  },
  {
    /* DOB only when explicitly anchored to a birth verb / asterisk —
       avoids redacting random dates that might be filing dates etc. */
    type: 'DOB',
    regex: /(?:\bgeb(?:oren)?\.?\s+am\s+|\bnée?\s+le\s+|\bnato\s+il\s+|\*\s*)\d{1,2}\.\d{1,2}\.(?:19|20)\d{2}/gi,
  },
  {
    /* Street + number, all four official languages.
       Matches "Bahnhofstrasse 12", "Rue du Rhône 65", "Via Pretorio 7",
       "Place de la Gare 4". The charset has to cover every Swiss-French
       and Swiss-Italian accented letter (ô, î, û, ê, ñ in addition to
       ä/ö/ü/é/à/è/ç) — otherwise streets like "Rue du Rhône" silently
       leak. */
    type: 'ADDRESS',
    regex: /\b(?:[A-ZÄÖÜ][A-Za-zäöüéàèçôîûêñÄÖÜÉÀÈÇÔÎÛÊÑ\-]{2,}(?:strasse|gasse|weg|platz|allee|str\.)|(?:Rue|Avenue|Boulevard|Chemin|Place|Route|Via|Piazza|Viale|Vicolo)(?:\s+(?:de|du|des|de\s+la|del|della|delle|dei)?)?\s+[A-Za-zäöüéàèçôîûêñÄÖÜÉÀÈÇÔÎÛÊÑ\-]{2,}(?:\s+[A-Za-zäöüéàèçôîûêñÄÖÜÉÀÈÇÔÎÛÊÑ\-]+){0,3})\s+\d+[a-z]?\b/g,
  },
  {
    /* PLZ + city: CH-8001 Zürich  /  8001 Zürich  /  1003 Lausanne. */
    type: 'POSTAL',
    regex: /\b(?:CH[\-\s])?\d{4}\s+[A-ZÄÖÜ][A-Za-zäöüéàèç\-]+(?:[\s\-][A-ZÄÖÜ][A-Za-zäöüéàèç\-]+){0,2}\b/g,
  },
  {
    /* Title-anchored personal names. Avoids false positives by
       requiring a leading honorific or professional title.

       Title list covers all four official languages:
         DE: Herr, Frau, Hr., Fr., Dr., Prof.
         FR: M., Mme, Mlle, Me, Maître
         IT: Sig., Sig.ra, Avv., Avvocato
       Counter `{0,3}` (was `{1,3}`) lets a single surname after the
       title match — common form in pleadings ("Hr. Müller"). */
    type: 'NAME',
    regex: /(?:Herr|Frau|Hr\.|Fr\.|Me\.?|Maître|Mme|Mlle|M\.|Sig\.(?:ra)?|Avv\.|Avvocato|Dr\.|Prof\.)\s+(?:Dr\.\s+|Prof\.\s+|med\.\s+|iur\.\s+)?[A-ZÄÖÜ][a-zäöüéàèçôîûêñ\-]+(?:\s+[A-ZÄÖÜ][a-zäöüéàèçôîûêñ\-]+){0,3}\b/g,
  },
];

var PII_TYPES = (function () {
  var t = {};
  for (var i = 0; i < PATTERNS.length; i++) t[PATTERNS[i].type] = PATTERNS[i].type;
  return t;
})();

/* Collect every match for one pattern using replace-callback. The
   callback returns the original substring (string is left unchanged)
   while we record (start, end, original) into `out`. We use
   replace-callback rather than the regex iterator method to keep the
   code straightforward across older WebView versions. */
function collectMatches(text, spec, sink) {
  var rx = new RegExp(spec.regex.source, spec.regex.flags);
  text.replace(rx, function (match, /* …captures, */ offset) {
    /* `offset` is always the second-to-last argument when there are
       no capture groups, but defensive — in our patterns the only
       capture-bearing one is DOB, where offset is still computable
       via arguments[arguments.length - 2]. */
    var off = (typeof offset === 'number') ? offset : arguments[arguments.length - 2];
    sink.push({
      type: spec.type,
      start: off,
      end: off + match.length,
      original: match,
      priority: spec.__priority,
    });
    return match;
  });
}

/* Shape: { redacted, replacements, summary }. Empty input returns the
   empty result. */
function redactPII(text /*, options */) {
  if (text == null || text === '') {
    return { redacted: '', replacements: [], summary: { byType: {}, total: 0 } };
  }

  var allMatches = [];
  for (var p = 0; p < PATTERNS.length; p++) {
    PATTERNS[p].__priority = p;
    collectMatches(text, PATTERNS[p], allMatches);
  }

  /* Resolve overlaps: sort by start ascending, then by priority (lower
     = more specific = wins). Keep a running "lastEnd" cursor and drop
     any match that begins before it. */
  allMatches.sort(function (a, b) {
    if (a.start !== b.start) return a.start - b.start;
    if (a.priority !== b.priority) return a.priority - b.priority;
    return b.end - a.end; /* prefer the longer span at same start */
  });

  var keep = [];
  var lastEnd = -1;
  for (var i = 0; i < allMatches.length; i++) {
    if (allMatches[i].start >= lastEnd) {
      keep.push(allMatches[i]);
      lastEnd = allMatches[i].end;
    }
  }

  var replacements = [];
  var counters = {};
  var out = '';
  var pos = 0;
  for (var k = 0; k < keep.length; k++) {
    var hit = keep[k];
    counters[hit.type] = (counters[hit.type] || 0) + 1;
    var placeholder = '[' + hit.type + '_' + counters[hit.type] + ']';
    out += text.slice(pos, hit.start) + placeholder;
    replacements.push({
      type: hit.type,
      original: hit.original,
      placeholder: placeholder,
      start: hit.start,
      end: hit.end,
    });
    pos = hit.end;
  }
  out += text.slice(pos);

  return {
    redacted: out,
    replacements: replacements,
    summary: { byType: counters, total: replacements.length },
  };
}

/* Reverse the mapping for displaying server output to the user. */
function unredact(text, replacements) {
  if (!text || !replacements || replacements.length === 0) return text || '';
  /* Sort by placeholder length DESC so [NAME_10] is replaced before
     [NAME_1] (otherwise "[NAME_1]0" would result). */
  var sorted = replacements.slice().sort(function (a, b) {
    return b.placeholder.length - a.placeholder.length;
  });
  var out = text;
  for (var i = 0; i < sorted.length; i++) {
    var r = sorted[i];
    out = out.split(r.placeholder).join(r.original);
  }
  return out;
}

/* Human-readable summary for the UI: "3 names, 1 AHV, 2 emails redacted". */
function formatSummary(summary, lang) {
  if (!summary || !summary.total) return '';
  lang = lang || 'de';
  var labels = {
    de: { EMAIL: 'E-Mail', AHV: 'AHV-Nr.', IBAN: 'IBAN', CHE: 'UID', PHONE: 'Telefon', DOB: 'Geburtsdatum', ADDRESS: 'Adresse', POSTAL: 'Ort/PLZ', NAME: 'Name' },
    fr: { EMAIL: 'e-mail', AHV: 'no AVS',  IBAN: 'IBAN', CHE: 'IDE', PHONE: 'téléphone', DOB: 'date de naissance', ADDRESS: 'adresse', POSTAL: 'NPA/lieu', NAME: 'nom' },
    it: { EMAIL: 'e-mail', AHV: 'no AVS',  IBAN: 'IBAN', CHE: 'IDI', PHONE: 'telefono',  DOB: 'data di nascita',   ADDRESS: 'indirizzo', POSTAL: 'NAP/località', NAME: 'nome' },
    en: { EMAIL: 'email',  AHV: 'AVS no.', IBAN: 'IBAN', CHE: 'UID', PHONE: 'phone',     DOB: 'date of birth',     ADDRESS: 'address', POSTAL: 'PC/city', NAME: 'name' },
  };
  var dict = labels[lang] || labels.en;
  var parts = [];
  for (var key in summary.byType) {
    if (Object.prototype.hasOwnProperty.call(summary.byType, key)) {
      var n = summary.byType[key];
      var lbl = dict[key] || key;
      parts.push(n + '× ' + lbl);
    }
  }
  return parts.join(', ');
}

/* Browser global. */
if (typeof window !== 'undefined') {
  window.redactPII = redactPII;
  window.unredactPII = unredact;
  window.PII_TYPES = PII_TYPES;
  window.formatPIISummary = formatSummary;
}

/* Node test runner. */
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    redactPII: redactPII,
    unredact: unredact,
    PII_TYPES: PII_TYPES,
    formatSummary: formatSummary,
    PATTERNS: PATTERNS,
  };
}
