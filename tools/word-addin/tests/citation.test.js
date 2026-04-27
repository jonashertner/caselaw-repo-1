/**
 * Tests for citation.js — Swiss court decision citation formatting.
 * Uses simple assert (no test framework). Run with: node citation.test.js
 */

var assert = require('assert');
var citation = require('../js/citation.js');

var formatCitation = citation.formatCitation;
var formatDate = citation.formatDate;

var passed = 0;
var failed = 0;

function test(name, fn) {
  try {
    fn();
    passed++;
    console.log('  PASS: ' + name);
  } catch (e) {
    failed++;
    console.log('  FAIL: ' + name);
    console.log('        ' + e.message);
  }
}

// ============================================================
// Test fixtures
// ============================================================

var bgeDecision = {
  docket: 'BGE 125 III 231',
  date: '1999-06-15',
  court_id: 'bge'
};

var bgerDecision = {
  docket: '4A_747/2012',
  date: '2013-04-05',
  court_id: 'bger'
};

var bvgerDecision = {
  docket: 'A-1234/2020',
  date: '2021-03-15',
  court_id: 'bvger'
};

var bstgerDecision = {
  docket: 'SK.2019.42',
  date: '2020-01-20',
  court_id: 'bstger'
};

var bpatgerDecision = {
  docket: 'O2018_001',
  date: '2018-11-30',
  court_id: 'bpatger'
};

// ============================================================
// BGE citations — all 4 languages
// ============================================================

console.log('\nBGE citations (no Erwaegung):');

test('BGE DE — no E.', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'de'), '(BGE 125 III 231)');
});

test('BGE FR — ATF prefix, no E.', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'fr'), '(ATF 125 III 231)');
});

test('BGE IT — DTF prefix, no E.', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'it'), '(DTF 125 III 231)');
});

test('BGE EN — BGE prefix, no E.', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'en'), '(BGE 125 III 231)');
});

console.log('\nBGE citations (with Erwaegung):');

test('BGE DE — with E. 3', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'de', '3'), '(BGE 125 III 231, E. 3)');
});

test('BGE FR — with consid. 3', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'fr', '3'), '(ATF 125 III 231, consid. 3)');
});

test('BGE IT — with consid. 3', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'it', '3'), '(DTF 125 III 231, consid. 3)');
});

test('BGE EN — with para. 3', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'en', '3'), '(BGE 125 III 231, para. 3)');
});

console.log('\nBGE citations (with sub-section Erwaegung):');

test('BGE DE — with E. 2.1', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'de', '2.1'), '(BGE 125 III 231, E. 2.1)');
});

test('BGE FR — with consid. 2.1', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'fr', '2.1'), '(ATF 125 III 231, consid. 2.1)');
});

test('BGE IT — with consid. 2.1', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'it', '2.1'), '(DTF 125 III 231, consid. 2.1)');
});

test('BGE EN — with para. 2.1', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'en', '2.1'), '(BGE 125 III 231, para. 2.1)');
});

// ============================================================
// BGer citations — all 4 languages
// ============================================================

console.log('\nBGer citations (no Erwaegung):');

test('BGer DE — no E.', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'de'), '(BGer 4A_747/2012 vom 5. April 2013)');
});

test('BGer FR — TF, no E.', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'fr'), '(TF 4A_747/2012 du 5 avril 2013)');
});

test('BGer IT — TF, no E.', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'it'), '(TF 4A_747/2012 del 5 aprile 2013)');
});

test('BGer EN — BGer, no E.', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'en'), '(BGer 4A_747/2012 of 5 April 2013)');
});

console.log('\nBGer citations (with Erwaegung):');

test('BGer DE — with E. 2', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'de', '2'), '(BGer 4A_747/2012 vom 5. April 2013, E. 2)');
});

test('BGer FR — with consid. 2', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'fr', '2'), '(TF 4A_747/2012 du 5 avril 2013, consid. 2)');
});

test('BGer IT — with consid. 2', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'it', '2'), '(TF 4A_747/2012 del 5 aprile 2013, consid. 2)');
});

test('BGer EN — with para. 2', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'en', '2'), '(BGer 4A_747/2012 of 5 April 2013, para. 2)');
});

console.log('\nBGer citations (with sub-section Erwaegung):');

test('BGer DE — with E. 2.1', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'de', '2.1'), '(BGer 4A_747/2012 vom 5. April 2013, E. 2.1)');
});

test('BGer FR — with consid. 2.1', function() {
  assert.strictEqual(formatCitation(bgerDecision, 'fr', '2.1'), '(TF 4A_747/2012 du 5 avril 2013, consid. 2.1)');
});

// ============================================================
// BVGer citations — all 4 languages
// ============================================================

console.log('\nBVGer citations (no Erwaegung):');

test('BVGer DE — no E.', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'de'), '(BVGer A-1234/2020 vom 15. März 2021)');
});

test('BVGer FR — TAF, no E.', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'fr'), '(TAF A-1234/2020 du 15 mars 2021)');
});

test('BVGer IT — TAF, no E.', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'it'), '(TAF A-1234/2020 del 15 marzo 2021)');
});

test('BVGer EN — BVGer, no E.', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'en'), '(BVGer A-1234/2020 of 15 March 2021)');
});

console.log('\nBVGer citations (with Erwaegung):');

test('BVGer DE — with E. 5', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'de', '5'), '(BVGer A-1234/2020 vom 15. März 2021, E. 5)');
});

test('BVGer FR — with consid. 5', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'fr', '5'), '(TAF A-1234/2020 du 15 mars 2021, consid. 5)');
});

test('BVGer IT — with consid. 5', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'it', '5'), '(TAF A-1234/2020 del 15 marzo 2021, consid. 5)');
});

test('BVGer EN — with para. 5', function() {
  assert.strictEqual(formatCitation(bvgerDecision, 'en', '5'), '(BVGer A-1234/2020 of 15 March 2021, para. 5)');
});

// ============================================================
// BStGer citations — all 4 languages
// ============================================================

console.log('\nBStGer citations (no Erwaegung):');

test('BStGer DE — no E.', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'de'), '(BStGer SK.2019.42 vom 20. Januar 2020)');
});

test('BStGer FR — TPF, no E.', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'fr'), '(TPF SK.2019.42 du 20 janvier 2020)');
});

test('BStGer IT — TPF, no E.', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'it'), '(TPF SK.2019.42 del 20 gennaio 2020)');
});

test('BStGer EN — BStGer, no E.', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'en'), '(BStGer SK.2019.42 of 20 January 2020)');
});

console.log('\nBStGer citations (with Erwaegung):');

test('BStGer DE — with E. 4', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'de', '4'), '(BStGer SK.2019.42 vom 20. Januar 2020, E. 4)');
});

test('BStGer FR — with consid. 4', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'fr', '4'), '(TPF SK.2019.42 du 20 janvier 2020, consid. 4)');
});

test('BStGer IT — with consid. 4', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'it', '4'), '(TPF SK.2019.42 del 20 gennaio 2020, consid. 4)');
});

test('BStGer EN — with para. 4', function() {
  assert.strictEqual(formatCitation(bstgerDecision, 'en', '4'), '(BStGer SK.2019.42 of 20 January 2020, para. 4)');
});

// ============================================================
// BPatGer citations
// ============================================================

console.log('\nBPatGer citations:');

test('BPatGer DE — no E.', function() {
  assert.strictEqual(formatCitation(bpatgerDecision, 'de'), '(BPatGer O2018_001 vom 30. November 2018)');
});

test('BPatGer FR — TFB', function() {
  assert.strictEqual(formatCitation(bpatgerDecision, 'fr'), '(TFB O2018_001 du 30 novembre 2018)');
});

test('BPatGer IT — TFB', function() {
  assert.strictEqual(formatCitation(bpatgerDecision, 'it'), '(TFB O2018_001 del 30 novembre 2018)');
});

test('BPatGer EN — BPatGer', function() {
  assert.strictEqual(formatCitation(bpatgerDecision, 'en'), '(BPatGer O2018_001 of 30 November 2018)');
});

// ============================================================
// Date formatting
// ============================================================

console.log('\nDate formatting:');

test('formatDate DE', function() {
  assert.strictEqual(formatDate('2013-04-05', 'de'), 'vom 5. April 2013');
});

test('formatDate FR', function() {
  assert.strictEqual(formatDate('2013-04-05', 'fr'), 'du 5 avril 2013');
});

test('formatDate IT', function() {
  assert.strictEqual(formatDate('2013-04-05', 'it'), 'del 5 aprile 2013');
});

test('formatDate EN', function() {
  assert.strictEqual(formatDate('2013-04-05', 'en'), 'of 5 April 2013');
});

test('formatDate — empty string returns empty', function() {
  assert.strictEqual(formatDate('', 'de'), '');
});

test('formatDate — null returns empty', function() {
  assert.strictEqual(formatDate(null, 'de'), '');
});

// ============================================================
// Edge cases
// ============================================================

console.log('\nEdge cases:');

test('BGE with underscore docket format', function() {
  var dec = { docket: 'BGE_134_V_231', court_id: 'bge' };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGE 134 V 231)');
});

test('BGE detected by docket pattern (no court_id)', function() {
  var dec = { docket: 'BGE 140 II 194', date: '2014-05-01' };
  assert.strictEqual(formatCitation(dec, 'fr'), '(ATF 140 II 194)');
});

test('Default language is DE', function() {
  assert.strictEqual(formatCitation(bgeDecision), '(BGE 125 III 231)');
});

test('Unknown court_id uses raw value', function() {
  var dec = { docket: 'XY-123/2020', date: '2020-06-15', court_id: 'zh_obergericht' };
  assert.strictEqual(formatCitation(dec, 'de'), '(zh_obergericht XY-123/2020 vom 15. Juni 2020)');
});

test('BGE no date — date is omitted (BGE format)', function() {
  // BGE citations never include date regardless
  var dec = { docket: 'BGE 125 III 231', court_id: 'bge' };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGE 125 III 231)');
});

test('Non-BGE with no date — no date segment', function() {
  var dec = { docket: '4A_747/2012', court_id: 'bger' };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGer 4A_747/2012)');
});

test('Deep sub-section E. 3.2.1', function() {
  assert.strictEqual(formatCitation(bgeDecision, 'de', '3.2.1'), '(BGE 125 III 231, E. 3.2.1)');
});

// ============================================================
// API canonical citation strings — verbatim, not reconstructed
// ============================================================

console.log('\nAPI-canonical citations (citation_string_{lang} fields):');

test('API canonical DE — used verbatim', function() {
  var dec = { citation_string_de: 'BGE 140 III 86', citation_string_fr: 'ATF 140 III 86', citation_string_it: 'DTF 140 III 86' };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGE 140 III 86)');
});

test('API canonical FR — uses citation_string_fr', function() {
  var dec = { citation_string_de: 'BGE 140 III 86', citation_string_fr: 'ATF 140 III 86', citation_string_it: 'DTF 140 III 86' };
  assert.strictEqual(formatCitation(dec, 'fr'), '(ATF 140 III 86)');
});

test('API canonical EN falls back to DE', function() {
  var dec = { citation_string_de: 'BGE 140 III 86', citation_string_fr: 'ATF 140 III 86' };
  assert.strictEqual(formatCitation(dec, 'en'), '(BGE 140 III 86)');
});

test('API canonical with Erwägung suffix — DE', function() {
  var dec = { citation_string_de: 'BGE 140 III 86' };
  assert.strictEqual(formatCitation(dec, 'de', '3.2'), '(BGE 140 III 86, E. 3.2)');
});

test('API canonical with Erwägung suffix — FR', function() {
  var dec = { citation_string_fr: 'ATF 140 III 86', citation_string_de: 'BGE 140 III 86' };
  assert.strictEqual(formatCitation(dec, 'fr', '3.2'), '(ATF 140 III 86, consid. 3.2)');
});

test('API canonical takes precedence over local reconstruction', function() {
  // Even with local fields available, the API string wins.
  var dec = {
    citation_string_de: 'BGer 4A_747/2012 vom 5. April 2013',
    docket: 'WRONG_LOCAL', date: '2099-01-01', court_id: 'bger',
  };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGer 4A_747/2012 vom 5. April 2013)');
});

test('No API field — local reconstruction still works', function() {
  // Backward compatible: when API fields are absent, falls back to old logic.
  var dec = { docket: 'BGE 125 III 231', court_id: 'bge' };
  assert.strictEqual(formatCitation(dec, 'de'), '(BGE 125 III 231)');
});

// ============================================================
// Court name mapping
// ============================================================

console.log('\nCourt name mapping:');

test('COURT_NAMES has all 5 courts', function() {
  assert.deepStrictEqual(Object.keys(citation.COURT_NAMES).sort(),
    ['bge', 'bger', 'bpatger', 'bstger', 'bvger']);
});

test('COURT_NAMES bger', function() {
  assert.deepStrictEqual(citation.COURT_NAMES.bger,
    { de: 'BGer', fr: 'TF', it: 'TF', en: 'BGer' });
});

test('COURT_NAMES bge', function() {
  assert.deepStrictEqual(citation.COURT_NAMES.bge,
    { de: 'BGE', fr: 'ATF', it: 'DTF', en: 'BGE' });
});

test('ERWAEGUNG_LABEL values', function() {
  assert.deepStrictEqual(citation.ERWAEGUNG_LABEL,
    { de: 'E.', fr: 'consid.', it: 'consid.', en: 'para.' });
});

// ============================================================
// Citation style picker
// ============================================================

console.log('\n=== Citation style picker ===');

test('default style is parenthesised (matches existing behaviour)', function () {
  var s = formatCitation(bgerDecision, 'de', '2');
  assert.strictEqual(s, '(BGer 4A_747/2012 vom 5. April 2013, E. 2)');
});

test('footnote style strips the parentheses', function () {
  var s = formatCitation(bgerDecision, 'de', '2', 'footnote');
  assert.strictEqual(s, 'BGer 4A_747/2012 vom 5. April 2013, E. 2');
});

test('brief style drops the date, keeps parentheses', function () {
  var s = formatCitation(bgerDecision, 'de', '2', 'brief');
  assert.strictEqual(s, '(BGer 4A_747/2012, E. 2)');
});

test('long style spells out the court + adds Urteil/Arrêt/Sentenza/Judgment', function () {
  var de = formatCitation(bgerDecision, 'de', '2', 'long');
  assert.ok(de.indexOf('Bundesgericht') >= 0, 'long DE should include Bundesgericht: ' + de);
  assert.ok(de.indexOf('Urteil') >= 0, 'long DE should include Urteil: ' + de);
  var fr = formatCitation(bgerDecision, 'fr', '2', 'long');
  assert.ok(fr.indexOf('Tribunal fédéral') >= 0, 'long FR should include Tribunal fédéral: ' + fr);
  assert.ok(fr.indexOf('Arrêt') >= 0, 'long FR should include Arrêt: ' + fr);
});

test('BGE always omits date regardless of style', function () {
  ['parenthesised', 'footnote', 'brief', 'long'].forEach(function (style) {
    var s = formatCitation(bgeDecision, 'de', '3', style);
    assert.ok(!/\d{4}/.test(s) || /125 III 231/.test(s), 'BGE should not include a year in style ' + style + ': ' + s);
  });
});

test('canonical citation_string respects brief style by stripping date suffix', function () {
  var d = { citation_string_de: 'BGer 4A_747/2012 vom 5. April 2013', court: 'bger' };
  assert.strictEqual(formatCitation(d, 'de', '2', 'brief'), '(BGer 4A_747/2012, E. 2)');
});

test('unknown style falls back to parenthesised', function () {
  var s = formatCitation(bgerDecision, 'de', '2', 'no-such-style');
  assert.strictEqual(s, '(BGer 4A_747/2012 vom 5. April 2013, E. 2)');
});

test('CITATION_STYLES is exported', function () {
  assert.deepStrictEqual(citation.CITATION_STYLES, ['parenthesised', 'footnote', 'brief', 'long']);
});

// ============================================================
// Summary
// ============================================================

console.log('\n' + '='.repeat(50));
console.log('Results: ' + passed + ' passed, ' + failed + ' failed');
console.log('='.repeat(50));

if (failed > 0) {
  process.exit(1);
}
