/**
 * Tests for redact.js — Swiss-aware PII redaction for the Pro
 * citation-checking flow. Run with: node tests/redact.test.js
 *
 * Two invariants we hold the implementation to:
 *   1. NO false positives on legal citations (BGE / BGer / dockets /
 *      Art. X). The whole point of the Pro feature is to verify these,
 *      so they MUST survive redaction unchanged.
 *   2. Round-trip: unredact(redact(x).redacted, redact(x).replacements)
 *      reconstructs `x` byte-for-byte.
 */

var assert = require('assert');
var r = require('../js/redact.js');

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
    console.log('        ' + (e.message || e));
  }
}

// ── Empty / null inputs ───────────────────────────────────────────────

console.log('\nEmpty input handling:');

test('empty string returns empty result', function () {
  var out = r.redactPII('');
  assert.strictEqual(out.redacted, '');
  assert.strictEqual(out.replacements.length, 0);
  assert.strictEqual(out.summary.total, 0);
});

test('null returns empty result without throwing', function () {
  var out = r.redactPII(null);
  assert.strictEqual(out.redacted, '');
  assert.strictEqual(out.summary.total, 0);
});

test('text with no PII passes through unchanged', function () {
  var src = 'Das Gericht entschied am Donnerstag.';
  var out = r.redactPII(src);
  assert.strictEqual(out.redacted, src);
  assert.strictEqual(out.summary.total, 0);
});

// ── Per-pattern coverage ──────────────────────────────────────────────

console.log('\nEMAIL:');

test('single email', function () {
  var out = r.redactPII('Bitte an info@kanzlei.ch melden.');
  assert.strictEqual(out.redacted, 'Bitte an [EMAIL_1] melden.');
  assert.strictEqual(out.summary.byType.EMAIL, 1);
});

test('two emails get sequential placeholders', function () {
  var out = r.redactPII('a@x.ch und b@y.ch');
  assert.strictEqual(out.redacted, '[EMAIL_1] und [EMAIL_2]');
  assert.strictEqual(out.summary.byType.EMAIL, 2);
});

console.log('\nAHV / AVS:');

test('dotted AHV: 756.1234.5678.90', function () {
  var out = r.redactPII('AHV-Nr. 756.1234.5678.90 wurde verifiziert.');
  assert.strictEqual(out.redacted, 'AHV-Nr. [AHV_1] wurde verifiziert.');
});

test('spaced AHV: 756 1234 5678 90', function () {
  var out = r.redactPII('AHV-Nr. 756 1234 5678 90 abgelegt.');
  assert.strictEqual(out.redacted, 'AHV-Nr. [AHV_1] abgelegt.');
});

console.log('\nIBAN:');

test('CH IBAN with spaces', function () {
  var out = r.redactPII('Konto: CH93 0076 2011 6238 5295 7');
  assert.ok(out.redacted.indexOf('[IBAN_1]') >= 0, 'placeholder missing: ' + out.redacted);
  assert.strictEqual(out.summary.byType.IBAN, 1);
});

test('CH IBAN no spaces', function () {
  var out = r.redactPII('Konto CH9300762011623852957 stop.');
  assert.strictEqual(out.redacted, 'Konto [IBAN_1] stop.');
});

console.log('\nCHE / UID:');

test('CHE-XXX.XXX.XXX', function () {
  var out = r.redactPII('UID: CHE-123.456.789, gültig.');
  assert.strictEqual(out.redacted, 'UID: [CHE_1], gültig.');
});

console.log('\nPHONE:');

test('+41 international format', function () {
  var out = r.redactPII('Tel +41 79 123 45 67');
  assert.strictEqual(out.redacted, 'Tel [PHONE_1]');
});

test('079 national format', function () {
  var out = r.redactPII('Tel 079 123 45 67');
  assert.strictEqual(out.redacted, 'Tel [PHONE_1]');
});

console.log('\nDOB:');

test('DE: geboren am 1.1.1980', function () {
  var out = r.redactPII('Klägerin, geboren am 1.1.1980, wohnhaft in Bern.');
  assert.ok(out.redacted.indexOf('[DOB_1]') >= 0, 'DOB not redacted: ' + out.redacted);
});

test('FR: né le 15.6.1975', function () {
  var out = r.redactPII('Demandeur, né le 15.6.1975, domicilié.');
  assert.ok(out.redacted.indexOf('[DOB_1]') >= 0);
});

test('IT: nato il 5.3.1962', function () {
  var out = r.redactPII('Attore, nato il 5.3.1962, residente.');
  assert.ok(out.redacted.indexOf('[DOB_1]') >= 0);
});

test('asterisk birth notation: *15.6.1975', function () {
  var out = r.redactPII('Müller, *15.6.1975, Schweizer.');
  assert.ok(out.redacted.indexOf('[DOB_1]') >= 0);
});

console.log('\nNAME (title-anchored):');

test('Herr Max Müller', function () {
  var out = r.redactPII('Herr Max Müller war Zeuge.');
  assert.strictEqual(out.redacted, '[NAME_1] war Zeuge.');
});

test('Frau Dr. Anna Schmid', function () {
  var out = r.redactPII('Frau Dr. Anna Schmid erschien.');
  assert.strictEqual(out.redacted, '[NAME_1] erschien.');
});

test('Me Pierre Dupont', function () {
  var out = r.redactPII('Me Pierre Dupont a plaidé.');
  assert.strictEqual(out.redacted, '[NAME_1] a plaidé.');
});

test('plain "Max Müller" without title is NOT redacted', function () {
  var out = r.redactPII('Max Müller war im Saal.');
  assert.strictEqual(out.summary.total, 0,
    'untitled name should not be redacted (title anchor required)');
});

console.log('\nADDRESS / POSTAL:');

test('Bahnhofstrasse 12', function () {
  var out = r.redactPII('Sitz: Bahnhofstrasse 12, Zürich.');
  assert.ok(out.redacted.indexOf('[ADDRESS_1]') >= 0, out.redacted);
});

test('CH-8001 Zürich (PLZ + city)', function () {
  var out = r.redactPII('CH-8001 Zürich');
  assert.strictEqual(out.redacted, '[POSTAL_1]');
});

test('1003 Lausanne', function () {
  var out = r.redactPII('1003 Lausanne');
  assert.strictEqual(out.redacted, '[POSTAL_1]');
});

// ── CRITICAL: legal citations must NOT be redacted ────────────────────

console.log('\nLegal citations are PRESERVED:');

test('BGE 143 III 480', function () {
  var src = 'Vgl. BGE 143 III 480, E. 3.2.';
  var out = r.redactPII(src);
  assert.strictEqual(out.redacted, src);
  assert.strictEqual(out.summary.total, 0);
});

test('Federal docket 6B_1234/2025', function () {
  var src = 'Urteil 6B_1234/2025 vom 15. März 2025';
  var out = r.redactPII(src);
  assert.strictEqual(out.redacted, src);
});

test('Statute reference Art. 41 OR', function () {
  var src = 'Gemäss Art. 41 OR ist Schadenersatz geschuldet.';
  var out = r.redactPII(src);
  assert.strictEqual(out.redacted, src);
});

test('ATF 125 V 351 (French)', function () {
  var src = 'Cf. ATF 125 V 351, consid. 4.';
  var out = r.redactPII(src);
  assert.strictEqual(out.redacted, src);
});

test('"Art. 756 OR" does NOT trigger AHV regex (no 4-digit follower)', function () {
  var src = 'Art. 756 OR regelt die Verantwortlichkeit.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0,
    '756 inside statute reference must not match AHV: ' + JSON.stringify(out));
});

test('docket "6B_756/2025" does NOT trigger AHV regex', function () {
  var src = 'Urteil 6B_756/2025 ist relevant.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0);
});

// ── Round-trip recovery ───────────────────────────────────────────────

console.log('\nRound-trip (unredact reverses redact):');

test('round-trip with multiple PII types', function () {
  var src = 'Herr Max Müller, geboren am 1.1.1980, AHV 756.1234.5678.90, ' +
           'erreichbar via max@firma.ch oder +41 79 123 45 67.';
  var out = r.redactPII(src);
  var back = r.unredact(out.redacted, out.replacements);
  assert.strictEqual(back, src);
});

test('round-trip with 12 names (placeholder ordering matters)', function () {
  /* Use a Latin-letter suffix per name so the NAME regex matches each
     one (digits would terminate the [a-z]+ word body). */
  var suffixes = ['Aaa', 'Bbb', 'Ccc', 'Ddd', 'Eee', 'Fff',
                  'Ggg', 'Hhh', 'Iii', 'Jjj', 'Kkk', 'Lll'];
  var src = suffixes.map(function (s) { return 'Herr Person ' + s + '.'; }).join(' ');
  var out = r.redactPII(src);
  /* Verify both [NAME_1] and [NAME_10]+ exist and reverse correctly */
  assert.ok(out.redacted.indexOf('[NAME_1]') >= 0,
    'expected [NAME_1] in: ' + out.redacted);
  assert.ok(out.redacted.indexOf('[NAME_10]') >= 0,
    'expected [NAME_10] in: ' + out.redacted);
  var back = r.unredact(out.redacted, out.replacements);
  assert.strictEqual(back, src);
});

test('round-trip on text with no PII is identity', function () {
  var src = 'Reines Recht ohne PII.';
  var out = r.redactPII(src);
  assert.strictEqual(r.unredact(out.redacted, out.replacements), src);
});

// ── Mixed real-world snippet ──────────────────────────────────────────

console.log('\nReal-world legal snippet:');

test('mixed paragraph: citations preserved, PII redacted', function () {
  var src =
    'Wie das Bundesgericht in BGE 143 III 480, E. 3.2 ausführt (vgl. auch ' +
    'Urteil 4A_747/2012 vom 5. April 2013), gilt nach Art. 41 OR Folgendes: ' +
    'Herr Max Müller, geboren am 1.1.1980, wohnhaft Bahnhofstrasse 12, ' +
    '8001 Zürich, AHV-Nr. 756.1234.5678.90, fordert Schadenersatz von ' +
    'der Y AG, UID CHE-123.456.789, erreichbar via max@firma.ch.';
  var out = r.redactPII(src);

  /* All citations + statutes survive */
  assert.ok(out.redacted.indexOf('BGE 143 III 480') >= 0, 'BGE missing');
  assert.ok(out.redacted.indexOf('4A_747/2012') >= 0, 'docket missing');
  assert.ok(out.redacted.indexOf('Art. 41 OR') >= 0, 'statute missing');

  /* All PII gone (no original substrings remain) */
  assert.ok(out.redacted.indexOf('Max Müller') < 0, 'name leaked');
  assert.ok(out.redacted.indexOf('756.1234.5678.90') < 0, 'AHV leaked');
  assert.ok(out.redacted.indexOf('CHE-123.456.789') < 0, 'CHE leaked');
  assert.ok(out.redacted.indexOf('max@firma.ch') < 0, 'email leaked');
  assert.ok(out.redacted.indexOf('1.1.1980') < 0, 'DOB leaked');

  /* Round-trip works */
  assert.strictEqual(r.unredact(out.redacted, out.replacements), src);
});

// ── Summary helper ────────────────────────────────────────────────────

console.log('\nSummary formatter:');

test('formatSummary builds DE label', function () {
  var s = r.formatSummary({ byType: { NAME: 2, AHV: 1 }, total: 3 }, 'de');
  assert.ok(s.indexOf('Name') >= 0 && s.indexOf('AHV-Nr.') >= 0, s);
});

test('formatSummary returns "" for empty summary', function () {
  assert.strictEqual(r.formatSummary({ byType: {}, total: 0 }, 'de'), '');
});

// ── Patterns metadata ─────────────────────────────────────────────────

console.log('\nModule shape:');

test('PII_TYPES exposes every pattern type', function () {
  assert.deepStrictEqual(
    Object.keys(r.PII_TYPES).sort(),
    ['ADDRESS', 'AHV', 'CHE', 'DOB', 'EMAIL', 'IBAN', 'NAME', 'PHONE', 'POSTAL']
  );
});

test('PATTERNS list is non-empty + each has type+regex', function () {
  assert.ok(r.PATTERNS.length >= 9);
  r.PATTERNS.forEach(function (p) {
    assert.ok(typeof p.type === 'string');
    assert.ok(p.regex instanceof RegExp);
  });
});

// ── Done ──────────────────────────────────────────────────────────────

console.log('\n' + '='.repeat(50));
console.log('Results: ' + passed + ' passed, ' + failed + ' failed');
console.log('='.repeat(50));
if (failed > 0) process.exit(1);
