/**
 * Extended/adversarial test pass for redact.js — targets the failure
 * modes a careless redactor would hit:
 *
 *   1. False positives that destroy useful legal text
 *   2. False negatives that leak PII (PII variants we missed)
 *   3. Round-trip failures (placeholder collisions, empty strings,
 *      multi-byte chars, overlapping pattern resolution)
 *   4. Cross-language coverage (DE/FR/IT/EN snippets)
 *   5. Performance (large inputs)
 *   6. Integration with the api.js _maybeRedact() helper (mocked)
 */

var assert = require('assert');
var r = require('../js/redact.js');
var passed = 0, failed = 0, failureMessages = [];

function test(name, fn) {
  try { fn(); passed++; console.log('  PASS: ' + name); }
  catch (e) { failed++; failureMessages.push(name + ': ' + (e.message || e));
              console.log('  FAIL: ' + name); console.log('        ' + (e.message || e)); }
}
function section(label) { console.log('\n' + label); }

// ────────────────────────────────────────────────────────────────────
// 1. FALSE POSITIVES — legal text that must NOT be touched
// ────────────────────────────────────────────────────────────────────
section('1. False-positive guards on legal text:');

test('Long BGE prose stays untouched', function () {
  var src = 'Wie das Bundesgericht in BGE 143 III 480, E. 3.2 (vgl. auch '
          + 'BGE 140 V 145, E. 4.1, m.w.H. auf BGE 138 III 232, E. 5.1) '
          + 'ausgeführt hat, gilt nach Art. 41 Abs. 1 OR i.V.m. Art. 8 BV '
          + 'das Verschuldensprinzip.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0,
    'No redactions expected; got: ' + JSON.stringify(out.replacements));
});

test('French legal snippet (ATF + TF) untouched', function () {
  var src = 'Cf. ATF 125 V 351, consid. 4 et arrêt TF 4A_747/2012 du 5 avril 2013.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0);
});

test('Italian legal snippet (DTF + TF) untouched', function () {
  var src = 'Si veda DTF 140 III 86, consid. 2.3 e sentenza TF 6B_500/2024.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0);
});

test('Statute references with word "Strasse" in code title NOT redacted as ADDRESS', function () {
  /* "Strassenverkehrsgesetz" should not match the address regex even though it ends with "strasse"... */
  var src = 'Art. 90 SVG (Strassenverkehrsgesetz) regelt die Geschwindigkeit.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0,
    'SVG title got matched as address: ' + JSON.stringify(out.replacements));
});

test('Year mention without anchor word NOT taken as DOB', function () {
  var src = 'Das Bundesgericht entschied am 15.6.2024 wie folgt.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0,
    'Plain date without "geboren am" anchor leaked as DOB: ' + JSON.stringify(out.replacements));
});

test('Court chamber name like "Strafrechtliche Abteilung" NOT taken as name', function () {
  var src = 'Die Strafrechtliche Abteilung des Bundesgerichts hat erwogen.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.byType.NAME || 0, 0);
});

test('SR number "SR 220" NOT eaten by any regex', function () {
  var src = 'Die SR-Nummer 220 (OR) ist relevant.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0);
});

test('PLZ-only number 8001 (no city after) is NOT matched', function () {
  /* POSTAL requires PLZ + capitalized city — bare "8001" alone shouldn't trigger */
  var src = 'Die Postleitzahl 8001 wurde geprüft.';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.total, 0);
});

test('Court abbreviation "Hr. Müller" caveat: title alone gets redacted with name (intended)', function () {
  /* Sanity: "Hr." is a title abbreviation and the regex DOES catch it */
  var out = r.redactPII('Hr. Müller war anwesend.');
  assert.ok(out.redacted.indexOf('Müller') < 0, 'Hr.-anchored name should be redacted');
});

// ────────────────────────────────────────────────────────────────────
// 2. FALSE NEGATIVES — PII that should be caught
// ────────────────────────────────────────────────────────────────────
section('2. False-negative checks (every PII variant):');

test('AHV with whitespace mix: "756.1234 5678.90" still caught', function () {
  /* spaces/dots interleaved — our regex allows [.\s] between groups */
  var out = r.redactPII('AHV 756.1234 5678.90 erfasst.');
  assert.strictEqual(out.summary.byType.AHV, 1);
});

test('AHV at start of line', function () {
  var out = r.redactPII('756.1234.5678.90 ist die Nummer.');
  assert.strictEqual(out.redacted.slice(0, 7), '[AHV_1]');
});

test('AHV at end of line (no trailing space)', function () {
  var out = r.redactPII('Die Nummer ist 756.1234.5678.90');
  assert.ok(out.redacted.endsWith('[AHV_1]'));
});

test('Email with subdomain + plus tag: jonas.hertner+tag@sub.kanzlei-mueller.ch', function () {
  var out = r.redactPII('Kontakt: jonas.hertner+tag@sub.kanzlei-mueller.ch');
  assert.strictEqual(out.redacted, 'Kontakt: [EMAIL_1]');
});

test('Multiple emails in one line', function () {
  var out = r.redactPII('a@x.ch, b@y.fr, c@z.it');
  assert.strictEqual(out.summary.byType.EMAIL, 3);
});

test('IBAN without "CH" letters but otherwise valid: NOT matched (intentional)', function () {
  /* Generic non-CH IBANs are out of scope — we focus on Swiss law-firm contexts */
  var out = r.redactPII('IBAN DE89370400440532013000');
  assert.strictEqual(out.summary.byType.IBAN || 0, 0);
});

test('CHE with hyphens: CHE-123-456-789', function () {
  var out = r.redactPII('UID CHE-123-456-789');
  assert.strictEqual(out.summary.byType.CHE, 1);
});

test('CHE with mixed dot+hyphen: CHE-123.456.789', function () {
  var out = r.redactPII('UID CHE-123.456.789');
  assert.strictEqual(out.summary.byType.CHE, 1);
});

test('Phone with hyphens: 079-123-45-67', function () {
  var out = r.redactPII('Tel 079-123-45-67');
  assert.strictEqual(out.summary.byType.PHONE, 1);
});

test('Phone with parens: +41 (0)79 123 45 67', function () {
  var out = r.redactPII('Tel +41 (0)79 123 45 67');
  assert.strictEqual(out.summary.byType.PHONE, 1);
});

test('Phone landline: 044 123 45 67', function () {
  var out = r.redactPII('Tel 044 123 45 67');
  assert.strictEqual(out.summary.byType.PHONE, 1);
});

test('DOB FR no accent on née: né le 15.6.1975', function () {
  var out = r.redactPII('Demandeur, né le 15.6.1975, domicilié.');
  assert.strictEqual(out.summary.byType.DOB, 1);
});

test('DOB FR feminine: née le 15.6.1975', function () {
  var out = r.redactPII('Demanderesse, née le 15.6.1975, domiciliée.');
  assert.strictEqual(out.summary.byType.DOB, 1);
});

test('Address Italian: Via Pretorio 7', function () {
  var out = r.redactPII('Sede: Via Pretorio 7, Lugano.');
  assert.ok(out.redacted.indexOf('[ADDRESS_1]') >= 0,
    'Italian address not redacted: ' + out.redacted);
});

test('Address French with "de": Rue de la Gare 12', function () {
  var out = r.redactPII('Adresse: Rue de la Gare 12, Genève.');
  assert.ok(out.redacted.indexOf('[ADDRESS_1]') >= 0,
    'French address not redacted: ' + out.redacted);
});

test('Address with letter suffix: Bahnhofstrasse 12a', function () {
  var out = r.redactPII('Sitz: Bahnhofstrasse 12a, Zürich.');
  assert.ok(out.redacted.indexOf('[ADDRESS_1]') >= 0);
});

test('Honorific "Maître" (FR avocat)', function () {
  var out = r.redactPII('Maître Pierre Dupont a plaidé pour le demandeur.');
  assert.strictEqual(out.summary.byType.NAME, 1);
});

test('Italian "Avv. Mario Rossi"', function () {
  var out = r.redactPII('Avv. Mario Rossi ha rappresentato il convenuto.');
  assert.strictEqual(out.summary.byType.NAME, 1);
});

test('Three-word name: Frau Anna Maria Schmid', function () {
  var out = r.redactPII('Frau Anna Maria Schmid wurde angehört.');
  assert.strictEqual(out.redacted, '[NAME_1] wurde angehört.');
});

test('Hyphenated surname: Herr Max Müller-Aebi', function () {
  var out = r.redactPII('Herr Max Müller-Aebi war Zeuge.');
  assert.ok(out.redacted.indexOf('Müller-Aebi') < 0,
    'hyphenated surname leaked: ' + out.redacted);
});

// ────────────────────────────────────────────────────────────────────
// 3. ROUND-TRIP & PLACEHOLDER COLLISIONS
// ────────────────────────────────────────────────────────────────────
section('3. Round-trip + placeholder edge cases:');

test('Round-trip preserves multi-byte chars (ä, ö, ü, é, à, è, ç)', function () {
  var src = 'Frau Aebischer Müller, geboren am 1.1.1980, wohnhaft Bahnhofstrasse 12, '
          + 'CH-8001 Zürich. Avocat: Maître Pierre Dupont à Genève. Avvocato: Avv. Mario Rossi.';
  var out = r.redactPII(src);
  var back = r.unredact(out.redacted, out.replacements);
  assert.strictEqual(back, src);
});

test('Round-trip with email containing chars that look like URL fragment', function () {
  var src = 'Kontakt: a.b+c-d_e@example.co.uk?subject=test';
  var out = r.redactPII(src);
  var back = r.unredact(out.redacted, out.replacements);
  assert.strictEqual(back, src);
});

test('Idempotency: redacting an already-redacted string changes nothing', function () {
  var first = r.redactPII('Herr Max Müller');
  var second = r.redactPII(first.redacted);
  assert.strictEqual(second.summary.total, 0,
    'Re-redaction should not produce new replacements: ' + JSON.stringify(second));
  assert.strictEqual(second.redacted, first.redacted);
});

test('Empty-replacement unredact returns text as-is', function () {
  assert.strictEqual(r.unredact('hello', []), 'hello');
  assert.strictEqual(r.unredact('hello', null), 'hello');
});

test('100 emails — counter goes to 100 + round-trip succeeds', function () {
  var src = '';
  for (var i = 0; i < 100; i++) src += 'mail' + i + '@x.ch ';
  var out = r.redactPII(src);
  assert.strictEqual(out.summary.byType.EMAIL, 100);
  assert.ok(out.redacted.indexOf('[EMAIL_100]') >= 0, 'must reach 100');
  assert.strictEqual(r.unredact(out.redacted, out.replacements), src);
});

test('Adjacent PII (no separator): info@x.chHerr Max Müller', function () {
  /* Email regex is greedy on the dot — but "ch" followed immediately by "Herr"
     means the email match ends at "ch" and the name starts after. */
  var out = r.redactPII('info@x.chHerr Max Müller war hier.');
  assert.ok(out.redacted.indexOf('Max Müller') < 0,
    'name leaked when adjacent to email: ' + out.redacted);
});

// ────────────────────────────────────────────────────────────────────
// 4. CROSS-LANGUAGE & MIXED-DOMAIN STRESS
// ────────────────────────────────────────────────────────────────────
section('4. Cross-language realistic snippets:');

test('Multilingual brief paragraph (DE+FR+IT) — all PII redacted, all citations kept', function () {
  var src = 'Im vorliegenden Verfahren beruft sich Herr Max Müller (geboren am 1.1.1980) auf BGE 143 III 480.\n'
          + 'Le défendeur, Me Pierre Dupont, conteste cette interprétation (cf. ATF 125 V 351, consid. 4).\n'
          + 'L\'attore Avv. Mario Rossi cita altresì DTF 140 III 86 e l\'art. 41 OR.\n'
          + 'Contact: dupont@avocat.ch, +41 22 123 45 67. Adresse: Rue du Rhône 65, 1204 Genève.';
  var out = r.redactPII(src);

  /* All citations + statutes preserved */
  ['BGE 143 III 480', 'ATF 125 V 351', 'DTF 140 III 86', 'art. 41 OR'].forEach(function (cite) {
    assert.ok(out.redacted.indexOf(cite) >= 0, 'citation lost: ' + cite + ' in ' + out.redacted);
  });

  /* All identified PII gone */
  ['Max Müller', 'Pierre Dupont', 'Mario Rossi', '1.1.1980', 'dupont@avocat.ch', '+41 22 123 45 67', 'Rue du Rhône'].forEach(function (pii) {
    assert.ok(out.redacted.indexOf(pii) < 0, 'PII leaked: ' + pii + ' in ' + out.redacted);
  });

  /* Round-trip */
  assert.strictEqual(r.unredact(out.redacted, out.replacements), src);

  /* At least one of each expected type was caught */
  assert.ok(out.summary.byType.NAME >= 3, 'expected ≥3 names, got ' + out.summary.byType.NAME);
  assert.ok(out.summary.byType.DOB >= 1);
  assert.ok(out.summary.byType.EMAIL >= 1);
  assert.ok(out.summary.byType.PHONE >= 1);
  assert.ok(out.summary.byType.ADDRESS >= 1);
});

test('Realistic German pleading paragraph', function () {
  var src = 'Der Kläger, Herr Hans Meier, geboren am 12.3.1965, AHV-Nr. 756.1234.5678.90, '
          + 'wohnhaft Bahnhofstrasse 12, 8001 Zürich, fordert von der Beklagten, der ABC AG '
          + '(UID CHE-123.456.789, Sitz Talackerstrasse 5, 8152 Glattbrugg), Schadenersatz '
          + 'i.S.v. Art. 41 OR (vgl. BGE 132 III 122).';
  var out = r.redactPII(src);
  /* Spot-checks */
  assert.ok(out.redacted.indexOf('Hans Meier') < 0);
  assert.ok(out.redacted.indexOf('756.1234.5678.90') < 0);
  assert.ok(out.redacted.indexOf('CHE-123.456.789') < 0);
  assert.ok(out.redacted.indexOf('BGE 132 III 122') >= 0);
  assert.ok(out.redacted.indexOf('Art. 41 OR') >= 0);
  assert.strictEqual(r.unredact(out.redacted, out.replacements), src);
});

test('Realistic French pleading paragraph', function () {
  var src = 'La demanderesse, Mme Marie Dubois, née le 15.6.1975, no AVS 756.4567.8901.23, '
          + 'domiciliée Rue de la Paix 8, 1003 Lausanne, conclut au paiement de CHF 50\u2019000.- '
          + 'à charge du défendeur, M. Pierre Dupont, au sens de l\'art. 41 CO (cf. ATF 125 V 351).';
  var out = r.redactPII(src);
  assert.ok(out.redacted.indexOf('Marie Dubois') < 0,
    'FR name leaked: ' + out.redacted);
  assert.ok(out.redacted.indexOf('756.4567.8901.23') < 0,
    'AHV leaked: ' + out.redacted);
  assert.ok(out.redacted.indexOf('ATF 125 V 351') >= 0);
});

// ────────────────────────────────────────────────────────────────────
// 5. PERFORMANCE
// ────────────────────────────────────────────────────────────────────
section('5. Performance on large inputs:');

test('50KB document redacts in <500ms', function () {
  /* Build a 50KB doc with sparse PII */
  var para = 'Wie das Bundesgericht in BGE 143 III 480 ausgeführt hat, gilt nach Art. 41 OR. '
           + 'Herr Max Müller (max@firma.ch, +41 79 123 45 67) klagt gegen die ABC AG. ';
  var src = '';
  while (src.length < 50000) src += para;
  var t0 = Date.now();
  var out = r.redactPII(src);
  var dt = Date.now() - t0;
  console.log('        (input ' + src.length + 'B, redactions ' + out.summary.total + ', ' + dt + 'ms)');
  assert.ok(dt < 500, 'too slow: ' + dt + 'ms');
});

test('200KB worst-case (PII-heavy) document', function () {
  var para = 'Herr Max Müller, geb. am 1.1.1980, AHV 756.1234.5678.90, '
           + 'IBAN CH9300762011623852957, UID CHE-123.456.789, max@x.ch, +41 79 111 22 33. ';
  var src = '';
  while (src.length < 200000) src += para;
  var t0 = Date.now();
  var out = r.redactPII(src);
  var dt = Date.now() - t0;
  console.log('        (input ' + src.length + 'B, redactions ' + out.summary.total + ', ' + dt + 'ms)');
  assert.ok(dt < 2000, 'too slow: ' + dt + 'ms');
});

// ────────────────────────────────────────────────────────────────────
// 6. INTEGRATION — simulate the api.js _maybeRedact path
// ────────────────────────────────────────────────────────────────────
section('6. api.js _maybeRedact() integration (mocked):');

test('opt-out via localStorage is honored end-to-end', function () {
  /* Simulate: stub window+localStorage, load api.js, flip opt-out, call _maybeRedact */
  global.localStorage = {
    _store: { 'ocl_pii_redact_optout': '1' },
    getItem: function (k) { return this._store[k] || null; },
    setItem: function (k, v) { this._store[k] = v; },
  };
  global.window = {
    redactPII: r.redactPII,
    unredactPII: r.unredact,
    PII_TYPES: r.PII_TYPES,
  };
  global.fetch = function () { return Promise.resolve({ ok: true, status: 200, json: function () { return {}; } }); };
  global.crypto = { randomUUID: function () { return 'x'; }, getRandomValues: function (b) { return b; } };
  global.TextEncoder = function () { return { encode: function (s) { return new Uint8Array(s.length); } }; };

  /* Read api.js into a fresh sandbox + grab _maybeRedact */
  var fs = require('fs');
  var src = fs.readFileSync(__dirname + '/../js/api.js', 'utf8');
  var sandbox = { window: global.window, localStorage: global.localStorage, fetch: global.fetch,
                   crypto: global.crypto, TextEncoder: global.TextEncoder, console: console };
  var vm = require('vm');
  vm.createContext(sandbox);
  /* Strip top-level `const` to avoid TDZ issues across vm reloads */
  vm.runInContext(src, sandbox);

  var result = sandbox._maybeRedact('Herr Max Müller, max@x.ch, AHV 756.1234.5678.90');
  assert.strictEqual(result.summary.total, 0,
    'opt-out flag must skip redaction; got: ' + JSON.stringify(result));
  assert.strictEqual(result.redacted, 'Herr Max Müller, max@x.ch, AHV 756.1234.5678.90');
});

test('opt-out OFF (default) → redaction runs end-to-end', function () {
  global.localStorage._store = {};  /* clear opt-out */
  var fs = require('fs');
  var src = fs.readFileSync(__dirname + '/../js/api.js', 'utf8');
  var sandbox = { window: global.window, localStorage: global.localStorage, fetch: global.fetch,
                   crypto: global.crypto, TextEncoder: global.TextEncoder, console: console };
  var vm = require('vm');
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);

  var result = sandbox._maybeRedact('Herr Max Müller (max@x.ch)');
  assert.ok(result.summary.total >= 2, 'should redact at least name + email; got: ' + JSON.stringify(result));
  assert.ok(result.redacted.indexOf('Max Müller') < 0);
});

test('_maybeRedact returns passthrough on null window.redactPII', function () {
  global.window.redactPII = undefined;
  var fs = require('fs');
  var src = fs.readFileSync(__dirname + '/../js/api.js', 'utf8');
  var sandbox = { window: global.window, localStorage: global.localStorage, fetch: global.fetch,
                   crypto: global.crypto, TextEncoder: global.TextEncoder, console: console };
  var vm = require('vm');
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);

  var result = sandbox._maybeRedact('Herr Max Müller');
  assert.strictEqual(result.summary.total, 0);
  assert.strictEqual(result.redacted, 'Herr Max Müller');
  /* restore */
  global.window.redactPII = r.redactPII;
});

// ────────────────────────────────────────────────────────────────────
// 7. UI summary formatter — every language
// ────────────────────────────────────────────────────────────────────
section('7. formatSummary across languages:');

['de', 'fr', 'it', 'en'].forEach(function (lang) {
  test('formatSummary builds non-empty string in ' + lang, function () {
    var s = r.formatSummary({ byType: { NAME: 2, AHV: 1, EMAIL: 3 }, total: 6 }, lang);
    assert.ok(s.length > 0);
    assert.ok(s.indexOf('2') >= 0 && s.indexOf('3') >= 0);
  });
});

test('formatSummary unknown type falls back to raw key', function () {
  var s = r.formatSummary({ byType: { CUSTOM_THING: 1 }, total: 1 }, 'de');
  assert.ok(s.indexOf('CUSTOM_THING') >= 0);
});

// ────────────────────────────────────────────────────────────────────
// SUMMARY
// ────────────────────────────────────────────────────────────────────
console.log('\n' + '='.repeat(60));
console.log('Extended results: ' + passed + ' passed, ' + failed + ' failed');
console.log('='.repeat(60));
if (failed > 0) {
  console.log('\nFailures:');
  failureMessages.forEach(function (m) { console.log('  • ' + m); });
  process.exit(1);
}
