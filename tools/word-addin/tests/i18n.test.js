/**
 * Tests for i18n.js — court name mapping and translation helpers.
 * Run with: node i18n.test.js
 */

var assert = require('assert');
var i18n = require('../js/i18n.js');
var t = i18n.t;
var getCourtName = i18n.getCourtName;

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
// Court name mapping
// ============================================================

console.log('\nCourt name mapping — federal courts:');

test('bger DE', function() {
  assert.strictEqual(getCourtName('bger', 'de'), 'Bundesgericht');
});

test('bger FR', function() {
  assert.strictEqual(getCourtName('bger', 'fr'), 'Tribunal fédéral');
});

test('bvger IT', function() {
  assert.strictEqual(getCourtName('bvger', 'it'), 'Tribunale amministrativo federale');
});

test('bstger EN', function() {
  assert.strictEqual(getCourtName('bstger', 'en'), 'Federal Criminal Court');
});

console.log('\nCourt name mapping — cantonal courts:');

test('ge_gerichte DE', function() {
  assert.strictEqual(getCourtName('ge_gerichte', 'de'), 'Gerichte GE');
});

test('ge_gerichte FR', function() {
  assert.strictEqual(getCourtName('ge_gerichte', 'fr'), 'Tribunaux GE');
});

test('zh_obergericht DE', function() {
  assert.strictEqual(getCourtName('zh_obergericht', 'de'), 'Obergericht ZH');
});

test('zh_obergericht FR', function() {
  assert.strictEqual(getCourtName('zh_obergericht', 'fr'), 'Tribunal supérieur ZH');
});

test('zh_sozialversicherungsgericht DE', function() {
  assert.strictEqual(getCourtName('zh_sozialversicherungsgericht', 'de'), 'Sozialversicherungsgericht ZH');
});

test('bs_appellationsgericht FR', function() {
  assert.strictEqual(getCourtName('bs_appellationsgericht', 'fr'), "Cour d'appel BS");
});

test('zh_bezirksgericht_zuerich DE', function() {
  assert.strictEqual(getCourtName('zh_bezirksgericht_zuerich', 'de'), 'Bezirksgericht Zürich');
});

test('zh_bezirksgericht_winterthur FR', function() {
  assert.strictEqual(getCourtName('zh_bezirksgericht_winterthur', 'fr'), 'Tribunal de district de Winterthour');
});

console.log('\nCourt name mapping — regulatory:');

test('finma DE', function() {
  assert.strictEqual(getCourtName('finma', 'de'), 'FINMA');
});

test('edoeb DE', function() {
  assert.strictEqual(getCourtName('edoeb', 'de'), 'EDÖB');
});

test('edoeb FR', function() {
  assert.strictEqual(getCourtName('edoeb', 'fr'), 'PFPDT');
});

test('weko FR', function() {
  assert.strictEqual(getCourtName('weko', 'fr'), 'COMCO');
});

console.log('\nCourt name mapping — edge cases:');

test('unknown court returns uppercase', function() {
  assert.strictEqual(getCourtName('xyz_unknown', 'de'), 'XYZ_UNKNOWN');
});

test('empty string returns empty', function() {
  assert.strictEqual(getCourtName('', 'de'), '');
});

test('null returns empty', function() {
  assert.strictEqual(getCourtName(null, 'de'), '');
});

test('case insensitive', function() {
  assert.strictEqual(getCourtName('BGER', 'de'), 'Bundesgericht');
});

test('default language is de', function() {
  assert.strictEqual(getCourtName('bger'), 'Bundesgericht');
});

// ============================================================
// t() translation helper
// ============================================================

console.log('\nt() translations:');

test('search_placeholder DE', function() {
  assert.strictEqual(t('search_placeholder', 'de'), 'BGE 133 III 121, Art. 41 OR, 4A_747/2012...');
});

test('search_placeholder FR', function() {
  assert.strictEqual(t('search_placeholder', 'fr'), 'ATF 133 III 121, Art. 41 CO, 4A_747/2012...');
});

test('search_placeholder IT', function() {
  assert.strictEqual(t('search_placeholder', 'it'), 'DTF 133 III 121, Art. 41 CO, 4A_747/2012...');
});

test('search_placeholder EN', function() {
  assert.strictEqual(t('search_placeholder', 'en'), 'BGE 133 III 121, Art. 41 OR, 4A_747/2012...');
});

test('btn_insert all languages', function() {
  assert.strictEqual(t('btn_insert', 'de'), 'Einfügen');
  assert.strictEqual(t('btn_insert', 'fr'), 'Insérer');
  assert.strictEqual(t('btn_insert', 'it'), 'Inserisci');
  assert.strictEqual(t('btn_insert', 'en'), 'Insert');
});

test('results_count with placeholder', function() {
  assert.strictEqual(t('results_count', 'de', { n: 42 }), '42 Entscheide gefunden');
  assert.strictEqual(t('results_count', 'fr', { n: 42 }), '42 décisions trouvées');
});

test('badge_citations with placeholder', function() {
  assert.strictEqual(t('badge_citations', 'de', { n: 15 }), '15 Zit.');
  assert.strictEqual(t('badge_citations', 'en', { n: 15 }), '15 cit.');
});

test('error_rate_wait with placeholder', function() {
  assert.strictEqual(t('error_rate_wait', 'de', { n: 30 }), 'Bitte 30s warten.');
});

test('unknown key returns key', function() {
  assert.strictEqual(t('nonexistent_key', 'de'), 'nonexistent_key');
});

test('unknown language falls back to de', function() {
  assert.strictEqual(t('btn_insert', 'xx'), 'Einfügen');
});

// ============================================================
// Coverage check — all 102 courts from list_courts
// ============================================================

console.log('\nCoverage — major courts mapped:');

var majorCourts = [
  'bger', 'bge', 'bvger', 'bstger', 'bpatger',
  'ge_gerichte', 'vd_gerichte', 'ti_gerichte', 'zh_obergericht',
  'be_verwaltungsgericht', 'bl_gerichte', 'gr_gerichte', 'fr_gerichte',
  'so_gerichte', 'bs_appellationsgericht', 'sg_versicherungsgericht',
  'ne_gerichte', 'vs_gerichte', 'zh_handelsgericht', 'ag_gerichte',
  'finma', 'weko', 'edoeb', 'ubi', 'elcom', 'postcom', 'comcom',
  'zh_bezirksgericht_zuerich', 'zh_bezirksgericht_winterthur',
];

majorCourts.forEach(function(code) {
  test('mapped: ' + code, function() {
    var name = getCourtName(code, 'de');
    assert.ok(name && name.length > 0,
      'Expected non-empty name for ' + code + ', got: ' + name);
    // Verify it's in the mapping (not just fallback uppercase)
    assert.ok(i18n.COURT_DISPLAY_NAMES[code],
      'Expected ' + code + ' in COURT_DISPLAY_NAMES');
  });
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
