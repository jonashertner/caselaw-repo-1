"""Regression tests for issues #42-#45 (sglbot MCP tool robustness batch)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402
from db_schema import SCHEMA_SQL  # noqa: E402


# ─────────────────────────── #42 phrase search ───────────────────────────
def test_sanitize_preserves_balanced_phrase():
    assert m._sanitize_fts5('"Treu und Glauben"') == '"Treu und Glauben"'
    assert m._sanitize_fts5('"in dubio pro reo"') == '"in dubio pro reo"'

def test_sanitize_phrase_interior_punctuation_stripped():
    assert m._sanitize_fts5('"Art. 42 BGG"') == '"Art 42 BGG"'

def test_sanitize_operator_inside_phrase_stays_literal():
    # OR inside a phrase must NOT be treated as a boolean/quoted separately
    assert m._sanitize_fts5('"foo OR bar"') == '"foo OR bar"'

def test_sanitize_two_phrases_with_operator():
    assert m._sanitize_fts5('"A" AND "B"') == '"A" AND "B"'

def test_sanitize_unquoted_unchanged():
    assert m._sanitize_fts5('Treu und Glauben') == 'Treu und Glauben'
    assert m._sanitize_fts5('Klimaschutz AND Kanton') == 'Klimaschutz AND Kanton'

def test_sanitize_empty_and_unbalanced_quotes_safe():
    assert m._sanitize_fts5('""') == ''
    assert m._sanitize_fts5('"unbalanced') == 'unbalanced'
    assert m._sanitize_fts5('"" foo') == 'foo'
    assert m._sanitize_fts5('""" ') == ''

def test_sanitize_OR_abbreviation_still_quoted_when_bare():
    assert m._sanitize_fts5('Art. 172 OR') == 'Art 172 "OR"'

def test_sanitized_phrases_accepted_by_fts5():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE VIRTUAL TABLE t USING fts5(b, tokenize='unicode61 remove_diacritics 2')")
    c.execute("INSERT INTO t(b) VALUES('Treu und Glauben Art 42 BGG foo OR bar')")
    for p in ['"Treu und Glauben"', '"Art. 42 BGG"', '"foo OR bar"', '"A" AND "B"',
              '""', '"unbalanced', 'Klimaschutz AND Kanton']:
        q = m._sanitize_fts5(p)
        if q:
            c.execute("SELECT COUNT(*) FROM t WHERE t MATCH ?", (q,)).fetchone()  # must not raise


# ─────────────────────────── #45 date parsing ───────────────────────────
def test_parse_date_iso_passthrough():
    assert m._parse_date_param('2024-01-01') == '2024-01-01'

def test_parse_date_swiss():
    assert m._parse_date_param('01.01.2024') == '2024-01-01'
    assert m._parse_date_param('1.1.2024') == '2024-01-01'
    assert m._parse_date_param('  29.02.2024  ') == '2024-02-29'  # leap day

def test_parse_date_rejects_bad():
    for v in ['01/01/2024', '1. Januar 2024', '20240101', '', None, '   ',
              '32.13.2024', "2024-01-01' OR 1=1", '29.02.2023']:  # 2023 not leap
        assert m._parse_date_param(v) is None, v


# ─────────────────── #44 docket extraction + LIKE guard ───────────────────
def test_extract_single_docket_from_decorated_citation():
    assert m._extract_single_docket('BGer 6B 1518/2021 vom 31. Januar 2022') == '6B 1518/2021'
    assert m._extract_single_docket('  6B 1518/2021  ') == '6B 1518/2021'
    assert m._extract_single_docket('6b_1518/2021') == '6B 1518/2021'

def test_extract_single_docket_rejects_ambiguous_and_trailing_digit():
    assert m._extract_single_docket('6B 1518/20210') is None       # trailing digit
    assert m._extract_single_docket('6B 1/2020 and 6B 2/2020') is None  # two dockets
    assert m._extract_single_docket('INVALID') is None
    assert m._extract_single_docket('') is None

def test_input_is_docket_like_guard():
    assert m._input_is_docket_like('6B 1518')      # digit + space
    assert m._input_is_docket_like('1518/2021')    # digit + slash
    assert not m._input_is_docket_like('INVALID')  # no digit
    assert not m._input_is_docket_like('123')      # digit but no sep
    assert not m._input_is_docket_like('@')
    assert not m._input_is_docket_like('')


# ─────────────────── #43/#44 serving (fixture DB) ───────────────────
def _fixture(path):
    c = sqlite3.connect(path)
    c.executescript(SCHEMA_SQL)
    c.execute("INSERT INTO decisions (decision_id, court, canton, docket_number, "
              "decision_date, language, title, full_text) VALUES "
              "('bge_BGE_140_III_86','bge','CH','140 III 86','2014-03-15','de','Regeste','Erwaegung')")
    c.execute("INSERT INTO decisions (decision_id, court, canton, docket_number, "
              "decision_date, language, title, full_text) VALUES "
              "('bger_6B_1518_2021','bger','CH','6B 1518/2021','2022-01-31','de',"
              "'Invalidenrente Streit','Voller Text ueber Invalidenrente')")
    c.commit(); c.close()
    return path

def _rconn(p):
    c = sqlite3.connect(p); c.row_factory = sqlite3.Row
    return c

def _setup(tmp_path, monkeypatch):
    dbp = str(_fixture(tmp_path / "decisions.db"))
    monkeypatch.setattr(m, "get_db", lambda: _rconn(dbp))
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(tmp_path / "missing.db"))

def test_43_atf_dtf_resolve(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    for ref in ['BGE 140 III 86', 'ATF 140 III 86', 'DTF 140 III 86', '140 III 86']:
        assert m._resolve_decision_id(ref) == 'bge_BGE_140_III_86', ref
        assert m._resolve_decision_id_strict(ref) == 'bge_BGE_140_III_86', ref

def test_44_resolve_own_citation(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    for ref in ['6B 1518/2021', '  6B 1518/2021  ', '6B 1518/2021 vom 31. Januar 2022',
                'BGer 6B 1518/2021 vom 31. Januar 2022']:
        r = m.get_decision_by_id(ref)
        assert r and r['decision_id'] == 'bger_6B_1518_2021', ref

def test_44_garbage_input_rejected(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    # non-docket junk must NOT resolve to a plausible-looking decision
    for junk in ['INVALID', 'Invalidenrente', '@', '', 'a', 'Bundesgericht']:
        assert m.get_decision_by_id(junk) is None, junk


# ─────────────────── hardening (Codex review) ───────────────────
def test_45_iso_calendar_validation():
    assert m._parse_date_param('2024-99-99') is None
    assert m._parse_date_param('2023-02-29') is None  # not a leap year
    assert m._parse_date_param('2024-13-01') is None

def test_44_docket_regex_rejects_trailing_char():
    assert m._extract_single_docket('6B 1518/2021x') is None
    assert m._extract_single_docket('6B 1518/2021_foo') is None
    assert m._extract_single_docket('6B 1518/20210') is None

def test_44_guard_rejects_wildcards_and_loose():
    assert not m._input_is_docket_like('6B %/2021')  # LIKE wildcard
    assert not m._input_is_docket_like('1 %')
    assert not m._input_is_docket_like('INVALID 1')
    assert not m._input_is_docket_like('123')
    assert m._input_is_docket_like('6B 1518')
    assert m._input_is_docket_like('18/2021')

def test_43_case_insensitive_and_letter_division_and_pinpoint():
    assert m._bge_ref_candidates('atf 140 iii 86')[0] == 'bge_BGE_140_III_86'
    assert m._bge_ref_candidates('BGE 116 Ia 28')[0] == 'bge_BGE_116_Ia_28'
    assert m._bge_ref_candidates('BGE 140 III 86, E. 2.3')[0] == 'bge_BGE_140_III_86'
    assert m._bge_ref_candidates('DTF 140 III 86 consid. 2')[0] == 'bge_BGE_140_III_86'
    assert m._bge_ref_candidates('131 III 12')[0] == 'bge_BGE_131_III_12'  # not 121
    assert m._bge_ref_candidates('6B 12/2020') == []  # docket, not BGE


def test_45_date_arg_error_contract():
    assert m._date_arg_error('2024-01-01', None) is None
    assert m._date_arg_error(None, None) is None
    assert m._date_arg_error('', '   ') is None
    assert m._date_arg_error('01/01/2024', None)  # US -> error
    assert m._date_arg_error('2024-99-99', None)  # bad ISO -> error
    assert m._date_arg_error('2024-01-01', '1. Januar 2024')  # date_to bad -> error

def test_44_normalize_docket_key_boundaries():
    import docket_aliases as da
    assert da.normalize_docket_key('6B 1518/2021') == '6B_1518/2021'
    assert da.normalize_docket_key('bger_6B_1518/2021') == '6B_1518/2021'
    assert da.normalize_docket_key('6B 1518/2021x') is None
    assert da.normalize_docket_key('6B_1518/2021_foo') is None
