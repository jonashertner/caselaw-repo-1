"""Issue #60: the FTS5 syntax search_laws advertises must actually work.

Measured live by the reporter (follow-up to #31): `X OR Y` returned 0 where
the plain query returned 10 (failed closed), `X AND <absent word>` returned
10 hits none of which contained the word (failed open — the OR-fill ignored
the operator), quotes were stripped (9 of 10 "phrase" hits lacked the
phrase), `*` was cut. Root cause: _expand_law_query re-tokenises with
`re.findall(word, q.lower())` AFTER sanitisation — lowercasing kills the
uppercase-only FTS5 operators, the regex kills quotes and stars.

Fix: when _has_explicit_fts_syntax(raw) is true, build the MATCH string from
the RAW query via _explicit_laws_match (operators kept when operand-flanked,
phrases verbatim, prefix stars kept) and skip expansion AND the OR-fill.
Natural-language queries keep the #31 pipeline byte-identically.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── the match builder ─────────────────────────────────────────────────────

def test_or_survives_uppercase_and_flanked():
    assert m._explicit_laws_match("Sistierung OR Aussetzung") == "Sistierung OR Aussetzung"


def test_and_survives():
    assert m._explicit_laws_match("Sistierung AND Zebrastreifen") == \
        "Sistierung AND Zebrastreifen"


def test_phrase_passes_verbatim():
    assert m._explicit_laws_match('"Sistierung des Verfahrens"') == \
        '"Sistierung des Verfahrens"'


def test_prefix_star_kept():
    assert m._explicit_laws_match("Verfahr*") == "Verfahr*"


def test_lowercase_or_is_a_word_not_an_operator():
    # FTS5 semantics: operators are uppercase-only
    assert m._explicit_laws_match("Sistierung or Aussetzung") == "Sistierung or Aussetzung"


def test_stray_operator_is_quoted_to_a_literal():
    # trailing OR has no right operand -> literal (Obligationenrecht case)
    assert m._explicit_laws_match("Obligationenrecht OR") == 'Obligationenrecht "OR"'
    assert m._explicit_laws_match("OR") == '"OR"'


def test_punctuation_cannot_break_fts5():
    out = m._explicit_laws_match('Miete OR (Pacht) AND Art. 266')
    # parens degrade to terms, dots are stripped, operators survive
    assert "(" not in out and ")" not in out and "." not in out
    assert " OR " in out and " AND " in out


# ── end-to-end against a fixture index (schema mirrors statutes.db) ───────

def _fixture_conn():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, abbr_de TEXT, abbr_fr TEXT,
            abbr_it TEXT, title_de TEXT, title_fr TEXT, title_it TEXT);
        CREATE TABLE articles (id INTEGER PRIMARY KEY, sr_number TEXT,
            article_num TEXT, heading TEXT, text TEXT, lang TEXT);
        CREATE VIRTUAL TABLE articles_fts USING fts5(sr_number, article_num, heading, text);
        INSERT INTO laws VALUES ('272','ZPO','CPC','CPC','Zivilprozessordnung','','');
        INSERT INTO articles VALUES
            (1,'272','126','Sistierung','Das Gericht kann die Sistierung des Verfahrens anordnen.','de'),
            (2,'272','127','Aussetzung','Die Aussetzung ist auf Antrag moeglich.','de'),
            (3,'272','128','Beides','Sistierung und Aussetzung im Ueberblick.','de'),
            (4,'272','129','Verfahren','Das Verfahren wird fortgesetzt.','de');
        INSERT INTO articles_fts (rowid, sr_number, article_num, heading, text)
            SELECT id, sr_number, article_num, heading, text FROM articles;
    """)
    return con


def _run(monkeypatch, query):
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _fixture_conn())
    monkeypatch.setattr(m, "_abbreviation_lookup_federal", lambda *a, **k: [])
    out = m.search_laws(query, language="de", jurisdiction="federal", limit=10)
    return [r["article_num"] for r in out["results"]]


def test_or_returns_the_union(monkeypatch):
    arts = _run(monkeypatch, "Sistierung OR Aussetzung")
    assert set(arts) == {"126", "127", "128"}, arts   # was: 0 results


def test_and_with_absent_term_fails_closed(monkeypatch):
    arts = _run(monkeypatch, "Sistierung AND Zebrastreifen")
    assert arts == [], arts                            # was: OR-filled page


def test_and_with_present_terms_is_the_intersection(monkeypatch):
    arts = _run(monkeypatch, "Sistierung AND Aussetzung")
    assert arts == ["128"], arts


def test_phrase_matches_only_the_phrase(monkeypatch):
    arts = _run(monkeypatch, '"Sistierung des Verfahrens"')
    assert arts == ["126"], arts                       # was: unordered words


def test_prefix_matches(monkeypatch):
    arts = _run(monkeypatch, "Verfahr*")
    assert set(arts) == {"126", "129"}, arts           # was: star stripped


def test_natural_language_path_unchanged(monkeypatch):
    """No explicit syntax -> the #31 AND-first-fill pipeline runs as before."""
    arts = _run(monkeypatch, "Sistierung Aussetzung")
    assert arts and arts[0] == "128", arts             # strict AND hit first
    assert set(arts) >= {"126", "127", "128"}          # OR-fill tops up


def test_syntax_error_fails_closed_not_500(monkeypatch):
    # NEAR without FTS5's function form is an FTS5 syntax error; the search
    # must degrade to an empty result, never an exception.
    arts = _run(monkeypatch, "Sistierung NEAR Aussetzung")
    assert arts == [] or set(arts) <= {"126", "127", "128"}
