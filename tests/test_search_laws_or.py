"""Issue #31: search_laws used implicit AND, so one term the user guessed but
absent from the statute text zeroed the result (e.g. "Sistierung Aussetzung des
Verfahrens" missed Art. 126 ZPO). Fix = AND-first-fill: _expand_law_query yields
a strict (AND) form by default and a broadened (OR) form on request; the federal
search runs strict first and only tops up with OR when the page under-fills, so
exact hits stay on top and recall fills the rest.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _no_expansions(monkeypatch):
    for name in (
        "LAW_SEARCH_EXPANSIONS", "LEGAL_QUERY_EXPANSIONS",
        "_LAW_FTS_NORMALIZED_EXPANSIONS", "_FTS_NORMALIZED_EXPANSIONS",
    ):
        monkeypatch.setattr(m, name, {})


# --- _expand_law_query: strict by default, OR only when asked ----------------

def test_default_is_strict_and(monkeypatch):
    _no_expansions(monkeypatch)
    assert m._expand_law_query("sistierung verfahren") == "sistierung verfahren"


def test_multi_or_broadens(monkeypatch):
    _no_expansions(monkeypatch)
    assert m._expand_law_query("sistierung verfahren", multi_or=True) == "sistierung OR verfahren"


def test_single_term_unchanged_either_way(monkeypatch):
    _no_expansions(monkeypatch)
    assert m._expand_law_query("sistierung") == "sistierung"
    assert m._expand_law_query("sistierung", multi_or=True) == "sistierung"


def test_quoted_phrase_never_broadened(monkeypatch):
    _no_expansions(monkeypatch)
    assert " OR " not in m._expand_law_query('"sistierung des verfahrens"', multi_or=True)


# --- AND-first-fill behavior, end-to-end against a fixture FTS index ----------

def _fixture_conn():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, abbr_de TEXT, abbr_fr TEXT,
            abbr_it TEXT, title_de TEXT, title_fr TEXT, title_it TEXT);
        CREATE TABLE articles (id INTEGER PRIMARY KEY, sr_number TEXT,
            article_num TEXT, heading TEXT, text TEXT, lang TEXT);
        CREATE VIRTUAL TABLE articles_fts USING fts5(sr_number, article_num, heading, text);
        """
    )
    con.execute("INSERT INTO laws VALUES ('272','ZPO',NULL,NULL,'Zivilprozessordnung',NULL,NULL)")
    con.execute("INSERT INTO laws VALUES ('171.10','ParlG',NULL,NULL,'Parlamentsgesetz',NULL,NULL)")
    arts = [
        (1, "272", "126", "Sistierung des Verfahrens",
         "Das Gericht kann das Verfahren sistieren wenn es angezeigt erscheint", "de"),
        (2, "171.10", "87", "Verfahren bei Differenzen",
         "Sistierung Aussetzung Verfahren der Differenzbereinigung im Rat", "de"),
        (3, "272", "124", "Prozessleitung",
         "Das Gericht leitet den Prozess und erlaesst Verfuegungen", "de"),
    ]
    for a in arts:
        con.execute("INSERT INTO articles VALUES (?,?,?,?,?,?)", a)
        con.execute(
            "INSERT INTO articles_fts(rowid, sr_number, article_num, heading, text) "
            "VALUES (?,?,?,?,?)", (a[0], a[1], a[2], a[3], a[4]))
    con.commit()
    return con


def test_and_first_fill_recovers_partial_match(monkeypatch):
    # "sistierung aussetzung verfahren": only ParlG 87 has all three (AND).
    # ZPO 126 lacks "aussetzung" -> it must arrive via the OR fill, BELOW ParlG.
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    out = m._search_laws_federal(
        "sistierung aussetzung verfahren", None, "de", 10,
        raw_query="sistierung aussetzung verfahren",
        or_query="sistierung OR aussetzung OR verfahren",
    )
    keys = [(r["sr_number"], r["article_num"]) for r in out]
    assert ("171.10", "87") in keys                       # strict all-terms hit
    assert ("272", "126") in keys                         # recovered by OR fill (was zero before)
    assert keys.index(("171.10", "87")) < keys.index(("272", "126"))  # exact hit on top


def test_no_fill_when_strict_fills_page(monkeypatch):
    # The strict pass alone fills the single slot -> the OR form must NOT be
    # consulted and must not pull in the partial-match noise (ParlG 87).
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    out = m._search_laws_federal(
        "gericht verfahren", None, "de", 1,
        raw_query="gericht verfahren", or_query="gericht OR verfahren",
    )
    keys = [(r["sr_number"], r["article_num"]) for r in out]
    assert keys == [("272", "126")]
