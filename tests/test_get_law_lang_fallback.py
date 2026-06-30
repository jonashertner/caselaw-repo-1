"""Issue #32: get_law(sr_number="272", article="168") returned "No articles
found" because ZPO Art. 168 is present in statutes.db in fr/it only (a gap in
the Fedlex German mirror), and get_law defaults to language="de". The handler
now falls back to any available language and flags it, instead of erroring out.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _fixture_conn():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, title_de TEXT, title_fr TEXT,
            title_it TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT, consolidation_date TEXT);
        CREATE TABLE articles (sr_number TEXT, article_num TEXT, heading TEXT, text TEXT, lang TEXT);
        """
    )
    con.execute("INSERT INTO laws VALUES ('272','ZPO de','CPC fr','CPC it','ZPO','CPC','CPC','2025-01-01')")
    # Art 168: present ONLY in fr + it (the bug)
    con.execute("INSERT INTO articles VALUES ('272','168','Moyens de preuve','Les moyens de preuve sont le temoignage...','fr')")
    con.execute("INSERT INTO articles VALUES ('272','168','Mezzi di prova','I mezzi di prova sono la testimonianza...','it')")
    # Art 167: present in de
    con.execute("INSERT INTO articles VALUES ('272','167','Folgen','Verweigert eine Partei...','de')")
    con.commit()
    return con


def test_falls_back_to_other_language_when_de_missing(monkeypatch):
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    r = m.get_law(sr_number="272", article="168", language="de")
    assert r.get("articles"), "Art. 168 (fr/it only) must return text, not empty"
    assert r["articles"][0]["article_num"] == "168"
    fb = r.get("article_language_fallback")
    assert fb and fb["requested"] == "de" and fb["served"] in ("fr", "it")


def test_no_fallback_when_requested_language_present(monkeypatch):
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    r = m.get_law(sr_number="272", article="167", language="de")
    assert r.get("articles") and "article_language_fallback" not in r


def test_genuinely_absent_article_still_empty(monkeypatch):
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    r = m.get_law(sr_number="272", article="9999", language="de")
    assert not r.get("articles") and "article_language_fallback" not in r


def test_rendered_text_shows_fallback_note(monkeypatch):
    con = _fixture_conn()
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: con)
    r = m.get_law(sr_number="272", article="168", language="de")
    rendered = m._format_get_law_response(r)
    assert "No articles found" not in rendered
    assert "not available in 'de'" in rendered
    assert "Les moyens de preuve" in rendered
