"""Tests for the two new Materialien MCP handlers:
  • _handle_search_botschaft — topical FTS5 across verbatim Botschaft corpus
  • _handle_get_article_history — per-article chronological timeline

Both use a tiny in-process materialien.db fixture so the tests don't
need the production corpus on disk.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


# ── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def tiny_materialien_db(tmp_path, monkeypatch):
    """Build a self-contained materialien.db with the Phase 2 tables
    populated by one Botschaft + 4 paragraphs (one anchored to an
    article). Each test gets a fresh DB.
    """
    db_path = tmp_path / "materialien.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE botschaft_documents (
            botschaft_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            bbl_year        INTEGER NOT NULL,
            bbl_page        INTEGER NOT NULL,
            bbl_citation    TEXT NOT NULL,
            eli_uri         TEXT,
            title           TEXT,
            publication_date TEXT,
            source_url      TEXT NOT NULL,
            format          TEXT NOT NULL,
            language        TEXT NOT NULL,
            page_count      INTEGER,
            text_hash       TEXT,
            ingested_at     TEXT NOT NULL,
            UNIQUE(bbl_year, bbl_page, language)
        );
        CREATE TABLE botschaft_paragraphs (
            paragraph_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            botschaft_id    INTEGER NOT NULL,
            para_order      INTEGER NOT NULL,
            page_number     INTEGER,
            section_path    TEXT,
            article_anchor  TEXT,
            text            TEXT NOT NULL,
            text_length     INTEGER NOT NULL
        );
        CREATE VIRTUAL TABLE botschaft_paragraphs_fts USING fts5(
            text, section_path, article_anchor UNINDEXED,
            content='botschaft_paragraphs', content_rowid='paragraph_id'
        );
        CREATE TRIGGER bp_ai AFTER INSERT ON botschaft_paragraphs BEGIN
            INSERT INTO botschaft_paragraphs_fts(rowid, text, section_path, article_anchor)
            VALUES (new.paragraph_id, new.text, new.section_path, new.article_anchor);
        END;
        CREATE TABLE article_botschaft_links (
            sr_number       TEXT NOT NULL,
            article         TEXT NOT NULL,
            botschaft_id    INTEGER NOT NULL,
            relation        TEXT NOT NULL,
            evidence        TEXT,
            PRIMARY KEY (sr_number, article, botschaft_id, relation)
        );
    """)
    # One document with paragraphs covering the test cases.
    conn.execute(
        """INSERT INTO botschaft_documents
        (bbl_year, bbl_page, bbl_citation, eli_uri, title, publication_date,
         source_url, format, language, page_count, ingested_at)
        VALUES (2024, 2485, 'BBl 2024 2485',
                'https://fedlex.data.admin.ch/eli/fga/2024/2485',
                'Botschaft Test', '2024-08-12',
                'https://example.test/2024-2485', 'akoma-ntoso-xml', 'de',
                42, '2026-05-11T10:00:00+00:00')""")
    bid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # 4 paragraphs.
    for i, (anchor, sec, text) in enumerate([
        (None,  "Allgemeines", "Die Vorlage regelt den Vaterschaftsurlaub im Sinne der parlamentarischen Initiative."),
        ("1",   "Zu Artikel 1", "Artikel 1 legt den Anwendungsbereich Vaterschaftsurlaub fest."),
        ("2",   "Zu Artikel 2", "Artikel 2 normiert die Dauer und die finanziellen Folgen."),
        (None,  "Beratung",     "Die parlamentarische Beratung zum Klimaschutz war kontrovers."),
    ], 1):
        conn.execute(
            """INSERT INTO botschaft_paragraphs
            (botschaft_id, para_order, page_number, section_path,
             article_anchor, text, text_length)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (bid, i, i + 10, sec, anchor, text, len(text)),
        )
    conn.execute(
        """INSERT INTO article_botschaft_links
        (sr_number, article, botschaft_id, relation, evidence)
        VALUES ('999.99', '1', ?, 'enacted', 'test fixture')""",
        (bid,),
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("SWISS_CASELAW_MATERIALIEN_DB", str(db_path))
    return db_path


# ── search_botschaft ─────────────────────────────────────────────────────


def test_search_botschaft_finds_anchored_passage(tiny_materialien_db):
    import mcp_server
    res = mcp_server._handle_search_botschaft(
        query="Vaterschaftsurlaub",
        limit=10,
    )
    assert res["total"] >= 1
    snippets = [r["snippet"] for r in res["results"]]
    assert any("Vaterschaftsurlaub" in s for s in snippets)
    # At least one result should have the article_anchor populated.
    assert any(r["article_anchor"] for r in res["results"])


def test_search_botschaft_language_filter_optional(tiny_materialien_db):
    """When `language` is None, search must NOT silently filter to one
    language. The contract is enforced by handler behaviour, not just
    the input schema.
    """
    import mcp_server
    # No language → all languages searched
    res_all = mcp_server._handle_search_botschaft(query="Klimaschutz", language=None)
    assert res_all["total"] >= 1
    # Explicit "de" → still finds it
    res_de = mcp_server._handle_search_botschaft(query="Klimaschutz", language="de")
    assert res_de["total"] >= 1
    # Explicit "fr" → no hits in our DE-only fixture (matches contract:
    # filter is APPLIED when caller asks, not silently)
    res_fr = mcp_server._handle_search_botschaft(query="Klimaschutz", language="fr")
    assert res_fr["total"] == 0


def test_search_botschaft_empty_query_rejected(tiny_materialien_db):
    import mcp_server
    res = mcp_server._handle_search_botschaft(query="")
    assert "error" in res


def test_search_botschaft_no_materialien_db(monkeypatch, tmp_path):
    """Handler must degrade gracefully when materialien.db is absent."""
    import mcp_server
    monkeypatch.setenv("SWISS_CASELAW_MATERIALIEN_DB",
                       str(tmp_path / "missing.db"))
    res = mcp_server._handle_search_botschaft(query="anything")
    assert "error" in res
    assert "materialien" in res["error"].lower()


def test_search_botschaft_phase2_tables_missing(tmp_path, monkeypatch):
    """Existing DB without Phase 2 tables → empty results + _hint."""
    db = tmp_path / "old.db"
    sqlite3.connect(db).executescript("CREATE TABLE meta (k TEXT)")
    monkeypatch.setenv("SWISS_CASELAW_MATERIALIEN_DB", str(db))
    import mcp_server
    res = mcp_server._handle_search_botschaft(query="test")
    assert res["total"] == 0
    assert "_hint" in res
    assert "phase 2" in res["_hint"].lower() or "fts5" in res["_hint"].lower()


# ── get_article_history ──────────────────────────────────────────────────


def test_get_article_history_returns_timeline(tiny_materialien_db, monkeypatch):
    """Without statutes.db / graph / commentaries available, the timeline
    still surfaces the linked Botschaft entry from materialien.db.
    """
    import mcp_server
    # Stub out other lookups so the test is independent of the
    # production sidecar DBs.
    monkeypatch.setattr(mcp_server, "_get_legislation_local",
                        lambda **kw: None)
    monkeypatch.setattr(mcp_server, "_find_leading_cases",
                        lambda **kw: {"results": []})
    monkeypatch.setattr(mcp_server, "get_commentary",
                        lambda **kw: {"error": "stubbed"})

    res = mcp_server._handle_get_article_history(
        sr_number="999.99", article="1",
    )
    assert res["sr_number"] == "999.99"
    assert res["article"] == "1"
    # Timeline should contain the linked Botschaft entry.
    kinds = [e["kind"] for e in res["timeline"]]
    assert "botschaft" in kinds
    bot_entry = next(e for e in res["timeline"] if e["kind"] == "botschaft")
    assert bot_entry["bbl_citation"] == "BBl 2024 2485"
    assert bot_entry["relation"] == "enacted"
    assert res["summary"]["botschaft_count"] == 1


def test_get_article_history_validates_inputs():
    import mcp_server
    assert "error" in mcp_server._handle_get_article_history(
        sr_number="", article="1")
    assert "error" in mcp_server._handle_get_article_history(
        sr_number="220", article="")


def test_get_article_history_no_materialien_db(monkeypatch, tmp_path):
    """Handler should still return a (statute-less, materialien-less)
    skeleton rather than crashing."""
    import mcp_server
    monkeypatch.setenv("SWISS_CASELAW_MATERIALIEN_DB",
                       str(tmp_path / "missing.db"))
    monkeypatch.setattr(mcp_server, "_get_legislation_local",
                        lambda **kw: None)
    monkeypatch.setattr(mcp_server, "_find_leading_cases",
                        lambda **kw: {"results": []})
    monkeypatch.setattr(mcp_server, "get_commentary",
                        lambda **kw: {"error": "stubbed"})
    res = mcp_server._handle_get_article_history(
        sr_number="220", article="41",
    )
    assert "error" not in res
    assert res["timeline"] == []
    assert res["summary"]["botschaft_count"] == 0
