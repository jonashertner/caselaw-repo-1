"""Verwaltungspraxis was in the corpus but undiscoverable, and its size was misreported.

A law firm evaluating the server in a Copilot agent asked whether we had any plans to
add Kreisschreiben and Wegleitungen. We had 790 of them. Their agent could not find
them: the server instructions never mentioned administrative practice and the
QUESTION -> TOOL ROUTING table had no row pointing at search_practice.

Two further defects in the same area:
  * `total` was `len(rows)`, i.e. capped by `limit`, so "how much guidance exists on
    this topic" was answered with the page size.
  * The inputSchema advertised sources/authorities/doc_types that return zero rows
    (SSK, ARE, EPA, handbuch, merkblatt) while omitting the 153 MWST documents.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _fixture_conn():
    """Practice DB with 25 matching rows, so total != page size is observable."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE practice (
            doc_id TEXT, source TEXT, issuing_authority TEXT, doc_type TEXT,
            doc_number TEXT, title TEXT, date TEXT, language TEXT,
            url TEXT, pdf_url TEXT, body_text TEXT, topics_json TEXT, scraped_at TEXT
        );
        CREATE VIRTUAL TABLE practice_fts USING fts5(title, doc_number, body_text);
        """
    )
    for i in range(1, 26):
        c.execute(
            "INSERT INTO practice (rowid, doc_id, source, issuing_authority, doc_type, "
            "doc_number, title, date, language, url, pdf_url, body_text) VALUES "
            "(?,?,'estv_ks','ESTV','kreisschreiben',?,?,'2024-01-01','de',"
            "'http://x',?,'Verdecktes Eigenkapital nach Art. 65 DBG')",
            (i, f"d{i}", f"KS {i}", f"Kreisschreiben Nr. {i}", f"http://x/{i}.pdf"),
        )
        c.execute(
            "INSERT INTO practice_fts (rowid, title, doc_number, body_text) VALUES "
            "(?,?,?, 'Verdecktes Eigenkapital nach Art. 65 DBG')",
            (i, f"Kreisschreiben Nr. {i}", f"KS {i}"),
        )
    c.commit()
    return c


# ------------------------------------------------------------------ total

def test_total_is_corpus_count_not_page_size(monkeypatch):
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    res = mcp_server._search_practice(query="Eigenkapital", limit=10)
    assert res["total"] == 25, res           # was 10 — the page size
    assert res["returned"] == 10
    assert len(res["results"]) == 10


def test_total_respects_filters(monkeypatch):
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    hit = mcp_server._search_practice(query="Eigenkapital", issuing_authority="ESTV", limit=5)
    assert hit["total"] == 25 and hit["returned"] == 5
    miss = mcp_server._search_practice(query="Eigenkapital", issuing_authority="SEM", limit=5)
    assert miss["total"] == 0 and miss["results"] == []


def test_formatter_discloses_truncation(monkeypatch):
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    out = mcp_server._format_search_practice_response(
        mcp_server._search_practice(query="Eigenkapital", limit=10))
    assert "Found 25" in out
    assert "showing the 10" in out


def test_pdf_rendered_as_markdown_link(monkeypatch):
    # Bare URLs are the form Microsoft documents as most likely to be stripped
    # by Copilot's @mention output sanitisation.
    monkeypatch.setattr(mcp_server, "_open_practice_db", _fixture_conn)
    out = mcp_server._format_search_practice_response(
        mcp_server._search_practice(query="Eigenkapital", limit=1))
    assert "](http://x/1.pdf)" in out


# ----------------------------------------------------------- discoverability

def _instructions() -> str:
    return mcp_server.server.instructions or ""


def test_instructions_mention_administrative_practice():
    ins = _instructions()
    for term in ("Verwaltungspraxis", "Kreisschreiben", "search_practice"):
        assert term in ins, f"{term!r} missing from server instructions"


def test_routing_table_routes_the_german_trigger_words():
    ins = _instructions()
    routing = ins[ins.index("QUESTION → TOOL ROUTING"):]
    for term in ("Kreisschreiben", "Weisung", "Wegleitung"):
        assert term in routing, f"{term!r} has no routing row"
    assert "search_practice" in routing
    # pre-2017 federal administrative decisions live in the decision corpus
    assert "ch_vb" in routing


# ------------------------------------------------------------------ schema

def test_schema_advertises_only_populated_values():
    """SSK / ARE / EPA / handbuch / merkblatt return zero rows — do not offer them."""
    src = Path(REPO / "mcp_server.py").read_text(encoding="utf-8")
    start = src.index('name="search_practice"')
    block = src[start:start + 4000]
    for phantom in ("ssk_ks", "are_vollzug", "epa_personalrecht",
                    "handbuch", "merkblatt"):
        assert phantom not in block, f"{phantom!r} is advertised but returns nothing"
    for real in ("seco_arg", "estv_ks", "estv_mwst", "bafu_vollzug", "sem_weisungen",
                 "wegleitung", "kreisschreiben", "vollzugshilfe", "mwst_info"):
        assert real in block, f"{real!r} exists in the corpus but is not advertised"


def test_schema_names_the_uncovered_authorities():
    """The description must say what is absent, so a gap is not read as 'no guidance'."""
    src = Path(REPO / "mcp_server.py").read_text(encoding="utf-8")
    start = src.index('name="search_practice"')
    block = src[start:start + 4000]
    for absent in ("BSV", "FINMA", "BAG", "cantonal"):
        assert absent in block, f"description does not disclose that {absent} is absent"
