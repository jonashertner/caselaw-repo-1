"""Tests for the verbatim Botschaft corpus pipeline (v0.2).

The v0.2 module wires three pieces together:
  - URL resolution via Fedlex SPARQL (``resolve_manifestation``)
  - Akoma Ntoso XML parser (``parse_akoma_ntoso_xml``)
  - PDF fallback (``parse_botschaft_text`` from v0.1)

These tests don't hit the SPARQL endpoint or the Fedlex filestore — they
unit-test the pure functions and the schema migration.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from search_stack.build_botschaft_corpus import (  # noqa: E402
    SCHEMA_SQL,
    _FORMAT_PRIORITY,
    bbl_citation,
    bbl_eli_uri,
    ensure_schema,
    parse_akoma_ntoso_xml,
    parse_botschaft_text,
)


# ── URL helpers ───────────────────────────────────────────────────────


def test_bbl_eli_uri_format() -> None:
    assert bbl_eli_uri(2024, 2945) == "https://fedlex.data.admin.ch/eli/fga/2024/2945"


def test_bbl_citation_format() -> None:
    assert bbl_citation(1999, 6013) == "BBl 1999 6013"


def test_format_priority_includes_xml_an() -> None:
    """xml-an is Akoma Ntoso XML — the cleanest source. Must come first."""
    assert _FORMAT_PRIORITY[0] == "xml-an"
    assert "pdf-a" in _FORMAT_PRIORITY


# ── Akoma Ntoso parser ────────────────────────────────────────────────


_AKN_FIXTURE = b"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
<act><body>
<chapter><heading>Allgemeiner Teil</heading>
<p>Das ist ein Einleitungsabsatz mit ausreichend Inhalt zum Testen.</p>
</chapter>
<chapter><heading>Besonderer Teil</heading>
<article eId="art_1"><heading>Artikel 1</heading>
<p>Diese Bestimmung legt den Geltungsbereich fest und gilt fuer alle.</p>
<p>Sie wurde 2024 angepasst um neue Anforderungen zu erfassen.</p>
</article>
<article eId="art_2">
<p>Persoenlicher Anwendungsbereich, definiert die betroffenen Personen.</p>
</article>
<article eId="art_41a">
<p>Letter-suffixed article (41a) - modern revision style.</p>
</article>
</chapter>
</body></act></akomaNtoso>"""


def test_akn_parser_yields_paragraphs() -> None:
    paras = list(parse_akoma_ntoso_xml(_AKN_FIXTURE))
    assert len(paras) == 5


def test_akn_parser_assigns_article_anchors() -> None:
    paras = list(parse_akoma_ntoso_xml(_AKN_FIXTURE))
    by_text_prefix = {p["text"][:25]: p for p in paras}
    assert by_text_prefix["Das ist ein Einleitungsab"]["article_anchor"] is None
    assert by_text_prefix["Diese Bestimmung legt den"]["article_anchor"] == "1"
    assert by_text_prefix["Sie wurde 2024 angepasst "]["article_anchor"] == "1"
    assert by_text_prefix["Persoenlicher Anwendungsb"]["article_anchor"] == "2"
    assert by_text_prefix["Letter-suffixed article ("]["article_anchor"] == "41a"


def test_akn_parser_tracks_section_path() -> None:
    paras = list(parse_akoma_ntoso_xml(_AKN_FIXTURE))
    assert paras[0]["section_path"] == "Allgemeiner Teil"
    assert paras[1]["section_path"] == "Besonderer Teil"
    assert paras[-1]["section_path"] == "Besonderer Teil"


def test_akn_parser_metadata_only_yields_nothing() -> None:
    """The 2024/2945 case: <akomaNtoso> with FRBR metadata but no <body>.
    Parser must yield 0 paragraphs cleanly so callers fall through to PDF."""
    metadata_only = b"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
<act><meta><identification><FRBRWork><FRBRname value="metadata only"/></FRBRWork>
</identification></meta></act></akomaNtoso>"""
    paras = list(parse_akoma_ntoso_xml(metadata_only))
    assert paras == []


def test_akn_parser_skips_short_paragraphs() -> None:
    """Same 20-char floor as the PDF parser — avoids picking up
    fragments like '<p>1.</p>'."""
    short = b"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
<act><body><p>x</p><p>Lange genug um eingeschlossen zu werden.</p></body></act>
</akomaNtoso>"""
    paras = list(parse_akoma_ntoso_xml(short))
    assert len(paras) == 1
    assert "Lange genug" in paras[0]["text"]


def test_akn_parser_handles_malformed_xml_gracefully() -> None:
    """Malformed XML must not crash — return empty iterator so caller
    falls through to PDF."""
    bad = b"<not><xml at all"
    paras = list(parse_akoma_ntoso_xml(bad))
    assert paras == []


# ── PDF parser invariants (v0.1, kept) ────────────────────────────────


def test_pdf_parser_tags_zu_artikel() -> None:
    """Same parser the v0.1 PoC used for PDF text."""
    sample_pages = [
        "Allgemeines\n\n"
        "Zu Artikel 1\nDiese Bestimmung legt den Geltungsbereich fest, "
        "siehe Art. 5.\n\n"
        "Zu Artikel 2\nPersoenlicher Anwendungsbereich.",
    ]
    paras = list(parse_botschaft_text(sample_pages, "de"))
    anchors = [p["article_anchor"] for p in paras]
    # Both "Zu Artikel 1" and "Zu Artikel 2" should be picked up as
    # article anchors (the surrounding mid-sentence "Art. 5" must NOT
    # be treated as an anchor).
    assert "1" in anchors
    assert "2" in anchors


# ── Schema migration ──────────────────────────────────────────────────


def test_ensure_schema_creates_required_tables(tmp_path: Path) -> None:
    db = tmp_path / "materialien_test.db"
    conn = sqlite3.connect(db)
    ensure_schema(conn)
    tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','index','trigger')"
        )
    }
    assert "botschaft_documents" in tables
    assert "botschaft_paragraphs" in tables
    assert "article_botschaft_links" in tables
    # FTS5 vtab + triggers
    assert "botschaft_paragraphs_fts" in tables
    assert "botschaft_paragraphs_ai" in tables  # insert trigger
    assert "botschaft_paragraphs_ad" in tables  # delete trigger
    conn.close()


def test_ensure_schema_is_idempotent(tmp_path: Path) -> None:
    """Critical for nightly re-runs — calling --schema-only twice in a
    row must not error."""
    db = tmp_path / "materialien_test2.db"
    conn = sqlite3.connect(db)
    ensure_schema(conn)
    ensure_schema(conn)  # second call should be no-op via IF NOT EXISTS
    n_tables = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
    ).fetchone()[0]
    # Just sanity-check that we have at least the expected core tables.
    assert n_tables >= 4
    conn.close()


def test_schema_format_check_constraint(tmp_path: Path) -> None:
    """The format column has a CHECK constraint allowing only
    akoma-ntoso-xml and pdf — the two stored format labels."""
    db = tmp_path / "materialien_test3.db"
    conn = sqlite3.connect(db)
    ensure_schema(conn)
    # Allowed values insert cleanly:
    conn.execute(
        "INSERT INTO botschaft_documents "
        "(bbl_year, bbl_page, bbl_citation, source_url, format, language, "
        " text_hash, ingested_at) "
        "VALUES (2024, 100, 'BBl 2024 100', 'http://example/x', 'pdf', 'de', "
        "'h', '2026-05-07T00:00:00Z')",
    )
    conn.execute(
        "INSERT INTO botschaft_documents "
        "(bbl_year, bbl_page, bbl_citation, source_url, format, language, "
        " text_hash, ingested_at) "
        "VALUES (2024, 101, 'BBl 2024 101', 'http://example/x', 'akoma-ntoso-xml', 'fr', "
        "'h', '2026-05-07T00:00:00Z')",
    )
    # Disallowed value rejected:
    import pytest
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO botschaft_documents "
            "(bbl_year, bbl_page, bbl_citation, source_url, format, language, "
            " text_hash, ingested_at) "
            "VALUES (2024, 102, 'BBl 2024 102', 'http://example/x', 'docx', 'it', "
            "'h', '2026-05-07T00:00:00Z')",
        )
    conn.close()
