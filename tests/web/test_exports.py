"""Practitioner-export module — pure functions, no DB / network calls."""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture(scope="module")
def exports():
    return importlib.import_module("exports")


@pytest.fixture
def sample_decision():
    return {
        "decision_id": "bge_BGE_140_III_86",
        "citation_string_de": "BGE 140 III 86",
        "citation_string_fr": "ATF 140 III 86",
        "citation_string_it": "DTF 140 III 86",
        "court": "bge",
        "court_name": "Bundesgericht",
        "decision_date": "2014-03-15",
        "docket_number": "4A_321/2013",
        "language": "de",
        "regeste": "Art. 42 Abs. 2 BGG; Pflicht zur Begründung der Rechtsverletzungen.",
        "full_text": "Erwägungen:\n\n1. Allgemein.\n\n2. Spezifisch.\n",
    }


# ── BibTeX ─────────────────────────────────────────────────────────

def test_bibtex_basic_shape(exports, sample_decision):
    body, mt, fname = exports.render_bibtex(sample_decision)
    assert "bibtex" in mt
    assert fname.endswith(".bib")
    text = body.decode("utf-8")
    assert text.startswith("@misc{")
    assert "bgeBGE140III86" in text          # bib key
    assert "BGE 140 III 86" in text
    assert "Bundesgericht" in text
    assert "2014" in text
    assert "https://mcp.opencaselaw.ch/entscheid/bge_BGE_140_III_86" in text


def test_bibtex_no_year_when_date_missing(exports, sample_decision):
    sample_decision["decision_date"] = ""
    body, _, _ = exports.render_bibtex(sample_decision)
    assert "year" not in body.decode("utf-8")


def test_bibtex_braces_escaped(exports, sample_decision):
    """Curly braces inside field values would break BibTeX parsing."""
    sample_decision["docket_number"] = "ABC{XYZ}123"
    text = exports.render_bibtex(sample_decision)[0].decode("utf-8")
    assert "{XYZ}" not in text   # raw braces must have been replaced
    assert "ABC(XYZ)123" in text


# ── RIS ────────────────────────────────────────────────────────────

def test_ris_basic_shape(exports, sample_decision):
    body, mt, fname = exports.render_ris(sample_decision)
    assert "research-info-systems" in mt
    assert fname.endswith(".ris")
    text = body.decode("utf-8")
    # Required RIS structure: TY first, ER last
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert lines[0].startswith("TY  - CASE")
    assert lines[-1].startswith("ER  -")
    assert "TI  - BGE 140 III 86" in text
    assert "DA  - 2014/03/15/" in text       # RIS date format
    assert "AN  - 4A_321/2013" in text       # accession (docket)
    assert "LA  - de" in text
    assert "ID  - bge_BGE_140_III_86" in text


def test_ris_uses_crlf_line_endings(exports, sample_decision):
    """Some RIS parsers (older EndNote) require CRLF."""
    body, _, _ = exports.render_ris(sample_decision)
    assert b"\r\n" in body


# ── DOCX (or txt fallback) ─────────────────────────────────────────

def test_docx_or_txt_fallback(exports, sample_decision):
    """Either python-docx is installed (binary docx, ZIP magic 'PK'),
    or the txt fallback is served."""
    body, mt, fname = exports.render_docx(sample_decision, [])
    if "wordprocessingml" in mt:
        assert body.startswith(b"PK")        # docx is a zip
        assert fname.endswith(".docx")
        assert len(body) > 1000              # non-empty document
    else:
        # plain-text fallback
        text = body.decode("utf-8")
        assert "BGE 140 III 86" in text
        assert "Bundesgericht" in text
        assert fname.endswith(".txt")


def test_docx_includes_paragraphs_when_provided(exports, sample_decision):
    paragraphs = [
        {"e_number": "1", "text": "Erste Erwägung mit eindeutigem Marker_AAA."},
        {"e_number": "2.3", "text": "Zweite Erwägung mit Marker_BBB."},
    ]
    body, mt, _ = exports.render_docx(sample_decision, paragraphs)
    if "wordprocessingml" in mt:
        # docx is a zip; check the document.xml part contains our text
        import zipfile, io
        zf = zipfile.ZipFile(io.BytesIO(body))
        doc_xml = zf.read("word/document.xml").decode("utf-8")
        assert "Marker_AAA" in doc_xml
        assert "Marker_BBB" in doc_xml
    else:
        text = body.decode("utf-8")
        assert "Marker_AAA" in text
        assert "Marker_BBB" in text


# ── Atom feed ──────────────────────────────────────────────────────

def test_atom_feed_well_formed(exports, sample_decision):
    body, mt, fname = exports.render_atom_feed(
        court="bger", court_label="Bundesgericht",
        decisions=[sample_decision],
    )
    assert "atom+xml" in mt
    assert fname.endswith(".xml")
    text = body.decode("utf-8")
    # Parse to verify well-formed XML
    import xml.etree.ElementTree as ET
    tree = ET.fromstring(text)
    assert tree.tag.endswith("feed")
    entries = tree.findall("{http://www.w3.org/2005/Atom}entry")
    assert len(entries) == 1
    title = entries[0].find("{http://www.w3.org/2005/Atom}title")
    assert title is not None
    assert "BGE 140 III 86" in title.text
    link = entries[0].find("{http://www.w3.org/2005/Atom}link")
    assert link.attrib.get("href") == \
        "https://mcp.opencaselaw.ch/entscheid/bge_BGE_140_III_86"


def test_atom_empty_feed(exports):
    """Empty decision list still produces a valid feed (no entries)."""
    body, _, _ = exports.render_atom_feed(
        court="bger", court_label="Bundesgericht", decisions=[],
    )
    import xml.etree.ElementTree as ET
    tree = ET.fromstring(body.decode("utf-8"))
    assert tree.findall("{http://www.w3.org/2005/Atom}entry") == []


def test_atom_html_escapes_unsafe_chars(exports):
    """A regeste with < > & should be escaped, not break the XML."""
    decision = {
        "decision_id": "bger_test_1",
        "citation_string_de": "BGer 1A_1/2024 <b>injection</b>",
        "decision_date": "2024-01-15",
        "regeste": "Art. 5 & Art. 6 — special chars: < > \"",
    }
    body, _, _ = exports.render_atom_feed(
        court="bger", court_label="Bundesgericht <inj>",
        decisions=[decision],
    )
    import xml.etree.ElementTree as ET
    tree = ET.fromstring(body.decode("utf-8"))
    # Successfully parsed → escaping worked. Inspect the entry title
    # (not the feed title) for the injection-shaped string.
    entry = tree.find("{http://www.w3.org/2005/Atom}entry")
    entry_title = entry.find("{http://www.w3.org/2005/Atom}title")
    assert "<b>injection</b>" in entry_title.text   # literal, not parsed as HTML


# ── Helpers ────────────────────────────────────────────────────────

def test_bib_key_strips_punctuation(exports):
    assert exports._bib_key("bge_BGE_140_III_86") == "bgeBGE140III86"
    assert exports._bib_key("bger_4A_747/2012")    == "bger4A7472012"
    assert exports._bib_key("") == "decision"


def test_safe_year_extracts_first_4_digits(exports):
    assert exports._safe_year("2014-03-15") == "2014"
    assert exports._safe_year("2014") == "2014"
    assert exports._safe_year("") == ""
    assert exports._safe_year("nope") == ""
