"""#89 second half: a result must match ≥1 substantive query term.

The reporter's repro surfaced "Le droit pour les lycéens" as a top hit for
a corruption/contract-nullity query, matching only on `de`, `du`, `droit`.
Corpus enrichment (2026-08-24: 25k→44.5k records) put a real hit at rank 1
but function-word noise still filled ranks 2-5. The gate drops rows whose
visible fields (title, authors, snippet) share zero substantive terms with
the query, refills the page from the over-fetch, and — per the reporter's
own words — prefers an honestly empty result over unrelated records.

Fixture DB mirrors the real schema (publications + external-content FTS5
with the same column order; snippet() targets column 2 = abstract).
"""
import sqlite3

import pytest

import mcp_server

SCHEMA = """
CREATE TABLE publications (
    id INTEGER PRIMARY KEY, pub_id TEXT, source TEXT, pub_type TEXT,
    title TEXT, authors TEXT, abstract TEXT, full_text TEXT, journal TEXT,
    keywords TEXT, subjects TEXT, language TEXT, year INTEGER, doi TEXT,
    url TEXT, pdf_url TEXT, license TEXT
);
CREATE VIRTUAL TABLE publications_fts USING fts5(
    title, authors, abstract, full_text, journal, keywords, subjects,
    content='publications', content_rowid='id',
    tokenize='unicode61 remove_diacritics 2'
);
"""

# Production pathology, faithfully: the search is implicit AND, and whole
# textbooks match because their FULL TEXT contains every query term once,
# while snippet() reads column 2 (the abstract), which holds only function
# words — so nothing visible connects the record to the query.
_ALL_TERMS = "corruption pot de vin nullité du contrat"

DOCS = [
    # A genuinely relevant record: substantive terms in title AND abstract.
    ("unige:1", "unige_law", "article",
     "La restitution des profits issus de la corruption",
     "Chappuis, Christine",
     "restitution de la corruption et nullité du contrat en droit privé",
     _ALL_TERMS + " et la restitution des profits", "SJ", "", "", "fr", 2011),
    # The reporter's noise pattern: every term deep in the book, abstract
    # made of function words only.
    ("unine:2", "libra_unine", "book",
     "Le droit pour les lycéens", "Müller, Christoph",
     "une introduction à la vie de tous les jours",
     "chapitre 12: " + _ALL_TERMS + " et beaucoup plus", "", "", "", "fr", 2025),
    ("unine:3", "libra_unine", "article",
     "Le travail au quotidien", "Dunand, Jean-Philippe",
     "les rapports de tous les jours et leur cadre",
     "annexe: " + _ALL_TERMS, "", "", "", "fr", 2025),
]


@pytest.fixture()
def scholarship(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    for i, (pid, src, ptype, title, authors, abstract, ft, journal,
            kw, subj, lang, year) in enumerate(DOCS, 1):
        conn.execute(
            "INSERT INTO publications (id, pub_id, source, pub_type, title,"
            " authors, abstract, full_text, journal, keywords, subjects,"
            " language, year, doi, url, pdf_url, license)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'','','','')",
            (i, pid, src, ptype, title, authors, abstract, ft, journal,
             kw, subj, lang, year))
        conn.execute(
            "INSERT INTO publications_fts (rowid, title, authors, abstract,"
            " full_text, journal, keywords, subjects)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (i, title, authors, abstract, ft, journal, kw, subj))
    conn.commit()
    monkeypatch.setattr(mcp_server, "_get_scholarship_conn", lambda: conn)
    return conn


def test_zero_substantive_match_rows_are_suppressed(scholarship):
    out = mcp_server.search_scholarship(
        query="corruption pot-de-vin nullité du contrat", limit=8)
    ids = [r["pub_id"] for r in out["results"]]
    assert "unige:1" in ids                       # real hit survives
    assert "unine:2" not in ids                   # function-word noise gated
    assert out["suppressed_zero_match"] >= 1


def test_all_noise_yields_honest_empty(scholarship):
    # A query whose substantive terms live only in the noise docs' full
    # text ('pot', 'vin' — never in a title/abstract): everything retrieved
    # is function-word noise, the gate empties the page, and the formatter
    # says so instead of serving unrelated records.
    out = mcp_server.search_scholarship(query="pot du vin", limit=8)
    noise_ids = {"unine:2", "unine:3"}
    assert not (noise_ids & {r["pub_id"] for r in out["results"]})
    if not out["results"] and out.get("suppressed_zero_match"):
        text = mcp_server._format_search_scholarship_response(out)
        assert "suppressed" in text
        assert "function" in text


def test_browse_path_is_never_gated(scholarship):
    out = mcp_server.search_scholarship(query="", author="Müller", limit=8)
    assert [r["pub_id"] for r in out["results"]] == ["unine:2"]
    assert out.get("suppressed_zero_match", 0) == 0
