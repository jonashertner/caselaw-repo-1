"""get_doctrine / get_law must bridge the statute path to the VERBATIM Botschaft
corpus. When a statute article has an `article_botschaft_links` row,
`_get_materialien_for_doctrine` attaches a `verbatim_botschaft` pointer
(get_article_purpose + bbl_citation) instead of returning "no materials" just
because the article isn't in the 167-row LLM-digest set.

Regression guard for the 2026-06-21 assessment: the natural statute path only
touched amendment_refs + the digest table and never `article_botschaft_links`,
so the 410K-paragraph verbatim corpus was invisible on the obvious tool path.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _fixture_conn(with_link: bool = True, with_digest: bool = False) -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE materialien(law_code TEXT, article TEXT, legislative_intent TEXT,
                                 key_arguments TEXT, bbl_ref TEXT, sr_number TEXT);
        CREATE TABLE amendment_refs(sr_number TEXT, article TEXT, ref_type TEXT,
                                    year INT, page INT, fedlex_url TEXT, context TEXT);
        CREATE TABLE article_botschaft_links(sr_number TEXT, article TEXT,
                                    botschaft_id INT, relation TEXT, evidence TEXT);
        CREATE TABLE botschaft_documents(botschaft_id INT, bbl_citation TEXT, eli_uri TEXT);
        """
    )
    if with_link:
        c.execute("INSERT INTO article_botschaft_links VALUES('220','41',1913,'considered','amendment_refs:1')")
        c.execute("INSERT INTO botschaft_documents VALUES(1913,'BBl 2020 1234','https://fedlex.admin.ch/eli/fga/2020/1234')")
    if with_digest:
        c.execute("INSERT INTO materialien VALUES('OR','41','intent text','args text','BBl 2020 1234','220')")
    return c


def test_doctrine_bridges_to_verbatim_botschaft(monkeypatch):
    conn = _fixture_conn(with_link=True)
    monkeypatch.setattr(mcp_server, "_get_materialien_conn", lambda: conn)
    out = mcp_server._get_materialien_for_doctrine("OR", "41")
    assert out is not None, "must not return None when an article->Botschaft link exists"
    vb = out.get("verbatim_botschaft")
    assert vb, "must attach a verbatim_botschaft pointer"
    assert vb["tool"] == "get_article_purpose"
    assert "BBl 2020 1234" in vb["bbl_citations"]
    assert "get_article_purpose" in vb["call"]
    assert '"220"' in vb["call"] and '"41"' in vb["call"]


def test_digest_path_also_attaches_pointer(monkeypatch):
    conn = _fixture_conn(with_link=True, with_digest=True)
    monkeypatch.setattr(mcp_server, "_get_materialien_conn", lambda: conn)
    out = mcp_server._get_materialien_for_doctrine("OR", "41")
    assert out.get("legislative_intent"), "digest still returned when present"
    assert out.get("verbatim_botschaft"), "digest path must also bridge to verbatim"


def test_no_link_no_pointer(monkeypatch):
    conn = _fixture_conn(with_link=False)
    monkeypatch.setattr(mcp_server, "_get_materialien_conn", lambda: conn)
    out = mcp_server._get_materialien_for_doctrine("OR", "999")
    assert out is None or "verbatim_botschaft" not in out


# ── PATH B: decision -> materials ────────────────────────────────────────────

def test_materials_for_decision(monkeypatch):
    g = sqlite3.connect(":memory:")
    g.row_factory = sqlite3.Row
    g.executescript(
        """
        CREATE TABLE statutes(statute_id TEXT, law_code TEXT, article TEXT, paragraph TEXT);
        CREATE TABLE decision_statutes(decision_id TEXT, statute_id TEXT, mention_count INT);
        INSERT INTO statutes VALUES('ART.41.OR','OR','41','');
        INSERT INTO statutes VALUES('ART.99.XX','XX','99','');
        INSERT INTO decision_statutes VALUES('bge_x','ART.41.OR',3);
        INSERT INTO decision_statutes VALUES('bge_x','ART.99.XX',1);
        """
    )
    m = _fixture_conn(with_link=True)
    monkeypatch.setattr(mcp_server, "_get_graph_conn", lambda: g)
    monkeypatch.setattr(mcp_server, "_get_materialien_conn", lambda: m)
    out = mcp_server._materials_for_decision("bge_x")
    assert out, "decision citing Art. 41 OR (Botschaft-linked) must yield a pointer"
    art41 = [x for x in out if x["article"] == "41"]
    assert art41 and art41[0]["sr_number"] == "220"
    assert "get_article_purpose" in art41[0]["call"]
    # XX/99 has no Botschaft link -> excluded
    assert not any(x["article"] == "99" for x in out)


def test_materials_for_decision_no_citations(monkeypatch):
    g = sqlite3.connect(":memory:")
    g.row_factory = sqlite3.Row
    g.executescript(
        "CREATE TABLE statutes(statute_id TEXT, law_code TEXT, article TEXT, paragraph TEXT);"
        "CREATE TABLE decision_statutes(decision_id TEXT, statute_id TEXT, mention_count INT);"
    )
    monkeypatch.setattr(mcp_server, "_get_graph_conn", lambda: g)
    monkeypatch.setattr(mcp_server, "_get_materialien_conn", lambda: _fixture_conn(with_link=True))
    assert mcp_server._materials_for_decision("bge_unknown") == []


def test_materials_section_md_renders():
    md = mcp_server._format_materials_section_md(
        [{"law": "OR", "article": "41", "sr_number": "220",
          "bbl_citations": ["BBl 2020 1234"],
          "call": 'get_article_purpose(sr_number="220", article="41")'}]
    )
    assert "Botschaft" in md and "Art. 41 OR" in md and "get_article_purpose" in md
    assert mcp_server._format_materials_section_md([]) == ""
