"""What the search tools actually hand a user.

Four defects found 2026-07-29 by reading real production responses rather
than status codes:

  1. Raw FTS5 sentinels reached the user: search_commentaries showed
     ">>>Willensmangel<<<" where a highlight was meant. Same in
     search_materialien, search_botschaft and find_scholarship_citing_decision.
  2. search_commentaries returned one commentary many times — a limit-5
     search for "Willensmängel" gave two distinct articles in five slots,
     because a commentary is indexed as several sections.
  3. search_scholarship never printed pub_id, which get_scholarship and
     get_scholarship_full_text REQUIRE — so those two tools could not be
     reached from the only tool that discovers their input.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


# ── 1. sentinels never reach a user ───────────────────────────────────────

def test_render_fts_snippet_makes_markdown_bold():
    assert mcp_server._render_fts_snippet("a >>>b<<< c") == "a **b** c"


def test_render_fts_snippet_strips_unbalanced_markers():
    """A snippet cut mid-highlight must not emit a stray marker either way."""
    out = mcp_server._render_fts_snippet("a >>>b truncated")
    assert ">>>" not in out and "" not in out


def test_render_fts_snippet_passes_plain_text_through():
    assert mcp_server._render_fts_snippet("nothing to mark") == "nothing to mark"
    assert mcp_server._render_fts_snippet("") == ""
    assert mcp_server._render_fts_snippet(None) == ""


def test_commentary_formatter_renders_the_snippet():
    out = mcp_server._format_search_commentaries_response({
        "query": "Willensmängel", "count": 1, "source": "OnlineKommentar.ch",
        "results": [{"abbreviation": "OR", "sr_number": "220",
                     "article_num": "785", "title": "Art. 785 OR",
                     "authors": ["A. Schneuwly"], "language": "de",
                     "snippet": "es wird lediglich ein >>>Willensmangel<<< vermutet",
                     "html_link": "https://onlinekommentar.ch/de/kommentare/or785"}],
    })
    assert ">>>" not in out and "<<<" not in out
    assert "**Willensmangel**" in out


def test_scholarship_citing_decision_formatter_renders_the_snippet():
    out = mcp_server._format_find_scholarship_citing_decision_response({
        "decision_id": "bge_BGE_125_III_70", "count": 1,
        "results": [{"source": "zora", "year": 2020, "title": "T",
                     "authors": "A", "snippet": "zitiert >>>BGE 125 III 70<<< dort"}],
    })
    assert ">>>" not in out
    assert "**BGE 125 III 70**" in out


def test_no_user_facing_formatter_emits_a_raw_snippet():
    """Guard against the next formatter that forgets. Any formatter
    interpolating r['snippet'] straight into user text is the bug."""
    import re
    src = Path(REPO, "mcp_server.py").read_text(encoding="utf-8")
    bad = []
    for m in re.finditer(r"^\s*(?:text \+=|lines\.append\().*\{r\['snippet'\]\}",
                         src, re.MULTILINE):
        line_no = src[:m.start()].count("\n") + 1
        bad.append(f"{line_no}: {m.group(0).strip()[:90]}")
    assert not bad, "raw FTS snippet interpolated into user-facing text:\n" + "\n".join(bad)


# ── 2. one commentary, one result ─────────────────────────────────────────

def test_search_commentaries_collapses_sections_of_one_article(monkeypatch):
    """Three indexed sections of Art. 785 OR are one commentary, not three."""
    rows = [
        {"sr_number": "220", "abbr": "OR", "article_num": "785",
         "title": "Art. 785 OR", "authors": None, "language": "de",
         "html_link": "x", "snippet": f"s{i}"} for i in range(3)
    ] + [
        {"sr_number": "220", "abbr": "OR", "article_num": "808c",
         "title": "Art. 808c OR", "authors": None, "language": "de",
         "html_link": "y", "snippet": "t"} for _ in range(2)
    ]

    class _Cur:
        def fetchall(self):
            return rows

    class _Conn:
        def execute(self, *a, **k):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr(mcp_server, "_get_ok_conn", lambda *a, **k: _Conn())
    out = mcp_server.search_commentaries("Willensmängel", limit=5)
    arts = [r["article_num"] for r in out["results"]]
    assert arts == ["785", "808c"], arts
    assert out["count"] == 2
    # the best-ranked section wins (rows arrive rank-ordered)
    assert out["results"][0]["snippet"] == "s0"


# ── 3. the search -> get chain is usable ──────────────────────────────────

def test_scholarship_search_prints_the_pub_id_get_scholarship_needs():
    out = mcp_server._format_search_scholarship_response({
        "query": "Mietrecht", "count": 1,
        "results": [{"pub_id": "zhaw_digitalcollection:11475/15688",
                     "source": "zhaw_digitalcollection", "year": 2019,
                     "title": "Miete von Wohn- und Geschäftsräumen",
                     "authors": "Theus Simoni, Fabiana",
                     "license": "Licence according to publishing contract",
                     "url": "https://hdl.handle.net/11475/15688"}],
    })
    assert "zhaw_digitalcollection:11475/15688" in out, \
        "get_scholarship requires pub_id; search must surface it"
    assert "pub_id" in out


def test_scholarship_search_without_pub_id_still_renders():
    out = mcp_server._format_search_scholarship_response({
        "query": "x", "count": 1,
        "results": [{"source": "s", "year": 2020, "title": "T"}],
    })
    assert "T" in out and "pub_id" not in out
