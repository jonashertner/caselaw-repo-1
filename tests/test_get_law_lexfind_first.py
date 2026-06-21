"""Issue #27: get_law must serve cantonal laws LexFind-first (always current +
complete), with the local mirror as a resilience fallback only.

Regression guard: ZH 550.1 (PolG) / 554.5 (HuG) are absent from the incomplete
cantonal_laws.db mirror but present in LexFind; get_law previously returned a
false "No law found". These tests are offline (LexFind + mirror are mocked).
"""
import mcp_server


# ── get_law routing → LexFind-first legislation path ──────────────────────────

def test_get_law_cantonal_returns_lexfind_law(monkeypatch):
    """get_law maps a LexFind-served cantonal law and extracts the article."""
    leg = {
        "lexfind_id": 22871,
        "systematic_number": "554.5",
        "title": "Hundegesetz (HuG)",
        "current_version": {"category": "Tiere", "active_since": "2025-06-01"},
        "source": "lexfind",
        "articles": [
            {"article_num": "1", "heading": "Zweck", "text": "Dieses Gesetz ..."},
            {"article_num": "2", "heading": "Geltungsbereich", "text": "..."},
        ],
    }
    monkeypatch.setattr(mcp_server, "_get_legislation", lambda **kw: leg)

    res = mcp_server._get_law_cantonal(
        sr_number="554.5", abbreviation=None, article="1",
        language="de", canton="ZH",
    )
    assert "error" not in res
    assert res["sr_number"] == "554.5"
    assert res["canton"] == "ZH"
    assert res["lexfind_id"] == 22871
    assert res["version_active_since"] == "2025-06-01"
    assert [a["article_num"] for a in res["articles"]] == ["1"]
    assert res["articles"][0]["text"].startswith("Dieses Gesetz")


def test_get_law_cantonal_passes_canton_and_sr_to_legislation(monkeypatch):
    """The delegated call carries canton + systematic_number through."""
    seen = {}
    def fake(**kw):
        seen.update(kw)
        return {"systematic_number": "550.1", "title": "PolG",
                "articles": [{"article_num": "33", "heading": "Wegweisung", "text": "x"}]}
    monkeypatch.setattr(mcp_server, "_get_legislation", fake)

    res = mcp_server._get_law_cantonal(
        sr_number="550.1", abbreviation=None, article="33",
        language="de", canton="zh",
    )
    assert seen["systematic_number"] == "550.1"
    assert seen["canton"] == "ZH"
    assert res["articles"][0]["heading"] == "Wegweisung"


def test_get_law_cantonal_no_law_when_legislation_errors(monkeypatch):
    """A genuine LexFind miss surfaces as 'No law found' (not a crash)."""
    monkeypatch.setattr(mcp_server, "_get_legislation",
                        lambda **kw: {"error": "No legislation found"})
    res = mcp_server._get_law_cantonal(
        sr_number="999.99", abbreviation=None, article="1",
        language="de", canton="ZH",
    )
    assert "error" in res
    assert "No law found" in res["error"]


def test_get_law_cantonal_handles_legislation_exception(monkeypatch):
    """A malformed LexFind response (exception inside _get_legislation) must
    degrade to a graceful error, not an unhandled 500."""
    def boom(**kw):
        raise KeyError("families")
    monkeypatch.setattr(mcp_server, "_get_legislation", boom)
    res = mcp_server._get_law_cantonal(
        sr_number="554.5", abbreviation=None, article="1",
        language="de", canton="ZH",
    )
    assert "error" in res


def test_get_law_cantonal_digit_abbreviation_used_as_sr(monkeypatch):
    """A digit-form abbreviation is used directly as the SR/LS number — no
    mirror lookup needed (REST /api/laws/{n}?canton=ZH path)."""
    seen = {}
    def fake(**kw):
        seen.update(kw)
        return {"systematic_number": "131.1", "title": "GG", "articles": []}
    monkeypatch.setattr(mcp_server, "_get_legislation", fake)
    res = mcp_server._get_law_cantonal(
        sr_number=None, abbreviation="131.1", article=None,
        language="de", canton="ZH",
    )
    assert seen["systematic_number"] == "131.1"
    assert "error" not in res


# ── _get_legislation cantonal: LexFind-first + mirror resilience fallback ──────

def _mock_lexfind_success(monkeypatch, *, sr, canton, lexfind_id, title):
    """Wire _lexfind_request + _fetch_lexfind_law_text to emulate a live hit."""
    def fake_request(method, endpoint, language, json_body=None, timeout=None):
        if endpoint == "systematic-search":
            return {"id": "S1", "session_id": "SID"}
        if endpoint.startswith("systematic-search/"):
            return {"texts_of_law_with_latest_version": [
                {"id": lexfind_id, "entity": {"abbreviation": canton},
                 "systematic_number": sr}], "number_of_pages": 1}
        if endpoint.startswith("texts-of-law/"):
            return {"id": lexfind_id, "systematic_number": sr,
                    "entity": {"abbreviation": canton, "name": canton},
                    "is_active": True, "dta_urls": [],
                    "families": [[[{"id": 1, "title": title,
                                    "info_badge": "current",
                                    "version_active_since": "2025-06-01",
                                    "is_active": True,
                                    "category": {"name": "X"}}]]]}
        return None
    monkeypatch.setattr(mcp_server, "_lexfind_request", fake_request)
    monkeypatch.setattr(mcp_server, "_fetch_lexfind_law_text",
                        lambda lid, language="de": {
                            "articles": [{"article_num": "1", "heading": "h", "text": "live"}],
                            "full_text": "live", "text_source": "lexfind_pdf"})
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_set", lambda k, v: None)
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)


def test_get_legislation_cantonal_prefers_lexfind_over_mirror(monkeypatch):
    """LexFind-first: a present-but-stale mirror must NOT short-circuit the
    live LexFind result."""
    _mock_lexfind_success(monkeypatch, sr="554.5", canton="ZH",
                          lexfind_id=22871, title="Hundegesetz")
    # Mirror would return a STALE copy — it must be ignored when LexFind works.
    monkeypatch.setattr(mcp_server, "_get_cantonal_local",
                        lambda **kw: {"title": "STALE", "source": "cantonal_local",
                                      "articles": [{"article_num": "1", "heading": "old", "text": "STALE"}]})
    res = mcp_server._get_legislation(systematic_number="554.5", canton="ZH", language="de")
    assert res["source"] == "lexfind"
    assert res["title"] == "Hundegesetz"
    assert res["articles"][0]["text"] == "live"


def test_get_legislation_cantonal_falls_back_to_mirror_when_lexfind_down(monkeypatch):
    """Resilience: LexFind unreachable → serve the local mirror, not an error."""
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request",
                        lambda *a, **k: None)  # every LexFind call fails
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_get_cantonal_local",
                        lambda **kw: {"title": "PolG", "source": "cantonal_local",
                                      "articles": [{"article_num": "1", "heading": "h", "text": "mirror"}]})
    res = mcp_server._get_legislation(systematic_number="550.1", canton="ZH", language="de")
    assert res["source"] == "cantonal_local"
    assert res["title"] == "PolG"


def test_get_legislation_cantonal_error_when_lexfind_down_and_no_mirror(monkeypatch):
    """LexFind down AND mirror miss → a real error, not a crash."""
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request", lambda *a, **k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_get_cantonal_local", lambda **kw: None)
    res = mcp_server._get_legislation(systematic_number="550.1", canton="ZH", language="de")
    assert "error" in res


# ── search: cantonal LexFind-first (issue #27, Option A) ──────────────────────

def _mock_lexfind_search(monkeypatch, *, sr, canton, title, snippet):
    def fake_request(method, endpoint, language, json_body=None, timeout=None):
        if endpoint == "fulltext-search":
            return {"id": "S1", "session_id": "SID"}
        if endpoint.startswith("fulltext-search/"):
            return {"texts_of_law_with_matches": [
                {"entity": {"abbreviation": canton, "name": canton},
                 "systematic_number": sr, "id": 23646, "is_active": True,
                 "dta_urls": [],
                 "matches": [{"title": title, "snippet": snippet,
                              "category": {"name": "x"}, "is_active": True}]}],
                "results": [{"number_of_results": 1}]}
        return None
    monkeypatch.setattr(mcp_server, "_lexfind_request", fake_request)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_set", lambda k, v: None)
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)


def test_search_legislation_cantonal_lexfind_first(monkeypatch):
    """search_legislation finds a law present in LexFind but absent from the
    incomplete local mirror (ZH PolG / 'Wegweisung')."""
    _mock_lexfind_search(monkeypatch, sr="550.1", canton="ZH",
                         title="Polizeigesetz (PolG)", snippet="...Wegweisung...")
    res = mcp_server._search_legislation(query="Wegweisung", canton="ZH", language="de")
    assert res["laws"], "expected a LexFind hit"
    assert res["laws"][0]["systematic_number"] == "550.1"


def test_search_legislation_cantonal_falls_back_to_mirror(monkeypatch):
    """LexFind unreachable → serve local mirror search, not an error/empty."""
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request", lambda *a, **k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_search_cantonal_local",
                        lambda **kw: {"laws": [{"systematic_number": "550.1",
                                                "title": "PolG", "entity": "ZH"}]})
    res = mcp_server._search_legislation(query="Wegweisung", canton="ZH", language="de")
    assert res["laws"][0]["systematic_number"] == "550.1"


def test_search_laws_cantonal_routes_to_lexfind(monkeypatch):
    """search_laws cantonal delegates to the LexFind path and reformats to the
    article-level shape, using the RAW query (not the FTS5-expanded one)."""
    seen = {}
    def fake_leg(**kw):
        seen.update(kw)
        return {"laws": [{"systematic_number": "550.1", "title": "PolG",
                          "entity": "ZH", "snippet": "...Wegweisung...",
                          "lexfind_id": 23646}]}
    monkeypatch.setattr(mcp_server, "_search_legislation", fake_leg)
    out = mcp_server._search_laws_cantonal(
        "(wegweisung OR ...) AND x", "ZH", "de", 10, raw_query="Wegweisung")
    assert seen["query"] == "Wegweisung"      # raw query, not FTS5-expanded
    assert seen["canton"] == "ZH"
    assert out[0]["sr_number"] == "550.1"
    assert out[0]["level"] == "cantonal"
    assert out[0]["title"] == "PolG"
