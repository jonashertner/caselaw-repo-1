"""get_legislation: argument validation when no identifier is supplied.

Production journals (2026-09-01/02) showed ~3x/day:

    tool_call: get_legislation {"canton": "FR", "language": "fr"}
    LexFind API 404: .../fr/texts-of-law/0/with-version-groups

The journal hides lexfind_id/systematic_number (not structural log keys), so
the line looks like "neither identifier given". A truly absent pair already hit
the Path B guard without a network call; the 0 in the URL is a client
placeholder (lexfind_id=0 or "0") that passed the `is None` check and went
straight to Path A. Both shapes must now return one clear validation error
naming the two accepted identifiers, before any local or network lookup.

These tests are offline: every LexFind and mirror entry point is replaced by a
tripwire that fails the test if reached.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _arm_tripwires(monkeypatch):
    """Every lookup path raises: a validation error must precede all of them."""
    def trip(*a, **k):
        raise AssertionError(f"lookup reached with args={a} kwargs={k}")
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request", trip)
    monkeypatch.setattr(mcp_server, "_fetch_lexfind_law_text", trip)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", trip)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_set", trip)
    monkeypatch.setattr(mcp_server, "_get_legislation_local", trip)
    monkeypatch.setattr(mcp_server, "_get_cantonal_local", trip)


def _assert_validation_error(res):
    assert isinstance(res, dict)
    err = res.get("error", "")
    assert err, res
    assert "lexfind_id" in err and "systematic_number" in err, err
    assert "search_legislation" in err, err
    assert "LexFind" not in err.split("`lexfind_id`")[0], err  # not a fetch failure


# ── both identifiers absent ────────────────────────────────────────────────────

def test_no_identifier_at_all_is_a_validation_error(monkeypatch):
    """The literal production shape: canton + language only."""
    _arm_tripwires(monkeypatch)
    res = mcp_server._get_legislation(canton="FR", language="fr")
    _assert_validation_error(res)


@pytest.mark.parametrize("lexfind_id", [0, "0", "", "  ", "abc", -5, False, None])
def test_placeholder_lexfind_id_counts_as_missing(monkeypatch, lexfind_id):
    """0 / "0" / "" used to reach LexFind as texts-of-law/0 (or //)."""
    _arm_tripwires(monkeypatch)
    res = mcp_server._get_legislation(
        lexfind_id=lexfind_id, canton="FR", language="fr")
    _assert_validation_error(res)


@pytest.mark.parametrize("systematic_number", [None, "", "   "])
def test_blank_systematic_number_counts_as_missing(monkeypatch, systematic_number):
    """A whitespace-only SR used to POST a systematic-search for '   '."""
    _arm_tripwires(monkeypatch)
    res = mcp_server._get_legislation(
        lexfind_id=0, systematic_number=systematic_number,
        canton="ZH", language="de")
    _assert_validation_error(res)


def test_validation_precedes_the_lexfind_disabled_message(monkeypatch):
    """Missing identifiers are the caller's problem whatever the backend state."""
    _arm_tripwires(monkeypatch)
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", False)
    res = mcp_server._get_legislation(canton="CH", language="de")
    _assert_validation_error(res)


def test_dispatch_returns_the_validation_text_without_a_network_call(monkeypatch):
    """End to end through the tool dispatcher, exactly as the journal saw it."""
    _arm_tripwires(monkeypatch)
    out = asyncio.run(mcp_server._handle_call_tool_inner(
        "get_legislation", {"canton": "FR", "language": "fr"}))
    assert isinstance(out, list) and len(out) == 1
    text = out[0].text
    assert "lexfind_id" in text and "systematic_number" in text
    assert "Failed to fetch legislation" not in text


# ── a usable identifier still goes through ────────────────────────────────────

def test_placeholder_id_with_real_systematic_number_resolves_via_path_b(monkeypatch):
    """lexfind_id=0 alongside a real SR must resolve the SR, not fetch law 0."""
    seen = []
    def fake_request(method, endpoint, language, json_body=None, timeout=None):
        seen.append(endpoint)
        if endpoint == "systematic-search":
            return {"id": "S1", "session_id": "SID"}
        if endpoint.startswith("systematic-search/"):
            return {"texts_of_law_with_latest_version": [
                {"id": 22871, "entity": {"abbreviation": "ZH"},
                 "systematic_number": "554.5"}], "number_of_pages": 1}
        if endpoint.startswith("texts-of-law/"):
            return {"id": 22871, "systematic_number": "554.5",
                    "entity": {"abbreviation": "ZH", "name": "Zürich"},
                    "is_active": True, "dta_urls": [],
                    "families": [[[{"id": 1, "title": "Hundegesetz",
                                    "info_badge": "current",
                                    "version_active_since": "2025-06-01",
                                    "is_active": True,
                                    "category": {"name": "Tiere"}}]]]}
        return None
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request", fake_request)
    monkeypatch.setattr(mcp_server, "_fetch_lexfind_law_text",
                        lambda lid, language="de": {"articles": [], "full_text": "",
                                                    "text_source": "lexfind_pdf"})
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_set", lambda k, v: None)
    monkeypatch.setattr(mcp_server, "_get_cantonal_local", lambda **kw: None)

    res = mcp_server._get_legislation(
        lexfind_id=0, systematic_number=" 554.5 ", canton="ZH", language="de")
    assert res.get("lexfind_id") == 22871, res
    assert res.get("title") == "Hundegesetz"
    assert "texts-of-law/0/with-version-groups" not in seen
    assert "texts-of-law/22871/with-version-groups" in seen


def test_numeric_string_lexfind_id_is_accepted_as_an_integer(monkeypatch):
    """A client that sends "22871" gets the same law as one that sends 22871."""
    seen = []
    def fake_request(method, endpoint, language, json_body=None, timeout=None):
        seen.append(endpoint)
        return {"id": 22871, "systematic_number": "554.5",
                "entity": {"abbreviation": "ZH", "name": "Zürich"},
                "is_active": True, "dta_urls": [], "families": []}
    monkeypatch.setattr(mcp_server, "LEXFIND_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_lexfind_request", fake_request)
    monkeypatch.setattr(mcp_server, "_fetch_lexfind_law_text",
                        lambda lid, language="de": None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(mcp_server, "_lexfind_cache_set", lambda k, v: None)
    monkeypatch.setattr(mcp_server, "_get_cantonal_local", lambda **kw: None)

    res = mcp_server._get_legislation(lexfind_id="22871", language="de")
    assert seen == ["texts-of-law/22871/with-version-groups"]
    assert res.get("lexfind_id") == 22871


def test_normalize_lexfind_id_contract():
    n = mcp_server._normalize_lexfind_id
    assert n(22871) == 22871
    assert n("22871") == 22871
    assert n(" 7 ") == 7
    for bad in (None, 0, "0", "", "  ", "abc", "1.5", -1, True, False, 3.7, [1]):
        assert n(bad) is None, bad


def test_tool_schema_says_one_identifier_is_required():
    tool = next(t for t in mcp_server._list_tools() if t.name == "get_legislation")
    props = tool.inputSchema["properties"]
    assert "0" in props["lexfind_id"]["description"]
    assert "required" in props["systematic_number"]["description"].lower()
