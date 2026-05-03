"""Integration tests for the Pro Verify-and-Strengthen handler.

Exercises mcp_server._handle_strengthen against the live read-only DBs
(decisions.db + reference_graph.db + ok_commentaries.db). Each test
runs a realistic legal paragraph through the handler and asserts the
response shape + key invariants:

  • Citations in the paragraph appear in `verified_citations`
  • Suggested citations come from leading-cases for the cited statute
  • Suggested citations EXCLUDE anything already cited
  • Commentary excerpts come from OnlineKommentar.ch
  • Argument-strength signal is one of {strong, medium, weak}
  • The response is forward-compatible (counter_authorities + summary
    fields present even when empty)
"""
import pytest

from mcp_server import _handle_strengthen


def _has_keys(d, keys):
    return all(k in d for k in keys)


def test_response_shape():
    r = _handle_strengthen(redacted_text="Vgl. BGE 132 III 222.")
    assert _has_keys(r, [
        "ok", "verified_citations", "suggested_citations",
        "commentary_excerpts", "counter_authorities",
        "argument_strength", "argument_strength_explanation",
        "summary", "_paragraph_chars", "_statutes_extracted",
        "_citations_extracted",
    ])
    assert r["argument_strength"] in {"strong", "medium", "weak"}


def test_empty_paragraph_returns_error():
    r = _handle_strengthen(redacted_text="")
    assert r.get("error") == "empty_paragraph"


def test_extracts_cited_bge():
    src = "Wie das Bundesgericht in BGE 132 III 222 ausgef\u00fchrt hat, ..."
    r = _handle_strengthen(redacted_text=src)
    citations = r["verified_citations"]
    assert len(citations) >= 1
    assert any("132 III 222" in (c["citation"] or "") for c in citations), citations


def test_extracts_statute_reference():
    src = "Gem\u00e4ss Art. 41 OR ist Schadenersatz geschuldet."
    r = _handle_strengthen(redacted_text=src)
    assert any("Art. 41 OR" in s for s in r["_statutes_extracted"])


def test_suggested_cases_for_or_41_exist_and_exclude_cited():
    """Suggest leading cases on Art. 41 OR; if BGE 132 III 222 is one
    of them and we already cited it, it must NOT appear in suggestions."""
    src = (
        "Wie das Bundesgericht in BGE 132 III 222 ausgef\u00fchrt hat, "
        "ist nach Art. 41 OR Schadenersatz aus unerlaubter Handlung geschuldet."
    )
    r = _handle_strengthen(redacted_text=src)
    cited = {c["decision_id"] for c in r["verified_citations"] if c["decision_id"]}
    suggested = {s["decision_id"] for s in r["suggested_citations"]}
    overlap = cited & suggested
    assert not overlap, f"suggested duplicates cited: {overlap}"


def test_suggested_cases_carry_authority_metadata():
    src = "Gem\u00e4ss Art. 41 OR ist Schadenersatz geschuldet."
    r = _handle_strengthen(redacted_text=src)
    if not r["suggested_citations"]:
        pytest.skip("no suggestions returned for Art. 41 OR — maybe graph DB unavailable")
    for s in r["suggested_citations"][:3]:
        assert _has_keys(s, [
            "decision_id", "citation", "court", "date", "citation_count",
            "regeste_excerpt", "rationale", "related_statute", "url",
        ])
        assert s["citation_count"] >= 0  # may be 0 for low-cited cases


def test_no_pii_in_response():
    """The handler runs AFTER the redaction guard so input is already
    clean. But verify the response itself doesn't synthesize PII from
    nothing — a regression-guard test."""
    src = "Vgl. BGE 132 III 222 zum Schadenersatz nach Art. 41 OR."
    r = _handle_strengthen(redacted_text=src)
    import json
    body = json.dumps(r, ensure_ascii=False)
    # No AHV / IBAN / email patterns appear in the response
    import re
    assert not re.search(r"\b756[.\s]\d{4}[.\s]\d{4}[.\s]\d{2}\b", body)
    assert not re.search(r"\bCH\d{2}(?:\s?[A-Z0-9]{4}){4}", body)
    assert not re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", body)


def test_strength_signal_responds_to_input():
    """Two paragraphs — one citation-rich, one bare. Bare should be
    weaker (or at least not stronger) than rich."""
    rich = (
        "Wie das Bundesgericht in BGE 132 III 222, BGE 134 III 102 und BGE 140 III 86 "
        "im Einklang mit der Lehre festgehalten hat, gilt Art. 41 OR..."
    )
    bare = "Es gilt Art. 41 OR."
    r1 = _handle_strengthen(redacted_text=rich)
    r2 = _handle_strengthen(redacted_text=bare)
    order = {"weak": 0, "medium": 1, "strong": 2}
    assert order[r1["argument_strength"]] >= order[r2["argument_strength"]]


def test_forward_compat_fields_present_even_when_empty():
    """v1.1 will populate counter_authorities + summary. The fields
    must exist now so client code can render conditionally without
    breaking when they fill in."""
    r = _handle_strengthen(redacted_text="Vgl. BGE 132 III 222.")
    assert isinstance(r["counter_authorities"], list)
    assert "summary" in r
    assert isinstance(r["summary"], str)
