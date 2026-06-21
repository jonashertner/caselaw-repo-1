"""Anti-hallucination contract (R2 / R4) must route legislative-intent and
verbatim quotation to the VERBATIM Botschaft tools — `get_article_purpose` /
`search_botschaft` (whose `text` field is verbatim Botschaft paragraphs with
BBl + eli_uri provenance) — NOT to `get_materialien`, whose digest fields
(legislative_intent / key_arguments / design_choices / rejected_alternatives)
are LLM PARAPHRASE for two laws only and are not quotable.

Regression guard for the 2026-06-21 materialien-interpretation assessment: R2
listed `get_materialien` in the verbatim-quote whitelist and the contract never
named `get_article_purpose` at all — inviting a disciplined model to quote
paraphrase as if it were the legislator's own words on a BBl page.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server  # noqa: E402


def _prompt() -> str:
    return mcp_server.server.instructions


def test_r2_authorizes_verbatim_botschaft_tool():
    p = _prompt()
    # The verbatim-quote whitelist (R2) must include the verbatim Botschaft tool.
    assert "get_article_purpose" in p, (
        "R2 must authorize get_article_purpose (verbatim Botschaft) for quotes"
    )
    # ...and must NOT present get_materialien as a verbatim-quote source.
    assert "`get_commentary`, or `get_materialien`" not in p, (
        "R2 still lists get_materialien in the verbatim-quote whitelist"
    )


def test_get_materialien_flagged_as_paraphrase():
    p = _prompt()
    assert "paraphrased digest" in p.lower(), (
        "contract must flag get_materialien digests as paraphrase (not verbatim)"
    )


def test_r4_routes_legislative_intent_to_verbatim_tool():
    p = _prompt()
    i = p.find("R4.")
    assert i != -1, "R4 not found in prompt"
    r4 = p[i:i + 450]
    assert "get_article_purpose" in r4, (
        "R4 (legislative intent / teleology) must route to get_article_purpose"
    )


def test_teleology_routing_points_to_verbatim_tool():
    p = _prompt()
    i = p.find("(teleology)")
    assert i != -1, "teleology routing line not found"
    line = p[i:i + 100]
    assert "get_article_purpose" in line, (
        "the teleology routing line must point to get_article_purpose"
    )


def test_search_botschaft_surfaced_in_contract():
    # The topical verbatim entry point should be discoverable in the contract.
    assert "search_botschaft" in _prompt(), (
        "contract should name search_botschaft as the topical verbatim entry point"
    )
