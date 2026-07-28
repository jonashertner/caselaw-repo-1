"""Tool-description budget + search-family de-overlap (BGPartner 2026-07).

Microsoft 365 Copilot silently ignores tool-description text beyond 1,024
characters — attest_response (1,711) lost 40% of its description including
the entire calling protocol, and search_decisions (1,074) lost its tail.
Microsoft also names near-identical descriptions as THE cause of wrong-tool
selection ("Names matter more than anything"); five of nine search tools
opened with the identical stem "Full-text search across".
"""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

SEARCH_FAMILY = [
    "search_decisions", "search_laws", "search_legislation",
    "search_scholarship", "search_commentaries", "search_botschaft",
    "search_materialien", "search_practice",
]


def test_every_description_at_most_1024_chars():
    over = [(t.name, len(t.description or ""))
            for t in m._list_tools() if len(t.description or "") > 1024]
    assert not over, f"silently truncated in M365 Copilot: {over}"


def test_search_family_first_sentences_pairwise_distinct():
    firsts = {t.name: (t.description or "")[:60]
              for t in m._list_tools() if t.name in SEARCH_FAMILY}
    dupes = [(a, b) for a, b in itertools.combinations(firsts, 2)
             if firsts[a] == firsts[b]]
    assert not dupes, dupes


def test_search_family_states_negative_scope():
    """Each of the three most-confused tools names at least one tool it is
    NOT for (Microsoft's worked-example pattern)."""
    tools = {t.name: t.description or "" for t in m._list_tools()}
    assert "search_laws" in tools["search_decisions"]
    assert "search_legislation" in tools["search_laws"]
    assert "search_laws" in tools["search_legislation"]
    assert "search_scholarship" in tools["search_commentaries"]


def test_attest_response_keeps_calling_protocol_in_budget():
    [t] = [t for t in m._list_tools() if t.name == "attest_response"]
    d = t.description or ""
    assert len(d) <= 1024
    # the calling protocol must survive within the un-truncated budget
    assert "CALL THIS BEFORE" in d
    assert "audit_grounding=true" in d
    assert "linked_text" in d


def test_practice_description_constraints_still_hold():
    """The discoverability test's asserted substrings must survive rewrites
    (BSV/SECO/FINMA/cantonal disclosure + corpus counts)."""
    [t] = [t for t in m._list_tools() if t.name == "search_practice"]
    d = t.description or ""
    for req in ("BSV", "SECO", "FINMA", "cantonal", "790", "ch_vb"):
        assert req in d, req
