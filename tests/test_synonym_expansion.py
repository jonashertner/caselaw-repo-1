"""Tests for colloquial→legal synonym expansion entries."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server import LEGAL_QUERY_EXPANSIONS, _get_query_expansions


def test_hundebiss_expands_to_tierhalterhaftung():
    exps = _get_query_expansions("hundebiss")
    assert any("tierhalterhaft" in e.lower() for e in exps), (
        f"Expected Tierhalterhaftung in expansions, got: {exps}"
    )


def test_autounfall_expands_to_haftpflicht():
    exps = _get_query_expansions("autounfall")
    assert any("haftpflicht" in e.lower() or "kausalzusammenhang" in e.lower() for e in exps), (
        f"Expected Haftpflicht or Kausalzusammenhang, got: {exps}"
    )


def test_erbschaft_expands_to_erbrecht():
    exps = _get_query_expansions("erbschaft")
    assert any("erbrecht" in e.lower() or "pflichtteil" in e.lower() for e in exps), (
        f"Expected Erbrecht or Pflichtteil, got: {exps}"
    )


def test_geschaeftsfuehrer_expands_to_organverantwortlichkeit():
    exps = _get_query_expansions("geschaeftsfuehrer")
    assert any("organverantwortlich" in e.lower() or "sorgfaltspflicht" in e.lower() for e in exps), (
        f"Expected Organverantwortlichkeit or Sorgfaltspflicht, got: {exps}"
    )


def test_mietrecht_kuendigung_expands():
    exps = _get_query_expansions("mietrecht")
    all_exps = " ".join(exps).lower()
    assert "mietzins" in all_exps, f"Expected Mietzins in mietrecht expansions, got: {exps}"
    assert "kuendigung" in all_exps or "kundigung" in all_exps, (
        f"Expected Kündigung in mietrecht expansions, got: {exps}"
    )


def test_no_existing_entries_removed():
    """Verify pre-existing entries still work."""
    asyl_exps = _get_query_expansions("asyl")
    assert "asile" in asyl_exps or "schutz" in asyl_exps, f"asyl expansions broken: {asyl_exps}"

    haftung_exps = _get_query_expansions("haftung")
    assert "responsabilite" in haftung_exps or "responsabilita" in haftung_exps, (
        f"haftung expansions broken: {haftung_exps}"
    )


def test_expansion_prompt_contains_fewshot_example():
    """Verify the LLM prompt includes at least one colloquial→legal example."""
    from mcp_server import EXPANSION_SYSTEM_PROMPT
    assert "Hundebiss" in EXPANSION_SYSTEM_PROMPT or "hundebiss" in EXPANSION_SYSTEM_PROMPT.lower(), (
        "Prompt should contain a colloquial→legal few-shot example (e.g., Hundebiss)"
    )
