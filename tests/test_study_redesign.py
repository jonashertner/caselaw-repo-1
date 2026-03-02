"""Tests for the redesigned study tools: get_case_brief, get_doctrine, generate_exam_question."""
import sys
import pytest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Skip all tests if the production DB is not present (integration tests require live DB)
_DB_PATH = Path(__file__).resolve().parent.parent / "output" / "decisions.db"
pytestmark = pytest.mark.skipif(
    not _DB_PATH.exists(),
    reason="Production DB not available — integration tests require live decisions.db",
)


# ── get_case_brief ────────────────────────────────────────────────────────────

def test_get_case_brief_returns_required_keys():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result, f"Unexpected error: {result.get('error')}"
    for key in ("decision_id", "regeste", "authority", "statutes"):
        assert key in result, f"Missing key: {key}"


def test_get_case_brief_authority_has_incoming():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert "incoming_citations" in result["authority"]
    assert isinstance(result["authority"]["incoming_citations"], int)


def test_get_case_brief_statutes_list():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert isinstance(result["statutes"], list)


def test_get_case_brief_unknown_case_returns_error():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 999 IX 999")
    assert "error" in result


def test_get_case_brief_related_structure():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert "related" in result
    assert "cited_by" in result["related"]
    assert "cites" in result["related"]


# ── get_doctrine ──────────────────────────────────────────────────────────────

def test_get_doctrine_statute_path_returns_leading_cases():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result, f"Unexpected error: {result.get('error')}"
    assert "leading_cases" in result
    assert len(result["leading_cases"]) > 0


def test_get_doctrine_statute_path_returns_statute_text():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    # statute field present (may be empty if statutes.db unavailable)
    assert "statute" in result


def test_get_doctrine_concept_path_works():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Tierhalterhaftung")
    assert "error" not in result
    assert "leading_cases" in result
    assert len(result["leading_cases"]) > 0


def test_get_doctrine_has_doctrine_timeline():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    assert "doctrine_timeline" in result
    assert isinstance(result["doctrine_timeline"], list)


def test_get_doctrine_leading_cases_have_required_fields():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    for case in result["leading_cases"]:
        assert "decision_id" in case
        assert "incoming_citations" in case
        assert "rule_summary" in case
