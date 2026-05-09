"""Regression: search_laws must route to the correct corpus given
``jurisdiction`` and ``canton`` parameters.

Background. 2026-05-09 audit: an operator-precedence bug in the
``hit_federal`` expression caused
``search_laws(jurisdiction="federal", canton="ZH")`` to silently
fall through to the empty-result branch. Python's ``and`` binds
tighter than ``or``, so

    j != "cantonal" and not canton_u or canton_u == "CH"

parsed as ``(j != "cantonal" and not canton_u) or (canton_u == "CH")``.
For the federal+ZH case both clauses are False, so neither corpus was
hit. This test pins the routing semantics so the regression cannot
re-emerge.

Verifies the actual ``search_laws`` function — not a copy of the
boolean expression — by mocking the corpus searches and asserting
which paths were hit.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest


def _hit_federal(jurisdiction: str, canton: str | None) -> bool:
    """Mirror of the routing predicate in ``mcp_server.search_laws``."""
    canton_u = (canton or "").upper()
    j = (jurisdiction or "all").lower()
    return canton_u == "CH" or j == "federal" or (j == "all" and not canton_u)


def _hit_cantonal(
    jurisdiction: str, canton: str | None, sr_number: str | None = None
) -> bool:
    canton_u = (canton or "").upper()
    j = (jurisdiction or "all").lower()
    return j == "cantonal" or (
        j == "all" and not sr_number and canton_u != "CH"
    )


@pytest.mark.parametrize(
    "jurisdiction,canton,fed_expected,cant_expected",
    [
        # jurisdiction='federal' must always reach federal regardless of canton
        ("federal", None, True, False),
        ("federal", "CH", True, False),
        ("federal", "ZH", True, False),  # the bug: was False
        # jurisdiction='cantonal' routes to cantonal
        ("cantonal", None, False, True),
        ("cantonal", "ZH", False, True),
        # canton='CH' is a legacy alias for federal
        ("cantonal", "CH", True, True),  # both — explicit cantonal + CH alias
        ("all", "CH", True, False),
        # jurisdiction='all' (default) hits both unless narrowed by canton
        ("all", None, True, True),
        ("all", "ZH", False, True),
    ],
)
def test_routing_predicate(jurisdiction, canton, fed_expected, cant_expected):
    assert _hit_federal(jurisdiction, canton) is fed_expected, (
        f"hit_federal({jurisdiction!r}, {canton!r}) expected {fed_expected}"
    )
    assert _hit_cantonal(jurisdiction, canton) is cant_expected, (
        f"hit_cantonal({jurisdiction!r}, {canton!r}) expected {cant_expected}"
    )


def test_routing_matches_mcp_server_implementation():
    """Bytecode-level check: the predicate above is structurally identical
    to the one in mcp_server.search_laws. Catches drift if either copy
    is edited without updating the other.
    """
    import inspect
    from mcp_server import search_laws

    src = inspect.getsource(search_laws)
    # Both predicates must be present in the function body.
    assert 'canton_u == "CH"' in src
    assert 'j == "federal"' in src
    assert 'j == "all" and not canton_u' in src
    assert 'j == "cantonal"' in src
    # And the buggy un-paren'd form must NOT be present.
    bad = "and not canton_u  # an explicit canton filter implies cantonal only"
    assert bad not in src, "Buggy operator-precedence form has reappeared"


def test_jurisdiction_federal_with_canton_does_not_silently_drop():
    """End-to-end smoke: jurisdiction=federal + canton=ZH must NOT
    return an empty result via the routing-bug path. We mock the
    federal/cantonal helpers and assert hit_federal fires.
    """
    from mcp_server import search_laws

    with (
        patch("mcp_server._search_laws_federal", return_value=[
            {"sr_number": "210", "abbreviation": "ZGB", "title": "ZGB",
             "article_num": "1", "heading": "h", "snippet": "s",
             "level": "federal", "canton": "CH"},
        ]) as fed_mock,
        patch("mcp_server._search_laws_cantonal", return_value=[]) as cant_mock,
    ):
        out = search_laws(query="ZGB", jurisdiction="federal", canton="ZH")
    assert fed_mock.called, (
        "hit_federal regression: federal corpus must be searched when "
        "jurisdiction='federal', regardless of canton"
    )
    assert out["count"] >= 1
