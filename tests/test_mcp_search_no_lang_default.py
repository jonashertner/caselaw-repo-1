"""Regression: SEARCH-style MCP tools must NOT default the `language`
filter to a specific language.

Background. 2026-05-08: a German conversation produced a "Latest BGer
rulings" list that silently excluded a French 2026-04-29 decision
(`5A_273/2026`) because the calling LLM auto-applied
`language="de"`. The user's instruction:

  > never restrict search results to a particular language unless
  > explicitly so instructed

Defence-in-depth:

  1. Tool inputSchema must NOT have `default: <lang>` on the
     `language` parameter. (The tool description should also tell
     the LLM not to auto-set it; that's a strong-suggestion layer
     not enforced by tests.)
  2. Tools whose name starts with `search_`, `find_`, `analyze_`,
     or `browse_` are SEARCH-style (return a result *list*) and
     are subject to the rule. `get_*` tools that fetch a single
     resource by ID may legitimately default to one language since
     they're pulling a specific representation, not filtering a list.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


@pytest.fixture(scope="module")
def tools():
    os.environ.setdefault("LEXFIND_ENABLED", "true")
    import mcp_server  # noqa: WPS433
    mcp_server.REMOTE_MODE = True
    return mcp_server._list_tools()


SEARCH_TOOL_PREFIXES = ("search_", "find_", "analyze_", "browse_")


def _is_search_tool(name: str) -> bool:
    return name.startswith(SEARCH_TOOL_PREFIXES)


def test_no_search_tool_has_language_default(tools):
    """Hard ban on `default: <lang>` for the language parameter on
    SEARCH-style tools. A default silently restricts result sets and
    produces incomplete top-N lists."""
    offenders: list[str] = []
    for t in tools:
        if not _is_search_tool(t.name):
            continue
        lang_schema = (t.inputSchema or {}).get("properties", {}).get("language")
        if lang_schema is None:
            continue
        if "default" in lang_schema:
            offenders.append(
                f"{t.name}: default={lang_schema['default']!r}"
            )
    assert not offenders, (
        "search-style tools must NOT default the `language` filter — "
        "doing so excludes the other languages of Switzerland's "
        "trilingual corpus from result lists. Offenders:\n  "
        + "\n  ".join(offenders)
    )


def test_search_tool_language_descriptions_warn_against_auto_apply(tools):
    """Tool descriptions on SEARCH tools that DO accept `language`
    must explicitly mark it OPTIONAL and warn the LLM against auto-
    applying it. Soft enforcement: the description must contain the
    word 'OPTIONAL' (case-insensitive) somewhere in the language
    parameter's description."""
    weak: list[str] = []
    for t in tools:
        if not _is_search_tool(t.name):
            continue
        lang_schema = (t.inputSchema or {}).get("properties", {}).get("language")
        if lang_schema is None:
            continue
        desc = (lang_schema.get("description") or "").lower()
        if "optional" not in desc:
            weak.append(f"{t.name}: {desc[:80]!r}")
    assert not weak, (
        "search-style tools' `language` parameter must mark itself "
        "OPTIONAL in its description — otherwise an LLM seeing a "
        "terse 'Filter by language: de, fr, it' will auto-apply it. "
        "Offenders:\n  " + "\n  ".join(weak)
    )
