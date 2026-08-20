"""An argument we do not recognise must not vanish in silence.

Unknown arguments were dropped without a word, so the tool answered a
question the caller had not asked and looked confident doing it. Measured
in a day of live capture: `search_decisions` received `legal_area` twelve
times and `courtLevel` once, `get_decision` received `include_full_text`,
`include_summary`, `highlight` and `format` — every one discarded, every
caller told nothing. A `search_decisions` call whose `legal_area` filter
evaporates comes back unfiltered and indistinguishable from a real answer.

Two responses, both cheap: alias the names that mean something we already
have, and name the rest in the reply so the model can correct itself on
the next call. Aliasing beats rejecting because a rejected call helps
nobody, and because `topic` on find_leading_cases follows our own
description ("for a topic or statute") against a parameter called `query`
— our wording invited it.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def test_declared_args_are_read_from_the_real_schemas():
    declared = m._declared_tool_args()
    assert "search_decisions" in declared
    assert {"query", "court", "limit"} <= declared["search_decisions"]
    assert "query" in declared["find_leading_cases"]


def test_topic_is_accepted_as_the_query_it_describes():
    args = {"topic": "Schuldneranweisung", "limit": 3}
    unknown = m._normalise_tool_args("find_leading_cases", args)
    assert args["query"] == "Schuldneranweisung"
    assert "topic" not in args
    assert unknown == [], "an aliased name is understood, not reported"


def test_an_alias_never_overwrites_an_explicit_value():
    args = {"topic": "ignored", "query": "explicit"}
    m._normalise_tool_args("find_leading_cases", args)
    assert args["query"] == "explicit"


def test_a_genuinely_unknown_argument_is_reported():
    """The live case: the filter silently evaporates."""
    args = {"query": "Mietzins", "legal_area": "Mietrecht"}
    assert m._normalise_tool_args("search_decisions", args) == ["legal_area"]


def test_known_arguments_are_never_reported():
    args = {"query": "x", "court": "bger", "limit": 5, "include_pinpoint": True}
    assert m._normalise_tool_args("search_decisions", args) == []


def test_the_warning_says_what_was_ignored_and_what_is_valid():
    from mcp.types import TextContent
    res = [TextContent(type="text", text="# Leading Cases (all, top 3)")]
    out = m._prepend_arg_warning(res, "find_leading_cases", ["legal_area"])
    text = out[0].text
    assert text.startswith("NOTE:")
    assert "legal_area" in text
    assert "did NOT filter" in text
    assert "query" in text, "the valid parameters have to be named"
    assert "# Leading Cases" in text, "the answer itself must survive"


def test_no_unknown_arguments_leaves_the_response_untouched():
    from mcp.types import TextContent
    res = [TextContent(type="text", text="body")]
    assert m._prepend_arg_warning(res, "search_decisions", [])[0].text == "body"


def test_an_unknown_tool_is_not_second_guessed():
    """Schemas we cannot see are not evidence that an argument is wrong."""
    assert m._normalise_tool_args("not_a_tool", {"whatever": 1}) == []


def test_normalisation_survives_odd_input():
    assert m._normalise_tool_args("search_decisions", None) == []
    assert m._prepend_arg_warning([], "search_decisions", ["x"]) == []
