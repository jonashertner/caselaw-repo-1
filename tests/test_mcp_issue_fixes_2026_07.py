"""Regression tests for the 2026-07 MCP tool-correctness batch (GitHub #47/#49/#50/#51).

Offline: exercises importable helpers/formatters only, no live DB or network.
"""
import ast

import mcp_server


# ---- #47: get_erwaegung / cite pinpoint prefix normalisation --------------
def test_strip_erw_prefix_handles_de_fr_it_and_bare_numbers():
    f = mcp_server._strip_erw_prefix
    # bare pinpoints pass through untouched
    assert f("2") == "2"
    assert f("2.3") == "2.3"
    # the old lstrip("E.") bug mangled these; now they resolve to the number
    assert f("E. 2") == "2"
    assert f("E.2") == "2"
    assert f("Erw. 2") == "2"        # was "rw. 2" under lstrip("E.")
    assert f("Erwägung 2") == "2"
    assert f("consid. 2") == "2"     # FR, previously unrecognised
    assert f("considérant 2") == "2"
    assert f("cons. 2") == "2"
    assert f("considerando 2") == "2"  # IT
    assert f(None) == ""
    assert f("") == ""


def test_strip_erw_prefix_does_not_eat_a_leading_digit_section():
    # "4.1" must not lose its leading 4 (no marker present)
    assert mcp_server._strip_erw_prefix("4.1") == "4.1"


# ---- #51: LAW_SEARCH_EXPANSIONS has no silently-dropped duplicate keys -----
def test_law_search_expansions_no_duplicate_keys():
    # Duplicate literal keys collapse in the live dict, so re-parse the source
    # AST (which preserves every key) to prove there are none.
    tree = ast.parse(open(mcp_server.__file__, encoding="utf-8").read())
    dict_node = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                and node.target.id == "LAW_SEARCH_EXPANSIONS"
                and isinstance(node.value, ast.Dict)):
            dict_node = node.value
            break
    assert dict_node is not None, "LAW_SEARCH_EXPANSIONS dict literal not found"
    keys = [k.value for k in dict_node.keys if isinstance(k, ast.Constant)]
    assert len(keys) == len(set(keys)), \
        "duplicate keys: " + ", ".join(sorted({k for k in keys if keys.count(k) > 1}))


def test_law_search_expansions_merged_terms_preserved():
    exp = mcp_server.LAW_SEARCH_EXPANSIONS
    # the previously-dropped topical synonyms must survive the merge
    assert "beistandschaft" in exp["kindesschutz"]      # was dropped by the dup
    assert "protection enfant" in exp["kindesschutz"]   # cross-lang side kept too
    assert "tierschutz" in exp["hund"] and "detentore" in exp["hund"]


# ---- #49: find_citations truncation / pagination surfaced in the response --
def test_citations_formatter_shows_total_and_next_offset_when_truncated():
    result = {
        "decision_id": "bge_133_III_393",
        "direction": "incoming",
        "limit": 200, "offset": 0, "next_offset": 200,
        "incoming": [],
        "incoming_total": 947,
        "incoming_returned": 200,
        "incoming_has_more": True,
    }
    text = mcp_server._format_citations_response(result)
    assert "947" in text                 # true total, not just the returned page
    assert "truncated" in text
    assert "next_offset=200" in text


def test_citations_formatter_complete_page_not_marked_truncated():
    result = {
        "decision_id": "bge_140_III_86",
        "direction": "incoming",
        "limit": 200, "offset": 0,
        "incoming": [],
        "incoming_total": 163,
        "incoming_returned": 163,
        "incoming_has_more": False,
    }
    text = mcp_server._format_citations_response(result)
    assert "163" in text
    assert "truncated" not in text


# ---- #42: pure quoted phrase stays an exact phrase MATCH (no OR-alternation) -
def test_is_pure_phrase_query():
    f = mcp_server._is_pure_phrase_query
    assert f('"Treu und Glauben"')
    assert f('  "ne bis in idem"  ')
    assert not f('Treu und Glauben')            # unquoted -> normal NL path
    assert not f('gamma NOT "alpha beta"')      # boolean -> must NOT restrict (keeps NOT working)
    assert not f('"alpha" "beta"')              # two phrases -> not a single pure phrase
    assert not f('')


def test_pure_phrase_strategy_drops_or_alternation():
    # A pure phrase must produce ONLY the phrase MATCH strategy — no nl_or/
    # expansion that would match a single token of the phrase (issue #42).
    strategies, llm_terms = mcp_server._build_query_strategies('"Treu und Glauben"')
    names = {s["name"] for s in strategies}
    assert names == {"raw"}, names
    assert strategies[0]["query"] == '"Treu und Glauben"'
    assert llm_terms == []
    # a boolean query with a quoted phrase keeps the full strategy set (NOT works)
    bool_strats, _ = mcp_server._build_query_strategies('gamma NOT "alpha beta"')
    assert len(bool_strats) > 1


def test_analyze_query_skips_haiku_parse_for_pure_phrase():
    # For a pure phrase the structured (Haiku) parse is skipped, so no doctrine/
    # synonym strategies get injected downstream to broaden past the phrase (#42).
    strategies, llm_terms, parsed = mcp_server._analyze_query('"Treu und Glauben"', False)
    assert parsed == {}
    assert [s["name"] for s in strategies] == ["raw"]
    assert llm_terms == []


def test_quoted_statute_ref_preserves_article_search():
    # Both a quoted doctrine phrase and a quoted statute ref drop the nl_or noise
    # (pure-phrase strategy short-circuit). But a quoted statute ref must STILL
    # extract a statute so statute-graph article search runs (the advertised
    # `"Art. 8 BV"` syntax reaches ~2x more decisions than a literal phrase). A
    # doctrine phrase extracts none, so it stays an exact literal match (#42).
    assert mcp_server._is_pure_phrase_query('"Art. 8 BV"')
    assert mcp_server._is_pure_phrase_query('"Treu und Glauben"')
    assert mcp_server._extract_query_statute_refs('"Art. 8 BV"')          # -> statute-graph
    assert not mcp_server._extract_query_statute_refs('"Treu und Glauben"')  # -> exact only


# ---- #48: BGE pinpoint page reference parsing ------------------------------
def test_parse_bge_ref_text_shapes():
    f = mcp_server._parse_bge_ref_text
    assert f("BGE 129 I 236") == ("129", "I", 236)
    assert f("ATF 129 I 236") == ("129", "I", 236)
    assert f("BGE 116 Ia 30") == ("116", "IA", 30)     # division upper-cased to storage form
    assert f("BGE 129 I 236, consid. 4") == ("129", "I", 236)  # pinpoint suffix stripped
    assert f("bger_6B_1_2025") is None                 # docket, not BGE
    assert f("4A_1/2020") is None
    assert f("") is None
