from search_stack.query_planner import QueryIntent, SearchFilters, build_hybrid_search_request, detect_query_intent


def test_detect_query_intent_docket():
    assert detect_query_intent("1A.122/2005") == QueryIntent.DOCKET


def test_detect_query_intent_statute():
    assert detect_query_intent("Art. 8 EMRK") == QueryIntent.STATUTE


def test_detect_query_intent_boolean():
    assert detect_query_intent("regeste:Asyl AND regeste:Wegweisung") == QueryIntent.BOOLEAN


def test_build_hybrid_request_with_vector_and_filters():
    body = build_hybrid_search_request(
        query="Asyl und Wegweisung",
        filters=SearchFilters(court="bvger", canton="CH", language="de"),
        query_vector=[0.1, 0.2, 0.3],
        size=15,
    )
    assert body["size"] == 15
    assert "query" in body
    assert "hybrid" in body["query"]
    assert len(body["query"]["hybrid"]) == 2
    assert body.get("search_pipeline") == "swiss-caselaw-hybrid-rrf-v1"


def test_build_hybrid_request_uses_custom_pipeline_name():
    body = build_hybrid_search_request(
        query="Asyl und Wegweisung",
        query_vector=[0.1, 0.2, 0.3],
        search_pipeline_name="custom-hybrid-rrf-v2",
    )
    assert body.get("search_pipeline") == "custom-hybrid-rrf-v2"


def test_build_lexical_request_without_vector():
    body = build_hybrid_search_request(
        query="Art. 34 BV Finanzreferendum",
        filters=SearchFilters(language="de"),
        query_vector=None,
    )
    assert "hybrid" not in body["query"]
    assert "bool" in body["query"]
