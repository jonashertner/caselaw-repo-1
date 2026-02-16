import mcp_server


def _decision(
    decision_id: str,
    *,
    court: str = "bger",
    docket: str = "1C_500/2023",
    date: str = "2025-01-01",
    lang: str = "de",
    title: str = "Plan d'affectation et permis de construire",
    regeste: str = "Parc éolien; permis de construire",
    score: float = 7.0,
):
    return {
        "decision_id": decision_id,
        "court": court,
        "decision_date": date,
        "docket_number": docket,
        "language": lang,
        "title": title,
        "regeste": regeste,
        "snippet": regeste,
        "source_url": "https://example.invalid/decision",
        "relevance_score": score,
    }


def test_collect_statute_requests_merges_explicit_and_extracted():
    refs = mcp_server._collect_statute_requests(
        query_text="Art. 8 EMRK und Art. 8 EMRK",
        explicit_statutes=[
            {"law_code": "AsylG", "article": "3"},
            {"law_code": "ASYLG", "article": "3"},
        ],
    )
    labels = {r["ref"] for r in refs}
    assert "Art. 3 ASYLG" in labels
    assert "Art. 8 EMRK" in labels
    assert len(labels) == len(refs)


def test_draft_mock_decision_includes_cases_and_statutes(monkeypatch):
    def _fake_search(query: str, **_kwargs):
        q = (query or "").lower()
        if "art. 3 asylg" in q:
            return [_decision("d_statute", docket="E-7414/2015", court="bvger", score=8.2)]
        return [
            _decision("d_main", docket="D-7801/2024", court="bvger", score=8.5),
            _decision("d_alt", docket="2C_186/2025", court="bger", score=6.8),
        ]

    monkeypatch.setattr(mcp_server, "search_fts5", _fake_search)
    monkeypatch.setattr(
        mcp_server,
        "_search_graph_decisions_for_statutes",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(mcp_server, "_load_fedlex_cache", lambda: {})
    monkeypatch.setattr(mcp_server, "_save_fedlex_cache", lambda cache: None)
    monkeypatch.setattr(
        mcp_server,
        "_fetch_fedlex_article_text",
        lambda **_kwargs: {
            "fedlex_url": "https://www.fedlex.admin.ch/eli/cc/1999/358/de",
            "text_excerpt": "Art. 3 AsylG: Flüchtling ist, wer ...",
        },
    )

    report = mcp_server.draft_mock_decision(
        facts="Die betroffene Person ersucht um Asyl. Wegweisung wurde verfügt.",
        question="Ist Art. 3 AsylG erfüllt?",
        statute_references=[{"law_code": "ASYLG", "article": "3"}],
        preferred_language="de",
        limit=5,
    )

    assert report["facts_summary"]
    assert report["relevant_case_law"]
    assert report["relevant_case_law"][0]["decision_id"] in {"d_main", "d_statute"}
    assert report["applicable_statutes"]
    assert report["applicable_statutes"][0]["status"] in {"fetched", "cache_hit"}
    assert report["applicable_statutes"][0]["text_excerpt"]
    assert report["mock_decision"]["conclusion_ready"] is False
    assert report["clarifying_questions"]


def test_draft_mock_decision_reaches_conclusion_after_clarifications(monkeypatch):
    monkeypatch.setattr(mcp_server, "search_fts5", lambda **_kwargs: [_decision("d_main")])
    monkeypatch.setattr(mcp_server, "_search_graph_decisions_for_statutes", lambda **_kwargs: [])
    monkeypatch.setattr(mcp_server, "_resolve_statute_materials", lambda **_kwargs: [])

    first = mcp_server.draft_mock_decision(
        facts="Asylfall mit Wegweisung.",
        question="Ist die Wegweisung zulässig?",
        preferred_language="de",
    )
    required = first["clarification_gate"]["required_high_priority"]
    clarifications = [{"id": qid, "answer": "provided"} for qid in required]

    second = mcp_server.draft_mock_decision(
        facts="Asylfall mit Wegweisung.",
        question="Ist die Wegweisung zulässig?",
        preferred_language="de",
        clarifications=clarifications,
    )
    assert second["mock_decision"]["conclusion_ready"] is True
    assert second["clarification_gate"]["status"] == "ready_for_conclusion"
    assert second["mock_decision"]["outcome_note"]


def test_format_mock_decision_report_contains_sections():
    text = mcp_server._format_mock_decision_report(
        {
            "disclaimer": "x",
            "deciding_court": "bger",
            "preferred_language": "de",
            "facts_summary": "Kurzsachverhalt.",
            "question": "Frage?",
            "key_issues": ["Issue A"],
            "clarification_gate": {
                "status": "needs_clarification",
                "unanswered_high_priority": ["timeline_dates"],
            },
            "clarifying_questions": [
                {
                    "id": "timeline_dates",
                    "question": "Welche Daten?",
                    "why_it_matters": "Fristen",
                    "priority": "high",
                }
            ],
            "clarification_answers": [],
            "applicable_statutes": [],
            "relevant_case_law": [],
            "mock_decision": {
                "outcome_note": "Tendenz.",
                "reasoning_steps": ["Schritt 1"],
                "essential_elements": ["Sachverhalt"],
            },
        }
    )
    assert "# Mock Decision Outline" in text
    assert "## Clarifying Questions" in text
    assert "## Applicable Statutes (Fedlex)" in text
    assert "## Most Relevant Case Law" in text
