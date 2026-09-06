"""Passage citation strings name the cantonal court: the canton reaches the citation builder."""
import mcp_server as m


def test_get_erwaegung_passes_the_canton_to_the_citation_builder(monkeypatch):
    for name in ("_capture_event", "_record_tool_call", "_record_tool_outcome", "_record_query"):
        monkeypatch.setattr(m, name, lambda *a, **k: None)
    monkeypatch.setattr(m, "_overlay_enabled", lambda: False)
    monkeypatch.setattr(m, "_auto_link_citations", lambda text: text)
    monkeypatch.setattr(m, "_rule_statement", lambda *a, **k: None)
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "_fetch_structure_row", lambda *a: {"court": "zh_obergericht", "canton": "ZH", "decision_date": "2021-06-15", "language": "de"})
    monkeypatch.setattr(m, "_fetch_structure_paragraphs", lambda *a: [{"e_number": "4.1", "depth": 2, "parent": "4", "text": "served text"}])
    monkeypatch.setattr(m, "get_decision_by_id", lambda *a: {"decision_id": "zh_obergericht_LA210005", "docket_number": "LA210005", "court": "zh_obergericht", "canton": "ZH"})
    seen = []
    def builder(decision, pinpoint=None):
        seen.append(dict(decision))
        return {"citation_string_de": "x", "citation_string_fr": "x", "citation_string_it": "x", "canonical_url": "https://example.invalid/r"}
    monkeypatch.setattr(m, "_build_citation_strings", builder)
    token = m._ctx_client_ua.set("")
    try:
        result = m._handle_get_erwaegung(decision_id="zh_obergericht_LA210005", e_number="4.1")
    finally:
        m._ctx_client_ua.reset(token)
    assert result.get("text") == "served text"
    assert seen and seen[0].get("canton") == "ZH" and seen[0].get("court") == "zh_obergericht"


def test_cantonal_label_needs_the_canton():
    with_canton = m._build_citation_strings({"decision_id": "zh_obergericht_LA210005", "docket_number": "LA210005", "court": "zh_obergericht", "canton": "ZH", "decision_date": "2021-06-15"})
    without = m._build_citation_strings({"decision_id": "zh_obergericht_LA210005", "docket_number": "LA210005", "court": "zh_obergericht", "decision_date": "2021-06-15"})
    assert "ZH" in with_canton["citation_string_de"] and with_canton["citation_string_de"] != without["citation_string_de"]
