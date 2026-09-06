"""cite never cites a decision that does not carry the written label (server-side identity rule)."""
import pytest

import mcp_server as m

RECORD = {"decision_id": "bger_6B_1247_2020", "docket_number": "6B_1247/2020", "court": "bger", "canton": "CH",
          "decision_date": "2021-10-07", "language": "de", "full_text": "x", "joined_dockets": ["6B_1250/2020"]}
STRINGS = {"citation_string_de": "BGer 6B_1247/2020 vom 7. Oktober 2021", "citation_string_fr": "TF 6B_1247/2020 du 7 octobre 2021",
           "citation_string_it": "TF 6B_1247/2020 del 7 ottobre 2021", "canonical_url": "https://example.invalid/r"}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    for name in ("_capture_event", "_record_tool_call", "_record_tool_outcome", "_record_query"):
        monkeypatch.setattr(m, name, lambda *a, **k: None)
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: "bger_6B_1247_2020")
    monkeypatch.setattr(m, "get_decision_by_id", lambda *a: dict(RECORD))
    monkeypatch.setattr(m, "_build_citation_strings", lambda decision, pinpoint=None: dict(STRINGS))
    monkeypatch.setattr(m, "_rule_statement", lambda *a, **k: None)
    monkeypatch.setattr(m, "_bge_volume_year_mismatch", lambda *a, **k: None)
    monkeypatch.setattr(m, "_docket_close_matches", lambda *a, **k: [])
    monkeypatch.setattr(m, "search_fts5", lambda **k: ([], None))


def test_a_docket_fragment_is_a_close_match_never_a_citation():
    out = m._handle_cite(reference="247/2020")
    assert out["exists"] is False and out["close_matches"][0]["decision_id"] == "bger_6B_1247_2020"
    assert out["close_matches"][0]["match_reason"] == "label_not_carried" and "citation_string" not in out


@pytest.mark.parametrize("reference, method", [
    ("6B_1247/2020", "exact_docket"),
    ("6B 1247/2020", "exact_docket"),
    ("BGer 6B_1247/2020 vom 7. Oktober 2021", "exact_server_citation"),
    ("Urteil des Bundesgerichts 6B_1247/2020 vom 7. Oktober 2021, E. 3", "exact_docket"),
    ("bger_6B_1247_2020", "exact_canonical_id"),
    ("6B_1250/2020", "exact_joined_docket"),
])
def test_written_labels_the_decision_carries_are_cited(reference, method):
    out = m._handle_cite(reference=reference)
    assert out["exists"] is True and out["identity"]["method"] == method and out["citation_string"] == STRINGS["citation_string_de"]


def test_a_docket_mentioned_after_the_label_does_not_carry_the_proposal():
    out = m._handle_cite(reference="Obergericht ZH LA210005 vom 15. Juni 2021 (vgl. auch BGer 6B_1247/2020)")
    assert out["exists"] is False and out["close_matches"][0]["match_reason"] == "label_not_carried"


def test_bge_labels_are_checked_by_tuple(monkeypatch):
    bge = {"decision_id": "bge_BGE_134_III_354", "docket_number": "134 III 354", "court": "bge", "decision_date": "2008-04-29", "full_text": "x"}
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: "bge_BGE_134_III_354")
    monkeypatch.setattr(m, "get_decision_by_id", lambda *a: dict(bge))
    assert m._handle_cite(reference="BGE 134 III 354 S. 357")["identity"]["method"] == "exact_bge_label"
    assert m._handle_cite(reference="ATF 134 III 354 consid. 2.1")["exists"] is True
    assert m._handle_cite(reference="BGE 134 III 355")["exists"] is False
