"""Quotations are checked against the served text; the served wording is authoritative."""
import json

from opencaselaw_cli import workflows
from opencaselaw_cli.cli import build_parser
from opencaselaw_cli.workflows import match_quote, normalise_quote

from test_workflows import FakeClient

PASSAGE = ("Selon l'art. 335 al. 1 CO, le contrat de travail conclu pour une durée indéter-\nminée peut être résilié par chacune des "
           "parties. Cette liberté [BGE 131 III 535](https://x/1) est limitée par les règles sur le congé abusif.")


def test_normalisation_folds_typography_hyphenation_and_links():
    assert normalise_quote("l’art. 335  al. 1 CO, « le contrat »") == "l'art. 335 al. 1 CO, \"le contrat\""
    assert normalise_quote("durée indéter-\nminée peut") == "durée indéterminée peut"
    assert normalise_quote("Cette liberté [BGE 131 III 535](https://x/1) est") == "Cette liberté BGE 131 III 535 est"


def test_exact_near_and_not_found():
    assert match_quote("le contrat de travail conclu pour une durée indéterminée peut être résilié", PASSAGE)["quote_status"] == "exact"
    near = match_quote("le contrat de travail conclu pour une durée déterminée peut être résilié", PASSAGE)
    assert near["quote_status"] == "near" and near["ratio"] >= 0.9
    assert any(d["quote"].strip() == "" or "in" in d["served"] for d in near["differences"])  # "indéterminée" vs "déterminée"
    assert match_quote("le juge apprécie librement les preuves", PASSAGE)["quote_status"] == "not_found"
    assert match_quote("", PASSAGE)["quote_status"] == "not_found"


def quote_args(*argv):
    return build_parser().parse_args(["quotes", "check", *argv])


def test_quote_in_the_passage_then_in_the_full_text(tmp_path):
    client = FakeClient({
        "/api/cite": {"exists": True, "decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513"},
        "/api/decisions/bge_BGE_136_III_513": lambda params: (
            {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513",
             "full_text": "1. Recevabilité.\n\n2.3 " + PASSAGE + "\n\n2.4 Il s'ensuit que le congé est abusif au sens de l'art. 336 CO."}
            if params and params.get("full_text") else {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513"}),
        "/api/erwaegung/bge_BGE_136_III_513/2.3": {"decision_id": "bge_BGE_136_III_513", "e_number": "2.3", "text": PASSAGE},
    })
    result, code = workflows.run(quote_args("BGE 136 III 513 E. 2.3", "--quote", "le contrat de travail conclu pour une durée indéterminée"), client)
    row = result["results"][0]
    assert code == 0 and row["quote_status"] == "exact" and row["found_in"] == "E. 2.3" and row["status"] == "resolved"
    # spans two paragraphs: not in the passage, found in the decision text
    path = tmp_path / "q.jsonl"
    path.write_text(json.dumps({"reference": "BGE 136 III 513", "pinpoint": "2.3", "quote": "règles sur le congé abusif. 2.4 Il s'ensuit que le congé est abusif"}) + "\n"
                    + json.dumps({"reference": "BGE 136 III 513", "quote": "une phrase qui ne figure nulle part dans cet arrêt"}) + "\n", encoding="utf-8")
    result, code = workflows.run(quote_args("--input", str(path)), client)
    rows = result["results"]
    assert code == 4 and rows[0]["quote_status"] == "exact" and rows[0]["found_in"] == "full_text"
    assert rows[1]["quote_status"] == "not_found" and rows[1]["status"] == "quote_not_found" and rows[1]["served"]
    assert result["counts"] == {"exact": 1, "not_found": 1}


def test_a_quote_on_a_resolve_row_is_checked_too(tmp_path):
    client = FakeClient({"/api/erwaegung/test_case/2.3": {"decision_id": "test_case", "e_number": "2.3", "text": "Exact served passage.\n"},
                         "/api/decisions/test_case": {"decision_id": "test_case", "citation_string_de": "server citation", "full_text": "Exact served passage."}})
    path = tmp_path / "refs.jsonl"
    path.write_text(json.dumps({"reference": "server citation", "pinpoint": "2.3", "quote": "Exact served passage"}) + "\n"
                    + json.dumps({"reference": "server citation", "pinpoint": "2.3", "quote": "Exactly served passage"}) + "\n", encoding="utf-8")
    result, code = workflows.run(build_parser().parse_args(["citations", "resolve", "--input", str(path)]), client)
    rows = result["results"]
    assert rows[0]["quote_check"]["quote_status"] == "exact" and rows[1]["quote_check"]["quote_status"] == "near"
    assert code == 4 and result["status"] == "partial"


def test_unresolved_reference_leaves_the_quote_unchecked():
    client = FakeClient({"/api/cite": {"exists": False, "close_matches": []}})
    result, code = workflows.run(quote_args("BGE 999 III 1", "--quote", "anything"), client)
    assert code == 4 and result["results"][0]["status"] == "missing" and result["results"][0]["quote_status"] == "not_checked"
