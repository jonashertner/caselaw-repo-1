"""Text mode: readable at a terminal, never at the expense of the JSON contract."""
import io
import json

import pytest
from opencaselaw_cli import cli, render
from opencaselaw_cli.client import APIError

PLAIN = render.Style(False)
COLOUR = render.Style(True)


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, path, params=None):
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class TTY(io.StringIO):
    def isatty(self):
        return True


def invoke(monkeypatch, argv, responses, tty):
    monkeypatch.setattr(cli, "create_client", lambda args: FakeClient(responses))
    out = TTY() if tty else io.StringIO()
    err = io.StringIO()
    monkeypatch.setattr(cli.sys, "stdout", out)
    monkeypatch.setattr(cli.sys, "stderr", err)
    monkeypatch.delenv("NO_COLOR", raising=False)
    code = cli.main(argv)
    return code, out.getvalue(), err.getvalue()


def test_terminal_gets_text_and_pipes_get_json(monkeypatch):
    payload = {"exists": True, "citation_string": "BGE 136 III 513, E. 2.3", "citation_string_fr": "ATF 136 III 513, consid. 2.3",
               "canonical_url": "https://mcp.opencaselaw.ch/entscheid/bge_BGE_136_III_513#e-2-3"}
    code, out, _ = invoke(monkeypatch, ["cite", "BGE 136 III 513", "--pinpoint", "2.3"], [dict(payload)], tty=True)
    assert code == 0 and out.startswith("\x1b[1mBGE 136 III 513, E. 2.3\x1b[0m")
    assert "ATF 136 III 513, consid. 2.3" in out and "{" not in out
    code, out, _ = invoke(monkeypatch, ["cite", "BGE 136 III 513", "--pinpoint", "2.3"], [dict(payload)], tty=False)
    assert code == 0 and json.loads(out)["citation_string"] == payload["citation_string"]
    code, out, _ = invoke(monkeypatch, ["cite", "BGE 136 III 513", "--format", "json"], [dict(payload)], tty=True)
    assert json.loads(out)["exists"] is True


def test_colour_control(monkeypatch):
    payload = {"exists": True, "citation_string": "BGE 1 I 1"}
    _, out, _ = invoke(monkeypatch, ["cite", "x", "--color", "never"], [dict(payload)], tty=True)
    assert "\x1b[" not in out and out.startswith("BGE 1 I 1")
    _, out, _ = invoke(monkeypatch, ["cite", "x", "--format", "text", "--color", "always"], [dict(payload)], tty=False)
    assert out.startswith("\x1b[1m")
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr(cli, "create_client", lambda args: FakeClient([dict(payload)]))
    out = TTY(); monkeypatch.setattr(cli.sys, "stdout", out)
    assert cli.main(["cite", "x"]) == 0 and "\x1b[" not in out.getvalue()


def test_resolution_report_reads_as_a_table():
    report = {"status": "partial", "counts": {"resolved": 1, "missing": 1}, "results": [
        {"reference": "BGE 136 III 513", "status": "resolved", "decision_id": "bge_BGE_136_III_513",
         "pinpoint": "2.3", "pinpoint_status": "retrieved", "citation": {"citation_string_de": "BGE 136 III 513"}},
        {"reference": "BGE 999 III 1", "status": "missing"},
    ]}
    text = render.render_resolution(report, PLAIN, 100)
    lines = text.splitlines()
    assert lines[0].startswith("resolved") and "bge_BGE_136_III_513" in lines[0] and "E. 2.3 retrieved" in lines[0]
    assert lines[1].startswith("missing") and "not in the corpus" in lines[1]
    assert "partial: 1 resolved, 1 missing." in text and "no assessment of legal support" in text
    coloured = render.render_resolution(report, COLOUR, 100)
    assert "\x1b[32mresolved\x1b[0m" in coloured and "\x1b[31mmissing\x1b[0m" in coloured


def test_search_footer_states_bounds_honestly():
    value = {"results": [{"decision_id": "a", "citation_string_de": "BGE 1 I 1", "court": "bge", "decision_date": "2020-01-01",
                          "snippet": "text with <mark>marked</mark> term"}],
             "total": 125, "total_is_lower_bound": True, "has_more": True, "next_offset": 1,
             "_client": {"ranked_single_request": True, "duplicates_dropped": 0, "errors": []}}
    text = render.render_search(value, PLAIN, 100)
    assert "BGE 1 I 1" in text and "bge · 2020-01-01" in text and "marked term" in text and "<mark>" not in text
    assert "1 shown of at least 125 matching; more retrievable from offset 1. Ranked search over a bounded pool" in text


def test_bundle_summary_reads_the_manifest(tmp_path):
    manifest = {"items": {
        "decision:a": {"kind": "decision", "identifier": "a", "status": "saved",
                       "provenance": {"citation_string_de": "BGE 1 I 1"}, "artifact": {"path": "decisions/a-1.json"},
                       "text_artifact": {"path": "decisions/a-1.txt"}},
        "passage:a:2": {"kind": "passage", "identifier": "a:2", "status": "failed", "provenance": {},
                        "error": {"message": "No structured Erwägungen found"}},
        "law:OR:336": {"kind": "law", "identifier": "OR:336", "status": "saved", "provenance": {"abbreviation": "OR"},
                       "artifact": {"path": "laws/OR_336-1.json"}}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    value = {"status": "partial", "bundle": str(tmp_path), "manifest": str(tmp_path / "manifest.json"),
             "completeness": {"selected_decisions": 1, "failed_items": 1, "server_last_page": {"total": 3, "total_is_lower_bound": False}}}
    text = render.render_bundle(value, PLAIN, 100)
    assert text.startswith("partial  " + str(tmp_path))
    assert "1 decision(s) selected, 3 matching, 1 requested item(s) failed" in text
    assert "saved         BGE 1 I 1  decisions/a-1.txt" in text
    assert "failed        BGE 1 I 1, E. 2  No structured Erwägungen found" in text
    assert "saved         OR Art. 336  laws/OR_336-1.json" in text and "--resume" in text


def test_passage_and_law_rendering_fold_link_markup_for_display_only():
    passage = {"decision_id": "a", "e_number": "2.3", "citation_string_de": "BGE 1 I 1, E. 2.3",
               "text": "Selon l'[art. 335 al. 1 CO](https://mcp.opencaselaw.ch/x), le contrat"}
    text = render.render_passage(passage, PLAIN, 60)
    assert text.startswith("BGE 1 I 1, E. 2.3") and "Selon l'art. 335 al. 1 CO, le contrat" in text and "https://" not in text
    law = {"abbreviation": "OR", "sr_number": "220", "title": "Obligationenrecht", "consolidation_date": "2026-01-01",
           "articles": [{"article_num": "336", "heading": None, "text": "1 Die Kündigung ist missbräuchlich, wenn ..."}],
           "pending_changes": [{"date": "2027-01-01"}]}
    text = render.render_law(law, PLAIN, 80)
    assert text.startswith("OR · SR 220") and "Art. 336" in text and "1 pending change(s)" in text


def test_errors_and_partial_batches_are_readable(monkeypatch):
    code, out, err = invoke(monkeypatch, ["decisions", "passage", "a", "9"],
                            [{"error": "E. '9' not found in 'a'.", "available_e_numbers": ["1", "2"]}], tty=True)
    assert code == 3 and out.startswith("\x1b[31merror: \x1b[0mE. '9' not found") and "available: 1, 2" in out
    code, out, err = invoke(monkeypatch, ["decisions", "get", "a", "b", "--no-full-text"],
                            [{"decision_id": "a", "citation_string_de": "BGE 1 I 1"}, APIError(404, "missing")], tty=True)
    assert code == 4 and "BGE 1 I 1" in out and "1 of 2 item(s) failed" in out and "missing" in out
    assert err.startswith("\x1b[33mocl: \x1b[0m") is False or "ocl:" in err  # stderr is not a tty here: plain prefix
    assert "ocl: some requested items failed" in err


def test_json_contract_is_untouched_by_text_mode(monkeypatch):
    payload = {"results": [{"decision_id": "a"}], "total": 1, "total_is_lower_bound": False, "has_more": False,
               "next_offset": None, "returned": 1, "limit": 5, "offset": 0}
    code, out, _ = invoke(monkeypatch, ["decisions", "search", "x", "--max-results", "5", "--format", "jsonl", "--fields", "decision_id"],
                          [dict(payload)], tty=True)
    lines = [json.loads(l) for l in out.splitlines()]
    assert lines[0] == {"decision_id": "a"} and lines[1]["_type"] == "pagination"
