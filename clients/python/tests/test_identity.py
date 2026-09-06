"""Identity: joined dockets of consolidated proceedings and duplicate representations.

Two field-test findings. A reference by a joined docket (1B_243/2022 of the
consolidated 1B_242/2022) came back `unrecognized` because the record shows
only the lead docket; and the same ruling stored under two ids appeared twice
in search results. The server now lists `joined_dockets` and
`canonical_decision_id`; these tests pin what the client does with them.
"""
import io
import json
from copy import deepcopy

from opencaselaw_cli import cli, render, workflows
from opencaselaw_cli.cli import build_parser
from opencaselaw_cli.render import Style

LEAD = "bger_1B_242_2022"
LEAD_RECORD = {"decision_id": LEAD, "docket_number": "1B 242/2022", "court": "bger", "canton": "CH",
               "decision_date": "2022-05-30", "joined_dockets": ["1B_243/2022", "1B_244/2022"],
               "source_url": "https://www.bger.ch/x", "citation_string_de": "BGer 1B_242/2022 vom 30. Mai 2022"}
LEAD_CITE = {"exists": True, "decision_id": LEAD, "citation_string": "BGer 1B_242/2022 vom 30. Mai 2022",
             "citation_string_de": "BGer 1B_242/2022 vom 30. Mai 2022"}
LEAD_HIT = {"decision_id": LEAD, "docket_number": "1B 242/2022", "court": "bger", "canton": "CH",
            "citation": "BGer 1B_242/2022 vom 30. Mai 2022", "joined_dockets": ["1B_243/2022", "1B_244/2022"]}


class FakeClient:
    """Answers by path, with overrides; the defaults describe one plain decision `test_case`."""
    base_url = "https://example.test"

    def __init__(self, overrides=None):
        self.calls = []
        self.overrides = overrides or {}
        self.requests = 0

    def get(self, path, params=None):
        self.calls.append((path, params))
        self.requests += 1
        if path in self.overrides:
            value = self.overrides[path]
            if isinstance(value, BaseException):
                raise value
            return deepcopy(value)
        if path == "/api/lookup":
            return {"is_case_number": True, "exact": True, "total": 1,
                    "results": [{"decision_id": "test_case", "docket_number": "test reference"}]}
        if path == "/api/cite":
            return {"exists": True, "decision_id": "test_case", "citation_string": "server citation"}
        if path.startswith("/api/decisions/"):
            return {"decision_id": path.rsplit("/", 1)[-1], "docket_number": "test reference",
                    "source_url": "https://court.example/document", "citation_string_de": "server citation"}
        raise AssertionError(path)


class PagedClient:
    """Answers search pages in order."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []
        self.requests = 0

    def get(self, path, params=None):
        self.calls.append((path, params))
        self.requests += 1
        return self.pages.pop(0)


def page(rows, *, offset=0, more=False):
    return {"results": rows, "returned": len(rows), "total": 10, "total_is_lower_bound": True,
            "has_more": more, "next_offset": offset + len(rows) if more else None, "offset": offset, "limit": len(rows)}


def invoke(monkeypatch, capsys, argv, responses):
    client = PagedClient(responses)
    monkeypatch.setattr(cli, "create_client", lambda args: client)
    monkeypatch.setattr(cli.sys, "stdin", io.StringIO(""))
    monkeypatch.setenv("OCL_JOBS", "1")
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config")
    code = cli.main(argv)
    return client, code, capsys.readouterr()


def resolve(reference, client):
    result, code = workflows.run(build_parser().parse_args(["citations", "resolve", reference]), client)
    return result["results"][0], code


# ── joined dockets ───────────────────────────────────────────────────────

def test_joined_docket_resolves_to_the_lead_decision():
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: LEAD_RECORD,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [LEAD_HIT]}})
    row, code = resolve("BGer 1B_243/2022 vom 30. Mai 2022", client)
    assert code == 0 and row["status"] == "resolved" and row["decision_id"] == LEAD
    check = row["identity_check"]
    assert check["method"] == "exact_server_joined_docket"
    assert check["joined_docket"] == "1B_243/2022" and check["lead_docket"] == "1B 242/2022"
    assert check["docket"] == "1B_243/2022" and check["uniqueness"] == "verified"
    assert check["matching_candidates"][0]["joined_dockets"] == ["1B_243/2022", "1B_244/2022"]
    assert [c[1]["q"] for c in client.calls if c[0] == "/api/lookup"] == ["1B_243/2022"]
    assert "canonical_decision_id" not in row  # nothing says this record is a duplicate


def test_lead_docket_itself_is_still_the_decisions_own_docket():
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: LEAD_RECORD,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [LEAD_HIT]}})
    row, code = resolve("1B_242/2022", client)
    assert code == 0 and row["identity_check"]["method"] == "exact_server_docket"
    assert "joined_docket" not in row["identity_check"]


def test_older_servers_without_joined_dockets_still_say_unrecognized():
    # Documents the field-test failure: with only the lead docket on the record
    # the decision carries no label written in the reference, and nothing is guessed.
    record = {k: v for k, v in LEAD_RECORD.items() if k != "joined_dockets"}
    hit = {k: v for k, v in LEAD_HIT.items() if k != "joined_dockets"}
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: record,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [hit]}})
    row, code = resolve("BGer 1B_243/2022", client)
    assert code == 4 and row["status"] == "unrecognized" and "decision_id" not in row
    assert row["service_candidate"]["decision_id"] == LEAD
    # a new record with an older lookup index: resolved, uniqueness not verified
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: LEAD_RECORD,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [hit]}})
    row, code = resolve("BGer 1B_243/2022", client)
    assert code == 0 and row["identity_check"]["method"] == "exact_server_joined_docket"
    assert row["identity_check"]["uniqueness"] == "unverified"


def test_joined_docket_carried_by_another_decision_is_ambiguous():
    other = {"decision_id": "bger_1B_243_2022", "docket_number": "1B 243/2022", "court": "bger", "canton": "CH"}
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: LEAD_RECORD,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [LEAD_HIT, other]}})
    row, code = resolve("BGer 1B_243/2022", client)
    assert code == 4 and row["status"] == "ambiguous"
    assert {c["decision_id"] for c in row["candidates"]} == {LEAD, "bger_1B_243_2022"}


def test_joined_docket_written_after_the_lead_docket_is_a_cross_reference():
    client = FakeClient({"/api/cite": LEAD_CITE, "/api/decisions/" + LEAD: LEAD_RECORD,
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [LEAD_HIT]}})
    row, code = resolve("BGer 1B_242/2022 (vereinigt mit 1B_243/2022)", client)
    assert code == 0 and row["identity_check"]["method"] == "exact_server_docket"
    assert row["other_dockets"] == ["1B_243/2022"]


# ── duplicate representations ────────────────────────────────────────────

def test_resolved_rows_report_the_canonical_record_without_changing_decision_id():
    duplicate = {"decision_id": "test_case", "docket_number": "test reference", "source_url": "https://court.example/document",
                 "canonical_decision_id": "canonical_case", "is_canonical": False}
    row, code = resolve("test_case", FakeClient({"/api/decisions/test_case": duplicate}))
    assert code == 0 and row["status"] == "resolved"
    assert row["decision_id"] == "test_case" and row["canonical_decision_id"] == "canonical_case"
    assert cli._project(row, ["reference"])["canonical_decision_id"] == "canonical_case"
    args = build_parser().parse_args(["citations", "resolve", "test_case", "--format", "table"])
    table = render.render_table({"results": [row], "counts": {"resolved": 1}, "status": "complete"}, args, 400)
    assert "canonical record canonical_case" in table
    canonical = {**duplicate, "canonical_decision_id": "test_case", "is_canonical": True}
    row, _ = resolve("test_case", FakeClient({"/api/decisions/test_case": canonical}))
    assert "canonical_decision_id" not in row


def test_cite_command_carries_the_canonical_record(monkeypatch, capsys):
    _client, code, output = invoke(monkeypatch, capsys, ["cite", "4A_747/2012", "--format", "json"], [
        {"exists": True, "decision_id": "bger_4A_747_2012", "citation_string": "BGer 4A_747/2012 vom 5. April 2013"},
        {"decision_id": "bger_4A_747_2012", "docket_number": "4A 747/2012", "canonical_decision_id": "bger_canonical", "is_canonical": False},
        {"is_case_number": True, "exact": True, "results": [{"decision_id": "bger_4A_747_2012", "docket_number": "4A 747/2012"}]}])
    assert code == 0
    out = json.loads(output.out)
    assert out["decision_id"] == "bger_4A_747_2012" and out["canonical_decision_id"] == "bger_canonical"


def linked(decision_id, canonical):
    return {"decision_id": decision_id, "court": "bge", "canonical_decision_id": canonical,
            "is_canonical": decision_id == canonical}


def test_search_collapses_duplicate_representations_keeping_the_canonical_row():
    rows = [linked("bge_143 III 38", "bge_BGE_143_III_38"), linked("bger_5A_790_2021", "bger_5A_790_2021"),
            linked("bge_BGE_143_III_38", "bge_BGE_143_III_38"), {"decision_id": "unlinked_row"}]
    result, code = cli.search(PagedClient([page(rows)]), {"query": "x"}, 10)
    assert code == 0
    assert [r["decision_id"] for r in result["results"]] == ["bge_BGE_143_III_38", "bger_5A_790_2021", "unlinked_row"]
    assert result["returned"] == 3
    assert result["_client"]["duplicates_collapsed"] == 1
    assert result["_client"]["collapsed_representations"] == [{"kept": "bge_BGE_143_III_38", "dropped": ["bge_143 III 38"]}]
    kept, code = cli.search(PagedClient([page(rows)]), {"query": "x"}, 10, collapse=False)
    assert code == 0 and len(kept["results"]) == 4 and kept["_client"]["duplicates_collapsed"] == 0
    # a group whose canonical record is not on the page keeps its first row
    rows = [linked("bge_143 III 38", "bge_BGE_143_III_38"), linked("bge_other_form", "bge_BGE_143_III_38")]
    result, _ = cli.search(PagedClient([page(rows)]), {"query": "x"}, 10)
    assert [r["decision_id"] for r in result["results"]] == ["bge_143 III 38"]
    assert result["_client"]["collapsed_representations"] == [{"kept": "bge_143 III 38", "dropped": ["bge_other_form"]}]


def test_filter_enumeration_collapses_across_pages_and_keeps_filling_the_window():
    client = PagedClient([page([linked("a", "a"), linked("a_dup", "a")], more=True), page([linked("b", "b")], offset=2)])
    result, code = cli.search(client, {"court": "bge"}, 2, 2)
    assert code == 0 and [r["decision_id"] for r in result["results"]] == ["a", "b"]
    assert result["_client"]["duplicates_collapsed"] == 1 and result["_client"]["max_results_reached"] is True
    assert [call[1]["limit"] for call in client.calls] == [2, 1]  # the second page fills the collapsed slot


def test_no_collapse_flag_and_footer(monkeypatch, capsys):
    rows = [linked("bge_143 III 38", "bge_BGE_143_III_38"), linked("bge_BGE_143_III_38", "bge_BGE_143_III_38")]
    _client, code, output = invoke(monkeypatch, capsys, ["decisions", "search", "x", "--format", "json"], [page(rows)])
    assert code == 0 and [r["decision_id"] for r in json.loads(output.out)["results"]] == ["bge_BGE_143_III_38"]
    _client, code, output = invoke(monkeypatch, capsys, ["decisions", "search", "x", "--no-collapse", "--format", "json"], [page(rows)])
    assert code == 0 and len(json.loads(output.out)["results"]) == 2
    value = {"results": [rows[1]], "total": 2, "total_is_lower_bound": False, "has_more": False,
             "_client": {"ranked_single_request": True, "duplicates_dropped": 0, "duplicates_collapsed": 1, "errors": []}}
    assert "1 further representation(s) of a listed ruling collapsed" in render.render_search(value, Style(False), 100)
