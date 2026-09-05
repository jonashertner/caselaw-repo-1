import io
import json

import pytest
from opencaselaw_cli import cli
from opencaselaw_cli.client import APIError


class FakeClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        item = self.pages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def page(ids, *, offset=0, total=10, lower=True, more=True):
    return {"results": [{"decision_id": id_, "court": "bger"} for id_ in ids],
            "returned": len(ids), "total": total, "total_is_lower_bound": lower,
            "has_more": more, "next_offset": offset + len(ids) if more else None,
            "offset": offset, "limit": len(ids), "result_set_id": "rs_fixture"}


def invoke(monkeypatch, capsys, argv, responses, stdin=""):
    client = FakeClient(responses)
    monkeypatch.setattr(cli, "create_client", lambda args: client)
    monkeypatch.setattr(cli.sys, "stdin", io.StringIO(stdin))
    code = cli.main(argv)
    out = capsys.readouterr()
    return client, code, out


def test_bounded_pagination_keeps_lower_bound_and_server_page_metadata():
    client = FakeClient([page(["a", "b"]), page(["c"], offset=2, lower=False)])
    result, code = cli.search(client, {"court": "bger", "offset": 0}, 3, 2)
    assert code == 0
    assert [row["decision_id"] for row in result["results"]] == ["a", "b", "c"]
    assert result["total_is_lower_bound"] is True
    assert result["has_more"] is True and result["next_offset"] == 3
    assert result["_client"]["max_results_reached"] is True
    assert result["_client"]["retrieval_complete"] is False
    assert [call[1]["limit"] for call in client.calls] == [2, 1]
    assert [call[1]["offset"] for call in client.calls] == [0, 2]
    assert result["_client"]["pages"][0]["result_set_id"] == "rs_fixture"


def test_text_query_is_one_ranked_request_within_the_server_cap():
    # Ranked pages are not composable: the candidate pool depends on the window.
    client = FakeClient([page([f"d{i}" for i in range(7)], more=True, total=500)])
    result, code = cli.search(client, {"query": "Rachekündigung"}, 7, 2)
    assert code == 0 and len(client.calls) == 1
    assert client.calls[0][1]["limit"] == 7 and client.calls[0][1]["offset"] == 0
    assert result["returned"] == 7 and result["has_more"] is True
    assert result["_client"]["ranked_single_request"] is True
    assert result["_client"]["retrieval_complete"] is False
    with pytest.raises(ValueError, match="800"):
        cli.search(FakeClient([]), {"query": "Rachekündigung"}, 801, 50)
    assert cli.search(FakeClient([page(["a"], more=False)]), {"court": "bge"}, 801, 50)[1] == 0


def test_filter_enumeration_drops_duplicate_rows_across_pages():
    client = FakeClient([page(["a", "b"]), page(["b", "c"], offset=2, more=False)])
    result, code = cli.search(client, {"court": "bge"}, 10, 2)
    assert code == 0
    assert [row["decision_id"] for row in result["results"]] == ["a", "b", "c"]
    assert result["returned"] == 3 and result["_client"]["duplicates_dropped"] == 1
    assert result["_client"]["ranked_single_request"] is False


def test_partial_search_failure_returns_evidence_and_retry_offset():
    client = FakeClient([page(["a", "b"]), APIError(503, "busy")])
    result, code = cli.search(client, {}, 4, 2)
    assert code == 4 and len(result["results"]) == 2
    assert result["has_more"] is True and result["next_offset"] == 2
    assert result["_client"]["errors"] == [{"offset": 2, "status": 503, "message": "busy"}]


def test_bounded_candidate_pool_does_not_claim_exhaustive_match_count():
    client = FakeClient([page(["a"], more=False, total=2000, lower=True)])
    result, code = cli.search(client, {}, 100)
    assert code == 0
    assert result["total"] == 2000 and result["total_is_lower_bound"] is True
    assert result["_client"]["retrieval_complete"] is True
    assert "exhaustive" not in result


def test_nonadvancing_pagination_stops_and_reports_partial():
    response = page(["a"])
    response["next_offset"] = 0
    client = FakeClient([response])
    result, code = cli.search(client, {}, 100)
    assert code == 4 and len(client.calls) == 1
    assert result["results"][0]["decision_id"] == "a"
    assert result["_client"]["errors"]


def test_jsonl_search_pipes_into_get_without_losing_error_metadata(monkeypatch, capsys):
    _, code, output = invoke(monkeypatch, capsys,
        ["decisions", "search", "OR", "--format", "jsonl", "--fields", "decision_id", "--max-results", "2"],
        [page(["a", "b"])])
    assert code == 0 and not output.err
    lines = [json.loads(line) for line in output.out.splitlines()]
    assert lines[:2] == [{"decision_id": "a"}, {"decision_id": "b"}]
    assert lines[2]["_type"] == "pagination" and lines[2]["total_is_lower_bound"] is True
    client, code, output = invoke(monkeypatch, capsys,
        ["decisions", "get", "--stdin", "--format", "jsonl"],
        [{"decision_id": "a", "full_text": "exact source"}, APIError(404, "missing")], output.out)
    assert code == 4 and "failed" in output.err
    lines = [json.loads(line) for line in output.out.splitlines()]
    assert lines[0]["full_text"] == "exact source"
    assert lines[1]["errors"][0]["decision_id"] == "b"
    assert len(client.calls) == 2


def test_invalid_batch_is_rejected_before_any_requests(monkeypatch, capsys):
    client, code, output = invoke(monkeypatch, capsys, ["decisions", "get", "--stdin"], [], 'valid\n{"wrong":"id"}\n')
    assert code == 2 and not client.calls and not output.out
    assert "stdin:2" in output.err


def test_cite_per_reference_pinpoint_overrides_common_default(monkeypatch, capsys):
    client, code, output = invoke(monkeypatch, capsys, ["cite", "--stdin", "--pinpoint", "1", "--language", "fr"],
        [{"citation_string": "server canonical citation"}, {"citation_string": "second"}],
        '{"reference":"BGE 140 III 86","pinpoint":"2.3"}\n{"reference":"BGE 136 III 513","pinpoint":null}\n')
    assert code == 0
    assert client.calls == [("/api/cite", {"reference": "BGE 140 III 86", "pinpoint": "2.3", "language": "fr"}),
                            ("/api/cite", {"reference": "BGE 136 III 513", "pinpoint": "1", "language": "fr"})]
    assert json.loads(output.out)["results"][0]["citation_string"] == "server canonical citation"


def test_plain_input_file(tmp_path, monkeypatch, capsys):
    path = tmp_path / "ids.txt"
    path.write_text("\ufeffa\nb\n", encoding="utf-8")  # editors may prepend a BOM
    client, code, _output = invoke(monkeypatch, capsys, ["decisions", "get", "--input", str(path), "--no-full-text"],
        [{"decision_id": "a"}, {"decision_id": "b"}])
    assert code == 0 and len(client.calls) == 2
    assert all(call[1] == {"full_text": False} for call in client.calls)


def test_top_level_error_is_stderr_only(monkeypatch, capsys):
    _, code, output = invoke(monkeypatch, capsys, ["decisions", "passage", "a", "2.3"], [APIError(404, "no passage")])
    assert code == 3 and not output.out
    assert json.loads(output.err)["error"]["status"] == 404


def test_http_200_error_is_retained_with_failure_exit(monkeypatch, capsys):
    _, code, output = invoke(monkeypatch, capsys, ["decisions", "passage", "a", "2.3", "--fields", "text"],
                            [{"error": "no structured passage", "hint": "retrieve full decision"}])
    assert code == 3
    assert json.loads(output.out) == {"error": "no structured passage", "hint": "retrieve full decision"}


def test_missing_citation_remains_explicit_under_field_projection(monkeypatch, capsys):
    _, code, output = invoke(monkeypatch, capsys, ["cite", "unknown", "--fields", "citation_string"],
                            [{"exists": False, "close_matches": ["candidate"], "warning": "unresolved"}])
    assert code == 4
    assert json.loads(output.out) == {"exists": False, "close_matches": ["candidate"], "warning": "unresolved"}


def test_citation_graph_jsonl_keeps_direction_and_pagination(monkeypatch, capsys):
    _, code, output = invoke(monkeypatch, capsys,
        ["citations", "list", "a", "--format", "jsonl", "--fields", "source_decision_id"],
        [{"incoming": [{"source_decision_id": "b", "confidence_score": 0.8}], "outgoing": [],
          "incoming_has_more": True, "outgoing_has_more": False, "next_offset": 1}])
    assert code == 0
    rows = [json.loads(line) for line in output.out.splitlines()]
    assert rows[0] == {"source_decision_id": "b", "_direction": "incoming"}
    assert rows[1]["incoming_has_more"] is True and rows[1]["next_offset"] == 1


@pytest.mark.parametrize("argv", [
    ["--format", "jsonl", "decisions", "search", "OR"],
    ["decisions", "--format", "jsonl", "search", "OR"],
    ["decisions", "search", "OR", "--format", "jsonl"],
])
def test_common_flags_work_at_each_level(argv):
    args = cli.build_parser().parse_args(argv)
    assert args.format == "jsonl" and args.base_url == "https://mcp.opencaselaw.ch"


def test_workflow_parser_contract():
    args = cli.build_parser().parse_args(["bundle", "create", "Art. 41 OR", "--out", "evidence", "--law", "OR:41", "--passage", "2.3", "--resume"])
    assert (args.command, args.action, args.query, args.out) == ("bundle", "create", "Art. 41 OR", "evidence")
    assert args.law == ["OR:41"] and args.passage == ["2.3"] and args.resume
    args = cli.build_parser().parse_args(["citations", "resolve", "BGE 140 III 86", "--language", "fr"])
    assert args.references == ["BGE 140 III 86"] and args.language == "fr"


def test_help_has_no_network_side_effect(monkeypatch, capsys):
    monkeypatch.setattr(cli, "create_client", lambda args: pytest.fail("help must not create a client"))
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["decisions", "search", "--help"])
    assert exit_info.value.code == 0
    assert "--max-results" in capsys.readouterr().out


def test_every_command_and_argument_explains_itself():
    parser = cli.build_parser()
    missing = []

    def walk(p, path):
        for action in p._actions:
            if isinstance(action, cli.argparse._SubParsersAction):
                for name, sub in action.choices.items():
                    if not sub.description:
                        missing.append(path + [name, "<description>"])
                    walk(sub, path + [name])
            elif action.dest not in ("help", "version") and not (action.help or "").strip():
                missing.append(path + [action.dest])

    walk(parser, ["ocl"])
    assert not missing, missing
    assert "examples:" in parser.format_help()


def test_closed_pipe_exits_cleanly(monkeypatch):
    class ClosedPipe:
        def write(self, _):
            raise BrokenPipeError()

    monkeypatch.setattr(cli, "create_client", lambda args: FakeClient([{"decision_id": "a"}]))
    monkeypatch.setattr(cli.sys, "stdout", ClosedPipe())
    assert cli.main(["decisions", "get", "a"]) == 0


def test_projection_keeps_server_qualification():
    payload = {"citation_string": "verbatim citation", "decision_date_warning": "source discrepancy",
               "rule_statement_note": "quotation withheld"}
    assert cli._project(payload, ["citation_string"]) == payload


@pytest.mark.parametrize("argv,path", [
    (["decisions", "get", "4A_747/2012"], "/api/decisions/bger_4A_747_2012"),
    (["decisions", "passage", "4A_747/2012", "2.3"], "/api/erwaegung/bger_4A_747_2012/2.3"),
    (["citations", "list", "4A_747/2012"], "/api/citations/bger_4A_747_2012"),
])
def test_docket_slashes_resolve_via_server_before_path_request(monkeypatch, capsys, argv, path):
    client, code, _output = invoke(monkeypatch, capsys, argv,
        [{"exists": True, "decision_id": "bger_4A_747_2012"},
         {"decision_id": "bger_4A_747_2012", "docket_number": "4A 747/2012"},
         {"decision_id": "bger_4A_747_2012"}])
    assert code == 0
    assert client.calls[0] == ("/api/cite", {"reference": "4A_747/2012"})
    assert client.calls[1] == ("/api/decisions/bger_4A_747_2012", {"full_text": False})
    assert client.calls[2][0] == path


def test_docket_fragment_matched_by_substring_is_rejected(monkeypatch, capsys):
    # The service resolves "247/2020" to the newest decision whose docket
    # contains it; the client must not print another chamber's passage.
    client, code, output = invoke(monkeypatch, capsys, ["decisions", "passage", "247/2020", "2"],
        [{"exists": True, "decision_id": "bger_6B_1247_2020", "citation_string_de": "BGer 6B_1247/2020 vom 7. Oktober 2021"},
         {"decision_id": "bger_6B_1247_2020", "docket_number": "6B_1247/2020"}])
    assert code == 3 and len(client.calls) == 2 and not output.out
    assert "6B_1247/2020" in json.loads(output.err)["error"]["message"]


def test_unresolved_docket_never_fabricates_a_canonical_id(monkeypatch, capsys):
    client, code, output = invoke(monkeypatch, capsys, ["decisions", "get", "4A_00000/2012"],
                                  [{"exists": False, "close_matches": [{"decision_id": "different"}]}])
    assert code == 3 and len(client.calls) == 1
    assert json.loads(output.out)["errors"][0]["decision_id"] == "4A_00000/2012"


def test_projection_keeps_workflow_completeness_and_artifact_pointer():
    payload = {"status": "partial", "bundle": "/tmp/evidence", "manifest": "/tmp/evidence/manifest.json",
               "completeness": {"selected_evidence_retrieved": False}, "scope": "bounded selection"}
    assert cli._project(payload, ["status"]) == payload


def test_law_projection_retains_edition_and_original_source():
    payload = {"articles": [{"article_num": "41", "text": "source"}], "version": "historical",
               "snapshot_date": "2017-04-01", "source_url": "https://www.fedlex.admin.ch/fixture"}
    assert cli._project(payload, ["articles"]) == payload


def test_projection_retains_citation_identity_evidence():
    payload = {"decision_id": "a", "status": "resolution_incomplete",
               "identity_check": {"method": "exact_candidate_label", "candidate_window_may_be_capped": True}}
    assert cli._project(payload, ["decision_id"]) == payload
