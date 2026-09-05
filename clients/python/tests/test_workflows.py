"""Offline regressions for retained evidence, recovery and honest resolution."""
import hashlib
import json
from copy import deepcopy
from urllib.parse import quote

import pytest
from opencaselaw_cli import workflows
from opencaselaw_cli.cli import build_parser
from opencaselaw_cli.client import APIError


class FakeClient:
    base_url = "https://example.test"

    def __init__(self, overrides=None):
        self.calls = []
        self.overrides = overrides or {}

    def get(self, path, params=None):
        self.calls.append((path, params))
        if path in self.overrides:
            value = self.overrides[path]
            if callable(value):
                return value(params)
            if isinstance(value, BaseException):
                raise value
            return deepcopy(value)
        if path == "/api/decisions":
            return {"results": [{"decision_id": "test_case"}], "total": 100,
                    "total_is_lower_bound": True, "has_more": True, "next_offset": 1,
                    "offset": 0, "limit": 1, "returned": 1, "result_set_id": "server-result"}
        if path == "/api/lookup":
            return {"is_case_number": True, "total": 1, "results": [{"decision_id": "test_case", "docket_number": "test reference"}]}
        if path == "/api/cite":
            return {"exists": True, "decision_id": "test_case", "citation_string": "server citation"}
        if path.startswith("/api/decisions/"):
            return {"decision_id": path.rsplit("/", 1)[-1], "full_text": "Court text  \nété\n",
                    "source_url": "https://court.example/document", "content_hash": "server-hash",
                    "canonical_key": "server-key", "citation_string_de": "server citation"}
        if path.startswith("/api/erwaegung/"):
            return {"decision_id": "test_case", "e_number": "2.3", "text": "Exact served passage.\n"}
        if path.startswith("/api/laws/"):
            return {"abbreviation": "TEST", "consolidation_date": "2026-01-01",
                    "source_url": "https://law.example/41",
                    "articles": [{"article_num": "41", "text": "Served statute text."}]}
        raise AssertionError(path)


def bundle_args(tmp_path, *extra):
    return build_parser().parse_args(["bundle", "create", "test query", "--out", str(tmp_path / "bundle"),
                                     "--max-results", "1", *extra])


def manifest(tmp_path):
    return json.loads((tmp_path / "bundle/manifest.json").read_text())


def test_bundle_preserves_served_text_hashes_and_bounded_completeness(tmp_path):
    result, code = workflows.run(bundle_args(tmp_path, "--passage", "2.3", "--law", "TEST:41"), FakeClient())
    assert code == 0
    saved = manifest(tmp_path)
    assert saved["completeness"]["exhaustive_legal_research"] is False
    assert saved["completeness"]["server_last_page"]["total_is_lower_bound"] is True
    assert saved["completeness"]["server_last_page"]["has_more"] is True
    assert saved["evidence_contract"]["corpus_snapshot"] is None
    item = saved["items"]["decision:test_case"]
    assert item["provenance"]["content_hash"] == "server-hash"
    assert (tmp_path / "bundle" / item["text_artifact"]["path"]).read_bytes() == "Court text  \nété\n".encode()
    for artifact in saved["artifacts"]:
        raw = (tmp_path / "bundle" / artifact["path"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == artifact["sha256"]
    assert saved["items"]["law:TEST:41"]["provenance"]["consolidation_date"] == "2026-01-01"
    assert result["status"] == "complete"


def test_bundle_files_are_readable_and_indexed(tmp_path):
    workflows.run(bundle_args(tmp_path, "--passage", "2.3", "--law", "TEST:41"), FakeClient())
    saved = manifest(tmp_path)
    paths = {item["kind"]: item["artifact"]["path"] for item in saved["items"].values()}
    assert paths["decision"].startswith("decisions/test_case-") and paths["decision"].endswith(".json")
    assert paths["passage"].startswith("passages/test_case_2.3-")
    assert paths["law"].startswith("laws/TEST_41-")
    index = (tmp_path / "bundle" / "INDEX.md").read_text(encoding="utf-8")
    assert "server citation" in index and paths["decision"] in index and "TEST Art. 41" in index
    assert "- server citation, E. 2.3: saved" in index and "E. 2.3 E. 2.3" not in index
    assert "Status: complete" in index
    assert not any(artifact["path"] == "INDEX.md" for artifact in saved["artifacts"])


def test_unsafe_identifier_characters_never_leave_the_bundle(tmp_path):
    weird = "zh_obergericht_II. ZK Nr. 159/Z_79"
    client = FakeClient({"/api/decisions": {"results": [{"decision_id": weird}], "total": 1,
                                            "total_is_lower_bound": False, "has_more": False, "next_offset": None},
                         "/api/decisions/" + quote(weird, safe=""): {"decision_id": weird, "full_text": "text"}})
    assert workflows.run(bundle_args(tmp_path), client)[1] == 0
    path = next(iter(manifest(tmp_path)["items"].values()))["artifact"]["path"]
    assert path.startswith("decisions/zh_obergericht_II._ZK_Nr._159_Z_79-") and "/" not in path[len("decisions/"):]
    assert workflows._slug("../../etc") == "etc" and workflows._slug("...") == "item"


def test_existing_directory_is_never_overwritten_and_completed_resume_is_offline(tmp_path):
    args = bundle_args(tmp_path)
    workflows.run(args, FakeClient())
    before = manifest(tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        workflows.run(args, FakeClient())
    args.resume = True
    client = FakeClient()
    assert workflows.run(args, client)[1] == 0
    assert client.calls == []
    assert manifest(tmp_path)["artifacts"] == before["artifacts"]


def test_failed_fetch_resumes_without_reselecting_or_overwriting_evidence(tmp_path):
    client = FakeClient({"/api/decisions/test_case": APIError(503, "Unavailable")})
    args = bundle_args(tmp_path)
    assert workflows.run(args, client)[1] == 4
    before = manifest(tmp_path)
    args.resume = True
    retry = FakeClient()
    assert workflows.run(args, retry)[1] == 0
    assert [path for path, _ in retry.calls] == ["/api/decisions/test_case"]
    after = manifest(tmp_path)
    assert before["artifacts"][0] == after["artifacts"][0]
    assert after["attempt_errors"][0]["status"] == 503


def test_missing_full_text_is_partial_and_retry_keeps_previous_response(tmp_path):
    args = bundle_args(tmp_path)
    client = FakeClient({"/api/decisions/test_case": {"decision_id": "test_case", "full_text": None}})
    assert workflows.run(args, client)[1] == 4
    before = manifest(tmp_path)
    args.resume = True
    assert workflows.run(args, FakeClient())[1] == 0
    after = manifest(tmp_path)
    assert all(item in after["artifacts"] for item in before["artifacts"])
    assert all(item["recorded_at"] for item in after["artifacts"])


def test_interrupted_checkpoint_can_resume_with_an_orphan_artifact(tmp_path, monkeypatch):
    original = workflows._checkpoint
    calls = 0

    def interrupt(directory, data):
        nonlocal calls
        calls += 1
        if calls == 2:  # search file exists, but manifest has not recorded it
            raise KeyboardInterrupt()
        return original(directory, data)

    monkeypatch.setattr(workflows, "_checkpoint", interrupt)
    args = bundle_args(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        workflows.run(args, FakeClient())
    monkeypatch.setattr(workflows, "_checkpoint", original)
    args.resume = True
    assert workflows.run(args, FakeClient())[1] == 0
    assert len(list((tmp_path / "bundle/search").glob("*.json"))) == 2
    assert len(manifest(tmp_path)["selection"]["pages"]) == 1


def test_resume_rejects_modified_evidence_and_changed_query(tmp_path):
    args = bundle_args(tmp_path)
    workflows.run(args, FakeClient())
    args.resume = True
    args.query = "changed"
    with pytest.raises(ValueError, match="original query"):
        workflows.run(args, FakeClient())
    args.query = "test query"
    saved = manifest(tmp_path)
    (tmp_path / "bundle" / saved["artifacts"][0]["path"]).write_text("changed")
    client = FakeClient()
    with pytest.raises(ValueError, match="changed or is missing"):
        workflows.run(args, client)
    assert not client.calls


def test_text_query_selection_is_one_ranked_request(tmp_path):
    args = bundle_args(tmp_path)
    args.max_results = 3
    client = FakeClient({"/api/decisions": {"results": [{"decision_id": f"case_{i}"} for i in range(3)], "total": 900,
                                            "total_is_lower_bound": True, "has_more": True, "next_offset": 3}})
    result, code = workflows.run(args, client)
    assert code == 0 and result["status"] == "complete"
    searches = [params for path, params in client.calls if path == "/api/decisions"]
    assert len(searches) == 1 and searches[0]["limit"] == 3
    assert manifest(tmp_path)["completeness"]["ranked_single_request"] is True
    args = bundle_args(tmp_path / "too-many")
    args.max_results = 801
    with pytest.raises(ValueError, match="800"):
        workflows.run(args, FakeClient())


def test_partial_search_resume_continues_original_offset(tmp_path):
    args = bundle_args(tmp_path)
    args.query = ""  # filter-only enumeration is stably ordered and pageable
    args.court = "bger"
    args.max_results = 2
    def pages(params):
        if params["offset"] == 0:
            return {"results": [{"decision_id": "test_case"}], "total": 2,
                    "total_is_lower_bound": False, "has_more": True, "next_offset": 1}
        raise APIError(503, "Try later")
    assert workflows.run(args, FakeClient({"/api/decisions": pages}))[1] == 4
    args.resume = True
    client = FakeClient({"/api/decisions": {"results": [{"decision_id": "other_case"}], "total": 2,
                                            "total_is_lower_bound": False, "has_more": False, "next_offset": None}})
    assert workflows.run(args, client)[1] == 0
    assert client.calls[0][1]["offset"] == 1
    assert "/api/decisions/test_case" not in [path for path, _ in client.calls]


def test_statutes_are_requested_in_a_statute_language(tmp_path):
    args = bundle_args(tmp_path, "--law", "TEST:41", "--language", "en")
    client = FakeClient()
    assert workflows.run(args, client)[1] == 0
    assert ("/api/laws/TEST", {"article": "41", "language": "de"}) in client.calls
    assert manifest(tmp_path)["request"]["law_language"] == "de"


@pytest.mark.parametrize("bad", [None, 123, "", "   "])
def test_statute_requires_requested_article_text(tmp_path, bad):
    args = bundle_args(tmp_path, "--law", "TEST:41")
    client = FakeClient({"/api/laws/TEST": {"articles": [{"article_num": "41", "text": bad}]}})
    assert workflows.run(args, client)[1] == 4
    assert manifest(tmp_path)["items"]["law:TEST:41"]["status"] == "failed"


@pytest.mark.parametrize("bad", [
    {"decision_id": "wrong", "e_number": "2.3", "text": "Other source"},
    {"decision_id": "test_case", "e_number": "9", "text": "Other paragraph"},
    {"decision_id": "test_case", "e_number": "2.3", "text": 123},
])
def test_passage_must_match_requested_source_and_number(tmp_path, bad):
    args = bundle_args(tmp_path, "--passage", "2.3")
    client = FakeClient({"/api/erwaegung/test_case/2.3": bad})
    assert workflows.run(args, client)[1] == 4


def resolution_args(*refs):
    return build_parser().parse_args(["citations", "resolve", *refs])


def test_resolution_preserves_canonical_citation_and_source_link():
    result, code = workflows.run(resolution_args("test reference"), FakeClient())
    assert code == 0
    row = result["results"][0]
    assert row["citation"]["citation_string"] == "server citation"
    assert row["provenance"]["source_url"] == "https://court.example/document"
    assert row["legal_support_assessed"] is False


def test_ambiguous_lookup_does_not_silently_choose_first_match():
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": [
        {"decision_id": "test_case", "docket_number": "ambiguous"},
        {"decision_id": "other_case", "docket_number": "ambiguous"}]}})
    result, code = workflows.run(resolution_args("ambiguous"), client)
    assert code == 4 and result["results"][0]["status"] == "ambiguous"
    assert result["results"][0]["identity_check"]["method"] == "exact_candidate_label"


def test_missing_reference_is_not_replaced_with_suggestion():
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": []},
                         "/api/cite": {"exists": False, "close_matches": [{"decision_id": "suggestion"}]}})
    result, code = workflows.run(resolution_args("missing"), client)
    assert code == 4 and result["results"][0]["status"] == "missing"
    assert not any(path.startswith("/api/decisions/") for path, _ in client.calls)


def test_formatted_pinpoint_does_not_claim_unretrieved_passage(tmp_path):
    path = tmp_path / "refs.jsonl"
    path.write_text(json.dumps({"reference": "test reference", "pinpoint": "2.3"}) + "\n")
    args = resolution_args("--input", str(path))
    client = FakeClient({"/api/erwaegung/test_case/2.3": {"error": "No structured passage"}})
    result, code = workflows.run(args, client)
    assert code == 4 and result["results"][0]["status"] == "pinpoint_unavailable"


def test_unrecognized_reference_requires_exact_canonical_id():
    client = FakeClient({"/api/lookup": {"is_case_number": False, "results": []}})
    result, code = workflows.run(resolution_args("vague reference"), client)
    assert code == 4 and result["results"][0]["status"] == "unrecognized"
    assert workflows.run(resolution_args("test_case"), client)[1] == 0


def test_bad_lookup_payload_is_a_reported_failure():
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": [{"decision_id": []}]}})
    result, code = workflows.run(resolution_args("test reference"), client)
    assert code == 4 and result["results"][0]["status"] == "error"


def test_exact_server_citation_avoids_topical_lookup_false_ambiguity():
    # Fully qualified source citation is identity evidence. A topical lookup
    # can contain many other decisions that merely cite this authority.
    client = FakeClient({"/api/lookup": APIError(503, "Must not call topical lookup")})
    result, code = workflows.run(resolution_args("server citation"), client)
    assert code == 0
    assert result["results"][0]["identity_check"]["method"] == "exact_server_citation"
    assert not any(path == "/api/lookup" for path, _ in client.calls)


def test_padded_lookup_window_with_one_exact_docket_resolves():
    # /api/lookup pads every docket query to a full page of related decisions;
    # a full page is not evidence of ambiguity when one row carries the label.
    candidates = [{"decision_id": "test_case", "docket_number": "4A 747/2012"}]
    candidates += [{"decision_id": f"other_{number}", "docket_number": f"4A_{number}/2013"} for number in range(24)]
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": candidates},
                         "/api/decisions/test_case": {"decision_id": "test_case", "docket_number": "4A_747/2012",
                                                      "source_url": "https://court.example/document"}})
    result, code = workflows.run(resolution_args("4A_747/2012"), client)
    row = result["results"][0]
    assert code == 0 and row["status"] == "resolved"
    assert row["identity_check"]["method"] == "exact_server_docket"
    assert row["identity_check"]["candidate_window_may_be_capped"] is True


def test_docket_reused_by_another_court_stays_ambiguous():
    candidates = [{"decision_id": "test_case", "docket_number": "test reference"},
                  {"decision_id": "other_court_case", "docket_number": "test reference"}]
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": candidates},
                         "/api/decisions/test_case": {"decision_id": "test_case", "docket_number": "test reference"}})
    result, code = workflows.run(resolution_args("test reference"), client)
    assert code == 4 and result["results"][0]["status"] == "ambiguous"


def test_window_full_of_exact_matches_remains_explicitly_incomplete():
    candidates = [{"decision_id": "test_case", "docket_number": "test reference"} for _ in range(25)]
    client = FakeClient({"/api/lookup": {"is_case_number": True, "results": candidates}})
    result, code = workflows.run(resolution_args("test reference"), client)
    assert code == 4 and result["results"][0]["status"] == "resolution_incomplete"


def test_embedded_pinpoint_matches_the_server_citation_without_lookup():
    client = FakeClient({"/api/lookup": APIError(503, "Must not call topical lookup")})
    result, code = workflows.run(resolution_args("server citation, E. 2.3"), client)
    assert code == 0
    assert result["results"][0]["identity_check"]["method"] == "exact_server_citation"


def test_citation_label_normalization_is_only_for_comparison():
    key = workflows.reference_key
    assert workflows._reference_key is key
    assert key("ATF 140 III 86") == key("BGE 140 III 86") == key("DTF 140 III 86") == key("140 III 86")
    assert key("BGE 141 III 433, E. 2.3") == key("BGE 141 III 433 consid. 2.3") == key("BGE 141 III 433")
    assert key("4A_747/2012") == key("4A 747/2012")
    assert key("140 III 86") != key("140 III 860")
    assert key("BGE 100 Ia 5") == "bge100ia5" and key(None) is None
