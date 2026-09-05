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
        if path == "/health":
            return {"status": "ok", "decisions": 25, "db_generation": 1700000000}
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
    assert "not an immutable copy" in saved["evidence_contract"]["corpus_snapshot"]
    assert saved["corpus_snapshot"]["db_generation"] == 1700000000
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
    assert [path for path, _ in retry.calls if path != "/health"] == ["/api/decisions/test_case"]
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
    searches = [params for path, params in client.calls if path == "/api/decisions"]
    assert searches[0]["offset"] == 1
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


def test_bundle_records_the_corpus_generation_and_cantonal_statutes(tmp_path):
    args = bundle_args(tmp_path, "--law", "ZH/StG:1", "--law", "OR:41")
    client = FakeClient({"/api/laws/StG": {"articles": [{"article_num": "1", "text": "Kantonaler Text"}]},
                         "/api/laws/OR": {"articles": [{"article_num": "41", "text": "Bundesrecht"}]}})
    result, code = workflows.run(args, client)
    assert code == 0 and result["completeness"]["corpus_generation"] == 1700000000
    saved = manifest(tmp_path)
    assert saved["corpus_snapshot"]["decisions"] == 25
    assert ("/api/laws/StG", {"article": "1", "language": "de", "canton": "ZH"}) in client.calls
    assert saved["items"]["law:ZH/StG:1"]["status"] == "saved" and saved["items"]["law:OR:41"]["status"] == "saved"
    assert "database generation 1700000000" in (tmp_path / "bundle" / "INDEX.md").read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="CANTON/ABBREVIATION"):
        workflows.run(bundle_args(tmp_path / "x", "--law", "ZH/StG"), FakeClient())


def test_bundle_verify_diff_and_add(tmp_path):
    args = bundle_args(tmp_path)
    workflows.run(args, FakeClient())
    verify = build_parser(config={}).parse_args(["bundle", "verify", str(tmp_path / "bundle")])
    report, code = workflows.run(verify, FakeClient())
    assert code == 0 and report["status"] == "verified" and report["counts"]["changed"] == 0
    saved = manifest(tmp_path)
    (tmp_path / "bundle" / saved["artifacts"][0]["path"]).write_bytes(b"tampered")
    (tmp_path / "bundle" / "stray.txt").write_text("not listed")
    report, code = workflows.run(verify, FakeClient())
    assert code == 4 and report["status"] == "failed"
    assert report["changed"] == [saved["artifacts"][0]["path"]] and report["unlisted"] == ["stray.txt"]
    # a second run of the same question against a changed corpus
    later = FakeClient({"/health": {"decisions": 26, "db_generation": 1700000999},
                        "/api/decisions": {"results": [{"decision_id": "other_case"}], "total": 1,
                                           "total_is_lower_bound": False, "has_more": False, "next_offset": None}})
    args2 = build_parser(config={}).parse_args(["bundle", "create", "test query", "--out", str(tmp_path / "later"), "--max-results", "1"])
    assert workflows.run(args2, later)[1] == 0
    diff = build_parser(config={}).parse_args(["bundle", "diff", str(tmp_path / "bundle"), str(tmp_path / "later")])
    report, code = workflows.run(diff, FakeClient())
    assert code == 0 and report["decisions"]["added"] == ["other_case"] and report["decisions"]["removed"] == ["test_case"]
    assert report["corpus_generation"] == {"old": 1700000000, "new": 1700000999, "changed": True}
    # add a decision found elsewhere, with the bundle's passages plus one more
    add = build_parser(config={}).parse_args(["bundle", "add", str(tmp_path / "later"), "test_case", "--passage", "2.3"])
    result, code = workflows.run(add, FakeClient())
    assert code == 0 and result["added"] == {"decision:test_case": "saved", "passage:test_case:2.3": "saved"}
    later_manifest = json.loads((tmp_path / "later" / "manifest.json").read_text())
    assert later_manifest["additions"][0]["decision_ids"] == ["test_case"]
    assert "test_case" in (tmp_path / "later" / "INDEX.md").read_text(encoding="utf-8")


def test_resolution_stops_after_repeated_transport_failures():
    client = FakeClient({"/api/cite": APIError(None, "Request failed: connection refused")})
    args = resolution_args(*[f"BGE {n} III 1" for n in range(100, 112)])
    args.jobs = 1
    result, code = workflows.run(args, client)
    statuses = [row["status"] for row in result["results"]]
    assert code == 4 and statuses[:5] == ["error"] * 5 and set(statuses[5:]) == {"skipped"}
    assert len([c for c in client.calls if c[0] == "/api/cite"]) == 5


def test_concurrent_fetches_are_recorded_in_order(tmp_path):
    import threading
    class SlowClient(FakeClient):
        def get(self, path, params=None):
            if path.startswith("/api/decisions/case_"):
                import time; time.sleep(0.01 * (3 - int(path[-1]) % 3))
            return super().get(path, params)
    ids = ["case_0", "case_1", "case_2", "case_3", "case_4"]
    client = SlowClient({"/api/decisions": {"results": [{"decision_id": i} for i in ids], "total": 5,
                                            "total_is_lower_bound": False, "has_more": False, "next_offset": None}})
    args = bundle_args(tmp_path); args.max_results = 5; args.jobs = 4
    assert workflows.run(args, client)[1] == 0
    saved = manifest(tmp_path)
    assert [k for k in saved["items"] if k.startswith("decision:")] == ["decision:" + i for i in ids]
    assert all(item["status"] == "saved" for item in saved["items"].values())


def test_long_form_references_resolve_through_their_docket():
    assert workflows.extract_docket("BGer 4A_747/2012 vom 5. April 2013") == "4A_747/2012"
    assert workflows.extract_docket("Urteil des Verwaltungsgerichts des Kantons Aargau WBE.2026.33") == "WBE.2026.33"
    assert workflows.extract_docket("4A_747/2012") is None and workflows.extract_docket("BGE 136 III 513") is None
    calls = []
    def cite(params):
        calls.append(params["reference"])
        if params["reference"] == "4A_747/2012":
            return {"exists": True, "decision_id": "bger_4A_747_2012", "citation_string_de": "BGer 4A_747/2012 vom 5. April 2013"}
        return {"exists": False, "queried": params["reference"], "close_matches": [{"decision_id": "bger_4A_747_2012"}]}
    client = FakeClient({"/api/cite": cite,
                         "/api/decisions/bger_4A_747_2012": {"decision_id": "bger_4A_747_2012", "docket_number": "4A_747/2012",
                                                             "citation_string_de": "BGer 4A_747/2012 vom 5. April 2013"},
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [{"decision_id": "bger_4A_747_2012", "docket_number": "4A_747/2012"}]}})
    result, code = workflows.run(resolution_args("BGer 4A_747/2012 vom 5. April 2013"), client)
    row = result["results"][0]
    assert code == 0 and row["status"] == "resolved" and row["query"] == "4A_747/2012"
    assert row["identity_check"]["method"] == "exact_server_citation"  # the service's own string equals the reference
    assert calls == ["4A_747/2012"]  # the docket inside the long form is queried directly
    assert result["requests"] is None or isinstance(result["requests"], int)
    # a long form around a docket stored with a space, identity through the docket, scoped to the federal court
    client = FakeClient({"/api/cite": {"exists": True, "decision_id": "bger_4A_255_2012", "citation_string_de": "BGer 4A_255/2012 vom 20. Juli 2012"},
                         "/api/decisions/bger_4A_255_2012": {"decision_id": "bger_4A_255_2012", "docket_number": "4A 255/2012", "court": "bger",
                                                             "citation_string_de": "BGer 4A_255/2012 vom 20. Juli 2012"},
                         "/api/erwaegung/bger_4A_255_2012/3": {"decision_id": "bger_4A_255_2012", "e_number": "3", "text": "served"},
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [
                             {"decision_id": "bger_4A_255_2012", "docket_number": "4A 255/2012", "court": "bger"},
                             {"decision_id": "ge_gerichte_4A_255_2012", "docket_number": "4A_255/2012", "court": "ge_gerichte", "canton": "GE"}]}})
    row = workflows.run(resolution_args("Urteil des Bundesgerichts 4A_255/2012, E. 3"), client)[0]["results"][0]
    assert row["status"] == "resolved" and row["identity_check"]["method"] == "exact_server_docket"
    assert [c["decision_id"] for c in row["identity_check"]["out_of_scope_candidates"]] == ["ge_gerichte_4A_255_2012"]
    assert row["pinpoint"] == "3" and row["pinpoint_source"] == "reference" and row["pinpoint_status"] == "retrieved"
    # the bare docket, with no court named, stays ambiguous and lists both carriers
    row = workflows.run(resolution_args("4A_255/2012"), client)[0]["results"][0]
    assert row["status"] == "ambiguous" and {c["decision_id"] for c in row["candidates"]} == {"bger_4A_255_2012", "ge_gerichte_4A_255_2012"}
    # a reference with no docket inside stays missing, with a note that close matches are not substitutes
    result, code = workflows.run(resolution_args("Bundesgericht, Urteil vom 5. April 2013"), FakeClient({"/api/cite": {"exists": False, "close_matches": [{"decision_id": "x"}]}}))
    assert code == 4 and result["results"][0]["status"] == "missing" and "never substitutes" in result["results"][0]["note"]


def test_unavailable_items_are_told_apart_from_failures(tmp_path):
    client = FakeClient({"/api/erwaegung/test_case/2.3": {"error": "No structured Erwägungen found for 'test_case'."},
                         "/api/laws/TEST": APIError(None, "Request failed: connection reset")})
    args = bundle_args(tmp_path, "--passage", "2.3", "--law", "TEST:41")
    result, code = workflows.run(args, client)
    saved = manifest(tmp_path)
    assert code == 4 and saved["items"]["passage:test_case:2.3"]["status"] == "unavailable"
    assert saved["items"]["law:TEST:41"]["status"] == "failed"
    assert result["completeness"] == {**result["completeness"], "failed_items": 1, "unavailable_items": 1, "saved_items": 1}
    index = (tmp_path / "bundle" / "INDEX.md").read_text(encoding="utf-8")
    assert "1 item(s) the service does not have" in index and "1 item(s) failed to download" in index
    assert isinstance(saved.get("requests"), (int, type(None)))


def test_discrepancies_flag_a_wrong_date_and_a_docket_that_names_another_ruling():
    def cite(params):
        ref = params["reference"]
        if ref == "4A_714/2014":
            return {"exists": True, "decision_id": "bger_4A_714_2014", "citation_string_de": "BGer 4A_714/2014 vom 22. Mai 2015"}
        if ref == "BGE 134 III 354":
            return {"exists": True, "decision_id": "bge_BGE_134_III_354", "citation_string_de": "BGE 134 III 354"}
        if ref == "4A_45/2008":
            return {"exists": True, "decision_id": "bger_4A_45_2008", "citation_string_de": "BGer 4A_45/2008 vom 23. April 2008"}
        if ref == "4A_47/2008":
            return {"exists": True, "decision_id": "bger_4A_47_2008", "citation_string_de": "BGer 4A_47/2008 vom 29. April 2008"}
        return {"exists": False, "close_matches": []}
    client = FakeClient({"/api/cite": cite,
                         "/api/decisions/bger_4A_714_2014": {"decision_id": "bger_4A_714_2014", "docket_number": "4A_714/2014", "court": "bger", "decision_date": "2015-05-22"},
                         "/api/decisions/bge_BGE_134_III_354": {"decision_id": "bge_BGE_134_III_354", "citation_string_de": "BGE 134 III 354", "decision_date": "2008-04-29"},
                         "/api/decisions/bger_4A_45_2008": {"decision_id": "bger_4A_45_2008", "decision_date": "2008-04-23"},
                         "/api/decisions/bger_4A_47_2008": {"decision_id": "bger_4A_47_2008", "decision_date": "2008-04-29"},
                         "/api/lookup": {"is_case_number": True, "exact": True, "results": [{"decision_id": "bger_4A_714_2014", "docket_number": "4A_714/2014", "court": "bger"}]}})
    result, code = workflows.run(resolution_args("BGer 4A_714/2014 vom 22. Mai 2016", "BGE 134 III 354 (4A_45/2008)", "BGE 134 III 354 (4A_47/2008)"), client)
    rows = result["results"]
    assert code == 4 and rows[0]["status"] == "discrepancy" and rows[0]["decision_id"] == "bger_4A_714_2014"
    assert rows[0]["discrepancies"] == [{"kind": "date", "written": "2016-05-22", "decision": "2015-05-22"}]
    assert rows[1]["status"] == "discrepancy" and rows[1]["discrepancies"][0]["kind"] == "docket" and rows[1]["discrepancies"][0]["resolves_to"] == "bger_4A_45_2008"
    assert rows[2]["status"] == "resolved" and rows[2]["related_docket"]["decision_id"] == "bger_4A_47_2008"
    assert result["counts"] == {"discrepancy": 2, "resolved": 1}


def test_service_strings_among_close_matches_resolve_and_fragments_do_not():
    own = "Obergericht ZH NG190020 vom 30. November 2020"
    def cite(params):
        if params["reference"] == "zh_obergericht_NG190020":
            return {"exists": True, "decision_id": "zh_obergericht_NG190020", "citation_string_de": own}
        return {"exists": False, "close_matches": [{"decision_id": "zh_obergericht_NG190020", "citation_string_de": own, "docket_number": "NG190020"},
                                                   {"decision_id": "zh_obergericht_NG190021", "citation_string_de": "Obergericht ZH NG190021 vom 1. Dezember 2020"}]}
    client = FakeClient({"/api/cite": cite,
                         "/api/decisions/zh_obergericht_NG190020": {"decision_id": "zh_obergericht_NG190020", "docket_number": "NG190020", "court": "zh_obergericht", "canton": "ZH", "citation_string_de": own}})
    row = workflows.run(resolution_args("Obergericht ZH, NG190020, 30.11.2020"), client)[0]["results"][0]
    assert row["status"] == "resolved" and row["matched_via"] == "close_match_label" and row["identity_check"]["method"] == "exact_server_docket"
    # a fragment that the service matches by substring is proposed, not adopted
    client = FakeClient({"/api/cite": {"exists": True, "decision_id": "bvger_D-1100_2015", "citation_string_de": "BVGer D-1100/2015 vom 7. November 2018"},
                         "/api/decisions/bvger_D-1100_2015": {"decision_id": "bvger_D-1100_2015", "docket_number": "D-1100/2015", "court": "bvger"},
                         "/api/lookup": {"is_case_number": False, "exact": True, "results": []}})
    row = workflows.run(resolution_args("100/2015"), client)[0]["results"][0]
    assert row["status"] == "unrecognized" and "decision_id" not in row and row["service_candidate"]["decision_id"] == "bvger_D-1100_2015"


def test_pinpoint_rows_carry_input_keys_and_fail_only_themselves(tmp_path):
    path = tmp_path / "refs.jsonl"
    path.write_text('{"reference": "server citation", "pinpoint": "consid. 2.3", "id": "row-7"}\n'
                    '{"reference": "server citation", "pinpoint": "nonsense"}\n'
                    '{"reference": "server citation E. 2a"}\n', encoding="utf-8")
    client = FakeClient({"/api/erwaegung/test_case/2a": {"error": "E. '2a' not found", "available_e_numbers": ["1", "2"]},
                         "/api/erwaegung/test_case/2": {"decision_id": "test_case", "e_number": "2", "text": "a) [BGE 1 I 1](https://x) text"}})
    result, code = workflows.run(resolution_args("--input", str(path)), client)
    rows = result["results"]
    assert code == 4
    assert rows[0]["status"] == "resolved" and rows[0]["pinpoint"] == "2.3" and rows[0]["pinpoint_source"] == "input" and rows[0]["input"] == {"id": "row-7"}
    assert rows[1]["status"] == "error" and rows[1]["error"]["status"] == 400 and "nonsense" in rows[1]["error"]["message"]
    assert rows[2]["status"] == "pinpoint_unavailable" and rows[2]["pinpoint_status"] == "parent_retrieved" and rows[2]["pinpoint_source"] == "reference"
    assert rows[2]["passage"]["e_number"] == "2" and rows[2]["passage"]["text_plain"] == "a) BGE 1 I 1 text"
    assert "E. 2 was retrieved" in rows[2]["pinpoint_note"]


def test_resume_retries_added_decisions_and_skips_unavailable_items(tmp_path):
    args = bundle_args(tmp_path, "--passage", "2.3")
    client = FakeClient({"/api/erwaegung/test_case/2.3": {"error": "not indexed"}})
    workflows.run(args, client)
    add = build_parser().parse_args(["bundle", "add", str(tmp_path / "bundle"), "added_case"])
    workflows.run(add, FakeClient({"/api/decisions/added_case": APIError(None, "Request failed: reset"),
                                   "/api/erwaegung/added_case/2.3": APIError(404, "Not Found")}))
    saved = manifest(tmp_path)
    assert saved["items"]["decision:added_case"]["status"] == "failed" and saved["items"]["passage:added_case:2.3"]["status"] == "unavailable"
    assert saved["completeness"] == {**saved["completeness"], "failed_items": 1, "unavailable_items": 2, "added_decisions": 1}
    index = (tmp_path / "bundle" / "INDEX.md").read_text(encoding="utf-8")
    assert "2 item(s) the service does not have" in index and "1 item(s) failed to download" in index
    resume = bundle_args(tmp_path, "--passage", "2.3", "--resume")
    client = FakeClient({"/api/erwaegung/test_case/2.3": {"error": "not indexed"}})
    workflows.run(resume, client)
    fetched = [path for path, _ in client.calls]
    assert "/api/decisions/added_case" in fetched and "/api/erwaegung/test_case/2.3" not in fetched and "/api/erwaegung/added_case/2.3" not in fetched
    assert manifest(tmp_path)["items"]["decision:added_case"]["status"] == "saved"
