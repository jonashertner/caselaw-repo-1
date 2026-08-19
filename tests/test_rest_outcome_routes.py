"""The six REST tools must declare their outcomes, not have them guessed.

`_mark_outcome` gave routes a way to say "this answered nothing"; this
file checks the routes actually use it, and that the classifier behind
it never labels a payload it cannot read.

Route registration is the part that can only fail at startup: with
`from __future__ import annotations` in force, FastAPI resolves the
`response: Response` injection through get_type_hints() against module
globals, so a missing module-level import is an eight-worker boot
failure and nothing importable would catch it. The app is built inside
main_remote(), which ends in uvicorn.run() — so we monkeypatch that,
capture the ASGI app, and exercise it offline.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


# ── the classifier ───────────────────────────────────────────────────

def test_declines_on_unknown_shape():
    """Silence beats a wrong label: an unrecognised payload must fall
    through to the status heuristic, which is counted as assumed."""
    assert m._payload_outcome(None) is None
    assert m._payload_outcome("text") is None
    assert m._payload_outcome({"note": "hello"}) is None


def test_empty_and_populated_lists():
    assert m._payload_outcome({"results": []}) == ("empty", None)
    assert m._payload_outcome({"results": [1]}) == ("substantive", None)
    # Any populated content key answers, even when a sibling is empty:
    # get_materialien returns sources: [] with botschaft_documents filled.
    assert m._payload_outcome(
        {"sources": [], "botschaft_documents": [{"x": 1}],
         "amendment_refs": []}) == ("substantive", None)


def test_total_without_a_list():
    assert m._payload_outcome({"total": 0}) == ("empty", None)
    assert m._payload_outcome({"total": 7}) == ("substantive", None)


def test_error_payloads_are_empty_answers_with_distinct_reasons():
    """A 200 carrying {"error": …} answered nothing. The three kinds are
    different findings and must not share a counter."""
    cases = {
        "Materialien database not available.": "corpus_not_built",
        "Statutes database not available. Deploy statutes.db to enable "
        "statute lookup.": "corpus_not_built",
        "Database error: no such table": "backend_error",
        "Materialien lookup failed: disk I/O error": "backend_error",
        "Provide sr_number or abbreviation.": "bad_request",
        "At least one of 'query' or 'law_code' is required.": "bad_request",
        "Reference graph not available.": "corpus_not_built",
        "No law found with abbreviation 'XYZ'.": "id_not_found",
        "Publication not found: abc": "id_not_found",
        # A version that does not exist is not a database that was never
        # deployed, however similarly the two are worded.
        "Historical version not available for SR 210 as of 2020-01-01.":
            "id_not_found",
    }
    for err, reason in cases.items():
        assert m._payload_outcome({"error": err}) == ("empty", reason), err


def test_blank_error_is_not_an_error():
    assert m._payload_outcome({"error": "", "results": [1]}) == (
        "substantive", None)


class _Resp:
    def __init__(self):
        self.headers: dict = {}


def test_declare_returns_the_payload_unchanged():
    payload = {"results": [1], "total": 1}
    r = _Resp()
    assert m._declare_outcome(r, payload) is payload
    assert r.headers["X-OCL-Outcome"] == "substantive"
    assert "X-OCL-Empty-Reason" not in r.headers


def test_caller_names_the_miss_only_when_the_payload_cannot():
    r = _Resp()
    m._declare_outcome(r, {"results": []}, "no_fts_match")
    assert r.headers["X-OCL-Empty-Reason"] == "no_fts_match"

    r2 = _Resp()
    m._declare_outcome(r2, {"error": "Statutes database not available."},
                       "no_fts_match")
    assert r2.headers["X-OCL-Empty-Reason"] == "corpus_not_built"


def test_declare_is_silent_on_an_unknown_shape():
    r = _Resp()
    m._declare_outcome(r, {"note": "?"}, "no_fts_match")
    assert r.headers == {}


def test_declare_never_raises():
    class Hostile:
        @property
        def headers(self):
            raise RuntimeError("nope")
    payload = {"results": []}
    assert m._declare_outcome(Hostile(), payload, "no_fts_match") is payload


# ── route wiring ─────────────────────────────────────────────────────

# Every route that can answer nothing with a 200, and the metrics tool
# each is charged to (see _classify_rest_metric: search and lookup share
# one tool name, so a bucket is only honest when all of its routes are
# labelled).
_LABELLED_ROUTES = {
    "api_search_decisions": "search_decisions",
    "api_find_leading_cases": "leading-cases",
    "api_search_laws": "laws",
    "api_get_law": "laws",
    "api_search_commentaries": "commentaries",
    "api_get_commentary": "commentaries",
    "api_search_scholarship": "scholarship",
    "api_get_scholarship": "scholarship",
    # Same bucket: _classify_rest_metric charges every /scholarship/*
    # path to one tool, so one unlabelled route keeps the whole bucket's
    # provenance mixed no matter what the search route says.
    "api_scholarship_cited_by_statute": "scholarship",
    "api_scholarship_cited_by_decision": "scholarship",
    "api_list_scholarship_sources": "scholarship",
    "api_scholarship_licenses": "scholarship",
    "api_scholarship_citation_stats": "scholarship",
    "api_get_materialien": "materialien",
    "api_search_materialien": "materialien",
}


def _handler_nodes():
    tree = ast.parse((REPO / "mcp_server.py").read_text())
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in _LABELLED_ROUTES:
            found[node.name] = node
    return found


def test_every_labelled_route_exists_and_takes_a_response():
    nodes = _handler_nodes()
    missing = sorted(set(_LABELLED_ROUTES) - set(nodes))
    assert not missing, f"handlers not found: {missing}"
    for name, node in sorted(nodes.items()):
        args = [a.arg for a in node.args.args]
        assert "response" in args, f"{name} cannot declare an outcome"
        ann = next(a.annotation for a in node.args.args if a.arg == "response")
        assert getattr(ann, "id", None) == "Response", name


def test_every_labelled_route_declares_its_outcome():
    """Either through the classifier or, where the payload's own shape
    cannot express the distinction, by marking the header directly."""
    for name, node in sorted(_handler_nodes().items()):
        calls = [n for n in ast.walk(node)
                 if isinstance(n, ast.Call)
                 and getattr(n.func, "id", "") in ("_declare_outcome",
                                                   "_mark_outcome")]
        assert calls, f"{name} never declares its outcome"


def test_response_is_a_module_global():
    """The precondition FastAPI needs to resolve the annotation. A
    function-local import would not be visible to get_type_hints()."""
    from starlette.responses import Response as StarletteResponse
    assert getattr(m, "Response", None) is StarletteResponse


# ── the app actually builds, and labels a live request ───────────────

@pytest.fixture(scope="module")
def rest_app():
    """Build the real ASGI app without serving it.

    main_remote() imports uvicorn itself and ends in uvicorn.run(); we
    patch run() on the module, capture the app and return. This is the
    only offline way to execute the route decorators, which is where a
    bad annotation would fail — and it would fail on all eight workers.
    """
    import uvicorn

    captured = {}

    def _fake_run(app, **kwargs):
        captured["app"] = app

    real_run = uvicorn.run
    uvicorn.run = _fake_run
    try:
        m.main_remote("127.0.0.1", 0)
    finally:
        uvicorn.run = real_run
    assert "app" in captured, "main_remote did not reach uvicorn.run"
    return captured["app"]


def test_the_app_builds_with_the_new_annotations(rest_app):
    """Route registration is where `response: Response` would explode."""
    assert rest_app is not None


def _client(app):
    from starlette.testclient import TestClient
    # No context manager: lifespan startup is not needed to route a
    # request and would open production databases.
    return TestClient(app)


# A law code no corpus can hold. With a materialien DB the payload is
# {"error": "No Materialien found for ZZZQQ…"}; without one it is
# {"error": "Materialien database not available."}. Both are 200s that
# answer nothing — which is exactly the blind spot this instrument
# exists to close — so the assertion holds with or without a corpus.
_MISS = "/api/materialien/ZZZQQ"


def test_an_empty_200_is_labelled_empty(rest_app):
    """The whole point: a 200 carrying no answer must say so."""
    r = _client(rest_app).get(_MISS)
    assert r.status_code == 200, r.text
    assert r.headers.get("X-OCL-Outcome") == "empty"
    assert r.headers.get("X-OCL-Empty-Reason") in (
        "id_not_found", "corpus_not_built")


def test_the_middleware_records_the_label_as_declared(rest_app):
    """End to end: route header → middleware → counters. `declared`
    rises and `status` does not, so the provenance field can be trusted
    to separate measured from assumed."""
    before_src = dict(m._metrics["outcome_sources"]["materialien"])
    before_out = dict(m._metrics["tool_outcomes"]["materialien"])
    _client(rest_app).get(_MISS)
    src = m._metrics["outcome_sources"]["materialien"]
    assert src["declared"] == before_src.get("declared", 0) + 1
    assert src.get("status", 0) == before_src.get("status", 0)
    assert (m._metrics["tool_outcomes"]["materialien"]["empty"]
            == before_out.get("empty", 0) + 1)
    assert sum(m._metrics["empty_reasons"]["materialien"].values()) >= 1


@pytest.mark.skipif(not m.DB_PATH.exists(), reason="no local decisions.db")
def test_a_decision_search_declares_its_outcome(rest_app):
    r = _client(rest_app).get(
        "/api/decisions", params={"q": "zzqqxx-no-such-term", "limit": 1})
    assert r.status_code == 200, r.text
    assert r.headers.get("X-OCL-Outcome") in ("empty", "substantive")
    if r.headers["X-OCL-Outcome"] == "empty":
        assert r.headers["X-OCL-Empty-Reason"] == "no_fts_match"


def test_a_rejected_request_is_not_an_answer(rest_app):
    """422 goes through HTTPException, so the route never declares — the
    status fallback must still not call it answered."""
    before = dict(m._metrics["tool_outcomes"]["materialien"])
    r = _client(rest_app).get("/api/materialien")     # no query: 422
    assert r.status_code == 422, r.text
    assert (m._metrics["tool_outcomes"]["materialien"]["empty"]
            == before.get("empty", 0) + 1)
    assert m._metrics["empty_reasons"]["materialien"]["http_422"] >= 1


def test_a_miss_that_offers_alternatives_is_counted_separately():
    """A dead end and a miss carrying real candidates are different
    results for the user; one counter for both would hide whether the
    cantonal fallback helps anyone."""
    assert m._payload_outcome({"error": "No cantonal law found for ZH …"}) == (
        "empty", "id_not_found")
    assert m._payload_outcome({
        "error": "No cantonal law found for ZH …",
        "candidates": [{"canton": "ZH", "sr_number": "631.1"}],
    }) == ("empty", "id_not_found_with_candidates")
