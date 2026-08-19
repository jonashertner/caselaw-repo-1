"""The rerank paths must emit trainable labels, not just impact counters.

We pay Haiku to order candidates over real queries; until 2026-08-19 the
ordering was discarded (only pre/post_top survived) and the cross-encoder
path traced nothing at all. These records are the raw material for
training a reranker and for CE-vs-Haiku agreement metrics.

Privacy shape is asserted here as strictly as the content: the Haiku
record carries the query[:200] the /datenschutz/ page already discloses,
the CE record carries only query_len, and neither may ever carry an IP,
user id, session id or cohort hash. Offline — no model, no network.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

_FORBIDDEN_KEYS = {"ip", "client_ip", "remote_addr", "user_id", "session_id",
                   "sid", "ua", "user_agent", "cohort", "install_cohort"}


def _capture(monkeypatch):
    seen: list[dict] = []
    monkeypatch.setattr(m, "_log_search_trace", lambda rec: seen.append(rec))
    return seen


def test_cross_encoder_trace_carries_scores_by_id(monkeypatch):
    seen = _capture(monkeypatch)

    class FakeEncoder:
        def predict(self, pairs):
            return [0.9, 0.1][: len(pairs)]

    rows = [
        (2.0, -1.0, 0, {"decision_id": "bge_1", "title": "t", "regeste": "r",
                        "snippet": ""}),
        (1.0, -0.5, 1, {"decision_id": "bge_2", "title": "t", "regeste": "r",
                        "snippet": ""}),
    ]
    monkeypatch.setattr(m, "_get_cross_encoder", lambda: FakeEncoder())
    monkeypatch.setattr(m, "CROSS_ENCODER_ENABLED", True)
    monkeypatch.setattr(m, "CROSS_ENCODER_TOP_N", 2)
    m._apply_cross_encoder_boosts(rows, "Tierhalterhaftung")

    ce = [r for r in seen if r.get("type") == "cross_encoder"]
    assert ce, "the CE path must trace its scores now"
    rec = ce[0]
    assert rec["scores_by_id"] == {"bge_1": 0.9, "bge_2": 0.1}
    # No query text on this record — it adds none of its own.
    assert "query" not in rec
    assert rec["query_len"] == len("Tierhalterhaftung")


def test_ce_trace_keeps_raw_scores_not_normalised(monkeypatch):
    """Normalisation maps any spread onto 0..1, erasing the difference
    between a confident board and a flat one. The trace must keep what
    the model actually said."""
    seen = _capture(monkeypatch)

    class FlatEncoder:
        def predict(self, pairs):
            return [0.42, 0.40][: len(pairs)]

    rows = [
        (2.0, -1.0, 0, {"decision_id": "a", "title": "", "regeste": "",
                        "snippet": ""}),
        (1.0, -0.5, 1, {"decision_id": "b", "title": "", "regeste": "",
                        "snippet": ""}),
    ]
    monkeypatch.setattr(m, "_get_cross_encoder", lambda: FlatEncoder())
    monkeypatch.setattr(m, "CROSS_ENCODER_ENABLED", True)
    monkeypatch.setattr(m, "CROSS_ENCODER_TOP_N", 2)
    m._apply_cross_encoder_boosts(rows, "q")
    rec = [r for r in seen if r.get("type") == "cross_encoder"][0]
    assert rec["scores_by_id"]["a"] == 0.42, "raw score, not normalised 1.0"


def test_ce_failure_still_traces_nothing_and_returns_input(monkeypatch):
    seen = _capture(monkeypatch)

    class BrokenEncoder:
        def predict(self, pairs):
            raise RuntimeError("model on fire")

    rows = [(1.0, -0.5, 0, {"decision_id": "a", "title": "", "regeste": "",
                            "snippet": ""})]
    monkeypatch.setattr(m, "_get_cross_encoder", lambda: BrokenEncoder())
    monkeypatch.setattr(m, "CROSS_ENCODER_ENABLED", True)
    out = m._apply_cross_encoder_boosts(rows, "q")
    assert out == rows
    assert not [r for r in seen if r.get("type") == "cross_encoder"]


def test_no_trace_record_may_carry_identifiers(monkeypatch):
    """The structural privacy property, asserted at the emitter."""
    seen = _capture(monkeypatch)

    class FakeEncoder:
        def predict(self, pairs):
            return [0.5] * len(pairs)

    rows = [(1.0, -0.5, 0, {"decision_id": "a", "title": "", "regeste": "",
                            "snippet": ""})]
    monkeypatch.setattr(m, "_get_cross_encoder", lambda: FakeEncoder())
    monkeypatch.setattr(m, "CROSS_ENCODER_ENABLED", True)
    m._apply_cross_encoder_boosts(rows, "q")
    for rec in seen:
        leaked = _FORBIDDEN_KEYS & set(rec)
        assert not leaked, f"identifier keys in trace: {leaked}"


def test_rerank_trace_shape_is_pinned_in_source():
    """The Haiku call needs network, so pin the record shape statically:
    the label fields must be present in the emitting code, and the
    record must not grow identifier fields."""
    import ast, inspect
    src = inspect.getsource(m._apply_llm_rerank)
    tree = ast.parse(src)
    dicts = [n for n in ast.walk(tree) if isinstance(n, ast.Dict)]
    keysets = [
        {k.value for k in d.keys if isinstance(k, ast.Constant)}
        for d in dicts
    ]
    trace = next((ks for ks in keysets if "candidate_ids" in ks), None)
    assert trace is not None, "rerank trace must carry candidate_ids"
    assert "llm_order" in trace, "rerank trace must carry Haiku's ordering"
    assert not (_FORBIDDEN_KEYS & trace), "no identifiers in the rerank trace"


def test_llm_terms_keeps_only_added_terms():
    """Expansion echoes the query's own tokens; the type-A trace stores
    query_len precisely so no query text rides along. A term whose every
    token already appears in the query is an echo and must be dropped —
    the ADDED terms are both the privacy-safe subset and the part a
    parser actually has to learn."""
    import re
    query = "Missbräuchliche Kündigung Arbeitsvertrag"
    llm_terms = ["Missbräuchliche Kündigung",          # pure echo -> out
                 "Art. 336 OR",                        # added -> kept
                 "Kündigungsschutz",                   # added -> kept
                 "kündigung arbeitsvertrag"]           # echo, case -> out
    qtoks = {t.lower() for t in re.findall(r"\w+", query)}
    kept = [t for t in llm_terms
            if not {w.lower() for w in re.findall(r"\w+", t)} <= qtoks]
    assert kept == ["Art. 336 OR", "Kündigungsschutz"]
