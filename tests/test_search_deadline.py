"""Search soft-deadline: when a query's elapsed time crosses the budget, skip the
expensive augmentation (cross-encoder + Haiku LLM-rerank) and stop launching more
FTS strategies, degrading to fast BM25/RRF results instead of a >50s hard timeout
that breaks token-limited connectors. The deadline only fires for pathological/
contended queries (default budget is generous), so normal searches are unaffected."""
import time

import mcp_server


def test_past_deadline():
    assert mcp_server._past_deadline(None) is False
    assert mcp_server._past_deadline(time.monotonic() - 1.0) is True
    assert mcp_server._past_deadline(time.monotonic() + 100.0) is False


def test_cross_encoder_skipped_when_past_deadline(monkeypatch):
    calls = []
    monkeypatch.setattr(mcp_server, "CROSS_ENCODER_ENABLED", True)
    monkeypatch.setattr(mcp_server, "_get_cross_encoder", lambda: (calls.append(1), None)[1])
    scored = [(1.0, 0.5, 0, {"decision_id": "x", "full_text": "t", "regeste": "r"})]
    # past deadline -> early return, cross-encoder NEVER consulted
    out = mcp_server._apply_cross_encoder_boosts(scored, "q", deadline=time.monotonic() - 1.0)
    assert out == scored
    assert calls == [], "cross-encoder consulted despite a passed deadline"
    # no deadline -> cross-encoder IS consulted (control, proves the gate is what skips it)
    mcp_server._apply_cross_encoder_boosts(scored, "q", deadline=None)
    assert calls == [1], "cross-encoder not consulted without a deadline"


def test_llm_rerank_skipped_when_past_deadline():
    scored = [(1.0, 0.5, 0, {"decision_id": "x"})]
    out = mcp_server._apply_llm_rerank(scored, "q", deadline=time.monotonic() - 1.0)
    assert out == scored  # early return, no Haiku call
