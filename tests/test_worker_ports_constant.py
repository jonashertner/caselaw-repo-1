"""Aggregators cover the full worker pool (2026-07).

The pool grew 4 -> 8 on 2026-06-30 (66ad3f6); /metrics/all and
/metrics/sessions kept range(8770, 8774) and silently reported half the
fleet. WORKER_PORTS is the single source of truth, env-tunable via
MCP_WORKER_COUNT.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def test_default_matches_health_alerts_precedent():
    assert m.WORKER_PORTS == tuple(range(8770, 8778))


def test_no_hardcoded_half_pool_range_left():
    src = Path(REPO / "mcp_server.py").read_text(encoding="utf-8")
    # executable forms only — the WORKER_PORTS comment legitimately
    # documents the old defect
    assert "for port in range(8770, 8774):" not in src
    assert 'combined["workers"] = 4' not in src
