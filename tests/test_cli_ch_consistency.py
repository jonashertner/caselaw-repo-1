"""Audit v4: cli:ch must render the same kind of citation consistently. The
corpus stores BGE dockets both with the 'BGE' prefix and without; both must
yield the same hyphenated canonical form (regression: '152.II.1' vs '131-III-12').
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import cli_ch  # noqa: E402


def test_bge_consistent_hyphenation_with_or_without_prefix():
    # both stored forms -> identical canonical hyphenated cli:ch
    a = cli_ch.mint_cli_ch_from_row({"decision_id": "bge_152 II 1", "court": "bge", "docket_number": "152 II 1"})
    b = cli_ch.mint_cli_ch_from_row({"decision_id": "bge_x", "court": "bge", "docket_number": "BGE 152 II 1"})
    assert a == b == "cli:ch:bge:152-II-1"
    c = cli_ch.mint_cli_ch_from_row({"decision_id": "bge_131 III 12", "court": "bge", "docket_number": "131 III 12"})
    assert c == "cli:ch:bge:131-III-12"
    # no more dotted divergence
    assert "." not in a and "." not in c


def test_bge_compact_handles_both_forms():
    assert cli_ch._bge_docket_compact("BGE 140 III 86") == "140-III-86"
    assert cli_ch._bge_docket_compact("140 III 86") == "140-III-86"
    assert cli_ch._bge_docket_compact("ATF 152 II 1") == "152-II-1"
    assert cli_ch._bge_docket_compact("not a bge") is None


def test_federal_docket_unchanged():
    # bger docket keeps its '/' canonical form
    out = cli_ch.mint_cli_ch_from_row({"decision_id": "bger_9C_113_2025", "court": "bger", "docket_number": "9C_113/2025"})
    assert out == "cli:ch:bger:9C_113/2025"
