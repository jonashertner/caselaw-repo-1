"""Audit M-2: get_decision's Citations array must not list the decision itself.
A BGE excerpt regex-harvests its own caption docket (e.g. 9C_113/2025 for BGE
152 II 1) and its own BGE number into cited_decisions; those self-references are
dropped, real citations kept.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

BGE_152_HEAD = ("Urteilskopf 152 II 1 1. Auszug aus dem Urteil ... 9C_113/2025 "
                "vom 27. September 2025 Regeste ...")


def test_drops_caption_docket_and_bge_number():
    result = {
        "decision_id": "bge_152 II 1", "court": "bge", "docket_number": "152 II 1",
        "full_text": BGE_152_HEAD,
        "cited_decisions": json.dumps(["9C_113/2025", "BGE 152 II 1", "2C_597/2022", "BGE 124 III 1"]),
    }
    out = m._filter_self_citations(result["cited_decisions"], result)
    assert "9C_113/2025" not in out          # own caption docket dropped
    assert "BGE 152 II 1" not in out          # own BGE number dropped
    assert "2C_597/2022" in out and "BGE 124 III 1" in out   # real citations kept


def test_docket_decision_drops_only_itself():
    result = {
        "decision_id": "bger_9C_113_2025", "court": "bger", "docket_number": "9C_113/2025",
        "full_text": "... 9C_113/2025 vom 27. September 2025 ...",
        "cited_decisions": json.dumps(["9C_113/2025", "2C_597/2022"]),
    }
    out = m._filter_self_citations(result["cited_decisions"], result)
    assert out == ["2C_597/2022"]


def test_empty_after_filter_returns_none():
    result = {"decision_id": "bge_x", "court": "bge", "docket_number": "100 II 5",
              "full_text": "", "cited_decisions": json.dumps(["BGE 100 II 5"])}
    assert m._filter_self_citations(result["cited_decisions"], result) is None


def test_robust_to_bad_input():
    r = {"decision_id": "x", "docket_number": "", "full_text": ""}
    assert m._filter_self_citations(None, r) is None
    assert m._filter_self_citations("not json", r) is None
    assert m._filter_self_citations("[]", r) is None


def test_norm_cite_ref_strips_prefixes_and_separators():
    assert m._norm_cite_ref("BGE 152 II 1") == m._norm_cite_ref("152 II 1") == "152II1"
    assert m._norm_cite_ref("9C_113/2025") == m._norm_cite_ref("9C 113/2025") == "9C1132025"
    assert m._norm_cite_ref("ATF 124 III 1") == "124III1"
