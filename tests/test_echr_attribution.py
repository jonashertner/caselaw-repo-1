"""© ECHR-CEDH attribution on every surface that reproduces Strasbourg text.

The Court's reuse terms permit reproduction on three CUMULATIVE conditions:
source acknowledged as © ECHR-CEDH, information/education purpose, free of
charge. Condition 1 was unmet — verified 2026-07-29 against production: not
one served response, tool description or dataset card carried the notice,
for 2,870 ECtHR rows already in the corpus.

These tests are functional, not source-scans, deliberately: the first draft
of this change referenced an unbound `decision` variable in two handlers and
the whole suite still passed, because nothing exercised those success paths.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

ECTHR_COURTS = ["ecthr_chamber", "ecthr_committee", "ecthr_grand_chamber",
                "hudoc_ch", "bge_egmr"]


def test_every_ecthr_court_is_recognised():
    for c in ECTHR_COURTS:
        assert m._is_ecthr_court(c), c
    for c in ("bger", "bge", "bvger", "zh_obergericht", "ch_vb", None, ""):
        assert not m._is_ecthr_court(c), c


def test_attribution_text_carries_the_three_required_elements():
    a = m._ECHR_ATTRIBUTION
    assert "© ECHR-CEDH" in a                    # condition 1: the acknowledgement
    assert "echr.coe.int/copyright-and-disclaimer" in a   # the terms themselves
    assert "CC0" in a                            # our own dedication does not extend to it


def test_note_fires_only_for_ecthr_rows():
    assert "© ECHR-CEDH" in m._ecthr_attribution_note([{"court": "ecthr_chamber"}])
    assert "© ECHR-CEDH" in m._ecthr_attribution_note({"court": "bge_egmr"})
    # mixed result set: one Strasbourg row is enough to require the notice
    assert "© ECHR-CEDH" in m._ecthr_attribution_note(
        [{"court": "bger"}, {"court": "hudoc_ch"}])
    # Swiss-only responses stay byte-identical to before
    assert m._ecthr_attribution_note([{"court": "bger"}, {"court": "bge"}]) == ""
    assert m._ecthr_attribution_note([]) == ""
    assert m._ecthr_attribution_note(None) == ""


def test_note_never_raises_on_malformed_rows():
    for bad in ([{}], [None], ["not-a-dict"], [{"court": 42}]):
        m._ecthr_attribution_note(bad)  # must not raise


# ---- functional: the handlers that reproduce verbatim text ----------------
# These call the real handlers with monkeypatched data access, which is what
# catches an unbound-variable slip in a rarely-exercised branch.

def _fake_decision(court):
    return {
        "decision_id": "ecthr_chamber_30696_09", "court": court, "canton": "CE",
        "docket_number": "30696/09", "decision_date": "2011-01-21",
        "language": "fr", "title": "AFFAIRE M.S.S. c. BELGIQUE ET GRECE",
        "regeste": "Violation de l'art. 3 CEDH.",
        "full_text": "PROCEDURE ... EN DROIT ...",
    }


def test_get_regeste_main_db_path_attributes(monkeypatch):
    """The `if not row:` fallback branch — where `decision` IS bound."""
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "_fetch_structure_row", lambda x: None)
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _fake_decision("ecthr_chamber"))
    out = m._handle_get_regeste(decision_id="ecthr_chamber_30696_09")
    assert out.get("copyright") == m._ECHR_ATTRIBUTION


def test_get_regeste_sidecar_path_attributes(monkeypatch):
    """The structured-sidecar branch — this one references `row`, not
    `decision`; the first draft used `decision` here and would have raised."""
    row = {"decision_id": "ecthr_chamber_30696_09", "court": "ecthr_chamber",
           "decision_date": "2011-01-21", "language": "fr",
           "regeste": "Violation de l'art. 3 CEDH."}
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "_fetch_structure_row", lambda x: row)
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _fake_decision("ecthr_chamber"))
    out = m._handle_get_regeste(decision_id="ecthr_chamber_30696_09")
    assert out.get("copyright") == m._ECHR_ATTRIBUTION


def test_get_regeste_swiss_decision_carries_no_copyright(monkeypatch):
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "_fetch_structure_row", lambda x: None)
    monkeypatch.setattr(m, "get_decision_by_id",
                        lambda x: dict(_fake_decision("bger"), court="bger"))
    out = m._handle_get_regeste(decision_id="bger_x")
    assert "copyright" not in out


def test_deep_research_fetch_attributes_in_text_and_metadata(monkeypatch):
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "get_decision_by_id", lambda x: _fake_decision("hudoc_ch"))
    out = m._deep_research_fetch("hudoc_ch_30696_09")
    assert "© ECHR-CEDH" in out["text"]                    # the document itself
    assert out["metadata"].get("copyright") == m._ECHR_ATTRIBUTION  # machine-readable


def test_deep_research_fetch_swiss_unchanged(monkeypatch):
    monkeypatch.setattr(m, "_resolve_decision_id", lambda x: x)
    monkeypatch.setattr(m, "get_decision_by_id",
                        lambda x: dict(_fake_decision("bger"), court="bger"))
    out = m._deep_research_fetch("bger_x")
    assert "ECHR-CEDH" not in out["text"]
    assert "copyright" not in out["metadata"]
