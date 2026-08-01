"""Citation-integrity anomaly checks (quality/checks/citation_anomalies.py).

The load-bearing property is the classifier: it must flag only citations
whose nonexistence is PROVABLE from the BGE series, and must not flag
coverage gaps — the 2026-07-31 recon showed 600k unresolved cantonal
self-citations (WBE_*) that are absences from the corpus, not anomalies in
the decisions. Two hard-won invariants from the same day's verification
pass (each produced a real-world false positive before):

  1. The series index must parse ALL THREE docket formats ("142 II 590",
     "1_I_26", "BGE 136 II 120") and match divisions case-insensitively
     ("120 IA 1" docket vs "BGE 120 Ia 1" citation) — the spaced-only
     pattern dropped 70% of series rows.
  2. Volumes are continuously paginated, so any mid-volume page is a
     plausible deep pin-cite into the case that starts before it
     ("BGE 141 V 312" = 31 pages into BGE 141 V 281). Only pages beyond
     the last case start + window are provably outside the series.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from quality.checks import citation_anomalies as ca  # noqa: E402
from quality.types import Severity  # noqa: E402


# ── classifier unit tests against a synthetic series index ────────────────

IDX = {
    (142, "II"): [540, 560, 590],       # dense tail: adaptive window = floor 30
    (127, "I"): [1, 200, 450],
    (96, "V"): [1, 60],
}
MAX_VOL = 152


def test_plausible_pincite_is_not_flagged():
    # 612 is within 30 pages of the case starting at 590
    assert ca._classify_bge("BGE 142 II 612", IDX, MAX_VOL) is None


def test_page_beyond_series_is_flagged():
    r = ca._classify_bge("BGE 142 II 650", IDX, MAX_VOL)
    assert r and r.startswith("page_beyond_series")


def test_mid_volume_page_is_a_plausible_deep_pincite():
    # 400 sits 200 pages after the case at 200 — under continuous
    # pagination it lies WITHIN that case, however long the gap
    assert ca._classify_bge("BGE 127 I 400", IDX, MAX_VOL) is None


def test_division_letters_match_case_insensitively():
    # docket side stores "120 IA 1"; citations write "BGE 120 Ia …"
    idx = {(120, "IA"): [1, 50]}
    assert ca._classify_bge("BGE 120 Ia 45", idx, MAX_VOL) is None
    r = ca._classify_bge("BGE 120 Ia 500", idx, MAX_VOL)
    assert r and r.startswith("page_beyond_series")


def test_series_regex_accepts_all_three_docket_formats():
    for docket in ("142 II 590", "1_I_26", "BGE 136 II 120", "120 IA 1"):
        assert ca._BGE_SERIES.match(docket), docket


def test_next_year_volume_gets_grace():
    # a court may cite the new year's volume before we ingest any of it
    assert ca._classify_bge("BGE 153 II 5", IDX, MAX_VOL) is None
    r = ca._classify_bge("BGE 154 II 5", IDX, MAX_VOL)
    assert r and r.startswith("volume_out_of_range")


def test_open_volumes_are_exempt_from_page_and_division_proofs():
    # the newest two volumes are still filling — nothing about their pages
    # or divisions is provable
    idx = {(152, "II"): [1, 40], (151, "V"): [1]}
    assert ca._classify_bge("BGE 152 II 900", idx, MAX_VOL) is None
    assert ca._classify_bge("BGE 151 V 900", idx, MAX_VOL) is None
    assert ca._classify_bge("BGE 151 III 10", idx, MAX_VOL) is None


def test_i_family_is_evaluated_as_a_union():
    # I vs Ia/Ib casefolds both ways in extraction; a family mismatch
    # proves nothing — only a page beyond the family's union does
    idx = {(100, "IA"): [1, 50], (100, "IB"): [1, 80]}
    assert ca._classify_bge("BGE 100 I 60", idx, MAX_VOL) is None
    r = ca._classify_bge("BGE 100 I 300", idx, MAX_VOL)
    assert r and r.startswith("page_beyond_series")


def test_last_case_window_adapts_to_the_volumes_longest_case():
    idx = {(143, "II"): [244, 297, 350]}    # contains a 53-page case
    assert ca._classify_bge("BGE 143 II 400", idx, MAX_VOL) is None
    r = ca._classify_bge("BGE 143 II 404", idx, MAX_VOL)
    assert r and r.startswith("page_beyond_series")


def test_future_volume_is_flagged():
    r = ca._classify_bge("BGE 199 II 5", IDX, MAX_VOL)
    assert r and r.startswith("volume_out_of_range")


def test_era_impossible_division_is_flagged():
    # division V exists only from vol 96 in the fixture; (29, V) is absent
    r = ca._classify_bge("BGE 29 V 177", IDX, MAX_VOL)
    assert r and r.startswith("division_absent_for_volume")


def test_year_as_page_carries_the_ocr_tag():
    r = ca._classify_bge("BGE 127 I 2002", IDX, MAX_VOL)
    assert r and "page_looks_like_year" in r


def test_malformed_token_is_skipped_not_flagged():
    assert ca._classify_bge("BGE irgendwas", IDX, MAX_VOL) is None


# ── end-to-end against a fixture graph ────────────────────────────────────

@pytest.fixture()
def rg(tmp_path, monkeypatch):
    p = tmp_path / "reference_graph.db"
    c = sqlite3.connect(p)
    c.executescript("""
        CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, docket_number TEXT,
            docket_norm TEXT, court TEXT, canton TEXT, language TEXT,
            decision_date TEXT);
        CREATE TABLE decision_citations (source_decision_id TEXT, target_ref TEXT,
            target_type TEXT, mention_count INT, is_prior_instance INT);
        CREATE TABLE citation_targets (source_decision_id TEXT, target_ref TEXT,
            target_decision_id TEXT, match_type TEXT, confidence_score REAL);
    """)
    # the BGE series (complete, tiny) — deliberately in all three docket
    # formats so the index build proves format coverage end-to-end
    c.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)", [
        ("bge_142 II 540", "142 II 540", "142 II 540", "bge", "CH", "de", "2016-01-01"),
        ("bge_142_II_560", "142_II_560", "142_II_560", "bge", "CH", "de", "2016-06-01"),
        ("bge_BGE_142_II_590", "BGE 142 II 590", "BGE 142 II 590", "bge", "CH", "de", "2016-09-01"),
        # closed-volume anchor: keeps 142 outside the open-volume exemption
        ("bge_150 II 1", "150 II 1", "150 II 1", "bge", "CH", "de", "2024-01-01"),
        # recent citing decisions
        ("bger_1C_1_2026", "1C_1/2026", "1C_1/2026", "bger", "CH", "de", "2026-07-01"),
        ("zh_x_1", "AB.2026.1", "AB.2026.1", "zh_obergericht", "ZH", "de", "2026-07-02"),
        # an old decision outside the window
        ("bger_old", "1C_9/2010", "1C_9/2010", "bger", "CH", "de", "2010-01-01"),
    ])
    c.executemany("INSERT INTO decision_citations VALUES (?,?,?,1,0)", [
        # provably nonexistent BGE (page beyond series) — in window
        ("bger_1C_1_2026", "BGE 142 II 650", "bge"),
        # plausible pin-cite (590+22) — must NOT flag
        ("bger_1C_1_2026", "BGE 142 II 612", "bge"),
        # anachronism: 2026 decision citing a 2028 docket
        ("zh_x_1", "5A_10/2028", "docket"),
        # cantonal self-cite, unresolved — must NOT count as federal-shaped
        ("zh_x_1", "WBE_2015_477", "docket"),
        # unresolved recent federal docket — informational only
        ("zh_x_1", "4A_999/2024", "docket"),
        # nonexistent BGE cited by an OLD decision — the BGE class is
        # corpus-wide (backfills are newly scraped even when decided long
        # ago), so this one COUNTS
        ("bger_old", "BGE 142 II 650", "bge"),
    ])
    # resolve the plausible pin-cite so only the truly unresolved remain
    c.execute("INSERT INTO citation_targets VALUES (?,?,?,?,?)",
              ("bger_1C_1_2026", "BGE 142 II 612", "bge_142 II 590",
               "bge_pincite", 0.85))
    c.commit()
    c.close()
    monkeypatch.setenv("SWISS_CASELAW_REFERENCE_GRAPH", str(p))
    monkeypatch.setenv("OCL_CITATION_ANOMALY_REPORT",
                       str(tmp_path / "report.json"))
    monkeypatch.setenv("OCL_CITATION_ANOMALY_WINDOW_DAYS", "365")
    return tmp_path


def test_nonexistent_bge_windowed_alert_with_corpus_metric(rg):
    """Alerting metric = recent window (outreach precision); corpus-wide
    total rides along as a QA metric (historical hits are mostly OCR noise
    in OUR scans of 19th-century texts, not court errors)."""
    res = ca.check_nonexistent_bge_citations(None)
    assert res.metric_value == 1, res.message      # only the recent decision
    assert "corpus-wide total 2" in res.message    # backfill case counted here
    assert res.passed
    import json
    rep = json.loads((rg / "report.json").read_text())
    assert rep["nonexistent_bge_total"] == 1
    assert rep["nonexistent_bge_corpus_total"] == 2
    assert rep["nonexistent_bge_corpus_by_reason"] == {"page_beyond_series": 2}
    hit = rep["nonexistent_bge"][0]
    assert hit["decision_id"] == "bger_1C_1_2026"
    assert hit["token"] == "BGE 142 II 650"
    assert hit["reason"].startswith("page_beyond_series")


def test_anachronistic_citation_found(rg):
    res = ca.check_anachronistic_citations(None)
    assert res.metric_value == 1, res.message


def test_unresolved_recent_bger_counts_only_federal_shapes(rg):
    res = ca.check_unresolved_recent_bger(None)
    # 4A_999/2024 counts; WBE_2015_477 (cantonal shape) and the resolved
    # pin-cite do not
    assert res.metric_value == 1, res.message
    assert res.severity == Severity.INFO


def test_absent_graph_skips_gracefully(monkeypatch, tmp_path):
    monkeypatch.setenv("SWISS_CASELAW_REFERENCE_GRAPH",
                       str(tmp_path / "nope.db"))
    res = ca.check_nonexistent_bge_citations(None)
    assert res.passed and res.metric_value == 0


def test_module_is_never_critical():
    assert ca.MODULE_NEVER_CRITICAL is True
