"""ATF/DTF extraction parity (search_stack/reference_extraction.py).

Motivation (2026-08-06/07): the graph's extractor recognised only the
literal prefix "BGE", so French "ATF 143 III 666" and Italian
"DTF 139 II 404" survived solely via the bare-form docket pattern —
target_type='docket', prefix discarded. Measured on decisions since 2024:
345,146 bge-typed tokens from DE sources, 283 from FR, 580 from IT — a
thousandfold artefact of the prefix gap. It also blinded the
citation-anomaly scan to FR/IT (all 204 back-scan findings were German)
and denied FR/IT citations the pin-cite fallback stratum.

The normalised form stays "BGE vol DIV page" for all three prefixes: one
canonical key per target, whichever language cited it.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from search_stack.build_reference_graph import build_graph  # noqa: E402
from search_stack.reference_extraction import extract_case_citations  # noqa: E402


def _by_type(refs):
    out = {"bge": set(), "docket": set()}
    for r in refs:
        out[r.citation_type].add(r.normalized)
    return out


def test_atf_is_extracted_as_bge_typed_and_not_double_counted():
    got = _by_type(extract_case_citations(
        "Selon l'ATF 143 III 666 consid. 5.3.3, tel est le cas."))
    assert got["bge"] == {"BGE 143 III 666"}
    assert "143 III 666" not in got["docket"]


def test_dtf_is_extracted_as_bge_typed():
    got = _by_type(extract_case_citations(
        "Secondo la DTF 139 II 404 consid. 5, cio vale."))
    assert got["bge"] == {"BGE 139 II 404"}
    assert "139 II 404" not in got["docket"]


def test_bge_behaviour_unchanged():
    got = _by_type(extract_case_citations(
        "Gemäss BGE 147 I 268 E. 3 ist dies klar."))
    assert got["bge"] == {"BGE 147 I 268"}
    assert "147 I 268" not in got["docket"]


def test_prefix_matching_is_case_insensitive_like_bge_always_was():
    got = _by_type(extract_case_citations("voir atf 130 I 312 consid. 1"))
    assert got["bge"] == {"BGE 130 I 312"}


def test_continuation_citation_still_arrives_as_bare_docket():
    """'ATF 139 III 126 consid. 3; 116 II 783 consid. 2a' — the second
    element carries no prefix of its own. It stays on the bare-form docket
    path (where the anomaly scanner's bare=True handling covers it)."""
    got = _by_type(extract_case_citations(
        "cf. ATF 139 III 126 consid. 3; 116 II 783 consid. 2a."))
    assert got["bge"] == {"BGE 139 III 126"}
    assert "116 II 783" in got["docket"]


def test_mixed_language_text_yields_one_canonical_key():
    got = _by_type(extract_case_citations(
        "BGE 143 III 666 E. 4; vgl. ATF 143 III 666 consid. 4; "
        "DTF 143 III 666 consid. 4."))
    assert got["bge"] == {"BGE 143 III 666"}


def test_french_wrong_citation_is_extracted_prefixed():
    """The flagship-paper case: GE wrote 'ATF 199 V 13 consid. 2a'. Before
    the fix this token reached the graph bare (docket-typed) and the
    anomaly scan's date guard had to treat it cautiously; now it arrives
    bge-typed, carrying the court's own prefix assertion."""
    got = _by_type(extract_case_citations(
        "un intérêt digne de protection (ATF 199 V 13 consid. 2a)."))
    assert got["bge"] == {"BGE 199 V 13"}


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False)
                              for r in rows) + "\n", encoding="utf-8")


def test_atf_citation_resolves_through_the_bge_stratum(tmp_path: Path):
    """End-to-end mirror of test_bge_citations_resolved_to_bge_decisions,
    with a French citing decision: ATF must land in the bge_norm stratum,
    not docket_norm."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {"decision_id": "bge_147_I_268", "docket_number": "147 I 268",
         "court": "bge", "canton": "CH", "language": "de",
         "decision_date": "2021-01-01", "title": "", "regeste": "",
         "full_text": ""},
        {"decision_id": "d_fr", "docket_number": "6B_200/2022",
         "court": "bger", "canton": "CH", "language": "fr",
         "decision_date": "2022-06-01", "title": "", "regeste": "",
         "full_text": "Selon l'ATF 147 I 268 consid. 2, tel est le cas."},
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)
    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    cit = conn.execute(
        "SELECT target_ref, target_type FROM decision_citations "
        "WHERE source_decision_id='d_fr' AND target_type='bge'").fetchone()
    assert cit is not None and cit[0] == "BGE 147 I 268"
    link = conn.execute(
        "SELECT target_decision_id, match_type FROM citation_targets "
        "WHERE source_decision_id='d_fr' AND target_ref='BGE 147 I 268'"
    ).fetchone()
    conn.close()
    assert link is not None and link[0] == "bge_147_I_268"
    assert link[1] == "bge_norm"
