"""The round-trip benchmark's comparison logic (offline)."""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("citation_roundtrip", Path(__file__).resolve().parents[1] / "benchmarks" / "citation_roundtrip.py")
bench = importlib.util.module_from_spec(spec); spec.loader.exec_module(bench)


def test_regressions_are_lost_real_references_and_wrong_ones_that_resolve():
    expected = [
        {"reference": "BGE 136 III 513", "expected_status": "resolved", "expected_decision_id": "bge_BGE_136_III_513", "kind": "real"},
        {"reference": "BGE 999 III 1", "expected_status": "missing", "kind": "deliberately_wrong"},
        {"reference": "4A_191/2019", "expected_status": "ambiguous", "kind": "review_case"},
        {"reference": "BGE 140 III 86", "pinpoint": "2.3", "expected_status": "pinpoint_unavailable", "kind": "real"},
    ]
    rows = [
        {"reference": "BGE 136 III 513", "status": "resolved", "decision_id": "bge_BGE_136_III_513"},
        {"reference": "BGE 999 III 1", "status": "resolved", "decision_id": "bge_BGE_999_III_1"},
        {"reference": "4A_191/2019", "status": "resolved", "decision_id": "bger_4A_191_2019"},
        {"reference": "BGE 140 III 86", "pinpoint": "2.3", "status": "resolved", "decision_id": "bge_BGE_140_III_86"},
    ]
    report = bench.compare(expected, rows)
    assert report["counts"] == {"checked": 4, "match": 1, "mismatch": 3, "regressions": 1}
    problems = {m["reference"]: m["problem"] for m in report["mismatches"]}
    assert problems["BGE 999 III 1"] == "a wrong reference now resolves" and problems["4A_191/2019"] == "status changed"
    assert problems["BGE 140 III 86"] == "status changed"  # an improvement (the passage got indexed), not a regression
    lost = bench.compare([expected[0]], [{"reference": "BGE 136 III 513", "status": "missing"}])
    assert lost["regressions"] == ["BGE 136 III 513"] and lost["mismatches"][0]["problem"] == "a real reference is lost"
    moved = bench.compare([expected[0]], [{"reference": "BGE 136 III 513", "status": "resolved", "decision_id": "other"}])
    assert moved["mismatches"][0]["problem"] == "resolved to another decision" and moved["regressions"]


def test_the_reference_set_is_well_formed():
    rows = bench.load_set(bench.SET)
    assert len(rows) >= 150 and all({"reference", "expected_status", "kind"} <= set(r) for r in rows)
    assert {r["kind"] for r in rows} == {"real", "deliberately_wrong", "review_case"}
