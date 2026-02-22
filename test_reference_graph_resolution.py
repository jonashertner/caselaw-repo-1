import json
import sqlite3
from pathlib import Path

from search_stack.build_reference_graph import build_graph


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    path.write_text(payload, encoding="utf-8")


def test_build_graph_resolves_docket_to_multiple_targets(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_old",
            "docket_number": "ZB.2016.28",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2017-04-13",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_new",
            "docket_number": "ZB.2016.28",
            "court": "bs_appellationsgericht",
            "canton": "BS",
            "language": "de",
            "decision_date": "2018-08-23",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_source",
            "docket_number": "4A_291/2017",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "Vgl. ZB.2016.28.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citations_resolved"] == 1
    assert stats["citation_target_links"] == 2

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT source_decision_id, target_ref, target_decision_id, confidence_score
        FROM citation_targets
        ORDER BY target_decision_id
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    assert links[0][:3] == ("d_source", "ZB_2016_28", "d_new")
    assert links[1][:3] == ("d_source", "ZB_2016_28", "d_old")
    assert 0.0 < float(links[0][3]) <= 1.0
    assert 0.0 < float(links[1][3]) <= 1.0
    assert float(links[1][3]) > float(links[0][3])


def test_build_graph_is_idempotent_across_rebuilds(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d1",
            "docket_number": "1A.122/2005",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2005-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Art. 8 EMRK. BGE 147 I 268.",
        }
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    build_graph(input_dir=input_dir, db_path=db_path)
    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    stat_mention = conn.execute("SELECT mention_count FROM decision_statutes").fetchone()[0]
    cit_mention = conn.execute("SELECT mention_count FROM decision_citations").fetchone()[0]
    conn.close()

    assert stat_mention == 1
    assert cit_mention == 1


def test_build_graph_preserves_existing_snapshot_on_source_error(tmp_path: Path):
    db_path = tmp_path / "reference_graph.db"

    seed = sqlite3.connect(db_path)
    seed.executescript(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            docket_number TEXT,
            docket_norm TEXT,
            court TEXT,
            canton TEXT,
            language TEXT,
            decision_date TEXT
        );
        INSERT INTO decisions(decision_id) VALUES ('seed');
        """
    )
    seed.commit()
    seed.close()

    missing_source = tmp_path / "missing.db"
    try:
        build_graph(
            input_dir=tmp_path / "unused",
            db_path=db_path,
            source_db=missing_source,
        )
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("expected sqlite3.OperationalError for missing source_db")

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.close()
    assert count == 1


def test_citation_confidence_prefers_bger_for_bger_docket_pattern(tmp_path: Path):
    """When a docket like '4A_291/2017' matches both BGer and a cantonal court,
    the BGer target should get higher confidence via docket-pattern inference."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bger",
            "docket_number": "4A_291/2017",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_ge",
            "docket_number": "4A_291/2017",
            "court": "ge_gerichte",
            "canton": "GE",
            "language": "fr",
            "decision_date": "2018-06-11",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_cantonal_source",
            "docket_number": "ZB.2016.28",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2019-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Vgl. 4A_291/2017.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citation_target_links"] == 2

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT target_decision_id, confidence_score
        FROM citation_targets
        WHERE source_decision_id = 'd_cantonal_source'
        ORDER BY confidence_score DESC
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    # BGer should win: docket pattern "4A_*" strongly implies bger
    assert links[0][0] == "d_bger"
    assert links[1][0] == "d_ge"
    assert float(links[0][1]) > float(links[1][1])


def test_citation_confidence_prefers_bvger_for_bvger_docket_pattern(tmp_path: Path):
    """Docket 'E-5783/2024' should strongly prefer BVGer over any other court."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "d_bvger",
            "docket_number": "E-5783/2024",
            "court": "bvger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_other",
            "docket_number": "E-5783/2024",
            "court": "zh_gerichte",
            "canton": "ZH",
            "language": "de",
            "decision_date": "2024-06-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_citing",
            "docket_number": "1A.100/2025",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2025-01-01",
            "title": "",
            "regeste": "",
            "full_text": "Gemäss E-5783/2024.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    links = conn.execute(
        """
        SELECT target_decision_id, confidence_score
        FROM citation_targets
        WHERE source_decision_id = 'd_citing'
        ORDER BY confidence_score DESC
        """
    ).fetchall()
    conn.close()

    assert len(links) == 2
    assert links[0][0] == "d_bvger"
    assert float(links[0][1]) > float(links[1][1])


def test_infer_court_from_docket():
    from search_stack.build_reference_graph import _infer_court_from_docket

    # BGer patterns
    assert _infer_court_from_docket("6B_1234_2025") == "bger"
    assert _infer_court_from_docket("4A_291_2017") == "bger"
    assert _infer_court_from_docket("1C_100_2024") == "bger"

    # BVGer patterns
    assert _infer_court_from_docket("E_5783_2024") == "bvger"
    assert _infer_court_from_docket("D_8226_2025") == "bvger"
    assert _infer_court_from_docket("A_1234_2025") == "bvger"

    # BStGer patterns
    assert _infer_court_from_docket("SK_2025_1234") == "bstger"
    assert _infer_court_from_docket("BB_2024_100") == "bstger"

    # Cantonal / unknown — no inference
    assert _infer_court_from_docket("ZB_2016_28") is None
    assert _infer_court_from_docket("VB_2018_00411") is None
    assert _infer_court_from_docket("") is None


def test_bge_citations_resolved_to_bge_decisions(tmp_path: Path):
    """Issue 4: BGE citations (target_type='bge') should resolve to BGE decisions."""
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "bge_147_I_268",
            "docket_number": "147 I 268",
            "court": "bge",
            "canton": "CH",
            "language": "de",
            "decision_date": "2021-01-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "d_citing",
            "docket_number": "6B_100/2022",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2022-06-01",
            "title": "",
            "regeste": "",
            "full_text": "Gemäss BGE 147 I 268 ist dies klar.",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    # Verify BGE citation was extracted
    bge_cit = conn.execute(
        "SELECT target_ref, target_type FROM decision_citations WHERE source_decision_id = 'd_citing' AND target_type = 'bge'"
    ).fetchone()
    assert bge_cit is not None
    assert bge_cit[0] == "BGE 147 I 268"

    # Verify it was resolved
    link = conn.execute(
        "SELECT target_decision_id, match_type, confidence_score FROM citation_targets WHERE source_decision_id = 'd_citing' AND target_ref = 'BGE 147 I 268'"
    ).fetchone()
    conn.close()

    assert link is not None, "BGE citation should be resolved to a target"
    assert link[0] == "bge_147_I_268"
    assert link[1] == "bge_norm"
    assert float(link[2]) > 0.5


def test_build_graph_normalizes_whitespace_dockets(tmp_path: Path):
    input_dir = tmp_path / "decisions"
    input_dir.mkdir(parents=True)
    db_path = tmp_path / "reference_graph.db"
    rows = [
        {
            "decision_id": "target",
            "docket_number": " VB.2018.00411 ",
            "court": "bs_gerichte",
            "canton": "BS",
            "language": "de",
            "decision_date": "2018-01-01",
            "title": "",
            "regeste": "",
            "full_text": "",
        },
        {
            "decision_id": "source",
            "docket_number": "X.2019.1",
            "court": "bger",
            "canton": "CH",
            "language": "de",
            "decision_date": "2019-01-01",
            "title": "",
            "regeste": "Vgl. VB.2018.00411.",
            "full_text": "",
        },
    ]
    _write_jsonl(input_dir / "sample.jsonl", rows)

    stats = build_graph(input_dir=input_dir, db_path=db_path)
    assert stats["citations_resolved"] == 1
    assert stats["citation_target_links"] == 1
