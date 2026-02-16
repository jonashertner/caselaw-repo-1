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
        SELECT source_decision_id, target_ref, target_decision_id
        FROM citation_targets
        ORDER BY target_decision_id
        """
    ).fetchall()
    conn.close()

    assert links == [
        ("d_source", "ZB_2016_28", "d_new"),
        ("d_source", "ZB_2016_28", "d_old"),
    ]


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
