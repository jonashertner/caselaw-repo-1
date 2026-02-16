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
