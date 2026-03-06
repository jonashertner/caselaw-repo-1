import sqlite3
from pathlib import Path

from coverage_report import (
    ensure_coverage_tables,
    generate_gap_report,
    record_snapshot,
    seed_targets_from_scrapers,
    sync_gap_queue_from_snapshots,
)


def _create_test_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            court TEXT NOT NULL,
            decision_date TEXT
        )
        """
    )
    ensure_coverage_tables(conn)
    return conn


def test_seed_targets_from_scrapers(tmp_path: Path):
    conn = _create_test_db(tmp_path / "coverage.db")
    inserted, updated = seed_targets_from_scrapers(conn, only_missing=False)

    assert inserted > 0
    assert updated == 0

    row = conn.execute(
        "SELECT source_key FROM coverage_targets WHERE source_key = 'bger'"
    ).fetchone()
    assert row is not None

    inserted2, updated2 = seed_targets_from_scrapers(conn, only_missing=True)
    assert inserted2 == 0
    assert updated2 == 0
    conn.close()


def test_gap_report_missing_ids_from_latest_snapshot(tmp_path: Path):
    conn = _create_test_db(tmp_path / "coverage.db")

    # Ingested decisions
    conn.executemany(
        "INSERT INTO decisions (decision_id, court, decision_date) VALUES (?, ?, ?)",
        [
            ("bger_1A_1_2024", "bger", "2024-01-10"),
            ("bger_1A_2_2024", "bger", "2024-02-11"),
        ],
    )
    conn.commit()

    # Older snapshot
    record_snapshot(
        conn,
        source_key="bger",
        snapshot_year=2024,
        snapshot_date="2026-03-01",
        decision_ids=["bger_1A_1_2024", "bger_1A_2_2024"],
        notes="older",
    )
    # Newer snapshot (should be selected)
    record_snapshot(
        conn,
        source_key="bger",
        snapshot_year=2024,
        snapshot_date="2026-03-05",
        decision_ids=["bger_1A_1_2024", "bger_1A_2_2024", "bger_1A_3_2024"],
        notes="latest",
    )

    report = generate_gap_report(
        conn,
        sources=["bger"],
        include_missing_ids=True,
        max_missing_ids=10,
    )

    assert len(report) == 1
    row = report[0]
    assert row["source_key"] == "bger"
    assert row["snapshot_year"] == 2024
    assert row["expected_count"] == 3
    assert row["ingested_count"] == 2
    assert row["missing_count"] == 1
    assert row["missing_ids"] == ["bger_1A_3_2024"]
    conn.close()


def test_gap_report_count_only_fallback(tmp_path: Path):
    conn = _create_test_db(tmp_path / "coverage.db")

    conn.executemany(
        "INSERT INTO decisions (decision_id, court, decision_date) VALUES (?, ?, ?)",
        [
            ("zh_gerichte_A_1_2025", "zh_gerichte", "2025-01-01"),
            ("zh_gerichte_A_2_2025", "zh_gerichte", "2025-06-15"),
        ],
    )
    conn.commit()

    # Empty expected_ids_json forces count fallback
    conn.execute(
        """
        INSERT INTO source_snapshots (
            source_key, snapshot_year, snapshot_date, expected_count, expected_ids_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("zh_gerichte", 2025, "2026-03-05", 5, "[]"),
    )
    conn.commit()

    report = generate_gap_report(conn, sources=["zh_gerichte"])
    assert len(report) == 1
    row = report[0]
    assert row["expected_count"] == 5
    assert row["ingested_count"] == 2
    assert row["missing_count"] == 3
    conn.close()


def test_sync_gap_queue_from_snapshots_resolves_when_ingested(tmp_path: Path):
    conn = _create_test_db(tmp_path / "coverage.db")

    conn.executemany(
        "INSERT INTO decisions (decision_id, court, decision_date) VALUES (?, ?, ?)",
        [
            ("bger_1A_1_2024", "bger", "2024-01-10"),
        ],
    )
    conn.commit()

    record_snapshot(
        conn,
        source_key="bger",
        snapshot_year=2024,
        snapshot_date="2026-03-05",
        decision_ids=["bger_1A_1_2024", "bger_1A_2_2024"],
    )

    stats1 = sync_gap_queue_from_snapshots(conn, sources=["bger"])
    assert stats1["upserted_missing"] == 1
    row1 = conn.execute(
        """
        SELECT status, retry_count
        FROM gap_queue
        WHERE source_key = 'bger' AND decision_year = 2024 AND decision_id = 'bger_1A_2_2024'
        """
    ).fetchone()
    assert tuple(row1) == ("open", 0)

    conn.execute(
        "INSERT INTO decisions (decision_id, court, decision_date) VALUES (?, ?, ?)",
        ("bger_1A_2_2024", "bger", "2024-02-11"),
    )
    conn.commit()

    stats2 = sync_gap_queue_from_snapshots(conn, sources=["bger"])
    assert stats2["resolved"] >= 1
    row2 = conn.execute(
        """
        SELECT status, resolution
        FROM gap_queue
        WHERE source_key = 'bger' AND decision_year = 2024 AND decision_id = 'bger_1A_2_2024'
        """
    ).fetchone()
    assert tuple(row2) == ("resolved", "snapshot_reconciled")
    conn.close()
