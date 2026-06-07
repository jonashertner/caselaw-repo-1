import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _write_health(path: Path, *, run_at: datetime, scrapers: dict) -> None:
    path.write_text(
        json.dumps(
            {
                "run_at": run_at.isoformat(),
                "scrapers": scrapers,
            }
        )
    )


def _write_snapshot_db(path: Path, *, court: str, snapshot_date: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE source_snapshots ("
            "source_key TEXT NOT NULL, "
            "snapshot_year INTEGER NOT NULL, "
            "snapshot_date TEXT NOT NULL, "
            "expected_ids_json TEXT NOT NULL, "
            "notes TEXT)"
        )
        conn.execute(
            "INSERT INTO source_snapshots "
            "(source_key, snapshot_year, snapshot_date, expected_ids_json, notes) "
            "VALUES (?, ?, ?, ?, ?)",
            (court, 2026, snapshot_date, "[]", "test"),
        )
        conn.commit()
    finally:
        conn.close()


def test_fresh_successful_zero_new_run_suppresses_snapshot_stale_false_positive(tmp_path):
    """A coverage snapshot records content changes, not successful attempts."""
    court = "test_court"
    coverage_db = tmp_path / "coverage.db"
    health_file = tmp_path / "scraper_health.json"
    alert_log = tmp_path / "alerts.log"

    now = datetime.now(timezone.utc)
    _write_snapshot_db(
        coverage_db,
        court=court,
        snapshot_date=(now - timedelta(days=45)).date().isoformat(),
    )
    _write_health(
        health_file,
        run_at=now,
        scrapers={
            court: {
                "success": True,
                "new_count": 0,
                "our_count": 2000,
                "portal_count": 2000,
                "duration_s": 90,
            }
        },
    )

    env = {
        **os.environ,
        "OCL_COVERAGE_DB": str(coverage_db),
    }
    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_scraper_freshness.py",
            "--health-file",
            str(health_file),
            "--alert-log",
            str(alert_log),
            "--state-dir",
            str(tmp_path),
            "--no-ntfy",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "STALE test_court" not in result.stdout
    assert "All checks passed" in result.stdout


def test_near_matching_portal_count_suppresses_fast_zero_new_warning(tmp_path):
    """Small corpus-vs-portal count drift should not look like a silent skip."""
    court = "test_court"
    coverage_db = tmp_path / "coverage.db"
    health_file = tmp_path / "scraper_health.json"
    alert_log = tmp_path / "alerts.log"

    now = datetime.now(timezone.utc)
    _write_snapshot_db(
        coverage_db,
        court=court,
        snapshot_date=now.date().isoformat(),
    )
    _write_health(
        health_file,
        run_at=now,
        scrapers={
            court: {
                "success": True,
                "new_count": 0,
                "our_count": 2000,
                "portal_count": 1998,
                "duration_s": 8,
            }
        },
    )

    env = {
        **os.environ,
        "OCL_COVERAGE_DB": str(coverage_db),
    }
    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_scraper_freshness.py",
            "--health-file",
            str(health_file),
            "--alert-log",
            str(alert_log),
            "--state-dir",
            str(tmp_path),
            "--no-ntfy",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "possible API outage" not in result.stdout
    assert "All checks passed" in result.stdout
