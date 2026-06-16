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


# ── A4: registry-vs-health reconciliation ────────────────────────────────


def _full_health_scrapers(drop=()):
    """A 'full run' health dict = every registered scraper minus `drop`, each
    a clean success (our_count<1000 + duration>30s avoid the silent-skip and
    STALE heuristics so the only signal under test is the reconciliation)."""
    from run_scraper import SCRAPERS
    return {
        s: {"success": True, "new_count": 0, "our_count": 50, "duration_s": 90}
        for s in SCRAPERS if s not in drop
    }


def _run_monitor(tmp_path, scrapers):
    health_file = tmp_path / "scraper_health.json"
    _write_health(health_file, run_at=datetime.now(timezone.utc), scrapers=scrapers)
    env = {**os.environ, "OCL_COVERAGE_DB": str(tmp_path / "nonexistent.db")}
    return subprocess.run(
        [sys.executable, "scripts/check_scraper_freshness.py",
         "--health-file", str(health_file), "--alert-log", str(tmp_path / "a.log"),
         "--state-dir", str(tmp_path), "--no-ntfy"],
        cwd=Path(__file__).resolve().parent.parent, env=env,
        text=True, capture_output=True, check=False,
    )


def test_reconciliation_flags_missing_registered_scraper(tmp_path):
    # bvger is registered, not dead, not es-only — silently dropping it from a
    # full run must surface (the be_steuerrekurs blind-spot class)
    res = _run_monitor(tmp_path, _full_health_scrapers(drop=("bvger",)))
    assert res.returncode == 0
    assert "bvger: registered scraper absent from a full" in res.stdout


def test_reconciliation_quiet_for_known_dead_missing(tmp_path):
    # a KNOWN_DEAD scraper (ow_gerichte) absent from health is expected — silent
    res = _run_monitor(tmp_path, _full_health_scrapers(drop=("ow_gerichte",)))
    assert res.returncode == 0
    assert "registered scraper absent" not in res.stdout


def test_reconciliation_skipped_on_partial_run(tmp_path):
    # <20 scrapers = a partial/manual run; reconciliation must NOT fire (else
    # ~58 false WARNs). This is also what keeps the 1-court tests above green.
    res = _run_monitor(tmp_path, {
        "bger": {"success": True, "new_count": 0, "our_count": 50, "duration_s": 90}})
    assert res.returncode == 0
    assert "registered scraper absent" not in res.stdout
