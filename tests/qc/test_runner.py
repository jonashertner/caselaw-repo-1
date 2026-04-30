"""Tests for quality.runner — discovery, parallel execution, reports."""
from __future__ import annotations

import json

from quality import runner, types


def test_discover_checks_finds_all_modules():
    checks = runner.discover_checks()
    assert len(checks) >= 30
    names = {f"{fn.__module__.split('.')[-1]}.{fn.__name__[len('check_'):]}"
             for fn in checks}
    # Spot-check that each module's checks are present
    for prefix in ("schema", "dates", "dockets", "courts", "languages",
                   "regeste", "urls", "duplicates", "text_quality",
                   "structure", "cross_db", "citation_graph",
                   "statute_graph", "floors", "per_court_drift",
                   "exports", "mcp_tools"):
        assert any(n.startswith(prefix + ".") for n in names), \
            f"no checks discovered from {prefix}"


def test_run_against_temp_db(temp_db, tmp_path):
    """End-to-end: run all checks against the small fixture DB and
    confirm the framework produces a valid JSON report."""
    report = runner.run(
        db_path=temp_db, record_history=False,
        only=["schema", "languages", "dockets"],
        parallel=False,
    )
    assert isinstance(report, types.CheckRunReport)
    assert report.duration_seconds >= 0
    assert len(report.results) >= 5
    payload = report.to_dict()
    # JSON-serialisable
    s = json.dumps(payload)
    assert "summary" in payload
    assert "results" in payload
    out = runner.write_report(report, out_dir=tmp_path)
    assert out.exists()
    assert (tmp_path / "latest.json").exists()


def test_critical_only_filter(temp_db):
    full = runner.run(db_path=temp_db, record_history=False, parallel=False)
    crit = runner.run(
        db_path=temp_db, record_history=False, critical_only=True,
        parallel=False,
    )
    assert all(r.severity is types.Severity.CRITICAL for r in crit.results)
    assert len(crit.results) <= len(full.results)


def test_only_filter_subset(temp_db):
    """`only=['schema']` runs schema checks only."""
    report = runner.run(
        db_path=temp_db, record_history=False, only=["schema"],
        parallel=False,
    )
    assert all(r.name.startswith("schema.") for r in report.results)
    assert len(report.results) >= 4


def test_buggy_check_does_not_kill_run(temp_db, monkeypatch):
    """A check that raises must surface as a CRITICAL result, not
    crash the whole run."""
    bad_called = {"n": 0}

    def bad_check(conn, **_):
        bad_called["n"] += 1
        raise RuntimeError("synthetic failure for test")

    bad_check.__module__ = "quality.checks.schema"
    bad_check.__name__ = "check_synthetic_buggy"

    real_discover = runner.discover_checks
    monkeypatch.setattr(runner, "discover_checks",
                        lambda: real_discover() + [bad_check])

    report = runner.run(db_path=temp_db, record_history=False, parallel=False)
    bad_results = [r for r in report.results if "synthetic_buggy" in r.name]
    assert len(bad_results) == 1
    assert bad_results[0].severity is types.Severity.CRITICAL
    assert not bad_results[0].passed
    assert "synthetic failure" in bad_results[0].message
