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
    # critical_only keeps CRITICAL (gating) + QUARANTINE (non-blocking but
    # must still alert/render) + FAILED WARNINGs (so the ntfy alert path is
    # reachable from the nightly gate); passing WARNINGs and INFO are dropped.
    assert all(
        r.severity in (types.Severity.CRITICAL, types.Severity.QUARANTINE)
        or (r.severity is types.Severity.WARNING and not r.passed)
        for r in crit.results
    )
    assert not any(r.severity is types.Severity.INFO for r in crit.results)
    assert len(crit.results) <= len(full.results)


def test_gate_keeps_failed_warnings_but_never_blocks():
    """A FAILED WARNING must survive the gate filter (for alerting) while a
    passing WARNING and INFO are dropped — and a failed WARNING alone must
    never flip the gate (passed stays CRITICAL-only)."""
    from quality import severity

    def mk(sev, passed):
        return types.CheckResult(
            name=f"synthetic.{sev.value}.{'pass' if passed else 'fail'}",
            severity=sev, passed=passed, metric_value=0, threshold=None,
            message="synthetic")

    results = [
        mk(types.Severity.CRITICAL, True),
        mk(types.Severity.QUARANTINE, False),
        mk(types.Severity.WARNING, False),
        mk(types.Severity.WARNING, True),
        mk(types.Severity.INFO, False),
    ]
    kept = runner.gate_visible_results(results)
    kinds = {(r.severity, r.passed) for r in kept}
    assert (types.Severity.WARNING, False) in kinds
    assert (types.Severity.WARNING, True) not in kinds
    assert (types.Severity.INFO, False) not in kinds
    assert (types.Severity.CRITICAL, True) in kinds
    assert (types.Severity.QUARANTINE, False) in kinds

    report = types.CheckRunReport(
        run_at="2026-07-01T00:00:00+00:00", db_path="synthetic",
        duration_seconds=0.0, results=kept,
    )
    assert report.passed is True          # no CRITICAL failure
    assert severity.exit_code_for(report) == 0   # gate does not block
    assert any(r.severity is types.Severity.WARNING
               for r in severity.alerting_results(report))  # alert reachable


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
                        lambda critical_only=False: real_discover(critical_only=critical_only) + [bad_check])

    report = runner.run(db_path=temp_db, record_history=False, parallel=False)
    bad_results = [r for r in report.results if "synthetic_buggy" in r.name]
    assert len(bad_results) == 1
    assert bad_results[0].severity is types.Severity.CRITICAL
    assert not bad_results[0].passed
    assert "synthetic failure" in bad_results[0].message


def test_gate_run_cannot_overwrite_the_full_archive(temp_db, tmp_path):
    """Regression (2026-08-19): the archive was keyed on the DAY, so the
    publish gate's --critical-only run replaced the day's full report
    with a filtered one under the same name — silent history loss. Two
    runs on one day must produce two files, and a partial report must
    say what it is, both in the filename and in the payload."""
    full = runner.run(db_path=temp_db, record_history=False,
                      parallel=False, only=["schema"])
    gate = runner.run(db_path=temp_db, record_history=False,
                      critical_only=True, parallel=False)
    p_full = runner.write_report(full, out_dir=tmp_path)
    p_gate = runner.write_report(gate, out_dir=tmp_path)
    assert p_full != p_gate, "two runs on one day must not collide"
    assert p_full.exists() and p_gate.exists()
    assert "critical_only" in p_gate.name, "a gate archive must be identifiable"
    assert gate.to_dict()["scope"] == "critical_only"
    assert full.to_dict()["scope"] == "subset"


def test_a_true_full_run_archives_without_a_scope_suffix(temp_db, tmp_path):
    report = runner.run(db_path=temp_db, record_history=False, parallel=False)
    out = runner.write_report(report, out_dir=tmp_path)
    assert report.scope == "full"
    assert "-" not in out.name.replace(report.run_at[:10], ""), out.name
