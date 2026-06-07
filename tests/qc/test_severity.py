"""Tests for the gate semantics (block-vs-alert routing)."""
from __future__ import annotations

from quality import severity, types


def _result(name, sev, passed):
    return types.CheckResult(
        name=name, severity=sev, passed=passed,
        metric_value=0, threshold=0, message=f"{name}={passed}",
    )


def _report(*results):
    return types.CheckRunReport(
        run_at="2026-04-30T00:00:00+00:00",
        db_path="test.db",
        duration_seconds=0.1,
        results=list(results),
    )


def test_publish_safe_when_only_warnings_fail():
    r = _report(
        _result("dates.future", types.Severity.CRITICAL, True),
        _result("regeste.short", types.Severity.WARNING, False),
    )
    assert severity.is_publish_safe(r) is True
    assert severity.exit_code_for(r) == 0


def test_publish_blocked_on_critical_fail():
    r = _report(
        _result("schema.fk", types.Severity.CRITICAL, False),
        _result("regeste.coverage", types.Severity.WARNING, True),
    )
    assert severity.is_publish_safe(r) is False
    assert severity.exit_code_for(r) == 1


def test_alerting_includes_warning_failures():
    r = _report(
        _result("dates.future", types.Severity.CRITICAL, False),
        _result("dates.null_floor", types.Severity.WARNING, False),
        _result("info.top10", types.Severity.INFO, False),
    )
    alerts = severity.alerting_results(r)
    names = {x.name for x in alerts}
    assert "dates.future" in names
    assert "dates.null_floor" in names
    assert "info.top10" not in names  # INFO never alerts


def test_format_alert_summary():
    r_crit = _report(
        _result("schema.fk", types.Severity.CRITICAL, False),
    )
    s = severity.format_alert_summary(r_crit)
    assert "BLOCKING" in s
    assert "schema.fk" in s

    r_warn = _report(
        _result("regeste.short", types.Severity.WARNING, False),
    )
    s = severity.format_alert_summary(r_warn)
    assert "WARN" in s
    assert "regeste.short" in s

    r_clean = _report(
        _result("dates.future", types.Severity.CRITICAL, True),
    )
    assert "All clear" in severity.format_alert_summary(r_clean)


def test_check_result_blocking_property():
    crit_fail = _result("c", types.Severity.CRITICAL, False)
    crit_pass = _result("c", types.Severity.CRITICAL, True)
    quar_fail = _result("q", types.Severity.QUARANTINE, False)
    warn_fail = _result("w", types.Severity.WARNING, False)
    info_fail = _result("i", types.Severity.INFO, False)

    assert crit_fail.is_blocking_failure
    assert not crit_pass.is_blocking_failure
    assert not quar_fail.is_blocking_failure   # QUARANTINE never blocks
    assert not warn_fail.is_blocking_failure
    assert not info_fail.is_blocking_failure

    assert crit_fail.is_alerting_failure
    assert quar_fail.is_alerting_failure        # but QUARANTINE does alert
    assert warn_fail.is_alerting_failure
    assert not info_fail.is_alerting_failure
    assert not crit_pass.is_alerting_failure


def test_quarantine_failure_does_not_block_publish():
    """Anti-freeze regression: a QUARANTINE failure (e.g. dates.pre_1700) must
    NOT cascade-block the publish — the 2026-06-03..06 freeze, where one bad
    date skipped HF upload + git push for 4 nights. It alerts, but the gate
    passes as long as no CRITICAL failed."""
    r = _report(
        _result("schema.fk", types.Severity.CRITICAL, True),          # structural ok
        _result("dates.pre_1700", types.Severity.QUARANTINE, False),  # cosmetic bad date
    )
    assert severity.is_publish_safe(r) is True
    assert severity.exit_code_for(r) == 0
    assert r.passed is True
    assert [x.name for x in r.quarantine_failures] == ["dates.pre_1700"]


def test_quarantine_still_alerts_and_is_summarised():
    r = _report(_result("dates.pre_1700", types.Severity.QUARANTINE, False))
    assert "dates.pre_1700" in {x.name for x in severity.alerting_results(r)}
    s = severity.format_alert_summary(r)
    assert "QUARANTINE" in s and "dates.pre_1700" in s and "All clear" not in s


def test_quarantine_does_not_mask_a_real_critical():
    """A CRITICAL (schema / dates.invalid_format) still blocks even when a
    QUARANTINE check also fails."""
    r = _report(
        _result("dates.invalid_format", types.Severity.CRITICAL, False),
        _result("dates.pre_1700", types.Severity.QUARANTINE, False),
    )
    assert severity.is_publish_safe(r) is False
    assert severity.exit_code_for(r) == 1
