"""Severity routing helpers — block-vs-alert decisions in one place.

Used by `quality.runner` and `publish.py` Step 6c to decide whether
to block the nightly git push or only alert.
"""
from __future__ import annotations

from quality.types import CheckResult, CheckRunReport, Severity


def is_publish_safe(report: CheckRunReport) -> bool:
    """True iff zero CRITICAL checks failed (the gate semantics).

    Mirror of `CheckRunReport.passed`. Kept as a free function so
    callers in publish.py can route on a single import.
    """
    return report.passed


def alerting_results(report: CheckRunReport) -> list[CheckResult]:
    """All results that should fire ntfy alerts (CRITICAL + QUARANTINE + WARNING failures)."""
    return [r for r in report.results if r.is_alerting_failure]


def format_alert_summary(report: CheckRunReport) -> str:
    """Compact 1-2 line summary suitable for an ntfy push notification."""
    n_crit = len(report.critical_failures)
    n_quar = len(report.quarantine_failures)
    n_warn = len(report.warning_failures)
    if n_crit:
        names = ", ".join(r.name for r in report.critical_failures[:3])
        more = f" (+{n_crit - 3} more)" if n_crit > 3 else ""
        return f"BLOCKING: {n_crit} critical QC failures: {names}{more}"
    if n_quar:
        names = ", ".join(r.name for r in report.quarantine_failures[:3])
        more = f" (+{n_quar - 3} more)" if n_quar > 3 else ""
        return f"QUARANTINE (auto-NULLed, not blocking): {n_quar}: {names}{more}"
    if n_warn:
        names = ", ".join(r.name for r in report.warning_failures[:3])
        more = f" (+{n_warn - 3} more)" if n_warn > 3 else ""
        return f"WARN: {n_warn} QC drift alerts: {names}{more}"
    return "All clear"


def exit_code_for(report: CheckRunReport) -> int:
    """0 if publish-safe, 1 if any CRITICAL failed.

    `publish.py` Step 6c uses the return code to gate Step 6 (git push).
    """
    return 0 if report.passed else 1
