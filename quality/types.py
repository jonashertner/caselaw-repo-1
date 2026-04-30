"""Core types for the quality-control framework."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field, asdict
from typing import Any


class Severity(enum.Enum):
    """A check's severity tier.

    CRITICAL → blocks publish.py Step 6 (git push) on failure.
    WARNING  → fires ntfy.sh alert, pipeline continues.
    INFO     → recorded in report, no alerting.
    """
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"

    def __lt__(self, other: "Severity") -> bool:
        order = {Severity.INFO: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}
        return order[self] < order[other]


@dataclass
class CheckResult:
    """The structured result of a single quality check.

    name           Stable identifier, e.g. "dates.future_dates" or
                   "per_court_drift.zh_obergericht.row_count".
    severity       Tier this metric defends; controls block-vs-alert.
    passed         True if the check is within its tolerance.
    metric_value   The measured value (e.g. 471 NULL dates).
    threshold      The configured threshold; None for descriptive checks.
    message        One-line human summary.
    sample_rows    Up to 5 offending rows, for triage. Each row is a
                   dict of column → value to keep the report compact.
    fix_advice     How to remediate. Either a codified remedy reference
                   ("auto-corrected by build_fts5._normalize_dates()")
                   or a manual instruction.
    court          Optional per-court scoping for drift / per-court checks.
    extra          Additional structured detail (e.g. per-bucket counts).
    """
    name: str
    severity: Severity
    passed: bool
    metric_value: float | int
    threshold: float | int | None
    message: str
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    fix_advice: str | None = None
    court: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable form. Severity rendered as its string value."""
        d = asdict(self)
        d["severity"] = self.severity.value
        return d

    @property
    def is_blocking_failure(self) -> bool:
        """True iff this result would block publish.py Step 6 (git push)."""
        return (not self.passed) and self.severity is Severity.CRITICAL

    @property
    def is_alerting_failure(self) -> bool:
        """True iff this result fires an ntfy alert (CRITICAL or WARNING failure)."""
        return (not self.passed) and self.severity is not Severity.INFO


@dataclass
class CheckRunReport:
    """Aggregate of one full QC run.

    Written to quality/reports/YYYY-MM-DD.json + reports/latest.json
    + appended to quality/history.db for drift detection.
    """
    run_at: str                       # ISO 8601 UTC timestamp
    db_path: str
    duration_seconds: float
    results: list[CheckResult]

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.is_blocking_failure]

    @property
    def warning_failures(self) -> list[CheckResult]:
        return [
            r for r in self.results
            if (not r.passed) and r.severity is Severity.WARNING
        ]

    @property
    def passed(self) -> bool:
        """True iff no CRITICAL check failed (= safe to git push)."""
        return not self.critical_failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_at": self.run_at,
            "db_path": self.db_path,
            "duration_seconds": self.duration_seconds,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.passed),
                "critical_failures": len(self.critical_failures),
                "warning_failures": len(self.warning_failures),
                "publish_safe": self.passed,
            },
            "results": [r.to_dict() for r in self.results],
        }
