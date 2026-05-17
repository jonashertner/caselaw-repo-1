"""Synthetic alert engine.

Defines alert rules and evaluates them against a health dict. Does
NOT fire external notifications — that wiring lives in a separate
follow-up PR (deployed only after the PR 1 Monday gate passes; see
``docs/decision_rules.md`` rule 5 and ``docs/observability.md``).

Until then, operators read ``/metrics/health`` (which embeds
``alerts_dry_run``) to see what *would* fire. Empty list = all clear.

Rules currently encoded:

- ``pipeline_stale``: full-pipeline last success > 26 h ago.
- ``quick_publish_stale``: on a weekday (Mon-Fri UTC), quick_publish
  hasn't run in > 2 h.
- ``mcp_error_rate_high``: error count / total tool calls > 1% over
  the in-process metric counters (requires ≥ 100 samples).

Adding a new rule: write a ``check_*`` function that takes ``health``
(and optionally ``metrics``), returns ``None`` for clear or a dict
``{level, key, message, ...}``. Register it in ``check_all``.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional


PIPELINE_STALE_THRESHOLD_HOURS = 26.0
QUICK_PUBLISH_STALE_THRESHOLD_HOURS = 2.0
MCP_ERROR_RATE_THRESHOLD = 0.01
MCP_ERROR_RATE_MIN_SAMPLES = 100


def _is_weekday_utc(ts: int) -> bool:
    """Mon-Fri in UTC.

    Saturday=5, Sunday=6 in Python's weekday() — so weekday < 5 = weekday.
    Using UTC because the bger-poller timer schedules in UTC.
    """
    return datetime.fromtimestamp(ts, tz=timezone.utc).weekday() < 5


def check_pipeline_stale(
    health: dict, now: Optional[int] = None,
) -> Optional[dict]:
    """Pipeline (full rebuild + quick publishes) last success too old."""
    now = now if now is not None else int(time.time())
    ts = health.get("pipeline_last_success_ts")
    if ts is None:
        return {
            "level": "warning",
            "key": "pipeline_unknown",
            "message": (
                "pipeline_last_success_ts unavailable — "
                "cannot verify recency"
            ),
        }
    age_h = (now - ts) / 3600
    if age_h > PIPELINE_STALE_THRESHOLD_HOURS:
        return {
            "level": "critical",
            "key": "pipeline_stale",
            "message": (
                f"pipeline last success {age_h:.1f}h ago "
                f"(threshold {PIPELINE_STALE_THRESHOLD_HOURS}h)"
            ),
            "age_hours": round(age_h, 2),
        }
    return None


def check_quick_publish_stale(
    health: dict, now: Optional[int] = None,
) -> Optional[dict]:
    """On a weekday, quick_publish hasn't run recently.

    Weekends are exempt because ``bger-poller.timer`` fires Mon-Fri
    only (by design — courts don't publish on weekends).
    """
    now = now if now is not None else int(time.time())
    if not _is_weekday_utc(now):
        return None
    ts = health.get("quick_publish_last_run_ts")
    if ts is None:
        return {
            "level": "warning",
            "key": "quick_publish_unknown",
            "message": (
                "quick_publish_last_run_ts unavailable on a weekday"
            ),
        }
    age_h = (now - ts) / 3600
    if age_h > QUICK_PUBLISH_STALE_THRESHOLD_HOURS:
        return {
            "level": "warning",
            "key": "quick_publish_stale",
            "message": (
                f"quick_publish last run {age_h:.1f}h ago on a weekday "
                f"(threshold {QUICK_PUBLISH_STALE_THRESHOLD_HOURS}h)"
            ),
            "age_hours": round(age_h, 2),
        }
    return None


def check_mcp_error_rate(metrics: dict) -> Optional[dict]:
    """In-process MCP tool error rate is high.

    Reads from the live ``_metrics`` counters; only fires once we have
    at least ``MCP_ERROR_RATE_MIN_SAMPLES`` calls, to avoid noisy
    fractions on slow days.
    """
    tool_calls = metrics.get("tool_calls", {}) or {}
    tool_errors = metrics.get("tool_errors", {}) or {}
    # Counter / dict — either works
    try:
        calls = sum(tool_calls.values())
        errors = sum(tool_errors.values())
    except (AttributeError, TypeError):
        return None
    if calls < MCP_ERROR_RATE_MIN_SAMPLES:
        return None
    rate = errors / calls
    if rate > MCP_ERROR_RATE_THRESHOLD:
        return {
            "level": "warning",
            "key": "mcp_error_rate_high",
            "message": (
                f"error rate {100 * rate:.2f}% "
                f"(threshold {100 * MCP_ERROR_RATE_THRESHOLD:.1f}%, "
                f"over {calls} calls)"
            ),
            "rate": round(rate, 4),
            "calls": calls,
            "errors": errors,
        }
    return None


def check_all(
    health: dict,
    metrics: Optional[dict] = None,
    now: Optional[int] = None,
) -> list[dict]:
    """Run every alert rule. Returns the list of would-fire alerts.

    Empty list = all clear. Order matches registration; downstream
    notifier (future PR) handles severity ranking and deduplication.

    This function never raises — each rule is wrapped so a bug in one
    can't suppress the others.
    """
    out: list[dict] = []

    for rule in (check_pipeline_stale, check_quick_publish_stale):
        try:
            result = rule(health, now=now)
        except Exception as e:  # noqa: BLE001 - intentional safety net
            result = {
                "level": "warning",
                "key": f"{rule.__name__}_error",
                "message": f"alert rule raised: {e}",
            }
        if result:
            out.append(result)

    if metrics is not None:
        try:
            result = check_mcp_error_rate(metrics)
        except Exception as e:  # noqa: BLE001
            result = {
                "level": "warning",
                "key": "check_mcp_error_rate_error",
                "message": f"alert rule raised: {e}",
            }
        if result:
            out.append(result)

    return out
