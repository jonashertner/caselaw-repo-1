"""dispatch_health_alerts.decide(): the dedup / re-nag / all-clear logic
that turns the (previously dark) alerts_dry_run list into ntfy sends."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

import dispatch_health_alerts as dha  # noqa: E402

NOW = 1_780_000_000.0
ALERT = {"level": "warning", "key": "quick_publish_stale", "message": "stale 3h"}


def test_new_alert_is_sent_and_recorded():
    to_send, clears, state = dha.decide([ALERT], {}, NOW)
    assert to_send == [ALERT]
    assert clears == []
    assert state["quick_publish_stale"]["last_sent"] == NOW


def test_recent_alert_is_not_resent():
    prior = {"quick_publish_stale": {"last_sent": NOW - 3600, "level": "warning"}}
    to_send, clears, state = dha.decide([ALERT], prior, NOW)
    assert to_send == []
    assert state["quick_publish_stale"]["last_sent"] == NOW - 3600  # unchanged


def test_stale_alert_renags_after_renag_hours():
    prior = {"quick_publish_stale": {
        "last_sent": NOW - (dha.RENAG_HOURS * 3600 + 1), "level": "warning"}}
    to_send, _, state = dha.decide([ALERT], prior, NOW)
    assert to_send == [ALERT]
    assert state["quick_publish_stale"]["last_sent"] == NOW


def test_cleared_alert_sends_all_clear_once_and_forgets():
    prior = {"quick_publish_stale": {"last_sent": NOW - 3600, "level": "warning"}}
    to_send, clears, state = dha.decide([], prior, NOW)
    assert to_send == []
    assert clears == ["quick_publish_stale"]
    assert state == {}  # forgotten -> a second empty poll sends nothing


def test_alert_without_key_is_ignored():
    to_send, clears, state = dha.decide([{"level": "warning"}], {}, NOW)
    assert to_send == [] and clears == [] and state == {}
