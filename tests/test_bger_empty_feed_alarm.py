"""BGer empty-Neuheiten alarm fires only when a blocked fetch is genuinely
indicated: a workday AND past BGer's ~10:00 UTC publication window. Before that
an empty feed is the normal pre-publication state (it produced ~5 false ERROR
alerts every workday morning)."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.bger_poller as bp  # noqa: E402


def _utc(y, m, d, h):
    return datetime(y, m, d, h, 0, tzinfo=timezone.utc)


def test_not_anomalous_in_the_pre_publication_window():
    # 2026-07-22 is a Wednesday. 05:00-10:00 UTC: BGer has not published yet.
    for h in (5, 6, 7, 8, 9, 10):
        assert bp._empty_feed_is_anomalous(_utc(2026, 7, 22, h)) is False, h


def test_anomalous_after_publication_window_on_a_workday():
    for h in (11, 12, 15, 16):
        assert bp._empty_feed_is_anomalous(_utc(2026, 7, 22, h)) is True, h


def test_never_anomalous_on_weekend():
    for h in (8, 12, 16):
        assert bp._empty_feed_is_anomalous(_utc(2026, 7, 18, h)) is False  # Sat
        assert bp._empty_feed_is_anomalous(_utc(2026, 7, 19, h)) is False  # Sun
