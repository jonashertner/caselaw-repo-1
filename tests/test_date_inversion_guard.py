"""build_fts5 gross date-inversion forward guard.

Policy (2026-06-13): decision_date mandatory + trusted; publication_date
optional. A court can't publish >1 month before it rules, so a pub_date
>31 days before the ruling is a mislabel/parse error and is NULLed on
every full rebuild. Only the GROSS band is corrected — the 0-3 day band
(possible dispatch dates) is left intact.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import build_fts5  # noqa: E402

g = build_fts5._date_inversion_guard_inline


def test_gross_year_inversion_nulled():
    # the ~1-year year-parse bug: ruled 2025, "published" 2024
    assert g("2025-03-14", "2024-03-14") is None


def test_gross_months_inversion_nulled():
    assert g("2024-05-22", "2024-03-22") is None  # pub 2 months before


def test_small_inversion_kept():
    # 0-3 day band — likely a dispatch date, left intact
    assert g("2025-09-05", "2025-09-03") == "2025-09-03"


def test_days_to_month_inversion_kept():
    # within 31 days early — not gross, kept
    assert g("2025-02-01", "2025-01-15") == "2025-01-15"


def test_boundary_31_days_kept_32_nulled():
    assert g("2025-02-01", "2025-01-01") == "2025-01-01"   # exactly 31 days
    assert g("2025-02-02", "2025-01-01") is None           # 32 days → gross


def test_normal_order_kept():
    # publication after the ruling — the common, correct case
    assert g("2024-06-05", "2025-01-01") == "2025-01-01"


def test_same_day_kept():
    assert g("2025-01-01", "2025-01-01") == "2025-01-01"


def test_missing_or_malformed_kept():
    assert g(None, "2025-01-01") == "2025-01-01"
    assert g("2024-06-05", None) is None          # pub already None
    assert g("notadate", "2025-01-01") == "2025-01-01"
    assert g("2024-06-05", "garbage") == "garbage"


def test_datetime_suffix_tolerated():
    # ISO strings with a time component still compare on the date part
    assert g("2025-03-14T00:00:00", "2024-03-14T09:00:00") is None
