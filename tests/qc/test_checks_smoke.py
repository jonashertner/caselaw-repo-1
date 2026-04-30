"""Smoke tests for individual check modules — exercise each check
function on the fixture DB, assert it returns a CheckResult and
behaves correctly on the known data."""
from __future__ import annotations

import pytest

from quality import types
from quality.checks import (
    courts, dates, dockets, duplicates, languages, regeste,
    schema, urls,
)


def _collect(maybe_iter):
    """Helper: a check function may return a single CheckResult or
    an iterable; this normalises to a list."""
    if isinstance(maybe_iter, types.CheckResult):
        return [maybe_iter]
    return list(maybe_iter or [])


# ── schema ─────────────────────────────────────────────────────────

def test_schema_decisions_table_exists(temp_db_conn):
    r = schema.check_decisions_table_exists(temp_db_conn)
    assert r.passed
    assert r.metric_value == 1


def test_schema_required_columns_not_null(temp_db_conn):
    rs = _collect(schema.check_required_columns_not_null(temp_db_conn))
    assert len(rs) == len(schema.REQUIRED_NOT_NULL)
    for r in rs:
        assert r.passed, f"{r.name}: {r.message}"


def test_schema_decision_id_uniqueness(temp_db_conn):
    r = schema.check_decision_id_uniqueness(temp_db_conn)
    assert r.passed
    assert r.metric_value == 0


def test_schema_corpus_total_count_below_floor_fails(temp_db_conn):
    """The fixture has only 5 rows — far below the 950k floor —
    so this MUST fail. That's the right behaviour: a tiny DB
    means a build went wrong."""
    r = schema.check_corpus_total_count(temp_db_conn)
    assert not r.passed


# ── dates ──────────────────────────────────────────────────────────

def test_dates_year_0000_passes(temp_db_conn):
    r = dates.check_year_0000_dates(temp_db_conn)
    assert r.passed


def test_dates_far_future_passes(temp_db_conn):
    r = dates.check_far_future_dates(temp_db_conn)
    assert r.passed


def test_dates_pre_1700_passes(temp_db_conn):
    r = dates.check_pre_1700_dates(temp_db_conn)
    assert r.passed


def test_dates_invalid_format_passes(temp_db_conn):
    r = dates.check_invalid_date_format(temp_db_conn)
    assert r.passed


def test_dates_total_null_within_floor(temp_db_conn):
    """Fixture has 1 NULL date (ti_gerichte) — under threshold."""
    r = dates.check_total_null_dates(temp_db_conn)
    assert r.passed


# ── dockets ────────────────────────────────────────────────────────

def test_dockets_whitespace_clean(temp_db_conn):
    r = dockets.check_whitespace_in_docket(temp_db_conn)
    assert r.passed


def test_dockets_no_internal_newlines(temp_db_conn):
    r = dockets.check_internal_newlines_in_docket(temp_db_conn)
    assert r.passed


def test_dockets_empty_pct_under_threshold(temp_db_conn):
    """Fixture: zh_obergericht_LE220012 has docket_number=LE220012,
    so empty_pct should be 0."""
    r = dockets.check_empty_docket_pct(temp_db_conn)
    assert r.passed


# ── courts ─────────────────────────────────────────────────────────

def test_courts_no_null_court(temp_db_conn):
    r = courts.check_no_null_or_empty_court(temp_db_conn)
    assert r.passed


def test_courts_canonical_format(temp_db_conn):
    rs = _collect(courts.check_canonical_court_code(temp_db_conn))
    for r in rs:
        assert r.passed


def test_courts_canton_field_consistency(temp_db_conn):
    rs = _collect(courts.check_canton_field_consistency(temp_db_conn))
    for r in rs:
        assert r.passed


# ── languages ──────────────────────────────────────────────────────

def test_languages_unexpected_values_passes(temp_db_conn):
    r = languages.check_unexpected_language_values(temp_db_conn)
    assert r.passed


def test_languages_null_pct_clean(temp_db_conn):
    r = languages.check_null_language_pct(temp_db_conn)
    assert r.passed


# ── urls ───────────────────────────────────────────────────────────

def test_urls_no_relative_source_urls(temp_db_conn):
    r = urls.check_no_relative_source_urls(temp_db_conn)
    assert r.passed


def test_urls_decision_id_url_safe(temp_db_conn):
    """Fixture decision_ids contain `/` which is now allowed."""
    r = urls.check_decision_id_url_safe(temp_db_conn)
    assert r.passed, f"got: {r.message} -- samples: {r.sample_rows}"


# ── duplicates ─────────────────────────────────────────────────────

def test_duplicates_no_court_docket_collisions(temp_db_conn):
    r = duplicates.check_court_docket_collisions(temp_db_conn)
    assert r.passed


def test_duplicates_egmr_no_dual_attribution(temp_db_conn):
    r = duplicates.check_egmr_no_dual_attribution(temp_db_conn)
    assert r.passed


def test_duplicates_decision_id_cross_court(temp_db_conn):
    r = duplicates.check_decision_id_collisions_across_courts(temp_db_conn)
    assert r.passed


# ── regeste ────────────────────────────────────────────────────────

def test_regeste_too_short_passes(temp_db_conn):
    r = regeste.check_regeste_too_short(temp_db_conn)
    assert r.passed


def test_regeste_excessive_length_passes(temp_db_conn):
    r = regeste.check_regeste_excessive_length(temp_db_conn)
    assert r.passed
