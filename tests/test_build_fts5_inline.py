"""Tests for build_fts5 inline cleanups (Revised A — 2026-05-05).

The inline helpers replace the post-import UPDATE passes that used to run
on decisions.db.tmp BEFORE atomic swap. Each helper is pure (no DB
access) so we can unit-test them against canonical inputs without
spinning up SQLite. The post-pass UPDATEs still run as a safety net
on every build, so any inline-helper bug is recoverable; this test
suite catches the bugs early.

Coverage:
  • _docket_normalize_inline       — whitespace + newline collapse
  • _source_url_normalize_inline   — bs/gl Tribuna host prefix
  • _date_normalize_inline         — year-0000 + far-future scrubbing
  • _date_recover_inline           — anchor-phrase + month regex on 5 safe courts
  • _regeste_truncate_inline       — HUDOC oversized-regeste cut
  • _compute_row_content_hash_inline — SHA-256 determinism + invariants
"""
from __future__ import annotations

import hashlib
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from build_fts5 import (  # noqa: E402
    _compute_row_content_hash_inline,
    _date_normalize_inline,
    _date_recover_inline,
    _docket_normalize_inline,
    _regeste_truncate_inline,
    _source_url_normalize_inline,
)


# ── _docket_normalize_inline ──────────────────────────────────────────


def test_docket_trims_leading_trailing_whitespace() -> None:
    assert _docket_normalize_inline("  AEG.2018.00004  ") == "AEG.2018.00004"


def test_docket_collapses_internal_newlines() -> None:
    # The QC-gate-2026-05-01 case: scraper joined two table cells with \n.
    assert _docket_normalize_inline("A 2024 015\nUrteil vom") == "A 2024 015 Urteil vom"
    assert _docket_normalize_inline("A\rB") == "A B"
    assert _docket_normalize_inline("A\tB") == "A B"


def test_docket_collapses_run_of_spaces() -> None:
    assert _docket_normalize_inline("A   B   C") == "A B C"


def test_docket_passes_through_none_and_empty() -> None:
    assert _docket_normalize_inline(None) is None
    assert _docket_normalize_inline("") == ""


def test_docket_idempotent() -> None:
    clean = "AEG.2018.00004"
    assert _docket_normalize_inline(_docket_normalize_inline(clean)) == clean


# ── _source_url_normalize_inline ──────────────────────────────────────


def test_source_url_prefixes_bs_gerichte_relative() -> None:
    assert _source_url_normalize_inline(
        "bs_gerichte", "/cgi-bin/nph-omniscgi.exe?session=42",
    ) == "https://www.gerichte.bs.ch/cgi-bin/nph-omniscgi.exe?session=42"


def test_source_url_prefixes_gl_gerichte_relative() -> None:
    assert _source_url_normalize_inline(
        "gl_gerichte", "/findinfo/path",
    ) == "https://findinfo.gl.ch/findinfo/path"


def test_source_url_passes_through_absolute() -> None:
    abs_url = "https://other.example.com/x"
    assert _source_url_normalize_inline("bs_gerichte", abs_url) == abs_url


def test_source_url_passes_through_unmapped_court() -> None:
    rel = "/cgi-bin/foo"
    assert _source_url_normalize_inline("bger", rel) == rel


def test_source_url_handles_none_and_empty() -> None:
    assert _source_url_normalize_inline("bs_gerichte", None) is None
    assert _source_url_normalize_inline("bs_gerichte", "") == ""
    assert _source_url_normalize_inline(None, "/x") == "/x"


# ── _date_normalize_inline ────────────────────────────────────────────


def test_date_year_0000_returns_none() -> None:
    assert _date_normalize_inline("0000-01-01") is None
    assert _date_normalize_inline("0000-12-31T00:00:00") is None


def test_date_far_future_returns_none() -> None:
    far = (date.today() + timedelta(days=400)).isoformat()
    assert _date_normalize_inline(far) is None


def test_date_acceptable_dates_pass_through() -> None:
    today = date.today().isoformat()
    near = (date.today() + timedelta(days=100)).isoformat()
    past = "1995-06-15"
    for d in (today, near, past):
        assert _date_normalize_inline(d) == d


def test_date_handles_none_and_empty() -> None:
    assert _date_normalize_inline(None) is None
    assert _date_normalize_inline("") == ""


# ── _date_recover_inline ──────────────────────────────────────────────


def test_date_recover_safe_court_extracts_anchor_de() -> None:
    ft = (
        "Verwaltungsgericht des Kantons Zürich. "
        "Endentscheid vom 15. März 2024 betreffend Beschwerde der A. AG gegen B."
    )
    assert _date_recover_inline("zh_verwaltungsgericht", ft) == "2024-03-15"


def test_date_recover_safe_court_extracts_anchor_fr() -> None:
    ft = (
        "Tribunal administratif du canton de Fribourg. "
        "Arrêt du 12 septembre 2023 dans la cause A. SA contre B."
    )
    assert _date_recover_inline("fr_gerichte", ft) == "2023-09-12"


def test_date_recover_unsafe_court_returns_none() -> None:
    """ti_gerichte / mkg / hudoc_ch / bger etc. are explicitly excluded."""
    ft = (
        "Tribunal cantonal. Urteil vom 15. März 2024 betreffend "
        "Beschwerde der A. gegen B."
    )
    assert _date_recover_inline("bger", ft) is None
    assert _date_recover_inline("ti_gerichte", ft) is None
    assert _date_recover_inline("mkg", ft) is None


def test_date_recover_short_text_returns_none() -> None:
    assert _date_recover_inline("zh_verwaltungsgericht", "Urteil 1.1.24") is None


def test_date_recover_handles_none() -> None:
    assert _date_recover_inline("zh_verwaltungsgericht", None) is None
    assert _date_recover_inline(None, "Urteil vom 15. März 2024" * 5) is None


def test_date_recover_returns_iso_format() -> None:
    """Output must be 'YYYY-MM-DD' string, not date object."""
    ft = "Endentscheid vom 1. Januar 2020 betreffend " + "X" * 100
    out = _date_recover_inline("zh_verwaltungsgericht", ft)
    assert out == "2020-01-01"
    assert isinstance(out, str)


# ── _regeste_truncate_inline ──────────────────────────────────────────


def test_regeste_small_unchanged() -> None:
    small = "Short head note about Art. 41 OR."
    assert _regeste_truncate_inline(small, "full text body") == small


def test_regeste_large_but_not_duplicating_full_text_unchanged() -> None:
    """Only triggers when regeste near-duplicates full_text (>=90%)."""
    big_regeste = "X" * 9000
    short_full = "tiny full text"
    assert _regeste_truncate_inline(big_regeste, short_full) == big_regeste


def test_regeste_oversized_dup_truncated_at_boundary() -> None:
    """The HUDOC failure mode: regeste >= 90% of full_text length AND >8000.

    The body-boundary markers are newline-flanked ('\\nSachverhalt\\n'),
    matching how the head-note ends in real HUDOC output.
    """
    head = "Head note about Art. 8 EMRK. " * 100  # ~3 K chars, no newlines
    full = head + "\nSachverhalt\n" + ("Details. " * 1000)
    regeste = full  # exact duplicate triggers the dup-detection branch
    out = _regeste_truncate_inline(regeste, full)
    assert out is not None
    assert len(out) < len(regeste)
    # Body marker stripped — head note preserved
    assert "Sachverhalt" not in out
    assert "Details" not in out
    assert out.startswith("Head note about Art. 8 EMRK.")


def test_regeste_oversized_no_marker_truncated_at_5000() -> None:
    """If no body-boundary marker present, fall back to 5000-char cap."""
    full = "X" * 9000
    regeste = full  # near-duplicate (100%)
    out = _regeste_truncate_inline(regeste, full)
    assert out is not None
    assert len(out) <= 5000


def test_regeste_handles_none() -> None:
    assert _regeste_truncate_inline(None, "x") is None
    assert _regeste_truncate_inline(None, None) is None


# ── _compute_row_content_hash_inline ──────────────────────────────────


def test_content_hash_is_deterministic() -> None:
    h1 = _compute_row_content_hash_inline("regeste a", "full b")
    h2 = _compute_row_content_hash_inline("regeste a", "full b")
    assert h1 == h2


def test_content_hash_is_64_hex_chars() -> None:
    h = _compute_row_content_hash_inline("a", "b")
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_content_hash_changes_with_either_field() -> None:
    base = _compute_row_content_hash_inline("regeste", "full")
    diff_regeste = _compute_row_content_hash_inline("REGESTE", "full")
    diff_full = _compute_row_content_hash_inline("regeste", "FULL")
    assert base != diff_regeste
    assert base != diff_full


def test_content_hash_treats_none_as_empty_string() -> None:
    a = _compute_row_content_hash_inline(None, None)
    b = _compute_row_content_hash_inline("", "")
    assert a == b


def test_content_hash_matches_post_pass_formula() -> None:
    """The post-pass _compute_content_hashes() concatenates regeste+full_text
    then SHA-256s the UTF-8 bytes. Inline helper MUST match exactly so the
    safety-net post-pass finds nothing to update on a clean rebuild."""
    regeste, full_text = "Regeste line", "Full text body"
    expected = hashlib.sha256(
        ((regeste or "") + (full_text or "")).encode("utf-8"),
    ).hexdigest()
    assert _compute_row_content_hash_inline(regeste, full_text) == expected
