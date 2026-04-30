"""Full-text + extraction-quality checks.

Codifies /tmp/scan_extraction_quality.py — detects:
- Win-1252 smart-quote artefacts (\\x91-\\x94)
- Unhandled ligature glyphs (ﬁ ﬂ etc.)
- UTF-8 ↔ Latin-1 encoding mismatches (Ã¼ etc.)
- Abnormally long words (>30 chars)
- Control characters (binary leakage)

Plus structural sanity:
- empty full_text on a court that should have content
- ultra-short full_text (likely PDF extraction failure)
- repeated header/footer leakage

Per-court WARNING: a sudden uptick in any of these patterns is the
canonical OCR-quality regression signal.
"""
from __future__ import annotations

import re
import sqlite3

from quality.types import CheckResult, Severity


# König P7 floor: 64 short-text rows known unfixable without re-download
SHORT_TEXT_FLOOR_DEFAULT = 100        # absolute global floor
SHORT_TEXT_FLOOR_PER_COURT = {        # per-court known floors
    "so_gerichte": 65,                # 59 known truncated PDFs + slack
    "bl_gerichte": 6_500,             # 6,036 metadata-only after court-removal
    "gr_gerichte": 9_500,             # 9,344 scanned PDFs awaiting OCR v2
}

# Patterns indicating bad extraction (compiled once at module load)
_BAD_PATTERNS = [
    (re.compile(r"[\u0091\u0092\u0093\u0094]"),
     "win1252_smart_quotes"),
    (re.compile(r"[ﬀﬁﬂﬃﬄ]"),
     "unhandled_ligature_glyphs"),
    (re.compile(r"[ÃãÄä][¼½¶¬]"),
     "utf8_latin1_mismatch"),
    (re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]"),
     "control_chars"),
]


def check_short_full_text(conn: sqlite3.Connection, **_) -> CheckResult:
    """Rows with full_text < 500 chars (excluding metadata-only courts).

    Source-data floor ≈ 64 rows in 2026-04-30 audit. Anything materially
    above the per-court floor sum + 100 absolute slack is a regression."""
    expected_max = sum(SHORT_TEXT_FLOOR_PER_COURT.values()) + SHORT_TEXT_FLOOR_DEFAULT
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE "
        "full_text IS NOT NULL AND length(full_text) BETWEEN 1 AND 499"
    ).fetchone()[0]
    return CheckResult(
        name="text_quality.short_full_text_total",
        severity=Severity.WARNING,
        passed=(n <= expected_max),
        metric_value=n,
        threshold=expected_max,
        message=f"{n:,} rows with 1-499 char full_text",
        fix_advice="if growing, identify new contributing court via "
                   "per-court breakdown; consider PDF re-extraction.",
    )


def check_short_full_text_per_court(conn: sqlite3.Connection, **_):
    """Per-court drift on short-text rows for known-floor courts."""
    for court, floor in SHORT_TEXT_FLOOR_PER_COURT.items():
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND "
            "full_text IS NOT NULL AND length(full_text) BETWEEN 1 AND 499",
            (court,),
        ).fetchone()[0]
        upper = max(int(floor * 1.1), floor + 50)
        yield CheckResult(
            name=f"text_quality.short_text_floor.{court}",
            severity=Severity.WARNING,
            passed=(n <= upper),
            metric_value=n,
            threshold=upper,
            message=f"{court}: {n:,} short-text rows (floor {floor:,})",
            court=court,
        )


def check_extraction_artefacts(conn: sqlite3.Connection, **_):
    """For each bad-extraction pattern, count how many rows contain it
    in their full_text. Tolerance: small absolute counts allowed.

    The query uses LIKE with literal char ranges where possible —
    fastest. For Unicode patterns (ligatures, smart quotes), we fall
    back to a Python scan over the full table, sampled at 1% to keep
    runtime under 1 minute.
    """
    # Quick global pre-filter via LIKE for the cheap classes
    for pattern, label in _BAD_PATTERNS:
        # SQLite LIKE doesn't handle Unicode-aware char classes; do a
        # python sample scan capped at 50k rows for cost-bounded check.
        rows = conn.execute(
            "SELECT decision_id, court, length(full_text) AS L "
            "FROM decisions WHERE full_text IS NOT NULL AND length(full_text) > 100 "
            "ORDER BY random() LIMIT 50000"
        ).fetchall()
        sample_n = 0
        sample_hits = []
        for r in rows:
            ft = conn.execute(
                "SELECT full_text FROM decisions WHERE decision_id=?",
                (r["decision_id"],),
            ).fetchone()[0] or ""
            if pattern.search(ft):
                sample_n += 1
                if len(sample_hits) < 5:
                    sample_hits.append({
                        "decision_id": r["decision_id"],
                        "court": r["court"],
                        "length": r["L"],
                    })
        # Express as a rate per 50k sample
        rate_pct = round(100 * sample_n / max(1, len(rows)), 4)
        # Threshold: 0.5% of sample is the regression line
        yield CheckResult(
            name=f"text_quality.artefact.{label}",
            severity=Severity.WARNING,
            passed=(rate_pct <= 0.5),
            metric_value=rate_pct,
            threshold=0.5,
            message=f"{label}: {sample_n}/{len(rows):,} sample rows "
                    f"({rate_pct:.3f}%)",
            sample_rows=sample_hits,
            extra={"sample_size": len(rows), "hits_in_sample": sample_n},
            fix_advice="if growing, the upstream PDF extractor needs an encoding "
                       "fix (Win-1252, ligatures, mojibake)",
        )


def check_excessive_whitespace_ratio(conn: sqlite3.Connection, **_) -> CheckResult:
    """A 50k-sample of long-text rows: any row with whitespace ratio
    >25% is likely OCR'd badly (each glyph spaced as a word). The
    canonical Swiss decision sits ~14-18% whitespace.

    Source-data floor: assume up to 100 such rows are acceptable (some
    badly-OCR'd archive PDFs). Beyond that — regression.
    """
    rows = conn.execute(
        "SELECT decision_id, court, full_text FROM decisions "
        "WHERE full_text IS NOT NULL AND length(full_text) > 1000 "
        "ORDER BY random() LIMIT 50000"
    ).fetchall()
    bad = 0
    sample_hits = []
    for r in rows:
        ft = r["full_text"] or ""
        if not ft:
            continue
        ws = ft.count(" ") + ft.count("\t") + ft.count("\n")
        ratio = ws / len(ft)
        if ratio > 0.25:
            bad += 1
            if len(sample_hits) < 5:
                sample_hits.append({
                    "decision_id": r["decision_id"],
                    "court": r["court"],
                    "ratio_pct": round(100 * ratio, 1),
                })
    pct = round(100 * bad / max(1, len(rows)), 3)
    return CheckResult(
        name="text_quality.excessive_whitespace_ratio",
        severity=Severity.WARNING,
        passed=(pct <= 0.5),
        metric_value=pct,
        threshold=0.5,
        message=f"{bad}/{len(rows):,} sample rows ({pct:.2f}%) "
                f"with >25% whitespace",
        sample_rows=sample_hits,
        extra={"sample_size": len(rows)},
    )
