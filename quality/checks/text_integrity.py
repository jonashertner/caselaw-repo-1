"""Gate-level text-integrity checks (control characters / NUL).

Permanent regression guard for the 2026-06-29 keystone incident class:
C0 control bytes in full_text truncate SQLite TEXT at insert (NUL) and
silently zero decision_structure extraction + search recall; the ge CID-font
class stored 58%-control garbage. Both are fixed at build time
(build_fts5._clean_text strips C0 except \\t \\n \\r since d0072b2; ge OCR
backfill 2026-07-01), so the corpus expectation is literally ZERO such
characters — any hit means the build-side guarantee regressed.

Unlike quality/checks/text_quality.py (MODULE_NEVER_CRITICAL, full-table
scans, never runs in the gate), this module is gate-safe by construction:
random-ROWID point lookups only, no table scans (a scraped_at range scan
measured >240s on the served DB; this design runs in ~1-2s).

Sampling is seeded by UTC date: deterministic within a day (reruns of a
gate see the same sample), rotating daily (coverage accumulates).
"""
from __future__ import annotations

import random
import re
import sqlite3
from datetime import date

from quality.types import CheckResult, Severity

# C0 controls except \t \n \r, plus DEL — exactly what _clean_text strips.
CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

SAMPLE_SIZE = 2000
# Omnis/Findinfo portals (embedded NUL class) + the ge CID-font OCR class.
RISK_COURTS = ("ti_gerichte", "ne_gerichte", "ge_gerichte", "so_gerichte")
RISK_SAMPLE_PER_COURT = 200


def _sample_by_rowid(conn: sqlite3.Connection, rowids, cap: int):
    """Fetch (decision_id, full_text) for a random subset of rowids —
    point lookups only."""
    rng = random.Random(date.today().isoformat())
    if len(rowids) > cap:
        rowids = rng.sample(rowids, cap)
    out = []
    for rid in rowids:
        r = conn.execute(
            "SELECT decision_id, full_text FROM decisions WHERE rowid=?",
            (rid,),
        ).fetchone()
        if r is not None and r[1]:
            out.append((r[0], r[1]))
    return out


def check_control_chars_sample(conn: sqlite3.Connection, **_) -> CheckResult:
    """Corpus-wide random sample: zero rows may contain C0 controls."""
    max_rowid = conn.execute("SELECT max(rowid) FROM decisions").fetchone()[0] or 0
    rng = random.Random(date.today().isoformat())
    candidates = [rng.randint(1, max_rowid) for _ in range(SAMPLE_SIZE * 2)]
    rows = _sample_by_rowid(conn, candidates, SAMPLE_SIZE)
    bad = [(did, len(CTRL_RE.findall(txt))) for did, txt in rows
           if CTRL_RE.search(txt)]
    return CheckResult(
        name="text_integrity.control_chars_sample",
        severity=Severity.CRITICAL,
        passed=(len(bad) == 0),
        metric_value=len(bad),
        threshold=0,
        message=(f"{len(bad)} of {len(rows)} sampled rows contain C0 control "
                 f"chars" if bad else
                 f"0 of {len(rows)} sampled rows contain control chars"),
        sample_rows=[{"decision_id": d, "ctrl_count": n} for d, n in bad[:5]],
        fix_advice="build_fts5._clean_text (d0072b2) must strip these at "
                   "insert — a hit means the build-side guarantee regressed "
                   "(check recent build_fts5/_clean_text changes)",
    )


def check_control_chars_risk_courts(conn: sqlite3.Connection, **_):
    """Per-court samples for the portals whose raw payloads carry control
    bytes (Omnis/Findinfo ti/ne/ge/so). Zero tolerance post-cleaning."""
    for court in RISK_COURTS:
        rowids = [r[0] for r in conn.execute(
            "SELECT rowid FROM decisions WHERE court=?", (court,))]
        rows = _sample_by_rowid(conn, rowids, RISK_SAMPLE_PER_COURT)
        bad = [did for did, txt in rows if CTRL_RE.search(txt)]
        yield CheckResult(
            name=f"text_integrity.control_chars.{court}",
            severity=Severity.CRITICAL,
            passed=(len(bad) == 0),
            metric_value=len(bad),
            threshold=0,
            message=(f"{court}: {len(bad)} of {len(rows)} sampled rows "
                     f"contain control chars" if bad else
                     f"{court}: clean ({len(rows)} sampled)"),
            sample_rows=[{"decision_id": d} for d in bad[:5]],
            court=court,
            fix_advice="raw portal payload leaked past _clean_text — check "
                       "the insert path for this court",
        )
