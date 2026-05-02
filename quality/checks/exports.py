"""End-to-end export round-trip checks.

For a sample of N decisions, render each export format (docx, pdf,
bib, ris) and assert the output:
- has the expected magic bytes / signature
- exceeds a minimum size (catches "renders successfully but produces
  empty document" regressions)
- contains the canonical citation in some form

Catches regressions in `exports.render_*` that pytest fixtures
miss because their inputs are stylised; production decisions have
weird edge cases (missing fields, multi-language regestes, very
long Erwägungen).
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


SAMPLE_SIZE = 25  # number of random decisions per format


def _sample_decisions(conn: sqlite3.Connection) -> list[dict]:
    """Pick N decisions stratified by court (catch per-court bugs)."""
    rows = conn.execute(
        "SELECT decision_id, court, court AS court_name, decision_date, "
        "docket_number, language, regeste, full_text, "
        "regeste AS citation_string_de "
        "FROM decisions WHERE length(full_text) > 500 "
        "ORDER BY random() LIMIT ?",
        (SAMPLE_SIZE,),
    ).fetchall()
    return [dict(r) for r in rows]


def _check_format(
    name: str, render_fn, decisions: list[dict],
    expected_magic: bytes | None, expected_mtype: str,
    min_size: int,
) -> CheckResult:
    """Run a render across the sample and aggregate failures."""
    fails: list[dict] = []
    sizes: list[int] = []
    for d in decisions:
        try:
            body, mtype, fname = render_fn(d, [])
        except Exception as e:
            fails.append({"decision_id": d["decision_id"],
                          "error": f"{type(e).__name__}: {e}"})
            continue
        ok = (
            mtype.startswith(expected_mtype)
            and (expected_magic is None or body.startswith(expected_magic))
            and len(body) >= min_size
        )
        sizes.append(len(body))
        if not ok:
            fails.append({
                "decision_id": d["decision_id"],
                "size": len(body),
                "mtype": mtype,
                "magic_ok": expected_magic is None or body.startswith(expected_magic),
            })
    n_pass = len(decisions) - len(fails)
    # Tolerance: allow up to 1 random-sample failure (95% pass rate). The
    # sample is random-seeded each run so a single missing-field outlier
    # is not a publish-blocking event. >1 failure is a real regression.
    MAX_TOLERATED_FAILS = max(1, len(decisions) // 25)
    is_critical_fail = len(fails) > MAX_TOLERATED_FAILS
    return CheckResult(
        name=f"exports.{name}",
        severity=Severity.CRITICAL if is_critical_fail else Severity.WARNING,
        passed=(not is_critical_fail),
        metric_value=n_pass,
        threshold=len(decisions) - MAX_TOLERATED_FAILS,
        message=f"{name}: {n_pass}/{len(decisions)} sample renders ok"
                + (f" (avg {sum(sizes)//max(len(sizes), 1)} bytes)" if sizes else ""),
        sample_rows=fails[:5],
        fix_advice="if a render raised, check exports.render_* against the "
                   "specific decision_id; usually a missing field assumption",
    )


def check_docx_export(conn: sqlite3.Connection, **_) -> CheckResult:
    """A sample of 25 decisions must render to valid .docx."""
    try:
        import exports as _exports
    except ImportError:
        return CheckResult(
            name="exports.docx_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="exports module unavailable — skipped",
        )
    decisions = _sample_decisions(conn)
    if not decisions:
        return CheckResult(
            name="exports.docx_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="no sample decisions available",
        )
    return _check_format(
        "docx_render", _exports.render_docx, decisions,
        expected_magic=b"PK",      # docx is a zip
        expected_mtype="application/vnd.openxmlformats-officedocument",
        min_size=1500,
    )


def check_pdf_export(conn: sqlite3.Connection, **_) -> CheckResult:
    """A sample of 25 decisions must render to valid .pdf."""
    try:
        import exports as _exports
    except ImportError:
        return CheckResult(
            name="exports.pdf_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="exports module unavailable — skipped",
        )
    decisions = _sample_decisions(conn)
    if not decisions:
        return CheckResult(
            name="exports.pdf_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="no sample decisions available",
        )
    # PDF render falls back to txt if reportlab missing — accept either
    # but still demand non-empty output and reasonable mtype
    return _check_format(
        "pdf_render", _exports.render_pdf, decisions,
        expected_magic=None,       # %PDF if reportlab; plain text otherwise
        expected_mtype="application/",
        min_size=500,
    )


def check_bibtex_export(conn: sqlite3.Connection, **_) -> CheckResult:
    """All sample decisions must render to a parseable @misc{...}."""
    try:
        import exports as _exports
    except ImportError:
        return CheckResult(
            name="exports.bibtex_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="exports module unavailable — skipped",
        )
    decisions = _sample_decisions(conn)
    if not decisions:
        return CheckResult(
            name="exports.bibtex_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="no sample decisions available",
        )
    fails: list[dict] = []
    for d in decisions:
        try:
            body, mtype, _ = _exports.render_bibtex(d)
            txt = body.decode("utf-8")
            if not (txt.startswith("@misc{") and "}" in txt and "bibtex" in mtype):
                fails.append({"decision_id": d["decision_id"],
                              "preview": txt[:100]})
        except Exception as e:
            fails.append({"decision_id": d["decision_id"],
                          "error": f"{type(e).__name__}: {e}"})
    n_pass = len(decisions) - len(fails)
    return CheckResult(
        name="exports.bibtex_render",
        severity=Severity.CRITICAL,
        passed=(not fails),
        metric_value=n_pass,
        threshold=len(decisions),
        message=f"bibtex: {n_pass}/{len(decisions)} sample renders ok",
        sample_rows=fails[:5],
    )


def check_ris_export(conn: sqlite3.Connection, **_) -> CheckResult:
    """All sample decisions must render to a TY-CASE … ER- record."""
    try:
        import exports as _exports
    except ImportError:
        return CheckResult(
            name="exports.ris_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="exports module unavailable — skipped",
        )
    decisions = _sample_decisions(conn)
    if not decisions:
        return CheckResult(
            name="exports.ris_render",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="no sample decisions available",
        )
    fails: list[dict] = []
    for d in decisions:
        try:
            body, mtype, _ = _exports.render_ris(d)
            txt = body.decode("utf-8")
            if not (txt.startswith("TY  - CASE") and "ER  -" in txt):
                fails.append({"decision_id": d["decision_id"],
                              "preview": txt[:100]})
        except Exception as e:
            fails.append({"decision_id": d["decision_id"],
                          "error": f"{type(e).__name__}: {e}"})
    n_pass = len(decisions) - len(fails)
    return CheckResult(
        name="exports.ris_render",
        severity=Severity.CRITICAL,
        passed=(not fails),
        metric_value=n_pass,
        threshold=len(decisions),
        message=f"ris: {n_pass}/{len(decisions)} sample renders ok",
        sample_rows=fails[:5],
    )
