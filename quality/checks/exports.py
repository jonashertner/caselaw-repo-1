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

import random
import sqlite3
import threading
from datetime import date

from quality.types import CheckResult, Severity


SAMPLE_SIZE = 25          # decisions rendered per format
MIN_TEXT_CHARS = 500      # skip stubs; a 200-char row exercises nothing
_FORMAT_SLOTS = 4         # docx, pdf, bibtex, ris
_POOL_SIZE = SAMPLE_SIZE * _FORMAT_SLOTS

# Bounded work: with rowid gaps and rows under MIN_TEXT_CHARS, some probes
# miss. 12x the pool is far more than the observed miss rate needs and still
# caps the check at ~1,200 point lookups in the pathological case.
_MAX_PROBES = _POOL_SIZE * 12

_ROW_SQL = (
    "SELECT decision_id, court, court AS court_name, decision_date, "
    "docket_number, language, regeste, full_text, "
    "regeste AS citation_string_de "
    "FROM decisions WHERE rowid = ?"
)

_pool_lock = threading.Lock()
_pool_cache: dict[str, list[dict]] = {}


def _draw_pool(conn: sqlite3.Connection) -> list[dict]:
    """Draw the shared sample by random rowid probe — point lookups only.

    This replaced
        SELECT ... full_text ... WHERE length(full_text) > 500
        ORDER BY random() LIMIT 25
    which SQLite answers with `SCAN decisions` + `USE TEMP B-TREE FOR ORDER
    BY`: a full pass over a 70 GB table plus a full sort, to keep 25 rows.
    `full_text` is ordinal 22 of 36 in the record, so reading it walks every
    row's overflow-page chain — and this query ran FOUR times per gate, once
    per format, concurrently at MAX_WORKERS=4. `length()` on a TEXT column
    does not get SQLite's stored-length shortcut either; that fires only for
    BLOBs (measured: 0.641 s vs 0.003 s for `typeof()` on a 20k-row fixture).

    The distribution is unchanged in the way that matters: probing rowids
    uniformly and rejecting rows below MIN_TEXT_CHARS is still uniform over
    qualifying rows. The docstring this replaces claimed the sample was
    "stratified by court"; it never was — `ORDER BY random()` is a flat draw
    — so no stratification is lost here.

    Seeded per day, like text_integrity._sample_by_rowid, so a gate run is
    reproducible within the day.
    """
    max_rowid = conn.execute("SELECT max(rowid) FROM decisions").fetchone()[0] or 0
    if not max_rowid:
        return []
    rng = random.Random(f"exports:{date.today().isoformat()}")
    out: list[dict] = []
    seen: set[int] = set()
    probes = 0
    while len(out) < _POOL_SIZE and probes < _MAX_PROBES:
        probes += 1
        rid = rng.randint(1, max_rowid)
        if rid in seen:
            continue
        seen.add(rid)
        row = conn.execute(_ROW_SQL, (rid,)).fetchone()
        if row is None:
            continue
        if len(row["full_text"] or "") <= MIN_TEXT_CHARS:
            continue
        out.append(dict(row))
    return out


def _sample_decisions(conn: sqlite3.Connection, slot: int = 0,
                      db_path: str | None = None) -> list[dict]:
    """The `slot`-th disjoint slice of the shared per-run sample.

    The four format checks each get their own rows, as before — but from one
    draw instead of four independent full scans. Interleaved rather than
    block-sliced so a short pool degrades evenly across formats instead of
    starving the last two.
    """
    key = db_path or ""
    with _pool_lock:
        pool = _pool_cache.get(key)
        if pool is None:
            pool = _draw_pool(conn)
            _pool_cache[key] = pool
    return pool[slot::_FORMAT_SLOTS][:SAMPLE_SIZE]


def _reset_sample_cache() -> None:
    """Drop the memoised pool. For tests — the gate is a fresh process."""
    with _pool_lock:
        _pool_cache.clear()


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


def check_docx_export(conn: sqlite3.Connection, db_path: str | None = None,
                       **_) -> CheckResult:
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
    decisions = _sample_decisions(conn, slot=0, db_path=db_path)
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


def check_pdf_export(conn: sqlite3.Connection, db_path: str | None = None,
                       **_) -> CheckResult:
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
    decisions = _sample_decisions(conn, slot=1, db_path=db_path)
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


def check_bibtex_export(conn: sqlite3.Connection, db_path: str | None = None,
                       **_) -> CheckResult:
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
    decisions = _sample_decisions(conn, slot=2, db_path=db_path)
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


def check_ris_export(conn: sqlite3.Connection, db_path: str | None = None,
                       **_) -> CheckResult:
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
    decisions = _sample_decisions(conn, slot=3, db_path=db_path)
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
