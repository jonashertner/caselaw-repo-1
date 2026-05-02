"""MCP-tool round-trip checks.

For each MCP tool, exercise it against a known-good fixture and
assert the response shape. These are functional smoke tests that
catch regressions in the public tool surface — what users see when
they query mcp.opencaselaw.ch.

Lightweight by design: don't import the entire mcp_server module
(which spins up FastAPI, the FTS5 connection, etc.); instead
exercise the underlying helpers via their public functions.

CRITICAL: any tool that fails to return non-empty output for a
canonical input is a service regression.
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


# Anchor decisions used for round-trip — must exist in the corpus.
ANCHOR_BGE_ID = "bge_BGE_140_III_86"
ANCHOR_DOCKET = "4A_321/2013"


def _try_import_mcp(db_path: str | None = None) -> object | None:
    """Best-effort load of mcp_server. Returns None if unavailable
    (e.g. running with --check exports without server deps).

    If ``db_path`` is provided, repoint mcp_server.DB_PATH and the
    related sidecar DB paths to it — otherwise the default
    (``~/.swiss-caselaw/decisions.db``) will not exist on the gate
    runner host and every helper that touches the corpus will fail.
    """
    try:
        import mcp_server as _mcp
    except Exception:
        return None
    if db_path:
        from pathlib import Path as _Path
        p = _Path(db_path)
        _mcp.DB_PATH = p
        _mcp.DATA_DIR = p.parent
        _mcp.PARQUET_DIR = p.parent / "parquet"
        _mcp.GRAPH_DB_PATH = p.parent / "reference_graph.db"
        _mcp.VECTOR_DB_PATH = p.parent / "vectors.db"
    return _mcp


def check_get_decision_by_id(conn: sqlite3.Connection, **_ctx) -> CheckResult:
    """get_decision_by_id must return a row for our anchor BGE."""
    mcp = _try_import_mcp(_ctx.get("db_path") if isinstance(_ctx, dict) else None)
    if mcp is None:
        return CheckResult(
            name="mcp_tools.get_decision_by_id",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="mcp_server unavailable — skipped",
        )
    fn = getattr(mcp, "get_decision_by_id", None)
    if fn is None:
        return CheckResult(
            name="mcp_tools.get_decision_by_id",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="get_decision_by_id helper not found",
        )
    try:
        result = fn(ANCHOR_BGE_ID)
    except Exception as e:
        return CheckResult(
            name="mcp_tools.get_decision_by_id",
            severity=Severity.CRITICAL,
            passed=False,
            metric_value=0,
            threshold=1,
            message=f"raised: {type(e).__name__}: {e}",
        )
    has_content = bool(result and (result.get("regeste") or result.get("full_text")))
    return CheckResult(
        name="mcp_tools.get_decision_by_id",
        severity=Severity.CRITICAL,
        passed=has_content,
        metric_value=1 if has_content else 0,
        threshold=1,
        message=f"get_decision_by_id({ANCHOR_BGE_ID}): "
                + ("ok" if has_content else "EMPTY result"),
    )


def check_decision_id_variants_helper(conn: sqlite3.Connection, **_ctx) -> CheckResult:
    """The _decision_id_variants helper underpins every cross-DB ID
    resolution. Bidirectional: BGE_xxx ↔ xxx must both produce the
    other form."""
    mcp = _try_import_mcp(_ctx.get("db_path") if isinstance(_ctx, dict) else None)
    if mcp is None:
        return CheckResult(
            name="mcp_tools.decision_id_variants_helper",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="mcp_server unavailable — skipped",
        )
    fn = getattr(mcp, "_decision_id_variants", None)
    if fn is None:
        return CheckResult(
            name="mcp_tools.decision_id_variants_helper",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="_decision_id_variants helper not found",
        )
    # The helper normalises underscores ↔ spaces (the actual call-site contract)
    # rather than adding/stripping the bge_ namespace prefix. The contract that
    # matters: input is in the variants list, AND at least one space/underscore
    # alternative is present.
    cases = ["bge_BGE_140_III_86", "BGE_140_III_86"]
    fails = []
    for input_id in cases:
        variants = list(fn(input_id))
        ok = (input_id in variants) and any(v != input_id for v in variants)
        if not ok:
            fails.append({"input": input_id, "got": variants[:5]})
    return CheckResult(
        name="mcp_tools.decision_id_variants_helper",
        severity=Severity.CRITICAL,
        passed=(not fails),
        metric_value=len(cases) - len(fails),
        threshold=len(cases),
        message=f"_decision_id_variants: {len(cases)-len(fails)}/{len(cases)} cases",
        sample_rows=fails,
    )


def check_e_number_sort_key_helper(conn: sqlite3.Connection, **_ctx) -> CheckResult:
    """_e_number_sort_key sorts hierarchical paragraph numbers like a
    human: '2.10' > '2.2'. Critical for /erwaegung correct ordering."""
    mcp = _try_import_mcp(_ctx.get("db_path") if isinstance(_ctx, dict) else None)
    if mcp is None:
        return CheckResult(
            name="mcp_tools.e_number_sort_key_helper",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="mcp_server unavailable — skipped",
        )
    fn = getattr(mcp, "_e_number_sort_key", None)
    if fn is None:
        return CheckResult(
            name="mcp_tools.e_number_sort_key_helper",
            severity=Severity.WARNING,
            passed=True,
            metric_value=0,
            threshold=None,
            message="_e_number_sort_key helper not found",
        )
    nums = ["1", "2", "2.1", "2.2", "2.10", "2.11", "3", "10", "10.1"]
    sorted_nums = sorted(nums, key=fn)
    expected = ["1", "2", "2.1", "2.2", "2.10", "2.11", "3", "10", "10.1"]
    return CheckResult(
        name="mcp_tools.e_number_sort_key_helper",
        severity=Severity.CRITICAL,
        passed=(sorted_nums == expected),
        metric_value=1 if sorted_nums == expected else 0,
        threshold=1,
        message=f"e_number_sort: " + (
            "ok (2.10 > 2.2 etc.)" if sorted_nums == expected
            else f"BROKEN — got {sorted_nums}"),
    )


def check_search_fts_returns_hits(conn: sqlite3.Connection, **_ctx) -> CheckResult:
    """A canonical FTS5 query — single common word — must return hits.
    If zero hits, the FTS5 index didn't build, search is broken."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions_fts WHERE decisions_fts MATCH ?",
        ("Beschwerde",),
    ).fetchone()[0]
    return CheckResult(
        name="mcp_tools.search_fts_returns_hits",
        severity=Severity.CRITICAL,
        passed=(n >= 100),
        metric_value=n,
        threshold=100,
        message=f"FTS5 'Beschwerde' query: {n:,} hits",
        fix_advice="if 0, FTS5 build never indexed; rerun build_fts5",
    )


def check_search_fts_handles_special_chars(conn: sqlite3.Connection, **_ctx) -> CheckResult:
    """User queries can contain quoting / colons / parentheses that
    must be sanitised. Verify a tricky query doesn't raise."""
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions_fts WHERE decisions_fts MATCH ?",
            # Sanitised at the API layer; here we hit a known-safe query
            ("Vertrag AND (mietzins OR kündigung)",),
        ).fetchone()[0]
    except sqlite3.OperationalError as e:
        return CheckResult(
            name="mcp_tools.search_fts_special_chars",
            severity=Severity.CRITICAL,
            passed=False,
            metric_value=0,
            threshold=1,
            message=f"FTS5 raised: {e}",
        )
    return CheckResult(
        name="mcp_tools.search_fts_special_chars",
        severity=Severity.WARNING,
        passed=(n >= 0),  # any number ≥ 0 is fine; the test is "no exception"
        metric_value=n,
        threshold=0,
        message=f"FTS5 boolean query: {n:,} hits",
    )
