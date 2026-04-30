"""Quality-check runner — discovers, executes, aggregates.

Discovery: every module in `quality.checks.*` is imported. Every
top-level function whose name starts with `check_` is treated as a
check. Each check accepts `(conn: sqlite3.Connection, **ctx)` and
returns a `CheckResult` (or a list of `CheckResult`s for per-court
fan-out).

Concurrency: runs checks in a thread pool (max 4 workers since the
SQLite read-only WAL is the bottleneck). Each check opens its own
read-only connection via `_open_db()`; they don't share a cursor.

Output: writes `quality/reports/latest.json` + dated archive,
optionally appends to `quality/history.db` for drift detection.
"""
from __future__ import annotations

import importlib
import logging
import pkgutil
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from quality.types import CheckResult, CheckRunReport, Severity

logger = logging.getLogger(__name__)

DEFAULT_DB = Path("output/decisions.db")
DEFAULT_REPORT_DIR = Path("quality/reports")
MAX_WORKERS = 4


def _open_db(db_path: Path) -> sqlite3.Connection:
    """Open the corpus DB read-only, immutable=1 for safe concurrent reads."""
    conn = sqlite3.connect(
        f"file:{db_path}?mode=ro&immutable=1", uri=True,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def discover_checks() -> list[Callable]:
    """Find every check_* function in quality.checks.*.

    Order is deterministic: alphabetical by module, then function name.
    A check function may return either a single CheckResult or an
    iterable of them (for per-court fan-out).
    """
    import quality.checks as checks_pkg
    found: list[tuple[str, Callable]] = []
    for _, name, _ in pkgutil.iter_modules(checks_pkg.__path__):
        mod = importlib.import_module(f"quality.checks.{name}")
        for attr_name in sorted(dir(mod)):
            if not attr_name.startswith("check_"):
                continue
            fn = getattr(mod, attr_name)
            if callable(fn) and getattr(fn, "_qc_check", True):
                found.append((f"{name}.{attr_name[len('check_'):]}", fn))
    found.sort(key=lambda t: t[0])
    return [fn for _, fn in found]


def _run_one(fn: Callable, db_path: Path, ctx: dict) -> list[CheckResult]:
    """Execute a single check. Wrap any exception as a CRITICAL failure
    so a buggy check never silently disappears from the report."""
    name = f"{fn.__module__.split('.')[-1]}.{fn.__name__[len('check_'):]}"
    conn = None
    try:
        conn = _open_db(db_path)
        out = fn(conn, **ctx)
        if isinstance(out, CheckResult):
            return [out]
        if out is None:
            return []
        results = list(out)
        if not all(isinstance(r, CheckResult) for r in results):
            raise TypeError(f"check {name} returned non-CheckResult")
        return results
    except Exception as e:
        logger.exception("check %s raised", name)
        return [CheckResult(
            name=name,
            severity=Severity.CRITICAL,
            passed=False,
            metric_value=-1,
            threshold=None,
            message=f"check raised exception: {type(e).__name__}: {e}",
            fix_advice="investigate the check itself; bug in the QC code",
        )]
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def run(
    db_path: Path | str = DEFAULT_DB,
    only: Iterable[str] | None = None,
    critical_only: bool = False,
    record_history: bool = True,
    parallel: bool = True,
) -> CheckRunReport:
    """Run all (or filtered) checks and return aggregate report.

    Args:
        db_path: SQLite corpus DB. Read-only.
        only: If set, only run checks whose dotted name matches one
              of these prefixes. e.g. ["dates", "schema.not_null"].
        critical_only: Skip WARNING + INFO checks. Used by the publish
              gate to keep gate runtime tight.
        record_history: Append measurements to quality/history.db for
              drift detection. Disable in tests / dry runs.
        parallel: Run checks in a thread pool. Disable for debugging.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"corpus DB not found: {db_path}")

    checks = discover_checks()
    if only:
        prefixes = list(only)

        def matches(check_fn: Callable) -> bool:
            n = f"{check_fn.__module__.split('.')[-1]}.{check_fn.__name__[len('check_'):]}"
            return any(n == p or n.startswith(p + ".") or n.split(".")[0] == p
                       for p in prefixes)

        checks = [c for c in checks if matches(c)]

    ctx: dict = {"critical_only": critical_only}
    started = time.monotonic()
    results: list[CheckResult] = []

    if parallel and len(checks) > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(_run_one, fn, db_path, ctx) for fn in checks]
            for fut in as_completed(futures):
                results.extend(fut.result())
    else:
        for fn in checks:
            results.extend(_run_one(fn, db_path, ctx))

    if critical_only:
        results = [r for r in results if r.severity is Severity.CRITICAL]

    duration = time.monotonic() - started
    report = CheckRunReport(
        run_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        db_path=str(db_path),
        duration_seconds=round(duration, 2),
        results=sorted(results, key=lambda r: (r.severity, r.name), reverse=True),
    )

    if record_history:
        try:
            from quality.baseline import append_measurements
            append_measurements(report)
        except Exception:
            logger.exception("history append failed (non-fatal)")

    return report


def write_report(report: CheckRunReport, out_dir: Path | str = DEFAULT_REPORT_DIR) -> Path:
    """Persist `latest.json` + dated archive. Returns the dated path."""
    import json

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dated = out_dir / f"{report.run_at[:10]}.json"
    latest = out_dir / "latest.json"
    payload = report.to_dict()
    for p in (dated, latest):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    return dated
