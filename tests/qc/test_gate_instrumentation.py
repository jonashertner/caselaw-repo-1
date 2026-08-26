"""Per-check timing, the progress trail, and the exports sampler rewrite.

Context (2026-08-26). publish.py Step 5c consumed its full 3600 s cap and
returned DEGRADED, and working out why took a forensic investigation because
the gate leaves nothing behind when it is killed: the CLI runs without
--verbose so logging sits at WARNING, `run()` never returns so no report is
written and no history row is appended, and docs/quality.json still holds the
previous day's verdict. Meanwhile the measured dominant cost was
`quality/checks/exports.py` running

    SELECT ... full_text ... WHERE length(full_text) > 500 ORDER BY random() LIMIT 25

four separate times — a full scan plus a full sort of a 70 GB table, per
format, to keep 25 rows.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from quality import runner  # noqa: E402
from quality.checks import exports as exports_check  # noqa: E402
from quality.types import CheckResult, Severity  # noqa: E402


# ----------------------------------------------------------------- fixtures


def _corpus(tmp_path: Path, n: int = 400, short_every: int = 7) -> Path:
    """A decisions table shaped like the real one for sampling purposes."""
    db = tmp_path / "decisions.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, decision_date TEXT, "
        "docket_number TEXT, language TEXT, regeste TEXT, full_text TEXT)"
    )
    rows = []
    for i in range(1, n + 1):
        short = (i % short_every == 0)
        rows.append((
            f"bger_{i}", "bger", "2026-01-01", f"6B_{i}/2026", "de",
            f"Regeste {i}", ("x" * 40) if short else ("y" * 2000),
        ))
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return db


def _ro(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture(autouse=True)
def _clean_sampler_cache():
    exports_check._reset_sample_cache()
    yield
    exports_check._reset_sample_cache()


# ------------------------------------------------------- the exports sampler


def test_the_sampler_no_longer_full_scans(tmp_path):
    """The query shape is the whole point, so assert it against SQLite's own
    planner rather than by grepping the source."""
    db = _corpus(tmp_path)
    conn = _ro(db)
    plan = conn.execute(
        "EXPLAIN QUERY PLAN " + exports_check._ROW_SQL, (1,)
    ).fetchall()
    detail = " ".join(str(r["detail"]) for r in plan)
    assert "SCAN" not in detail.upper(), detail
    assert "TEMP B-TREE" not in detail.upper(), detail

    # max(rowid) is answered from the rowid B-tree, not by reading the table.
    plan2 = conn.execute(
        "EXPLAIN QUERY PLAN SELECT max(rowid) FROM decisions").fetchall()
    assert "SCAN" not in " ".join(str(r["detail"]) for r in plan2).upper()


def test_sample_returns_only_substantial_rows(tmp_path):
    db = _corpus(tmp_path)
    conn = _ro(db)
    rows = exports_check._sample_decisions(conn, slot=0, db_path=str(db))
    assert rows, "sampler returned nothing on a healthy corpus"
    for r in rows:
        assert len(r["full_text"]) > exports_check.MIN_TEXT_CHARS
    # and it carries the columns the renderers need
    for key in ("decision_id", "court", "court_name", "decision_date",
                "docket_number", "language", "regeste", "citation_string_de"):
        assert key in rows[0], key


def test_the_four_formats_get_disjoint_rows(tmp_path):
    """Each format used to draw its own independent 25. Preserve that
    coverage — one pool, four non-overlapping slices."""
    db = _corpus(tmp_path)
    conn = _ro(db)
    slices = [
        {r["decision_id"] for r in
         exports_check._sample_decisions(conn, slot=i, db_path=str(db))}
        for i in range(4)
    ]
    for s in slices:
        assert len(s) == exports_check.SAMPLE_SIZE
    union = set().union(*slices)
    assert len(union) == sum(len(s) for s in slices), "slices overlap"


def test_the_pool_is_drawn_once_for_all_four_formats(tmp_path):
    """Four full scans became one cheap draw; if the memo breaks, the cost
    silently quadruples again."""
    db = _corpus(tmp_path)
    conn = _ro(db)
    calls = {"n": 0}
    real = exports_check._draw_pool

    def counting(c):
        calls["n"] += 1
        return real(c)

    exports_check._draw_pool = counting
    try:
        for i in range(4):
            exports_check._sample_decisions(conn, slot=i, db_path=str(db))
    finally:
        exports_check._draw_pool = real
    assert calls["n"] == 1, f"pool drawn {calls['n']} times, expected 1"


def test_a_thin_corpus_degrades_evenly_across_formats(tmp_path):
    """Interleaved slicing, not block slicing: a short pool must not starve
    the last two formats down to zero."""
    db = _corpus(tmp_path, n=12, short_every=100)
    conn = _ro(db)
    counts = [len(exports_check._sample_decisions(conn, slot=i, db_path=str(db)))
              for i in range(4)]
    assert min(counts) >= 2, counts
    assert max(counts) - min(counts) <= 1, counts


def test_sampling_is_reproducible_within_a_day(tmp_path):
    db = _corpus(tmp_path)
    conn = _ro(db)
    first = [r["decision_id"] for r in
             exports_check._sample_decisions(conn, slot=0, db_path=str(db))]
    exports_check._reset_sample_cache()
    second = [r["decision_id"] for r in
              exports_check._sample_decisions(conn, slot=0, db_path=str(db))]
    assert first == second


def test_empty_corpus_is_handled(tmp_path):
    db = tmp_path / "empty.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE decisions (decision_id TEXT, court TEXT, "
                 "decision_date TEXT, docket_number TEXT, language TEXT, "
                 "regeste TEXT, full_text TEXT)")
    conn.commit()
    conn.close()
    assert exports_check._sample_decisions(_ro(db), slot=0, db_path=str(db)) == []


# --------------------------------------------------------- per-check timing


def _ok_result(name="t.ok"):
    return CheckResult(name=name, severity=Severity.INFO, passed=True,
                       metric_value=1, threshold=None, message="ok")


def test_results_carry_their_own_elapsed(tmp_path):
    db = _corpus(tmp_path, n=5)

    def check_slow(conn, **_):
        time.sleep(0.05)
        return _ok_result()
    check_slow.__module__ = "quality.checks.fake"

    out = runner._run_one(check_slow, db, {})
    assert len(out) == 1
    assert out[0].elapsed_s is not None
    assert out[0].elapsed_s >= 0.05


def test_fanned_out_results_all_carry_the_call_cost(tmp_path):
    db = _corpus(tmp_path, n=5)

    def check_fan(conn, **_):
        return [_ok_result(f"t.{i}") for i in range(3)]
    check_fan.__module__ = "quality.checks.fake"

    out = runner._run_one(check_fan, db, {})
    assert len(out) == 3
    assert all(r.elapsed_s is not None for r in out)
    assert len({r.elapsed_s for r in out}) == 1


def test_a_raising_check_still_gets_timed(tmp_path):
    db = _corpus(tmp_path, n=5)

    def check_boom(conn, **_):
        raise ValueError("nope")
    check_boom.__module__ = "quality.checks.fake"

    out = runner._run_one(check_boom, db, {})
    assert len(out) == 1 and not out[0].passed
    assert out[0].elapsed_s is not None


def test_elapsed_survives_serialisation():
    r = _ok_result()
    r.elapsed_s = 1.25
    assert r.to_dict()["elapsed_s"] == 1.25


# ---------------------------------------------------------- progress trail


def test_progress_trail_records_start_and_finish(tmp_path):
    """A killed gate leaves no report at all. The trail is what tells you
    which check was in flight when the cap fired."""
    db = _corpus(tmp_path, n=5)
    reports = tmp_path / "reports"
    runner._progress_begin(reports)

    def check_a(conn, **_):
        return _ok_result("a.one")
    check_a.__module__ = "quality.checks.fake"

    runner._run_one(check_a, db, {})
    lines = [json.loads(x) for x in
             (reports / runner.PROGRESS_FILENAME).read_text().splitlines() if x]
    assert [l["event"] for l in lines] == ["start", "done"]
    assert lines[0]["check"] == "fake.a"
    assert lines[1]["elapsed_s"] >= 0
    assert lines[1]["n_results"] == 1


def test_an_in_flight_check_is_identifiable(tmp_path):
    """The diagnostic that did not exist: start-without-done names the
    check that was running when the process was killed."""
    db = _corpus(tmp_path, n=5)
    reports = tmp_path / "reports"
    runner._progress_begin(reports)
    trail = reports / runner.PROGRESS_FILENAME

    def check_peek(conn, **_):
        # observe the trail from *inside* the check, i.e. mid-flight
        seen = [json.loads(x) for x in trail.read_text().splitlines() if x]
        assert [s["event"] for s in seen] == ["start"]
        assert seen[0]["check"] == "fake.peek"
        return _ok_result("peek")
    check_peek.__module__ = "quality.checks.fake"

    runner._run_one(check_peek, db, {})


def test_progress_is_truncated_per_run(tmp_path):
    reports = tmp_path / "reports"
    runner._progress_begin(reports)
    runner._progress("start", "stale.check")
    runner._progress_begin(reports)
    assert (reports / runner.PROGRESS_FILENAME).read_text() == ""


def test_progress_failure_never_breaks_a_check(tmp_path):
    """Best-effort: instrumentation must not be able to fail a gate."""
    db = _corpus(tmp_path, n=5)
    runner._progress_begin(tmp_path / "reports")
    runner._progress_path = tmp_path / "no" / "such" / "dir" / "x.jsonl"

    def check_a(conn, **_):
        return _ok_result("a.one")
    check_a.__module__ = "quality.checks.fake"

    out = runner._run_one(check_a, db, {})   # must not raise
    assert out and out[0].passed


def test_run_writes_no_progress_trail_by_default(tmp_path, monkeypatch):
    """The first version of this wrote quality/reports/gate-progress.jsonl on
    every run() call, so a test suite silently accumulated 17 KB of debris in
    the working tree. The trail is opt-in."""
    db = _corpus(tmp_path, n=5)
    monkeypatch.chdir(tmp_path)
    runner.run(db_path=db, only=["nonexistent_module"], record_history=False)
    assert not (tmp_path / "quality" / "reports").exists()
    assert runner._progress_path is None


def test_run_writes_the_trail_when_asked(tmp_path):
    db = _corpus(tmp_path, n=5)
    out = tmp_path / "reports"
    runner.run(db_path=db, only=["nonexistent_module"], record_history=False,
               progress_dir=out)
    assert (out / runner.PROGRESS_FILENAME).exists()


def test_the_cli_keeps_the_trail_out_of_the_docs_tree():
    """--output docs/quality.json must not put operational debris in the
    published docs directory."""
    src = (REPO / "quality" / "cli.py").read_text(encoding="utf-8")
    assert "progress_dir=runner.DEFAULT_REPORT_DIR" in src
