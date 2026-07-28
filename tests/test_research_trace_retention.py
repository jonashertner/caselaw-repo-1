"""30-day search-trace retention (/datenschutz/ commitment, 2026-07).

The rerank trace persists query[:200]; retention was unbounded (564 MB /
123 daily files since April). _prune_search_traces deletes trace files
older than RESEARCH_TRACE_RETENTION_DAYS, keyed on the FILENAME date
(never mtime), once per day per process, and can never touch
daily_metrics.jsonl or non-conforming names.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _setup(tmp_path, monkeypatch, files):
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)
    monkeypatch.setattr(m, "_last_trace_prune_day", None)
    for name in files:
        (tmp_path / name).write_text("{}\n")


def test_deletes_traces_older_than_retention(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch, [
        "search_traces_2026-04-01.jsonl",   # 118 days old → gone
        "search_traces_2026-06-20.jsonl",   # 38 days old → gone
        "search_traces_2026-07-05.jsonl",   # 23 days old → kept
        "search_traces_2026-07-28.jsonl",   # today → kept
    ])
    m._prune_search_traces("2026-07-28")
    left = sorted(f.name for f in tmp_path.iterdir())
    assert left == ["search_traces_2026-07-05.jsonl",
                    "search_traces_2026-07-28.jsonl"]


def test_daily_metrics_and_nonconforming_names_survive(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch, [
        "daily_metrics.jsonl",
        "search_traces_notadate.jsonl",
        "search_traces_2026-01-01.jsonl.bak",
        "search_traces_2026-01-01.jsonl",   # only this one conforms + is old
    ])
    m._prune_search_traces("2026-07-28")
    left = sorted(f.name for f in tmp_path.iterdir())
    assert left == ["daily_metrics.jsonl",
                    "search_traces_2026-01-01.jsonl.bak",
                    "search_traces_notadate.jsonl"]


def test_runs_once_per_day_per_process(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch, ["search_traces_2026-01-01.jsonl"])
    m._prune_search_traces("2026-07-28")
    # recreate the old file; the second same-day call must be a no-op
    (tmp_path / "search_traces_2026-01-01.jsonl").write_text("{}\n")
    m._prune_search_traces("2026-07-28")
    assert (tmp_path / "search_traces_2026-01-01.jsonl").exists()
    # a NEW day prunes again
    m._prune_search_traces("2026-07-29")
    assert not (tmp_path / "search_traces_2026-01-01.jsonl").exists()


def test_retention_zero_disables_pruning(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch, ["search_traces_2020-01-01.jsonl"])
    monkeypatch.setattr(m, "_RESEARCH_TRACE_RETENTION_DAYS", 0)
    m._prune_search_traces("2026-07-28")
    assert (tmp_path / "search_traces_2020-01-01.jsonl").exists()


def test_prune_failure_never_breaks_logging(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch, [])
    monkeypatch.setattr(m, "_RESEARCH_LOG_DIR", tmp_path)

    def boom(*a, **k):
        raise OSError("disk went away")

    monkeypatch.setattr(Path, "glob", boom)
    m._prune_search_traces("2026-07-28")  # must not raise
    # and the trace writer still writes
    m._log_search_trace({"query_len": 5})
    files = list(tmp_path.iterdir())
    assert any(f.name.startswith("search_traces_") for f in files)


def test_writer_calls_prune():
    src = Path(REPO / "mcp_server.py").read_text(encoding="utf-8")
    i = src.index("def _log_search_trace")
    assert "_prune_search_traces(day)" in src[i:i + 800]
