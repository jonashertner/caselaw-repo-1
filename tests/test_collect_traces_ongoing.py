"""Ongoing trace export must be exactly-once per source day (2026-08-24).

The daily collector now passes --include-traces, so collect_traces() runs
every night instead of once by hand. Shards are named by COLLECTION date
inside a per-SOURCE-day directory, so a naive re-run would re-export the
whole 30-day retention window nightly — ~280k records duplicated ~30× a
month. Two skips make it idempotent, and they are what these tests pin:

* a source day that already has a shard is never re-exported;
* today's file is still being appended to by the 8 workers, so a day is
  exported only after it closes.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.collect_dev_data as cdd  # noqa: E402


def _setup(tmp_path, days):
    # collect_traces reads REPO/"output"/"research_logs" — the "output"
    # level is load-bearing and an earlier version of this fixture omitted it.
    logs = tmp_path / "output" / "research_logs"
    logs.mkdir(parents=True, exist_ok=True)
    for day in days:
        (logs / f"search_traces_{day}.jsonl").write_text(
            '{"type":"rerank","query":"x","timestamp":"%sT10:00:00Z"}\n' % day,
            encoding="utf-8")
    (tmp_path / cdd.TRACES_ALLOWED_MARKER).write_text("2026-08-19")


def _shards(tmp_path):
    return sorted(p.name for p in tmp_path.rglob("*.jsonl.gz"))


def test_already_archived_source_day_is_not_reexported(tmp_path, monkeypatch):
    _setup(tmp_path, ["2026-08-20", "2026-08-21"])
    monkeypatch.setattr(cdd, "REPO", tmp_path)

    c = cdd.Collector(tmp_path)
    c.today = "2026-08-22"
    c.collect_traces()
    first = _shards(tmp_path)
    assert len(first) == 2

    c2 = cdd.Collector(tmp_path)
    c2.today = "2026-08-23"      # next night, same source files still present
    c2.collect_traces()
    assert _shards(tmp_path) == first


def test_open_day_waits_until_it_closes(tmp_path, monkeypatch):
    _setup(tmp_path, ["2026-08-22"])
    monkeypatch.setattr(cdd, "REPO", tmp_path)

    c = cdd.Collector(tmp_path)
    c.today = "2026-08-22"       # same day → file still being written
    c.collect_traces()
    assert _shards(tmp_path) == []

    c2 = cdd.Collector(tmp_path)
    c2.today = "2026-08-23"      # closed → exported exactly once
    c2.collect_traces()
    assert len(_shards(tmp_path)) == 1
