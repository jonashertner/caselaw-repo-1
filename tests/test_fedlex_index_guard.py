"""Build hygiene (2026-09-03 review): scrapers/fedlex.py wrote laws.json in
place with no floor check, so a SPARQL run that came back short would have
shrunk the statute index for the month (and build_statutes_db then dropped
the missing laws from statutes.db). The index is now written via
laws.json.tmp + os.replace, the previous file is kept as laws.json.prev, and
a full crawl refuses to shrink below 90 % of the previous entries unless
FEDLEX_ALLOW_SHRINK=1.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers import fedlex


def _entries(n: int) -> list[dict]:
    return [{"sr_number": f"1{i:03d}", "work_uri": f"https://fedlex.data.admin.ch/eli/cc/2000/{i}"}
            for i in range(n)]


def test_first_write_creates_index_without_prev(tmp_path: Path):
    p = tmp_path / "laws.json"
    fedlex._write_law_index(p, _entries(10), full_crawl=True)
    assert json.loads(p.read_text()) == _entries(10)
    assert not (tmp_path / "laws.json.prev").exists()
    assert not (tmp_path / "laws.json.tmp").exists()


def test_rewrite_keeps_previous_as_prev(tmp_path: Path):
    p = tmp_path / "laws.json"
    fedlex._write_law_index(p, _entries(10), full_crawl=True)
    fedlex._write_law_index(p, _entries(11), full_crawl=True)
    assert len(json.loads(p.read_text())) == 11
    assert len(json.loads((tmp_path / "laws.json.prev").read_text())) == 10
    assert not (tmp_path / "laws.json.tmp").exists()


def test_full_crawl_shrink_below_90_percent_exits_2(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("FEDLEX_ALLOW_SHRINK", raising=False)
    p = tmp_path / "laws.json"
    fedlex._write_law_index(p, _entries(100), full_crawl=True)
    before = p.read_text()
    with pytest.raises(SystemExit) as exc:
        fedlex._write_law_index(p, _entries(89), full_crawl=True)
    assert exc.value.code == 2
    assert p.read_text() == before
    assert not (tmp_path / "laws.json.tmp").exists()
    # 90 exactly is allowed
    fedlex._write_law_index(p, _entries(90), full_crawl=True)
    assert len(json.loads(p.read_text())) == 90


def test_shrink_allowed_with_env(tmp_path: Path, monkeypatch):
    p = tmp_path / "laws.json"
    fedlex._write_law_index(p, _entries(100), full_crawl=True)
    monkeypatch.setenv("FEDLEX_ALLOW_SHRINK", "1")
    fedlex._write_law_index(p, _entries(5), full_crawl=True)
    assert len(json.loads(p.read_text())) == 5
    assert len(json.loads((tmp_path / "laws.json.prev").read_text())) == 100


def test_partial_crawl_is_not_guarded(tmp_path: Path, monkeypatch):
    # --sr / --top rewrite a smaller index by design; the statutes floor
    # check is the backstop for that.
    monkeypatch.delenv("FEDLEX_ALLOW_SHRINK", raising=False)
    p = tmp_path / "laws.json"
    fedlex._write_law_index(p, _entries(100), full_crawl=True)
    fedlex._write_law_index(p, _entries(1), full_crawl=False)
    assert len(json.loads(p.read_text())) == 1


def test_unreadable_previous_index_skips_guard(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("FEDLEX_ALLOW_SHRINK", raising=False)
    p = tmp_path / "laws.json"
    p.write_text("{not json")
    fedlex._write_law_index(p, _entries(3), full_crawl=True)
    assert len(json.loads(p.read_text())) == 3
