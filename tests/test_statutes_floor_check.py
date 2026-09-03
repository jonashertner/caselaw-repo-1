"""Build hygiene (2026-09-03 review): build_statutes_db replaced the live
statutes.db with whatever it produced. A broken laws.json, a partial crawl
(`--sr 220` rewrites laws.json with one entry) or a wrong --fedlex-dir would
have swapped a near-empty DB into production, with no copy of the previous
one. check_floor() now gates the swap and the previous DB is kept as
statutes.db.prev.
"""
from __future__ import annotations

import importlib
import json
import sqlite3
from pathlib import Path

import pytest

import search_stack.build_statutes_db as b

AKN = b.AKN_NS


def _stats(laws: int, articles: int, srs) -> dict:
    return {"laws": laws, "articles": articles, "srs_with_articles": set(srs)}


OLD = _stats(100, 10_000, [f"sr{i}" for i in range(100)])
INDEX = {f"sr{i}" for i in range(100)}


def test_no_previous_db_passes():
    v = b.check_floor(None, _stats(1, 1, ["x"]), {"x"})
    assert v["ok"] and v["problems"] == [] and v["lost"] == []


def test_unchanged_passes():
    v = b.check_floor(OLD, _stats(100, 10_000, OLD["srs_with_articles"]), INDEX)
    assert v["ok"], v


def test_small_growth_and_small_shrink_pass():
    assert b.check_floor(OLD, _stats(101, 10_200, OLD["srs_with_articles"] | {"sr100"}), INDEX | {"sr100"})["ok"]
    assert b.check_floor(OLD, _stats(91, 9_000, OLD["srs_with_articles"]), INDEX)["ok"]


def test_laws_shrink_below_90_percent_fails():
    v = b.check_floor(OLD, _stats(89, 10_000, OLD["srs_with_articles"]), INDEX)
    assert not v["ok"]
    assert any("laws 89 < 90 %" in p for p in v["problems"])


def test_articles_shrink_below_90_percent_fails():
    v = b.check_floor(OLD, _stats(100, 8_999, OLD["srs_with_articles"]), INDEX)
    assert not v["ok"]
    assert any(p.startswith("articles 8999 < 90 %") for p in v["problems"])


def test_growth_beyond_110_percent_plus_50_fails():
    # 100 laws -> 160 is fine (110 % + 50 = 160), 161 is a wrong directory
    assert b.check_floor(OLD, _stats(160, 10_000, OLD["srs_with_articles"]), INDEX)["ok"]
    v = b.check_floor(OLD, _stats(161, 10_000, OLD["srs_with_articles"]), INDEX)
    assert not v["ok"]
    assert any("wrong --fedlex-dir" in p for p in v["problems"])


def test_lost_srs_threshold():
    kept = set(sorted(OLD["srs_with_articles"])[5:])  # 5 lost
    v = b.check_floor(OLD, _stats(100, 9_500, kept), INDEX)
    assert v["ok"] and len(v["lost"]) == 5
    kept = set(sorted(OLD["srs_with_articles"])[6:])  # 6 lost
    v = b.check_floor(OLD, _stats(100, 9_500, kept), INDEX)
    assert not v["ok"] and len(v["lost"]) == 6
    assert any("6 SRs still indexed but now without articles" in p for p in v["problems"])


def test_abrogated_srs_are_not_lost():
    # 20 SRs left laws.json (abrogated): they are not in new_index_srs, so
    # their disappearance is expected, not a loss.
    abrogated = set(sorted(OLD["srs_with_articles"])[:20])
    kept = OLD["srs_with_articles"] - abrogated
    v = b.check_floor(OLD, _stats(80, 9_000, kept), INDEX - abrogated)
    assert v["lost"] == []
    # laws 80 < 90 still fails (it is a real 20 % shrink of the corpus)
    assert not v["ok"]


def test_allow_shrink_waives_shrink_and_lost_but_not_growth():
    kept = set(sorted(OLD["srs_with_articles"])[10:])
    v = b.check_floor(OLD, _stats(50, 5_000, kept), INDEX, allow_shrink=True)
    assert v["ok"] and v["problems"] == []
    assert len(v["waived"]) == 3 and len(v["lost"]) == 10
    v = b.check_floor(OLD, _stats(500, 50_000, OLD["srs_with_articles"]), INDEX, allow_shrink=True)
    assert not v["ok"]


# ── end to end ───────────────────────────────────────────────────────────────

def _doc(n_articles: int) -> str:
    arts = "".join(
        f'<article eId="art_{i}"><num>Art. {i}</num><paragraph><content><p>Text {i}.</p></content>'
        f"</paragraph></article>"
        for i in range(1, n_articles + 1)
    )
    return f'<?xml version="1.0"?><akomaNtoso xmlns="{AKN}"><act><body>{arts}</body></act></akomaNtoso>'


def _fedlex_dir(tmp_path: Path, srs: list[str]) -> Path:
    fedlex_dir = tmp_path / "fedlex"
    xml_dir = fedlex_dir / "xml"
    xml_dir.mkdir(parents=True, exist_ok=True)
    for sr in srs:
        d = xml_dir / sr.replace(".", "_")
        d.mkdir(exist_ok=True)
        (d / "de.xml").write_text(_doc(10), encoding="utf-8")
    (fedlex_dir / "laws.json").write_text(json.dumps([
        {"sr_number": sr, "title_de": f"Gesetz {sr}", "consolidation_date": "2026-01-01"} for sr in srs
    ]))
    return fedlex_dir


def _builder(monkeypatch, fedlex_dir: Path, db_path: Path):
    import search_stack.build_statutes_db as bsd
    importlib.reload(bsd)
    monkeypatch.setattr(bsd, "FEDLEX_DIR", fedlex_dir)
    monkeypatch.setattr(bsd, "OUTPUT_DB", db_path)
    monkeypatch.delenv("STATUTES_ALLOW_SHRINK", raising=False)
    return bsd


def _law_count(db_path: Path) -> int:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return con.execute("SELECT COUNT(*) FROM laws").fetchone()[0]
    finally:
        con.close()


def test_second_build_with_half_the_laws_exits_2_and_leaves_old_db(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "statutes.db"
    fedlex_dir = _fedlex_dir(tmp_path, ["220", "210"])
    bsd = _builder(monkeypatch, fedlex_dir, db_path)
    bsd.build_db()  # first build: no baseline, no .prev
    assert _law_count(db_path) == 2
    assert not (tmp_path / "statutes.db.prev").exists()
    before = db_path.read_bytes()

    # the "crawl" now returns one law (what --sr 220 does to laws.json)
    (fedlex_dir / "laws.json").write_text(json.dumps([
        {"sr_number": "220", "title_de": "OR", "consolidation_date": "2026-01-01"}
    ]))
    with pytest.raises(SystemExit) as exc:
        bsd.build_db()
    assert exc.value.code == 2
    assert db_path.read_bytes() == before, "live DB must be untouched"
    assert (tmp_path / "statutes.tmp").exists(), "tmp DB left for inspection"
    assert not (tmp_path / "statutes.db.prev").exists()


def test_allow_shrink_flag_and_env_let_it_through(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "statutes.db"
    fedlex_dir = _fedlex_dir(tmp_path, ["220", "210"])
    bsd = _builder(monkeypatch, fedlex_dir, db_path)
    bsd.build_db()
    (fedlex_dir / "laws.json").write_text(json.dumps([
        {"sr_number": "220", "title_de": "OR", "consolidation_date": "2026-01-01"}
    ]))
    bsd.build_db(allow_shrink=True)
    assert _law_count(db_path) == 1
    assert (tmp_path / "statutes.db.prev").exists()
    assert _law_count(tmp_path / "statutes.db.prev") == 2

    # env var form, back to two laws then down again
    _fedlex_dir(tmp_path, ["220", "210"])
    bsd.build_db()
    (fedlex_dir / "laws.json").write_text(json.dumps([
        {"sr_number": "210", "title_de": "ZGB", "consolidation_date": "2026-01-01"}
    ]))
    monkeypatch.setenv("STATUTES_ALLOW_SHRINK", "1")
    bsd.build_db()
    assert _law_count(db_path) == 1


def test_successful_rebuild_keeps_previous_db_as_prev(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "statutes.db"
    fedlex_dir = _fedlex_dir(tmp_path, ["220", "210"])
    bsd = _builder(monkeypatch, fedlex_dir, db_path)
    bsd.build_db()
    first = db_path.read_bytes()
    bsd.build_db()
    prev = tmp_path / "statutes.db.prev"
    assert prev.exists() and prev.read_bytes() == first
    assert _law_count(db_path) == 2
    assert not (tmp_path / "statutes.tmp").exists()


def test_baseline_option_checks_against_another_db(tmp_path: Path, monkeypatch):
    # Validation-on-a-copy flow: build to a scratch path, floor-check against
    # the production DB.
    prod = tmp_path / "prod.db"
    bsd = _builder(monkeypatch, _fedlex_dir(tmp_path, ["220", "210"]), prod)
    bsd.build_db()

    scratch = tmp_path / "scratch" / "statutes.new.db"
    scratch.parent.mkdir()
    fedlex_dir = _fedlex_dir(tmp_path / "other", ["220"])
    bsd = _builder(monkeypatch, fedlex_dir, scratch)
    with pytest.raises(SystemExit):
        bsd.build_db(baseline=prod)
    assert not scratch.exists()
    # without a baseline the scratch build has nothing to compare against
    bsd.build_db()
    assert _law_count(scratch) == 1


def test_main_accepts_flags(tmp_path: Path, monkeypatch):
    fedlex_dir = _fedlex_dir(tmp_path, ["220"])
    db_path = tmp_path / "statutes.db"
    bsd = _builder(monkeypatch, fedlex_dir, db_path)
    monkeypatch.setattr("sys.argv", [
        "build_statutes_db", "--fedlex-dir", str(fedlex_dir), "--output", str(db_path),
        "--allow-shrink", "--baseline", str(tmp_path / "nonexistent.db"),
    ])
    bsd.main()
    assert _law_count(db_path) == 1
