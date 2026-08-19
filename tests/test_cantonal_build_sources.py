"""Which source each canton's laws come from, and when to say so loudly.

`build()` composes the cantonal corpus by choosing ONE file per canton:
the direct shard scraped from the canton's own portal if there is one,
the LexFind PDF fallback otherwise. The choice is wholesale, so a direct
shard that exists but is truncated does not merge with the fallback — it
replaces it. On 2026-08-19 a stale 3-law ZH shard displaced 1,374 LexFind
laws and flipped 22 other cantons to the fallback, and the only visible
symptom was the total going UP (15,608 -> 28,957), because the fallback
indexes more instruments than the portals do.

These tests pin the selection rule and the warning that makes a
suspicious override audible.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from search_stack.build_cantonal_laws_db import build  # noqa: E402


def _law(canton, sr, title, pad=0):
    return {"canton": canton, "sr_number": sr, "title": title,
            "language": "de", "full_text": "x" * pad,
            "articles": [{"article_num": "1", "heading": title,
                          "text": "Der Text."}]}


def _shard(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def _laws_in(db: Path):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return {(r[0], r[1]): r[2] for r in
                con.execute("SELECT canton, sr_number, title FROM laws")}
    finally:
        con.close()


def _build(tmp_path):
    db = tmp_path / "out" / "cantonal_laws.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    build(tmp_path / "direct", tmp_path / "lexfind", db)
    return db


def test_a_canton_without_a_direct_shard_falls_back_to_lexfind(tmp_path):
    """JU, SZ and VD have no portal scraper; losing them is the failure
    mode of pointing --input-lexfind at an empty directory."""
    _shard(tmp_path / "lexfind" / "JU.jsonl", [_law("JU", "173.11", "Loi")])
    _shard(tmp_path / "direct" / "ZG.jsonl", [_law("ZG", "231.1", "EG SchKG")])
    laws = _laws_in(_build(tmp_path))
    assert ("JU", "173.11") in laws, "the fallback is the only source for JU"
    assert ("ZG", "231.1") in laws


def test_direct_replaces_lexfind_wholesale_for_the_same_canton(tmp_path):
    """Not a merge: the LexFind row for the same canton does not survive
    alongside the direct one, even under a different sr_number."""
    _shard(tmp_path / "lexfind" / "ZH.jsonl",
           [_law("ZH", "554.5", "Hundegesetz (LexFind)"),
            _law("ZH", "170.4", "Gemeindegesetz (LexFind)")])
    _shard(tmp_path / "direct" / "ZH.jsonl",
           [_law("ZH", "554.5", "Hundegesetz (Portal)")])
    laws = _laws_in(_build(tmp_path))
    assert laws[("ZH", "554.5")] == "Hundegesetz (Portal)"
    assert ("ZH", "170.4") not in laws, \
        "the override is wholesale — this is why a stale shard loses laws"


def test_a_direct_shard_shadowing_a_much_larger_fallback_warns(tmp_path, caplog):
    """The 2026-08-19 signature: a tiny direct shard displacing a large
    fallback. Legitimate when the portal is simply more selective, so it
    warns rather than aborts — but it must not pass in silence."""
    _shard(tmp_path / "lexfind" / "ZH.jsonl",
           [_law("ZH", f"1.{i}", f"Gesetz {i}", pad=2000) for i in range(50)])
    _shard(tmp_path / "direct" / "ZH.jsonl", [_law("ZH", "554.5", "Portal")])
    with caplog.at_level("WARNING"):
        _build(tmp_path)
    assert any("shadows" in r.getMessage() for r in caplog.records), \
        "a silent truncation is the bug"


def test_a_comparable_direct_shard_does_not_warn(tmp_path, caplog):
    _shard(tmp_path / "lexfind" / "ZG.jsonl", [_law("ZG", "231.1", "A", 500)])
    _shard(tmp_path / "direct" / "ZG.jsonl", [_law("ZG", "231.1", "B", 500)])
    with caplog.at_level("WARNING"):
        _build(tmp_path)
    assert not [r for r in caplog.records if "shadows" in r.getMessage()]
