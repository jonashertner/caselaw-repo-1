"""Offline tests for the additive representation dual-count in generate_stats.

Guarantees: `total` is never renamed or dropped; the unique-decision keys appear
only when a generation-matched manifest sidecar is present; a stale sidecar
withholds the number; an absent sidecar leaves `total` untouched.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import generate_stats as gs  # noqa: E402


def _decisions_db(path: Path, n: int, user_version: int):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE decisions (decision_id TEXT, court TEXT, canton TEXT,"
                 " decision_date TEXT, scraped_at TEXT, language TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?)",
                     [(f"d{i}", "bger", "CH", "2020-01-01", "2020-01-01", "de") for i in range(n)])
    conn.execute(f"PRAGMA user_version = {user_version}")
    conn.commit()
    conn.close()


def _manifest(path: Path, meta: dict):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE manifest_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany("INSERT INTO manifest_meta VALUES (?,?)", list(meta.items()))
    conn.commit()
    conn.close()


def _conn(db):
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def test_absent_manifest_yields_no_extra_keys(tmp_path):
    db = tmp_path / "decisions.db"
    _decisions_db(db, 100, 7)
    out = gs._representation_dual_count(db, _conn(db))
    assert out == {}


def _meta(total, dup, band=2, algo="2026-07-23.1"):
    return {
        "algo_version": algo,
        "source_user_version": "1",
        "source_total_rows": str(total),
        "duplicate_representations": str(dup),
        "band_unlinked_date_disagree": str(band),
        "estimated_unique_decisions": str(total - dup),
        "estimated_unique_lower_bound": str(total - dup - band),
    }


def test_fresh_manifest_emits_unique_keys(tmp_path):
    db = tmp_path / "decisions.db"
    _decisions_db(db, 100, 7)
    _manifest(tmp_path / "representation_manifest.db", _meta(100, 8, band=2))
    out = gs._representation_dual_count(db, _conn(db))
    assert out["source_representations"] == 100
    assert out["unique_decisions"] == 92          # 100 - 8
    assert out["unique_decisions_lower_bound"] == 90
    assert out["duplicate_representations"] == 8
    assert out["unique_decisions_status"] == "current"
    assert out["representation_method_version"] == "2026-07-23.1"


def test_small_drift_within_tolerance_tracks_live_total(tmp_path):
    # Live corpus grew +10 since the manifest (a daytime incremental). The count
    # stays current and tracks the live total (new rows are singletons).
    db = tmp_path / "decisions.db"
    _decisions_db(db, 110, 9)
    _manifest(tmp_path / "representation_manifest.db", _meta(100, 8))
    out = gs._representation_dual_count(db, _conn(db))
    assert out["unique_decisions_status"] == "current"
    assert out["source_representations"] == 110
    assert out["unique_decisions"] == 102          # 110 - 8, dup count stable
    # invariant: unique + duplicates == live total
    assert out["unique_decisions"] + out["duplicate_representations"] == 110


def test_large_drift_withholds_unique_number(tmp_path):
    # Corpus moved far past the manifest -> stale, number withheld (fail-closed).
    db = tmp_path / "decisions.db"
    _decisions_db(db, 100, 9)
    _manifest(tmp_path / "representation_manifest.db", _meta(100 + gs._MANIFEST_DRIFT_TOLERANCE + 1, 8))
    out = gs._representation_dual_count(db, _conn(db))
    assert out["unique_decisions_status"] == "stale"
    assert "unique_decisions" not in out
    assert out["source_representations"] == 100


def test_generate_stats_keeps_total_and_adds_dual_count(tmp_path):
    db = tmp_path / "decisions.db"
    _decisions_db(db, 50, 3)
    _manifest(tmp_path / "representation_manifest.db", _meta(50, 5, band=1))
    stats = gs.generate_stats(db)
    assert stats["total"] == 50               # never renamed / dropped
    assert stats["unique_decisions"] == 45
    assert stats["source_representations"] == 50
