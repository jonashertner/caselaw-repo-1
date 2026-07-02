"""P3.1: the denominators table — corpus counts + portal totals + curated
notes, distinguishing 'not published' from 'not captured'."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from generate_coverage import build_coverage  # noqa: E402


def test_build_coverage(tmp_path):
    db = tmp_path / "d.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE decisions (decision_id TEXT, court TEXT, decision_date TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?)", [
        ("a", "bger", "2024-01-01"), ("b", "bger", "2024-06-01"),
        ("c", "bger", "2025-02-02"), ("d", "bger", None),
        ("e", "ow_gerichte", "2021-05-05"),
    ])
    conn.commit(); conn.close()
    health = tmp_path / "h.json"
    health.write_text(json.dumps({"run_at": "2026-07-02T02:32:00",
        "scrapers": {"bger": {"our_count": 4, "portal_count": 6, "gap": 2}}}))
    notes = tmp_path / "n.json"
    notes.write_text(json.dumps({"ow_gerichte": "Portal offline since Dec 2022."}))

    cov = build_coverage(db, health, notes)
    assert cov["schema"] == "coverage/v1"
    assert cov["totals"] == {"decisions": 5, "courts": 2,
                             "courts_with_portal_total": 1, "courts_with_note": 1}
    bger = next(c for c in cov["courts"] if c["court"] == "bger")
    assert bger["total"] == 4 and bger["undated"] == 1
    assert bger["by_year"] == {"2024": 2, "2025": 1}
    assert (bger["first_year"], bger["last_year"]) == ("2024", "2025")
    assert bger["portal_total"] == 6 and bger["gap"] == 2
    ow = next(c for c in cov["courts"] if c["court"] == "ow_gerichte")
    assert "offline" in ow["note"]
    assert ow["portal_total"] is None


def test_missing_inputs_are_tolerated(tmp_path):
    db = tmp_path / "d.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE decisions (decision_id TEXT, court TEXT, decision_date TEXT)")
    conn.execute("INSERT INTO decisions VALUES ('a','bger','2024-01-01')")
    conn.commit(); conn.close()
    cov = build_coverage(db, tmp_path / "absent.json", tmp_path / "absent2.json")
    assert cov["totals"]["decisions"] == 1
    assert cov["courts"][0]["portal_total"] is None
