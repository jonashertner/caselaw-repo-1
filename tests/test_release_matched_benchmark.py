import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.run_release_matched_benchmark import _compare_profile, _profile_db


def test_profile_db_ignores_invalid_dates_and_counts_languages(tmp_path):
    db_path = tmp_path / "decisions.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE decisions (
                decision_id TEXT PRIMARY KEY,
                court TEXT,
                decision_date TEXT,
                language TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO decisions(decision_id, court, decision_date, language) VALUES (?, ?, ?, ?)",
            [
                ("d1", "bger", "2026-03-17", "de"),
                ("d2", "bvger", "2026-03-01", "fr"),
                ("d3", "bger", "0000-00-00", "de"),
                ("d4", "weko", "", "it"),
                ("d5", "weko", "0000-01-01", "de"),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    profile = _profile_db(db_path)
    assert profile["decisions"] == 5
    assert profile["court_count"] == 3
    assert profile["date_range"] == {"earliest": "2026-03-01", "latest": "2026-03-17"}
    assert profile["by_language"] == {"de": 3, "fr": 1, "it": 1}


def test_compare_profile_reports_all_key_mismatches():
    expected = {
        "decisions": 962272,
        "court_count": 101,
        "date_range": {"earliest": "1875-01-01", "latest": "2026-03-17"},
        "by_language": {"de": 448215, "fr": 434470, "it": 79587},
    }
    actual = {
        "decisions": 1078177,
        "court_count": 93,
        "date_range": {"earliest": "0000-00-00", "latest": "2026-03-09"},
        "by_language": {"de": 571726, "fr": 423660, "it": 82791},
    }
    mismatches = _compare_profile(expected, actual)
    fields = {item["field"] for item in mismatches}
    assert "decisions" in fields
    assert "court_count" in fields
    assert "date_range.earliest" in fields
    assert "date_range.latest" in fields
    assert "by_language.de" in fields
    assert "by_language.fr" in fields
    assert "by_language.it" in fields
