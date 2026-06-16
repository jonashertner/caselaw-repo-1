"""enrich_quality is READ-ONLY post-FTS5 (2026-06-16).

Step 2d runs after the atomic swap on the LIVE served decisions.db, so it must
open read-only (mode=ro&immutable=1) and never UPDATE — the old write path
violated the immutable / atomic-swap invariants and lost the lock race against
MCP readers (the recurring "database is locked" that exited the nightly
publish `failed`). It now emits only the (read-only) dedup report.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.enrich_quality as eq  # noqa: E402


def _make_db(path: Path):
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE decisions ("
        "decision_id TEXT, court TEXT, docket_number TEXT, "
        "decision_date TEXT, content_hash TEXT, title TEXT, "
        "regeste TEXT, full_text TEXT)"
    )
    conn.executemany(
        "INSERT INTO decisions (decision_id, court, docket_number, decision_date, "
        "content_hash, title, regeste, full_text) VALUES (?,?,?,?,?,?,?,?)",
        [
            ("a1", "bger", "1C_1/2024", "2024-01-01", "h1", None, None, "Gegenstand: X"),
            ("a2", "bger", "1C_2/2024", "2024-01-02", "h1", None, None, "Gegenstand: Y"),
        ],
    )
    conn.commit()
    conn.close()


def _snapshot(path: Path):
    conn = sqlite3.connect(str(path))
    rows = conn.execute(
        "SELECT decision_id, title, regeste, decision_date, content_hash "
        "FROM decisions ORDER BY decision_id"
    ).fetchall()
    conn.close()
    return rows


def test_enrich_is_read_only_and_emits_dedup(tmp_path):
    db = tmp_path / "decisions.db"
    _make_db(db)
    before = _snapshot(db)

    summary = eq.run(db_path=db, output_dir=tmp_path, dry_run=False)

    # did not raise; the write substeps are disabled (no titles/regeste/dates/
    # hashes keys are produced — they're force-skipped), dedup still runs
    assert "dedup" in summary
    assert "titles" not in summary and "hashes" not in summary
    # read-only: NULL title/regeste rows were left untouched (old code UPDATEd them)
    assert _snapshot(db) == before
    # the read-only dedup report is emitted (h1 is a cross-row content dup)
    assert (tmp_path / "dedup_report.json").exists()
