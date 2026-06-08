"""Recent-publication overlay (closes the ~24h publish-lag for recently-published
BGer decisions; the poller captures them with a publication_date before the
nightly publish ingests them). See the 2026-06-08 recency investigation.

IDs here use the REAL canonical form make_decision_id produces — bger_7B_121_2026
(underscores), docket 7B_121/2026 (slash) — so the canonical-id lookup path is
actually exercised (an earlier version used fake slash-IDs and missed a bug)."""
import json
import sqlite3
from datetime import date
from pathlib import Path

from search_stack.build_recent_overlay import build_overlay, lookup_overlay


def _write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_overlay_selects_only_recent_publications(tmp_path):
    inp = tmp_path / "bger.jsonl"
    _write_jsonl(inp, [
        {"decision_id": "bger_7B_121_2026", "docket_number": "7B_121/2026", "court": "bger",
         "decision_date": "2026-04-29", "publication_date": "2026-06-08",
         "full_text": "recent ruling text", "language": "de", "regeste": "R."},
        {"decision_id": "bger_9C_9_2026", "docket_number": "9C_9/2026", "court": "bger",
         "decision_date": "2026-01-01", "publication_date": "2026-05-01",  # >14d before today
         "full_text": "old publication", "language": "de"},
        {"decision_id": "bger_2C_2_2026", "docket_number": "2C_2/2026", "court": "bger",
         "decision_date": "2026-06-01", "full_text": "no pub date (AZA)", "language": "de"},
    ])
    out = tmp_path / "recent_overlay.db"
    n = build_overlay(inp, out, days=14, today=date(2026, 6, 8))
    assert n == 1
    conn = sqlite3.connect(out)
    rows = conn.execute(
        "SELECT decision_id, docket_number, full_text, decision_date FROM recent_decisions"
    ).fetchall()
    conn.close()
    assert {r[0] for r in rows} == {"bger_7B_121_2026"}
    assert rows[0][2] == "recent ruling text"
    assert rows[0][3] == "2026-04-29"


def test_lookup_overlay_by_docket_and_by_canonical_id(tmp_path):
    inp = tmp_path / "bger.jsonl"
    _write_jsonl(inp, [
        {"decision_id": "bger_7B_121_2026", "docket_number": "7B_121/2026", "court": "bger",
         "decision_date": "2026-04-29", "publication_date": "2026-06-08",
         "full_text": "T", "language": "de"},
    ])
    out = tmp_path / "recent_overlay.db"
    build_overlay(inp, out, days=14, today=date(2026, 6, 8))
    conn = sqlite3.connect(out)
    conn.row_factory = sqlite3.Row
    assert lookup_overlay(conn, "bger_7B_121_2026")["docket_number"] == "7B_121/2026"
    assert lookup_overlay(conn, "7B_121/2026")["decision_id"] == "bger_7B_121_2026"
    assert lookup_overlay(conn, "9C_999/2099") is None
    conn.close()


# ── server-side hook (get_decision miss → overlay) ──────────────────
import mcp_server


def test_lookup_recent_overlay_by_canonical_id_and_docket(tmp_path, monkeypatch):
    inp = tmp_path / "bger.jsonl"
    _write_jsonl(inp, [
        {"decision_id": "bger_7B_121_2026", "docket_number": "7B_121/2026", "court": "bger",
         "decision_date": "2026-04-29", "publication_date": "2026-06-08",
         "full_text": "fresh text", "language": "de"},
    ])
    out = tmp_path / "recent_overlay.db"
    build_overlay(inp, out, days=14, today=date(2026, 6, 8))
    monkeypatch.setattr(mcp_server, "RECENT_OVERLAY_DB_PATH", out)
    monkeypatch.setenv("OCL_RECENT_OVERLAY", "1")
    # the REAL canonical id (underscores) — what get_decision callers actually pass — must resolve
    r = mcp_server._lookup_recent_overlay("bger_7B_121_2026")
    assert r and r["decision_id"] == "bger_7B_121_2026" and r["full_text"] == "fresh text"
    # the docket form (slash) resolves too
    assert mcp_server._lookup_recent_overlay("7B_121/2026")["docket_number"] == "7B_121/2026"
    assert mcp_server._lookup_recent_overlay("9C_999/2099") is None
    # disabled flag → no lookup even if the file exists
    monkeypatch.delenv("OCL_RECENT_OVERLAY", raising=False)
    assert mcp_server._lookup_recent_overlay("bger_7B_121_2026") is None
