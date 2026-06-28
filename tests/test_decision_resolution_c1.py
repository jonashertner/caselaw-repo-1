"""Bug C-1 (2026-06-28 audit): get_decision('131 III 12') silently returned the
DIFFERENT ruling BGE 131 III 121 because the docket LIKE '%131 III 12%' fallback
matched the longer number. The fix: resolve BGE references by exact (vol,part,page)
tuple first, and never accept a docket that is the input followed by extra digits.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _db(path: Path) -> str:
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, docket_number TEXT, "
              "decision_date TEXT, court TEXT, full_text TEXT, json_data TEXT)")
    c.execute("INSERT INTO decisions VALUES('bge_BGE_131_III_12','131 III 12','2004-09-14','bge','Prädisposition Art. 42-44 OR',NULL)")
    c.execute("INSERT INTO decisions VALUES('bge_BGE_131_III_121','131 III 121','2004-12-10','bge','Markenrecht',NULL)")
    c.commit()
    c.close()
    return str(path)


def _conn(p):
    c = sqlite3.connect(p)
    c.row_factory = sqlite3.Row
    return c


def test_bge_ref_candidates_exact_tuple():
    assert m._bge_ref_candidates("131 III 12") == [
        "bge_BGE_131_III_12", "bge_131_III_12", "bge_131 III 12"]
    assert m._bge_ref_candidates("BGE 131 III 121")[0] == "bge_BGE_131_III_121"
    assert m._bge_ref_candidates("not a bge ref") == []


def test_docket_prefix_of_longer_guard():
    assert m._docket_is_prefix_of_longer("131 III 12", "131 III 121") is True
    assert m._docket_is_prefix_of_longer("131 III 12", "131 III 12") is False
    assert m._docket_is_prefix_of_longer("4A_5/2020", "4A_5/2020") is False


def test_get_decision_by_id_resolves_short_bge_ref_exactly(tmp_path, monkeypatch):
    dbp = _db(tmp_path / "d.db")
    monkeypatch.setattr(m, "get_db", lambda: _conn(dbp))
    out = m.get_decision_by_id("131 III 12")
    assert out is not None
    assert out["decision_id"] == "bge_BGE_131_III_12", "must NOT return 131 III 121"
    # the longer one still resolves to itself
    assert m.get_decision_by_id("131 III 121")["decision_id"] == "bge_BGE_131_III_121"
