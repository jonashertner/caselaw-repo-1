"""Phase 2: get_decision reads the canonical_identity sidecar to serve a
text-verified decision date over a synthetic YYYY-01-01 placeholder (C-2), adds
publication_date + cli:ch, and a date_is_estimated flag. Degrades gracefully when
the sidecar is absent (never overrides a real date).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _decisions(path):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, "
              "docket_number TEXT, decision_date TEXT, publication_date TEXT, language TEXT, "
              "full_text TEXT, json_data TEXT)")
    c.execute("INSERT INTO decisions VALUES('bge_152 II 1','bge','CH','152 II 1','2026-01-01',NULL,'de','x',NULL)")
    c.execute("INSERT INTO decisions VALUES('bger_5A_1_2024','bger','CH','5A_1/2024','2024-05-06',NULL,'de','y',NULL)")
    c.commit(); c.close()
    return path


def _sidecar(path):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE canonical_identity(decision_id TEXT PRIMARY KEY, decision_date TEXT, "
              "decision_date_provenance TEXT, publication_date TEXT, publication_date_provenance TEXT, "
              "ecli TEXT, canonical_key TEXT)")
    c.execute("INSERT INTO canonical_identity VALUES('bge_152 II 1','2025-09-27','extracted_from_text',"
              "'2026-01-01','volume_year','ECLI:CH:BGER:2025:9C_113.2025','ECLI:CH:BGER:2025:9C_113.2025')")
    c.commit(); c.close()
    return path


def _rconn(p):
    c = sqlite3.connect(p); c.row_factory = sqlite3.Row
    return c


def test_get_decision_serves_corrected_date_and_cli_ch(tmp_path, monkeypatch):
    dbp = _decisions(tmp_path / "d.db")
    scp = _sidecar(tmp_path / "ci.db")
    monkeypatch.setattr(m, "get_db", lambda: _rconn(dbp))
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(scp))
    monkeypatch.setattr(m, "_canonical_warned", False)
    out = m.get_decision_by_id("bge_152 II 1")
    assert out["decision_date"] == "2025-09-27"          # text-verified, overrides 2026-01-01
    assert out["date_provenance"] == "extracted_from_text"
    assert out["publication_date"] == "2026-01-01"
    assert out["cli_ch"] == "cli:ch:bge:152.II.1"   # Swiss-native identifier, minted on the fly
    assert out["date_is_estimated"] is False


def test_unrecovered_synthetic_is_flagged_estimated(tmp_path, monkeypatch):
    # decision with synthetic date and NO sidecar entry -> flagged estimated, not overridden
    dbp = _decisions(tmp_path / "d.db")
    monkeypatch.setattr(m, "get_db", lambda: _rconn(dbp))
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(tmp_path / "missing.db"))
    monkeypatch.setattr(m, "_canonical_warned", False)
    out = m.get_decision_by_id("bge_152 II 1")
    assert out["decision_date"] == "2026-01-01"
    assert out["date_is_estimated"] is True


def test_real_date_never_overridden_and_not_estimated(tmp_path, monkeypatch):
    dbp = _decisions(tmp_path / "d.db")
    monkeypatch.setattr(m, "get_db", lambda: _rconn(dbp))
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(tmp_path / "missing.db"))
    monkeypatch.setattr(m, "_canonical_warned", False)
    out = m.get_decision_by_id("bger_5A_1_2024")
    assert out["decision_date"] == "2024-05-06"
    assert out["date_is_estimated"] is False
