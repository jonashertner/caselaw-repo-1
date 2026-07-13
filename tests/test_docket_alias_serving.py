"""Serve-side resolution of joined/secondary dockets (issue #41)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402
import build_fts5  # noqa: E402
from db_schema import SCHEMA_SQL  # noqa: E402


def _fixture(path):
    c = sqlite3.connect(path)
    c.executescript(SCHEMA_SQL)
    # a consolidated decision (lead 1B_242/2022, joined 243 + 244)
    c.execute(
        "INSERT INTO decisions (decision_id, court, canton, docket_number, "
        "decision_date, language, title, full_text, source_url) VALUES "
        "('bger_1B_242_2022','bger','CH','1B 242/2022','2022-05-30','de',"
        "'Strafverfahren',"
        "'Bundesgericht 30.05.2022 1B 242/2022 (1B_242/2022)\n"
        "1B_242/2022, 1B_243/2022 und 1B_244/2022\nUrteil vom 30. Mai 2022\n',"
        "'https://www.bger.ch/x')"
    )
    # some same-prefix same-year and different-year neighbours (close-match test)
    for dk, yr, did in [("1B 240/2022","2022","bger_1B_240_2022"),
                        ("1B 243/2023","2023","bger_1B_243_2023")]:
        c.execute(
            "INSERT INTO decisions (decision_id, court, canton, docket_number, "
            "decision_date, language, full_text, source_url) VALUES "
            f"('{did}','bger','CH','{dk}','{yr}-01-01','de','t','https://x')"
        )
    c.commit()
    build_fts5._build_docket_aliases(c)
    c.commit()
    c.close()
    return path


def _rconn(p):
    c = sqlite3.connect(p)
    c.row_factory = sqlite3.Row
    return c


def _setup(tmp_path, monkeypatch):
    dbp = str(_fixture(tmp_path / "decisions.db"))
    monkeypatch.setattr(m, "get_db", lambda: _rconn(dbp))
    monkeypatch.setattr(m, "_canonical_warned", False, raising=False)
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(tmp_path / "missing.db"))
    return dbp


def test_get_decision_by_joined_docket(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    r = m.get_decision_by_id("1B_243/2022")
    assert r and r["decision_id"] == "bger_1B_242_2022"
    assert r.get("resolved_via") == "joined_docket_alias"
    assert r.get("queried_docket") == "1B_243/2022"
    assert r.get("canonical_docket") == "1B 242/2022"


def test_get_decision_by_canonical_form_secondary(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    r = m.get_decision_by_id("bger_1B_244_2022")
    assert r and r["decision_id"] == "bger_1B_242_2022"


def test_resolve_decision_id_joined(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    assert m._resolve_decision_id("1B_243/2022") == "bger_1B_242_2022"


def test_resolve_decision_id_strict_joined(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    assert m._resolve_decision_id_strict("1B_244/2022") == "bger_1B_242_2022"


def test_lead_docket_still_wins_directly(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    r = m.get_decision_by_id("1B 242/2022")
    assert r and r["decision_id"] == "bger_1B_242_2022"
    assert "resolved_via" not in r  # direct primary hit, not via alias


def test_cite_joined_docket_resolves_to_lead(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    res = m._handle_cite(reference="1B_243/2022")
    assert res.get("exists") is True, res
    assert res.get("decision_id") == "bger_1B_242_2022"


def test_cite_close_matches_never_different_year(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    # 1B_999/2022 doesn't exist; close matches must be 2022-only, never 2023.
    res = m._handle_cite(reference="1B_999/2022")
    assert res.get("exists") is False
    years = {(cm.get("docket_number") or "")[-4:] for cm in res.get("close_matches", [])}
    assert "2023" not in years, res.get("close_matches")


def test_guard_when_alias_table_absent(tmp_path, monkeypatch):
    # A DB built before #41 has no alias table; lookup must degrade, not raise.
    p = str(tmp_path / "old.db")
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, court TEXT, "
              "canton TEXT, docket_number TEXT, decision_date TEXT, language TEXT, "
              "full_text TEXT, json_data TEXT)")
    c.execute("INSERT INTO decisions VALUES('bger_1B_242_2022','bger','CH',"
              "'1B 242/2022','2022-05-30','de','t',NULL)")
    c.commit(); c.close()
    monkeypatch.setattr(m, "get_db", lambda: _rconn(p))
    monkeypatch.setattr(m, "CANONICAL_DB_PATH", Path(tmp_path / "missing.db"))
    assert m.get_decision_by_id("1B_243/2022") is None  # no alias table, no crash
    assert m.get_decision_by_id("1B 242/2022")["decision_id"] == "bger_1B_242_2022"
