"""text_integrity: gate-level control-char guard (2026-06-29 incident class).
Must be gate-visible (NOT MODULE_NEVER_CRITICAL) and catch a NUL/C0 leak."""
from __future__ import annotations

import sqlite3

from quality.checks import text_integrity
from quality.types import Severity


def _db(tmp_path, rows):
    db = tmp_path / "d.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, "
                 "court TEXT, full_text TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?)", rows)
    conn.commit()
    return sqlite3.connect(db)


def test_module_is_gate_visible():
    assert not getattr(text_integrity, "MODULE_NEVER_CRITICAL", False)


def test_clean_corpus_passes(tmp_path):
    conn = _db(tmp_path, [
        (f"d{i}", "ti_gerichte", f"Sauberer Text {i} mit\nZeilen\tund Tabs.")
        for i in range(50)
    ])
    res = text_integrity.check_control_chars_sample(conn)
    assert res.passed is True and res.metric_value == 0
    assert res.severity is Severity.CRITICAL
    per_court = list(text_integrity.check_control_chars_risk_courts(conn))
    ti = [r for r in per_court if r.court == "ti_gerichte"][0]
    assert ti.passed is True


def test_nul_leak_is_caught(tmp_path):
    conn = _db(tmp_path, [
        ("ok1", "ge_gerichte", "Texte propre."),
        ("bad1", "ge_gerichte", "in fatto\n\x00\x02garbage after the NUL"),
        ("ok2", "ne_gerichte", "Encore propre."),
    ])
    res = text_integrity.check_control_chars_sample(conn)
    assert res.passed is False
    assert res.metric_value >= 1
    assert any(s["decision_id"] == "bad1" for s in res.sample_rows)
    ge = [r for r in text_integrity.check_control_chars_risk_courts(conn)
          if r.court == "ge_gerichte"][0]
    assert ge.passed is False


def test_tabs_newlines_cr_are_not_flagged(tmp_path):
    conn = _db(tmp_path, [("d1", "so_gerichte", "a\tb\nc\rd")])
    res = text_integrity.check_control_chars_sample(conn)
    assert res.passed is True
