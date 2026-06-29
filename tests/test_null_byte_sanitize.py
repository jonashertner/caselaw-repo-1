"""TDD: Omnis/Findinfo portal text (ti/ne/ge/so) carries embedded NUL (\\x00)
+ C0 control bytes. Python's sqlite3 TRUNCATES a TEXT value at the first NUL on
insert, so the whole judgment body was dropped — only the ~500-char header
survived. That zeroed decision_structure Erwaegungen extraction for ~4.5k
current decisions (the structure drift-gate blocker) and cost search recall.

Root cause proven 2026-06-29: ti_gerichte_34.2024.28 had a \\x00 at offset 498
("in fatto\\n\\n\\x00\\x02..."); decisions.db stored 495 chars vs 77,544 in the
JSONL shard. Fix: _clean_text must strip control chars (keeping \\t \\n \\r) so
no NUL reaches the insert and the body survives.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from build_fts5 import _clean_text  # noqa: E402


# Real shape of the ti_gerichte_34.2024.28 truncation point (offset ~498).
_TI_SAMPLE = (
    "Incarto n.34.2024.28\n\nLugano\n\n12 maggio 2025\n\n"
    "Il Tribunale cantonale delle assicurazioni\n\nin fatto\n\n"
    "\x00\x02\x02\x02\x02\x02\x02\n\n"
    "Con osservazioni del 10 giugno il ricorrente chiede l'annullamento ..."
)


def test_clean_text_removes_null_byte():
    assert "\x00" not in _clean_text(_TI_SAMPLE)


def test_clean_text_removes_c0_control_bytes():
    assert "\x02" not in _clean_text(_TI_SAMPLE)


def test_clean_text_preserves_body_after_null():
    # Everything after the NUL must survive — the whole point of the fix.
    assert "Con osservazioni del 10 giugno" in _clean_text(_TI_SAMPLE)


def test_clean_text_keeps_legitimate_whitespace():
    out = _clean_text("riga uno\triga\nseconda\r\nterza")
    for tok in ("riga uno", "seconda", "terza"):
        assert tok in out


def test_clean_text_none_and_empty_passthrough():
    assert _clean_text(None) is None
    assert _clean_text("") == ""


def test_sqlite_length_undercounts_until_cleaned():
    """The real production mechanism: the incremental structure builder selects
    rows via ``WHERE length(full_text) >= 500``. SQLite's length() stops at the
    first NUL, so a body that begins after an early NUL is undercounted and the
    row is silently excluded from extraction (and on the VPS's older Python the
    read truncates outright). Stripping the NUL restores the true length."""
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE d(full_text TEXT)")
    c.execute("INSERT INTO d VALUES (?)", (_TI_SAMPLE,))
    raw_len = c.execute("SELECT length(full_text) FROM d").fetchone()[0]
    assert raw_len <= 100  # length() stopped at the NUL (offset 100)

    c.execute("INSERT INTO d VALUES (?)", (_clean_text(_TI_SAMPLE),))
    clean_len = c.execute(
        "SELECT length(full_text) FROM d ORDER BY rowid DESC LIMIT 1"
    ).fetchone()[0]
    assert clean_len > 150  # full length recovered after stripping the NUL
