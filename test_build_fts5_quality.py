"""Tests for data quality functions in build_fts5.py."""
import sqlite3

import pytest

from build_fts5 import (
    _clean_text,
    _dedup_decisions,
    _extract_regeste_from_text,
    _fill_missing_regeste,
    _fix_mojibake,
    _log_quality_summary,
    insert_decision,
)
from db_schema import SCHEMA_SQL
from models import make_canonical_key


# ── _fix_mojibake ────────────────────────────────────────────


def test_fix_mojibake_double_encoded():
    # 'ä' (U+00E4) stored as Latin-1 bytes C3 A4 → "Ã¤"
    broken = "Ã¤"  # \xc3\xa4 decoded as Latin-1
    assert _fix_mojibake(broken) == "ä"


def test_fix_mojibake_leaves_correct_utf8():
    assert _fix_mojibake("ä ö ü") == "ä ö ü"


def test_fix_mojibake_leaves_ascii():
    assert _fix_mojibake("hello world") == "hello world"


# ── _clean_text ──────────────────────────────────────────────


def test_clean_text_strips_html():
    assert _clean_text("Hello <b>world</b>") == "Hello world"


def test_clean_text_strips_br():
    assert _clean_text("line1<br>line2") == "line1 line2"


def test_clean_text_replaces_entities():
    assert _clean_text("a&nbsp;b&amp;c") == "a b&c"


def test_clean_text_fixes_mojibake():
    # \xc3 triggers mojibake check
    broken = "Ã¤nderung"
    result = _clean_text(broken)
    assert result == "änderung"


def test_clean_text_normalizes_whitespace():
    assert _clean_text("a   b\t\tc") == "a b c"


def test_clean_text_none():
    assert _clean_text(None) is None


def test_clean_text_empty():
    assert _clean_text("") == ""


def test_clean_text_combined():
    raw = "Das <b>Urteil</b>&nbsp;betrifft<br>Ã¤nderung   des Vertrags"
    result = _clean_text(raw)
    assert "<" not in result
    assert "&nbsp;" not in result
    assert "  " not in result
    assert "änderung" in result


# ── _extract_regeste_from_text ───────────────────────────────


def test_extract_regeste_simple():
    text = (
        "Urteilskopf\n\n"
        "Regeste\n"
        "Art. 41 OR. Haftung für unerlaubte Handlung. "
        "Der Geschädigte hat den Schaden zu beweisen.\n\n"
        "Sachverhalt\n"
        "A. Der Kläger machte geltend..."
    )
    result = _extract_regeste_from_text(text)
    assert result is not None
    assert "Art. 41 OR" in result
    assert "Sachverhalt" not in result


def test_extract_regeste_french_end_marker():
    text = (
        "Regeste\n"
        "Responsabilité civile. Preuve du dommage.\n\n"
        "Faits\n"
        "A. Le demandeur..."
    )
    result = _extract_regeste_from_text(text)
    assert result is not None
    assert "Responsabilité" in result
    assert "Faits" not in result


def test_extract_regeste_no_header():
    text = "Some decision without a regeste section."
    assert _extract_regeste_from_text(text) is None


def test_extract_regeste_too_short():
    text = "Regeste\nShort.\nSachverhalt\nDetails..."
    assert _extract_regeste_from_text(text) is None


# ── DB helper: create test database ─────────────────────────


@pytest.fixture
def db():
    """Create an in-memory SQLite database with schema."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL)
    yield conn
    conn.close()


def _insert_row(conn, **kwargs):
    """Insert a test decision with defaults."""
    defaults = {
        "decision_id": "test_1",
        "court": "bger",
        "canton": "CH",
        "chamber": None,
        "docket_number": "6B_1/2025",
        "decision_date": "2025-01-15",
        "publication_date": None,
        "language": "de",
        "title": None,
        "legal_area": None,
        "regeste": None,
        "full_text": "Test decision full text " * 20,
        "decision_type": None,
        "outcome": None,
        "source_url": None,
        "pdf_url": None,
        "cited_decisions": "[]",
        "scraped_at": "2025-01-16",
        "source": "test",
        "source_id": None,
        "source_spider": None,
        "content_hash": None,
        "json_data": "{}",
        "canonical_key": None,
    }
    defaults.update(kwargs)
    # Auto-compute canonical_key if not explicitly set
    if defaults["canonical_key"] is None:
        defaults["canonical_key"] = make_canonical_key(
            defaults["court"], defaults["docket_number"], defaults.get("decision_date"),
        )
    cols = list(defaults.keys())
    vals = [defaults[c] for c in cols]
    placeholders = ",".join("?" for _ in cols)
    conn.execute(
        f"INSERT INTO decisions ({','.join(cols)}) VALUES ({placeholders})", vals
    )
    conn.commit()


# ── _dedup_decisions ─────────────────────────────────────────


def test_dedup_removes_shorter_duplicate(db):
    _insert_row(db, decision_id="native_1", court="bl_gerichte", docket_number="400-2020-1",
                decision_date="2020-05-01", full_text="long " * 200, regeste="Some regeste")
    _insert_row(db, decision_id="es_bl_1", court="bl_gerichte", docket_number="400-2020-1",
                decision_date="2020-05-01", full_text="short", regeste=None)

    deleted = _dedup_decisions(db)
    assert deleted == 1

    remaining = db.execute("SELECT decision_id FROM decisions").fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == "native_1"


def test_dedup_prefers_regeste(db):
    _insert_row(db, decision_id="no_regeste", court="bger", docket_number="1C_1/2025",
                decision_date="2025-01-01", full_text="long " * 200, regeste=None)
    _insert_row(db, decision_id="has_regeste", court="bger", docket_number="1C_1/2025",
                decision_date="2025-01-01", full_text="shorter " * 50, regeste="A real regeste")

    deleted = _dedup_decisions(db)
    assert deleted == 1

    remaining = db.execute("SELECT decision_id FROM decisions").fetchall()
    assert remaining[0][0] == "has_regeste"


def test_dedup_no_duplicates(db):
    _insert_row(db, decision_id="a", docket_number="1A_1/2025")
    _insert_row(db, decision_id="b", docket_number="1A_2/2025")

    deleted = _dedup_decisions(db)
    assert deleted == 0
    assert db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 2


def test_dedup_skips_empty_docket(db):
    _insert_row(db, decision_id="x", docket_number="", decision_date="2025-01-01")
    _insert_row(db, decision_id="y", docket_number="", decision_date="2025-01-01")

    deleted = _dedup_decisions(db)
    assert deleted == 0  # empty docket numbers are excluded from dedup


def test_dedup_canonical_key_collapses_dot_vs_underscore(db):
    """Docket 'BL.2020.1' and 'BL_2020_1' collapse to the same canonical key."""
    _insert_row(db, decision_id="native", court="bl_gerichte",
                docket_number="400.2020.1", decision_date="2020-05-01",
                full_text="long " * 200, regeste="Good regeste")
    _insert_row(db, decision_id="es_dup", court="bl_gerichte",
                docket_number="400_2020_1", decision_date="2020-05-01",
                full_text="short", regeste=None)

    deleted = _dedup_decisions(db)
    assert deleted == 1

    remaining = db.execute("SELECT decision_id FROM decisions").fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == "native"  # longer text + regeste wins


def test_dedup_canonical_key_collapses_slash_vs_underscore(db):
    """Docket '6B_1/2025' and '6B_1_2025' collapse."""
    _insert_row(db, decision_id="a", court="bger",
                docket_number="6B_1/2025", decision_date="2025-01-01",
                full_text="text " * 100, regeste=None)
    _insert_row(db, decision_id="b", court="bger",
                docket_number="6B_1_2025", decision_date="2025-01-01",
                full_text="text " * 100, regeste="Has regeste")

    deleted = _dedup_decisions(db)
    assert deleted == 1

    remaining = db.execute("SELECT decision_id FROM decisions").fetchall()
    assert remaining[0][0] == "b"  # regeste wins


def test_dedup_canonical_key_different_court_not_collapsed(db):
    """Same docket at different courts are distinct cases."""
    _insert_row(db, decision_id="a", court="bger",
                docket_number="6B_1/2025", decision_date="2025-01-01")
    _insert_row(db, decision_id="b", court="bge",
                docket_number="6B_1/2025", decision_date="2025-01-01")

    deleted = _dedup_decisions(db)
    assert deleted == 0
    assert db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 2


# ── make_canonical_key ───────────────────────────────────────


def test_canonical_key_normalizes_docket_variants():
    assert make_canonical_key("bl_gerichte", "BL.2020.1", "2020-05-01") == \
           make_canonical_key("bl_gerichte", "BL_2020_1", "2020-05-01")

    assert make_canonical_key("bger", "6B_1/2025", "2025-01-01") == \
           make_canonical_key("bger", "6B_1_2025", "2025-01-01")

    assert make_canonical_key("bger", "6B 1/2025", "2025-01-01") == \
           make_canonical_key("bger", "6b_1/2025", "2025-01-01")


def test_canonical_key_different_court():
    assert make_canonical_key("bger", "6B_1/2025", "2025-01-01") != \
           make_canonical_key("bge", "6B_1/2025", "2025-01-01")


def test_canonical_key_different_date():
    assert make_canonical_key("bger", "6B_1/2025", "2025-01-01") != \
           make_canonical_key("bger", "6B_1/2025", "2025-01-02")


# ── _fill_missing_regeste ────────────────────────────────────


def test_fill_missing_regeste(db):
    text = (
        "Urteilskopf\n\n"
        "Regeste\n"
        "Art. 41 OR. Haftung für unerlaubte Handlung. "
        "Der Geschädigte hat den Schaden nachzuweisen.\n\n"
        "Sachverhalt\n"
        "A. Der Kläger machte geltend..." + " x" * 100
    )
    _insert_row(db, decision_id="bger_1", court="bger", regeste=None, full_text=text)

    filled = _fill_missing_regeste(db)
    assert filled == 1

    regeste = db.execute("SELECT regeste FROM decisions WHERE decision_id='bger_1'").fetchone()[0]
    assert "Art. 41 OR" in regeste


def test_fill_missing_regeste_skips_non_bger(db):
    text = "Regeste\nSome text here for extraction.\nSachverhalt\nMore..." + " x" * 100
    _insert_row(db, decision_id="zh_1", court="zh_gerichte", regeste=None, full_text=text)

    filled = _fill_missing_regeste(db)
    assert filled == 0


def test_fill_missing_regeste_skips_existing(db):
    text = "Regeste\nExtracted text.\nSachverhalt\nMore..." + " x" * 100
    _insert_row(db, decision_id="bger_2", court="bger", regeste="Existing regeste", full_text=text)

    filled = _fill_missing_regeste(db)
    assert filled == 0

    regeste = db.execute("SELECT regeste FROM decisions WHERE decision_id='bger_2'").fetchone()[0]
    assert regeste == "Existing regeste"


# ── insert_decision with cleaning ────────────────────────────


def test_insert_decision_cleans_html(db):
    row = {
        "decision_id": "test_clean",
        "court": "bger",
        "canton": "CH",
        "docket_number": "1C_1/2025",
        "language": "de",
        "full_text": "Hello <b>world</b>&nbsp;test",
        "regeste": "A <br>regeste",
        "title": "Title <i>here</i>",
    }
    assert insert_decision(db, row)

    result = db.execute(
        "SELECT full_text, regeste, title FROM decisions WHERE decision_id='test_clean'"
    ).fetchone()
    assert "<b>" not in result[0]
    assert "&nbsp;" not in result[0]
    assert "<br>" not in result[1]
    assert "<i>" not in result[2]

    # Verify canonical_key was populated
    ckey = db.execute(
        "SELECT canonical_key FROM decisions WHERE decision_id='test_clean'"
    ).fetchone()[0]
    assert ckey == "bger|1C12025|"


# ── _log_quality_summary ─────────────────────────────────────


def test_log_quality_summary_no_error(db):
    _insert_row(db, decision_id="q1", full_text="short")
    _insert_row(db, decision_id="q2", full_text="long " * 200, regeste="reg")
    # Should not raise
    _log_quality_summary(db)
