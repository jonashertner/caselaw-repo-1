"""Inline derivation at INSERT (2026-07-03): branch, chamber fill,
proceeding_type, appealed_*, bge docket_number_2 are born with the row —
no full-table post-pass UPDATE loops (the WAL-pinning lesson)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import db_schema  # noqa: E402
from build_fts5 import insert_decision, _derive_bge_docket2_inline  # noqa: E402


def _db():
    conn = sqlite3.connect(":memory:")
    conn.executescript(db_schema.SCHEMA_SQL)
    return conn


def _row(**kw):
    base = dict(decision_id="bger_4A_1_2024", court="bger", canton="CH",
                docket_number="4A_1/2024", decision_date="2024-05-01",
                language="de", full_text="x" * 300, scraped_at="2026-07-03")
    base.update(kw)
    return base


def test_branch_and_proceeding_born_at_insert():
    conn = _db()
    assert insert_decision(conn, _row())
    r = conn.execute("SELECT branch, proceeding_type, procedural_code "
                     "FROM decisions").fetchone()
    assert r == ("zivil", "bgg_beschwerde_zivil", "bgg")


def test_chamber_filled_from_docket_code_inline():
    conn = _db()
    assert insert_decision(conn, _row())
    assert conn.execute("SELECT chamber FROM decisions").fetchone()[0] == "4A"


def test_appealed_fields_extracted_for_bger():
    conn = _db()
    t = ("Urteil. Beschwerde gegen das Urteil des Obergerichts des Kantons "
         "Zürich, II. Zivilkammer, vom 30. September 2021 (LB210012). "
         "Sachverhalt: A. " + "x " * 200)
    assert insert_decision(conn, _row(full_text=t))
    r = conn.execute("SELECT appealed_date, appealed_docket FROM decisions").fetchone()
    assert r == ("2021-09-30", "LB210012")


def test_bge_docket2_derived_with_year_guard():
    assert _derive_bge_docket2_inline(
        "BGE 151 III 481 ... (Beschwerde in Zivilsachen) 4A_576/2024 vom "
        "29. April 2025", "2025-04-29") == "4A_576/2024"
    # a cited old docket outside the plausibility window is skipped
    assert _derive_bge_docket2_inline(
        "Regeste ... vgl. 4C.44/1998 ... (Beschwerde) 5A_10/2024 vom ...",
        "2025-01-01") == "5A_10/2024"
    assert _derive_bge_docket2_inline("Regeste only, no docket", "2025-01-01") is None
    conn = _db()
    assert insert_decision(conn, _row(
        decision_id="bge_151_III_481", court="bge", docket_number="151 III 481",
        full_text="i.S. A. gegen B. (Beschwerde in Zivilsachen) 4A_576/2024 "
                  "vom 29. April 2025. Regeste: ... " + "x " * 200,
        decision_date="2025-04-29"))
    assert conn.execute("SELECT docket_number_2 FROM decisions").fetchone()[0] == "4A_576/2024"


def test_portal_chamber_never_overwritten():
    conn = _db()
    assert insert_decision(conn, _row(chamber="I. zivilrechtliche Abteilung"))
    assert conn.execute("SELECT chamber FROM decisions").fetchone()[0] == \
        "I. zivilrechtliche Abteilung"


def test_ensure_derived_columns_patches_old_schema():
    from build_fts5 import ensure_derived_columns
    conn = sqlite3.connect(":memory:")
    # simulate the served pre-07-03 schema: no derived columns at all
    conn.execute("""CREATE TABLE decisions (
        decision_id TEXT PRIMARY KEY, court TEXT NOT NULL,
        canton TEXT NOT NULL, chamber TEXT, docket_number TEXT NOT NULL,
        docket_number_2 TEXT, decision_date TEXT, publication_date TEXT,
        marked_for_publication INTEGER, language TEXT NOT NULL, title TEXT,
        legal_area TEXT, regeste TEXT, abstract_de TEXT, abstract_fr TEXT,
        abstract_it TEXT, full_text TEXT, decision_type TEXT, outcome TEXT,
        source_url TEXT, pdf_url TEXT, cited_decisions TEXT, scraped_at TEXT,
        source TEXT, source_id TEXT, source_spider TEXT, content_hash TEXT,
        json_data TEXT, canonical_key TEXT)""")
    assert ensure_derived_columns(conn) == 6
    assert ensure_derived_columns(conn) == 0  # idempotent
    # insert_decision now works against the patched old-schema copy
    assert insert_decision(conn, _row())
    assert conn.execute("SELECT branch FROM decisions").fetchone()[0] == "zivil"
