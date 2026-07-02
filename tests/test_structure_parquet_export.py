"""P1.4: the structure sidecar leaves the MCP silo — per-decision section
metadata + the erwaegungen paragraph segmentation WITH verbatim text."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

pq = pytest.importorskip("pyarrow.parquet")
from export_parquet import (PARAGRAPH_SCHEMA, STRUCTURE_META_SCHEMA,  # noqa: E402
                            export_decision_structure)


def test_structure_export_roundtrip(tmp_path):
    db = tmp_path / "decision_structure.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE structure (decision_id TEXT, court TEXT, canton TEXT,
            language TEXT, decision_date TEXT, regeste TEXT,
            sachverhalt TEXT, sachverhalt_method TEXT,
            erwaegungen TEXT, erwaegungen_method TEXT,
            erwaegungen_paragraph_count INTEGER,
            dispositiv TEXT, dispositiv_method TEXT, dispositiv_orders TEXT,
            extracted_at TEXT);
        CREATE TABLE erwaegungen_paragraph (decision_id TEXT, e_number TEXT,
            depth INTEGER, parent TEXT, text TEXT);
        INSERT INTO structure VALUES
            ('bger_1', 'bger', 'CH', 'de', '2024-01-01', 'reg',
             'A. Sachverhalt...', 'anchor', 'Erwägungen...', 'anchor', 2,
             'Demnach erkennt...', 'anchor', '[]', '2026-07-02'),
            ('zh_1', 'zh_gerichte', 'ZH', 'de', '2024-02-02', NULL,
             NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, '2026-07-02');
        INSERT INTO erwaegungen_paragraph VALUES
            ('bger_1', '1', 1, NULL, 'Die Beschwerde richtet sich gegen...'),
            ('bger_1', '1.1', 2, '1', 'Nach Art. 76 BGG ist...'),
            ('bger_1', NULL, NULL, NULL, '');
    """)
    conn.commit(); conn.close()

    out = tmp_path / "dataset"
    # default: lean metadata only (paragraphs are 4.8 GB, weekly opt-in)
    counts = export_decision_structure(db, out)
    assert counts == {"structure": 2}
    assert not (out / "structure" / "erwaegungen_paragraphs.parquet").exists()
    counts = export_decision_structure(db, out, include_paragraphs=True)
    assert counts == {"structure": 2, "erwaegungen_paragraphs": 2}  # empty text excluded

    meta = pq.read_table(out / "structure" / "structure.parquet")
    assert meta.schema.equals(STRUCTURE_META_SCHEMA)
    rows = {r["decision_id"]: r for r in meta.to_pylist()}
    assert rows["bger_1"]["has_erwaegungen"] is True
    assert rows["bger_1"]["erwaegungen_paragraph_count"] == 2
    assert rows["zh_1"]["has_sachverhalt"] is False

    paras = pq.read_table(out / "structure" / "erwaegungen_paragraphs.parquet")
    assert paras.schema.equals(PARAGRAPH_SCHEMA)
    p = paras.to_pylist()
    assert p[1]["e_number"] == "1.1" and p[1]["depth"] == 2 and p[1]["parent"] == "1"


def test_missing_sidecar_is_clean_skip(tmp_path):
    assert export_decision_structure(tmp_path / "absent.db", tmp_path / "o") == {}
