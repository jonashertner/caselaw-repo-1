"""Parquet export: marked_for_publication is stored in SQLite as int 0/1/NULL
but the Parquet schema declares it bool. Without coercion,
pa.Table.from_pylist(rows, schema=DECISION_SCHEMA) raises
ArrowInvalid("Could not convert 0 with type int: tried to convert to boolean"),
which failed nightly Step 3 (and cascaded to skip HF upload + the final git
push) on 2026-06-24. These pin the coercion.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import pyarrow as pa  # noqa: E402
import export_parquet as ep  # noqa: E402


def test_coerce_bool():
    assert ep._coerce_bool(None) is None
    assert ep._coerce_bool("") is None
    assert ep._coerce_bool(0) is False
    assert ep._coerce_bool(1) is True
    assert ep._coerce_bool(True) is True
    assert ep._coerce_bool(False) is False
    assert ep._coerce_bool("0") is False
    assert ep._coerce_bool("1") is True
    assert ep._coerce_bool("true") is True


def test_normalize_row_coerces_marked_for_publication():
    assert ep.normalize_row({"marked_for_publication": 0})["marked_for_publication"] is False
    assert ep.normalize_row({"marked_for_publication": 1})["marked_for_publication"] is True
    assert ep.normalize_row({"marked_for_publication": None})["marked_for_publication"] is None
    assert ep.normalize_row({})["marked_for_publication"] is None  # absent -> None (nullable)


def _row(did, mfp):
    return {
        "decision_id": did, "court": "bger", "canton": "CH",
        "docket_number": did, "language": "de", "full_text": "x",
        "marked_for_publication": mfp,
    }


def test_from_pylist_builds_table_with_sqlite_int_flag():
    # Rows exactly as SQLite yields them (int 0/1 and NULL) used to crash; after
    # normalize_row they build a valid table with a real bool column.
    schema_fields = {f.name for f in ep.DECISION_SCHEMA}
    normalized = [ep.normalize_row(_row(d, m)) for d, m in (("a", 1), ("b", 0), ("c", None))]
    clean = [{k: r.get(k) for k in schema_fields} for r in normalized]
    table = pa.Table.from_pylist(clean, schema=ep.DECISION_SCHEMA)  # must not raise
    assert table.column("marked_for_publication").to_pylist() == [True, False, None]
    # has_full_text (the other bool field) stays a real non-null bool
    assert table.column("has_full_text").to_pylist() == [True, True, True]
