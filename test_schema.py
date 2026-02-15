#!/usr/bin/env python3
"""
test_schema.py — Verify schema consistency across all modules
===============================================================

Checks that db_schema.py, export_parquet.py, and models.py stay in sync.
Run as part of CI to catch schema drift.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def test_db_schema_imports():
    """Verify db_schema.py exports are importable and consistent."""
    from db_schema import SCHEMA_SQL, INSERT_COLUMNS, INSERT_SQL, INSERT_OR_IGNORE_SQL

    # INSERT_COLUMNS should match the CREATE TABLE columns
    assert "decision_id" in INSERT_COLUMNS
    assert "json_data" in INSERT_COLUMNS
    assert INSERT_COLUMNS[0] == "decision_id"
    assert INSERT_COLUMNS[-1] == "json_data"

    # INSERT_SQL should have the right number of placeholders
    placeholders = INSERT_SQL.count("?")
    assert placeholders == len(INSERT_COLUMNS), (
        f"INSERT_SQL has {placeholders} placeholders but INSERT_COLUMNS has {len(INSERT_COLUMNS)} entries"
    )

    # Schema SQL should contain key elements
    assert "CREATE TABLE" in SCHEMA_SQL
    assert "decisions_fts" in SCHEMA_SQL
    assert "decisions_ai" in SCHEMA_SQL  # insert trigger
    assert "decisions_ad" in SCHEMA_SQL  # delete trigger
    assert "decisions_au" in SCHEMA_SQL  # update trigger

    # Provenance columns should exist
    assert "source" in INSERT_COLUMNS
    assert "source_id" in INSERT_COLUMNS
    assert "content_hash" in INSERT_COLUMNS

    print("  OK: db_schema exports consistent")


def test_parquet_schema_fields():
    """Verify Parquet schema covers all expected fields."""
    from export_parquet import DECISION_SCHEMA

    field_names = {f.name for f in DECISION_SCHEMA}

    # Required fields
    required = {
        "decision_id", "court", "canton", "docket_number", "language",
        "full_text", "source_url", "has_full_text", "text_length",
    }
    missing = required - field_names
    assert not missing, f"Parquet schema missing required fields: {missing}"

    # Provenance fields
    provenance = {"scraped_at", "source", "source_id", "source_spider", "content_hash"}
    missing_prov = provenance - field_names
    assert not missing_prov, f"Parquet schema missing provenance fields: {missing_prov}"

    print(f"  OK: Parquet schema has {len(field_names)} fields, all required present")


def test_model_fields():
    """Verify the Decision model has expected fields."""
    from models import Decision

    model_fields = set(Decision.model_fields.keys())

    required = {
        "decision_id", "court", "canton", "docket_number", "language",
        "full_text", "source_url", "decision_date",
    }
    missing = required - model_fields
    assert not missing, f"Decision model missing fields: {missing}"

    print(f"  OK: Decision model has {len(model_fields)} fields")


def test_db_parquet_alignment():
    """Verify DB schema columns align with Parquet schema."""
    from db_schema import INSERT_COLUMNS
    from export_parquet import DECISION_SCHEMA

    db_cols = set(INSERT_COLUMNS)
    pq_cols = {f.name for f in DECISION_SCHEMA}

    # These columns are only in the DB (json_data is a blob)
    db_only_expected = {"json_data"}
    # These columns are only in Parquet (computed fields)
    pq_only_expected = {
        "has_full_text", "text_length", "chamber", "docket_number_2",
        "abstract_de", "abstract_fr", "abstract_it",
        "decision_date", "publication_date",
        "title", "legal_area", "regeste", "outcome", "decision_type",
        "judges", "clerks", "collection", "appeal_info",
        "pdf_url", "bge_reference", "cited_decisions",
        "external_id", "source_spider",
    }

    db_only = db_cols - pq_cols - db_only_expected
    pq_only = pq_cols - db_cols - pq_only_expected

    # Core columns that should be in BOTH
    core_shared = {"decision_id", "court", "canton", "language", "full_text",
                   "source_url", "scraped_at", "source", "source_id", "content_hash"}
    for col in core_shared:
        assert col in db_cols, f"Core column '{col}' missing from DB schema"
        assert col in pq_cols, f"Core column '{col}' missing from Parquet schema"

    print(f"  OK: Core columns aligned between DB ({len(db_cols)}) and Parquet ({len(pq_cols)})")


def test_normalize_row():
    """Verify normalize_row handles edge cases."""
    from export_parquet import normalize_row

    # Minimal valid row
    row = {
        "decision_id": "test_123",
        "court": "bger",
        "canton": "CH",
        "docket_number": "1A_1/2025",
        "language": "de",
        "full_text": "This is a test decision with enough text to pass.",
        "source_url": "https://example.com",
    }
    result = normalize_row(dict(row))
    assert result["has_full_text"] is True
    assert result["text_length"] == len(row["full_text"])
    assert result["decision_date"] is None  # not provided = None, not today

    # Row with entscheidsuche fields → mapped to generic
    row2 = dict(row)
    row2["entscheidsuche_signatur"] = "SIG_123"
    row2["entscheidsuche_spider"] = "CH_BGer"
    result2 = normalize_row(row2)
    assert result2["source_id"] == "SIG_123"
    assert result2["source_spider"] == "CH_BGer"

    # Row with "1970-01-01" date → None
    row3 = dict(row)
    row3["decision_date"] = "1970-01-01"
    result3 = normalize_row(row3)
    assert result3["decision_date"] is None

    print("  OK: normalize_row handles edge cases correctly")


def test_models_utilities():
    """Verify models.py utility functions."""
    from datetime import date
    from models import make_decision_id, normalize_docket, parse_date, detect_language, extract_citations

    # make_decision_id
    assert make_decision_id("bger", "6B_1234/2025") == "bger_6B_1234_2025"

    # normalize_docket
    assert normalize_docket("6B_1234/2025") == "6B_1234_2025"
    assert normalize_docket("A-1234/2025") == "A-1234_2025"

    # parse_date
    assert parse_date("15.02.2025") == date(2025, 2, 15)
    assert parse_date("2025-02-15") == date(2025, 2, 15)
    assert parse_date("") is None
    assert parse_date(None) is None

    # detect_language
    assert detect_language("Der Beschwerdeführer hat die Kosten") == "de"
    assert detect_language("Le recourant a les frais") == "fr"
    assert detect_language("Il ricorrente ha i costi della procedura") == "it"

    # extract_citations
    cites = extract_citations("Gemäss BGE 140 III 264 und Urteil 6B_1234/2025")
    assert "BGE 140 III 264" in cites
    assert "6B_1234/2025" in cites

    print("  OK: models.py utilities work correctly")


if __name__ == "__main__":
    print("Schema Consistency Tests")
    print("=" * 50)

    tests = [
        test_db_schema_imports,
        test_parquet_schema_fields,
        test_model_fields,
        test_db_parquet_alignment,
        test_normalize_row,
        test_models_utilities,
    ]

    failed = 0
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1

    print(f"\n{'=' * 50}")
    if failed:
        print(f"FAILED: {failed}/{len(tests)} tests")
        sys.exit(1)
    else:
        print(f"ALL {len(tests)} TESTS PASSED")
