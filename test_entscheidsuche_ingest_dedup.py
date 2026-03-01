import json
from pathlib import Path

from scrapers.entscheidsuche_ingest import ingest_spider, load_existing_ids


def _meta(*, signatur: str, docket: str, datum: str) -> dict:
    return {
        "Signatur": signatur,
        "Spider": "BL_Gerichte",
        "Sprache": "de",
        "Datum": datum,
        "Num": [docket],
        "Kopfzeile": [],
        "Meta": [],
        "Abstract": [],
    }


def test_load_existing_ids_tracks_source_and_date_keys(tmp_path: Path):
    existing_dir = tmp_path / "existing"
    existing_dir.mkdir(parents=True)
    row = {
        "decision_id": "bl_gerichte_BL.2020.1",
        "court": "bl_gerichte",
        "docket_number": "BL.2020.1",
        "decision_date": "2024-01-01",
        "source_id": "SIG-001",
    }
    (existing_dir / "existing.jsonl").write_text(
        json.dumps(row, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    known = load_existing_ids(existing_dir)
    assert "bl_gerichte_BL.2020.1" in known
    assert "bl_gerichte::BL.2020.1::2024-01-01" in known
    assert "bl_gerichte::source_id::SIG-001" in known


def test_ingest_spider_keeps_same_docket_if_decision_date_differs(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    existing_dir = tmp_path / "existing"
    spider_dir = input_dir / "BL_Gerichte"
    spider_dir.mkdir(parents=True)
    existing_dir.mkdir(parents=True)

    existing = {
        "decision_id": "bl_gerichte_BL.2020.1",
        "court": "bl_gerichte",
        "docket_number": "BL.2020.1",
        "decision_date": "2024-01-01",
    }
    (existing_dir / "baseline.jsonl").write_text(
        json.dumps(existing, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    (spider_dir / "doc_a.json").write_text(
        json.dumps(_meta(signatur="SIG-A", docket="BL.2020.1", datum="2024-01-01"), ensure_ascii=False),
        encoding="utf-8",
    )
    (spider_dir / "doc_b.json").write_text(
        json.dumps(_meta(signatur="SIG-B", docket="BL.2020.1", datum="2024-06-01"), ensure_ascii=False),
        encoding="utf-8",
    )

    known = load_existing_ids(existing_dir)
    processed, new_count, skipped = ingest_spider(
        "BL_Gerichte",
        input_dir=input_dir,
        output_dir=output_dir,
        known_ids=known,
        dry_run=False,
    )

    assert processed == 2
    assert skipped == 1
    assert new_count == 1

    out_file = output_dir / "es_bl_gerichte.jsonl"
    assert out_file.exists()
    rows = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["docket_number"] == "BL.2020.1"
    assert rows[0]["decision_date"] == "2024-06-01"
