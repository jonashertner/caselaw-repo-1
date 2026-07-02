"""P0.4 (LegalStats wishlist P0.5): delta-{date}.parquet must carry the SAME
schema as the full corpus export — real types, has_full_text/text_length —
so consumers append deltas to the base without remapping. Pre-fix the delta
was an all-string parquet in the delta-sqlite vocabulary."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from export_parquet import DECISION_SCHEMA  # noqa: E402
from search_stack import publish_delta  # noqa: E402


@pytest.fixture
def mini_db(tmp_path):
    db = tmp_path / "decisions.db"
    conn = sqlite3.connect(db)
    cols = [c.strip() for c in publish_delta._SELECT_COLS.replace("\n", " ").split(",")
            if c.strip() and not c.strip().startswith("#")]
    conn.execute(f"CREATE TABLE decisions ({', '.join(f'{c} TEXT' for c in cols)})")
    base = {c: None for c in cols}
    r1 = dict(base, decision_id="bger_5A_1_2024", court="bger", canton="CH",
              docket_number="5A_1/2024", decision_date="2024-03-01",
              language="de", title="Urteil", full_text="Volltext des Urteils.",
              source_url="https://example.ch/1", marked_for_publication="1")
    r2 = dict(base, decision_id="zh_LB240001", court="zh_gerichte", canton="ZH",
              docket_number="LB240001", decision_date="2024-05-01",
              language="de", title="Beschluss", full_text="",
              source_url="https://example.ch/2")
    for r in (r1, r2):
        conn.execute(
            f"INSERT INTO decisions ({', '.join(cols)}) VALUES "
            f"({', '.join('?' for _ in cols)})", [r[c] for c in cols])
    conn.commit()
    conn.close()
    return db


def test_delta_parquet_matches_base_schema(mini_db, tmp_path):
    out = tmp_path / "delta-2026-07-02.parquet"
    n = publish_delta._export_parquet_base_schema(
        mini_db, {"bger_5A_1_2024", "zh_LB240001"}, out)
    assert n == 2
    table = pq.read_table(out)
    assert table.schema.equals(DECISION_SCHEMA), (
        f"delta schema diverges from base:\n{table.schema}\nvs\n{DECISION_SCHEMA}")
    rows = {r["decision_id"]: r for r in table.to_pylist()}
    assert rows["bger_5A_1_2024"]["has_full_text"] is True
    assert rows["bger_5A_1_2024"]["text_length"] == len("Volltext des Urteils.")
    assert rows["bger_5A_1_2024"]["marked_for_publication"] is True
    assert rows["zh_LB240001"]["has_full_text"] is False
    # shard-only fields are schema-present but NULL in deltas
    assert rows["bger_5A_1_2024"]["judges"] is None


def test_iter_raw_vs_mapped_vocabulary(mini_db):
    raw = list(publish_delta.iter_decisions_by_ids(
        mini_db, {"bger_5A_1_2024"}, raw=True))
    assert len(raw) == 1 and raw[0]["decision_id"] == "bger_5A_1_2024"
    assert "full_text" in raw[0]
    mapped = list(publish_delta.iter_decisions_by_ids(
        mini_db, {"bger_5A_1_2024"}))
    assert len(mapped) == 1 and mapped[0]["id"] == "bger_5A_1_2024"
    assert "content_text" in mapped[0]  # delta vocabulary unchanged
