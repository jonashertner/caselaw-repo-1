"""In-build canonical date correction (audit C-2): apply_to_db replaces synthetic
YYYY-01-01 BGE dates with the text-verified Urteilsdatum on the built decisions
DB, so search/sort/filter use the real date. Only synthetic BGE dates are
touched; real dates and non-BGE rows are never changed. Exercised through the
real decisions_fts + sync triggers so the UPDATE is proven to pass them.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import backfill_canonical_identity as bci  # noqa: E402

BGE_152_HEAD = ("Urteilskopf 152 II 1 1. Auszug aus dem Urteil … 9C_113/2025 "
                "vom 27. September 2025 Regeste …")


def _db(path):
    c = sqlite3.connect(path)
    c.executescript(
        """
        CREATE TABLE decisions(decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT,
            docket_number TEXT, decision_date TEXT, publication_date TEXT,
            language TEXT, title TEXT, regeste TEXT, full_text TEXT);
        CREATE VIRTUAL TABLE decisions_fts USING fts5(decision_id UNINDEXED, court,
            canton, docket_number, language, title, regeste, full_text);
        CREATE TRIGGER decisions_ai AFTER INSERT ON decisions BEGIN
          INSERT INTO decisions_fts(decision_id,court,canton,docket_number,language,title,regeste,full_text)
          VALUES(new.decision_id,new.court,new.canton,new.docket_number,new.language,new.title,new.regeste,new.full_text);
        END;
        CREATE TRIGGER decisions_ad AFTER DELETE ON decisions BEGIN
          DELETE FROM decisions_fts WHERE decision_id=old.decision_id; END;
        CREATE TRIGGER decisions_au AFTER UPDATE ON decisions BEGIN
          DELETE FROM decisions_fts WHERE decision_id=old.decision_id;
          INSERT INTO decisions_fts(decision_id,court,canton,docket_number,language,title,regeste,full_text)
          VALUES(new.decision_id,new.court,new.canton,new.docket_number,new.language,new.title,new.regeste,new.full_text);
        END;
        """
    )
    rows = [
        ("bge_152 II 1", "bge", "CH", "152 II 1", "2026-01-01", None, "de", "", "", BGE_152_HEAD),
        ("bge_old", "bge", "CH", "100 II 5", "1974-01-01", None, "de", "", "", "no parseable date here"),
        ("bger_real", "bger", "CH", "9C_113/2025", "2025-09-27", None, "de", "", "", "x"),
    ]
    c.executemany("INSERT INTO decisions VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
    c.commit()
    return c


def test_apply_corrects_only_synthetic_bge(tmp_path):
    c = _db(tmp_path / "d.db")
    n_date, n_pub = bci.apply_to_db(c, max_date="2026-06-28")
    assert n_date == 1 and n_pub >= 1
    g = lambda did: c.execute("SELECT decision_date, publication_date, date_provenance "
                              "FROM decisions WHERE decision_id=?", (did,)).fetchone()
    # synthetic BGE with recoverable text date -> corrected + provenance + pub date
    assert g("bge_152 II 1") == ("2025-09-27", "2026-01-01", "extracted_from_text")
    # synthetic BGE with NO recoverable date -> date NOT changed, flagged
    assert g("bge_old")[0] == "1974-01-01" and g("bge_old")[2] in ("volume_synthetic", "null")
    # real non-BGE date untouched
    assert g("bger_real")[0] == "2025-09-27"
    # search-by-corrected-date now works (the whole point)
    found = c.execute("SELECT decision_id FROM decisions WHERE decision_date='2025-09-27' "
                      "AND court='bge'").fetchone()
    assert found and found[0] == "bge_152 II 1"


def test_apply_is_idempotent(tmp_path):
    c = _db(tmp_path / "d.db")
    bci.apply_to_db(c, max_date="2026-06-28")
    n_date, _ = bci.apply_to_db(c, max_date="2026-06-28")   # second run: nothing left synthetic
    assert n_date == 0
