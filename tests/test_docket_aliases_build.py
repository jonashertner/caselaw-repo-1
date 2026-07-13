"""Build-pass tests for _build_docket_aliases (issue #41)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import build_fts5  # noqa: E402
from db_schema import SCHEMA_SQL  # noqa: E402


def _conn():
    c = sqlite3.connect(":memory:")
    c.executescript(SCHEMA_SQL)
    return c


def _ins(c, decision_id, court, docket, full_text):
    c.execute(
        "INSERT INTO decisions (decision_id, court, canton, docket_number, "
        "language, full_text) VALUES (?,?,?,?,?,?)",
        (decision_id, court, "CH", docket, "de", full_text),
    )


def test_build_maps_joined_dockets():
    c = _conn()
    _ins(c, "bger_1B_242_2022", "bger", "1B 242/2022",
         "Bundesgericht 30.05.2022 1B 242/2022 (1B_242/2022)\n"
         "1B_242/2022, 1B_243/2022 und 1B_244/2022\nUrteil vom 30. Mai 2022\n")
    _ins(c, "bger_9C_1_2020", "bger", "9C 1/2020",
         "Bundesgericht 9C 1/2020\n9C_1/2020\nUrteil vom 1. Januar 2020\n")  # not consolidated
    n = build_fts5._build_docket_aliases(c)
    assert n == 2, n
    got = dict(c.execute(
        "SELECT alias_docket_norm, canonical_decision_id FROM decision_docket_aliases"
    ).fetchall())
    assert got == {"1B_243/2022": "bger_1B_242_2022",
                   "1B_244/2022": "bger_1B_242_2022"}, got


def test_alias_that_is_a_primary_docket_is_skipped():
    # Reciprocal consolidation: both 1B_100/2011 and 1B_99/2011 exist as leads.
    # Neither alias is stored (each resolves directly as a primary).
    c = _conn()
    _ins(c, "bger_1B_100_2011", "bger", "1B 100/2011",
         "Bundesgericht\n1B_100/2011, 1B_99/2011\nUrteil vom 28. März 2011\n")
    _ins(c, "bger_1B_99_2011", "bger", "1B 99/2011",
         "Bundesgericht\n1B_99/2011, 1B_100/2011\nUrteil vom 28. März 2011\n")
    n = build_fts5._build_docket_aliases(c)
    assert n == 0, "aliases that are themselves primary dockets must be skipped"
    assert c.execute("SELECT COUNT(*) FROM decision_docket_aliases").fetchone()[0] == 0


def test_revision_reference_not_mapped():
    c = _conn()
    _ins(c, "bger_1A.104_2005", "bger", "1A.104/2005",
         "Bundesgericht 28.04.2005 1A.104/2005\n"
         "Revision des bundesgerichtlichen Urteils vom 17. März 2005 (1A.278/2004)\n"
         "1A.104/2005 /ggs\nUrteil vom 28. April 2005\n")
    n = build_fts5._build_docket_aliases(c)
    assert n == 0, "a revision reference in prose is not a joined docket"


def test_rebuild_is_idempotent():
    c = _conn()
    _ins(c, "bger_1B_242_2022", "bger", "1B 242/2022",
         "1B_242/2022, 1B_243/2022\nUrteil vom 30. Mai 2022\n")
    build_fts5._build_docket_aliases(c)
    build_fts5._build_docket_aliases(c)  # second run must not double-insert
    assert c.execute("SELECT COUNT(*) FROM decision_docket_aliases").fetchone()[0] == 1


def test_ambiguous_alias_kept_as_multiple_rows():
    # Same secondary docket named by two different consolidations (rare) -> two
    # rows, so the serve side can detect ambiguity. Neither alias is itself a
    # primary here.
    c = _conn()
    _ins(c, "bger_6B_10_2020", "bger", "6B 10/2020",
         "6B_10/2020, 6B_500/2020\nUrteil vom 1. Januar 2020\n")
    _ins(c, "bger_6B_20_2020", "bger", "6B 20/2020",
         "6B_20/2020, 6B_500/2020\nUrteil vom 2. Januar 2020\n")
    build_fts5._build_docket_aliases(c)
    targets = [r[0] for r in c.execute(
        "SELECT canonical_decision_id FROM decision_docket_aliases "
        "WHERE alias_docket_norm='6B_500/2020'")]
    assert set(targets) == {"bger_6B_10_2020", "bger_6B_20_2020"}, targets
