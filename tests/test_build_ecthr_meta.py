"""ecthr_meta sidecar: the fields the Decision schema cannot carry.

Every assertion here is about something that is currently lost on the way
into decisions.db — the respondent state behind canton 'CE', the full
application-number list behind the three-appno docket, importance beyond
the marked_for_publication boolean, and the structured conclusion behind
the concatenated regeste.

Offline throughout: stubs are fixtures shaped like scrapers/hudoc.py's
_group_judgments output. No network, per the tests-stay-offline invariant.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from search_stack.build_ecthr_meta import (
    _importance,
    _respondent_states,
    _separate_opinion,
    build,
    stub_to_rows,
)


def _stub(**over):
    """A discovery stub with the field names _group_judgments emits."""
    base = {
        "court": "ecthr_chamber",
        "decision_id": "ecthr_chamber_332_57_19601114",
        "docket_number": "332/57",
        "decision_date": date(1960, 11, 14),
        "item_id": "001-57516",
        "appno": "332/57",
        "docname": "AFFAIRE LAWLESS c. IRLANDE (N° 1)",
        "doc_type": "Judgment (Merits)",
        "respondent": "IRL",
        "ecli": "ECLI:CE:ECHR:1960:1114JUD000033257",
        "article": "8;8-1;41",
        "conclusion": "Exception préliminaire rejetée",
        "violation": "8",
        "nonviolation": "6-1",
        "importance": "1",
    }
    base.update(over)
    return base


# ── field parsing ───────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("1", 1), ("2", 2), ("3", 3),
    ("Key case", 1), ("key cases", 1),
    ("", None), (None, None), ("nonsense", None),
])
def test_importance_parsing(raw, expected):
    assert _importance(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("CHE", ["CHE"]),
    ("che", ["CHE"]),
    # Inter-state and joined cases name several respondents; a single
    # column would make "cases against Switzerland" a substring match.
    ("CHE;FRA", ["CHE", "FRA"]),
    ("CHE, FRA", ["CHE", "FRA"]),
    ("CHE;CHE", ["CHE"]),
    ("", []), (None, []),
])
def test_respondent_splitting(raw, expected):
    assert _respondent_states(raw) == expected


def test_respondent_order_is_hudoc_order():
    assert _respondent_states("FRA;CHE") == ["FRA", "CHE"]


@pytest.mark.parametrize("raw,expected", [
    ("TRUE", 1), ("FALSE", 0), ("true", 1),
    (True, 1), (False, 0),
    ("", None), (None, None),
])
def test_separate_opinion_parsing(raw, expected):
    """HUDOC sends 'TRUE'/'FALSE' text — bool('FALSE') would be True."""
    assert _separate_opinion(raw) == expected


def test_stub_to_rows_preserves_what_the_decision_schema_drops():
    meta_row, resp = stub_to_rows(_stub())
    assert meta_row[0] == "ecthr_chamber_332_57_19601114"
    assert meta_row[4] == 1                      # importance, not a bool
    assert meta_row[5] == "8;8-1;41"             # structured articles
    assert meta_row[6] == "Exception préliminaire rejetée"
    assert meta_row[12] == "1960-11-14"          # date serialised
    assert resp == [("ecthr_chamber_332_57_19601114", "IRL")]


# ── build ───────────────────────────────────────────────────────────────

def _open(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def test_build_writes_meta_and_respondents(tmp_path):
    out = tmp_path / "ecthr_meta.db"
    stats = build([
        _stub(),
        _stub(decision_id="ecthr_grand_chamber_2170_24",
              court="ecthr_grand_chamber", respondent="CHE", importance="1"),
    ], out)

    assert stats["judgments"] == 2
    assert stats["swiss_respondent"] == 1

    conn = _open(out)
    assert conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0] == 2
    swiss = conn.execute(
        "SELECT decision_id FROM ecthr_respondent WHERE state='CHE'"
    ).fetchall()
    assert [r[0] for r in swiss] == ["ecthr_grand_chamber_2170_24"]
    conn.close()


def test_multi_respondent_case_is_queryable_by_each_state(tmp_path):
    """The reason respondent is a table, not a column."""
    out = tmp_path / "m.db"
    build([_stub(decision_id="ecthr_chamber_multi", respondent="CHE;FRA")], out)

    conn = _open(out)
    for state in ("CHE", "FRA"):
        got = conn.execute(
            "SELECT COUNT(*) FROM ecthr_respondent WHERE state=?", (state,)
        ).fetchone()[0]
        assert got == 1, f"{state} should match the joined case"
    conn.close()


def test_full_appno_list_survives(tmp_path):
    """The docket truncates to three appnos; the sidecar must not.

    Multi-applicant judgments carry thousands of characters of
    application numbers (Turan and Others v. Turkey reaches 3,795).
    """
    many = ";".join(f"{i}/20" for i in range(60))
    out = tmp_path / "a.db"
    build([_stub(decision_id="ecthr_chamber_many", appno=many)], out)

    conn = _open(out)
    stored = conn.execute(
        "SELECT appno_full FROM ecthr_meta WHERE decision_id='ecthr_chamber_many'"
    ).fetchone()[0]
    assert stored == many
    assert stored.count(";") == 59
    conn.close()


def test_rebuild_is_idempotent_and_atomic(tmp_path):
    """Second build replaces rather than duplicating, and leaves no .tmp."""
    out = tmp_path / "i.db"
    build([_stub()], out)
    build([_stub(conclusion="Violation de l'art. 8")], out)

    conn = _open(out)
    rows = conn.execute("SELECT conclusion FROM ecthr_meta").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Violation de l'art. 8"
    conn.close()
    assert not (tmp_path / "i.db.tmp").exists()


def test_stub_without_decision_id_is_skipped_not_fatal(tmp_path):
    out = tmp_path / "s.db"
    stats = build([_stub(), {"court": "ecthr_chamber"}], out)
    assert stats["judgments"] == 1
    assert stats["skipped"] == 1


def test_meta_table_records_provenance(tmp_path):
    out = tmp_path / "p.db"
    build([_stub()], out, generated_at="2026-08-28T00:00:00+00:00")
    conn = _open(out)
    meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    assert meta["generated_at"] == "2026-08-28T00:00:00+00:00"
    assert meta["judgments"] == "1"
    conn.close()


def test_swiss_respondent_query_is_the_point(tmp_path):
    """'Every judgment against Switzerland' as an indexed lookup.

    This is what canton='CH' approximated and could not express for
    joined cases.
    """
    out = tmp_path / "ch.db"
    build([
        _stub(decision_id="a", respondent="CHE"),
        _stub(decision_id="b", respondent="IRL"),
        _stub(decision_id="c", respondent="FRA;CHE"),
    ], out)

    conn = _open(out)
    got = {r[0] for r in conn.execute(
        "SELECT decision_id FROM ecthr_respondent WHERE state='CHE'"
    )}
    assert got == {"a", "c"}
    conn.close()


# ── swap guard ──────────────────────────────────────────────────────────
# An atomic swap installs a truncated sidecar just as cleanly as a complete
# one. Discovery is 68 independent year shards, so a HUDOC outage yields a
# well-formed, wrong answer rather than an error.

def test_shrinking_build_is_refused(tmp_path):
    from search_stack.build_ecthr_meta import IncompleteBuild

    out = tmp_path / "g.db"
    build([_stub(decision_id=f"d{i}") for i in range(100)], out)

    with pytest.raises(IncompleteBuild, match="refusing to publish"):
        build([_stub(decision_id="d0")], out)   # 1 of 100 — a lost shard

    # The live sidecar must be untouched by the refusal.
    conn = _open(out)
    assert conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0] == 100
    conn.close()
    # ...and the short read is kept for inspection, not silently discarded.
    assert (tmp_path / "g.db.tmp").exists()


def test_small_shrink_within_tolerance_is_allowed(tmp_path):
    """HUDOC withdrawals are real; only a collapse needs a human."""
    out = tmp_path / "t.db"
    build([_stub(decision_id=f"d{i}") for i in range(100)], out)
    build([_stub(decision_id=f"d{i}") for i in range(95)], out)

    conn = _open(out)
    assert conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0] == 95
    conn.close()


def test_growth_is_always_allowed(tmp_path):
    out = tmp_path / "grow.db"
    build([_stub(decision_id="d0")], out)
    build([_stub(decision_id=f"d{i}") for i in range(50)], out)
    conn = _open(out)
    assert conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0] == 50
    conn.close()


def test_allow_shrink_overrides_the_guard(tmp_path):
    out = tmp_path / "o.db"
    build([_stub(decision_id=f"d{i}") for i in range(100)], out)
    build([_stub(decision_id="d0")], out, min_ratio=None)
    conn = _open(out)
    assert conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0] == 1
    conn.close()


def test_first_build_has_nothing_to_compare_against(tmp_path):
    out = tmp_path / "first.db"
    stats = build([_stub()], out)
    assert stats["judgments"] == 1
