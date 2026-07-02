"""dates.docket_year_plausibility — a decision cannot predate its docket's
registration year by more than the year-boundary edge case. Strict year
extraction only (cause-year and cited-docket noise must NOT count)."""
from __future__ import annotations

import sqlite3

from quality.checks import dates
from quality.types import Severity


def test_strict_extraction_positions():
    f = dates.docket_registration_year
    # trusted: trailing separator-year
    assert f("5A_1008/2025") == 2025
    assert f("HC/2024.15") == 2024        # code/year.number
    assert f("VSKLA.2024.5") == 2024      # code.year.number
    assert f("SR2 2025 84") == 2025       # code year number
    assert f("RRB.2023.000445") == 2023
    assert f("ACJC/35/2007") == 2007      # trailing year
    # NOT trusted (NULL over guess):
    assert f("150 II 1") is None          # BGE volume, no year
    assert f("C/17720") is None
    assert f("A 12") is None
    assert f("") is None
    # cause-number year mid-string without code prefix match
    assert f("17720/2005 vs 4A_1/2020") == 2020  # trailing wins, single year


def test_check_counts_only_impossible_rows(tmp_path):
    db = tmp_path / "d.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, "
        "docket_number TEXT, decision_date TEXT)")
    rows = [
        # impossible: dated 2 years before registration -> COUNTS
        ("a1", "ag_verwaltungsgericht", "WBE.2020.195", "2010-09-16"),
        # year-boundary edge (delta -1) -> tolerated
        ("a2", "zh_gerichte", "LB250001/2025", "2024-12-30"),
        # normal (delta 0/+1) -> fine
        ("a3", "bger", "5A_1/2024", "2024-03-01"),
        ("a4", "bger", "4A_2/2023", "2024-01-15"),
        # slow proceeding (delta +5) -> NOT counted (only negative is impossible)
        ("a5", "bvger", "A-123/2019", "2024-06-01"),
        # no extractable year -> skipped
        ("a6", "bge", "150 II 1", "2024-05-01"),
        # naive-extraction trap: trailing year is the registration year,
        # earlier cause year must not create a false positive
        ("a7", "ge_gerichte", "ACJC/35/2007", "2007-01-19"),
    ]
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?)",
                     [(r[0], r[1], r[2], r[3]) for r in rows])
    conn.commit()

    ro = sqlite3.connect(db)
    ro.row_factory = sqlite3.Row
    res = dates.check_docket_year_plausibility(ro)
    assert res.metric_value == 1                      # only a1
    assert res.severity is Severity.WARNING           # alerts, never blocks
    assert res.passed is True                          # 1 <= baseline
    assert res.extra["by_court"] == {"ag_verwaltungsgericht": 1}
    assert res.sample_rows and res.sample_rows[0]["decision_id"] == "a1"
