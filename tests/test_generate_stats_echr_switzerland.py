"""Regression test for generate_stats echr_switzerland respondent filter.

Background — 2026-08-28 bug:
  out["echr_switzerland"] counted ALL rows in the five ECHR courts
  (bge_egmr, hudoc_ch, ecthr_chamber, ecthr_committee,
  ecthr_grand_chamber). But ecthr_chamber / ecthr_grand_chamber hold
  judgments against all 46 Council of Europe respondent states, so the
  "total cases against Switzerland" headline was inflated by ~8,000
  non-Swiss rows, the grand_chamber sub-count included every state's
  Grand Chamber judgments, and most_recent could point at a case that
  had nothing to do with Switzerland.

The respondent axis lives in `canton` — historically named but holding
a jurisdiction code: CH = Swiss-respondent (also set by construction
for bge_egmr / hudoc_ch), CE = non-Swiss ECtHR respondent states (see
scrapers/hudoc.py _RESPONDENT_TO_CANTON). The fix filters every
Swiss-slice query on canton='CH' and reports the unfiltered corpus
size separately as ecthr_corpus_total.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

import sys

sys.path.insert(0, str(REPO))

from db_schema import SCHEMA_SQL  # noqa: E402
from generate_stats import collect_interesting_stats  # noqa: E402

THIS_YEAR = datetime.now(timezone.utc).year

# (decision_id, court, canton, docket, decision_date)
FIXTURE_ROWS = [
    # Swiss-respondent rows — the 4 that belong in the headline.
    ("bge_egmr_1", "bge_egmr", "CH", "12345/90", "1995-06-01"),
    ("hudoc_ch_1", "hudoc_ch", "CH", "23456/94", "2001-03-15"),
    ("ecthr_chamber_ch_1", "ecthr_chamber", "CH", "34567/18",
     f"{THIS_YEAR}-02-10"),
    ("ecthr_gc_ch_1", "ecthr_grand_chamber", "CH", "53600/20",
     f"{THIS_YEAR}-04-09"),
    # Non-Swiss respondents (canton='CE') — must NOT count toward the
    # Swiss slice, and must not win most_recent despite being newest.
    ("ecthr_chamber_ce_1", "ecthr_chamber", "CE", "45678/19",
     f"{THIS_YEAR}-05-01"),
    ("ecthr_chamber_ce_2", "ecthr_chamber", "CE", "56789/20",
     f"{THIS_YEAR}-06-01"),
    ("ecthr_gc_ce_1", "ecthr_grand_chamber", "CE", "67890/21",
     f"{THIS_YEAR}-07-01"),
    # Unrelated domestic court — never part of the ECHR block.
    ("bger_1", "bger", "CH", "6B_1/2020", f"{THIS_YEAR}-01-20"),
]


@pytest.fixture()
def repo_dir(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    out.mkdir()
    conn = sqlite3.connect(out / "decisions.db")
    conn.executescript(SCHEMA_SQL)
    conn.executemany(
        "INSERT INTO decisions "
        "(decision_id, court, canton, docket_number, decision_date, "
        " language, full_text) "
        "VALUES (?, ?, ?, ?, ?, 'de', 'x')",
        FIXTURE_ROWS,
    )
    conn.commit()
    conn.close()
    return tmp_path


def test_swiss_slice_excludes_other_respondent_states(repo_dir: Path):
    stats = collect_interesting_stats(repo_dir)
    block = stats["echr_switzerland"]

    # 4 Swiss-respondent rows, not the 7-row ECHR corpus and not the
    # 8-row database.
    assert block["total"] == 4
    # Only the Swiss Grand Chamber judgment, not the CE one.
    assert block["grand_chamber"] == 1
    # This year's Swiss-respondent rows: chamber CH + grand chamber CH.
    assert block[f"in_{THIS_YEAR}"] == 2
    # The full corpus across all respondents is still reported, so the
    # gap between this block and a by_court sum stays self-explaining.
    assert block["ecthr_corpus_total"] == 7


def test_most_recent_is_a_swiss_case(repo_dir: Path):
    block = collect_interesting_stats(repo_dir)["echr_switzerland"]
    # The newest rows in the fixture are CE; most_recent must skip them.
    assert block["most_recent"]["docket"] == "53600/20"
