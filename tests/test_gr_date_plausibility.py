"""Backlog L2: GR Praxis-digest (PKG/PVG) rows carried body-text dates —
statute-validity dates, LOWER-court ruling dates, referendum cutoffs — as
decision_date (proven 2026-07-02 via the shard's date_extraction provenance,
e.g. 'PVG 2021 1' dated 2018-01-01 = the IVV validity date in the digest
head). Fix: docket-year plausibility guards both recovery paths, and a
normalize pass NULLs the gr stock (NULL over guess)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import build_fts5 as b  # noqa: E402


def test_docket_registration_year_positions():
    assert b._docket_registration_year("PKG 2022 6") == 2022
    assert b._docket_registration_year("PVG 2021 1") == 2021
    assert b._docket_registration_year("SR2 2025 84") == 2025
    assert b._docket_registration_year("5A_1008/2025") == 2025
    assert b._docket_registration_year("150 II 1") is None
    assert b._docket_registration_year("") is None
    assert b._docket_registration_year(None) is None


def test_plausibility_band():
    # >3y before the docket year = junk
    assert b._recovered_date_plausible("2012-03-11", "PVG 2022 15") is False
    assert b._recovered_date_plausible("2017-01-01", "PVG 2021 1") is False
    # boundary (exactly -3) is KEPT: a Praxis volume can publish a 3y-old
    # decision, so the known-junk 'PVG 2021 1'->2018-01-01 deliberately
    # survives this conservative band (still visible in the QC baseline).
    assert b._recovered_date_plausible("2018-01-01", "PVG 2021 1") is True
    # within band: volume may publish decisions up to 3y old
    assert b._recovered_date_plausible("2019-04-30", "PKG 2022 6") is True
    assert b._recovered_date_plausible("2022-05-01", "PKG 2022 6") is True
    # no strict docket year, or garbage date -> no veto
    assert b._recovered_date_plausible("2012-01-01", "150 II 1") is True
    assert b._recovered_date_plausible("garbage", "PKG 2022 6") is True


def test_inline_recovery_skips_junk_and_finds_true_date():
    # First anchor carries the LOWER court's 2019 date (junk for a 2024
    # volume); a later anchor carries the true 2024 ruling date.
    text = (
        "Praxis des Kantonsgerichts. "
        "Mit Entscheid vom 30. April 2019 schrieb das Regionalgericht das "
        "Verfahren ab. Dagegen ging A. in Berufung. "
        "Mit Urteil vom 12. September 2024 erkennt das Kantonsgericht: ..."
        + " Fülltext" * 20
    )
    assert b._date_recover_inline("gr_gerichte", text, "PKG 2024 3") == "2024-09-12"
    # With ONLY the junk date present, the guard returns None (not the junk).
    text_junk_only = (
        "Praxis. Mit Entscheid vom 30. April 2019 schrieb das Regionalgericht "
        "das Verfahren ab." + " Fülltext" * 20
    )
    assert b._date_recover_inline("gr_gerichte", text_junk_only, "PKG 2024 3") is None
    # Same text WITHOUT a docket year: no veto, first anchor date wins.
    assert b._date_recover_inline("gr_gerichte", text_junk_only, None) == "2019-04-30"


def test_null_implausible_gr_dates_scoped():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, "
        "docket_number TEXT, decision_date TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?)", [
        ("g1", "gr_gerichte", "PVG 2022 15", "2012-03-11"),   # junk -> NULL
        ("g2", "gr_gerichte", "PKG 2022 6", "2019-04-30"),    # -3, kept
        ("g3", "gr_gerichte", "SR2 2025 84", "2012-09-25"),   # junk -> NULL
        ("g4", "gr_gerichte", "ZK1 2024 12", "2024-05-01"),   # sane, kept
        ("o1", "zh_gerichte", "LB 2024 1", "2012-01-01"),     # other court, untouched
    ])
    conn.commit()
    n = b._null_implausible_gr_dates(conn)
    assert n == 2
    dates = dict(conn.execute(
        "SELECT decision_id, decision_date FROM decisions"))
    assert dates["g1"] is None and dates["g3"] is None
    assert dates["g2"] == "2019-04-30"
    assert dates["g4"] == "2024-05-01"
    assert dates["o1"] == "2012-01-01"
