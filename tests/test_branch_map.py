"""P1.1: branch derivation — court map, per-court docket rules, chamber
keywords; NULL over guess. Shared by build_fts5 and export_parquet."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from branch_map import derive_branch  # noqa: E402


def test_single_branch_courts():
    assert derive_branch("zh_sozialversicherungsgericht") == "sozialversicherung"
    assert derive_branch("bstger") == "straf"
    assert derive_branch("bvger") == "oeffentlich"
    assert derive_branch("bpatger") == "zivil"
    assert derive_branch("finma_versicherungsrecht") == "oeffentlich"  # regulator, NOT Sozialversicherung
    assert derive_branch("ecthr_grand_chamber") == "oeffentlich"


def test_bger_division_prefixes():
    f = lambda dk: derive_branch("bger", None, dk)
    assert f("1C_146/2025") == "oeffentlich"
    assert f("2C_212/2025") == "oeffentlich"
    assert f("4A_156/2025") == "zivil"
    assert f("5A_1008/2025") == "zivil"
    assert f("6B_1/2024") == "straf"
    assert f("9C_652/2025") == "sozialversicherung"
    assert f("I 123/04") == "sozialversicherung"     # EVG register
    assert f("U 15/2003") == "sozialversicherung"
    assert f("3X_1/2020") is None                     # unknown division


def test_bge_volume_romans():
    f = lambda dk: derive_branch("bge", None, dk)
    assert f("150 II 1") == "oeffentlich"
    assert f("148 III 21") == "zivil"
    assert f("149 IV 9") == "straf"
    assert f("151 V 5") == "sozialversicherung"
    assert f("120 Ia 1") == "oeffentlich"
    assert f("BGE ohne Nummer") is None


def test_ge_and_vd_series():
    assert derive_branch("ge_gerichte", None, "ATAS/1001/2007") == "sozialversicherung"
    assert derive_branch("ge_gerichte", None, "ATA/55/2020") == "oeffentlich"   # ATAS must win before ATA
    assert derive_branch("ge_gerichte", None, "ACJC/35/2007") == "zivil"
    assert derive_branch("ge_gerichte", None, "AARP/12/2023") == "straf"
    assert derive_branch("vd_gerichte", None, "CASSO 2020/1") == "sozialversicherung"
    assert derive_branch("vd_findinfo", None, "CDAP GE.2021.0001") == "oeffentlich"
    assert derive_branch("vd_omni", None, "CACI 15/2019") == "zivil"


def test_zh_og_and_be_registers():
    assert derive_branch("zh_obergericht", None, "LB200012") == "zivil"
    assert derive_branch("zh_obergericht", None, "RT250043") == "zivil"
    assert derive_branch("zh_obergericht", None, "SB240100") == "straf"
    assert derive_branch("zh_obergericht", None, "XY123") is None
    assert derive_branch("be_zivilstraf", None, "ZK 2020 123") == "zivil"
    assert derive_branch("be_zivilstraf", None, "SK 2021 45") == "straf"


def test_chamber_keywords_and_traps():
    assert derive_branch("ti_gerichte", "Camera civile", "X") == "zivil"
    assert derive_branch("so_gerichte", "Strafkammer", "X") == "straf"
    assert derive_branch("lu_gerichte", "Kantonsgericht, Abteilung Sozialversicherungsrecht", "X") == "sozialversicherung"
    assert derive_branch("gr_gerichte", "Verwaltungsgericht", "X") == "oeffentlich"
    # trap: Zivilstandswesen is administrative, must NOT map to zivil
    assert derive_branch("gr_gerichte", "Zivilstandswesen", "X") is None
    # NULL over guess
    assert derive_branch("lu_gerichte", None, "1A 12 34") is None
    assert derive_branch(None) is None


def test_build_pass_populates_column(tmp_path):
    import build_fts5 as b
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, "
                 "court TEXT, chamber TEXT, docket_number TEXT, branch TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?,NULL)", [
        ("d1", "bger", None, "4A_156/2025"),
        ("d2", "bvger", None, "A-1/2020"),
        ("d3", "lu_gerichte", None, "opaque"),
    ])
    conn.commit()
    n = b._derive_branch_column(conn)
    assert n == 2
    got = dict(conn.execute("SELECT decision_id, branch FROM decisions"))
    assert got == {"d1": "zivil", "d2": "oeffentlich", "d3": None}


def test_parquet_normalize_derives_branch():
    from export_parquet import normalize_row
    row = normalize_row({"decision_id": "x", "court": "bger",
                         "docket_number": "6B_9/2024", "full_text": "t",
                         "source_url": "u", "language": "de", "canton": "CH"})
    assert row["branch"] == "straf"


def test_separator_variants_from_corpus():
    # space-separated BGer (~45k rows) + dot pre-2007 + underscore EVG
    assert derive_branch("bger", None, "1C 146/2025") == "oeffentlich"
    assert derive_branch("bger", None, "2A.123/2005") == "oeffentlich"
    assert derive_branch("bger", None, "I_350/1999") == "sozialversicherung"
    # historical BGE with underscores
    assert derive_branch("bge", None, "84_II_437") == "oeffentlich"
    assert derive_branch("bge", None, "112_V_9") == "sozialversicherung"
    # VD registers, GE JTBL, GR series
    assert derive_branch("vd_findinfo", None, "AI 123/09 - 456") == "sozialversicherung"
    assert derive_branch("vd_findinfo", None, "ML / 2010 / 55") == "zivil"
    assert derive_branch("ge_gerichte", None, "JTBL/565/2021") == "zivil"
    assert derive_branch("gr_gerichte", None, "VR2 2025 66") == "oeffentlich"
    assert derive_branch("gr_gerichte", None, "ZR1 2025 80") == "zivil"
    assert derive_branch("gr_gerichte", None, "PKG 2022 6") is None  # mixed volume


def test_docket_chamber_code_shapes():
    from branch_map import docket_chamber_code as f
    # register shape CODE.YYYY.N / CODE YYYY N
    assert f("ag_verwaltungsgericht", "WBE.2020.195") == "WBE"
    assert f("gr_gerichte", "SR2 2025 84") == "SR2"
    assert f("sz_gerichte", "VSKLA.2024.5") == "VSKLA"
    # series slash
    assert f("ge_gerichte", "ATAS/1001/2007") == "ATAS"
    assert f("ge_gerichte", "JTAPI/12/2020") == "JTAPI"
    # bger divisions + EVG
    assert f("bger", "5A_1008/2025") == "5A"
    assert f("bger", "1C 146/2025") == "1C"
    assert f("bger", "I_350/1999") == "I"
    # bge volume roman
    assert f("bge", "150 II 1") == "II"
    # vd loose forms are vd-scoped
    assert f("vd_findinfo", "HC / 2010 / 123") == "HC"
    assert f("vd_findinfo", "AI 123/09 - 456") == "AI"
    assert f("vd_gerichte", "CDAP GE.2021.0001") == "CDAP"
    assert f("zh_gerichte", "HC / 2010 / 123") is None  # loose rule NOT global
    # document-type words are never chamber codes
    assert f("vd_findinfo", "Jug / 2014 / 33") is None
    assert f("vd_findinfo", "Arrêt / 2014 / 7") is None
    assert f("ti_gerichte", "34.2025.27") is None  # numeric register: no code
    assert f("bger", None) is None


def test_fill_chamber_pass_never_overwrites(tmp_path):
    import sqlite3, build_fts5 as b
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, "
                 "court TEXT, chamber TEXT, docket_number TEXT)")
    conn.executemany("INSERT INTO decisions VALUES (?,?,?,?)", [
        ("d1", "ag_verwaltungsgericht", None, "WBE.2020.195"),   # filled
        ("d2", "bger", "", "5A_1/2024"),                          # filled
        ("d3", "ge_gerichte", "Chambre civile", "ACJC/1/2020"),   # portal value kept
        ("d4", "ti_gerichte", None, "34.2025.27"),                # no code -> stays NULL
    ])
    conn.commit()
    n = b._fill_chamber_from_docket(conn)
    assert n == 2
    got = dict(conn.execute("SELECT decision_id, chamber FROM decisions"))
    assert got == {"d1": "WBE", "d2": "5A", "d3": "Chambre civile", "d4": None}


def test_be_zivilstraf_extended_registers():
    # LegalStats feedback 2026-07-02: their chamber dictionary was richer
    # (403 vs 603 zivil rows) — ABS/KES/HG/CIV are zivil registers.
    for dk, want in [("ABS 2021 12", "zivil"), ("KES 2023 5", "zivil"),
                     ("HG 2020 1", "zivil"), ("CIV 2019 44", "zivil"),
                     ("SK 2022 9", "straf")]:
        assert derive_branch("be_zivilstraf", None, dk) == want, dk
