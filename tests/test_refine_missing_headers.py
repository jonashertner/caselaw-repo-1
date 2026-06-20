"""Tests for the header-scan refinement of the 'missing' set."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from refine_missing_headers import (  # noqa: E402
    build_header_index,
    extract_header_dockets,
    refine,
)


def test_extract_underlying_docket_with_marker():
    head = "Urteilskopf 151 III 481 47. Auszug aus dem Urteil 4A_576/2024 vom 29. April 2025"
    keys = extract_header_dockets(head)
    assert "d:4a_576_2024" in keys          # underlying docket, followed by 'vom DATE'


def test_extract_marker_in_three_languages():
    assert "d:4a_282_2024" in extract_header_dockets("... 4A_282/2024 du 7 mai 2025")
    assert "d:e_3431_2021" in extract_header_dockets("Urteil E-3431/2021 vom 1. Juli 2021")
    assert "d:sk_2020_42" in extract_header_dockets("Sentenza SK.2020.42 del 5 maggio 2020")


def test_cited_docket_without_marker_is_excluded():
    # the decision's OWN docket (9C_612/2016, has the marker) is captured;
    # a CITED docket (9C_667/2016, no marker) is NOT — the false-positive case.
    head = "Urteil 9C_612/2016 vom 3. Juni 2016 ... mit Hinweis auf 9C_667/2016 E. 2"
    keys = extract_header_dockets(head)
    assert "d:9c_612_2016" in keys
    assert "d:9c_667_2016" not in keys


def test_build_index_and_refine_splits_present_vs_missing():
    d = sqlite3.connect(":memory:")
    d.execute("CREATE TABLE decisions (decision_id TEXT, full_text TEXT)")
    # a leading case stored under its BGE number, header names 4A_101/2014
    d.execute("INSERT INTO decisions VALUES ('bge_140 III 1', "
              "'Urteilskopf 140 III 1 Auszug aus dem Urteil 4A_101/2014 vom 3. Maerz 2014 Regeste')")
    idx = build_header_index(d)
    assert idx.get("d:4a_101_2014") == "bge_140 III 1"

    g = sqlite3.connect(":memory:")
    g.execute("CREATE TABLE citation_gaps (target_ref TEXT, citation_count INTEGER, "
              "normalized_key TEXT, classification TEXT)")
    g.executemany("INSERT INTO citation_gaps VALUES (?,?,?,?)", [
        ("4A_101/2014", 826, "d:4a_101_2014", "missing"),    # present under BGE → reclassify
        ("9Z_999/2099", 5, "d:9z_999_2099", "missing"),      # truly missing
    ])
    r = refine(g, idx)
    assert r["present_alt_id"] == 1
    assert r["truly_missing"] == 1
    assert r["examples"][0] == ("4A_101/2014", "bge_140 III 1")
