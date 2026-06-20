"""Tests for the unresolved-citation gap oracle (Completeness Plan 1a).

Cases are real refs from the live unresolved-citation preview (2026-06-20), so
the classifier is validated against actual data, not invented examples.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

from citation_gap_oracle import (  # noqa: E402
    build_gap_table,
    classify_ref,
    corpus_keys_for,
    extract_underlying_dockets,
    normalize_ref,
)


# ── normalize_ref ───────────────────────────────────────────────────────────

def test_normalize_bge_bare_form():
    assert normalize_ref("123 V 419") == "bge:123:v:419"
    assert normalize_ref("133 II 629") == "bge:133:ii:629"
    assert normalize_ref("9 V 547") == "bge:9:v:547"
    # with an explicit BGE/ATF prefix → same key
    assert normalize_ref("BGE 123 V 419") == "bge:123:v:419"
    assert normalize_ref("ATF 133 II 629") == "bge:133:ii:629"


def test_normalize_bger_docket():
    assert normalize_ref("4C_310_1996") == "d:4c_310_1996"
    assert normalize_ref("4C.310/1996") == "d:4c_310_1996"   # separator-agnostic
    assert normalize_ref("1P_477_1993") == "d:1p_477_1993"


def test_normalize_bvger_docket():
    assert normalize_ref("E_3431_2021") == "d:e_3431_2021"
    assert normalize_ref("A-6366-2017") == "d:a_6366_2017"


def test_normalize_rejects_noise():
    assert normalize_ref("URK_ \n2") is None
    assert normalize_ref("URK_ 2") is None
    assert normalize_ref("COO_2207_105") is None
    assert normalize_ref("") is None
    assert normalize_ref("see above") is None


# ── classify_ref ────────────────────────────────────────────────────────────

def test_classify_missing_when_not_in_corpus():
    # well-formed pre-2000 BGer docket, absent from corpus → genuine gap
    assert classify_ref("4C_310_1996", "docket", set()) == "missing"


def test_classify_resolution_bug_when_corpus_has_it():
    # corpus HAS BGE 123 V 419 (docket stored with prefix); bare ref didn't resolve
    corpus = corpus_keys_for(docket_number="BGE 123 V 419", court="bge")
    assert "bge:123:v:419" in corpus
    assert classify_ref("123 V 419", "docket", corpus) == "resolution_bug"


def test_classify_noise_for_garbage_and_nondecision_type():
    assert classify_ref("URK_ 2", "docket", set()) == "noise"
    assert classify_ref("4C_310_1996", "statute", set()) == "noise"  # wrong type


def test_corpus_keys_from_all_identifier_fields():
    # decision_id form (space/prefix variants) must key the same as the ref
    assert corpus_keys_for(decision_id="bger_5D_78_2017") == {"d:5d_78_2017"}
    assert corpus_keys_for(docket_number="5D 78/2017") == {"d:5d_78_2017"}  # space sep
    assert corpus_keys_for(decision_id="es_bger_2C_590_2013") == {"d:2c_590_2013"}
    assert corpus_keys_for(decision_id="bge_BGE_140_III_86") == {"bge:140:iii:86"}


def test_classify_resolution_bug_via_decision_id_form():
    # the 18% false-missing case: present as bger_5D_78_2017, cited as 5D_78/2017
    corpus = corpus_keys_for(decision_id="bger_5D_78_2017", docket_number="5D 78/2017")
    assert classify_ref("5D_78/2017", "docket", corpus) == "resolution_bug"


def test_extract_underlying_dockets_bger_to_bge_crossref():
    head = ("Urteilskopf 151 III 481 47. Auszug aus dem Urteil der I. "
            "zivilrechtlichen Abteilung i.S. A. AG gegen B. 4A_576/2024 vom 29. April 2025")
    assert "d:4a_576_2024" in extract_underlying_dockets(head)
    # so a citation by the BGer docket resolves to the BGE entry
    corpus = set(extract_underlying_dockets(head))
    assert classify_ref("4A_576/2024", "docket", corpus) == "resolution_bug"


# ── end-to-end build over synthetic DBs ─────────────────────────────────────

def _graph_db():
    g = sqlite3.connect(":memory:")
    g.execute("CREATE TABLE citation_targets (target_ref TEXT, target_decision_id TEXT)")
    g.execute("CREATE TABLE decision_citations (source_decision_id TEXT, target_ref TEXT, "
              "target_type TEXT, mention_count INTEGER, is_prior_instance INTEGER)")
    return g


def test_build_gap_table_classifies_and_ranks():
    g = _graph_db()
    # one resolved ref (BGE 140 III 86, cited by s1)
    g.execute("INSERT INTO citation_targets VALUES ('140 III 86', 'bge_140_iii_86')")
    cites = [
        ("s1", "140 III 86", "bge", 1, 0),       # resolved → excluded
        ("s1", "4C_310_1996", "docket", 1, 0),    # missing, 2 sources
        ("s2", "4C_310_1996", "docket", 1, 0),
        ("s3", "4C_310_1996", "docket", 1, 0),
        ("s1", "123 V 419", "docket", 1, 0),      # resolution_bug (corpus has it)
        ("s1", "URK_ 2", "docket", 1, 0),         # noise
    ]
    g.executemany("INSERT INTO decision_citations VALUES (?,?,?,?,?)", cites)

    decisions = sqlite3.connect(":memory:")
    decisions.execute("CREATE TABLE decisions (decision_id TEXT, docket_number TEXT, "
                      "docket_number_2 TEXT, court TEXT, full_text TEXT)")
    decisions.execute("INSERT INTO decisions VALUES "
                      "('bge_123_V_419', 'BGE 123 V 419', NULL, 'bge', NULL)")

    out = sqlite3.connect(":memory:")
    summary = build_gap_table(g, decisions, out)

    assert summary == {"missing": 1, "resolution_bug": 1, "noise": 1}
    top = out.execute(
        "SELECT target_ref, citation_count, distinct_sources FROM citation_gaps "
        "WHERE classification='missing'"
    ).fetchone()
    assert top == ("4C_310_1996", 3, 3)
    assert out.execute(
        "SELECT classification FROM citation_gaps WHERE target_ref='123 V 419'"
    ).fetchone()[0] == "resolution_bug"
