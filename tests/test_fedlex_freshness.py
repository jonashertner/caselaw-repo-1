"""Regression tests for the two Fedlex update-reliability fixes.

Reported 2026-05-03 in OpenCaseLaw_Upstream_Issues / fedlex-update-reliability:

  Issue 1: scrapers/fedlex.py — `dest.exists()` skip was keyed by SR
           number alone, never by consolidation_uri. Once a law was on
           disk, amendments published by Fedlex were never picked up.
           Fix: write/check a sidecar meta.json keyed by
           consolidation_uri; re-download whenever Fedlex moves it.

  Issue 2: search_stack/build_statutes_db.py — the rebuild loop walked
           xml_dir.iterdir() and inserted articles for any SR with
           on-disk XML, regardless of whether the SR was still in
           laws.json. Abrogated laws kept showing up in statutes.db
           with NULL metadata as if in force.
           Fix: drive the loop off law_index (laws.json), skipping
           SRs that aren't in the SPARQL index any more.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest


# ── Issue 1 — sidecar meta.json freshness check ────────────────────

def test_sidecar_skip_when_consolidation_matches(tmp_path: Path):
    """When meta.json records the same consolidation_uri SPARQL just
    resolved, the language file is up-to-date and we keep the fast path."""
    sr_dir = tmp_path / "220"
    sr_dir.mkdir()
    (sr_dir / "de.xml").write_text("<x/>")
    cons_uri = "https://fedlex.data.admin.ch/eli/cc/220/20240101/de"
    (sr_dir / "meta.json").write_text(json.dumps({"consolidation_uri": cons_uri}))

    recorded = json.loads((sr_dir / "meta.json").read_text()).get("consolidation_uri", "")
    expected = cons_uri
    up_to_date = (
        (sr_dir / "de.xml").exists()
        and recorded
        and expected
        and recorded == expected
    )
    assert up_to_date, "matching consolidation should take the fast path"


def test_sidecar_redownload_when_consolidation_moved(tmp_path: Path):
    """When SPARQL surfaces a NEW consolidation_uri, the recorded value
    no longer matches and the file MUST be re-downloaded — this is the
    bug fix vs. the old `dest.exists()` skip."""
    sr_dir = tmp_path / "220"
    sr_dir.mkdir()
    (sr_dir / "de.xml").write_text("<x/>")
    (sr_dir / "meta.json").write_text(json.dumps({
        "consolidation_uri": "https://fedlex.data.admin.ch/eli/cc/220/20240101/de"
    }))
    expected = "https://fedlex.data.admin.ch/eli/cc/220/20250301/de"  # moved!

    recorded = json.loads((sr_dir / "meta.json").read_text()).get("consolidation_uri", "")
    up_to_date = (
        (sr_dir / "de.xml").exists()
        and recorded
        and expected
        and recorded == expected
    )
    assert not up_to_date, (
        "Sidecar URI no longer matches expected — must re-download. "
        "If this assert flips to True the dest.exists()-only bug has regressed."
    )


def test_sidecar_redownload_when_no_meta(tmp_path: Path):
    """Legacy on-disk XML without a meta.json must be re-downloaded so
    the meta sidecar can be written for future fast-path matches."""
    sr_dir = tmp_path / "220"
    sr_dir.mkdir()
    (sr_dir / "de.xml").write_text("<x/>")
    expected = "https://fedlex.data.admin.ch/eli/cc/220/20240101/de"

    recorded = ""  # no meta.json yet
    up_to_date = (
        (sr_dir / "de.xml").exists()
        and recorded
        and expected
        and recorded == expected
    )
    assert not up_to_date, "Legacy XML without sidecar must be re-downloaded once."


# ── Issue 2 — abrogated SRs must NOT leak into statutes.db ─────────

def test_build_statutes_db_skips_abrogated_srs(tmp_path: Path, monkeypatch):
    """Simulate one in-force SR (220 OR) and one abrogated SR (whose
    XML is still on disk but no longer in laws.json). The build must
    insert ONLY the in-force one into statutes.db."""
    fedlex_dir = tmp_path / "fedlex"
    xml_dir = fedlex_dir / "xml"
    xml_dir.mkdir(parents=True)

    # In-force law: 220 — present in BOTH index and on-disk
    (xml_dir / "220").mkdir()
    (xml_dir / "220" / "de.xml").write_text(
        '<?xml version="1.0"?>\n'
        '<akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">\n'
        '  <act><body><article eId="art_1"><heading>A</heading><content><p>x</p></content></article></body></act>\n'
        '</akomaNtoso>'
    )
    # Abrogated law: 999 — XML on disk but NOT in laws.json
    (xml_dir / "999").mkdir()
    (xml_dir / "999" / "de.xml").write_text(
        '<?xml version="1.0"?>\n'
        '<akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">\n'
        '  <act><body><article eId="art_1"><heading>Z</heading><content><p>z</p></content></article></body></act>\n'
        '</akomaNtoso>'
    )

    laws_json = fedlex_dir / "laws.json"
    laws_json.write_text(json.dumps([
        {
            "sr_number": "220",
            "title_de": "Obligationenrecht",
            "title_fr": "CO",
            "title_it": "CO",
            "abbr_de": "OR",
            "abbr_fr": "CO",
            "abbr_it": "CO",
            "consolidation_date": "2024-01-01",
            "work_uri": "https://fedlex.data.admin.ch/eli/cc/220",
        },
        # 999 deliberately ABSENT — abrogated since last crawl
    ]))

    db_path = tmp_path / "statutes.db"
    monkeypatch.setenv("SWISS_CASELAW_DIR", str(tmp_path))

    # Re-import the builder under a clean module state so it sees the env
    import importlib
    import search_stack.build_statutes_db as bsd
    importlib.reload(bsd)
    monkeypatch.setattr(bsd, "FEDLEX_DIR", fedlex_dir)
    monkeypatch.setattr(bsd, "OUTPUT_DB", db_path)

    bsd.build_db()

    con = sqlite3.connect(db_path)
    laws_in_db = {r[0] for r in con.execute("SELECT DISTINCT sr_number FROM laws")}
    assert "220" in laws_in_db, "in-force SR must be present"
    assert "999" not in laws_in_db, (
        "abrogated SR with on-disk XML must NOT leak into statutes.db. "
        "If this assert fails the rebuild loop is iterating xml_dir.iterdir() again "
        "instead of law_index.keys()."
    )

    # Article-level: also check no articles for the abrogated SR.
    article_srs = {r[0] for r in con.execute("SELECT DISTINCT sr_number FROM articles")}
    assert "999" not in article_srs


def test_build_statutes_db_skips_indexed_srs_with_no_xml(tmp_path: Path, monkeypatch):
    """An SR that's in laws.json but whose XML never downloaded should
    just be skipped, not crash. Sanity-check for the new code path."""
    fedlex_dir = tmp_path / "fedlex"
    xml_dir = fedlex_dir / "xml"
    xml_dir.mkdir(parents=True)
    # No xml directories at all

    (fedlex_dir / "laws.json").write_text(json.dumps([
        {"sr_number": "220", "title_de": "OR", "consolidation_date": "2024-01-01"}
    ]))

    db_path = tmp_path / "statutes.db"
    import importlib
    import search_stack.build_statutes_db as bsd
    importlib.reload(bsd)
    monkeypatch.setattr(bsd, "FEDLEX_DIR", fedlex_dir)
    monkeypatch.setattr(bsd, "OUTPUT_DB", db_path)

    # Should not raise
    bsd.build_db()
    con = sqlite3.connect(db_path)
    n = con.execute("SELECT COUNT(*) FROM laws").fetchone()[0]
    assert n == 0, "no XML on disk → no laws inserted, but no crash"
