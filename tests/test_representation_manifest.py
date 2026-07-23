"""Offline fixture tests for the cross-identifier representation manifest builder.

Covers each linking rule (GE shared-url, VD unambiguous cross-ref, SH docket,
ch_vb byte-identical, nw/edoeb/ur shared-url date-agree), the conservative
date-disagreement holdout, the UR 1905-sentinel wildcard, member-uniqueness, the
VD interim/final ambiguity guard (never collapse two distinct rulings), and the
atomic write + manifest_meta. Fully offline; builds a synthetic decisions.db.
"""
from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _mk_db(path: Path, rows):
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE decisions (decision_id TEXT, court TEXT, docket_number TEXT,"
        " decision_date TEXT, source_url TEXT, content_hash TEXT, full_text TEXT)")
    conn.executemany(
        "INSERT INTO decisions (decision_id, court, docket_number, decision_date,"
        " source_url, content_hash, full_text) VALUES (?,?,?,?,?,?,?)", rows)
    conn.execute("PRAGMA user_version = 424242")
    conn.commit()
    conn.close()


def _fixture_rows():
    r = []
    # GE: two rows share source_url; one procedure page (A/...), one judgment code.
    r += [
        ("ge_j", "ge_gerichte", "ACJC/100/2024", "2024-05-01", "u/ge1", "h1", "judgment text long"),
        ("ge_p", "ge_gerichte", "A/500/2023", "2024-05-01", "u/ge1", "h2", "pub page"),
    ]
    # VD: findinfo rubrum cites an UNAMBIGUOUS procedure -> link.
    r += [
        ("vd_g1", "vd_gerichte", "PE21.006087", "2022-03-03", "u/vd1", "hv1", "arret"),
        ("vd_f1", "vd_findinfo", "HC/999", "2022-03-03", "u/vdf1", "hv2",
         "Rubrum PE21.006087 objet du present arret " + "x" * 50),
    ]
    # VD: an AMBIGUOUS procedure (interim + final share one procedure number) ->
    # must NOT be linked (would collapse two distinct rulings).
    r += [
        ("vd_g2a", "vd_gerichte", "PE20.111111", "2021-01-01", "u/vd2a", "hv3", "interim"),
        ("vd_g2b", "vd_gerichte", "PE20.111111", "2021-06-06", "u/vd2b", "hv4", "final"),
        ("vd_f2", "vd_findinfo", "HC/111", "2021-06-06", "u/vdf2", "hv5",
         "Rubrum PE20.111111 " + "y" * 50),
    ]
    # SH: sh_gerichte + sh_obergericht share normalized docket.
    r += [
        ("sh_g", "sh_gerichte", "Nr. 60/2024/13", "2024-02-02", "u/sh1", "hs1", "og text"),
        ("sh_o", "sh_obergericht", "60/2024/13", "2024-02-02", "u/sh2", "hs2", "archive"),
    ]
    # ch_vb: byte-identical (same source_url + content_hash) ingested twice.
    r += [
        ("vb_a", "ch_vb", "VPB 60.1", "2001-01-01", "u/vb1", "SAME", "vb long text here"),
        ("vb_b", "ch_vb", "<td>garbage</td>", "2001-01-01", "u/vb1", "SAME", "vb long text"),
    ]
    # nw: shared source_url, size-2, date-agree -> link.
    r += [
        ("nw_a", "nw_gerichte", "NW 1", "2019-09-09", "u/nw1", "hn1", "nw long text A"),
        ("nw_b", "nw_gerichte", "NW 1b", "2019-09-09", "u/nw1", "hn2", "nw B"),
    ]
    # ur: shared source_url pair where the twin carries the 1905 sentinel ->
    # wildcard, still linked.
    r += [
        ("ur_a", "ur_gerichte", "OG Z 24 2", "2025-06-24", "u/ur1", "hu1", "ur long text A"),
        ("ur_b", "ur_gerichte", "999", "1905-01-01", "u/ur1", "hu2", "ur B"),
    ]
    # edoeb: shared source_url but decision_dates DISAGREE (not the sentinel) ->
    # held out, NOT linked (conservative).
    r += [
        ("ed_a", "edoeb", "E 1", "2018-01-01", "u/ed1", "he1", "edoeb A long text"),
        ("ed_b", "edoeb", "E 2", "2019-12-31", "u/ed1", "he2", "edoeb B"),
    ]
    return r


@pytest.fixture()
def built(tmp_path, monkeypatch):
    _mk_db(tmp_path / "decisions.db", _fixture_rows())
    monkeypatch.setenv("SWISS_CASELAW_DIR", str(tmp_path))
    # import fresh so module-level SRC/OUT pick up the env
    sys.modules.pop("scripts.build_representation_manifest", None)
    mod = importlib.import_module("scripts.build_representation_manifest")
    assert mod.main() == 0
    conn = sqlite3.connect(str(tmp_path / "representation_manifest.db"))
    conn.row_factory = sqlite3.Row
    yield conn, tmp_path
    conn.close()


def _links(conn):
    return list(conn.execute(
        "SELECT * FROM decision_representations WHERE canonical_decision_id != member_decision_id"))


def test_ge_shared_url_links_publication_to_judgment(built):
    conn, _ = built
    rows = [r for r in _links(conn) if r["canton"] == "GE"]
    assert len(rows) == 1
    assert rows[0]["canonical_decision_id"] == "ge_j" and rows[0]["member_decision_id"] == "ge_p"


def test_vd_unambiguous_links_but_ambiguous_is_skipped(built):
    conn, _ = built
    vd = [r for r in _links(conn) if r["canton"] == "VD"]
    members = {r["member_decision_id"] for r in vd}
    assert "vd_f1" in members            # unambiguous procedure linked
    assert "vd_f2" not in members        # interim+final ambiguity -> NOT linked


def test_ambiguous_vd_procedure_never_collapses_distinct_rulings(built):
    conn, _ = built
    # neither interim nor final may appear as a member of the other
    members = {r["member_decision_id"] for r in _links(conn)}
    assert "vd_g2a" not in members and "vd_g2b" not in members


def test_sh_docket_link(built):
    conn, _ = built
    sh = [r for r in _links(conn) if r["canton"] == "SH"]
    assert len(sh) == 1 and sh[0]["canonical_decision_id"] == "sh_g"


def test_chvb_byte_identical_link(built):
    conn, _ = built
    vb = [r for r in _links(conn) if r["evidence_method"] == "byte_identical"]
    assert len(vb) == 1 and vb[0]["member_decision_id"] in {"vb_a", "vb_b"}


def test_nw_shared_url_date_agree_links(built):
    conn, _ = built
    nw = [r for r in _links(conn) if r["canton"] == "NW"]
    assert len(nw) == 1


def test_ur_1905_sentinel_is_date_wildcard(built):
    conn, _ = built
    ur = [r for r in _links(conn) if r["canton"] == "UR"]
    assert len(ur) == 1, "sentinel twin must still link"


def test_edoeb_date_disagreement_is_held_out(built):
    conn, _ = built
    ed = [r for r in _links(conn) if r["canton"] == "CH" and r["member_decision_id"].startswith("ed_")]
    assert ed == [], "genuinely date-disagreeing pair must NOT be linked"


def test_member_uniqueness_invariant(built):
    conn, _ = built
    n_rows, n_members = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT member_decision_id) FROM decision_representations").fetchone()
    assert n_rows == n_members


def test_manifest_meta_present_and_consistent(built):
    conn, tmp_path = built
    meta = dict(conn.execute("SELECT key, value FROM manifest_meta"))
    assert meta["source_user_version"] == "424242"
    assert meta["algo_version"]
    total = int(meta["source_total_rows"])
    reduction = int(meta["duplicate_representations"])
    assert int(meta["estimated_unique_decisions"]) == total - reduction
    # lower bound accounts for the held-out edoeb pair (band >= 1)
    assert int(meta["band_unlinked_date_disagree"]) >= 1
    assert int(meta["estimated_unique_lower_bound"]) <= int(meta["estimated_unique_decisions"])


def test_atomic_write_leaves_no_tmp(built):
    conn, tmp_path = built
    assert not (tmp_path / "representation_manifest.db.tmp").exists()
    assert (tmp_path / "representation_manifest.db").exists()
