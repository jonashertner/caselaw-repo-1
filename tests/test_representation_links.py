"""Phase-4 representation-link consumption (2026-07-15 design doc §7).

The manifest maps duplicate representations (same decision, two portal
identifiers — GE/VD/SH) onto one canonical decision_id. Phase 4 is the
link-only canary: `_representation_info` exposes the links additively and
`get_decision` annotates; NO row is hidden, NO search collapse (that is
Phase 5, invariant-#5-gated, explicit approval required).

Pinned here:
- member → canonical resolution via the UNIQUE(member) index;
- self-links (canonical,canonical) never surface as members;
- unlinked ids and singletons return None (the hot path — most of the
  corpus is unlinked and must not pay for the feature);
- an absent manifest degrades to None, never raises (sidecar posture).
"""
import sqlite3

import pytest

import mcp_server

SCHEMA = """
CREATE TABLE decision_representations (
    canonical_decision_id TEXT NOT NULL,
    member_decision_id    TEXT NOT NULL,
    canton                TEXT NOT NULL,
    relation_type         TEXT NOT NULL,
    evidence_method       TEXT NOT NULL,
    confidence            REAL NOT NULL,
    date_match            INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (canonical_decision_id, member_decision_id),
    UNIQUE (member_decision_id)
);
"""

ROWS = [
    # canonical A with one member B (the GE pair shape), incl. self-links
    # exactly as build_representation_manifest writes them
    ("ge_A", "ge_A", "GE", "alt_representation", "self", 1.0),
    ("ge_A", "ge_B", "GE", "alt_representation", "shared_source_url", 1.0),
    # three-member group: canonical X, members Y and Z
    ("vd_X", "vd_X", "VD", "alt_representation", "self", 1.0),
    ("vd_X", "vd_Y", "VD", "alt_representation", "procedure_cross_reference", 0.9),
    ("vd_X", "vd_Z", "VD", "alt_representation", "procedure_cross_reference", 0.9),
    # singleton: self-link only
    ("zh_S", "zh_S", "ZH", "alt_representation", "self", 1.0),
]


@pytest.fixture()
def manifest(tmp_path, monkeypatch):
    db = tmp_path / "representation_manifest.db"
    conn = sqlite3.connect(db)
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT INTO decision_representations "
        "(canonical_decision_id, member_decision_id, canton, relation_type,"
        " evidence_method, confidence) VALUES (?,?,?,?,?,?)", ROWS)
    conn.commit()
    conn.close()
    monkeypatch.setattr(mcp_server, "REPRESENTATION_MANIFEST_DB_PATH", db)
    monkeypatch.setattr(mcp_server, "_manifest_warned", False)
    return db


def test_member_resolves_to_canonical(manifest):
    info = mcp_server._representation_info("ge_B")
    assert info == {
        "canonical_decision_id": "ge_A",
        "is_canonical": False,
        "members": [],  # no OTHER non-canonical members; canonical is its own field
    }


def test_canonical_lists_members_without_self_link(manifest):
    info = mcp_server._representation_info("ge_A")
    assert info["is_canonical"] is True
    assert info["canonical_decision_id"] == "ge_A"
    assert [m["decision_id"] for m in info["members"]] == ["ge_B"]
    assert info["members"][0]["evidence_method"] == "shared_source_url"


def test_member_of_three_group_sees_sibling(manifest):
    info = mcp_server._representation_info("vd_Y")
    assert info["canonical_decision_id"] == "vd_X"
    assert info["is_canonical"] is False
    assert [m["decision_id"] for m in info["members"]] == ["vd_Z"]


def test_singleton_and_unlinked_return_none(manifest):
    assert mcp_server._representation_info("zh_S") is None
    assert mcp_server._representation_info("bger_1C_1_2020") is None
    assert mcp_server._representation_info("") is None
    assert mcp_server._representation_info(None) is None


def test_absent_manifest_degrades_to_none(tmp_path, monkeypatch):
    monkeypatch.setattr(mcp_server, "REPRESENTATION_MANIFEST_DB_PATH",
                        tmp_path / "nope.db")
    monkeypatch.setattr(mcp_server, "_manifest_warned", False)
    assert mcp_server._representation_info("ge_B") is None
