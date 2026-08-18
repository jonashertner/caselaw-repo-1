"""Version store: reconstruction must be exact, or history is worthless.

Design: docs/design/decision-versioning.md
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from versioning import (  # noqa: E402
    VersionStore, apply_reverse_diff, classify_change, content_hash,
    make_reverse_diff,
)

DEC = """Sachverhalt
A. Die Beschwerdefuehrerin ruegt eine Verletzung von Art. 29 BV.
B. Das Bundesgericht hat in BGE 148 IV 356 E. 2.1 entschieden.
Erwaegungen
1. Die Beschwerde ist zulaessig (BGE 141 IV 289 E. 1).
2. Der Grundsatz gilt seit 6B_267/2012 vom 9. Juli 2012.
"""


@pytest.fixture()
def store(tmp_path):
    s = VersionStore(tmp_path / "v.db")
    yield s
    s.close()


# ── round-trip: the property everything else rests on ────────────────

@pytest.mark.parametrize("old,new", [
    (DEC, DEC.replace("148 IV 356", "148 I 356")),          # citation edit
    (DEC, DEC + "3. Neue Erwaegung angefuegt.\n"),          # append
    (DEC, DEC.replace("Erwaegungen\n", "")),                # deletion
    (DEC, "vollstaendig ersetzt\n"),                        # full rewrite
    ("", DEC),                                              # empty -> text
    (DEC, ""),                                              # text -> empty
    ("eine zeile ohne newline", "andere zeile ohne newline"),
])
def test_reverse_diff_round_trips(old, new):
    blob = make_reverse_diff(new, old)
    assert apply_reverse_diff(new, blob) == old


def test_round_trip_preserves_hash(store):
    newer = DEC.replace("148 IV 356", "148 I 356")
    store.record_observation("d1", regeste=None, full_text=DEC)
    store.record_observation("d1", regeste=None, full_text=newer,
                             previous_text=DEC)
    got = store.reconstruct("d1", 1, newer)
    assert got == DEC
    assert content_hash(None, got) == content_hash(None, DEC)


# ── lifecycle ────────────────────────────────────────────────────────

def test_first_seen_then_unchanged(store):
    assert store.record_observation("d", regeste=None, full_text=DEC) == "first_seen"
    assert store.record_observation("d", regeste=None, full_text=DEC) == "unchanged"
    assert len(store.versions("d")) == 1


def test_change_creates_second_version(store):
    newer = DEC.replace("148 IV 356", "148 I 356")
    store.record_observation("d", regeste=None, full_text=DEC)
    klass = store.record_observation("d", regeste=None, full_text=newer,
                                     previous_text=DEC)
    assert klass == "citation_affecting"
    vs = store.versions("d")
    assert len(vs) == 2
    assert vs[0]["superseded_at"] is not None
    assert vs[1]["superseded_at"] is None          # v2 is current
    assert vs[1]["content_hash"] == content_hash(None, newer)


def test_three_versions_chain(store):
    v2 = DEC.replace("zulaessig", "unzulaessig")
    v3 = v2 + "4. Zusatz.\n"
    store.record_observation("d", regeste=None, full_text=DEC)
    store.record_observation("d", regeste=None, full_text=v2, previous_text=DEC)
    store.record_observation("d", regeste=None, full_text=v3, previous_text=v2)
    assert store.reconstruct("d", 2, v3) == v2
    assert len(store.versions("d")) == 3


def test_missing_previous_text_is_flagged_not_guessed(store):
    store.record_observation("d", regeste=None, full_text=DEC)
    klass = store.record_observation("d", regeste=None, full_text="anders\n")
    assert klass == "text_unavailable"
    assert store.versions("d")[0]["reverse_diff"] is None


def test_wrong_previous_text_is_rejected(store):
    """Storing a diff against text we never held would corrupt the chain."""
    store.record_observation("d", regeste=None, full_text=DEC)
    with pytest.raises(ValueError, match="hash mismatch"):
        store.record_observation("d", regeste=None, full_text="neu\n",
                                 previous_text="etwas ganz anderes\n")


# ── classification drives the review queue ───────────────────────────

def test_citation_change_wins_over_text_change():
    old = DEC
    new = DEC.replace("148 IV 356", "148 I 356").replace("Sachverhalt", "SV")
    assert classify_change(old, new) == "citation_affecting"


def test_docket_citation_change_detected():
    assert classify_change(DEC, DEC.replace("6B_267/2012", "6B_268/2012")) \
        == "citation_affecting"


def test_whitespace_only_is_metadata():
    assert classify_change(DEC, DEC.replace("\n", "\n ")) == "metadata_only"


def test_substantive_text_change():
    assert classify_change(DEC, DEC.replace("zulaessig", "unzulaessig")) \
        == "text_substantive"


# ── refresh rotation ─────────────────────────────────────────────────

def test_unseen_decisions_are_due(store):
    assert store.due_for_check(["a", "b"]) == ["a", "b"]


def test_recently_checked_not_due(store):
    store.record_observation("a", regeste=None, full_text=DEC)
    assert store.due_for_check(["a"], max_age_days=30) == []


def test_stale_check_is_due(store):
    store.record_observation("a", regeste=None, full_text=DEC,
                             observed_at="2020-01-01T00:00:00+00:00")
    assert store.due_for_check(["a"], max_age_days=30) == ["a"]


# ── governance ───────────────────────────────────────────────────────

def test_removal_deletes_content_keeps_tombstone(store):
    newer = DEC + "Zusatz\n"
    store.record_observation("d", regeste=None, full_text=DEC)
    store.record_observation("d", regeste=None, full_text=newer,
                             previous_text=DEC)
    store.remove_version("d", 1, authority="Obergericht X", reason="Anonymisierung")
    assert store.versions("d")[0]["reverse_diff"] is None
    row = store.conn.execute(
        "SELECT * FROM version_removals WHERE decision_id='d'").fetchone()
    assert row["authority"] == "Obergericht X"
    assert row["version_no"] == 1


def test_hash_recipe_matches_build_fts5():
    """Must equal SHA-256((regeste or '') + (full_text or '')) exactly."""
    import hashlib
    expect = hashlib.sha256(("R" + "F").encode("utf-8")).hexdigest()
    assert content_hash("R", "F") == expect
    assert content_hash(None, None) == hashlib.sha256(b"").hexdigest()
