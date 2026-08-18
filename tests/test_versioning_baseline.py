"""Baseline pass: corpus-wide, network-free, idempotent, and it must not
invent a hash it does not have."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from versioning import VersionStore, content_hash  # noqa: E402
from versioning.baseline import run  # noqa: E402

H1 = content_hash(None, "text one")
H2 = content_hash(None, "text two")


@pytest.fixture()
def corpus(tmp_path):
    p = tmp_path / "decisions.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, "
              "content_hash TEXT, court TEXT, source_url TEXT, "
              "regeste TEXT, full_text TEXT)")
    c.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?)", [
        ("bger_1", H1, "bger", "https://x/1", None, "text one"),
        ("bger_2", H2, "bger", "https://x/2", None, "text two"),
        ("zh_3", None, "zh_obergericht", "https://x/3", None, "unhashed"),
        ("zh_4", "short", "zh_obergericht", "https://x/4", None, "bad hash"),
    ])
    c.commit()
    c.close()
    return str(p)


def test_baselines_every_hashed_decision(corpus, tmp_path):
    st = run(corpus, str(tmp_path / "v.db"))
    assert st["scanned"] == 4
    assert st["baselined"] == 2
    assert st["no_hash"] == 2          # NULL and malformed are skipped
    assert st["already"] == 0


def test_skipped_decisions_are_not_invented(corpus, tmp_path):
    """A guessed baseline hash would fire a phantom change on first
    refresh — worse than no baseline at all."""
    sp = str(tmp_path / "v.db")
    run(corpus, sp)
    s = VersionStore(sp)
    assert s.current_hash("zh_3") is None
    assert s.current_hash("zh_4") is None
    assert s.current_hash("bger_1") == H1
    s.close()


def test_idempotent(corpus, tmp_path):
    sp = str(tmp_path / "v.db")
    run(corpus, sp)
    second = run(corpus, sp)
    assert second["baselined"] == 0
    assert second["already"] == 2
    s = VersionStore(sp)
    assert len(s.versions("bger_1")) == 1
    s.close()


def test_dry_run_writes_nothing(corpus, tmp_path):
    sp = str(tmp_path / "v.db")
    st = run(corpus, sp, dry_run=True)
    assert st["baselined"] == 2
    s = VersionStore(sp)
    assert s.current_hash("bger_1") is None
    s.close()


def test_baseline_then_change_produces_version_two(corpus, tmp_path):
    """End to end: baseline from the corpus, then a court edit."""
    sp = str(tmp_path / "v.db")
    run(corpus, sp)
    s = VersionStore(sp)
    klass = s.record_observation("bger_1", regeste=None,
                                 full_text="text one, corrected",
                                 previous_text="text one")
    assert klass in ("text_substantive", "citation_affecting")
    vs = s.versions("bger_1")
    assert len(vs) == 2
    assert s.reconstruct("bger_1", 1, "text one, corrected") == "text one"
    s.close()


def test_baseline_marks_verification_unchecked(corpus, tmp_path):
    """Baselining is not a portal check: these rows must still be due for
    their first real refresh."""
    sp = str(tmp_path / "v.db")
    run(corpus, sp)
    s = VersionStore(sp)
    row = s.conn.execute("SELECT check_count FROM verification_log "
                         "WHERE decision_id='bger_1'").fetchone()
    assert row["check_count"] == 0
    s.close()


def test_limit_is_honoured(corpus, tmp_path):
    st = run(corpus, str(tmp_path / "v.db"), limit=1)
    assert st["scanned"] == 1
