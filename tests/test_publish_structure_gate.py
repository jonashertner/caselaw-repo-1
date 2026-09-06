"""Step 2g builds the structure sidecar from the served text, behind a coverage gate."""
import shutil
import sqlite3
from pathlib import Path

import publish


def _decisions(path, ids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT)")
    con.executemany("INSERT INTO decisions VALUES (?, 'bger')", [(i,) for i in ids])
    con.commit(); con.close()


def _sidecar(path, ids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE erwaegungen_paragraph (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text TEXT)")
    con.executemany("INSERT INTO erwaegungen_paragraph VALUES (?, '1', 1, NULL, 'x')", [(i,) for i in ids])
    con.execute("CREATE INDEX idx_erw_decision ON erwaegungen_paragraph(decision_id)")
    con.commit(); con.close()


def test_structure_coverage_counts_current_decisions_only(tmp_path):
    _decisions(tmp_path / "decisions.db", ["a", "b", "c"])
    _sidecar(tmp_path / "side.db", ["a", "b", "stale_1", "stale_2"])
    assert publish._structure_coverage(tmp_path / "side.db", tmp_path / "decisions.db") == 2
    assert publish._structure_coverage(tmp_path / "missing.db", tmp_path / "decisions.db") is None


def _setup(tmp_path, monkeypatch, old_ids, new_ids, ok=True):
    out = tmp_path / "output"; out.mkdir()
    _decisions(out / "decisions.db", ["a", "b", "c", "d", "e"])
    _sidecar(out / "decision_structure.db", old_ids)
    new = tmp_path / "new.db"; _sidecar(new, new_ids)
    monkeypatch.setattr(publish, "OUTPUT_DIR", out)
    monkeypatch.setattr(publish, "REPO_DIR", Path(publish.__file__).parent)
    calls = []
    def fake_run_cmd(cmd, description, dry_run=False, **kwargs):
        calls.append((cmd, description, kwargs))
        if ok:
            shutil.copy(new, out / "decision_structure.db.tmp")
        return ok
    monkeypatch.setattr(publish, "run_cmd", fake_run_cmd)
    fallback = []
    monkeypatch.setattr(publish, "_step_2g_from_shards", lambda dry_run=False: fallback.append(dry_run) or True)
    return out, calls, fallback


def test_served_text_sidecar_is_swapped_in_when_coverage_holds(tmp_path, monkeypatch):
    out, calls, fallback = _setup(tmp_path, monkeypatch, old_ids=["a", "b", "c"], new_ids=["a", "b", "c", "d"])
    assert publish.step_2g_build_decision_structure() is True
    cmd = calls[0][0]
    assert "extract_decision_structure_incremental.py" in cmd[1] and "--output" in cmd and cmd[-1].endswith("decision_structure.db.tmp")
    assert calls[0][2]["timeout"] == 14400 and not fallback
    assert not (out / "decision_structure.db.tmp").exists()
    assert publish._structure_coverage(out / "decision_structure.db", out / "decisions.db") == 4


def test_coverage_gate_keeps_the_old_sidecar(tmp_path, monkeypatch):
    out, calls, fallback = _setup(tmp_path, monkeypatch, old_ids=["a", "b", "c", "d", "e"], new_ids=["a"])
    assert publish.step_2g_build_decision_structure() is False
    assert not (out / "decision_structure.db.tmp").exists() and not fallback
    assert publish._structure_coverage(out / "decision_structure.db", out / "decisions.db") == 5


def test_failed_extractor_falls_back_to_the_shard_build(tmp_path, monkeypatch):
    out, calls, fallback = _setup(tmp_path, monkeypatch, old_ids=["a"], new_ids=["a", "b"], ok=False)
    assert publish.step_2g_build_decision_structure() is True and fallback == [False]
    assert publish._structure_coverage(out / "decision_structure.db", out / "decisions.db") == 1


def test_full_rebuild_forces_a_full_extraction(tmp_path, monkeypatch):
    out, calls, fallback = _setup(tmp_path, monkeypatch, old_ids=["a"], new_ids=["a", "b"])
    assert publish.step_2g_build_decision_structure(full_rebuild=True) is True and "--force-full" in calls[0][0]
