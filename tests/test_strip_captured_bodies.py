"""Removing bodies from capture files without breaking capture itself.

`attest_response` names its document `draft_text`, which was missing from
the body list, so 169 drafted legal opinions reached disk before it was
caught. This script removed them in place.

Two things it has to get right, and the second one bit on the first run.

It must replace the body with its length and leave the rest of the record
alone — the timestamp, tool, session and client are the telemetry the
notice describes and are worth keeping.

And it must preserve owner and mode across the swap. The server appends
to these files as `mcp`; run as root, `Path.replace` leaves them
root-owned, every later append fails with EACCES, and `_capture_event`
swallows the error — so capture stops dead and reports nothing. Five
minutes of production capture were lost that way before the file was
noticed not growing.
"""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
for p in (str(REPO), str(REPO / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from strip_captured_bodies import process, strip_record  # noqa: E402


def _write(tmp_path: Path, records) -> Path:
    f = tmp_path / "capture_2026-08-20.jsonl"
    f.write_text("\n".join(json.dumps(r) for r in records) + "\n",
                 encoding="utf-8")
    return f


def test_a_body_becomes_its_length_and_the_record_survives():
    rec = {"src": "mcp", "ts": "t", "sid": "s", "client": "chatgpt",
           "tool": "attest_response",
           "args": {"draft_text": "x" * 3000, "audit_grounding": True}}
    out, n = strip_record(rec)
    assert n == 1
    assert out["args"] == {"draft_text_len": 3000, "audit_grounding": True}
    assert out["tool"] == "attest_response" and out["client"] == "chatgpt"
    assert out["ts"] == "t" and out["sid"] == "s"


def test_a_search_query_is_left_alone():
    """Queries are what the notice says we collect; they must survive."""
    out, n = strip_record({"args": {"query": "Mietzinsdepot", "limit": 5}})
    assert n == 0 and out["args"]["query"] == "Mietzinsdepot"


def test_an_unlisted_long_string_is_caught_by_length():
    out, n = strip_record({"args": {"never_seen_before": "y" * 900}})
    assert n == 1 and out["args"]["never_seen_before_len"] == 900


def test_records_without_args_are_untouched():
    rec = {"src": "rest", "path": "/api/decisions", "status": 200}
    out, n = strip_record(rec)
    assert n == 0 and out == rec


def test_the_file_keeps_every_record(tmp_path):
    f = _write(tmp_path, [
        {"src": "mcp", "args": {"draft_text": "x" * 2000}},
        {"src": "mcp", "args": {"query": "Mietrecht"}},
        {"src": "rest", "path": "/x"},
    ])
    lines, n = process(f, apply=True)
    kept = [json.loads(x) for x in f.read_text(encoding="utf-8").splitlines()
            if x.strip()]
    assert lines == 3 and n == 1 and len(kept) == 3
    assert kept[0]["args"] == {"draft_text_len": 2000}
    assert kept[1]["args"]["query"] == "Mietrecht"


def test_owner_and_mode_survive_the_swap(tmp_path):
    """The bug that cost five minutes of live capture: a root-owned file
    the `mcp` worker can no longer append to, failing silently."""
    f = _write(tmp_path, [{"src": "mcp", "args": {"draft_text": "x" * 2000}}])
    os.chmod(f, 0o640)
    before = f.stat()
    process(f, apply=True)
    after = f.stat()
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert stat.S_IMODE(after.st_mode) == stat.S_IMODE(before.st_mode)


def test_a_dry_run_writes_nothing(tmp_path):
    f = _write(tmp_path, [{"src": "mcp", "args": {"draft_text": "x" * 2000}}])
    original = f.read_text(encoding="utf-8")
    lines, n = process(f, apply=False)
    assert n == 1, "it still reports what it would remove"
    assert f.read_text(encoding="utf-8") == original
