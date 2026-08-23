"""The collector's three hard properties, tested offline.

1. Privacy is structural: a record carrying an identifier key anywhere in
   its tree aborts the run before a byte is written.
2. Trace export is gated on the /datenschutz/ amendment marker — the
   collector must refuse, not warn, while the marker is absent.
3. The identifier-scan exemption is exactly ONE dataset — the per-IP cost
   ledger archived by the 2026-08-23 decision. Growing that set must be a
   conscious act (a privacy-notice amendment), so the set is pinned here.

Everything else (which datasets, which paths) is configuration the
manifest documents; these three are the properties that must not regress.
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.collect_dev_data import (  # noqa: E402
    IDENTIFIER_EXEMPT_DATASETS,
    Collector,
    _scan,
)


def test_identifier_scan_finds_keys_at_any_depth():
    assert _scan({"ip": "1.2.3.4"})
    assert _scan({"a": {"b": [{"session_id": "x"}]}})
    assert _scan({"snapshot": {"clients": {"user_agent": "curl"}}})
    assert not _scan({"decision_id": "bge_1", "elapsed_s": 1.2,
                      "steps": {"2": True}})


def test_a_poisoned_record_aborts_before_anything_is_written(tmp_path):
    c = Collector(tmp_path)
    with pytest.raises(SystemExit, match="PRIVACY ABORT"):
        c._write_jsonl_gz("d", [{"ok": 1}, {"nested": {"ip": "x"}}], "s")
    assert not list(tmp_path.rglob("*.jsonl.gz")), \
        "abort must happen before the first write"


def test_clean_records_produce_a_shard_and_a_card(tmp_path):
    c = Collector(tmp_path)
    c._write_jsonl_gz("publish_runs", [{"type": "step", "elapsed_s": 3.1}],
                      "schema note here")
    d = tmp_path / "datasets" / "publish_runs"
    shard = d / f"{c.today}.jsonl.gz"
    assert shard.exists()
    with gzip.open(shard, "rt") as fh:
        assert json.loads(fh.readline())["type"] == "step"
    card = (d / "CARD.md").read_text()
    assert "schema note here" in card
    assert "May leave the private repo:** no" in card


def test_trace_export_refuses_without_the_amendment_marker(tmp_path):
    """The 0c gate. Export becomes possible the day the amended notice is
    live, by creating the marker — never by editing code."""
    c = Collector(tmp_path)
    with pytest.raises(SystemExit, match="traces export refused"):
        c.collect_traces()


def test_dry_run_writes_nothing(tmp_path):
    c = Collector(tmp_path, dry_run=True)
    c._write_jsonl_gz("d", [{"a": 1}], "s")
    assert not list(tmp_path.rglob("*"))


def test_exemption_set_is_exactly_the_ip_ledger():
    """Adding a dataset here means amending /datenschutz/ first. Widening
    this set without that step is the regression this test exists to stop."""
    assert IDENTIFIER_EXEMPT_DATASETS == {"llm_cost_by_ip"}


def test_ledger_dataset_may_carry_ip_and_still_writes(tmp_path):
    c = Collector(tmp_path)
    c._write_jsonl_gz(
        "llm_cost_by_ip",
        [{"ts": "2026-08-23T10:00:00+00:00", "ip": "203.0.113.7",
          "feature": "attest", "source": "rest", "cost_usd": 0.004}],
        "raw ledger archive")
    shard = tmp_path / "datasets" / "llm_cost_by_ip" / f"{c.today}.jsonl.gz"
    assert shard.exists()
    with gzip.open(shard, "rt") as fh:
        assert json.loads(fh.readline())["ip"] == "203.0.113.7"


def test_exemption_does_not_leak_to_other_datasets(tmp_path):
    """The same record that the ledger may carry still aborts anywhere else."""
    c = Collector(tmp_path)
    with pytest.raises(SystemExit, match="PRIVACY ABORT"):
        c._write_jsonl_gz(
            "llm_usage",
            [{"ts": "2026-08-23T10:00:00+00:00", "ip": "203.0.113.7"}],
            "s")
