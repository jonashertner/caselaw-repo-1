"""Privacy contract (/datenschutz/ #2): search query content is never logged.
Guards the tool_call logging whitelist against re-introduction of free-text,
user-intent-revealing fields (regression 2026-07-06: query text was reaching
the systemd journal, contradicting the public contract)."""
from __future__ import annotations

import re
from pathlib import Path

SRC = (Path(__file__).resolve().parents[1] / "mcp_server.py").read_text()


def _structural_keys():
    m = re.search(r"_STRUCTURAL_LOG_KEYS\s*=\s*\(([^)]*)\)", SRC, re.S)
    assert m, "_STRUCTURAL_LOG_KEYS tuple not found"
    return set(re.findall(r'"([a-z_]+)"', m.group(1)))


def test_freetext_fields_never_logged():
    keys = _structural_keys()
    for banned in ("query", "case", "topic", "facts", "reference"):
        assert banned not in keys, f"free-text field {banned!r} must not be logged"


def test_log_args_uses_the_guarded_whitelist():
    # the tool_call logger must build _log_args from the structural whitelist,
    # not an inline set that could drift.
    assert "_log_args = {k: v for k, v in arguments.items() if k in _STRUCTURAL_LOG_KEYS}" in SRC


def test_structural_keys_are_nonfreetext():
    # sanity: the retained keys are filters / identifiers, not content
    keys = _structural_keys()
    assert "court" in keys and "language" in keys and "decision_id" in keys
    assert keys  # non-empty
