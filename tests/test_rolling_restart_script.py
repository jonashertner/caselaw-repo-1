"""Rolling-restart script + practice-unit wiring (2026-07).

opencaselaw-practice.service restarted 4 of 8 workers simultaneously every
Saturday 06:00 UTC — downtime under ip_hash, and workers 8774-8777 kept a
stale practice.db handle forever.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "rolling_restart_workers.sh"
UNIT = REPO / "systemd" / "opencaselaw-practice.service"


def test_script_parses():
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def test_script_discovers_units_and_gates_on_health():
    src = SCRIPT.read_text()
    assert "list-units 'mcp-server@*.service'" in src
    assert "/health" in src
    # one sick worker must not abort the fleet (the comment explaining this
    # legitimately mentions `set -e` — check executable lines only)
    executable = [l for l in src.splitlines() if not l.lstrip().startswith("#")]
    assert not any(l.strip().startswith("set -e") for l in executable)


def test_practice_unit_uses_rolling_script():
    src = UNIT.read_text()
    assert "rolling_restart_workers.sh" in src
    assert "restart mcp-server@8770 mcp-server@8771" not in src
