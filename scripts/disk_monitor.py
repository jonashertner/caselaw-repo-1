#!/usr/bin/env python3
"""Disk-space monitor for the OpenCaseLaw VPS — every mount that matters.

Runs every 30 min via systemd timer. Emits ntfy alerts at two thresholds
so the operator hears about a disk filling up *before* the next nightly
publish blocks at pre-flight.

Watched mounts (default — both must clear or both alert):
  /                           — root filesystem (system + /opt/caselaw repo)
  /mnt/HC_Volume_104655575    — data volume (FTS5, sidecars, parquet)

Both have caused publish-cascade incidents:
  2026-05-02 13:49 UTC — / filled to 100% mid-build because
                         decision_structure.db (44 GB) was not symlinked
                         to /mnt and its .tmp pushed root over the line.
  2026-05-02 03:30 UTC — /mnt was at 75% with a stale .quick file from a
                         crashed BGer-poller run; build pre-flight blocked
                         until cleanup ran.

Thresholds (percent of mount used):
  - WARNING (>= 80%): default-priority ntfy
  - URGENT  (>= 95%): urgent-priority ntfy
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Default mounts the OpenCaseLaw VPS depends on.
DEFAULT_MOUNTS = [
    Path("/"),
    Path("/mnt/HC_Volume_104655575"),
]
# Where to look for "top biggest files" when alerting. Maps mount → dir.
# Falls back to the mount itself if no specific dir is registered.
TOP_DIR_FOR_MOUNT = {
    Path("/"):
        Path("/opt/caselaw/repo/output"),
    Path("/mnt/HC_Volume_104655575"):
        Path("/mnt/HC_Volume_104655575/output"),
}
WARN_PCT = 80
URGENT_PCT = 95
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "opencaselaw-prod")
NTFY_URL = os.environ.get("NTFY_URL", "https://ntfy.sh")


def _ntfy(title: str, body: str, priority: str = "default") -> None:
    """Post a notification. Best-effort — never raises."""
    try:
        subprocess.run(
            [
                "curl", "-fsS",
                "-H", f"Title: {title}",
                "-H", f"Priority: {priority}",
                "-d", body,
                f"{NTFY_URL}/{NTFY_TOPIC}",
            ],
            timeout=10, check=False, capture_output=True,
        )
    except Exception as e:
        print(f"ntfy failed: {e}", file=sys.stderr)


def _top_files(directory: Path, n: int = 10) -> str:
    if not directory.exists():
        return "(directory missing)"
    rows = []
    try:
        for p in directory.iterdir():
            try:
                if p.is_file() and not p.is_symlink():
                    rows.append((p, p.stat().st_size))
            except (OSError, FileNotFoundError):
                continue
    except (OSError, PermissionError):
        return "(directory unreadable)"
    rows.sort(key=lambda x: -x[1])
    rows = rows[:n]
    return "\n".join(f"  {sz / 1e9:>6.1f} GB  {p.name}" for p, sz in rows)


def _check_mount(mount: Path, top_dir: Path,
                 warn_pct: int, urgent_pct: int) -> int:
    """Check a single mount; emit ntfy if over threshold. Returns the
    used-percent (rounded down) so the caller can report a per-mount
    summary. Always returns successfully (never raises)."""
    if not mount.exists():
        print(f"Mount not present: {mount}", file=sys.stderr)
        return 0
    usage = shutil.disk_usage(mount)
    used_pct = (usage.used / usage.total) * 100
    free_gb = usage.free / 1e9
    used_gb = usage.used / 1e9
    total_gb = usage.total / 1e9

    line = (
        f"{mount}: {used_pct:.1f}% used "
        f"({used_gb:.0f} / {total_gb:.0f} GB; {free_gb:.0f} GB free)"
    )
    print(line)

    if used_pct >= urgent_pct:
        _ntfy(
            f"Disk URGENT [{mount}]: {used_pct:.0f}% used",
            f"{line}\n\nTop files in {top_dir}:\n{_top_files(top_dir)}\n\n"
            f"Next nightly publish will block at pre-flight if free < 80 GB.",
            priority="urgent",
        )
    elif used_pct >= warn_pct:
        _ntfy(
            f"Disk WARNING [{mount}]: {used_pct:.0f}% used",
            f"{line}\n\nTop files in {top_dir}:\n{_top_files(top_dir)}",
            priority="default",
        )
    return int(used_pct)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mount", type=Path, action="append",
                   help="Mount to monitor. May be passed multiple times. "
                        "Defaults to / and the data volume if omitted.")
    p.add_argument("--warn-pct", type=int, default=WARN_PCT)
    p.add_argument("--urgent-pct", type=int, default=URGENT_PCT)
    args = p.parse_args()

    mounts = args.mount if args.mount else DEFAULT_MOUNTS
    for m in mounts:
        top_dir = TOP_DIR_FOR_MOUNT.get(m, m)
        _check_mount(m, top_dir, args.warn_pct, args.urgent_pct)

    # Always exit 0 — ntfy is the signal, systemd just re-fires us.
    return 0


if __name__ == "__main__":
    sys.exit(main())
