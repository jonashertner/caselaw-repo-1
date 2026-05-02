#!/usr/bin/env python3
"""Disk-space monitor for the OpenCaseLaw data volume.

Runs every 30 min via systemd timer. Emits ntfy alerts at two thresholds
so the operator hears about the disk filling up *before* the next nightly
publish blocks at pre-flight (which is what burned the 2026-05-02
nightly: stale .quick + .tmp files filled /mnt to 75% used overnight).

Thresholds (percent of volume used):
  - WARNING (>= 80%): default-priority ntfy, daily reminder until cleared
  - URGENT (>= 95%): urgent-priority ntfy, every run until cleared

The script also dumps the top-10 largest files in /mnt/.../output so the
operator immediately sees the candidate cleanup targets.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_VOLUME = Path("/mnt/HC_Volume_104655575")
DEFAULT_OUTPUT_DIR = DEFAULT_VOLUME / "output"
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
    rows = sorted(
        ((p, p.stat().st_size) for p in directory.iterdir() if p.is_file()),
        key=lambda x: -x[1],
    )[:n]
    return "\n".join(f"  {sz / 1e9:>6.1f} GB  {p.name}" for p, sz in rows)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--volume", type=Path, default=DEFAULT_VOLUME)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--warn-pct", type=int, default=WARN_PCT)
    p.add_argument("--urgent-pct", type=int, default=URGENT_PCT)
    p.add_argument("--quiet", action="store_true",
                   help="Don't emit ntfy on under-threshold runs")
    args = p.parse_args()

    if not args.volume.exists():
        print(f"Volume not present: {args.volume}", file=sys.stderr)
        return 1

    usage = shutil.disk_usage(args.volume)
    used_pct = (usage.used / usage.total) * 100
    free_gb = usage.free / 1e9
    used_gb = usage.used / 1e9
    total_gb = usage.total / 1e9

    line = (
        f"{args.volume}: {used_pct:.1f}% used "
        f"({used_gb:.0f} / {total_gb:.0f} GB; {free_gb:.0f} GB free)"
    )
    print(line)

    # Exit 0 on every successful check (regardless of threshold) so systemd
    # does not also fire OnFailure= on top of our own ntfy alert. The
    # ntfy notification is the signal; systemd's job is just to re-run
    # us every 30 min.
    if used_pct >= args.urgent_pct:
        _ntfy(
            f"Disk URGENT: {used_pct:.0f}% used",
            f"{line}\n\nTop files:\n{_top_files(args.output_dir)}\n\n"
            f"Next nightly publish will fail at pre-flight if free < 80 GB.",
            priority="urgent",
        )
    elif used_pct >= args.warn_pct:
        _ntfy(
            f"Disk WARNING: {used_pct:.0f}% used",
            f"{line}\n\nTop files:\n{_top_files(args.output_dir)}",
            priority="default",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
