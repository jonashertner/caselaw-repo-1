#!/usr/bin/env python3
"""
BGer Neuheiten Poller
======================
Lightweight poller that checks the BGer Eurospider "Neuheiten" page
for new decisions. If new decisions are detected, triggers the BGer scraper.

Designed to run every 15 min via systemd timer during business hours.
Single HTTP request per run (~1s), minimal load on BGer servers.

Architecture:
  1. Fetch index_aza.php?date=YYYYMMDD&mode=news (today's date)
  2. Extract docket numbers from page
  3. Compare against last known count (stored in state file)
  4. If new decisions found → run bger scraper
  5. Update state file

State: /tmp/bger_poller_state.json
  {"date": "2026-04-07", "count": 22, "dockets": ["5A_24/2024", ...]}

Usage:
  python3 scripts/bger_poller.py          # check and trigger if needed
  python3 scripts/bger_poller.py --dry-run # check only, don't trigger
  python3 scripts/bger_poller.py --force   # trigger scraper regardless
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[1]
STATE_FILE = Path("/tmp/bger_poller_state.json")
LOG_FILE = REPO_DIR / "logs" / "bger_poller.log"

NEUHEITEN_URL = (
    # search.bger.ch — confirmed working 2026-05-06.
    # www.bger.ch silently returned an empty page on this date which made the
    # poller log "0 decisions" all morning while 29 fresh dockets were live
    # at search.bger.ch (the host the rest of the scraper already uses).
    "https://search.bger.ch/ext/eurospider/live/de/php/aza/http/index_aza.php"
    "?date={date}&lang=de&mode=news"
)

DOCKET_RE = re.compile(r"\b\d[A-Z]_\d+/\d{4}\b")

logger = logging.getLogger("bger_poller")


def _get_pow_cookies() -> dict:
    """Generate PoW cookies for BGer Eurospider."""
    sys.path.insert(0, str(REPO_DIR))
    from base_scraper import make_pow_cookies
    return make_pow_cookies(16)


def _fetch_neuheiten(date_str: str) -> set[str]:
    """Fetch today's Neuheiten page and extract docket numbers."""
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    session.cookies.update(_get_pow_cookies())

    url = NEUHEITEN_URL.format(date=date_str)
    r = session.get(url, timeout=15)
    r.raise_for_status()

    dockets = set(DOCKET_RE.findall(r.text))
    return dockets


def _load_state() -> dict:
    """Load last known state."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"date": "", "count": 0, "dockets": []}


def _save_state(today: str, dockets: set[str]):
    """Save current state."""
    STATE_FILE.write_text(json.dumps({
        "date": today,
        "count": len(dockets),
        "dockets": sorted(dockets),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }))


def _trigger_scraper():
    """Run the BGer scraper, then quick-publish new decisions into FTS5."""
    # Step 1: Scrape
    cmd = [sys.executable, str(REPO_DIR / "run_scraper.py"), "bger"]
    logger.info("Triggering BGer scraper: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if result.returncode == 0:
        new_count = 0
        for line in result.stdout.splitlines():
            if "Done. New:" in line:
                m = re.search(r"New: (\d+)", line)
                if m:
                    new_count = int(m.group(1))
        logger.info("BGer scraper completed: %d new decisions", new_count)
    else:
        logger.error("BGer scraper failed (exit %d): %s", result.returncode, result.stderr[-500:])
        return

    # Step 2: Quick-publish into FTS5 DB (so decisions are searchable immediately)
    quick_pub = REPO_DIR / "scripts" / "quick_publish.py"
    if quick_pub.exists():
        cmd = [sys.executable, str(quick_pub), "--courts", "bger"]
        logger.info("Quick-publishing: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=str(REPO_DIR),
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if "Inserted" in line or "new decisions" in line:
                    logger.info("Quick-publish: %s", line.split("INFO")[-1].strip() if "INFO" in line else line)
        else:
            logger.error("Quick-publish failed (exit %d): %s", result.returncode, result.stderr[-300:])


def main():
    parser = argparse.ArgumentParser(description="BGer Neuheiten poller")
    parser.add_argument("--dry-run", action="store_true", help="Check only, don't trigger scraper")
    parser.add_argument("--force", action="store_true", help="Trigger scraper regardless")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        # Only StreamHandler — systemd captures stdout to the log file.
        # Adding FileHandler would cause duplicate lines.
    )

    today = date.today().strftime("%Y%m%d")
    today_iso = date.today().isoformat()

    if args.force:
        logger.info("Force mode — triggering scraper")
        _trigger_scraper()
        return

    # Fetch current Neuheiten
    try:
        current_dockets = _fetch_neuheiten(today)
    except Exception as e:
        logger.error("Failed to fetch Neuheiten: %s", e)
        return

    logger.info("Neuheiten %s: %d decisions", today_iso, len(current_dockets))

    # Compare against last state
    state = _load_state()
    prev_dockets = set(state.get("dockets", []))

    if state.get("date") != today_iso:
        # New day — start fresh (don't carry over yesterday's dockets)
        prev_dockets = set()

    new_dockets = current_dockets - prev_dockets

    if new_dockets:
        logger.info("NEW decisions detected: %d (%s)", len(new_dockets), ", ".join(sorted(new_dockets)[:5]))
        _save_state(today_iso, current_dockets)
        if not args.dry_run:
            _trigger_scraper()
        else:
            logger.info("[dry-run] Would trigger BGer scraper")
    else:
        logger.info("No new decisions since last check")
        _save_state(today_iso, current_dockets)


if __name__ == "__main__":
    main()
