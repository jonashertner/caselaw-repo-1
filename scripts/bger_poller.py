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
import time
from datetime import date, datetime, timedelta, timezone
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
    """Fetch today's Neuheiten page and extract docket numbers.

    The endpoint sits behind Imperva/Incapsula on Hetzner IPs, so a
    bare requests.Session gets a 838-byte iframe stub instead of the
    real page. We harvest valid Incapsula cookies via the same
    IncapsulaCookieManager bger.py uses (browser-automated, then
    cached on disk), and detect block-pages on the response so we
    raise loudly instead of silently logging "0 decisions".
    """
    import requests
    sys.path.insert(0, str(REPO_DIR))
    from incapsula_bypass import IncapsulaCookieManager

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Incapsula cookies first; PoW cookies on top.
    incap_mgr = IncapsulaCookieManager(cache_dir=REPO_DIR / "state")
    session.cookies.update(incap_mgr.get_cookies("search.bger.ch"))
    session.cookies.update(_get_pow_cookies())

    url = NEUHEITEN_URL.format(date=date_str)
    r = session.get(url, timeout=30)
    r.raise_for_status()

    # Detect Incapsula block page. If we got one, force-refresh the
    # cookies (re-runs the headless-browser challenge) and retry once.
    if IncapsulaCookieManager.is_incapsula_blocked_response(r):
        logger.warning("Incapsula block detected — refreshing cookies and retrying")
        session.cookies.clear()
        session.cookies.update(incap_mgr.refresh_cookies("search.bger.ch"))
        session.cookies.update(_get_pow_cookies())
        r = session.get(url, timeout=30)
        r.raise_for_status()
        if IncapsulaCookieManager.is_incapsula_blocked_response(r):
            raise RuntimeError(
                "Incapsula still blocking after cookie refresh — manual "
                "intervention needed (browser automation not bypassing)"
            )

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


def _recently_ingested_dockets(window_seconds: int = 3600) -> set[str]:
    """Return docket_numbers in bger.jsonl whose ``scraped_at`` is within
    the last ``window_seconds`` seconds.

    The poller uses this to confirm that a triggered scraper actually
    saved its targets to JSONL. Dockets that BGer's document-service
    returns as error pages (the chronic failure mode 2026-05-08 onward)
    never make it into JSONL — those should NOT be marked "seen" so
    the next poll retries them. Naturally bounded: BGer rotates older
    decisions off the Neuheiten feed, so a permanent doc-service
    failure stops being retried within ~24h.

    Reads the last ``tail_bytes`` of the file (default 8 MB). A typical
    BGer decision JSONL row is 30–80 KB (full_text + metadata), so 8 MB
    holds ~100–200 rows — comfortable for any plausible poller batch
    (Neuheiten exposes ~30 dockets / day at peak). Earlier 256 KB cap
    silently truncated batches with substantial text bodies and made
    the success/fail accounting wrong (2026-05-08: 14 of 27 ingested
    rows were misreported as 'not in JSONL').
    """
    p = REPO_DIR / "output" / "decisions" / "bger.jsonl"
    if not p.exists():
        return set()
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    tail_bytes = 8 * 1024 * 1024  # 8 MB
    out: set[str] = set()
    try:
        with open(p, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            seek_back = min(tail_bytes, size)
            f.seek(size - seek_back, 0)
            chunk = f.read().decode("utf-8", errors="replace")
        # Drop first partial line if we seeked into the middle of one.
        if seek_back == tail_bytes and "\n" in chunk:
            chunk = chunk.split("\n", 1)[1]
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            sa = d.get("scraped_at")
            dn = d.get("docket_number")
            if not sa or not dn:
                continue
            try:
                sa_dt = datetime.fromisoformat(sa.replace("Z", "+00:00"))
            except Exception:
                continue
            if sa_dt >= cutoff:
                out.add(dn)
    except Exception as e:
        logger.warning("could not scan bger.jsonl tail: %s", e)
    return out


def _trigger_scraper():
    """Run the BGer scraper, then quick-publish new decisions into FTS5.

    The scraper is allowed up to 10 minutes to run. If it stalls past
    that (the typical failure mode is camoufox returning partial
    Incapsula cookies under heavy challenge state, which then causes
    bger.py's retry loop to grind without progress), we kill it and
    still run quick_publish on whatever decisions it managed to write
    to JSONL before stalling. This is essential because today's stall
    showed the previous code path was: timeout → exception → poller
    crashes → quick_publish never fires → decisions sit unpublished.
    """
    # Step 1: Scrape — capped at 10 min via Popen + process-group kill
    # (subprocess.run with timeout doesn't reliably kill grandchildren
    # like camoufox's headless browser).
    import os
    import signal as _sig
    # --neuheiten-only: skip the AZA search backfill step, which was
    # the stall site under heavy Imperva challenge today (3 sockets in
    # CLOSE_WAIT, 16 min stuck). Neuheiten alone covers the daily delta
    # the poller is responsible for; manual `run_scraper.py bger` (no
    # flag) remains the path for backfill + nightly publish.
    cmd = [
        sys.executable, str(REPO_DIR / "run_scraper.py"),
        "bger", "--neuheiten-only",
    ]
    logger.info("Triggering BGer scraper: %s", " ".join(cmd))
    SCRAPER_TIMEOUT_S = 600
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = proc.communicate(timeout=SCRAPER_TIMEOUT_S)
        rc = proc.returncode
        timed_out = False
    except subprocess.TimeoutExpired:
        logger.warning(
            "BGer scraper exceeded %ds — process-group SIGKILL",
            SCRAPER_TIMEOUT_S,
        )
        try:
            os.killpg(os.getpgid(proc.pid), _sig.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        rc = -1
        timed_out = True

    if rc == 0:
        new_count = 0
        for line in (out or "").splitlines():
            # run_scraper actually emits "[bger] Done. +13 new, 92581, ..."
            # (not "Done. New: 13"). The old regex never matched, so the
            # poller has been logging "0 new decisions" since the
            # 2026-05-07 run_scraper output-format refactor — misleading
            # because the scraper genuinely DID get the dockets.
            m = re.search(r"Done\.\s*\+(\d+)\s+new\b", line)
            if m:
                new_count = int(m.group(1))
                break
        logger.info("BGer scraper completed: %d new decisions", new_count)
    elif timed_out:
        logger.warning(
            "BGer scraper killed after timeout — proceeding to quick-publish "
            "on whatever data is in JSONL"
        )
    else:
        logger.error(
            "BGer scraper failed (exit %d): %s", rc, (err or "")[-500:],
        )
        # No `return` here — even on non-timeout failures, JSONL may
        # contain decisions worth publishing.

    # Step 2: Quick-publish into FTS5 DB (so decisions are searchable immediately)
    quick_pub = REPO_DIR / "scripts" / "quick_publish.py"
    inserted_count = 0
    if quick_pub.exists():
        cmd = [sys.executable, str(quick_pub), "--courts", "bger"]
        logger.info("Quick-publishing: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=str(REPO_DIR),
            capture_output=True,
            text=True,
            timeout=900,
        )
        if result.returncode == 0:
            skipped_due_to_lock = False
            for line in result.stdout.splitlines():
                if "Inserted" in line:
                    m = re.search(r"Inserted (\d+)/", line)
                    if m:
                        inserted_count = int(m.group(1))
                if "Inserted" in line or "new decisions" in line:
                    logger.info("Quick-publish: %s", line.split("INFO")[-1].strip() if "INFO" in line else line)
                # quick_publish exits with rc=0 + this exact string when
                # publish.py holds the lock. Surface it loudly so the
                # operator can see that pending JSONL rows are waiting
                # on the running full publish, not just "nothing new".
                if "skipping quick_publish" in line or "Full publish.py is running" in line:
                    skipped_due_to_lock = True
            if skipped_due_to_lock:
                logger.warning(
                    "Quick-publish SKIPPED: full publish.py holds the lock "
                    "— freshly-scraped BGer dockets remain in bger.jsonl "
                    "and will be inserted on the next poll once the lock "
                    "releases (after Step 2 swap + integrity check)"
                )
        else:
            logger.error("Quick-publish failed (exit %d): %s", result.returncode, result.stderr[-300:])

    # Step 3: refresh dashboard stats.json (only if we actually inserted
    # decisions, since regen takes ~22 min). Without this, the
    # opencaselaw.ch dashboard's "Neueste Einträge" section stays stale
    # until the next 03:00 UTC nightly publish — even though the
    # decisions are already searchable via /api and MCP.
    _maybe_update_stats(inserted_count)


def _maybe_update_stats(new_decisions: int) -> None:
    """Regenerate stats.json + git push when quick_publish inserted rows."""
    if new_decisions <= 0:
        logger.info("No new decisions inserted — skipping stats.json refresh")
        return

    # Single-flight lock: a 22-min generate_stats can outlive the 15-min
    # poll interval. The next poll's stats step would race with this
    # one; flock makes it skip cleanly.
    import fcntl
    lock_path = Path("/tmp/opencaselaw-stats.lock")
    lock_fd = open(lock_path, "w")
    try:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.warning(
                "Another stats.json refresh is in progress (lock held) — "
                "skipping; the running instance will catch up our %d new rows",
                new_decisions,
            )
            return

        # Step 3a: regenerate stats.json. --no-interesting-stats keeps
        # the heavy weekly block from the on-disk file (the weekly timer
        # owns it) and re-runs only the daily/recent blocks.
        gs = REPO_DIR / "generate_stats.py"
        if not gs.exists():
            logger.warning("generate_stats.py not found — cannot refresh stats.json")
            return
        gs_cmd = [sys.executable, str(gs), "--no-interesting-stats"]
        logger.info("Regenerating stats.json (~22 min): %s", " ".join(gs_cmd))
        gs_t0 = time.time()
        gs_res = subprocess.run(
            gs_cmd,
            cwd=str(REPO_DIR),
            capture_output=True,
            text=True,
            timeout=2700,  # 45 min cap (steady-state ~22 min)
        )
        if gs_res.returncode != 0:
            logger.error(
                "generate_stats failed (exit %d): %s",
                gs_res.returncode, (gs_res.stderr or "")[-500:],
            )
            return
        logger.info(
            "stats.json regenerated in %.0fs",
            time.time() - gs_t0,
        )

        # Step 3b: only push if there's actually a diff (the generator
        # rewrites generated_at every run, so this is essentially always
        # true after a content change — cheap to verify).
        diff_check = subprocess.run(
            ["git", "diff", "--quiet", "docs/stats.json"],
            cwd=str(REPO_DIR), capture_output=True, text=True, timeout=30,
        )
        if diff_check.returncode == 0:
            logger.info("No diff in docs/stats.json — nothing to push")
            return

        # Step 3c: commit + push to GitHub Pages.
        for cmd in (
            ["git", "add", "docs/stats.json"],
            [
                "git",
                "-c", "user.name=opencaselaw-bot",
                "-c", "user.email=bot@opencaselaw.ch",
                "commit", "-m",
                f"Update stats.json — BGer poller +{new_decisions} new decisions",
            ],
            ["git", "push", "origin", "main"],
        ):
            r = subprocess.run(
                cmd,
                cwd=str(REPO_DIR),
                capture_output=True,
                text=True,
                timeout=300,
            )
            if r.returncode != 0:
                logger.error(
                    "git step failed (%s): %s",
                    " ".join(cmd[:3]), (r.stderr or "")[-300:],
                )
                return
        logger.info(
            "stats.json pushed (+%d decisions) — opencaselaw.ch refreshes in 1-2 min",
            new_decisions,
        )
    finally:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_fd.close()


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
        logger.info(
            "NEW decisions detected: %d (%s)",
            len(new_dockets), ", ".join(sorted(new_dockets)[:5]),
        )
        if args.dry_run:
            logger.info("[dry-run] Would trigger BGer scraper")
            _save_state(today_iso, current_dockets)
        else:
            _trigger_scraper()
            # Confirm which dockets ACTUALLY landed in JSONL (BGer's
            # document-service intermittently returns error pages for
            # freshly-Neuheiten'd dockets — those return None from the
            # scraper and never reach JSONL). Mark only the ingested
            # subset as "seen"; the rest stay un-seen so the next poll
            # retries them. Naturally bounded by Neuheiten rotation.
            ingested_recent = _recently_ingested_dockets(window_seconds=3600)
            seen_state = prev_dockets | (current_dockets & ingested_recent)
            _save_state(today_iso, seen_state)
            unsaved = new_dockets - ingested_recent
            if unsaved:
                logger.warning(
                    "%d/%d new dockets NOT in JSONL (likely BGer "
                    "doc-service error pages); will retry next poll. "
                    "First few: %s",
                    len(unsaved), len(new_dockets),
                    ", ".join(sorted(unsaved)[:5]),
                )
    else:
        logger.info("No new decisions since last check")
        _save_state(today_iso, current_dockets)


if __name__ == "__main__":
    main()
