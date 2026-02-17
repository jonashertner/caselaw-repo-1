#!/usr/bin/env python3
"""
run_all_scrapers.py — Daily scraper orchestration
===================================================

Runs all registered scrapers with controlled parallelism, timeouts, and
health checks. Designed to be called by cron before publish.py.

Architecture:
- Scrapers are grouped into batches that run concurrently (max_parallel)
- Each scraper gets a per-scraper timeout (default 2h, configurable)
- Scrapers that complete quickly (few new decisions) free up slots
- All output is logged per-scraper to logs/{court}.log
- A summary is written to logs/daily_scrape.log

Special scrapers:
- ow_gerichte: Needs Playwright (chromium), ~6s/decision, ~3.7h full run
- ju_gerichte: Needs SOCKS proxy on localhost:1080 (SSH tunnel)
- bger, bge: Need Incapsula bypass via Playwright

Cron (run before publish.py):
    0 1 * * * cd /opt/caselaw/repo && python3 run_all_scrapers.py >> logs/daily_scrape.log 2>&1
    15 3 * * * cd /opt/caselaw/repo && python3 publish.py >> logs/publish.log 2>&1

Usage:
    python3 run_all_scrapers.py                    # run all scrapers
    python3 run_all_scrapers.py --courts bger,bge  # run specific scrapers
    python3 run_all_scrapers.py --exclude ow_gerichte  # skip specific scrapers
    python3 run_all_scrapers.py --parallel 4       # max 4 concurrent
    python3 run_all_scrapers.py --timeout 3600     # 1h timeout per scraper
    python3 run_all_scrapers.py --dry-run          # show what would run
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("daily_scrape")

REPO_DIR = Path(__file__).parent.resolve()

# Default timeout per scraper (seconds)
DEFAULT_TIMEOUT = 7200  # 2 hours

# Maximum concurrent scrapers
DEFAULT_PARALLEL = 6

# Scrapers that need extra time (Playwright-based, large volume)
SLOW_SCRAPERS = {
    "ow_gerichte": 14400,   # 4h — Playwright, ~6s/decision
    "vd_gerichte": 14400,   # 4h — 40k+ decisions, PDF heavy
    "bger": 10800,          # 3h — 90k decisions, Incapsula
    "bge": 10800,           # 3h — Incapsula
    "bvger": 10800,         # 3h — 90k decisions
    "zh_gerichte": 10800,   # 3h — 34k decisions
    "zh_sozialversicherungsgericht": 10800,  # 3h — 34k decisions
}

# Scrapers to skip by default (broken, redundant, or handled separately)
SKIP_BY_DEFAULT: set[str] = set()


def get_all_courts() -> list[str]:
    """Get all registered court codes from run_scraper.py."""
    # Import here to avoid circular imports
    sys.path.insert(0, str(REPO_DIR))
    from run_scraper import SCRAPERS
    return sorted(SCRAPERS.keys())


def run_single_scraper(court: str, timeout: int) -> dict:
    """
    Run a single scraper as a subprocess.

    Returns dict with: court, success, new_count, duration, error
    """
    start = time.time()
    log_path = REPO_DIR / "logs" / f"{court}.log"
    log_path.parent.mkdir(exist_ok=True)
    log_start = log_path.stat().st_size if log_path.exists() else 0

    cmd = [sys.executable, str(REPO_DIR / "run_scraper.py"), court]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            cwd=str(REPO_DIR),
        )
        duration = time.time() - start

        # Parse only this run's appended log region:
        # [court] Done. New: 42, Skips: 5, Errors: 3, ...
        new_count = 0
        skip_count = 0
        error_count = 0
        error_tail: deque[str] = deque(maxlen=6)
        if log_path.exists():
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                if log_start > 0:
                    f.seek(log_start)
                for line in f:
                    if "Done. New:" in line:
                        try:
                            new_count = int(line.split("New:")[1].split(",")[0].strip())
                        except (ValueError, IndexError):
                            pass
                        if "Skips:" in line:
                            try:
                                skip_count = int(line.split("Skips:")[1].split(",")[0].strip())
                            except (ValueError, IndexError):
                                pass
                        try:
                            error_count = int(line.split("Errors:")[1].split(",")[0].strip())
                        except (ValueError, IndexError):
                            pass
                    if " ERROR " in line or "Traceback" in line:
                        error_tail.append(line.strip())

        error = None
        if result.returncode != 0:
            error = " | ".join(error_tail) if error_tail else f"Exit code {result.returncode}"
        elif error_count > 0:
            error = f"{error_count} scraping errors"

        return {
            "court": court,
            "success": result.returncode == 0 and error_count == 0,
            "new_count": new_count,
            "skip_count": skip_count,
            "error_count": error_count,
            "duration": duration,
            "error": error,
        }

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        return {
            "court": court,
            "success": False,
            "new_count": 0,
            "skip_count": 0,
            "error_count": 0,
            "duration": duration,
            "error": f"Timed out after {timeout}s",
        }
    except Exception as e:
        duration = time.time() - start
        return {
            "court": court,
            "success": False,
            "new_count": 0,
            "skip_count": 0,
            "error_count": 0,
            "duration": duration,
            "error": str(e)[:200],
        }


def main():
    parser = argparse.ArgumentParser(description="Run all scrapers daily")
    parser.add_argument(
        "--courts", type=str, default="",
        help="Comma-separated court codes to run (default: all)",
    )
    parser.add_argument(
        "--exclude", type=str, default="",
        help="Comma-separated court codes to skip",
    )
    parser.add_argument(
        "--parallel", type=int, default=DEFAULT_PARALLEL,
        help=f"Max concurrent scrapers (default: {DEFAULT_PARALLEL})",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help=f"Default timeout per scraper in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.parallel < 1:
        parser.error("--parallel must be at least 1")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Determine which courts to run
    all_courts = get_all_courts()

    if args.courts:
        courts = [c.strip() for c in args.courts.split(",") if c.strip()]
        unknown = set(courts) - set(all_courts)
        if unknown:
            logger.error(f"Unknown courts: {unknown}. Available: {all_courts}")
            sys.exit(1)
    else:
        courts = [c for c in all_courts if c not in SKIP_BY_DEFAULT]

    if args.exclude:
        exclude = {c.strip() for c in args.exclude.split(",")}
        courts = [c for c in courts if c not in exclude]

    # Ensure logs directory exists
    (REPO_DIR / "logs").mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    logger.info(f"=== Daily scrape — {now.isoformat()} ===")
    logger.info(f"Courts: {len(courts)}, Parallel: {args.parallel}, Timeout: {args.timeout}s")

    if args.dry_run:
        for court in courts:
            t = SLOW_SCRAPERS.get(court, args.timeout)
            logger.info(f"  [dry-run] Would run: {court} (timeout: {t}s)")
        return

    # Run scrapers with controlled parallelism
    results = []
    total_start = time.time()

    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {}
        for court in courts:
            timeout = SLOW_SCRAPERS.get(court, args.timeout)
            future = executor.submit(run_single_scraper, court, timeout)
            futures[future] = court

        for future in as_completed(futures):
            court = futures[future]
            try:
                result = future.result()
                results.append(result)
                status = "OK" if result["success"] else "FAILED"
                logger.info(
                    f"  [{status}] {result['court']}: "
                    f"+{result['new_count']} new, "
                    f"{result['duration']:.0f}s"
                    f"{' — ' + result['error'] if result['error'] else ''}"
                )
            except Exception as e:
                logger.error(f"  [ERROR] {court}: {e}")
                results.append({
                    "court": court,
                    "success": False,
                    "new_count": 0,
                    "skip_count": 0,
                    "error_count": 0,
                    "duration": 0,
                    "error": str(e)[:200],
                })

    # Summary
    total_elapsed = time.time() - total_start
    succeeded = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])
    total_new = sum(r["new_count"] for r in results)

    logger.info("\n=== Summary ===")
    logger.info(f"  Succeeded: {succeeded}/{len(results)}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Total new decisions: {total_new}")
    logger.info(f"  Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")

    if failed:
        logger.info("\n  Failed scrapers:")
        for r in results:
            if not r["success"]:
                logger.info(f"    - {r['court']}: {r['error']}")

    # Persist health data for dashboard
    health = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "run_duration_s": round(total_elapsed, 1),
        "scrapers": {
            r["court"]: {
                "success": r["success"],
                "new_count": r["new_count"],
                "skip_count": r["skip_count"],
                "error_count": r["error_count"],
                "duration_s": round(r["duration"], 1),
                "error": r["error"],
            }
            for r in results
        },
    }
    health_path = REPO_DIR / "logs" / "scraper_health.json"
    health_path.write_text(json.dumps(health, indent=2))
    logger.info(f"Health data written to {health_path}")

    # Completeness mode: fail when any scraper fails.
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
