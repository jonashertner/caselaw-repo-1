#!/usr/bin/env python3
"""
scrape_cantonal_laws.py — Scrape cantonal laws from official portals.

Usage:
    python3 scrape_cantonal_laws.py                     # all implemented cantons
    python3 scrape_cantonal_laws.py --canton ZH          # single canton
    python3 scrape_cantonal_laws.py --canton ZH --max 5  # pilot
    python3 scrape_cantonal_laws.py --list               # show available cantons
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("scrape_cantonal_laws")

OUTPUT_DIR = Path("output/cantonal_laws_direct")


def scrape_canton(
    canton: str,
    output_dir: Path = OUTPUT_DIR,
    max_laws: int | None = None,
    delay: float = 0.5,
) -> dict:
    """Scrape a single canton and write JSONL."""
    from scrapers.cantonal_laws import CANTONAL_LAW_SCRAPERS

    if canton not in CANTONAL_LAW_SCRAPERS:
        return {"canton": canton, "ok": False, "error": f"No scraper for {canton}"}

    module_name, class_name = CANTONAL_LAW_SCRAPERS[canton]
    mod = importlib.import_module(module_name)
    scraper_class = getattr(mod, class_name)

    # Instantiate scraper (LexWork takes canton as param)
    scraper = scraper_class(canton=canton)
    scraper.REQUEST_DELAY = delay

    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / f"{canton}.jsonl"

    start = time.time()
    fetched = 0
    skipped = 0
    errors = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for i, stub in enumerate(scraper.enumerate_laws()):
            if max_laws and fetched >= max_laws:
                logger.info(f"{canton}: reached --max {max_laws}")
                break

            if stub.get("abrogated"):
                skipped += 1
                continue

            # LexWork: skip laws without structured content (concordats etc.)
            if "structured_document_id" in stub and not stub["structured_document_id"]:
                skipped += 1
                continue

            try:
                law = scraper.fetch_law(stub)
                if law and law.get("full_text"):
                    law["fetched_at"] = datetime.now(timezone.utc).isoformat()
                    f.write(json.dumps(law, ensure_ascii=False, default=str) + "\n")
                    fetched += 1
                    if fetched % 50 == 0:
                        elapsed = time.time() - start
                        logger.info(
                            f"{canton}: {fetched} laws fetched, "
                            f"{elapsed:.0f}s, {fetched / elapsed * 3600:.0f}/hour"
                        )
                else:
                    skipped += 1
            except Exception as e:
                errors += 1
                logger.error(f"{canton} {stub.get('sr_number', '?')}: {e}")
                if errors > 20:
                    logger.error(f"{canton}: too many errors, stopping")
                    break

    elapsed = time.time() - start
    portal_count = getattr(scraper, "portal_count", None)
    file_size = jsonl_path.stat().st_size / 1024 / 1024

    coverage = f"{fetched}"
    if portal_count:
        coverage = f"{fetched}/{portal_count}"

    logger.info(
        f"{canton}: done. {coverage} laws, {skipped} skipped, "
        f"{errors} errors, {elapsed:.0f}s, {file_size:.1f} MB"
    )

    return {
        "canton": canton,
        "ok": errors == 0,
        "fetched": fetched,
        "skipped": skipped,
        "errors": errors,
        "portal_count": portal_count,
        "duration": elapsed,
        "file_size_mb": round(file_size, 1),
    }


def main():
    from scrapers.cantonal_laws import CANTONAL_LAW_SCRAPERS

    parser = argparse.ArgumentParser(description="Scrape cantonal laws from official portals")
    parser.add_argument("--canton", type=str, help="Single canton to scrape (e.g., ZH)")
    parser.add_argument("--max", type=int, help="Max laws per canton")
    parser.add_argument("--delay", type=float, default=0.5, help="Request delay in seconds")
    parser.add_argument("--list", action="store_true", help="List available cantons")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    for noisy in ("urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if args.list:
        for canton in sorted(CANTONAL_LAW_SCRAPERS.keys()):
            mod, cls = CANTONAL_LAW_SCRAPERS[canton]
            print(f"  {canton}: {mod}.{cls}")
        return

    output_dir = Path(args.output)

    if args.canton:
        cantons = [args.canton.upper()]
        if cantons[0] not in CANTONAL_LAW_SCRAPERS:
            print(f"No scraper for {cantons[0]}. Available: {sorted(CANTONAL_LAW_SCRAPERS.keys())}")
            sys.exit(1)
    else:
        cantons = sorted(CANTONAL_LAW_SCRAPERS.keys())

    results = []
    for canton in cantons:
        result = scrape_canton(canton, output_dir, args.max, args.delay)
        results.append(result)

    # Summary
    ok = sum(1 for r in results if r["ok"])
    total_laws = sum(r.get("fetched", 0) for r in results)
    print(f"\nDone: {ok}/{len(results)} cantons, {total_laws} laws total")
    for r in results:
        status = "OK" if r["ok"] else "FAIL"
        print(f"  [{status}] {r['canton']}: {r.get('fetched', 0)} laws, {r.get('duration', 0):.0f}s")

    if any(not r["ok"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
