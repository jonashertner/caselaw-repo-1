#!/usr/bin/env python3
"""
Run a scraper and persist full decisions to JSONL incrementally.

Unlike pipeline.py which writes Parquet at the end, this writes each
decision as a JSON line immediately after scraping — crash-safe.

Usage:
    python3 run_scraper.py ag_gerichte
    python3 run_scraper.py ag_gerichte --max 10
    python3 run_scraper.py ag_gerichte --since 2024-01-01
    python3 run_scraper.py bger --since 2000-01-01

The output JSONL at output/decisions/{court}.jsonl contains full Decision
objects (including full_text). This is the primary data store.

The state file at state/{court}.jsonl contains only decision IDs (for
skip-on-restart). Both files are append-only.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("run_scraper")

# Scraper registry — add new scrapers here
SCRAPERS = {
    # Federal
    "bger": ("scrapers.bger", "BgerScraper"),
    "bge": ("scrapers.bge", "BGELeitentscheideScraper"),
    "bvger": ("scrapers.bvger", "BVGerScraper"),
    "bstger": ("scrapers.bstger", "BStGerScraper"),
    "bpatger": ("scrapers.bpatger", "BPatGerScraper"),
    # Federal — regulatory
    "finma": ("scrapers.finma", "FINMAScraper"),
    "weko": ("scrapers.weko", "WEKOScraper"),
    "edoeb": ("scrapers.edoeb", "EDOEBScraper"),
    # Cantonal — implemented
    "ag_gerichte": ("scrapers.cantonal.ag_gerichte", "AGGerichteScraper"),
    "ai_gerichte": ("scrapers.cantonal.ai_gerichte", "AIGerichteScraper"),
    "bs_gerichte": ("scrapers.cantonal.bs_gerichte", "BSGerichteScraper"),
    "zh_gerichte": ("scrapers.cantonal.zh_gerichte", "ZHGerichteScraper"),
    "zh_obergericht": ("scrapers.cantonal.zh_obergericht", "ZHObergerichtScraper"),
    "zh_verwaltungsgericht": ("scrapers.cantonal.zh_verwaltungsgericht", "ZHVerwaltungsgerichtScraper"),
    "zh_sozialversicherungsgericht": ("scrapers.cantonal.zh_sozialversicherungsgericht", "ZHSozialversicherungsgerichtScraper"),
    "zh_baurekursgericht": ("scrapers.cantonal.zh_baurekursgericht", "ZHBaurekursgerichtScraper"),
    "zh_steuerrekursgericht": ("scrapers.cantonal.zh_steuerrekursgericht", "ZHSteuerrekursgerichtScraper"),
    # Cantonal — Tribuna GWT-RPC
    "be_anwaltsaufsicht": ("scrapers.cantonal.be_anwaltsaufsicht", "BEAnwaltsaufsichtScraper"),
    "be_steuerrekurs": ("scrapers.cantonal.be_steuerrekurs", "BESteuerrekursScraper"),
    "be_verwaltungsgericht": ("scrapers.cantonal.be_verwaltungsgericht", "BEVerwaltungsgerichtScraper"),
    "be_zivilstraf": ("scrapers.cantonal.be_zivilstraf", "BEZivilStrafScraper"),
    "fr_gerichte": ("scrapers.cantonal.fr_gerichte", "FRGerichteScraper"),
    "gr_gerichte": ("scrapers.cantonal.gr_gerichte", "GRGerichteScraper"),
    "zg_verwaltungsgericht": ("scrapers.cantonal.zg_gerichte", "ZGVerwaltungsgerichtScraper"),
    "sz_gerichte": ("scrapers.cantonal.sz_gerichte", "SZGerichteScraper"),
    "sz_verwaltungsgericht": ("scrapers.cantonal.sz_verwaltungsgericht", "SZVerwaltungsgerichtScraper"),
    # Cantonal — Weblaw LEv4
    "ar_gerichte": ("scrapers.cantonal.ar_gerichte", "ARGerichteScraper"),
    # Cantonal — FindInfo / Omnis
    "ti_gerichte": ("scrapers.cantonal.ti_gerichte", "TIGerichteScraper"),
    "gl_gerichte": ("scrapers.cantonal.gl_gerichte", "GLGerichteScraper"),
    "ne_gerichte": ("scrapers.cantonal.ne_gerichte", "NEGerichteScraper"),
    # Cantonal — Custom platforms
    "vd_gerichte": ("scrapers.cantonal.vd_gerichte", "VDGerichteScraper"),
    "so_gerichte": ("scrapers.cantonal.so_gerichte", "SOGerichteScraper"),
    "lu_gerichte": ("scrapers.cantonal.lu_gerichte", "LUGerichteScraper"),
    "ge_gerichte": ("scrapers.cantonal.ge_gerichte", "GEGerichteScraper"),
    "vs_gerichte": ("scrapers.cantonal.vs_gerichte", "VSGerichteScraper"),
    "tg_gerichte": ("scrapers.cantonal.tg_gerichte", "TGGerichteScraper"),
    "sh_gerichte": ("scrapers.cantonal.sh_gerichte", "SHGerichteScraper"),
    "ur_gerichte": ("scrapers.cantonal.ur_gerichte", "URGerichteScraper"),
    # Cantonal — ICMS
    "nw_gerichte": ("scrapers.cantonal.nw_gerichte", "NWGerichteScraper"),
    # Cantonal — TYPO3/DIAM
    "sg_publikationen": ("scrapers.cantonal.sg_publikationen", "SGPublikationenScraper"),
    # Cantonal — Swisslex
    "bl_gerichte": ("scrapers.cantonal.bl_gerichte", "BLGerichteScraper"),
    # Cantonal — Tribuna (JU)
    "ju_gerichte": ("scrapers.cantonal.ju_gerichte", "JUGerichteScraper"),
    # Cantonal — Weblaw Vaadin
    "ow_gerichte": ("scrapers.cantonal.ow_gerichte", "OWGerichteScraper"),
}


def serialize_decision(d) -> str:
    """Serialize a Decision to a JSON string (one line)."""
    data = d.model_dump()
    # Convert date/datetime to ISO strings
    for key, val in data.items():
        if isinstance(val, (date, datetime)):
            data[key] = val.isoformat()
    return json.dumps(data, ensure_ascii=False, default=str)


def run_with_persistence(
    scraper_key: str,
    since_date: str | None = None,
    max_decisions: int | None = None,
    output_dir: Path = Path("output"),
    state_dir: Path = Path("state"),
) -> int:
    """Run scraper and write each decision to JSONL incrementally.

    Returns the number of errors encountered.
    """

    if scraper_key not in SCRAPERS:
        logger.error(f"Unknown scraper: {scraper_key}. Available: {list(SCRAPERS.keys())}")
        return -1

    module_name, class_name = SCRAPERS[scraper_key]

    # Import scraper class
    import importlib
    mod = importlib.import_module(module_name)
    scraper_class = getattr(mod, class_name)

    # Prepare output
    decisions_dir = output_dir / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = decisions_dir / f"{scraper_key}.jsonl"

    # Load already-written decision IDs from output JSONL (for dedup on restart)
    written_ids = set()
    if jsonl_path.exists():
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        written_ids.add(obj.get("decision_id", ""))
                    except json.JSONDecodeError:
                        pass
        logger.info(f"Loaded {len(written_ids)} already-written decisions from {jsonl_path}")

    # Initialize scraper
    scraper = scraper_class(state_dir=state_dir)

    # Parse since_date
    since = None
    if since_date:
        since = date.fromisoformat(since_date)

    # Run discovery and fetch
    new_count = 0
    skips = 0
    errors = 0
    start = time.time()

    logger.info(
        f"[{scraper_key}] Starting. Known: {scraper.state.count()}, "
        f"Written: {len(written_ids)}"
    )

    for i, stub in enumerate(scraper.discover_new(since)):
        if max_decisions and new_count >= max_decisions:
            logger.info(f"[{scraper_key}] Reached max_decisions={max_decisions}")
            break

        try:
            decision = scraper.fetch_decision(stub)
            if decision:
                # Write full decision to JSONL (skip if already written)
                if decision.decision_id not in written_ids:
                    with open(jsonl_path, "a", encoding="utf-8") as f:
                        f.write(serialize_decision(decision) + "\n")
                        f.flush()
                    written_ids.add(decision.decision_id)
                    new_count += 1

                    if new_count % 100 == 0:
                        elapsed = time.time() - start
                        rate = new_count / elapsed * 3600
                        logger.info(
                            f"[{scraper_key}] Progress: {new_count} decisions, "
                            f"{rate:.0f}/hour, file: {jsonl_path.stat().st_size / 1024 / 1024:.1f} MB"
                        )

                # Mark scraped AFTER durable write to avoid gaps on crash
                scraper.state.mark_scraped(decision.decision_id)

                logger.info(
                    f"[{scraper_key}] Scraped: {decision.decision_id} "
                    f"({decision.decision_date})"
                )
            else:
                skips += 1
                logger.warning(
                    f"[{scraper_key}] Skipped (fetch returned None): "
                    f"{stub.get('docket_number', '?')}"
                )

        except Exception as e:
            errors += 1
            logger.error(
                f"[{scraper_key}] Error scraping {stub.get('docket_number', '?')}: {e}",
                exc_info=True,
            )
            if errors > getattr(scraper, "MAX_ERRORS", 50):
                logger.error(f"[{scraper_key}] Too many errors ({errors}), stopping.")
                break

    elapsed = time.time() - start
    file_size = jsonl_path.stat().st_size / 1024 / 1024 if jsonl_path.exists() else 0

    # Touch JSONL file on successful runs so dashboard doesn't mark as stale
    if jsonl_path.exists():
        jsonl_path.touch()

    logger.info(
        f"[{scraper_key}] Done. New: {new_count}, Skips: {skips}, Errors: {errors}, "
        f"Total written: {len(written_ids)}, Time: {elapsed / 60:.1f} min, "
        f"File: {jsonl_path} ({file_size:.1f} MB)"
    )

    # Return non-zero only if the run was aborted due to too many errors.
    # A few scattered errors (transient network issues, corrupt PDFs) are
    # normal and should not mark the entire run as failed.
    max_err = getattr(scraper, "MAX_ERRORS", 50)
    if errors > max_err:
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description="Run scraper with JSONL persistence")
    parser.add_argument("scraper", nargs="?", choices=list(SCRAPERS.keys()), help="Scraper to run")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scraper codes and exit",
    )
    parser.add_argument("--since", type=str, help="Only scrape since date (YYYY-MM-DD)")
    parser.add_argument("--max", type=int, help="Max decisions to scrape")
    parser.add_argument("--output", type=str, default="output", help="Output directory")
    parser.add_argument("--state", type=str, default="state", help="State directory")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    if args.list:
        for scraper_key in sorted(SCRAPERS.keys()):
            print(scraper_key)
        return

    if not args.scraper:
        parser.error("the following argument is required: scraper (unless --list is used)")

    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"logs/{args.scraper}.log"),
        ],
    )

    # Suppress noisy third-party loggers (pdfminer floods debug with every token)
    for noisy in ("pdfminer", "pdfplumber", "urllib3", "chardet", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    exit_code = run_with_persistence(
        scraper_key=args.scraper,
        since_date=args.since,
        max_decisions=args.max,
        output_dir=Path(args.output),
        state_dir=Path(args.state),
    )

    if exit_code:
        sys.exit(1)


if __name__ == "__main__":
    main()
