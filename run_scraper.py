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
import re
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("run_scraper")

# Scraper registry — add new scrapers here
SCRAPERS = {
    # Federal
    "bger": ("scrapers.bger", "BgerScraper"),
    "bge": ("scrapers.bge", "BGELeitentscheideScraper"),
    "bge_egmr": ("scrapers.bge_egmr", "BGEEGMRScraper"),
    "bvger": ("scrapers.bvger", "BVGerScraper"),
    "bstger": ("scrapers.bstger", "BStGerScraper"),
    "bpatger": ("scrapers.bpatger", "BPatGerScraper"),
    # Federal — administrative
    "ch_bundesrat": ("scrapers.ch_bundesrat", "CHBundesratScraper"),
    # Federal — regulatory
    "finma": ("scrapers.finma", "FINMAScraper"),
    "weko": ("scrapers.weko", "WEKOScraper"),
    "edoeb": ("scrapers.edoeb", "EDOEBScraper"),
    "ubi": ("scrapers.ubi", "UBIScraper"),
    "elcom": ("scrapers.elcom", "ElComScraper"),
    "postcom": ("scrapers.postcom", "PostComScraper"),
    "comcom": ("scrapers.comcom", "ComComScraper"),
    # Cantonal — implemented
    "ag_gerichte": ("scrapers.cantonal.ag_gerichte", "AGGerichteScraper"),
    "ai_gerichte": ("scrapers.cantonal.ai_gerichte", "AIGerichteScraper"),
    "bs_gerichte": ("scrapers.cantonal.bs_gerichte", "BSGerichteScraper"),
    "zh_gerichte": ("scrapers.cantonal.zh_gerichte", "ZHGerichteScraper"),
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
    "zg_obergericht": ("scrapers.cantonal.zg_obergericht", "ZGObergerichtScraper"),
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
    # Federal — Sports Tribunal
    "ta_sst": ("scrapers.ta_sst", "TaSSTScraper"),
    # Federal — historical / international
    "emark": ("scrapers.emark", "EMARKScraper"),
    "bge_historical": ("scrapers.bge_historical", "BGEHistoricalScraper"),
    "hudoc_ch": ("scrapers.hudoc", "HUDOCScraper"),
}


def serialize_decision(d) -> str:
    """Serialize a Decision to a JSON string (one line)."""
    data = d.model_dump()
    # Convert date/datetime to ISO strings
    for key, val in data.items():
        if isinstance(val, (date, datetime)):
            data[key] = val.isoformat()
    return json.dumps(data, ensure_ascii=False, default=str)


_YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b")


def _infer_decision_year(
    *,
    decision_id: str,
    docket_number: str | None,
    decision_date: date | datetime | str | None,
) -> int | None:
    """Infer decision year from explicit date first, then docket/ID text."""
    if isinstance(decision_date, datetime):
        year = int(decision_date.year)
        return year if 1800 <= year <= 2100 else None
    if isinstance(decision_date, date):
        year = int(decision_date.year)
        return year if 1800 <= year <= 2100 else None
    if isinstance(decision_date, str):
        decision_date = decision_date.strip()
        if len(decision_date) >= 4 and decision_date[:4].isdigit():
            year = int(decision_date[:4])
            if 1800 <= year <= 2100:
                return year

    for text in (docket_number or "", decision_id or ""):
        for match in _YEAR_RE.finditer(text):
            year = int(match.group(0))
            if 1800 <= year <= 2100:
                return year
    return None


def _load_written_ids_and_years(jsonl_path: Path) -> tuple[set[str], dict[int, set[str]]]:
    """Load already-written decision IDs and year buckets from a JSONL file."""
    written_ids: set[str] = set()
    ids_by_year: dict[int, set[str]] = defaultdict(set)
    if not jsonl_path.exists():
        return written_ids, ids_by_year

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            decision_id = str(obj.get("decision_id", "")).strip()
            if not decision_id:
                continue
            written_ids.add(decision_id)

            year = _infer_decision_year(
                decision_id=decision_id,
                docket_number=str(obj.get("docket_number", "") or ""),
                decision_date=obj.get("decision_date"),
            )
            if year is not None:
                ids_by_year[year].add(decision_id)

    return written_ids, ids_by_year


def _record_coverage_snapshots(
    *,
    scraper_key: str,
    output_dir: Path,
    ids_by_year: dict[int, set[str]],
    changed_years: set[int],
) -> None:
    """Persist per-year source snapshots for this scraper into coverage tables."""
    if not ids_by_year:
        return

    from coverage_report import ensure_coverage_tables, record_snapshot

    db_path = output_dir / "decisions.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        ensure_coverage_tables(conn)
        existing_rows = int(
            conn.execute(
                "SELECT COUNT(*) FROM source_snapshots WHERE source_key = ?",
                (scraper_key,),
            ).fetchone()[0]
        )
        years_to_write = set(changed_years)
        if existing_rows == 0:
            # Initial backfill for this source from existing JSONL.
            years_to_write = set(ids_by_year.keys())
        if not years_to_write:
            return

        snapshot_date = date.today().isoformat()
        for year in sorted(years_to_write):
            ids = sorted(ids_by_year.get(year, set()))
            if not ids:
                continue
            record_snapshot(
                conn,
                source_key=scraper_key,
                snapshot_year=year,
                snapshot_date=snapshot_date,
                decision_ids=ids,
                notes="auto: run_scraper",
            )
        logger.info(
            f"[{scraper_key}] Coverage snapshots updated for years: {sorted(years_to_write)}"
        )
    finally:
        conn.close()


class _RunEventWriter:
    """Persist discovery/fetch events and maintain gap queue for one run."""

    def __init__(self, *, output_dir: Path, source_key: str, run_id: str):
        self.output_dir = output_dir
        self.source_key = source_key
        self.run_id = run_id
        self._conn: sqlite3.Connection | None = None
        self._attempt_counts: dict[str, int] = defaultdict(int)
        self._pending = 0
        self._enabled = False
        self._init_db()

    def _init_db(self) -> None:
        try:
            from coverage_report import ensure_coverage_tables

            db_path = self.output_dir / "decisions.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path))
            ensure_coverage_tables(conn)
            self._conn = conn
            self._enabled = True
        except Exception as e:
            logger.warning(f"[{self.source_key}] Event tracking disabled: {e}")
            self._enabled = False

    def _maybe_commit(self) -> None:
        if not self._conn:
            return
        self._pending += 1
        if self._pending >= 200:
            self._conn.commit()
            self._pending = 0

    def close(self) -> None:
        if not self._conn:
            return
        try:
            if self._pending:
                self._conn.commit()
        finally:
            self._conn.close()
            self._conn = None
            self._enabled = False

    def log_discovery(self, stub: dict) -> None:
        if not self._enabled or not self._conn:
            return
        try:
            decision_id = str(stub.get("decision_id", "")).strip() or None
            docket_number = str(stub.get("docket_number", "")).strip() or None
            decision_year = _infer_decision_year(
                decision_id=decision_id or "",
                docket_number=docket_number,
                decision_date=stub.get("decision_date"),
            )
            stub_json = json.dumps(stub, ensure_ascii=False, default=str)
            self._conn.execute(
                """
                INSERT INTO source_discoveries (
                    run_id, source_key, decision_id, docket_number, decision_year,
                    status, stub_json, discovered_at
                ) VALUES (?, ?, ?, ?, ?, 'discovered', ?, datetime('now'))
                """,
                (
                    self.run_id,
                    self.source_key,
                    decision_id,
                    docket_number,
                    decision_year,
                    stub_json[:20000],
                ),
            )
            self._maybe_commit()
        except Exception as e:
            logger.debug(f"[{self.source_key}] discovery event logging failed: {e}")

    def log_fetch_attempt(
        self,
        *,
        stub: dict,
        status: str,
        decision_id: str | None = None,
        docket_number: str | None = None,
        decision_date: date | datetime | str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if not self._enabled or not self._conn:
            return

        did = decision_id or str(stub.get("decision_id", "")).strip() or None
        docket = docket_number or str(stub.get("docket_number", "")).strip() or None
        year = _infer_decision_year(
            decision_id=did or "",
            docket_number=docket,
            decision_date=decision_date if decision_date is not None else stub.get("decision_date"),
        )
        attempt_key = did or docket or "<unknown>"
        self._attempt_counts[attempt_key] += 1
        attempt_no = self._attempt_counts[attempt_key]

        try:
            self._conn.execute(
                """
                INSERT INTO source_fetch_attempts (
                    run_id, source_key, decision_id, docket_number, decision_year,
                    attempt_no, status, error_type, error_message, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    self.run_id,
                    self.source_key,
                    did,
                    docket,
                    year,
                    attempt_no,
                    status,
                    error_type,
                    (error_message or "")[:1000] or None,
                ),
            )

            if did and year is not None:
                from coverage_report import mark_gap_failure, mark_gap_resolved

                if status == "success":
                    mark_gap_resolved(
                        self._conn,
                        source_key=self.source_key,
                        decision_year=year,
                        decision_id=did,
                        resolution="ingested",
                    )
                elif status in {"none", "error"}:
                    mark_gap_failure(
                        self._conn,
                        source_key=self.source_key,
                        decision_year=year,
                        decision_id=did,
                        error_message=(error_message or status)[:500],
                        retry_delay_days=1,
                    )

            self._maybe_commit()
        except Exception as e:
            logger.debug(f"[{self.source_key}] fetch event logging failed: {e}")


def run_with_persistence(
    scraper_key: str,
    since_date: str | None = None,
    max_decisions: int | None = None,
    output_dir: Path = Path("output"),
    state_dir: Path = Path("state"),
    auto_coverage_snapshot: bool = True,
) -> int:
    """Run scraper and write each decision to JSONL incrementally.

    Returns the total number of scrape failures encountered.
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
    written_ids, written_ids_by_year = _load_written_ids_and_years(jsonl_path)
    if jsonl_path.exists():
        logger.info(f"Loaded {len(written_ids)} already-written decisions from {jsonl_path}")

    # Initialize scraper
    scraper = scraper_class(state_dir=state_dir)
    run_id = f"{scraper_key}:{datetime.now().isoformat(timespec='seconds')}"
    event_writer = _RunEventWriter(
        output_dir=output_dir,
        source_key=scraper_key,
        run_id=run_id,
    )

    # Parse since_date
    since = None
    if since_date:
        since = date.fromisoformat(since_date)

    # Run discovery and fetch
    new_count = 0
    skips = 0
    errors = 0
    none_count = 0
    changed_years: set[int] = set()
    start = time.time()

    logger.info(
        f"[{scraper_key}] Starting. Known: {scraper.state.count()}, "
        f"Written: {len(written_ids)}"
    )

    try:
        for i, stub in enumerate(scraper.discover_new(since)):
            if max_decisions and new_count >= max_decisions:
                logger.info(f"[{scraper_key}] Reached max_decisions={max_decisions}")
                break

            event_writer.log_discovery(stub)

            try:
                decision = scraper.fetch_decision(stub)
                if decision:
                    # Write full decision to JSONL (skip if already written)
                    if decision.decision_id not in written_ids:
                        with open(jsonl_path, "a", encoding="utf-8") as f:
                            f.write(serialize_decision(decision) + "\n")
                            f.flush()
                        written_ids.add(decision.decision_id)
                        year = _infer_decision_year(
                            decision_id=decision.decision_id,
                            docket_number=decision.docket_number,
                            decision_date=decision.decision_date,
                        )
                        if year is not None:
                            written_ids_by_year[year].add(decision.decision_id)
                            changed_years.add(year)
                        new_count += 1
                        if new_count % 100 == 0:
                            elapsed = time.time() - start
                            rate = new_count / elapsed * 3600
                            logger.info(
                                f"[{scraper_key}] Progress: {new_count} decisions, "
                                f"{rate:.0f}/hour, file: {jsonl_path.stat().st_size / 1024 / 1024:.1f} MB"
                            )
                    else:
                        skips += 1

                    # Mark scraped AFTER durable write to avoid gaps on crash
                    scraper.state.mark_scraped(decision.decision_id)
                    event_writer.log_fetch_attempt(
                        stub=stub,
                        status="success",
                        decision_id=decision.decision_id,
                        docket_number=decision.docket_number,
                        decision_date=decision.decision_date,
                    )

                    logger.info(
                        f"[{scraper_key}] Scraped: {decision.decision_id} "
                        f"({decision.decision_date})"
                    )
                else:
                    none_count += 1
                    event_writer.log_fetch_attempt(
                        stub=stub,
                        status="none",
                        error_type="NoneReturn",
                        error_message="fetch_decision returned None",
                    )
                    logger.warning(
                        f"[{scraper_key}] fetch_decision returned None ({none_count}): "
                        f"{stub.get('docket_number', '?')}"
                    )
                    # Consecutive Nones beyond a threshold suggest a systemic issue
                    max_none = getattr(scraper, "MAX_NONE_RETURNS", 200)
                    if none_count >= max_none:
                        errors += 1  # promote to real error for exit code
                        logger.error(
                            f"[{scraper_key}] Too many None returns ({none_count}), "
                            f"possible portal issue — stopping."
                        )
                        break

            except Exception as e:
                errors += 1
                event_writer.log_fetch_attempt(
                    stub=stub,
                    status="error",
                    error_type=e.__class__.__name__,
                    error_message=str(e),
                )
                logger.error(
                    f"[{scraper_key}] Error scraping {stub.get('docket_number', '?')}: {e}",
                    exc_info=True,
                )
                if errors >= getattr(scraper, "MAX_ERRORS", 50):
                    logger.error(f"[{scraper_key}] Too many errors ({errors}), stopping.")
                    break
    finally:
        event_writer.close()

    elapsed = time.time() - start
    file_size = jsonl_path.stat().st_size / 1024 / 1024 if jsonl_path.exists() else 0

    # Touch JSONL file on successful runs so dashboard doesn't mark as stale
    if jsonl_path.exists():
        jsonl_path.touch()

    logger.info(
        f"[{scraper_key}] Done. New: {new_count}, Skips: {skips}, "
        f"NoneReturns: {none_count}, Errors: {errors}, "
        f"Total written: {len(written_ids)}, Time: {elapsed / 60:.1f} min, "
        f"File: {jsonl_path} ({file_size:.1f} MB)"
    )

    if auto_coverage_snapshot:
        try:
            _record_coverage_snapshots(
                scraper_key=scraper_key,
                output_dir=output_dir,
                ids_by_year=written_ids_by_year,
                changed_years=changed_years,
            )
        except Exception as e:
            logger.warning(f"[{scraper_key}] Coverage snapshot update failed: {e}")

    # Return total failures so callers can enforce strict completeness.
    return errors


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
    parser.add_argument(
        "--no-coverage-snapshot",
        action="store_true",
        help="Disable automatic source snapshot update after scrape run",
    )
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
        auto_coverage_snapshot=not args.no_coverage_snapshot,
    )

    if exit_code:
        sys.exit(1)


if __name__ == "__main__":
    main()
