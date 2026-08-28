"""Build ecthr_meta.db — structured Strasbourg metadata, keyed by decision_id.

HUDOC returns far more per judgment than the unified Decision schema can
carry.  ``scrapers/hudoc.py`` selects seventeen fields (``_SELECT_FIELDS``)
and, on the way into a Decision, flattens or discards most of them:

  respondent      → collapsed to canton 'CH' (Switzerland) or 'CE' (the
                    other 45 states); *which* state is lost
  importance      → collapsed to marked_for_publication (importance == '1')
  article         → rendered into a keyword tail on the regeste
  conclusion      → concatenated into the regeste string
  violation       → concatenated into the regeste string
  nonviolation    → concatenated into the regeste string
  separateopinion → selected from HUDOC, never carried into the stub
  originatingbody → selected from HUDOC, never carried into the stub

The last two are already paid for on the wire but dropped in
``_group_judgments``, so their columns here stay NULL until that dict
gains the two keys.  The columns exist now rather than later because the
schema is right either way and adding them post-hoc means a rebuild.
  appno           → truncated to three application numbers, because
                    make_decision_id does not truncate and multi-applicant
                    cases reach 3,795 characters (_MAX_DOCKET_APPNOS)

None of that is recoverable from decisions.db, so this sidecar goes back to
HUDOC rather than to the corpus.  It reuses the scraper's own discovery
pass, which already pages the full metadata 500 rows at a time — the same
requests the nightly ECtHR scrape makes, without fetching any document
bodies.  That keeps the builder independent of decisions.db entirely: no
read of the serving database, so it is safe to run inside the nightly
build window.

Respondent state gets its own table rather than a column.  HUDOC reports
inter-state and multi-respondent cases with several states on one judgment
(``'CHE;FRA'``), so "every judgment against Switzerland" is only an
indexed lookup if the relation is normalised.

Output: output/ecthr_meta.db (atomic swap via .db.tmp)

Usage:
    python3 -m search_stack.build_ecthr_meta
    python3 -m search_stack.build_ecthr_meta --from-year 2020
    python3 -m search_stack.build_ecthr_meta --output output/ecthr_meta.db
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

logger = logging.getLogger("build_ecthr_meta")

# Lawless v. Ireland (1960) is the first judgment in the corpus; start a
# year early so a re-dated entry cannot fall off the front, matching
# scrapers/hudoc.py's _CORPUS_START_YEAR.
CORPUS_START_YEAR = 1959

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ecthr_meta (
    decision_id      TEXT PRIMARY KEY,
    item_id          TEXT,
    hudoc_ecli       TEXT,
    appno_full       TEXT,
    importance       INTEGER,
    articles         TEXT,
    conclusion       TEXT,
    violation        TEXT,
    nonviolation     TEXT,
    separate_opinion INTEGER,
    originating_body TEXT,
    doc_type         TEXT,
    decision_date    TEXT,
    court            TEXT
);

CREATE INDEX IF NOT EXISTS idx_ecthr_meta_importance
    ON ecthr_meta(importance);
CREATE INDEX IF NOT EXISTS idx_ecthr_meta_court
    ON ecthr_meta(court);

-- Normalised: a judgment can name several respondent states.
CREATE TABLE IF NOT EXISTS ecthr_respondent (
    decision_id TEXT NOT NULL,
    state       TEXT NOT NULL,
    PRIMARY KEY (decision_id, state)
);

CREATE INDEX IF NOT EXISTS idx_ecthr_respondent_state
    ON ecthr_respondent(state);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def _importance(raw: str | None) -> int | None:
    """HUDOC importance as an int.

    The scale is inverted from intuition: 1 = Key cases, 2 = high,
    3 = medium, 4 = low/repetitive. The scraper only ingests 1-3.
    Anything non-numeric (HUDOC also emits the literal 'Key case') maps
    to 1 when it says so, else None.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    return 1 if s.lower().startswith("key") else None


def _respondent_states(raw: str | None) -> list[str]:
    """Split HUDOC's respondent field into ISO alpha-3 codes.

    HUDOC separates multiple respondents with ';' (inter-state and
    joined cases). Codes are upper-cased and de-duplicated while keeping
    HUDOC's order, so the first-named respondent stays first.
    """
    if not raw:
        return []
    seen: dict[str, None] = {}
    for part in str(raw).replace(",", ";").split(";"):
        code = part.strip().upper()
        if code:
            seen.setdefault(code, None)
    return list(seen)


def _separate_opinion(raw) -> int | None:
    """HUDOC's separateopinion is 'TRUE'/'FALSE' text, not a bool."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return int(raw)
    s = str(raw).strip().lower()
    if s in ("true", "1", "yes"):
        return 1
    if s in ("false", "0", "no"):
        return 0
    return None


def stub_to_rows(stub: dict) -> tuple[tuple, list[tuple[str, str]]]:
    """One discovery stub → (ecthr_meta row, [(decision_id, state), ...])."""
    did = stub["decision_id"]
    dd = stub.get("decision_date")
    meta_row = (
        did,
        stub.get("item_id") or None,
        stub.get("ecli") or None,
        # The full application-number list, not the docket's truncation.
        stub.get("appno") or None,
        _importance(stub.get("importance")),
        stub.get("article") or None,
        stub.get("conclusion") or None,
        stub.get("violation") or None,
        stub.get("nonviolation") or None,
        _separate_opinion(stub.get("separateopinion")),
        stub.get("originatingbody") or None,
        stub.get("doc_type") or None,
        dd.isoformat() if isinstance(dd, date) else (dd or None),
        stub.get("court") or None,
    )
    resp = [(did, s) for s in _respondent_states(stub.get("respondent"))]
    return meta_row, resp


_INSERT_META = """INSERT OR REPLACE INTO ecthr_meta
    (decision_id, item_id, hudoc_ecli, appno_full, importance, articles,
     conclusion, violation, nonviolation, separate_opinion,
     originating_body, doc_type, decision_date, court)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

_INSERT_RESP = """INSERT OR IGNORE INTO ecthr_respondent
    (decision_id, state) VALUES (?,?)"""


class IncompleteBuild(RuntimeError):
    """A build that would have shrunk the sidecar was refused."""


def _existing_count(path: Path) -> int | None:
    """Judgment count in the live sidecar, or None if there isn't one."""
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return conn.execute("SELECT COUNT(*) FROM ecthr_meta").fetchone()[0]
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def build(
    stubs: Iterable[dict],
    output_path: Path,
    *,
    generated_at: str | None = None,
    min_ratio: float | None = 0.9,
) -> dict:
    """Write every stub into a fresh DB, then atomically swap it in.

    Mirrors the corpus convention: build a .db.tmp and os.replace() it, so
    a reader never observes a half-written sidecar and no restart is needed.

    The swap is refused if the new build holds less than ``min_ratio`` of
    the judgments already published.  An atomic swap is exactly as good at
    installing a truncated sidecar as a complete one, and the upstream is a
    68-shard discovery pass where one HUDOC outage silently yields a short
    read (``HUDOCFullScraper.shard_failures`` counts them, but a lost year
    still produces a well-formed, wrong answer).  Growth is always allowed;
    only shrinkage needs a human.  Pass ``min_ratio=None`` to override —
    the corpus genuinely shrinks when HUDOC withdraws judgments.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".db.tmp")
    for leftover in (tmp_path, Path(str(tmp_path) + "-wal"), Path(str(tmp_path) + "-shm")):
        if leftover.exists():
            leftover.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    n_meta = 0
    n_resp = 0
    n_swiss = 0
    skipped = 0
    try:
        for stub in stubs:
            if not stub.get("decision_id"):
                skipped += 1
                continue
            meta_row, resp_rows = stub_to_rows(stub)
            conn.execute(_INSERT_META, meta_row)
            n_meta += 1
            for row in resp_rows:
                conn.execute(_INSERT_RESP, row)
                n_resp += 1
                if row[1] == "CHE":
                    n_swiss += 1
            if n_meta % 2000 == 0:
                conn.commit()
                logger.info("  %d judgments...", n_meta)

        stamp = generated_at or datetime.now(timezone.utc).isoformat()
        conn.executemany(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)",
            [
                ("generated_at", stamp),
                ("judgments", str(n_meta)),
                ("respondent_links", str(n_resp)),
                ("swiss_respondent", str(n_swiss)),
                ("source", "hudoc.echr.coe.int discovery metadata"),
            ],
        )
        conn.commit()
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
    finally:
        conn.close()

    for sidecar in (Path(str(tmp_path) + "-wal"), Path(str(tmp_path) + "-shm")):
        if sidecar.exists():
            sidecar.unlink()

    # Guard the swap, not the build: the .tmp is left in place so the short
    # read can be inspected rather than re-fetched.
    prior = _existing_count(output_path) if min_ratio is not None else None
    if prior and n_meta < prior * min_ratio:
        raise IncompleteBuild(
            f"refusing to publish {n_meta} judgments over the existing "
            f"{prior} (min_ratio={min_ratio}); HUDOC discovery likely lost "
            f"a year shard. Inspect {tmp_path}, then re-run — or pass "
            f"--allow-shrink if the corpus really did shrink."
        )

    os.replace(str(tmp_path), str(output_path))

    return {
        "judgments": n_meta,
        "respondent_links": n_resp,
        "swiss_respondent": n_swiss,
        "skipped": skipped,
        "output": str(output_path),
    }


def hudoc_stubs(
    from_year: int, to_year: int, status: dict | None = None
) -> Iterator[dict]:
    """Discovery stubs straight from HUDOC, one calendar year at a time.

    Reuses the scraper's own paging and its two-attempt shard merge, so
    this sees exactly the judgments the nightly scrape sees. No document
    bodies are fetched — discovery metadata only.
    """
    # HUDOCFullScraper is the Council-of-Europe-wide scraper; HUDOCScraper
    # is the Switzerland-only hudoc_ch one and would see 853 of the 9,610.
    from scrapers.hudoc import HUDOCFullScraper

    scraper = HUDOCFullScraper()
    for year in range(from_year, to_year + 1):
        rows = scraper._discover_year(year)
        if not rows:
            continue
        stubs = scraper._group_judgments(rows)
        logger.info("  %d: %d judgments", year, len(stubs))
        yield from stubs
    failures = getattr(scraper, "shard_failures", 0)
    if status is not None:
        status["shard_failures"] = failures
    if failures:
        # A lost year is a silently missing slice. Reported to the caller
        # via `status` so it can refuse the swap rather than publish a
        # well-formed, wrong sidecar.
        logger.warning(
            "%d year shard(s) failed — sidecar is INCOMPLETE", failures
        )


def jsonl_stubs(path: Path) -> Iterator[dict]:
    """Discovery stubs from a captured JSONL, for offline rebuilds/tests."""
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--output", default="output/ecthr_meta.db",
        help="Destination DB (default: output/ecthr_meta.db)",
    )
    parser.add_argument(
        "--from-year", type=int, default=CORPUS_START_YEAR,
        help=f"First year to discover (default: {CORPUS_START_YEAR})",
    )
    parser.add_argument(
        "--to-year", type=int, default=date.today().year,
        help="Last year to discover (default: current year)",
    )
    parser.add_argument(
        "--from-jsonl",
        help="Build from captured discovery stubs instead of querying HUDOC",
    )
    parser.add_argument(
        "--allow-shrink", action="store_true",
        help="Publish even if the build holds fewer judgments than the live "
             "sidecar (HUDOC withdrawals), and even if a year shard failed",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    status: dict = {}
    if args.from_jsonl:
        logger.info("Building from %s", args.from_jsonl)
        stubs = jsonl_stubs(Path(args.from_jsonl))
    else:
        logger.info(
            "Discovering ECtHR metadata from HUDOC, %d-%d",
            args.from_year, args.to_year,
        )
        stubs = hudoc_stubs(args.from_year, args.to_year, status)

    try:
        stats = build(
            stubs, Path(args.output),
            min_ratio=None if args.allow_shrink else 0.9,
        )
    except IncompleteBuild as e:
        logger.error("%s", e)
        return 2

    # A failed shard can still clear the 90% bar (one thin year out of 68),
    # so check it independently of the size guard.
    if status.get("shard_failures") and not args.allow_shrink:
        logger.error(
            "%d HUDOC year shard(s) failed; %s is published but INCOMPLETE. "
            "Re-run to repair.", status["shard_failures"], args.output,
        )
        return 3
    logger.info(
        "Wrote %s: %d judgments, %d respondent links (%d against Switzerland)",
        stats["output"], stats["judgments"],
        stats["respondent_links"], stats["swiss_respondent"],
    )
    if stats["skipped"]:
        logger.warning("%d stub(s) skipped: no decision_id", stats["skipped"])
    return 0 if stats["judgments"] else 1


if __name__ == "__main__":
    sys.exit(main())
