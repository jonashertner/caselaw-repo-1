"""Enrich decisions with derive-from-text fields: corrected date + provenance,
normalized docket, ECLI, canonical_key. Per the canonical-decision-identity spec
(docs/superpowers/specs/2026-06-28-canonical-decision-identity-design.md).

This is the reusable enrichment the nightly build step will call. Run modes:

    python backfill_canonical_identity.py --db DECISIONS.db --report
        read-only: print the corpus-wide impact (no writes).
    python backfill_canonical_identity.py --db SRC.db --write OUT.db
        write an enriched copy (for shadow validation); never the live volume.

Invariant: never overwrite a real (source_metadata) date; only synthetic/NULL
dates are replaced, and every change is provenance-stamped.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import derive_from_text as d  # noqa: E402

FEDERAL = ("bge", "bger", "bvger", "bstger", "bpatger", "mkg")


def _enrich_row(court, stored_date, docket, full_text, max_year, max_date=None):
    """Return (best_date, provenance, norm_docket, ecli)."""
    if stored_date and not d.is_synthetic_date(stored_date):
        best, prov = stored_date, "source_metadata"
    else:
        best, prov = d.derive_date(stored_date, full_text, max_year=max_year, max_date=max_date)
    # For BGE the docket_number field holds the BGE CITATION ('152 II 1'), not the
    # originating federal docket. The canonical key must be the FEDERAL docket
    # (it is what the paired docket row also yields), so always read it from the
    # Urteilskopf for BGE.
    if court == "bge":
        uk = d.extract_urteilskopf(full_text, max_year=max_year)
        if uk.get("docket"):
            docket = uk["docket"]
    nd = d.normalize_docket(docket) if docket else None
    ecli = d.build_ecli(court, best, nd)
    return best, prov, nd, ecli


def run_report(db_path: str, max_year: int) -> None:
    c = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    today = date.today().isoformat()

    total = c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    real = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NOT NULL "
        "AND decision_date<>'' AND decision_date NOT LIKE '%-01-01'").fetchone()[0]
    future = c.execute("SELECT COUNT(*) FROM decisions WHERE decision_date > ?", (today,)).fetchone()[0]

    prov = {"source_metadata": real, "extracted_from_text": 0, "volume_synthetic": 0, "null": 0}
    pub_recovered = 0          # publication_date recovered from the volume year
    date_changed = 0
    canon: dict[str, int] = {}     # ECLI -> members (collisions = dedup pairs)
    ecli_total = 0

    # only rows that need extraction (synthetic / NULL) read full_text
    need = c.execute(
        "SELECT decision_id,court,decision_date,publication_date,docket_number,full_text "
        "FROM decisions WHERE decision_date IS NULL OR decision_date='' OR decision_date LIKE '%-01-01'")
    for did, court, sd, spub, docket, ft in need:
        # demux decision vs publication date (they are distinct)
        dd, dprov, pd, pprov = d.derive_dates(sd, spub, ft, max_year=max_year, max_date=today)
        prov[dprov] += 1
        if dprov == "extracted_from_text":
            date_changed += 1
        if pprov == "volume_year" and not spub:
            pub_recovered += 1
        # ECLI keyed on the DECISION year + federal docket
        _, _, nd, ecli = _enrich_row(court, sd, docket, ft, max_year, today)
        if ecli:
            ecli_total += 1
            canon[ecli] = canon.get(ecli, 0) + 1

    # ECLI for the real-dated federal rows (no full_text needed) -> dedup picture
    for court, sd, docket in c.execute(
        "SELECT court,decision_date,docket_number FROM decisions "
        "WHERE court IN (%s) AND decision_date NOT LIKE '%%-01-01' "
        "AND decision_date IS NOT NULL" % ",".join("'%s'" % x for x in FEDERAL)):
        ecli = d.build_ecli(court, sd, docket)
        if ecli:
            ecli_total += 1
            canon[ecli] = canon.get(ecli, 0) + 1

    dedup_pairs = sum(1 for v in canon.values() if v > 1)

    print("=== CANONICAL IDENTITY — corpus-wide impact (read-only) ===")
    print(f"  decisions total: {total:,}")
    print(f"  date provenance distribution:")
    for k in ("source_metadata", "extracted_from_text", "volume_synthetic", "null"):
        print(f"    {k:22} {prov[k]:>8,} ({100*prov[k]/total:.1f}%)")
    print(f"  DECISION dates CORRECTED (synthetic/NULL -> real from text): {date_changed:,}")
    print(f"  PUBLICATION dates recovered from volume year (were NULL): {pub_recovered:,}")
    print(f"  still-unverified decision dates after extraction (flagged): {prov['volume_synthetic']+prov['null']:,}")
    print(f"  impossible future dates currently served: {future}")
    print(f"  ECLI built (federal): {ecli_total:,}")
    print(f"  canonical-key collisions (BGE<->docket dedup pairs): {dedup_pairs:,}")


def run_write(src_path: str, out_path: str, max_year: int) -> None:
    """Build the canonical_identity SIDECAR (one row per enriched decision) from a
    read-only source. The nightly build would JOIN this onto decisions.db; this is
    the shadow artifact for validation. full_text is read only where extraction is
    needed (synthetic/NULL dates + BGE headers), not for the whole corpus.
    """
    src = sqlite3.connect(f"file:{src_path}?mode=ro&immutable=1", uri=True)
    today = date.today().isoformat()
    out = sqlite3.connect(out_path)
    out.executescript(
        "PRAGMA journal_mode=OFF;"
        "DROP TABLE IF EXISTS canonical_identity;"
        "CREATE TABLE canonical_identity("
        " decision_id TEXT PRIMARY KEY,"
        " decision_date TEXT, decision_date_provenance TEXT,"
        " publication_date TEXT, publication_date_provenance TEXT,"
        " ecli TEXT, canonical_key TEXT);")
    INS = ("INSERT OR REPLACE INTO canonical_identity VALUES (?,?,?,?,?,?,?)")
    batch: list = []
    written = scanned = 0

    def flush():
        if batch:
            out.executemany(INS, batch)
            batch.clear()

    # Pass 1 — federal, real-dated, NOT bge: ECLI from the docket_number field
    # (no full_text needed). bge always needs the header (pass 2).
    fed_nobge = tuple(x for x in FEDERAL if x != "bge")
    q1 = ("SELECT decision_id,court,decision_date,docket_number FROM decisions "
          "WHERE court IN (%s) AND decision_date NOT LIKE '%%-01-01' "
          "AND decision_date IS NOT NULL" % ",".join("'%s'" % x for x in fed_nobge))
    for did, court, sd, docket in src.execute(q1):
        scanned += 1
        ecli = d.build_ecli(court, sd, docket)
        if ecli:
            batch.append((did, sd, "source_metadata", None, "null", ecli, ecli))
            written += 1
            if len(batch) >= 5000:
                flush()
    flush()

    # Pass 2 — everything needing text extraction: synthetic/NULL dates + all bge.
    q2 = ("SELECT decision_id,court,decision_date,publication_date,docket_number,full_text "
          "FROM decisions WHERE court='bge' OR decision_date IS NULL OR decision_date='' "
          "OR decision_date LIKE '%-01-01'")
    for did, court, sd, spub, docket, ft in src.execute(q2):
        scanned += 1
        dd, dprov, pd, pprov = d.derive_dates(sd, spub, ft, max_year=max_year, max_date=today)
        _, _, nd, ecli = _enrich_row(court, sd, docket, ft, max_year, today)
        ck = ecli
        # only write rows that actually carry enrichment
        if dprov == "extracted_from_text" or pprov == "volume_year" or ecli:
            batch.append((did, dd, dprov, pd, pprov, ecli, ck))
            written += 1
            if len(batch) >= 5000:
                flush()
    flush()
    out.commit()
    print("=== canonical_identity sidecar written ===")
    print(f"  -> {out_path}")
    print(f"  scanned {scanned:,} | enriched rows written {written:,}")
    n = out.execute("SELECT COUNT(*) FROM canonical_identity WHERE ecli IS NOT NULL").fetchone()[0]
    dc = out.execute("SELECT COUNT(*) FROM canonical_identity WHERE decision_date_provenance='extracted_from_text'").fetchone()[0]
    pc = out.execute("SELECT COUNT(*) FROM canonical_identity WHERE publication_date_provenance='volume_year'").fetchone()[0]
    print(f"  with ECLI: {n:,} | decision dates extracted: {dc:,} | pub dates recovered: {pc:,}")
    out.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--write", metavar="OUT")
    ap.add_argument("--max-year", type=int, default=date.today().year)
    a = ap.parse_args()
    if a.write:
        run_write(a.db, a.write, a.max_year)
    else:
        run_report(a.db, a.max_year)


if __name__ == "__main__":
    main()
