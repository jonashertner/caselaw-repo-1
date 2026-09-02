#!/usr/bin/env python3
"""Backfill the NE fiche collisions found by the 2026-09-02 gap forensics.

The NE portals (jurisprudence.ne.ch, jurisprudenceadm.ne.ch) list multiple
fiches per docket — interim and final rulings each carry their own nF30_KEY.
decision_id is docket-keyed, so discovery permanently suppressed every later
fiche of an already-held docket (~45 + ~37 rulings). The scrapers are now
fiche-aware (per-fiche sidecar state/{court}.nf30.txt, "-F<nf30>" suffixed
ids for collision fiches) but run in LEGACY mode until the sidecar exists.

This script (1) seeds the sidecar from the corpus JSONL's source_urls, then
(2) runs the fiche-aware discovery, which yields exactly the suppressed
fiches, fetches them and appends them to the corpus JSONL + state.

Run ON THE VPS through the residential tunnel, OUTSIDE the nightly build
window (03:30-~19:00 UTC) — it appends to the corpus JSONL the build reads:
  SCRAPER_PROXY=socks5h://127.0.0.1:1080 \
  REQUESTS_CA_BUNDLE=/opt/caselaw/certs/ca-bundle.pem \
  python3 backfill_ne_fiche_collisions.py --court all --dry-run   # inspect
  ... --court all                                                  # fetch

Idempotent: an existing non-empty sidecar is left alone (use --reseed to
rebuild it); already-fetched fiches are in the sidecar and yield nothing.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from run_scraper import serialize_decision  # noqa: E402

RE_NF30 = re.compile(r"nF30_KEY=(\d+)")

COURTS = {
    "ne_gerichte": ("scrapers.cantonal.ne_gerichte", "NEGerichteScraper"),
    "ne_jurisprudence_adm": (
        "scrapers.cantonal.ne_jurisprudence_adm", "NEJurisprudenceAdmScraper"),
}


def corpus_jsonl(court: str) -> Path:
    return REPO / "output" / "decisions" / f"{court}.jsonl"


def seed_sidecar(scraper, court: str, reseed: bool) -> int:
    side = scraper._nf30_sidecar()
    if side.exists() and side.stat().st_size > 0 and not reseed:
        n = len([l for l in side.read_text().splitlines() if l.strip()])
        print(f"[{court}] sidecar already seeded ({n} fiches) — kept")
        return n
    src = corpus_jsonl(court)
    if not src.exists():
        raise SystemExit(f"[{court}] corpus JSONL missing: {src}")
    pairs = []
    seen = set()
    with open(src, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = RE_NF30.search(line)
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            did = ""
            dm = re.search(r'"decision_id"\s*:\s*"([^"]+)"', line)
            if dm:
                did = dm.group(1)
            pairs.append(f"{m.group(1)}\t{did}")
    if not pairs:
        raise SystemExit(
            f"[{court}] corpus yielded 0 nf30 pairs — refusing to write an "
            "empty sidecar (it would read as seeded to this script and as "
            "half-seeded to the loader)")
    tmp = side.with_suffix(".tmp")
    tmp.write_text("\n".join(pairs) + "\n")
    tmp.replace(side)      # atomic — a crash mid-seed leaves no half file
    print(f"[{court}] sidecar seeded with {len(pairs)} fiches from corpus")
    return len(pairs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--court", choices=[*COURTS, "all"], default="all")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed-only", action="store_true")
    ap.add_argument("--reseed", action="store_true")
    ap.add_argument("--max", type=int, default=200,
                    help="safety cap on fetched fiches per court")
    args = ap.parse_args()

    targets = list(COURTS) if args.court == "all" else [args.court]
    grand_total = 0
    for court in targets:
        mod_name, cls_name = COURTS[court]
        import importlib
        cls = getattr(importlib.import_module(mod_name), cls_name)
        scraper = cls(state_dir=REPO / "state")
        print(f"[{court}] state:   {scraper.state.state_file}")
        print(f"[{court}] sidecar: {scraper._nf30_sidecar()}")
        print(f"[{court}] corpus:  {corpus_jsonl(court)}")
        known_n = len(getattr(scraper.state, "_seen", []) or [])
        cj = corpus_jsonl(court)
        if not cj.exists():
            raise SystemExit(f"[{court}] corpus JSONL missing: {cj} — "
                             "run on the VPS, not a dev checkout")
        if known_n == 0 and cj.stat().st_size > 0:
            raise SystemExit(
                f"[{court}] state knows 0 ids but the corpus is non-empty — "
                "wrong state path; refusing (collision fiches would be "
                "written under plain held ids)")
        seed_sidecar(scraper, court, args.reseed)
        if args.seed_only:
            continue

        stubs = []
        for stub in scraper.discover_new():
            stubs.append(stub)
            if len(stubs) >= args.max:
                print(f"[{court}] hit --max {args.max}; rerun to continue")
                break
        collisions = [s for s in stubs if "-F" in s["decision_id"]]
        fresh = [s for s in stubs if "-F" not in s["decision_id"]]
        print(f"[{court}] discovery: {len(collisions)} collision fiches, "
              f"{len(fresh)} genuinely new")
        # Same-date warning: a collision fiche dated identically to the
        # held plain row shares its canonical key; build dedup would keep
        # only the longer text (re-verify, minor). Flag for manual review.
        held_dates = {}
        with open(corpus_jsonl(court), encoding="utf-8",
                  errors="replace") as fh:
            import json as _json
            wanted = {c["docket_number"] for c in collisions}
            for line in fh:
                try:
                    row = _json.loads(line)
                except Exception:
                    continue
                if row.get("docket_number") in wanted:
                    held_dates.setdefault(
                        row["docket_number"], set()).add(
                        str(row.get("decision_date")))
        for s in stubs:
            tag = "COLLISION" if s in collisions else "new"
            warn = ""
            if (tag == "COLLISION" and str(s.get("decision_date"))
                    in held_dates.get(s["docket_number"], set())):
                warn = "  ** SAME DATE as held row — dedup risk, review **"
            print(f"  [{tag}] {s['decision_id']}  docket={s['docket_number']} "
                  f"nf30={s.get('nf30_key')} date={s.get('decision_date')}{warn}")
        if args.dry_run:
            continue

        out = corpus_jsonl(court)
        # trailing-newline repair so our append cannot glue onto a
        # truncated last line
        with open(out, "rb+") as fh:
            fh.seek(0, 2)
            if fh.tell() > 0:
                fh.seek(-1, 2)
                if fh.read(1) != b"\n":
                    fh.write(b"\n")
        fetched = 0
        for s in stubs:
            # Re-filter against CURRENT state: the first fiche of a new
            # docket claims the plain id via mark_scraped below, so its
            # sibling in this same batch must take the -F path here
            # (two-phase collect-then-fetch would otherwise write both
            # under one id — review F3/F6).
            s = scraper._stub_filter(s)
            if s is None:
                continue
            d = scraper.fetch_decision(s)
            if d is None:
                print(f"  NONE: {s['decision_id']}")
                continue
            if d.decision_id != s["decision_id"]:
                # fetch_decision must honour the stub id — hard error,
                # never write a row under a colliding id.
                print(f"  ID MISMATCH (skipped): stub={s['decision_id']} "
                      f"decision={d.decision_id}")
                continue
            with open(out, "a", encoding="utf-8") as fh:
                fh.write(serialize_decision(d) + "\n")
            scraper.state.mark_scraped(d.decision_id)
            scraper._mark_nf30(s.get("nf30_key"), d.decision_id)
            fetched += 1
            print(f"  FETCHED: {d.decision_id} ({d.decision_date})")
        grand_total += fetched
        print(f"[{court}] backfilled {fetched}")
    print(f"TOTAL backfilled: {grand_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
