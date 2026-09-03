#!/usr/bin/env python3
"""Seed the BGer date sidecar and recover same-docket second rulings.

BGer issues several rulings under one docket (recusal / provisional
measures / final judgment). decision_id is docket-keyed, so once a docket
was held, discovery permanently skipped every later ruling under it —
2C_532/2025: interim ruling of 2025-11-18 held, final judgment of
2026-07-21 listed on Neuheiten 2026-09-03 and never fetched (the poller
misreported it as a doc-service error page). The scraper is now
date-aware (sidecar state/bger.dates.txt, "-D<YYYYMMDD>" suffixed ids for
collision rulings) but runs in LEGACY mode until the sidecar exists.

This script (1) seeds the sidecar from the corpus JSONL's (docket_number,
decision_date) pairs, then (2) runs the date-aware Neuheiten discovery,
which yields exactly the suppressed rulings of the last 14 days, fetches
them and appends them to the corpus JSONL + state. Genuinely new dockets
are left to the poller (its accounting expects to fetch them itself).

Run ON THE VPS, OUTSIDE the nightly build window (03:30–~17:00 UTC): the
seed streams the 1.5 GB corpus JSONL once (page-cache pressure the quality
gate cannot absorb — see CLAUDE.md invariant 9), and fetch mode appends to
the JSONL the build reads. Through the residential tunnel:
  cd /opt/caselaw/repo
  nice -n 19 ionice -c 3 python3 backfill_bger_docket_collisions.py --seed-only
  BGER_PROXY=socks5h://127.0.0.1:1080 \\
  REQUESTS_CA_BUNDLE=/opt/caselaw/certs/ca-bundle.pem \\
  python3 backfill_bger_docket_collisions.py --dry-run      # list collisions
  ...                                                        # fetch + append

Idempotent: an existing non-empty sidecar is left alone (--reseed rebuilds
it); already-fetched rulings are in the sidecar and yield nothing. Rows the
poller quick-publishes itself (next hourly run) are simply found "known".
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from models import make_decision_id  # noqa: E402

RE_ID = re.compile(r'"decision_id"\s*:\s*"([^"]+)"')
RE_DOCKET = re.compile(r'"docket_number"\s*:\s*"([^"]+)"')
RE_DATE = re.compile(r'"decision_date"\s*:\s*"(\d{4}-\d{2}-\d{2})')
HEAD = 8192   # the three fields sit at the start of every row


def corpus_jsonl() -> Path:
    return REPO / "output" / "decisions" / "bger.jsonl"


def _fields(line: str) -> tuple[str | None, str | None, str | None]:
    head = line[:HEAD]
    out = []
    for rx in (RE_ID, RE_DOCKET, RE_DATE):
        m = rx.search(head) or rx.search(line)
        out.append(m.group(1) if m else None)
    return out[0], out[1], out[2]


def seed_sidecar(scraper, src: Path, reseed: bool) -> int:
    side = scraper._dates_sidecar()
    if side.exists() and side.stat().st_size > 0 and not reseed:
        n = len([l for l in side.read_text().splitlines() if l.strip()])
        print(f"[bger] sidecar already seeded ({n} pairs) — kept")
        return n
    if not src.exists():
        raise SystemExit(f"[bger] corpus JSONL missing: {src}")
    pairs: list[str] = []
    seen: set[str] = set()
    undated = 0
    with open(src, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            did, docket, d_iso = _fields(line)
            if not did or not docket:
                continue
            if not d_iso:
                undated += 1
                continue
            base = make_decision_id("bger", docket)
            key = f"{base}|{d_iso}"
            if key in seen:
                continue
            seen.add(key)
            pairs.append(f"{base}\t{d_iso}\t{did}")
    if not pairs:
        raise SystemExit(
            "[bger] corpus yielded 0 (docket, date) pairs — refusing to "
            "write an empty sidecar (it would read as seeded to this script "
            "and as half-seeded to the loader)")
    tmp = side.with_suffix(".tmp")
    tmp.write_text("\n".join(pairs) + "\n")
    tmp.replace(side)      # atomic — a crash mid-seed leaves no half file
    print(f"[bger] sidecar seeded with {len(pairs)} pairs from corpus "
          f"({undated} undated rows skipped)")
    return len(pairs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="seed (if needed) and list collisions; fetch nothing")
    ap.add_argument("--seed-only", action="store_true")
    ap.add_argument("--reseed", action="store_true")
    ap.add_argument("--max", type=int, default=50,
                    help="safety cap on fetched rulings")
    ap.add_argument("--state", default=str(REPO / "state"))
    ap.add_argument("--jsonl", default=str(corpus_jsonl()))
    args = ap.parse_args()

    from scrapers.bger import BgerScraper
    scraper = BgerScraper(state_dir=Path(args.state))
    src = Path(args.jsonl)
    print(f"[bger] state:   {scraper.state.state_file}")
    print(f"[bger] sidecar: {scraper._dates_sidecar()}")
    print(f"[bger] corpus:  {src}")
    known_n = len(getattr(scraper.state, "_seen", []) or [])
    if not src.exists():
        raise SystemExit(f"[bger] corpus JSONL missing: {src} — run on the "
                         "VPS, not a dev checkout")
    if known_n == 0 and src.stat().st_size > 0:
        raise SystemExit(
            "[bger] state knows 0 ids but the corpus is non-empty — wrong "
            "state path; refusing (collision rulings would be written under "
            "plain held ids)")
    seed_sidecar(scraper, src, args.reseed)
    if args.seed_only:
        return 0

    # Neuheiten only (14-day window): the AZA search is the Imperva stall
    # site and is not needed — every publication appears on Neuheiten.
    scraper.neuheiten_only = True
    if not os.environ.get("BGER_PROXY") and not os.environ.get("SCRAPER_PROXY"):
        print("[bger] WARNING: no BGER_PROXY/SCRAPER_PROXY — search.bger.ch "
              "hard-blocks the Hetzner IP; expect empty pages")
    collisions = []
    fresh = 0
    for stub in scraper.discover_new():
        if "-D" in stub["decision_id"]:
            collisions.append(stub)
        else:
            fresh += 1
    print(f"[bger] discovery: {len(collisions)} same-docket second rulings, "
          f"{fresh} genuinely new dockets (left to the poller)")
    if scraper._known_dates is None:
        print("[bger] scraper ran in LEGACY mode — sidecar not accepted "
              "(see log: half-seed guard / unreadable); nothing to do")
        return 2
    for s in collisions:
        print(f"    {s['docket_number']:14s} {s.get('decision_date')}  "
              f"→ {s['decision_id']}")
    if args.dry_run or not collisions:
        return 0

    from run_scraper import serialize_decision
    fetched = 0
    with open(src, "a", encoding="utf-8") as out:
        for s in collisions[: args.max]:
            dec = scraper.fetch_decision(s)
            if dec is None:
                print(f"    {s['docket_number']}: fetch returned None "
                      "(doc-service error page?) — left for the next run")
                continue
            out.write(serialize_decision(dec) + "\n")
            out.flush()
            scraper.state.mark_scraped(dec.decision_id)   # after durable write
            fetched += 1
            print(f"    {dec.docket_number} {dec.decision_date} "
                  f"→ {dec.decision_id} ({len(dec.full_text or '')} chars)")
    print(f"[bger] appended {fetched} rulings to {src}; they reach "
          "decisions.db with the next quick_publish / nightly build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
