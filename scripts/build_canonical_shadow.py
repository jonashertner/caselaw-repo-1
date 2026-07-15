#!/usr/bin/env python3
"""Phase 3 — shadow canonical build + validation (READ-ONLY).

Reads decisions.db + representation_manifest.db, materializes the canonicalization
into a SEPARATE output/canonical_shadow.db, and validates it WITHOUT touching
decisions.db or the pipeline. It proves the two things that could go wrong in the
eventual production collapse (Phase 5, invariant-#5-gated):

  1. No broken references — every representation member (and its docket) resolves
     to exactly ONE canonical decision.
  2. No data loss — the retain-and-link merge recovers the metadata that lives on
     the member side (VD regeste, GE appeal chain, legal_area, chamber).

Writes: identifier_aliases (every member id + docket -> canonical) and a harvest
report. NO production change.

Design: docs/superpowers/specs/2026-07-15-cross-identifier-representations-design.md
"""
from __future__ import annotations
import collections
import os
import sqlite3
import time
from pathlib import Path

DDIR = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
DEC = f"file:{DDIR}/decisions.db?mode=ro&immutable=1"
MAN = f"file:{DDIR}/representation_manifest.db?mode=ro"
OUT = DDIR / "canonical_shadow.db"

APP_RE = None  # appeal harvest is text-based; measured separately (Phase 3b)


def main() -> int:
    t0 = time.time()

    def el() -> str:
        return f"[{time.time() - t0:.0f}s]"

    dec = sqlite3.connect(DEC, uri=True)
    dec.row_factory = sqlite3.Row
    man = sqlite3.connect(MAN, uri=True)
    man.row_factory = sqlite3.Row

    # 1. Load links (excluding self-links), detecting member -> multiple canonical.
    member_to_canon: dict[str, str] = {}
    canon_members: dict[str, list] = collections.defaultdict(list)
    conflicts = 0
    for r in man.execute(
        "SELECT canonical_decision_id c, member_decision_id m FROM decision_representations "
        "WHERE canonical_decision_id != member_decision_id"
    ):
        m, c = r["m"], r["c"]
        if m in member_to_canon and member_to_canon[m] != c:
            conflicts += 1
        member_to_canon[m] = c
        canon_members[c].append(m)
    print(f"links: {len(member_to_canon):,} members -> {len(canon_members):,} canonicals | "
          f"member->multiple-canonical conflicts: {conflicts} {el()}", flush=True)

    # 2. Fetch metadata (no full_text) for every involved id.
    ids = list(set(member_to_canon) | set(canon_members))
    meta: dict[str, dict] = {}
    for i in range(0, len(ids), 400):
        ph = ",".join("?" * len(ids[i:i + 400]))
        for r in dec.execute(
            "SELECT decision_id, court, docket_number, regeste, legal_area, chamber, "
            f"source_url FROM decisions WHERE decision_id IN ({ph})", ids[i:i + 400]
        ):
            meta[r["decision_id"]] = dict(r)
    print(f"metadata fetched for {len(meta):,} / {len(ids):,} ids {el()}", flush=True)

    # 3. Validate resolution: every member id and every canonical exists in decisions.
    missing_members = sum(1 for m in member_to_canon if m not in meta)
    missing_canon = sum(1 for c in canon_members if c not in meta)
    print(f"resolution: members missing in decisions.db: {missing_members} | "
          f"canonicals missing: {missing_canon}")

    # 4. Metadata harvest: canonicals recovering a field from a member.
    reg_rec = leg_rec = cham_rec = 0
    for c, members in canon_members.items():
        cm = meta.get(c, {})
        rows = [cm] + [meta.get(m, {}) for m in members]
        if not cm.get("regeste") and any(x.get("regeste") for x in rows):
            reg_rec += 1
        if not cm.get("legal_area") and any(x.get("legal_area") for x in rows):
            leg_rec += 1
        if not cm.get("chamber") and any(x.get("chamber") for x in rows):
            cham_rec += 1
    print(f"metadata HARVEST into canonicals: regeste +{reg_rec:,}  "
          f"legal_area +{leg_rec:,}  chamber +{cham_rec:,}")

    # 5. Write the alias/resolution table (every member + its docket -> canonical).
    if OUT.exists():
        OUT.unlink()
    out = sqlite3.connect(str(OUT))
    out.execute(
        "CREATE TABLE identifier_aliases (alias_decision_id TEXT PRIMARY KEY, "
        "alias_docket TEXT, canonical_decision_id TEXT NOT NULL, canton TEXT)")
    out.execute("CREATE INDEX idx_alias_canon ON identifier_aliases(canonical_decision_id)")
    rows = [
        (m, meta.get(m, {}).get("docket_number", ""), c,
         (meta.get(m, {}).get("court", "") or "")[:2].upper())
        for m, c in member_to_canon.items()
    ]
    out.executemany("INSERT OR IGNORE INTO identifier_aliases VALUES (?,?,?,?)", rows)
    out.commit()
    alias_n = out.execute("SELECT COUNT(*) FROM identifier_aliases").fetchone()[0]

    total = dec.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    canonical_count = total - len(member_to_canon)
    print(f"aliases written: {alias_n:,}")
    print(f"=== corpus records: {total:,}  -  members: {len(member_to_canon):,}  "
          f"=  canonical decisions: {canonical_count:,} ===")
    print(f"shadow written: {OUT} {el()}", flush=True)
    out.close()
    dec.close()
    man.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
