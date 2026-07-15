#!/usr/bin/env python3
"""Phase 2 — cross-identifier representation manifest (READ-ONLY).

Reads decisions.db (immutable) and emits `decision_representations`, linking each
member row to a canonical decision, for the cantons where a portal publishes one
decision under two identifiers:

  * GE  — the two rows share the SAME source_url (portal document). Canonical =
          the judgment (decision-number chamber code); members = the procedure-
          number publication page(s).
  * VD  — vd_findinfo publication pages declare the vd_gerichte procedure number
          in their text (content hashing does NOT work: 0 overlap, different text
          + different portals). Canonical = the direct vd_gerichte row.
  * SH  — sh_gerichte ↔ sh_obergericht by normalized docket. Canonical =
          sh_gerichte (official current); member = sh_obergericht (archive).

NO deletions, NO writes to decisions.db. Writes output/representation_manifest.db
and prints exact per-canton reductions + the corrected unique-decision count.

Design: docs/superpowers/specs/2026-07-15-cross-identifier-representations-design.md
"""
from __future__ import annotations
import collections
import os
import re
import sqlite3
import time
from pathlib import Path

DDIR = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
SRC = f"file:{DDIR}/decisions.db?mode=ro&immutable=1"
OUT = DDIR / "representation_manifest.db"

# GE cause/procedure prefixes (the publication-page side); everything else is a
# chamber decision code (the judgment side).
GE_PROC_PREFIXES = {"A", "P", "C", "AC", "PS", "PM", "PN", "PC", "(none)"}
# A VD internal docket token, any observed format (PE21.006087, E124.029276, …).
VD_TOKEN = re.compile(r"[A-Z]{1,3}\d{1,3}\.\d{4,6}")


def _sh_norm(dk: str | None) -> str:
    return re.sub(r"^(Nr\.?\s*)", "", (dk or ""), flags=re.I).strip()


def main() -> int:
    t0 = time.time()

    def el() -> str:
        return f"[{time.time() - t0:.0f}s]"

    src = sqlite3.connect(SRC, uri=True)
    src.row_factory = sqlite3.Row
    if OUT.exists():
        OUT.unlink()
    out = sqlite3.connect(str(OUT))
    out.executescript(
        """
        CREATE TABLE decision_representations (
            canonical_decision_id TEXT NOT NULL,
            member_decision_id    TEXT NOT NULL,
            canton                TEXT NOT NULL,
            relation_type         TEXT NOT NULL,
            evidence_method       TEXT NOT NULL,
            confidence            REAL NOT NULL,
            PRIMARY KEY (canonical_decision_id, member_decision_id)
        );
        CREATE INDEX idx_repr_member ON decision_representations(member_decision_id);
        """
    )
    links: list[tuple] = []

    def emit(canon, member, canton, rel, ev, conf):
        links.append((canon, member, canton, rel, ev, conf))

    # ---------- GE: shared source_url ----------
    by_url = collections.defaultdict(list)
    for r in src.execute(
        "SELECT source_url, decision_id, docket_number, length(full_text) L "
        "FROM decisions WHERE court='ge_gerichte' AND source_url != ''"
    ):
        by_url[r["source_url"]].append((r["decision_id"], r["docket_number"] or "", r["L"] or 0))

    def _is_proc(dk: str) -> bool:
        return (dk.split("/")[0] if "/" in dk else dk) in GE_PROC_PREFIXES

    ge_members = 0
    for url, rows in by_url.items():
        decs = [x for x in rows if not _is_proc(x[1])]
        canon = max(decs, key=lambda x: x[2]) if decs else max(rows, key=lambda x: x[2])
        for x in rows:
            rel = "judgment" if x[0] == canon[0] else (
                "publication_page" if _is_proc(x[1]) else "alt_representation")
            emit(canon[0], x[0], "GE", rel, "shared_source_url", 1.0)
            if x[0] != canon[0]:
                ge_members += 1
    print(f"GE: {len(by_url):,} canonical, {ge_members:,} member representations {el()}", flush=True)

    # ---------- VD: procedure-number cross-reference ----------
    vg = {}  # normalized docket -> vd_gerichte decision_id
    for r in src.execute(
        "SELECT decision_id, docket_number FROM decisions "
        "WHERE court='vd_gerichte' AND docket_number != ''"
    ):
        vg[re.sub(r"\s+", "", r["docket_number"]).upper()] = r["decision_id"]
    vd_canon = set()
    vd_members = 0
    for r in src.execute(
        "SELECT decision_id, substr(full_text, 1, 2500) h FROM decisions WHERE court='vd_findinfo'"
    ):
        for m in VD_TOKEN.finditer((r["h"] or "").upper()):
            cn = vg.get(m.group(0))
            if cn:
                if cn not in vd_canon:
                    emit(cn, cn, "VD", "judgment", "procedure_cross_reference", 1.0)
                    vd_canon.add(cn)
                emit(cn, r["decision_id"], "VD", "publication_page", "procedure_cross_reference", 0.9)
                vd_members += 1
                break
    print(f"VD: {len(vd_canon):,} vd_gerichte linked, {vd_members:,} vd_findinfo members {el()}", flush=True)

    # ---------- SH: normalized docket ----------
    sg = {}
    for r in src.execute("SELECT decision_id, docket_number FROM decisions WHERE court='sh_gerichte'"):
        sg[_sh_norm(r["docket_number"])] = r["decision_id"]
    sh_members = 0
    for r in src.execute("SELECT decision_id, docket_number FROM decisions WHERE court='sh_obergericht'"):
        cn = sg.get(_sh_norm(r["docket_number"]))
        if cn:
            emit(cn, cn, "SH", "judgment", "normalized_docket", 1.0)
            emit(cn, r["decision_id"], "SH", "archive_copy", "normalized_docket", 0.85)
            sh_members += 1
    print(f"SH: {sh_members:,} linked pairs {el()}", flush=True)

    out.executemany(
        "INSERT OR IGNORE INTO decision_representations VALUES (?,?,?,?,?,?)", links)
    out.commit()

    total = src.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    reduction = out.execute(
        "SELECT COUNT(*) FROM decision_representations "
        "WHERE canonical_decision_id != member_decision_id"
    ).fetchone()[0]
    print(f"=== manifest rows: {len(links):,} | duplicate representations: {reduction:,} ===")
    print(f"=== corpus records: {total:,}  ->  unique decisions ~= {total - reduction:,} ===")
    print(f"=== written: {OUT} {el()} ===")
    out.close()
    src.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
