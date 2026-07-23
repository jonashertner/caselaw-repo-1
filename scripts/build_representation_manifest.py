#!/usr/bin/env python3
"""Cross-identifier representation manifest (READ-ONLY, counting-grade).

Reads decisions.db (immutable) and emits `decision_representations`, linking each
member row to a canonical decision, for the courts where a portal publishes one
decision under two identifiers (or byte-identical twice):

  * GE  — the two rows share the SAME source_url (portal document). Canonical =
          the judgment (decision-number chamber code); members = the procedure-
          number publication page(s).
  * VD  — vd_findinfo publication pages declare the vd_gerichte procedure number
          in their rubrum. Canonical = the direct vd_gerichte row. Linked only
          when the procedure token maps to EXACTLY ONE vd_gerichte (unambiguous).
  * SH  — sh_gerichte <-> sh_obergericht by normalized docket. Canonical =
          sh_gerichte (official current); member = sh_obergericht (archive).
  * ch_vb — BYTE-IDENTICAL rows: same (source_url, content_hash). Frozen
          entscheidsuche archive; a document ingested twice. Canonical = longest
          full_text (arbitrary; refine at merge time).
  * nw / edoeb / ur — same source_url, size-2 group, DIFFERENT text (two
          identifiers, differently-extracted). Same two-identifier pattern as GE.

CONSERVATIVE by design: a shared-source_url pair is linked only when the two
rows AGREE on decision_date (never collapse two rows that could be distinct
rulings). The one exception is UR's known 1905-01-01 sentinel bug: a
1905-vs-real-date pair is still the same decision, so the sentinel is treated as
a date wildcard. Date-disagreeing pairs are reported as an UPPER-bound band, not
linked. ch_vb byte-identical rows are linked regardless of the date field.

NO deletions, NO writes to decisions.db. Writes output/representation_manifest.db
ATOMICALLY (.tmp then os.replace) with a manifest_meta table carrying the source
DB generation, so a stale manifest (built against a superseded corpus) can be
detected and its count suppressed rather than served as current.

Design: docs/superpowers/specs/2026-07-15-cross-identifier-representations-design.md
"""
from __future__ import annotations
import collections
import json
import os
import re
import sqlite3
import time
from pathlib import Path

ALGO_VERSION = "2026-07-23.1"  # bump when a linking rule changes
DATE_SENTINEL = "1905-01-01"   # UR portal placeholder (see ur_gerichte date bug)

DDIR = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
SRC = f"file:{DDIR}/decisions.db?mode=ro&immutable=1"
OUT = DDIR / "representation_manifest.db"
TMP = DDIR / "representation_manifest.db.tmp"

# GE cause/procedure prefixes (the publication-page side); everything else is a
# chamber decision code (the judgment side).
GE_PROC_PREFIXES = {"A", "P", "C", "AC", "PS", "PM", "PN", "PC", "(none)"}
# A VD internal docket token, any observed format (PE21.006087, E124.029276, ...).
VD_TOKEN = re.compile(r"[A-Z]{1,3}\d{1,3}\.\d{4,6}")

# Courts linked purely by shared source_url + size-2 group + date agreement
# (the two-identifier pattern; distinct text on each identifier).
SHARED_URL_COURTS = ("nw_gerichte", "edoeb", "ur_gerichte")


def _sh_norm(dk: str | None) -> str:
    return re.sub(r"^(Nr\.?\s*)", "", (dk or ""), flags=re.I).strip()


def _dates_compatible(a: str | None, b: str | None) -> bool:
    """Two representations are date-compatible when their decision_date matches,
    or one side carries UR's known 1905 sentinel (a bug, not a real date)."""
    if a == b:
        return True
    return DATE_SENTINEL in (a, b) and (a or b)


def main() -> int:
    t0 = time.time()

    def el() -> str:
        return f"[{time.time() - t0:.0f}s]"

    src = sqlite3.connect(SRC, uri=True)
    src.row_factory = sqlite3.Row

    if TMP.exists():
        TMP.unlink()
    out = sqlite3.connect(str(TMP))
    out.executescript(
        """
        CREATE TABLE decision_representations (
            canonical_decision_id TEXT NOT NULL,
            member_decision_id    TEXT NOT NULL,
            canton                TEXT NOT NULL,
            relation_type         TEXT NOT NULL,
            evidence_method       TEXT NOT NULL,
            confidence            REAL NOT NULL,
            date_match            INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (canonical_decision_id, member_decision_id),
            UNIQUE (member_decision_id)
        );
        CREATE INDEX idx_repr_member ON decision_representations(member_decision_id);
        CREATE TABLE manifest_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    links: list[tuple] = []
    # per-court upper-bound: date-disagreeing shared-url pairs we did NOT link.
    band_unlinked = collections.Counter()

    def emit(canon, member, canton, rel, ev, conf, date_match=1):
        links.append((canon, member, canton, rel, ev, conf, date_match))

    # ---------- GE: shared source_url ----------
    by_url = collections.defaultdict(list)
    for r in src.execute(
        "SELECT source_url, decision_id, docket_number, decision_date, length(full_text) L "
        "FROM decisions WHERE court='ge_gerichte' AND source_url != ''"
    ):
        by_url[r["source_url"]].append(
            (r["decision_id"], r["docket_number"] or "", r["L"] or 0, r["decision_date"]))

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

    # ---------- VD: procedure-number cross-reference (unambiguous only) ----------
    vg = collections.defaultdict(list)  # normalized docket -> [vd_gerichte (id, date)]
    for r in src.execute(
        "SELECT decision_id, docket_number, decision_date FROM decisions "
        "WHERE court='vd_gerichte' AND docket_number != ''"
    ):
        vg[re.sub(r"\s+", "", r["docket_number"]).upper()].append((r["decision_id"], r["decision_date"]))
    # Restrict the match to the rubrum/header (first ~700 chars): a TWIN carries
    # the procedure number in its own case header, whereas a CITATION mentions it
    # deep in the reasoning.  A procedure token that maps to >1 vd_gerichte row is
    # AMBIGUOUS (interim + final ruling on one procedure) -> not linked, to avoid
    # collapsing two distinct rulings.
    vd_canon = set()
    vd_members = 0
    vd_ambiguous = 0
    seen_members = set()
    for r in src.execute(
        "SELECT decision_id, decision_date, substr(full_text, 1, 700) h FROM decisions WHERE court='vd_findinfo'"
    ):
        if r["decision_id"] in seen_members:
            continue
        for m in VD_TOKEN.finditer((r["h"] or "").upper()):
            cands = vg.get(m.group(0))
            if not cands:
                continue
            if len(cands) > 1:
                vd_ambiguous += 1
                break
            cn, cn_date = cands[0]
            dm = 1 if _dates_compatible(cn_date, r["decision_date"]) else 0
            if cn not in vd_canon:
                emit(cn, cn, "VD", "judgment", "procedure_cross_reference", 1.0)
                vd_canon.add(cn)
            emit(cn, r["decision_id"], "VD", "publication_page",
                 "procedure_cross_reference", 0.9, dm)
            seen_members.add(r["decision_id"])
            vd_members += 1
            break
    print(f"VD: {len(vd_canon):,} vd_gerichte linked, {vd_members:,} vd_findinfo members "
          f"({vd_ambiguous:,} ambiguous procedures skipped) {el()}", flush=True)

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

    # ---------- ch_vb: byte-identical (same source_url + content_hash) ----------
    chvb_members = 0
    groups = collections.defaultdict(list)
    for r in src.execute(
        "SELECT source_url, content_hash, decision_id, length(full_text) L "
        "FROM decisions WHERE court='ch_vb' AND source_url!='' "
        "AND content_hash IS NOT NULL AND content_hash!=''"
    ):
        groups[(r["source_url"], r["content_hash"])].append((r["decision_id"], r["L"] or 0))
    for key, rows in groups.items():
        if len(rows) < 2:
            continue
        canon = max(rows, key=lambda x: x[1])
        emit(canon[0], canon[0], "CH", "judgment", "byte_identical", 1.0)
        for x in rows:
            if x[0] != canon[0]:
                emit(canon[0], x[0], "CH", "duplicate_ingest", "byte_identical", 1.0)
                chvb_members += 1
    print(f"ch_vb: {chvb_members:,} byte-identical duplicate representations {el()}", flush=True)

    # ---------- nw / edoeb / ur: shared source_url, size-2, date-agree ----------
    for court in SHARED_URL_COURTS:
        canton = {"nw_gerichte": "NW", "edoeb": "CH", "ur_gerichte": "UR"}[court]
        by_u = collections.defaultdict(list)
        for r in src.execute(
            "SELECT source_url, decision_id, decision_date, length(full_text) L "
            "FROM decisions WHERE court=? AND source_url!=''", (court,)
        ):
            by_u[r["source_url"]].append((r["decision_id"], r["decision_date"], r["L"] or 0))
        linked = 0
        for url, rows in by_u.items():
            if len(rows) < 2:
                continue
            dates = [x[1] for x in rows]
            # link only when every row is date-compatible with the group (1905
            # sentinel treated as wildcard); otherwise hold out as upper band.
            ref = next((d for d in dates if d and d != DATE_SENTINEL), dates[0])
            if not all(_dates_compatible(ref, d) for d in dates):
                band_unlinked[court] += len(rows) - 1
                continue
            canon = max(rows, key=lambda x: x[2])
            emit(canon[0], canon[0], canton, "judgment", "shared_source_url", 1.0)
            for x in rows:
                if x[0] != canon[0]:
                    dm = 1 if x[1] == canon[1] else 0
                    emit(canon[0], x[0], canton, "alt_representation",
                         "shared_source_url", 0.9, dm)
                    linked += 1
        print(f"{court}: {linked:,} linked (held out {band_unlinked[court]:,} date-disagreeing) {el()}",
              flush=True)

    # ---------- write (atomic) ----------
    out.executemany(
        "INSERT OR IGNORE INTO decision_representations VALUES (?,?,?,?,?,?,?)", links)
    out.commit()

    inserted = out.execute("SELECT COUNT(*) FROM decision_representations").fetchone()[0]
    if inserted != len(links):
        # a member was claimed by two canonicals -> UNIQUE(member) dropped it.
        # For counting integrity this must never happen silently.
        raise SystemExit(
            f"FATAL: member-uniqueness violated: attempted {len(links):,} links, "
            f"inserted {inserted:,} ({len(links) - inserted:,} dropped). Investigate "
            f"before trusting the count.")
    # independent invariant: distinct members == rows
    n_rows, n_members = out.execute(
        "SELECT COUNT(*), COUNT(DISTINCT member_decision_id) FROM decision_representations").fetchone()
    assert n_rows == n_members, f"member-uniqueness broken: {n_rows} rows, {n_members} distinct members"

    total = src.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    src_gen = src.execute("PRAGMA user_version").fetchone()[0]
    reduction = out.execute(
        "SELECT COUNT(*) FROM decision_representations "
        "WHERE canonical_decision_id != member_decision_id"
    ).fetchone()[0]
    band = sum(band_unlinked.values())
    unique_pt = total - reduction
    unique_lo = total - reduction - band  # if every held-out pair were also a dupe

    per_method = {
        row[0]: row[1] for row in out.execute(
            "SELECT evidence_method, COUNT(*) FROM decision_representations "
            "WHERE canonical_decision_id != member_decision_id GROUP BY evidence_method")
    }
    meta = {
        "algo_version": ALGO_VERSION,
        "source_user_version": str(src_gen),
        "source_total_rows": str(total),
        "duplicate_representations": str(reduction),
        "band_unlinked_date_disagree": str(band),
        "estimated_unique_decisions": str(unique_pt),
        "estimated_unique_lower_bound": str(unique_lo),
        "reduction_by_method": json.dumps(per_method, sort_keys=True),
        "build_epoch": str(int(t0)),
    }
    out.executemany("INSERT INTO manifest_meta VALUES (?,?)", list(meta.items()))
    out.commit()
    out.close()
    src.close()

    os.replace(TMP, OUT)

    print(f"=== manifest rows: {len(links):,} | duplicate representations: {reduction:,} "
          f"| date-disagree band: {band:,} ===")
    print(f"=== corpus records: {total:,}  ->  unique decisions ~= {unique_pt:,} "
          f"(lower bound {unique_lo:,}) ===")
    print(f"=== reduction by method: {json.dumps(per_method, sort_keys=True)} ===")
    print(f"=== source_user_version={src_gen} algo={ALGO_VERSION} written: {OUT} {el()} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
