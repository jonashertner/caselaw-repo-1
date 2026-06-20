"""Header-scan refinement of the citation-gap 'missing' set (Completeness Step 1).

A 'missing' ref is one not found via decision_id / docket fields / the 600-char
BGer-only BGE cross-reference. Some are not truly missing — they are present
under an ALTERNATE identity whose header (Urteilskopf) declares the cited docket:
notably a leading case stored under its BGE number whose header names the
underlying BGer docket. This pass builds a header-docket index over EVERY
decision (wider window + all docket formats) and reclassifies:

  present_alt_id  — the docket appears in some decision's header → present, not a gap
  truly_missing   — not in any header → a genuine, uncollected gap

Read-only; reuses the oracle's normalize_ref so keys match. Only `substr(full_text,1,N)`
is read, so the scan is cheap despite the large full_text column.
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from citation_gap_oracle import normalize_ref  # noqa: E402

HEADER_WINDOW = 500

# A decision's OWN docket in its Urteilskopf is followed by a "decided-on"
# marker: "4A_576/2024 vom 29. April 2025" (de) / "du 7 mai 2025" (fr) /
# "del 7 maggio 2025" (it). Requiring that marker is what distinguishes the
# decision's identity (incl. the underlying BGer docket of a BGE) from a docket
# merely CITED in the header/Regeste — without it the scan over-counts citations
# as alternate-identity matches. The capture group is the docket.
_VOM = r"(?:vom|du|del|della|dell['’]?)\s+\d"
_OWN_DOCKET_RES = [
    re.compile(rf"(\d{{1,2}}[A-Za-z]{{0,2}}[._/ -]\d{{1,5}}[._/ -]\d{{4}})\s+{_VOM}", re.IGNORECASE),  # BGer
    re.compile(rf"([A-Fa-f][._/ -]\d{{1,5}}[._/ -]\d{{4}})\s+{_VOM}", re.IGNORECASE),                  # BVGer
    re.compile(rf"([A-Za-z]{{2}}[._]\d{{4}}[._]\d{{1,5}})\s+{_VOM}", re.IGNORECASE),                   # BStGer/cantonal
]


def extract_header_dockets(text: str | None, window: int = HEADER_WINDOW) -> set[str]:
    """Keys for the docket(s) a decision declares as ITS OWN in the header —
    those immediately followed by a 'vom/du/del DATE' marker. Citations in the
    header are excluded (they lack the marker)."""
    if not text:
        return set()
    head = text[:window]
    keys: set[str] = set()
    for rx in _OWN_DOCKET_RES:
        for m in rx.findall(head):
            k = normalize_ref(m)
            if k:
                keys.add(k)
    return keys


def build_header_index(decisions: sqlite3.Connection, window: int = HEADER_WINDOW) -> dict[str, str]:
    """header-docket key → decision_id, over every decision (own/underlying dockets)."""
    idx: dict[str, str] = {}
    for did, head in decisions.execute(
        f"SELECT decision_id, substr(full_text,1,{int(window)}) FROM decisions "
        "WHERE full_text IS NOT NULL"
    ):
        for k in extract_header_dockets(head, window):
            idx.setdefault(k, did)
    return idx


def refine(gaps: sqlite3.Connection, header_index: dict[str, str]) -> dict:
    """Split 'missing' into present_alt_id vs truly_missing using the index."""
    rows = gaps.execute(
        "SELECT target_ref, citation_count, normalized_key FROM citation_gaps "
        "WHERE classification='missing'"
    ).fetchall()
    present = 0
    truly = 0
    present_cites = 0
    examples = []
    for ref, c, key in rows:
        if key and key in header_index:
            present += 1
            present_cites += c
            if len(examples) < 8:
                examples.append((ref, header_index[key]))
        else:
            truly += 1
    return {
        "missing_in": len(rows),
        "present_alt_id": present,
        "present_alt_id_citations": present_cites,
        "truly_missing": truly,
        "examples": examples,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    base = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
    p.add_argument("--decisions", type=Path, default=base / "decisions.db")
    p.add_argument("--gaps", type=Path, default=base / "citation_gaps.db")
    p.add_argument("--window", type=int, default=HEADER_WINDOW)
    args = p.parse_args()

    d = sqlite3.connect(f"file:{args.decisions}?mode=ro&immutable=1", uri=True)
    idx = build_header_index(d, args.window)
    d.close()
    print(f"header-docket index: {len(idx):,} keys (window={args.window})", file=sys.stderr)

    g = sqlite3.connect(f"file:{args.gaps}?mode=ro", uri=True)
    r = refine(g, idx)
    g.close()
    print("=== header-scan refinement of 'missing' ===")
    print(f"  missing (oracle):        {r['missing_in']:,}")
    print(f"  present under alt ID:    {r['present_alt_id']:,}  ({r['present_alt_id_citations']:,} citations)")
    print(f"  TRULY MISSING:           {r['truly_missing']:,}")
    for ref, did in r["examples"]:
        print(f"      alt-present: {ref:16} -> {did}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
