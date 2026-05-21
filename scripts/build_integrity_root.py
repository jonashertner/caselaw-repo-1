#!/usr/bin/env python3
"""
Compute the daily Merkle root over the full Swiss caselaw corpus.

Reads the FTS5 ``decisions`` table, computes a leaf hash per decision
that commits to (decision_id, cli:ch, ECLI, content_hash, decision_date),
builds the RFC 6962 Merkle tree, and writes the root + manifest to
``docs/integrity/<YYYY-MM-DD>.{root,json}``.

Optionally stamps the root file via OpenTimestamps when the ``ots`` CLI
is available (``pip install opentimestamps-client``), producing a
``.root.ots`` proof anchored to Bitcoin in ~1 block.

This script is part of Bestimmung 06 of the Open Law Standards
proposal at https://opencaselaw.ch/standards/.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from integrity import (
    canonical_leaf,
    leaf_hash,
    merkle_root,
    hex_root,
)
from cli_ch import mint_cli_ch
from ecli import mint_ecli


def _default_db_path() -> Path:
    """Resolve the FTS5 DB path used by the publish pipeline."""
    env_dir = os.environ.get("SWISS_CASELAW_DIR")
    if env_dir:
        return Path(env_dir) / "decisions.db"
    # Local dev fallback (also works on VPS where this exists as a symlink).
    return REPO_ROOT / "output" / "decisions.db"


def _resolve_decisions_table(cur: sqlite3.Cursor) -> str:
    """Find the canonical decisions table — usually 'decisions'."""
    for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='decisions'"
    ).fetchall():
        return r[0]
    raise SystemExit("error: no 'decisions' table found in DB")


def _required_columns(cur: sqlite3.Cursor, table: str) -> dict[str, bool]:
    """Return which expected columns exist on the decisions table."""
    cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    required = ["decision_id", "court", "docket_number",
                "decision_date", "content_hash"]
    missing = [c for c in required if c not in cols]
    if missing:
        raise SystemExit(f"error: decisions table missing columns: {missing}")
    return {c: True for c in required}


def build_root(db_path: Path) -> dict:
    """Compute the Merkle root over all decisions. Returns a manifest dict."""
    started_at = dt.datetime.now(dt.timezone.utc)
    t0 = time.monotonic()

    uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    table = _resolve_decisions_table(cur)
    _required_columns(cur, table)

    # Deterministic order: by decision_id ascending.
    rows = cur.execute(
        f"SELECT decision_id, court, docket_number, decision_date, content_hash "
        f"FROM {table} ORDER BY decision_id"
    )

    leaves: list[bytes] = []
    # Also collect (decision_id, leaf_hash) in the same order so we can
    # persist a queryable index for the inclusion-proof API endpoint.
    leaf_index: list[tuple[str, bytes]] = []
    first_id: str | None = None
    last_id: str | None = None
    skipped_no_content_hash = 0
    n = 0
    for row in rows:
        did = row["decision_id"]
        if not did:
            continue
        if first_id is None:
            first_id = did
        last_id = did

        ch = row["content_hash"]
        if not ch:
            # Skip rows without a content hash — they aren't yet hashable.
            # The publish pipeline computes content_hash before this step
            # so this should be 0 in practice.
            skipped_no_content_hash += 1
            continue

        court = row["court"] or ""
        docket = row["docket_number"] or ""
        dec_date = row["decision_date"] or ""

        cli_ch_uri = mint_cli_ch(decision_id=did, court=court,
                                 docket_number=docket)
        ecli_uri = mint_ecli(decision_id=did, court=court,
                             docket_number=docket,
                             decision_date=dec_date or None)

        leaf_bytes = canonical_leaf(did, cli_ch_uri, ecli_uri, ch, dec_date)
        lh = leaf_hash(leaf_bytes)
        leaves.append(lh)
        leaf_index.append((did, lh))
        n += 1

    t_iter = time.monotonic() - t0
    print(f"  iterated {n} decisions in {t_iter:.1f}s "
          f"(skipped {skipped_no_content_hash} without content_hash)")

    t1 = time.monotonic()
    root = merkle_root(leaves)
    t_merkle = time.monotonic() - t1
    print(f"  merkle root computed in {t_merkle:.1f}s")

    finished_at = dt.datetime.now(dt.timezone.utc)
    return {
        "leaf_index": leaf_index,  # consumed by write_leaves_db; popped before serializing manifest
        "date": finished_at.strftime("%Y-%m-%d"),
        "root": hex_root(root),
        "algorithm": "RFC6962-SHA256",
        "leaf_encoding": "decision_id\\ncli_ch\\necli\\ncontent_hash\\ndecision_date",
        "decisions_count": n,
        "skipped_no_content_hash": skipped_no_content_hash,
        "first_decision_id": first_id,
        "last_decision_id": last_id,
        "build_started_at": started_at.isoformat(),
        "build_finished_at": finished_at.isoformat(),
        "build_duration_s": round(time.monotonic() - t0, 1),
        "iter_duration_s": round(t_iter, 1),
        "merkle_duration_s": round(t_merkle, 1),
    }


def _ots_stamp(root_file: Path) -> str | None:
    """Stamp the root file via OpenTimestamps. Returns the .ots filename
    if successful, None if ``ots`` CLI is not available."""
    if not shutil.which("ots"):
        print("  ots CLI not installed — skipping OpenTimestamps anchor.")
        print("  install: pip install opentimestamps-client")
        return None
    try:
        result = subprocess.run(
            ["ots", "stamp", str(root_file)],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            print(f"  ots stamp failed: {result.stderr.strip()}")
            return None
        ots_file = root_file.with_suffix(root_file.suffix + ".ots")
        if ots_file.exists():
            print(f"  ots anchor written: {ots_file.name}")
            return ots_file.name
        return None
    except (subprocess.TimeoutExpired, OSError) as e:
        print(f"  ots stamp error: {e}")
        return None


def write_leaves_db(leaf_index: list[tuple[str, bytes]], db_path: Path) -> None:
    """Write the (decision_id → idx, leaf_hash) mapping to a SQLite file.

    Used by the /api/integrity/<decision_id> endpoint to look up a leaf
    in O(1) and then compute the inclusion proof. Indexed on decision_id
    for fast point lookups; idx is the primary key so iteration over leaf
    order is sequential.

    ~80 MB for 972k decisions; lives outside docs/ so it doesn't bloat git.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Replace atomically — write to .tmp, rename.
    tmp_path = db_path.with_suffix(db_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    conn = sqlite3.connect(tmp_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE leaves (
            idx          INTEGER PRIMARY KEY,
            decision_id  TEXT NOT NULL,
            leaf_hash    BLOB NOT NULL
        )
    """)
    cur.execute("CREATE UNIQUE INDEX idx_decision_id ON leaves(decision_id)")
    cur.executemany(
        "INSERT INTO leaves(idx, decision_id, leaf_hash) VALUES (?, ?, ?)",
        ((i, did, lh) for i, (did, lh) in enumerate(leaf_index)),
    )
    conn.commit()
    cur.execute("PRAGMA optimize")
    conn.close()
    if db_path.exists():
        db_path.unlink()
    tmp_path.rename(db_path)
    print(f"  wrote {db_path} ({len(leaf_index)} rows)")


def write_outputs(manifest: dict, out_dir: Path) -> Path:
    """Write the root file + manifest JSON + optional OTS proof.

    Returns the path of the root file (for OTS to anchor)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    date = manifest["date"]

    root_file = out_dir / f"{date}.root"
    root_file.write_text(manifest["root"] + "\n")
    print(f"  wrote {root_file}")

    json_file = out_dir / f"{date}.json"
    json_file.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"  wrote {json_file}")

    # Convenience: latest.json + latest.root pointers.
    latest_root = out_dir / "latest.root"
    latest_root.write_text(manifest["root"] + "\n")
    latest_json = out_dir / "latest.json"
    latest_json.write_text(json.dumps(manifest, indent=2) + "\n")

    return root_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=None,
                        help="Path to the FTS5 decisions.db "
                             "(default: $SWISS_CASELAW_DIR/decisions.db)")
    parser.add_argument("--out-dir", type=Path,
                        default=REPO_ROOT / "docs" / "integrity",
                        help="Output directory (default: docs/integrity/)")
    parser.add_argument("--leaves-dir", type=Path,
                        default=REPO_ROOT / "output" / "integrity",
                        help="Where to write <date>.leaves.db (default: output/integrity/, not in git)")
    parser.add_argument("--no-ots", action="store_true",
                        help="Skip OpenTimestamps anchoring")
    parser.add_argument("--no-leaves-db", action="store_true",
                        help="Skip writing the leaves SQLite index")
    parser.add_argument("--leaves-only", action="store_true",
                        help="Only write the leaves DB; don't touch root/manifest/ots")
    args = parser.parse_args()

    db_path = args.db or _default_db_path()
    if not db_path.exists():
        raise SystemExit(f"error: DB not found at {db_path}")

    print(f"computing Merkle root over {db_path}")
    manifest = build_root(db_path)
    print(f"  root: {manifest['root']}")

    # Pop the leaf_index off the manifest before it's serialised to JSON.
    leaf_index = manifest.pop("leaf_index")
    if not args.no_leaves_db:
        leaves_db = args.leaves_dir / f"{manifest['date']}.leaves.db"
        write_leaves_db(leaf_index, leaves_db)
        # Symlink (or copy) latest.leaves.db → today's file so the API
        # endpoint can always find the freshest index.
        latest_db = args.leaves_dir / "latest.leaves.db"
        if latest_db.exists() or latest_db.is_symlink():
            latest_db.unlink()
        try:
            latest_db.symlink_to(leaves_db.name)
        except OSError:
            # Fall back to copy on filesystems that don't support symlinks.
            import shutil as _sh
            _sh.copy(leaves_db, latest_db)

    if args.leaves_only:
        print("done (leaves-only).")
        return

    root_file = write_outputs(manifest, args.out_dir)

    if not args.no_ots:
        ots_name = _ots_stamp(root_file)
        if ots_name:
            manifest["ots_stamp"] = ots_name
            # Re-write the manifest with the stamp filename recorded.
            (args.out_dir / f"{manifest['date']}.json").write_text(
                json.dumps(manifest, indent=2) + "\n")
            (args.out_dir / "latest.json").write_text(
                json.dumps(manifest, indent=2) + "\n")

    print("done.")


if __name__ == "__main__":
    main()
