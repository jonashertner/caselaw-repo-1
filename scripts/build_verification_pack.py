#!/usr/bin/env python3
"""Build the verification pack: one SQLite file with everything the research CLI
needs to check citations, pinpoints and quotations offline.

    python scripts/build_verification_pack.py --decisions-db output/decisions.db \\
        --structure-db output/decision_structure.db --output output/dataset/verification_pack.sqlite

Contents (schema_version 2): decision metadata with the service's own citation
strings (built by mcp_server._build_citation_strings, so R1 holds offline),
docket aliases, canonical representations, every indexed Erwägung paragraph
with its text zlib-compressed per row, and a `courts` table (court, canton,
decisions, first_year, last_year; schema 2) grouped from the pack's own
decisions so the offline report can qualify "not in the corpus" by the court's
coverage. Clients tolerate packs without it. No full texts: the pack is
for verification, not for reading. Read-only on its inputs; writes a .tmp and
renames it. The pipeline runs it weekly (publish.py step 3b) and uploads the
gzip to the HuggingFace mirror; `ocl pack pull` fetches it. With --gzip the
script also writes <output>.gz.sha256 in sha256sum format ("<hex>  <name>"),
the sidecar `ocl pack pull` verifies the download against before unpacking.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 2
SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE courts (court TEXT, canton TEXT, decisions INTEGER, first_year TEXT, last_year TEXT, PRIMARY KEY (court, canton));
CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY, court TEXT, canton TEXT, language TEXT, decision_date TEXT,
    docket_number TEXT, docket_number_2 TEXT, citation_string_de TEXT, citation_string_fr TEXT,
    citation_string_it TEXT, canonical_url TEXT, source_url TEXT, content_hash TEXT,
    canonical_decision_id TEXT, has_full_text INTEGER
);
CREATE INDEX idx_pack_docket ON decisions(docket_number);
CREATE INDEX idx_pack_docket2 ON decisions(docket_number_2);
CREATE TABLE aliases (alias_docket_norm TEXT, canonical_decision_id TEXT);
CREATE INDEX idx_pack_alias ON aliases(alias_docket_norm);
CREATE TABLE paragraphs (decision_id TEXT, e_number TEXT, depth INTEGER, parent TEXT, text_z BLOB,
                         PRIMARY KEY (decision_id, e_number)) WITHOUT ROWID;
"""
META_COLUMNS = ("decision_id", "court", "canton", "language", "decision_date", "docket_number", "docket_number_2",
                "source_url", "content_hash", "collection", "bge_reference")


def _log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {message}", file=sys.stderr, flush=True)


def _citation_builder(repo_dir: Path):
    """mcp_server._build_citation_strings, imported from the checkout so the pack
    carries the service's own strings; a minimal fallback keeps the builder
    usable in tests without the server module."""
    try:
        sys.path.insert(0, str(repo_dir))
        os.environ.setdefault("SWISS_CASELAW_DIR", str(repo_dir / "output"))
        import mcp_server  # noqa: WPS433
        return mcp_server._build_citation_strings
    except Exception as exc:  # noqa: BLE001
        _log(f"mcp_server not importable ({exc}); citation strings will be absent")
        return None


def build(decisions_db: Path, structure_db: Path, output: Path, *, repo_dir: Path, manifest_db: Path | None = None,
          limit: int | None = None) -> dict:
    started = time.time()
    tmp = output.with_name(output.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    build_strings = _citation_builder(repo_dir)
    dec = sqlite3.connect(f"file:{decisions_db}?mode=ro&immutable=1", uri=True)
    dec.row_factory = sqlite3.Row
    columns = {r[1] for r in dec.execute("PRAGMA table_info(decisions)")}
    selected = [c for c in META_COLUMNS if c in columns]
    has_text = "full_text" in columns
    pack = sqlite3.connect(tmp)
    pack.executescript(SCHEMA)
    canonical: dict[str, str] = {}
    if manifest_db and manifest_db.exists():
        try:
            man = sqlite3.connect(f"file:{manifest_db}?mode=ro&immutable=1", uri=True)
            canonical = {r[0]: r[1] for r in man.execute(
                "SELECT member_decision_id, canonical_decision_id FROM decision_representations "
                "WHERE canonical_decision_id != member_decision_id")}
            man.close()
        except sqlite3.Error as exc:
            _log(f"representation manifest skipped: {exc}")
    query = "SELECT " + ", ".join(selected) + (", (full_text IS NOT NULL AND length(full_text) > 0) AS has_full_text" if has_text else ", 0 AS has_full_text") + " FROM decisions"
    if limit:
        query += f" LIMIT {int(limit)}"
    rows_out = 0
    batch = []
    for row in dec.execute(query):
        record = {k: row[k] for k in selected}
        strings = {}
        if build_strings is not None:
            try:
                strings = build_strings(dict(record))
            except Exception:  # noqa: BLE001
                strings = {}
        batch.append((record["decision_id"], record.get("court"), record.get("canton"), record.get("language"),
                      record.get("decision_date"), record.get("docket_number"), record.get("docket_number_2"),
                      strings.get("citation_string_de"), strings.get("citation_string_fr"), strings.get("citation_string_it"),
                      strings.get("canonical_url"), record.get("source_url"), record.get("content_hash"),
                      canonical.get(record["decision_id"], record["decision_id"]), int(row["has_full_text"] or 0)))
        if len(batch) >= 5000:
            pack.executemany("INSERT OR REPLACE INTO decisions VALUES (" + ",".join("?" * 15) + ")", batch)
            rows_out += len(batch); batch = []
            if rows_out % 100000 == 0:
                _log(f"decisions {rows_out:,}")
    if batch:
        pack.executemany("INSERT OR REPLACE INTO decisions VALUES (" + ",".join("?" * 15) + ")", batch)
        rows_out += len(batch)
    pack.commit()
    _log(f"decisions {rows_out:,} in {time.time() - started:.0f}s")
    # Per-court coverage, from the pack's own decisions table, so the offline report
    # can say what "not in the corpus" means for the court a reference names.
    pack.execute("INSERT OR REPLACE INTO courts SELECT court, canton, COUNT(*), "
                 "substr(MIN(decision_date), 1, 4), substr(MAX(decision_date), 1, 4) FROM decisions "
                 "WHERE court IS NOT NULL GROUP BY court, canton")
    pack.commit()
    _log(f"courts {pack.execute('SELECT COUNT(*) FROM courts').fetchone()[0]:,}")
    try:
        aliases = dec.execute("SELECT alias_docket_norm, canonical_decision_id FROM decision_docket_aliases").fetchall()
        pack.executemany("INSERT INTO aliases VALUES (?, ?)", [(r[0], r[1]) for r in aliases])
        _log(f"aliases {len(aliases):,}")
    except sqlite3.Error as exc:
        _log(f"aliases skipped: {exc}")
    dec.close()
    st = sqlite3.connect(f"file:{structure_db}?mode=ro&immutable=1", uri=True)
    paragraphs = 0
    batch = []
    cursor = st.execute("SELECT decision_id, e_number, depth, parent, text FROM erwaegungen_paragraph" + (f" LIMIT {int(limit) * 20}" if limit else ""))
    for decision_id, e_number, depth, parent, text in cursor:
        if not isinstance(text, str) or not text:
            continue
        batch.append((decision_id, str(e_number), int(depth or 0), parent, zlib.compress(text.encode("utf-8"), 6)))
        if len(batch) >= 5000:
            pack.executemany("INSERT OR REPLACE INTO paragraphs VALUES (?,?,?,?,?)", batch)
            paragraphs += len(batch); batch = []
            if paragraphs % 500000 == 0:
                pack.commit()
                _log(f"paragraphs {paragraphs:,}")
    if batch:
        pack.executemany("INSERT OR REPLACE INTO paragraphs VALUES (?,?,?,?,?)", batch)
        paragraphs += len(batch)
    st.close()
    generation = ""
    try:
        health = json.loads((repo_dir / "output" / "stats.json").read_text(encoding="utf-8")) if (repo_dir / "output" / "stats.json").exists() else {}
        generation = str(health.get("db_generation") or "")
    except (OSError, ValueError):
        pass
    if not generation:
        generation = str(int(decisions_db.stat().st_mtime))
    meta = {"schema_version": str(SCHEMA_VERSION), "built_at": datetime.now(timezone.utc).isoformat(), "db_generation": generation,
            "decisions": str(rows_out), "paragraphs": str(paragraphs), "source": "OpenCaseLaw verification pack; citation strings by the service; CC0 data"}
    pack.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    pack.commit()
    pack.execute("VACUUM")
    pack.close()
    os.replace(tmp, output)
    meta["bytes"] = output.stat().st_size
    meta["seconds"] = round(time.time() - started)
    _log(f"pack {output} {meta['bytes'] / 1e9:.2f} GB in {meta['seconds']}s")
    return meta


class _HashingWriter:
    """Forwards writes to a file and feeds the same bytes to a digest, so the gzip is hashed as it is written."""

    def __init__(self, fp, digest):
        self._fp, self._digest = fp, digest

    def write(self, data):
        self._digest.update(data)
        return self._fp.write(data)

    def flush(self):
        self._fp.flush()


def gzip_file(path: Path) -> tuple[Path, str]:
    """Write <path>.gz (level 6); returns the gzip path and the sha256 hex digest of its bytes."""
    target = path.with_name(path.name + ".gz")
    tmp = target.with_name(target.name + ".tmp")
    digest = hashlib.sha256()
    with open(path, "rb") as src, open(tmp, "wb") as raw, gzip.GzipFile(filename=target.name, mode="wb", compresslevel=6,
                                                                       fileobj=_HashingWriter(raw, digest)) as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    os.replace(tmp, target)
    return target, digest.hexdigest()


def write_sha256(gz: Path, digest: str) -> Path:
    """<gz>.sha256 in sha256sum format, so `sha256sum -c <gz>.sha256` and `ocl pack pull` both verify it."""
    sidecar = gz.with_name(gz.name + ".sha256")
    tmp = sidecar.with_name(sidecar.name + ".tmp")
    tmp.write_text(f"{digest}  {gz.name}\n", encoding="utf-8")
    os.replace(tmp, sidecar)
    return sidecar


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--decisions-db", type=Path, required=True)
    ap.add_argument("--structure-db", type=Path, required=True)
    ap.add_argument("--manifest-db", type=Path, default=None, help="representation_manifest.db (optional)")
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--repo-dir", type=Path, default=Path(__file__).resolve().parents[1])
    ap.add_argument("--gzip", action="store_true", help="also write <output>.gz and <output>.gz.sha256 (sha256sum format) for upload")
    ap.add_argument("--limit", type=int, default=None, help="build a small pack (tests)")
    args = ap.parse_args()
    meta = build(args.decisions_db, args.structure_db, args.output, repo_dir=args.repo_dir, manifest_db=args.manifest_db, limit=args.limit)
    if args.gzip:
        gz, digest = gzip_file(args.output)
        sidecar = write_sha256(gz, digest)
        meta["gzip_bytes"] = gz.stat().st_size
        meta["gzip_sha256"] = digest
        meta["sha256_file"] = str(sidecar)
        _log(f"gzip {gz} {meta['gzip_bytes'] / 1e9:.2f} GB ({meta['gzip_bytes']:,} bytes) sha256 {digest} -> {sidecar.name}")
    print(json.dumps(meta, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
