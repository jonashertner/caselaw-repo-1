"""Daily delta publisher — ports the old private-repo pipeline into
caselaw-repo-1 so the same process that produces the FTS5 DB also emits
the incremental delta artifacts that consumers (Adrian et al.) already
wired up against `voilaj/swiss-caselaw`.

What this writes to HF `voilaj/swiss-caselaw` (identical paths to the
previous pipeline — zero consumer breakage):

  artifacts/sqlite/deltas/{date}.sqlite.zst   ← daily delta, zstd-compressed
  artifacts/parquet/deltas/{date}.parquet     ← same data, parquet (columns)
  data/delta-{date}.parquet                   ← append-form for load_dataset
  artifacts/manifest.json                     ← append this delta entry

Source of truth: the FTS5 decisions.db on the VPS.

Delta semantics: we detect "new in our corpus since last publish" via
a snapshot of all decision_ids stored locally. Today's delta = current
FTS5 ids − yesterday's snapshot ids. Robust to backfills, scraper gap-
fills, and `scraped_at` backdating. Avoids any reliance on a date
filter over the 62 GB FTS5 DB (which has no scraped_at index).

Why we diverge from the old private-repo schema in one place:
the old `id` column was a PostgreSQL UUID. Consumers (per Adrian)
compute their own decision_id via `make_decision_id(court, docket)`.
Here we set `id = decision_id` directly (e.g. "bger_4A_747_2012"),
which IS Adrian's expected decision_id format. Consumers can read `id`
unchanged and skip the make_decision_id step entirely.
"""
from __future__ import annotations

import datetime as _dt
import hashlib as _hashlib
import json as _json
import logging as _logging
import os as _os
import shutil as _shutil
import sqlite3 as _sqlite3
from pathlib import Path as _Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

log = _logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────

HF_REPO_ID = "voilaj/swiss-caselaw"
MANIFEST_PATH_IN_REPO = "artifacts/manifest.json"
MANIFEST_SCHEMA = "swiss-caselaw-artifacts-v1"

# Courts classified as federal-level for the `level` column.
FEDERAL_COURTS = frozenset({
    "bger", "bge", "bvger", "bstger", "bpatger", "mkg",
    "finma", "finma_versicherungsrecht",
    "weko", "edoeb", "ubi", "elcom", "postcom", "comcom",
    "ch_bundesrat", "ch_vb",
    "ta_sst", "bge_egmr", "hudoc_ch", "emark", "bge_historical",
})

# Columns of the delta SQLite — preserved from the old pipeline so
# existing consumers see an identical schema.
DECISION_COLS: List[str] = [
    "id", "source_id", "source_name", "level", "canton", "court", "chamber",
    "language", "docket", "decision_date", "published_date", "title",
    "url", "pdf_url", "content_text", "content_sha256",
    "fetched_at", "updated_at",
]

# Columns written to data/delta-{date}.parquet (subset for load_dataset())
BASE_COLS: List[str] = [
    "id", "source_id", "source_name", "level", "canton", "court", "chamber",
    "docket", "decision_date", "published_date", "title", "language",
    "url", "pdf_url", "content_text",
]


# ── Helpers ─────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def _sha256_text(s: str) -> str:
    return _hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()


def _sha256_file(path: _Path) -> str:
    h = _hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _classify_level(court: str | None, canton: str | None) -> str:
    c = (court or "").lower()
    if c in FEDERAL_COURTS:
        return "federal"
    canton_norm = (canton or "").upper()
    if canton_norm == "CH" or canton_norm == "":
        # Fall-through: unknown court with no canton → assume federal
        return "federal" if not canton_norm else "cantonal"
    return "cantonal"


def _normalize_canton(canton: str | None) -> str | None:
    """Old pipeline used NULL for federal-scope decisions; preserve that."""
    if not canton or canton.upper() == "CH":
        return None
    return canton.upper()


def _row_to_decision(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map our FTS5 row → delta-schema dict."""
    court = row.get("court") or ""
    canton = _normalize_canton(row.get("canton"))
    content_text = row.get("full_text") or ""
    title = row.get("title") or ""
    docket = row.get("docket_number") or ""
    content_sha = row.get("content_hash") or _sha256_text(f"{content_text}\n{title}\n{docket}")
    scraped_at = row.get("scraped_at") or _utc_now_iso()
    return {
        "id": row.get("decision_id") or "",
        "source_id": row.get("source_id") or "",
        "source_name": row.get("source") or row.get("source_spider") or "",
        "level": _classify_level(court, row.get("canton")),
        "canton": canton,
        "court": court,
        "chamber": row.get("chamber") or "",
        "language": row.get("language") or "",
        "docket": docket,
        "decision_date": row.get("decision_date") or "",
        "published_date": row.get("publication_date") or "",
        "title": title,
        "url": row.get("source_url") or "",
        "pdf_url": row.get("pdf_url") or "",
        "content_text": content_text,
        "content_sha256": content_sha,
        "fetched_at": scraped_at,
        "updated_at": scraped_at,
    }


# ── FTS5 source ─────────────────────────────────────────────────────────

def snapshot_all_ids(db_path: _Path) -> set[str]:
    """Return every decision_id currently in the FTS5 DB. O(N) scan, cheap —
    a ~1M-row SELECT on an immutable DB is a single sequential pass."""
    uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = _sqlite3.connect(uri, uri=True)
    try:
        cur = conn.execute("SELECT decision_id FROM decisions")
        return {row[0] for row in cur if row[0]}
    finally:
        conn.close()


def iter_decisions_by_ids(db_path: _Path, ids: Iterable[str], chunk: int = 500) -> Iterator[Dict[str, Any]]:
    """Stream full decision rows for the given decision_ids. Chunks the IN
    clause to keep SQLite parameter count sane."""
    id_list = list(ids)
    if not id_list:
        return
    uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = _sqlite3.connect(uri, uri=True)
    conn.row_factory = _sqlite3.Row
    try:
        for i in range(0, len(id_list), chunk):
            batch_ids = id_list[i : i + chunk]
            placeholders = ",".join(["?"] * len(batch_ids))
            cur = conn.execute(
                f"""
                SELECT decision_id, court, canton, chamber, docket_number, docket_number_2,
                       decision_date, publication_date, language, title, regeste,
                       full_text, source_url, pdf_url, scraped_at, source, source_id,
                       source_spider, content_hash
                FROM decisions WHERE decision_id IN ({placeholders})
                """,
                batch_ids,
            )
            for row in cur:
                yield _row_to_decision(dict(row))
    finally:
        conn.close()


def load_snapshot_ids(snapshot_path: _Path) -> set[str]:
    """Load previously-seen decision_ids from a local state file.
    Returns empty set if file doesn't exist (first-run behavior)."""
    if not snapshot_path.exists():
        return set()
    try:
        data = _json.loads(snapshot_path.read_text(encoding="utf-8"))
        return set(data.get("ids", []))
    except Exception as e:
        log.warning("snapshot load failed (%s); treating as empty", e)
        return set()


def save_snapshot_ids(snapshot_path: _Path, ids: set[str], date: str) -> None:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = snapshot_path.with_suffix(".tmp")
    tmp.write_text(
        _json.dumps({"saved_at": _utc_now_iso(), "date": date, "count": len(ids),
                     "ids": sorted(ids)}, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(snapshot_path)


# ── Delta SQLite build ──────────────────────────────────────────────────

def _create_delta_db(path: _Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = _sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
              id TEXT PRIMARY KEY,
              source_id TEXT,
              source_name TEXT,
              level TEXT,
              canton TEXT,
              court TEXT,
              chamber TEXT,
              language TEXT,
              docket TEXT,
              decision_date TEXT,
              published_date TEXT,
              title TEXT,
              url TEXT,
              pdf_url TEXT,
              content_text TEXT,
              content_sha256 TEXT,
              fetched_at TEXT,
              updated_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delta_decision_date ON decisions(decision_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delta_court ON decisions(court)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delta_level ON decisions(level)")
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
    finally:
        conn.close()


def _bulk_insert(delta_db: _Path, decisions: Iterable[Dict[str, Any]], batch: int = 2000) -> int:
    conn = _sqlite3.connect(str(delta_db))
    inserted = 0
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        placeholders = ",".join(["?"] * len(DECISION_COLS))
        sql = f"INSERT OR REPLACE INTO decisions ({','.join(DECISION_COLS)}) VALUES ({placeholders})"
        buf: List[List[Any]] = []
        for d in decisions:
            buf.append([d.get(c) for c in DECISION_COLS])
            if len(buf) >= batch:
                conn.executemany(sql, buf)
                inserted += len(buf)
                buf.clear()
        if buf:
            conn.executemany(sql, buf)
            inserted += len(buf)
        conn.commit()
    finally:
        conn.close()
    return inserted


# ── Parquet export ──────────────────────────────────────────────────────

def _export_parquet(delta_db: _Path, out_path: _Path, columns: List[str] | None = None) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    cols = columns or DECISION_COLS
    schema = pa.schema([pa.field(c, pa.string()) for c in cols])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = _sqlite3.connect(str(delta_db))
    conn.row_factory = _sqlite3.Row
    writer = pq.ParquetWriter(str(out_path), schema=schema, compression="zstd", use_dictionary=True)
    total = 0
    try:
        col_list = ",".join(cols)
        cur = conn.execute(f"SELECT {col_list} FROM decisions ORDER BY decision_date, id")
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            batch = {c: [] for c in cols}
            for r in rows:
                for c in cols:
                    batch[c].append(r[c])
            table = pa.Table.from_pydict(batch, schema=schema)
            writer.write_table(table)
            total += table.num_rows
    finally:
        writer.close()
        conn.close()
    return total


# ── Zstd compression ────────────────────────────────────────────────────

def _compress_zst(src: _Path, dst: _Path, level: int = 10) -> None:
    import zstandard as zstd
    dst.parent.mkdir(parents=True, exist_ok=True)
    cctx = zstd.ZstdCompressor(level=level, threads=-1)
    with src.open("rb") as ifh, dst.open("wb") as ofh:
        cctx.copy_stream(ifh, ofh)


# ── Manifest ────────────────────────────────────────────────────────────

def _empty_manifest() -> Dict[str, Any]:
    return {
        "schema": MANIFEST_SCHEMA,
        "generated_at": _utc_now_iso(),
        "snapshot": None,
        "deltas": [],
    }


def _add_delta_to_manifest(m: Dict[str, Any], date: str, sqlite_zst: Dict, parquet: Optional[Dict]) -> Dict[str, Any]:
    m = dict(m)
    m["schema"] = MANIFEST_SCHEMA
    m["generated_at"] = _utc_now_iso()
    entries = [e for e in (m.get("deltas") or []) if e.get("date") != date]
    entries.append({"date": date, "sqlite_zst": sqlite_zst, "parquet": parquet})
    entries.sort(key=lambda e: e.get("date") or "")
    m["deltas"] = entries
    return m


def _download_manifest(hf_repo: str) -> Dict[str, Any]:
    import httpx
    url = f"https://huggingface.co/datasets/{hf_repo}/resolve/main/{MANIFEST_PATH_IN_REPO}"
    try:
        with httpx.Client(follow_redirects=True, timeout=60.0) as client:
            r = client.get(url)
            if r.status_code == 404:
                return _empty_manifest()
            r.raise_for_status()
            return _json.loads(r.text)
    except Exception as e:
        log.warning("manifest download failed (%s); starting fresh", e)
        return _empty_manifest()


# ── HF upload ───────────────────────────────────────────────────────────

def _upload_file(local: _Path, repo: str, path_in_repo: str, token: str, commit_message: str) -> None:
    from huggingface_hub import HfApi
    api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=str(local),
        path_in_repo=path_in_repo,
        repo_id=repo,
        repo_type="dataset",
        commit_message=commit_message,
    )


# ── Top-level build + publish ───────────────────────────────────────────

def build_delta(
    *,
    db_path: _Path,
    date: str,
    build_dir: _Path,
    snapshot_path: _Path,
    zstd_level: int = 10,
    update_snapshot: bool = True,
) -> Dict[str, Any]:
    """Build delta artifacts for `date` from the FTS5 `db_path` via
    snapshot-diff. After publishing, the current id-set becomes the new
    snapshot (unless update_snapshot=False, useful for dry-runs).

    Returns dict with local paths + row count.
    """
    work = build_dir / "delta" / date
    if work.exists():
        _shutil.rmtree(work)
    work.mkdir(parents=True)

    delta_sqlite = work / f"delta-{date}.sqlite"
    _create_delta_db(delta_sqlite)

    log.info("build_delta: scanning FTS5 decision_ids")
    current_ids = snapshot_all_ids(db_path)
    prev_ids = load_snapshot_ids(snapshot_path)
    new_ids = current_ids - prev_ids
    log.info("build_delta: current=%d prev=%d new=%d", len(current_ids), len(prev_ids), len(new_ids))

    log.info("build_delta: fetching %d new decisions", len(new_ids))
    row_count = _bulk_insert(delta_sqlite, iter_decisions_by_ids(db_path, new_ids))
    log.info("build_delta: %d rows inserted", row_count)

    delta_zst = work / f"delta-{date}.sqlite.zst"
    _compress_zst(delta_sqlite, delta_zst, level=zstd_level)

    delta_parquet = work / f"delta-{date}.parquet"
    pq_rows = _export_parquet(delta_sqlite, delta_parquet)

    data_parquet = work / f"delta-{date}-data.parquet"
    _export_parquet(delta_sqlite, data_parquet, columns=BASE_COLS)

    return {
        "date": date,
        "rows": row_count,
        "sqlite_zst": delta_zst,
        "sqlite_zst_bytes": delta_zst.stat().st_size,
        "sqlite_zst_sha256": _sha256_file(delta_zst),
        "parquet": delta_parquet,
        "parquet_bytes": delta_parquet.stat().st_size,
        "parquet_sha256": _sha256_file(delta_parquet),
        "data_parquet": data_parquet,
        "data_parquet_bytes": data_parquet.stat().st_size,
        "work_dir": work,
        "current_ids": current_ids,
        "snapshot_path": snapshot_path,
        "update_snapshot": update_snapshot,
    }


def publish_delta(
    *,
    build_info: Dict[str, Any],
    hf_repo: str = HF_REPO_ID,
    hf_token: str | None = None,
    dry_run: bool = False,
) -> None:
    """Upload delta artifacts + update manifest. When dry_run, only logs."""
    date = build_info["date"]
    hf_token = hf_token or _os.environ.get("HF_TOKEN")
    if not hf_token and not dry_run:
        raise RuntimeError("HF_TOKEN not set")

    sqlite_path_in_repo = f"artifacts/sqlite/deltas/{date}.sqlite.zst"
    parquet_path_in_repo = f"artifacts/parquet/deltas/{date}.parquet"
    data_path_in_repo = f"data/delta-{date}.parquet"

    if build_info["rows"] == 0:
        log.info("publish_delta: 0 rows for %s — nothing to upload", date)
        return

    if dry_run:
        log.info("[dry-run] would upload:")
        log.info("  → %s (%d bytes)", sqlite_path_in_repo, build_info["sqlite_zst_bytes"])
        log.info("  → %s (%d bytes)", parquet_path_in_repo, build_info["parquet_bytes"])
        log.info("  → %s (%d bytes)", data_path_in_repo, build_info["data_parquet_bytes"])
        log.info("  → artifacts/manifest.json (add delta entry)")
        return

    # Upload artifacts in dependency order (manifest LAST so consumers never
    # see a manifest referencing files that haven't been uploaded yet).
    _upload_file(build_info["sqlite_zst"], hf_repo, sqlite_path_in_repo, hf_token,
                 commit_message=f"delta {date} (sqlite)")
    _upload_file(build_info["parquet"], hf_repo, parquet_path_in_repo, hf_token,
                 commit_message=f"delta {date} (parquet)")
    _upload_file(build_info["data_parquet"], hf_repo, data_path_in_repo, hf_token,
                 commit_message=f"daily delta {date}")

    manifest = _download_manifest(hf_repo)
    sqlite_meta = {
        "path": sqlite_path_in_repo,
        "sha256": build_info["sqlite_zst_sha256"],
        "bytes": build_info["sqlite_zst_bytes"],
    }
    parquet_meta = {
        "path": parquet_path_in_repo,
        "sha256": build_info["parquet_sha256"],
        "bytes": build_info["parquet_bytes"],
    }
    manifest = _add_delta_to_manifest(manifest, date, sqlite_meta, parquet_meta)
    manifest_local = build_info["work_dir"] / "manifest.json"
    manifest_local.write_text(_json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                              encoding="utf-8")
    _upload_file(manifest_local, hf_repo, MANIFEST_PATH_IN_REPO, hf_token,
                 commit_message=f"manifest: add delta {date}")
    log.info("publish_delta: %s published (%d rows)", date, build_info["rows"])

    # Persist new snapshot AFTER successful HF publish — so a mid-upload
    # failure means tomorrow's delta will re-try the same IDs.
    if build_info.get("update_snapshot"):
        save_snapshot_ids(build_info["snapshot_path"],
                          build_info["current_ids"], date)
        log.info("publish_delta: snapshot saved (%d ids)", len(build_info["current_ids"]))


# ── CLI ─────────────────────────────────────────────────────────────────

def _cli() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Daily delta publisher for voilaj/swiss-caselaw")
    p.add_argument("--db", type=_Path,
                   default=_Path("/opt/caselaw/repo/output/decisions.db"),
                   help="Path to FTS5 decisions.db")
    p.add_argument("--date", type=str,
                   default=_dt.date.today().isoformat(),
                   help="YYYY-MM-DD (UTC day to publish delta for). Default: today.")
    p.add_argument("--build-dir", type=_Path,
                   default=_Path("/tmp/caselaw_delta_build"),
                   help="Scratch directory for build artifacts")
    p.add_argument("--snapshot", type=_Path,
                   default=_Path("/opt/caselaw/repo/state/hf_delta_snapshot.json"),
                   help="Path to local id-snapshot state file")
    p.add_argument("--dry-run", action="store_true",
                   help="Build locally; do NOT upload to HF, do NOT update snapshot")
    p.add_argument("--hf-repo", default=HF_REPO_ID)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    _logging.basicConfig(
        level=_logging.DEBUG if args.verbose else _logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    info = build_delta(
        db_path=args.db,
        date=args.date,
        build_dir=args.build_dir,
        snapshot_path=args.snapshot,
        update_snapshot=not args.dry_run,
    )
    summary = {k: v for k, v in info.items()
               if k in ("date", "rows", "sqlite_zst_bytes", "parquet_bytes", "data_parquet_bytes")}
    summary["current_ids"] = len(info["current_ids"])
    log.info("build summary: %s", summary)
    publish_delta(build_info=info, hf_repo=args.hf_repo, dry_run=args.dry_run)


if __name__ == "__main__":
    _cli()
