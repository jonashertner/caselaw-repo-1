"""Encode every Erwägung paragraph in decision_structure.db with a
multilingual sentence-transformer and persist to paragraph_embeddings.db.

Used by the pinpoint resolver as a *semantic rescue* signal — fires
only when lexical (BM25) returns no confident match, addressing the
vocabulary-mismatch FNs documented in the bench (Regeste vs. Erwägung
text use different vocabulary even within one decision).

Architecture:
  • Source:  decision_structure.db / erwaegungen_paragraph
  • Output:  paragraph_embeddings.db
              ┌─────────────────────────────────────────────┐
              │ paragraph_embeddings                        │
              │   decision_id TEXT, e_number TEXT,          │
              │   embedding BLOB, model_name TEXT,          │
              │   PRIMARY KEY (decision_id, e_number)       │
              │ encode_progress                             │
              │   model_name TEXT PRIMARY KEY,              │
              │   last_offset INTEGER, ts TEXT              │
              └─────────────────────────────────────────────┘
  • Per-decision queries: <300 vectors → raw BLOB + numpy is plenty
    fast (~5 ms cosine sim per decision). No sqlite-vec needed.

Resume safety: writes a watermark every BATCH_SIZE rows; on restart
picks up from there. Atomic-rebuild not needed — embedding rows are
idempotent (PRIMARY KEY upsert).

Cost (full corpus):
  • 8.8 M paragraphs × ~50 docs/sec on CPU = ~50 hours
  • On a single GPU (RTX 4090): ~12 hours
  • Storage: 8.8 M × 384 dim × 4 bytes = ~13 GB

Usage:
  python -m search_stack.build_paragraph_embeddings \\
      --structure-db output/decision_structure.db \\
      --output-db output/paragraph_embeddings.db \\
      --batch-size 64 --limit 1000  # for testing; omit --limit for full
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("build_paragraph_embeddings")

# Default model: small + multilingual + fast on CPU. 384-dim normalised
# embeddings → cosine = dot product. Released by sentence-transformers,
# CC-licensed, ~470 MB, supports 50+ languages including DE/FR/IT/RM.
DEFAULT_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


SCHEMA = """
CREATE TABLE IF NOT EXISTS paragraph_embeddings (
    decision_id TEXT NOT NULL,
    e_number    TEXT NOT NULL,
    embedding   BLOB NOT NULL,
    model_name  TEXT NOT NULL,
    PRIMARY KEY (decision_id, e_number)
);

CREATE INDEX IF NOT EXISTS idx_pe_decision ON paragraph_embeddings(decision_id);

CREATE TABLE IF NOT EXISTS encode_progress (
    model_name  TEXT PRIMARY KEY,
    last_offset INTEGER NOT NULL,
    ts          TEXT NOT NULL
);
"""


def _open_target(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _open_source(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _to_blob(vec) -> bytes:
    """Pack a 1-D float32 numpy array as little-endian bytes."""
    import numpy as np
    arr = np.ascontiguousarray(vec, dtype=np.float32)
    return arr.tobytes()


def _from_blob(blob: bytes, dim: int = 384):
    import numpy as np
    return np.frombuffer(blob, dtype=np.float32).reshape((dim,))


def _resume_offset(target: sqlite3.Connection, model_name: str) -> int:
    row = target.execute(
        "SELECT last_offset FROM encode_progress WHERE model_name = ?",
        (model_name,),
    ).fetchone()
    return int(row[0]) if row else 0


def _save_offset(target: sqlite3.Connection, model_name: str, offset: int) -> None:
    target.execute(
        "INSERT OR REPLACE INTO encode_progress(model_name, last_offset, ts) "
        "VALUES (?, ?, ?)",
        (model_name, offset, datetime.now(timezone.utc).isoformat()),
    )


def encode_paragraphs(
    structure_db: Path,
    output_db: Path,
    *,
    model_name: str = DEFAULT_MODEL,
    batch_size: int = 64,
    limit: int | None = None,
    restart: bool = False,
) -> dict:
    """Encode all paragraphs (resume-safe) and write to output_db.

    Returns a summary dict with counts + timings.
    """
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    logger.info("loaded model %s (dim=%d)", model_name, model.get_sentence_embedding_dimension())

    src = _open_source(structure_db)
    dst = _open_target(output_db)

    start_offset = 0 if restart else _resume_offset(dst, model_name)
    total = src.execute(
        "SELECT COUNT(*) FROM erwaegungen_paragraph"
    ).fetchone()[0]
    if limit is not None:
        total = min(total, start_offset + limit)
    logger.info("encoding %d paragraphs (resume from offset=%d)", total, start_offset)

    encoded = 0
    skipped = 0
    started = time.monotonic()
    offset = start_offset

    while offset < total:
        rows = src.execute(
            "SELECT decision_id, e_number, text FROM erwaegungen_paragraph "
            "ORDER BY decision_id, e_number "
            "LIMIT ? OFFSET ?",
            (batch_size, offset),
        ).fetchall()
        if not rows:
            break
        # Skip paragraphs we already have (incremental encoding).
        # Use distinct decision_ids — one placeholder per distinct id.
        distinct_ids = list({r["decision_id"] for r in rows})
        existing = {
            (r[0], r[1])
            for r in dst.execute(
                "SELECT decision_id, e_number FROM paragraph_embeddings "
                "WHERE decision_id IN ({})".format(",".join("?" * len(distinct_ids))),
                distinct_ids,
            ).fetchall()
        }
        to_encode = [
            (r["decision_id"], r["e_number"], r["text"])
            for r in rows
            if (r["decision_id"], r["e_number"]) not in existing
            and r["text"]
        ]
        skipped += len(rows) - len(to_encode)
        if to_encode:
            texts = [t[2] for t in to_encode]
            vectors = model.encode(
                texts,
                batch_size=batch_size,
                normalize_embeddings=True,  # cosine = dot product
                show_progress_bar=False,
            )
            dst.executemany(
                "INSERT OR REPLACE INTO paragraph_embeddings"
                "(decision_id, e_number, embedding, model_name) "
                "VALUES (?, ?, ?, ?)",
                [
                    (did, en, _to_blob(vec), model_name)
                    for (did, en, _), vec in zip(to_encode, vectors)
                ],
            )
            encoded += len(to_encode)

        offset += len(rows)
        _save_offset(dst, model_name, offset)
        dst.commit()

        if encoded and encoded % (batch_size * 10) == 0:
            elapsed = time.monotonic() - started
            rate = encoded / max(elapsed, 1e-3)
            eta = (total - offset) / max(rate, 1e-3)
            logger.info("encoded=%d skipped=%d offset=%d rate=%.1f/s eta=%.1fs",
                        encoded, skipped, offset, rate, eta)

    elapsed = time.monotonic() - started
    src.close()
    dst.close()

    return {
        "encoded": encoded,
        "skipped": skipped,
        "total_processed": offset - start_offset,
        "elapsed_sec": round(elapsed, 1),
        "rate_per_sec": round(encoded / max(elapsed, 1e-3), 2),
        "model": model_name,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--structure-db", required=True, type=Path)
    ap.add_argument("--output-db", required=True, type=Path)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap total paragraphs (for testing); omit for full corpus")
    ap.add_argument("--restart", action="store_true",
                    help="Ignore stored watermark and start from offset 0")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    )

    summary = encode_paragraphs(
        args.structure_db, args.output_db,
        model_name=args.model, batch_size=args.batch_size,
        limit=args.limit, restart=args.restart,
    )
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
