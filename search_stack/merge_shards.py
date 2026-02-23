"""Merge sharded vector DBs into a single vectors.db.

Usage:
    python3 -m search_stack.merge_shards \
        --shards output/vectors_shard_0.db output/vectors_shard_1.db ... \
        --output output/vectors.db \
        --enable-sparse
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def merge_shards(
    shard_paths: list[Path],
    output_path: Path,
    *,
    enable_sparse: bool = False,
) -> dict:
    """Merge multiple shard DBs into one combined vectors.db.

    Reads from each shard and inserts into a fresh combined DB.
    Uses atomic rename for safety.
    """
    from search_stack.build_vectors import (
        create_vec_db,
        serialize_f32,
    )

    t0 = time.time()
    tmp_path = output_path.parent / f".{output_path.name}.tmp"

    # Create target DB
    conn = create_vec_db(str(tmp_path), enable_sparse=enable_sparse)

    total_dense = 0
    total_sparse = 0

    for shard_path in shard_paths:
        if not shard_path.exists():
            logger.warning("Shard %s does not exist, skipping", shard_path)
            continue

        logger.info("Merging shard: %s", shard_path)

        shard = sqlite3.connect(str(shard_path))
        try:
            shard.enable_load_extension(True)
            import sqlite_vec
            sqlite_vec.load(shard)
        except Exception as exc:
            logger.error("Failed to load sqlite-vec for shard %s: %s", shard_path, exc)
            shard.close()
            continue

        # Copy dense vectors
        cursor = shard.execute(
            "SELECT decision_id, embedding, language FROM vec_decisions"
        )
        batch = []
        for row in cursor:
            batch.append(row)
            if len(batch) >= 1000:
                conn.executemany(
                    "INSERT INTO vec_decisions (decision_id, embedding, language) "
                    "VALUES (?, ?, ?)",
                    batch,
                )
                total_dense += len(batch)
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT INTO vec_decisions (decision_id, embedding, language) "
                "VALUES (?, ?, ?)",
                batch,
            )
            total_dense += len(batch)
        conn.commit()

        # Copy sparse terms
        if enable_sparse:
            try:
                cursor = shard.execute(
                    "SELECT decision_id, token_id, weight FROM sparse_terms"
                )
                batch = []
                for row in cursor:
                    batch.append(row)
                    if len(batch) >= 10000:
                        conn.executemany(
                            "INSERT INTO sparse_terms (decision_id, token_id, weight) "
                            "VALUES (?, ?, ?)",
                            batch,
                        )
                        total_sparse += len(batch)
                        batch.clear()
                if batch:
                    conn.executemany(
                        "INSERT INTO sparse_terms (decision_id, token_id, weight) "
                        "VALUES (?, ?, ?)",
                        batch,
                    )
                    total_sparse += len(batch)
                conn.commit()
            except sqlite3.OperationalError:
                logger.warning("No sparse_terms in shard %s", shard_path)

        shard.close()
        logger.info(
            "Merged shard %s: total dense=%d, sparse=%d",
            shard_path.name, total_dense, total_sparse,
        )

    conn.close()
    os.replace(str(tmp_path), str(output_path))

    elapsed = time.time() - t0
    stats = {
        "output": str(output_path),
        "shards_merged": len(shard_paths),
        "total_dense": total_dense,
        "total_sparse": total_sparse,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("Merge complete: %s", stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge sharded vector DBs into a single vectors.db"
    )
    parser.add_argument(
        "--shards",
        type=Path,
        nargs="+",
        required=True,
        help="Paths to shard DB files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/vectors.db"),
        help="Output merged DB path",
    )
    parser.add_argument(
        "--enable-sparse",
        action="store_true",
        help="Merge sparse_terms table too",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    import json
    stats = merge_shards(args.shards, args.output, enable_sparse=args.enable_sparse)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
