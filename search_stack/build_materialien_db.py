"""Build materialien.db from openlegalcommentary's preparatory-materials JSON.

Reads per-law digest files (bv.json, bgfa.json, ...) produced by the
openlegalcommentary project and builds a SQLite FTS5 database that the
MCP server can query via get_materialien / search_materialien.

Input:  JSON files following the openlegalcommentary schema:
        { "law": "BV", "sr_number": "101", "articles": { "1": {...}, ... } }

Output: output/materialien.db (atomic swap via .db.tmp)

Usage:
    python3 -m search_stack.build_materialien_db
    python3 -m search_stack.build_materialien_db --input-dir ../openlegalcommentary/scripts/preparatory_materials
    python3 -m search_stack.build_materialien_db --input-dir data/materialien
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("build_materialien_db")

# Default input: look for JSON files in data/materialien/ within the repo,
# then fall back to the openlegalcommentary sibling directory.
DEFAULT_INPUT_DIRS = [
    Path("data/materialien"),
    Path("../openlegalcommentary/scripts/preparatory_materials"),
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS materialien (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    law_code TEXT NOT NULL,
    sr_number TEXT,
    article TEXT NOT NULL,
    bbl_ref TEXT NOT NULL,
    bbl_page_refs TEXT,
    legislative_intent TEXT,
    key_arguments TEXT,
    design_choices TEXT,
    rejected_alternatives TEXT,
    general_context TEXT
);

CREATE INDEX IF NOT EXISTS idx_materialien_law_art
    ON materialien(law_code, article);

CREATE TABLE IF NOT EXISTS parliamentary_modifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    law_code TEXT NOT NULL,
    council TEXT,
    date TEXT,
    text TEXT
);

CREATE INDEX IF NOT EXISTS idx_parl_mod_law
    ON parliamentary_modifications(law_code);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS materialien_fts USING fts5(
    law_code,
    article,
    bbl_ref,
    legislative_intent,
    key_arguments,
    design_choices,
    rejected_alternatives,
    general_context,
    content=materialien,
    content_rowid=id,
    tokenize='unicode61 remove_diacritics 2'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS materialien_ai AFTER INSERT ON materialien BEGIN
    INSERT INTO materialien_fts(
        rowid, law_code, article, bbl_ref,
        legislative_intent, key_arguments, design_choices,
        rejected_alternatives, general_context
    ) VALUES (
        new.id, new.law_code, new.article, new.bbl_ref,
        new.legislative_intent, new.key_arguments, new.design_choices,
        new.rejected_alternatives, new.general_context
    );
END;

CREATE TRIGGER IF NOT EXISTS materialien_ad AFTER DELETE ON materialien BEGIN
    INSERT INTO materialien_fts(
        materialien_fts, rowid, law_code, article, bbl_ref,
        legislative_intent, key_arguments, design_choices,
        rejected_alternatives, general_context
    ) VALUES (
        'delete', old.id, old.law_code, old.article, old.bbl_ref,
        old.legislative_intent, old.key_arguments, old.design_choices,
        old.rejected_alternatives, old.general_context
    );
END;
"""

# Laws we know about (SR number mapping)
SR_NUMBERS: dict[str, str] = {
    "BV": "101", "ZGB": "210", "OR": "220", "ZPO": "272",
    "StGB": "311.0", "StPO": "312.0", "SchKG": "281.1",
    "VwVG": "172.021", "BGFA": "935.61",
}


def _list_join(items: list) -> str:
    """Join a list of strings into a single searchable text block."""
    if not items:
        return ""
    return "\n".join(f"- {item}" for item in items)


def ingest_law(conn: sqlite3.Connection, path: Path) -> int:
    """Ingest a single law's digest JSON into the database.

    Returns the number of article-source rows inserted.
    """
    data = json.loads(path.read_text())
    law = data.get("law", "")
    sr = data.get("sr_number", SR_NUMBERS.get(law, ""))
    articles = data.get("articles", {})

    if not law or not articles:
        logger.warning("Skipping %s: no law code or articles", path.name)
        return 0

    # Track parliamentary modifications at law level (deduplicated)
    seen_mods: set[str] = set()
    count = 0

    for art_key, art_data in articles.items():
        for source in art_data.get("sources", []):
            conn.execute(
                """INSERT INTO materialien
                   (law_code, sr_number, article, bbl_ref, bbl_page_refs,
                    legislative_intent, key_arguments, design_choices,
                    rejected_alternatives, general_context)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    law,
                    sr,
                    art_key,
                    source.get("bbl_ref", ""),
                    json.dumps(source.get("bbl_page_refs", [])),
                    source.get("legislative_intent", ""),
                    _list_join(source.get("key_arguments", [])),
                    _list_join(source.get("design_choices", [])),
                    _list_join(source.get("rejected_alternatives", [])),
                    source.get("general_context", ""),
                ),
            )
            count += 1

        for mod in art_data.get("parliamentary_modifications", []):
            mod_key = f"{mod.get('council')}|{mod.get('date')}|{mod.get('text')}"
            if mod_key not in seen_mods:
                seen_mods.add(mod_key)
                conn.execute(
                    """INSERT INTO parliamentary_modifications
                       (law_code, council, date, text) VALUES (?, ?, ?, ?)""",
                    (law, mod.get("council", ""), mod.get("date", ""), mod.get("text", "")),
                )

    return count


def build_db(input_dir: Path, output_path: Path) -> None:
    """Build materialien.db from all JSON files in input_dir."""
    # Find all law digest JSON files (exclude registry.json)
    json_files = sorted(
        p for p in input_dir.glob("*.json")
        if p.name != "registry.json" and p.stat().st_size > 100
    )

    if not json_files:
        logger.error("No digest JSON files found in %s", input_dir)
        sys.exit(1)

    logger.info("Found %d digest files in %s", len(json_files), input_dir)

    # Build to a .tmp file, then atomic swap
    tmp_path = output_path.with_suffix(".db.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    total = 0
    for path in json_files:
        law_name = path.stem.upper()
        n = ingest_law(conn, path)
        logger.info("  %s: %d article-source rows", law_name, n)
        total += n

    # Write metadata
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('built_at', ?)",
        (datetime.now(timezone.utc).isoformat(),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('total_rows', ?)",
        (str(total),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('laws', ?)",
        (json.dumps([p.stem.upper() for p in json_files]),),
    )

    # Optimize FTS5
    conn.execute("INSERT INTO materialien_fts(materialien_fts) VALUES ('optimize')")
    conn.commit()

    # Switch to DELETE journal mode for immutable=1 compatibility
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()

    # Atomic swap
    real_path = Path(os.path.realpath(str(output_path)))
    real_tmp = real_path.with_suffix(".db.tmp")
    if tmp_path != real_tmp:
        tmp_path.rename(real_tmp)
    os.replace(str(real_tmp), str(real_path))

    logger.info("Built %s: %d total rows from %d laws", output_path, total, len(json_files))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir", type=Path, default=None,
        help="Directory containing per-law digest JSON files "
             "(default: data/materialien/ or ../openlegalcommentary/scripts/preparatory_materials/)",
    )
    parser.add_argument(
        "--output", type=Path,
        default=Path(os.environ.get("SWISS_CASELAW_DIR", "output")) / "materialien.db",
        help="Output SQLite DB path",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Find input directory
    input_dir = args.input_dir
    if input_dir is None:
        for candidate in DEFAULT_INPUT_DIRS:
            if candidate.exists() and any(candidate.glob("*.json")):
                input_dir = candidate
                break
    if input_dir is None or not input_dir.exists():
        logger.error(
            "No input directory found. Provide --input-dir or place JSON files in %s",
            " or ".join(str(d) for d in DEFAULT_INPUT_DIRS),
        )
        sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    build_db(input_dir, args.output)


if __name__ == "__main__":
    main()
