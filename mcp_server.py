"""
Swiss Case Law MCP Server
==========================

Local MCP server for searching Swiss court decisions.
Runs over stdio, searches a local SQLite FTS5 database.

Architecture:
    HuggingFace (voilaj/swiss-caselaw)
        ↓ download Parquet files
    ~/.swiss-caselaw/decisions.db  (SQLite + FTS5)
        ↓ search via MCP stdio
    Claude / Cursor / any MCP client

Installation:
    pip install mcp pydantic huggingface_hub pyarrow

Usage with Claude Desktop:
    claude mcp add swiss-caselaw -- python3 /path/to/mcp_server.py

    Or in claude_desktop_config.json:
    {
      "mcpServers": {
        "swiss-caselaw": {
          "command": "python3",
          "args": ["/path/to/mcp_server.py"]
        }
      }
    }

First run downloads ~800MB from HuggingFace and builds the local
search index. Subsequent runs use the cached database.

Tools exposed:
    search_decisions  — Full-text search with filters (court, canton,
                        language, date range). Returns BM25-ranked results
                        with highlighted snippets.
    get_decision      — Fetch a single decision by ID or docket number.
                        Returns full text and all metadata.
    list_courts       — List available courts with decision counts.
    get_statistics    — Aggregate statistics by court, canton, year,
                        language.
    update_database   — Check for and download new data from HuggingFace.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Add repo root to path so db_schema can be imported when run from any directory
sys.path.insert(0, str(Path(__file__).parent))
from db_schema import SCHEMA_SQL, INSERT_OR_IGNORE_SQL, INSERT_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stderr,  # MCP uses stdout for protocol, logs go to stderr
)
logger = logging.getLogger("swiss-caselaw-mcp")

# ── Configuration ─────────────────────────────────────────────
HF_REPO = "voilaj/swiss-caselaw"
DATA_DIR = Path(os.environ.get(
    "SWISS_CASELAW_DIR",
    Path.home() / ".swiss-caselaw",
))
DB_PATH = DATA_DIR / "decisions.db"
PARQUET_DIR = DATA_DIR / "parquet"

MAX_SNIPPET_LEN = 500  # chars per snippet
DEFAULT_LIMIT = 20
MAX_LIMIT = 100
MAX_RERANK_CANDIDATES = 500

# Known FTS-searchable columns for explicit column filters (e.g., regeste:foo)
FTS_COLUMNS = {
    "decision_id",
    "court",
    "canton",
    "docket_number",
    "language",
    "title",
    "regeste",
    "full_text",
}

# Lightweight multilingual stopword set for natural-language fallback queries.
NL_STOPWORDS = {
    # German
    "ich", "suche", "zur", "der", "die", "das", "und", "in", "zum", "von",
    "mit", "ohne", "für", "was", "sagt", "dem", "den", "des", "ein", "eine",
    "einer", "einem", "im", "am", "an", "zu", "auf", "über", "unter", "als",
    "oder", "nicht", "art",
    # French
    "je", "cherche", "sur", "le", "la", "les", "de", "du", "des", "un", "une",
    "et", "ou", "dans", "avec", "sans", "pour", "au", "aux", "d",
    # Italian
    "cerco", "una", "uno", "un", "sul", "sulla", "sui", "del", "della", "delle",
    "di", "e", "o", "con", "senza", "per", "nel", "nella", "nei", "agli", "ai",
    "al",
    # English
    "i", "search", "for", "the", "and", "or", "in", "of", "with", "without",
    "to", "on", "about", "a", "an",
}

MAX_NL_TOKENS = 16
RERANK_TERM_LIMIT = 24
NL_AND_TERM_LIMIT = 8


# ── Database ──────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Get a connection to the local SQLite database.

    On first run, automatically downloads from HuggingFace and builds the index.
    """
    if not DB_PATH.exists():
        logger.info("Database not found — downloading from HuggingFace (first run)...")
        result = update_from_huggingface()
        logger.info(result)
        if not DB_PATH.exists():
            raise FileNotFoundError(
                f"Database not found at {DB_PATH}. "
                f"Automatic download failed. Try running the 'update_database' tool manually."
            )
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")  # read-only for safety
    return conn


def get_db_stats() -> dict:
    """Get database statistics."""
    try:
        conn = get_db()
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        courts = conn.execute(
            "SELECT court, COUNT(*) as n FROM decisions GROUP BY court ORDER BY n DESC"
        ).fetchall()
        date_range = conn.execute(
            "SELECT MIN(decision_date), MAX(decision_date) FROM decisions"
        ).fetchone()
        conn.close()
        return {
            "total_decisions": total,
            "courts": {r["court"]: r["n"] for r in courts},
            "earliest_date": date_range[0],
            "latest_date": date_range[1],
            "db_path": str(DB_PATH),
            "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 1),
        }
    except FileNotFoundError:
        return {"error": "Database not found. Run 'update_database' first."}


# ── Search functions ──────────────────────────────────────────

def search_fts5(
    query: str,
    court: str | None = None,
    canton: str | None = None,
    language: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> list[dict]:
    """
    Full-text search using SQLite FTS5 with BM25 ranking.

    The FTS5 query supports:
    - Simple words: verfassungsrecht
    - Phrases: "Treu und Glauben"
    - Boolean: arbeitsrecht AND kündigung
    - Prefix: verfassung*
    - Column filters: full_text:miete AND regeste:kündigung
    """
    conn = get_db()
    limit = max(1, min(limit, MAX_LIMIT))

    fts_query = query.strip()
    if not fts_query:
        # No search query — return recent decisions with filters
        return _list_recent(conn, court, canton, language, date_from, date_to, limit)

    # Build WHERE clause for filters (applied to main table via JOIN)
    filters = []
    params: list = []

    if court:
        filters.append("d.court = ?")
        params.append(court.lower())
    if canton:
        filters.append("d.canton = ?")
        params.append(canton.upper())
    if language:
        filters.append("d.language = ?")
        params.append(language.lower())
    if date_from:
        filters.append("d.decision_date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("d.decision_date <= ?")
        params.append(date_to)

    where = (" AND " + " AND ".join(filters)) if filters else ""

    # Docket-style lookups should prioritize exact/near-exact docket matches.
    if _looks_like_docket_query(fts_query):
        docket_results = _search_by_docket(conn, fts_query, where, params, limit)
        if docket_results:
            return docket_results

    sql = f"""
        SELECT
            d.decision_id,
            d.court,
            d.canton,
            d.chamber,
            d.docket_number,
            d.decision_date,
            d.language,
            d.title,
            d.regeste,
            snippet(decisions_fts, 7, '<mark>', '</mark>', '...', 40) as snippet,
            d.source_url,
            d.pdf_url,
            bm25(decisions_fts, 0.8, 0.8, 0.8, 2.0, 0.8, 6.0, 5.0, 1.2) as bm25_score
        FROM decisions_fts
        JOIN decisions d ON d.rowid = decisions_fts.rowid
        WHERE decisions_fts MATCH ?{where}
        ORDER BY bm25_score ASC
        LIMIT ?
    """

    try:
        had_success = False
        candidate_rows: dict[str, sqlite3.Row] = {}
        for match_query in _build_query_strategies(fts_query):
            try:
                candidate_limit = min(max(limit * 5, 50), MAX_RERANK_CANDIDATES)
                if _looks_like_docket_query(fts_query):
                    candidate_limit = max(candidate_limit, min(300, MAX_RERANK_CANDIDATES))
                rows = conn.execute(
                    sql,
                    [match_query] + params + [candidate_limit],
                ).fetchall()
                had_success = True
            except sqlite3.OperationalError as e:
                logger.info(
                    "FTS query failed, trying fallback strategy: %s (%s)",
                    _truncate(match_query, 120),
                    e,
                )
                continue

            for row in rows:
                decision_id = row["decision_id"]
                current = candidate_rows.get(decision_id)
                if current is None:
                    candidate_rows[decision_id] = row
                    continue
                # Keep the better lexical candidate when duplicated across strategies.
                if _to_float(row["bm25_score"]) < _to_float(current["bm25_score"]):
                    candidate_rows[decision_id] = row

        if candidate_rows:
            return _rerank_rows(list(candidate_rows.values()), fts_query, limit)

        # All strategies executed but none returned results.
        # Return empty list (never propagate parser errors to user queries).
        if had_success:
            return []
        return []
    finally:
        conn.close()


def _search_by_docket(
    conn: sqlite3.Connection,
    raw_query: str,
    where: str,
    params: list,
    limit: int,
) -> list[dict]:
    """Docket-first retrieval for docket-like queries."""
    query_norm = _normalize_docket(raw_query)
    if not query_norm:
        return []

    norm_expr = (
        "replace(replace(replace(replace(lower(d.docket_number),'.',''),'/',''),'_',''),'-','')"
    )
    sql = f"""
        SELECT
            d.decision_id,
            d.court,
            d.canton,
            d.chamber,
            d.docket_number,
            d.decision_date,
            d.language,
            d.title,
            d.regeste,
            NULL as snippet,
            d.source_url,
            d.pdf_url,
            CASE
                WHEN {norm_expr} = ? THEN 0
                WHEN {norm_expr} LIKE ? THEN 1
                ELSE 2
            END AS docket_rank
        FROM decisions d
        WHERE ({norm_expr} = ? OR {norm_expr} LIKE ?){where}
        ORDER BY docket_rank ASC,
                 abs(length({norm_expr}) - ?) ASC,
                 d.decision_date DESC
        LIMIT ?
    """
    rows = conn.execute(
        sql,
        [
            query_norm,
            f"{query_norm}%",
            query_norm,
            f"%{query_norm}%",
            *params,
            len(query_norm),
            limit,
        ],
    ).fetchall()
    results = []
    for r in rows:
        results.append({
            "decision_id": r["decision_id"],
            "court": r["court"],
            "canton": r["canton"],
            "chamber": r["chamber"],
            "docket_number": r["docket_number"],
            "decision_date": r["decision_date"],
            "language": r["language"],
            "title": r["title"],
            "regeste": _truncate(r["regeste"], MAX_SNIPPET_LEN) if r["regeste"] else None,
            "snippet": r["snippet"],
            "source_url": r["source_url"],
            "pdf_url": r["pdf_url"],
            "relevance_score": round(100.0 - float(r["docket_rank"]), 4),
        })
    return results


def _rerank_rows(rows: list[sqlite3.Row], raw_query: str, limit: int) -> list[dict]:
    """
    Re-rank lexical FTS candidates with lightweight query-intent signals.

    The FTS index provides robust candidate retrieval; this stage improves top-k
    quality for practitioner-style natural-language and docket-centric queries.
    """
    if not rows:
        return []

    rank_terms = _extract_rank_terms(raw_query)
    cleaned_phrase = _clean_for_phrase(raw_query)
    query_norm = _normalize_docket(raw_query)

    scored: list[tuple[float, float, int, sqlite3.Row]] = []
    for idx, row in enumerate(rows):
        bm25_score = _to_float(row["bm25_score"])
        bm25_component = -bm25_score

        title_text = (row["title"] or "").lower()
        regeste_text = (row["regeste"] or "").lower()
        snippet_text = (row["snippet"] or "").lower()
        docket_text = (row["docket_number"] or "").lower()
        docket_norm = _normalize_docket(docket_text)

        if rank_terms:
            title_cov = _term_coverage(rank_terms, title_text)
            regeste_cov = _term_coverage(rank_terms, regeste_text)
            snippet_cov = _term_coverage(rank_terms, snippet_text)
        else:
            title_cov = regeste_cov = snippet_cov = 0.0

        phrase_hit = 0.0
        if cleaned_phrase:
            if cleaned_phrase in title_text or cleaned_phrase in regeste_text:
                phrase_hit += 1.0
            if cleaned_phrase in snippet_text:
                phrase_hit += 0.5

        docket_exact = 1.0 if query_norm and docket_norm and query_norm == docket_norm else 0.0
        docket_partial = 0.0
        if query_norm and docket_norm and not docket_exact:
            if len(query_norm) >= 5 and query_norm in docket_norm:
                docket_partial = 1.0

        signal = (
            6.0 * docket_exact
            + 2.0 * docket_partial
            + 3.0 * title_cov
            + 2.2 * regeste_cov
            + 0.8 * snippet_cov
            + 1.8 * phrase_hit
        )
        final_score = bm25_component + signal

        scored.append((final_score, bm25_score, idx, row))

    scored.sort(key=lambda x: (-x[0], x[1], x[2]))

    results: list[dict] = []
    for final_score, _bm25, _idx, row in scored[:limit]:
        results.append({
            "decision_id": row["decision_id"],
            "court": row["court"],
            "canton": row["canton"],
            "chamber": row["chamber"],
            "docket_number": row["docket_number"],
            "decision_date": row["decision_date"],
            "language": row["language"],
            "title": row["title"],
            "regeste": _truncate(row["regeste"], MAX_SNIPPET_LEN) if row["regeste"] else None,
            "snippet": row["snippet"],
            "source_url": row["source_url"],
            "pdf_url": row["pdf_url"],
            "relevance_score": round(final_score, 4),
        })
    return results


def _build_query_strategies(raw_query: str) -> list[str]:
    """
    Build parser-safe FTS query strategies.

    For explicit FTS syntax, preserve raw query first.
    For natural language, prefer tokenized OR query first for robustness.
    """
    raw = raw_query.strip()
    nl_and = _build_nl_and_query(raw)
    nl_or = _build_nl_or_query(raw)
    cleaned = _clean_for_phrase(raw)
    quoted = f'"{cleaned}"' if cleaned else ""

    if _has_explicit_fts_syntax(raw):
        candidates = [raw, quoted, nl_and, nl_or]
    else:
        candidates = [nl_and, nl_or, quoted, raw]

    # Dedupe while preserving order
    seen: set[str] = set()
    strategies: list[str] = []
    for c in candidates:
        c = c.strip()
        if c and c not in seen:
            strategies.append(c)
            seen.add(c)
    return strategies


def _has_explicit_fts_syntax(query: str) -> bool:
    """Detect advanced query syntax where raw execution should be prioritized."""
    if re.search(r"\b(AND|OR|NOT|NEAR)\b", query, re.IGNORECASE):
        return True
    if "*" in query:
        return True
    if re.search(rf"\b(?:{'|'.join(sorted(FTS_COLUMNS))})\s*:", query, re.IGNORECASE):
        return True
    # Balanced quoted phrase usually indicates intentional syntax.
    if query.count('"') >= 2 and query.count('"') % 2 == 0:
        return True
    return False


def _clean_for_phrase(query: str) -> str:
    """Normalize punctuation/quotes for safe phrase fallback."""
    # Remove quotes and punctuation that frequently break FTS parser.
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower())
    return " ".join(tokens[:MAX_NL_TOKENS])


def _build_nl_or_query(query: str) -> str:
    """Tokenize natural-language input into a robust OR-based FTS query."""
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower())
    keep: list[str] = []
    seen: set[str] = set()

    for tok in tokens:
        if tok in NL_STOPWORDS:
            continue
        if not tok.isdigit() and len(tok) < 3:
            continue
        if tok in seen:
            continue
        keep.append(tok)
        seen.add(tok)
        if len(keep) >= MAX_NL_TOKENS:
            break

    return " OR ".join(keep)


def _build_nl_and_query(query: str) -> str:
    """Tokenize natural-language input into a stricter AND query."""
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower())
    keep: list[str] = []
    seen: set[str] = set()

    for tok in tokens:
        if tok in NL_STOPWORDS:
            continue
        if not tok.isdigit() and len(tok) < 3:
            continue
        if tok in seen:
            continue
        keep.append(tok)
        seen.add(tok)
        if len(keep) >= NL_AND_TERM_LIMIT:
            break

    if len(keep) < 2:
        return ""
    return " AND ".join(keep)


def _extract_rank_terms(query: str) -> list[str]:
    """Extract deduplicated content-bearing terms for second-pass reranking."""
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower())
    terms: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        if tok in NL_STOPWORDS:
            continue
        if tok in FTS_COLUMNS:
            continue
        if tok in {"and", "or", "not", "near"}:
            continue
        if not tok.isdigit() and len(tok) < 3:
            continue
        if tok in seen:
            continue
        terms.append(tok)
        seen.add(tok)
        if len(terms) >= RERANK_TERM_LIMIT:
            break
    return terms


def _term_coverage(terms: list[str], text: str) -> float:
    """Fraction of query terms appearing in text."""
    if not terms:
        return 0.0
    hits = sum(1 for t in terms if t in text)
    return hits / len(terms)


def _normalize_docket(value: str) -> str:
    """Normalize docket-like strings for exact/partial matching."""
    return re.sub(r"[^0-9a-z]+", "", (value or "").lower())


def _looks_like_docket_query(query: str) -> bool:
    """Heuristic: identify docket-number style queries."""
    q = query.strip()
    if not q:
        return False
    if re.search(r"[A-Za-z]\d|\d[A-Za-z]", q) and any(ch in q for ch in ("/", "_", ".")):
        return True
    if re.search(r"\b[0-9]{1,4}\s+[A-Z]{1,4}\s+[0-9]{1,4}\b", q):
        return True
    return False


def _to_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 1e9


def get_decision_by_id(decision_id: str) -> dict | None:
    """Fetch a single decision with full text."""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM decisions WHERE decision_id = ?",
        (decision_id,),
    ).fetchone()

    if not row:
        # Try searching by docket number
        row = conn.execute(
            "SELECT * FROM decisions WHERE docket_number = ? LIMIT 1",
            (decision_id,),
        ).fetchone()

    if not row:
        # Try partial match on docket
        row = conn.execute(
            "SELECT * FROM decisions WHERE docket_number LIKE ? LIMIT 1",
            (f"%{decision_id}%",),
        ).fetchone()

    conn.close()

    if not row:
        return None

    result = dict(row)
    # Remove json_data blob from response (redundant)
    result.pop("json_data", None)
    return result


def get_statistics(
    court: str | None = None,
    canton: str | None = None,
    year: int | None = None,
) -> dict:
    """Get aggregate statistics."""
    conn = get_db()

    filters = []
    params: list = []
    if court:
        filters.append("court = ?")
        params.append(court.lower())
    if canton:
        filters.append("canton = ?")
        params.append(canton.upper())
    if year:
        filters.append("decision_date LIKE ?")
        params.append(f"{year}-%")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    total = conn.execute(
        f"SELECT COUNT(*) FROM decisions {where}", params
    ).fetchone()[0]

    by_court = conn.execute(
        f"SELECT court, COUNT(*) as n FROM decisions {where} GROUP BY court ORDER BY n DESC",
        params,
    ).fetchall()

    by_language = conn.execute(
        f"SELECT language, COUNT(*) as n FROM decisions {where} GROUP BY language ORDER BY n DESC",
        params,
    ).fetchall()

    by_year = conn.execute(
        f"SELECT substr(decision_date, 1, 4) as year, COUNT(*) as n "
        f"FROM decisions {where} GROUP BY year ORDER BY year DESC LIMIT 20",
        params,
    ).fetchall()

    conn.close()

    return {
        "total": total,
        "by_court": {r["court"]: r["n"] for r in by_court},
        "by_language": {r["language"]: r["n"] for r in by_language},
        "by_year": {r["year"]: r["n"] for r in by_year},
    }


def list_courts() -> list[dict]:
    """List all available courts with decision counts."""
    conn = get_db()
    rows = conn.execute("""
        SELECT
            court,
            canton,
            COUNT(*) as decision_count,
            MIN(decision_date) as earliest,
            MAX(decision_date) as latest,
            COUNT(DISTINCT language) as languages
        FROM decisions
        GROUP BY court, canton
        ORDER BY decision_count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _list_recent(
    conn: sqlite3.Connection,
    court: str | None,
    canton: str | None,
    language: str | None,
    date_from: str | None,
    date_to: str | None,
    limit: int,
) -> list[dict]:
    """List recent decisions without FTS query (just filters)."""
    filters = []
    params: list = []

    if court:
        filters.append("court = ?")
        params.append(court.lower())
    if canton:
        filters.append("canton = ?")
        params.append(canton.upper())
    if language:
        filters.append("language = ?")
        params.append(language.lower())
    if date_from:
        filters.append("decision_date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("decision_date <= ?")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = conn.execute(
        f"""SELECT decision_id, court, canton, chamber, docket_number,
            decision_date, language, title, regeste, source_url, pdf_url
        FROM decisions {where}
        ORDER BY decision_date DESC
        LIMIT ?""",
        params + [limit],
    ).fetchall()

    return [dict(r) for r in rows]


def _truncate(text: str | None, max_len: int) -> str | None:
    if not text:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


# ── Data management ───────────────────────────────────────────

def update_from_huggingface() -> str:
    """Download latest data from HuggingFace and rebuild the database."""
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        return "Error: huggingface_hub not installed. Run: pip install huggingface_hub"

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading from {HF_REPO}...")
    try:
        snapshot_download(
            repo_id=HF_REPO,
            repo_type="dataset",
            local_dir=str(PARQUET_DIR),
            allow_patterns="*.parquet",
        )
    except Exception as e:
        return f"Download failed: {e}"

    # Build SQLite from Parquet files
    logger.info("Building SQLite FTS5 database...")
    try:
        _build_db_from_parquet()
    except Exception as e:
        return f"Database build failed: {e}"

    stats = get_db_stats()
    return (
        f"Database updated successfully.\n"
        f"Total decisions: {stats.get('total_decisions', '?')}\n"
        f"Courts: {', '.join(stats.get('courts', {}).keys())}\n"
        f"Date range: {stats.get('earliest_date', '?')} to {stats.get('latest_date', '?')}\n"
        f"Database: {stats.get('db_path', '?')} ({stats.get('db_size_mb', '?')} MB)"
    )


def _build_db_from_parquet():
    """Build SQLite FTS5 database from downloaded Parquet files."""
    import pyarrow.parquet as pq

    # Remove old DB
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Use canonical schema from db_schema.py
    conn.executescript(SCHEMA_SQL)

    # Import all Parquet files
    imported = 0
    parquet_files = list(PARQUET_DIR.rglob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} Parquet files")

    for pf in parquet_files:
        try:
            table = pq.read_table(pf)
            for batch in table.to_batches():
                for row in batch.to_pylist():
                    try:
                        values = tuple(
                            json.dumps(row, default=str) if col == "json_data"
                            else row.get(col)
                            for col in INSERT_COLUMNS
                        )
                        conn.execute(INSERT_OR_IGNORE_SQL, values)
                        imported += 1
                    except Exception as e:
                        logger.debug(f"Skip {row.get('decision_id', '?')}: {e}")
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to read {pf}: {e}")

    # Optimize
    conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
    conn.execute("PRAGMA optimize")
    conn.commit()
    conn.close()

    logger.info(f"Built database: {imported} decisions → {DB_PATH}")


# ── MCP Server ────────────────────────────────────────────────

server = Server("swiss-caselaw")


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="search_decisions",
            description=(
                "Search Swiss court decisions using full-text search. "
                "Supports keywords, phrases (in quotes), Boolean operators "
                "(AND, OR, NOT), and prefix matching (word*). "
                "Filter by court, canton, language, and date range. "
                "Returns BM25-ranked results with snippets."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Search query. Examples:\n"
                            "- Simple: Mietrecht Kündigung\n"
                            "- Phrase: \"Treu und Glauben\"\n"
                            "- Boolean: Arbeitsrecht AND Kündigung NOT Probezeit\n"
                            "- Prefix: Verfassung*\n"
                            "- By docket: 6B_1234/2025\n"
                            "- By article: \"Art. 8 BV\"\n"
                            "- Column: regeste:Mietrecht AND full_text:Kündigung"
                        ),
                    },
                    "court": {
                        "type": "string",
                        "description": (
                            "Filter by court code. "
                            "Federal: bger, bge, bvger, bstger, bpatger. "
                            "Cantonal: zh_obergericht, be_verwaltungsgericht, etc."
                        ),
                    },
                    "canton": {
                        "type": "string",
                        "description": "Filter by canton (CH for federal, ZH, BE, GE, etc.)",
                    },
                    "language": {
                        "type": "string",
                        "description": "Filter by language: de, fr, it, rm",
                        "enum": ["de", "fr", "it", "rm"],
                    },
                    "date_from": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD)",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 20, max 100)",
                        "default": 20,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_decision",
            description=(
                "Fetch a single court decision with full text. "
                "Look up by decision_id (e.g., bger_6B_1234_2025), "
                "docket number (e.g., 6B_1234/2025), or partial match."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "decision_id": {
                        "type": "string",
                        "description": "Decision ID, docket number, or partial docket",
                    },
                },
                "required": ["decision_id"],
            },
        ),
        Tool(
            name="list_courts",
            description=(
                "List all available courts with decision counts, date ranges, "
                "and language coverage. Use this to discover what data is available."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_statistics",
            description=(
                "Get aggregate statistics about the dataset. "
                "Optionally filter by court, canton, or year."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "court": {"type": "string", "description": "Filter by court code"},
                    "canton": {"type": "string", "description": "Filter by canton code"},
                    "year": {"type": "integer", "description": "Filter by year"},
                },
            },
        ),
        Tool(
            name="update_database",
            description=(
                "Download the latest Swiss caselaw data from HuggingFace "
                "and rebuild the local search database. Run this on first use "
                "or to get the latest decisions."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name == "search_decisions":
            results = search_fts5(
                query=arguments.get("query", ""),
                court=arguments.get("court"),
                canton=arguments.get("canton"),
                language=arguments.get("language"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                limit=arguments.get("limit", DEFAULT_LIMIT),
            )
            if not results:
                text = "No decisions found matching your query."
            else:
                text = f"Found {len(results)} decisions:\n\n"
                for i, r in enumerate(results, 1):
                    text += (
                        f"**{i}. {r['docket_number']}** ({r['decision_date']}) "
                        f"[{r['court']}] [{r['language']}]\n"
                    )
                    if r.get("title"):
                        text += f"   Title: {r['title']}\n"
                    if r.get("regeste"):
                        text += f"   Regeste: {r['regeste']}\n"
                    if r.get("snippet"):
                        text += f"   ...{r['snippet']}...\n"
                    if r.get("source_url"):
                        text += f"   URL: {r['source_url']}\n"
                    text += "\n"

            return [TextContent(type="text", text=text)]

        elif name == "get_decision":
            result = get_decision_by_id(arguments["decision_id"])
            if not result:
                return [TextContent(
                    type="text",
                    text=f"Decision not found: {arguments['decision_id']}",
                )]
            # Format full decision
            text = (
                f"# {result['docket_number']}\n"
                f"**Court:** {result['court']} | "
                f"**Date:** {result['decision_date']} | "
                f"**Language:** {result['language']}\n"
            )
            if result.get("chamber"):
                text += f"**Chamber:** {result['chamber']}\n"
            if result.get("title"):
                text += f"**Title:** {result['title']}\n"
            if result.get("regeste"):
                text += f"\n## Regeste\n{result['regeste']}\n"
            if result.get("full_text"):
                ft = result["full_text"]
                if len(ft) > 50000:
                    text += f"\n## Full Text (first 50,000 of {len(ft)} chars)\n{ft[:50000]}\n..."
                else:
                    text += f"\n## Full Text\n{ft}\n"
            if result.get("source_url"):
                text += f"\n**Source:** {result['source_url']}\n"
            if result.get("pdf_url"):
                text += f"**PDF:** {result['pdf_url']}\n"
            if result.get("cited_decisions"):
                text += f"\n**Citations:** {result['cited_decisions']}\n"
            return [TextContent(type="text", text=text)]

        elif name == "list_courts":
            courts = list_courts()
            if not courts:
                return [TextContent(type="text", text="No data available. Run 'update_database' first.")]
            text = "Available courts:\n\n"
            text += f"{'Court':<25} {'Canton':<8} {'Decisions':>10}  {'Earliest':>12} {'Latest':>12}\n"
            text += "-" * 75 + "\n"
            for c in courts:
                text += (
                    f"{c['court']:<25} {c['canton']:<8} "
                    f"{c['decision_count']:>10,}  "
                    f"{c['earliest']:>12} {c['latest']:>12}\n"
                )
            return [TextContent(type="text", text=text)]

        elif name == "get_statistics":
            stats = get_statistics(
                court=arguments.get("court"),
                canton=arguments.get("canton"),
                year=arguments.get("year"),
            )
            return [TextContent(
                type="text",
                text=json.dumps(stats, indent=2, ensure_ascii=False),
            )]

        elif name == "update_database":
            result = update_from_huggingface()
            return [TextContent(type="text", text=result)]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except FileNotFoundError as e:
        return [TextContent(
            type="text",
            text=(
                f"Database not found. Run the 'update_database' tool first to "
                f"download Swiss caselaw data from HuggingFace.\n\nError: {e}"
            ),
        )]
    except Exception as e:
        logger.error(f"Tool error {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Error: {e}")]


# ── Main ──────────────────────────────────────────────────────

async def main():
    logger.info(f"Swiss Case Law MCP Server starting")
    logger.info(f"Database: {DB_PATH}")

    if DB_PATH.exists():
        stats = get_db_stats()
        logger.info(
            f"Database loaded: {stats.get('total_decisions', '?')} decisions, "
            f"{stats.get('db_size_mb', '?')} MB"
        )
    else:
        logger.info("No database found. Use 'update_database' tool to download data.")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
