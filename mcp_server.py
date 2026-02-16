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
    draft_mock_decision — Build a research-only mock decision outline from
                        user facts, grounded in caselaw and statute references
                        (optionally enriched from Fedlex).
    update_database   — Check for and download new data from HuggingFace.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
import html as html_lib
from datetime import datetime, timezone
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
DEFAULT_LIMIT = 50
MAX_LIMIT = 100
MAX_FACT_DECISION_LIMIT = 20
MAX_RERANK_CANDIDATES = 300
MIN_CANDIDATE_POOL = 60
TARGET_POOL_MULTIPLIER = 4
DOCKET_MIN_CANDIDATE_POOL = 80
RRF_RANK_CONSTANT = 60
FULL_TEXT_RERANK_CHARS = 1400
PASSAGE_SENTENCE_WINDOW = 4

CROSS_ENCODER_ENABLED = os.environ.get("SWISS_CASELAW_CROSS_ENCODER", "0").lower() in {
    "1",
    "true",
    "yes",
}
CROSS_ENCODER_MODEL = os.environ.get(
    "SWISS_CASELAW_CROSS_ENCODER_MODEL",
    "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
)
CROSS_ENCODER_TOP_N = max(1, int(os.environ.get("SWISS_CASELAW_CROSS_ENCODER_TOP_N", "30")))
CROSS_ENCODER_WEIGHT = float(os.environ.get("SWISS_CASELAW_CROSS_ENCODER_WEIGHT", "1.4"))

GRAPH_DB_PATH = Path(os.environ.get("SWISS_CASELAW_GRAPH_DB", "output/reference_graph.db"))
GRAPH_SIGNALS_ENABLED = os.environ.get("SWISS_CASELAW_GRAPH_SIGNALS", "0").lower() not in {
    "0",
    "false",
    "no",
}
FEDLEX_CACHE_PATH = Path(
    os.environ.get("SWISS_CASELAW_FEDLEX_CACHE", str(DATA_DIR / "fedlex_cache.json"))
)
FEDLEX_TIMEOUT_SECONDS = float(os.environ.get("SWISS_CASELAW_FEDLEX_TIMEOUT", "5"))
FEDLEX_USER_AGENT = os.environ.get(
    "SWISS_CASELAW_FEDLEX_USER_AGENT",
    "swiss-caselaw-mcp/1.0 (+https://github.com/jonashertner/caselaw-repo-1)",
)

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
MAX_EXPANSIONS_PER_TERM = 2

# Legal term expansion map (multilingual + doctrine variants).
# Keys and values are normalized token forms.
LEGAL_QUERY_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "asyl": ("asile", "asilo", "schutz", "refugee"),
    "asile": ("asyl", "asilo", "protection"),
    "asilo": ("asyl", "asile", "protezione"),
    "wegweisung": ("renvoi", "allontanamento", "ausweisung"),
    "renvoi": ("wegweisung", "expulsion", "allontanamento"),
    "allontanamento": ("wegweisung", "renvoi", "espulsione"),
    "ausweisung": ("expulsion", "renvoi", "wegweisung"),
    "kuendigung": ("resiliation", "disdetta", "termination"),
    "kundigung": ("resiliation", "disdetta", "termination"),
    "resiliation": ("kuendigung", "kundigung", "termination"),
    "disdetta": ("kuendigung", "resiliation", "termination"),
    "mietrecht": ("bail", "locazione", "mietvertrag"),
    "mietvertrag": ("bail", "locazione", "mietrecht"),
    "permis": ("baubewilligung", "baugesuch", "autorizzazione"),
    "construire": ("baubewilligung", "bauen", "construction"),
    "construction": ("baubewilligung", "baugesuch", "construire"),
    "baubewilligung": ("permis", "construction", "autorizzazione"),
    "baugesuch": ("permis", "construction", "autorizzazione"),
    "eolien": ("windpark", "windenergie", "eolienne"),
    "eolienne": ("windpark", "windenergie", "eolien"),
    "windpark": ("eolien", "eolienne", "parc"),
    "immissionen": ("nuisances", "immissioni", "laerm"),
    "laerm": ("bruit", "rumore", "immissionen"),
    "beschleunigt": ("verkurzt", "schnellverfahren", "accelerato"),
    "beschleunigtes": ("verkurzte", "schnellverfahren", "accelerato"),
    "verkurzt": ("beschleunigt", "beschleunigtes"),
    "verkurzte": ("beschleunigtes", "beschleunigt"),
    "steuer": ("impot", "tax", "imposta"),
    "impot": ("steuer", "tax", "imposta"),
    "imposta": ("steuer", "impot", "tax"),
    "unfallversicherung": ("accident", "assicurazione", "assurance"),
    "kausalzusammenhang": ("causalite", "causalita", "causale"),
    "verjaehrung": ("prescription", "prescrizione"),
    "verfassung": ("constitution", "costituzione", "bv"),
    "datenschutz": ("protection", "privacy", "donnees"),
    "persoenlichkeitsschutz": ("privacy", "protection", "personalita"),
}
ASYL_QUERY_TERMS = {"asyl", "asile", "asilo", "wegweisung", "renvoi", "allontanamento"}
LEGAL_ANCHOR_PAIRS: tuple[tuple[str, str], ...] = (
    ("asyl", "wegweisung"),
    ("asile", "renvoi"),
    ("asilo", "allontanamento"),
    ("parc", "eolien"),
    ("permis", "construire"),
    ("baubewilligung", "windpark"),
)
DECISION_INTENT_TERMS = {
    "arret",
    "entscheid",
    "jugement",
    "sentenza",
    "urteil",
    "bundesgericht",
    "tribunal",
    "gericht",
}
HIGH_COURTS = {"bger", "bge", "bvger", "bstger", "egmr"}
ACCELERATED_PROCEDURE_TERMS = {
    "beschleunigt",
    "beschleunigtes",
    "beschleunigte",
    "verkurzt",
    "verkurzte",
    "schnellverfahren",
    "accelerato",
    "accelere",
}
FEDLEX_LAW_CODE_BASE_URLS = {
    # Curated set of high-impact federal statutes. Extend over time.
    "ASYLG": "https://www.fedlex.admin.ch/eli/cc/1999/358",
    "AIG": "https://www.fedlex.admin.ch/eli/cc/2007/758",
    "LSTRI": "https://www.fedlex.admin.ch/eli/cc/2007/758",
}
COURT_QUERY_HINTS: dict[str, tuple[str, ...]] = {
    "bger": ("bger", "bundesgericht", "tribunal federal", "tribunale federale"),
    "bvger": (
        "bvger",
        "bundesverwaltungsgericht",
        "tribunal administratif federal",
        "tribunale amministrativo federale",
    ),
    "bstger": ("bstger", "bundesstrafgericht", "tribunal penal federal"),
}
COURT_QUERY_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "bger": ("bge",),
}

LANGUAGE_HINT_TERMS: dict[str, set[str]] = {
    "de": {
        "und", "wegweisung", "kuendigung", "kundigung", "mietrecht",
        "bundesgericht", "gericht", "baubewilligung", "immissionen", "laerm",
        "steuer", "asyl",
    },
    "fr": {
        "arrt", "arret", "arrêt", "permis", "construire", "droit", "impot",
        "impt", "asile", "renvoi", "jugement", "tribunal",
    },
    "it": {
        "sentenza", "ricorso", "responsabilita", "responsabilità", "danno",
        "morale", "asilo", "allontanamento", "imposta", "diritto", "tribunale",
    },
}

QUERY_STATUTE_PATTERN = re.compile(
    r"""
    \b(?:Art\.?|Artikel)\s*
    (?P<article>\d+(?:\s*(?:bis|ter|quater|quinquies|sexies)|[a-z](?![a-z]))?)\s*
    (?:(?:Abs\.?|Absatz|al\.?|alin(?:ea)?\.?|cpv\.?|co\.?|para\.?)\s*(?P<paragraph>\d+(?:\s*(?:bis|ter|quater|quinquies|sexies)|[a-z](?![a-z]))?))?\s*
    (?P<law>[A-Z][A-Z0-9]{1,11}(?:/[A-Z0-9]{2,6})?)
    \b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)
QUERY_STATUTE_INVALID_LAWS = {
    "AL",
    "ABS",
    "ABSATZ",
    "ALIN",
    "ALINEA",
    "CPV",
    "PARA",
    "BIS",
    "TER",
    "QUATER",
    "QUINQUIES",
    "SEXIES",
}
QUERY_BGE_PATTERN = re.compile(
    r"\bBGE\s+\d{2,3}\s+[IVX]{1,4}\s+\d{1,4}\b",
    flags=re.IGNORECASE,
)
QUERY_DOCKET_PATTERNS = [
    re.compile(r"\b[A-Z0-9]{1,4}[._-]\d{1,6}[/_]\d{4}\b", flags=re.IGNORECASE),
    re.compile(r"\b[A-Z]{1,6}\.\d{4}\.\d{1,6}\b", flags=re.IGNORECASE),
]

_CROSS_ENCODER = None
_CROSS_ENCODER_FAILED = False


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
    last_error = None
    for _ in range(3):
        try:
            conn = sqlite3.connect(
                f"file:{DB_PATH}?mode=ro",
                uri=True,
                check_same_thread=False,
                timeout=1.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only = ON")  # read-only for safety
            return conn
        except sqlite3.OperationalError as e:
            last_error = e
            time.sleep(0.2)

    raise sqlite3.OperationalError(
        f"Unable to open SQLite database at {DB_PATH}: {last_error}"
    )


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

    is_docket_query = _looks_like_docket_query(fts_query)
    has_explicit_syntax = _has_explicit_fts_syntax(fts_query)
    inline_docket_candidates = _extract_inline_docket_candidates(fts_query)
    inline_docket_results: list[dict] = []

    # Docket-style lookups should prioritize exact/near-exact docket matches.
    if is_docket_query:
        try:
            docket_results = _search_by_docket(conn, fts_query, where, params, limit)
            if docket_results:
                return docket_results
        except sqlite3.OperationalError as e:
            logger.debug("Docket-first query failed, falling back to FTS: %s", e)
    if inline_docket_candidates:
        per_docket_limit = max(4, min(limit, 10))
        for candidate in inline_docket_candidates[:3]:
            try:
                inline_docket_results.extend(
                    _search_by_docket(conn, candidate, where, params, per_docket_limit)
                )
            except sqlite3.OperationalError as e:
                logger.debug("Inline docket lookup failed for %s: %s", candidate, e)
                continue
        inline_docket_results = _dedupe_results_by_decision_id(inline_docket_results)

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
            d.full_text AS full_text_raw,
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
        candidate_meta: dict[str, dict] = {}
        strategies = _build_query_strategies(fts_query)
        target_pool = _target_candidate_pool(
            limit=limit,
            is_docket=is_docket_query,
            has_explicit_syntax=has_explicit_syntax,
        )

        for idx, strategy in enumerate(strategies):
            match_query = strategy["query"]
            strategy_name = strategy.get("name", "")
            strategy_weight = float(strategy.get("weight", 1.0))
            expensive_strategy = strategy_name in {"nl_or", "nl_or_expanded"}
            early_enough = max(limit * 2, 20)
            if expensive_strategy and len(candidate_meta) >= early_enough:
                break
            if expensive_strategy and _query_has_numeric_terms(fts_query):
                continue
            try:
                candidate_limit = min(max(target_pool, limit * 2), MAX_RERANK_CANDIDATES)
                if strategy_name in {"regeste_focus", "title_focus"}:
                    candidate_limit = min(
                        MAX_RERANK_CANDIDATES,
                        max(candidate_limit, target_pool * 4),
                    )
                rows = conn.execute(
                    sql,
                    [match_query] + params + [candidate_limit],
                ).fetchall()
                had_success = True
            except sqlite3.OperationalError as e:
                logger.debug(
                    "FTS query failed, trying next strategy: %s (%s)",
                    _truncate(match_query, 120),
                    e,
                )
                continue

            for rank, row in enumerate(rows, start=1):
                decision_id = row["decision_id"]
                current = candidate_meta.get(decision_id)
                if current is None:
                    current = {
                        "row": row,
                        "best_bm25": _to_float(row["bm25_score"]),
                        "rrf_score": 0.0,
                        "strategy_hits": 0,
                    }
                    candidate_meta[decision_id] = current

                bm25 = _to_float(row["bm25_score"])
                if bm25 < float(current["best_bm25"]):
                    current["best_bm25"] = bm25
                    current["row"] = row

                current["rrf_score"] = float(current["rrf_score"]) + (
                    strategy_weight / (RRF_RANK_CONSTANT + rank)
                )
                current["strategy_hits"] = int(current["strategy_hits"]) + 1

            if len(candidate_meta) >= target_pool:
                # We already have enough candidates for reranking.
                break
            if idx == 0 and has_explicit_syntax and len(candidate_meta) >= limit:
                # For explicit syntax, one successful strategy with enough top-k
                # candidates is typically sufficient.
                break

        if candidate_meta:
            rows_for_rerank = [m["row"] for m in candidate_meta.values()]
            fusion_scores = {
                did: {
                    "rrf_score": float(meta["rrf_score"]),
                    "strategy_hits": int(meta["strategy_hits"]),
                }
                for did, meta in candidate_meta.items()
            }
            reranked = _rerank_rows(
                rows_for_rerank,
                fts_query,
                limit,
                fusion_scores=fusion_scores,
            )
            if inline_docket_results:
                return _merge_priority_results(
                    primary=inline_docket_results,
                    secondary=reranked,
                    limit=limit,
                )
            return reranked

        # All strategies executed but none returned results.
        # Return empty list (never propagate parser errors to user queries).
        if had_success:
            if inline_docket_results:
                return inline_docket_results[:limit]
            return []
        if inline_docket_results:
            return inline_docket_results[:limit]
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
    variants = _build_docket_variants(raw_query)
    if not variants:
        return []
    preferred_courts = _detect_query_preferred_courts(raw_query)

    exact_variants = sorted(variants)
    exact_placeholders = ",".join("?" for _ in exact_variants)
    rank_expr = f"CASE WHEN d.docket_number IN ({exact_placeholders}) THEN 0 ELSE 1 END"

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
            ({rank_expr}) AS docket_rank
        FROM decisions d
        WHERE d.docket_number IN ({exact_placeholders}){where}
        ORDER BY docket_rank ASC,
                 d.decision_date DESC
        LIMIT ?
    """
    sql_limit = max(limit * 4, limit)
    rows = conn.execute(
        sql,
        [
            *exact_variants,
            *exact_variants,
            *params,
            sql_limit,
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
    if preferred_courts:
        results.sort(
            key=lambda r: (
                0 if (r.get("court") or "").lower() in preferred_courts else 1,
                -_date_sort_key(str(r.get("decision_date") or "")),
                str(r.get("decision_id") or ""),
            ),
        )
    return results[:limit]


def _build_docket_variants(raw_query: str) -> set[str]:
    q = re.sub(r"\s+", "", (raw_query or ""))
    if not q:
        return set()
    variants = {
        q,
        q.upper(),
        q.replace("_", "/"),
        q.replace("-", "/"),
        q.replace(".", "/"),
        q.replace("/", "_"),
        q.replace("-", "_"),
        q.replace(".", "_"),
        q.replace("/", "-"),
        q.replace("_", "-"),
        q.replace(".", "-"),
    }
    clean: set[str] = set()
    for v in variants:
        v = re.sub(r"[/_.-]{2,}", lambda m: m.group(0)[0], v).strip("/_.-")
        if v:
            clean.add(v)
    return clean


def _detect_query_preferred_courts(query: str) -> set[str]:
    text = _normalize_text_for_match(query)
    if not text:
        return set()
    preferred: set[str] = set()
    for court, hints in COURT_QUERY_HINTS.items():
        for hint in hints:
            norm_hint = _normalize_text_for_match(hint)
            if norm_hint and norm_hint in text:
                preferred.add(court)
                preferred.update(COURT_QUERY_EXPANSIONS.get(court, ()))
                break
    return preferred


def _extract_inline_docket_candidates(query: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for pattern in QUERY_DOCKET_PATTERNS:
        for match in pattern.finditer(query or ""):
            raw = (match.group(0) or "").strip()
            norm = _normalize_docket_ref(raw)
            if not raw or len(norm) < 5 or norm in seen:
                continue
            seen.add(norm)
            out.append(raw)
            if len(out) >= 5:
                return out
    return out


def _dedupe_results_by_decision_id(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        did = row.get("decision_id")
        if not did or did in seen:
            continue
        seen.add(did)
        out.append(row)
    return out


def _merge_priority_results(
    *,
    primary: list[dict],
    secondary: list[dict],
    limit: int,
) -> list[dict]:
    merged = _dedupe_results_by_decision_id((primary or []) + (secondary or []))
    return merged[: max(1, limit)]


def _extract_query_statute_refs(query: str) -> set[str]:
    refs: set[str] = set()
    for match in QUERY_STATUTE_PATTERN.finditer(query or ""):
        article = re.sub(r"\s+", "", (match.group("article") or "").lower())
        if not article:
            continue
        paragraph_raw = match.group("paragraph") or ""
        paragraph = re.sub(r"\s+", "", paragraph_raw.lower()) or None
        law = (match.group("law") or "").upper()
        if not law or law in QUERY_STATUTE_INVALID_LAWS:
            continue
        refs.add(f"ART.{article}.{law}")
        if paragraph:
            refs.add(f"ART.{article}.ABS.{paragraph}.{law}")
    return refs


def _extract_query_citation_refs(query: str) -> set[str]:
    refs: set[str] = set()
    q = query or ""

    for match in QUERY_BGE_PATTERN.finditer(q):
        text = re.sub(r"\s+", " ", match.group(0).strip().upper())
        refs.add(text)

    for pattern in QUERY_DOCKET_PATTERNS:
        for match in pattern.finditer(q):
            normalized = _normalize_docket_ref(match.group(0))
            if normalized:
                refs.add(normalized)

    if _looks_like_docket_query(q):
        normalized = _normalize_docket_ref(q)
        if normalized:
            refs.add(normalized)

    return refs


def _normalize_docket_ref(value: str) -> str:
    text = (value or "").strip().upper()
    if not text:
        return ""
    text = text.replace("-", "_").replace(".", "_").replace("/", "_")
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _sqlite_has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _sqlite_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return False
    return any(str(r[1]).lower() == column.lower() for r in rows)


def _load_graph_signal_map(
    decision_ids: list[str],
    *,
    query_statutes: set[str],
    query_citations: set[str],
) -> dict[str, dict[str, int]]:
    if not GRAPH_SIGNALS_ENABLED or not decision_ids:
        return {}
    if not GRAPH_DB_PATH.exists():
        return {}

    unique_ids = list(dict.fromkeys([did for did in decision_ids if did]))
    if not unique_ids:
        return {}

    signal_map: dict[str, dict[str, int]] = {
        did: {
            "statute_mentions": 0,
            "query_citation_hits": 0,
            "incoming_citations": 0,
        }
        for did in unique_ids
    }

    conn = None
    try:
        conn = sqlite3.connect(str(GRAPH_DB_PATH), timeout=0.5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        has_citation_targets = _sqlite_has_table(conn, "citation_targets")
        has_legacy_target_column = _sqlite_has_column(
            conn, "decision_citations", "target_decision_id"
        )
        has_confidence_score = (
            has_citation_targets
            and _sqlite_has_column(conn, "citation_targets", "confidence_score")
        )

        placeholders = ",".join("?" for _ in unique_ids)
        if query_statutes:
            statute_refs = sorted(query_statutes)
            statute_placeholders = ",".join("?" for _ in statute_refs)
            rows = conn.execute(
                f"""
                SELECT decision_id, SUM(mention_count) AS n
                FROM decision_statutes
                WHERE decision_id IN ({placeholders})
                  AND statute_id IN ({statute_placeholders})
                GROUP BY decision_id
                """,
                tuple(unique_ids) + tuple(statute_refs),
            ).fetchall()
            for row in rows:
                signal_map[row["decision_id"]]["statute_mentions"] = int(row["n"] or 0)

        if query_citations:
            citation_refs = sorted(query_citations)
            citation_placeholders = ",".join("?" for _ in citation_refs)
            rows = conn.execute(
                f"""
                SELECT source_decision_id AS decision_id, SUM(mention_count) AS n
                FROM decision_citations
                WHERE source_decision_id IN ({placeholders})
                  AND target_ref IN ({citation_placeholders})
                GROUP BY source_decision_id
                """,
                tuple(unique_ids) + tuple(citation_refs),
            ).fetchall()
            for row in rows:
                signal_map[row["decision_id"]]["query_citation_hits"] = int(row["n"] or 0)

        if has_citation_targets:
            if has_confidence_score:
                rows = conn.execute(
                    f"""
                    SELECT
                        ct.target_decision_id AS decision_id,
                        SUM(dc.mention_count * COALESCE(ct.confidence_score, 1.0)) AS n
                    FROM citation_targets ct
                    JOIN decision_citations dc
                      ON dc.source_decision_id = ct.source_decision_id
                     AND dc.target_ref = ct.target_ref
                    WHERE ct.target_decision_id IN ({placeholders})
                    GROUP BY ct.target_decision_id
                    """,
                    tuple(unique_ids),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT ct.target_decision_id AS decision_id, SUM(dc.mention_count) AS n
                    FROM citation_targets ct
                    JOIN decision_citations dc
                      ON dc.source_decision_id = ct.source_decision_id
                     AND dc.target_ref = ct.target_ref
                    WHERE ct.target_decision_id IN ({placeholders})
                    GROUP BY ct.target_decision_id
                    """,
                    tuple(unique_ids),
                ).fetchall()
        elif has_legacy_target_column:
            rows = conn.execute(
                f"""
                SELECT target_decision_id AS decision_id, SUM(mention_count) AS n
                FROM decision_citations
                WHERE target_decision_id IN ({placeholders})
                GROUP BY target_decision_id
                """,
                tuple(unique_ids),
            ).fetchall()
        else:
            rows = []
        for row in rows:
            signal_map[row["decision_id"]]["incoming_citations"] = max(
                0,
                int(round(float(row["n"] or 0))),
            )
    except sqlite3.Error as e:
        logger.debug("Graph-signal lookup failed: %s", e)
        return {}
    finally:
        if conn is not None:
            conn.close()

    return signal_map


def _text_matches_any_statute_hint(text: str, statutes: set[str]) -> bool:
    for ref in statutes:
        article, paragraph, law = _parse_statute_ref(ref)
        if not article or not law:
            continue
        hints = [
            f"art {article} {law.lower()}",
            f"{article} {law.lower()}",
        ]
        if paragraph:
            hints.extend(
                [
                    f"abs {paragraph}",
                    f"al {paragraph}",
                    f"cpv {paragraph}",
                    f"co {paragraph}",
                    f"alin {paragraph}",
                ]
            )
        if any(hint in text for hint in hints):
            return True
    return False


def _parse_statute_ref(ref: str) -> tuple[str | None, str | None, str | None]:
    m = re.match(
        r"^ART\.(?P<article>\d+[a-z]?)(?:\.ABS\.(?P<paragraph>\d+[a-z]?))?\.(?P<law>[A-Z0-9/]+)$",
        ref,
    )
    if not m:
        return None, None, None
    return m.group("article"), m.group("paragraph"), m.group("law")


def _text_matches_any_citation_hint(text: str, citations: set[str]) -> bool:
    for ref in citations:
        ref_text = ref.lower().replace("_", " ")
        if ref_text and ref_text in text:
            return True
    return False


def _rerank_rows(
    rows: list[sqlite3.Row],
    raw_query: str,
    limit: int,
    *,
    fusion_scores: dict[str, dict] | None = None,
) -> list[dict]:
    """
    Re-rank lexical FTS candidates with lightweight query-intent signals.

    The FTS index provides robust candidate retrieval; this stage improves top-k
    quality for practitioner-style natural-language and docket-centric queries.
    """
    if not rows:
        return []

    fusion_scores = fusion_scores or {}
    rank_terms = _extract_rank_terms(raw_query)
    expanded_rank_terms = _expand_rank_terms_for_match(rank_terms)
    all_rank_terms = set(rank_terms) | set(expanded_rank_terms)
    query_has_asyl_signal = any(t in ASYL_QUERY_TERMS for t in rank_terms)
    query_has_decision_intent = any(t in DECISION_INTENT_TERMS for t in rank_terms)
    query_has_accelerated_signal = any(
        t in ACCELERATED_PROCEDURE_TERMS or t.startswith("beschleunig")
        for t in all_rank_terms
    )
    query_languages = set(_detect_query_languages(raw_query))
    cleaned_phrase = _normalize_text_for_match(_clean_for_phrase(raw_query))
    query_norm = _normalize_docket(raw_query)
    query_statutes = _extract_query_statute_refs(raw_query)
    query_citations = _extract_query_citation_refs(raw_query)
    graph_signals = _load_graph_signal_map(
        [r["decision_id"] for r in rows],
        query_statutes=query_statutes,
        query_citations=query_citations,
    )

    scored: list[tuple[float, float, int, sqlite3.Row]] = []
    for idx, row in enumerate(rows):
        decision_id = row["decision_id"]
        bm25_score = _to_float(row["bm25_score"])
        bm25_component = -bm25_score

        title_text = _normalize_text_for_match(row["title"])
        regeste_text = _normalize_text_for_match(row["regeste"])
        snippet_text = _normalize_text_for_match(row["snippet"])
        docket_text = (row["docket_number"] or "").lower()
        docket_norm = _normalize_docket(docket_text)

        if rank_terms:
            title_cov = _term_coverage(rank_terms, title_text)
            regeste_cov = _term_coverage(rank_terms, regeste_text)
            snippet_cov = _term_coverage(rank_terms, snippet_text)
        else:
            title_cov = regeste_cov = snippet_cov = 0.0
        if expanded_rank_terms:
            expanded_title_cov = _term_coverage(expanded_rank_terms, title_text)
            expanded_regeste_cov = _term_coverage(expanded_rank_terms, regeste_text)
        else:
            expanded_title_cov = expanded_regeste_cov = 0.0

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

        fusion = fusion_scores.get(decision_id, {})
        rrf_score = float(fusion.get("rrf_score", 0.0))
        strategy_hits = int(fusion.get("strategy_hits", 0))

        graph = graph_signals.get(decision_id, {})
        statute_mentions = int(graph.get("statute_mentions", 0))
        query_citation_hits = int(graph.get("query_citation_hits", 0))
        incoming_citations = int(graph.get("incoming_citations", 0))

        statute_signal = 0.0
        citation_signal = 0.0
        authority_signal = 0.0
        if query_statutes and statute_mentions > 0:
            statute_signal = 2.2 + min(1.2, 0.25 * statute_mentions)
        if query_citations and query_citation_hits > 0:
            citation_signal = 2.4 + min(1.2, 0.30 * query_citation_hits)
        if incoming_citations > 0:
            authority_signal = min(1.0, incoming_citations * 0.03)

        local_ref_signal = 0.0
        local_text = f"{title_text} {regeste_text} {snippet_text}"
        if query_statutes and _text_matches_any_statute_hint(local_text, query_statutes):
            local_ref_signal += 0.8
        if query_citations and _text_matches_any_citation_hint(local_text, query_citations):
            local_ref_signal += 0.8

        court_prior_signal = 0.0
        if query_has_asyl_signal:
            court = (row["court"] or "").lower()
            docket = (row["docket_number"] or "")
            if court == "bvger":
                court_prior_signal += 1.7
            if court == "bger":
                court_prior_signal -= 0.2
            if docket.upper().startswith("E-"):
                court_prior_signal += 0.45

        court_intent_signal = 0.0
        if query_has_decision_intent:
            court = (row["court"] or "").lower()
            if court in HIGH_COURTS:
                court_intent_signal += 0.65

        procedure_signal = 0.0
        if query_has_asyl_signal and query_has_accelerated_signal:
            if any(term in local_text for term in ACCELERATED_PROCEDURE_TERMS):
                procedure_signal += 0.9

        language_signal = 0.0
        row_language = (row["language"] or "").lower()
        if query_languages and row_language in query_languages:
            language_signal += 0.9

        signal = (
            6.0 * docket_exact
            + 2.0 * docket_partial
            + 3.0 * title_cov
            + 2.2 * regeste_cov
            + 0.8 * snippet_cov
            + 1.2 * expanded_regeste_cov
            + 0.8 * expanded_title_cov
            + 1.8 * phrase_hit
            + 32.0 * rrf_score
            + 0.18 * min(strategy_hits, 8)
            + statute_signal
            + citation_signal
            + authority_signal
            + local_ref_signal
            + court_prior_signal
            + court_intent_signal
            + procedure_signal
            + language_signal
        )
        final_score = bm25_component + signal

        scored.append((final_score, bm25_score, idx, row))

    scored = _apply_cross_encoder_boosts(scored, raw_query)
    scored.sort(key=lambda x: (-x[0], x[1], x[2]))

    results: list[dict] = []
    for final_score, _bm25, _idx, row in scored[:limit]:
        full_text = _row_get(row, "full_text_raw")
        best_snippet = _select_best_passage_snippet(
            full_text,
            rank_terms=rank_terms,
            phrase=cleaned_phrase,
            fallback=row["snippet"],
        )
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
            "snippet": best_snippet,
            "source_url": row["source_url"],
            "pdf_url": row["pdf_url"],
            "relevance_score": round(final_score, 4),
        })
    return results


def _build_query_strategies(raw_query: str) -> list[dict]:
    """
    Build parser-safe FTS query strategies.

    For explicit FTS syntax, preserve raw query first.
    For natural language, prefer tokenized OR query first for robustness.
    """
    raw = raw_query.strip()
    has_explicit_syntax = _has_explicit_fts_syntax(raw)
    nl_and = _build_nl_and_query(raw)
    nl_or = _build_nl_or_query(raw, include_expansions=False)
    nl_or_expanded = _build_nl_or_query(raw, include_expansions=True)
    anchor_focus = _build_anchor_pair_strategies(raw)
    regeste_focus = _build_field_focus_query(raw, field="regeste")
    title_focus = _build_field_focus_query(raw, field="title")
    detected_languages = _detect_query_languages(raw)
    language_focus = _build_language_focus_strategies(
        raw,
        detected_languages=detected_languages,
        has_explicit_syntax=has_explicit_syntax,
    )
    cleaned = _clean_for_phrase(raw)
    quoted = f'"{cleaned}"' if cleaned else ""

    if has_explicit_syntax:
        candidates = [
            {"name": "raw", "query": raw, "weight": 1.5},
            {"name": "quoted", "query": quoted, "weight": 1.1},
            {"name": "regeste_focus", "query": regeste_focus, "weight": 0.95},
            {"name": "title_focus", "query": title_focus, "weight": 0.85},
            *anchor_focus,
            *language_focus,
            {"name": "nl_and", "query": nl_and, "weight": 0.9},
            {"name": "nl_or", "query": nl_or, "weight": 0.7},
        ]
    else:
        candidates = [
            *anchor_focus,
            {"name": "nl_and", "query": nl_and, "weight": 1.3},
            {"name": "regeste_focus", "query": regeste_focus, "weight": 1.05},
            {"name": "title_focus", "query": title_focus, "weight": 0.95},
            *language_focus,
            {"name": "quoted", "query": quoted, "weight": 1.15},
            {"name": "nl_or", "query": nl_or, "weight": 1.0},
            {"name": "nl_or_expanded", "query": nl_or_expanded, "weight": 0.85},
        ]
        if _should_try_raw_fallback(raw):
            candidates.append({"name": "raw_fallback", "query": raw, "weight": 0.65})

    # Dedupe while preserving order
    seen: set[str] = set()
    strategies: list[dict] = []
    for candidate in candidates:
        q = (candidate.get("query") or "").strip()
        if q and q not in seen:
            strategies.append({
                "name": candidate.get("name", "query"),
                "query": q,
                "weight": float(candidate.get("weight", 1.0)),
            })
            seen.add(q)
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


def _query_has_numeric_terms(query: str) -> bool:
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query or "")
    return any(tok.isdigit() for tok in tokens)


def _clean_for_phrase(query: str) -> str:
    """Normalize punctuation/quotes for safe phrase fallback."""
    terms = _extract_query_terms(
        query,
        limit=MAX_NL_TOKENS,
        include_variants=False,
        include_expansions=False,
    )
    return " ".join(terms)


def _build_nl_or_query(query: str, *, include_expansions: bool) -> str:
    """Tokenize natural-language input into a robust OR-based FTS query."""
    terms = _extract_query_terms(
        query,
        limit=MAX_NL_TOKENS,
        include_variants=True,
        include_expansions=include_expansions,
    )
    return " OR ".join(terms)


def _build_nl_and_query(query: str) -> str:
    """Tokenize natural-language input into a stricter AND query."""
    keep = _extract_query_terms(
        query,
        limit=NL_AND_TERM_LIMIT,
        include_variants=False,
        include_expansions=False,
    )

    if len(keep) < 2:
        return ""
    return " AND ".join(keep)


def _build_anchor_pair_strategies(query: str) -> list[dict]:
    terms = _extract_query_terms(
        query,
        limit=MAX_NL_TOKENS,
        include_variants=False,
        include_expansions=False,
    )
    if len(terms) < 2:
        return []

    pairs = _pick_anchor_pairs(terms)
    if not pairs:
        return []

    out: list[dict] = []
    for idx, (left, right) in enumerate(pairs, start=1):
        out.append({
            "name": f"anchor_pair_{idx}",
            "query": f"{left} AND {right}",
            "weight": 1.2 if idx == 1 else 1.0,
        })
        if len(left) >= 4 and len(right) >= 4:
            out.append({
                "name": f"anchor_phrase_{idx}",
                "query": f'"{left} {right}"',
                "weight": 0.85,
            })
    return out


def _pick_anchor_pairs(terms: list[str]) -> list[tuple[str, str]]:
    term_set = set(terms)
    out: list[tuple[str, str]] = []

    for left, right in LEGAL_ANCHOR_PAIRS:
        if left in term_set and right in term_set:
            out.append((left, right))
            if len(out) >= 2:
                return out

    return out[:2]


def _build_field_focus_query(query: str, *, field: str) -> str:
    terms = _extract_query_terms(
        query,
        limit=6,
        include_variants=False,
        include_expansions=False,
    )
    safe_terms = [
        t for t in terms
        if t and re.fullmatch(r"[a-z0-9_]+", t) and not t.isdigit()
    ]
    if len(safe_terms) < 2:
        return ""
    core = safe_terms[:2]
    return " AND ".join(f"{field}:{_fts_prefix_term(term)}" for term in core)


def _build_language_focus_strategies(
    query: str,
    *,
    detected_languages: list[str],
    has_explicit_syntax: bool,
) -> list[dict]:
    out: list[dict] = []
    if not detected_languages:
        return out

    for lang in detected_languages[:2]:
        and_query = _build_language_focus_query(query, language=lang, mode="and")
        or_query = _build_language_focus_query(query, language=lang, mode="or")
        if and_query:
            out.append({"name": f"lang_{lang}_and", "query": and_query, "weight": 1.1})
        if or_query and not has_explicit_syntax:
            out.append({"name": f"lang_{lang}_or", "query": or_query, "weight": 0.8})
    return out


def _build_language_focus_query(query: str, *, language: str, mode: str) -> str:
    if mode == "and":
        base = _build_nl_and_query(query)
    else:
        base = _build_nl_or_query(query, include_expansions=False)
    if not base:
        return ""
    return f"language:{language} AND ({base})"


def _fts_prefix_term(term: str) -> str:
    # Prefix search improves recall for German compounds (e.g., asyl* -> Asylgesuch).
    if len(term) >= 4 and not term.endswith("*"):
        return f"{term}*"
    return term


def _detect_query_languages(query: str) -> list[str]:
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", (query or "").lower())
    normalized = [_normalize_token_for_fts(t) for t in tokens]
    normalized = [t for t in normalized if t]
    if not normalized:
        return []

    scores: dict[str, int] = {lang: 0 for lang in LANGUAGE_HINT_TERMS}
    for tok in normalized:
        for lang, hints in LANGUAGE_HINT_TERMS.items():
            if tok in hints:
                scores[lang] += 2
    for tok in normalized:
        if tok in NL_STOPWORDS:
            continue
        if tok.endswith("tion") or tok.endswith("mente"):
            scores["fr"] += 1
            scores["it"] += 1
        if tok.endswith("ung") or tok.endswith("keit"):
            scores["de"] += 1

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    if not ranked or ranked[0][1] <= 0:
        return []

    top_score = ranked[0][1]
    out: list[str] = []
    for lang, score in ranked:
        if score <= 0:
            break
        if score >= max(1, top_score - 2):
            out.append(lang)
        if len(out) >= 2:
            break
    return out


def _extract_rank_terms(query: str) -> list[str]:
    """Extract deduplicated content-bearing terms for second-pass reranking."""
    tokens = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower())
    terms: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        if tok in NL_STOPWORDS:
            continue
        norm_tok = _normalize_token_for_match(tok)
        if not norm_tok:
            continue
        if norm_tok in FTS_COLUMNS:
            continue
        if norm_tok in {"and", "or", "not", "near"}:
            continue
        if not norm_tok.isdigit() and len(norm_tok) < 3:
            continue
        if norm_tok in seen:
            continue
        terms.append(norm_tok)
        seen.add(norm_tok)
        if len(terms) >= RERANK_TERM_LIMIT:
            break
    return terms


def _expand_rank_terms_for_match(terms: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set(terms)
    for term in terms:
        for expansion in _get_query_expansions(term):
            normalized = _normalize_token_for_match(expansion)
            if not normalized:
                continue
            if normalized in seen:
                continue
            if not normalized.isdigit() and len(normalized) < 3:
                continue
            out.append(normalized)
            seen.add(normalized)
            if len(out) >= RERANK_TERM_LIMIT:
                return out
    return out


def _term_coverage(terms: list[str], text: str) -> float:
    """Fraction of query terms appearing in text."""
    if not terms:
        return 0.0
    hits = sum(1 for t in terms if t in text)
    return hits / len(terms)


def _target_candidate_pool(*, limit: int, is_docket: bool, has_explicit_syntax: bool) -> int:
    pool = max(MIN_CANDIDATE_POOL, limit * TARGET_POOL_MULTIPLIER)
    if has_explicit_syntax:
        pool = max(pool, limit * 2)
    if is_docket:
        pool = max(pool, DOCKET_MIN_CANDIDATE_POOL)
    return min(pool, MAX_RERANK_CANDIDATES)


def _should_try_raw_fallback(query: str) -> bool:
    # Raw queries with punctuation frequently trigger parser errors.
    return bool(re.fullmatch(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_\s]+", query))


def _extract_query_terms(
    query: str,
    *,
    limit: int,
    include_variants: bool,
    include_expansions: bool,
) -> list[str]:
    """Extract deduplicated FTS-safe terms from a natural-language query."""
    keep: list[str] = []
    seen: set[str] = set()
    for tok in re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", query.lower()):
        if tok in NL_STOPWORDS:
            continue
        normalized = _normalize_token_for_fts(tok)
        if not normalized:
            continue
        if not normalized.isdigit() and len(normalized) < 3:
            continue
        variants = [normalized]
        if include_variants:
            alt = _collapse_umlaut_variants(normalized)
            if alt and alt != normalized:
                variants.append(alt)
        if include_expansions:
            for expansion in _get_query_expansions(normalized):
                if expansion and expansion not in variants:
                    variants.append(expansion)
        for term in variants:
            if term in seen:
                continue
            keep.append(term)
            seen.add(term)
            if len(keep) >= limit:
                return keep
    return keep


def _get_query_expansions(term: str) -> list[str]:
    expansions = LEGAL_QUERY_EXPANSIONS.get(term, ())
    out: list[str] = []
    for exp in expansions[:MAX_EXPANSIONS_PER_TERM]:
        normalized = _normalize_token_for_fts(exp)
        if normalized and normalized != term:
            out.append(normalized)
    return out


def _normalize_token_for_fts(token: str) -> str:
    token = token.strip().lower()
    if not token:
        return ""
    token = (
        token
        .replace("ß", "ss")
        .replace("æ", "ae")
        .replace("œ", "oe")
    )
    token = unicodedata.normalize("NFKD", token)
    token = "".join(ch for ch in token if not unicodedata.combining(ch))
    token = re.sub(r"[^0-9a-z_]+", "", token)
    return token


def _normalize_token_for_match(token: str) -> str:
    token = _normalize_token_for_fts(token)
    return _collapse_umlaut_variants(token)


def _collapse_umlaut_variants(token: str) -> str:
    return token.replace("ae", "a").replace("oe", "o").replace("ue", "u")


def _normalize_text_for_match(text: str | None) -> str:
    if not text:
        return ""
    normalized_tokens: list[str] = []
    for tok in re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_]+", text.lower()):
        norm = _normalize_token_for_match(tok)
        if norm:
            normalized_tokens.append(norm)
    return " ".join(normalized_tokens)


def _normalize_docket(value: str) -> str:
    """Normalize docket-like strings for exact/partial matching."""
    return re.sub(r"[^0-9a-z]+", "", (value or "").lower())


def _looks_like_docket_query(query: str) -> bool:
    """Heuristic: identify docket-number style queries."""
    q = query.strip()
    if not q:
        return False

    nonspace = re.sub(r"\s+", "", q)
    if not nonspace:
        return False

    if QUERY_BGE_PATTERN.fullmatch(q):
        return True
    for pattern in QUERY_DOCKET_PATTERNS:
        if pattern.fullmatch(q):
            return True

    # Accept only if a docket-like fragment dominates the whole query.
    for pattern in QUERY_DOCKET_PATTERNS:
        for match in pattern.finditer(q):
            fragment = re.sub(r"\s+", "", match.group(0))
            if len(fragment) / len(nonspace) >= 0.7:
                return True

    if re.fullmatch(r"[0-9]{1,4}\s+[A-Z]{1,4}\s+[0-9]{1,4}", q):
        return True
    return False


def _to_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 1e9


def _date_sort_key(value: str) -> int:
    text = (value or "").strip()
    if not text:
        return 0
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if not m:
        return 0
    try:
        return int(f"{m.group(1)}{m.group(2)}{m.group(3)}")
    except Exception:
        return 0


def _row_get(row: sqlite3.Row | dict, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _apply_cross_encoder_boosts(
    scored: list[tuple[float, float, int, sqlite3.Row]],
    query: str,
) -> list[tuple[float, float, int, sqlite3.Row]]:
    if not CROSS_ENCODER_ENABLED or not scored:
        return scored

    encoder = _get_cross_encoder()
    if encoder is None:
        return scored

    top_n = min(CROSS_ENCODER_TOP_N, len(scored))
    if top_n <= 0:
        return scored

    pre_sorted = sorted(scored, key=lambda x: (-x[0], x[1], x[2]))
    rerank_subset = pre_sorted[:top_n]
    pairs = [(query, _build_rerank_document(row)) for _s, _b, _i, row in rerank_subset]
    if not pairs:
        return scored

    try:
        raw_scores = encoder.predict(pairs)
    except Exception as e:
        logger.debug("Cross-encoder prediction failed: %s", e)
        return scored

    normalized = _normalize_score_list(raw_scores)
    ce_by_id = {
        row["decision_id"]: score
        for score, (_s, _b, _i, row) in zip(normalized, rerank_subset)
    }

    boosted: list[tuple[float, float, int, sqlite3.Row]] = []
    for score, bm25, idx, row in scored:
        ce_score = ce_by_id.get(row["decision_id"], 0.0)
        boosted.append((score + CROSS_ENCODER_WEIGHT * ce_score, bm25, idx, row))
    return boosted


def _get_cross_encoder():
    global _CROSS_ENCODER, _CROSS_ENCODER_FAILED
    if not CROSS_ENCODER_ENABLED:
        return None
    if _CROSS_ENCODER is not None:
        return _CROSS_ENCODER
    if _CROSS_ENCODER_FAILED:
        return None
    try:
        from sentence_transformers import CrossEncoder
    except Exception as e:
        logger.debug("sentence-transformers unavailable for cross-encoder reranking: %s", e)
        _CROSS_ENCODER_FAILED = True
        return None

    try:
        _CROSS_ENCODER = CrossEncoder(CROSS_ENCODER_MODEL)
        return _CROSS_ENCODER
    except Exception as e:
        logger.debug("Cross-encoder model load failed (%s): %s", CROSS_ENCODER_MODEL, e)
        _CROSS_ENCODER_FAILED = True
        return None


def _normalize_score_list(scores) -> list[float]:
    values = [float(s) for s in scores]
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if hi <= lo:
        return [0.5 for _ in values]
    span = hi - lo
    return [(v - lo) / span for v in values]


def _build_rerank_document(row: sqlite3.Row | dict) -> str:
    title = _row_get(row, "title") or ""
    regeste = _row_get(row, "regeste") or ""
    snippet = _row_get(row, "snippet") or ""
    full_text = (_row_get(row, "full_text_raw") or "").strip()
    if len(full_text) > FULL_TEXT_RERANK_CHARS:
        full_text = full_text[:FULL_TEXT_RERANK_CHARS]
    parts = [title, regeste, snippet, full_text]
    return " ".join(p for p in parts if p).strip()


def _select_best_passage_snippet(
    full_text: str | None,
    *,
    rank_terms: list[str],
    phrase: str,
    fallback: str | None,
) -> str | None:
    if not full_text:
        return fallback

    passages = _split_passages(full_text)
    if not passages:
        return fallback

    best_text = None
    best_score = -1.0
    for passage in passages:
        if not passage:
            continue
        normalized = _normalize_text_for_match(passage)
        if not normalized:
            continue
        term_hits = sum(1 for t in rank_terms if t in normalized)
        phrase_hit = 1 if phrase and phrase in normalized else 0
        density = term_hits / max(1, min(12, len(normalized.split())))
        score = (2.4 * phrase_hit) + term_hits + (4.0 * density)
        if score > best_score:
            best_score = score
            best_text = passage

    if best_text and best_score > 0:
        compact = re.sub(r"\s+", " ", best_text).strip()
        return _truncate(compact, MAX_SNIPPET_LEN)
    return fallback


def _split_passages(full_text: str) -> list[str]:
    text = (full_text or "").strip()
    if not text:
        return []

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
    if len(paragraphs) >= 2:
        return paragraphs[:40]

    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
    if len(sentences) <= PASSAGE_SENTENCE_WINDOW:
        return [text]

    out: list[str] = []
    for i in range(0, len(sentences), max(1, PASSAGE_SENTENCE_WINDOW // 2)):
        window = " ".join(sentences[i:i + PASSAGE_SENTENCE_WINDOW]).strip()
        if window:
            out.append(window)
        if len(out) >= 40:
            break
    return out


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


def draft_mock_decision(
    *,
    facts: str,
    question: str | None = None,
    preferred_language: str | None = None,
    deciding_court: str | None = None,
    statute_references: list[dict] | None = None,
    fedlex_urls: list[str] | None = None,
    clarifications: list[dict] | None = None,
    limit: int = 8,
) -> dict:
    """
    Build a structured mock-decision outline from facts using:
    - local Swiss caselaw retrieval
    - statute references (explicit + extracted from facts/question)
    - optional statute text enrichment from Fedlex
    """
    facts_text = (facts or "").strip()
    if not facts_text:
        raise ValueError("facts must not be empty")

    preferred_lang = ((preferred_language or "").strip().lower() or None)
    if preferred_lang and preferred_lang not in {"de", "fr", "it", "rm", "en"}:
        raise ValueError("preferred_language must be one of de, fr, it, rm, en")

    limit = max(3, min(int(limit or 8), MAX_FACT_DECISION_LIMIT))
    question_text = (question or "").strip()
    query_text = facts_text if not question_text else f"{facts_text}\n{question_text}"

    statute_requests = _collect_statute_requests(
        query_text=query_text,
        explicit_statutes=statute_references or [],
    )
    case_law = _retrieve_case_law_for_facts(
        query_text=query_text,
        statute_requests=statute_requests,
        preferred_language=preferred_lang,
        limit=limit,
    )
    statute_materials = _resolve_statute_materials(
        statute_requests=statute_requests,
        fedlex_urls=fedlex_urls or [],
        preferred_language=preferred_lang or "de",
    )

    facts_summary = _summarize_facts_text(facts_text)
    key_issues = _derive_key_issues(
        facts_text=facts_text,
        question_text=question_text,
        statute_requests=statute_requests,
        case_law=case_law,
    )
    clarification_questions = _build_clarification_questions(
        facts_text=facts_text,
        question_text=question_text,
        statute_requests=statute_requests,
    )
    clarification_answers = _normalize_clarification_answers(clarifications or [])
    high_priority_ids = [
        q["id"] for q in clarification_questions
        if q.get("priority") == "high"
    ]
    unanswered_high_priority = [
        qid for qid in high_priority_ids
        if not (clarification_answers.get(qid) or "").strip()
    ]
    can_conclude = len(unanswered_high_priority) == 0

    reasoning_steps = _build_reasoning_steps(
        statute_materials=statute_materials,
        case_law=case_law,
        can_conclude=can_conclude,
    )
    outcome_note = (
        _build_outcome_note(case_law=case_law, statute_materials=statute_materials)
        if can_conclude
        else (
            "No conclusion yet. Please answer the high-priority clarification "
            "questions first."
        )
    )

    return {
        "disclaimer": (
            "Research-only mock outline, not legal advice. "
            "Validate against current law and full judgments."
        ),
        "facts_summary": facts_summary,
        "question": question_text or None,
        "deciding_court": deciding_court or "unknown",
        "preferred_language": preferred_lang or "auto",
        "key_issues": key_issues,
        "clarification_gate": {
            "status": "ready_for_conclusion" if can_conclude else "needs_clarification",
            "required_high_priority": high_priority_ids,
            "unanswered_high_priority": unanswered_high_priority,
        },
        "clarifying_questions": clarification_questions,
        "clarification_answers": [
            {"id": qid, "answer": answer}
            for qid, answer in clarification_answers.items()
            if (answer or "").strip()
        ],
        "applicable_statutes": statute_materials,
        "relevant_case_law": case_law,
        "mock_decision": {
            "conclusion_ready": can_conclude,
            "outcome_note": outcome_note,
            "reasoning_steps": reasoning_steps,
            "essential_elements": [
                "Sachverhalt / faits pertinents / fatti rilevanti",
                "Zulässigkeit / recevabilité / ammissibilità",
                "Anwendbare Normen",
                "Subsumtion nach zentralen Tatbestandsmerkmalen",
                "Ergebnis / dispositif",
            ],
        },
    }


def _collect_statute_requests(
    *,
    query_text: str,
    explicit_statutes: list[dict],
) -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()

    def _add(law_code: str, article: str, paragraph: str | None):
        law = _normalize_statute_law_code(law_code)
        art = (article or "").strip().lower()
        para = (paragraph or "").strip().lower() or None
        if not law or not art:
            return
        key = f"{law}|{art}|{para or ''}"
        if key in seen:
            return
        seen.add(key)
        items.append({
            "law_code": law,
            "article": art,
            "paragraph": para,
            "ref": f"Art. {art}{(' Abs. ' + para) if para else ''} {law}",
        })

    for st in explicit_statutes or []:
        _add(
            str(st.get("law_code") or st.get("law") or ""),
            str(st.get("article") or ""),
            st.get("paragraph"),
        )

    for ref in _extract_query_statute_refs(query_text):
        article, paragraph, law = _parse_statute_ref(ref)
        if article and law:
            _add(law, article, paragraph)

    return items


def _normalize_statute_law_code(value: str) -> str:
    raw = (value or "").strip().upper()
    if not raw:
        return ""
    return re.sub(r"[^A-Z0-9/]+", "", raw)


def _retrieve_case_law_for_facts(
    *,
    query_text: str,
    statute_requests: list[dict],
    preferred_language: str | None,
    limit: int,
) -> list[dict]:
    pool_limit = min(MAX_LIMIT, max(limit * 3, 18))
    scored: dict[str, dict] = {}

    def _add(rows: list[dict], *, source: str, extra_score: float = 0.0):
        for rank, row in enumerate(rows, start=1):
            decision_id = row.get("decision_id")
            if not decision_id:
                continue
            base = float(row.get("relevance_score") or 0.0)
            rank_bonus = max(0.0, 1.0 - (rank - 1) * 0.04)
            lang_bonus = 0.35 if preferred_language and row.get("language") == preferred_language else 0.0
            score = base + rank_bonus + extra_score + lang_bonus
            current = scored.get(decision_id)
            if current is None or score > float(current["match_score"]):
                scored[decision_id] = {
                    "decision_id": decision_id,
                    "court": row.get("court"),
                    "decision_date": row.get("decision_date"),
                    "docket_number": row.get("docket_number"),
                    "language": row.get("language"),
                    "title": _truncate(row.get("title"), 240),
                    "regeste": _truncate(row.get("regeste"), 320),
                    "snippet": _truncate(row.get("snippet"), 360),
                    "source_url": row.get("source_url"),
                    "source_match": source,
                    "match_score": round(score, 4),
                }

    base_rows = search_fts5(query=query_text, limit=pool_limit)
    _add(base_rows, source="facts_query", extra_score=0.4)

    for st in statute_requests[:5]:
        q = f"Art. {st['article']} {st['law_code']}"
        if st.get("paragraph"):
            q = f"Art. {st['article']} Abs. {st['paragraph']} {st['law_code']}"
        rows = search_fts5(query=q, limit=min(25, pool_limit))
        _add(rows, source=f"statute_query:{st['law_code']}:{st['article']}", extra_score=0.55)

    graph_rows = _search_graph_decisions_for_statutes(statute_requests=statute_requests, limit=pool_limit)
    _add(graph_rows, source="statute_graph", extra_score=0.75)

    ranked = sorted(
        scored.values(),
        key=lambda r: (
            -float(r["match_score"]),
            str(r.get("decision_date") or ""),
            str(r.get("decision_id") or ""),
        ),
        reverse=False,
    )
    ranked.sort(key=lambda r: float(r["match_score"]), reverse=True)
    return ranked[:limit]


def _search_graph_decisions_for_statutes(*, statute_requests: list[dict], limit: int) -> list[dict]:
    if not statute_requests or not GRAPH_DB_PATH.exists():
        return []

    mentions: dict[str, int] = {}
    graph_conn = None
    try:
        graph_conn = sqlite3.connect(str(GRAPH_DB_PATH), timeout=0.5)
        graph_conn.row_factory = sqlite3.Row
        graph_conn.execute("PRAGMA query_only = ON")
        for st in statute_requests[:8]:
            law = st["law_code"]
            article = st["article"]
            paragraph = st.get("paragraph")
            if paragraph:
                rows = graph_conn.execute(
                    """
                    SELECT ds.decision_id, SUM(ds.mention_count) AS n
                    FROM decision_statutes ds
                    JOIN statutes s ON s.statute_id = ds.statute_id
                    WHERE s.law_code = ? AND s.article = ? AND IFNULL(s.paragraph, '') = ?
                    GROUP BY ds.decision_id
                    ORDER BY n DESC
                    LIMIT ?
                    """,
                    (law, article, paragraph, max(20, limit)),
                ).fetchall()
            else:
                rows = graph_conn.execute(
                    """
                    SELECT ds.decision_id, SUM(ds.mention_count) AS n
                    FROM decision_statutes ds
                    JOIN statutes s ON s.statute_id = ds.statute_id
                    WHERE s.law_code = ? AND s.article = ?
                    GROUP BY ds.decision_id
                    ORDER BY n DESC
                    LIMIT ?
                    """,
                    (law, article, max(20, limit)),
                ).fetchall()
            for row in rows:
                did = row["decision_id"]
                mentions[did] = mentions.get(did, 0) + int(row["n"] or 0)
    except sqlite3.Error as e:
        logger.debug("Graph statute lookup failed: %s", e)
        return []
    finally:
        if graph_conn is not None:
            graph_conn.close()

    ranked_ids = [
        did for did, _n in sorted(mentions.items(), key=lambda x: x[1], reverse=True)[:limit]
    ]
    if not ranked_ids:
        return []
    rows = _fetch_decision_rows_by_ids(ranked_ids)
    rows_by_id = {r["decision_id"]: r for r in rows}
    out: list[dict] = []
    for did in ranked_ids:
        row = rows_by_id.get(did)
        if not row:
            continue
        mention_count = mentions.get(did, 0)
        out.append({
            "decision_id": row["decision_id"],
            "court": row["court"],
            "decision_date": row["decision_date"],
            "docket_number": row["docket_number"],
            "language": row["language"],
            "title": row.get("title"),
            "regeste": row.get("regeste"),
            "snippet": row.get("regeste"),
            "source_url": row.get("source_url"),
            "relevance_score": 0.25 + min(2.0, mention_count * 0.1),
        })
    return out


def _fetch_decision_rows_by_ids(decision_ids: list[str]) -> list[dict]:
    ids = [d for d in dict.fromkeys(decision_ids) if d]
    if not ids:
        return []
    conn = get_db()
    try:
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT decision_id, court, decision_date, docket_number, language,
                   title, regeste, source_url
            FROM decisions
            WHERE decision_id IN ({placeholders})
            """,
            tuple(ids),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _resolve_statute_materials(
    *,
    statute_requests: list[dict],
    fedlex_urls: list[str],
    preferred_language: str,
) -> list[dict]:
    if not statute_requests:
        return []

    cache = _load_fedlex_cache()
    out: list[dict] = []
    dirty_cache = False
    for st in statute_requests[:8]:
        resolved = _resolve_fedlex_statute_article(
            law_code=st["law_code"],
            article=st["article"],
            paragraph=st.get("paragraph"),
            preferred_language=preferred_language,
            fedlex_urls=fedlex_urls,
            cache=cache,
        )
        out.append(resolved)
        if resolved.get("_cache_dirty"):
            dirty_cache = True

    if dirty_cache:
        _save_fedlex_cache(cache)
    for row in out:
        row.pop("_cache_dirty", None)
    return out


def _resolve_fedlex_statute_article(
    *,
    law_code: str,
    article: str,
    paragraph: str | None,
    preferred_language: str,
    fedlex_urls: list[str],
    cache: dict,
) -> dict:
    result = {
        "law_code": law_code,
        "article": article,
        "paragraph": paragraph,
        "ref": f"Art. {article}{(' Abs. ' + paragraph) if paragraph else ''} {law_code}",
        "fedlex_url": None,
        "text_excerpt": None,
        "status": "not_fetched",
        "_cache_dirty": False,
    }
    candidates = _fedlex_candidate_urls(
        law_code=law_code,
        preferred_language=preferred_language,
        explicit_urls=fedlex_urls,
    )
    if not candidates:
        result["status"] = "no_candidate_url"
        return result

    for url in candidates:
        cache_key = f"{url}|{law_code}|{article}|{paragraph or ''}|{preferred_language}"
        cached = cache.get(cache_key)
        if isinstance(cached, dict) and cached.get("text_excerpt"):
            result["fedlex_url"] = cached.get("fedlex_url") or url
            result["text_excerpt"] = cached["text_excerpt"]
            result["status"] = "cache_hit"
            return result

        fetched = _fetch_fedlex_article_text(
            url=url,
            article=article,
            paragraph=paragraph,
        )
        if fetched:
            result["fedlex_url"] = fetched.get("fedlex_url") or url
            result["text_excerpt"] = fetched.get("text_excerpt")
            result["status"] = "fetched"
            cache[cache_key] = {
                "fedlex_url": result["fedlex_url"],
                "text_excerpt": result["text_excerpt"],
                "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            result["_cache_dirty"] = True
            return result

    result["status"] = "fetch_failed"
    if candidates:
        result["fedlex_url"] = candidates[0]
    return result


def _fedlex_candidate_urls(
    *,
    law_code: str,
    preferred_language: str,
    explicit_urls: list[str],
) -> list[str]:
    lang = (preferred_language or "de").lower()
    out: list[str] = []
    seen: set[str] = set()

    def _add(url: str):
        u = (url or "").strip()
        if not u or u in seen:
            return
        seen.add(u)
        out.append(u)

    for url in explicit_urls:
        _add(url)
        _add(f"{url.rstrip('/')}/{lang}")

    base = FEDLEX_LAW_CODE_BASE_URLS.get(_normalize_statute_law_code(law_code))
    if base:
        _add(base)
        _add(f"{base.rstrip('/')}/{lang}")

    return out[:8]


def _fetch_fedlex_article_text(*, url: str, article: str, paragraph: str | None) -> dict | None:
    try:
        import requests
    except Exception:
        return None

    headers = {"User-Agent": FEDLEX_USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=FEDLEX_TIMEOUT_SECONDS)
        if resp.status_code >= 400 or not resp.text:
            return None
    except Exception:
        return None

    excerpt = _extract_article_excerpt_from_html(
        html=resp.text,
        article=article,
        paragraph=paragraph,
    )
    if not excerpt:
        return None
    return {
        "fedlex_url": resp.url or url,
        "text_excerpt": excerpt,
    }


def _extract_article_excerpt_from_html(*, html: str, article: str, paragraph: str | None) -> str | None:
    compact = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", html or "")
    compact = re.sub(r"(?s)<[^>]+>", " ", compact)
    compact = html_lib.unescape(compact)
    compact = re.sub(r"\s+", " ", compact).strip()
    if not compact:
        return None

    art = re.escape((article or "").strip())
    if not art:
        return None

    block_pattern = re.compile(
        rf"(Art\.?\s*{art}[a-zA-Z]?\b.*?)(?=Art\.?\s*\d+[a-zA-Z]?\b|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = block_pattern.search(compact)
    if not match:
        return None

    excerpt = match.group(1).strip()
    if paragraph:
        para_text = str(paragraph).strip().lower()
        if para_text and f"abs. {para_text}" not in excerpt.lower():
            # Keep the article block anyway; Fedlex formatting differs by language.
            pass

    return _truncate(excerpt, 1200)


def _load_fedlex_cache() -> dict:
    if not FEDLEX_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(FEDLEX_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_fedlex_cache(cache: dict):
    try:
        FEDLEX_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FEDLEX_CACHE_PATH.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.debug("Failed to persist Fedlex cache: %s", e)


def _summarize_facts_text(text: str) -> str:
    parts = [p.strip() for p in re.split(r"(?:\n+|(?<=[.!?])\s+)", text or "") if p.strip()]
    if not parts:
        return ""
    return " ".join(parts[:3])


def _derive_key_issues(
    *,
    facts_text: str,
    question_text: str,
    statute_requests: list[dict],
    case_law: list[dict],
) -> list[str]:
    issues: list[str] = []
    rank_terms = _extract_rank_terms(f"{facts_text} {question_text}")
    if statute_requests:
        refs = ", ".join(st["ref"] for st in statute_requests[:4])
        issues.append(f"Auslegung und Anwendung von {refs}.")
    if any(t in ASYL_QUERY_TERMS for t in rank_terms):
        issues.append("Materiell- und verfahrensrechtliche Anforderungen im Asyl-/Wegweisungskontext.")
    if case_law:
        courts = sorted({str(c.get("court") or "") for c in case_law[:6] if c.get("court")})
        if courts:
            issues.append(f"Einordnung in die publizierte Rechtsprechung ({', '.join(courts)}).")
    if not issues:
        issues.append("Subsumtion der Tatsachen unter die wahrscheinlich einschlägigen Normen.")
    return issues[:5]


def _build_clarification_questions(
    *,
    facts_text: str,
    question_text: str,
    statute_requests: list[dict],
) -> list[dict]:
    text = (facts_text + " " + question_text).lower()
    out: list[dict] = []

    def _add(question_id: str, prompt: str, why: str, priority: str = "high"):
        out.append({
            "id": question_id,
            "question": prompt,
            "why_it_matters": why,
            "priority": priority,
        })

    if not re.search(r"\b(20\d{2}|19\d{2})\b", text):
        _add(
            "timeline_dates",
            "What are the key dates (administrative decision, service date, appeal filing date)?",
            "Admissibility and deadline checks depend on exact timing.",
            "high",
        )

    if not re.search(
        r"\b(beschwerde|rekurs|einsprache|appeal|recours|ricorso|verfahren|proc[eé]dure)\b",
        text,
    ):
        _add(
            "procedural_posture",
            "What is the procedural posture (first instance, appeal, or extraordinary remedy)?",
            "Applicable standards and review scope differ by stage.",
            "high",
        )

    if not re.search(
        r"\b(beantragt|antrag|relief|conclusion|conclusions|demande|fordert|wants|seek)\b",
        text,
    ):
        _add(
            "requested_relief",
            "What exact relief is requested (annulment, remand, stay, damages, etc.)?",
            "The dispositive part must match the requested remedy.",
            "high",
        )

    if not re.search(r"\b(sem|kanton|tribunal|gericht|beh[oö]rde|autorit[eé])\b", text):
        _add(
            "issuing_authority",
            "Which authority/court issued the contested decision?",
            "Jurisdiction and legal basis depend on the issuing authority.",
            "medium",
        )

    rank_terms = set(_extract_rank_terms(text))
    if rank_terms.intersection(ASYL_QUERY_TERMS):
        if not re.search(
            r"\b(herkunft|nationalit[aä]t|ethnie|religion|origin|nationality|provenance)\b",
            text,
        ):
            _add(
                "asylum_profile",
                "What is the claimant's origin/profile relevant for asylum risk assessment?",
                "Risk assessment requires country/profile-specific facts.",
                "high",
            )
        if not re.search(
            r"\b(verfolg|gef[aä]hrd|risk|risque|danger|torture|persecution)\b",
            text,
        ):
            _add(
                "asylum_risk",
                "What concrete persecution or return risks are alleged and evidenced?",
                "Material asylum analysis turns on individualized risk.",
                "high",
            )

    if statute_requests and not re.search(
        r"\b(beweis|evidence|preuve|prova|akten|document|unterlagen)\b",
        text,
    ):
        _add(
            "evidence_status",
            "Which key evidence is available or disputed?",
            "Subsumption under statutes depends on proven facts.",
            "medium",
        )

    return out[:8]


def _normalize_clarification_answers(clarifications: list[dict]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in clarifications or []:
        if not isinstance(item, dict):
            continue
        qid = str(item.get("id") or "").strip()
        answer = str(item.get("answer") or "").strip()
        if qid and answer:
            out[qid] = answer
    return out


def _build_reasoning_steps(
    *,
    statute_materials: list[dict],
    case_law: list[dict],
    can_conclude: bool,
) -> list[str]:
    steps = [
        "Sachverhalt strukturieren und streitige Kerntatsachen festhalten.",
        "Zulässigkeit/Vorfragen prüfen (Zuständigkeit, Fristen, Beschwerdelegitimation).",
    ]
    if statute_materials:
        refs = ", ".join(st["ref"] for st in statute_materials[:4])
        steps.append(f"Normative Prüfung entlang der Normen: {refs}.")
    if case_law:
        top = ", ".join(
            f"{c.get('docket_number') or c.get('decision_id')}"
            for c in case_law[:3]
        )
        steps.append(f"Abgleich mit Leitlinien aus den ähnlichsten Entscheiden ({top}).")
    if can_conclude:
        steps.append("Ergebnis mit Begründungstiefe und offenem Risikoabschnitt formulieren.")
    else:
        steps.append(
            "Vorläufige Einordnung ohne Schlussfolgerung; zuerst offene Klärungsfragen beantworten."
        )
    return steps


def _build_outcome_note(*, case_law: list[dict], statute_materials: list[dict]) -> str:
    if not case_law:
        return "Zu wenige vergleichbare Entscheide für eine belastbare Tendenz."
    if statute_materials and any(st.get("text_excerpt") for st in statute_materials):
        return "Tendenz auf Basis ähnlicher Entscheide und verfügbarer Normtexte; Ergebnis bleibt fallabhängig."
    return "Tendenz nur auf Basis caselaw-Ähnlichkeit; Normtexte konnten nicht vollständig geladen werden."


def _format_mock_decision_report(report: dict) -> str:
    text = "# Mock Decision Outline\n"
    text += f"**Disclaimer:** {report.get('disclaimer')}\n\n"
    text += f"**Deciding court (hypothetical):** {report.get('deciding_court')}\n"
    text += f"**Language:** {report.get('preferred_language')}\n\n"
    text += "## Facts Summary\n"
    text += (report.get("facts_summary") or "-") + "\n\n"
    if report.get("question"):
        text += "## Question\n"
        text += report["question"] + "\n\n"

    text += "## Key Issues\n"
    for issue in report.get("key_issues", []):
        text += f"- {issue}\n"
    text += "\n"

    gate = report.get("clarification_gate") or {}
    text += "## Clarification Gate\n"
    text += f"- Status: {gate.get('status')}\n"
    unanswered = gate.get("unanswered_high_priority") or []
    if unanswered:
        text += f"- Unanswered high-priority IDs: {', '.join(unanswered)}\n"
    else:
        text += "- All high-priority clarification questions answered.\n"
    text += "\n"

    text += "## Clarifying Questions\n"
    questions = report.get("clarifying_questions") or []
    if not questions:
        text += "- No additional clarification questions identified.\n\n"
    else:
        for q in questions:
            text += f"- [{q.get('priority')}] {q.get('id')}: {q.get('question')}\n"
            if q.get("why_it_matters"):
                text += f"  Why: {q.get('why_it_matters')}\n"
        text += "\n"

    answers = report.get("clarification_answers") or []
    if answers:
        text += "## Clarification Answers Provided\n"
        for ans in answers:
            text += f"- {ans.get('id')}: {ans.get('answer')}\n"
        text += "\n"

    text += "## Applicable Statutes (Fedlex)\n"
    statutes = report.get("applicable_statutes") or []
    if not statutes:
        text += "- No statute references detected.\n\n"
    else:
        for st in statutes:
            text += f"- **{st.get('ref')}** ({st.get('status')})\n"
            if st.get("fedlex_url"):
                text += f"  Source: {st['fedlex_url']}\n"
            if st.get("text_excerpt"):
                text += f"  Excerpt: {st['text_excerpt']}\n"
        text += "\n"

    text += "## Most Relevant Case Law\n"
    cases = report.get("relevant_case_law") or []
    if not cases:
        text += "- No sufficiently similar decisions found.\n\n"
    else:
        for i, row in enumerate(cases, start=1):
            text += (
                f"{i}. **{row.get('docket_number') or row.get('decision_id')}** "
                f"({row.get('decision_date')}, {row.get('court')}, {row.get('language')})\n"
            )
            if row.get("title"):
                text += f"   Title: {row['title']}\n"
            if row.get("regeste"):
                text += f"   Regeste: {row['regeste']}\n"
            if row.get("snippet"):
                text += f"   Snippet: {row['snippet']}\n"
            if row.get("source_url"):
                text += f"   URL: {row['source_url']}\n"
            text += f"   Match: {row.get('source_match')} | Score: {row.get('match_score')}\n\n"

    mock = report.get("mock_decision") or {}
    text += "## Mock Decision Elements\n"
    text += f"- Outcome note: {mock.get('outcome_note')}\n"
    for step in mock.get("reasoning_steps", []):
        text += f"- {step}\n"
    text += "\n"
    text += "## Essential Structure\n"
    for elem in mock.get("essential_elements", []):
        text += f"- {elem}\n"
    return text


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
            name="draft_mock_decision",
            description=(
                "Build a research-only mock decision outline from user facts. "
                "Combines relevant Swiss case law retrieval with statute references. "
                "If possible, enriches statutes with Fedlex text excerpts. "
                "Asks high-priority clarification questions before providing a conclusion."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "facts": {
                        "type": "string",
                        "description": "Detailed facts of the hypothetical or real case.",
                    },
                    "question": {
                        "type": "string",
                        "description": "Optional legal question to decide.",
                    },
                    "preferred_language": {
                        "type": "string",
                        "description": "Output/analysis language preference.",
                        "enum": ["de", "fr", "it", "rm", "en"],
                    },
                    "deciding_court": {
                        "type": "string",
                        "description": "Hypothetical deciding court (e.g., bger, bvger).",
                    },
                    "statute_references": {
                        "type": "array",
                        "description": (
                            "Optional explicit statute list. "
                            "Each item: {law_code, article, paragraph?}."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "law_code": {"type": "string"},
                                "article": {"type": "string"},
                                "paragraph": {"type": "string"},
                            },
                            "required": ["law_code", "article"],
                        },
                    },
                    "fedlex_urls": {
                        "type": "array",
                        "description": (
                            "Optional Fedlex URLs used to fetch statute text. "
                            "Useful when no built-in URL mapping is available."
                        ),
                        "items": {"type": "string"},
                    },
                    "clarifications": {
                        "type": "array",
                        "description": (
                            "Optional answers to prior clarification questions. "
                            "Each item: {id, answer}. The tool withholds conclusion "
                            "until high-priority clarifications are answered."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "answer": {"type": "string"},
                            },
                            "required": ["id", "answer"],
                        },
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of case-law exemplars (default 8, max 20).",
                        "default": 8,
                    },
                },
                "required": ["facts"],
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

        elif name == "draft_mock_decision":
            report = draft_mock_decision(
                facts=arguments.get("facts", ""),
                question=arguments.get("question"),
                preferred_language=arguments.get("preferred_language"),
                deciding_court=arguments.get("deciding_court"),
                statute_references=arguments.get("statute_references"),
                fedlex_urls=arguments.get("fedlex_urls"),
                clarifications=arguments.get("clarifications"),
                limit=arguments.get("limit", 8),
            )
            return [TextContent(
                type="text",
                text=_format_mock_decision_report(report),
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
    logger.info("Swiss Case Law MCP Server starting")
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
