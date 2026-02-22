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
    Claude / any MCP client

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

First run requires calling the 'update_database' tool to download ~5.7GB
from HuggingFace and build the local search index (~65GB disk, 30-60 min).
Subsequent runs use the cached database.

Tools exposed:
    search_decisions  — Full-text search with filters (court, canton,
                        language, date range). Returns BM25-ranked results
                        with highlighted snippets.
    get_decision      — Fetch a single decision by ID or docket number.
                        Returns full text and all metadata.
    list_courts       — List available courts with decision counts.
    get_statistics    — Aggregate statistics by court, canton, year,
                        language.
    find_citations    — Show what a decision cites and what cites it.
                        Uses the reference graph (7.85M citation edges).
    find_leading_cases — Find most-cited decisions for a topic or statute.
    analyze_legal_trend — Year-by-year decision counts for jurisprudence
                        evolution analysis.
    draft_mock_decision — Build a research-only mock decision outline from
                        user facts, grounded in caselaw and statute references
                        (optionally enriched from Fedlex).
    update_database   — Check for and download new data from HuggingFace.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import threading
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
from db_schema import SCHEMA_SQL, INSERT_OR_IGNORE_SQL, INSERT_COLUMNS  # noqa: E402

# Set to True when running with --remote (SSE transport).
# Gates off update_database / check_update_status for remote clients.
REMOTE_MODE = False

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
MAX_LIMIT = 2000           # FTS searches with reranking
FILTER_MAX_LIMIT = 10000   # filter-only queries (no FTS, no reranking)
MAX_FACT_DECISION_LIMIT = 20
MAX_RERANK_CANDIDATES = 2500
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
GRAPH_SIGNALS_ENABLED = os.environ.get("SWISS_CASELAW_GRAPH_SIGNALS", "1").lower() not in {
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

# ── Remote transport security ────────────────────────────────
# Bearer token for SSE endpoint.  If set, every HTTP request (except /health)
# must carry  Authorization: Bearer <token>.  Empty string = auth disabled.
AUTH_TOKEN = os.environ.get("SWISS_CASELAW_AUTH_TOKEN", "")

# Comma-separated allowed CORS origins.  Empty = CORS middleware not mounted
# (only same-origin / non-browser clients can connect).
_cors_raw = os.environ.get("SWISS_CASELAW_CORS_ORIGINS", "")
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_raw.split(",") if o.strip()]

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
    # Constitutional rights
    "diskriminierung": ("gleichbehandlung", "rechtsgleichheit", "discrimination"),
    "gleichbehandlung": ("diskriminierung", "rechtsgleichheit", "egalite"),
    "rechtsgleichheit": ("gleichbehandlung", "diskriminierung", "egalite"),
    "willkuer": ("arbitraire", "arbitrio", "willkuerverbot"),
    "willkuerverbot": ("willkuer", "arbitraire", "arbitrio"),
    "arbitraire": ("willkuer", "willkuerverbot", "arbitrio"),
    "grundrechte": ("droits", "fondamentaux", "diritti", "fondamentali"),
    "verhaeltnismaessigkeit": ("proportionnalite", "proporzionalita"),
    "proportionnalite": ("verhaeltnismaessigkeit", "proporzionalita"),
    # Contract / tort
    "haftung": ("responsabilite", "responsabilita", "liability"),
    "responsabilite": ("haftung", "responsabilita", "liability"),
    "schadenersatz": ("dommages", "risarcimento", "indemnite"),
    "dommages": ("schadenersatz", "risarcimento", "indemnite"),
    "vertrag": ("contrat", "contratto", "contract"),
    "contrat": ("vertrag", "contratto", "contract"),
    # Procedure
    "beschwerde": ("recours", "ricorso", "appel"),
    "recours": ("beschwerde", "ricorso", "appel"),
    "vorsorgliche": ("provisoire", "cautelare", "superprovisorisch"),
    "rechtskraft": ("autorite", "giudicato", "chose"),
    # Criminal
    "freiheitsstrafe": ("peine", "privative", "liberte"),
    "betrug": ("escroquerie", "truffa", "fraud"),
    "diebstahl": ("vol", "furto", "theft"),
    # Family
    "scheidung": ("divorce", "divorzio", "ehescheidung"),
    "unterhalt": ("entretien", "alimenti", "pension"),
    "sorgerecht": ("garde", "custodia", "autorite", "parentale"),
    # Employment (augment existing)
    "fristlos": ("immediat", "immediato", "fristlose"),
    "fristlose": ("fristlos", "immediat", "immediato"),
    "arbeitsvertrag": ("contrat", "travail", "contratto", "lavoro"),
    "treuepflicht": ("fidelite", "fedelta", "loyaute"),
    # Competition / data protection
    "kartell": ("cartel", "cartello", "wettbewerb"),
    "wettbewerb": ("concurrence", "concorrenza", "competition"),
}
ASYL_QUERY_TERMS = {"asyl", "asile", "asilo", "wegweisung", "renvoi", "allontanamento"}
LEGAL_ANCHOR_PAIRS: tuple[tuple[str, str], ...] = (
    ("asyl", "wegweisung"),
    ("asile", "renvoi"),
    ("asilo", "allontanamento"),
    ("parc", "eolien"),
    ("permis", "construire"),
    ("baubewilligung", "windpark"),
    ("fristlos", "kuendigung"),
    ("fristlose", "entlassung"),
    ("schadenersatz", "haftung"),
    ("scheidung", "unterhalt"),
    ("diskriminierung", "gleichbehandlung"),
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
    # Constitution
    "BV": "https://www.fedlex.admin.ch/eli/cc/1999/404",
    "CST": "https://www.fedlex.admin.ch/eli/cc/1999/404",
    "COST": "https://www.fedlex.admin.ch/eli/cc/1999/404",
    # Core private law
    "OR": "https://www.fedlex.admin.ch/eli/cc/27/317_321_377",
    "CO": "https://www.fedlex.admin.ch/eli/cc/27/317_321_377",
    "ZGB": "https://www.fedlex.admin.ch/eli/cc/24/233_245_233",
    "CC": "https://www.fedlex.admin.ch/eli/cc/24/233_245_233",
    # Criminal law
    "STGB": "https://www.fedlex.admin.ch/eli/cc/54/757_781_799",
    "CP": "https://www.fedlex.admin.ch/eli/cc/54/757_781_799",
    "STPO": "https://www.fedlex.admin.ch/eli/cc/2010/267",
    "CPP": "https://www.fedlex.admin.ch/eli/cc/2010/267",
    # Procedural law
    "ZPO": "https://www.fedlex.admin.ch/eli/cc/2010/262",
    "CPC": "https://www.fedlex.admin.ch/eli/cc/2010/262",
    "BGG": "https://www.fedlex.admin.ch/eli/cc/2006/218",
    "LTF": "https://www.fedlex.admin.ch/eli/cc/2006/218",
    "VWVG": "https://www.fedlex.admin.ch/eli/cc/1969/737_755_755",
    "PA": "https://www.fedlex.admin.ch/eli/cc/1969/737_755_755",
    "VGG": "https://www.fedlex.admin.ch/eli/cc/2006/2197",
    "LTAF": "https://www.fedlex.admin.ch/eli/cc/2006/2197",
    # Debt enforcement & bankruptcy
    "SCHKG": "https://www.fedlex.admin.ch/eli/cc/11/529_545_529",
    "LP": "https://www.fedlex.admin.ch/eli/cc/11/529_545_529",
    # Migration / asylum
    "ASYLG": "https://www.fedlex.admin.ch/eli/cc/1999/358",
    "AIG": "https://www.fedlex.admin.ch/eli/cc/2007/758",
    "LSTRI": "https://www.fedlex.admin.ch/eli/cc/2007/758",
    # Social insurance
    "ATSG": "https://www.fedlex.admin.ch/eli/cc/2002/510",
    "AHVG": "https://www.fedlex.admin.ch/eli/cc/63/837_843_843",
    "LAVS": "https://www.fedlex.admin.ch/eli/cc/63/837_843_843",
    "IVG": "https://www.fedlex.admin.ch/eli/cc/1959/827_857_845",
    "LAI": "https://www.fedlex.admin.ch/eli/cc/1959/827_857_845",
    "BVG": "https://www.fedlex.admin.ch/eli/cc/1983/797_797_797",
    "LPP": "https://www.fedlex.admin.ch/eli/cc/1983/797_797_797",
    "UVG": "https://www.fedlex.admin.ch/eli/cc/1982/1676_1676_1676",
    "LAA": "https://www.fedlex.admin.ch/eli/cc/1982/1676_1676_1676",
    "KVG": "https://www.fedlex.admin.ch/eli/cc/1995/1328_1328_1328",
    "AVIG": "https://www.fedlex.admin.ch/eli/cc/1982/2184_2184_2184",
    "LACI": "https://www.fedlex.admin.ch/eli/cc/1982/2184_2184_2184",
    # Tax
    "DBG": "https://www.fedlex.admin.ch/eli/cc/1991/1184_1184_1184",
    "LIFD": "https://www.fedlex.admin.ch/eli/cc/1991/1184_1184_1184",
    "STHG": "https://www.fedlex.admin.ch/eli/cc/1991/1256_1256_1256",
    "LHID": "https://www.fedlex.admin.ch/eli/cc/1991/1256_1256_1256",
    "MWSTG": "https://www.fedlex.admin.ch/eli/cc/2009/5203",
    "LTVA": "https://www.fedlex.admin.ch/eli/cc/2009/5203",
    # Transport
    "SVG": "https://www.fedlex.admin.ch/eli/cc/1959/679_705_685",
    "LCR": "https://www.fedlex.admin.ch/eli/cc/1959/679_705_685",
    # Employment
    "ARG": "https://www.fedlex.admin.ch/eli/cc/1966/57_65_57",
    "LTR": "https://www.fedlex.admin.ch/eli/cc/1966/57_65_57",
    # Intellectual property
    "URG": "https://www.fedlex.admin.ch/eli/cc/1993/1798_1798_1798",
    "LDA": "https://www.fedlex.admin.ch/eli/cc/1993/1798_1798_1798",
    "MSCHG": "https://www.fedlex.admin.ch/eli/cc/1993/274_274_274",
    "LPM": "https://www.fedlex.admin.ch/eli/cc/1993/274_274_274",
    # Environment & planning
    "USG": "https://www.fedlex.admin.ch/eli/cc/1984/1122_1122_1122",
    "LPE": "https://www.fedlex.admin.ch/eli/cc/1984/1122_1122_1122",
    "RPG": "https://www.fedlex.admin.ch/eli/cc/1979/1573_1573_1573",
    "LAT": "https://www.fedlex.admin.ch/eli/cc/1979/1573_1573_1573",
    # Regulatory
    "KG": "https://www.fedlex.admin.ch/eli/cc/1996/546_546_546",
    "LCART": "https://www.fedlex.admin.ch/eli/cc/1996/546_546_546",
    "DSG": "https://www.fedlex.admin.ch/eli/cc/2022/491",
    "LPD": "https://www.fedlex.admin.ch/eli/cc/2022/491",
    "BGO": "https://www.fedlex.admin.ch/eli/cc/2006/355",
    "BGOE": "https://www.fedlex.admin.ch/eli/cc/2006/355",
    # Financial markets
    "BANKG": "https://www.fedlex.admin.ch/eli/cc/51/117_121_117",
    "LB": "https://www.fedlex.admin.ch/eli/cc/51/117_121_117",
    "FINMAG": "https://www.fedlex.admin.ch/eli/cc/2008/5207",
    "LFINMA": "https://www.fedlex.admin.ch/eli/cc/2008/5207",
    # International
    "EMRK": "https://www.fedlex.admin.ch/eli/cc/1974/2151_2151_2151",
    "CEDH": "https://www.fedlex.admin.ch/eli/cc/1974/2151_2151_2151",
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
    """Get a read-only connection to the local SQLite database.

    Raises FileNotFoundError if the database hasn't been built yet,
    prompting the user to run the 'update_database' tool.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            f"Run the 'update_database' tool to download and build the search index. "
            f"This requires ~65 GB free disk space and takes 30-60 minutes."
        )
    last_error = None
    for _ in range(3):
        try:
            conn = sqlite3.connect(
                f"file:{DB_PATH}?immutable=1",
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
    key = ("get_db_stats",)
    cached = _cache_get(key)
    if cached is not None:
        return cached

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
        return _cache_set(key, {
            "total_decisions": total,
            "courts": {r["court"]: r["n"] for r in courts},
            "earliest_date": date_range[0],
            "latest_date": date_range[1],
            "db_path": str(DB_PATH),
            "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 1),
        })
    except FileNotFoundError:
        return {"error": "Database not found. Run 'update_database' first."}


# ── Query cache (cleared on DB rebuild) ──────────────────────
# Caches expensive aggregation queries (list_courts, get_statistics, get_db_stats).
# Keyed by (function_name, args_tuple). Invalidated when DB is rebuilt.
_query_cache: dict[tuple, object] = {}


def _cache_get(key: tuple):
    return _query_cache.get(key)


def _cache_set(key: tuple, value):
    _query_cache[key] = value
    return value


def _cache_clear():
    _query_cache.clear()
    logger.info("Query cache cleared")


# ── Search functions ──────────────────────────────────────────

def search_fts5(
    query: str,
    court: str | None = None,
    canton: str | None = None,
    language: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    chamber: str | None = None,
    decision_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    sort: str | None = None,
) -> tuple[list[dict], int]:
    """
    Full-text search using SQLite FTS5 with BM25 ranking.

    Returns (results, total_count) where total_count is the approximate
    total number of matching decisions (exact for filter-only queries).

    The FTS5 query supports:
    - Simple words: verfassungsrecht
    - Phrases: "Treu und Glauben"
    - Boolean: arbeitsrecht AND kündigung
    - Prefix: verfassung*
    - Column filters: full_text:miete AND regeste:kündigung
    """
    conn = get_db()
    try:
        return _search_fts5_inner(
            conn, query, court, canton, language,
            date_from, date_to, chamber, decision_type, limit, offset,
            sort=sort,
        )
    finally:
        conn.close()


def _search_fts5_inner(
    conn: sqlite3.Connection,
    query: str,
    court: str | None,
    canton: str | None,
    language: str | None,
    date_from: str | None,
    date_to: str | None,
    chamber: str | None,
    decision_type: str | None,
    limit: int,
    offset: int = 0,
    sort: str | None = None,
) -> tuple[list[dict], int]:
    """Inner search logic. Returns (results, total_count). Caller closes conn."""
    is_filter_only = not query.strip()
    effective_max = FILTER_MAX_LIMIT if is_filter_only else MAX_LIMIT
    limit = max(1, min(limit, effective_max))
    offset = max(0, offset)

    fts_query = query.strip()
    if not fts_query:
        # No search query — return recent decisions with filters
        return _list_recent(conn, court, canton, language, date_from, date_to, chamber, decision_type, limit, offset, sort=sort)

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
    if chamber:
        filters.append("d.chamber LIKE ?")
        params.append(f"%{chamber}%")
    if decision_type:
        filters.append("d.decision_type LIKE ?")
        params.append(f"%{decision_type}%")

    where = (" AND " + " AND ".join(filters)) if filters else ""

    is_docket_query = _looks_like_docket_query(fts_query)
    has_explicit_syntax = _has_explicit_fts_syntax(fts_query)
    inline_docket_candidates = _extract_inline_docket_candidates(fts_query)
    # Try collapsing space-separated queries into docket form
    collapsed = _collapse_spaced_docket(fts_query)
    if collapsed and collapsed not in inline_docket_candidates:
        inline_docket_candidates.insert(0, collapsed)
    inline_docket_results: list[dict] = []
    query_preferred_courts = _detect_query_preferred_courts(fts_query)

    # Docket-style lookups should prioritize exact/near-exact docket matches.
    if is_docket_query:
        # Extract just the docket portion from mixed queries like "BGer 4A_291/2017"
        docket_search_query = inline_docket_candidates[0] if inline_docket_candidates else fts_query
        try:
            docket_results = _search_by_docket(
                conn, docket_search_query, where, params, offset + limit,
                preferred_courts=query_preferred_courts,
            )
            if docket_results:
                if sort in ("date_desc", "date_asc"):
                    reverse = sort == "date_desc"
                    docket_results.sort(
                        key=lambda r: r.get("decision_date") or "", reverse=reverse,
                    )
                total = len(docket_results)
                return docket_results[offset:offset + limit], total
        except sqlite3.OperationalError as e:
            logger.debug("Docket-first query failed, falling back to FTS: %s", e)
    if inline_docket_candidates:
        per_docket_limit = max(4, min(limit, 10))
        for candidate in inline_docket_candidates[:3]:
            try:
                inline_docket_results.extend(
                    _search_by_docket(
                        conn, candidate, where, params, per_docket_limit,
                        preferred_courts=query_preferred_courts,
                    )
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

    had_success = False
    candidate_meta: dict[str, dict] = {}
    strategies = _build_query_strategies(fts_query)
    target_pool = _target_candidate_pool(
        limit=limit,
        offset=offset,
        is_docket=is_docket_query,
        has_explicit_syntax=has_explicit_syntax,
    )
    query_has_expandable_terms = _query_has_expandable_terms(fts_query)

    for idx, strategy in enumerate(strategies):
        match_query = strategy["query"]
        strategy_name = strategy.get("name", "")
        strategy_weight = float(strategy.get("weight", 1.0))
        expensive_strategy = strategy_name in {"nl_or", "nl_or_expanded"}
        effective_need = offset + limit
        early_enough = max(effective_need * 2, 20)
        if expensive_strategy and len(candidate_meta) >= early_enough:
            break
        if strategy_name == "nl_or_expanded" and not query_has_expandable_terms:
            continue
        if expensive_strategy and _query_has_numeric_terms(fts_query):
            continue
        try:
            candidate_limit = min(max(target_pool, effective_need * 2), MAX_RERANK_CANDIDATES)
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
            break
        if idx == 0 and has_explicit_syntax and len(candidate_meta) >= effective_need:
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
        total_candidates = len(candidate_meta)
        if inline_docket_results:
            # When merging with docket results, get enough from reranker
            # (offset+limit) and let merge handle final pagination.
            reranked = _rerank_rows(
                rows_for_rerank,
                fts_query,
                offset + limit,
                fusion_scores=fusion_scores,
                offset=0,
                sort=sort,
            )
            merged = _merge_priority_results(
                primary=inline_docket_results,
                secondary=reranked,
                limit=limit,
                offset=offset,
            )
            # Total after dedup
            all_ids = {r["decision_id"] for r in inline_docket_results}
            all_ids.update(candidate_meta.keys())
            return merged, len(all_ids)
        reranked = _rerank_rows(
            rows_for_rerank,
            fts_query,
            limit,
            fusion_scores=fusion_scores,
            offset=offset,
            sort=sort,
        )
        return reranked, total_candidates

    if had_success:
        if inline_docket_results:
            total = len(inline_docket_results)
            return inline_docket_results[offset:offset + limit], total
        return [], 0
    if inline_docket_results:
        total = len(inline_docket_results)
        return inline_docket_results[offset:offset + limit], total
    return [], 0


def _search_by_docket(
    conn: sqlite3.Connection,
    raw_query: str,
    where: str,
    params: list,
    limit: int,
    *,
    preferred_courts: set[str] | None = None,
) -> list[dict]:
    """Docket-first retrieval for docket-like queries."""
    variants = _build_docket_variants(raw_query)
    if not variants:
        return []
    if preferred_courts is None:
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
    if len(results) < limit:
        primary_court = (results[0].get("court") or "").lower() if results else None
        related = _search_related_docket_family(
            conn,
            raw_query=raw_query,
            where=where,
            params=params,
            preferred_courts=preferred_courts,
            primary_court=primary_court,
            existing_ids={r["decision_id"] for r in results if r.get("decision_id")},
            limit=max(limit * 3, 20),
        )
        if related:
            results = _dedupe_results_by_decision_id(results + related)
    return results[:limit]


def _search_related_docket_family(
    conn: sqlite3.Connection,
    *,
    raw_query: str,
    where: str,
    params: list,
    preferred_courts: set[str],
    primary_court: str | None,
    existing_ids: set[str],
    limit: int,
) -> list[dict]:
    family = _parse_docket_family(raw_query)
    if family is None:
        return []

    prefix, serial, year = family
    candidates = _build_docket_family_candidates(prefix=prefix, serial=serial, year=year)
    if not candidates:
        return []
    candidate_placeholders = ",".join("?" for _ in candidates)
    family_filters = [f"d.docket_number IN ({candidate_placeholders})"]
    family_params: list = [*candidates]

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
            d.pdf_url
        FROM decisions d
        WHERE {" AND ".join(family_filters)}{where}
        LIMIT ?
    """
    query_limit = max(limit * 12, 240)
    rows = conn.execute(
        sql,
        [*family_params, *params, query_limit],
    ).fetchall()
    if not rows:
        return []

    preferred_rank_courts = set(preferred_courts or ())
    if primary_court:
        preferred_rank_courts.add(primary_court)

    ranked_rows: list[tuple[tuple, sqlite3.Row]] = []
    for row in rows:
        decision_id = row["decision_id"]
        if not decision_id or decision_id in existing_ids:
            continue
        row_docket = row["docket_number"] or ""
        row_serial = _extract_docket_serial(row_docket, prefix=prefix, year=year)
        distance = abs(row_serial - serial) if row_serial is not None else 10_000_000
        preferred_rank = 0 if (row["court"] or "").lower() in preferred_rank_courts else 1
        ranked_rows.append(
            (
                (
                    preferred_rank,
                    distance,
                    -_date_sort_key(str(row["decision_date"] or "")),
                    str(decision_id),
                ),
                row,
            )
        )
    ranked_rows.sort(key=lambda item: item[0])

    out: list[dict] = []
    for _key, r in ranked_rows:
        out.append({
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
            "relevance_score": 96.0,
        })
        if len(out) >= limit:
            break
    return out


def _build_docket_family_candidates(*, prefix: str, serial: int, year: str) -> list[str]:
    if serial <= 0 or not prefix or not year:
        return []
    serial_window = 40
    lo = max(1, serial - serial_window)
    hi = serial + serial_window
    variants: list[str] = []
    seen: set[str] = set()
    for n in range(lo, hi + 1):
        for sep1 in (".", "_", "-"):
            for sep2 in ("/", "_"):
                candidate = f"{prefix}{sep1}{n}{sep2}{year}"
                if candidate in seen:
                    continue
                seen.add(candidate)
                variants.append(candidate)
    return variants


def _parse_docket_family(raw_query: str) -> tuple[str, int, str] | None:
    text = re.sub(r"\s+", "", (raw_query or "")).upper()
    m = re.fullmatch(
        r"(?P<prefix>[A-Z0-9]{1,4})[._-](?P<serial>\d{1,6})[/_](?P<year>\d{4})",
        text,
    )
    if not m:
        return None
    try:
        serial = int(m.group("serial"))
    except Exception:
        return None
    return m.group("prefix"), serial, m.group("year")


def _extract_docket_serial(docket: str, *, prefix: str, year: str) -> int | None:
    m = re.search(
        rf"{re.escape(prefix)}[._-](?P<serial>\d{{1,6}})[/_]{re.escape(year)}$",
        (docket or "").upper(),
    )
    if not m:
        return None
    try:
        return int(m.group("serial"))
    except Exception:
        return None


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
    matches_with_pos: list[tuple[int, str]] = []
    seen: set[str] = set()
    for pattern in QUERY_DOCKET_PATTERNS:
        for match in pattern.finditer(query or ""):
            raw = (match.group(0) or "").strip()
            norm = _normalize_docket_ref(raw)
            if not raw or len(norm) < 5 or norm in seen:
                continue
            seen.add(norm)
            matches_with_pos.append((match.start(), raw))
    matches_with_pos.sort(key=lambda x: x[0])
    return [raw for _, raw in matches_with_pos[:5]]


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
    offset: int = 0,
) -> list[dict]:
    merged = _dedupe_results_by_decision_id((primary or []) + (secondary or []))
    return merged[offset:offset + max(1, limit)]


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


def _get_graph_conn() -> sqlite3.Connection | None:
    """Open a read-only connection to the reference graph DB, or None if unavailable."""
    if not GRAPH_DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(GRAPH_DB_PATH), timeout=0.5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn
    except sqlite3.Error as e:
        logger.debug("Failed to open graph DB: %s", e)
        return None


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
) -> dict[str, dict[str, float]]:
    if not GRAPH_SIGNALS_ENABLED or not decision_ids:
        return {}

    unique_ids = list(dict.fromkeys([did for did in decision_ids if did]))
    if not unique_ids:
        return {}

    signal_map: dict[str, dict[str, float]] = {
        did: {
            "statute_mentions": 0.0,
            "query_citation_hits": 0.0,
            "incoming_citations": 0.0,
        }
        for did in unique_ids
    }

    conn = _get_graph_conn()
    if conn is None:
        return {}
    try:
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
                signal_map[row["decision_id"]]["statute_mentions"] = float(row["n"] or 0.0)

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
                signal_map[row["decision_id"]]["query_citation_hits"] = float(row["n"] or 0.0)

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
                0.0,
                float(row["n"] or 0.0),
            )
    except sqlite3.Error as e:
        logger.debug("Graph-signal lookup failed: %s", e)
        return {}
    finally:
        conn.close()

    return signal_map


def _count_citations(decision_id: str) -> tuple[int, int]:
    """Return (incoming_count, outgoing_count) for a decision from the graph DB.

    Returns (0, 0) if graph DB unavailable or decision not found.
    """
    conn = _get_graph_conn()
    if conn is None:
        return (0, 0)
    try:
        incoming = 0
        if _sqlite_has_table(conn, "citation_targets"):
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM citation_targets WHERE target_decision_id = ?",
                (decision_id,),
            ).fetchone()
            incoming = int(row["n"]) if row else 0

        row = conn.execute(
            "SELECT COUNT(*) AS n FROM decision_citations WHERE source_decision_id = ?",
            (decision_id,),
        ).fetchone()
        outgoing = int(row["n"]) if row else 0

        return (incoming, outgoing)
    except sqlite3.Error as e:
        logger.debug("Citation count failed: %s", e)
        return (0, 0)
    finally:
        conn.close()


def _find_outgoing_citations(
    decision_id: str, *, min_confidence: float = 0.3, limit: int = 50
) -> list[dict]:
    """Find citations made by this decision (what it cites)."""
    conn = _get_graph_conn()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT dc.target_ref, dc.target_type, dc.mention_count,
                   ct.target_decision_id, ct.confidence_score,
                   d.docket_number, d.court, d.decision_date
            FROM decision_citations dc
            LEFT JOIN citation_targets ct
              ON ct.source_decision_id = dc.source_decision_id
             AND ct.target_ref = dc.target_ref
            LEFT JOIN decisions d
              ON d.decision_id = ct.target_decision_id
            WHERE dc.source_decision_id = ?
              AND (ct.confidence_score IS NULL OR ct.confidence_score >= ?)
            ORDER BY dc.mention_count DESC, ct.confidence_score DESC
            LIMIT ?
            """,
            (decision_id, min_confidence, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logger.debug("Outgoing citations lookup failed: %s", e)
        return []
    finally:
        conn.close()


def _find_incoming_citations(
    decision_id: str, *, min_confidence: float = 0.3, limit: int = 50
) -> list[dict]:
    """Find decisions that cite this decision."""
    conn = _get_graph_conn()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT ct.source_decision_id, ct.confidence_score, ct.target_ref,
                   dc.mention_count,
                   d.docket_number, d.court, d.decision_date
            FROM citation_targets ct
            JOIN decision_citations dc
              ON dc.source_decision_id = ct.source_decision_id
             AND dc.target_ref = ct.target_ref
            JOIN decisions d
              ON d.decision_id = ct.source_decision_id
            WHERE ct.target_decision_id = ?
              AND ct.confidence_score >= ?
            ORDER BY d.decision_date DESC, ct.confidence_score DESC
            LIMIT ?
            """,
            (decision_id, min_confidence, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logger.debug("Incoming citations lookup failed: %s", e)
        return []
    finally:
        conn.close()


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
        (
            r"^ART\."
            r"(?P<article>\d+(?:bis|ter|quater|quinquies|sexies|[a-z])?)"
            r"(?:\.ABS\.(?P<paragraph>\d+(?:bis|ter|quater|quinquies|sexies|[a-z])?))?"
            r"\.(?P<law>[A-Z0-9/]+)$"
        ),
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
    offset: int = 0,
    sort: str | None = None,
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
        statute_mentions = float(graph.get("statute_mentions", 0.0))
        query_citation_hits = float(graph.get("query_citation_hits", 0.0))
        incoming_citations = float(graph.get("incoming_citations", 0.0))

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

    # Apply user-requested sort order (overrides relevance ranking)
    if sort in ("date_desc", "date_asc"):
        reverse = sort == "date_desc"
        scored.sort(key=lambda x: (x[3]["decision_date"] or ""), reverse=reverse)

    results: list[dict] = []
    for final_score, _bm25, _idx, row in scored[offset:offset + limit]:
        full_text = _row_get(row, "full_text_raw")
        best_snippet = _select_best_passage_snippet(
            full_text,
            rank_terms=rank_terms,
            phrase=cleaned_phrase,
            raw_query=raw_query,
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


def _target_candidate_pool(*, limit: int, offset: int = 0, is_docket: bool, has_explicit_syntax: bool) -> int:
    effective = offset + limit
    pool = max(MIN_CANDIDATE_POOL, effective * TARGET_POOL_MULTIPLIER)
    if has_explicit_syntax:
        pool = max(pool, effective * 2)
    if is_docket:
        pool = max(pool, DOCKET_MIN_CANDIDATE_POOL)
    return min(pool, MAX_RERANK_CANDIDATES)


def _should_try_raw_fallback(query: str) -> bool:
    # Raw queries with punctuation frequently trigger parser errors.
    return bool(re.fullmatch(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ_\s]+", query))


def _query_has_expandable_terms(query: str) -> bool:
    terms = _extract_query_terms(
        query,
        limit=MAX_NL_TOKENS,
        include_variants=False,
        include_expansions=False,
    )
    return any(term in LEGAL_QUERY_EXPANSIONS for term in terms)


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


def _collapse_spaced_docket(query: str) -> str | None:
    """Try collapsing space-separated tokens into a docket-like string.

    Handles queries like '6B 1234 2025' → '6B_1234/2025' or '7W 15 25' → '7W_15/2025'.
    Returns the collapsed form if it matches a known docket pattern, else None.
    """
    parts = query.strip().split()
    if not (2 <= len(parts) <= 4):
        return None
    if not all(re.match(r"^[A-Z0-9]{1,6}$", p, re.IGNORECASE) for p in parts):
        return None
    # First part should contain at least one letter
    if not re.search(r"[A-Za-z]", parts[0]):
        return None

    variants = []
    for sep1 in ("_", ".", "-"):
        for sep2 in ("/", "_", "."):
            if len(parts) == 2:
                variants.append(f"{parts[0]}{sep1}{parts[1]}")
            elif len(parts) == 3:
                variants.append(f"{parts[0]}{sep1}{parts[1]}{sep2}{parts[2]}")
            elif len(parts) == 4:
                variants.append(f"{parts[0]}{sep1}{parts[1]}{sep2}{parts[2]}{sep1}{parts[3]}")

    # Also try expanding 2-digit year to 4-digit
    last = parts[-1]
    if len(last) == 2 and last.isdigit():
        expanded = parts[:-1] + ["20" + last]
        for sep1 in ("_", ".", "-"):
            for sep2 in ("/", "_", "."):
                if len(expanded) == 3:
                    variants.append(f"{expanded[0]}{sep1}{expanded[1]}{sep2}{expanded[2]}")
                elif len(expanded) == 4:
                    variants.append(f"{expanded[0]}{sep1}{expanded[1]}{sep2}{expanded[2]}{sep1}{expanded[3]}")

    for variant in variants:
        for pattern in QUERY_DOCKET_PATTERNS:
            if pattern.fullmatch(variant):
                return variant
    return None


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
    # Try collapsing spaces: "6B 1234 2025" → "6B_1234/2025"
    if _collapse_spaced_docket(q):
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
    raw_query: str = "",
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
        truncated = _truncate(compact, MAX_SNIPPET_LEN)
        return _highlight_terms(truncated, rank_terms, phrase, raw_query)
    return fallback


# Terms too common in Swiss legal text to be worth highlighting.
# These appear in virtually every decision and create visual noise.
_HIGHLIGHT_STOPWORDS = {
    # Court names
    "bge", "bger", "bvger", "bstger", "bpatger",
    "bundesgericht", "tribunal", "obergericht", "gericht",
    # Structural terms (appear in every decision)
    "art", "abs", "lit", "ziff", "bgb", "erw", "vol",
    "urteil", "beschluss", "verfügung", "entscheid", "sachverhalt",
    "arrêt", "décision", "jugement", "sentenza", "fait",
    # Common procedural
    "beschwerde", "berufung", "rekurs", "klage", "recours",
    "antrag", "begründung", "erwägung", "dispositiv",
    # Roman numerals (BGE volume dividers, court divisions)
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
    "xi", "xii", "xiii", "xiv", "xv",
}


def _is_trivial_highlight(term: str) -> bool:
    """Return True if a term is too common/trivial to highlight."""
    t = term.lower().strip("*")
    if t in _HIGHLIGHT_STOPWORDS:
        return True
    # Bare years (1900-2099) are trivial — they appear in every date
    if re.fullmatch(r"(?:19|20)\d{2}", t):
        return True
    # Pure numbers (docket fragments, page numbers) under 5 digits
    if t.isdigit() and len(t) < 5:
        return True
    return False


def _highlight_terms(
    text: str | None,
    rank_terms: list[str],
    phrase: str,
    raw_query: str = "",
) -> str | None:
    """Wrap matched search terms in <mark> tags for frontend highlighting.

    Tries full raw query phrase first, then individual terms for leftovers.
    Skips trivial terms (BGE, years, etc.) that add visual noise.
    """
    if not text:
        return text

    # Build ordered list: raw query phrase (longest) first, then individual terms
    candidates: list[str] = []

    # Try the full raw query as a phrase (strip FTS operators)
    if raw_query:
        clean_raw = re.sub(r"\b(AND|OR|NOT)\b", " ", raw_query, flags=re.IGNORECASE)
        clean_raw = clean_raw.strip(' "')
        clean_raw = re.sub(r"\s+", " ", clean_raw).strip()
        if clean_raw and len(clean_raw.split()) > 1:
            candidates.append(clean_raw)

    # Then individual rank_terms, skipping trivial ones
    for t in rank_terms:
        if t not in candidates and not _is_trivial_highlight(t):
            candidates.append(t)

    for term in candidates:
        # Allow flexible whitespace/punctuation between words for multi-word phrases
        if len(term.split()) > 1:
            words = term.split()
            pattern = r"\b" + r"[\s,;:.·/\-]+".join(re.escape(w) for w in words) + r"\b"
        else:
            pattern = rf"\b{re.escape(term)}\b"
        # Apply highlighting only to text outside existing <mark> tags
        text = _apply_highlight_outside_marks(text, pattern)
    return text


def _apply_highlight_outside_marks(text: str, pattern: str) -> str:
    """Apply a highlight pattern only to text segments not already inside <mark>."""
    parts = re.split(r"(<mark>.*?</mark>)", text, flags=re.IGNORECASE)
    for i, part in enumerate(parts):
        if part.startswith("<mark>"):
            continue  # already highlighted
        parts[i] = re.sub(
            rf"({pattern})", r"<mark>\1</mark>", part, flags=re.IGNORECASE
        )
    return "".join(parts)


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
        # Try searching by docket number — prefer newest decision
        row = conn.execute(
            "SELECT * FROM decisions WHERE docket_number = ? "
            "ORDER BY decision_date DESC LIMIT 1",
            (decision_id,),
        ).fetchone()

    if not row:
        # Try partial match on docket — prefer newest decision
        row = conn.execute(
            "SELECT * FROM decisions WHERE docket_number LIKE ? "
            "ORDER BY decision_date DESC LIMIT 1",
            (f"%{decision_id}%",),
        ).fetchone()

    conn.close()

    if not row:
        return None

    result = dict(row)
    # Remove json_data blob from response (redundant)
    result.pop("json_data", None)
    return result


def find_citations(
    *,
    decision_id: str,
    direction: str = "both",
    min_confidence: float = 0.3,
    limit: int = 50,
) -> dict:
    """Find outgoing and/or incoming citations for a decision."""
    limit = max(1, min(limit, 200))
    min_confidence = max(0.0, min(min_confidence, 1.0))

    result: dict = {"decision_id": decision_id, "direction": direction}

    check_conn = _get_graph_conn()
    if check_conn is None:
        result["error"] = "Reference graph not available."
        return result
    check_conn.close()

    if direction in ("both", "outgoing"):
        result["outgoing"] = _find_outgoing_citations(
            decision_id, min_confidence=min_confidence, limit=limit,
        )

    if direction in ("both", "incoming"):
        result["incoming"] = _find_incoming_citations(
            decision_id, min_confidence=min_confidence, limit=limit,
        )

    return result


def _find_leading_cases(
    *,
    query: str | None = None,
    law_code: str | None = None,
    article: str | None = None,
    court: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 20,
) -> dict:
    """Find the most-cited decisions for a topic or statute."""
    limit = max(1, min(limit, 100))
    original_query = query  # preserve for response metadata

    # Determine path: statute (graph DB) or global/court-filtered
    conn = _get_graph_conn()
    if conn is None:
        return {"error": "Reference graph not available."}

    try:
        candidates: list[tuple[str, int]] = []  # (decision_id, citation_count)

        if law_code and article:
            # Statute-filtered: find decisions citing this statute, ranked by incoming citations
            overfetch = limit * 3 if query else limit
            rows = conn.execute(
                """
                SELECT ct.target_decision_id AS decision_id, COUNT(*) AS cite_count
                FROM citation_targets ct
                JOIN decisions d ON d.decision_id = ct.target_decision_id
                WHERE ct.target_decision_id IN (
                    SELECT ds.decision_id
                    FROM decision_statutes ds
                    JOIN statutes s ON s.statute_id = ds.statute_id
                    WHERE s.law_code = ? AND s.article = ?
                )
                """
                + (" AND d.court = ?" if court else "")
                + (" AND d.decision_date >= ?" if date_from else "")
                + (" AND d.decision_date <= ?" if date_to else "")
                + """
                GROUP BY ct.target_decision_id
                ORDER BY cite_count DESC
                LIMIT ?
                """,
                tuple(
                    v
                    for v in (
                        law_code, article,
                        court if court else None,
                        date_from if date_from else None,
                        date_to if date_to else None,
                        overfetch,
                    )
                    if v is not None
                ),
            ).fetchall()
            candidates = [(r["decision_id"], int(r["cite_count"])) for r in rows]
        elif query:
            # Query-only: FTS-first approach — find matching decisions, then rank by citations
            conn.close()
            conn = None  # signal we closed it
            try:
                fts_conn = get_db()
                fts_sql = """
                    SELECT d.decision_id FROM decisions_fts f
                    JOIN decisions d ON d.decision_id = f.decision_id
                    WHERE decisions_fts MATCH ?
                """
                fts_params: list = [query]
                if court:
                    fts_sql += " AND d.court = ?"
                    fts_params.append(court)
                if date_from:
                    fts_sql += " AND d.decision_date >= ?"
                    fts_params.append(date_from)
                if date_to:
                    fts_sql += " AND d.decision_date <= ?"
                    fts_params.append(date_to)
                fts_sql += " LIMIT 5000"
                fts_rows = fts_conn.execute(fts_sql, tuple(fts_params)).fetchall()
                fts_conn.close()
                fts_ids = [r["decision_id"] for r in fts_rows]
            except sqlite3.Error as e:
                logger.debug("FTS lookup for leading cases failed: %s", e)
                return {"error": f"FTS query failed: {e}"}

            if not fts_ids:
                return {"results": [], "total": 0}

            # Look up citation counts from graph for FTS matches
            graph2 = _get_graph_conn()
            if graph2 is not None:
                try:
                    placeholders = ",".join("?" for _ in fts_ids)
                    rows = graph2.execute(
                        f"""
                        SELECT target_decision_id AS decision_id, COUNT(*) AS cite_count
                        FROM citation_targets
                        WHERE target_decision_id IN ({placeholders})
                        GROUP BY target_decision_id
                        ORDER BY cite_count DESC
                        LIMIT ?
                        """,
                        (*fts_ids, limit),
                    ).fetchall()
                    candidates = [(r["decision_id"], int(r["cite_count"])) for r in rows]
                except sqlite3.Error as e:
                    logger.debug("Graph citation lookup failed: %s", e)
                finally:
                    graph2.close()
            # Skip the post-hoc FTS filter since we already started from FTS
            query = None  # prevent double-filtering below
        else:
            # Global most-cited (no query, no statute)
            sql = """
                SELECT ct.target_decision_id AS decision_id, COUNT(*) AS cite_count
                FROM citation_targets ct
            """
            params: list = []
            conditions = []
            if court or date_from or date_to:
                sql += " JOIN decisions d ON d.decision_id = ct.target_decision_id"
                if court:
                    conditions.append("d.court = ?")
                    params.append(court)
                if date_from:
                    conditions.append("d.decision_date >= ?")
                    params.append(date_from)
                if date_to:
                    conditions.append("d.decision_date <= ?")
                    params.append(date_to)
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " GROUP BY ct.target_decision_id ORDER BY cite_count DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, tuple(params)).fetchall()
            candidates = [(r["decision_id"], int(r["cite_count"])) for r in rows]
    except sqlite3.Error as e:
        logger.debug("Leading cases graph query failed: %s", e)
        return {"error": f"Graph query failed: {e}"}
    finally:
        if conn is not None:
            conn.close()

    if not candidates:
        return {"results": [], "total": 0}

    # If query provided, filter via FTS5
    if query:
        candidate_ids = [c[0] for c in candidates]
        try:
            fts_conn = get_db()
            placeholders = ",".join("?" for _ in candidate_ids)
            matched = fts_conn.execute(
                f"""
                SELECT decision_id FROM decisions_fts
                WHERE decisions_fts MATCH ? AND decision_id IN ({placeholders})
                """,
                (query, *candidate_ids),
            ).fetchall()
            fts_conn.close()
            matched_ids = {r["decision_id"] for r in matched}
            candidates = [(did, cnt) for did, cnt in candidates if did in matched_ids]
        except sqlite3.Error as e:
            logger.debug("FTS filter for leading cases failed: %s", e)

    # Truncate to limit
    candidates = candidates[:limit]

    if not candidates:
        return {"results": [], "total": 0}

    # Enrich with metadata from FTS5 decisions table
    candidate_ids = [c[0] for c in candidates]
    rows = _fetch_decision_rows_by_ids(candidate_ids)
    rows_by_id = {r["decision_id"]: r for r in rows}

    results = []
    for did, cite_count in candidates:
        row = rows_by_id.get(did, {})
        results.append({
            "decision_id": did,
            "docket_number": row.get("docket_number", did),
            "decision_date": row.get("decision_date", ""),
            "court": row.get("court", ""),
            "citation_count": cite_count,
            "regeste": (row.get("regeste") or "")[:300],
            "source_url": row.get("source_url", ""),
        })

    return {
        "results": results,
        "total": len(results),
        "law_code": law_code,
        "article": article,
        "query": original_query,
    }


def analyze_legal_trend(
    *,
    query: str | None = None,
    law_code: str | None = None,
    article: str | None = None,
    court: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Year-by-year decision counts for a statute or topic."""
    if not query and not law_code:
        return {"error": "At least one of 'query' or 'law_code' is required."}

    year_counts: dict[int, int] = {}

    # Statute path: use graph DB
    if law_code and article:
        conn = _get_graph_conn()
        if conn is None:
            return {"error": "Reference graph not available."}
        try:
            sql = """
                SELECT CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) AS year,
                       COUNT(DISTINCT ds.decision_id) AS cnt
                FROM decision_statutes ds
                JOIN statutes s ON s.statute_id = ds.statute_id
                JOIN decisions d ON d.decision_id = ds.decision_id
                WHERE s.law_code = ? AND s.article = ?
                  AND d.decision_date IS NOT NULL
                  AND CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) > 1800
                  AND CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) < 2100
            """
            params: list = [law_code, article]
            if court:
                sql += " AND d.court = ?"
                params.append(court)
            if date_from:
                sql += " AND d.decision_date >= ?"
                params.append(date_from)
            if date_to:
                sql += " AND d.decision_date <= ?"
                params.append(date_to)
            sql += " GROUP BY year ORDER BY year"
            rows = conn.execute(sql, tuple(params)).fetchall()
            for r in rows:
                year_counts[int(r["year"])] = int(r["cnt"])
        except sqlite3.Error as e:
            logger.debug("Trend statute query failed: %s", e)
            return {"error": f"Statute trend query failed: {e}"}
        finally:
            conn.close()

    # FTS path: text query
    if query:
        try:
            fts_conn = get_db()
            sql = """
                SELECT CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) AS year,
                       COUNT(*) AS cnt
                FROM decisions_fts f
                JOIN decisions d ON d.decision_id = f.decision_id
                WHERE decisions_fts MATCH ?
                  AND d.decision_date IS NOT NULL
                  AND CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) > 1800
                  AND CAST(SUBSTR(d.decision_date, 1, 4) AS INTEGER) < 2100
            """
            params2: list = [query]
            if court:
                sql += " AND d.court = ?"
                params2.append(court)
            if date_from:
                sql += " AND d.decision_date >= ?"
                params2.append(date_from)
            if date_to:
                sql += " AND d.decision_date <= ?"
                params2.append(date_to)
            sql += " GROUP BY year ORDER BY year"
            rows = fts_conn.execute(sql, tuple(params2)).fetchall()
            fts_conn.close()
            # Merge with statute counts (additive if both paths used)
            for r in rows:
                y = int(r["year"])
                if law_code and article:
                    # Both paths: take max (intersection would undercount)
                    year_counts[y] = max(year_counts.get(y, 0), int(r["cnt"]))
                else:
                    year_counts[y] = int(r["cnt"])
        except sqlite3.Error as e:
            logger.debug("Trend FTS query failed: %s", e)
            if not year_counts:
                return {"error": f"FTS trend query failed: {e}"}

    total = sum(year_counts.values())
    years_sorted = sorted(year_counts.items())

    return {
        "years": [{"year": y, "count": c} for y, c in years_sorted],
        "total": total,
        "law_code": law_code,
        "article": article,
        "query": query,
    }


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


# Extended stopwords for facts distillation (DE/FR/IT common words unlikely to
# improve legal concept matching in FTS5).
_FACTS_STOPWORDS = NL_STOPWORDS | {
    # German
    "ist", "war", "hat", "wurde", "wird", "sind", "waren", "haben", "hatte",
    "sei", "dass", "sich", "auch", "noch", "nach", "bei", "aus", "mehr",
    "wie", "aber", "wenn", "nur", "es", "er", "sie", "wir", "kann", "dieser",
    "diese", "dieses", "diesem", "diesen", "gegen", "bis", "vom", "seit",
    "seiner", "seine", "seinen", "seinem", "ihrer", "ihre", "ihrem", "ihren",
    "sowie", "bereits", "dabei", "jedoch", "dazu", "daher", "dann", "damit",
    "hier", "dort", "nun", "so", "ob", "da", "vor", "ab", "alle", "allem",
    "allen", "aller", "alles", "andere", "anderen", "anderer", "anderes",
    "wo", "welche", "welcher", "welches", "werden", "worden", "deren",
    "dessen", "gemaess", "gemass", "bzw", "etc", "vgl", "bzw",
    # French
    "est", "sont", "ont", "ete", "par", "pas", "qui", "que", "il", "elle",
    "ils", "elles", "nous", "vous", "son", "ses", "leur", "leurs", "ce",
    "cette", "ces", "mais", "plus", "entre", "aussi", "tres", "bien",
    "fait", "etre", "avoir", "peut", "tout", "tous", "toute", "toutes",
    # Italian
    "che", "non", "sono", "era", "stato", "hanno", "aveva", "come", "anche",
    "piu", "suo", "sua", "suoi", "sue", "questo", "questa", "questi",
    "queste", "dal", "dei", "degli", "alle",
    # Numbers / generic
    "chf", "fr", "eur", "nr", "abs",
}


def _extract_legal_query_from_facts(
    text: str,
    statute_requests: list[dict],
) -> str:
    """Distill a facts narrative into a focused legal query for FTS5.

    Instead of sending the entire narrative (which matches on incidental words
    like city names or party descriptions), this extracts:
    1. Statute references (Art. X Law)
    2. Tokens that appear in LEGAL_QUERY_EXPANSIONS (known legal concepts)
    3. Capitalized German legal nouns (> 5 chars, likely Fachbegriffe)
    Limits output to ~12 most distinctive terms.
    """
    # Collect statute ref strings
    ref_terms: list[str] = []
    for st in statute_requests[:6]:
        ref_terms.append(f'Art. {st["article"]} {st["law_code"]}')

    # Tokenize and normalize
    raw_tokens = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿß]+", text)
    normalized_tokens: list[str] = []
    seen_norm: set[str] = set()
    for raw in raw_tokens:
        norm = _normalize_token_for_fts(raw)
        if not norm or len(norm) < 3 or norm in _FACTS_STOPWORDS:
            continue
        if norm in seen_norm:
            continue

        # Priority 1: known legal concept in expansion dictionary
        is_legal_concept = norm in LEGAL_QUERY_EXPANSIONS
        # Priority 2: capitalized German noun > 5 chars (likely legal term)
        is_legal_noun = (
            not is_legal_concept
            and raw[0].isupper()
            and len(raw) > 5
            and norm not in {"zurich", "bern", "basel", "luzern", "geneve",
                             "lausanne", "schweiz", "suisse", "svizzera",
                             "kanton", "gemeinde", "bezirk", "herr", "frau",
                             "arbeitnehmer", "arbeitgeber", "klaeger",
                             "beklagter", "beschwerdefuhrer",
                             "beschwerdefuhrerin", "gesuchsteller",
                             "gesuchstellerin"}
        )
        if is_legal_concept or is_legal_noun:
            seen_norm.add(norm)
            # Use original form for FTS matching (FTS5 is case-insensitive)
            normalized_tokens.append((0 if is_legal_concept else 1, raw))

    # Sort: legal concepts first, then legal nouns
    normalized_tokens.sort(key=lambda x: x[0])
    concept_terms = [tok for _priority, tok in normalized_tokens[:12]]

    # Combine: statute refs first, then concept terms
    parts = ref_terms + concept_terms
    if not parts:
        # Fallback: return original text (truncated) if no terms extracted
        return text[:500]

    return " ".join(parts)


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

    focused_query = _extract_legal_query_from_facts(query_text, statute_requests)
    base_rows, _ = search_fts5(query=focused_query, limit=pool_limit)
    _add(base_rows, source="facts_query", extra_score=0.4)

    # Broader fallback with raw text at lower weight (only if focused query differs)
    if focused_query != query_text:
        fallback_rows, _ = search_fts5(query=query_text, limit=max(8, pool_limit // 2))
        _add(fallback_rows, source="facts_broad", extra_score=0.15)

    for st in statute_requests[:5]:
        q = f"Art. {st['article']} {st['law_code']}"
        if st.get("paragraph"):
            q = f"Art. {st['article']} Abs. {st['paragraph']} {st['law_code']}"
        rows, _ = search_fts5(query=q, limit=min(25, pool_limit))
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
    if not statute_requests:
        return []

    mentions: dict[str, int] = {}
    graph_conn = _get_graph_conn()
    if graph_conn is None:
        return []
    try:
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


def _format_citations_response(result: dict) -> str:
    """Format find_citations result into markdown."""
    if result.get("error"):
        return result["error"]

    did = result["decision_id"]
    text = f"# Citations for {did}\n\n"

    outgoing = result.get("outgoing", [])
    if outgoing is not None:
        text += f"## Outgoing ({len(outgoing)} \u2014 what this decision cites)\n"
        if not outgoing:
            text += "No outgoing citations found.\n"
        for i, c in enumerate(outgoing, 1):
            target_did = c.get("target_decision_id")
            if target_did:
                docket = c.get("docket_number") or target_did
                date = c.get("decision_date") or ""
                court = c.get("court") or ""
                conf = c.get("confidence_score")
                mentions = c.get("mention_count") or 1
                conf_str = f" conf={conf:.2f}" if conf is not None else ""
                text += (
                    f"{i}. **{docket}** ({date}) [{court}]{conf_str} mentions={mentions}\n"
                    f"   ID: {target_did}\n"
                )
            else:
                ref = c.get("target_ref", "?")
                ttype = c.get("target_type", "")
                mentions = c.get("mention_count") or 1
                text += f"{i}. {ref} (unresolved, type={ttype}) mentions={mentions}\n"
        text += "\n"

    incoming = result.get("incoming", [])
    if incoming is not None:
        text += f"## Incoming ({len(incoming)} \u2014 what cites this decision)\n"
        if not incoming:
            text += "No incoming citations found.\n"
        for i, c in enumerate(incoming, 1):
            src_did = c.get("source_decision_id", "?")
            docket = c.get("docket_number") or src_did
            date = c.get("decision_date") or ""
            court = c.get("court") or ""
            conf = c.get("confidence_score")
            mentions = c.get("mention_count") or 1
            conf_str = f" conf={conf:.2f}" if conf is not None else ""
            text += (
                f"{i}. **{docket}** ({date}) [{court}]{conf_str} mentions={mentions}\n"
                f"   ID: {src_did}\n"
            )

    return text


def _format_leading_cases_response(result: dict) -> str:
    """Format find_leading_cases result into markdown."""
    if result.get("error"):
        return result["error"]

    items = result.get("results", [])
    total = result.get("total", 0)
    law_code = result.get("law_code")
    article = result.get("article")
    query = result.get("query")

    header_parts = []
    if law_code and article:
        header_parts.append(f"Art. {article} {law_code}")
    if query:
        header_parts.append(f'"{query}"')
    header = " + ".join(header_parts) if header_parts else "all"

    text = f"# Leading Cases ({header}, top {total} most-cited)\n\n"
    if not items:
        text += "No results found.\n"
        return text

    for i, r in enumerate(items, 1):
        text += (
            f"**{i}. {r['docket_number']}** ({r['decision_date']}) "
            f"[{r['court']}] \u2014 **{r['citation_count']} citations**\n"
        )
        if r.get("regeste"):
            text += f"   Regeste: {r['regeste']}\n"
        if r.get("source_url"):
            text += f"   URL: {r['source_url']}\n"
        text += f"   ID: {r['decision_id']}\n\n"

    return text


def _format_trend_response(result: dict) -> str:
    """Format analyze_legal_trend result into markdown."""
    if result.get("error"):
        return result["error"]

    years = result.get("years", [])
    total = result.get("total", 0)
    law_code = result.get("law_code")
    article = result.get("article")
    query = result.get("query")

    header_parts = []
    if law_code and article:
        header_parts.append(f"Art. {article} {law_code}")
    if query:
        header_parts.append(f'"{query}"')
    header = " + ".join(header_parts) if header_parts else "all"

    text = "# Legal Trend Analysis\n"
    text += f"**Filter:** {header}\n"
    text += f"**Total:** {total:,} decisions\n\n"

    if not years:
        text += "No data found.\n"
        return text

    max_count = max(y["count"] for y in years)
    bar_max = 40  # max bar width in chars

    text += f"{'Year':<6} {'Count':>7}  Bar\n"
    text += "-" * 60 + "\n"
    for y in years:
        bar_len = round(y["count"] / max_count * bar_max) if max_count > 0 else 0
        bar = "\u2588" * bar_len
        text += f"{y['year']:<6} {y['count']:>7,}  {bar}\n"

    return text


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
    key = ("get_statistics", court, canton, year)
    cached = _cache_get(key)
    if cached is not None:
        return cached

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

    return _cache_set(key, {
        "total": total,
        "by_court": {r["court"]: r["n"] for r in by_court},
        "by_language": {r["language"]: r["n"] for r in by_language},
        "by_year": {r["year"]: r["n"] for r in by_year},
    })


def list_courts() -> list[dict]:
    """List all available courts with decision counts."""
    key = ("list_courts",)
    cached = _cache_get(key)
    if cached is not None:
        return cached

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
    return _cache_set(key, [dict(r) for r in rows])


def _list_recent(
    conn: sqlite3.Connection,
    court: str | None,
    canton: str | None,
    language: str | None,
    date_from: str | None,
    date_to: str | None,
    chamber: str | None = None,
    decision_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    sort: str | None = None,
) -> tuple[list[dict], int]:
    """List recent decisions without FTS query (just filters).
    Returns (results, total_count) with exact count."""
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
    if chamber:
        filters.append("chamber LIKE ?")
        params.append(f"%{chamber}%")
    if decision_type:
        filters.append("decision_type LIKE ?")
        params.append(f"%{decision_type}%")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    total_count = conn.execute(
        f"SELECT COUNT(*) FROM decisions {where}", params,
    ).fetchone()[0]

    order_dir = "ASC" if sort == "date_asc" else "DESC"
    rows = conn.execute(
        f"""SELECT decision_id, court, canton, chamber, docket_number,
            decision_date, language, title, regeste, source_url, pdf_url
        FROM decisions {where}
        ORDER BY decision_date {order_dir}
        LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    return [dict(r) for r in rows], total_count


def _truncate(text: str | None, max_len: int) -> str | None:
    if not text:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


# ── Data management ───────────────────────────────────────────

REQUIRED_SPACE_GB = 65

_REQUIRED_PARQUET_COLUMNS = {"decision_id", "court", "canton", "full_text"}

# ── Update state (shared between background thread and tool handlers) ──

_update_state: dict = {
    "status": "idle",       # idle | running | done | failed
    "phase": "",            # download | import | optimize
    "message": "",          # latest human-readable status line
    "step": 0,
    "total": 0,
    "started_at": 0.0,
    "result": "",           # final summary or error message
}
_update_thread: threading.Thread | None = None


def _check_disk_space() -> str:
    """Check free disk space. Returns human-readable message or raises."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(DATA_DIR)
    free_gb = usage.free / (1024 ** 3)
    if free_gb < REQUIRED_SPACE_GB:
        raise RuntimeError(
            f"Insufficient disk space: {free_gb:.1f} GB free, "
            f"but ~{REQUIRED_SPACE_GB} GB required. "
            f"Free up space or set SWISS_CASELAW_DIR to a larger volume."
        )
    return f"Disk space OK: {free_gb:.1f} GB free"


class _StateReporter:
    """Updates the shared _update_state dict from the worker thread."""

    def report(self, progress: float, total: float, message: str) -> None:
        logger.info(message)
        _update_state["step"] = int(progress)
        _update_state["total"] = int(total)
        _update_state["message"] = message


class _NullReporter:
    """Fallback reporter that only logs (for non-MCP callers)."""

    def report(self, progress: float, total: float, message: str) -> None:
        logger.info(message)


def _download_parquet_files(reporter) -> int:
    """Download parquet files one-by-one with per-file progress.

    Returns the number of files downloaded.
    """
    from huggingface_hub import hf_hub_download, list_repo_files

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old parquet files before download to prevent schema mixing
    old_files = list(PARQUET_DIR.rglob("*.parquet"))
    if old_files:
        reporter.report(0, 1, f"Removing {len(old_files)} old parquet files...")
        for f in old_files:
            f.unlink()

    # Enumerate remote files
    all_files = list_repo_files(HF_REPO, repo_type="dataset")
    parquet_files = sorted(f for f in all_files if f.startswith("data/") and f.endswith(".parquet"))
    total = len(parquet_files)

    if total == 0:
        raise RuntimeError(f"No parquet files found in {HF_REPO}/data/")

    for i, remote_path in enumerate(parquet_files, 1):
        name = Path(remote_path).stem
        reporter.report(i, total, f"Downloading {name} ({i}/{total})")
        hf_hub_download(
            repo_id=HF_REPO,
            repo_type="dataset",
            filename=remote_path,
            local_dir=str(PARQUET_DIR),
        )

    reporter.report(total, total, f"Download complete: {total} files")
    return total


def _build_db_from_parquet(reporter=None) -> dict:
    """Build SQLite FTS5 database from downloaded Parquet files.

    Returns dict with keys: imported, duplicates, skipped_files.
    """
    import pyarrow.parquet as pq

    if reporter is None:
        reporter = _NullReporter()

    # Build into a temp file, then atomically rename on success
    tmp_path = DB_PATH.with_suffix(".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Use canonical schema from db_schema.py
    conn.executescript(SCHEMA_SQL)

    # Import all Parquet files
    imported = 0
    duplicates = 0
    skipped_files = []
    parquet_files = sorted(PARQUET_DIR.rglob("*.parquet"))
    total_files = len(parquet_files)
    reporter.report(0, total_files, f"Found {total_files} Parquet files to import")

    for file_idx, pf in enumerate(parquet_files, 1):
        file_imported = 0
        try:
            schema = pq.read_schema(pf)
            file_columns = set(schema.names)
            missing = _REQUIRED_PARQUET_COLUMNS - file_columns
            if missing:
                logger.warning(
                    f"Skipping {pf.name}: missing required columns {missing} "
                    f"(has: {sorted(file_columns)[:8]}...)"
                )
                skipped_files.append(pf.name)
                continue

            table = pq.read_table(pf)
            for batch in table.to_batches():
                for row in batch.to_pylist():
                    try:
                        values = tuple(
                            json.dumps(row, default=str) if col == "json_data"
                            else row.get(col)
                            for col in INSERT_COLUMNS
                        )
                        cursor = conn.execute(INSERT_OR_IGNORE_SQL, values)
                        if cursor.rowcount > 0:
                            imported += 1
                            file_imported += 1
                        else:
                            duplicates += 1
                    except Exception as e:
                        logger.debug(f"Skip {row.get('decision_id', '?')}: {e}")
            conn.commit()
            reporter.report(
                file_idx, total_files,
                f"Imported {pf.stem}: {file_imported:,} decisions "
                f"({file_idx}/{total_files} files, {imported:,} total)",
            )
        except Exception as e:
            logger.warning(f"Failed to read {pf}: {e}")
            skipped_files.append(pf.name)

    # Optimize
    reporter.report(total_files, total_files, "Optimizing FTS5 index (this takes a while)...")
    conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
    conn.execute("PRAGMA optimize")
    conn.commit()
    conn.close()

    # Atomic replace: os.replace is atomic on POSIX (no gap where DB is missing)
    os.replace(str(tmp_path), str(DB_PATH))

    logger.info(
        f"Built database: {imported} imported, {duplicates} duplicates, "
        f"{len(skipped_files)} skipped files → {DB_PATH}"
    )
    if skipped_files:
        logger.warning(f"Skipped files: {skipped_files}")

    return {"imported": imported, "duplicates": duplicates, "skipped_files": skipped_files}


def _update_with_progress(reporter) -> str:
    """Full update: download + build + sanity check. Runs in a worker thread."""
    t0 = time.monotonic()

    # 1. Disk space
    _update_state["phase"] = "disk_check"
    reporter.report(0, 1, "Checking disk space...")
    msg = _check_disk_space()
    reporter.report(0, 1, msg)

    # 2. Download
    _update_state["phase"] = "download"
    _download_parquet_files(reporter)

    # 3. Build DB
    _update_state["phase"] = "import"
    reporter.report(0, 1, "Building SQLite FTS5 database...")
    result = _build_db_from_parquet(reporter)

    # 4. Sanity check — raise so background wrapper sets status="failed"
    MIN_EXPECTED_DECISIONS = 500_000
    if result["imported"] < MIN_EXPECTED_DECISIONS:
        raise RuntimeError(
            f"Database build FAILED sanity check: only {result['imported']} decisions "
            f"imported (minimum {MIN_EXPECTED_DECISIONS}). "
            f"Skipped files: {result['skipped_files']}, duplicates: {result['duplicates']}. "
            f"The database at {DB_PATH} may be corrupt — investigate before using."
        )

    # 5. Summary
    elapsed = time.monotonic() - t0
    minutes, seconds = divmod(int(elapsed), 60)
    _cache_clear()

    stats = get_db_stats()
    reporter.report(1, 1, "Database ready!")
    return (
        f"Database updated successfully in {minutes}m {seconds:02d}s.\n"
        f"Total: {stats.get('total_decisions', '?'):,} decisions\n"
        f"Courts: {len(stats.get('courts', {}))} courts\n"
        f"Date range: {stats.get('earliest_date', '?')} to {stats.get('latest_date', '?')}\n"
        f"Database: {stats.get('db_path', '?')} ({stats.get('db_size_mb', '?')} MB)\n"
        f"Import: {result['imported']:,} inserted, {result['duplicates']:,} duplicates, "
        f"{len(result['skipped_files'])} files skipped"
    )


def _run_update_background() -> None:
    """Target for the background thread. Updates _update_state on completion."""
    reporter = _StateReporter()
    try:
        summary = _update_with_progress(reporter)
        _update_state["status"] = "done"
        _update_state["result"] = summary
    except Exception as e:
        logger.error(f"Background update failed: {e}", exc_info=True)
        _update_state["status"] = "failed"
        _update_state["result"] = f"Update failed: {e}"


def update_from_huggingface() -> str:
    """Download latest data from HuggingFace and rebuild the database.

    Thin wrapper for non-MCP callers (publish.py, CLI). Uses NullReporter.
    """
    try:
        return _update_with_progress(_NullReporter())
    except ImportError:
        return "Error: huggingface_hub not installed. Run: pip install huggingface_hub"
    except Exception as e:
        return f"Update failed: {e}"


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
                "Filter by court, canton, language, date range, chamber, and decision type. "
                "Also handles docket number lookup (e.g., 6B_1234/2025) and "
                "column-scoped search (regeste:keyword, full_text:keyword). "
                "Returns BM25-ranked results with snippets. "
                "Use offset for pagination through large result sets."
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
                    "chamber": {
                        "type": "string",
                        "description": (
                            "Filter by chamber/division (substring match). "
                            "Examples: 'Abteilung V' (BVGer asylum), "
                            "'Zivilrechtliche', 'CASSO', 'Strafrechtliche'"
                        ),
                    },
                    "decision_type": {
                        "type": "string",
                        "description": (
                            "Filter by decision type (substring match). "
                            "Examples: 'Urteil', 'Beschluss', 'Leitentscheid', "
                            "'BVGE', 'Verfügung', 'Endentscheid'"
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (max 2000). Omit to use default of 50. Do not set low values like 5 or 10 unless the user explicitly asked for fewer results.",
                        "default": 50,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Skip this many results (for pagination). Default 0.",
                        "default": 0,
                    },
                    "sort": {
                        "type": "string",
                        "description": "Sort order: 'relevance' (default for FTS), 'date_desc', 'date_asc'.",
                        "enum": ["relevance", "date_desc", "date_asc"],
                    },
                    "fields": {
                        "type": "string",
                        "description": "Response detail level: 'full' (default) includes snippet/regeste/URL, 'compact' returns only docket, date, court, language, decision_id.",
                        "enum": ["full", "compact"],
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
                "docket number (e.g., 6B_1234/2025), or partial match. "
                "Full text is truncated at 50,000 characters for very long decisions. "
                "Set full_text=false to get only metadata and regeste."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "decision_id": {
                        "type": "string",
                        "description": "Decision ID, docket number, or partial docket",
                    },
                    "full_text": {
                        "type": "boolean",
                        "description": "Include full text in response (default true). Set false to get only metadata and regeste.",
                        "default": True,
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
            name="find_citations",
            description=(
                "Given a decision_id, show what it cites and what cites it. "
                "Uses the reference graph database with 7.85M citation edges. "
                "Returns resolved citations with confidence scores and unresolved references."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "decision_id": {
                        "type": "string",
                        "description": "Decision ID (e.g., bger_6B_1_2025)",
                    },
                    "direction": {
                        "type": "string",
                        "description": "Citation direction: 'both' (default), 'outgoing', or 'incoming'",
                        "enum": ["both", "outgoing", "incoming"],
                        "default": "both",
                    },
                    "min_confidence": {
                        "type": "number",
                        "description": "Minimum confidence score for resolved citations (0-1, default 0.3)",
                        "default": 0.3,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max citations per direction (default 50, max 200)",
                        "default": 50,
                    },
                },
                "required": ["decision_id"],
            },
        ),
        Tool(
            name="find_leading_cases",
            description=(
                "Find the most-cited decisions for a topic or statute. "
                "Authority ranking based on citation graph. "
                "Filter by statute (law_code + article), topic query, court, and date range."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Optional text query to filter by topic (FTS search)",
                    },
                    "law_code": {
                        "type": "string",
                        "description": "Optional law code (e.g., BV, OR, ZGB, EMRK, StGB)",
                    },
                    "article": {
                        "type": "string",
                        "description": "Optional article number (requires law_code)",
                    },
                    "court": {
                        "type": "string",
                        "description": "Optional court filter (e.g., bger, bge, bvger)",
                    },
                    "date_from": {
                        "type": "string",
                        "description": "Optional start date (YYYY-MM-DD)",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "Optional end date (YYYY-MM-DD)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 20, max 100)",
                        "default": 20,
                    },
                },
            },
        ),
        Tool(
            name="analyze_legal_trend",
            description=(
                "Year-by-year decision counts showing jurisprudence evolution. "
                "Use with a statute reference (law_code + article), a text query, or both. "
                "Returns yearly counts with visual bar chart."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Optional text query (FTS search)",
                    },
                    "law_code": {
                        "type": "string",
                        "description": "Optional law code (e.g., BV, OR, EMRK). Requires article.",
                    },
                    "article": {
                        "type": "string",
                        "description": "Article number (requires law_code)",
                    },
                    "court": {
                        "type": "string",
                        "description": "Optional court filter",
                    },
                    "date_from": {
                        "type": "string",
                        "description": "Optional start date (YYYY-MM-DD)",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "Optional end date (YYYY-MM-DD)",
                    },
                },
            },
        ),
        Tool(
            name="draft_mock_decision",
            description=(
                "Build a research-only mock decision outline from user facts. "
                "Combines relevant Swiss case law retrieval with statute references. "
                "If possible, enriches statutes with Fedlex text excerpts. "
                "IMPORTANT: The tool may return clarification questions (high/medium priority). "
                "High-priority clarifications must be answered (via the clarifications parameter) "
                "before the tool will provide a conclusion. Call again with clarifications "
                "to get the full analysis."
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
        *([] if REMOTE_MODE else [
            Tool(
                name="update_database",
                description=(
                    "Download the latest Swiss caselaw data from HuggingFace "
                    "and rebuild the local search database. Run this on first use "
                    "or to get the latest decisions. "
                    "Starts in background (~30-60 min). Use check_update_status to monitor."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="check_update_status",
                description=(
                    "Check progress of a running database update. "
                    "Returns current phase, file being processed, and elapsed time. "
                    "Call this after update_database to monitor progress."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
        ]),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if REMOTE_MODE and name in ("update_database", "check_update_status"):
            return [TextContent(type="text", text="This tool is not available on the remote server.")]

        if name == "search_decisions":
            req_offset = int(arguments.get("offset", 0))
            sort_arg = arguments.get("sort")
            fields_arg = arguments.get("fields", "full")
            results, total_count = await asyncio.to_thread(
                search_fts5,
                query=arguments.get("query", ""),
                court=arguments.get("court"),
                canton=arguments.get("canton"),
                language=arguments.get("language"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                chamber=arguments.get("chamber"),
                decision_type=arguments.get("decision_type"),
                limit=arguments.get("limit", DEFAULT_LIMIT),
                offset=req_offset,
                sort=sort_arg,
            )
            if not results:
                text = f"No decisions found matching your query (total: {total_count})."
            else:
                # Strip <mark> tags from snippets (noise for LLM consumers)
                for r in results:
                    if r.get("snippet"):
                        r["snippet"] = r["snippet"].replace("<mark>", "").replace("</mark>", "")

                end = req_offset + len(results)
                text = f"Found {total_count} decisions (showing {req_offset + 1}\u2013{end}):\n\n"

                if fields_arg == "compact":
                    for i, r in enumerate(results, 1):
                        text += (
                            f"{i}. {r['docket_number']} ({r['decision_date']}) "
                            f"[{r['court']}] [{r['language']}] "
                            f"ID:{r['decision_id']}\n"
                        )
                else:
                    for i, r in enumerate(results, 1):
                        text += (
                            f"**{i}. {r['docket_number']}** ({r['decision_date']}) "
                            f"[{r['court']}] [{r['language']}]\n"
                        )
                        if r.get("decision_id"):
                            text += f"   ID: {r['decision_id']}\n"
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
            result = await asyncio.to_thread(get_decision_by_id, arguments["decision_id"])
            if not result:
                return [TextContent(
                    type="text",
                    text=f"Decision not found: {arguments['decision_id']}",
                )]
            include_full_text = arguments.get("full_text", True)
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
            if include_full_text and result.get("full_text"):
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
            # Add citation graph counts
            incoming, outgoing = _count_citations(result["decision_id"])
            if incoming > 0 or outgoing > 0:
                text += f"\n**Citation graph:** Cited by {incoming} decisions | Cites {outgoing} decisions\n"
            return [TextContent(type="text", text=text)]

        elif name == "list_courts":
            courts = await asyncio.to_thread(list_courts)
            if not courts:
                return [TextContent(type="text", text="No data available. Run 'update_database' first.")]
            text = "Available courts:\n\n"
            text += f"{'Court':<25} {'Canton':<8} {'Decisions':>10}  {'Languages':>4}  {'Earliest':>12} {'Latest':>12}\n"
            text += "-" * 83 + "\n"
            for c in courts:
                text += (
                    f"{c['court']:<25} {c['canton']:<8} "
                    f"{c['decision_count']:>10,}  "
                    f"{c['languages']:>4}  "
                    f"{c['earliest']:>12} {c['latest']:>12}\n"
                )
            return [TextContent(type="text", text=text)]

        elif name == "get_statistics":
            stats = await asyncio.to_thread(
                get_statistics,
                court=arguments.get("court"),
                canton=arguments.get("canton"),
                year=arguments.get("year"),
            )
            total = stats.get("total", 0)
            courts_count = len(stats.get("by_court", {}))
            langs = len(stats.get("by_language", {}))
            summary = f"Total: {total:,} decisions across {courts_count} courts in {langs} languages.\n\n"
            return [TextContent(
                type="text",
                text=summary + json.dumps(stats, indent=2, ensure_ascii=False),
            )]

        elif name == "find_citations":
            result = await asyncio.to_thread(
                find_citations,
                decision_id=arguments["decision_id"],
                direction=arguments.get("direction", "both"),
                min_confidence=float(arguments.get("min_confidence", 0.3)),
                limit=int(arguments.get("limit", 50)),
            )
            return [TextContent(type="text", text=_format_citations_response(result))]

        elif name == "find_leading_cases":
            result = await asyncio.to_thread(
                _find_leading_cases,
                query=arguments.get("query"),
                law_code=arguments.get("law_code"),
                article=arguments.get("article"),
                court=arguments.get("court"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                limit=int(arguments.get("limit", 20)),
            )
            return [TextContent(type="text", text=_format_leading_cases_response(result))]

        elif name == "analyze_legal_trend":
            result = await asyncio.to_thread(
                analyze_legal_trend,
                query=arguments.get("query"),
                law_code=arguments.get("law_code"),
                article=arguments.get("article"),
                court=arguments.get("court"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
            )
            return [TextContent(type="text", text=_format_trend_response(result))]

        elif name == "draft_mock_decision":
            report = await asyncio.to_thread(
                draft_mock_decision,
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
            global _update_thread
            if _update_state["status"] == "running":
                return [TextContent(
                    type="text",
                    text="Database update already in progress. Use check_update_status to monitor.",
                )]

            # Reset state and launch background thread
            _update_state.update(
                status="running", phase="starting", message="Starting update...",
                step=0, total=0, started_at=time.monotonic(), result="",
            )
            _update_thread = threading.Thread(
                target=_run_update_background, daemon=True, name="db-update",
            )
            _update_thread.start()

            return [TextContent(
                type="text",
                text=(
                    "Database update started in background.\n"
                    "This downloads ~5.7 GB and builds a ~56 GB search index (30-60 min).\n"
                    "Use the check_update_status tool to monitor progress."
                ),
            )]

        elif name == "check_update_status":
            status = _update_state["status"]

            if status == "idle":
                return [TextContent(
                    type="text",
                    text="No update running. Use update_database to start one.",
                )]

            elapsed = time.monotonic() - _update_state["started_at"]
            minutes, seconds = divmod(int(elapsed), 60)
            time_str = f"{minutes}m {seconds:02d}s"

            if status == "running":
                step = _update_state["step"]
                total = _update_state["total"]
                phase = _update_state["phase"]
                message = _update_state["message"]
                progress = f" ({step}/{total})" if total > 0 else ""
                return [TextContent(
                    type="text",
                    text=(
                        f"Status: RUNNING ({time_str} elapsed)\n"
                        f"Phase: {phase}{progress}\n"
                        f"Current: {message}"
                    ),
                )]

            # done or failed
            return [TextContent(
                type="text",
                text=f"Status: {status.upper()} ({time_str} elapsed)\n\n{_update_state['result']}",
            )]

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

def _log_startup():
    """Log database status on startup."""
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


async def main_stdio():
    """Run the MCP server over stdio (default, local mode)."""
    _log_startup()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main_remote(host: str, port: int):
    """Run the MCP server over SSE (remote mode)."""
    global REMOTE_MODE
    REMOTE_MODE = True

    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Mount, Route
    import uvicorn

    _log_startup()
    logger.info(f"Remote SSE mode on {host}:{port}")
    if AUTH_TOKEN:
        logger.info("Bearer-token auth enabled")
    else:
        logger.warning("No SWISS_CASELAW_AUTH_TOKEN set — endpoint is unauthenticated")

    # Size thread pool for concurrent DB queries (default is too small)
    import concurrent.futures
    pool_size = max(32, (os.cpu_count() or 4) * 4)
    loop = asyncio.new_event_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=pool_size))
    asyncio.set_event_loop(loop)
    logger.info(f"Thread pool: {pool_size} workers")

    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await server.run(
                streams[0], streams[1], server.create_initialization_options()
            )

    # ── Health / readiness endpoint (exempt from auth) ────────
    async def handle_health(request):
        try:
            conn = get_db()
            row = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()
            conn.close()
            return JSONResponse({"status": "ok", "decisions": row[0]})
        except Exception as e:
            return JSONResponse(
                {"status": "error", "detail": str(e)}, status_code=503,
            )

    app = Starlette(
        routes=[
            Route("/health", endpoint=handle_health),
            Route("/sse", endpoint=handle_sse),
            Route("/", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    # ── CORS (inner layer) ────────────────────────────────────
    # Only mounted when explicit origins are configured via env var.
    # Non-browser clients (mcp-remote, Claude Code) ignore CORS entirely.
    if CORS_ORIGINS:
        from starlette.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=CORS_ORIGINS,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type"],
        )

    # ── Bearer-token auth (outer layer) ───────────────────────
    # Wraps the ASGI app; checks Authorization header on every HTTP
    # request except /health.  Disabled when AUTH_TOKEN is empty.
    asgi_app = app
    if AUTH_TOKEN:
        _inner = app

        class _BearerAuthMiddleware:
            async def __call__(self, scope, receive, send):
                if scope["type"] == "http":
                    path = scope.get("path", "")
                    if path != "/health":
                        headers = dict(scope.get("headers", []))
                        auth = headers.get(b"authorization", b"").decode()
                        if auth != f"Bearer {AUTH_TOKEN}":
                            resp = Response(
                                "Unauthorized", status_code=401,
                                headers={"WWW-Authenticate": "Bearer"},
                            )
                            await resp(scope, receive, send)
                            return
                await _inner(scope, receive, send)

        asgi_app = _BearerAuthMiddleware()

    uvicorn.run(asgi_app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Swiss Case Law MCP Server")
    parser.add_argument("--remote", action="store_true",
                        help="Run in remote SSE mode instead of stdio")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Host to bind to in remote mode (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765,
                        help="Port to listen on in remote mode (default: 8765)")
    args = parser.parse_args()

    if args.remote:
        main_remote(args.host, args.port)
    else:
        asyncio.run(main_stdio())
