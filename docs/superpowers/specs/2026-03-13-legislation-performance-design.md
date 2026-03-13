# Legislation Performance: Persistent Cache + Fedlex Expansion + Local-First Lookup

**Date**: 2026-03-13
**Status**: Approved

## Problem

Three MCP legislation tools (`search_legislation`, `get_legislation`, `browse_legislation_changes`) rely on the LexFind API. `get_legislation` takes 10.8s for a systematic number lookup due to POST + paginated GET. The in-memory cache (5min TTL) is lost on every server restart, so the first call after restart always hits the slow path. Additionally, `statutes.db` only contains 5 federal laws despite infrastructure supporting hundreds.

## Solution

Three independent improvements:

1. **Persistent LexFind cache** — SQLite-backed cache surviving restarts
2. **Expanded Fedlex coverage** — 5 laws to ~200 (top cited)
3. **Local-first `get_legislation`** — serve federal laws from statutes.db when available

## 1. Persistent LexFind Cache

### Schema (`output/lexfind_cache.db`)

```sql
CREATE TABLE cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,      -- JSON-serialized
    expires_at REAL NOT NULL  -- unix timestamp
);
CREATE INDEX idx_cache_expires ON cache(expires_at);
```

### TTL Tiers

| Key prefix | TTL | Rationale |
|------------|-----|-----------|
| `search:*` | 24h | Search results change as legislation updates |
| `sysnum:*` | 30d | SR-to-ID mappings are stable |
| `law:*` | 7d | Legislation details and versions |
| `changes:*` | 24h | Recent changes should stay fresh |

### Implementation

Replace `_lexfind_cache_get` / `_lexfind_cache_set` internals. Same function signatures and cache key format — callers unchanged. `_lexfind_cache_set` internally calls `_ttl_for_key(key)` to determine the correct TTL based on key prefix (replaces the hardcoded `LEXFIND_CACHE_TTL = 300`).

Connection via `_get_lexfind_cache_conn()` following existing patterns (`_get_statutes_conn`, `_get_ok_conn`). Lazy-initialized. Path from `SWISS_CASELAW_LEXFIND_CACHE` env var, default `{SWISS_CASELAW_DIR}/lexfind_cache.db`.

**Schema initialization**: `_get_lexfind_cache_conn()` runs `CREATE TABLE IF NOT EXISTS` on first connection. No separate build step needed.

**Multi-worker safety**: Cache DB uses WAL mode (`PRAGMA journal_mode = WAL`) and busy timeout (`PRAGMA busy_timeout = 3000`). This is required because 4 uvicorn workers write concurrently — unlike other DBs which are read-only.

Cleanup: `DELETE FROM cache WHERE expires_at < ?` when row count exceeds 5000, triggered on write.

**In-memory cache removal**: The existing `_LEXFIND_CACHE` dict and `LEXFIND_CACHE_TTL` constant are removed. The SQLite cache is the sole cache layer.

### Resilience

If `lexfind_cache.db` is unreadable or corrupt, set a process-level flag (`_lexfind_cache_broken = True`) on first failure and skip all SQLite cache attempts for the process lifetime. Requests proceed without caching (direct LexFind calls). Never block a request because the cache is broken.

## 2. Expanded Fedlex Coverage

No code changes to the scraper. Run on VPS:

```bash
python3 -m scrapers.fedlex --top 200
python3 -m search_stack.build_statutes_db
```

Expected output: ~200 laws, ~15,000-25,000 articles across 3 languages. DB size: ~80-150 MB (up from 17 MB for 5 laws).

Zero-downtime: `build_statutes_db` already builds to `.db.tmp` and does atomic `os.replace()`.

**Symlink check**: Verify whether `output/statutes.db` is symlinked on VPS. If so, add `.resolve()` to `build_statutes_db.py` before temp file creation (same fix applied to `build_fts5.py`, `build_reference_graph.py`, etc.).

Optional: add to weekly cron to pick up Fedlex consolidation updates.

## 3. Local-First `get_legislation`

### Flow Change in `_get_legislation()`

1. If `systematic_number` provided and no `canton` param (or `canton == "CH"`):
   - Normalize systematic number: strip "SR " prefix, whitespace
   - Check statutes.db: `SELECT * FROM laws WHERE sr_number = ?`
   - If found: return article text from local DB (~2ms)
   - Optionally merge cached LexFind metadata (URLs, version info) if available in lexfind_cache.db — do not block on LexFind API call
2. If not found locally or cantonal law: existing LexFind flow (now persistent-cached)

### Response Format

When serving from local DB, response includes: `systematic_number`, `entity: "CH"`, `source: "local"`, law metadata (title, abbreviation, consolidation_date from `laws` table), and `articles` array with article text. Fields only available from LexFind (`is_active`, `lexfind_id`, version history) are omitted rather than faked. The formatter must handle both local and LexFind response shapes.

### Edge Cases

- `canton` param set to non-CH value: skip local-first, go to LexFind
- statutes.db unavailable: proceed to LexFind as before
- Law in statutes.db but user requests `include_versions=True`: merge with cached LexFind version data if available, otherwise note versions unavailable locally
- `LEXFIND_ENABLED=false`: local-first path still works for federal laws in statutes.db (bypasses the `LEXFIND_ENABLED` guard for local hits). Only returns error if law not found locally.

## Success Criteria

| Scenario | Target Latency |
|----------|---------------|
| `get_legislation(SR 220)` — local hit | < 50ms |
| `get_legislation(SR 220)` — persistent cache hit | < 10ms |
| `search_legislation(...)` — persistent cache hit | < 10ms |
| `get_legislation(cantonal)` — first call | Same as current (~10s) |
| `get_legislation(cantonal)` — after restart | < 10ms (cache survives) |
| All 19 MCP tools functional | Pass |

## Files Modified

- `mcp_server.py`: `_lexfind_cache_get`, `_lexfind_cache_set`, `_get_legislation`, new `_get_lexfind_cache_conn`, `_ttl_for_key`
- VPS: run Fedlex scraper + rebuild statutes.db (no code change)

## Out of Scope

- Cantonal legislation local DB (LexFind remains the source)
- Cron-based cache warming (TTL handles freshness)
- Changes to `search_legislation` or `browse_legislation_changes` flow (only cache backend changes)
