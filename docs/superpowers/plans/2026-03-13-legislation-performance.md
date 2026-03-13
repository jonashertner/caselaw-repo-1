# Legislation Performance Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace in-memory LexFind cache with persistent SQLite cache, expand Fedlex from 5 to ~200 laws, and add local-first `get_legislation` for federal laws.

**Architecture:** Three independent changes to `mcp_server.py` plus a VPS-side data expansion. The persistent cache replaces the in-memory dict. The local-first path intercepts `_get_legislation` before LexFind calls. The Fedlex expansion is a run-only step with a symlink fix.

**Tech Stack:** Python 3, SQLite (WAL mode), existing Fedlex SPARQL scraper, existing `build_statutes_db.py`

**Spec:** `docs/superpowers/specs/2026-03-13-legislation-performance-design.md`

---

## Chunk 1: Persistent LexFind Cache

### Task 1: Add `_get_lexfind_cache_conn()` and `_ttl_for_key()`

**Files:**
- Modify: `mcp_server.py:143-144` (add env var for cache path)
- Modify: `mcp_server.py:219-220` (remove in-memory cache globals)
- Modify: `mcp_server.py:6304-6319` (replace cache functions)

- [ ] **Step 1: Add the cache DB path constant and broken flag**

At `mcp_server.py:144` (after `OK_COMMENTARIES_DB_PATH`), add:

```python
LEXFIND_CACHE_DB_PATH = Path(os.environ.get("SWISS_CASELAW_LEXFIND_CACHE", str(DATA_DIR / "lexfind_cache.db")))
```

At `mcp_server.py:219-220`, replace:

```python
_LEXFIND_CACHE: dict[str, tuple[float, object]] = {}  # key -> (expiry_ts, data)
LEXFIND_CACHE_TTL = 300  # 5 minutes
```

with:

```python
_lexfind_cache_broken = False  # set True on first SQLite failure, skip cache for process lifetime
```

- [ ] **Step 2: Write `_ttl_for_key()` and `_get_lexfind_cache_conn()`**

At `mcp_server.py:6302` (the `# ── LexFind legislation helpers` section), replace the cache functions (lines 6304-6319) with:

```python
_LEXFIND_CACHE_TTL_MAP = {
    "search:": 86400,      # 24h
    "sysnum:": 2592000,    # 30d
    "law:": 604800,        # 7d
    "changes:": 86400,     # 24h
}

def _ttl_for_key(key: str) -> float:
    """Return TTL in seconds based on cache key prefix."""
    for prefix, ttl in _LEXFIND_CACHE_TTL_MAP.items():
        if key.startswith(prefix):
            return ttl
    return 86400  # default 24h


def _get_lexfind_cache_conn() -> sqlite3.Connection | None:
    """Open or create the LexFind cache DB. Returns None if broken."""
    global _lexfind_cache_broken
    if _lexfind_cache_broken:
        return None
    try:
        conn = sqlite3.connect(str(LEXFIND_CACHE_DB_PATH), timeout=3.0)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 3000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at)
        """)
        conn.commit()
        return conn
    except Exception as e:
        logger.warning("LexFind cache DB broken, disabling: %s", e)
        _lexfind_cache_broken = True
        return None


def _lexfind_cache_get(key: str) -> object | None:
    """Get a value from the persistent LexFind cache. Returns None on miss/expired/error."""
    conn = _get_lexfind_cache_conn()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT value FROM cache WHERE key = ? AND expires_at > ?",
            (key, time.time()),
        ).fetchone()
        return json.loads(row[0]) if row else None
    except Exception as e:
        logger.warning("LexFind cache read error: %s", e)
        return None
    finally:
        conn.close()


def _lexfind_cache_set(key: str, value: object) -> None:
    """Write a value to the persistent LexFind cache with prefix-based TTL."""
    conn = _get_lexfind_cache_conn()
    if conn is None:
        return
    try:
        expires_at = time.time() + _ttl_for_key(key)
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)",
            (key, json.dumps(value, ensure_ascii=False), expires_at),
        )
        # Prune expired entries when cache grows large
        count = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
        if count > 5000:
            conn.execute("DELETE FROM cache WHERE expires_at < ?", (time.time(),))
        conn.commit()
    except Exception as e:
        logger.warning("LexFind cache write error: %s", e)
    finally:
        conn.close()
```

- [ ] **Step 3: Verify no remaining references to old cache globals**

Search `mcp_server.py` for `_LEXFIND_CACHE` and `LEXFIND_CACHE_TTL`. There should be zero references after the replacement. The only callers of cache functions are `_search_legislation`, `_get_legislation`, and `_browse_legislation_changes` — they call `_lexfind_cache_get`/`_lexfind_cache_set` which are unchanged in signature.

- [ ] **Step 4: Test locally**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
import mcp_server as m
# Test cache round-trip
m._lexfind_cache_set('test:hello', {'data': 42})
result = m._lexfind_cache_get('test:hello')
assert result == {'data': 42}, f'Expected dict, got {result}'
# Test expired entry
import time
m._lexfind_cache_set('test:expired', {'old': True})
# Manually expire it
conn = m._get_lexfind_cache_conn()
conn.execute(\"UPDATE cache SET expires_at = 0 WHERE key = 'test:expired'\")
conn.commit(); conn.close()
assert m._lexfind_cache_get('test:expired') is None
# Test TTL tiers
assert m._ttl_for_key('search:foo') == 86400
assert m._ttl_for_key('sysnum:bar') == 2592000
assert m._ttl_for_key('law:baz') == 604800
assert m._ttl_for_key('changes:ch') == 86400
print('All cache tests passed')
"
```

- [ ] **Step 5: Commit**

```bash
git add mcp_server.py
git commit -m "feat: replace in-memory LexFind cache with persistent SQLite cache

WAL mode + busy_timeout for multi-worker safety. TTL tiers:
search 24h, sysnum 30d, law 7d, changes 24h."
```

---

## Chunk 2: Local-First `get_legislation`

### Task 2: Add local-first path in `_get_legislation()`

**Files:**
- Modify: `mcp_server.py:6478-6660` (`_get_legislation` function)
- Modify: `mcp_server.py:6756-6800` (`_format_get_legislation_response`)

- [ ] **Step 1: Add local lookup helper**

Insert before `_get_legislation` (around line 6476):

```python
def _get_legislation_local(
    systematic_number: str, language: str = "de"
) -> dict | None:
    """Try to serve legislation from local statutes.db. Returns None if not found."""
    conn = _get_statutes_conn()
    if conn is None:
        return None
    try:
        sr = re.sub(r"^SR\s*", "", systematic_number.strip(), flags=re.IGNORECASE)
        law = conn.execute(
            "SELECT * FROM laws WHERE sr_number = ?", (sr,)
        ).fetchone()
        if not law:
            return None

        articles = conn.execute(
            "SELECT article_num, heading, text FROM articles WHERE sr_number = ? AND lang = ? ORDER BY rowid",
            (sr, language),
        ).fetchall()

        return {
            "systematic_number": sr,
            "entity": "CH",
            "entity_name": "Bund",
            "source": "local",
            "title": law[f"title_{language}"] or law["title_de"],
            "abbreviation": law[f"abbr_{language}"] or law["abbr_de"],
            "consolidation_date": law["consolidation_date"],
            "articles": [
                {
                    "article_num": a["article_num"],
                    "heading": a["heading"],
                    "text": a["text"],
                }
                for a in articles
            ],
            "article_count": len(articles),
            "language": language,
        }
    except Exception as e:
        logger.warning("Local legislation lookup failed: %s", e)
        return None
    finally:
        conn.close()
```

- [ ] **Step 2: Modify `_get_legislation()` to try local first**

Replace the opening of `_get_legislation` (lines 6478-6488):

```python
def _get_legislation(
    *,
    lexfind_id: int | None = None,
    systematic_number: str | None = None,
    canton: str | None = None,
    include_versions: bool = False,
    language: str = "de",
) -> dict:
    """Get legislation details by LexFind ID or systematic number."""
    if not LEXFIND_ENABLED:
        return {"error": "Legislation lookup is disabled (LEXFIND_ENABLED=false)."}
```

with:

```python
def _get_legislation(
    *,
    lexfind_id: int | None = None,
    systematic_number: str | None = None,
    canton: str | None = None,
    include_versions: bool = False,
    language: str = "de",
) -> dict:
    """Get legislation details by LexFind ID or systematic number."""
    language = language if language in ("de", "fr", "it") else "de"

    # Local-first: serve federal laws from statutes.db when available
    if (
        lexfind_id is None
        and systematic_number
        and (canton is None or canton.upper() == "CH")
        and not include_versions  # version history requires LexFind (spec allows merge but deferred for simplicity)
    ):
        local = _get_legislation_local(systematic_number, language)
        if local is not None:
            return local

    if not LEXFIND_ENABLED:
        # Still check local for federal laws even when LexFind is off
        if systematic_number and (canton is None or canton.upper() == "CH"):
            local = _get_legislation_local(systematic_number, language)
            if local is not None:
                return local
        return {"error": "Legislation lookup is disabled (LEXFIND_ENABLED=false)."}
```

Note: Remove the duplicate `language = language if language in ("de", "fr", "it") else "de"` line that exists at line 6490 (it's now in the new opening block).

- [ ] **Step 3: Update `_format_get_legislation_response` to handle local responses**

Replace the function (starting at line 6756):

```python
def _format_get_legislation_response(result: dict) -> str:
    if result.get("error"):
        return result["error"]

    # Local source: has articles array
    if result.get("source") == "local":
        text = f"# {result.get('title', 'Unknown')}\n"
        text += f"**SR Number:** {result.get('systematic_number', '?')}\n"
        text += f"**Abbreviation:** {result.get('abbreviation', '?')}\n"
        text += f"**Entity:** {result.get('entity_name', '?')} ({result.get('entity', '?')})\n"
        text += f"**Consolidation date:** {result.get('consolidation_date', '?')}\n"
        text += f"**Articles:** {result.get('article_count', 0)}\n"
        text += f"**Source:** Local Fedlex database\n\n"

        articles = result.get("articles", [])
        if len(articles) <= 30:
            for a in articles:
                heading = f" — {a['heading']}" if a.get("heading") else ""
                text += f"### Art. {a['article_num']}{heading}\n"
                text += f"{a['text']}\n\n"
        else:
            text += f"_Law has {len(articles)} articles. Use `get_law` with `article` parameter to read specific articles._\n"
        return text

    # LexFind source: existing format
    cv = result.get("current_version") or {}
    text = f"# {cv.get('title', 'Unknown')}\n"
    text += f"**SR Number:** {result.get('systematic_number', '?')}\n"
    text += f"**Entity:** {result.get('entity_name', '?')} ({result.get('entity', '?')})\n"
    text += f"**Status:** {'Active' if result.get('is_active') else 'Abrogated'}\n"

    if cv.get("category"):
        text += f"**Category:** {cv['category']}\n"
    if cv.get("keywords"):
        text += f"**Keywords:** {cv['keywords']}\n"
    if cv.get("active_since"):
        text += f"**In force since:** {cv['active_since']}\n"
    if cv.get("inactive_since"):
        text += f"**Abrogated:** {cv['inactive_since']}\n"

    text += f"**LexFind ID:** {result.get('lexfind_id', '?')}\n"

    # URLs
    urls = result.get("urls", {})
    if urls:
        text += "\n## Sources\n"
        for lang, url_info in sorted(urls.items()):
            if url_info.get("original_url"):
                text += f"- [{lang.upper()}] {url_info['original_url']}\n"
            if url_info.get("lexfind_pdf"):
                text += f"- [{lang.upper()} PDF] {url_info['lexfind_pdf']}\n"

    # Version history
    versions = result.get("versions")
    if versions:
        text += f"\n## Version History ({len(versions)} versions)\n"
        for v in versions[:20]:
            since = v.get("active_since", "?")
            until = v.get("inactive_since")
            line = f"- **{v.get('title', '?')}** ({since}"
            if until:
                line += f" – {until}"
            line += f") [{v.get('status', '')}]\n"
            text += line
        if len(versions) > 20:
            text += f"_... and {len(versions) - 20} more versions_\n"

    return text
```

- [ ] **Step 4: Test locally**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
import mcp_server as m

# Test local-first for OR (SR 220) - should hit statutes.db
result = m._get_legislation(systematic_number='220')
assert result.get('source') == 'local', f'Expected local source, got {result.get(\"source\")}'
assert result.get('abbreviation') in ('OR', None), f'Unexpected abbr: {result.get(\"abbreviation\")}'
assert len(result.get('articles', [])) > 0, 'Expected articles'
print(f'Local OR: {result[\"article_count\"]} articles')

# Test with 'SR ' prefix normalization
result2 = m._get_legislation(systematic_number='SR 220')
assert result2.get('source') == 'local'
print('SR prefix normalization works')

# Test cantonal bypass
result3 = m._get_legislation(systematic_number='220', canton='ZH')
assert result3.get('source') != 'local', 'Should not use local for cantonal request'
print('Cantonal bypass works')

# Test formatter
text = m._format_get_legislation_response(result)
assert 'Local Fedlex database' in text
print('Formatter works')

print('All local-first tests passed')
"
```

- [ ] **Step 5: Commit**

```bash
git add mcp_server.py
git commit -m "feat: add local-first get_legislation for federal laws

Serves federal laws from statutes.db (~2ms) instead of LexFind API (~10s).
Falls back to LexFind for cantonal laws, unknown SR numbers, or version requests."
```

---

## Chunk 3: Symlink Fix for `build_statutes_db.py`

### Task 3: Fix atomic swap to handle symlinks

**Files:**
- Modify: `search_stack/build_statutes_db.py:220-227,325-327`

- [ ] **Step 1: Add symlink resolution and use `os.replace()`**

At line 220-222 of `build_statutes_db.py`, replace:

```python
    # Prepare output
    tmp_db = OUTPUT_DB.with_suffix(".tmp")
    tmp_db.unlink(missing_ok=True)
```

with:

```python
    # Prepare output — resolve symlinks so temp file is on same filesystem (atomic rename)
    resolved_db = OUTPUT_DB.resolve()
    tmp_db = resolved_db.with_suffix(".tmp")
    tmp_db.unlink(missing_ok=True)
```

At lines 325-327, replace:

```python
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()
    tmp_db.rename(OUTPUT_DB)
```

with:

```python
    os.replace(str(tmp_db), str(resolved_db))
```

- [ ] **Step 2: Verify `os` is imported**

Check that `import os` exists at the top of `build_statutes_db.py`. It's already there (used for `os.environ`).

- [ ] **Step 3: Commit**

```bash
git add search_stack/build_statutes_db.py
git commit -m "fix: add symlink resolution to build_statutes_db for atomic swap

Same pattern as build_fts5.py, build_reference_graph.py. Uses os.replace()
instead of unlink+rename for true atomic swap."
```

---

## Chunk 4: VPS Deployment — Fedlex Expansion + Cache DB

### Task 4: Expand Fedlex to ~200 laws on VPS

**Files:**
- No code changes — VPS run only

- [ ] **Step 1: Check if statutes.db is symlinked on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'ls -la /opt/caselaw/repo/output/statutes.db'
```

If symlinked, the symlink fix from Task 3 must be deployed first.

- [ ] **Step 2: Deploy code changes**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase origin main'
```

- [ ] **Step 3: Run Fedlex scraper**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -m scrapers.fedlex --top 200 -v 2>&1 | tail -30'
```

This downloads Akoma Ntoso XML for ~200 laws. Expect 10-30 minutes depending on Fedlex API speed.

- [ ] **Step 4: Rebuild statutes.db**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -m search_stack.build_statutes_db -v 2>&1 | tail -20'
```

Atomic swap ensures zero downtime.

- [ ] **Step 5: Restart MCP servers to pick up new code**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

- [ ] **Step 6: Verify**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -c "
import sqlite3
conn = sqlite3.connect(\"output/statutes.db\")
laws = conn.execute(\"SELECT COUNT(*) FROM laws\").fetchone()[0]
articles = conn.execute(\"SELECT COUNT(*) FROM articles\").fetchone()[0]
print(f\"Laws: {laws}, Articles: {articles}\")
conn.close()
"'
```

Expected: ~100-200 laws, ~15,000-25,000 articles.

---

## Chunk 5: End-to-End Benchmark

### Task 5: Run latency benchmark and verify success criteria

**Files:**
- No code changes — benchmark only

- [ ] **Step 1: Run the benchmark script on VPS**

Upload and run the benchmark from the prior session (`/tmp/bench_mcp_v3.py`), updated with the correct function names. Key measurements:

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -c "
import sys, os, time, json
sys.path.insert(0, \"/opt/caselaw/repo\")
os.environ.setdefault(\"SWISS_CASELAW_DIR\", \"/opt/caselaw/repo/output\")
os.environ.setdefault(\"LEXFIND_ENABLED\", \"true\")
import mcp_server as m

# Test 1: get_legislation local-first (SR 220 = OR)
t0 = time.perf_counter()
r = m._get_legislation(systematic_number=\"220\")
t1 = (time.perf_counter() - t0) * 1000
src = r.get('source', '?')
ac = r.get('article_count', 0)
print(f'get_legislation(220) local: {t1:.1f}ms source={src} articles={ac}')

# Test 2: search_legislation cold then cached
t0 = time.perf_counter()
r = m._search_legislation(query='Datenschutz', limit=5)
t1 = (time.perf_counter() - t0) * 1000
print(f'search_legislation cold: {t1:.1f}ms')

t0 = time.perf_counter()
r = m._search_legislation(query='Datenschutz', limit=5)
t2 = (time.perf_counter() - t0) * 1000
print(f'search_legislation cached: {t2:.1f}ms')

# Test 3: get_legislation cantonal (persistent cache)
t0 = time.perf_counter()
r = m._get_legislation(systematic_number='220', canton='ZH')
t1 = (time.perf_counter() - t0) * 1000
print(f'get_legislation(220, ZH) cold: {t1:.1f}ms')

t0 = time.perf_counter()
r = m._get_legislation(systematic_number='220', canton='ZH')
t2 = (time.perf_counter() - t0) * 1000
print(f'get_legislation(220, ZH) cached: {t2:.1f}ms')

# Test 4: cache survives (check file exists)
import pathlib
cache_path = pathlib.Path(os.environ.get('SWISS_CASELAW_DIR', 'output')) / 'lexfind_cache.db'
exists = cache_path.exists()
sz = cache_path.stat().st_size if exists else 0
print(f'Cache DB exists: {exists}, size: {sz} bytes')
"'
```

- [ ] **Step 2: Verify success criteria**

| Scenario | Target | Actual |
|----------|--------|--------|
| `get_legislation(SR 220)` local hit | < 50ms | _fill in_ |
| `search_legislation` cached | < 10ms | _fill in_ |
| `get_legislation(cantonal)` cached | < 10ms | _fill in_ |
| Cache DB exists after restart | Yes | _fill in_ |

- [ ] **Step 3: Run full 19-tool functional test**

Run the same benchmark script from before to verify all tools still pass:

```bash
# Use the bench_mcp_v3.py script updated with correct function names
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 /tmp/bench_mcp_v3.py 2>&1'
```

All 19 tools should return OK (except `get_commentary(OR 41)` which is a known data gap, and `draft_mock_decision` which is skipped).
