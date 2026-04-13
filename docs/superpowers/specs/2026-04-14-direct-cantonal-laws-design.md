# Direct Cantonal Law Scraping — Design Spec

**Date:** 2026-04-14
**Status:** Approved
**Goal:** Replace lossy LexFind PDF extraction with direct scraping from each canton's official law publication platform.

## Problem

LexFind PDFs are lossy: text extraction mangles article structure, loses numbered sub-paragraphs, corrupts tables, and drops cross-references. The 26 cantonal sources publish authoritative HTML text that preserves the official structure.

## Architecture

### New scraper framework: `scrapers/cantonal_laws/`

```
scrapers/cantonal_laws/
  __init__.py          # Registry: canton code → scraper class
  base.py              # CantonalLawScraper ABC
  zh.py                # ZH: zhlex.zh.ch
  be.py                # BE: belex.sites.be.ch
  ag.py                # AG: gesetzessammlungen.ag.ch
  ...                  # one file per canton (or per platform group)
```

### Base class (`base.py`)

```python
class CantonalLawScraper(ABC):
    CANTON: str              # "ZH", "BE", etc.
    LANGUAGE: str            # primary publication language
    TEXT_SOURCE: str         # source identifier for provenance ("zhlex", "belex", ...)
    REQUEST_DELAY: float     # polite delay between requests

    def enumerate_laws(self) -> Iterator[dict]:
        """Yield law stubs: {sr_number, title, url, category, is_active, ...}"""

    def fetch_law(self, stub: dict) -> dict | None:
        """Fetch full text + segmented articles.
        Returns: {full_text, articles: [{article_num, heading, text}], ...}
        """
```

### Output format

Same JSONL as current `lexfind_cantonal.py`. Only `text_source` changes:

```json
{
  "canton": "ZH",
  "sr_number": "131.1",
  "title": "Kantonsverfassung",
  "language": "de",
  "is_active": true,
  "category": "Gesetz",
  "original_url": "https://zhlex.zh.ch/...",
  "text_source": "zhlex",
  "full_text": "...",
  "articles": [
    {"article_num": "1", "heading": "Menschenwürde", "text": "..."}
  ]
}
```

### Text extraction rules

- Parse HTML, strip tags, preserve whitespace structure (paragraph breaks, numbered sub-items, indentation)
- Prefer the source's own article boundaries (HTML structure) over regex splitting
- Tables: render as aligned text rows
- No markdown, no HTML — clean text that reads like the published law

### Runner: `scrape_cantonal_laws.py`

```
python3 scrape_cantonal_laws.py                      # all implemented cantons
python3 scrape_cantonal_laws.py --canton ZH           # single canton
python3 scrape_cantonal_laws.py --canton ZH --max 5   # pilot
python3 scrape_cantonal_laws.py --list                # show implemented cantons
```

Output: `output/cantonal_laws_direct/{CANTON}.jsonl`

## Migration strategy

- `build_cantonal_laws_db.py` updated to read from both `output/cantonal_laws_direct/` (priority) and `output/lexfind_cantonal/` (fallback)
- Per-canton: if direct JSONL exists and is non-empty, use it; else use LexFind JSONL
- `text_source` field tracks provenance — no data ambiguity
- No MCP changes: `_search_cantonal_local()`, `_get_cantonal_local()`, `_get_law_cantonal()` all work unchanged
- LexFind remains permanent fallback for API queries and unimplemented cantons

## Implementation order

Phase 1 (largest cantons, ~60% of laws):
1. ZH — zhlex.zh.ch
2. BE — belex.sites.be.ch (bilingual de/fr)
3. AG — gesetzessammlungen.ag.ch
4. VD — rsv.vd.ch (French)
5. GE — silgeneve.ch (French)

Phase 2 (remaining German-speaking):
6-16. LU, SG, TG, SO, BL, BS, SH, AR, AI, GL, NW, OW, UR, SZ, ZG, GR, NW

Phase 3 (remaining French/Italian):
17-22. FR, NE, JU, VS, TI

Build base classes as platform patterns emerge during Phase 1.

## What stays unchanged

- `cantonal_laws.db` schema (laws, articles, articles_fts)
- All MCP functions
- LexFind as fallback (both API and cached data)
- `lexfind_cantonal.py` (kept, not deleted)

## Success criteria

- All 26 cantons have direct scrapers
- Text quality visibly better than LexFind PDFs (article boundaries, sub-paragraphs, formatting)
- Zero coverage regression (every law in LexFind is also in direct source, or LexFind fills the gap)
- `portal_count` reported per canton for gap tracking
