# Issue #22 — structured (Akoma Ntoso XML) form for /laws/{abbr}

Status: implemented + tested 2026-06-20 (XML form). JSON form deferred (Phase 2).
GitHub issue #22: serve statute article text in structured XML, not just flat
text, so clients can render enumerations, footnotes, sub-paragraphs.

## Current state (before)
- The Fedlex scraper already downloads **Akoma Ntoso XML** to
  `output/fedlex/xml/{sr_number}/{lang}.xml` — richly structured (`<article>`,
  `<paragraph>`, `<blockList>`/`<item>` enumerations, `<authorialNote>` footnotes).
- `search_stack/build_statutes_db.py` **flattens** it to plain text in
  `statutes.db.articles(article_num, heading, footnote, text, lang)` — structure lost.
- `get_law` (MCP) + REST `/api/laws/{abbr}` serve the flattened text.

The structured source already exists on disk → **no re-scrape needed**.

## Design
- **Phase 1 (this change): per-article verbatim AN XML fragment, additive.**
  Store each `<article>` subtree (clean AN namespace) in a new
  `statutes.db.articles.xml` column; serve it as an additive `xml` field on
  single-article `get_law`, and `?format=xml` on the REST endpoint
  (`application/xml`). `text` is unchanged (backward compatible).
- **Phase 2 (deferred): parsed structured JSON** (paragraph/item/footnote tree)
  if clients want it pre-parsed.

Chosen over a JSON-only approach because the XML already exists, is faithful
(no lossy re-interpretation), and Akoma Ntoso is a documented standard already
core to the project's open-law-standards positioning.

## Implementation
`search_stack/build_statutes_db.py`:
- `ET.register_namespace("", AKN_NS)` → fragments serialize as
  `<article xmlns="…akn…">…</article>` (no `ns0:` prefixes).
- `parse_xml` adds `"xml": ET.tostring(art_elem, encoding="unicode")`.
- `articles` schema gains a nullable `xml TEXT` column; INSERT carries it.

`mcp_server.py`:
- `get_law` single-article path: conditional column list — includes `xml` when
  the column exists (resilient to a pre-rebuild DB, so the API never errors
  during staged rollout). `dict(a)` flows it through. Whole-law *list* mode
  stays compact (no `xml`).
- REST `api_get_law`: `?format=xml` returns the single article's verbatim AN
  XML as `application/xml`; 404 if none (e.g. whole-law request or pre-rebuild).

## Verification
- `tests/test_statutes_xml.py`: fixture AN article (blockList + authorialNote)
  → `parse_xml` yields an `xml` fragment that preserves `<blockList>` +
  `<authorialNote>` (no ns prefix). Passes.
- End-to-end local build (OR/ZGB/StGB/BV/BGG, 11,429 articles): `xml` populated
  on **11,429/11,429**. `get_law(220, "41")` returns the `<article eId="art_41">`
  subtree; whole-law list stays `['article_num','heading']` (1,686).
- 55 statute/law/fedlex tests pass, no regression.

## Deployment (pipeline-gated)
1. Deploy code (build_statutes_db.py + mcp_server.py): push + restart MCP
   workers. Backward-compatible — `xml` simply omitted until the DB is rebuilt.
2. Rebuild `statutes.db` on the VPS: re-run `build_statutes_db` (re-parses the
   on-disk `output/fedlex/xml/`, offline `.tmp` + atomic swap). Populates `xml`.
   No re-scrape. Then `xml` / `?format=xml` go live.

## Phase 2 (follow-up)
Parsed structured JSON (`?format=json-structured` or a `structured` field):
walk the AN subtree into `{paragraphs:[{num, text, items:[…], notes:[…]}]}`.
