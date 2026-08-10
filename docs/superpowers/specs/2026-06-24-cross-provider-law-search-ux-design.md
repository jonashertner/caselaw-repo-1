# Cross-provider law-search UX: highlighted results plus progressive-enhancement widgets

Date: 2026-06-24
Status: DRAFT for review (no code written yet)
Owner: Jonas Hertner

## 1. Goal and non-goals

Goal: make OpenCaseLaw law search feel like a polished product (omnilex quality: highlighted matches, clean scannable results, click into the article) and make it **excellent on all four LLM providers (Claude, ChatGPT, Gemini, Copilot)**, with a true in-client "app" experience where the client supports it.

Strategy: progressive enhancement from one canonical result. The markdown text is excellent everywhere and is always the fallback and the citable source; interactive widgets are layered on for the clients that render them.

In scope:
- Tier A (universal text): highlighted, structured law-search results plus a machine-readable `structuredContent` payload. Works on all four providers today.
- Tier B (enhancement): a Claude MCP-Apps widget, then a ChatGPT Apps-SDK component, both rendering from the same `structuredContent`.

Non-goals (explicitly out, can be follow-ups):
- A standalone web app at law.opencaselaw.ch (that was the other branch; deferred).
- Changing search ranking or relevance (Tier A changes formatting and payload only, not the algorithm).
- A unified `laws_fts` rebuild (no new FTS table; federal `articles_fts` already exists). Optional later upgrade.
- Decisions search (laws first; the pattern generalises to decisions in a later phase).

## 2. Current state (grounding, mcp_server.py unless noted)

Dispatch is an `if/elif name == ...` chain in `_handle_call_tool_inner` (18902), via `_handle_call_tool_wrapper` (18865) and `_dispatch_with_timeout` (18837).

| Tool | branch | core fn | formatter |
|---|---|---|---|
| get_law | 19359 | `get_law` (14872) | `_format_get_law_response` (15539) |
| search_laws | 19373 | `search_laws` (15443) | `_format_search_laws_response` (15587) |
| search_legislation | 19487 | `_search_legislation` (15760) | `_format_search_legislation_response` (16830) |
| get_legislation | 19506 | `_get_legislation` (16208) | `_format_get_legislation_response` (16879) |

Data sources: federal statutes = local `statutes.db` (Fedlex mirror, `articles_fts`); cantonal = LexFind live first (`_search_legislation`), `cantonal_laws.db` fallback. Federal snippet uses `snippet(articles_fts, 3, '>>>','<<<','...',40)` (15172 et al); cantonal-local uses `<b>`/`</b>` (16001); LexFind highlight spans are stripped by `_clean_lexfind_html` (15739).

Findings that shape the plan:
- No `<mark>` highlighting on laws (decisions-only, origin 2379, stripped 18979 to 18982, rendered 19032).
- Search rows carry `level, canton, sr_number, abbreviation, title, article_num, heading, snippet (, lexfind_id)` (federal/cantonal) and `lexfind_id, title, systematic_number, entity, entity_name, is_active, category, keywords, snippet, original_url, version_active_since` (legislation). No `citation_string`; legislation has `original_url`, statute rows have no url.
- Verbatim article `text` comes from `get_law` (federal: `statutes.db.articles.text`; cantonal: LexFind or `cantonal_laws.db`). `_format_get_law_response` emits `### Art./§ <num> — <heading>` plus raw text. R2 verbatim source is this `text` field.
- No programmatic law citation/reference builder exists; the model composes "Art. 41 OR" from the printed `abbreviation` + `article_num` + `sr_number`.
- Imports: only `TextContent` (line 93). No `ImageContent`, `EmbeddedResource`, resource handlers, `_meta`, `outputSchema`, or `ui://`. Return annotation `-> list[TextContent]` (18902). Deep-research `search`/`fetch` return a positional `(content, structuredContent)` tuple (18930 to 18954); the wrapper passes tuples straight through (18871 to 18873) and otherwise appends `_OPEN_ACCESS_NOTE` to the last text block, guarded by `hasattr(last,"text")` (18877).
- Token budget is char/row caps only: `MAX_SNIPPET_LEN=500` (234), `DEFAULT_LIMIT=50` (235), `MAX_LIMIT=2000` (236), `_truncate` (7910), `fields` full/compact and `offset` pagination on `search_decisions`. No token accumulator.

## 3. Architecture: render once, three surfaces

One canonical structured result per query, produced once, rendered three ways:

```
canonical law-result list  (the single source of truth)
        |-- markdown text         -> all 4 providers (always present, always citable)
        |-- MCP-Apps HTML widget   -> Claude (Tier B1)
        |-- ChatGPT Apps component -> ChatGPT (Tier B2)
```

Gemini and Copilot consume the markdown (no reliable inline widget). The widget is presentation only; the model quotes from the verbatim text fields, never the widget.

### Canonical law-result schema (the contract both Tier A and Tier B consume)

```
LawHit = {
  "level": "federal" | "cantonal",
  "canton": "CH" | "ZH" | ...,
  "sr_number": str | None,
  "systematic_number": str | None,     # cantonal SR-equivalent
  "abbreviation": str | None,          # e.g. "OR"
  "title": str,                        # law title
  "article_num": str | None,
  "marker": "Art." | "§",              # canton-specific, reuse logic at 15605
  "heading": str | None,               # article heading
  "snippet": str,                      # contains internal highlight sentinels (see 4.1)
  "reference": str,                    # display label, assembled from verbatim parts (see 4.4)
  "url": str | None,                   # canonical link (LexFind original_url or Fedlex eli)
  "lexfind_id": str | None,
  "language": "de" | "fr" | "it" | None
}
```

Verbatim full article text is NOT in `LawHit` (it stays in `get_law`); search returns the snippet plus the link, the widget lazy-loads full text via `get_law` on click.

## 4. Tier A: universal highlighted text plus structuredContent

### 4.1 Highlighting (no pipeline change)

Introduce one internal highlight convention and render it per surface. Use invisible sentinels to avoid collisions with law text:

```
HL_OPEN  = ""   # private-use unicode, cannot occur in source text
HL_CLOSE = ""
```

Normalise all snippet sources to the sentinels at the search layer:
- Federal/cantonal-mirror FTS: change the `snippet(... '>>>','<<<' ...)` markers to the sentinels (15172, 15186, 15200, 15214, 15327).
- Cantonal-local: change `<b>`/`</b>` (16001) to sentinels.
- LexFind: stop discarding match spans; in `_clean_lexfind_html` (15739) convert LexFind's highlight tags to sentinels before stripping the rest of the HTML.
- Fallback when a source gives no highlight info: on-the-fly mark the sanitised, synonym-expanded query terms (reuse `_expand_law_query` output, 15379) in the snippet, word-boundary, case-insensitive, multilingual; never mark FTS operator tokens (respect `_sanitize_fts5`, invariant #3).

Render helper:
```
_render_highlight(snippet: str, surface: "text" | "html") -> str
  text: HL_OPEN/HL_CLOSE -> "**" / "**"   (markdown bold; identical on all 4 providers)
  html: HL_OPEN/HL_CLOSE -> "<mark>" / "</mark>"
```
Markdown bold is chosen over `<mark>` for text because `<mark>` does not render in all four LLM clients; bold does.

### 4.2 Improved cards

Rewrite `_format_search_laws_response` (15587) and `_format_search_legislation_response` (16830) to emit consistent, scannable cards:
```
**{i}. {reference}** {markdown-link(title, url)}  ({canton}, SR {sr_number})
   {rendered, highlighted snippet}
```
Lean: one snippet per hit (capped at `MAX_SNIPPET_LEN`), no redundant fields, markdown link to the full law, `reference` as the bold lead so results are scannable. Keep the federal/cantonal interleave (15517).

### 4.3 Token budget for ChatGPT and Copilot

Add explicit response-size discipline (those two have the tightest limits; cf. the enumerate-all fix):
- Cap snippet length (`MAX_SNIPPET_LEN`), cap default result count, expose `offset` pagination on `search_laws`/`search_legislation` (mirror `search_decisions`).
- Add a `fields` switch (`full`/`compact`) so callers can request lean cards.
- A soft total-size guard: if the assembled text exceeds a byte budget, reduce per-hit snippet length before dropping hits (never silently truncate the hit list; page instead).

### 4.4 Law reference helper and R1 to R3

Add `_build_law_reference(hit) -> str` that assembles a display reference ("Art. 41 OR", "§ 12 [ZH] ...") from the **verbatim** components already pulled from the mirror (`abbreviation`, `article_num`, `sr_number`, canton marker). This is assembly of verbatim parts, not fabrication, and matches what the model already does manually; it makes the reference consistent and lets the widget show it.

R1 to R3 handling:
- The highlight only wraps snippet text that is itself verbatim from the article (FTS snippet of the stored article text); marks are presentational and are stripped for any length/verbatim comparison.
- The citable verbatim article text remains sourced from `get_law` (R2 unchanged).
- `reference` is a display label; quotes still come from `get_law`/`get_regeste`. Document this in the tool description so models do not treat `reference` as a quotable string.

### 4.5 structuredContent (enables Tier B)

Return the `(content, structuredContent)` tuple for `search_laws` and `search_legislation` (mirror search/fetch at 18930 to 18954): `content` is the markdown `TextContent`, `structuredContent` is `{"query": ..., "hits": [LawHit, ...], "total": N, "offset": ...}`. Handle the `_OPEN_ACCESS_NOTE` append for tuple returns (either append inside the tuple's text block or teach the wrapper at 18871 to 18877 to append on tuples too).

### 4.6 Tier A tests (offline, fixtures, no live network)

- `_render_highlight`: sentinel to bold/`<mark>`, idempotent, no-op when no sentinel.
- Snippet normalisation: federal `>>>`, cantonal-local `<b>`, LexFind spans all map to sentinels; on-the-fly fallback marks expanded terms, skips operators.
- Verbatim preservation: snippet with marks removed equals the source substring.
- Cards: structure, link present, reference lead, multilingual.
- Token budget: snippet cap, pagination, compact mode, soft size guard reduces snippet before dropping hits.
- `structuredContent`: schema matches `LawHit`; tuple shape; open-access-note still applied.
- Fixtures: small `statutes.db` and `cantonal_laws.db` fixtures; LexFind mocked.

## 5. Tier B: in-client widgets (enhancement)

Both widgets render from the Tier A `structuredContent`. Always paired with the Tier A text (fallback and citable source). Behind a feature flag (`OCL_UI_WIDGETS`), default off until validated per client.

### 5.1 Claude (MCP Apps), Phase B1

Per the current MCP Apps spec: a tool declares UI via `_meta.ui.resourceUri` pointing at a `ui://` resource of mimeType `text/html;profile=mcp-app`; the host fetches it via `resources/read` and renders it, passing the tool result's `structuredContent` to the widget.

Net-new work:
- Widen imports (line 93) only as needed; add `@server.list_resources` and `@server.read_resource` handlers (none exist today).
- Register `ui://opencaselaw/law-search` returning the widget HTML (`text/html;profile=mcp-app`).
- Set `_meta={"ui":{"resourceUri":"ui://opencaselaw/law-search"}}` on the `search_laws`/`search_legislation` `Tool(...)` definitions (defs begin 16998).
- Widget HTML: self-contained, sandboxed iframe; inline CSS/JS; reads the injected `structuredContent`; renders hit cards with `<mark>` highlights, the reference, the title link; click a hit to expand full article text via `postMessage({type:'tool', payload:{toolName:'get_law', params:{...}}})`; multilingual labels (DE/FR/IT/EN). No external dependencies (CSP/sandbox safe).

### 5.2 ChatGPT (Apps SDK), Phase B2

ChatGPT renders via `window.openai`. The component reads `window.openai.toolOutput` (the `structuredContent`) and renders the same cards. Wire via the Apps-SDK `_meta` keys (e.g. `openai/outputTemplate`) on the same tools, or an adapter that maps the MCP-Apps `postMessage` contract to `window.openai`. Reuse the existing ChatGPT shim plumbing (search/fetch already return structuredContent).

### 5.3 Gemini and Copilot

No widget (no reliable inline UI). They receive the Tier A text, which is already omnilex quality. Documented, not a gap.

### 5.4 Tier B tests

- `list_resources` includes `ui://opencaselaw/law-search`; `read_resource` returns the widget with mimeType `text/html;profile=mcp-app`.
- Tool defs carry `_meta.ui.resourceUri` when the flag is on, absent when off.
- Widget template, given sample `structuredContent`, contains the expected hit elements and `<mark>` highlights (HTML assertion).
- postMessage intent maps to a valid `get_law` call (contract test).
- Fallback: the `TextContent` is present and complete even when the widget is attached.

## 6. Phasing

- A1: schema, `_build_law_reference`, sentinel and `_render_highlight` helpers (+ tests).
- A2: highlighting wired into the two search formatters; improved cards; lean fields; pagination (+ tests).
- A3: `structuredContent` tuple for both search tools; open-access-note handling (+ tests). Ship Tier A; validate on all four providers.
- B1: resources infra; Claude widget; `_meta.ui.resourceUri`; flag (+ tests). Validate on Claude.
- B2: ChatGPT Apps component; reuse structuredContent; flag (+ tests). Validate on ChatGPT.
- B3 (optional): generalise the widget to `search_decisions`.

## 7. Risks and mitigations

- Client widget support varies and the MCP Apps spec is emerging: keep widgets thin, behind a flag, text always excellent and always present.
- Token limits (ChatGPT/Copilot): section 4.3 budget; pagination; compact mode.
- R1 to R3: highlight wraps verbatim only; `reference` is a display label; quotes from `get_law`; described in the tool text (section 4.4).
- FTS5 sanitisation (invariant #3): on-the-fly fallback marks only sanitised/expanded terms, never operators.
- No pipeline change in Tier A (no new FTS table); Tier B touches only the serving layer (mcp_server.py), not the build. No `publish.py`/schema/state changes.
- Tuple return bypasses the open-access-note path: handle explicitly (4.5).
- Return-type and import widening (line 93, 18902) for Tier B: localised, the wrapper already tolerates non-text trailing blocks (18877).

## 8. Verification

- `make test` green (new offline tests).
- Per-provider manual matrix: Claude (text + widget), ChatGPT (text + component), Gemini (text), Copilot (text): highlighting renders, links work, no token-limit errors, references correct, full text loads on click (widgets).
- Search quality unchanged: Tier A alters formatting/payload, not ranking; confirm MRR benchmark unmoved.
- Response-size checks: snippets capped, no oversize responses, pagination correct.
- R1 to R3 audit: snippet-minus-marks equals source; verbatim text from `get_law`; reference assembled from verbatim fields only.

## 9. Open questions for review

1. Reference helper: OK to add `_build_law_reference` that assembles "Art. 41 OR" from verbatim components, framed as a display label (not a quotable citation)? It removes ambiguity but is the one place we compose a reference string.
2. Widget interactions beyond click-to-expand (e.g. in-widget follow-up search, language toggle): include in B1 or keep B1 read-only and add later?
3. Feature-flag default: ship Tier B widgets off-by-default and enable per client after manual validation (recommended), or enable on Claude immediately once tests pass?
4. ChatGPT path (B2): native Apps-SDK component vs an MCP-Apps-to-window.openai adapter. Adapter is less code but adds a translation layer; native is more control. Preference?
