# OpenCaseLaw Word Plugin — Design Spec

## Goal

A Microsoft Word Add-in that lets lawyers search Swiss court decisions, insert formatted citations, look up statutes, and verify opposing counsel's case references — all without leaving Word.

## Architecture

A **Microsoft Office Web Add-in** (Office.js) — a single-page web app hosted at `word.opencaselaw.ch`, loaded in Word's task pane sidebar. Works on Word for Windows, Mac, and Web (same codebase). Communicates with the existing REST API at `mcp.opencaselaw.ch/api/`.

**Two tiers:**

- **Tier A (free, no key):** Search decisions, view details, insert citations, look up statutes — all via public REST API
- **Tier B (user's API key):** Reference verification — select text in Word, plugin uses Claude Haiku (via user's own Anthropic API key) to check if the cited decision supports the claim

### Components

| Component | Technology | Hosted at |
|-----------|-----------|-----------|
| Add-in manifest | `manifest.xml` (Office Add-in schema) | `word.opencaselaw.ch/manifest.xml` |
| Task pane UI | HTML/CSS/JS (vanilla, no framework) | `word.opencaselaw.ch/` |
| API backend | Existing REST API | `mcp.opencaselaw.ch/api/` |
| LLM verification | Client-side call to Anthropic API | User's browser (key stays local) |

No new backend infrastructure needed. The add-in is purely static files served by nginx + API calls to the existing server.

### CORS Configuration Required

The REST API's CORS is controlled by the `SWISS_CASELAW_CORS_ORIGINS` env var. Before deployment, add `https://word.opencaselaw.ch` to `SWISS_CASELAW_CORS_ORIGINS` in `/opt/caselaw/repo/.env.mcp` and restart workers. Without this, all fetch() calls from the plugin will fail.

### Existing API Endpoints Used

| Feature | Endpoint | Key Parameters |
|---------|----------|---------------|
| Search decisions | `GET /api/decisions` | `query`, `court`, `canton`, `language`, `date_from`, `date_to`, `limit` (default 50), `offset`, `sort` |
| Get decision detail | `GET /api/decisions/{decision_id}` | `full_text` (bool) |
| Case brief (for verify) | `GET /api/case-brief/{case}` | path param: docket number or BGE ref |
| List courts | `GET /api/courts` | — |
| Search laws | `GET /api/laws/search` | `query`, `sr_number`, `language`, `limit` |
| Get statute article | `GET /api/laws/{abbreviation}` | `article`, `language` |
| Leading cases | `GET /api/leading-cases` | `query`, `law_code`, `article`, `limit` |
| Doctrine | `GET /api/doctrine` | `query` |

## UI Design

### Theme & Layout

Light theme matching Word's native look. Task pane width: 350px (Office.js default). Language selector (DE/FR/IT/EN) in header bar. System font stack (`-apple-system, Segoe UI, sans-serif`).

### Header

- OpenCaseLaw logo (16px red square "OCL") + "OpenCaseLaw" text
- Language dropdown (DE ▾) — persists via `Office.context.roamingSettings` (syncs across devices)

### Views

**1. Search View (default)**

- Unified search bar: "Entscheide, Gesetze, Doktrin suchen..."
- Quick-action pills: "Art. _ OR" shortcut, "Leitentscheide" shortcut
- Filter dropdowns: Gericht, Datum, Sprache (collapsed by default)
- Result cards:
  - Docket number (bold), date, court
  - Leading case badge (★ Leitentscheid) when applicable
  - Regeste excerpt (2-3 lines)
  - Two buttons: green "Einfügen" (insert citation at cursor), grey "Volltext" (open detail)
- **Pagination:** Show 20 results initially, "Weitere laden" button at bottom loads next 20 (offset-based)
- **Loading state:** Skeleton cards (3 grey pulsing rectangles) while fetching
- **Empty state:** "Keine Treffer" with suggestion to broaden search
- **Error state:** "Verbindungsfehler — erneut versuchen" with retry button. On 429: "Zu viele Anfragen — bitte warten" with countdown

**2. Detail View**

- Back link "← Zurück zur Suche"
- Decision title (docket number, large)
- Date, chamber, court
- Tags: Leitentscheid badge, citation count, legal area
- Regeste section (white card)
- Erwägungen section: list of numbered Erwägungen, each with:
  - Section number (E. 2, E. 3, etc.)
  - First-line excerpt
  - Per-section green "Einfügen" button (inserts citation with specific E. reference)
- Statute articles section: clickable pills (Art. 253a OR, Art. 266l OR, etc.)
- Main "Einfügen" button at bottom (inserts citation without specific E.)

**Erwägung extraction:** The `/api/case-brief/{case}` endpoint returns pre-segmented Erwägungen with section numbers. The detail view renders these directly — no client-side parsing needed.

**3. Laws View**

- Search bar for statute articles
- Results showing article number, text excerpt
- "Einfügen" button to insert article text at cursor

**4. Verify View (Tier B — requires API key)**

- Triggered by selecting text in Word and clicking "Referenz prüfen"
- Shows selected text (blue left border, italic)
- Color-coded verdict card:
  - Green (✓): "Zutreffend" — reference supports the claim
  - Yellow (⚠): "Teilweise zutreffend" — partially relevant or nuanced
  - Red (✗): "Nicht zutreffend" — contradicts or is irrelevant
- Explanation text citing the specific Erwägung
- Quoted Erwägung text
- Two action buttons:
  - "Kommentar in Word einfügen" (color matches verdict) — inserts a Word comment
  - "Volltext anzeigen" — navigates to detail view
- Footer: confidence %, model used, "Eigener API-Key"

**5. Settings View**

- Language selector (DE/FR/IT/EN) with preview of citation format
- Tier B: Anthropic API key field (stored in localStorage, never transmitted to our servers)
- "API-Key testen" button to validate
- "Schlüssel löschen" button
- Link to opencaselaw.ch, GitHub, SECURITY.md

## Citation Format

Inserts **inline bracketed reference** at cursor position. Uses `Word.Range.insertText()` with `InsertLocation.after` on the current selection range to avoid overwriting selected text. Format adapts to selected language:

### BGE (Leitentscheide)

| Language | Without Erwägung | With Erwägung |
|----------|-----------------|---------------|
| DE | `(BGE 125 III 231)` | `(BGE 125 III 231, E. 3)` |
| FR | `(ATF 125 III 231)` | `(ATF 125 III 231, consid. 3)` |
| IT | `(DTF 125 III 231)` | `(DTF 125 III 231, consid. 3)` |
| EN | `(BGE 125 III 231)` | `(BGE 125 III 231, para. 3)` |

### BGer/BVGer/other courts

| Language | Without Erwägung | With Erwägung |
|----------|-----------------|---------------|
| DE | `(BGer 4A_747/2012 vom 5. April 2013)` | `(BGer 4A_747/2012 vom 5. April 2013, E. 2)` |
| FR | `(TF 4A_747/2012 du 5 avril 2013)` | `(TF 4A_747/2012 du 5 avril 2013, consid. 2)` |
| IT | `(TF 4A_747/2012 del 5 aprile 2013)` | `(TF 4A_747/2012 del 5 aprile 2013, consid. 2)` |
| EN | `(BGer 4A_747/2012 of 5 April 2013)` | `(BGer 4A_747/2012 of 5 April 2013, para. 2)` |

### Court name mapping by language

| Court | DE | FR | IT | EN |
|-------|----|----|----|----|
| bger | BGer | TF | TF | BGer |
| bge | BGE | ATF | DTF | BGE |
| bvger | BVGer | TAF | TAF | BVGer |
| bstger | BStGer | TPF | TPF | BStGer |
| bpatger | BPatGer | TFB | TFB | BPatGer |

## Reference Verification (Tier B)

### Flow

1. User selects a paragraph in Word containing one or more case references
2. Clicks "Referenz prüfen" button in the sidebar
3. Plugin extracts citation(s) from selected text via regex:
   - BGE pattern: `(BGE|ATF|DTF)\s+\d+\s+[IVX]+\s+\d+`
   - BGer pattern: `\d[A-Z]_\d+/\d{4}`
4. Fetches each cited decision via `/api/case-brief/{case}`
5. Sends prompt to Claude Haiku via Anthropic API (using user's key):
   - System: "You are a Swiss legal reference checker..."
   - User: selected text + decision brief
   - Output: JSON with verdict (supports/partial/contradicts), explanation, relevant_erwaegung
6. Displays color-coded verdict in sidebar
7. "Kommentar einfügen" inserts a Word comment via `Word.Range.insertComment()` with the verdict text

### Word Comment Insertion — API Version

`Word.Range.insertComment()` requires **WordApi 1.4** (Word 2019+). The manifest declares WordApi 1.1 as minimum for Tier A features. The "Kommentar einfügen" button is **feature-detected at runtime**: if `insertComment` is unavailable (Word 2016), the button is replaced with "Ergebnis als Text einfügen" which inserts the verdict as inline text instead.

### Anthropic API — Browser Access

Direct browser calls to `api.anthropic.com` require the `anthropic-dangerous-direct-browser-access: true` header. This is functional but Anthropic discourages it for production use.

**Fallback plan:** If Anthropic tightens browser CORS, add a thin proxy endpoint at `mcp.opencaselaw.ch/api/verify` that forwards the request using the user's key (passed as a header, not stored server-side). This is a single-endpoint change, not a redesign.

### API Key Security

- Stored in browser localStorage (key: `ocl_anthropic_key`)
- Sent directly from browser to `api.anthropic.com` with `anthropic-dangerous-direct-browser-access: true`
- Never transmitted to opencaselaw.ch servers
- Settings view has "Schlüssel löschen" button
- Note: In some Office clients (especially Word Online), localStorage may be scoped to the task pane iframe. This is acceptable — the key is only needed within the plugin.

### LLM Prompt

```
You are a Swiss legal reference verification assistant. Given a text passage
that cites a court decision, and the decision's case brief, determine whether
the citation accurately supports the claim made in the text.

Respond in JSON:
{
  "verdict": "supports" | "partial" | "contradicts",
  "explanation": "Brief explanation in the document's language",
  "relevant_erwaegung": "The most relevant E./consid. number",
  "quote": "Key quote from the decision (max 200 chars)"
}
```

## File Structure

```
tools/word-addin/
├── manifest.xml          # Office Add-in manifest (XML, Office schema v1.1)
├── index.html            # Task pane entry point
├── css/
│   └── style.css         # Light theme, system font, Word-matching
├── js/
│   ├── app.js            # Main: routing, state management, render loop
│   ├── api.js            # REST API client (fetch wrapper, error handling, retry)
│   ├── citation.js       # Citation formatting (4 languages × court types)
│   ├── word-api.js       # Word interaction (insert text, insert comment, get selection)
│   └── verify.js         # Tier B: extract refs, call Haiku, display verdict
├── assets/
│   ├── icon-16.png       # Ribbon icon 16×16
│   ├── icon-32.png       # Ribbon icon 32×32
│   └── icon-80.png       # Store icon 80×80
└── README.md             # Installation guide (sideload + AppSource)
```

## Hosting & Distribution

### Hosting

Static files served by nginx on the existing VPS at `46.225.79.22` (same server as `mcp.opencaselaw.ch`). Add a new server block for `word.opencaselaw.ch` with TLS (Let's Encrypt). The add-in is ~50KB total — negligible load.

CSP headers on the static hosting:
```
default-src 'self'; connect-src https://mcp.opencaselaw.ch https://api.anthropic.com; style-src 'self' 'unsafe-inline'; img-src 'self' data:;
```

### DNS

Add A record: `word.opencaselaw.ch` → `46.225.79.22` (same server as mcp.opencaselaw.ch)

### Distribution

1. **Sideload (immediate):** Download manifest.xml from `opencaselaw.ch/word`, add via File → Get Add-ins → Upload My Add-in
2. **AppSource (parallel):** Submit to Microsoft Partner Center for free listing. Users install directly from Word's Add-in store. Approval ~2 weeks.
3. **Installation guide** at `opencaselaw.ch/word` with step-by-step screenshots

### Manifest Requirements

- `<AppDomains>`: `mcp.opencaselaw.ch`
- `<Hosts>`: Document (Word only)
- `<Requirements>`: WordApi 1.1+ (covers Word 2016+, Word for Mac, Word Online)
- `<DefaultSettings><SourceLocation>`: `https://word.opencaselaw.ch/index.html`

## Testing

- **Unit tests:** citation formatting (all 4 languages × court types × with/without E.)
- **Manual test matrix:** Word for Windows, Word for Mac, Word Online
- **API integration:** search, get decision, get law — verify results render correctly
- **Tier B:** mock Anthropic API response for verification flow
- **Edge cases:** no results, API timeout (show retry), 429 rate limit (show countdown), invalid API key (show error in settings), decision not found, no citation found in selected text, Word 2016 without insertComment support
- **Citation insertion:** verify `insertText` with `InsertLocation.after` does not overwrite selected text

## Success Criteria

1. User can search and find a decision in <3 seconds
2. Inserted citation is correctly formatted for the selected language
3. Citation includes correct Erwägung number when inserted from detail view
4. Reference verification returns accurate verdict for a known case
5. Works identically on Word for Windows, Mac, and Web
6. Plugin loads in <1 second
