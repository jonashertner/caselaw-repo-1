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

### Existing API Endpoints Used

| Feature | Endpoint | Method |
|---------|----------|--------|
| Search decisions | `/api/decisions` | GET |
| Get decision detail | `/api/decisions/{id}` | GET |
| Case brief (for verify) | `/api/case-brief/{case}` | GET |
| List courts | `/api/courts` | GET |
| Search laws | `/api/laws/search` | GET |
| Get statute article | `/api/laws/{abbreviation}` | GET |
| Leading cases | `/api/leading-cases` | GET |
| Doctrine | `/api/doctrine` | GET |

All endpoints already have `Access-Control-Allow-Origin: *` CORS headers. No API changes required.

## UI Design

### Theme & Layout

Light theme matching Word's native look. Task pane width: 350px (Office.js default). Language selector (DE/FR/IT/EN) in header bar.

### Header

- OpenCaseLaw logo (16px red square "OCL") + "OpenCaseLaw" text
- Language dropdown (DE ▾) — persists in localStorage

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
- Link to opencaselaw.ch, GitHub, SECURITY.md

## Citation Format

Inserts **inline bracketed reference** at cursor position using `Office.context.document.setSelectedDataAsync()`. Format adapts to selected language:

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
   - BGE pattern: `BGE \d+ [IVX]+ \d+` / `ATF` / `DTF`
   - BGer pattern: `\d[A-Z]_\d+/\d{4}`
4. Fetches each cited decision via `/api/case-brief/{case}`
5. Sends prompt to Claude Haiku via Anthropic API (using user's key):
   - System: "You are a Swiss legal reference checker..."
   - User: selected text + decision brief
   - Output: JSON with verdict (supports/partial/contradicts), explanation, relevant_erwaegung
6. Displays color-coded verdict in sidebar
7. "Kommentar einfügen" calls `Office.context.document.getSelection()` then inserts a comment via the Word API with the verdict text

### API Key Security

- Stored in browser localStorage (key: `ocl_anthropic_key`)
- Sent directly from browser to `api.anthropic.com` (CORS-enabled)
- Never transmitted to opencaselaw.ch servers
- Settings view has "Schlüssel löschen" button

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
│   ├── api.js            # REST API client (fetch wrapper, error handling)
│   ├── citation.js       # Citation formatting (4 languages × court types)
│   ├── office.js         # Word API: insert text, insert comment, get selection
│   └── verify.js         # Tier B: extract refs, call Haiku, display verdict
├── assets/
│   ├── icon-16.png       # Ribbon icon 16×16
│   ├── icon-32.png       # Ribbon icon 32×32
│   └── icon-80.png       # Store icon 80×80
└── README.md             # Installation guide (sideload + AppSource)
```

## Hosting & Distribution

### Hosting

Static files served by nginx on the existing VPS. Add a new server block for `word.opencaselaw.ch` with TLS (Let's Encrypt). The add-in is ~50KB total — negligible load.

```
word.opencaselaw.ch → nginx → static files (tools/word-addin/)
```

### DNS

Add A record: `word.opencaselaw.ch` → `46.225.212.40` (same VPS)

### Distribution

1. **Sideload (immediate):** Download manifest.xml from `opencaselaw.ch/word`, add via File → Get Add-ins → Upload My Add-in
2. **AppSource (parallel):** Submit to Microsoft Partner Center for free listing. Users install directly from Word's Add-in store. Approval ~2 weeks.
3. **Installation guide** at `opencaselaw.ch/word` with step-by-step screenshots

### Manifest Requirements

- `<AppDomains>`: `mcp.opencaselaw.ch`, `api.anthropic.com`
- `<Hosts>`: Document (Word only)
- `<Requirements>`: WordApi 1.1+ (covers Word 2016+, Word for Mac, Word Online)
- `<DefaultSettings><SourceLocation>`: `https://word.opencaselaw.ch/index.html`

## Testing

- **Unit tests:** citation formatting (all 4 languages × court types × with/without E.)
- **Manual test matrix:** Word for Windows, Word for Mac, Word Online
- **API integration:** search, get decision, get law — verify results render correctly
- **Tier B:** mock Anthropic API response for verification flow
- **Edge cases:** no results, API timeout, invalid API key, decision not found, no citation in selected text

## Success Criteria

1. User can search and find a decision in <3 seconds
2. Inserted citation is correctly formatted for the selected language
3. Citation includes correct Erwägung number when inserted from detail view
4. Reference verification returns accurate verdict for a known case
5. Works identically on Word for Windows, Mac, and Web
6. Plugin loads in <1 second
