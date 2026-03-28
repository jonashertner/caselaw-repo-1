# OpenCaseLaw Word Plugin Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Microsoft Word Add-in that lets lawyers search 963K Swiss court decisions, insert formatted citations, look up statutes, and verify case references — directly from Word's sidebar.

**Architecture:** Office Web Add-in (vanilla HTML/CSS/JS) hosted as static files at `word.opencaselaw.ch`, calling the existing REST API at `mcp.opencaselaw.ch/api/`. Tier A is free (search/cite/laws). Tier B adds LLM-powered reference verification using the user's own Anthropic API key.

**Tech Stack:** Office.js (Word API), vanilla JS (no framework), existing REST API, Anthropic API (client-side, Tier B only)

**Spec:** `docs/superpowers/specs/2026-03-28-word-plugin-design.md`

---

## Chunk 1: Foundation — Citation Module + Manifest + Scaffold

### Task 1: Citation Formatting Module

This is the core logic with zero dependencies — pure functions, fully testable.

**Files:**
- Create: `tools/word-addin/js/citation.js`
- Create: `tools/word-addin/tests/citation.test.js`

- [ ] **Step 1: Write citation tests**

Create `tools/word-addin/tests/citation.test.js` — a simple Node.js test file (no test framework needed). Test all 4 languages x BGE + BGer + BVGer + BStGer, with and without Erwägung numbers, including sub-section numbers like "2.1". Test the court name mapping (BGE→ATF in FR, BGE→DTF in IT). Test date formatting per language (DE: "vom 5. April 2013", FR: "du 5 avril 2013", IT: "del 5 aprile 2013", EN: "of 5 April 2013").

Run with: `node tools/word-addin/tests/citation.test.js`

- [ ] **Step 2: Run tests to verify they fail**

Run: `node tools/word-addin/tests/citation.test.js`
Expected: "Module not found" error

- [ ] **Step 3: Implement citation.js**

Create `tools/word-addin/js/citation.js` with:

- `COURT_NAMES` — mapping of court codes to display names per language (bger→BGer/TF/TF/BGer, bge→BGE/ATF/DTF/BGE, bvger→BVGer/TAF/TAF/BVGer, bstger→BStGer/TPF/TPF/BStGer, bpatger→BPatGer/TFB/TFB/BPatGer)
- `ERWAEGUNG_LABEL` — {de: 'E.', fr: 'consid.', it: 'consid.', en: 'para.'}
- `MONTH_NAMES` — full month names in all 4 languages
- `DATE_PREFIX` — {de: 'vom', fr: 'du', it: 'del', en: 'of'}
- `formatDate(dateStr, lang)` — "vom 5. April 2013" (DE uses period after day, others don't)
- `formatCitation(decision, lang, erwaegung?)` — returns `(BGE 125 III 231, E. 3)` or `(BGer 4A_747/2012 vom 5. April 2013, E. 2)` etc.
  - For BGE: extract vol/div/page from docket, replace BGE→ATF/DTF per language, no date
  - For other courts: court name + docket + formatted date
  - If erwaegung provided, append ", E. 3" / ", consid. 3" / ", para. 3" per language
- Export for both Node.js (tests) and browser

- [ ] **Step 4: Run tests to verify they pass**

Run: `node tools/word-addin/tests/citation.test.js`
Expected: All PASS

- [ ] **Step 5: Commit**

```
git add tools/word-addin/js/citation.js tools/word-addin/tests/citation.test.js
git commit -m "feat(word): citation formatting module with tests (4 langs x 5 courts)"
```

---

### Task 2: Office Add-in Manifest

**Files:**
- Create: `tools/word-addin/manifest.xml`

- [ ] **Step 1: Create manifest.xml**

Office Add-in XML manifest with:
- `Id`: a unique GUID (generate one)
- `Version`: 1.0.0
- `ProviderName`: OpenCaseLaw
- `DefaultLocale`: de-CH
- `DisplayName`: "OpenCaseLaw"
- `Description`: "Schweizer Rechtsprechung durchsuchen und zitieren — direkt in Word. 963'000+ Entscheide aller Gerichte."
- `IconUrl`: `https://word.opencaselaw.ch/assets/icon-32.png`
- `HighResolutionIconUrl`: `https://word.opencaselaw.ch/assets/icon-80.png`
- `SupportUrl`: `https://github.com/jonashertner/caselaw-repo-1`
- `AppDomains`: `mcp.opencaselaw.ch`
- `Hosts`: Document (Word only)
- `Requirements`: WordApi 1.1+
- `SourceLocation`: `https://word.opencaselaw.ch/index.html`
- `Permissions`: ReadWriteDocument

- [ ] **Step 2: Validate manifest XML**

Run: `xmllint --noout tools/word-addin/manifest.xml`
Expected: no errors

- [ ] **Step 3: Commit**

```
git add tools/word-addin/manifest.xml
git commit -m "feat(word): Office Add-in manifest (WordApi 1.1+, Word for Win/Mac/Web)"
```

---

### Task 3: HTML Scaffold + CSS

**Files:**
- Create: `tools/word-addin/index.html`
- Create: `tools/word-addin/css/style.css`
- Create: `tools/word-addin/assets/icon-16.png`, `icon-32.png`, `icon-80.png`

- [ ] **Step 1: Create index.html**

Single-page HTML entry point:
- Loads Office.js from CDN: `https://appsforoffice.microsoft.com/lib/1.1/hosted/office.js`
- Links `css/style.css`
- Header with: logo (red "OCL" square), "OpenCaseLaw" brand text, language select (DE/FR/IT/EN), settings gear button
- `<main id="app">` container — all views rendered here by app.js
- Script tags loading (in order): citation.js, api.js, word-api.js, verify.js, app.js

- [ ] **Step 2: Create style.css**

Light theme matching Word's native look. Key elements:
- CSS custom properties for colors (--bg: #fafafa, --card: #fff, --border: #e5e5e5, --accent: #d1242f, --green: #059669, --yellow: #f59e0b, --red: #dc2626, --blue: #2563eb)
- System font: `-apple-system, 'Segoe UI', system-ui, sans-serif`
- Sticky header with logo + language selector
- `.search-bar` — full-width input with subtle shadow, blue border on focus
- `.result-card` — white card with border, shadow, docket/date/regeste/action buttons
- `.badge` variants: badge-leading (red), badge-citations (blue), badge-area (green)
- `.btn-insert` (green), `.btn-detail` (grey), `.btn-full` (full width)
- `.section-card` — white card for detail view sections (Regeste, Erwägungen, Gesetze)
- `.erwaegung-row` — flex row with number, text excerpt, insert button
- `.pill` — small clickable statute article badges
- `.verdict-card` — color-coded card with .supports/.partial/.contradicts variants
- `.skeleton` — loading shimmer animation
- `.state-message` — centered empty/error text with optional retry button
- `.load-more` — full-width "load more" button
- Settings fields with labels and hints

- [ ] **Step 3: Create placeholder icon assets**

Create 16x16, 32x32, 80x80 PNG icons — red square (#d1242f) with white "OCL" text. Use ImageMagick if available, otherwise create manually.

- [ ] **Step 4: Verify HTML loads in browser**

Open `tools/word-addin/index.html` in a browser. Verify header renders with logo, brand, language selector, settings button. No JS errors in console (Office.js will fail outside Word, that's OK).

- [ ] **Step 5: Commit**

```
git add tools/word-addin/index.html tools/word-addin/css/style.css tools/word-addin/assets/
git commit -m "feat(word): HTML scaffold + CSS theme matching Word's light UI"
```

---

## Chunk 2: API Client + Word API + App Shell

### Task 4: REST API Client

**Files:**
- Create: `tools/word-addin/js/api.js`

- [ ] **Step 1: Create api.js**

Functions calling `mcp.opencaselaw.ch/api/`:
- `apiFetch(path, params)` — base fetch wrapper: builds URL with query params, handles 429 (throws `{type: 'rate_limit', retryAfter}`), handles non-OK (throws `{type: 'http_error', status, message}`)
- `searchDecisions(query, filters)` — GET /api/decisions with query, court, canton, language, date_from, date_to, limit (default 20), offset, sort
- `getDecision(decisionId)` — GET /api/decisions/{id}
- `getCaseBrief(caseRef)` — GET /api/case-brief/{case}
- `listCourts()` — GET /api/courts
- `searchLaws(query, opts)` — GET /api/laws/search
- `getLaw(abbreviation, article, language)` — GET /api/laws/{abbreviation}
- `getLeadingCases(query, lawCode, article)` — GET /api/leading-cases
- `getDoctrine(query)` — GET /api/doctrine

All functions are plain `async` functions in global scope (no module system — these are loaded via script tags).

- [ ] **Step 2: Commit**

```
git add tools/word-addin/js/api.js
git commit -m "feat(word): REST API client with rate-limit handling"
```

---

### Task 5: Word API Integration

**Files:**
- Create: `tools/word-addin/js/word-api.js`

- [ ] **Step 1: Create word-api.js**

Functions wrapping Office.js Word API:
- `insertTextAtCursor(text)` — uses `Word.run()`, gets selection range, calls `range.insertText(text, Word.InsertLocation.after)`, moves cursor to end
- `getSelectedText()` — uses `Word.run()`, gets selection range, loads text, returns it
- `insertComment(text)` — feature-detects WordApi 1.4 via `Office.context.requirements.isSetSupported('WordApi', '1.4')`, calls `range.insertComment(text)`, returns true/false
- `supportsComments()` — returns boolean for UI to show correct button

- [ ] **Step 2: Commit**

```
git add tools/word-addin/js/word-api.js
git commit -m "feat(word): Word API integration (insert text, get selection, comments)"
```

---

### Task 6: Main Application (app.js)

**Files:**
- Create: `tools/word-addin/js/app.js`

- [ ] **Step 1: Create app.js**

Main application with state management and view rendering:

**State object:** view (search/detail/laws/verify/settings), lang, query, results[], total, offset, loading, error, detail, caseBrief, verifyResult, verifyText, filters, courts[]

**Initialization:** `Office.onReady()` — load language from roamingSettings, bind language select + settings button, pre-fetch court list, call render()

**View renderers (each returns HTML string):**
- `renderSearch()` — search bar, filter buttons (including "Referenz prüfen"), skeleton loading, error state, empty state, result cards with badges + insert/detail buttons, "Weitere laden" pagination
- `renderResultCard(r, idx)` — single result card with docket, date, court, Leitentscheid badge, citation count, legal area, regeste excerpt, insert + detail buttons
- `renderDetail()` — back link, title, date, court, badges, Regeste section card, Erwägungen with per-section insert, statute pills, main insert button
- `renderLaws()` — back link, search input, results area
- `renderVerify()` — back link, title, selected text display, loading state, verdict card (color-coded), quoted Erwägung, comment/text insert button + fulltext button, footer
- `renderSettings()` — back link, citation format preview, API key field + save/test/delete buttons, links

**Event binding:** `bindEvents()` — delegated click handler on `#app` using `data-action` attributes. Actions: insert, detail, insert-main, insert-ew, back, load-more, retry, verify-ref, insert-comment, insert-verdict-text, verify-fulltext, save-key, test-key, delete-key. Enter key on search inputs triggers search.

**Action functions:**
- `doSearch(query)` — set loading, call searchDecisions, update state, render
- `loadMore()` — increment offset, fetch next page, append results
- `showDetail(decision)` — switch to detail view, fetch full decision + case brief in parallel
- `insertCitation(decision, erwaegung?)` — format with citation.js, insert with word-api.js
- `startVerify()` — check API key (redirect to settings if missing), get selected text, switch to verify view, call verifyReference
- `doInsertComment()` — format verdict text, call insertComment (fallback to insertText if unsupported)
- `doLawSearch(query)` — search laws, render results inline
- `testApiKey()` — send minimal request to Anthropic API, show alert

**Utilities:** `escHtml(s)` — HTML entity escaping for XSS prevention

- [ ] **Step 2: Verify in browser**

Open index.html in browser. Verify search bar renders, settings view works (language selector, API key field). Actual Word/API functionality requires Office context.

- [ ] **Step 3: Commit**

```
git add tools/word-addin/js/app.js
git commit -m "feat(word): main app with search, detail, laws, verify, settings views"
```

---

## Chunk 3: Verification Module + Deployment + Testing

### Task 7: Reference Verification (Tier B)

**Files:**
- Create: `tools/word-addin/js/verify.js`

- [ ] **Step 1: Create verify.js**

Functions for Tier B reference verification:
- `CITATION_PATTERNS` — two regexes: `(BGE|ATF|DTF)\s+\d+\s+[IVX]+\s+\d+` and `\d[A-Z]_\d+/\d{4}`
- `extractCitations(text)` — run both patterns, deduplicate, return array of citation strings
- `VERIFY_SYSTEM_PROMPT` — prompt instructing Claude to return JSON with verdict/explanation/relevant_erwaegung/quote
- `verifyReference(selectedText, apiKey, lang)`:
  1. Extract citations, throw if none found
  2. Fetch case brief for first citation via API
  3. Build brief text (regeste + sachverhalt + erwägungen + dispositiv, max 4000 chars)
  4. POST to `https://api.anthropic.com/v1/messages` with headers including `anthropic-dangerous-direct-browser-access: true`, model `claude-haiku-4-5-20251001`
  5. Parse JSON response, attach _decision for navigation
  6. Handle errors: 401 → invalid key, parse errors → try regex JSON extraction

- [ ] **Step 2: Commit**

```
git add tools/word-addin/js/verify.js
git commit -m "feat(word): reference verification module (Tier B, client-side Haiku)"
```

---

### Task 8: Server Deployment

- [ ] **Step 1: Add DNS record**

At GoDaddy: `word.opencaselaw.ch` A → `46.225.79.22`, TTL 600

- [ ] **Step 2: Deploy static files to VPS**

```
scp -i ~/.ssh/caselaw -r tools/word-addin root@46.225.79.22:/opt/caselaw/repo/tools/word-addin
```

- [ ] **Step 3: Create nginx server block**

SSH to VPS, create `/etc/nginx/sites-enabled/word-addin` with:
- HTTPS server block for `word.opencaselaw.ch`
- Root: `/opt/caselaw/repo/tools/word-addin`
- CSP header: `default-src 'self'; connect-src https://mcp.opencaselaw.ch https://api.anthropic.com; style-src 'self' 'unsafe-inline'; img-src 'self' data:; script-src 'self' https://appsforoffice.microsoft.com;`
- X-Frame-Options: `ALLOW-FROM https://word.officeapps.live.com`
- HTTP → HTTPS redirect block

- [ ] **Step 4: Obtain TLS certificate and reload nginx**

```
certbot certonly --nginx -d word.opencaselaw.ch
nginx -t && systemctl reload nginx
```

- [ ] **Step 5: Add CORS origin for Word plugin**

Append to `/opt/caselaw/repo/.env.mcp`:
`SWISS_CASELAW_CORS_ORIGINS=https://word.opencaselaw.ch`

Restart workers: `systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773`

- [ ] **Step 6: Verify deployment**

- `curl -s -o /dev/null -w "%{http_code}" https://word.opencaselaw.ch/` → 200
- `curl -s -o /dev/null -w "%{http_code}" https://word.opencaselaw.ch/manifest.xml` → 200
- `curl -s -H "Origin: https://word.opencaselaw.ch" -I https://mcp.opencaselaw.ch/api/courts | grep -i access-control` → shows Allow-Origin header

- [ ] **Step 7: Commit and push**

```
git add tools/word-addin/
git commit -m "feat(word): complete Word Add-in ready for sideload deployment"
git push origin main
```

---

### Task 9: Installation Guide

**Files:**
- Create: `tools/word-addin/README.md`

- [ ] **Step 1: Create README.md**

Installation guide covering:
- Sideload for Word Windows/Mac: download manifest.xml → File → Get Add-ins → Upload
- Sideload for Word Online: Insert → Add-ins → Upload My Add-in → paste URL
- Feature list: Tier A (free search/cite/laws) and Tier B (reference verification with API key)
- Citation format examples in all 4 languages
- License: Code MIT, Data CC0 1.0

- [ ] **Step 2: Commit**

```
git add tools/word-addin/README.md
git commit -m "docs(word): installation guide and feature overview"
```

---

### Task 10: End-to-End Testing

- [ ] **Step 1: Test in Word for Mac**

1. Sideload manifest.xml from `https://word.opencaselaw.ch/manifest.xml`
2. Search "Mietrecht Kündigung" → verify results
3. Click "Einfügen" → verify citation inserted at cursor
4. Click "Volltext" → verify detail view with Erwägungen
5. Insert specific E. → verify E. number in citation
6. Change language to FR → verify ATF format
7. Settings → API key → Test
8. Select paragraph with BGE → "Referenz prüfen" → verify verdict
9. "Kommentar einfügen" → verify Word comment

- [ ] **Step 2: Test in Word for Windows**

Repeat all steps from Step 1.

- [ ] **Step 3: Test in Word Online**

Repeat all steps from Step 1 (using online sideload method).

- [ ] **Step 4: Fix any issues found**

Address bugs, commit fixes.
