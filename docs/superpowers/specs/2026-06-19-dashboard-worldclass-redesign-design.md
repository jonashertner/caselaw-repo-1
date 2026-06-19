# opencaselaw.ch — World-Class Redesign (Refined Swiss Modernist)

Status: Design — approved 2026-06-19. Scope: Tier 0–3.
Source of the gaps: the 6-dimension audit (workflow w5vy1pbwx) + visual review.

## Goal
Make opencaselaw.ch render as a *world-class standout* — distinctive, credible, fast,
accessible — so the presentation matches the (genuinely best-in-class) substance:
a complete CC0 Swiss legal corpus + cryptographic Merkle provenance + AI-verifiable citations.

## Aesthetic: Refined Swiss Modernist
Evolve the existing International-Typographic-Style DNA with conviction. Keep austere,
add a surgical Swiss-red accent, real type hierarchy, hairline-grid depth, and one
orchestrated motion pass. Authoritative, timeless, on-brand for a Swiss legal institution.

### Design tokens (docs/static/css/design-system.css — the single shared sheet; changes propagate to all pages)
- Color (light): ink `#0A0A0A`, paper `#FFFFFF` / warm `#FAFAF8`; (dark): bg `#0F0F10`, text `#F5F5F4`.
- Accent: `--red #DA291C` (fills, live pulse, active, decorative), `--red-ink #BE1622` (links/text, ≥4.5:1). Red ≤5% of surface.
- Fix contrast: `--text-3 #a1a1aa → #6b7280` (4.5:1); collapse divergent green/accent inline tokens to the one AA design-system value.
- Hairline: `rgba(0,0,0,.10)` / dark `rgba(255,255,255,.12)`. Soft elevation shadow token for hover.
- Type: IBM Plex Sans 400/500/600/700 (self-hosted subset, tabular lining figures) + JetBrains Mono 400/500. Display = Plex 600/700 tight tracking. Keep the mono "data-terminal" section labels.
- Motion tokens + a global `@media (prefers-reduced-motion: reduce)` that disables all of it.

### Hero (docs/index.html)
- Real `<h1>`: "Every published Swiss court decision — open, free, and verifiable." Giant number = proof figure (count-up, bold, red thin-space separators), `aria-hidden` with the h1 carrying the sentence.
- Subhead: scale + CC0 (990,000+ band — single-sourced).
- CTAs: primary red-filled "Connect your AI →" (+ "30-sec setup, no API key"), secondary ghost "Search the corpus".
- Trust band: `● live · CC0 · updated daily · Merkle-provenanced (RFC 6962) · ~9,000 tool calls/day to practitioners` (anonymized adopter proxy — no names until permission confirmed).
- Persona router: practitioner · researcher · developer · student (anchor links).
- Faint Swiss-grid hero background + hairline framing.

### Signature visualization
Switzerland cantonal-coverage map: 26 cantons + federal courts, shaded by decision count.
Inline SVG, lazy-loaded (IntersectionObserver), reserved height (CLS 0), accessible `<table>` fallback + `role=img` aria-label. Tells the completeness story. (Fallback option: minimal citation-graph constellation.)

## Program (each tier = its own commit(s), verified on a local server + Playwright, deployed with approval)

### Tier 0 — Correctness / credibility (do first; unambiguous; mostly safe)
1. Single-source every number to one band ("990,000+") in static HTML now; real build-time injection in Tier 2. Files: index.html meta/og/hero-fallback/cards; entscheide.
2. Add the homepage `<h1>` (+ keep one-h1-per-page invariant).
3. Delete dead Chart.js `<script>` + `renderCharts`/`destroyCharts` dead code (−70KB; also remove the `renderCharts` call from the shared init try{} so a CDN failure can't blank the KPI grid). Apply on all 5 chart pages.
4. Connect widget: make it a conformant ARIA tabs component (reuse the demo-tab keyboard module → one `initTabs()`), or drop `role=tablist`. Add focus-visible.
5. Contrast + targets + focus: bump `--text-3`; lang-switch pills ≥24px; one shared `:focus-visible` (2px `--red`) covering connect-tab/demo-tab/copy/scholarship-search.
6. Render `governance-and-removal-policy.md` → `docs/governance/index.html` (shared shell); repoint the 5 footer links.
7. Branded 5-language `docs/404.html` with a /search/ link.
8. `<meta name="theme-color">` per scheme + manual light/dark/auto toggle (data-theme plumbing exists).
9. i18n the 5 scholarship-search dynamic strings (curLang pattern). Propagate the Qualität fix to courts/search/laws/coverage NAV_I18N.
10. Remove redundant `aria-label` on #sch-q; guard clipboard copy `.catch`.

### Tier 1 — Visual redesign
- design-system.css: new tokens (color/accent/type/depth/motion), button system (primary red / ghost), card hover-lift, focus rings, hairline-grid utilities.
- index.html: hero rebuild (h1 + count-up + CTAs + trust band + persona router + grid bg), section rhythm (a dark "Verifiable" feature section, alternating treatments, surface the Merkle/provenance claim), card refinement, motion (staggered load + scroll-reveal + live pulse).
- Signature cantonal map section (+ data wired from stats.json by-canton/by-court).
- Self-host fonts (subset woff2 + preload hero weight).
- Verify: local serve + Playwright across 5 langs + light/dark + mobile; show hero prototype before deploy.

### Tier 2 — Foundation (make consistency permanent)
- Finish `tools/build_docs.py`: migrate all hand-edited hubs into `src/pages/*` under one `tools/layout.html`; reconcile the toolbar-vs-header.site split; seo_pages.py consumes the same chrome fragment.
- Single locale source of truth (`locales/*.json` or one dict) consumed by landing data-t, hub nav/footer, /word; extend `test_dashboard_i18n_parity.py` to glob all pages.
- Build-time number injection from stats.json into every meta/og/JSON-LD/visible slot + sitemap `<lastmod>` + Dataset `dateModified`.
- CI gates: `build_docs.py --check`, i18n parity, number-drift grep, axe-core a11y over key pages.

### Tier 3 — Bigger bets (need a couple of your decisions)
- Host unification: serve `/entscheid/*` + the detail sitemap under opencaselaw.ch (reverse proxy on the Hetzner nginx) so brand + canonical + corpus are one entity. (Decision: proxy vs CNAME.)
- Zenodo DOI + richer Dataset schema (variableMeasured→PropertyValue, identifier/sameAs/dateModified/publisher/includedInDataCatalog); register with Google Dataset Search + opendata.swiss. (You mint the DOI.)
- BreadcrumbList JSON-LD + per-entity OG cards across detail pages; per-language hreflang clustering.
- robots.txt → add the mcp sitemap; real per-lang hreflang or drop the no-op set.

## Verification & safety
- All edits in `docs/` + `tools/` + `seo_pages.py`; live site = GitHub Pages on push.
- Per tier: build, serve `docs/` locally, Playwright audit (5 langs × light/dark × desktop/mobile), confirm no console errors / no raw-key leaks / Lighthouse-style checks; keep `make test` green; commit + deploy on approval.
- Romansh redesign copy reuses existing translations; new strings flagged for the existing RM review doc.

---

## v2 — RADICAL whole-site rethink (locked 2026-06-19, supersedes the incremental plan above)

Feedback that reset the brief: *"not consistent at all and still too crowded. rethink the
entire site, including integrity/standards/quality. every single element. absolute world-class
standout in all respects. edit radically if necessary."* + the strategic locks below.

### Strategic locks (from the owner)
- **Central message = COMPLETENESS + DAILY UPDATES.** Positioning: *"The complete record of
  Swiss law — rebuilt every day."* The giant number is proof of completeness; the daily-fresh
  signal proves it is living. This is the throughline on every page.
- **Keep on the homepage:** the live corpus stats (completeness + freshness proof) and a
  radically-simplified Connect flow. **Demote** verifiable/provenance and scholarship to
  secondary links (not homepage real estate). The 4-tab live demo → its own page or cut.
- **Rethink navigation entirely** — kill the flat 10-item engineering list. New nav is minimal
  and purposeful (corpus browse + a Connect CTA + language); the meta/trust pages
  (API, Standards, Integrity, Quality, Methodology, Word) move to the footer / a "Docs" group.
- **Execution = system + all pages in parallel**, against one locked contract.

### Three non-negotiable principles
1. **Consistency → one source of truth.** ONE rebuilt `design-system.css` (v3) + ONE page
   shell (`tools/layout.html`) + ONE locale file. EVERY page is generated from them — the
   inline-styled 6,750-line homepage island is deleted and rebuilt as a fragment. Finishing
   `tools/build_docs.py` is the *precondition*, not a later tier. No page is a hand-edited island.
2. **Restraint → editorial reduction.** Generous spacing + type scale; each page does ONE job
   with air; hairline rules instead of card-walls; density is the exception. Homepage ~27 → ~6
   sections.
3. **Standout → committed Swiss-modernist, identical everywhere.** Strong display type + mono
   labels, the red accent surgical and consistent, the grid as real structure, ONE orchestrated
   motion pass per page, and a few genuine signature moments.

### New homepage architecture (~6 sections, de-crowded)
1. **Hero** — completeness number (proof) + daily-fresh signal + the "every…every…every —
   complete, updated every day" h1 + ONE primary Connect CTA + a trust line (CC0 · all 26
   cantons + federal · since 1875).
2. **What it is** — three calm pillars (case law · legislation · scholarship) as editorial rows,
   not card grids; scholarship is a one-line mention + link.
3. **Connect** — radically simplified: one prominent copy-paste URL + a compact client list
   (not 7 crammed tabs).
4. **Signature viz** — Switzerland cantonal-coverage map (visual proof of completeness: all 26
   cantons + federal courts, shaded by count), lazy-loaded SVG + table fallback.
5. **Live corpus** — minimal live stats + the daily delta ("+N today"); link the demo out.
6. **Footer** — the meta/trust pages live here.

### Other pages, rethought as flagship editorial (same shell + system)
- **Integrity** — a calm, beautiful "prove any decision" page; Merkle root visualized, the
  verification path explained simply. Should feel like the most impressive page on the site.
- **Standards** — clean editorial (layered-identifier / open-law-standards positioning).
- **Quality** — a calm metrics page (a few big QC numbers + air), not a dense table.
- **Hubs (search/courts/laws/coverage) + detail pages** — inherit the identical shell + system.

### Execution plan
1. Lock the contract: build `design-system.css` v3 (restrained, completeness-centered) + the
   single `tools/layout.html` shell with the NEW nav + footer + one locale source + the
   canonical homepage fragment as the worked example.
2. Fan out (Workflow) the parallel rebuild of every page as `src/pages/*` fragments against the
   locked shell; `build_docs.py` emits `docs/`; `--check` gates drift.
3. Verify (local serve + Playwright: 5 langs × light/dark × mobile, no console errors / no
   raw-key leaks / one-h1 / contrast) → show → deploy on approval.

## Out of scope (for now)
- The undeployed `web_ui/` React app (decide separately: deploy-from-source or delete).
- Re-introducing charts (only if wanted; self-hosted tree-shaken + lazy-loaded).
