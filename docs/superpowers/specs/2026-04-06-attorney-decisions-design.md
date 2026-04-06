# Attorney Ethical & Bar Decisions — Design Spec

**Date**: 2026-04-06
**Scope**: New scrapers for attorney disciplinary decisions + tagging pipeline for attorney law BGer decisions

## Background

Attorney discipline in Switzerland is fully decentralized. Each canton's Aufsichtsbehörde handles discipline independently; the SAV (Swiss Bar Association) is a professional association, not a regulatory body. Most cantonal supervisory authorities do not publish decisions online (BGFA Art. 17 makes proceedings confidential by default).

We already capture attorney decisions through several existing scrapers:
- **BE**: `be_anwaltsaufsicht` (~65 decisions, Tribuna platform)
- **ZH**: `zh_gerichte` captures Aufsichtskommission as Obergericht chamber (41/42 decisions)
- **VD**: `vd_gerichte` captures CAVO (Chambre des avocats, 155 decisions)
- **BS**: `bs_gerichte` captures VD-type appeal decisions at Appellationsgericht

The SAV portal at sav-fsa.ch/rechtsprechung maintains curated digests of ~550 BGer attorney law decisions (organized by BGFA article) plus ~40 unique cantonal supervisory decisions.

## Part 1: New Scrapers

### 1a. GE Commission du barreau (+96 decisions)

**Change**: Add `"dcba": "GE_DCBA_001"` to `SECTIONS` dict in `scrapers/cantonal/ge_gerichte.py`.

No new code beyond the one-line addition. Same Drupal/Solr platform, same pagination/PDF logic. Decisions get IDs like `ge_gerichte_DCBA_1_2020`. Source: `justice.ge.ch/apps/decis/fr/dcba/search`. Language: French. Categories: DISCIP, SEC, LEV.

### 1b. SAV Kantone scraper (`scrapers/sav_kantone.py`, +40 decisions)

New `BaseScraper` subclass.
- **court_code**: `sav_kantone`
- **canton**: `CH` (federal-level aggregation of cantonal decisions)
- **Source**: `sav-fsa.ch/kantone` — Liferay AssetPublisher page with 40 entries, each linking to a PDF
- **discover_new()**: Fetch the Kantone page, parse all 40 entry links and metadata (canton, date, title from page text)
- **fetch_decision()**: Download each PDF, extract text via pdfplumber/fitz, detect language, extract citations
- **REQUEST_DELAY**: 2.0
- **Notes**: Static collection (~40 decisions, nothing added since ~2013). Runs once then idles on subsequent runs. Cantons represented: AG, BE, GE, GL, LU, OW, SG, UR, VD, ZG, ZH.

### 1c. SAV International scraper (`scrapers/sav_international.py`, +6 decisions)

New `BaseScraper` subclass.
- **court_code**: `sav_international`
- **canton**: `CH`
- **Source**: `sav-fsa.ch/international` — 6 accordion sections with PDFs and external links (HUDOC, CURIA)
- **discover_new()**: Parse accordion sections, extract PDF URLs and external links
- **fetch_decision()**: Download PDFs or fetch external HTML, extract text
- **REQUEST_DELAY**: 2.0
- **Notes**: Check overlap with existing `hudoc_ch` scraper via docket number matching, skip duplicates. Decisions from ECtHR and CJEU relevant to Swiss attorney law.

### 1d. TG Anwaltskommission (`scrapers/cantonal/tg_anwaltskommission.py`, +handful)

New `BaseScraper` subclass.
- **court_code**: `tg_anwaltskommission`
- **canton**: `TG`
- **Source**: `register.tg.ch/anwaltskommission/entscheide.html/10330`
- **discover_new()**: Parse HTML listing page
- **fetch_decision()**: Download linked PDFs, extract text via pdfplumber/fitz
- **REQUEST_DELAY**: 2.0
- **Notes**: Small volume. Confirmed at least 1 decision (AK.2018.45, Aug 2019, professional secrecy).

### 1e. FR Commission du barreau (`scrapers/cantonal/fr_anwaltsaufsicht.py`, +3)

New `BaseScraper` subclass.
- **court_code**: `fr_anwaltsaufsicht`
- **canton**: `FR`
- **Source**: `fr.ch/etat-et-droit/justice/commission-du-barreau` (Jurisprudence section)
- **discover_new()**: Parse page, find PDF links in Jurisprudence section
- **fetch_decision()**: Download PDFs, extract text, language: French
- **REQUEST_DELAY**: 2.0
- **Notes**: ~3 anonymized summary PDFs (2021, 2023, 2024). Started publishing Feb 2024 after Fribourg Law Review ceased. Tiny volume.

## Part 2: SAV BGFA/Bund Tagging Pipeline

### Purpose

Parse the 36 SAV-curated PDFs (25 BGFA article PDFs + 11 Bund period PDFs), extract BGer docket numbers, resolve them against our FTS5 DB, and store the mappings in a standalone SQLite DB. This allows filtering/discovering the ~550 BGer decisions that are about attorney law without duplicating them.

### Build script: `search_stack/build_anwaltsrecht_tags.py`

1. Download all 36 PDFs from sav-fsa.ch to a temp dir
2. Extract text via pdfplumber
3. Regex extraction of docket numbers:
   - BGer patterns: `2C_xxx/yyyy`, `2P.xxx/yyyy`, `5A_xxx/yyyy`, `1B_xxx/yyyy`, etc.
   - BGE references: `BGE 130 II 270`, `ATF 140 II 102`, etc.
4. For BGFA PDFs: associate each docket with the specific article (Art. 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25, 27, 28, 29, 34, 36)
5. For Bund PDFs: tag as source `bund` with no specific article
6. Resolve docket numbers against FTS5 DB (exact match on docket_number field)
7. Deduplicate (same decision may appear in both BGFA and Bund PDFs, or under multiple articles)

### Output: `output/anwaltsrecht_tags.db`

```sql
CREATE TABLE anwaltsrecht_tags (
    decision_id TEXT NOT NULL,       -- FTS5 decision_id
    bgfa_article TEXT,               -- e.g. "Art. 12", "Art. 17", NULL for Bund-only
    source TEXT NOT NULL,            -- "bgfa" or "bund"
    docket_number TEXT,              -- original docket from PDF
    PRIMARY KEY (decision_id, bgfa_article, source)
);
CREATE INDEX idx_article ON anwaltsrecht_tags(bgfa_article);
CREATE INDEX idx_decision ON anwaltsrecht_tags(decision_id);
```

### MCP integration

- `search_decisions()` gains optional parameter `legal_area` — when set to `"anwaltsrecht"`, JOINs against tags DB to filter results
- `search_decisions()` also accepts optional `bgfa_article` parameter (e.g. `"Art. 12"`) for article-specific filtering
- Tags DB opened read-only with `immutable=1` alongside existing DBs in MCP server startup
- Falls back gracefully if tags DB doesn't exist (filter parameters ignored, no error)

### Publishing pipeline integration

- Added as step in `publish.py` after FTS5 build
- Lightweight: ~36 PDF downloads + regex extraction, estimated <5 min
- Atomic write: build to `.db.tmp`, `os.replace()`
- Rebuilt on every publish cycle (cheap enough to always rebuild)
- PDF URLs are stable (Liferay document store with UUID paths)

## Part 3: Verification & Registration

### Verify existing coverage

Before building, confirm these are already captured:
- **ZH Aufsichtskommission**: 41/42 decisions in `zh_obergericht` with chamber containing "Aufsichtskommission". The 1 missing decision (no docket, 05.02.2009, "Zulässigkeit der Bezeichnung Rechtsanwälte und Notare") — add manually if possible.
- **VD CAVO**: 155 decisions in `vd_gerichte` with authority = CAVO
- **BS VD-type**: Attorney discipline appeals in `bs_gerichte`
- **AG Anwaltskommission**: Check AGVE entries in `ag_gerichte` for attorney commission decisions
- **SG Anwaltskammer**: Check `sg_publikationen` for any Anwaltskammer entries

### Scraper registration

Add to `SCRAPERS` dict in `run_scraper.py`:
```python
"sav_kantone": ("scrapers.sav_kantone", "SAVKantoneScraper"),
"sav_international": ("scrapers.sav_international", "SAVInternationalScraper"),
"tg_anwaltskommission": ("scrapers.cantonal.tg_anwaltskommission", "TGAnwaltskommissionScraper"),
"fr_anwaltsaufsicht": ("scrapers.cantonal.fr_anwaltsaufsicht", "FRAnwaltsaufsichtScraper"),
```

### Scheduling

- New scrapers added to daily scrape timer (same systemd timer as all others)
- All are low-volume/static — run in seconds after initial scrape
- `build_anwaltsrecht_tags.py` added to publish pipeline step sequence

### Testing

1. Each scraper tested locally: `python3 run_scraper.py <key> --max 3 -v`
2. Each scraper tested on VPS: `ssh ... 'timeout 60 python3 run_scraper.py <key> --max 3 -v 2>&1 | tail -20'`
3. Tags pipeline tested with subset of PDFs, verify docket resolution rate against FTS5 DB
4. MCP `legal_area=anwaltsrecht` filter verified with known BGer attorney law decisions
5. Full scraper health check after deploy: all 57 scrapers (53 existing + 4 new) show `success: true`

## Summary

| Component | New Decisions | Effort |
|---|---|---|
| GE dcba section | +96 | One-line change |
| SAV Kantone scraper | +40 | New scraper (simple) |
| SAV International scraper | +6 | New scraper (simple) |
| TG Anwaltskommission scraper | +handful | New scraper (simple) |
| FR Anwaltsaufsicht scraper | +3 | New scraper (simple) |
| Anwaltsrecht tags pipeline | 0 (tags ~550 existing) | New build script + MCP filter |
| **Total new decisions** | **~145+** | |

Not published online (confirmed): SAV national, ZAV Standesgericht, LU, GR, TI, VS, NE, SO cantonal supervisory authorities.
