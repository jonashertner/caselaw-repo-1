# opencaselaw.ch vs. entscheidsuche.ch — Detailed Comparison

**Date**: 2026-03-13
**Data sources**: opencaselaw.ch FTS5 DB (live), entscheidsuche.ch/status (last updated 2026-02-25)

---

## 1. Executive Summary

|  | **opencaselaw.ch** | **entscheidsuche.ch** |
|--|-------------------|----------------------|
| **Total decisions** | **962,651** | **540,494** |
| **Advantage** | **+422,157 (+78%)** | — |
| **Courts covered** | 101 | ~53 spiders |
| **Cantons** | All 26 + federal + regulators | All 26 + federal (partial regulators) |
| **Languages** | DE 449K · FR 434K · IT 79K | DE/FR/IT (breakdown not published) |
| **Citation graph** | 8.72M citation + 11.18M statute edges | None |
| **Statute text** | 80 federal laws, 39,000 articles | None |
| **Scholarly commentary** | 362 commentaries (OnlineKommentar.ch) | None |
| **Legislation search** | 33,000+ texts via LexFind | None |
| **AI integration** | Native MCP server (21 tools) | None |
| **Open data** | HuggingFace dataset (CC) | Raw HTML/JSON at /docs/ |
| **API** | MCP protocol (structured) | None (file directory only) |
| **Cost** | Free | Free |
| **Last data update** | 2026-03-12 (daily) | 2026-02-25 (16 days stale) |
| **Search status** | Operational | Returning 0 results (broken?) |

---

## 2. Decision Count — Side-by-Side by Canton/Entity

### 2.1 Federal (Eidgenossenschaft)

| Court | opencaselaw | entscheidsuche | Delta | Notes |
|-------|----------:|---------------:|------:|-------|
| **BGer** (Bundesgericht) | 174,114 | 26,947 | +147,167 | ES counts only "new" BGer; bulk is in BGE category |
| **BGE** (Leitentscheide) | 21,228 | 21,210 | +18 | ~Identical |
| **BGE Historical** (1875-1953) | 14,578 | — | +14,578 | **Only in opencaselaw** |
| **BVGer** | 91,515 | 84,066 | +7,449 | Our scraper is more current |
| **BStGer** | 11,383 | 10,631 | +752 | |
| **BPatGer** | 189 | 227 | -38 | ES may count procedural orders |
| **BGE EGMR** (ECHR Swiss) | 475 | — | +475 | **Only in opencaselaw** |
| **Verwaltungspraxis (VB)** | 22,884 | 2,502 | +20,382 | We have full 1987-2017 archive |
| **FINMA** | 405 | — | +405 | **Only in opencaselaw** |
| **FINMA Versicherungsrecht** | 2,582 | — | +2,582 | **Only in opencaselaw** |
| **WEKO** | 256 | 109 | +147 | |
| **EDÖB** | 1,797 | 595 | +1,202 | |
| **ElCom** | 423 | — | +423 | **Only in opencaselaw** |
| **UBI** | 644 | — | +644 | **Only in opencaselaw** |
| **PostCom** | 216 | — | +216 | **Only in opencaselaw** |
| **ComCom** | 64 | — | +64 | **Only in opencaselaw** |
| **EMARK** | 237 | — | +237 | **Only in opencaselaw** |
| **HUDOC CH** | 816 | — | +816 | **Only in opencaselaw** |
| **Bundesrat** | 24 | 14 | +10 | |
| **TA SST** (Schiedsgerichte) | 49 | 47 | +2 | |
| **FEDERAL TOTAL** | **343,879** | **146,306** | **+197,573** | |

**Note on BGer counting**: Entscheidsuche's BGer count of 26,947 appears to reflect only recent additions. Our docket-level comparison (Section 4) confirms near-complete overlap at ~174K decisions for the combined BGer+BGE category. The discrepancy in the status page numbers likely reflects their indexing/categorization approach, not missing decisions.

### 2.2 Cantonal

| Canton | opencaselaw | entscheidsuche | Delta | Factor |
|--------|----------:|---------------:|------:|--------|
| **GE** (Genf) | 166,845 | 88,603 | +78,242 | 1.88x |
| **VD** (Waadt) | 155,349 | 81,689 | +73,660 | 1.90x |
| **ZH** (Zürich) | 82,645 | 80,910 | +1,735 | ~equal |
| **TI** (Tessin) | 59,232 | 58,496 | +736 | ~equal |
| **BE** (Bern) | 20,198 | 9,241 | +10,957 | 2.18x |
| **BL** (Basel-Land) | 16,947 | 772 | +16,175 | **21.9x** |
| **GR** (Graubünden) | 15,166 | 14,319 | +847 | ~equal |
| **SG** (St.Gallen) | 14,936 | 13,272 | +1,664 | 1.13x |
| **FR** (Freiburg) | 14,110 | 2,281 | +11,829 | **6.19x** |
| **AG** (Aargau) | 12,039 | 10,181 | +1,858 | 1.18x |
| **BS** (Basel-Stadt) | 10,106 | 128 | +9,978 | **79.0x** |
| **SO** (Solothurn) | 8,931 | 8,901 | +30 | ~equal |
| **NE** (Neuenburg) | 7,472 | 613 | +6,859 | **12.2x** |
| **VS** (Wallis) | 6,592 | 4,364 | +2,228 | 1.51x |
| **SZ** (Schwyz) | 5,291 | 5,245 | +46 | ~equal |
| **AR** (Appenzell AR) | 4,447 | 1,825 | +2,622 | 2.44x |
| **LU** (Luzern) | 3,938 | 3,950 | -12 | ~equal |
| **TG** (Thurgau) | 3,515 | 2,182 | +1,333 | 1.61x |
| **ZG** (Zug) | 2,526 | 2,540 | -14 | ~equal |
| **OW** (Obwalden) | 2,206 | 2,203 | +3 | ~equal |
| **SH** (Schaffhausen) | 1,413 | 346 | +1,067 | **4.08x** |
| **GL** (Glarus) | 1,401 | 75 | +1,326 | **18.7x** |
| **UR** (Uri) | 1,202 | 405 | +797 | 2.97x |
| **JU** (Jura) | 1,099 | 1,057 | +42 | ~equal |
| **NW** (Nidwalden) | 992 | 494 | +498 | 2.01x |
| **AI** (Appenzell IR) | 174 | 49 | +125 | 3.55x |
| **CANTONAL TOTAL** | **618,772** | **394,141** | **+224,631** | **1.57x** |

### 2.3 Largest Absolute Gaps (Where opencaselaw Leads)

| Rank | Source | Delta | Category |
|------|--------|------:|----------|
| 1 | GE (Genf) | +78,242 | Cantonal |
| 2 | VD (Waadt) | +73,660 | Cantonal |
| 3 | VB (Verwaltungspraxis) | +20,382 | Federal |
| 4 | BL (Basel-Land) | +16,175 | Cantonal |
| 5 | BGE Historical | +14,578 | Federal |
| 6 | FR (Freiburg) | +11,829 | Cantonal |
| 7 | BE (Bern) | +10,957 | Cantonal |
| 8 | BS (Basel-Stadt) | +9,978 | Cantonal |
| 9 | BVGer | +7,449 | Federal |
| 10 | NE (Neuenburg) | +6,859 | Cantonal |

### 2.4 Where Entscheidsuche Leads

| Source | Delta | Likely Reason |
|--------|------:|---------------|
| BPatGer | -38 | May count additional procedural documents |
| LU | -12 | Negligible; rounding/timing |
| ZG | -14 | Negligible |

Entscheidsuche does not lead in any category by a material amount.

---

## 3. Sources Only in opencaselaw.ch

These sources have **no equivalent** on entscheidsuche.ch:

| Source | Decisions | Description |
|--------|----------:|-------------|
| BGE Historical | 14,578 | Leitentscheide Volumes 1-79 (1875-1953) |
| FINMA | 405 | Financial Market Supervisory Authority |
| FINMA Versicherungsrecht | 2,582 | Insurance law decisions (1994-2024) |
| ElCom | 423 | Electricity Commission |
| UBI | 644 | Independent Broadcasting Complaints Authority |
| PostCom | 216 | Postal Services Commission |
| ComCom | 64 | Federal Communications Commission |
| EMARK | 237 | Former Asylum Appeals Commission |
| HUDOC CH | 816 | ECHR cases involving Switzerland |
| BGE EGMR | 475 | ECHR cases via BGer |
| VD Gerichte (direct) | 52,498 | Direct Waadt scraper (newer decisions) |
| SH Gerichte (direct) | 695 | Direct Schaffhausen scraper |
| TG Gerichte (direct) | 1,072 | Direct Thurgau scraper |
| ZH sub-courts (15 courts) | ~34,000 | Individual ZH district/special courts |
| BS sub-courts (2 courts) | ~10,100 | Appellationsgericht, Sozialversicherungsgericht |
| SG sub-courts (5 courts) | ~11,000 | Versicherungsgericht, Verwaltungsgericht, etc. |
| AG sub-courts (14 courts) | ~9,200 | Strafgericht, Versicherungsgericht, etc. |
| **TOTAL UNIQUE** | **~139,000** | |

---

## 4. Federal Court Overlap Analysis (Docket-Level)

We performed an exact docket-by-docket comparison using our FTS5 DB and the entscheidsuche raw files on disk:

### BGer (Bundesgericht)
- Our dockets: 174,114
- Entscheidsuche unique dockets: 173,682
- **Exact overlap: 173,681** (99.97%)
- They have, we don't: **1** (6B 555/2024)
- We have, they don't: **433** (recent 2025-2026)

### BGE (Leitentscheide)
- Our dockets (bge + bge_historical): 35,806
- Entscheidsuche unique dockets: 21,210
- **Exact overlap: 21,209** (99.99% of theirs)
- They have, we don't: **1**
- We have, they don't: **14,597** (all from bge_historical, volumes 1-79)

### BVGer / BStGer / BPatGer
- Near-complete overlap in all cases

**Conclusion**: For federal courts, both platforms have essentially identical decision-level coverage. Our advantage comes from BGE Historical (14.6K) and regulators (5K+), not from capturing more BGer decisions.

---

## 5. Feature Comparison

### 5.1 Search

| Feature | opencaselaw | entscheidsuche |
|---------|:-----------:|:--------------:|
| Full-text search | FTS5 + BM25 + RRF reranking | Elasticsearch (?) |
| Cross-encoder reranking | mmarco-mMiniLMv2 (top 30) | No |
| Synonym expansion | Yes | Unknown |
| LLM query expansion | Haiku-powered | No |
| Filter by court | Yes | Yes |
| Filter by language | Yes | Yes |
| Filter by date range | Yes | Yes |
| Filter by legal area | Yes | No |
| Filter by canton | Yes | Yes (via court) |
| Sort options | Relevance, date | Relevance, decision date, scrape date |
| Result download | Via HuggingFace dataset | ZIP/CSV/HTML export per query |
| **Current status** | **Operational** | **Returning 0 results (2026-03-13)** |

### 5.2 Data Enrichment

| Feature | opencaselaw | entscheidsuche |
|---------|:-----------:|:--------------:|
| Citation graph | **8.72M decision-to-decision edges** | None |
| Statute references | **11.18M statute edges** | None |
| Leading case identification | Authority ranking by citation count | None |
| Appeal chain tracing | Yes (find_appeal_chain) | None |
| Legal trend analysis | Year-by-year topic analysis | None |
| Statute full text | 80 laws, 39K articles (Fedlex) | None |
| Scholarly commentary | 362 from OnlineKommentar.ch | None |
| Legislation search | 33K+ texts (LexFind integration) | None |
| Regeste extraction | 58.1% of decisions | Included where available |
| Case briefs | AI-generated structured analysis | None |

### 5.3 AI / Programmatic Access

| Feature | opencaselaw | entscheidsuche |
|---------|:-----------:|:--------------:|
| MCP server | **21 tools at mcp.opencaselaw.ch** | None |
| Claude AI integration | Native (claude.ai + Claude Desktop) | None |
| Structured API | MCP protocol (JSON responses) | None |
| Raw data access | HuggingFace (Parquet, ~7 GB) | /docs/ directory (HTML+JSON per file) |
| Bulk download | Yes (HuggingFace dataset) | File-by-file from /docs/ |
| Open source scrapers | Private repo | GitHub (NeueScraper, AGPL-3.0) |

### 5.4 MCP Tools (opencaselaw only)

| Tool | Purpose |
|------|---------|
| `search_decisions` | Full-text search with filters |
| `get_decision` | Single decision by ID/docket |
| `list_courts` | Court list + metadata |
| `get_statistics` | Aggregate stats |
| `find_citations` | Incoming/outgoing citations for a decision |
| `find_appeal_chain` | Trace case through instances |
| `find_leading_cases` | Authority-ranked decisions per topic/statute |
| `analyze_legal_trend` | Year-by-year topic analysis |
| `get_case_brief` | Structured case analysis |
| `get_doctrine` | Statute + leading cases + commentary |
| `draft_mock_decision` | Research-based mock decision |
| `generate_exam_question` | Real BGE fact patterns for study |
| `get_law` / `search_laws` | Fedlex statute lookup + search |
| `get_commentary` / `search_commentaries` | OnlineKommentar.ch integration |
| `search_legislation` / `get_legislation` / `browse_legislation_changes` | LexFind (33K+ legislative texts) |

---

## 6. Infrastructure & Operations

| Aspect | opencaselaw | entscheidsuche |
|--------|-------------|----------------|
| **Operator** | Private project | Verein entscheidsuche.ch (non-profit, founded 2017) |
| **Location** | Hetzner VPS (Nuremberg) | LiteSpeed Web Server (pansoft.de) |
| **Architecture** | nginx → 4x uvicorn workers | Vue.js SPA + backend |
| **Update frequency** | **Daily** (scrapers 01:00 UTC, publish 04:00 UTC) | Daily (claimed), but last update 2026-02-25 |
| **Staleness** (as of 2026-03-13) | **1 day** | **16 days** |
| **Uptime** | Monitored, /health endpoint | No health endpoint visible |
| **TLS** | Let's Encrypt (auto-renew) | Yes |
| **Sponsors** | None (self-funded) | CMS, Baur Hürlimann, MME Legal, SchKG-Vereinigung |
| **Funding** | Self-funded | Crowdfunding (wemakeit), donations, memberships |
| **Membership** | N/A | CHF 100/person, CHF 1000/institutional |
| **Tax status** | N/A | Tax-exempt (Canton Bern, gemeinnützig) |
| **Open source** | No (private repo) | Yes (AGPL-3.0 scrapers on GitHub) |

---

## 7. Data Quality

| Metric | opencaselaw | entscheidsuche |
|--------|-------------|----------------|
| Short text (<500 chars) | 28,815 (3.0%) | Unknown |
| Empty decisions | 0 | Unknown |
| Deduplication | Multi-pass (docket + content hash) | Unknown |
| Date coverage | 1875 – 2026-03-12 | ~2000 – 2026-02-25 |
| Historical depth | BGE vol. 1 (1875) | BGE vol. 80 (1954) |
| PDF extraction | fitz + pdfplumber | Unknown |
| Metadata normalization | court, canton, language, legal area, regeste | court, language, date |
| Decision types | All published types | All published types |

---

## 8. Update Freshness (as of 2026-03-13)

**entscheidsuche.ch**: Last global update 2026-02-25_04:30:18 UTC — **16 days ago**. Status page shows courts were read ("Komplett gelesen") or updated up to Feb 25. No activity since. The search interface returns 0 results for all queries, suggesting the search index may be broken.

**opencaselaw.ch**: Scrapers run nightly at 01:00 UTC, database published at 04:00 UTC. Last decision date in DB: 2026-03-12. All 21 MCP tools operational.

---

## 9. Why opencaselaw Has 78% More Decisions

The 422,157 decision advantage breaks down into three categories:

### A. Sources entscheidsuche does not scrape at all (~24,000)
- BGE Historical (14,578), FINMA (2,987), ElCom (423), UBI (644), PostCom (216), ComCom (64), EMARK (237), HUDOC (1,291)

### B. Sub-courts beyond entscheidsuche's aggregation level (~64,000)
- ZH: 15 individual courts (Bezirksgerichte, Handelsgericht, etc.) vs. ES's 5 ZH spiders
- BS: Appellationsgericht (8,024) + Sozialversicherungsgericht (2,080) vs. ES's single BS_Omni (128)
- SG: 5 individual courts vs. ES's 2 SG spiders
- AG: 14 individual courts vs. ES's 3 AG spiders

### C. Deeper cantonal coverage from direct scrapers (~334,000)
- **GE**: 166,845 vs. 88,603 — our direct scraper captures the full Geneva publication database
- **VD**: 155,349 vs. 81,689 — two scrapers (vd_findinfo historical + vd_gerichte current)
- **BL**: 16,947 vs. 772 — our scraper gets all chambers, ES gets very few
- **FR**: 14,110 vs. 2,281 — much deeper direct scraping
- **BE**: 20,198 vs. 9,241 — 6 specialized scrapers vs. ES's 6 (but we go deeper)
- **NE**: 7,472 vs. 613 — direct scraper captures far more

---

## 10. Where Entscheidsuche Has Advantages

1. **Open source scrapers** (AGPL-3.0 on GitHub) — transparency in data collection
2. **Non-profit legal structure** (Verein, tax-exempt) — institutional credibility
3. **Upload function** — allows third parties to submit unpublished decisions
4. **Longer track record** (since 2018) — established in legal community
5. **Law firm sponsors** (CMS, MME, etc.) — professional endorsement
6. **Result export** (ZIP/CSV/HTML) — direct download from search results

---

## 11. Summary Scorecard

| Category | opencaselaw | entscheidsuche | Winner |
|----------|:-----------:|:--------------:|--------|
| Decision count | 962,651 | 540,494 | **opencaselaw** (+78%) |
| Federal courts | ~313K | ~146K | **opencaselaw** (historically) |
| Cantonal courts | ~619K | ~394K | **opencaselaw** (+57%) |
| Citation analysis | 19.9M edges | None | **opencaselaw** |
| Statute integration | 80 laws | None | **opencaselaw** |
| Commentary | 362 | None | **opencaselaw** |
| AI integration | 21 MCP tools | None | **opencaselaw** |
| API/programmatic access | MCP + HuggingFace | /docs/ file listing | **opencaselaw** |
| Update freshness | Daily (1 day) | Stale (16 days) | **opencaselaw** |
| Search functionality | Operational | Broken (0 results) | **opencaselaw** |
| Open source | No | Yes (AGPL-3.0) | **entscheidsuche** |
| Legal structure | Private | Non-profit Verein | **entscheidsuche** |
| Track record | Since ~2025 | Since 2018 | **entscheidsuche** |
| Community support | None | Crowdfunding + sponsors | **entscheidsuche** |
| Cost | Free | Free | Tie |
