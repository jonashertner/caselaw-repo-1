# OpenCaseLaw — Technical Presentation (15 min)

## Slide 1: The Problem (1 min)

Swiss case law is published across 101 separate websites. The Bundesgericht publishes on bger.ch. The BVGer uses a Weblaw platform. Each of the 26 cantons runs its own portal — different formats, different search interfaces, different publication schedules.

If you want to find the leading case on Tierhalterhaftung, you need to know which court decided it, which portal to search, and which search syntax to use. Cross-court search doesn't exist in the open ecosystem. Swisslex and Weblaw solve this but charge subscription fees.

We set out to build the open equivalent: scrape every published Swiss court decision, deduplicate, index, extract citations, and make it searchable from any AI assistant.

---

## Slide 2: The Corpus (2 min)

The March 18, 2026 snapshot contains 962,272 decisions from 101 sources.

- **7 federal courts**: BGer (174,213 decisions), BVGer (91,560), BStGer (11,406), BPatGer (189), plus BGE published (21,228) and BGE historical back to 1875 (14,578).
- **81 cantonal courts** across all 26 cantons. Geneva is the largest with 166,912; Appenzell Innerrhoden the smallest with 79. That reflects publication practices, not scraping gaps.
- **11 federal quasi-judicial bodies**: FINMA, WEKO, EDÖB, ElCom, PostCom, ComCom, UBI, and others. These are not courts in the strict sense — the taxonomy matters and we state it explicitly.
- **2 historical/supranational sources**: ECHR Swiss cases and the former asylum commission (EMARK).

Languages: German 448,215 (46.6%), French 434,470 (45.2%), Italian 79,587 (8.3%). The near-parity between German and French comes from Geneva and Vaud, which together contribute over 320,000 French decisions.

Every decision has full text, structured metadata across 28 core fields, a source URL linking back to the originating court, and — for roughly half the corpus — an official regeste.

---

## Slide 3: Collection and Deduplication (2 min)

54 scrapers run nightly at 01:00 UTC. Each targets one publication channel. They're idempotent — they track what they've already fetched and only download new decisions.

The scrapers produce 1.24 million raw JSONL entries. After deduplication, 962,272 remain. The dedup logic normalizes court code, docket number, and date into a canonical key. "BL.2020.1", "BL_2020_1", and "BL-2020-1" all collapse to the same key. When duplicates exist, we keep the version with the longest full text.

Two things we deliberately do *not* deduplicate: A BGE leading case and its underlying BGer decision are different records — different courts, different docket numbers, different content scope. The BGE is an excerpt; the BGer ruling is the full decision. Similarly, a cantonal decision and its appeal to the Bundesgericht remain as two separate entries. These are distinct proceedings, not duplicates.

Cross-court overlap is handled by explicit groups. Zürich decisions can appear under 17 different court codes. We define overlap groups for cantons with this problem and deduplicate within each group.

One important caveat: the deduplication has not been formally evaluated against manual annotations. It works well in practice, but we haven't measured precision and recall.

---

## Slide 4: The Citation Graph (2 min)

We read every full text and extract citations using regular expressions. Four formats:

- BGE references: "BGE 131 III 115"
- Docket numbers: "4A_372/2019"
- BVGE references: "BVGE 2013/10"
- Statute provisions: "Art. 41 OR"

Each case-citation reference is resolved against the corpus. We normalize the docket and look for a match. Resolution rate: 73.8%. The 26% that don't resolve typically cite unpublished lower court decisions or use non-standard formatting.

| What | Count |
|------|-------|
| Extracted case-citation references | 9.86 million |
| Resolved to in-corpus decisions | 6.46 million |
| Statute-decision links | 11.3 million |
| Distinct statute provisions | 281,391 |

These are separate quantities — case citations and statute links are stored in different tables and should not be added together.

The most-cited decision is BGE 125 V 351 with 54,000 incoming citations. This kind of authority signal is useful for search: if a decision is cited by thousands of other decisions, it's likely a leading case.

The extraction is regex-based and not manually validated for precision/recall. It reliably catches standard citation formats but misses informal references and negative citations (where a case is distinguished rather than followed).

---

## Slide 5: How Search Works (3 min)

I'll walk through a concrete example. Someone asks: **"Hundebiss — wer haftet?"**

**Step 1 — The query goes to Claude Haiku.** Haiku knows Swiss law. It translates the colloquial question into legal doctrine:

```json
{
  "statutes": ["OR 56"],
  "doctrine": "Tierhalterhaftung",
  "synonyms": ["responsabilité du détenteur d'animaux"]
}
```

The user said "Hundebiss" but the decisions say "Tierhalterhaftung". Without this translation, the search finds nothing relevant. Cost: 0.01 Rappen.

**Step 2 — Multiple search strategies run in parallel.** We search for "Tierhalterhaftung", "Hundebiss", "Haftpflicht", "Art. 56 OR", and the French synonym, each as a separate FTS5 query. Some search only in regesten, some only in titles, some in the full text. Each strategy produces a ranked list.

**Step 3 — The lists are fused.** A decision found by five different strategies ranks higher than one found by only one. This is Reciprocal Rank Fusion — it rewards consistency across retrieval approaches.

**Step 4 — Citation graph signals are added.** If a candidate decision has 1,000 incoming citations, it's probably a leading case. If five of the ten search results all cite the same decision, that decision is probably the authoritative one for this query, even if it didn't rank first in the text search.

**Step 5 — The top 15 go back to Haiku for reranking.** Haiku sees each candidate's regeste and re-orders them by legal relevance. This step is skipped when the answer is obvious (docket lookups, clear winners). Cost: 0.02 Rappen.

**The output** includes the court name in readable form, the legal area (derived from which statutes are cited, ignoring procedural law like BGG), the top statute articles, the citation count, and a leading-case flag.

The frozen offline baseline — without any LLM features, purely lexical — achieves MRR@10 of 0.47. With Haiku parsing and reranking on the hosted system, MRR@10 reaches 0.65 on a 100-query test set. The hardest queries are concept-match cases where the user's vocabulary doesn't overlap with the legal doctrine terms at all.

---

## Slide 6: What a Result Looks Like (2 min)

Here's what comes back for "Tierhalterhaftung":

```
1. BGE 131 III 115
   Bundesgericht (BGE) | Zivilrecht | 04.10.2004
   Art. 56 Abs. 1 OR
   67 Zitierungen | ★ Leitentscheid

   "Tierhalterhaftung. Haftungsvoraussetzungen und
    Befreiungsbeweis des Tierhalters; Anforderungen
    an die Umzäunung einer Pferdeweide."
```

The result tells you immediately: this is a Bundesgericht decision from 2004, it's about civil law, specifically Art. 56 OR, it's been cited 67 times, and it's flagged as a leading case. The regeste summarizes the holding.

From here you can:
- Read the full text (`get_decision`)
- See what cites this decision and what it cites (`find_citations`)
- Trace the appeal chain from the cantonal court up (`find_appeal_chain`)
- Find all leading cases on Art. 56 OR ranked by authority (`find_leading_cases`)
- Look up the actual statute text (`get_law`)
- Read scholarly commentary on Art. 56 OR (`get_commentary`)

All from the same interface, all in the same session. The AI chains the tools as needed.

---

## Slide 7: Infrastructure (1.5 min)

Everything runs on a single VPS — 16 CPU cores, 64 GB RAM, about 116 euros per month.

The databases:
- FTS5 search index: 65 GB (full text of 962K decisions, inverted index for every token)
- Citation graph: 3.9 GB
- Statutes: 80 federal laws, 39,000 articles in three languages, from Fedlex
- Legislation: 33,000+ federal and cantonal texts via LexFind
- Commentary: 362 scholarly articles from OnlineKommentar.ch (CC-BY-4.0)

The nightly pipeline:
```
01:00  Scrapers fetch new decisions
04:00  Build FTS5 → export Parquet → upload HuggingFace → update dashboard
```

Total pipeline time: about 80 minutes. Zero-downtime deployment — the database is built to a temporary file and atomically swapped in.

The MCP server supports SSE transport (Claude) and Streamable HTTP (ChatGPT). No authentication required. Published decisions are excluded from copyright under Art. 5 URG.

---

## Slide 8: Limitations (1.5 min)

**Coverage is broad, not audited.** We scrape what courts publish online. We haven't done a court-by-court recall audit against official publication counts. Some cantons publish selectively. The corpus reflects published decisions, not all rendered decisions.

**About 26,000 decisions have less than 500 characters of extracted text.** These are primarily PDFs where text extraction underperformed. We're currently running re-extraction and OCR on all of them — most turn out to have embedded text that fitz can extract without OCR. The biggest blocks are Graubünden (9,300) and Basel-Landschaft (6,000).

**The citation extraction is not validated.** 73.8% resolution rate is a system-level number without per-type precision/recall on manually annotated samples. Informal references and negative citations are not reliably captured.

**The search benchmark is single-annotator.** 100 queries, judged by the author, guided by citation graph authority. No inter-annotator agreement. Useful for regression testing; not yet a publishable shared-task benchmark.

**No editorial annotation.** Everything is automatically extracted. The regeste-based Haiku reranking disadvantages the 48% of decisions without a regeste.

---

## Slide 9: Numbers and Access (1 min)

| | |
|---|---|
| Decisions | 962,272 |
| Courts and public bodies | 101 |
| Case-citation references | 9.86 million extracted, 9.22 million resolved (93.5 %) |
| Resolved citation links | 6.46 million (73.8%) |
| Statute-decision links | 11.3 million |
| Federal laws indexed | 80 (39,000 articles) |
| Legislative texts | 33,000+ |
| Languages | DE (46.6%), FR (45.2%), IT (8.3%) |
| Update frequency | Daily |
| Offline search MRR@10 | 0.47 |
| Online search MRR@10 | 0.65 |
| Infrastructure | 1 VPS, EUR 116/month |
| Code | MIT |
| Data packaging | CC0-1.0 |

Access:
- **MCP**: `mcp.opencaselaw.ch` — Claude, ChatGPT (GPT-5.3), Gemini CLI
- **Download**: `huggingface.co/datasets/voilaj/swiss-caselaw`
- **REST API**: `mcp.opencaselaw.ch/api/docs`
- **Dashboard**: `opencaselaw.ch`
- **Decision pages**: `mcp.opencaselaw.ch/entscheid/{id}` — 962K pages with Schema.org LegalCase
- **Source code**: `github.com/jonashertner/caselaw-repo-1`
