---
license: cc0-1.0
language:
  - de
  - fr
  - it
tags:
  - legal
  - swiss-law
  - case-law
  - court-decisions
  - nlp
  - full-text
  - citation-graph
  - mcp
  - multilingual
pretty_name: Swiss Case Law
authors:
  - Jonas Hertner
size_categories:
  - 100K<n<1M
task_categories:
  - text-classification
  - summarization
  - question-answering
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/*.parquet
---

# Swiss Case Law Dataset

**969,000+ published decisions from Swiss federal, cantonal, and regulatory bodies.**

Full text, structured metadata, extracted case-citation references, and daily updates. The dataset contains German, French, and Italian decisions; the export schema also reserves `rm` for Romansh.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![GitHub](https://img.shields.io/badge/GitHub-source-black)](https://github.com/jonashertner/caselaw-repo-1)
[![MCP Server](https://img.shields.io/badge/MCP-live-blue)](https://mcp.opencaselaw.ch/health)
[![Data License: CC0--1.0](https://img.shields.io/badge/Data_License-CC0--1.0-blue.svg)](https://creativecommons.org/publicdomain/zero/1.0/)
[![Code License: MIT](https://img.shields.io/badge/Code_License-MIT-green.svg)](https://github.com/jonashertner/caselaw-repo-1/blob/main/LICENSE)

## Dataset Summary

The largest open collection of Swiss court decisions: 969,000+ decisions from 108 federal, cantonal, regulatory, and international courts, scraped from official publication channels. New decisions are added every night.

- **20+ federal courts and bodies**: BGer, BVGer, BStGer, BPatGer, BGE, FINMA, WEKO, EDÖB, MKG (Militärkassationsgericht), VPB, Sports Tribunal, and more
- **80+ cantonal courts** across all 26 cantons
- **ECHR/EGMR**: 834 Swiss-respondent judgments (HUDOC) + general ECtHR Grand Chamber / Chamber / Committee (1,421 judgments live; full-corpus backfill in progress)
- **Current decision languages**: German (447,783; 46.2%), French (441,094; 45.5%), Italian (80,696; 8.3%); the export schema also reserves `rm`
- **Temporal range**: 1875–present (BGE historical vol. 1 from 1875)
- **9.04 million extracted case-citation references** (resolved with confidence scores)
- **11.63 million statute-decision links** (e.g., which decisions cite Art. 41 OR)
- **5,510 federal laws indexed** with 132,586 articles in 3 languages (from Fedlex SPARQL)
- **15,722 cantonal laws** with 353,464 articles (direct-scraped from 19 cantonal portals; LexFind fallback for the rest)
- **Legislative history (Materialien)** for 2,500 federal laws: Botschaft references for 33,000 statute articles, structured Botschaft digests for BV and BGFA, parliamentary debate transcripts (Amtliches Bulletin) for the BV
- **34 structured fields** per decision in Parquet; 27 in the FTS5 search index

## Quick Start

### Load with HuggingFace datasets

```python
from datasets import load_dataset

# Load all courts
ds = load_dataset("voilaj/swiss-caselaw")

# Load a single court
bger = load_dataset("voilaj/swiss-caselaw", data_files="data/bger.parquet")
```

### Load with pandas

```python
import pandas as pd

df = pd.read_parquet("hf://datasets/voilaj/swiss-caselaw/data/bger.parquet")
df_recent = df[df["decision_date"] >= "2024-01-01"]
print(f"{len(df_recent)} decisions since 2024")

# Filter by language
df_french = df[df["language"] == "fr"]

# Group by legal area
df.groupby("legal_area").size().sort_values(ascending=False).head(10)
```

### Direct download

Every court is a single Parquet file:

```
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/bger.parquet
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/bvger.parquet
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/zh_gerichte.parquet
```

Full list: [huggingface.co/datasets/voilaj/swiss-caselaw/tree/main/data](https://huggingface.co/datasets/voilaj/swiss-caselaw/tree/main/data)

### REST API (no setup)

Query via the HuggingFace Datasets Server — no installation required:

```bash
# Get rows
curl "https://datasets-server.huggingface.co/rows?dataset=voilaj/swiss-caselaw&config=default&split=train&offset=0&length=5"

# Dataset info
curl "https://datasets-server.huggingface.co/info?dataset=voilaj/swiss-caselaw"
```

### Full-text search via MCP

Connect the dataset to Claude, ChatGPT, Cursor, Gemini, Grok, or any MCP client for natural-language search over all 969,000+ decisions, statute lookup, citation graph traversal, legislative history, and more. The MCP server exposes 31 tools (27 in remote mode — 2 local-only `update_database` / `check_update_status` tools are hidden when REMOTE_MODE=True). Tools include verbatim head-note retrieval (`get_regeste`), structured Erwägung-paragraph access (`get_erwaegung`), and full decision-structure decomposition (`get_decision_structure`).

**Remote (no download needed):**

```bash
# Claude Code
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch

# Claude Desktop: Settings → Connectors → Add custom connector → https://mcp.opencaselaw.ch

# ChatGPT: Settings → Apps → Developer mode → Create app → https://mcp.opencaselaw.ch/sse (auth: None)
# Recommended with GPT-5.3

# Gemini CLI: add to ~/.gemini/settings.json
# { "mcpServers": { "swiss-caselaw": { "url": "https://mcp.opencaselaw.ch" } } }
```

Search results include enriched metadata: court name (human-readable), court level, legal area, statute articles cited, citation count, and leading-case flag.

**Local (offline access, ~65 GB disk):**

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install mcp pydantic huggingface-hub pyarrow
claude mcp add swiss-caselaw -- /path/to/.venv/bin/python3 /path/to/mcp_server.py
# Windows: use .venv\Scripts\python.exe instead
```

On first search, the server downloads the Parquet files (~7 GB) from this dataset and builds a local SQLite FTS5 index (~58 GB). This takes 30–60 minutes and only happens once. After that, searches are instant.

## Dataset Statistics

| Metric | Value |
|--------|-------|
| Total decisions | 969,573 (live count, updated daily) |
| Courts | 108 |
| Temporal range | 1875–present |
| Average decision length | 22,775 characters |
| Full text coverage | 100% |
| Regeste (headnote) coverage | 38.7% |
| Case-citation references | 9.04 million |
| Statute-decision links | 11.63 million |
| Federal laws indexed | 5,510 (132,586 articles in DE/FR/IT) |
| Cantonal laws indexed | 15,722 (353,464 articles, direct-scraped + LexFind) |
| Laws with Botschaft refs | 2,500 (33,000 articles) |
| Legislation texts searchable | 33,000+ (federal + cantonal + intercantonal) |
| Scholarly commentaries | 1,058 (OnlineKommentar.ch + OpenLegalCommentary.ch) |
| MCP tools | 31 (29 remote / 31 local) |

**Language distribution:**

| Language | Count | Share |
|----------|-------|-------|
| German (de) | 447,783 | 46.2% |
| French (fr) | 441,094 | 45.5% |
| Italian (it) | 80,696 | 8.3% |

**Reference graph:** 9.04 million resolved decision-to-decision citation edges and 11.63 million statute-to-decision links. The most-cited decision is BGE 125 V 351 with 63,061 incoming citations.

**Search benchmark (frozen offline baseline):** `benchmarks/search_benchmark_2026-03-19_offline_full.json` records a 100-query run against a 1,078,177-row local `decisions.db`, with MRR@10 = 0.4697, Recall@10 = 0.4958, nDCG@10 = 0.5250, and Hit@1 = 0.33. This is a reproducible offline baseline, not a fully provisioned hosted-system score.

## Intended Uses

- **Legal research and case law analysis**: full-text search and citation network analysis across the Swiss court system
- **NLP research on multilingual legal text**: classification, summarization, named entity recognition, and cross-lingual tasks on German/French/Italian legal corpora
- **Legal tech development**: building search engines, citation analysis tools, and document drafting assistants grounded in Swiss jurisprudence
- **Academic study of Swiss jurisprudence**: tracking doctrinal evolution, identifying leading cases, analyzing court output over time

**Not intended for**: automated legal advice or replacing professional legal counsel. This dataset is a research and analysis resource, not a substitute for qualified legal representation.

## Limitations

- **Temporal coverage varies by court**: federal courts from 1996, some cantonal courts from 2000+; historical BGE volumes from 1875
- **Historical OCR artifacts**: BGE decisions from volumes 1–79 (1875–1953) were digitized from print and may contain OCR errors
- **Publication delays**: some cantonal courts have irregular publication schedules; decisions may appear weeks after being rendered
- **Language distribution is unbalanced by design**: it reflects actual court output (German and French cantons are larger), not balanced sampling
- **Anonymization varies by court**: most federal decisions are anonymized; some cantonal decisions may contain personal names or details
- **~1.9% short-text decisions**: some decisions are PDF-only publications where text extraction produced fewer than 500 characters; full text may be available at the source URL

## Dataset Creation

**Collection**: 59 automated scrapers target official court websites, APIs, and publication portals (Weblaw, Tribuna, FindInfo, Omnis, and direct court APIs). Each scraper is rate-limited and resumable — it tracks already-seen decisions and fetches only new ones.

**Deduplication**: `decision_id` is a deterministic hash of court code + normalized docket number. Decisions appearing across multiple sources are grouped and the version with the longest full text is kept. Cross-court overlap groups cover courts whose decisions are published on multiple portals (ZH: 17 sub-courts, AG: 18, VD: 3, BS: 3, BE: 2).

**Quality control**: content hashing (MD5 of full text) detects duplicate text; stub removal discards entries with fewer than 10 characters in both full text and regeste; text length validation flags suspicious entries.

**Pipeline**: scrapers run daily at 01:00 UTC; the publish pipeline starts at 03:30 UTC and rebuilds the FTS5 index, the reference graph, the Parquet export, and uploads to HuggingFace. Every run is a full rebuild with atomic swap (zero-downtime); a typical run takes 4–6 h. The "Notable" landing-page factoids refresh weekly on Sunday at 04:30 UTC; tunnel-dependent cantonal scrapers (JU, NE) retry at 10:00 UTC after the local SOCKS reverse-tunnel comes back up.

## Schema

The Parquet files use a 34-field schema. The 24 columns available in the FTS5 search index are listed below.

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `decision_id` | string | Unique ID: `{court}_{docket_normalized}` |
| 2 | `court` | string | Court code (e.g., `bger`, `zh_obergericht`) |
| 3 | `canton` | string | `CH` for federal, two-letter canton code otherwise |
| 4 | `chamber` | string | Chamber / Abteilung |
| 5 | `docket_number` | string | Original docket number (e.g., `6B_1234/2025`) |
| 6 | `decision_date` | string | ISO date of decision |
| 7 | `publication_date` | string | Date published online |
| 8 | `language` | string | Language code: `de`, `fr`, `it`, `rm` |
| 9 | `title` | string | Subject / Gegenstand |
| 10 | `legal_area` | string | Rechtsgebiet / Domaine juridique |
| 11 | `regeste` | string | Headnote / Regeste (present in 54.3% of decisions) |
| 12 | `full_text` | string | Complete decision text |
| 13 | `decision_type` | string | Urteil, Beschluss, Verfügung, etc. |
| 14 | `outcome` | string | Decision outcome (Gutheissung, Abweisung, ...) |
| 15 | `source_url` | string | Permanent URL to the original |
| 16 | `pdf_url` | string | Direct PDF link |
| 17 | `cited_decisions` | string | JSON array of cited decision references |
| 18 | `scraped_at` | string | Scrape timestamp |
| 19 | `source` | string | Data source identifier |
| 20 | `source_id` | string | Source-specific ID (e.g., Signatur) |
| 21 | `source_spider` | string | Name of the scraper that collected this decision |
| 22 | `content_hash` | string | MD5 hash of full_text for deduplication |
| 23 | `json_data` | string | Complete 34-field record as JSON |
| 24 | `canonical_key` | string | Normalized key for cross-source deduplication |

Full 34-field Parquet export schema: [`export_parquet.py`](https://github.com/jonashertner/caselaw-repo-1/blob/main/export_parquet.py)

## Court Coverage

### Federal Courts (20)

| Court | Code | Decisions | Period |
|-------|------|-----------|--------|
| Federal Supreme Court (BGer) | `bger` | ~174,000 | 1996–present |
| Federal Administrative Court (BVGer) | `bvger` | ~91,500 | 2007–present |
| BGE Leading Cases | `bge` | ~21,200 | 1954–present |
| BGE Historical (vol. 1–79) | `bge_historical` | ~14,600 | 1875–1953 |
| Federal Admin. Practice (VPB) | `ch_vb` | ~22,900 | 1982–2016 |
| Federal Criminal Court (BStGer) | `bstger` | ~11,400 | 2004–present |
| EDÖB (Data Protection) | `edoeb` | ~1,800 | 1994–present |
| FINMA | `finma` | ~405 | 2008–present |
| ECHR (Swiss cases, BGer-published) | `bge_egmr` | ~475 | 1974–present |
| ECHR Switzerland (HUDOC) | `hudoc_ch` | 834 | 1959–present |
| ECtHR Chamber judgments | `ecthr_chamber` | 193 (growing) | 1959–present |
| ECtHR Committee judgments | `ecthr_committee` | 30 (growing) | |
| ECtHR Grand Chamber | `ecthr_grand_chamber` | 13 (growing) | |
| Militärkassationsgericht (MKG) | `mkg` | 1,244 | 1915–2025 |
| Federal Patent Court (BPatGer) | `bpatger` | ~189 | 2012–present |
| Competition Commission (WEKO) | `weko` | ~256 | 2009–present |
| Sports Tribunal | `ta_sst` | ~49 | 2024–present |

### Cantonal Courts (26 cantons, 81 courts)

| Canton | Courts | Decisions | Period |
|--------|--------|-----------|--------|
| Genève (GE) | 1 | ~167,000 | 1993–present |
| Vaud (VD) | 3 | ~155,000 | 1984–present |
| Zürich (ZH) | 21 | ~81,000 | 1980–present |
| Ticino (TI) | 1 | ~59,000 | 1995–present |
| Bern (BE) | 6 | ~20,000 | 2002–present |
| Basel-Landschaft (BL) | 1 | ~17,000 | 2000–present |
| Graubünden (GR) | 1 | ~14,400 | 2002–present |
| Fribourg (FR) | 1 | ~14,100 | 2007–present |
| St. Gallen (SG) | 7 | ~13,100 | 2001–present |
| Aargau (AG) | 17 | ~11,800 | 1993–present |
| Basel-Stadt (BS) | 3 | ~10,100 | 2001–present |

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live per-court statistics: **[Dashboard](https://opencaselaw.ch)**

## Data Sources

**Court decisions** — 59 scrapers targeting official court platforms directly (federal court APIs, Weblaw, Tribuna, FindInfo, Omnis, plus custom portals for the smaller cantons). No third-party aggregator in the case-law pipeline; we go to the source.

**Federal legislation** — Fedlex SPARQL endpoint (Bundeskanzlei). Mirrored monthly into `statutes.db`; covers every consolidated federal act in DE/FR/IT.

**Cantonal legislation** — dual-source pipeline. **19 cantons** are scraped directly from their official Gesetzessammlungen (LexWork + SIL platforms — the same publishing systems the cantons themselves operate), parsed natively as HTML for clean article-level data. The **remaining 7 cantons** fall back to PDF extraction via [LexFind.ch](https://www.lexfind.ch). Combined into `cantonal_laws.db` (15,722 laws / 353,464 articles) and federated with `statutes.db` via SQLite FTS5. The live LexFind API also serves as a real-time fallback for SR numbers not yet in the local mirror, and as the discovery catalog for the broader `search_legislation` tool which spans 33,000+ legislation texts including ordinances and intercantonal agreements.

Decisions appearing in multiple sources are deduplicated by `decision_id` (a deterministic hash of court code + normalized docket number). The version with the longest full text is kept.

## Update Frequency

The dataset is updated daily via automated pipeline. New decisions are scraped, deduplicated, exported to Parquet, and uploaded to HuggingFace.

## Legal Basis

This dataset contains only publicly available, officially published decisions. Under Swiss law, published judicial decisions are official works; OpenCaseLaw republishes those source texts and links every record back to the originating court or public body.

## License

The **code** for OpenCaseLaw is released under the MIT license.

The **dataset packaging and added metadata** are dedicated under **CC0-1.0**, to the extent any copyright or database rights exist in those additions. The underlying decision texts remain official published court decisions sourced from the originating courts or public bodies.

See the governance policy for source withdrawals, re-anonymization, and verified correction/removal requests: [`docs/governance-and-removal-policy.md`](https://github.com/jonashertner/caselaw-repo-1/blob/main/docs/governance-and-removal-policy.md).

## Citation

```bibtex
@dataset{swiss_caselaw_2026,
  title={Swiss Case Law Dataset: 969,000+ Court Decisions with Reference Graph and ECtHR Coverage},
  author={Jonas Hertner},
  year={2026},
  url={https://huggingface.co/datasets/voilaj/swiss-caselaw},
  note={969,000+ Swiss federal, cantonal, and regulatory decisions with full text,
        structured metadata, 8.85M citation edges, 11.34M statute links,
        5,500+ federal laws, 26,043 cantonal legislative texts,
        and legislative history for 2,500 laws.
        Searchable via 31 MCP tools (29 remote + 2 local-only) (Claude, ChatGPT, Cursor, Gemini, Grok). Updated daily.}
}
```

## Links

- **Website**: [opencaselaw.ch](https://opencaselaw.ch) — live coverage statistics and dashboard
- **GitHub**: [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) — source code, scrapers, pipeline
- **MCP Server**: [setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) — full-text search for Claude Code, Claude Desktop, ChatGPT, and Gemini
