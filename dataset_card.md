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

**962,724 published decisions from Swiss federal, cantonal, and regulatory bodies.**

Full text, structured metadata, extracted case-citation references, and daily updates. The March 20, 2026 snapshot contains German, French, and Italian decisions; the export schema also reserves `rm` for Romansh.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![GitHub](https://img.shields.io/badge/GitHub-source-black)](https://github.com/jonashertner/caselaw-repo-1)
[![MCP Server](https://img.shields.io/badge/MCP-live-blue)](https://mcp.opencaselaw.ch/health)
[![Data License: CC0--1.0](https://img.shields.io/badge/Data_License-CC0--1.0-blue.svg)](https://creativecommons.org/publicdomain/zero/1.0/)
[![Code License: MIT](https://img.shields.io/badge/Code_License-MIT-green.svg)](https://github.com/jonashertner/caselaw-repo-1/blob/main/LICENSE)

## Dataset Summary

The largest open collection of Swiss court decisions: 962,724 decisions from 102 federal, cantonal, and regulatory courts or public bodies, scraped from official publication channels. New decisions are added every night.

- **20 federal courts and bodies**: BGer, BVGer, BStGer, BPatGer, BGE, FINMA, WEKO, EDÖB, ECHR (Swiss cases), VPB, Sports Tribunal, and more
- **82 cantonal courts** across all 26 cantons
- **Current decision languages**: German (448,461; 46.6%), French (434,663; 45.1%), Italian (79,600; 8.3%); the export schema also reserves `rm`
- **Temporal range**: 1875–present (BGE historical vol. 1 from 1875)
- **8.76 million extracted case-citation references**
- **6.42 million resolved decision-to-decision links** (with confidence scores)
- **11.23 million statute-decision links** (e.g., which decisions cite Art. 41 OR)
- **80 federal laws indexed** with 39,000 articles in 3 languages
- **34 structured fields** per decision in Parquet; 24 in the FTS5 search index

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

Connect the dataset to Claude, ChatGPT, or Gemini for natural-language search over all 962,724 decisions. The MCP surface is deployment-dependent: local deployments can expose up to 21 tools, remote mode omits local update tools, and legislation tools depend on LexFind-backed configuration.

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
| Total decisions | 962,724 |
| Courts | 101 |
| Temporal range | 1875–present |
| Average decision length | ~22,000 characters |
| Full text coverage | 100% |
| Regeste (headnote) coverage | ~54% |
| Extracted case-citation references | 8.76 million |
| Resolved decision links | 6.42 million |
| Statute-decision links | 11.23 million |
| Federal laws indexed | 80 (39,000 articles) |
| Legislation texts searchable | 33,000+ |
| MCP tools | Deployment-dependent (up to 21) |

**Language distribution:**

| Language | Count | Share |
|----------|-------|-------|
| German (de) | 448,215 | 46.58% |
| French (fr) | 434,470 | 45.15% |
| Italian (it) | 79,587 | 8.27% |

**Reference graph:** 8.76 million extracted case-citation references, 6.42 million resolved decision-to-decision links, and 11.23 million statute-to-decision links. The most-cited decision is BGE 125 V 351 with 54,000 incoming citations.

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

**Collection**: 54 automated scrapers target official court websites, APIs, and publication portals (Weblaw, Tribuna, FindInfo, Omnis, and direct court APIs). Each scraper is rate-limited and resumable — it tracks already-seen decisions and fetches only new ones.

**Deduplication**: `decision_id` is a deterministic hash of court code + normalized docket number. Decisions appearing across multiple sources are grouped and the version with the longest full text is kept. Cross-court overlap groups cover courts whose decisions are published on multiple portals (ZH: 17 sub-courts, AG: 18, VD: 3, BS: 3, BE: 2).

**Quality control**: content hashing (MD5 of full text) detects duplicate text; stub removal discards entries with fewer than 10 characters in both full text and regeste; text length validation flags suspicious entries.

**Pipeline**: daily at 01:00 UTC scrapers run; at 04:00 UTC the pipeline builds the FTS5 index, exports Parquet files, and uploads to HuggingFace. Mon–Sat runs are incremental (byte-offset checkpointing, typically under a minute); Sunday runs a full rebuild and FTS5 optimization.

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
| ECHR (Swiss cases) | `bge_egmr` | ~475 | 1974–present |
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

**Official court websites** — direct scraping from federal and cantonal court platforms (54 scrapers targeting court APIs, Weblaw, Tribuna, FindInfo, Omnis, and other portals).

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
  title={Swiss Case Law Dataset: 962,724 Court Decisions with Reference Graph},
  author={Jonas Hertner},
  year={2026},
  url={https://huggingface.co/datasets/voilaj/swiss-caselaw},
  note={962,724 Swiss federal, cantonal, and regulatory decisions with full text,
        structured metadata, 8.76M extracted case-citation references,
        6.42M resolved decision links, and 11.23M statute links.
        Searchable via MCP (Claude, ChatGPT, Gemini). Updated daily.}
}
```

## Links

- **Website**: [opencaselaw.ch](https://opencaselaw.ch) — live coverage statistics and dashboard
- **GitHub**: [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) — source code, scrapers, pipeline
- **MCP Server**: [setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) — full-text search for Claude Code, Claude Desktop, ChatGPT, and Gemini
