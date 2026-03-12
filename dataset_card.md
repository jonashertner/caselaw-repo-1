---
license: mit
language:
  - de
  - fr
  - it
  - rm
tags:
  - legal
  - swiss-law
  - case-law
  - court-decisions
  - nlp
  - full-text
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

**956,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages (DE/FR/IT/RM), updated daily. The largest open collection of Swiss jurisprudence.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![GitHub](https://img.shields.io/badge/GitHub-source-black)](https://github.com/jonashertner/caselaw-repo-1)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/jonashertner/caselaw-repo-1/blob/main/LICENSE)

## Dataset Summary

The largest open collection of Swiss court decisions — over 956,000 decisions from 100 courts across all 26 cantons, scraped from official court websites and cantonal court portals. New decisions are added every night.

- **19 federal courts and bodies**: BGer, BVGer, BStGer, BPatGer, BGE, FINMA, WEKO, EDÖB, ECHR (Swiss cases), VPB, Sports Tribunal, and more
- **81 cantonal courts** across all 26 cantons
- **4 languages**: German (46.4%), French (45.3%), Italian (8.3%), Romansh
- **Temporal range**: 1875–present (BGE historical vol. 1 from 1875)
- **24 structured fields** per decision in the FTS5 search index; full 34-field schema in Parquet

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

Connect the dataset to Claude, ChatGPT, or Gemini for natural-language search over all 956,000+ decisions. 21 tools available — see [full documentation](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai).

**Remote (no download needed):**

```bash
# Claude Code
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch

# Claude Desktop: Settings → Connectors → Add custom connector → https://mcp.opencaselaw.ch

# ChatGPT: Settings → Apps → Advanced settings → Developer mode → Create app → https://mcp.opencaselaw.ch

# Gemini CLI: add to ~/.gemini/settings.json
# { "mcpServers": { "swiss-caselaw": { "url": "https://mcp.opencaselaw.ch" } } }
```

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
| Total decisions | 956,603 |
| Courts | 100 |
| Temporal range | 1875–present |
| Average decision length | ~22,000 characters |
| Full text coverage | 100% |
| Regeste (headnote) coverage | 54.3% |
| Citation graph edges | 8.77 million |
| Resolved citation links | 2.33 million |

**Language distribution:**

| Language | Count | Share |
|----------|-------|-------|
| German (de) | 443,332 | 46.3% |
| French (fr) | 433,360 | 45.3% |
| Italian (it) | 79,911 | 8.4% |

**Recent annual volumes:**

| Year | Decisions |
|------|-----------|
| 2026 | 7,054 (partial) |
| 2025 | 43,049 |
| 2024 | 44,256 |
| 2023 | 44,758 |

The citation graph links decisions to each other and to statute provisions extracted from full text. Historical BGE decisions (volumes 1–79, 1875–1953) contribute 25,548 resolved citation links.

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

Full schema definition (all 34 fields): [`models.py`](https://github.com/jonashertner/caselaw-repo-1/blob/main/models.py)

## Court Coverage

### Federal Courts (19)

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

Court decisions are public records under Swiss law. Article 27 BGG requires the Federal Supreme Court to publish its decisions. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This dataset contains only publicly available, officially published decisions.

## License

MIT License. The underlying court decisions are public domain under Swiss law.

## Citation

```bibtex
@dataset{swiss_caselaw_2026,
  title={Swiss Case Law Dataset},
  author={Jonas Hertner},
  year={2026},
  url={https://huggingface.co/datasets/voilaj/swiss-caselaw},
  note={956,000+ Swiss federal and cantonal court decisions with full text and structured metadata}
}
```

## Links

- **Website**: [opencaselaw.ch](https://opencaselaw.ch) — live coverage statistics and dashboard
- **GitHub**: [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) — source code, scrapers, pipeline
- **MCP Server**: [setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) — full-text search for Claude Code, Claude Desktop, ChatGPT, and Gemini
