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
size_categories:
  - 1M<n<10M
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

**1,000,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages. Updated daily.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/jonashertner/caselaw-repo-1/blob/main/LICENSE)

## Dataset Summary

This dataset contains over one million Swiss court decisions scraped from official court websites and [entscheidsuche.ch](https://entscheidsuche.ch). It covers:

- **5 federal courts**: Federal Supreme Court (BGer), Federal Administrative Court (BVGer), Federal Criminal Court (BStGer), Federal Patent Court (BPatGer), plus BGE leading cases
- **Federal regulatory bodies**: FINMA, WEKO, EDÖB, and more
- **Cantonal courts** across all 26 cantons (93 courts total)
- **4 languages**: German, French, Italian, Romansh
- **Temporal range**: 1880 to present

Each decision includes the complete decision text alongside 34 structured metadata fields (court, canton, docket number, date, language, legal area, judges, citations, and more).

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
```

### Full-text search via MCP

Connect the dataset to Claude Code for natural-language search over all 1M+ decisions. Everything runs locally on your machine.

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install mcp pydantic huggingface-hub pyarrow
claude mcp add swiss-caselaw -- /path/to/.venv/bin/python3 /path/to/mcp_server.py
# Windows: use .venv\Scripts\python.exe instead
```

On first search, the server downloads the Parquet files (~6.5 GB) from this dataset and builds a local SQLite FTS5 index (~56 GB). This takes 30-60 minutes and only happens once. After that, searches are instant.

Then ask: *"Find BGer decisions on tenant eviction from 2024"*

To update: ask Claude to run the `update_database` tool. It re-downloads the latest Parquet files and rebuilds the index.

See the [full setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) for details.

## Schema

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `decision_id` | string | Unique ID: `{court}_{docket_normalized}` |
| 2 | `court` | string | Court code (e.g., `bger`, `zh_obergericht`) |
| 3 | `canton` | string | `CH` for federal, two-letter canton code otherwise |
| 4 | `chamber` | string | Chamber / Abteilung |
| 5 | `docket_number` | string | Original docket number (e.g., `6B_1234/2025`) |
| 6 | `docket_number_2` | string | Secondary docket number |
| 7 | `decision_date` | string | ISO date of decision |
| 8 | `publication_date` | string | Date published online |
| 9 | `language` | string | Language code: `de`, `fr`, `it`, `rm` |
| 10 | `title` | string | Subject / Gegenstand |
| 11 | `legal_area` | string | Rechtsgebiet / Domaine juridique |
| 12 | `regeste` | string | Headnote / Regeste |
| 13 | `abstract_de` | string | German abstract |
| 14 | `abstract_fr` | string | French abstract |
| 15 | `abstract_it` | string | Italian abstract |
| 16 | `full_text` | string | Complete decision text |
| 17 | `outcome` | string | Decision outcome |
| 18 | `decision_type` | string | Urteil, Beschluss, Verfügung, etc. |
| 19 | `judges` | string | Participating judges |
| 20 | `clerks` | string | Court clerks |
| 21 | `collection` | string | Official collection reference |
| 22 | `appeal_info` | string | Appeal status |
| 23 | `source_url` | string | Permanent URL to original |
| 24 | `pdf_url` | string | Direct PDF link |
| 25 | `bge_reference` | string | BGE reference if published |
| 26 | `cited_decisions` | string | JSON array of cited references |
| 27 | `scraped_at` | string | Scrape timestamp |
| 28 | `external_id` | string | External cross-reference ID |
| 29 | `source` | string | Data source: `entscheidsuche`, `direct_scrape` |
| 30 | `source_id` | string | Source-specific ID (e.g. Signatur) |
| 31 | `source_spider` | string | Source spider/scraper name |
| 32 | `content_hash` | string | MD5 hash of full_text for deduplication |
| 33 | `has_full_text` | bool | Whether full text is non-empty |
| 34 | `text_length` | int | Character count of full_text |

## Court Coverage

### Federal Courts

| Court | Code | Decisions | Period |
|-------|------|-----------|--------|
| Federal Supreme Court (BGer) | `bger` | ~173,000 | 1996-present |
| Federal Administrative Court (BVGer) | `bvger` | ~91,000 | 2007-present |
| BGE Leading Cases | `bge` | ~45,000 | 1954-present |
| Federal Admin. Practice (VPB) | `ch_vb` | ~23,000 | 1982-2016 |
| Federal Criminal Court (BStGer) | `bstger` | ~11,000 | 2004-present |
| EDÖB (Data Protection) | `edoeb` | ~1,200 | 1994-present |
| FINMA | `finma` | ~1,200 | 2008-2024 |
| ECHR (Swiss cases) | `bge_egmr` | ~470 | - |
| Federal Patent Court (BPatGer) | `bpatger` | ~190 | 2012-present |
| Competition Commission (WEKO) | `weko` | ~120 | 2009-present |
| Sports Tribunal | `ta_sst` | ~50 | 2024-present |
| Federal Council | `ch_bundesrat` | ~15 | 2012-present |

### Cantonal Courts (26 cantons, 93 courts)

| Canton | Courts | Decisions | Period |
|--------|--------|-----------|--------|
| Vaud (VD) | 3 | ~155,000 | 1984-present |
| Zurich (ZH) | 20 | ~126,000 | 1980-present |
| Geneve (GE) | 1 | ~86,000 | 1993-present |
| Ticino (TI) | 1 | ~58,000 | 1995-present |
| St. Gallen (SG) | 7 | ~35,000 | 2001-present |
| Graubunden (GR) | 1 | ~29,000 | 2002-present |
| Basel-Landschaft (BL) | 1 | ~26,000 | 2000-present |
| Bern (BE) | 6 | ~26,000 | 2002-present |
| Aargau (AG) | 18 | ~21,000 | 1993-present |
| Basel-Stadt (BS) | 3 | ~19,000 | 2001-present |

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live coverage statistics: **[Dashboard](https://opencaselaw.ch)**

## Data Sources

1. **Official court websites** — direct scraping from federal and cantonal court platforms (43 scrapers)
2. **[entscheidsuche.ch](https://entscheidsuche.ch)** — public archive maintained by the Swiss legal community

Decisions appearing in multiple sources are deduplicated by `decision_id` (a deterministic hash of court code + normalized docket number). The most metadata-rich version is kept.

## Update Frequency

The dataset is updated daily via automated pipeline. New decisions are scraped, deduplicated, exported to Parquet, and uploaded.

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
  note={1M+ Swiss federal and cantonal court decisions with full text and structured metadata}
}
```

## Links

- **Website**: [opencaselaw.ch](https://opencaselaw.ch) — live coverage statistics and dashboard
- **GitHub**: [github.com/jonashertner/caselaw-repo-1](https://github.com/jonashertner/caselaw-repo-1) — source code, scrapers, pipeline
- **MCP Server**: [setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) — full-text search for Claude Code and Claude Desktop
