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

**1,040,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages (DE/FR/IT/RM), updated daily. The largest open collection of Swiss jurisprudence.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![GitHub](https://img.shields.io/badge/GitHub-source-black)](https://github.com/jonashertner/caselaw-repo-1)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/jonashertner/caselaw-repo-1/blob/main/LICENSE)

## Dataset Summary

The largest open collection of Swiss court decisions — over 1 million decisions from 93 courts across all 26 cantons, scraped from official court websites and [entscheidsuche.ch](https://entscheidsuche.ch). New decisions are added every night.

- **12 federal courts and bodies**: BGer, BVGer, BStGer, BPatGer, BGE, FINMA, WEKO, EDÖB, ECHR (Swiss cases), VPB, and more
- **93 cantonal courts** across all 26 cantons
- **4 languages**: German, French, Italian, Romansh
- **Temporal range**: 1880–present
- **34 structured fields** per decision: full text, docket number, date, court, canton, language, legal area, judges, citations, headnote, and more

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

Connect the dataset to Claude Code or Claude Desktop for natural-language search over all 1M+ decisions.

**Remote (no download needed):**

```bash
# Claude Code
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch

# Claude Desktop: Settings → Connectors → Add custom connector → https://mcp.opencaselaw.ch
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

On first search, the server downloads the Parquet files (~7 GB) from this dataset and builds a local SQLite FTS5 index (~58 GB). This takes 30-60 minutes and only happens once. After that, searches are instant.

#### MCP tools

| Tool | Description |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range, chamber, decision type) |
| `get_decision` | Fetch a single decision by docket number or ID. Includes citation graph counts. |
| `list_courts` | List all courts with decision counts |
| `get_statistics` | Aggregate stats by court, canton, or year |
| `find_citations` | Show what a decision cites and what cites it, with confidence scores |
| `find_leading_cases` | Find the most-cited decisions for a topic or statute |
| `analyze_legal_trend` | Year-by-year decision counts for a statute or topic |
| `draft_mock_decision` | Research-only mock decision outline from facts, grounded in caselaw + statutes |
| `update_database` | Re-download latest data and rebuild the local database (local mode only) |

The citation graph tools (`find_citations`, `find_leading_cases`, `analyze_legal_trend`) use a **reference graph** with 7.85 million citation edges linking 1M+ decisions and 330K statute references:

- *"What are the leading cases on Art. 8 EMRK?"* → Top decisions ranked by citation count
- *"Show me the citation network for BGE 138 III 374"* → 13 outgoing, 13,621 incoming citations
- *"How has Art. 29 BV jurisprudence evolved?"* → Year-by-year trend from 2000 to present

See the [full setup guide](https://github.com/jonashertner/caselaw-repo-1#1-search-with-ai) for details.

### Web UI — chat interface with cited decisions

A local chat interface for legal research. Ask questions in natural language, get answers backed by cited Swiss court decisions. Supports 5 LLM providers: Claude, OpenAI, Gemini (cloud), plus Qwen 2.5 and Llama 3.3 via [Ollama](https://ollama.com) (local, no API key needed).

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
pip install fastapi uvicorn python-dotenv mcp pyarrow pydantic openai
cd web_ui && npm install && cd ..
./scripts/run_web_local.sh
```

Open http://localhost:5173. For local models, install Ollama and run `ollama pull qwen2.5:14b` — the UI auto-detects it.

See the [Web UI guide](https://github.com/jonashertner/caselaw-repo-1#4-web-ui) for full details.

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
| Federal Supreme Court (BGer) | `bger` | ~173,000 | 1996–present |
| Federal Administrative Court (BVGer) | `bvger` | ~91,000 | 2007–present |
| BGE Leading Cases | `bge` | ~45,000 | 1954–present |
| Federal Admin. Practice (VPB) | `ch_vb` | ~23,000 | 1982–2016 |
| Federal Criminal Court (BStGer) | `bstger` | ~11,000 | 2004–present |
| EDÖB (Data Protection) | `edoeb` | ~1,200 | 1994–present |
| FINMA | `finma` | ~1,200 | 2008–2024 |
| ECHR (Swiss cases) | `bge_egmr` | ~475 | 1974–present |
| Federal Patent Court (BPatGer) | `bpatger` | ~190 | 2012–present |
| Competition Commission (WEKO) | `weko` | ~120 | 2009–present |
| Sports Tribunal | `ta_sst` | ~50 | 2024–present |
| Federal Council | `ch_bundesrat` | ~15 | 2012–present |

### Cantonal Courts (26 cantons, 93 courts)

| Canton | Courts | Decisions | Period |
|--------|--------|-----------|--------|
| Vaud (VD) | 3 | ~155,000 | 1984–present |
| Zürich (ZH) | 20 | ~126,000 | 1980–present |
| Genève (GE) | 1 | ~116,000 | 1993–present |
| Ticino (TI) | 1 | ~58,000 | 1995–present |
| St. Gallen (SG) | 7 | ~35,000 | 2001–present |
| Graubünden (GR) | 1 | ~29,000 | 2002–present |
| Basel-Landschaft (BL) | 1 | ~26,000 | 2000–present |
| Bern (BE) | 6 | ~26,000 | 2002–present |
| Aargau (AG) | 18 | ~21,000 | 1993–present |
| Basel-Stadt (BS) | 3 | ~19,000 | 2001–present |

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live coverage statistics: **[Dashboard](https://opencaselaw.ch)**

## Data Sources

1. **Official court websites** — direct scraping from federal and cantonal court platforms (45 scrapers)
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
- **Web UI**: [setup guide](https://github.com/jonashertner/caselaw-repo-1#4-web-ui) — chat interface with Claude, OpenAI, Gemini, or local models via Ollama
