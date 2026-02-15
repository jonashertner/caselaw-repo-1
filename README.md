# Swiss Case Law Open Dataset

**1,000,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full-text search and bulk download. Open, structured, updated daily.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://jonashertner.github.io/caselaw-repo-1/)
[![HuggingFace](https://img.shields.io/badge/HuggingFace-dataset-blue)](https://huggingface.co/datasets/voilaj/swiss-caselaw)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Use the dataset

### Download (researchers, data scientists)

The full dataset is on HuggingFace as Parquet — one file per court, ~30 structured fields per decision including full text.

```python
from datasets import load_dataset

# Load all courts
ds = load_dataset("voilaj/swiss-caselaw")

# Load a single court
bger = load_dataset("voilaj/swiss-caselaw", data_files="data/bger.parquet")

# Work with pandas
import pandas as pd
df = pd.read_parquet("hf://datasets/voilaj/swiss-caselaw/data/bger.parquet")
df_recent = df[df["decision_date"] >= "2024-01-01"]
```

### Search (lawyers, researchers)

Connect the dataset to Claude or Cursor for natural-language search across all Swiss court decisions.

```bash
# One-line setup
claude mcp add swiss-caselaw -- uvx mcp-swiss-caselaw
```

Then ask Claude:
- *"Find BGer decisions on tenant eviction from 2024"*
- *"What did the BVGer rule on asylum from Eritrea?"*
- *"Show me the full text of 6B_1234/2023"*

### API (developers)

Query via the HuggingFace Datasets Server:

```
GET https://datasets-server.huggingface.co/rows
    ?dataset=voilaj/swiss-caselaw
    &config=default&split=bger
    &offset=0&length=10
```

## What's in each decision

| Field | Description |
|-------|-------------|
| `decision_id` | Unique key: `{court}_{docket_normalized}` |
| `court` | Court code (`bger`, `bvger`, `zh_obergericht`, ...) |
| `canton` | `CH` (federal) or two-letter cantonal code |
| `docket_number` | Original case number (`6B_1234/2025`) |
| `decision_date` | Date of the ruling |
| `language` | `de`, `fr`, `it`, or `rm` |
| `full_text` | Complete decision text |
| `regeste` | Legal headnote / summary |
| `legal_area` | Area of law |
| `judges` | Panel composition |
| `cited_decisions` | Extracted citation references |
| `source_url` | Permanent link to the original |

Plus 18 more fields (chamber, title, outcome, decision type, clerks, abstracts, PDF URL, appeal info, ...). Full schema: [`models.py`](models.py)

## Coverage

### Federal courts (5)

| Court | Code | Decisions | Period |
|-------|------|-----------|--------|
| Federal Supreme Court | `bger` | ~173,000 | 2000–present |
| BGE Leading Cases | `bge` | ~21,000 | 1954–present |
| Federal Administrative Court | `bvger` | ~91,000 | 2007–present |
| Federal Criminal Court | `bstger` | ~11,000 | 2005–present |
| Federal Patent Court | `bpatger` | ~100 | 2012–present |

### Cantonal courts (88 courts across 26 cantons)

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live coverage statistics: **[Dashboard](https://jonashertner.github.io/caselaw-repo-1/)**

## Data sources

1. **Official court websites** — direct scraping from federal court APIs (bger.ch, bvger.ch, bstger.ch, bpatger.ch, bundespatentgericht.ch)
2. **Cantonal court portals** — direct scraping from 26 cantonal court platforms (decwork, weblaw, entscheidsammlungen)
3. **[entscheidsuche.ch](https://entscheidsuche.ch)** — public archive of Swiss court decisions maintained by the Swiss legal community

## Daily automation

New decisions are scraped, deduplicated, exported, and uploaded automatically every night:

```
01:00 UTC  run_all_scrapers.py   — scrape all courts (parallel, 2h timeout each)
04:00 UTC  publish.py            — FTS5 → Parquet → HuggingFace → stats → dashboard
```

## Running locally

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
pip install -e ".[all]"

# Scrape a few recent decisions from any court
python run_scraper.py bger --max 5 -v
python run_scraper.py bvger --since 2026-01-01 --max 10 -v

# Build local full-text search database
python build_fts5.py --output output

# Export to Parquet
python export_parquet.py --input output/decisions --output output/dataset
```

## Architecture

```
Court websites ──► Scrapers ──► JSONL files ──┬──► Parquet ──► HuggingFace
                   (35 scrapers,              │
                    rate-limited,              └──► FTS5 DB ──► MCP Server
                    resumable)                                  (Claude/Cursor)
```

## Legal basis

Court decisions are public records under Swiss law. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This project scrapes only publicly available, officially published decisions.

## License

MIT. See [LICENSE](LICENSE).
