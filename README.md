# Swiss Case Law Open Dataset

**1,000,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages (DE/FR/IT/RM), updated daily. The largest open collection of Swiss jurisprudence.

[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://jonashertner.github.io/caselaw-repo-1/)
[![HuggingFace](https://img.shields.io/badge/HuggingFace-dataset-blue)](https://huggingface.co/datasets/voilaj/swiss-caselaw)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## What this is

A structured, searchable archive of Swiss court decisions — from the Federal Supreme Court (BGer) down to cantonal courts in all 26 cantons. Every decision includes the full decision text, docket number, date, language, legal area, judges, cited decisions, and 20+ additional metadata fields.

The dataset is built from three sources: direct scraping of official court websites, cantonal court portals, and [entscheidsuche.ch](https://entscheidsuche.ch). New decisions are scraped, deduplicated, and published every night.

There are three ways to use it, depending on what you need:

| Method | For whom | What you get |
|--------|----------|-------------|
| [**Search with AI**](#1-search-with-ai) | Lawyers, researchers | Natural-language queries over the full corpus |
| [**Download**](#2-download-the-dataset) | Data scientists, NLP researchers | Bulk Parquet files with all 1M+ decisions |
| [**REST API**](#3-rest-api) | Developers | Programmatic row-level access, no setup |

---

## 1. Search with AI

The dataset comes with an [MCP server](https://modelcontextprotocol.io) that lets AI tools search across all 1M+ decisions. You ask a question in natural language; the tool runs a full-text search and returns matching decisions with snippets.

### Setup with Claude Code

[Claude Code](https://docs.anthropic.com/en/docs/claude-code) is Anthropic's CLI for working with Claude in the terminal. One command adds Swiss case law search:

```bash
claude mcp add swiss-caselaw -- uvx mcp-swiss-caselaw
```

That's it. On first use, the server downloads the search database from HuggingFace (~5 GB). This happens once.

Now ask Claude:

```
> Search for BGer decisions on Mietrecht Kündigung from 2024

> What did the BVGer rule on asylum seekers from Eritrea?

> Show me the full text of 6B_1234/2023

> How many decisions does each court in canton Zürich have?

> Find decisions citing Art. 8 BV
```

Claude calls the MCP tools automatically — you see the search results inline and can ask follow-up questions about specific decisions.

### Other MCP clients

The same server works with any MCP-compatible client. For Claude Desktop or Cursor, add to the JSON config:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "uvx",
      "args": ["mcp-swiss-caselaw"]
    }
  }
}
```

- **Claude Desktop**: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
- **Cursor**: `.cursor/mcp.json` in your project root

### What the MCP server can do

| Tool | Description |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range) |
| `get_decision` | Fetch a single decision by docket number or ID |
| `list_courts` | List all 93 courts with decision counts |
| `get_statistics` | Aggregate stats by court, canton, or year |
| `update_database` | Re-download the latest data from HuggingFace |

---

## 2. Download the dataset

The full dataset is on [HuggingFace](https://huggingface.co/datasets/voilaj/swiss-caselaw) as Parquet files — one file per court, 30 fields per decision including complete decision text.

### With Python (datasets library)

**Step 1.** Install the library:

```bash
pip install datasets
```

**Step 2.** Load the data:

```python
from datasets import load_dataset

# Load a single court (~170k decisions, ~800 MB)
bger = load_dataset("voilaj/swiss-caselaw", data_files="data/bger.parquet")

# Load all courts (~1M decisions, ~5.7 GB download)
ds = load_dataset("voilaj/swiss-caselaw", data_files="data/*.parquet")
```

**Step 3.** Explore:

```python
# Print a single decision
decision = bger["train"][0]
print(decision["docket_number"])   # "6B_1/2024"
print(decision["decision_date"])   # "2024-03-15"
print(decision["language"])        # "de"
print(decision["regeste"][:200])   # First 200 chars of the headnote
print(decision["full_text"][:500]) # First 500 chars of the full text
```

### With pandas

```python
import pandas as pd

# Load one court
df = pd.read_parquet("hf://datasets/voilaj/swiss-caselaw/data/bger.parquet")

# Filter by date
df_recent = df[df["decision_date"] >= "2024-01-01"]
print(f"{len(df_recent)} decisions since 2024")

# Filter by language
df_french = df[df["language"] == "fr"]

# Group by legal area
df.groupby("legal_area").size().sort_values(ascending=False).head(10)
```

### Direct download

Every court is a single Parquet file. Download directly:

```
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/bger.parquet
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/bvger.parquet
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/data/zh_gerichte.parquet
...
```

Full list of files: [huggingface.co/datasets/voilaj/swiss-caselaw/tree/main/data](https://huggingface.co/datasets/voilaj/swiss-caselaw/tree/main/data)

---

## 3. REST API

Query the dataset over HTTP without installing anything. This uses the [HuggingFace Datasets Server](https://huggingface.co/docs/datasets-server/).

**Get rows:**

```bash
curl "https://datasets-server.huggingface.co/rows?dataset=voilaj/swiss-caselaw&config=default&split=train&offset=0&length=5"
```

**Get dataset info:**

```bash
curl "https://datasets-server.huggingface.co/info?dataset=voilaj/swiss-caselaw"
```

**Search by SQL** (DuckDB endpoint):

```bash
curl -X POST "https://datasets-server.huggingface.co/search?dataset=voilaj/swiss-caselaw&config=default&split=train" \
  -d '{"query": "SELECT docket_number, decision_date, language FROM data WHERE court = '\''bger'\'' LIMIT 10"}'
```

> Note: The REST API serves the auto-converted version of the dataset. For per-court Parquet files with the full 30-field schema, use the [download method](#2-download-the-dataset) above.

---

## What's in each decision

Every decision has 30 structured fields:

### Core fields

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `decision_id` | string | `bger_6B_1234_2025` | Unique key: `{court}_{docket_normalized}` |
| `court` | string | `bger` | Court code ([full list](https://jonashertner.github.io/caselaw-repo-1/)) |
| `canton` | string | `CH` | `CH` for federal, `ZH`/`BE`/`GE`/... for cantonal |
| `docket_number` | string | `6B_1234/2025` | Original case number as published |
| `decision_date` | date | `2025-03-15` | Date the decision was rendered |
| `language` | string | `de` | `de`, `fr`, `it`, or `rm` |
| `full_text` | string | *(complete text)* | Full decision text, typically 5–50 pages |
| `source_url` | string | `https://bger.ch/...` | Permanent link to the original |

### Legal content

| Field | Type | Description |
|-------|------|-------------|
| `regeste` | string | Legal headnote / summary (Regeste) |
| `legal_area` | string | Area of law (Strafrecht, Zivilrecht, ...) |
| `title` | string | Subject line (Gegenstand) |
| `outcome` | string | Result: Gutheissung, Abweisung, Nichteintreten, ... |
| `decision_type` | string | Type: Urteil, Beschluss, Verfügung, ... |
| `cited_decisions` | list | Extracted references to other decisions |
| `bge_reference` | string | BGE collection reference if published |
| `abstract_de` | string | German abstract (primarily BGE) |
| `abstract_fr` | string | French abstract |
| `abstract_it` | string | Italian abstract |

### Court metadata

| Field | Type | Description |
|-------|------|-------------|
| `chamber` | string | Chamber (e.g., "I. zivilrechtliche Abteilung") |
| `judges` | string | Panel composition |
| `clerks` | string | Court clerks (Gerichtsschreiber) |
| `collection` | string | Official collection reference |
| `appeal_info` | string | Appeal status / subsequent proceedings |

### Technical fields

| Field | Type | Description |
|-------|------|-------------|
| `docket_number_2` | string | Secondary docket number |
| `publication_date` | date | Date published online |
| `pdf_url` | string | Direct URL to PDF |
| `external_id` | string | Cross-reference ID |
| `scraped_at` | datetime | When this decision was scraped |
| `has_full_text` | bool | Whether `full_text` is non-empty |
| `text_length` | int | Character count of `full_text` |

Full schema definition: [`models.py`](models.py)

---

## Coverage

### Federal courts

| Court | Code | Decisions | Period | Source |
|-------|------|-----------|--------|--------|
| Federal Supreme Court (BGer) | `bger` | ~173,000 | 2000–present | bger.ch |
| BGE Leading Cases | `bge` | ~21,000 | 1954–present | bger.ch |
| Federal Administrative Court (BVGer) | `bvger` | ~91,000 | 2007–present | bvger.ch |
| Federal Criminal Court (BStGer) | `bstger` | ~11,000 | 2005–present | bstger.weblaw.ch |
| Federal Patent Court (BPatGer) | `bpatger` | ~100 | 2012–present | bpatger.ch |

### Cantonal courts

88 courts across all 26 cantons. The largest cantonal collections:

| Canton | Courts | Decisions | Period |
|--------|--------|-----------|--------|
| Genève (GE) | 1 | ~78,000 | 2000–present |
| Vaud (VD) | 1 | ~75,000 | 2002–present |
| Ticino (TI) | 1 | ~58,000 | 2006–present |
| Zürich (ZH) | 6 | ~46,000 | 2005–present |
| Bern (BE) | 2 | ~11,000 | 2010–present |

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live per-court statistics: **[Dashboard](https://jonashertner.github.io/caselaw-repo-1/)**

---

## How it works

```
                        ┌──────────────────────────────────────────────────┐
                        │                  Daily Pipeline                  │
                        │                                                  │
Court websites ────────►│  Scrapers ──► JSONL ──┬──► Parquet ──► HuggingFace
  bger.ch               │  (38 scrapers,        │                          │
  bvger.ch              │   rate-limited,        └──► FTS5 DB ──► MCP Server
  cantonal portals      │   resumable)                                     │
  entscheidsuche.ch     │                                                  │
                        │  01:00 UTC  scrape     04:00 UTC  publish        │
                        └──────────────────────────────────────────────────┘
```

### Step by step

1. **Scrape** (01:00 UTC daily) — 38 scrapers run in parallel, each targeting a specific court's website or API. Every scraper is rate-limited and resumable: it tracks which decisions it has already seen and only fetches new ones. Output: one JSONL file per court.

2. **Deduplicate** — Decisions from multiple sources (e.g., a BGer decision scraped directly *and* found on entscheidsuche.ch) are merged by `decision_id`. Direct scrapes take priority because they typically have richer metadata.

3. **Build search index** (04:00 UTC) — All JSONL files are loaded into a SQLite FTS5 database for full-text search. This powers the MCP server.

4. **Export** — JSONL files are converted to Parquet (one file per court) with a fixed 30-field schema.

5. **Upload** — Parquet files are pushed to HuggingFace. The MCP server and `datasets` library pick up the new data automatically.

6. **Update dashboard** — `stats.json` is regenerated and pushed to GitHub Pages.

---

## Running locally

### Prerequisites

- Python 3.10+
- pip

### Install

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
pip install -e ".[all]"
```

This installs all dependencies including PDF parsing, crypto, and the FastAPI server. For a minimal install without optional dependencies, use `pip install -e .` instead.

### Scrape decisions

```bash
# Scrape 5 recent decisions from the Federal Supreme Court
python run_scraper.py bger --max 5 -v

# Scrape BVGer decisions since a specific date
python run_scraper.py bvger --since 2025-01-01 --max 20 -v

# Scrape a cantonal court
python run_scraper.py zh_gerichte --max 10 -v
```

Output is written to `output/decisions/{court}.jsonl` — one JSON object per line, one file per court. The scraper remembers what it has already fetched (state stored in `state/`), so you can run it repeatedly to get only new decisions.

Available court codes: `bger`, `bge`, `bvger`, `bstger`, `bpatger`, `ag_gerichte`, `ai_gerichte`, `ar_gerichte`, `be_zivilstraf`, `bl_gerichte`, `bs_gerichte`, `fr_gerichte`, `ge_gerichte`, `gl_gerichte`, `gr_gerichte`, `ju_gerichte`, `lu_gerichte`, `ne_gerichte`, `nw_gerichte`, `ow_gerichte`, `sg_publikationen`, `sh_gerichte`, `so_gerichte`, `sz_gerichte`, `tg_gerichte`, `ti_gerichte`, `ur_gerichte`, `vd_gerichte`, `vs_gerichte`, `zh_gerichte`, `zh_verwaltungsgericht`, `zh_sozialversicherungsgericht`, and more. Run `python run_scraper.py --help` for the full list.

### Build a local search database

```bash
python build_fts5.py --output output -v
```

This reads all JSONL files from `output/decisions/` and builds a SQLite FTS5 database at `output/decisions.db`. For 1M decisions, this takes about 3 hours and produces a ~48 GB database.

### Export to Parquet

```bash
python export_parquet.py --input output/decisions --output output/dataset -v
```

Converts JSONL files to Parquet format (one file per court). Output goes to `output/dataset/`.

---

## Data sources

| Source | What | How |
|--------|------|-----|
| **Official court websites** | Federal courts (bger.ch, bvger.ch, bstger.ch, bpatger.ch) | JSON APIs, structured HTML |
| **Cantonal court portals** | 26 cantonal platforms (Weblaw, Entscheidsammlungen, custom portals) | Court-specific scrapers |
| **[entscheidsuche.ch](https://entscheidsuche.ch)** | Community-maintained archive of Swiss court decisions | Bulk download + ingest |

Decisions appearing in multiple sources are deduplicated by `decision_id` (a deterministic hash of court code + normalized docket number). The most metadata-rich version is kept.

---

## Legal basis

Court decisions are public records under Swiss law. Article 27 BGG requires the Federal Supreme Court to publish its decisions. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This project scrapes only publicly available, officially published decisions.

---

## License

MIT. See [LICENSE](LICENSE).
