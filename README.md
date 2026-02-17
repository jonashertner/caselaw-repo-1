# Swiss Case Law Open Dataset

**1,000,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages (DE/FR/IT/RM), updated daily. The largest open collection of Swiss jurisprudence.

[![CI](https://github.com/jonashertner/caselaw-repo-1/actions/workflows/ci.yml/badge.svg)](https://github.com/jonashertner/caselaw-repo-1/actions/workflows/ci.yml)
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

The dataset comes with an [MCP server](https://modelcontextprotocol.io) that lets AI tools search across all 1M+ decisions locally on your machine. You ask a question in natural language; the tool runs a full-text search and returns matching decisions with snippets.

### Setup with Claude Code

[Claude Code](https://docs.anthropic.com/en/docs/claude-code) is Anthropic's CLI for working with Claude in the terminal.

**Step 1.** Clone this repository:

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
```

**Step 2.** Create a virtual environment and install the MCP server dependencies:

```bash
python3 -m venv .venv

# macOS / Linux
source .venv/bin/activate
pip install mcp pydantic huggingface-hub pyarrow

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
pip install mcp pydantic huggingface-hub pyarrow
```

**Step 3.** Register the MCP server with Claude Code:

```bash
# macOS / Linux
claude mcp add swiss-caselaw -- /path/to/caselaw-repo-1/.venv/bin/python3 /path/to/caselaw-repo-1/mcp_server.py

# Windows
claude mcp add swiss-caselaw -- C:\path\to\caselaw-repo-1\.venv\Scripts\python.exe C:\path\to\caselaw-repo-1\mcp_server.py
```

Use the full absolute path to the Python binary inside `.venv` so that the server always finds its dependencies, regardless of which directory you run Claude Code from.

**Step 4.** Restart Claude Code and run your first search.

On first use, the server automatically:
1. Downloads all Parquet files (~5.2 GB) from [HuggingFace](https://huggingface.co/datasets/voilaj/swiss-caselaw) to `~/.swiss-caselaw/parquet/`
2. Builds a local SQLite FTS5 full-text search index at `~/.swiss-caselaw/decisions.db` (~48 GB)

This takes 30–60 minutes depending on your machine and connection. It only happens once — after that, searches run instantly against the local database.

**Total disk usage:** ~53 GB in `~/.swiss-caselaw/`.

Example queries:

```
> Search for BGer decisions on Mietrecht Kündigung from 2024

> What did the BVGer rule on asylum seekers from Eritrea?

> Show me the full text of 6B_1234/2023

> How many decisions does each court in canton Zürich have?

> Find decisions citing Art. 8 BV
```

Claude calls the MCP tools automatically — you see the search results inline and can ask follow-up questions about specific decisions.

### Keeping the data current

The dataset is updated daily. To get the latest decisions, ask Claude to run the `update_database` tool, or call it explicitly. This re-downloads the Parquet files from HuggingFace and rebuilds the local database.

### Other MCP clients

The same server works with any MCP-compatible client. For Claude Desktop, add to the JSON config:

#### Claude Desktop extension (`.mcpb`) (recommended)

Build a local extension bundle from this repo:

```bash
./scripts/build_desktop_extension.sh
```

Requires Node.js/NPM and internet access on first run (for `npx @anthropic-ai/mcpb`).

This creates `dist/swiss-caselaw-local.mcpb` and auto-configures the bundle to run with:
- `./.venv/bin/python3` if present
- otherwise `python3` from your PATH

Install it in Claude Desktop:
1. `Settings` -> `Extensions`
2. `Advanced settings`
3. `Install Extension...`
4. Select `dist/swiss-caselaw-local.mcpb`

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "/path/to/caselaw-repo-1/.venv/bin/python3",
      "args": ["/path/to/caselaw-repo-1/mcp_server.py"]
    }
  }
}
```

On Windows, use `".venv\\Scripts\\python.exe"` instead of `".venv/bin/python3"`.

- **Claude Desktop**: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows)

### What the MCP server can do

| Tool | Description |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range, chamber, decision type) |
| `get_decision` | Fetch a single decision by docket number or ID |
| `list_courts` | List all courts with decision counts |
| `get_statistics` | Aggregate stats by court, canton, or year |
| `draft_mock_decision` | Build a research-only mock decision outline from facts, grounded in caselaw + statute references; asks clarification questions before conclusion (optionally enriched from Fedlex) |
| `update_database` | Re-download latest Parquet files from HuggingFace and rebuild the local database |

`draft_mock_decision` can use optional Fedlex URLs and caches fetched statute excerpts in
`~/.swiss-caselaw/fedlex_cache.json` (configurable via `SWISS_CASELAW_FEDLEX_CACHE`).

### How the local database works

```
~/.swiss-caselaw/
├── parquet/          # Downloaded Parquet files from HuggingFace (~5.2 GB)
│   └── data/
│       ├── bger.parquet
│       ├── bvger.parquet
│       └── ...       # 93 files, one per court
└── decisions.db      # SQLite FTS5 search index (~48 GB)
```

All data stays on your machine. No API calls are made during search — the MCP server queries the local SQLite database directly.

**Database structure.** `decisions.db` is a single SQLite file with two tables:

- **`decisions`** — the main table with one row per decision. Holds all 23 columns (decision_id, court, canton, chamber, docket_number, full_text, regeste, etc.) plus a `json_data` column with the complete 34-field record. Indexed on `court`, `canton`, `decision_date`, `language`, `docket_number`, `chamber`, and `decision_type` for fast filtered queries.

- **`decisions_fts`** — an FTS5 virtual table that mirrors 7 text columns from `decisions`: `court`, `canton`, `docket_number`, `language`, `title`, `regeste`, and `full_text`. FTS5 builds an inverted index over these columns, enabling sub-second full-text search across 1M+ decisions. The tokenizer is `unicode61 remove_diacritics 2`, which handles accented characters across German, French, Italian, and Romansh. Insert/update/delete triggers keep the FTS index in sync with the main table automatically.

**Why ~48 GB.** The full text of 1M+ court decisions averages ~15 KB per decision. The FTS5 inverted index adds overhead for every unique token, its position, and the column it appears in. This is a known trade-off: FTS5 indexes over large text corpora are substantially larger than the source data, but they enable instant ranked search without external infrastructure.

**Search pipeline.** When you search, the server:

1. **Detects query intent** — docket number lookup (`6B_1234/2023`), explicit FTS syntax (`Mietrecht AND Kündigung`), or natural language (`decisions on tenant eviction`).

2. **Runs multiple FTS5 query strategies** — For natural-language queries, the server generates several FTS query variants (AND, OR, phrase, field-focused on regeste/title, with multilingual term expansion) and executes them in sequence. Each strategy produces a ranked candidate set. For explicit syntax (AND/OR/NOT, quoted phrases, column filters), the raw query is tried first.

3. **Fuses candidates via RRF** — Results from all strategies are merged using Reciprocal Rank Fusion: each candidate's score is the weighted sum of `1/(k + rank)` across all strategies that returned it. Decisions found by multiple strategies get a boost.

4. **Reranks with signal scoring** — The top candidates are reranked using a composite score that combines:
   - **BM25 score** (from FTS5, with custom column weights: `full_text` 1.2, `regeste` 5.0, `title` 6.0 — headnotes and titles are weighted heavily over body text)
   - **Term coverage** in title (3.0x), regeste (2.2x), and snippet (0.8x)
   - **Phrase match** in title/regeste (1.8x)
   - **Docket match** — exact (6.0x) or partial (2.0x)
   - **Statute/citation graph signals** — if the query mentions an article (e.g., "Art. 8 BV"), decisions that cite that provision are boosted
   - **Court prior** — e.g., asylum queries boost BVGer results

5. **Selects the best passage** — For each result, the server scans the full text for the most relevant passage and returns it as a snippet.

### Search quality benchmark

Use a fixed golden query set to track search relevance over time:

```bash
python3 benchmarks/run_search_benchmark.py \
  --db ~/.swiss-caselaw/decisions.db \
  -k 10 \
  --json-output benchmarks/latest_search_benchmark.json
```

Metrics: `MRR@k`, `Recall@k`, `nDCG@k`, `Hit@1`

You can enforce minimum quality gates (non-zero exit on failure):

```bash
python3 benchmarks/run_search_benchmark.py \
  --db ~/.swiss-caselaw/decisions.db \
  -k 10 \
  --min-mrr 0.70 \
  --min-recall 0.80 \
  --min-ndcg 0.90
```

### Build Reference Graph (Optional)

For statute/citation-aware reranking, build the local graph database:

```bash
python3 search_stack/build_reference_graph.py \
  --source-db ~/.swiss-caselaw/decisions.db \
  --courts bger,bge,bvger \
  --db output/reference_graph.db
```

Then point the server to it:

```bash
export SWISS_CASELAW_GRAPH_DB=output/reference_graph.db
```

Graph signals are enabled by default. To disable them, set `SWISS_CASELAW_GRAPH_SIGNALS=0`.

---

## 2. Download the dataset

The full dataset is on [HuggingFace](https://huggingface.co/datasets/voilaj/swiss-caselaw) as Parquet files — one file per court, 34 fields per decision including complete decision text.

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

> Note: The REST API serves the auto-converted version of the dataset. For per-court Parquet files with the full 34-field schema, use the [download method](#2-download-the-dataset) above.

---

## What's in each decision

Every decision has 34 structured fields:

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
| `cited_decisions` | string | JSON array of cited decision references |
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
| `source` | string | Data source (`entscheidsuche`, `direct_scrape`) |
| `source_id` | string | Source-specific ID (e.g. Signatur) |
| `source_spider` | string | Source spider/scraper name |
| `content_hash` | string | MD5 of full_text for deduplication |
| `has_full_text` | bool | Whether `full_text` is non-empty |
| `text_length` | int | Character count of `full_text` |

Full schema definition: [`models.py`](models.py)

---

## Coverage

### Federal courts

| Court | Code | Decisions | Period | Source |
|-------|------|-----------|--------|--------|
| Federal Supreme Court (BGer) | `bger` | ~173,000 | 2000–present | bger.ch |
| BGE Leading Cases | `bge` | ~45,000 | 1954–present | bger.ch |
| Federal Administrative Court (BVGer) | `bvger` | ~91,000 | 2007–present | bvger.ch |
| Federal Criminal Court (BStGer) | `bstger` | ~11,000 | 2005–present | bstger.weblaw.ch |
| Federal Admin. Practice (VPB) | `ch_vb` | ~23,000 | 1982–2016 | bk.admin.ch |
| ECHR (Swiss cases) | `bge_egmr` | ~470 | — | bger.ch |
| FINMA | `finma` | ~1,200 | 2014–2024 | finma.ch |
| EDÖB (Data Protection) | `edoeb` | ~1,200 | 2002–present | edoeb.admin.ch |
| Federal Patent Court (BPatGer) | `bpatger` | ~190 | 2012–present | bpatger.ch |
| Competition Commission (WEKO) | `weko` | ~120 | 2009–present | weko.admin.ch |
| Sports Tribunal | `ta_sst` | ~50 | 2024–present | sportstribunal.ch |
| Federal Council | `ch_bundesrat` | ~15 | 2012–present | admin.ch |

### Cantonal courts

93 courts across all 26 cantons. The largest cantonal collections:

| Canton | Courts | Decisions | Period |
|--------|--------|-----------|--------|
| Vaud (VD) | 3 | ~155,000 | 1984–present |
| Zürich (ZH) | 20 | ~126,000 | 1980–present |
| Genève (GE) | 1 | ~78,000 | 1993–present |
| Ticino (TI) | 1 | ~58,000 | 1995–present |
| St. Gallen (SG) | 7 | ~35,000 | 2001–present |
| Graubünden (GR) | 1 | ~29,000 | 2002–present |
| Basel-Landschaft (BL) | 1 | ~26,000 | 2000–present |
| Bern (BE) | 6 | ~26,000 | 2002–present |
| Aargau (AG) | 18 | ~21,000 | 1993–present |

All 26 cantons covered: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH.

Live per-court statistics: **[Dashboard](https://jonashertner.github.io/caselaw-repo-1/)**

---

## How it works

```
                        ┌──────────────────────────────────────────────────┐
                        │                  Daily Pipeline                  │
                        │                                                  │
Court websites ────────►│  Scrapers ──► JSONL ──┬──► Parquet ──► HuggingFace
  bger.ch               │  (43 scrapers,        │                          │
  bvger.ch              │   rate-limited,        └──► FTS5 DB ──► MCP Server
  cantonal portals      │   resumable)                                     │
  entscheidsuche.ch     │                                                  │
                        │  01:00 UTC  scrape     04:00 UTC  publish        │
                        └──────────────────────────────────────────────────┘
```

### Step by step

1. **Scrape** (01:00 UTC daily) — 43 scrapers run in parallel, each targeting a specific court's website or API. Every scraper is rate-limited and resumable: it tracks which decisions it has already seen and only fetches new ones. Output: one JSONL file per court.

2. **Build search index** (04:00 UTC) — JSONL files are ingested into a SQLite FTS5 database for full-text search. On Mon–Sat, this runs in **incremental mode**: a byte-offset checkpoint tracks how far each JSONL file has been read, so only newly appended decisions are processed (typically < 1 minute). On Sundays, a **full rebuild** compacts the FTS5 index and resets the checkpoint (~3 hours). Decisions from multiple sources (e.g., a BGer decision scraped directly *and* found on entscheidsuche.ch) are merged by `decision_id`. Direct scrapes take priority because they typically have richer metadata. A quality enrichment step fills in missing titles, regestes, and content hashes.

3. **Export** — JSONL files are converted to Parquet (one file per court) with a fixed 34-field schema.

4. **Upload** — Parquet files are pushed to HuggingFace. The MCP server and `datasets` library pick up the new data automatically.

5. **Update dashboard** — `stats.json` is regenerated (including scraper health status from the last run) and pushed to GitHub Pages.

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

43 court codes are available. Run `python run_scraper.py --list` for the full list, or see the [dashboard](https://jonashertner.github.io/caselaw-repo-1/) for per-court statistics.

### Build a local search database

```bash
# Full build (reads all JSONL, optimizes FTS index — ~3h for 1M decisions)
python build_fts5.py --output output -v

# Incremental build (reads only new JSONL bytes, skips optimize — seconds)
python build_fts5.py --output output --incremental --no-optimize -v

# Full rebuild (deletes DB + checkpoint, rebuilds from scratch)
python build_fts5.py --output output --full-rebuild -v
```

This reads JSONL files from `output/decisions/` and builds a SQLite FTS5 database at `output/decisions.db`. A full build of 1M decisions takes about 3 hours and produces a ~48 GB database. Incremental mode uses a checkpoint file (`output/.fts5_checkpoint.json`) to skip unchanged files and seek past already-processed bytes, completing in seconds when few new decisions exist.

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
| **Federal regulatory bodies** | FINMA, WEKO, EDÖB, VPB | Sitecore/custom APIs |
| **Cantonal court portals** | 26 cantonal platforms (Weblaw, Tribuna, FindInfo, custom portals) | Court-specific scrapers |
| **[entscheidsuche.ch](https://entscheidsuche.ch)** | Community-maintained archive of Swiss court decisions | Bulk download + ingest |

Decisions appearing in multiple sources are deduplicated by `decision_id` (a deterministic hash of court code + normalized docket number). The most metadata-rich version is kept.

---

## Legal basis

Court decisions are public records under Swiss law. Article 27 BGG requires the Federal Supreme Court to publish its decisions. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This project scrapes only publicly available, officially published decisions.

---

## License

MIT. See [LICENSE](LICENSE).
