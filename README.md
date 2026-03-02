# Swiss Case Law Open Dataset

**907,000+ court decisions from all Swiss federal courts and 26 cantons.**

Full text, structured metadata, four languages (DE/FR/IT/RM), updated daily. The largest open collection of Swiss jurisprudence.

[![CI](https://github.com/jonashertner/caselaw-repo-1/actions/workflows/ci.yml/badge.svg)](https://github.com/jonashertner/caselaw-repo-1/actions/workflows/ci.yml)
[![Dashboard](https://img.shields.io/badge/Dashboard-live-d1242f)](https://opencaselaw.ch)
[![HuggingFace](https://img.shields.io/badge/HuggingFace-dataset-blue)](https://huggingface.co/datasets/voilaj/swiss-caselaw)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## What this is

A structured, searchable archive of Swiss court decisions — from the Federal Supreme Court (BGer) down to cantonal courts in all 26 cantons. Every decision includes the full decision text, docket number, date, language, legal area, judges, cited decisions, and 20+ additional metadata fields.

The dataset is built by direct scraping of official court websites and cantonal court portals. New decisions are scraped, deduplicated, and published every night.

There are six ways to use it, depending on what you need:

| Method | For whom | What you get |
|--------|----------|-------------|
| [**Search with AI**](#1-search-with-ai) | Lawyers, researchers | Natural-language search in Claude, ChatGPT, or Gemini — instant access, no download |
| [**Citation Analysis**](#citation-graph-tools) | Legal scholars | Leading cases, citation networks, appeal chains, jurisprudence trends |
| [**Statute Lookup**](#statute-lookup-tools) | Legal professionals | Full article text from 40+ federal laws (OR, ZGB, StGB, BV, ...) |
| [**Legislation Search**](#legislation-tools) | Legal professionals | Search 33,000+ federal and cantonal legislative texts via LexFind.ch |
| [**Download**](#2-download-the-dataset) | Data scientists, NLP researchers | Bulk Parquet files with all 900K+ decisions |
| [**REST API**](#3-rest-api) | Developers | Programmatic row-level access, no setup |
| [**Web UI**](#4-web-ui) | Everyone | Chat interface — ask questions, get answers with cited decisions |

> **Not sure where to start?** Connect to the [remote MCP server](#option-a-remote-server-recommended) — works with Claude, ChatGPT, and Gemini CLI. Instant access to all 900K+ decisions, citation analysis, statute lookup, legislation search, and education tools, no download needed.

---

## 1. Search with AI

The dataset comes with an [MCP server](https://modelcontextprotocol.io) with 19 tools that lets AI search across all 900K+ decisions, look up federal law articles, search 33,000+ legislative texts, analyze citation networks, and study leading cases. You ask a question in natural language; the tool runs a full-text search and returns matching decisions with snippets.

### Remote vs. local

|  | Remote | Local |
|---|---|---|
| **Setup** | 30 seconds | 30–60 minutes |
| **Disk** | None | ~65 GB |
| **Tools** | 17 | 19 (+update_database, check_update_status) |
| **Freshness** | Nightly (automatic) | Manual |
| **Offline** | No | Yes |
| **Requires** | Claude, ChatGPT, or Gemini CLI (see plans below) | Any MCP client |

> Start with the remote server. Switch to local only if you need offline access.

### Option A: Remote server (recommended)

Connect directly to the hosted MCP server — no data download, no local database, instant access to 900K+ decisions.

**Claude.ai / Claude Desktop** (easiest):

1. Open **Settings** → **Connectors**
2. Click **"Add custom connector"**
3. Paste `https://mcp.opencaselaw.ch`
4. Click **Add**

Same steps in the browser (claude.ai) and the desktop app. No Node.js, no config files, no downloads.

> Available on Pro, Max, Team, and Enterprise plans. For the free plan, use Claude Code or the [manual JSON config](#alternative-manual-json-config-if-custom-connectors-arent-available).

**Claude Code:**

```bash
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch
```

<details>
<summary>Alternative: manual JSON config (if custom connectors aren't available)</summary>

Add to `claude_desktop_config.json` ([Node.js](https://nodejs.org) required):

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://mcp.opencaselaw.ch"]
    }
  }
}
```

Restart Claude Desktop after saving.

</details>

**ChatGPT:**

1. Open **Settings** → **Connectors** → enable **Developer Mode**
2. Click **Create**
3. Enter name `swiss-caselaw` and paste `https://mcp.opencaselaw.ch`
4. Check **"I trust this application"** → **Create**

> Available on Plus, Pro, Team, Enterprise, and Edu plans.

**Gemini CLI:**

Add to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "url": "https://mcp.opencaselaw.ch"
    }
  }
}
```

Restart Gemini CLI after saving. No account plan required — Gemini CLI is free.

> See the **[full MCP setup guide](docs/claude-desktop-setup.md)** for detailed instructions for all platforms.

> The `update_database` and `check_update_status` tools are only available on the local server — the remote dataset is updated automatically every night.

### Option B: Local server (offline access)

Run the MCP server locally with your own copy of the database (~65 GB disk). This gives you offline access and full control over the data.

#### Setup with Claude Code

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
1. Downloads all Parquet files (~7 GB) from [HuggingFace](https://huggingface.co/datasets/voilaj/swiss-caselaw) to `~/.swiss-caselaw/parquet/`
2. Builds a local SQLite FTS5 full-text search index at `~/.swiss-caselaw/decisions.db` (~58 GB)

This takes 30–60 minutes depending on your machine and connection. It only happens once — after that, searches run instantly against the local database.

**Total disk usage:** ~65 GB in `~/.swiss-caselaw/` (macOS/Linux) or `%USERPROFILE%\.swiss-caselaw\` (Windows).

#### Setup with Claude Desktop

See the **[Claude Desktop setup guide](docs/claude-desktop-setup.md)** for step-by-step instructions (macOS + Windows).

**Quick version** — add this to your `claude_desktop_config.json`:

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

Config file location: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows). On Windows, use `.venv\\Scripts\\python.exe` instead.

Any MCP-compatible client works with the same `command` + `args` pattern.

#### Keeping the data current

The dataset is updated daily. To get the latest decisions, ask Claude to run the `update_database` tool, or call it explicitly. This re-downloads the Parquet files from HuggingFace and rebuilds the local database.

#### How the local database works

```
~/.swiss-caselaw/
├── parquet/              # Downloaded Parquet files from HuggingFace (~7 GB)
│   └── data/
│       ├── bger.parquet
│       ├── bvger.parquet
│       └── ...           # 93 files, one per court
├── decisions.db          # SQLite FTS5 search index (~58 GB)
├── reference_graph.db    # Citation graph (7.85M edges, ~3.5 GB)
└── statutes.db           # Federal law articles from Fedlex (~42 MB)
```

All data stays on your machine. No API calls are made during search — the MCP server queries the local SQLite database directly.

**Database structure.** `decisions.db` is a single SQLite file with two tables:

- **`decisions`** — the main table with one row per decision. Holds all 23 columns (decision_id, court, canton, chamber, docket_number, full_text, regeste, etc.) plus a `json_data` column with the complete 34-field record. Indexed on `court`, `canton`, `decision_date`, `language`, `docket_number`, `chamber`, and `decision_type` for fast filtered queries.

- **`decisions_fts`** — an FTS5 virtual table that mirrors 7 text columns from `decisions`: `court`, `canton`, `docket_number`, `language`, `title`, `regeste`, and `full_text`. FTS5 builds an inverted index over these columns, enabling sub-second full-text search across 900K+ decisions. The tokenizer is `unicode61 remove_diacritics 2`, which handles accented characters across German, French, Italian, and Romansh. Insert/update/delete triggers keep the FTS index in sync with the main table automatically.

**Why ~58 GB.** The full text of 900K+ court decisions averages ~15 KB per decision. The FTS5 inverted index adds overhead for every unique token, its position, and the column it appears in. This is a known trade-off: FTS5 indexes over large text corpora are substantially larger than the source data, but they enable instant ranked search without external infrastructure.

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

### Available tools

Available on both remote and local unless noted.

| Tool | Description |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range, chamber, decision type) |
| `get_decision` | Fetch a single decision by docket number or ID. Includes citation graph counts (cited by / cites). |
| `list_courts` | List all courts with decision counts |
| `get_statistics` | Aggregate stats by court, canton, or year |
| `find_citations` | Show what a decision cites and what cites it, with confidence scores |
| `find_appeal_chain` | Trace the appeal chain (Instanzenzug) — prior and subsequent instances |
| `find_leading_cases` | Find the most-cited decisions for a topic or statute |
| `analyze_legal_trend` | Year-by-year decision counts for a statute or topic |
| `draft_mock_decision` | Build a research-only mock decision outline from facts, grounded in caselaw + statute references; asks clarification questions before conclusion |
| `study_leading_case` | Interactive Socratic study of a BGE — questions, hypotheticals, reading guide (guided/brief/quick modes) |
| `list_study_curriculum` | Browse the curriculum: 14 legal areas, 100 canonical BGEs, difficulty 1–5 |
| `check_case_brief` | Check a student's case brief against the actual decision with scoring rubric |
| `get_law` | Look up a Swiss federal law by SR number or abbreviation, with full article text |
| `search_laws` | Full-text search across Swiss federal law articles (Fedlex statute database) |
| `search_legislation` | Search 33,000+ Swiss legislative texts (federal + all 26 cantons) via LexFind.ch |
| `get_legislation` | Get details for a specific law by LexFind ID or SR number, with version history and source URLs |
| `browse_legislation_changes` | Browse recent legislation changes for a canton or federal level |
| `update_database` | Re-download latest Parquet files from HuggingFace and rebuild the local database *(local only)* |
| `check_update_status` | Check progress of a running database update *(local only)* |

### Example queries

These work on both the remote and local server:

```
> Search for BGer decisions on Mietrecht Kündigung from 2024

> What did the BVGer rule on asylum seekers from Eritrea?

> Show me the full text of 6B_1234/2023

> How many decisions does each court in canton Zürich have?

> Find decisions citing Art. 8 BV

> What are the leading cases on Art. 8 EMRK?

> Show me the citation network for BGE 138 III 374

> How has Mietrecht jurisprudence evolved over time?

> Show me Art. 41 OR (statute lookup)

> Search for statute provisions about Verjährung

> Trace the appeal chain for 5A_234/2026

> Show me the study curriculum for criminal law

> Let's study BGE 133 III 121 together

> Check my case brief for BGE 133 III 121: [your summary]

> Search for cantonal data protection laws

> Show me the details of SR 220 (Obligationenrecht)

> What legislation changed recently in Zürich?
```

The AI calls the MCP tools automatically — you see the search results inline and can ask follow-up questions about specific decisions.

### Citation graph tools

Four tools expose the **reference graph** — 7.85 million citation edges linking 900K+ decisions, plus 330K statute references. These require the graph database (`output/reference_graph.db`); if it's not available, the tools return a message instead of failing.

**`find_citations`** — Given a decision, show its outgoing citations (what it references) and incoming citations (what references it). Each resolved citation includes the target decision's metadata and a confidence score. Unresolved references (e.g., older decisions not in the dataset) appear with their raw reference text.

```
> Show me the citation network for BGE 138 III 374

## Outgoing (13 — what this decision cites)
1. BGE 136 III 365 (2010-01-01) [bge] conf=0.98 mentions=1
2. BGE 133 III 189 (2007-01-01) [bge] conf=0.93 mentions=1
...

## Incoming (13,621 — what cites this decision)
1. 5A_234/2026 (2026-02-19) [bger] conf=0.92 mentions=2
2. 5A_117/2026 (2026-01-30) [bger] conf=0.88 mentions=1
...
```

Parameters: `decision_id` (required), `direction` (both/outgoing/incoming), `min_confidence` (0–1, default 0.3), `limit` (default 50, max 200).

**`find_appeal_chain`** — Trace the appeal chain (Instanzenzug) for a decision. Shows prior instances (lower courts) and subsequent instances (appeals to higher courts), reconstructing the full procedural path (e.g., Bezirksgericht → Obergericht → Bundesgericht).

Parameters: `decision_id` (required), `min_confidence` (0–1, default 0.3).

**`find_leading_cases`** — Find the most-cited decisions, ranked by how many other decisions reference them. Filter by statute (law code + article), text query, court, or date range.

```
> What are the leading cases on Art. 8 EMRK?

1. BGE 135 I 143 (2009) — 3,840 citations
   Regeste: Anspruch auf Aufenthaltsbewilligung...
2. BGE 130 II 281 (2004) — 3,155 citations
   Regeste: Familiennachzug; gefestigtes Anwesenheitsrecht...
```

Parameters: `query` (optional text), `law_code` + `article` (optional statute), `court`, `date_from`, `date_to`, `limit` (default 20, max 100). At least one of `query` or `law_code` is recommended; without any filter it returns the globally most-cited decisions.

**`analyze_legal_trend`** — Year-by-year decision counts showing how jurisprudence on a topic or statute has evolved over time. Returns a table with counts and a visual bar chart.

```
> How has Art. 29 BV jurisprudence evolved?

**Filter:** Art. 29 BV
**Total:** 34,669 decisions

Year     Count  Bar
2000       275  █████
2005       733  ████████████
2010     1,028  █████████████████
2015     1,536  ██████████████████████████
2020     1,896  ████████████████████████████████
2025     2,400  ████████████████████████████████████████
```

Parameters: `query` (optional text), `law_code` + `article` (optional statute), `court`, `date_from`, `date_to`. At least one of `query` or `law_code` is required.

### Statute lookup tools

Two tools provide direct access to **Swiss federal law text** from the Classified Compilation (SR/RS), powered by a Fedlex statute database with 40+ laws and 25,000+ articles in three languages.

**`get_law`** — Look up a law by SR number or abbreviation, optionally fetching a specific article with full text.

```
> Show me Art. 8 BV

# BV — SR 101
**Bundesverfassung der Schweizerischen Eidgenossenschaft vom 18. April 1999**

### Art. 8 — Rechtsgleichheit
1 Alle Menschen sind vor dem Gesetz gleich.
2 Niemand darf diskriminiert werden, namentlich nicht wegen der Herkunft, ...
```

Parameters: `sr_number` or `abbreviation` (at least one required), `article` (optional — omit to see the full article list), `language` (de/fr/it, default de).

**`search_laws`** — Full-text search across all statute articles. Finds which law articles deal with a specific legal topic.

```
> Search for statute provisions about Verjährung

1. Art. 130 OR (SR 220): Die Verjährung beginnt mit der Fälligkeit der Forderung...
2. Art. 132 OR (SR 220): Bei der Berechnung der Frist ist der Tag...
3. Art. 790 ZGB (SR 210): Die Grundlast ist keiner Verjährung unterworfen...
```

Parameters: `query` (required, FTS5 syntax), `sr_number` (optional — restrict to one law), `language` (de/fr/it), `limit` (1–50).

The statute database covers the top 40 most-cited Swiss federal laws, including OR, ZGB, StGB, BV, BGG, StPO, ZPO, SchKG, and more. Data is sourced from [Fedlex](https://www.fedlex.admin.ch) (Akoma Ntoso XML).

### Legislation tools

Three tools provide access to **Swiss legislation** across all levels of government — federal, cantonal, and intercantonal — via the [LexFind.ch](https://www.lexfind.ch) API. This covers 33,000+ legislative texts including laws, ordinances, regulations, and international treaties.

> **Legislation vs. statute tools:** The statute tools (`get_law`, `search_laws`) provide article-level text from 40+ federal laws via a local Fedlex database. The legislation tools (`search_legislation`, `get_legislation`, `browse_legislation_changes`) cover a broader scope — all 26 cantons plus federal — but link to official sources rather than returning inline article text.

**`search_legislation`** — Full-text search across all Swiss legislative texts. Filter by canton, active/abrogated status, and whether to search titles only or full content.

```
> Search for cantonal data protection laws

1. Gesetz über die Information und den Datenschutz (SR 170.4, ZH)
   URL: https://www.zh.ch/...
2. Loi sur l'information du public, la protection des données... (SR 170.4, VD)
   URL: https://prestations.vd.ch/...
```

Parameters: `query` (required), `canton` (optional — CH, ZH, BE, etc.), `active_only` (default true), `search_in_content` (default false — searches titles/keywords; set true to search law text), `language` (de/fr/it), `limit` (1–60, default 20).

**`get_legislation`** — Get details for a specific law including metadata, version history, and links to official sources (Fedlex, cantonal portals).

```
> Show me the Obligationenrecht on LexFind

SR 220 — Bundesgesetz betreffend die Ergänzung des Schweizerischen
Zivilgesetzbuches (Fünfter Teil: Obligationenrecht)
Entity: Bund (CH) | Category: Gesetz | Keywords: OR
In force since: 01.01.2026
Sources:
  DE: https://www.fedlex.admin.ch/eli/cc/27/317_321_377/de
  FR: https://www.fedlex.admin.ch/eli/cc/27/317_321_377/fr
```

Parameters: `lexfind_id` (from search results) or `systematic_number` + `canton` (e.g., "220" + "CH"), `include_versions` (default false), `language` (de/fr/it).

**`browse_legislation_changes`** — Recent legislation changes for a canton or federal level. Shows new laws, amendments, and abrogations with dates and links.

```
> What legislation changed recently in Zürich?

1. [01.03.2026] version — Vollzugsverordnung zur Finanzverordnung (SR 181.131)
2. [01.03.2026] new — Studienordnung für den Masterstudiengang... (SR 414.253.125)
3. [01.03.2026] version — Organisationsverordnung der Finanzdirektion (SR 172.110.3)
```

Parameters: `canton` (default CH), `language` (de/fr/it).

### Education tools

Three tools support **Socratic legal education** with structured study of 100 canonical BGE decisions across Swiss law.

**`list_study_curriculum`** — Browse the curriculum: 14 legal areas, 100 landmark BGEs, each with difficulty rating (1–5) and module grouping. Areas covered: Vertragsrecht · Haftpflicht · Sachenrecht · Familienrecht · Arbeitsrecht · Mietrecht · Strafrecht AT · Strafrecht BT · Grundrechte · Erbrecht · Gesellschaftsrecht · Zivilprozessrecht · Strafprozessrecht · Öffentliches Prozessrecht.

**`study_leading_case`** — Socratic study package for a BGE. Returns the parsed decision structure, a reading guide, 5 Socratic questions (levels 1–5 with hints and model answers), 2 hypothetical variations with discussion points, and the key Erwägungen. Three modes: `guided` (full package), `brief` (structure + questions only), `quick` (ratio + key statutes).

**`check_case_brief`** — Compare a student's brief against the actual decision. Returns the ground truth (ratio, key Erwägungen, Dispositiv, statutes) alongside the brief text so the AI can give structured feedback.

`draft_mock_decision` can use optional Fedlex URLs and caches fetched statute excerpts in
`~/.swiss-caselaw/fedlex_cache.json` (configurable via `SWISS_CASELAW_FEDLEX_CACHE`).

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
  --min-mrr 0.50 \
  --min-recall 0.75 \
  --min-ndcg 0.85
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

### Build Statutes Database (Optional)

For statute lookup (`get_law`, `search_laws`), build the Fedlex statute database:

```bash
# Download the top 100 most-cited federal laws from Fedlex
python3 -m scrapers.fedlex --top 100

# Build the SQLite FTS5 statutes database
python3 -m search_stack.build_statutes_db
```

Then copy to the data directory:

```bash
cp output/statutes.db ~/.swiss-caselaw/
```

Or set the path explicitly:

```bash
export SWISS_CASELAW_STATUTES_DB=output/statutes.db
```

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

# Load all courts (~900K decisions, ~6.5 GB download)
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

> Note: The REST API queries the dataset as configured in the HuggingFace repo (per-court Parquet files, full 34-field schema). For bulk access or local analysis, use the [download method](#2-download-the-dataset) above.

---

## 4. Web UI

A local chat interface for searching Swiss court decisions. Ask questions in natural language, and an AI assistant searches the full corpus and answers with cited decisions.

```
Browser (localhost:5173)  →  FastAPI backend  →  MCP server  →  Local SQLite FTS5 DB
```

Everything runs on your machine. No data leaves your computer (except LLM API calls to the provider you choose).

### What you need

| Requirement | How to check | Where to get it |
|-------------|-------------|-----------------|
| **Python 3.10+** | `python3 --version` (macOS/Linux) or `python --version` (Windows) | [python.org/downloads](https://www.python.org/downloads/) |
| **Node.js 18+** | `node --version` | [nodejs.org](https://nodejs.org) — download the LTS version |
| **An LLM provider** | *(see below)* | At least one cloud API key **or** a local model via Ollama |
| **~65 GB free disk** | `df -h .` (macOS/Linux) | For the search index (downloaded on first run) |

> **Windows users:** Install Python from [python.org](https://www.python.org/downloads/) and check "Add Python to PATH" during installation. Node.js installs npm automatically.

#### Cloud providers (choose at least one, or use Ollama below)

| Provider | Env variable | Where to get a key | Cost |
|----------|-------------|---------------------|------|
| **Google Gemini** | `GEMINI_API_KEY` | [aistudio.google.com/apikey](https://aistudio.google.com/apikey) | Free tier available |
| **OpenAI** | `OPENAI_API_KEY` | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) | Free credits for new accounts |
| **Anthropic (Claude)** | `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com/) | Pay-as-you-go |

> **Important:** A Claude Desktop or Claude Pro *subscription* does NOT include an API key. You need a separate developer account at [console.anthropic.com](https://console.anthropic.com/).

#### Local models (no API key needed)

If you prefer not to use cloud APIs, you can run everything locally with [Ollama](https://ollama.com):

| Model | Command to install | Download size | RAM needed |
|-------|-------------------|---------------|------------|
| Qwen 2.5 (14B) | `ollama pull qwen2.5:14b` | ~9 GB | ~16 GB |
| Llama 3.3 (70B) | `ollama pull llama3.3:70b` | ~40 GB | ~48 GB |

Install Ollama from [ollama.com](https://ollama.com) (macOS, Linux, Windows), then:

```bash
ollama serve          # start the Ollama server (leave running)
ollama pull qwen2.5:14b   # download a model (one-time)
```

The Web UI auto-detects Ollama and shows local models as available.

### Step-by-step setup

**Step 1. Clone the repository:**

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
```

**Step 2. Create a Python virtual environment:**

```bash
python3 -m venv .venv
```

Activate it:

| OS | Command |
|----|---------|
| **macOS / Linux** | `source .venv/bin/activate` |
| **Windows (PowerShell)** | `.venv\Scripts\Activate.ps1` |
| **Windows (cmd)** | `.venv\Scripts\activate.bat` |

> You'll know it's active when your terminal prompt starts with `(.venv)`.

**Step 3. Install Python dependencies:**

```bash
pip install fastapi uvicorn python-dotenv mcp pyarrow pydantic
```

Then install at least one LLM provider SDK:

```bash
pip install anthropic        # for Claude
pip install openai           # for OpenAI / GPT-4o / local models via Ollama
pip install google-genai     # for Google Gemini
```

> **Tip:** The `openai` package is also used for local Ollama models (Ollama exposes an OpenAI-compatible API). If you only want to use local models, `pip install openai` is sufficient — no cloud API key required.

**Step 4. Install the frontend:**

```bash
cd web_ui && npm install && cd ..
```

**Step 5. Configure your API key:**

```bash
cp .env.example .env
```

Open `.env` in a text editor and paste your API key on the appropriate line. For example, if you have a Gemini key, change `GEMINI_API_KEY=AI...` to your actual key. Leave the other provider lines as-is — they will be ignored if empty.

> You can also skip this step and configure keys from the **Settings** panel inside the UI after starting.

**Step 6. Start the app:**

| OS | Command |
|----|---------|
| **macOS / Linux** | `./scripts/run_web_local.sh` |
| **Windows (PowerShell)** | `.\scripts\run_web_local.ps1` |

Open **http://localhost:5173** in your browser.

**What to expect on first run:** The MCP server will automatically download the dataset (~7 GB) from HuggingFace and build a local search index (~58 GB). This takes **30–60 minutes** depending on your connection and disk speed. You'll see progress in the terminal. After this one-time setup, the app starts instantly.

### Features

- **5 models**: Claude, OpenAI, Gemini (cloud) + Qwen 2.5 and Llama 3.3 via Ollama (local)
- **Local-first option**: Run entirely on your machine with Ollama — no cloud API keys needed
- **Streaming**: Responses appear token-by-token in real time
- **Tool-augmented chat**: The AI calls search, get_decision, list_courts, etc. automatically
- **Decision cards**: Clickable statute references with inline Fedlex article text
- **Filters**: Narrow results by court, canton, language, and date range
- **In-app settings**: Configure API keys and Ollama connection from the UI
- **Export**: Download conversations as Markdown, Word, or PDF

### Troubleshooting

| Problem | Solution |
|---------|----------|
| `python3: command not found` (Windows) | Use `python` instead of `python3`, or reinstall Python with "Add to PATH" checked |
| `npm: command not found` | Install Node.js from [nodejs.org](https://nodejs.org) |
| `ModuleNotFoundError: No module named 'fastapi'` | Activate your venv (`source .venv/bin/activate`) and re-run `pip install ...` |
| "No provider configured" banner | Click the gear icon (Settings) and paste an API key, or start Ollama |
| "Database not found" on first run | Wait for the initial download to finish (check terminal for progress) |
| Port already in use | Edit `BACKEND_PORT` or `FRONTEND_PORT` in `.env` |
| PowerShell script blocked (Windows) | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once |

For advanced configuration (custom ports, MCP server path, timeouts), see [`.env.example`](.env.example).

---

## What's in each decision

Every decision has 34 structured fields:

### Core fields

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `decision_id` | string | `bger_6B_1234_2025` | Unique key: `{court}_{docket_normalized}` |
| `court` | string | `bger` | Court code ([full list](https://opencaselaw.ch)) |
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
| `source` | string | Data source identifier |
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
| Federal Supreme Court (BGer) | `bger` | ~173,000 | 1996–present | bger.ch |
| BGE Leading Cases | `bge` | ~45,000 | 1954–present | bger.ch CLIR |
| Federal Administrative Court (BVGer) | `bvger` | ~91,000 | 2007–present | bvger.ch |
| Federal Admin. Practice (VPB) | `ch_vb` | ~23,000 | 1982–2016 | admin.ch |
| Federal Criminal Court (BStGer) | `bstger` | ~11,000 | 2004–present | bstger.weblaw.ch |
| EDÖB (Data Protection) | `edoeb` | ~1,200 | 1994–present | edoeb.admin.ch |
| FINMA | `finma` | ~1,200 | 2008–2024 | finma.ch |
| ECHR (Swiss cases) | `bge_egmr` | ~475 | 1974–present | bger.ch CLIR |
| Federal Patent Court (BPatGer) | `bpatger` | ~190 | 2012–present | bpatger.ch |
| Competition Commission (WEKO) | `weko` | ~120 | 2009–present | weko.admin.ch |
| Sports Tribunal | `ta_sst` | ~50 | 2024–present | ta-sst.ch |
| Federal Council | `ch_bundesrat` | ~15 | 2012–present | bj.admin.ch |

### Cantonal courts

92 courts across all 26 cantons. The largest cantonal collections:

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

Live per-court statistics: **[Dashboard](https://opencaselaw.ch)**

---

## How it works

```
                        ┌──────────────────────────────────────────────────────┐
                        │                    Daily Pipeline                    │
                        │                                                      │
Court websites ────────►│  Scrapers ──► JSONL ──┬──► Parquet ──► HuggingFace   │
  bger.ch               │  (45 scrapers,        │                              │
  bvger.ch              │   rate-limited,        ├──► FTS5 DB ───┐             │
  cantonal portals      │   resumable)           │               ├► MCP Server │
                        │                        └──► Graph DB ──┘             │
                        │                                          ▲           │
Fedlex (SPARQL) ───────►│  Fedlex scraper ──► XML ──► Statutes DB ─┘           │
                        │                                                      │
                        │  01:00 UTC  scrape        04:00 UTC  publish         │
                        └──────────────────────────────────────────────────────┘
```

### Step by step

1. **Scrape** (01:00 UTC daily) — 45 scrapers run in parallel, each targeting a specific court's website or API. Every scraper is rate-limited and resumable: it tracks which decisions it has already seen and only fetches new ones. Output: one JSONL file per court. A separate Fedlex scraper downloads federal law texts (Akoma Ntoso XML) via SPARQL for the statute database.

2. **Build search index** (04:00 UTC) — JSONL files are ingested into a SQLite FTS5 database for full-text search. On Mon–Sat, this runs in **incremental mode**: a byte-offset checkpoint tracks how far each JSONL file has been read, so only newly appended decisions are processed (typically < 1 minute). On Sundays, a **full rebuild** compacts the FTS5 index and resets the checkpoint (~3 hours). Decisions appearing in multiple sources are deduplicated by `decision_id`, keeping the version with the longest full text. A quality enrichment step fills in missing titles, regestes, and content hashes.

3. **Export** — JSONL files are converted to Parquet (one file per court) with a fixed 34-field schema.

4. **Upload** — Parquet files are pushed to HuggingFace. The MCP server and `datasets` library pick up the new data automatically.

5. **Update dashboard** — `stats.json` is regenerated (including scraper health status from the last run) and pushed to GitHub Pages.

---

## Running locally (developer)

For contributors and developers who want to run scrapers, build the pipeline, or modify the codebase.

### Prerequisites

- Python 3.10+
- pip

### Install

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux — on Windows: .venv\Scripts\Activate.ps1
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

45 court codes are available. Run `python run_scraper.py --list` for the full list, or see the [dashboard](https://opencaselaw.ch) for per-court statistics.

### Build a local search database

```bash
# Full build (reads all JSONL, optimizes FTS index — ~3h for 900K decisions)
python build_fts5.py --output output -v

# Incremental build (reads only new JSONL bytes, skips optimize — seconds)
python build_fts5.py --output output --incremental --no-optimize -v

# Full rebuild (deletes DB + checkpoint, rebuilds from scratch)
python build_fts5.py --output output --full-rebuild -v
```

This reads JSONL files from `output/decisions/` and builds a SQLite FTS5 database at `output/decisions.db`. A full build of 900K decisions takes about 3 hours and produces a ~58 GB database. Incremental mode uses a checkpoint file (`output/.fts5_checkpoint.json`) to skip unchanged files and seek past already-processed bytes, completing in seconds when few new decisions exist.

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

Decisions appearing in multiple sources are deduplicated by `decision_id` (a deterministic hash of court code + normalized docket number). The version with the longest full text is kept.

---

## Legal basis

Court decisions are public records under Swiss law. Article 27 BGG requires the Federal Supreme Court to publish its decisions. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This project scrapes only publicly available, officially published decisions.

---

## License

MIT. See [LICENSE](LICENSE).

---

## Contact

Questions, feedback, or ideas? Reach out at **team@jonashertner.com**.

You can also [open an issue](https://github.com/jonashertner/caselaw-repo-1/issues) on GitHub.
