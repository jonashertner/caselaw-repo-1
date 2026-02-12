# Swiss Jurisprudence Open Access Dataset

Open infrastructure for Swiss court decisions — scraping, full-text search, and bulk analysis.

**200,000+ decisions scraped. 760,000+ target. Daily updates.**

This project collects every published court decision in Switzerland — federal and cantonal — and makes them freely available as a structured, searchable dataset. Court decisions are public records under Swiss law (BGE 133 I 106, BGE 139 I 129).

## Use the dataset

### Option 1 — Download (researchers, data scientists)

The full dataset is available on HuggingFace in Parquet format. One file per court, ~30 structured fields per decision, including full text.

```python
from datasets import load_dataset

# Load all courts
ds = load_dataset("voilaj/swiss-caselaw")

# Load a single court
bger = load_dataset("voilaj/swiss-caselaw", data_files="bger.parquet")

# Filter in pandas
import pandas as pd
df = pd.read_parquet("hf://datasets/voilaj/swiss-caselaw/bger.parquet")
df_2024 = df[df["decision_date"] >= "2024-01-01"]
```

### Option 2 — Search from Claude (lawyers, researchers, anyone)

Connect the dataset to Claude and ask questions in natural language. The MCP server downloads the database automatically on first use (~800 MB).

1. **Install Claude** — download [Claude for Desktop](https://claude.ai/download) or install Claude Code (`npm install -g @anthropic-ai/claude-code`)

2. **Install Python dependencies**
   ```bash
   pip install mcp huggingface-hub pyarrow
   ```

3. **Connect the dataset**
   ```bash
   claude mcp add swiss-caselaw -- python3 mcp_server.py
   ```

4. **Ask questions** — Claude can now search across all Swiss court decisions:
   - *"Find BGer decisions on tenant eviction from 2024"*
   - *"What did the BVGer rule on asylum from Eritrea?"*
   - *"Show me the full text of 6B_1234/2023"*

### Option 3 — Query directly (developers)

Clone the repo and use the Python tools directly.

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
pip install -e ".[all]"

# Build or update the local search database
python build_fts5.py --output output --db output/decisions.db

# Search from Python
import sqlite3
conn = sqlite3.connect("output/decisions.db")
results = conn.execute("""
    SELECT decision_id, court, decision_date, snippet(decisions_fts, 7, '»', '«', '…', 40)
    FROM decisions_fts
    WHERE decisions_fts MATCH 'Mietrecht AND Kündigung'
    ORDER BY bm25(decisions_fts)
    LIMIT 10
""").fetchall()
```

## What's in the dataset

Every decision is a structured record with ~30 fields:

| Field | Description |
|-------|-------------|
| `decision_id` | Unique key: `{court}_{docket_normalized}` |
| `court` | Court code (`bger`, `bvger`, `zh_obergericht`, ...) |
| `canton` | `CH` (federal) or two-letter cantonal code |
| `docket_number` | Original case number (`6B_1234/2025`, `A-668/2020`) |
| `decision_date` | Date of the ruling |
| `language` | `de`, `fr`, `it`, or `rm` |
| `full_text` | Complete decision text |
| `regeste` | Legal headnote / summary |
| `title` | Subject matter |
| `legal_area` | Area of law |
| `decision_type` | Urteil, Beschluss, Verfügung, ... |
| `outcome` | Gutheissung, Abweisung, Nichteintreten, ... |
| `judges` | Panel composition |
| `cited_decisions` | Extracted citation references |
| `source_url` | Permanent link to the original |
| ... | + 15 more fields (chamber, clerks, abstracts, appeal info, PDF URL, ...) |

Full schema: [`models.py`](models.py)

## Court coverage

### Federal courts

| Court | Code | Decisions | Coverage | Source |
|-------|------|-----------|----------|--------|
| Federal Supreme Court | `bger` | ~250,000 | 2000–present | bger.ch (Eurospider) |
| BGE Leading Cases | `bge` | ~15,000 | 1954–present | search.bger.ch |
| Federal Administrative Court | `bvger` | ~91,000 | 2007–present | bvger.weblaw.ch |
| Federal Criminal Court | `bstger` | ~5,000 | 2005–present | bstger.weblaw.ch |
| Federal Patent Court | `bpatger` | ~250 | 2012–present | bundespatentgericht.ch |

### Cantonal courts

| Canton | Courts | Decisions | Source |
|--------|--------|-----------|--------|
| Zürich (ZH) | Obergericht, Verwaltungsgericht, Sozialversicherungsgericht, Baurekursgericht, Steuerrekursgericht | ~100,000 | zh.ch, entscheidsuche.ch |
| Aargau (AG) | Alle Gerichte | ~10,000 | decwork.ag.ch |
| Basel-Stadt (BS) | Alle Gerichte | ~10,000 | entscheidsuche.ch |
| Genève (GE) | Tous les tribunaux | ~88,000 | entscheidsuche.ch |
| Ticino (TI) | Tutti i tribunali | ~58,000 | entscheidsuche.ch |
| Vaud (VD) | Tous les tribunaux | ~71,000 | entscheidsuche.ch |
| + 20 more | via entscheidsuche.ch | ~150,000+ | entscheidsuche.ch |

Target: all 26 cantons. See [`scrapers/cantonal/registry.py`](scrapers/cantonal/registry.py) for the full mapping.

## Architecture

```
Court websites
     │
     ▼
┌─────────────────────────┐
│  Scrapers               │  5 federal + cantonal scrapers
│  (BaseScraper + models) │  Rate-limited, stateful, resumable
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  JSONL files            │  output/decisions/{court}.jsonl
│  (one per court)        │  Append-only, crash-safe
└────────────┬────────────┘
             │
     ┌───────┴───────┐
     ▼               ▼
┌──────────┐   ┌──────────────┐
│ Parquet  │   │ SQLite FTS5  │
│ export   │   │ full-text    │
│          │   │ search index │
└────┬─────┘   └──────┬───────┘
     │                │
     ▼                ▼
┌──────────┐   ┌──────────────┐
│ Hugging  │   │ MCP server   │
│ Face     │   │ (Claude,     │
│ dataset  │   │  Cursor)     │
└──────────┘   └──────────────┘
```

### Key files

| File | Purpose |
|------|---------|
| `models.py` | Unified `Decision` schema (Pydantic, 30 fields) |
| `base_scraper.py` | Abstract base: rate limiting, sessions, PoW, state |
| `run_scraper.py` | Run a single scraper with JSONL persistence |
| `pipeline.py` | Orchestrator: scrape → Parquet → HuggingFace → FTS5 |
| `build_fts5.py` | Build SQLite FTS5 search database from JSONL/Parquet |
| `export_parquet.py` | JSONL → deduplicated Parquet (one file per court) |
| `generate_stats.py` | Database → `docs/stats.json` for the dashboard |
| `mcp_server.py` | MCP server for Claude/Cursor integration |
| `publish.py` | Daily cron pipeline (ingest → FTS5 → Parquet → HF → stats → git push) |
| `scrapers/` | Federal court scrapers (bger, bge, bvger, bstger, bpatger) |
| `scrapers/cantonal/` | Cantonal scrapers + registry + platform base classes |
| `docs/index.html` | Public statistics dashboard |

## Running the scrapers

```bash
# Install
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
pip install -e ".[all]"

# Scrape 5 recent decisions from any court
python run_scraper.py bger --max 5 -v
python run_scraper.py bvger --since 2026-01-01 --max 10 -v

# Full historical scrape (takes hours/days)
python run_scraper.py bger --since 2000-01-01 -v

# Build search database from scraped JSONL
python build_fts5.py --output output --db output/decisions.db

# Export to Parquet for HuggingFace
python export_parquet.py --input output/decisions --output output/dataset

# Generate dashboard statistics
python generate_stats.py --db output/decisions.db --output docs/stats.json
```

## Technical notes

**BGer Proof-of-Work.** The Federal Supreme Court's Eurospider platform requires a SHA-256 proof-of-work cookie. The scraper mines a nonce where `SHA256(fingerprint + nonce)` has 16 leading zero bits (~65k hashes, under 1 second). This is only needed for `search.bger.ch`, not for `relevancy.bger.ch` direct fetches.

**BVGer dual-mode.** BVGer migrated from ICEfaces (jurispub.admin.ch) to Weblaw Lawsearch v4 (bvger.weblaw.ch) in 2023. Both platforms remain active as of Feb 2026. The scraper tries the Weblaw JSON API first and falls back to ICEfaces automatically.

**Rate limiting.** All scrapers enforce a minimum 2-second delay between requests. BVGer and BStGer use adaptive date windowing (start with 64-day ranges, halve if results exceed 100) to avoid overwhelming the APIs.

**Entscheidsuche.ch.** Cantonal decisions are sourced from [entscheidsuche.ch](https://entscheidsuche.ch), a public directory of Swiss court decisions operated by volunteers. The download script (`scrapers/entscheidsuche_download.py`) fetches JSON+HTML files from 52 court spiders covering all 26 cantons.

**Legal basis.** Court decisions are public records under Swiss law. The Bundesgericht has consistently held that court decisions must be made accessible to the public (BGE 133 I 106, BGE 139 I 129). This project scrapes only publicly available, officially published decisions.

## Daily automation

The `publish.py` script runs as a daily cron job on a VPS:

```bash
# Crontab entry
15 3 * * * cd /opt/caselaw/repo && python3 publish.py >> logs/publish.log 2>&1
```

It runs 6 steps in sequence: ingest new data → rebuild FTS5 → export Parquet → upload to HuggingFace → regenerate stats → git push dashboard.

## Contributing

Contributions are welcome:

- **Cantonal scrapers** — most are ~10 lines of config over the base classes. See `scrapers/cantonal/registry.py`
- **Data quality** — better outcome detection, judge extraction, citation parsing
- **Tests** — validation against live court endpoints

## License

MIT. See [LICENSE](LICENSE).
