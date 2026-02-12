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
  - caselaw
  - court-decisions
  - nlp
pretty_name: Swiss Caselaw
size_categories:
  - 100K<n<1M
task_categories:
  - text-classification
  - summarization
---

# Swiss Caselaw Dataset

Comprehensive collection of Swiss court decisions from federal and cantonal courts, covering all four national languages (German, French, Italian, Romansh).

## Dataset Summary

This dataset contains **700k+** Swiss court decisions scraped from official court websites and [entscheidsuche.ch](https://entscheidsuche.ch). It includes full-text decisions from the Swiss Federal Supreme Court (Bundesgericht), Federal Administrative Court (Bundesverwaltungsgericht), Federal Criminal Court (Bundesstrafgericht), Federal Patent Court (Bundespatentgericht), and cantonal courts across all 26 cantons.

Each decision includes structured metadata (court, canton, docket number, date, language, legal area) alongside the complete decision text.

## Schema

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `decision_id` | string | Unique ID: `{court}_{docket_normalized}` |
| 2 | `court` | string | Court code (e.g., `bger`, `zh_obergericht`) |
| 3 | `canton` | string | `CH` for federal, two-letter canton code otherwise |
| 4 | `chamber` | string? | Chamber/Abteilung |
| 5 | `docket_number` | string | Original docket number (e.g., `6B_1234/2025`) |
| 6 | `docket_number_2` | string? | Secondary docket number |
| 7 | `decision_date` | string | ISO date of decision |
| 8 | `publication_date` | string? | Date published online |
| 9 | `language` | string | Language code: `de`, `fr`, `it`, `rm` |
| 10 | `title` | string? | Subject/Gegenstand |
| 11 | `legal_area` | string? | Rechtsgebiet/Domaine juridique |
| 12 | `regeste` | string? | Headnote/Regeste |
| 13 | `abstract_de` | string? | German abstract |
| 14 | `abstract_fr` | string? | French abstract |
| 15 | `abstract_it` | string? | Italian abstract |
| 16 | `full_text` | string | Complete decision text |
| 17 | `outcome` | string? | Decision outcome |
| 18 | `decision_type` | string? | Urteil, Beschluss, Verfügung, etc. |
| 19 | `judges` | string? | Participating judges |
| 20 | `clerks` | string? | Court clerks |
| 21 | `collection` | string? | Official collection reference |
| 22 | `appeal_info` | string? | Appeal status |
| 23 | `source_url` | string | Permanent URL to original |
| 24 | `pdf_url` | string? | Direct PDF link |
| 25 | `bge_reference` | string? | BGE reference if published |
| 26 | `cited_decisions` | string | JSON array of cited references |
| 27 | `scraped_at` | string | Scrape timestamp |
| 28 | `external_id` | string? | External cross-reference ID |
| 29 | `has_full_text` | bool | Whether full text is non-empty |
| 30 | `text_length` | int | Character count of full_text |

## Usage

```python
from datasets import load_dataset

# Load entire dataset
ds = load_dataset("voilaj/swiss-caselaw")

# Load a specific court
bger = load_dataset("voilaj/swiss-caselaw", data_files="bger.parquet")

# Filter by language
german = ds.filter(lambda x: x["language"] == "de")

# Search in regeste
mietrecht = ds.filter(lambda x: x["regeste"] and "Mietrecht" in x["regeste"])
```

### With pandas

```python
import pandas as pd

df = pd.read_parquet("hf://datasets/voilaj/swiss-caselaw/bger.parquet")
print(df.groupby("language").size())
print(df["decision_date"].min(), "—", df["decision_date"].max())
```

### Real-time Search via MCP

For full-text search integrated into Claude or Cursor:

```bash
# Install MCP server
claude mcp add swiss-caselaw -- python3 /path/to/mcp_server.py

# Then ask Claude:
# "Search Swiss caselaw for recent decisions on Mietrecht Kündigung"
```

## Court Coverage

### Federal Courts

| Court | Code | Description |
|-------|------|-------------|
| Bundesgericht | `bger` | Swiss Federal Supreme Court |
| BGE Leitentscheide | `bge` | Leading decisions (published collection) |
| Bundesverwaltungsgericht | `bvger` | Federal Administrative Court |
| Bundesstrafgericht | `bstger` | Federal Criminal Court |
| Bundespatentgericht | `bpatger` | Federal Patent Court |

### Cantonal Courts

All 26 cantons: AG, AI, AR, BE, BL, BS, FR, GE, GL, GR, JU, LU, NE, NW, OW, SG, SH, SO, SZ, TG, TI, UR, VD, VS, ZG, ZH

Court codes follow the pattern `{canton_lower}_{court_type}`, e.g., `zh_obergericht`, `ge_cour_justice`.

## Data Sources

1. **Official court websites** — Direct scraping from federal court APIs (bger.ch, bvger.ch, etc.)
2. **entscheidsuche.ch** — Comprehensive archive of Swiss court decisions maintained by the Swiss legal community

## Update Frequency

The dataset is updated daily via automated pipeline. New decisions are ingested, deduplicated, and published.

## License

MIT License. The underlying court decisions are public domain (Swiss government publications).

## Citation

```bibtex
@dataset{swiss_caselaw_2025,
  title={Swiss Caselaw Dataset},
  author={voilaj},
  year={2025},
  url={https://huggingface.co/datasets/voilaj/swiss-caselaw},
  note={Comprehensive collection of Swiss federal and cantonal court decisions}
}
```

## Links

- **Dashboard**: [swiss-caselaw.github.io](https://voilaj.github.io/swiss-caselaw/) — Live coverage statistics
- **MCP Server**: Real-time full-text search for Claude/Cursor
- **Source**: [github.com/voilaj/swiss-caselaw](https://github.com/voilaj/swiss-caselaw)
