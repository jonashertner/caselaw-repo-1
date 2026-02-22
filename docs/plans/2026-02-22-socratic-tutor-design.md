# Socratic Case Law Tutor — Design Document

**Date:** 2026-02-22
**Status:** Approved

## Overview

Transform opencaselaw.ch from a legal research platform into a legal education platform by adding Socratic case study tools. Law students can study leading cases (Leitentscheide / BGE) interactively with structured parsing, curated curricula, and scaffolded briefing exercises.

## Core Design Principle

**Tools return structured data; the calling LLM teaches.** No embedded LLM calls, no template-based question generation. The MCP tools handle what LLMs can't: parsing Swiss legal text structure, querying the citation graph, loading verified curriculum data. The calling LLM (Claude) generates Socratic questions, evaluates briefs, and adapts to the student.

## Components

### 1. Decision Structure Parser (`study/parser.py`)

Pure Python regex-based parser for BGE decision full_text.

**Input:** `full_text` string + `language` hint

**Output:**
```python
@dataclass
class ParsedDecision:
    sachverhalt: str
    erwagungen: list[Erwagung]
    dispositiv: str
    regeste: str
    language: str
    parse_quality: float  # 0.0-1.0

@dataclass
class Erwagung:
    number: str       # "3.2", "4.1.1"
    text: str
    statute_refs: list[str]
    depth: int        # 1=top, 2=sub, 3=sub-sub
```

**Section detection patterns:**

| Section | DE | FR | IT |
|---------|----|----|-----|
| Facts | `Sachverhalt`, `Aus den Sachverhalt`, `A.` | `Faits`, `En fait`, `A.` | `Fatti`, `In fatto`, `A.` |
| Reasoning | `Erwägung(en)`, numbered `1.`, `1.1.` | `Considérant(s)`, `En droit` | `Considerand(i/o)`, `In diritto` |
| Dispositiv | `Demnach erkennt` | `Par ces motifs` | `Per questi motivi` |

Erwägungen parsed by `^\s*(\d+\.(?:\d+\.)*)\s` with depth = number of dot-separated parts.

Statute refs per Erwägung via existing `extract_statute_references()` from `search_stack/reference_extraction.py`.

`parse_quality` = 0.3 per section found (Sachverhalt, Erwägungen, Dispositiv) + 0.1 for numbered Erwägungen.

### 2. Curriculum Engine (`study/curriculum_engine.py`)

Stateless JSON loader with case selection logic.

**Functions:**
- `load_curriculum(area=None)` — load one or all curriculum files
- `find_case(topic, difficulty=None, language=None)` — search curriculum by topic/concept/statute, fallback to `find_leading_cases`
- `list_areas(language="de")` — summary of available areas

**Case selection:** Search module names, concept lists, and statute fields. Filter by difficulty. Fallback to existing `find_leading_cases(query=topic, court="bge")`.

### 3. Curriculum Data (`study/curriculum/*.json`)

Five files covering core Rechtsgebiete:
- `vertragsrecht.json` — Contract law (OR AT)
- `haftpflicht.json` — Tort law (OR)
- `sachenrecht.json` — Property law (ZGB)
- `grundrechte.json` — Constitutional rights (BV)
- `strafrecht_at.json` — Criminal law general part (StGB)

Each: 3-6 modules, 8-15 verified BGE per module, difficulty 1-5, prerequisites, multilingual labels (DE/FR/IT).

**Population:** One-time script (`study/populate_curriculum.py`) queries `find_leading_cases` for each statute range, verifies decision_ids exist, outputs curated JSON.

### 4. MCP Tools (3 new tools in `mcp_server.py`)

#### `study_leading_case`
- **Params:** `topic`, `decision_id`, `difficulty`, `language`, `mode` (guided/brief/quick)
- **Returns:** Parsed decision structure + curriculum metadata + citation counts + related cases
- **Modes:**
  - `guided`: Full parsed sections, Erwägung list with statute refs, reading guide pointers, related cases, citation counts
  - `brief`: Parsed sections + briefing template fields + statute refs
  - `quick`: Ratio (from regeste) + key Erwägung numbers + statutes + citation count

#### `list_study_curriculum`
- **Params:** `area`, `difficulty`, `language`
- **Returns:** Curriculum tree — areas, modules, cases with metadata

#### `check_case_brief`
- **Params:** `decision_id`, `brief`, `language`
- **Returns:** Parsed decision ground truth (ratio, statutes, Erwägung summaries, Dispositiv) alongside student's brief text, structured for LLM comparison

### 5. Tests

- `tests/test_study_parser.py` — Parser against real BGE text (DE/FR/IT, old/new), section detection, parse_quality scoring
- `tests/test_study_curriculum.py` — Schema validation, decision_id existence, DAG check on prerequisites
- `tests/test_study_tools.py` — MCP tool integration tests with mocked DB

### 6. Optional CLI (`study/cli.py`)

Thin wrapper for testing: `python -m study.cli study "Art. 41 OR"`, `python -m study.cli curriculum`, `python -m study.cli check <id> --brief "..."`.

## File Structure

```
study/
├── __init__.py
├── parser.py
├── curriculum_engine.py
├── socratic.py              # study package assembly + brief comparison
├── populate_curriculum.py   # one-time curriculum builder
├── cli.py
└── curriculum/
    ├── vertragsrecht.json
    ├── haftpflicht.json
    ├── sachenrecht.json
    ├── grundrechte.json
    └── strafrecht_at.json

tests/
├── test_study_parser.py
├── test_study_curriculum.py
└── test_study_tools.py
```

## What This Does NOT Include

- User accounts, progress tracking, spaced repetition
- Embedded LLM calls in tools
- Changes to existing MCP tools or search infrastructure
- Exam simulation, professor dashboard
- Changes to scraping pipeline or data model
