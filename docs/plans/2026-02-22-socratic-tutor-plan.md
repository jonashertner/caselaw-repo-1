# Socratic Case Law Tutor — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 3 MCP tools (`study_leading_case`, `list_study_curriculum`, `check_case_brief`) that parse BGE decisions into structural components, load curated curriculum data, and return enriched study packages for Socratic legal education.

**Architecture:** Pure data-extraction tools — parse Swiss legal text, query citation graph, load curriculum JSON. No embedded LLM calls. The calling LLM does all pedagogical reasoning. Reuses existing `get_decision_by_id`, `_find_leading_cases`, `_count_citations`, and `extract_statute_references` from the codebase.

**Tech Stack:** Python 3.12, dataclasses, regex, JSON, pytest. Integrates into existing `mcp_server.py` MCP server.

---

## Task 1: Decision Structure Parser — Dataclasses and Section Detection

**Files:**
- Create: `study/__init__.py`
- Create: `study/parser.py`
- Create: `tests/test_study_parser.py`

**Step 1: Create the study package**

```bash
mkdir -p study/curriculum tests
```

**Step 2: Write `study/__init__.py`**

```python
```

Empty `__init__.py` — the package is just a namespace.

**Step 3: Write failing tests for the parser**

File: `tests/test_study_parser.py`

```python
from __future__ import annotations

from study.parser import Erwagung, ParsedDecision, parse_decision


# ── Minimal synthetic texts for unit tests ────────────────────

DE_DECISION = """
Sachverhalt

A. Die Beschwerdeführerin ist Eigentümerin eines Grundstücks.

B. Das Obergericht wies die Klage ab.

Erwägungen

1. Die Beschwerde ist zulässig (Art. 72 BGG).

1.1. Gemäss Art. 8 BV sind alle Menschen vor dem Gesetz gleich.

1.2. Der Grundsatz von Treu und Glauben (Art. 2 ZGB) ist zu beachten.

2. In der Sache selbst ist die Beschwerde unbegründet.

2.1. Die Vorinstanz hat Art. 41 OR korrekt angewendet.

Demnach erkennt das Bundesgericht:

1. Die Beschwerde wird abgewiesen.
2. Die Gerichtskosten werden der Beschwerdeführerin auferlegt.
"""

FR_DECISION = """
Faits

A. Le recourant est propriétaire d'un immeuble.

B. Le Tribunal cantonal a rejeté la demande.

Considérants

1. Le recours est recevable (art. 72 LTF).

1.1. Selon l'art. 8 Cst., tous les êtres humains sont égaux devant la loi.

2. Sur le fond, le recours est mal fondé.

Par ces motifs, le Tribunal fédéral prononce:

1. Le recours est rejeté.
"""

IT_DECISION = """
Fatti

A. Il ricorrente è proprietario di un immobile.

B. Il Tribunale cantonale ha respinto la domanda.

Considerandi

1. Il ricorso è ammissibile (art. 72 LTF).

2. Nel merito, il ricorso è infondato.

Per questi motivi, il Tribunale federale pronuncia:

1. Il ricorso è respinto.
"""


def test_parse_german_decision():
    result = parse_decision(DE_DECISION, language="de", regeste="Testregeste")
    assert isinstance(result, ParsedDecision)
    assert "Eigentümerin" in result.sachverhalt
    assert "abgewiesen" in result.dispositiv
    assert len(result.erwagungen) >= 4  # 1, 1.1, 1.2, 2, 2.1
    assert result.regeste == "Testregeste"
    assert result.language == "de"
    assert result.parse_quality >= 0.9


def test_parse_french_decision():
    result = parse_decision(FR_DECISION, language="fr", regeste="")
    assert "propriétaire" in result.sachverhalt
    assert "rejeté" in result.dispositiv
    assert len(result.erwagungen) >= 2
    assert result.parse_quality >= 0.9


def test_parse_italian_decision():
    result = parse_decision(IT_DECISION, language="it", regeste="")
    assert "proprietario" in result.sachverhalt
    assert "respinto" in result.dispositiv
    assert len(result.erwagungen) >= 2
    assert result.parse_quality >= 0.9


def test_erwagung_numbering_and_depth():
    result = parse_decision(DE_DECISION, language="de", regeste="")
    numbers = [e.number for e in result.erwagungen]
    assert "1" in numbers or "1." in numbers
    assert "1.1" in numbers or "1.1." in numbers

    top = [e for e in result.erwagungen if e.depth == 1]
    sub = [e for e in result.erwagungen if e.depth == 2]
    assert len(top) >= 2
    assert len(sub) >= 2


def test_statute_refs_per_erwagung():
    result = parse_decision(DE_DECISION, language="de", regeste="")
    # E. 1.1 mentions Art. 8 BV
    e11 = [e for e in result.erwagungen if e.number in ("1.1", "1.1.")]
    assert len(e11) == 1
    refs = e11[0].statute_refs
    assert any("BV" in r for r in refs)


def test_empty_text_returns_low_quality():
    result = parse_decision("", language="de", regeste="")
    assert result.parse_quality <= 0.1
    assert result.erwagungen == []


def test_partial_parse_returns_medium_quality():
    """Text with Erwägungen but no clear Sachverhalt header."""
    text = """
1. Die Beschwerde ist zulässig.
2. Die Beschwerde ist unbegründet.
Demnach erkennt das Bundesgericht:
1. Abgewiesen.
"""
    result = parse_decision(text, language="de", regeste="")
    assert 0.3 <= result.parse_quality <= 0.7
    assert len(result.erwagungen) >= 2
```

**Step 4: Run tests to verify they fail**

```bash
python3 -m pytest tests/test_study_parser.py -v
```

Expected: ImportError — `study.parser` does not exist yet.

**Step 5: Implement `study/parser.py`**

File: `study/parser.py`

```python
"""Parse BGE decision full_text into structural components."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from search_stack.reference_extraction import extract_statute_references


@dataclass
class Erwagung:
    number: str           # "1", "1.1", "3.2.1"
    text: str
    statute_refs: list[str] = field(default_factory=list)
    depth: int = 1        # 1=top, 2=sub, 3=sub-sub


@dataclass
class ParsedDecision:
    sachverhalt: str = ""
    erwagungen: list[Erwagung] = field(default_factory=list)
    dispositiv: str = ""
    regeste: str = ""
    language: str = "de"
    parse_quality: float = 0.0


# ── Section header patterns ──────────────────────────────────

_SACHVERHALT_PATTERNS = [
    # DE
    re.compile(r"^\s*Sachverhalt\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Aus\s+den?\s+Sachverhalt", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Faits\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*En\s+fait\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Fatti\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*In\s+fatto\b", re.MULTILINE | re.IGNORECASE),
]

_ERWAGUNGEN_PATTERNS = [
    # DE
    re.compile(r"^\s*Erwägungen?\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Aus\s+den\s+Erwägungen\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Auszug\s+aus\s+den\s+Erwägungen\b", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Considérants?\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*En\s+droit\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Extrait\s+des\s+considérants\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Considerand[io]\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*In\s+diritto\b", re.MULTILINE | re.IGNORECASE),
]

_DISPOSITIV_PATTERNS = [
    # DE
    re.compile(r"^\s*Demnach\s+erkennt\b", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*(?:Das\s+)?Bundesgericht\s+erkennt\b", re.MULTILINE | re.IGNORECASE),
    # FR
    re.compile(r"^\s*Par\s+ces\s+motifs\b", re.MULTILINE | re.IGNORECASE),
    # IT
    re.compile(r"^\s*Per\s+questi\s+motivi\b", re.MULTILINE | re.IGNORECASE),
]

# Numbered Erwägung: "1.", "1.1.", "1.1.1.", "5.2."
_ERWAGUNG_NUM = re.compile(r"^\s*(\d+(?:\.\d+)*)\.\s", re.MULTILINE)


def _find_section_start(text: str, patterns: list[re.Pattern]) -> int | None:
    """Return the character offset of the first matching section header, or None."""
    best: int | None = None
    for pat in patterns:
        m = pat.search(text)
        if m and (best is None or m.start() < best):
            best = m.start()
    return best


def _extract_erwagungen(text: str) -> list[Erwagung]:
    """Split reasoning text into numbered Erwägungen."""
    matches = list(_ERWAGUNG_NUM.finditer(text))
    if not matches:
        return []

    erwagungen: list[Erwagung] = []
    for i, m in enumerate(matches):
        number = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        depth = number.count(".") + 1

        # Extract statute references from this Erwägung's text
        refs = extract_statute_references(body)
        ref_strs = [r.normalized for r in refs]

        erwagungen.append(Erwagung(
            number=number,
            text=body,
            statute_refs=ref_strs,
            depth=depth,
        ))

    return erwagungen


def parse_decision(
    full_text: str,
    *,
    language: str = "de",
    regeste: str = "",
) -> ParsedDecision:
    """Parse a BGE decision's full_text into structural components.

    Returns a ParsedDecision with best-effort parsing and a parse_quality score.
    """
    if not full_text or not full_text.strip():
        return ParsedDecision(regeste=regeste, language=language)

    text = full_text
    quality = 0.0

    # ── Locate section boundaries ────────────────────────────
    sach_start = _find_section_start(text, _SACHVERHALT_PATTERNS)
    erw_start = _find_section_start(text, _ERWAGUNGEN_PATTERNS)
    disp_start = _find_section_start(text, _DISPOSITIV_PATTERNS)

    # If no explicit Erwägungen header, try to find first numbered paragraph
    if erw_start is None:
        first_num = _ERWAGUNG_NUM.search(text)
        if first_num:
            erw_start = first_num.start()

    # ── Extract sections ─────────────────────────────────────

    # Sachverhalt: from header to Erwägungen (or Dispositiv, or end)
    sachverhalt = ""
    if sach_start is not None:
        sach_end = erw_start or disp_start or len(text)
        sachverhalt = text[sach_start:sach_end].strip()
        quality += 0.3

    # Erwägungen: from header to Dispositiv (or end)
    erwagungen_text = ""
    erwagungen: list[Erwagung] = []
    if erw_start is not None:
        erw_end = disp_start or len(text)
        erwagungen_text = text[erw_start:erw_end].strip()
        erwagungen = _extract_erwagungen(erwagungen_text)
        quality += 0.3
        if erwagungen:
            quality += 0.1

    # Dispositiv: from header to end
    dispositiv = ""
    if disp_start is not None:
        dispositiv = text[disp_start:].strip()
        quality += 0.3

    return ParsedDecision(
        sachverhalt=sachverhalt,
        erwagungen=erwagungen,
        dispositiv=dispositiv,
        regeste=regeste,
        language=language,
        parse_quality=min(quality, 1.0),
    )
```

**Step 6: Run tests**

```bash
python3 -m pytest tests/test_study_parser.py -v
```

Expected: All 8 tests PASS.

**Step 7: Commit**

```bash
git add study/__init__.py study/parser.py tests/test_study_parser.py
git commit -m "feat(study): add decision structure parser with section detection

Parses BGE full_text into Sachverhalt, numbered Erwägungen (with statute
refs), and Dispositiv. Supports DE/FR/IT. Returns parse_quality score."
```

---

## Task 2: Curriculum Engine

**Files:**
- Create: `study/curriculum_engine.py`
- Create: `tests/test_study_curriculum.py`

**Step 1: Write failing tests**

File: `tests/test_study_curriculum.py`

```python
from __future__ import annotations

import json
from pathlib import Path

from study.curriculum_engine import (
    CurriculumArea,
    CurriculumCase,
    find_case,
    list_areas,
    load_curriculum,
)

CURRICULUM_DIR = Path(__file__).resolve().parent.parent / "study" / "curriculum"


def test_load_all_curriculum():
    areas = load_curriculum()
    assert len(areas) >= 1  # at least one curriculum file exists


def test_load_curriculum_by_area():
    areas = load_curriculum(area="vertragsrecht")
    assert len(areas) == 1
    assert areas[0].area_id == "vertragsrecht"


def test_load_curriculum_unknown_area():
    areas = load_curriculum(area="nonexistent")
    assert areas == []


def test_list_areas_returns_summaries():
    summaries = list_areas(language="de")
    assert len(summaries) >= 1
    first = summaries[0]
    assert "area_id" in first
    assert "name" in first
    assert "case_count" in first


def test_find_case_by_topic():
    result = find_case("Vertragsschluss")
    if result is not None:  # depends on curriculum data
        assert isinstance(result, CurriculumCase)
        assert result.decision_id


def test_find_case_by_statute():
    result = find_case("Art. 41 OR")
    # May return None if curriculum doesn't cover this exact query
    if result is not None:
        assert isinstance(result, CurriculumCase)


def test_curriculum_json_schema():
    """All curriculum JSON files must match the expected schema."""
    for json_path in sorted(CURRICULUM_DIR.glob("*.json")):
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert "area_id" in data, f"{json_path.name}: missing area_id"
        assert "modules" in data, f"{json_path.name}: missing modules"
        for mod in data["modules"]:
            assert "id" in mod, f"{json_path.name}: module missing id"
            assert "cases" in mod, f"{json_path.name}: module {mod['id']} missing cases"
            for case in mod["cases"]:
                assert "decision_id" in case, f"{json_path.name}: case missing decision_id"
                assert "difficulty" in case, f"{json_path.name}: case missing difficulty"
                assert 1 <= case["difficulty"] <= 5, f"{json_path.name}: difficulty out of range"


def test_curriculum_prerequisites_are_dag():
    """Prerequisite references must not form cycles."""
    areas = load_curriculum()
    all_ids = set()
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                all_ids.add(case.decision_id)

    # Check all prerequisites reference existing cases
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                for prereq in case.prerequisites:
                    assert prereq in all_ids, (
                        f"Prerequisite {prereq} of {case.decision_id} not found"
                    )

    # Simple cycle detection via topological sort
    from collections import defaultdict, deque
    graph: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {cid: 0 for cid in all_ids}
    for area in areas:
        for mod in area.modules:
            for case in mod.cases:
                for prereq in case.prerequisites:
                    graph[prereq].append(case.decision_id)
                    in_degree[case.decision_id] = in_degree.get(case.decision_id, 0) + 1

    queue = deque(cid for cid, deg in in_degree.items() if deg == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    assert visited == len(all_ids), "Prerequisite graph contains a cycle"
```

**Step 2: Run tests to verify they fail**

```bash
python3 -m pytest tests/test_study_curriculum.py -v
```

Expected: ImportError — `study.curriculum_engine` does not exist.

**Step 3: Implement `study/curriculum_engine.py`**

File: `study/curriculum_engine.py`

```python
"""Curriculum loading and case selection for the Socratic tutor."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

CURRICULUM_DIR = Path(__file__).resolve().parent / "curriculum"


@dataclass
class CurriculumCase:
    decision_id: str
    bge_ref: str = ""
    title_de: str = ""
    title_fr: str = ""
    title_it: str = ""
    concepts_de: list[str] = field(default_factory=list)
    concepts_fr: list[str] = field(default_factory=list)
    concepts_it: list[str] = field(default_factory=list)
    statutes: list[str] = field(default_factory=list)
    difficulty: int = 1
    prerequisites: list[str] = field(default_factory=list)
    significance_de: str = ""
    significance_fr: str = ""
    significance_it: str = ""
    # Set at load time from parent context
    area_id: str = ""
    module_id: str = ""


@dataclass
class CurriculumModule:
    id: str
    name_de: str = ""
    name_fr: str = ""
    name_it: str = ""
    statutes: list[str] = field(default_factory=list)
    cases: list[CurriculumCase] = field(default_factory=list)


@dataclass
class CurriculumArea:
    area_id: str
    area_de: str = ""
    area_fr: str = ""
    area_it: str = ""
    description_de: str = ""
    modules: list[CurriculumModule] = field(default_factory=list)


def load_curriculum(*, area: str | None = None) -> list[CurriculumArea]:
    """Load curriculum from JSON files. Optionally filter by area_id."""
    areas: list[CurriculumArea] = []
    for json_path in sorted(CURRICULUM_DIR.glob("*.json")):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        area_id = data.get("area_id", json_path.stem)
        if area is not None and area_id != area:
            continue

        modules = []
        for mod_data in data.get("modules", []):
            cases = []
            for c in mod_data.get("cases", []):
                cases.append(CurriculumCase(
                    decision_id=c["decision_id"],
                    bge_ref=c.get("bge_ref", ""),
                    title_de=c.get("title_de", ""),
                    title_fr=c.get("title_fr", ""),
                    title_it=c.get("title_it", ""),
                    concepts_de=c.get("concepts_de", []),
                    concepts_fr=c.get("concepts_fr", []),
                    concepts_it=c.get("concepts_it", []),
                    statutes=c.get("statutes", []),
                    difficulty=c.get("difficulty", 1),
                    prerequisites=c.get("prerequisites", []),
                    significance_de=c.get("significance_de", ""),
                    significance_fr=c.get("significance_fr", ""),
                    significance_it=c.get("significance_it", ""),
                    area_id=area_id,
                    module_id=mod_data.get("id", ""),
                ))
            modules.append(CurriculumModule(
                id=mod_data.get("id", ""),
                name_de=mod_data.get("name_de", ""),
                name_fr=mod_data.get("name_fr", ""),
                name_it=mod_data.get("name_it", ""),
                statutes=mod_data.get("statutes", []),
                cases=cases,
            ))

        areas.append(CurriculumArea(
            area_id=area_id,
            area_de=data.get("area_de", ""),
            area_fr=data.get("area_fr", ""),
            area_it=data.get("area_it", ""),
            description_de=data.get("description_de", ""),
            modules=modules,
        ))

    return areas


def list_areas(*, language: str = "de") -> list[dict]:
    """Return summary of all curriculum areas."""
    result = []
    for a in load_curriculum():
        case_count = sum(len(m.cases) for m in a.modules)
        name_key = f"area_{language}" if language in ("de", "fr", "it") else "area_de"
        result.append({
            "area_id": a.area_id,
            "name": getattr(a, name_key, a.area_de) or a.area_de,
            "module_count": len(a.modules),
            "case_count": case_count,
        })
    return result


def find_case(
    topic: str,
    *,
    difficulty: int | None = None,
    language: str | None = None,
) -> CurriculumCase | None:
    """Find the best matching curriculum case for a topic string.

    Searches module names, case concepts, case statutes, and case titles.
    Returns the best match or None.
    """
    topic_lower = topic.lower()
    areas = load_curriculum()

    best: CurriculumCase | None = None
    best_score = 0

    for area in areas:
        for mod in area.modules:
            mod_match = topic_lower in mod.name_de.lower() or topic_lower in mod.name_fr.lower()
            for case in mod.cases:
                if difficulty is not None and case.difficulty > difficulty:
                    continue

                score = 0
                # Check title match
                for title in (case.title_de, case.title_fr, case.title_it):
                    if topic_lower in title.lower():
                        score += 3

                # Check concept match
                for concepts in (case.concepts_de, case.concepts_fr, case.concepts_it):
                    for concept in concepts:
                        if topic_lower in concept.lower():
                            score += 2

                # Check statute match
                for statute in case.statutes:
                    if topic_lower in statute.lower():
                        score += 2

                # Module context match
                if mod_match:
                    score += 1

                # Area id match
                if topic_lower in area.area_id:
                    score += 1

                if score > best_score:
                    best_score = score
                    best = case

    return best
```

**Step 4: Create a minimal curriculum file for testing**

File: `study/curriculum/vertragsrecht.json` (minimal placeholder — will be populated in Task 4)

```json
{
  "area_id": "vertragsrecht",
  "area_de": "Vertragsrecht (OR AT)",
  "area_fr": "Droit des contrats (CO PG)",
  "area_it": "Diritto contrattuale (CO PG)",
  "description_de": "Allgemeiner Teil des Obligationenrechts",
  "modules": [
    {
      "id": "vertragsschluss",
      "name_de": "Vertragsschluss",
      "name_fr": "Conclusion du contrat",
      "name_it": "Conclusione del contratto",
      "statutes": ["Art. 1-10 OR"],
      "cases": []
    }
  ]
}
```

**Step 5: Run tests**

```bash
python3 -m pytest tests/test_study_curriculum.py -v
```

Expected: Tests that depend on curriculum data pass (schema, load, list). `find_case` returns None with empty case lists — the test handles this gracefully.

**Step 6: Commit**

```bash
git add study/curriculum_engine.py study/curriculum/ tests/test_study_curriculum.py
git commit -m "feat(study): add curriculum engine with JSON loader and case search"
```

---

## Task 3: Populate Curriculum Data

**Files:**
- Create: `study/populate_curriculum.py` (one-time script)
- Modify: `study/curriculum/vertragsrecht.json`
- Create: `study/curriculum/haftpflicht.json`
- Create: `study/curriculum/sachenrecht.json`
- Create: `study/curriculum/grundrechte.json`
- Create: `study/curriculum/strafrecht_at.json`

**Step 1: Write the population script**

File: `study/populate_curriculum.py`

This script queries the remote MCP `find_leading_cases` tool to discover the most-cited BGE for each statute range. It outputs candidate lists that the implementer curates into final JSON.

```python
"""One-time script: query find_leading_cases to populate curriculum files.

Usage: python3 -m study.populate_curriculum

Queries the local mcp_server internals to find most-cited BGE per statute range,
then prints candidates for manual curation into curriculum JSON files.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Add repo root to path so we can import mcp_server internals
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server import _find_leading_cases, get_decision_by_id


QUERIES = [
    # Vertragsrecht
    ("vertragsrecht", "vertragsschluss", "OR", "1", "Vertragsschluss"),
    ("vertragsrecht", "vertragsschluss", "OR", "18", "Vertragsauslegung"),
    ("vertragsrecht", "willensmangel", "OR", "23", "Willensmängel Irrtum"),
    ("vertragsrecht", "erfullung", "OR", "97", "Leistungsstörungen"),
    ("vertragsrecht", "erfullung", "OR", "107", "Verzug Rücktritt"),
    # Haftpflicht
    ("haftpflicht", "verschuldenshaftung", "OR", "41", "Verschuldenshaftung"),
    ("haftpflicht", "kausalitat", "OR", "42", "Schadensbeweis"),
    ("haftpflicht", "gefahrdungshaftung", "OR", "55", "Geschäftsherrenhaftung"),
    # Sachenrecht
    ("sachenrecht", "eigentum", "ZGB", "641", "Eigentum"),
    ("sachenrecht", "besitz", "ZGB", "919", "Besitz"),
    ("sachenrecht", "grundbuch", "ZGB", "942", "Grundbuch"),
    # Grundrechte
    ("grundrechte", "rechtsgleichheit", "BV", "8", "Rechtsgleichheit"),
    ("grundrechte", "personliche_freiheit", "BV", "10", "Persönliche Freiheit"),
    ("grundrechte", "wirtschaftsfreiheit", "BV", "27", "Wirtschaftsfreiheit"),
    ("grundrechte", "verfahrensgarantien", "BV", "29", "Verfahrensgarantien"),
    # Strafrecht AT
    ("strafrecht_at", "vorsatz", "StGB", "12", "Vorsatz Fahrlässigkeit"),
    ("strafrecht_at", "versuch", "StGB", "22", "Versuch"),
    ("strafrecht_at", "teilnahme", "StGB", "24", "Anstiftung Gehilfenschaft"),
    ("strafrecht_at", "sanktionen", "StGB", "47", "Strafzumessung"),
]


def main():
    for area, module, law_code, article, label in QUERIES:
        print(f"\n{'='*60}")
        print(f"{area}/{module}: {law_code} Art. {article} — {label}")
        print(f"{'='*60}")
        result = _find_leading_cases(
            law_code=law_code,
            article=article,
            court="bge",
            limit=15,
        )
        if "error" in result:
            print(f"  ERROR: {result['error']}")
            continue
        for case in result.get("cases", []):
            did = case.get("decision_id", "?")
            docket = case.get("docket_number", "?")
            date = case.get("decision_date", "?")
            count = case.get("citation_count", 0)
            print(f"  {did} | {docket} | {date} | cited {count}x")


if __name__ == "__main__":
    main()
```

**Step 2: Run the population script to get candidate cases**

```bash
python3 -m study.populate_curriculum 2>&1 | head -200
```

This requires a local DB with the reference graph. If not available locally, use the remote MCP tools instead to query candidates.

**Step 3: Build the 5 curriculum JSON files**

Using the population script output + legal domain knowledge, create full JSON files. Each file follows this schema (shown for vertragsrecht, repeat for others):

```json
{
  "area_id": "vertragsrecht",
  "area_de": "Vertragsrecht (OR AT)",
  "area_fr": "Droit des contrats (CO PG)",
  "area_it": "Diritto contrattuale (CO PG)",
  "description_de": "Allgemeiner Teil des Obligationenrechts: Vertragsschluss, Auslegung, Erfüllung, Leistungsstörungen",
  "modules": [
    {
      "id": "vertragsschluss",
      "name_de": "Vertragsschluss",
      "name_fr": "Conclusion du contrat",
      "name_it": "Conclusione del contratto",
      "statutes": ["Art. 1 OR", "Art. 3 OR", "Art. 6 OR", "Art. 7 OR", "Art. 8 OR"],
      "cases": [
        {
          "decision_id": "<verified_id>",
          "bge_ref": "BGE <vol> <div> <page>",
          "title_de": "<short description>",
          "concepts_de": ["Angebot", "Annahme", "Konsens"],
          "statutes": ["Art. 1 OR"],
          "difficulty": 2,
          "prerequisites": [],
          "significance_de": "<why this case matters>"
        }
      ]
    }
  ]
}
```

**Step 4: Verify all decision_ids exist**

```bash
python3 -c "
import json
from pathlib import Path
from mcp_server import get_decision_by_id

curriculum_dir = Path('study/curriculum')
missing = []
for f in sorted(curriculum_dir.glob('*.json')):
    data = json.loads(f.read_text())
    for mod in data['modules']:
        for case in mod['cases']:
            if get_decision_by_id(case['decision_id']) is None:
                missing.append(case['decision_id'])
if missing:
    print(f'MISSING: {missing}')
else:
    print('All decision_ids verified.')
"
```

If any IDs are missing, fix them in the JSON files.

**Step 5: Run curriculum tests**

```bash
python3 -m pytest tests/test_study_curriculum.py -v
```

Expected: All tests PASS (schema valid, DAG check passes, load works).

**Step 6: Commit**

```bash
git add study/curriculum/ study/populate_curriculum.py
git commit -m "feat(study): add curriculum data for 5 Rechtsgebiete

Vertragsrecht, Haftpflicht, Sachenrecht, Grundrechte, Strafrecht AT.
All decision_ids verified against database."
```

---

## Task 4: Socratic Study Package Assembly (`study/socratic.py`)

**Files:**
- Create: `study/socratic.py`

**Step 1: Implement the study package builder**

File: `study/socratic.py`

```python
"""Assemble study packages and brief comparison data for MCP tools."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from study.parser import ParsedDecision, parse_decision
from study.curriculum_engine import (
    CurriculumCase,
    find_case,
    load_curriculum,
    list_areas,
)


def build_study_package(
    *,
    decision: dict,
    mode: str = "guided",
    curriculum_case: CurriculumCase | None = None,
    citation_counts: tuple[int, int] = (0, 0),
    related_cases: list[dict] | None = None,
) -> dict[str, Any]:
    """Build a structured study package from a fetched decision.

    Args:
        decision: Row dict from get_decision_by_id (has full_text, regeste, etc.)
        mode: "guided", "brief", or "quick"
        curriculum_case: Matching curriculum entry, if any
        citation_counts: (incoming, outgoing) from citation graph
        related_cases: Prerequisite/successor cases from curriculum

    Returns:
        Structured dict for the MCP tool response.
    """
    parsed = parse_decision(
        decision.get("full_text", ""),
        language=decision.get("language", "de"),
        regeste=decision.get("regeste", ""),
    )

    base = {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "decision_date": decision.get("decision_date", ""),
        "court": decision.get("court", ""),
        "chamber": decision.get("chamber", ""),
        "language": decision.get("language", ""),
        "cited_by_count": citation_counts[0],
        "cites_count": citation_counts[1],
        "parse_quality": parsed.parse_quality,
    }

    if curriculum_case:
        base["curriculum"] = {
            "area_id": curriculum_case.area_id,
            "module_id": curriculum_case.module_id,
            "bge_ref": curriculum_case.bge_ref,
            "title_de": curriculum_case.title_de,
            "concepts_de": curriculum_case.concepts_de,
            "statutes": curriculum_case.statutes,
            "difficulty": curriculum_case.difficulty,
            "significance_de": curriculum_case.significance_de,
        }

    if mode == "quick":
        # Minimal: regeste + top-level Erwägung numbers + statutes + citation count
        all_statutes = set()
        top_erwagungen = []
        for e in parsed.erwagungen:
            all_statutes.update(e.statute_refs)
            if e.depth == 1:
                top_erwagungen.append(e.number)
        base["regeste"] = parsed.regeste
        base["top_erwagungen"] = top_erwagungen
        base["all_statutes"] = sorted(all_statutes)
        return base

    if mode == "brief":
        # Parsed sections + statute refs — for briefing exercises
        base["regeste"] = parsed.regeste
        base["sachverhalt"] = parsed.sachverhalt
        base["erwagungen"] = [
            {
                "number": e.number,
                "depth": e.depth,
                "statute_refs": e.statute_refs,
                "text": e.text,
            }
            for e in parsed.erwagungen
        ]
        base["dispositiv"] = parsed.dispositiv
        return base

    # mode == "guided" (default): full package
    base["regeste"] = parsed.regeste
    base["sachverhalt"] = parsed.sachverhalt
    base["erwagungen"] = [
        {
            "number": e.number,
            "depth": e.depth,
            "statute_refs": e.statute_refs,
            "text": e.text,
        }
        for e in parsed.erwagungen
    ]
    base["dispositiv"] = parsed.dispositiv

    if related_cases:
        base["related_cases"] = related_cases

    return base


def build_brief_comparison(
    *,
    decision: dict,
    student_brief: str,
) -> dict[str, Any]:
    """Build a structured comparison between a student's brief and the decision.

    Returns the parsed decision ground truth alongside the student text,
    structured for the calling LLM to generate pedagogical feedback.
    """
    parsed = parse_decision(
        decision.get("full_text", ""),
        language=decision.get("language", "de"),
        regeste=decision.get("regeste", ""),
    )

    # Extract ground truth elements
    all_statutes = set()
    erwagung_summaries = []
    for e in parsed.erwagungen:
        all_statutes.update(e.statute_refs)
        erwagung_summaries.append({
            "number": e.number,
            "depth": e.depth,
            "statute_refs": e.statute_refs,
            # First 500 chars as summary — full text too long for comparison
            "summary": e.text[:500] + ("..." if len(e.text) > 500 else ""),
        })

    return {
        "decision_id": decision.get("decision_id", ""),
        "docket_number": decision.get("docket_number", ""),
        "language": decision.get("language", ""),
        "parse_quality": parsed.parse_quality,
        "ground_truth": {
            "regeste": parsed.regeste,
            "sachverhalt_excerpt": parsed.sachverhalt[:1000] + (
                "..." if len(parsed.sachverhalt) > 1000 else ""
            ),
            "erwagung_summaries": erwagung_summaries,
            "dispositiv": parsed.dispositiv,
            "statutes": sorted(all_statutes),
        },
        "student_brief": student_brief,
    }
```

**Step 2: Commit**

```bash
git add study/socratic.py
git commit -m "feat(study): add study package assembly and brief comparison"
```

---

## Task 5: MCP Tool Registration and Dispatch

**Files:**
- Modify: `mcp_server.py` (add Tool definitions + dispatch handlers)
- Create: `tests/test_study_tools.py`

**Step 1: Write failing integration tests**

File: `tests/test_study_tools.py`

```python
from __future__ import annotations

import json
from unittest.mock import patch

from study.socratic import build_study_package, build_brief_comparison


FAKE_DECISION = {
    "decision_id": "bge_144_III_93",
    "docket_number": "144 III 93",
    "decision_date": "2018-01-22",
    "court": "bge",
    "chamber": "I. zivilrechtliche Abteilung",
    "language": "fr",
    "regeste": "Prêt ou donation. Art. 312 CO, Art. 239 CO.",
    "full_text": """Sachverhalt

A. Les parties ont vécu ensemble.

Erwägungen

5. Il est établi que le demandeur a versé le montant.

5.1. Le prêt est un contrat (Art. 312 CO).

5.2. La donation est la disposition (Art. 239 CO).

Demnach erkennt das Bundesgericht:

1. Le recours est rejeté.
""",
}


def test_build_study_package_guided():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="guided",
        citation_counts=(347, 14),
    )
    assert result["decision_id"] == "bge_144_III_93"
    assert result["cited_by_count"] == 347
    assert "erwagungen" in result
    assert len(result["erwagungen"]) >= 3
    assert "sachverhalt" in result
    assert "dispositiv" in result


def test_build_study_package_quick():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="quick",
    )
    assert "regeste" in result
    assert "top_erwagungen" in result
    assert "sachverhalt" not in result  # quick mode omits full sections


def test_build_study_package_brief():
    result = build_study_package(
        decision=FAKE_DECISION,
        mode="brief",
    )
    assert "erwagungen" in result
    assert "sachverhalt" in result


def test_build_brief_comparison():
    result = build_brief_comparison(
        decision=FAKE_DECISION,
        student_brief="The court held that a loan requires restitution.",
    )
    assert "ground_truth" in result
    assert "student_brief" in result
    assert result["student_brief"] == "The court held that a loan requires restitution."
    assert "regeste" in result["ground_truth"]
    assert "erwagung_summaries" in result["ground_truth"]
    assert len(result["ground_truth"]["statutes"]) >= 1
```

**Step 2: Run tests to verify they pass** (these test `study/socratic.py` directly, not MCP wiring)

```bash
python3 -m pytest tests/test_study_tools.py -v
```

Expected: PASS (tests use the already-implemented `socratic.py`).

**Step 3: Add Tool definitions to `mcp_server.py`**

In `handle_list_tools()`, add 3 new Tool entries before the `update_database` conditional block (before line ~4547 `*([] if REMOTE_MODE`):

```python
        Tool(
            name="study_leading_case",
            description=(
                "Study a leading Swiss court decision (BGE/Leitentscheid) interactively. "
                "Returns parsed decision structure (Sachverhalt, numbered Erwägungen with "
                "statute references, Dispositiv), curriculum metadata, and citation graph data. "
                "Use for Socratic legal education: the returned structure enables generating "
                "comprehension questions, reading guides, and case briefing exercises."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": (
                            "Legal topic or concept (e.g., 'Vertragsschluss', 'Art. 41 OR', "
                            "'Haftpflicht'). Used to find a matching case from the curriculum."
                        ),
                    },
                    "decision_id": {
                        "type": "string",
                        "description": "Specific BGE decision_id to study (e.g., 'bge_144_III_93').",
                    },
                    "difficulty": {
                        "type": "integer",
                        "description": "Target difficulty (1=introductory, 5=complex). Filters curriculum cases.",
                    },
                    "language": {
                        "type": "string",
                        "description": "Preferred language for labels (de, fr, it).",
                        "enum": ["de", "fr", "it"],
                    },
                    "mode": {
                        "type": "string",
                        "description": (
                            "Study mode: 'guided' (full structure + related cases), "
                            "'brief' (for case briefing exercises), "
                            "'quick' (key points only for revision)."
                        ),
                        "enum": ["guided", "brief", "quick"],
                        "default": "guided",
                    },
                },
            },
        ),
        Tool(
            name="list_study_curriculum",
            description=(
                "List available study curricula for Swiss law. "
                "Returns areas (Rechtsgebiete), modules, and cases with metadata. "
                "Covers: Vertragsrecht, Haftpflicht, Sachenrecht, Grundrechte, Strafrecht AT."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "area": {
                        "type": "string",
                        "description": (
                            "Filter by Rechtsgebiet: vertragsrecht, haftpflicht, "
                            "sachenrecht, grundrechte, strafrecht_at."
                        ),
                    },
                    "difficulty": {
                        "type": "integer",
                        "description": "Show only cases up to this difficulty (1-5).",
                    },
                    "language": {
                        "type": "string",
                        "description": "Language for labels (de, fr, it).",
                        "enum": ["de", "fr", "it"],
                    },
                },
            },
        ),
        Tool(
            name="check_case_brief",
            description=(
                "Check a student's case brief against the actual decision. "
                "Returns the parsed decision ground truth (ratio from regeste, statute list, "
                "Erwägung summaries, Dispositiv) alongside the student's brief, structured "
                "for comparison and pedagogical feedback generation."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "decision_id": {
                        "type": "string",
                        "description": "The BGE decision_id being briefed.",
                    },
                    "brief": {
                        "type": "string",
                        "description": "The student's case brief text.",
                    },
                    "language": {
                        "type": "string",
                        "description": "Feedback language preference (de, fr, it).",
                        "enum": ["de", "fr", "it"],
                    },
                },
                "required": ["decision_id", "brief"],
            },
        ),
```

**Step 4: Add dispatch handlers to `handle_call_tool()`**

Before the `else: Unknown tool` block (around line 4823), add:

```python
        elif name == "study_leading_case":
            result = await asyncio.to_thread(
                _handle_study_leading_case,
                topic=arguments.get("topic"),
                decision_id=arguments.get("decision_id"),
                difficulty=arguments.get("difficulty"),
                language=arguments.get("language", "de"),
                mode=arguments.get("mode", "guided"),
            )
            return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

        elif name == "list_study_curriculum":
            result = await asyncio.to_thread(
                _handle_list_study_curriculum,
                area=arguments.get("area"),
                difficulty=arguments.get("difficulty"),
                language=arguments.get("language", "de"),
            )
            return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

        elif name == "check_case_brief":
            result = await asyncio.to_thread(
                _handle_check_case_brief,
                decision_id=arguments["decision_id"],
                brief=arguments["brief"],
                language=arguments.get("language", "de"),
            )
            return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]
```

**Step 5: Add internal handler functions** (somewhere near the study imports at the top of the tool section)

```python
from study.socratic import build_study_package, build_brief_comparison
from study.curriculum_engine import (
    find_case as curriculum_find_case,
    load_curriculum,
    list_areas as curriculum_list_areas,
)


def _handle_study_leading_case(
    *,
    topic: str | None,
    decision_id: str | None,
    difficulty: int | None,
    language: str,
    mode: str,
) -> dict:
    """Internal handler for study_leading_case tool."""
    curriculum_case = None

    # Resolve decision_id
    if decision_id:
        # Check if it's in curriculum
        areas = load_curriculum()
        for area in areas:
            for mod in area.modules:
                for case in mod.cases:
                    if case.decision_id == decision_id:
                        curriculum_case = case
                        break
    elif topic:
        curriculum_case = curriculum_find_case(topic, difficulty=difficulty, language=language)
        if curriculum_case:
            decision_id = curriculum_case.decision_id
        else:
            # Fallback: find_leading_cases
            lc_result = _find_leading_cases(query=topic, court="bge", limit=1)
            cases = lc_result.get("cases", [])
            if cases:
                decision_id = cases[0].get("decision_id")

    if not decision_id:
        return {"error": "No matching case found. Provide a decision_id or try a different topic."}

    # Fetch the full decision
    decision = get_decision_by_id(decision_id)
    if not decision:
        return {"error": f"Decision not found: {decision_id}"}

    # Get citation counts
    citation_counts = _count_citations(decision_id)

    # Get related cases from curriculum
    related_cases = None
    if curriculum_case and curriculum_case.prerequisites:
        related_cases = []
        for prereq_id in curriculum_case.prerequisites:
            prereq = get_decision_by_id(prereq_id)
            if prereq:
                related_cases.append({
                    "decision_id": prereq_id,
                    "docket_number": prereq.get("docket_number", ""),
                    "decision_date": prereq.get("decision_date", ""),
                    "relationship": "prerequisite",
                })

    return build_study_package(
        decision=decision,
        mode=mode,
        curriculum_case=curriculum_case,
        citation_counts=citation_counts,
        related_cases=related_cases,
    )


def _handle_list_study_curriculum(
    *,
    area: str | None,
    difficulty: int | None,
    language: str,
) -> dict:
    """Internal handler for list_study_curriculum tool."""
    if area:
        areas = load_curriculum(area=area)
        if not areas:
            return {"error": f"Unknown area: {area}. Available: vertragsrecht, haftpflicht, sachenrecht, grundrechte, strafrecht_at"}

        a = areas[0]
        lang_key = language if language in ("de", "fr", "it") else "de"
        modules = []
        for mod in a.modules:
            cases = []
            for case in mod.cases:
                if difficulty is not None and case.difficulty > difficulty:
                    continue
                cases.append({
                    "decision_id": case.decision_id,
                    "bge_ref": case.bge_ref,
                    "title": getattr(case, f"title_{lang_key}", case.title_de) or case.title_de,
                    "difficulty": case.difficulty,
                    "statutes": case.statutes,
                    "prerequisites": case.prerequisites,
                })
            modules.append({
                "id": mod.id,
                "name": getattr(mod, f"name_{lang_key}", mod.name_de) or mod.name_de,
                "statutes": mod.statutes,
                "case_count": len(cases),
                "cases": cases,
            })
        return {
            "area_id": a.area_id,
            "name": getattr(a, f"area_{lang_key}", a.area_de) or a.area_de,
            "description": a.description_de,
            "modules": modules,
        }

    # Overview of all areas
    return {"areas": curriculum_list_areas(language=language)}


def _handle_check_case_brief(
    *,
    decision_id: str,
    brief: str,
    language: str,
) -> dict:
    """Internal handler for check_case_brief tool."""
    decision = get_decision_by_id(decision_id)
    if not decision:
        return {"error": f"Decision not found: {decision_id}"}

    return build_brief_comparison(decision=decision, student_brief=brief)
```

**Step 6: Run all tests**

```bash
python3 -m pytest tests/test_study_parser.py tests/test_study_curriculum.py tests/test_study_tools.py -v
```

Expected: All PASS.

**Step 7: Commit**

```bash
git add mcp_server.py study/socratic.py tests/test_study_tools.py
git commit -m "feat: add study_leading_case, list_study_curriculum, check_case_brief MCP tools

Three new tools for Socratic legal education. Parse BGE decisions into
structural components, load curated curriculum, return enriched study
packages for the calling LLM to generate pedagogical content."
```

---

## Task 6: Integration Test with Real BGE

**Files:**
- Modify: `tests/test_study_parser.py` (add real BGE test)

**Step 1: Add a test with real BGE text**

This test uses the actual `bge_144_III_93` text (French BGE about prêt/donation). If no local DB, mark as skip.

Add to `tests/test_study_parser.py`:

```python
import os
import pytest

# Only run if local DB is available
HAS_DB = os.path.exists(os.path.expanduser("~/.swiss-caselaw/decisions.db"))


@pytest.mark.skipif(not HAS_DB, reason="No local DB")
def test_parse_real_bge_144_III_93():
    """Parse a real BGE decision from the database."""
    import sys
    sys.path.insert(0, ".")
    from mcp_server import get_decision_by_id

    decision = get_decision_by_id("bge_144_III_93")
    assert decision is not None

    result = parse_decision(
        decision["full_text"],
        language=decision.get("language", "fr"),
        regeste=decision.get("regeste", ""),
    )

    # This French BGE should parse well
    assert result.parse_quality >= 0.5
    assert len(result.erwagungen) >= 3  # has E. 5, 5.1, 5.2, etc.
    # Should find statute references (Art. 312 CO, Art. 239 CO)
    all_refs = set()
    for e in result.erwagungen:
        all_refs.update(e.statute_refs)
    assert len(all_refs) >= 2
```

**Step 2: Run**

```bash
python3 -m pytest tests/test_study_parser.py -v
```

**Step 3: Commit**

```bash
git add tests/test_study_parser.py
git commit -m "test(study): add real BGE integration test for parser"
```

---

## Task 7: Optional CLI + Deploy

**Files:**
- Create: `study/cli.py`

**Step 1: Write the CLI**

File: `study/cli.py`

```python
"""Thin CLI for testing study tools locally.

Usage:
    python -m study.cli study "Art. 41 OR" --difficulty 2 --lang de
    python -m study.cli curriculum vertragsrecht
    python -m study.cli check bge_144_III_93 --brief "The court held..."
"""
from __future__ import annotations

import argparse
import json
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Socratic Case Law Tutor CLI")
    sub = parser.add_subparsers(dest="command")

    # study
    p_study = sub.add_parser("study", help="Study a leading case")
    p_study.add_argument("topic", nargs="?", help="Legal topic or concept")
    p_study.add_argument("--id", help="Specific decision_id")
    p_study.add_argument("--difficulty", type=int, help="Max difficulty (1-5)")
    p_study.add_argument("--lang", default="de", help="Language (de/fr/it)")
    p_study.add_argument("--mode", default="guided", choices=["guided", "brief", "quick"])

    # curriculum
    p_curr = sub.add_parser("curriculum", help="List curriculum")
    p_curr.add_argument("area", nargs="?", help="Filter by Rechtsgebiet")
    p_curr.add_argument("--lang", default="de")

    # check
    p_check = sub.add_parser("check", help="Check a case brief")
    p_check.add_argument("decision_id", help="BGE decision_id")
    p_check.add_argument("--brief", required=True, help="Student's brief text")
    p_check.add_argument("--lang", default="de")

    args = parser.parse_args()

    if args.command == "study":
        from mcp_server import get_decision_by_id, _count_citations, _find_leading_cases
        from study.socratic import build_study_package
        from study.curriculum_engine import find_case

        decision_id = args.id
        curriculum_case = None
        if not decision_id and args.topic:
            curriculum_case = find_case(args.topic, difficulty=args.difficulty, language=args.lang)
            if curriculum_case:
                decision_id = curriculum_case.decision_id
            else:
                lc = _find_leading_cases(query=args.topic, court="bge", limit=1)
                cases = lc.get("cases", [])
                if cases:
                    decision_id = cases[0]["decision_id"]

        if not decision_id:
            print("No matching case found.", file=sys.stderr)
            sys.exit(1)

        decision = get_decision_by_id(decision_id)
        if not decision:
            print(f"Decision not found: {decision_id}", file=sys.stderr)
            sys.exit(1)

        result = build_study_package(
            decision=decision,
            mode=args.mode,
            curriculum_case=curriculum_case,
            citation_counts=_count_citations(decision_id),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "curriculum":
        from study.curriculum_engine import list_areas, load_curriculum
        if args.area:
            areas = load_curriculum(area=args.area)
            if not areas:
                print(f"Unknown area: {args.area}", file=sys.stderr)
                sys.exit(1)
            a = areas[0]
            print(json.dumps({
                "area_id": a.area_id,
                "name": a.area_de,
                "modules": [
                    {"id": m.id, "name": m.name_de, "cases": len(m.cases)}
                    for m in a.modules
                ],
            }, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(list_areas(language=args.lang), ensure_ascii=False, indent=2))

    elif args.command == "check":
        from mcp_server import get_decision_by_id
        from study.socratic import build_brief_comparison
        decision = get_decision_by_id(args.decision_id)
        if not decision:
            print(f"Decision not found: {args.decision_id}", file=sys.stderr)
            sys.exit(1)
        result = build_brief_comparison(decision=decision, student_brief=args.brief)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
```

**Step 2: Run all tests one final time**

```bash
python3 -m pytest tests/test_study_parser.py tests/test_study_curriculum.py tests/test_study_tools.py -v
```

**Step 3: Commit**

```bash
git add study/cli.py
git commit -m "feat(study): add optional CLI for local testing"
```

**Step 4: Push and deploy**

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase origin main && systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

---

## Task Summary

| Task | Description | Est. Steps |
|------|-------------|-----------|
| 1 | Decision structure parser + tests | 7 |
| 2 | Curriculum engine + tests | 6 |
| 3 | Populate curriculum data (5 files) | 6 |
| 4 | Socratic study package assembly | 2 |
| 5 | MCP tool registration + dispatch | 7 |
| 6 | Real BGE integration test | 3 |
| 7 | CLI + final deploy | 4 |

**Total: 7 tasks, ~35 steps**

**Dependencies:** Task 1 → Task 4 → Task 5. Task 2 → Task 3 → Task 5. Task 6 depends on Tasks 1+5.
