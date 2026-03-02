# Study Tools Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the existing `study_leading_case`, `list_study_curriculum`, and `check_case_brief` MCP tools with three focused tools — `get_case_brief`, `get_doctrine`, and `generate_exam_question` — that return rich legal data for Claude to use as a tutor.

**Architecture:** All three new tools are handler functions in `mcp_server.py` that compose existing helpers (`get_decision_by_id`, `_find_leading_cases`, `_count_citations`, `_get_graph_conn`, `_get_statutes_conn`, `_resolve_decision_id`, `_extract_query_statute_refs`). No new DB schemas. The old tools are removed from the tool registry and their handler functions are deleted. The curriculum JSON files are kept and reused as a case pool for `generate_exam_question`.

**Tech Stack:** Python, SQLite (`decisions.db`, `reference_graph.db`, `statutes.db`), existing `mcp_server.py` helpers, `study/curriculum_engine.py` for case pool access.

---

## Context for the implementer

### Key files
- `mcp_server.py` — all tool handlers and registration live here. The tool registry is at the bottom (~line 6100+), each tool is a `Tool(name=..., description=..., inputSchema=...)` object in a list.
- `study/curriculum_engine.py` — `load_curriculum()` returns `list[CurriculumArea]`, each with `.modules[].cases[]`. Each `CurriculumCase` has: `decision_id`, `bge_ref`, `difficulty`, `statutes`, `area_id`, `module_id`, `significance_de`.
- `study/socratic.py` — contains `build_study_package()` and `build_brief_comparison()`. These will be unused after the redesign but keep the file (other code may import it).

### Key existing helpers to reuse
- `get_decision_by_id(decision_id: str) -> dict | None` (line 3213) — fetches full decision from FTS5 DB
- `_resolve_decision_id(decision_id: str) -> str` (line 1854) — resolves any reference format to stored decision_id
- `_count_citations(decision_id: str) -> tuple[int, int]` (line 1911) — (incoming, outgoing)
- `_find_leading_cases(*, query, law_code, article, court, limit) -> dict` (line 3283) — returns `{"cases": [...]}`, each case has `decision_id`, `cite_count`, `regeste`, `docket_number`, `decision_date`
- `_get_graph_conn() -> sqlite3.Connection | None` (line 1432) — reference_graph.db read-only connection
- `_get_statutes_conn() -> sqlite3.Connection | None` (line 1479) — statutes.db read-only connection
- `_extract_query_statute_refs(query: str) -> set[str]` (line 1379) — parses "Art. 41 OR" → `{"ART.41.OR"}`
- `QUERY_BGE_PATTERN` (line 512) — regex to detect "BGE 133 III 121" in a string

### Existing tool registration pattern
Find the Tool registration block (search for `name="study_leading_case"` around line 6335) to see the pattern. Each tool is:
```python
Tool(
    name="tool_name",
    description="...",
    inputSchema={"type": "object", "properties": {...}, "required": [...]},
),
```

The `call_tool` dispatcher (search for `elif name == "study_leading_case"`) routes to the handler. Add new cases there.

### BGE section extraction
BGEs (German) have a consistent structure in `full_text`:
- Facts: starts with `Sachverhalt:` or `A.-` heading
- Reasoning: starts with `Erwägungen:` or `Das Bundesgericht zieht in Erwägung:`
- Holding: starts with `Dispositiv:` or `Aus diesen Gründen:`

For non-BGE decisions the structure is less consistent — fall back to slicing full_text.

---

## Task 1: Add `_handle_get_case_brief` handler

**Files:**
- Modify: `mcp_server.py` — add handler function near line 5106 (after existing study handlers)
- Create: `tests/test_study_redesign.py`

### Step 1: Write the failing test

Create `tests/test_study_redesign.py`:

```python
"""Tests for the redesigned study tools: get_case_brief, get_doctrine, generate_exam_question."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── get_case_brief ────────────────────────────────────────────────────────────

def test_get_case_brief_returns_required_keys():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result, f"Unexpected error: {result.get('error')}"
    for key in ("decision_id", "regeste", "authority", "statutes"):
        assert key in result, f"Missing key: {key}"


def test_get_case_brief_authority_has_incoming():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert "incoming_citations" in result["authority"]
    assert isinstance(result["authority"]["incoming_citations"], int)


def test_get_case_brief_statutes_list():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert isinstance(result["statutes"], list)


def test_get_case_brief_unknown_case_returns_error():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 999 IX 999")
    assert "error" in result


def test_get_case_brief_related_structure():
    from mcp_server import _handle_get_case_brief
    result = _handle_get_case_brief(case="BGE 133 III 121")
    assert "error" not in result
    assert "related" in result
    assert "cited_by" in result["related"]
    assert "cites" in result["related"]
```

### Step 2: Run test to verify it fails

```bash
cd /Users/jonashertner/caselaw-repo-1
python -m pytest tests/test_study_redesign.py -v
```

Expected: `ImportError` or `AttributeError` — `_handle_get_case_brief` doesn't exist yet.

### Step 3: Implement `_handle_get_case_brief`

Add this function in `mcp_server.py` after the existing `_handle_check_case_brief` function (~line 5246):

```python
def _handle_get_case_brief(*, case: str) -> dict:
    """Handler for get_case_brief tool.

    Accepts any case reference: BGE ref ("BGE 133 III 121", "133 III 121"),
    decision_id, or docket number. Returns structured case data for Claude
    to use as a tutor — facts, reasoning, statutes, authority, related cases.
    """
    if not case or not case.strip():
        return {"error": "Provide a case reference (BGE ref, decision_id, or docket number)."}

    # Resolve to a stored decision_id
    resolved_id = _resolve_decision_id(case.strip())
    decision = get_decision_by_id(resolved_id)
    if not decision:
        return {"error": f"Case not found: {case!r}. Try a BGE reference like 'BGE 133 III 121'."}

    decision_id = decision.get("decision_id", resolved_id)
    full_text = decision.get("full_text") or ""
    regeste = decision.get("regeste") or ""

    # Extract Sachverhalt (facts section)
    sachverhalt = _extract_section(
        full_text,
        start_patterns=[r"^Sachverhalt\s*:", r"^A\.\s*[-–]", r"^Faits\s*:"],
        end_patterns=[r"^Erwägungen\s*:", r"^Considérant\s*", r"^Das Bundesgericht"],
        fallback_chars=800,
    )

    # Extract key Erwägungen (numbered reasoning sections)
    key_erwaegungen = _extract_erwaegungen(full_text)

    # Extract Dispositiv (holding)
    dispositiv = _extract_section(
        full_text,
        start_patterns=[r"^Dispositiv\s*:", r"^Aus diesen Gründen", r"^Par ces motifs"],
        end_patterns=[],
        fallback_chars=0,
        from_end=True,
    )

    # Statutes from reference graph
    statutes = _get_decision_statutes(decision_id, limit=5)

    # Authority (citation counts)
    incoming, outgoing = _count_citations(decision_id)

    # Related cases (cited_by and cites) — top 3 each
    related = _get_related_cases(decision_id, limit=3)

    return {
        "decision_id": decision_id,
        "bge_ref": decision.get("docket_number", ""),
        "court": decision.get("court", ""),
        "date": decision.get("decision_date", ""),
        "language": decision.get("language", ""),
        "regeste": regeste,
        "sachverhalt": sachverhalt,
        "key_erwaegungen": key_erwaegungen,
        "dispositiv": dispositiv,
        "statutes": statutes,
        "authority": {
            "incoming_citations": incoming,
            "outgoing_citations": outgoing,
        },
        "related": related,
    }


def _extract_section(
    text: str,
    *,
    start_patterns: list[str],
    end_patterns: list[str],
    fallback_chars: int = 800,
    from_end: bool = False,
) -> str:
    """Extract a named section from decision full_text using header patterns.

    Tries each start_pattern in order. Extracts text until an end_pattern
    is found or until 1200 chars. Returns fallback_chars from start/end if
    no pattern matches.
    """
    import re as _re
    lines = text.splitlines()
    start_idx = None

    for i, line in enumerate(lines):
        for pat in start_patterns:
            if _re.match(pat, line.strip(), _re.IGNORECASE):
                start_idx = i + 1  # skip the header line itself
                break
        if start_idx is not None:
            break

    if start_idx is None:
        if fallback_chars <= 0:
            return ""
        if from_end:
            return text[-fallback_chars:].strip()
        return text[:fallback_chars].strip()

    # Collect until end pattern or 1200 chars
    collected: list[str] = []
    total_chars = 0
    for line in lines[start_idx:]:
        if end_patterns:
            for pat in end_patterns:
                if _re.match(pat, line.strip(), _re.IGNORECASE):
                    return "\n".join(collected).strip()
        collected.append(line)
        total_chars += len(line)
        if total_chars >= 1200:
            break

    return "\n".join(collected).strip()


def _extract_erwaegungen(full_text: str) -> list[dict]:
    """Extract numbered Erwägungen sections from a BGE full_text.

    Returns list of {"number": "3.1", "text": "..."} for up to 5 sections.
    """
    import re as _re
    # Find the Erwägungen block
    erw_start = None
    lines = full_text.splitlines()
    for i, line in enumerate(lines):
        if _re.match(r"^Erwägungen\s*:", line.strip(), _re.IGNORECASE) or \
           _re.match(r"^Das Bundesgericht zieht in Erwägung", line.strip(), _re.IGNORECASE) or \
           _re.match(r"^Considérant\s*", line.strip(), _re.IGNORECASE):
            erw_start = i + 1
            break

    if erw_start is None:
        return []

    # Numbered section pattern: "3.", "3.1", "E. 4" etc.
    section_pat = _re.compile(r"^(\d+(?:\.\d+)?)\.\s+\S")
    sections: list[dict] = []
    current_num: str | None = None
    current_lines: list[str] = []

    for line in lines[erw_start:]:
        m = section_pat.match(line.strip())
        if m:
            if current_num is not None:
                text = " ".join(current_lines).strip()
                sections.append({"number": current_num, "text": text[:400]})
                if len(sections) >= 5:
                    break
            current_num = m.group(1)
            current_lines = [line.strip()]
        elif current_num is not None:
            current_lines.append(line.strip())

    if current_num is not None and len(sections) < 5:
        text = " ".join(current_lines).strip()
        sections.append({"number": current_num, "text": text[:400]})

    return sections


def _get_decision_statutes(decision_id: str, *, limit: int = 5) -> list[dict]:
    """Return top statutes cited by a decision, with Fedlex text if available."""
    conn = _get_graph_conn()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT ds.statute_id, s.law_code, s.article, s.paragraph,
                   ds.mention_count
            FROM decision_statutes ds
            JOIN statutes s ON s.statute_id = ds.statute_id
            WHERE ds.decision_id = ?
            ORDER BY ds.mention_count DESC
            LIMIT ?
            """,
            (decision_id, limit),
        ).fetchall()
    except Exception:
        return []
    finally:
        conn.close()

    statutes = []
    for row in rows:
        entry: dict = {
            "statute_id": row["statute_id"],
            "law_code": row["law_code"],
            "article": row["article"],
            "mention_count": row["mention_count"],
            "text_excerpt": "",
        }
        # Try to fetch Fedlex article text
        stat_conn = _get_statutes_conn()
        if stat_conn:
            try:
                art_row = stat_conn.execute(
                    """
                    SELECT text_de FROM articles
                    WHERE sr_number IN (
                        SELECT sr_number FROM laws WHERE UPPER(abbr_de) = UPPER(?)
                    )
                    AND article_number = ?
                    LIMIT 1
                    """,
                    (row["law_code"], row["article"]),
                ).fetchone()
                if art_row:
                    entry["text_excerpt"] = (art_row["text_de"] or "")[:300]
            except Exception:
                pass
            finally:
                stat_conn.close()
        statutes.append(entry)
    return statutes


def _get_related_cases(decision_id: str, *, limit: int = 3) -> dict:
    """Return top cited_by and cites cases with their regeste."""
    # cited_by: decisions that cite this one
    conn = _get_graph_conn()
    cited_by: list[dict] = []
    cites: list[dict] = []

    if conn is not None:
        try:
            # cited_by: top incoming citations by confidence
            if _sqlite_has_table(conn, "citation_targets"):
                rows = conn.execute(
                    """
                    SELECT ct.source_decision_id, ct.confidence_score
                    FROM citation_targets ct
                    WHERE ct.target_decision_id = ?
                    ORDER BY ct.confidence_score DESC
                    LIMIT ?
                    """,
                    (decision_id, limit),
                ).fetchall()
                cited_by_ids = [r["source_decision_id"] for r in rows]

            # cites: outgoing citations
            rows = conn.execute(
                """
                SELECT ct.target_decision_id
                FROM decision_citations dc
                JOIN citation_targets ct ON ct.source_decision_id = dc.source_decision_id
                    AND ct.target_ref = dc.target_ref
                WHERE dc.source_decision_id = ?
                  AND ct.target_decision_id IS NOT NULL
                ORDER BY dc.mention_count DESC
                LIMIT ?
                """,
                (decision_id, limit),
            ).fetchall()
            cites_ids = [r["target_decision_id"] for r in rows]
        except Exception:
            cited_by_ids = []
            cites_ids = []
        finally:
            conn.close()

        # Fetch regeste for each
        fts_conn = get_db()
        try:
            for did in cited_by_ids:
                row = fts_conn.execute(
                    "SELECT decision_id, docket_number, regeste FROM decisions WHERE decision_id = ?",
                    (did,),
                ).fetchone()
                if row:
                    cited_by.append({
                        "decision_id": row["decision_id"],
                        "bge_ref": row["docket_number"],
                        "regeste": (row["regeste"] or "")[:200],
                    })
            for did in cites_ids:
                row = fts_conn.execute(
                    "SELECT decision_id, docket_number, regeste FROM decisions WHERE decision_id = ?",
                    (did,),
                ).fetchone()
                if row:
                    cites.append({
                        "decision_id": row["decision_id"],
                        "bge_ref": row["docket_number"],
                        "regeste": (row["regeste"] or "")[:200],
                    })
        finally:
            fts_conn.close()

    return {"cited_by": cited_by, "cites": cites}
```

### Step 4: Run test to verify it passes

```bash
python -m pytest tests/test_study_redesign.py::test_get_case_brief_returns_required_keys \
  tests/test_study_redesign.py::test_get_case_brief_authority_has_incoming \
  tests/test_study_redesign.py::test_get_case_brief_statutes_list \
  tests/test_study_redesign.py::test_get_case_brief_unknown_case_returns_error \
  tests/test_study_redesign.py::test_get_case_brief_related_structure \
  -v
```

Expected: All 5 PASS.

### Step 5: Commit

```bash
git add mcp_server.py tests/test_study_redesign.py
git commit -m "feat: add _handle_get_case_brief with section extraction helpers"
```

---

## Task 2: Add `_handle_get_doctrine` handler

**Files:**
- Modify: `mcp_server.py` — add handler after `_handle_get_case_brief`
- Modify: `tests/test_study_redesign.py` — add tests

### Step 1: Write the failing tests

Add to `tests/test_study_redesign.py`:

```python
# ── get_doctrine ──────────────────────────────────────────────────────────────

def test_get_doctrine_statute_path_returns_leading_cases():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result, f"Unexpected error: {result.get('error')}"
    assert "leading_cases" in result
    assert len(result["leading_cases"]) > 0


def test_get_doctrine_statute_path_returns_statute_text():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    # statute field present (may be empty if statutes.db unavailable)
    assert "statute" in result


def test_get_doctrine_concept_path_works():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Tierhalterhaftung")
    assert "error" not in result
    assert "leading_cases" in result
    assert len(result["leading_cases"]) > 0


def test_get_doctrine_has_doctrine_timeline():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    assert "doctrine_timeline" in result
    assert isinstance(result["doctrine_timeline"], list)


def test_get_doctrine_leading_cases_have_required_fields():
    from mcp_server import _handle_get_doctrine
    result = _handle_get_doctrine(query="Art. 41 OR")
    assert "error" not in result
    for case in result["leading_cases"]:
        assert "decision_id" in case
        assert "incoming_citations" in case
        assert "rule_summary" in case
```

### Step 2: Run tests to verify they fail

```bash
python -m pytest tests/test_study_redesign.py -k "doctrine" -v
```

Expected: `AttributeError` — `_handle_get_doctrine` not defined.

### Step 3: Implement `_handle_get_doctrine`

Add after `_handle_get_case_brief` in `mcp_server.py`:

```python
def _handle_get_doctrine(*, query: str) -> dict:
    """Handler for get_doctrine tool.

    Accepts a statute reference ("Art. 41 OR") or legal concept
    ("Tierhalterhaftung"). Returns statute text + top authority-ranked BGEs
    + the rule each establishes + doctrine evolution timeline.
    """
    if not query or not query.strip():
        return {"error": "Provide a statute reference or legal concept."}

    q = query.strip()

    # Detect statute reference
    statute_refs = _extract_query_statute_refs(q)
    statute_info: dict = {}
    leading_cases: list[dict] = []

    if statute_refs:
        # Statute path: pick the first parsed ref
        ref = next(iter(statute_refs))
        # ref format: "ART.41.OR"
        parts = ref.split(".")
        if len(parts) >= 3:
            article = parts[1]
            law_code = parts[2]
        else:
            article = ""
            law_code = ""

        # Fetch statute text from statutes.db
        if article and law_code:
            statute_info = _fetch_statute_text(law_code=law_code, article=article)

        # Find leading cases via graph (statute path)
        lc_result = _find_leading_cases(
            law_code=law_code, article=article, court=None, limit=8
        )
        raw_cases = lc_result.get("cases", [])
    else:
        # Concept path: FTS search
        lc_result = _find_leading_cases(query=q, limit=8)
        raw_cases = lc_result.get("cases", [])

    # Enrich each case with authority count and rule_summary
    for case in raw_cases:
        did = case.get("decision_id", "")
        incoming, _ = _count_citations(did)
        regeste = case.get("regeste") or ""
        # rule_summary: first sentence of regeste, max 150 chars
        first_sentence = regeste.split(".")[0].strip() if regeste else ""
        leading_cases.append({
            "decision_id": did,
            "bge_ref": case.get("docket_number", ""),
            "date": case.get("decision_date", ""),
            "regeste": regeste[:300],
            "incoming_citations": incoming if incoming else case.get("cite_count", 0),
            "rule_summary": first_sentence[:150],
        })

    # Sort by incoming_citations desc (already sorted but re-sort after enrichment)
    leading_cases.sort(key=lambda c: c["incoming_citations"], reverse=True)

    # Doctrine timeline: same cases sorted chronologically
    timeline = sorted(
        [
            {
                "year": (c["date"] or "")[:4],
                "bge_ref": c["bge_ref"],
                "rule_added": c["rule_summary"],
            }
            for c in leading_cases
            if c.get("date")
        ],
        key=lambda x: x["year"],
    )

    return {
        "query": q,
        "statute": statute_info,
        "leading_cases": leading_cases,
        "doctrine_timeline": timeline,
    }


def _fetch_statute_text(*, law_code: str, article: str) -> dict:
    """Fetch statute article text from statutes.db. Returns {} if unavailable."""
    conn = _get_statutes_conn()
    if conn is None:
        return {}
    try:
        # Find SR number for the law abbreviation
        law_row = conn.execute(
            "SELECT sr_number FROM laws WHERE UPPER(abbr_de) = UPPER(?) "
            "OR UPPER(abbr_fr) = UPPER(?) OR UPPER(abbr_it) = UPPER(?) LIMIT 1",
            (law_code, law_code, law_code),
        ).fetchone()
        if not law_row:
            return {"law_code": law_code, "article": article}
        sr = law_row["sr_number"]

        art_row = conn.execute(
            "SELECT article_number, text_de, text_fr, text_it FROM articles "
            "WHERE sr_number = ? AND article_number = ? LIMIT 1",
            (sr, article),
        ).fetchone()
        if not art_row:
            return {"law_code": law_code, "article": article, "sr_number": sr}

        return {
            "law_code": law_code,
            "article": article,
            "sr_number": sr,
            "text_de": (art_row["text_de"] or "")[:600],
            "text_fr": (art_row["text_fr"] or "")[:600],
            "text_it": (art_row["text_it"] or "")[:300],
        }
    except Exception:
        return {"law_code": law_code, "article": article}
    finally:
        conn.close()
```

### Step 4: Run tests to verify they pass

```bash
python -m pytest tests/test_study_redesign.py -k "doctrine" -v
```

Expected: All 5 PASS.

### Step 5: Run full test suite

```bash
python -m pytest tests/ --ignore=tests/web -x -q
```

Expected: All pass.

### Step 6: Commit

```bash
git add mcp_server.py tests/test_study_redesign.py
git commit -m "feat: add _handle_get_doctrine with statute and concept paths"
```

---

## Task 3: Add `_handle_generate_exam_question` handler

**Files:**
- Modify: `mcp_server.py` — add handler after `_handle_get_doctrine`
- Modify: `tests/test_study_redesign.py` — add tests

### Step 1: Write the failing tests

Add to `tests/test_study_redesign.py`:

```python
# ── generate_exam_question ────────────────────────────────────────────────────

def test_generate_exam_question_returns_fact_pattern():
    from mcp_server import _handle_generate_exam_question
    result = _handle_generate_exam_question(topic="Haftpflichtrecht")
    assert "error" not in result, f"Unexpected error: {result.get('error')}"
    assert "fact_pattern" in result
    assert len(result["fact_pattern"]) > 50


def test_generate_exam_question_has_hidden_analysis():
    from mcp_server import _handle_generate_exam_question
    result = _handle_generate_exam_question(topic="Haftpflichtrecht")
    assert "error" not in result
    assert "analysis" in result
    analysis = result["analysis"]
    assert "applicable_statutes" in analysis
    assert "leading_case" in analysis
    assert "legal_test" in analysis


def test_generate_exam_question_has_difficulty():
    from mcp_server import _handle_generate_exam_question
    result = _handle_generate_exam_question(topic="Vertragsrecht")
    assert "error" not in result
    assert "difficulty" in result
    assert 1 <= result["difficulty"] <= 5


def test_generate_exam_question_has_hint():
    from mcp_server import _handle_generate_exam_question
    result = _handle_generate_exam_question(topic="Mietrecht")
    assert "error" not in result
    assert "hint" in result
    assert isinstance(result["hint"], str)


def test_generate_exam_question_exclude_ids():
    """Verify exclude_ids prevents returning the same case twice."""
    from mcp_server import _handle_generate_exam_question
    result1 = _handle_generate_exam_question(topic="Haftpflichtrecht")
    assert "error" not in result1
    source_id = result1["source_decision_id"]
    result2 = _handle_generate_exam_question(
        topic="Haftpflichtrecht", exclude_ids=[source_id]
    )
    # Either returns a different case or an error if no alternatives
    if "error" not in result2:
        assert result2["source_decision_id"] != source_id
```

### Step 2: Run tests to verify they fail

```bash
python -m pytest tests/test_study_redesign.py -k "exam_question" -v
```

Expected: `AttributeError` — function not defined.

### Step 3: Implement `_handle_generate_exam_question`

Add after `_handle_get_doctrine`:

```python
def _handle_generate_exam_question(
    *, topic: str, exclude_ids: list[str] | None = None
) -> dict:
    """Handler for generate_exam_question tool.

    Returns a real BGE fact pattern as a Fallbearbeitung exercise.
    The analysis is included but Claude should reveal it only after
    the student submits their answer.
    """
    if not topic or not topic.strip():
        return {"error": "Provide a legal topic, area, or statute reference."}

    exclude = set(exclude_ids or [])

    # Build candidate pool: top-30 from find_leading_cases, filtered
    lc_result = _find_leading_cases(query=topic.strip(), limit=30)
    candidates = lc_result.get("cases", [])

    # Also check curriculum for topic-matching cases
    try:
        curriculum_cases = _get_curriculum_cases_for_topic(topic)
        curriculum_ids = {c["decision_id"] for c in curriculum_cases}
    except Exception:
        curriculum_ids = set()

    # Filter and score candidates
    selected = None
    for case in candidates:
        did = case.get("decision_id", "")
        if did in exclude:
            continue
        # Must have usable text
        decision = get_decision_by_id(did)
        if not decision:
            continue
        full_text = decision.get("full_text") or ""
        regeste = decision.get("regeste") or ""
        if len(full_text) < 1000 or len(regeste) < 50:
            continue
        selected = (case, decision)
        break

    if selected is None:
        return {"error": f"No suitable case found for topic '{topic}'. Try a broader topic."}

    case_meta, decision = selected
    decision_id = decision.get("decision_id", "")
    full_text = decision.get("full_text") or ""
    regeste = decision.get("regeste") or ""

    # Extract fact pattern
    fact_pattern = _extract_section(
        full_text,
        start_patterns=[r"^Sachverhalt\s*:", r"^A\.\s*[-–]", r"^Faits\s*:"],
        end_patterns=[r"^Erwägungen\s*:", r"^Considérant\s*", r"^Das Bundesgericht"],
        fallback_chars=600,
    )
    if not fact_pattern:
        fact_pattern = full_text[:600].strip()

    # Difficulty: prefer curriculum difficulty, else proxy from citation count
    difficulty = 3  # default
    if decision_id in curriculum_ids:
        for c in curriculum_cases:
            if c["decision_id"] == decision_id:
                difficulty = c.get("difficulty", 3)
                break

    # Statutes for hidden analysis
    statutes = _get_decision_statutes(decision_id, limit=3)
    statute_labels = [
        f"{s['law_code']} {s['article']}" for s in statutes if s.get("law_code")
    ]

    # Legal test and outcome from regeste
    regeste_parts = regeste.split(".")
    legal_test = regeste_parts[0].strip()[:150] if regeste_parts else regeste[:150]
    correct_outcome = regeste_parts[-2].strip()[:150] if len(regeste_parts) > 2 else regeste[-150:].strip()

    # Hint: one-line pointer to the relevant doctrine area
    hint = f"Prüfen Sie, welches Rechtsgebiet auf den Sachverhalt anwendbar ist."

    return {
        "fact_pattern": fact_pattern,
        "difficulty": difficulty,
        "hint": hint,
        "source_decision_id": decision_id,
        "analysis": {
            "applicable_statutes": statute_labels,
            "leading_case": decision.get("docket_number", decision_id),
            "legal_test": legal_test,
            "correct_outcome": correct_outcome,
        },
    }


def _get_curriculum_cases_for_topic(topic: str) -> list[dict]:
    """Return curriculum cases matching topic (area_id or keyword search)."""
    from study.curriculum_engine import load_curriculum
    areas = load_curriculum()
    results = []
    topic_lower = topic.lower()
    for area in areas:
        if topic_lower in area.area_id.lower() or topic_lower in (area.area_de or "").lower():
            for mod in area.modules:
                for case in mod.cases:
                    results.append({
                        "decision_id": case.decision_id,
                        "difficulty": case.difficulty,
                        "area_id": area.area_id,
                    })
    return results
```

### Step 4: Run tests to verify they pass

```bash
python -m pytest tests/test_study_redesign.py -k "exam_question" -v
```

Expected: All 5 PASS.

### Step 5: Commit

```bash
git add mcp_server.py tests/test_study_redesign.py
git commit -m "feat: add _handle_generate_exam_question using real BGE fact patterns"
```

---

## Task 4: Register the three new tools and remove the old ones

**Files:**
- Modify: `mcp_server.py` — tool registry (~line 6335) and `call_tool` dispatcher

### Step 1: Write the failing tests

Add to `tests/test_study_redesign.py`:

```python
# ── Tool registration ─────────────────────────────────────────────────────────

def test_new_tools_registered():
    """Verify new tools appear in the tool list."""
    from mcp_server import _list_tools
    tool_names = {t.name for t in _list_tools()}
    assert "get_case_brief" in tool_names
    assert "get_doctrine" in tool_names
    assert "generate_exam_question" in tool_names


def test_old_tools_removed():
    """Verify old study tools are gone from the tool list."""
    from mcp_server import _list_tools
    tool_names = {t.name for t in _list_tools()}
    assert "study_leading_case" not in tool_names
    assert "list_study_curriculum" not in tool_names
    assert "check_case_brief" not in tool_names
```

Run to see them fail:
```bash
python -m pytest tests/test_study_redesign.py -k "registered or removed" -v
```

### Step 2: Find the tool registry

Search `mcp_server.py` for `def _list_tools` or the list building function. The tools list is built in a function that returns `list[Tool]`. Find it with:
```bash
grep -n "def _list_tools\|list_tools\|Tool(" mcp_server.py | head -20
```

### Step 3: Remove old tools from registry

Find and delete (or comment out) the three `Tool(name="study_leading_case", ...)`, `Tool(name="list_study_curriculum", ...)`, and `Tool(name="check_case_brief", ...)` blocks.

### Step 4: Add three new Tool registrations

Add these three Tool entries in the same location:

```python
Tool(
    name="get_case_brief",
    description=(
        "Get a structured case brief for any Swiss court decision. "
        "Accepts any reference format: BGE reference ('BGE 133 III 121', '133 III 121'), "
        "decision_id, or docket number. Returns: regeste (official headnote), "
        "Sachverhalt (facts), key Erwägungen (reasoning excerpts with paragraph numbers), "
        "Dispositiv (holding), applicable statutes with Fedlex text, citation authority "
        "(how often this case is cited), and related cases (what it cites, what cites it). "
        "Use this when a student wants to understand or brief a specific case."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "case": {
                "type": "string",
                "description": (
                    "Any case reference: BGE ref ('BGE 133 III 121', '133 III 121'), "
                    "decision_id ('bge_BGE_133_III_121'), or docket number."
                ),
            },
        },
        "required": ["case"],
    },
),
Tool(
    name="get_doctrine",
    description=(
        "Get the leading cases and doctrine for a Swiss law statute article or legal concept. "
        "Input: statute reference ('Art. 41 OR', 'Art. 8 BV') or legal concept "
        "('Tierhalterhaftung', 'culpa in contrahendo', 'Vertragsfreiheit'). "
        "Returns: statute text (from Fedlex), top 5-8 BGEs ranked by citation authority, "
        "the specific rule each case establishes (from regeste), and a doctrine evolution "
        "timeline showing how the rule developed chronologically. "
        "Use this when a student asks about the leading cases on a statute or doctrine, "
        "or needs to understand how a legal rule developed over time."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Statute article ('Art. 41 OR', 'Art. 8 BV') or legal concept "
                    "('Tierhalterhaftung', 'culpa in contrahendo'). German preferred."
                ),
            },
        },
        "required": ["query"],
    },
),
Tool(
    name="generate_exam_question",
    description=(
        "Generate a Swiss law exam practice question (Fallbearbeitung) based on a real BGE. "
        "Returns a fact pattern (Sachverhalt) from a real court decision and a hidden analysis "
        "(applicable statutes, leading case, legal test, correct outcome). "
        "Workflow: present the fact_pattern and hint to the student, wait for their analysis, "
        "then reveal the analysis field and compare. "
        "The student can then call get_case_brief(source_decision_id) to study the full case. "
        "Pass exclude_ids from previous calls to avoid repeating the same case."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "topic": {
                "type": "string",
                "description": (
                    "Legal area, statute, or concept. Examples: 'Haftpflichtrecht', "
                    "'Art. 41 OR', 'Mietrecht', 'Strafrecht', 'Vertragsrecht'."
                ),
            },
            "exclude_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "decision_ids already used in this session — avoids repetition.",
            },
        },
        "required": ["topic"],
    },
),
```

### Step 5: Add the three new cases to `call_tool` dispatcher

Find `elif name == "study_leading_case":` in the dispatcher and replace the entire `study_leading_case` / `list_study_curriculum` / `check_case_brief` elif blocks with:

```python
elif name == "get_case_brief":
    case = args.get("case", "")
    result = _handle_get_case_brief(case=case)
    return [TextContent(type="text", text=_format_json(result))]

elif name == "get_doctrine":
    query = args.get("query", "")
    result = _handle_get_doctrine(query=query)
    return [TextContent(type="text", text=_format_json(result))]

elif name == "generate_exam_question":
    topic = args.get("topic", "")
    exclude_ids = args.get("exclude_ids", [])
    result = _handle_generate_exam_question(topic=topic, exclude_ids=exclude_ids)
    return [TextContent(type="text", text=_format_json(result))]
```

If `_format_json` doesn't exist, use `json.dumps(result, ensure_ascii=False, indent=2)` instead.

### Step 6: Run all tests

```bash
python -m pytest tests/test_study_redesign.py -v
python -m pytest tests/ --ignore=tests/web -x -q
```

Expected: All pass.

### Step 7: Commit

```bash
git add mcp_server.py
git commit -m "feat: register get_case_brief, get_doctrine, generate_exam_question; remove old study tools"
```

---

## Task 5: Deploy, smoke test, update docs

**Files:**
- Modify: `mcp_server.py` — tool count in description if hardcoded anywhere
- No new files

### Step 1: Push and deploy

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && git pull --rebase origin main && \
   systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

### Step 2: Smoke test the three new tools on VPS

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && \
  SWISS_CASELAW_DIR=/opt/caselaw/repo/output python3 -c "
import json
from mcp_server import _handle_get_case_brief, _handle_get_doctrine, _handle_generate_exam_question

# Test 1: get_case_brief
r = _handle_get_case_brief(case=\"BGE 133 III 121\")
print(\"get_case_brief:\", \"OK\" if \"decision_id\" in r else \"FAIL\", r.get(\"error\",\"\"))

# Test 2: get_doctrine
r = _handle_get_doctrine(query=\"Art. 41 OR\")
print(\"get_doctrine:\", \"OK\" if \"leading_cases\" in r else \"FAIL\", r.get(\"error\",\"\"))

# Test 3: generate_exam_question
r = _handle_generate_exam_question(topic=\"Haftpflichtrecht\")
print(\"generate_exam_question:\", \"OK\" if \"fact_pattern\" in r else \"FAIL\", r.get(\"error\",\"\"))
"'
```

Expected output:
```
get_case_brief: OK
get_doctrine: OK
generate_exam_question: OK
```

### Step 3: Commit deploy confirmation (no code changes needed)

If smoke test passes, no further changes. If errors appear, fix them and commit before closing.
