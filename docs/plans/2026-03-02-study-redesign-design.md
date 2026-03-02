# Study Tools Redesign — Design

**Goal:** Replace the existing study tools with three focused tools that serve the three core Swiss law student workflows: understanding a case, understanding a doctrine, and practicing exam subsumption.

**Architecture:** Tools return rich legal *data*; Claude (the tutor) generates all pedagogy dynamically. No pre-computed Socratic questions or hypotheticals. Students access via Claude Desktop/claude.ai. The three tools compose naturally — `generate_exam_question` leads to `get_case_brief` for deeper reading; `get_doctrine` leads to `generate_exam_question` for practice.

**Tech Stack:** Python, SQLite FTS5 (`decisions.db`), reference graph (`reference_graph.db`), statute DB (`statutes.db`), existing `_resolve_decision_id()` / `_extract_query_statute_refs()` / `find_leading_cases` infrastructure in `mcp_server.py`.

---

## What changes

**Dropped:**
- `study_leading_case` — pre-computed Socratic content, too static
- `list_study_curriculum` — curriculum browser nobody uses
- `check_case_brief` — rubric checker, secondary value

**Added:**
- `get_case_brief` — any case reference → structured case data
- `get_doctrine` — statute article or legal concept → ranked cases + doctrine timeline
- `generate_exam_question` — topic → real BGE fact pattern + hidden analysis

---

## Tool 1: `get_case_brief`

**Input:** any case reference — "BGE 133 III 121", "133 III 121", decision_id, or docket number

**Output:**
```json
{
  "decision_id": "bge_BGE_133_III_121",
  "court": "bger",
  "date": "2007-01-15",
  "language": "de",
  "regeste": "...",
  "sachverhalt": "...",
  "key_erwaegungen": [
    {"number": "3.1", "text": "..."},
    {"number": "4", "text": "..."}
  ],
  "dispositiv": "...",
  "statutes": [
    {"statute_id": "ART.41.OR", "article": "41", "law_code": "OR", "text_excerpt": "..."}
  ],
  "authority": {
    "incoming_citations": 842,
    "outgoing_citations": 23
  },
  "related": {
    "cited_by": [{"decision_id": "...", "bge_ref": "...", "regeste": "..."}],
    "cites": [{"decision_id": "...", "bge_ref": "...", "regeste": "..."}]
  }
}
```

**Implementation:**
1. Resolve case reference via `_resolve_decision_id()` (already handles BGE refs, dockets, decision_ids)
2. Fetch full_text + regeste + metadata from `decisions.db`
3. Extract Sachverhalt: BGEs have consistent `Sachverhalt:` / `A.-` section headers; fall back to first 800 chars for non-BGE
4. Extract key Erwägungen: detect `Erwägungen:` header, extract numbered sections (regex `^\d+\.(\d+\.)?`)
5. Extract Dispositiv: detect `Dispositiv:` / `Aus diesen Gründen` header
6. Fetch statute data from `decision_statutes` JOIN `statutes.db` for top 3 statutes by mention_count
7. Fetch citation counts from `citation_targets` (incoming) and `decision_citations` (outgoing)
8. Fetch top 3 cited_by and top 3 cites from citation graph

---

## Tool 2: `get_doctrine`

**Input:** statute article ("Art. 41 OR", "Art. 8 BV") or legal concept ("culpa in contrahendo", "Tierhalterhaftung")

**Output:**
```json
{
  "query": "Art. 41 OR",
  "statute": {
    "law_code": "OR",
    "article": "41",
    "text_de": "...",
    "text_fr": "...",
    "text_it": "..."
  },
  "leading_cases": [
    {
      "decision_id": "bge_BGE_132_III_379",
      "bge_ref": "BGE 132 III 379",
      "date": "2006-03-10",
      "regeste": "...",
      "incoming_citations": 1243,
      "rule_summary": "Für den Kausalzusammenhang genügt..."
    }
  ],
  "doctrine_timeline": [
    {"year": 1985, "bge_ref": "BGE 111 II 55", "rule_added": "Grundsatz der ..."},
    {"year": 2006, "bge_ref": "BGE 132 III 379", "rule_added": "Präzisierung des ..."}
  ]
}
```

**Implementation — statute path** (detected by `_extract_query_statute_refs()`):
1. Query `decision_statutes` WHERE statute_id matches → get all decision_ids
2. Join with `citation_targets` to get incoming_citation_count per decision
3. Order by incoming_citations DESC, take top 8
4. Fetch regeste for each from `decisions.db`
5. Fetch statute text from `statutes.db` (get_law)
6. Build timeline: same cases sorted by decision_date ASC

**Implementation — concept path** (no statute detected):
1. Run `find_leading_cases(query=concept, limit=8)` — uses existing FTS + graph ranking
2. Same enrichment as above (regeste, citation counts)
3. Same timeline output

**rule_summary:** first sentence of regeste, truncated at 120 chars. No LLM call needed — regestes are already structured as legal rules.

---

## Tool 3: `generate_exam_question`

**Input:** `topic` string — legal area, statute, or concept (e.g. "Haftpflichtrecht", "Art. 41 OR", "Mietrecht")

**Output:**
```json
{
  "fact_pattern": "A. ist Halter eines Hundes...",
  "difficulty": 3,
  "hint": "Prüfen Sie, wer für Schäden durch Tiere haftet.",
  "source_decision_id": "bge_BGE_123_IV_17",
  "analysis": {
    "applicable_statutes": ["ZGB 56", "OR 41"],
    "leading_case": "BGE 123 IV 17",
    "legal_test": "Der Tierhalter haftet...",
    "correct_outcome": "Klage gutgeheissen, Schadenersatz CHF..."
  }
}
```

**Case selection:**
1. Run `find_leading_cases(query=topic, limit=30)` to get candidate pool
2. Filter: must have regeste (≥ 100 chars) + full_text (≥ 2000 chars)
3. Prefer difficulty 2–4 (from curriculum if available, else proxy by text length + citation count)
4. Exclude recently used (caller passes optional `exclude_ids` list)
5. Pick highest-authority case that passes filters

**fact_pattern extraction:**
- BGE: extract Sachverhalt section (text between `Sachverhalt:` and `Erwägungen:` headers), truncate at 800 chars
- Other courts: first 600 chars of full_text
- Strip party names (replace A., B., C. — already anonymized in Swiss decisions)

**hidden analysis:**
- `applicable_statutes`: from `decision_statutes` (top 2-3 by mention_count)
- `leading_case`: the source BGE itself
- `legal_test`: first 150 chars of regeste
- `correct_outcome`: last 150 chars of regeste (dispositiv summary)

**Interaction flow (no extra tool calls needed):**
1. Claude calls `generate_exam_question(topic="Haftpflicht")`
2. Claude presents fact_pattern + hint to student
3. Student writes analysis
4. Claude reveals analysis, compares, gives feedback
5. Student can say "show me the full case" → Claude calls `get_case_brief(source_decision_id)`

---

## What stays unchanged

- `get_decision` — still useful for reading full text of any decision
- `find_leading_cases` — still useful for research
- `search_decisions` — still useful for open-ended search
- `find_citations` — still useful for citation graph exploration
- All curriculum JSON files — repurposed as case selection pool for `generate_exam_question`

---

## Success criteria

- A student can type "explain BGE 133 III 121" and get a structured brief Claude can teach from
- A student can type "what are the leading cases on Art. 41 OR" and get the doctrine in 5 seconds
- A student can type "give me a practice case on Haftpflicht" and get a real Fallbearbeitung with feedback
- No stored Socratic questions in the codebase
