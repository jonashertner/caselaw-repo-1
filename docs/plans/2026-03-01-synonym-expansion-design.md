# Synonym Expansion Design

**Goal:** Improve concept-match search quality by expanding `LEGAL_QUERY_EXPANSIONS` with colloquial→legal mappings and adding few-shot examples to the LLM expansion prompt.

**Architecture:** Two changes to `mcp_server.py` only — no schema changes, no VPS rebuilds. (1) Static dict additions target known failing queries immediately (zero latency). (2) Improved LLM prompt covers the long tail at inference time.

**Tech Stack:** Python, existing `LEGAL_QUERY_EXPANSIONS` dict and `EXPANSION_SYSTEM_PROMPT` string in `mcp_server.py`.

---

## Component 1: Static Synonym Dictionary

Extend `LEGAL_QUERY_EXPANSIONS` with ~20 colloquial→legal mappings targeting the failing concept-match benchmark queries. Each entry maps a plain-language German term to 2-3 legal equivalents FTS5 can match.

Target entries:
- `"hundebiss"` → `["Tierhalterhaftung", "OR 56", "Haftpflicht Tier"]`
- `"autounfall"` → `["Haftpflicht", "Kausalzusammenhang", "Fahrlässigkeit"]`
- `"kündigung mietrecht"` → `["Mietzins", "Mietvertrag Kündigung", "OR 271"]`
- `"erbschaft"` → `["Erbrecht", "Pflichtteil", "Nachlassplanung"]`
- `"geschäftsführer haftung"` → `["Organverantwortlichkeit", "OR 754", "Sorgfaltspflicht"]`
- Plus ~15 more covering short/vague queries (Mietrecht, Erbrecht, Steuerbetrug, etc.)

The existing `_expand_query()` function applies these as OR-expanded FTS5 terms — no pipeline changes needed.

## Component 2: LLM Expansion Prompt

Add 3-4 few-shot examples to `EXPANSION_SYSTEM_PROMPT` showing the colloquial→legal translation pattern. Current prompt asks for "3-6 additional search terms, German/French/Italian equivalents, related legal doctrines" without guiding the model toward Swiss legal terminology bridging.

New examples to add:
```
Example: "Hundebiss" → "Tierhalterhaftung, ZGB 56, Haftpflicht Tier"
Example: "Autounfall Schuld" → "Haftpflicht, Kausalzusammenhang, Fahrlässigkeit OR 41"
Example: "Mietrecht" → "Mietvertrag, Kündigung, Mietzins, OR 253"
```

## Scope

- Single file: `mcp_server.py`
- ~40 lines total (dict entries + prompt additions)
- No VPS rebuilds, no schema changes, no new dependencies
- Test: run benchmark locally (or on VPS) before and after; compare MRR@10

## Success Criteria

- MRR@10 ≥ 0.32 (baseline 0.2876, +3pp minimum)
- concept-match category MRR ≥ 0.15 (currently 0.067)
- No regression on statute, FR/IT, or direct-lookup categories
- Latency: no change for static dict path; LLM path already runs
