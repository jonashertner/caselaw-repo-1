# Synonym Expansion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve concept-match search quality by (1) extending `LEGAL_QUERY_EXPANSIONS` with colloquial→legal mappings and (2) adding few-shot examples to the LLM expansion prompt.

**Architecture:** Two changes to `mcp_server.py` only — no schema changes, no VPS rebuilds. Static dict additions target known failing queries at zero latency; improved LLM prompt covers the long tail at inference time.

**Tech Stack:** Python, `mcp_server.py:LEGAL_QUERY_EXPANSIONS` (dict[str, tuple[str,...]], line 227), `mcp_server.py:EXPANSION_SYSTEM_PROMPT` (str, line 158).

---

## Context for the implementer

The benchmark has 50 queries in `benchmarks/search_relevance_golden.json`. The worst category is concept-match (MRR=0.067) — queries use plain German ("Hundebiss", "Autounfall") but the relevant decisions use legal terminology ("Tierhalterhaftung", "Haftpflicht"). FTS5 never finds them because no tokens overlap.

`LEGAL_QUERY_EXPANSIONS` at line 227 of `mcp_server.py` maps normalized token strings → tuple of expansion terms. The function `_get_query_expansions(term)` at line 2721 looks up single tokens from this dict and OR-expands the FTS5 query. Current entries are multilingual equivalents (de/fr/it), but none bridge colloquial→legal.

`EXPANSION_SYSTEM_PROMPT` at line 158 is a system prompt used when calling Claude Haiku for LLM-based query expansion. Currently it asks for "3-6 additional search terms" but doesn't guide the model toward Swiss legal terminology translation.

Benchmark baseline (cross-encoder enabled, 50 queries):
- MRR@10=0.2876, Recall@10=0.3950, nDCG@10=0.4309
- concept-match MRR=0.067 (worst category)

Run the benchmark with:
```bash
SWISS_CASELAW_CROSS_ENCODER=true SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
python3 benchmarks/run_search_benchmark.py --db /opt/caselaw/repo/output/decisions.db
```

---

## Task 1: Add colloquial→legal entries to `LEGAL_QUERY_EXPANSIONS`

**Files:**
- Modify: `mcp_server.py:227–302` (the `LEGAL_QUERY_EXPANSIONS` dict)
- Test: `tests/test_synonym_expansion.py` (create new)

**Step 1: Write the failing test**

Create `tests/test_synonym_expansion.py`:

```python
"""Tests for colloquial→legal synonym expansion entries."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server import LEGAL_QUERY_EXPANSIONS, _get_query_expansions


def test_hundebiss_expands_to_tierhalterhaftung():
    exps = _get_query_expansions("hundebiss")
    assert any("tierhalterhaft" in e.lower() for e in exps), (
        f"Expected Tierhalterhaftung in expansions, got: {exps}"
    )


def test_autounfall_expands_to_haftpflicht():
    exps = _get_query_expansions("autounfall")
    assert any("haftpflicht" in e.lower() or "kausalzusammenhang" in e.lower() for e in exps), (
        f"Expected Haftpflicht or Kausalzusammenhang, got: {exps}"
    )


def test_erbschaft_expands_to_erbrecht():
    exps = _get_query_expansions("erbschaft")
    assert any("erbrecht" in e.lower() or "pflichtteil" in e.lower() for e in exps), (
        f"Expected Erbrecht or Pflichtteil, got: {exps}"
    )


def test_geschaeftsfuehrer_expands_to_organverantwortlichkeit():
    exps = _get_query_expansions("geschaeftsfuehrer")
    assert any("organverantwortlich" in e.lower() or "sorgfaltspflicht" in e.lower() for e in exps), (
        f"Expected Organverantwortlichkeit or Sorgfaltspflicht, got: {exps}"
    )


def test_mietrecht_kuendigung_expands():
    # "mietrecht" already exists but should expand to Kündigung-related terms
    exps = _get_query_expansions("mietrecht")
    # should include mietvertrag (already there) and kuendigung
    all_exps = " ".join(exps).lower()
    assert "kuendigung" in all_exps or "kundigung" in all_exps or "kuend" in all_exps, (
        f"Expected Kündigung in mietrecht expansions, got: {exps}"
    )


def test_no_existing_entries_removed():
    """Verify pre-existing entries still work."""
    asyl_exps = _get_query_expansions("asyl")
    assert "asile" in asyl_exps or "schutz" in asyl_exps, f"asyl expansions broken: {asyl_exps}"

    haftung_exps = _get_query_expansions("haftung")
    assert len(haftung_exps) >= 2, f"haftung expansions broken: {haftung_exps}"
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/jonashertner/caselaw-repo-1
python -m pytest tests/test_synonym_expansion.py -v
```

Expected: FAIL on `test_hundebiss_expands_to_tierhalterhaftung` and others — those keys don't exist yet.

**Step 3: Add the new entries to `LEGAL_QUERY_EXPANSIONS`**

In `mcp_server.py`, find the closing `}` of `LEGAL_QUERY_EXPANSIONS` at line 302 and add these entries before it (after the `# Employment` section):

```python
    # Colloquial→legal concept bridges
    "hundebiss": ("Tierhalterhaftung", "ZGB 56", "Haftpflicht Tier"),
    "tierhalterhaftung": ("hundebiss", "ZGB 56", "Haftpflicht"),
    "autounfall": ("Haftpflicht", "Kausalzusammenhang", "Fahrlassigkeit"),
    "verkehrsunfall": ("Haftpflicht", "Kausalzusammenhang", "SVG"),
    "erbschaft": ("Erbrecht", "Pflichtteil", "Nachlassplanung"),
    "erbe": ("Erbrecht", "Pflichtteil", "Testament", "letztwillig"),
    "pflichtteil": ("erbschaft", "erbe", "Erbrecht", "ZGB 470"),
    "geschaeftsfuehrer": ("Organverantwortlichkeit", "Sorgfaltspflicht", "OR 754"),
    "organverantwortlichkeit": ("Sorgfaltspflicht", "OR 754", "Aktienrecht"),
    "steuerbetrug": ("Steuerhinterziehung", "Steuerpflicht", "DBG"),
    "steuerhinterziehung": ("Steuerbetrug", "Steuerpflicht", "DBG"),
    "entlassung": ("fristlose Kuendigung", "Arbeitsrecht", "OR 337"),
    "mobbing": ("Persoenlichkeitsschutz", "Arbeitsrecht", "OR 328"),
    "nachbarrecht": ("Immissionen", "Grundeigentum", "ZGB 684"),
    "laermschutz": ("Immissionen", "laerm", "Grundeigentum"),
    "eigentuemer": ("Grundeigentum", "Sachenrecht", "ZGB 641"),
    "mietrecht": ("bail", "locazione", "mietvertrag", "Kuendigung", "Mietzins"),
```

Note: the last entry for `"mietrecht"` replaces the existing one at line 239. Find and replace it:

Existing (line 239):
```python
    "mietrecht": ("bail", "locazione", "mietvertrag"),
```
Replace with:
```python
    "mietrecht": ("bail", "locazione", "mietvertrag", "Kuendigung", "Mietzins"),
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/test_synonym_expansion.py -v
```

Expected: All 6 tests PASS.

**Step 5: Run full test suite to verify no regression**

```bash
python -m pytest tests/ -v --ignore=tests/web -x -q
```

Expected: All existing tests pass (no regressions in curriculum, study, etc.).

**Step 6: Commit**

```bash
git add tests/test_synonym_expansion.py mcp_server.py
git commit -m "feat: add colloquial→legal synonym expansions for concept-match queries"
```

---

## Task 2: Improve LLM expansion prompt with few-shot examples

**Files:**
- Modify: `mcp_server.py:158–166` (the `EXPANSION_SYSTEM_PROMPT` string)
- Test: `tests/test_synonym_expansion.py` (add a new test to the same file)

**Step 1: Write the failing test**

Add this test to `tests/test_synonym_expansion.py`:

```python
def test_expansion_prompt_contains_fewshot_example():
    """Verify the LLM prompt includes at least one colloquial→legal example."""
    from mcp_server import EXPANSION_SYSTEM_PROMPT
    assert "Hundebiss" in EXPANSION_SYSTEM_PROMPT or "hundebiss" in EXPANSION_SYSTEM_PROMPT.lower(), (
        "Prompt should contain a colloquial→legal few-shot example"
    )
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_synonym_expansion.py::test_expansion_prompt_contains_fewshot_example -v
```

Expected: FAIL — "Hundebiss" not in current prompt.

**Step 3: Update `EXPANSION_SYSTEM_PROMPT`**

In `mcp_server.py`, find `EXPANSION_SYSTEM_PROMPT` at line 158 and replace the entire block:

Old:
```python
EXPANSION_SYSTEM_PROMPT = (
    "You are a Swiss legal search assistant. Given a user's search query about "
    "Swiss law, output 3-6 additional search terms that would help find relevant "
    "court decisions. Include:\n"
    "- German/French/Italian equivalents (Swiss legal terminology)\n"
    "- Related legal doctrines and article references (e.g. Art. 56 OR)\n"
    "- Broader/narrower legal concepts\n"
    "Output ONLY the terms, one per line, no numbering or explanation."
)
```

New:
```python
EXPANSION_SYSTEM_PROMPT = (
    "You are a Swiss legal search assistant. Given a user's search query about "
    "Swiss law, output 3-6 additional search terms that would help find relevant "
    "court decisions. Include:\n"
    "- German/French/Italian equivalents (Swiss legal terminology)\n"
    "- Related legal doctrines and article references (e.g. Art. 56 OR)\n"
    "- Broader/narrower legal concepts\n"
    "IMPORTANT: If the query uses colloquial language, translate to the legal "
    "doctrine name. Examples:\n"
    "  'Hundebiss' → Tierhalterhaftung, ZGB 56, Haftpflicht Tier\n"
    "  'Autounfall Schuld' → Haftpflicht, Kausalzusammenhang, Fahrlässigkeit OR 41\n"
    "  'Mietrecht' → Mietvertrag, Kündigung, Mietzins, OR 253\n"
    "  'Erbschaft' → Erbrecht, Pflichtteil, Testament, ZGB 470\n"
    "Output ONLY the terms, one per line, no numbering or explanation."
)
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/test_synonym_expansion.py -v
```

Expected: All 7 tests PASS (6 from Task 1 + new prompt test).

**Step 5: Commit**

```bash
git add mcp_server.py tests/test_synonym_expansion.py
git commit -m "feat: add few-shot examples to LLM expansion prompt for colloquial→legal bridging"
```

---

## Task 3: Deploy and benchmark on VPS

**Files:** None (deploy only)

**Step 1: Push to VPS**

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && git pull --rebase origin main && \
   systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

Expected: No errors, workers restart cleanly.

**Step 2: Run benchmark on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && \
   SWISS_CASELAW_CROSS_ENCODER=true \
   SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db \
   --json-output benchmarks/latest_search_benchmark.json'
```

Expected output (success criteria):
- MRR@10 ≥ 0.32 (was 0.2876, +3pp minimum)
- concept-match MRR ≥ 0.15 (was 0.067)
- Recall@10 ≥ 0.39 (no regression)
- nDCG@10 ≥ 0.43 (no regression)

If MRR@10 < 0.30 (worse than expected): check per-category breakdown and investigate which category regressed.

**Step 3: Pull the updated benchmark JSON**

```bash
scp -i ~/.ssh/caselaw root@46.225.212.40:/opt/caselaw/repo/benchmarks/latest_search_benchmark.json \
  benchmarks/latest_search_benchmark_synonym.json
```

**Step 4: Commit results**

```bash
git add benchmarks/latest_search_benchmark_synonym.json
git commit -m "bench: synonym expansion benchmark results"
```
