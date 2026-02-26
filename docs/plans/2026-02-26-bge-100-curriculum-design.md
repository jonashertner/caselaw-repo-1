# Design: BGE-100 Canonical Curriculum

**Date:** 2026-02-26
**Status:** Approved
**Goal:** Identify the 100 most impactful BGE decisions every Swiss law student must know, and build a structured 14-area curriculum around them.

---

## Context

The existing study curriculum has 44 cases across 8 legal areas with rich metadata (Socratic Q&A, hypotheticals, review cards). The task is to expand it to 100 landmark BGEs covering the full Swiss law school canon.

---

## Decisions

| Question | Choice | Rationale |
|----------|--------|-----------|
| Selection methodology | Hybrid (citation graph + canon) | Citation count finds objectively impactful cases; canon filter corrects for age bias and pedagogical value |
| Existing 44 cases | Absorb (review, keep good, replace weak) | Preserves rich existing metadata; replaces any weak picks with stronger BGEs |
| Area coverage | 14 areas (8 existing + 6 new) | Covers the full Swiss law school/bar exam spectrum |
| Implementation | Two-phase (curate list, then batch-enrich metadata) | Separation of concerns: curation is human judgment; metadata generation is automatable |

---

## 14-Area Structure

| # | Area ID | Area (DE) | Modules | Cases |
|---|---------|-----------|---------|-------|
| 1 | `vertragsrecht` | Vertragsrecht (OR AT) | Vertragsschluss & AGB · Auslegung · Willensmängel · Leistungsstörungen | 8 |
| 2 | `haftpflicht` | Haftpflicht | Verschuldenshaftung · Kausalität & Schaden · Kausalhaftungen | 8 |
| 3 | `sachenrecht` | Sachenrecht | Eigentumserwerb · Besitz & Grundbuch · Dienstbarkeiten & Nachbarrecht | 7 |
| 4 | `familienrecht` | Familienrecht | Scheidung & Güterrecht · Kindesrecht & Sorge · Unterhalt | 7 |
| 5 | `arbeitsrecht` | Arbeitsrecht | Kündigung · Arbeitnehmerpflichten · Lohn & Gleichstellung | 7 |
| 6 | `mietrecht` | Mietrecht | Mietzins & Schutz · Kündigung · Mängel & Rückgabe | 6 |
| 7 | `strafrecht_at` | Strafrecht AT | Vorsatz & Fahrlässigkeit · Versuch & Teilnahme · Sanktionen | 8 |
| 8 | `grundrechte` | Grundrechte & Verfassungsrecht | Freiheitsrechte · Rechtsgleichheit · Verfahrensgarantien | 7 |
| 9 | `strafrecht_bt` | Strafrecht BT | Vermögensdelikte · Körper- & Sexualdelikte · Sonderdelikte | 7 |
| 10 | `erbrecht` | Erbrecht | Pflichtteil & Herabsetzung · Testamentsrecht · Erbteilung & Ausgleichung | 7 |
| 11 | `gesellschaftsrecht` | Gesellschaftsrecht | AG-Recht & Generalversammlung · Verantwortlichkeit · GmbH & Konzern | 7 |
| 12 | `zivilprozessrecht` | Zivilprozessrecht | Zuständigkeit & Parteien · Beweisrecht · Rechtsmittel (BGG) | 7 |
| 13 | `strafprozessrecht` | Strafprozessrecht | Grundsätze & Unschuldsvermutung · Zwangsmassnahmen · Beweise & Verwertung | 7 |
| 14 | `oeffentliches_prozessrecht` | Öffentliches Prozessrecht | Verwaltungsverfahren (VwVG) · Beschwerde & Legitimation · Kognition (BGG) | 7 |
| | | **Total** | | **100** |

---

## Hybrid Selection Criteria

A case qualifies for the canonical 100 if it meets **at least one** of:

1. **Citation count ≥ 50** in the reference graph (incoming citations from other BGEs/court decisions)
2. **Canon signal**: appears in ≥ 2 major Swiss law casebooks, commentaries (BSK, ZK, BK), or standard law school syllabi
3. **Doctrinal necessity**: tests a rule that students are expected to apply in exams — not just a procedural outcome

**Difficulty grading:**
- 1–2: First-year canon (foundational rules, clean facts)
- 3: Mainstream (standard exam level)
- 4–5: Advanced (nuanced doctrine, seminar-level analysis)

**Absorb logic for existing 44:** each case is evaluated against the above criteria. Cases that pass are carried forward. Cases that fail (e.g. narrowly procedural, low pedagogical value, superseded by a better BGE) are replaced by a stronger decision in the same doctrinal slot within the same module.

---

## Phase 1: Curate the Canonical List

**Output:** 14 curriculum JSON files (`study/curriculum/<area_id>.json`)

- 8 existing files updated (absorb review + expansion to target count)
- 6 new files created: `strafrecht_bt.json`, `erbrecht.json`, `gesellschaftsrecht.json`, `zivilprozessrecht.json`, `strafprozessrecht.json`, `oeffentliches_prozessrecht.json`

**Thin metadata per case (Phase 1):**
```json
{
  "decision_id": "",
  "bge_ref": "BGE 135 III 1",
  "actual_language": "de",
  "title_de": "...",
  "title_fr": "...",
  "title_it": "...",
  "concepts_de": ["..."],
  "statutes": ["Art. X OR"],
  "difficulty": 3,
  "prerequisites": [],
  "significance_de": "...",
  "key_erwagungen": [],
  "socratic_questions": [],
  "hypotheticals": []
}
```

Cases with existing rich metadata (Socratic Q&A, hypotheticals) carry that metadata forward unchanged.

---

## Phase 2: Resolve Decision IDs

**Script:** `study/resolve_decision_ids.py`

- Walks all curriculum JSON files
- For each case with blank `decision_id`, queries the FTS5 DB on docket_number / full_text for the BGE ref (court=`bge`)
- Writes resolved `decision_id` back to JSON in-place
- Outputs: resolved / not-found / already-set counts
- Non-destructive: never overwrites an existing `decision_id`
- Run once on VPS after Phase 1 is committed

---

## Phase 3: Enrich Metadata

**Script:** `study/enrich_curriculum.py`

For each curriculum case missing `socratic_questions` or `hypotheticals`:

1. Fetches full decision text from DB via `get_decision_by_id(decision_id)`
2. Builds a structured prompt with: BGE ref, significance, statutes, key Erwägungen, regeste, and the Socratic question schema
3. Calls `claude-sonnet-4-6` to generate:
   - 5 Bloom-level Socratic questions with `model_answer` + `hint`
   - 2 hypotheticals with `discussion_points` + `likely_outcome_shift`
   - Reading guides in DE/FR/IT
   - `significance_fr` and `significance_it` (if only DE exists)
   - `key_erwagungen` list
4. Validates JSON response shape before writing
5. Writes results back to curriculum JSON
6. Rate-limits at 3 req/s; skips cases where `decision_id` is still blank
7. Flags: `--dry-run` (print without writing), `--area <id>` (restrict to one area)

**Requires:** `ANTHROPIC_API_KEY` in environment.

---

## Infrastructure Impact

**No changes needed to MCP tools.** The `load_curriculum()` function in `study/curriculum_engine.py` uses `glob("*.json")` — the 6 new JSON files are picked up automatically. `list_study_curriculum`, `study_leading_case`, and `check_case_brief` all work unchanged.

---

## Files Created / Modified

| File | Action |
|------|--------|
| `study/curriculum/vertragsrecht.json` | Updated (absorb + expand to 8) |
| `study/curriculum/haftpflicht.json` | Updated (absorb + expand to 8) |
| `study/curriculum/sachenrecht.json` | Updated (absorb + expand to 7) |
| `study/curriculum/familienrecht.json` | Updated (absorb + expand to 7) |
| `study/curriculum/arbeitsrecht.json` | Updated (absorb + expand to 7) |
| `study/curriculum/mietrecht.json` | Updated (absorb + expand to 6) |
| `study/curriculum/strafrecht_at.json` | Updated (absorb + expand to 8) |
| `study/curriculum/grundrechte.json` | Updated (absorb + expand to 7) |
| `study/curriculum/strafrecht_bt.json` | **New** (7 cases) |
| `study/curriculum/erbrecht.json` | **New** (7 cases) |
| `study/curriculum/gesellschaftsrecht.json` | **New** (7 cases) |
| `study/curriculum/zivilprozessrecht.json` | **New** (7 cases) |
| `study/curriculum/strafprozessrecht.json` | **New** (7 cases) |
| `study/curriculum/oeffentliches_prozessrecht.json` | **New** (7 cases) |
| `study/resolve_decision_ids.py` | **New** |
| `study/enrich_curriculum.py` | **New** |
