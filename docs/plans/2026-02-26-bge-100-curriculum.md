# BGE-100 Canonical Curriculum Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expand the study curriculum from 44 to 100 landmark BGE decisions across 14 legal areas, add 6 new area JSON files, and build two utility scripts (resolver + enricher).

**Architecture:** Phase 1 curates the canonical 100-case JSON files (8 updated + 6 new). Phase 2 (`resolve_decision_ids.py`) maps BGE refs to DB decision_ids. Phase 3 (`enrich_curriculum.py`) batch-generates Socratic Q&A via Anthropic API for cases lacking metadata.

**Tech Stack:** Python 3.11+, existing `study/curriculum_engine.py`, SQLite FTS5 DB (`output/swiss_caselaw_fts5.db`), `anthropic` SDK, pytest.

---

## Canonical 100-Case Reference Table

This is the authoritative list. BGE refs marked `[find]` require a reference-graph lookup during implementation — use `find_leading_cases(law_code=X, article=Y, court="bge", limit=5)` and pick the most-cited result that tests the stated doctrine.

### Existing areas — absorb decisions

| Area | Module | BGE Ref | Doctrine | Diff | Action |
|------|--------|---------|----------|------|--------|
| vertragsrecht | vertragsschluss | BGE 135 III 1 | AGB-Kontrolle, Ungewöhnlichkeitsregel | 3 | KEEP |
| vertragsrecht | vertragsschluss | BGE 123 III 35 | Konsens bei Verweisungsverträgen | 4 | KEEP |
| vertragsrecht | vertragsauslegung | BGE 131 III 606 | Vertragsauslegung, Vertrauensprinzip | 2 | KEEP |
| vertragsrecht | vertragsauslegung | BGE 113 II 25 | culpa in contrahendo | 3 | REPLACE BGE 144 III 93 |
| vertragsrecht | willensmangel | BGE 128 III 70 | Irrtum und Täuschung Art. 23–28 OR | 2 | KEEP |
| vertragsrecht | willensmangel | BGE 132 III 737 | Grundlagenirrtum bei Vergleich | 4 | KEEP |
| vertragsrecht | erfullung | BGE 127 III 543 | Schlechterfüllung (positive Vertragsverletzung) | 3 | KEEP |
| vertragsrecht | erfullung | BGE 123 III 16 | Wahlrecht Gläubiger bei Schuldnerverzug | 2 | KEEP |
| haftpflicht | verschuldenshaftung | BGE 123 III 110 | Adäquater Kausalzusammenhang | 2 | KEEP |
| haftpflicht | verschuldenshaftung | [find OR Art.41 "Widerrechtlichkeit"] | Widerrechtlichkeit Art. 41 OR | 2 | NEW |
| haftpflicht | schadensbeweis | BGE 122 III 219 | Schadensschätzung Art. 42 Abs. 2 | 2 | KEEP |
| haftpflicht | schadensbeweis | BGE 131 III 12 | Konstitutionelle Prädisposition | 3 | KEEP |
| haftpflicht | schadensbeweis | [find OR Art.47 "Genugtuung"] | Genugtuung Art. 47/49 OR | 2 | REPLACE BGE 133 III 323 |
| haftpflicht | kausalhaftungen | BGE 122 III 225 | Organ/Hilfsperson Art. 55 OR | 3 | KEEP |
| haftpflicht | kausalhaftungen | [find OR Art.58 "Werkeigentümer"] | Werkeigentümerhaftung Art. 58 OR | 3 | NEW |
| haftpflicht | kausalhaftungen | [find OR Art.97 "Schutzwirkung Dritter"] | Vertrag mit Schutzwirkung für Dritte | 4 | NEW |
| sachenrecht | eigentum | BGE 132 III 651 | Actio negatoria, Eigentumsschutz | 3 | KEEP |
| sachenrecht | eigentum | BGE 127 III 506 | Nutzungsrechte Stockwerkeigentum | 4 | KEEP |
| sachenrecht | eigentum | [find ZGB Art.973 "öffentlicher Glaube"] | Öffentlicher Glaube Grundbuch | 3 | NEW |
| sachenrecht | besitz | BGE 135 III 633 | Besitzesschutz, verbotene Eigenmacht | 2 | KEEP |
| sachenrecht | besitz | [find ZGB Art.714 "Eigentumserwerb Fahrnis"] | Eigentumserwerb bewegliche Sachen | 2 | NEW |
| sachenrecht | dienstbarkeiten | BGE 128 III 265 | Dingliche vs. obligatorische Wirkung | 3 | KEEP |
| sachenrecht | dienstbarkeiten | BGE 137 III 145 | Inhalt und Umfang Wegrecht | 2 | KEEP |
| familienrecht | unterhalt | BGE 147 III 249 | Unterhaltsberechnung, zweistufige Methode | 3 | KEEP |
| familienrecht | unterhalt | [find ZGB Art.125 "nachehelicher Unterhalt"] | Nachehelicher Unterhalt | 3 | NEW |
| familienrecht | sorgerecht | BGE 142 III 481 | Gemeinsame elterliche Sorge als Regelfall | 2 | KEEP |
| familienrecht | sorgerecht | BGE 144 III 349 | Betreuungsunterhalt | 3 | KEEP |
| familienrecht | sorgerecht | [find ZGB Art.296 "Kindeswohl Sorgerecht"] | Kindeswohl-Grundsatz | 2 | NEW |
| familienrecht | guterrecht | BGE 141 III 145 | Güterrechtliche Auseinandersetzung | 2 | KEEP |
| familienrecht | guterrecht | [find ZGB Art.204 "Gütertrennung"] | Gütertrennung auf Antrag | 3 | NEW |
| arbeitsrecht | kundigung_ar | BGE 136 III 513 | Missbräuchliche Kündigung, Rachekündigung | 2 | KEEP |
| arbeitsrecht | kundigung_ar | BGE 132 III 115 | Fristlose Entlassung | 3 | KEEP |
| arbeitsrecht | kundigung_ar | [find OR Art.336c "Kündigung zur Unzeit"] | Kündigung zur Unzeit | 2 | NEW |
| arbeitsrecht | lohn | BGE 129 III 171 | Überstundenvergütung, Darlegungslast | 2 | KEEP |
| arbeitsrecht | lohn | [find GlG Art.3 "Lohngleichheit Diskriminierung"] | Lohngleichheit, Diskriminierungsverbot | 3 | NEW |
| arbeitsrecht | konkurrenzverbot | BGE 138 III 67 | Konkurrenzverbot, Einblick Kundenstamm | 3 | KEEP |
| arbeitsrecht | konkurrenzverbot | [find OR Art.330a "Arbeitszeugnis"] | Arbeitszeugnis, Wohlwollenspflicht | 2 | NEW |
| mietrecht | mietzins | BGE 141 III 569 | Anfangsmietzins, Anfechtung | 2 | KEEP |
| mietrecht | mietzins | BGE 140 III 433 | Nettorendite, Überrendite | 3 | KEEP |
| mietrecht | kundigung_mr | BGE 142 III 91 | Kündigung wegen Zahlungsrückstand | 1 | KEEP |
| mietrecht | kundigung_mr | BGE 138 III 59 | Missbräuchliche Kündigung Mietrecht | 3 | KEEP |
| mietrecht | mangel | BGE 135 III 345 | Mängelrechte, Mietzinsherabsetzung | 2 | KEEP |
| mietrecht | mangel | [find OR Art.272 "Mieterstreckung"] | Mieterstreckung | 3 | NEW |
| strafrecht_at | vorsatz | BGE 130 IV 58 | Eventualvorsatz vs. bewusste Fahrlässigkeit | 2 | KEEP |
| strafrecht_at | vorsatz | BGE 133 IV 9 | Eventualvorsatz Strassenverkehr | 3 | KEEP |
| strafrecht_at | vorsatz | [find StGB Art.15 "Notwehr Rechtfertigung"] | Notwehr Art. 15 StGB | 3 | NEW |
| strafrecht_at | versuch | BGE 131 IV 1 | Versuch schwere Körperverletzung (HIV) | 3 | KEEP |
| strafrecht_at | teilnahme | BGE 118 IV 397 | Mittäterschaft bei BtM-Delikten | 3 | KEEP |
| strafrecht_at | teilnahme | BGE 144 IV 265 | Anstiftung/Gehilfenschaft, Begehungsort | 4 | KEEP |
| strafrecht_at | sanktionen | BGE 134 IV 17 | Strafzumessung bei BtM-Delikten | 2 | KEEP |
| strafrecht_at | sanktionen | BGE 144 IV 313 | Gesamtstrafenbildung, Asperationsprinzip | 3 | KEEP |
| grundrechte | rechtsgleichheit | BGE 129 I 232 | Einbürgerungsentscheide, Begründungspflicht | 2 | KEEP |
| grundrechte | rechtsgleichheit | [find BV Art.8 "Rechtsgleichheit Diskriminierung"] | Rechtsgleichheit, Diskriminierungsverbot | 2 | NEW |
| grundrechte | personliche_freiheit | BGE 134 I 140 | Schutzmassnahmen häusliche Gewalt | 3 | KEEP |
| grundrechte | personliche_freiheit | BGE 142 I 135 | Administrativhaft, Freiheitsentzug | 4 | KEEP |
| grundrechte | wirtschaftsfreiheit | BGE 142 I 99 | Sondernutzungskonzession | 3 | KEEP |
| grundrechte | verfahrensgarantien | BGE 136 I 229 | Anfechtung Prüfungsergebnisse | 2 | KEEP |
| grundrechte | verfahrensgarantien | BGE 134 II 244 | Begründungsanforderungen Beschwerden | 1 | KEEP |

### New areas (all NEW cases)

| Area | Module | BGE Ref | Doctrine | Diff |
|------|--------|---------|----------|------|
| strafrecht_bt | vermogensdelikte | BGE 124 IV 127 | Betrug, Arglist Art. 146 StGB | 3 |
| strafrecht_bt | vermogensdelikte | BGE 133 IV 228 | Geldwäscherei Art. 305bis StGB | 4 |
| strafrecht_bt | vermogensdelikte | [find StGB Art.139 "Diebstahl Gewahrsam"] | Diebstahl, Gewahrsam | 2 |
| strafrecht_bt | koerper_sexualdelikte | BGE 131 IV 83 | Vergewaltigung Art. 190 StGB | 3 |
| strafrecht_bt | koerper_sexualdelikte | [find StGB Art.122 "schwere Körperverletzung"] | Schwere Körperverletzung | 2 |
| strafrecht_bt | sonderdelikte | BGE 128 IV 18 | Urkundenfälschung Art. 251 StGB | 3 |
| strafrecht_bt | sonderdelikte | BGE 137 IV 113 | Nötigung Art. 181 StGB | 3 |
| erbrecht | pflichtteil | BGE 115 II 323 | Pflichtteilsanspruch Art. 471 ZGB | 2 |
| erbrecht | pflichtteil | [find ZGB Art.522 "Herabsetzungsklage"] | Herabsetzungsklage | 3 |
| erbrecht | testamentsrecht | BGE 120 II 177 | Testament, Auslegung Art. 467 ZGB | 3 |
| erbrecht | testamentsrecht | [find ZGB Art.494 "Erbvertrag Bindungswirkung"] | Erbvertrag | 3 |
| erbrecht | erbteilung | BGE 131 III 601 | Ausgleichung Art. 626 ZGB | 3 |
| erbrecht | erbteilung | BGE 138 III 354 | Erbunwürdigkeit Art. 540 ZGB | 4 |
| erbrecht | erbteilung | BGE 143 III 369 | Güterrecht und Erbrecht, Zusammenspiel | 4 |
| gesellschaftsrecht | ag_recht | BGE 136 III 278 | Vinkulierung Namenaktien Art. 685 OR | 3 |
| gesellschaftsrecht | ag_recht | [find OR Art.698 "Generalversammlung Beschluss"] | GV-Beschlussanfechtung | 3 |
| gesellschaftsrecht | verantwortlichkeit | BGE 128 III 142 | Sorgfaltspflicht Organe Art. 754 OR | 3 |
| gesellschaftsrecht | verantwortlichkeit | BGE 132 III 564 | Verantwortlichkeitsklage Art. 754 OR | 4 |
| gesellschaftsrecht | verantwortlichkeit | BGE 102 II 224 | Konzernhaftung, Durchgriff | 4 |
| gesellschaftsrecht | gmbh_konzern | BGE 140 III 533 | GmbH nach neuem Recht Art. 772 ff. OR | 3 |
| gesellschaftsrecht | gmbh_konzern | [find OR Art.717 "Treuepflicht Aktionäre"] | Treuepflicht Verwaltungsrat | 3 |
| zivilprozessrecht | zustandigkeit | [find ZPO Art.17 "Prorogation Gerichtsstand"] | Prorogation, Gerichtsstandsvereinbarung | 3 |
| zivilprozessrecht | zustandigkeit | [find ZPO Art.59 "Prozessvoraussetzungen Klage"] | Prozessvoraussetzungen | 2 |
| zivilprozessrecht | beweisrecht | [find ZPO Art.157 "Beweismass Glaubhaftmachung"] | Beweismass, Glaubhaftmachung | 3 |
| zivilprozessrecht | beweisrecht | [find ZPO Art.197 "Schlichtungsverfahren"] | Schlichtungsverfahren | 2 |
| zivilprozessrecht | rechtsmittel | [find BGG Art.74 "Beschwerde Zivilsachen Streitwert"] | BGG Streitwertgrenze Zivilsachen | 3 |
| zivilprozessrecht | rechtsmittel | [find ZPO Art.317 "neue Tatsachen Noven Berufung"] | Echte Noven Art. 317 ZPO | 4 |
| zivilprozessrecht | rechtsmittel | [find ZPO Art.47 "Ausstandspflicht Richter"] | Ausstandspflicht | 3 |
| strafprozessrecht | grundsatze | BGE 133 IV 329 | In dubio pro reo Art. 10 StPO | 2 |
| strafprozessrecht | grundsatze | [find StPO Art.9 "Anklagegrundsatz"] | Anklagegrundsatz | 3 |
| strafprozessrecht | zwangsmassnahmen | BGE 137 IV 33 | Untersuchungshaft, Kollusionsgefahr | 3 |
| strafprozessrecht | zwangsmassnahmen | [find StPO Art.5 "Beschleunigungsgebot"] | Beschleunigungsgebot | 3 |
| strafprozessrecht | beweise | BGE 139 IV 179 | Beweisverwertungsverbot Art. 141 StPO | 3 |
| strafprozessrecht | beweise | [find StPO Art.168 "Zeugnisverweigerung"] | Zeugnisverweigerungsrecht | 3 |
| strafprozessrecht | beweise | [find StPO Art.319 "Einstellung Freispruch Abgrenzung"] | Einstellung vs. Freispruch | 4 |
| oeffentliches_prozessrecht | verwaltungsverfahren | BGE 110 Ia 1 | Rechtliches Gehör, Begründungspflicht (VwVG) | 2 |
| oeffentliches_prozessrecht | verwaltungsverfahren | [find BGG Art.89 "Beschwerdelegitimation öffentliches Recht"] | Beschwerdelegitimation | 3 |
| oeffentliches_prozessrecht | beschwerde | [find BV Art.29a "Rechtsweggarantie"] | Rechtsweggarantie Art. 29a BV | 3 |
| oeffentliches_prozessrecht | beschwerde | BGE 133 I 201 | Zugang zum Gericht Art. 6 EMRK | 3 |
| oeffentliches_prozessrecht | kognition | BGE 137 II 40 | Kognition BGer bei Ermessen | 4 |
| oeffentliches_prozessrecht | kognition | BGE 142 I 155 | Verhältnismässigkeit im öffentlichen Recht | 3 |
| oeffentliches_prozessrecht | kognition | BGE 138 I 274 | Willkür Art. 9 BV im Verwaltungsrecht | 2 |

**Total: 100 cases** (58 from existing areas + 42 from 6 new areas)

---

## Case JSON Schema (thin metadata for Phase 1)

For every case that does NOT already have rich metadata, use this template:

```json
{
  "decision_id": "",
  "bge_ref": "BGE XXX YYY ZZZ",
  "actual_language": "de",
  "title_de": "Short German title",
  "title_fr": "Short French title",
  "title_it": "Short Italian title",
  "concepts_de": ["Concept1", "Concept2"],
  "concepts_fr": ["Concept1FR", "Concept2FR"],
  "concepts_it": [],
  "statutes": ["Art. X OR"],
  "difficulty": 3,
  "prerequisites": [],
  "significance_de": "One-sentence significance statement.",
  "significance_fr": "",
  "significance_it": "",
  "actual_language": "de",
  "key_erwagungen": [],
  "reading_guide_de": "",
  "reading_guide_fr": "",
  "reading_guide_it": "",
  "socratic_questions": [],
  "hypotheticals": []
}
```

For `[find ...]` cases: run `find_leading_cases` first, pick the top result, then fill in `bge_ref` and `decision_id` from that result. Leave `decision_id` as `""` if unresolved — the resolver script will fill it.

---

## Task 1: Write Curriculum Validation Tests

**Files:**
- Create: `tests/test_curriculum_100.py`

**Step 1: Write the failing tests**

```python
# tests/test_curriculum_100.py
"""Validation tests for the BGE-100 canonical curriculum."""
import pytest
from study.curriculum_engine import load_curriculum

EXPECTED_AREAS = {
    "vertragsrecht": 8,
    "haftpflicht": 8,
    "sachenrecht": 7,
    "familienrecht": 7,
    "arbeitsrecht": 7,
    "mietrecht": 6,
    "strafrecht_at": 8,
    "grundrechte": 7,
    "strafrecht_bt": 7,
    "erbrecht": 7,
    "gesellschaftsrecht": 7,
    "zivilprozessrecht": 7,
    "strafprozessrecht": 7,
    "oeffentliches_prozessrecht": 7,
}


def test_total_case_count():
    areas = load_curriculum()
    total = sum(len(m.cases) for a in areas for m in a.modules)
    assert total == 100, f"Expected 100 cases, got {total}"


def test_area_count():
    areas = load_curriculum()
    assert len(areas) == 14, f"Expected 14 areas, got {len(areas)}"


def test_required_area_ids():
    area_ids = {a.area_id for a in load_curriculum()}
    assert area_ids == set(EXPECTED_AREAS.keys()), (
        f"Missing: {set(EXPECTED_AREAS) - area_ids}, "
        f"Extra: {area_ids - set(EXPECTED_AREAS)}"
    )


def test_area_case_counts():
    areas = load_curriculum()
    for a in areas:
        count = sum(len(m.cases) for m in a.modules)
        expected = EXPECTED_AREAS.get(a.area_id, -1)
        assert count == expected, (
            f"{a.area_id}: expected {expected} cases, got {count}"
        )


def test_no_duplicate_bge_refs():
    areas = load_curriculum()
    refs = [c.bge_ref for a in areas for m in a.modules for c in m.cases]
    dupes = [r for r in refs if refs.count(r) > 1]
    assert not dupes, f"Duplicate BGE refs: {set(dupes)}"


def test_required_fields_per_case():
    areas = load_curriculum()
    for a in areas:
        for m in a.modules:
            for c in m.cases:
                assert c.bge_ref, f"Missing bge_ref in {a.area_id}/{m.id}"
                assert c.significance_de, (
                    f"Missing significance_de for {c.bge_ref}"
                )
                assert c.difficulty in range(1, 6), (
                    f"Invalid difficulty {c.difficulty} for {c.bge_ref}"
                )
                assert c.statutes, f"Missing statutes for {c.bge_ref}"


def test_difficulty_distribution():
    """No area should have all cases at the same difficulty."""
    areas = load_curriculum()
    for a in areas:
        diffs = [c.difficulty for m in a.modules for c in m.cases]
        assert len(set(diffs)) > 1, (
            f"{a.area_id}: all cases have the same difficulty {diffs[0]}"
        )
```

**Step 2: Run to verify tests fail**

```bash
cd /Users/jonashertner/caselaw-repo-1
python -m pytest tests/test_curriculum_100.py -v 2>&1 | head -40
```

Expected: multiple FAILED — `test_area_count` (14 vs 8), `test_total_case_count` (100 vs 44), area id errors for 6 missing new areas.

**Step 3: Commit failing tests**

```bash
git add tests/test_curriculum_100.py
git commit -m "test: add failing validation tests for BGE-100 curriculum"
```

---

## Task 2: Update Existing 8 Curriculum JSON Files

For each file, the steps are: read → apply absorb logic → write → run tests to check progress.

**Files:** `study/curriculum/` — all 8 existing `.json` files

### 2a: vertragsrecht.json (target: 8 cases)

**Absorb action:** Replace `BGE 144 III 93` with `BGE 113 II 25` (culpa in contrahendo). Keep all other 7 cases unchanged (including their rich metadata).

New case to add (`BGE 113 II 25`, module `vertragsauslegung`):

```json
{
  "decision_id": "",
  "bge_ref": "BGE 113 II 25",
  "actual_language": "de",
  "title_de": "Culpa in contrahendo — vorvertragliche Haftung",
  "title_fr": "Culpa in contrahendo — responsabilité précontractuelle",
  "title_it": "Culpa in contrahendo — responsabilità precontrattuale",
  "concepts_de": ["Culpa in contrahendo", "Vorvertragliche Haftung", "Vertragsverhandlungen", "Art. 2 ZGB"],
  "concepts_fr": ["Culpa in contrahendo", "Responsabilité précontractuelle", "Négociations contractuelles"],
  "concepts_it": [],
  "statutes": ["Art. 2 ZGB", "Art. 41 OR"],
  "difficulty": 3,
  "prerequisites": [],
  "significance_de": "Grundsatzentscheid zur vorvertraglichen Haftung: Aufklärungs- und Sorgfaltspflichten bei Vertragsverhandlungen nach Treu und Glauben.",
  "significance_fr": "Arrêt de principe sur la responsabilité précontractuelle: obligations d'information et de diligence lors des négociations.",
  "significance_it": "",
  "key_erwagungen": [],
  "reading_guide_de": "",
  "reading_guide_fr": "",
  "reading_guide_it": "",
  "socratic_questions": [],
  "hypotheticals": []
}
```

**Step:** In `vertragsrecht.json`, find the `vertragsauslegung` module's `cases` array. Remove the entry with `"bge_ref": "BGE 144 III 93"`. Add the new BGE 113 II 25 entry above.

### 2b: haftpflicht.json (target: 8 cases, currently 5)

**Absorb action:**
- Replace `BGE 133 III 323` (IPR Geldwäscherei — too narrow) with the Genugtuung case (find below)
- Rename module `geschaftsherrenhaftung` → `kausalhaftungen`, add statutes `["Art. 55 OR", "Art. 58 OR"]`
- Add 3 new cases (slots: Widerrechtlichkeit, Genugtuung replacement, Werkeigentümerhaftung, Schutzwirkung Dritter)

**Before adding new cases, run find_leading_cases queries** (do this by calling the MCP tool or `mcp_server._find_leading_cases` directly):

```python
# Run in Python to identify best BGE for each [find] slot:
import sys; sys.path.insert(0, '.')
from mcp_server import _find_leading_cases

# Slot: Widerrechtlichkeit
print(_find_leading_cases(law_code="OR", article="41", court="bge", limit=5))

# Slot: Genugtuung
print(_find_leading_cases(law_code="OR", article="47", court="bge", limit=5))

# Slot: Werkeigentümerhaftung
print(_find_leading_cases(law_code="OR", article="58", court="bge", limit=5))

# Slot: Vertrag mit Schutzwirkung
print(_find_leading_cases(law_code="OR", article="97", court="bge", limit=5))
```

Use the top result for each slot that best tests the stated doctrine. Add 4 thin-metadata case objects to `haftpflicht.json` (3 in `kausalhaftungen`, 1 replacing BGE 133 III 323 in `schadensbeweis`).

**Run tests after:**
```bash
python -m pytest tests/test_curriculum_100.py::test_area_case_counts -v
```

### 2c–2h: Update remaining 6 existing areas

Apply the same pattern: read existing JSON → add `[find]` cases using reference graph queries → write JSON.

| File | Target | New cases needed | Slots |
|------|--------|-----------------|-------|
| sachenrecht.json | 7 | +2 | Öff. Glaube Grundbuch (ZGB 973), Eigentumserwerb Fahrnis (ZGB 714) |
| familienrecht.json | 7 | +3 | Nachehelicher Unterhalt (ZGB 125), Kindeswohl (ZGB 296), Gütertrennung (ZGB 204) |
| arbeitsrecht.json | 7 | +3 | Kündigung zur Unzeit (OR 336c), Lohngleichheit (GlG 3), Arbeitszeugnis (OR 330a) |
| mietrecht.json | 6 | +1 | Mieterstreckung (OR 272) |
| strafrecht_at.json | 8 | +1 | Notwehr (StGB 15) |
| grundrechte.json | 7 | +1 | Rechtsgleichheit Art. 8 BV (BV 8) |

For each `[find]` slot, run the equivalent `_find_leading_cases` query and pick the top result.

**Step: After all 8 files updated, run:**
```bash
python -m pytest tests/test_curriculum_100.py -v 2>&1 | grep -E "PASSED|FAILED|ERROR"
```

Expected at this point: `test_area_case_counts` passes for 8 existing areas; `test_area_count` and `test_total_case_count` still fail (new areas not yet created).

**Commit:**
```bash
git add study/curriculum/vertragsrecht.json study/curriculum/haftpflicht.json \
    study/curriculum/sachenrecht.json study/curriculum/familienrecht.json \
    study/curriculum/arbeitsrecht.json study/curriculum/mietrecht.json \
    study/curriculum/strafrecht_at.json study/curriculum/grundrechte.json
git commit -m "feat: absorb and expand existing 8 curriculum areas to target case counts"
```

---

## Task 3: Create strafrecht_bt.json

**File:** `study/curriculum/strafrecht_bt.json`

For the 2 `[find]` slots (Diebstahl, schwere Körperverletzung), run:
```python
print(_find_leading_cases(law_code="StGB", article="139", court="bge", limit=5))  # Diebstahl
print(_find_leading_cases(law_code="StGB", article="122", court="bge", limit=5))  # schwere KV
```

Then write the full JSON:

```json
{
  "area_id": "strafrecht_bt",
  "area_de": "Strafrecht Besonderer Teil",
  "area_fr": "Droit pénal partie spéciale",
  "area_it": "Diritto penale parte speciale",
  "description_de": "Ausgewählte Tatbestände: Vermögensdelikte, Körper- und Sexualdelikte, Sonderdelikte",
  "modules": [
    {
      "id": "vermogensdelikte",
      "name_de": "Vermögensdelikte",
      "name_fr": "Infractions contre le patrimoine",
      "name_it": "Reati contro il patrimonio",
      "statutes": ["Art. 139 StGB", "Art. 146 StGB", "Art. 305bis StGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 124 IV 127",
          "actual_language": "de",
          "title_de": "Betrug — Begriff der Arglist",
          "title_fr": "Escroquerie — notion d'astuce",
          "title_it": "Truffa — nozione di astuzia",
          "concepts_de": ["Betrug", "Arglist", "Täuschung", "Vermögensschaden"],
          "concepts_fr": ["Escroquerie", "Astuce", "Tromperie", "Dommage patrimonial"],
          "concepts_it": [],
          "statutes": ["Art. 146 StGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zum Arglistmerkmal beim Betrug: wann eine Täuschung als arglistig gilt und wann Eigenverantwortung des Opfers die Strafbarkeit ausschliesst.",
          "significance_fr": "Arrêt de principe sur l'astuce dans l'escroquerie: quand une tromperie est astucieuse et quand la responsabilité propre de la victime exclut la punissabilité.",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 133 IV 228",
          "actual_language": "de",
          "title_de": "Geldwäscherei — Vortat und Einziehung",
          "title_fr": "Blanchiment d'argent — infraction préalable et confiscation",
          "title_it": "Riciclaggio — reato presupposto e confisca",
          "concepts_de": ["Geldwäscherei", "Vortat", "Verbrecherische Herkunft", "Art. 305bis StGB"],
          "concepts_fr": ["Blanchiment d'argent", "Infraction préalable", "Origine criminelle"],
          "concepts_it": [],
          "statutes": ["Art. 305bis StGB", "Art. 70 StGB"],
          "difficulty": 4,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Geldwäscherei: Anforderungen an die Vortat, Begriff der 'verbrecherischen Herkunft' und Verhältnis zur Einziehung.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_StGB_139",
          "actual_language": "de",
          "title_de": "Diebstahl — Begriff des Gewahrsams",
          "title_fr": "Vol — notion de détention",
          "title_it": "Furto — nozione di detenzione",
          "concepts_de": ["Diebstahl", "Gewahrsam", "Wegnahme", "Fremdheit"],
          "concepts_fr": ["Vol", "Détention", "Soustraction"],
          "concepts_it": [],
          "statutes": ["Art. 139 StGB"],
          "difficulty": 2,
          "prerequisites": [],
          "significance_de": "Massgebender Entscheid zum Gewahrsamsbegriff: wann eine Sache im Gewahrsam einer Person steht und was Wegnahme bedeutet.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "koerper_sexualdelikte",
      "name_de": "Körper- und Sexualdelikte",
      "name_fr": "Infractions contre l'intégrité corporelle et sexuelle",
      "name_it": "Reati contro l'integrità corporale e sessuale",
      "statutes": ["Art. 122 StGB", "Art. 123 StGB", "Art. 190 StGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 131 IV 83",
          "actual_language": "de",
          "title_de": "Vergewaltigung — Begriff des Nötigungsmittels",
          "title_fr": "Viol — notion du moyen de contrainte",
          "title_it": "Stupro — nozione del mezzo coercitivo",
          "concepts_de": ["Vergewaltigung", "Nötigungsmittel", "Gewalt", "Drohung"],
          "concepts_fr": ["Viol", "Moyen de contrainte", "Violence", "Menace"],
          "concepts_it": [],
          "statutes": ["Art. 190 StGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Vergewaltigung: Definition der Nötigungsmittel (Gewalt, Drohung, Druck) und Abgrenzung zu sexuellen Nötigungsformen.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_StGB_122",
          "actual_language": "de",
          "title_de": "Schwere Körperverletzung — Abgrenzung zur einfachen",
          "title_fr": "Lésions corporelles graves — délimitation",
          "title_it": "Lesioni corporali gravi — distinzione",
          "concepts_de": ["Schwere Körperverletzung", "Einfache Körperverletzung", "Lebensgefahr", "Verstümmelung"],
          "concepts_fr": ["Lésions corporelles graves", "Lésions corporelles simples", "Danger de mort"],
          "concepts_it": [],
          "statutes": ["Art. 122 StGB", "Art. 123 StGB"],
          "difficulty": 2,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Abgrenzung schwere/einfache Körperverletzung: Kriterien für Lebensgefahr, dauernde Unfähigkeit und Verstümmelung.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "sonderdelikte",
      "name_de": "Sonderdelikte",
      "name_fr": "Infractions spéciales",
      "name_it": "Reati speciali",
      "statutes": ["Art. 181 StGB", "Art. 251 StGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 128 IV 18",
          "actual_language": "de",
          "title_de": "Urkundenfälschung — Begriff der Urkunde",
          "title_fr": "Faux dans les titres — notion de titre",
          "title_it": "Falsità in documenti — nozione di documento",
          "concepts_de": ["Urkundenfälschung", "Urkunde", "Beweisbestimmung", "Beweiseignung"],
          "concepts_fr": ["Faux dans les titres", "Titre", "Destination probatoire"],
          "concepts_it": [],
          "statutes": ["Art. 251 StGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Massgebender Entscheid zum Urkundenbegriff: Beweisbestimmung und -eignung als kumulative Voraussetzungen für eine strafrechtlich relevante Urkunde.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 137 IV 113",
          "actual_language": "de",
          "title_de": "Nötigung — Begriff des Nötigungsmittels",
          "title_fr": "Contrainte — notion du moyen de contrainte",
          "title_it": "Coazione — nozione del mezzo coercitivo",
          "concepts_de": ["Nötigung", "Nötigungsmittel", "Gewalt", "Androhung ernstlicher Nachteile"],
          "concepts_fr": ["Contrainte", "Moyen de contrainte", "Violence", "Menace de préjudice sérieux"],
          "concepts_it": [],
          "statutes": ["Art. 181 StGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Nötigung: Definition der Nötigungsmittel und Abgrenzung zum erlaubten sozialen Druck.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    }
  ]
}
```

**Note:** Replace `"bge_ref": "RESOLVE_StGB_139"` and `"bge_ref": "RESOLVE_StGB_122"` with the actual BGE ref returned by the `find_leading_cases` query above before writing the file.

**Step: Run tests**
```bash
python -m pytest tests/test_curriculum_100.py::test_required_area_ids -v
```

**Commit:**
```bash
git add study/curriculum/strafrecht_bt.json
git commit -m "feat: add strafrecht_bt curriculum (7 cases)"
```

---

## Task 4: Create erbrecht.json

**File:** `study/curriculum/erbrecht.json`

For `[find]` slots, run:
```python
print(_find_leading_cases(law_code="ZGB", article="522", court="bge", limit=5))  # Herabsetzung
print(_find_leading_cases(law_code="ZGB", article="494", court="bge", limit=5))  # Erbvertrag
```

```json
{
  "area_id": "erbrecht",
  "area_de": "Erbrecht",
  "area_fr": "Droit des successions",
  "area_it": "Diritto successorio",
  "description_de": "Gesetzliche Erbfolge, Pflichtteil, Testament und Erbvertrag, Erbteilung und Ausgleichung",
  "modules": [
    {
      "id": "pflichtteil",
      "name_de": "Pflichtteil und Herabsetzung",
      "name_fr": "Réserve et réduction",
      "name_it": "Quota legittima e riduzione",
      "statutes": ["Art. 471 ZGB", "Art. 522 ZGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 115 II 323",
          "actual_language": "de",
          "title_de": "Pflichtteilsanspruch — Berechnung und Geltendmachung",
          "title_fr": "Réserve héréditaire — calcul et exercice",
          "title_it": "Quota legittima — calcolo ed esercizio",
          "concepts_de": ["Pflichtteil", "Verfügbare Quote", "Herabsetzungsklage", "Nachlasswert"],
          "concepts_fr": ["Réserve", "Quotité disponible", "Action en réduction", "Masse successorale"],
          "concepts_it": [],
          "statutes": ["Art. 471 ZGB", "Art. 522 ZGB"],
          "difficulty": 2,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Pflichtteilsberechnung: Zusammensetzung der Berechnungsgrundlage und Voraussetzungen der Herabsetzungsklage.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_ZGB_522",
          "actual_language": "de",
          "title_de": "Herabsetzungsklage — Reihenfolge und Verjährung",
          "title_fr": "Action en réduction — ordre et prescription",
          "title_it": "Azione di riduzione — ordine e prescrizione",
          "concepts_de": ["Herabsetzungsklage", "Reihenfolge", "Verjährung", "Art. 533 ZGB"],
          "concepts_fr": ["Action en réduction", "Ordre de réduction", "Prescription"],
          "concepts_it": [],
          "statutes": ["Art. 522 ZGB", "Art. 527 ZGB", "Art. 533 ZGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Herabsetzungsklage: Reihenfolge der Herabsetzung und Verjährungsbeginn.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "testamentsrecht",
      "name_de": "Testament und Erbvertrag",
      "name_fr": "Testament et pacte successoral",
      "name_it": "Testamento e patto successorio",
      "statutes": ["Art. 467 ZGB", "Art. 494 ZGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 120 II 177",
          "actual_language": "de",
          "title_de": "Testament — Auslegung und Andeutungstheorie",
          "title_fr": "Testament — interprétation et théorie de l'indice",
          "title_it": "Testamento — interpretazione e teoria del minimo segno",
          "concepts_de": ["Testamentsauslegung", "Andeutungstheorie", "Erblasserwille", "Art. 467 ZGB"],
          "concepts_fr": ["Interprétation du testament", "Théorie de l'indice", "Volonté du testateur"],
          "concepts_it": [],
          "statutes": ["Art. 467 ZGB", "Art. 469 ZGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Testamentsauslegung: die Andeutungstheorie als Schranke — der Wille des Erblassers muss im Wortlaut mindestens angedeutet sein.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_ZGB_494",
          "actual_language": "de",
          "title_de": "Erbvertrag — Bindungswirkung und Widerruf",
          "title_fr": "Pacte successoral — force obligatoire et révocation",
          "title_it": "Patto successorio — efficacia vincolante e revoca",
          "concepts_de": ["Erbvertrag", "Bindungswirkung", "Widerruf", "Lebzeitige Verfügungen"],
          "concepts_fr": ["Pacte successoral", "Force obligatoire", "Révocation", "Actes entre vifs"],
          "concepts_it": [],
          "statutes": ["Art. 494 ZGB", "Art. 497 ZGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Bindungswirkung des Erbvertrags: Grenzen lebzeitiger Verfügungen und Schutz des Erbvertragsberechtigten.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "erbteilung",
      "name_de": "Erbteilung und Ausgleichung",
      "name_fr": "Partage successoral et rapport",
      "name_it": "Divisione ereditaria e conferimento",
      "statutes": ["Art. 540 ZGB", "Art. 607 ZGB", "Art. 626 ZGB"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 131 III 601",
          "actual_language": "de",
          "title_de": "Ausgleichung — Zuwendungen unter Lebenden",
          "title_fr": "Rapport — libéralités entre vifs",
          "title_it": "Conferimento — liberalità tra vivi",
          "concepts_de": ["Ausgleichung", "Zuwendung", "Ausgleichungspflicht", "Art. 626 ZGB"],
          "concepts_fr": ["Rapport", "Libéralité", "Obligation de rapport"],
          "concepts_it": [],
          "statutes": ["Art. 626 ZGB", "Art. 630 ZGB"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Ausgleichungspflicht: welche Zuwendungen ausgleichungspflichtig sind und wie der Ausgleichungswert berechnet wird.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 138 III 354",
          "actual_language": "de",
          "title_de": "Erbunwürdigkeit — Voraussetzungen Art. 540 ZGB",
          "title_fr": "Indignité — conditions art. 540 CC",
          "title_it": "Indegnità successoria — presupposti art. 540 CC",
          "concepts_de": ["Erbunwürdigkeit", "Tötungsversuch", "Schwere Verletzung Pflichten", "Art. 540 ZGB"],
          "concepts_fr": ["Indignité", "Tentative de meurtre", "Violation grave des devoirs"],
          "concepts_it": [],
          "statutes": ["Art. 540 ZGB"],
          "difficulty": 4,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Erbunwürdigkeit: abschliessende Aufzählung der Unwürdigkeitsgründe und ihr zwingender Charakter.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 143 III 369",
          "actual_language": "de",
          "title_de": "Güterrecht und Erbrecht — Vorausempfang und Ausgleichung",
          "title_fr": "Droit matrimonial et successoral — avancement d'hoirie",
          "title_it": "Diritto matrimoniale e successorio — anticipo d'eredità",
          "concepts_de": ["Güterrecht", "Erbrecht", "Vorausempfang", "Schnittstelle ZGB"],
          "concepts_fr": ["Droit matrimonial", "Droit successoral", "Avancement d'hoirie"],
          "concepts_it": [],
          "statutes": ["Art. 204 ZGB", "Art. 626 ZGB"],
          "difficulty": 4,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zum Zusammenspiel von Güterrecht und Erbrecht bei der Berechnung des Nachlasses und der Ausgleichungspflicht.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    }
  ]
}
```

**Commit:**
```bash
git add study/curriculum/erbrecht.json
git commit -m "feat: add erbrecht curriculum (7 cases)"
```

---

## Task 5: Create gesellschaftsrecht.json

**File:** `study/curriculum/gesellschaftsrecht.json`

For `[find]` slots run:
```python
print(_find_leading_cases(law_code="OR", article="698", court="bge", limit=5))  # GV-Beschluss
print(_find_leading_cases(law_code="OR", article="717", court="bge", limit=5))  # Treuepflicht VR
```

```json
{
  "area_id": "gesellschaftsrecht",
  "area_de": "Gesellschaftsrecht",
  "area_fr": "Droit des sociétés",
  "area_it": "Diritto societario",
  "description_de": "Aktiengesellschaft, Organhaftung, GmbH und Konzernrecht",
  "modules": [
    {
      "id": "ag_recht",
      "name_de": "AG-Recht und Generalversammlung",
      "name_fr": "Droit de la SA et assemblée générale",
      "name_it": "Diritto della SA e assemblea generale",
      "statutes": ["Art. 620 OR", "Art. 685 OR", "Art. 698 OR"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 136 III 278",
          "actual_language": "de",
          "title_de": "Vinkulierung von Namenaktien — Ablehnungsgründe",
          "title_fr": "Restriction de la transmissibilité des actions nominatives",
          "title_it": "Limitazione della trasferibilità di azioni nominative",
          "concepts_de": ["Vinkulierung", "Namenaktien", "Ablehnungsgründe", "Art. 685b OR"],
          "concepts_fr": ["Restriction de transmissibilité", "Actions nominatives", "Motifs de refus"],
          "concepts_it": [],
          "statutes": ["Art. 685b OR", "Art. 685c OR"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Vinkulierung: zulässige Ablehnungsgründe und Verhältnismässigkeitsgebot bei der Übertragungsbeschränkung.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_OR_698",
          "actual_language": "de",
          "title_de": "Generalversammlung — Anfechtung von Beschlüssen",
          "title_fr": "Assemblée générale — annulation de décisions",
          "title_it": "Assemblea generale — impugnazione di deliberazioni",
          "concepts_de": ["Generalversammlung", "Beschlussanfechtung", "Art. 706 OR", "Mehrheitsmissbrauch"],
          "concepts_fr": ["Assemblée générale", "Annulation de décisions", "Abus de majorité"],
          "concepts_it": [],
          "statutes": ["Art. 698 OR", "Art. 706 OR"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur Anfechtung von GV-Beschlüssen: Voraussetzungen, Frist und Rechtsfolgen der actio pro socio.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "verantwortlichkeit",
      "name_de": "Organhaftung und Verantwortlichkeit",
      "name_fr": "Responsabilité des organes",
      "name_it": "Responsabilità degli organi",
      "statutes": ["Art. 754 OR", "Art. 756 OR"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 128 III 142",
          "actual_language": "de",
          "title_de": "Sorgfaltspflicht der Organe — Business Judgement",
          "title_fr": "Devoir de diligence des organes — business judgement",
          "title_it": "Dovere di diligenza degli organi",
          "concepts_de": ["Sorgfaltspflicht", "Verwaltungsrat", "Business Judgement Rule", "Art. 717 OR"],
          "concepts_fr": ["Devoir de diligence", "Conseil d'administration", "Business judgement rule"],
          "concepts_it": [],
          "statutes": ["Art. 717 OR", "Art. 754 OR"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Sorgfaltspflicht des Verwaltungsrats: Massstab und Überprüfungsbefugnis des Gerichts (business judgement).",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 132 III 564",
          "actual_language": "de",
          "title_de": "Verantwortlichkeitsklage — Schadensbeweis und Kausalität",
          "title_fr": "Action en responsabilité — preuve du dommage et causalité",
          "title_it": "Azione di responsabilità — prova del danno e causalità",
          "concepts_de": ["Verantwortlichkeitsklage", "Schaden", "Kausalität", "Art. 754 OR"],
          "concepts_fr": ["Action en responsabilité", "Dommage", "Causalité"],
          "concepts_it": [],
          "statutes": ["Art. 754 OR", "Art. 756 OR"],
          "difficulty": 4,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur aktienrechtlichen Verantwortlichkeitsklage: Schadensnachweis und Kausalität bei Massenverantwortlichkeit.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "BGE 102 II 224",
          "actual_language": "de",
          "title_de": "Konzernhaftung — Durchgriff und Haftungserstreckung",
          "title_fr": "Responsabilité du groupe — levée du voile corporatif",
          "title_it": "Responsabilità del gruppo — disregard of legal entity",
          "concepts_de": ["Konzernhaftung", "Durchgriff", "Haftungserstreckung", "Rechtsmissbrauch"],
          "concepts_fr": ["Responsabilité du groupe", "Levée du voile", "Abus de droit"],
          "concepts_it": [],
          "statutes": ["Art. 2 ZGB", "Art. 754 OR"],
          "difficulty": 4,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Konzernhaftung: Voraussetzungen des Durchgriffs und Haftungserstreckung auf Muttergesellschaft.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    },
    {
      "id": "gmbh_konzern",
      "name_de": "GmbH und Konzernrecht",
      "name_fr": "Sàrl et droit des groupes",
      "name_it": "Sagl e diritto dei gruppi",
      "statutes": ["Art. 717 OR", "Art. 772 OR"],
      "cases": [
        {
          "decision_id": "",
          "bge_ref": "BGE 140 III 533",
          "actual_language": "de",
          "title_de": "GmbH — Stammkapital und Kaduzierung nach neuem Recht",
          "title_fr": "Sàrl — capital social et déchéance selon nouveau droit",
          "title_it": "Sagl — capitale sociale e decadenza secondo nuovo diritto",
          "concepts_de": ["GmbH", "Stammkapital", "Kaduzierung", "Art. 777c OR"],
          "concepts_fr": ["Sàrl", "Capital social", "Déchéance"],
          "concepts_it": [],
          "statutes": ["Art. 772 OR", "Art. 777c OR"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Leitentscheid zur GmbH nach dem revidierten Recht (2008): Kaduzierung von Stammanteilen und Rechtsfolgen.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        },
        {
          "decision_id": "",
          "bge_ref": "RESOLVE_OR_717",
          "actual_language": "de",
          "title_de": "Treuepflicht des Verwaltungsrats — Interessenkonflikte",
          "title_fr": "Devoir de loyauté du CA — conflits d'intérêts",
          "title_it": "Dovere di fedeltà del CdA — conflitti d'interesse",
          "concepts_de": ["Treuepflicht", "Interessenkonflikt", "Verwaltungsrat", "Art. 717 Abs. 1 OR"],
          "concepts_fr": ["Devoir de loyauté", "Conflit d'intérêts", "Conseil d'administration"],
          "concepts_it": [],
          "statutes": ["Art. 717 OR"],
          "difficulty": 3,
          "prerequisites": [],
          "significance_de": "Grundsatzentscheid zur Treuepflicht: Umgang mit Interessenkonflikten im Verwaltungsrat und Offenlegungspflicht.",
          "significance_fr": "",
          "significance_it": "",
          "key_erwagungen": [],
          "reading_guide_de": "",
          "reading_guide_fr": "",
          "reading_guide_it": "",
          "socratic_questions": [],
          "hypotheticals": []
        }
      ]
    }
  ]
}
```

**Commit:**
```bash
git add study/curriculum/gesellschaftsrecht.json
git commit -m "feat: add gesellschaftsrecht curriculum (7 cases)"
```

---

## Task 6: Create zivilprozessrecht.json

**File:** `study/curriculum/zivilprozessrecht.json`

All 7 cases require `[find]` queries since ZPO is post-2011. Run:
```python
for article, query in [
    ("17", "Prorogation Gerichtsstand ZPO"),
    ("59", "Prozessvoraussetzungen Klage ZPO"),
    ("157", "Beweismass Glaubhaftmachung ZPO"),
    ("197", "Schlichtungsverfahren Klagebewilligung"),
    ("317", "neue Tatsachen echte Noven Berufung"),
    ("47", "Ausstandspflicht Richter ZPO"),
]:
    result = _find_leading_cases(law_code="ZPO", article=article, court="bge", limit=5)
    print(f"ZPO Art. {article}: {result.get('cases', [{}])[0].get('docket_number','?')}")

# BGG for Streitwertgrenze
print(_find_leading_cases(law_code="BGG", article="74", court="bge", limit=5))
```

Write `zivilprozessrecht.json` following the same schema with:
- `area_id`: `"zivilprozessrecht"`
- `area_de`: `"Zivilprozessrecht"`
- 3 modules: `zustandigkeit` (2 cases), `beweisrecht` (2 cases), `rechtsmittel` (3 cases)
- Thin metadata per case (significance_de required, socratic_questions/hypotheticals empty)

**Commit:**
```bash
git add study/curriculum/zivilprozessrecht.json
git commit -m "feat: add zivilprozessrecht curriculum (7 cases)"
```

---

## Task 7: Create strafprozessrecht.json

**File:** `study/curriculum/strafprozessrecht.json`

Specific BGE refs available for 3 cases; run `[find]` for remaining 4:
```python
for article, query in [
    ("9", "Anklagegrundsatz StPO"),
    ("5", "Beschleunigungsgebot StPO"),
    ("168", "Zeugnisverweigerungsrecht StPO"),
    ("319", "Einstellung Freispruch Abgrenzung StPO"),
]:
    print(_find_leading_cases(law_code="StPO", article=article, court="bge", limit=5))
```

- `area_id`: `"strafprozessrecht"`
- 3 modules: `grundsatze` (BGE 133 IV 329 + Anklagegrundsatz find), `zwangsmassnahmen` (BGE 137 IV 33 + Beschleunigung find), `beweise` (BGE 139 IV 179 + Zeugnisverweigerung find + Einstellung find)

**Commit:**
```bash
git add study/curriculum/strafprozessrecht.json
git commit -m "feat: add strafprozessrecht curriculum (7 cases)"
```

---

## Task 8: Create oeffentliches_prozessrecht.json

**File:** `study/curriculum/oeffentliches_prozessrecht.json`

Specific BGE refs: BGE 110 Ia 1, BGE 133 I 201, BGE 137 II 40, BGE 142 I 155, BGE 138 I 274. Run `[find]` for 2 slots:
```python
print(_find_leading_cases(law_code="BGG", article="89", court="bge", limit=5))  # Legitimation
print(_find_leading_cases(law_code="BV", article="29a", court="bge", limit=5))  # Rechtsweggarantie
```

- `area_id`: `"oeffentliches_prozessrecht"`
- 3 modules: `verwaltungsverfahren` (BGE 110 Ia 1 + Legitimation find), `beschwerde` (Rechtsweggarantie find + BGE 133 I 201), `kognition` (BGE 137 II 40 + BGE 142 I 155 + BGE 138 I 274)

**Step: After creating all 6 new files, run full test suite:**
```bash
python -m pytest tests/test_curriculum_100.py -v
```

Expected: ALL tests pass.

**Commit:**
```bash
git add study/curriculum/oeffentliches_prozessrecht.json
git commit -m "feat: add oeffentliches_prozessrecht curriculum (7 cases)"
git commit -m "feat: complete BGE-100 canonical curriculum — 100 cases across 14 areas"
```

---

## Task 9: Implement resolve_decision_ids.py

**Files:**
- Create: `tests/test_resolve_decision_ids.py`
- Create: `study/resolve_decision_ids.py`

### Step 1: Write failing tests

```python
# tests/test_resolve_decision_ids.py
"""Tests for the BGE ref → decision_id resolver."""
import json
import pytest
from unittest.mock import patch, MagicMock
from study.resolve_decision_ids import parse_bge_ref, build_fts_query, resolve_all


def test_parse_bge_ref_standard():
    result = parse_bge_ref("BGE 135 III 1")
    assert result == {"volume": "135", "collection": "III", "page": "1"}


def test_parse_bge_ref_two_digit_page():
    result = parse_bge_ref("BGE 84 II 122")
    assert result == {"volume": "84", "collection": "II", "page": "122"}


def test_parse_bge_ref_invalid():
    assert parse_bge_ref("not a bge ref") is None
    assert parse_bge_ref("") is None


def test_build_fts_query():
    query = build_fts_query("BGE 135 III 1")
    # Should produce a query that would match docket_number in FTS5
    assert "135" in query
    assert "III" in query


def test_resolve_all_skips_existing(tmp_path):
    """resolve_all should not overwrite non-empty decision_ids."""
    curriculum_dir = tmp_path / "curriculum"
    curriculum_dir.mkdir()
    data = {
        "area_id": "test",
        "area_de": "Test",
        "modules": [{
            "id": "mod1",
            "cases": [{
                "decision_id": "bge_already_resolved",
                "bge_ref": "BGE 135 III 1",
            }]
        }]
    }
    (curriculum_dir / "test.json").write_text(json.dumps(data))

    stats = resolve_all(curriculum_dir=str(curriculum_dir), db_path=":memory:")
    assert stats["already_set"] == 1
    assert stats["resolved"] == 0


def test_resolve_all_fills_blank(tmp_path):
    """resolve_all should fill blank decision_ids."""
    curriculum_dir = tmp_path / "curriculum"
    curriculum_dir.mkdir()
    data = {
        "area_id": "test",
        "area_de": "Test",
        "modules": [{
            "id": "mod1",
            "cases": [{
                "decision_id": "",
                "bge_ref": "BGE 135 III 1",
            }]
        }]
    }
    (curriculum_dir / "test.json").write_text(json.dumps(data))

    mock_result = [{"decision_id": "bge_135 III 1", "docket_number": "135 III 1"}]

    with patch("study.resolve_decision_ids._query_db", return_value=mock_result):
        stats = resolve_all(curriculum_dir=str(curriculum_dir), db_path=":memory:")

    assert stats["resolved"] == 1
    updated = json.loads((curriculum_dir / "test.json").read_text())
    assert updated["modules"][0]["cases"][0]["decision_id"] == "bge_135 III 1"
```

### Step 2: Run to verify failure

```bash
python -m pytest tests/test_resolve_decision_ids.py -v 2>&1 | head -20
```
Expected: ImportError — `study.resolve_decision_ids` does not exist.

### Step 3: Implement the script

```python
# study/resolve_decision_ids.py
"""Map BGE refs in curriculum JSON files to actual decision_ids in the FTS5 DB.

Usage:
    python -m study.resolve_decision_ids [--db PATH] [--dry-run]

Walks all curriculum JSON files, queries the FTS5 DB for each blank decision_id,
writes resolved IDs back in place. Non-destructive: never overwrites existing IDs.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

CURRICULUM_DIR = Path(__file__).resolve().parent / "curriculum"
DEFAULT_DB = Path(__file__).resolve().parent.parent / "output" / "swiss_caselaw_fts5.db"

_BGE_PATTERN = re.compile(
    r"BGE\s+(\d+)\s+(I{1,3}V?|VI?|IV)\s+(\d+)", re.IGNORECASE
)


def parse_bge_ref(bge_ref: str) -> dict[str, str] | None:
    """Parse 'BGE 135 III 1' → {'volume': '135', 'collection': 'III', 'page': '1'}."""
    m = _BGE_PATTERN.search(bge_ref or "")
    if not m:
        return None
    return {"volume": m.group(1), "collection": m.group(2).upper(), "page": m.group(3)}


def build_fts_query(bge_ref: str) -> str:
    """Build FTS5 query string for a BGE ref."""
    parts = parse_bge_ref(bge_ref)
    if not parts:
        return ""
    return f'"{parts["volume"]} {parts["collection"]} {parts["page"]}"'


def _query_db(db_path: str, bge_ref: str) -> list[dict[str, Any]]:
    """Query FTS5 DB for a BGE ref. Returns list of matching rows."""
    parts = parse_bge_ref(bge_ref)
    if not parts:
        return []
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        # Search docket_number for pattern like "135 III 1"
        pattern = f"%{parts['volume']} {parts['collection']} {parts['page']}%"
        cur.execute(
            "SELECT decision_id, docket_number FROM decisions "
            "WHERE court = 'bge' AND docket_number LIKE ? LIMIT 3",
            (pattern,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        con.close()
        return rows
    except sqlite3.OperationalError:
        return []


def resolve_all(
    *,
    curriculum_dir: str | None = None,
    db_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Resolve blank decision_ids in all curriculum JSON files.

    Returns stats dict with keys: resolved, not_found, already_set, errors.
    """
    cdir = Path(curriculum_dir) if curriculum_dir else CURRICULUM_DIR
    dbp = db_path if db_path is not None else str(DEFAULT_DB)
    stats: dict[str, int] = {
        "resolved": 0, "not_found": 0, "already_set": 0, "errors": 0,
    }

    for json_path in sorted(cdir.glob("*.json")):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  ERROR reading {json_path.name}: {e}")
            stats["errors"] += 1
            continue

        changed = False
        for mod in data.get("modules", []):
            for case in mod.get("cases", []):
                if case.get("decision_id"):
                    stats["already_set"] += 1
                    continue
                bge_ref = case.get("bge_ref", "")
                if not bge_ref or bge_ref.startswith("RESOLVE_"):
                    stats["not_found"] += 1
                    print(f"  SKIP (placeholder): {bge_ref}")
                    continue
                rows = _query_db(dbp, bge_ref)
                if rows:
                    did = rows[0]["decision_id"]
                    print(f"  RESOLVED: {bge_ref} → {did}")
                    if not dry_run:
                        case["decision_id"] = did
                    stats["resolved"] += 1
                    changed = True
                else:
                    print(f"  NOT FOUND: {bge_ref}")
                    stats["not_found"] += 1

        if changed and not dry_run:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve BGE refs to decision_ids")
    parser.add_argument("--db", default=None, help="Path to FTS5 DB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    stats = resolve_all(db_path=args.db, dry_run=args.dry_run)
    print(f"\nSummary: {stats}")


if __name__ == "__main__":
    main()
```

### Step 4: Run tests

```bash
python -m pytest tests/test_resolve_decision_ids.py -v
```
Expected: ALL PASS.

### Step 5: Commit

```bash
git add study/resolve_decision_ids.py tests/test_resolve_decision_ids.py
git commit -m "feat: add resolve_decision_ids.py — maps BGE refs to DB decision_ids"
```

---

## Task 10: Implement enrich_curriculum.py

**Files:**
- Create: `tests/test_enrich_curriculum.py`
- Create: `study/enrich_curriculum.py`

### Step 1: Write failing tests

```python
# tests/test_enrich_curriculum.py
"""Tests for the Anthropic-API-based curriculum enrichment script."""
import json
import pytest
from unittest.mock import patch, MagicMock
from study.enrich_curriculum import (
    build_enrichment_prompt,
    parse_enrichment_response,
    needs_enrichment,
)
from study.curriculum_engine import CurriculumCase


def _make_case(**kwargs) -> CurriculumCase:
    defaults = dict(
        decision_id="bge_135 III 1",
        bge_ref="BGE 135 III 1",
        significance_de="Test significance.",
        statutes=["Art. 1 OR"],
        difficulty=3,
        key_erwagungen=["2", "3"],
    )
    defaults.update(kwargs)
    return CurriculumCase(**defaults)


def test_needs_enrichment_true():
    case = _make_case(socratic_questions=[], hypotheticals=[])
    assert needs_enrichment(case) is True


def test_needs_enrichment_false_when_full():
    case = _make_case(
        socratic_questions=[{"level": 1, "question": "Q?", "model_answer": "A."}],
        hypotheticals=[{"scenario": "S", "likely_outcome_shift": "O"}],
    )
    assert needs_enrichment(case) is False


def test_needs_enrichment_false_when_no_decision_id():
    case = _make_case(decision_id="", socratic_questions=[], hypotheticals=[])
    assert needs_enrichment(case) is False


def test_build_prompt_contains_required_fields():
    case = _make_case()
    decision_text = "Sachverhalt: ... Erwägungen: 2. ... 3. ..."
    prompt = build_enrichment_prompt(case, decision_text=decision_text)
    assert "BGE 135 III 1" in prompt
    assert "Art. 1 OR" in prompt
    assert "socratic_questions" in prompt
    assert "hypotheticals" in prompt
    assert "model_answer" in prompt


def test_parse_enrichment_response_valid():
    response = json.dumps({
        "socratic_questions": [
            {"level": i, "level_label": f"L{i}", "question": f"Q{i}?",
             "hint": "hint", "model_answer": f"A{i}."}
            for i in range(1, 6)
        ],
        "hypotheticals": [
            {"type": "add_complication", "scenario": "S1",
             "discussion_points": ["D1"], "likely_outcome_shift": "O1"},
            {"type": "swap_parties", "scenario": "S2",
             "discussion_points": ["D2"], "likely_outcome_shift": "O2"},
        ],
        "reading_guide_de": "Lesen Sie E. 2.",
        "reading_guide_fr": "Lisez le considérant 2.",
        "reading_guide_it": "",
        "key_erwagungen": ["2", "3"],
        "significance_fr": "Arrêt de principe.",
        "significance_it": "",
    })
    result = parse_enrichment_response(response)
    assert len(result["socratic_questions"]) == 5
    assert len(result["hypotheticals"]) == 2
    assert result["reading_guide_de"] == "Lesen Sie E. 2."


def test_parse_enrichment_response_invalid_json():
    with pytest.raises(ValueError, match="Invalid JSON"):
        parse_enrichment_response("not json")


def test_parse_enrichment_response_wrong_question_count():
    response = json.dumps({
        "socratic_questions": [{"level": 1, "question": "Q?", "model_answer": "A."}],
        "hypotheticals": [],
    })
    with pytest.raises(ValueError, match="5 socratic questions"):
        parse_enrichment_response(response)
```

### Step 2: Run to verify failure

```bash
python -m pytest tests/test_enrich_curriculum.py -v 2>&1 | head -20
```
Expected: ImportError.

### Step 3: Implement

```python
# study/enrich_curriculum.py
"""Batch-generate Socratic Q&A and hypotheticals for curriculum cases.

Usage:
    python -m study.enrich_curriculum [--area AREA_ID] [--dry-run]

Requires: ANTHROPIC_API_KEY env var.
For each case missing socratic_questions or hypotheticals:
  1. Fetches full decision text from DB
  2. Calls Claude to generate metadata
  3. Writes results back to curriculum JSON
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from study.curriculum_engine import CurriculumCase, load_curriculum, CURRICULUM_DIR

ENRICHMENT_PROMPT_TEMPLATE = """\
You are enriching a Swiss law school curriculum entry for the landmark decision {bge_ref}.

CASE METADATA:
- BGE reference: {bge_ref}
- Legal area: {area_id} / {module_id}
- Key statutes: {statutes}
- Significance: {significance_de}
- Key Erwägungen: {key_erwagungen}
- Difficulty: {difficulty}/5

DECISION TEXT (excerpt):
{decision_text}

Generate the following JSON object (no markdown, pure JSON):
{{
  "socratic_questions": [
    // Exactly 5 entries, one per Bloom level (1=Verständnis, 2=Regelidentifikation,
    // 3=Anwendung, 4=Analyse, 5=Bewertung). Each entry:
    {{
      "level": <1-5>,
      "level_label": "<label in German>",
      "question": "<case-specific question in German>",
      "hint": "<where to look in the decision>",
      "model_answer": "<2-4 sentence answer in German>"
    }}
  ],
  "hypotheticals": [
    // Exactly 2 entries, types: "add_complication" and "swap_parties"
    {{
      "type": "<type>",
      "scenario": "<what-if scenario in German>",
      "discussion_points": ["<point 1>", "<point 2>"],
      "likely_outcome_shift": "<how outcome changes, 2-3 sentences>"
    }}
  ],
  "reading_guide_de": "<2-3 sentence reading guide in German pointing to key Erwägungen>",
  "reading_guide_fr": "<same in French>",
  "reading_guide_it": "<same in Italian, can be empty string>",
  "key_erwagungen": [<list of Erwägung numbers as strings, e.g. "2", "3.1">],
  "significance_fr": "<1-sentence significance in French>",
  "significance_it": "<1-sentence significance in Italian, can be empty>"
}}
"""


def needs_enrichment(case: CurriculumCase) -> bool:
    """Return True if the case is missing metadata and has a resolved decision_id."""
    if not case.decision_id:
        return False
    missing_questions = not case.socratic_questions
    missing_hypotheticals = not case.hypotheticals
    return missing_questions or missing_hypotheticals


def build_enrichment_prompt(case: CurriculumCase, *, decision_text: str) -> str:
    """Build the enrichment prompt for a case."""
    text_excerpt = decision_text[:6000] if decision_text else "(text unavailable)"
    return ENRICHMENT_PROMPT_TEMPLATE.format(
        bge_ref=case.bge_ref,
        area_id=case.area_id,
        module_id=case.module_id,
        statutes=", ".join(case.statutes) or "–",
        significance_de=case.significance_de or "–",
        key_erwagungen=", ".join(case.key_erwagungen) or "–",
        difficulty=case.difficulty,
        decision_text=text_excerpt,
    )


def parse_enrichment_response(response_text: str) -> dict[str, Any]:
    """Parse and validate the Claude response. Raises ValueError on invalid shape."""
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from enrichment response: {e}") from e

    questions = data.get("socratic_questions", [])
    if len(questions) != 5:
        raise ValueError(
            f"Expected 5 socratic questions, got {len(questions)}"
        )
    for q in questions:
        if not q.get("model_answer"):
            raise ValueError(f"Socratic question missing model_answer: {q}")

    return data


def _fetch_decision_text(decision_id: str, db_path: str) -> str:
    """Fetch full_text from FTS5 DB for a decision_id."""
    import sqlite3
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cur = con.cursor()
        cur.execute(
            "SELECT full_text, regeste FROM decisions WHERE decision_id = ?",
            (decision_id,),
        )
        row = cur.fetchone()
        con.close()
        if row:
            return (row[1] or "") + "\n\n" + (row[0] or "")
    except Exception:
        pass
    return ""


def _call_claude(prompt: str, api_key: str) -> str:
    """Call Claude API and return the text response."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def enrich_all(
    *,
    area: str | None = None,
    dry_run: bool = False,
    rate_limit_sleep: float = 0.35,
    db_path: str | None = None,
) -> dict[str, int]:
    """Enrich all curriculum cases missing metadata."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not dry_run:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    from pathlib import Path as P
    dbp = db_path or str(
        P(__file__).resolve().parent.parent / "output" / "swiss_caselaw_fts5.db"
    )

    stats = {"enriched": 0, "skipped": 0, "errors": 0}
    areas = load_curriculum(area=area)

    for curr_area in areas:
        json_path = CURRICULUM_DIR / f"{curr_area.area_id}.json"
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except OSError:
            continue

        changed = False
        for mod_data, mod in zip(data["modules"], curr_area.modules):
            for case_data, case in zip(mod_data["cases"], mod.cases):
                if not needs_enrichment(case):
                    stats["skipped"] += 1
                    continue

                print(f"  Enriching {case.bge_ref} ({curr_area.area_id}/{mod.id})...")
                decision_text = _fetch_decision_text(case.decision_id, dbp)
                prompt = build_enrichment_prompt(case, decision_text=decision_text)

                if dry_run:
                    print(f"    [DRY RUN] Would call API for {case.bge_ref}")
                    stats["skipped"] += 1
                    continue

                try:
                    response = _call_claude(prompt, api_key)
                    enrichment = parse_enrichment_response(response)
                    # Write back to case_data dict
                    case_data["socratic_questions"] = enrichment["socratic_questions"]
                    case_data["hypotheticals"] = enrichment["hypotheticals"]
                    if enrichment.get("reading_guide_de"):
                        case_data["reading_guide_de"] = enrichment["reading_guide_de"]
                    if enrichment.get("reading_guide_fr"):
                        case_data["reading_guide_fr"] = enrichment["reading_guide_fr"]
                    if enrichment.get("reading_guide_it"):
                        case_data["reading_guide_it"] = enrichment["reading_guide_it"]
                    if enrichment.get("key_erwagungen"):
                        case_data["key_erwagungen"] = enrichment["key_erwagungen"]
                    if enrichment.get("significance_fr"):
                        case_data["significance_fr"] = enrichment["significance_fr"]
                    if enrichment.get("significance_it"):
                        case_data["significance_it"] = enrichment["significance_it"]
                    changed = True
                    stats["enriched"] += 1
                    print(f"    OK — {len(enrichment['socratic_questions'])} Qs, "
                          f"{len(enrichment['hypotheticals'])} hyps")
                    time.sleep(rate_limit_sleep)
                except (ValueError, Exception) as e:
                    print(f"    ERROR: {e}")
                    stats["errors"] += 1

        if changed and not dry_run:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print(f"  Saved {json_path.name}")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich curriculum metadata via Claude API")
    parser.add_argument("--area", default=None, help="Restrict to one area_id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", default=None)
    args = parser.parse_args()
    stats = enrich_all(area=args.area, dry_run=args.dry_run, db_path=args.db)
    print(f"\nSummary: {stats}")


if __name__ == "__main__":
    main()
```

### Step 4: Run tests

```bash
python -m pytest tests/test_enrich_curriculum.py -v
```
Expected: ALL PASS.

### Step 5: Commit

```bash
git add study/enrich_curriculum.py tests/test_enrich_curriculum.py
git commit -m "feat: add enrich_curriculum.py — batch Anthropic API enrichment for curriculum cases"
```

---

## Task 11: Final Validation and VPS Deployment

### Step 1: Run full test suite

```bash
python -m pytest tests/test_curriculum_100.py tests/test_resolve_decision_ids.py \
    tests/test_enrich_curriculum.py tests/test_study_curriculum.py \
    tests/test_study_tools.py -v
```

Expected: ALL PASS (100+ tests).

### Step 2: Run resolver locally (optional smoke test)

```bash
python -m study.resolve_decision_ids --dry-run 2>&1 | tail -20
```

### Step 3: Push and deploy to VPS

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && git pull --rebase origin main && \
   python3 -m study.resolve_decision_ids && \
   echo "Resolver done" && \
   systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

### Step 4: Run enricher on VPS (Phase 3)

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && \
   export ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY .env.mcp | cut -d= -f2) && \
   nohup python3 -m study.enrich_curriculum >> logs/enrich_curriculum.log 2>&1 &'
```

Monitor:
```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'tail -f /opt/caselaw/repo/logs/enrich_curriculum.log'
```

### Step 5: Verify via MCP tool

After enricher completes, test the curriculum via MCP:
```
list_study_curriculum → should show 14 areas, 100 cases total
study_leading_case topic="Betrug" → should resolve to BGE 124 IV 127
study_leading_case topic="Vinkulierung" → should resolve to BGE 136 III 278
```

---

## Summary

| Phase | Deliverable | Cases added |
|-------|-------------|------------|
| Task 1 | Validation test suite | — |
| Task 2 | 8 updated existing JSONs | +18 new, 2 replaced |
| Tasks 3–8 | 6 new area JSONs | +42 |
| Task 9 | resolve_decision_ids.py | — |
| Task 10 | enrich_curriculum.py | — |
| Task 11 | VPS deployment + enrichment | — |
| **Total** | | **100 cases, 14 areas** |
