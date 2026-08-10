# Design: Per-statute preparatory materials (Botschaften + Erläuterungsberichte) from Fedlex

**Status:** Design (approved 2026-06-04) — pending spec review → implementation plan
**Owner:** Jonas Hertner / OpenCaseLaw
**Component:** `materialien.db` corpus + Fedlex materialien pipeline + MCP materials tools

---

## 1. Problem & goal

Give users, for **every federal statute** (by SR number, optionally by article), authoritative access to its **preparatory materials as Fedlex links them**:

- **Acts** (Bundesgesetze / Bundesbeschlüsse, passed by Parliament) → **Botschaft** (Federal Council message, in the Bundesblatt) + (future) parliamentary debates.
- **Ordinances** (Verordnungen, issued by the Federal Council / departments) → **Erläuterungsbericht** (explanatory report, via the Vernehmlassung / consultation).

Materials must be **verbatim and quotable** (R1–R3): full text in-corpus, FTS-searchable, with pinpoint provenance — not links-only and not LLM digests.

### Success criteria
- Given an SR (+article), a user/agent gets the right material type for that statute, verbatim, with Fedlex provenance.
- Erläuterungsberichte are first-class: searchable + R1–R3-quotable, identical machinery to Botschaften.
- Coverage is honestly reported: near-complete for Acts; partial-but-Fedlex-traceable for Ordinances; explicit "no Fedlex-linked report found" where absent.

### Non-goals
- Active web-recovery of reports Fedlex doesn't reference (department-site hunting beyond Fedlex's outbound links). Decided against — provenance/brittleness cost.
- Parliamentary debates (AB/Amtliches Bulletin) — out of scope here (separate thread).
- Cantonal materials — federal only.

---

## 2. Decisions (locked in brainstorming)

1. **Scope:** comprehensive — ingest Erläuterungsberichte **and** build unified per-statute access.
2. **Coverage strategy:** **Fedlex-anchored + follow its links** — use Fedlex's consultation dataset as the authoritative SR↔report linkage, follow its outbound links to fetch the actual report PDF (even on admin.ch/dept hosts), record the Fedlex consultation as the provenance anchor.
3. **Architecture:** **Approach A** — unify into the existing `materialien.db` corpus (one corpus, one pipeline, one tool surface), additive + backward-compatible.
4. **Verbatim** ingestion (FTS-indexed), consistent with the materialien commitment.

---

## 3. Current state (what exists, reused)

- **`materialien.db`** (read by MCP with `?mode=ro&immutable=1`; rebuilt/maintained by `build_botschaft_corpus.py`, run by `opencaselaw-materialien.service` @ 04:30 UTC, single-writer DELETE-mode):
  - `botschaft_documents(botschaft_id, bbl_year, bbl_page, bbl_citation, eli_uri, title, publication_date, source_url, format∈{akoma-ntoso-xml,pdf}, language, page_count, text_hash, ingested_at; UNIQUE(bbl_year,bbl_page,language))`
  - `botschaft_paragraphs` + `botschaft_paragraphs_fts` (FTS5)
  - `article_botschaft_links(sr_number, article, botschaft_id→FK, relation∈{enacted,amended,considered}, evidence; PK(sr_number,article,botschaft_id,relation))`
- **Fedlex discovery** (`scrapers/fedlex_materialien.py`): `discover_acts`, `discover_amendment_acts`, `discover_fga_botschaften` (typeDocument=23), and **`discover_consultations`** — already yields `ConsultationRecord(eli_uri, sr_number, impacts_work_uri, status, consultation_id, realizations[])` via SPARQL (`?cons a jolux:Consultation; jolux:foreseenImpactToLegalResource ?work; historicalLegalId ?srNumber`). **The SR↔consultation link already exists in Fedlex.** A `Manifestation(file_url, format, language)` model exists for downloadable files.
- **MCP tools:** `get_materialien`, `search_materialien`, `search_botschaft`, `get_article_purpose`, `get_article_history` (Botschaft-only today).
- **The gap:** Erläuterungsberichte (ordinance reports) are not ingested; consultations are discoverable but their report *documents* aren't resolved/fetched.

---

## 4. Architecture

### 4.1 Data model — generalize `materialien.db` (additive, backward-compatible)
Introduce a **`material_type`** dimension ∈ {`botschaft`, `erlaeuterungsbericht`} (extensible: `vernehmlassung_ergebnis`, `ab_debatte` later).

- **`materials`** (generalized `botschaft_documents`): `material_id` PK, `material_type`, BBl-identity fields (`bbl_year/page/citation`) **nullable** (reports have none), `eli_uri` (Botschaft FGA URI *or* consultation eli_uri = the Fedlex anchor), `title`, `source_url`, `format`, `language`, `text_hash`, `ingested_at`. Type-specific identity carried in `eli_uri` + a nullable `consultation_id`.
- **`material_paragraphs`** + **`material_paragraphs_fts`** — span both types; add `material_type` as a filter column (UNINDEXED in FTS).
- **`material_sr_links`** (generalized `article_botschaft_links`): `sr_number`, `article` **nullable** (reports link at SR level; Botschaften keep article-level), `material_id`→FK, `material_type`, `relation`, `evidence`.
- **Backward-compat:** keep `botschaft_documents` / `botschaft_paragraphs(_fts)` / `article_botschaft_links` as **SQL views** (filtered `WHERE material_type='botschaft'`) so existing tool code and the durable-preserve logic keep working unchanged. Migration runs inside the build (transform the preserved rows; the corpus is ingest-and-preserve, not rebuilt from source).

Rationale: one coherent "preparatory materials" corpus; the FTS, verbatim, provenance, and preserve machinery (incl. the 2026-06-03 commit-before-DETACH fix) are reused; views protect every current reader on the live DB.

### 4.2 Discovery + ingest (Fedlex-anchored, follow links)
1. `discover_consultations()` → consultations with `eli_uri` + `sr_number` + `impacts_work_uri`. (Exists.)
2. **NEW — manifestation resolver:** per consultation, resolve its document set and pick the **Erläuterungsbericht** among them (draft ordinance vs **Erläuterungsbericht** vs Ergebnisbericht), by Fedlex document-type/title. Implementation mirrors the existing Act/Botschaft manifestation handling (the `Manifestation` model + the consultation's expressions/manifestations in jolux). *(Exact jolux predicate for consultation→documents is an implementation-research item — §7.)*
3. **Fetch + extract:** follow the manifestation `file_url` (Fedlex or admin.ch/dept host), fetch the PDF, extract text via `fitz`/`pdfplumber` (reused), paragraph-split, FTS-index.
4. **Store** as a `material_type='erlaeuterungsbericht'` row: provenance = {Fedlex consultation `eli_uri` (anchor), source PDF `source_url`}; link to SR via `material_sr_links` (article NULL unless the report is article-structured).
5. **Durability:** idempotent via `text_hash`; runs in `build_botschaft_corpus.py` / the 04:30 service; preserve-safe.

### 4.3 Act-vs-Ordinance routing ("for each statute")
- Determine each SR's legal form (Act vs Ordinance) from `statutes.db` (legal-form metadata) or Fedlex `typeDocument`. *(Confirm the source field — §7.)*
- Acts → Botschaft links (existing); Ordinances → Erläuterungsbericht links (consultation→SR). A unified resolver returns the right material(s) per SR(/article), with the type labelled.

### 4.4 Access affordance (extend existing tools — no new tool, YAGNI)
- **`get_law(sr, article)`** → add a **`preparatory_materials`** section: the Botschaft (Acts) or Erläuterungsbericht (Ordinances) for that SR/article — verbatim excerpt + Fedlex provenance + link.
- **`get_materialien` / `get_article_purpose`** → made **type-aware** (already serve Botschaften; now also reports).
- **`search_materialien`** → searches both types with a `material_type` filter; **`search_botschaft`** kept as a Botschaft-filtered alias (compat).
- **R1–R3:** verbatim text from the corpus; Fedlex-cited provenance; never model-constructed.

---

## 5. Coverage & honesty
- Acts/Botschaften: near-complete (Fedlex/BBl is comprehensive).
- Ordinances/Erläuterungsberichte: **partial** — only where a Fedlex consultation exists *and* a report manifestation is resolvable. The affordance returns an explicit "no Fedlex-linked report found" for the rest; a coverage report (SR with ≥1 material, split Act/Ordinance) ships with Phase 2.

## 6. Rollout (each phase pipeline-gated → explicit approval + test against a data copy, per CLAUDE.md invariant #5)
1. **Schema migration** — additive generalization + compat views. Ships first, no behavior change. Test: existing tools unaffected on a DB copy.
2. **Erläuterungsbericht ingest** — manifestation resolver + fetch/extract/index; one-time backfill, then 04:30 delta. Ships with the coverage report.
3. **Access generalization** — type-aware `get_materialien`/`get_article_purpose`/`search_materialien` + `preparatory_materials` in `get_law`.

## 7. Open technical questions (resolve during implementation)
- Exact jolux predicate chain from `jolux:Consultation` → its documents → `Manifestation` file URLs (mirror `discover_acts`' manifestation resolution).
- How to classify a consultation's documents into {draft ordinance, Erläuterungsbericht, Ergebnisbericht} — Fedlex document-type vocabulary vs. title heuristics.
- Authoritative Act-vs-Ordinance legal-form field (`statutes.db` column vs Fedlex `typeDocument`).
- Article-level linking for reports that *are* article-structured (optional enrichment; default SR-level).

## 8. Invariants respected
- R1–R3 (verbatim + cited); `immutable=1` read side (compat views keep readers intact); atomic-swap + durable-preserve (incl. the commit-before-DETACH fix); pipeline gate (approval + data-copy testing for `build_botschaft_corpus.py` / schema / the service).

## 9. Testing
- Schema: unit test the migration + views on a copy; assert existing Botschaft queries unchanged.
- Ingest: golden-fixture test for the manifestation resolver + PDF extraction on a sample consultation; idempotency (re-run = no dupes) via `text_hash`.
- Access: tool tests asserting the right `material_type` per SR (an Act and an Ordinance), verbatim excerpt + provenance present, "not found" path.
- Coverage report sanity vs. Fedlex counts.
