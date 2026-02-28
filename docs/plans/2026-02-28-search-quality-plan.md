# Search Quality Upgrade — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable the existing cross-encoder re-ranker and expand the benchmark golden set from 16 to ~65 queries, so search quality can be measured and improved reliably.

**Architecture:** Two sequential tracks — (B) expand golden set first so Track A can be measured accurately, then (A) enable cross-encoder and benchmark. The cross-encoder code already exists in `mcp_server.py` (see `_apply_cross_encoder_boosts`, `_get_cross_encoder`); it just needs to be switched on via `SWISS_CASELAW_CROSS_ENCODER=true` in `.env.mcp` on the VPS.

**Tech Stack:** Python, SQLite FTS5, sentence-transformers CrossEncoder, `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` (multilingual MiniLM).

---

## Key files

| Path | Purpose |
|------|---------|
| `benchmarks/search_relevance_golden.json` | Golden query set — **the only file modified in Track B** |
| `benchmarks/run_search_benchmark.py` | Benchmark runner |
| `mcp_server.py:118-128` | `CROSS_ENCODER_ENABLED`, `CROSS_ENCODER_MODEL`, `CROSS_ENCODER_TOP_N`, `CROSS_ENCODER_WEIGHT` |
| `mcp_server.py:2873-2910` | `_apply_cross_encoder_boosts()` — re-ranks top-N by CE score |
| `mcp_server.py:2913-2934` | `_get_cross_encoder()` — lazy-loads CrossEncoder model |
| `/opt/caselaw/repo/.env.mcp` | VPS environment — **the only file modified in Track A** |

## Baseline

FTS5 + LLM expansion (VPS, k=10): **MRR=0.394, Recall=0.625, nDCG=0.698**

Run command:
```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db 2>&1 | grep -E "^(Search|MRR|Recall|nDCG|Hit|Latency)"'
```

---

## Track B — Expand Golden Query Set

### Task 1: Add ~50 new queries to `benchmarks/search_relevance_golden.json`

**File:** `benchmarks/search_relevance_golden.json`

No tests needed — correctness is validated in Task 2. Replace the `"queries"` array with the expanded list below. Keep all 16 existing queries unchanged; append the new ones starting at `q017`.

**Grades:** 3 = primary relevant case, 2 = relevant, 1 = marginally relevant.

Add the following entries to the `"queries"` array:

```json
{
  "id": "q017",
  "query": "Art. 41 OR Schadenersatz ausservertragliche Haftung",
  "tags": ["de", "liability", "statute"],
  "relevant": [
    {"decision_id": "bge_BGE_105_II_87", "grade": 3}
  ]
},
{
  "id": "q018",
  "query": "Art. 97 OR Haftung Vertragsverletzung Schaden",
  "tags": ["de", "liability", "statute"],
  "relevant": [
    {"decision_id": "bge_BGE_135_III_405", "grade": 3}
  ]
},
{
  "id": "q019",
  "query": "Art. 28 ZGB Persönlichkeitsschutz Verletzung",
  "tags": ["de", "personality", "statute"],
  "relevant": [
    {"decision_id": "bge_127 III 481", "grade": 3},
    {"decision_id": "bge_BGE_127_III_481", "grade": 2}
  ]
},
{
  "id": "q020",
  "query": "Art. 56 OR Tierhalterhaftung",
  "tags": ["de", "liability", "statute"],
  "relevant": [
    {"decision_id": "bge_142 III 433", "grade": 3},
    {"decision_id": "bge_131 III 115", "grade": 2},
    {"decision_id": "bge_BGE_110_II_136", "grade": 2}
  ]
},
{
  "id": "q021",
  "query": "Art. 336c OR Kündigungsschutz Krankheit Sperrfrist",
  "tags": ["de", "employment", "statute"],
  "relevant": [
    {"decision_id": "bge_115 V 437", "grade": 3}
  ]
},
{
  "id": "q022",
  "query": "BGE 127 III 481",
  "tags": ["de", "direct-lookup", "exact"],
  "relevant": [
    {"decision_id": "bge_127 III 481", "grade": 3},
    {"decision_id": "bge_BGE_127_III_481", "grade": 2}
  ]
},
{
  "id": "q023",
  "query": "BGE 115 IV 162",
  "tags": ["de", "direct-lookup", "exact"],
  "relevant": [
    {"decision_id": "bge_115 IV 162", "grade": 3},
    {"decision_id": "bge_BGE_115_IV_162", "grade": 2}
  ]
},
{
  "id": "q024",
  "query": "BGE 132 III 677",
  "tags": ["de", "direct-lookup", "exact"],
  "relevant": [
    {"decision_id": "bge_132 III 677", "grade": 3},
    {"decision_id": "bge_BGE_132_III_677", "grade": 2}
  ]
},
{
  "id": "q025",
  "query": "BVGE 2013/10",
  "tags": ["de", "direct-lookup", "asylum"],
  "relevant": [
    {"decision_id": "bvger_BVGE 2013_10", "grade": 3}
  ]
},
{
  "id": "q026",
  "query": "impôt sur la fortune évaluation fiscale immobilière",
  "tags": ["fr", "tax", "nl"],
  "relevant": [
    {"decision_id": "bge_BGE_144_II_313", "grade": 3},
    {"decision_id": "bge_BGE_143_II_65", "grade": 2}
  ]
},
{
  "id": "q027",
  "query": "entretien après divorce contribution conjoint",
  "tags": ["fr", "family", "nl"],
  "relevant": [
    {"decision_id": "bge_BGE_132_III_209", "grade": 3},
    {"decision_id": "bge_BGE_137_III_102", "grade": 2}
  ]
},
{
  "id": "q028",
  "query": "double imposition intercantonale impôt",
  "tags": ["fr", "tax"],
  "relevant": [
    {"decision_id": "bge_BGE_143_II_65", "grade": 3},
    {"decision_id": "bge_BGE_140_II_157", "grade": 2}
  ]
},
{
  "id": "q029",
  "query": "grève syndicat licenciement droit du travail",
  "tags": ["fr", "employment", "nl"],
  "relevant": [
    {"decision_id": "bger_4A_64_2018", "grade": 3}
  ]
},
{
  "id": "q030",
  "query": "danno morale responsabilità civile risarcimento",
  "tags": ["it", "liability", "nl"],
  "relevant": [
    {"decision_id": "ti_gerichte_12.2009.196", "grade": 3}
  ]
},
{
  "id": "q031",
  "query": "Asyl Eritrea abgelehnt Wegweisung BVGer",
  "tags": ["de", "asylum", "nl"],
  "relevant": [
    {"decision_id": "bvger_E-4286_2008", "grade": 3},
    {"decision_id": "bvger_E-2041_2013", "grade": 2},
    {"decision_id": "bvger_E-7414_2015", "grade": 1}
  ]
},
{
  "id": "q032",
  "query": "Kausalzusammenhang Unfall IV-Rente Unfallversicherung",
  "tags": ["de", "insurance", "nl"],
  "relevant": [
    {"decision_id": "bger_8C_720_2017", "grade": 3},
    {"decision_id": "bger_8C_568_2015", "grade": 2}
  ]
},
{
  "id": "q033",
  "query": "Lärmschutzverordnung Baubewilligung Immissionsgrenzwert",
  "tags": ["de", "construction", "nl"],
  "relevant": [
    {"decision_id": "zh_verwaltungsgericht_VB.2010.00061", "grade": 3},
    {"decision_id": "zh_verwaltungsgericht_VB.2022.00249", "grade": 2},
    {"decision_id": "zh_verwaltungsgericht_VB.2001.00187", "grade": 1}
  ]
},
{
  "id": "q034",
  "query": "Auslieferung Beschwerdekammer Strafrecht",
  "tags": ["de", "criminal", "nl"],
  "relevant": [
    {"decision_id": "bstger_RR.2012.25", "grade": 3},
    {"decision_id": "bstger_RR.2013.89", "grade": 2}
  ]
},
{
  "id": "q035",
  "query": "Notwehr Angriff straflos Strafrecht",
  "tags": ["de", "criminal"],
  "relevant": [
    {"decision_id": "bge_115 IV 162", "grade": 3},
    {"decision_id": "bge_BGE_115_IV_162", "grade": 2}
  ]
},
{
  "id": "q036",
  "query": "Pflichtteil Erbrecht Enterbung Testament",
  "tags": ["de", "inheritance", "nl"],
  "relevant": [
    {"decision_id": "bge_132 III 677", "grade": 3},
    {"decision_id": "bge_BGE_132_III_677", "grade": 2}
  ]
},
{
  "id": "q037",
  "query": "Stockwerkeigentum Beschlüsse Gemeinschaft Stimmrecht",
  "tags": ["de", "property"],
  "relevant": [
    {"decision_id": "bge_119 II 404", "grade": 3},
    {"decision_id": "bge_BGE_119_II_404", "grade": 2}
  ]
},
{
  "id": "q038",
  "query": "Vertragsirrtum Anfechtung Willensmangel",
  "tags": ["de", "contract"],
  "relevant": [
    {"decision_id": "bge_128 III 70", "grade": 3},
    {"decision_id": "bge_BGE_128_III_70", "grade": 2}
  ]
},
{
  "id": "q039",
  "query": "Persönlichkeitsverletzung Medien Privatsphäre",
  "tags": ["de", "personality", "nl"],
  "relevant": [
    {"decision_id": "bge_143 III 297", "grade": 3},
    {"decision_id": "bge_BGE_143_III_297", "grade": 2}
  ]
},
{
  "id": "q040",
  "query": "Datenschutz Auskunftsrecht Personendaten",
  "tags": ["de", "data-protection", "nl"],
  "relevant": [
    {"decision_id": "bge_127 III 481", "grade": 2},
    {"decision_id": "bge_133 V 359", "grade": 3}
  ]
},
{
  "id": "q041",
  "query": "vorläufige Aufnahme Flüchtling Art. 8 EMRK",
  "tags": ["de", "asylum", "human-rights"],
  "relevant": [
    {"decision_id": "bge_147 I 268", "grade": 3},
    {"decision_id": "bge_151 I 62", "grade": 2},
    {"decision_id": "bge_150 I 93", "grade": 1}
  ]
},
{
  "id": "q042",
  "query": "Mietrecht",
  "tags": ["de", "tenancy", "short"],
  "relevant": [
    {"decision_id": "bs_appellationsgericht_ZB.2020.42", "grade": 2}
  ]
},
{
  "id": "q043",
  "query": "Erbschaft Streit Familie",
  "tags": ["de", "inheritance", "short"],
  "relevant": [
    {"decision_id": "bge_132 III 677", "grade": 2}
  ]
},
{
  "id": "q044",
  "query": "Was ist der Unterschied zwischen ausservertraglicher und vertraglicher Haftung?",
  "tags": ["de", "liability", "nl", "concept-match"],
  "relevant": [
    {"decision_id": "bge_BGE_105_II_87", "grade": 3},
    {"decision_id": "bge_BGE_135_III_405", "grade": 2}
  ],
  "note": "Concept: query uses explanatory framing, decisions use technical OR terms"
},
{
  "id": "q045",
  "query": "Tierhalterhaftung Hund Biss Eigentümer",
  "tags": ["de", "liability", "concept-match"],
  "relevant": [
    {"decision_id": "bge_131 III 115", "grade": 3},
    {"decision_id": "bge_81 II 512", "grade": 3},
    {"decision_id": "bge_BGE_110_II_136", "grade": 2},
    {"decision_id": "bge_142 III 433", "grade": 2}
  ],
  "note": "Keyword variant of q014 — uses legal term 'Tierhalterhaftung' instead of 'Hundebiss'"
},
{
  "id": "q046",
  "query": "détenteur d'animal responsabilité chien morsure",
  "tags": ["fr", "liability", "cross-lingual", "concept-match"],
  "relevant": [
    {"decision_id": "bge_BGE_85_II_243", "grade": 3},
    {"decision_id": "bge_BGE_110_II_136", "grade": 2},
    {"decision_id": "bge_131 III 115", "grade": 2}
  ]
},
{
  "id": "q047",
  "query": "Fristlose Entlassung wichtiger Grund Arbeitnehmer",
  "tags": ["de", "employment"],
  "relevant": [
    {"decision_id": "bge_129 III 177", "grade": 3}
  ]
},
{
  "id": "q048",
  "query": "Scheidung Unterhalt Kinder Sorgerecht",
  "tags": ["de", "family", "nl"],
  "relevant": [
    {"decision_id": "bge_BGE_132_III_209", "grade": 2}
  ]
},
{
  "id": "q049",
  "query": "Finanzreferendum Verpflichtungskredit Volksrechte",
  "tags": ["de", "constitutional"],
  "relevant": [
    {"decision_id": "bge_151 I 32", "grade": 3}
  ]
},
{
  "id": "q050",
  "query": "Art. 8 EMRK Privatleben Familie Ausweisung",
  "tags": ["de", "human-rights", "statute"],
  "relevant": [
    {"decision_id": "bge_147 I 268", "grade": 3},
    {"decision_id": "bge_151 I 62", "grade": 2}
  ]
}
```

After appending, the file should have 50 total queries (q001–q050). The JSON structure is:
```json
{
  "version": 1,
  "description": "...",
  "queries": [ ... all 50 entries ... ]
}
```

**Step 1: Read the current file**

```bash
cat benchmarks/search_relevance_golden.json
```

**Step 2: Edit — append the new queries to the `"queries"` array before the closing `]`**

Use the Edit tool to insert the 34 new query objects (q017–q050) before the final `]` of the `"queries"` array.

**Step 3: Validate JSON is well-formed**

```bash
python3 -c "import json; data=json.load(open('benchmarks/search_relevance_golden.json')); print(f'OK: {len(data[\"queries\"])} queries')"
```

Expected: `OK: 50 queries`

**Step 4: Commit**

```bash
git add benchmarks/search_relevance_golden.json
git commit -m "feat: expand golden query set from 16 to 50 queries"
```

---

### Task 2: Push to VPS, run benchmark, establish new baseline

**Step 1: Push**

```bash
git push origin main
```

**Step 2: Pull on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase origin main'
```

**Step 3: Run benchmark on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db 2>&1 | grep -E "^(Search|MRR|Recall|nDCG|Hit|Latency|-)"'
```

**Step 4: Save results**

Record the new MRR@10, Recall@10, nDCG@10 numbers. These are the new baseline for Track A.

Note: some new queries may score 0 if those decisions rank below 10 — that is expected and is the point (they reveal gaps).

---

## Track A — Enable Cross-Encoder

### Task 3: Smoke-test CrossEncoder on VPS

**Step 1: Check sentence_transformers is installed**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'python3 -c "from sentence_transformers import CrossEncoder; m = CrossEncoder(\"cross-encoder/mmarco-mMiniLMv2-L12-H384-v1\"); scores = m.predict([(\"Kausalzusammenhang Unfall\", \"Unfallversicherung Rente IV\")]); print(\"OK score:\", scores[0])"'
```

Expected: prints `OK score: <float>` (takes ~30 s on first run to download model).

**If it fails with `ModuleNotFoundError: sentence_transformers`:**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'pip3 install sentence-transformers --break-system-packages'
```

Then re-run the smoke test.

**Step 2: Verify model file is cached**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'ls ~/.cache/huggingface/hub/ | grep mmarco'
```

Expected: a directory named `models--cross-encoder--mmarco-mMiniLMv2-L12-H384-v1` exists.

---

### Task 4: Enable cross-encoder in `.env.mcp` and benchmark

**Context:** The existing code in `mcp_server.py` reads `CROSS_ENCODER_ENABLED` from `SWISS_CASELAW_CROSS_ENCODER`. Default is `"0"` (disabled). The model and all logic are already implemented.

Current `.env.mcp` on VPS (verify with `cat /opt/caselaw/repo/.env.mcp`):
```
REMOTE_MODE=True
SWISS_CASELAW_DIR=/opt/caselaw/repo/output
ANTHROPIC_API_KEY=<key>
LLM_EXPANSION_ENABLED=true
SPARSE_SEARCH_ENABLED=false
SWISS_CASELAW_VECTOR_SEARCH=false
```

**Step 1: Add cross-encoder flag**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'echo "SWISS_CASELAW_CROSS_ENCODER=true" >> /opt/caselaw/repo/.env.mcp && cat /opt/caselaw/repo/.env.mcp'
```

**Step 2: Restart workers**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773 && echo "Restarted"'
```

**Step 3: Run benchmark**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db 2>&1 | grep -E "^(Search|MRR|Recall|nDCG|Hit|Latency|-)"'
```

**Step 4: Compare**

Compare against the Task 2 baseline (50-query set without cross-encoder).

| Metric | Baseline | With CE | Delta |
|--------|---------|---------|-------|
| MRR@10 | (from Task 2) | ? | ? |
| Recall@10 | (from Task 2) | ? | ? |
| nDCG@10 | (from Task 2) | ? | ? |
| Latency avg | (from Task 2) | ? | ? |

**Decision criteria:**
- If MRR ≥ baseline AND Recall ≥ baseline → keep enabled (done)
- If MRR improves but Recall regresses → try lowering `CROSS_ENCODER_TOP_N` (see Task 5)
- If both regress → disable and note for future investigation

---

### Task 5: Tune cross-encoder (only if Task 4 shows regression)

**Skip this task if Task 4 results are positive.**

The two tunable parameters are:
- `CROSS_ENCODER_TOP_N` (default 30): how many top-FTS5 candidates to re-rank. Try 15 and 50.
- `CROSS_ENCODER_WEIGHT` (default 1.4): how strongly CE score is added. Try 0.7 and 2.0.

**Step 1: Try lower TOP_N (15) to reduce noise**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   SWISS_CASELAW_CROSS_ENCODER=true SWISS_CASELAW_CROSS_ENCODER_TOP_N=15 \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db 2>&1 | grep -E "^(MRR|Recall|nDCG|Latency)"'
```

**Step 2: Try higher TOP_N (50)**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'cd /opt/caselaw/repo && SWISS_CASELAW_DIR=/opt/caselaw/repo/output \
   SWISS_CASELAW_CROSS_ENCODER=true SWISS_CASELAW_CROSS_ENCODER_TOP_N=50 \
   python3 benchmarks/run_search_benchmark.py \
   --db /opt/caselaw/repo/output/decisions.db 2>&1 | grep -E "^(MRR|Recall|nDCG|Latency)"'
```

**Step 3: Pick the best configuration and write it to `.env.mcp`**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'sed -i "s/^SWISS_CASELAW_CROSS_ENCODER_TOP_N=.*/SWISS_CASELAW_CROSS_ENCODER_TOP_N=<best_value>/" \
   /opt/caselaw/repo/.env.mcp || echo "SWISS_CASELAW_CROSS_ENCODER_TOP_N=<best_value>" >> /opt/caselaw/repo/.env.mcp'
```

**Step 4: Restart and confirm final benchmark numbers**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 \
  'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

---

### Task 6: Update memory with final results

Update `/Users/jonashertner/.claude/projects/-Users-jonashertner-caselaw-repo-1/memory/MEMORY.md`:

- Update the `## Search Quality` section with the new 50-query baseline numbers
- Add the cross-encoder result (enabled/disabled, final MRR/Recall/nDCG)
- Update `## Pending Tasks`

---

## Summary of what changes

| File | Change |
|------|--------|
| `benchmarks/search_relevance_golden.json` | 16 → 50 queries |
| `/opt/caselaw/repo/.env.mcp` | Add `SWISS_CASELAW_CROSS_ENCODER=true` (and optional TOP_N) |
| Memory file | Updated benchmark numbers |

No Python code changes needed — everything is already implemented.
