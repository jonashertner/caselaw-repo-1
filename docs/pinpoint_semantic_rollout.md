# Pinpoint semantic-rescue rollout

Adding multilingual embedding-based semantic similarity as a fallback
to the lexical pinpoint resolver. Closes the lexical ceiling identified
in the deep empirical investigation: **even the official Regeste of a
decision doesn't lexically match its own Erwägungen** (vocabularies
diverge between summary and reasoning), so BM25 alone has a hard
recall ceiling on Regeste-style claims.

## Architecture (already shipped, commit `97790c5`)

```
                ┌──────────────────────────────────────────────┐
                │ User claim                                   │
                └──────────────────────────────────────────────┘
                                  │
                                  ▼
                ┌──────────────────────────────────────────────┐
                │ _compute_pinpoint(decision_id, claim)        │
                │                                              │
                │   1. Two-pass FTS5 (phrase → OR fallback)    │
                │   2. Shared scorer (gap + coverage + abs)    │
                │   3. If lexical confident → return           │
                │      source="lexical" + "high"|"medium"      │
                │                                              │
                │   ──── lexical returned None ────            │
                │                                              │
                │   4. _compute_pinpoint_semantic_rescue:      │
                │      • encode claim (~30 ms CPU)             │
                │      • cosine vs ALL paragraphs of decision  │
                │      • if cosine ≥ 0.70 → high               │
                │        if cosine ≥ 0.55 → medium             │
                │        else → None                           │
                │      • returns source="semantic"             │
                └──────────────────────────────────────────────┘
```

**Safety properties** (pinned by `tests/test_pinpoint_semantic_rescue.py`):
- Semantic NEVER overrides confident lexical match (lexical-winner test)
- Semantic suppressed below cos 0.55 floor (no spurious matches)
- Graceful no-op when feature flag off / model load fails / DB missing
- Source-tagged so callers can hedge differently for semantic-only matches

## Empirical proof (2026-05-10, on real production data)

Encoded 19 paragraphs across 3 known-FN BGE decisions. The same claims
that returned `(none)` from lexical alone now surface via semantic:

| BGE | Lexical | Semantic Rescue |
|---|---|---|
| BGE 134 V 231 (UVG/fMRT)        | `(none)` | **E.5.2 medium cos=0.69** |
| BGE 140 III 86 (FR procedure)   | `(none)` | `(none)` (cross-lang gap) |
| BGE 133 III 121 (civil)         | `(none)` | **E.4.1.2 medium cos=0.70** |

E.5.2 in BGE 134 V 231 IS the paragraph beginning "Bei der funktionellen
Magnetresonanztomographie (fMRT; englisch: functional magnetic resonance
imaging…)" — a perfect semantic match for "Beweiswert diagnostischer
Methoden funktionelle Magnetresonanztomographie".

## Encoding the corpus (in progress, started 2026-05-10 07:51 UTC)

```bash
# Started on VPS:
nohup nice -n 19 ionice -c 3 \
  env OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 PYTHONUNBUFFERED=1 \
  python3 -m search_stack.build_paragraph_embeddings \
    --structure-db /opt/caselaw/repo/output/decision_structure.db \
    --output-db   /mnt/HC_Volume_104655575/output/paragraph_embeddings.db \
    --batch-size 128 --verbose \
  >> /var/log/encode_paragraphs.log 2>&1 &
```

- **Model**: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
  (117M params, 384-dim, supports DE/FR/IT/RM + 47 other languages)
- **Throughput**: ~33 docs/sec on shared CPU (will speed up post-publish)
- **ETA**: ~73 h (3 days) for 8.8 M paragraphs
- **Output size**: ~17 GB (BLOB + minor index overhead)
- **Resume-safe**: writes a watermark to `encode_progress` table every
  batch; restart from same offset on interruption.

### Monitoring

```bash
# Progress log (every batch)
tail -f /var/log/encode_paragraphs.log

# Encoded row count
ssh root@46.225.212.40 'python3 -c "
import sqlite3
c = sqlite3.connect(\"file:/mnt/HC_Volume_104655575/output/paragraph_embeddings.db?immutable=1\", uri=True)
n = c.execute(\"SELECT COUNT(*) FROM paragraph_embeddings\").fetchone()[0]
total = 8_832_631
print(f\"{n:,} / {total:,} = {n/total:.1%}\")"'

# DB size
ls -lh /mnt/HC_Volume_104655575/output/paragraph_embeddings.db

# Process state
ps -p $(pgrep -f build_paragraph_embeddings) -o pid,ni,%cpu,%mem,stat,etime,cmd
```

### Resume after interruption

The encoder is idempotent. Re-running the same command picks up from
the last watermark. To restart from offset 0, add `--restart`.

```bash
# Resume:
nohup nice -n 19 ionice -c 3 \
  env OMP_NUM_THREADS=4 PYTHONUNBUFFERED=1 \
  python3 -m search_stack.build_paragraph_embeddings \
    --structure-db /opt/caselaw/repo/output/decision_structure.db \
    --output-db   /mnt/HC_Volume_104655575/output/paragraph_embeddings.db \
    --batch-size 128 --verbose \
  >> /var/log/encode_paragraphs.log 2>&1 &
```

## Activation (after encoding completes)

When the row count reaches ~8.8 M (full corpus encoded):

```bash
# 1. Add to /opt/caselaw/repo/.env.mcp:
echo 'PINPOINT_SEMANTIC_ENABLED=true' >> /opt/caselaw/repo/.env.mcp

# 2. Restart MCP workers (~3 sec each, rolling restart for zero downtime)
systemctl restart 'mcp-server@8770' 'mcp-server@8771' \
                  'mcp-server@8772' 'mcp-server@8773'

# 3. Verify the rescue is firing
curl -s "https://mcp.opencaselaw.ch/api/relevant-erwaegung/bge_BGE_134_V_231\
?claim=Beweiswert%20diagnostischer%20Methoden%20funktionelle%20Magnetresonanztomographie&top_k=1" \
  | python3 -m json.tool
# Expect:  "source": "semantic", "confidence": "medium", e_number: "5.2"

# 4. Watch latency for the first day
journalctl -u mcp-server@8770 -f | grep -i pinpoint
```

## Deactivation (rollback)

If anything goes wrong, single change reverts to lexical-only behavior:

```bash
# Remove or set to false in /opt/caselaw/repo/.env.mcp
sed -i '/PINPOINT_SEMANTIC_ENABLED/d' /opt/caselaw/repo/.env.mcp
systemctl restart 'mcp-server@8770' 'mcp-server@8771' \
                  'mcp-server@8772' 'mcp-server@8773'
```

## Tunables (env vars)

| Var | Default | Purpose |
|---|---|---|
| `PINPOINT_SEMANTIC_ENABLED`            | `false` | Master flag |
| `PINPOINT_SEMANTIC_MODEL`              | `paraphrase-multilingual-MiniLM-L12-v2` | Encoder model |
| `PINPOINT_SEMANTIC_HIGH`               | `0.70` | Cosine threshold for "high" |
| `PINPOINT_SEMANTIC_MEDIUM`             | `0.55` | Cosine threshold for "medium" |
| `SWISS_CASELAW_PARAGRAPH_EMBEDDINGS_DB` | `<DATA_DIR>/paragraph_embeddings.db` | Embedding DB path |

The thresholds (0.70 / 0.55) were calibrated on a 3-decision empirical
sample. They may want re-tuning against a larger labeled bench once
encoding completes — see `tests/test_pinpoint_semantic_rescue.py` for
the threshold semantics.

## Open questions / future iterations

- **BGE-M3 upgrade**: 568M-param model with 8K context (vs MiniLM's 128
  token limit) would handle long Erwägungen without truncation and
  has stronger cross-lingual transfer. Worth re-encoding with BGE-M3
  if cross-language FNs (BGE 140 III 86 case) prove a real problem.
- **Paragraph-level result combination**: currently semantic and
  lexical compete; a true hybrid scorer (`α·BM25 + (1-α)·cosine`)
  could surface higher-confidence pinpoints when both agree.
- **Larger labeled bench**: 50–100 case Regeste-grounded test set
  would let us tune thresholds + measure FPR/recall properly.
- **Incremental encoding**: every nightly publish that adds new
  decisions should re-run the encoder (encoder is incremental — only
  encodes new paragraphs).
