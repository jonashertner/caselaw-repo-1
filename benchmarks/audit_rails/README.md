# Audit-rail adversarial probes

A small synthetic test set that **adversarially stresses** the five-rail closing
audit, complementing the §8 end-to-end RAG bench which evaluates the audit on
*natural* drafts.

## Why

The paper §8 bench reports the audit's behaviour on the 30 drafts the
generator actually produced. That measures the rails in their native
operating condition — but does not tell us:

- **Does R1 catch a citation we know is fake?** (positive control)
- **Does R1 reject a citation we know is real?** (false-positive control)
- **What's the precision/recall of R1 in isolation?**

To know those answers we need probes with **ground-truth labels**. This
directory holds them.

## Files

- `R1_adversarial_probes_v1.jsonl` — 17 single-citation probes for the
  case-citation existence rail (R1). 5 positive (real citations that must
  pass), 12 negative (fabricated / mistyped / mis-attributed citations
  that must fail).

## Schema

```json
{
  "probe_id":   "R1-pos-001",
  "expected":   "pass | fail",
  "category":   "real_BGE | fabricated_BGE_volume | plausible_typo | ...",
  "draft_text": "free-form text containing exactly one citation under test",
  "rationale":  "human-readable reason this probe was constructed"
}
```

## Running

A runner is *not yet provided* — this is the v1.2 deliverable. The pattern
is:

```python
from mcp_server import _audit_case_citations  # internal function

with open("R1_adversarial_probes_v1.jsonl") as f:
    probes = [json.loads(l) for l in f]

confusion = {"tp": 0, "fp": 0, "tn": 0, "fn": 0}
for probe in probes:
    issues = _audit_case_citations(probe["draft_text"])
    flagged = bool(issues)
    is_fail = (probe["expected"] == "fail")
    if is_fail and flagged: confusion["tp"] += 1
    elif is_fail and not flagged: confusion["fn"] += 1
    elif not is_fail and flagged: confusion["fp"] += 1
    else: confusion["tn"] += 1

precision = confusion["tp"] / (confusion["tp"] + confusion["fp"])
recall    = confusion["tp"] / (confusion["tp"] + confusion["fn"])
```

## Roadmap

- v1.1: this set (R1, 17 probes)
- v1.2: extend to R2 (statute resolution), R3 (verbatim-quote matching),
  R4 (date sanity), R5 (proposition grounding)
- v1.3: grow to ~100 probes per rail with stratified categories
- v2.0: lawyer-authored probes with adversarial intent

## License

CC0 — same as the underlying OpenCaseLaw dataset.
