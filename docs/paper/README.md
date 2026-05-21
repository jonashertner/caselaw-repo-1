# OpenCaseLaw papers

This directory contains the OpenCaseLaw paper history and the current
two-paper split.

## Current papers

### `p1-resource/` — Paper 1 (resource release) — READY TO SHIP

**"OpenCaseLaw: An Open Multilingual Citation Graph for Swiss Jurisprudence"**

The focused resource paper. Killer empirical fact: **34.0% of resolved
decision-to-decision citations in Swiss jurisprudence span two different
official languages** — a structural property never measured before
because no resolved cross-court Swiss citation graph existed.

Substantive scope:
- The corpus (972,882 decisions, 109 courts, DE/FR/IT, 28 layers)
- The resolved citation graph (8.05M edges, 92.9% resolution, pin-cite resolver)
- The statute graph + Materialien bridge (first open programmatic
  link from Swiss case law to Federal Council messages)
- cli:ch + ECLI layered identifier
- RFC-6962 Merkle root + OpenTimestamps Bitcoin anchor + per-decision
  inclusion-proof API
- Live infrastructure + 4-layer dataset health

Snapshot: 2026-05-21.

### `p2-eval/` — Paper 2 (evaluation diagnostics) — IN PREPARATION

**Working title**: *"Cross-Lingual Retrieval and Verification Diagnostics
for Swiss Legal RAG"*

Scope (planned):
- Cross-lingual retrieval diagnostic with multi-annotator IAA (n≥200)
- Five-rail closing audit with adversarial probes against each rail
- Human calibration of the LLM judge in the grounding rail (R5)
- Statistical analysis layer (Wilson CIs, McNemar, bootstrap, permutation)
- Prior-only vs RAG-augmented bench at n≥200

Blockers for ship:
1. Multi-annotator panel (3–5 paid Swiss lawyers, DE/FR/IT native)
2. Lawyer-authored held-out cross-lingual queries
3. Human-graded calibration set for the grounding judge
4. Annotation pass over the 400-sample citation-precision audit
5. Expanded RAG bench from n=30 to n≥200

Status: framework code shipped (`benchmarks/swiss_legal_rag_bench/statistical_analysis.py`
and `benchmarks/audit_rails/`); evaluation passes pending budgeted human work.

## Why two papers

The monolithic v3 draft tried to be both a resource paper and an
evaluation paper. After the v1.2 review (2026-05-21):

- The resource contributions (corpus, graph, Materialien bridge, identifier,
  provenance) are publication-quality TODAY.
- The evaluation contributions (cross-lingual diagnostic at n=150,
  RAG bench at n=30, audit-rail framework) need v2.0 work to be
  statistically defensible against peer review. The McNemar test on
  the n=30 bench returned p=0.065 — below conventional significance.

Splitting clarifies what each contribution is and lets each be defended
at the right venue. The resource paper has a clean story; the evaluation
paper can be developed without rushing the multi-annotator work.

## History

### `v3/` — Monolithic v1.1/v1.2 draft (preserved for history)

27 pages, 12 sections, 5 appendices. Last revision 2026-05-21. Replaced
by the two-paper split above; preserved so reviewers can see the
predecessor and what got cut/promoted.

### `v2/` — Earlier draft (preserved for history)

Pre-v3 structure. Preserved as artefacts only.
