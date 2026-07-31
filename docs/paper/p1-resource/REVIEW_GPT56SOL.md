# External review — GPT-5.6-Sol (xhigh), 2026-07-31

**Provenance, stated plainly:** the review ran as a Codex task
(`gpt-5.6-sol`, reasoning effort xhigh) over the full manuscript, tables,
frozen snapshot JSON, bibliography, and repository context. The reviewer
completed its evidence audit and applied its prose revisions directly to
`paper.tex`, but the session stalled before writing its own findings memo.
This document reconstructs the findings from the reviewer's recorded
in-session statements (verbatim quotes below), with each finding then
verified and resolved by the maintainer loop. The reviewer's applied edits
were kept; every numeric claim was left untouched by the reviewer per its
own discipline ("I have deliberately left every disputed number unchanged").

The reviewer classed its findings in three groups:

> "The review will distinguish three classes clearly: true snapshot
> mismatches/omissions, claims supported only by non-frozen operational
> evidence, and defensible claims that only needed narrower wording."

## Major findings

### 1. Comparison table misstated Open Legal Data (fairness)
> "the comparison table marks Open Legal Data as having no live API,
> although its own paper and current site describe a REST/OpenAPI surface."

**Verified: correct.** Inherited from the 2026-05 draft.
**Resolution:** Table 1 now credits OLD with `yes (REST)` in the Live API
column. OpenCaseLaw's differentiators in the table remain cross-language
links, the statute graph, the interpretive bridges, and MCP+REST serving.

### 2. "Sessions" semantics overclaimed (telemetry honesty)
> "the telemetry's 'sessions' counter is not a unique-user or
> unique-session measure across transports; in the implementation it mixes
> SSE/Streamable-HTTP opens or requests."

**Verified: correct** (counter increments on transport opens and certain
requests; no cross-transport dedup).
**Resolution (applied by the reviewer, kept):** all occurrences relabelled
to "MCP transport opens / requests"; abstract, adoption table caption, and
caveats paragraph now state explicitly that the counter identifies neither
people nor deduplicated protocol sessions.

### 3. Prose counts absent from the frozen snapshot (reproducibility)
> "two prose counts are absent from the frozen JSON"

**Verified — and the maintainer audit found six:** the scholarship-bridge
edge counts (19,044 / 37,713), the aggregated most-cited figure (100,760),
tool count, test count, and audit-check count.
**Resolution, two-part:** (a) the data-derived figures (bridge edges,
canonical most-cited) were added to the snapshot generator
(`scripts/paper_snapshot_stats.py`) and the frozen JSON regenerated —
values confirmed identical to the prose; (b) the genuinely operational
figures (tools, tests, checks, smoke cadence, telemetry) are now explicitly
scoped in the Reproducibility capsule as repository/deployment facts as of
the snapshot date, verifiable from the cited scripts rather than the frozen
JSON.

### 4. QA table disagreed with the executable checks
> "the QA row disagrees with the executable checks"

**Verified: both numbers wrong.** The smoke suite runs **8** probes (paper
said 6); the test suite collects **1,568** cases (paper said 1,556).
**Resolution:** Table 5 corrected to 8 probes and 1,568 cases.

## Reviewer-applied prose revisions (kept after maintainer vetting)

- "full interpretive stack" → "several components of Swiss legal
  interpretation" (abstract, §5 intro) — scope honesty.
- Contribution 4's "to our knowledge the first open legal resource
  reporting measured agentic consumption" removed; telemetry described
  without a novelty claim.
- Agent-behaviour language narrowed to "traffic patterns consistent with
  programmatic or autonomous operation".
- CAP described as "single-country, predominantly English" (not
  "monolingual").
- ECtHR language layer corrected to the precise fact: the Court publishes
  authoritative texts in EN/FR; the current HUDOC ingestion selects the
  French authoritative version.
- Introduction's "the mitigation the profession converges on" softened to
  "one proposed mitigation"; "no public surface aggregates them" narrowed
  to "no open release known to us combines...".
- Figure 1 caption gains "Individual records need not have an edge to every
  surrounding layer."
- Reproducibility capsule scoped to "corpus and graph counts" and the
  table-builder path corrected to the p1-local script.

## Process note

The Codex session stalled after ~3 hours (last file activity 01:36 UTC,
process idle thereafter) and was cancelled at 02:15 UTC; the memo above is
the reconstruction. The stall cost nothing of substance: the evidence audit
had completed and the edits were on disk. All four major findings were
independently re-verified against ground truth before resolution.
