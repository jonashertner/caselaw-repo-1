# Decision Rules — OpenCaseLaw 120-day Plan

Status: ratified 2026-05-17. These rules govern every change in the
sub-hour-freshness / signed-provenance / cross-lingual-retrieval plan.

## 1. No automatic rollback after public emission

Once `decisions.db` has been served by any MCP worker, `stats.json`
pushed to git, or an HF delta emitted, **rollback is a manual
operator decision with a runbook — never silent**.

The rationale: an auto-rollback after a published count change
creates non-monotonic public state. Downstream consumers (HF, MCP
clients, opencaselaw.ch) will already have observed the new state.
A silent revert produces "the corpus shrank" without an audible
event, which is worse than the failure that triggered the rollback.

A runbook (`runbooks/manual_rollback.md`) defines: how to detect a
bad publish, how to communicate the rollback to consumers, and how
to roll back.

## 2. Schema and DB layout changes deploy Saturday only

The Sunday full rebuild is the consistency baseline. Any change
that affects on-disk schema (columns, indexes, journal mode,
canonical-key derivation, content_hash semantics) deploys **on
Saturday**, gets exercised by Sunday's rebuild, and is observable
by Monday morning.

Weekday deploys are reserved for code that does not alter on-disk
shape (MCP server logic, scraper internals, dashboard).

## 3. Every production change ships with a tested runbook

A change without a runbook is not done. The runbook must include:
prerequisites, deploy commands, verification, rollback procedure,
and escalation. The author validates the runbook by cold-deploying
*from the runbook alone* (no reliance on memory of having just
written the change).

## 4. Live-DB writes require an explicit contract

Any code path that mutates `decisions.db` while MCP workers are
serving must:

- Bump `PRAGMA user_version` to a new `db_generation` after the
  final durable write and before the atomic swap.
- Use atomic `os.replace` of a fully-written temp file (no
  in-place mutation while workers read).
- Be covered by a test that asserts MCP workers see the new
  generation on their next request.

This rule extends to: `build_fts5.py`, `scripts/quick_publish.py`,
and any future writer (e.g. live-insert, delta-merge).

## 5. Public claims must be measurable and dashboarded

External pages on opencaselaw.ch make at most three claims, each
backed by a dashboard cell with a live number:

- "Sub-hour federal freshness" — backed by
  `freshness_seconds_by_court` p95 over rolling 30 days.
- "Signed verifiable corpus" — backed by daily manifest URL +
  public key + one-command verify.
- "Benchmarked cross-lingual retrieval" — backed by per-cell MRR +
  reviewer IRR κ.

Aspirational internal framing ("world-class", "best-for-AI-agents")
stays internal. The homepage only states what the dashboard proves.

## 6. Sunday is the reconciliation invariant — with an escape hatch

The Sunday full rebuild is the ground-truth source. Weekday quick
publishes and per-court pollers feed into it; Sunday reconciles.

But Sunday is not the only possible recovery path. A documented
manual full-rebuild command (`runbooks/emergency_full_rebuild.md`)
exists for incident response on any day. Sunday is the invariant,
not the only door.
