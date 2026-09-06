---
name: evidence-bundle
description: Keep and verify the evidence behind a memo with ocl bundles (decisions, passages, statute articles, SHA-256 manifest), so a reviewer can audit what the agent relied on.
---

# Evidence bundles with `ocl`

Use when a deliverable relies on decisions or statutes and someone will
review it later (a memo, a brief, an answer with legal authorities).

1. Collect: `ocl bundle create '<query>' --max-results <n> --passage <e-number> --law <ABBR:ART> --out <folder>`
   saves every served response (JSON and plain text), a plain-language
   `INDEX.md` and `manifest.json` with requests, timestamps, source links and
   SHA-256 hashes. `--court`, `--language`, `--date-from/--date-to` narrow the
   selection; cantonal statutes take `--law ZH/GOG:1`.
2. Add decisions found elsewhere: `ocl bundle add <folder> <decision_id> ... --passage <n>`.
3. Verify before handing over: `ocl bundle verify <folder>` (exit 0 only when
   every listed file is intact). Compare two runs of the same question:
   `ocl bundle diff <old> <new>`.
4. Report the bundle path, `completeness` (an `unavailable` item is one the
   service does not have; a `failed` one is retried with `--resume`), and the
   `corpus_snapshot.db_generation` the answers came from.

Rules: the bundle is what was served, not an assessment of legal support;
never edit files inside it; cite from `INDEX.md`'s labels, which are the
service's citation strings.
