# Changelog

The client follows semantic versioning. The research API contract it consumes is
versioned separately (`x-opencaselaw-contract-version` in `/api/research/openapi.json`).

## 0.2.1 (2026-09-05)

Findings from an agent running the citation-check procedure on a draft memo:

- Long-form references such as `BGer 4A_747/2012 vom 5. April 2013` or
  `Verwaltungsgericht des Kantons Aargau WBE.2026.33` no longer come back
  `missing`: the docket they contain is retried, reported as
  `docket_extracted`, and the decision must carry that docket label.
  `decisions get`, `passage` and `citations list` accept the same forms.
- Bundles tell apart `unavailable` (the service answered that a passage or
  article is not there; a rerun will not change it) from `failed` (transport
  or validation; `--resume` retries). `completeness.unavailable_items` and
  INDEX.md say which is which.
- `--verbose` logs every request to stderr; search, batch, resolution and
  bundle outputs carry the request count.
- The resolution table and text show the decision-level citation, never a
  pinpointed string for a pinpoint the index lacks; a `missing` row notes
  that close matches are for the author, not substitutes.
- `citations resolve --help` explains `identity_check`.

## 0.2.0 (2026-09-05, first release on PyPI)

- `cite --pinpoint` verifies that the Erwägung exists; a missing passage reports
  `pinpoint_exists: false` and exits 4 (`--no-verify-pinpoint` restores formatting only).
- Batch commands (`decisions get`, `cite`, `citations resolve`) and bundle collection run
  up to `--jobs` requests concurrently (default 4) under the same 200 ms pacing, and stop
  after five consecutive transport failures instead of retrying every remaining item.
- Output formats `table`, `csv` and `md` for pasting into documents; `--fields` stays
  JSON-only.
- Defaults from `~/.config/ocl/config` (`key = value`) and `OCL_BASE_URL`, `OCL_TIMEOUT`,
  `OCL_RETRIES`, `OCL_FORMAT`, `OCL_COLOR`, `OCL_LANGUAGE`, `OCL_JOBS`.
- Shell completion: `ocl completion bash|zsh|fish`.
- Bundles record the service's database generation and decision count at run start
  (`corpus_snapshot`); `bundle verify` re-hashes a folder, `bundle diff` compares two
  bundles, `bundle add` appends decisions; `--law ZH/StG:1` reaches cantonal statutes.
- TLS certificate failures are not retried; Windows consoles get UTF-8 output.
- Version is defined once in `opencaselaw_cli/_version.py`.

## 0.1.0 (2026-09-05)

- First release: search, decisions, passages, statutes, citation graph, canonical
  citations, research bundles, citation resolution; readable text mode at a terminal.
