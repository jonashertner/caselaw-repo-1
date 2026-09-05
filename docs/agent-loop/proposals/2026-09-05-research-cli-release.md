# Research CLI and shared response contracts

Implementation completed on 2026-09-05 against `0f8d0dbfef6136e93edeaff26df4f8cc07261ed3`, branch `codex/research-cli-20260905`, re-based on `8fda0c84` and reviewed the same day (section "Independent review and fixes" below). User direction: implement the reviewed CLI/interface strategy. Commit, push and deployment remain subject to the repository's explicit approval rule.

## Result

`opencaselaw-cli` is a standalone, dependency-free Python package exposing `ocl`. Its public GET operations cover bounded search, decision and passage retrieval, statutes, citation graph, canonical citations, source-preserving research bundles and citation-resolution reports. JSON/JSONL, batch input, meaningful errors and exit codes, request pacing, retry limits, evidence hashes and partial-resume behavior are implemented. Existing bundles are not overwritten. A resume validates retained file hashes and records new response timestamps.

Six core MCP tools now return structured results with shared additive schemas, while retaining their human text and widget behavior. Compact search projection and the existing 200,000-character MCP decision-text bound remain enforced; truncation is explicit and links to complete REST retrieval. The corresponding public research API schemas plus lookup are available through a curated seven-route `/api/research/openapi.json`; the typed schemas are injected into that document only, so `/api/openapi.json` and the Copilot Studio subset keep their existing response documentation (only the `/decisions` and `/lookup` description strings change). REST response dictionaries remain unchanged. `research_contracts.py` is included in server packaging.

README, the CLI guide, homepage and API page describe chat, automation and bulk-download access. CLI installation is from this checkout; there is no claim of a published PyPI release. Public docs describe the new curated schema as available after server deployment.

## Validation

- Full `make test`: 2,730 passed, 24 skipped; existing deprecation warnings remain. Final focused standalone package suite: 63 passed. Source lint and whitespace checks pass.
- `make verify-offline`: committed corpus, citation graph and cross-language benchmark snapshots reproduce.
- Full and curated OpenAPI documents validate; tests cover actual MCP wire results, handled errors, ignored arguments, null/extra fields, compact projection, text truncation and unchanged source objects.
- Standalone wheel and source distribution build. An isolated installation contains only `opencaselaw-cli`, with no server, FastAPI or Pydantic dependency. All 13 help entry points pass. Declared Python support is 3.10+; this run used Python 3.12.14.
- Bounded live checks succeeded for filtered search, decision retrieval, statute lookup, exact passage retrieval, canonical and printed-reference resolution, and a complete filtered research bundle. Bundle file hashes matched saved bytes. The final printed BGE plus pinpoint/canonical-ID resolution completed successfully in 1.379 seconds; this is a smoke observation, not a performance benchmark.
- A broad `Art. 41 OR` text query timed out and returned a structured failure. A BGE-number search returned related/citing decisions; its missing requested passage was retained as a partial failure. The resolver now compares source-provided canonical labels before using the topical lookup window. Bare-docket ambiguity and capped windows remain explicit.
- The existing incremental command-sequence tests depended on wall-clock time and failed during the production late-start window. Their dry-run helper now uses the existing `--now-utc` test option with an on-time value; dedicated late-start tests still override it. No production pipeline code changed.

Build and live-check evidence is retained locally in ignored `artifacts/cli-release/`; full test logs are recorded in the project wiki source register. No test changed a production database, sent messages or invoked paid generation routes.

## Independent review and fixes (2026-09-05)

A nine-dimension review with three-lens adversarial verification (85 agents) ran against the re-based branch. Confirmed findings and their fixes, all included in this branch:

- CI blocker: `tests/test_research_contracts.py` imports `openapi_spec_validator`, which only the `dev` extra declared. Both CI workflows now install it; the import stays hard so the OpenAPI validation cannot silently vanish.
- Copilot Studio regression: `responses=` on the seven live routes replaced the hand-typed 200 schemas of `/api/openapi.copilot.json` with `anyOf` unions and swapped `HTTPValidationError` for an ambiguous 422 model. The live routes no longer carry `responses=`; `research_openapi()` injects the typed schemas (OpenAPI 3.0.3 form, `nullable` instead of null branches) into the curated document only. A test pins both live documents to their previous shape.
- Ranked search pages are not composable (the server sizes the candidate pool from the requested window). `ocl decisions search` and `bundle create` now send a text query as one request of `--max-results` (at most 800); filter-only enumeration keeps paging, drops duplicate rows and reports `duplicates_dropped`.
- `citations resolve` never resolved a BGer docket: `/api/lookup` pads every docket query to a full 25-row page, which the client read as a capped window. The resolver now uses the resolved decision's own docket label as identity evidence, counts only label-matching lookup rows, folds the federal docket separator (`4A_747/2012` = `4A 747/2012`), a trailing pinpoint and the ATF/DTF labels for the comparison, and keeps a docket carried by two decisions ambiguous.
- `decisions get/passage` and `citations list` accepted any `exists: true` from `/api/cite`, which also matches docket fragments (`247/2020` resolves to `6B_1247/2020`). The client now requires the resolved decision to carry the given docket or citation label and otherwise stops with an error.
- Guide examples used `BGE 140 III 86 E. 2.3`, a passage that does not exist in the structure index (available: 2, 4.1, 4.2); examples now use `BGE 136 III 513 E. 2.3` (verified live). The server's own tool descriptions still cite the non-existent passage (pre-existing, follow-up).
- Usability: every `ocl` command and argument now carries a plain-language help text with examples (`ocl --help`, `ocl bundle create --help`); bundle files are named after the decision, passage or article (`decisions/bge_BGE_136_III_513-5b3e22ef.txt`) instead of a hash, and each bundle gets an `INDEX.md` that lists what was saved in plain language while `manifest.json` stays the hashed record; the guide opens with the two concrete jobs (citation check, evidence folder) and real output.
- Smaller: statute requests in `en`/`rm` search languages fell back to HTTP 500 (now `de`); `http.client` exceptions and UTF-8 BOM input are handled; explicit `"pinpoint": null` rows no longer discard `--pinpoint`; `composed_of` is lifted into resolution rows; the contract-violation log line names the failing fields (never values) and the client text no longer says "retry"; an unknown-argument NOTE no longer corrupts JSON error payloads; `LookupHit` documents the lookup hit fields.

Deliberate behaviour changes to be aware of: the six MCP tools return `isError: true` for handled errors (not found, invalid input, timeout, contract violation), where the old text-only results carried `isError: false`; `structuredContent` duplicates the text payload (a 200,000-character decision doubles on the wire); `tools/list` grows by about 27 KB.

Verification after the fixes: full suite green under Python 3.12 with mcp 1.27.0 (production) and mcp 1.29.1 (CI); CLI package tests under Python 3.10 and 3.12; `make verify-offline`; regenerated `/api/openapi.json` and `/api/openapi.copilot.json` identical to origin/main apart from the two description strings; read-only production probe of real handler payloads for all six tools plus lookup against the strict contracts and generated JSON schemas; bounded live CLI checks (docket, embedded-pinpoint and ATF references resolve; a fragment is rejected; ranked search is one request).

## Release sequence

1. Review the branch diff, then obtain the explicit commit/push approval required by `AGENTS.md`. Proposed commit: `Add composable research CLI and shared API/MCP contracts`.
2. Publish the branch and review the change as a single feature. There are no ingestion, DB schema, billing, secret or production configuration changes.
3. With deployment authority, deploy the server module and its new `research_contracts.py` dependency together through the established release process; check the curated schema and six MCP operations after rollout. Preserve the previous release for rollback.
4. Publish the corresponding site documentation. Publish the standalone package only after package-index name/ownership and release credentials are verified; publishing is not implied by a local wheel build.

The CLI already works with the existing hosted API. Typed server/MCP contracts and the website edits are prepared locally and are not yet live. Any `ocl 0.1.0` installed from the wheel retained before the review predates these fixes; reinstall from this checkout (`python3 -m pip install ./clients/python`).
