# Changelog

The client follows semantic versioning. The research API contract it consumes is
versioned separately (`x-opencaselaw-contract-version` in `/api/research/openapi.json`).


## 0.8.0 (unreleased)

- Windows installer for machines without Python (courts, managed desktops):
  `OpenCaseLaw-CLI-<version>-setup.exe`, built by
  `.github/workflows/installer-cli.yml` on every `cli-v*` tag from
  `clients/python/installer/`. Ships the python.org embeddable runtime
  (3.13.7, PSF-signed, digest pinned) and this package under
  `Program Files\OpenCaseLaw` with `ocl.cmd`, a "Send to" entry
  "Entwurf prüfen (offline)" (`ocl check DRAFT --local`, then opens the
  report) and a Start-menu entry that runs `ocl pack pull`. SHA-256 file and
  build provenance attestation on the release; not yet Authenticode-signed.
  Install notes for court IT: `docs/court-it-install.md`.

## 0.7.0 (2026-09-06)

- `ocl check memo.docx`: hand over the draft itself. The document is read
  (Word including footnotes, Markdown, HTML, text), the citations and the
  quotations next to them are found in the prose, every one is checked, and
  a report is written next to the draft (`memo.check.html`; a `.md` name
  gives Markdown): what held, what needs attention, and what to do. Exit 4
  when anything needs attention. `--format json` returns the rows and the
  found citations for scripts.

## 0.6.0 (2026-09-06)

- Offline mode. `ocl pack pull` downloads the verification pack (one SQLite
  file, published weekly on the HuggingFace mirror: decision metadata with
  the service's own citation strings, docket aliases, canonical
  representations, every indexed Erwägung); `ocl --local ...` then answers
  `citations resolve`, `cite`, `decisions passage`, `decisions get`
  (metadata), `quotes check` and bundles on this machine only. Search, laws
  and tools say "not available offline". `ocl pack info` shows the pack's
  generation. Full texts are not in the pack.
- `ocl tool call` and `opencaselaw_cli.api.tool` fetch the tool's own dict
  from the new `POST /api/tool/{name}` endpoint (fields for every tool, not
  Markdown), falling back to the MCP call on older servers.

## 0.5.1 (2026-09-06)

- `ocl tool call NAME key=value --option` accepts the pairs in any position on
  Python 3.10 and 3.11 as well (argparse there does not take positionals after
  an option; the pairs are now moved behind the tool name before parsing).
  0.5.0 failed its own test on 3.10 for this reason and is superseded.

## 0.5.0 (2026-09-06)

The agent release: everything the service offers, from the command line,
with a contract an agent can rely on.

- `ocl tool list|schema|call`: every research tool of the service (leading
  cases, relevant considerations, scholarship, commentaries, practice,
  materials, legislation changes, case briefs, claim support, ...) called
  over the same origin, with the tool's structured output; `key=value`
  arguments are typed when they parse as JSON, `--args` takes an object,
  `--stdin`/`--input` run one call per row; a tool-reported error is exit 4.
- `--cache DIR` / `OCL_CACHE`: responses cached on disk, keyed by the
  server's database generation, so repeated calls in a session cost nothing
  and stay consistent until the nightly rebuild.
- `ocl doctor`: connection, server generation and size, tool count, one
  timed citation lookup, cache state; exit 3 when the service does not answer.
- Skills shipped in the package: `citation-check`, `research`,
  `evidence-bundle`; `ocl skills list|show|install --claude|--dir`.
- `ocl agent-guide` prints the agent guide (contract, commands, statuses,
  rules); `opencaselaw_cli.api` is the same functionality as a library.



- `ocl quotes check`: quotations verified against the cited Erwägung and the
  decision text, with `exact` / `near` (differing spans, served wording) /
  `not_found`; typography, OCR hyphenation, whitespace and link markup are
  folded for the comparison only. A `quote` field on a `citations resolve`
  row is checked the same way (`quote_check`); exit 4 unless every quotation
  is exact.
- `laws get --article ... --as-of`: an edition whose PDF window holds only
  the article heading (`text_status` heading_only/empty) is unresolved (exit 4).

Two identity gaps from the field test, closed together with the server
(which now lists `joined_dockets` on decisions, lookup hits and `cite`, and
`canonical_decision_id` + `is_canonical` on decisions, lookup hits, `cite` and
search rows while its representation manifest is loaded):

- A reference by a joined docket of a consolidated proceeding (`BGer
  1B_243/2022`, filed under the lead docket 1B_242/2022) resolves instead of
  coming back `unrecognized`: `identity_check.method` is
  `exact_server_joined_docket`, with `joined_docket` and `lead_docket`.
  Uniqueness is still checked through `/api/lookup?exact=true`, whose hits
  carry their joined dockets too. Against an older server the row stays
  `unrecognized`; nothing is guessed.
- `decisions search` keeps one row per ruling the service stores under
  several ids (the canonical record, at the group's first-seen rank); the
  others are counted in `_client.duplicates_collapsed` and listed under
  `_client.collapsed_representations`. `--no-collapse` keeps every row.
- `citations resolve` and `cite` rows carry `canonical_decision_id` when the
  resolved record is a duplicate representation of another stored decision;
  `decision_id` is never changed. `--fields` keeps it.

## 0.3.1 (2026-09-06)

Packaging only: project links, classifiers and keywords on PyPI; a README
that says what the tool does. No code changes.

## 0.3.0 (2026-09-05)

A field test by five agents (179 round trips over French, Italian, German,
cantonal, statute and research-workflow scenarios; 44 verified findings)
reshaped citation checking around how references are actually written.

- References are parsed as written: collection label (BGE/ATF/DTF), docket
  in any stored separator (`4A_747/2012`, `4A 747/2012`, `4C.230/2005`),
  cantonal dockets (`LA210005`, `C/11532/2013`, `HC / 2020 / 38`, `K 2015/3`,
  `810 16 9`), court words, date, page references (`S. 357`, `p. 305`, `ff.`)
  and an inline pinpoint (`E. 2.3`, `consid. 3b`, `E. 3c/aa`). The service's
  own citation strings for every court now resolve, as do long forms around
  a spaced docket; wrapping parentheses and trailing punctuation no longer
  turn an existing decision into `missing`.
- An inline pinpoint is verified, not folded away: `BGE 136 III 510 E. 99`
  is `pinpoint_unavailable`. `pinpoint_source` says whether it came from the
  reference or the input row. Lettered sub-numbers the index lacks
  (`E. 2a`, `E. 3c/aa`) retrieve their parent number with
  `pinpoint_status: parent_retrieved` so the reader can locate the letter.
  Pinpoint fields accept the author's spelling (`consid. 2.3`, `E. 3b`); an
  invalid one fails that row, never the batch.
- New status `discrepancy`: the decision was identified, but the date
  written in the reference or a docket written next to the BGE label
  contradicts the record (`discrepancies` lists what).
- Identity is scoped by the court the reference names: `BGer 4A_191/2019
  vom 5. November 2019` resolves although a Geneva summary carries the same
  docket; the bare docket stays `ambiguous` with the candidates listed. A
  bare numeric docket (`1/2020`) carried by two decisions is `ambiguous`.
- `unrecognized` rows carry no `decision_id`; the service's proposal is under
  `service_candidate`. `--fields` on `citations resolve` is a real projection
  (reference, status, errors, notes and discrepancies always survive), and
  extra input keys come back under `input`.
- `cite` identifies every reference the same way (a bare docket the service
  matches by substring, such as `247/2020`, is no longer cited as another
  decision), verifies an inline pinpoint, and returns the decision-level
  string when the Erwägung does not exist. A missing reference still returns
  the service's `exists: false` answer with its close matches.
- The label written first is the citation: a docket mentioned later in the
  reference (`vgl. auch BGer 4A_747/2012`, a joined file) is listed under
  `other_dockets`, never taken for the decision cited. A docket next to a BGE
  label is reported as verified only when the BGE record itself names it; a
  ruling of the same day is noted, not affirmed.
  `decisions get`, `passage` and `citations list` accept every reference
  form; `passage` returns a parent number with a note (exit 4) for a lettered
  sub-number.
- Passages carry `text_plain`: the served text with the service's Markdown
  cross-reference links reduced to their labels, for comparisons with the
  decision text. `text` stays the served string.
- Exit codes follow the cause: 4 for a decision or passage the service does
  not have and for a reference that names no single decision, 3 only for
  transport or server failures.
- Bundles: `failed_items` counts failed items only (`unavailable_items` and
  `missing_text_items` separately), INDEX.md is computed from item statuses,
  `--resume` retries the decisions added with `bundle add` and does not
  re-request unavailable items, a 404 is `unavailable`. `bundle verify`,
  `diff` and `add` have readable text output.

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
