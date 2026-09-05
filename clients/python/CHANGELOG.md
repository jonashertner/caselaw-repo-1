# Changelog

The client follows semantic versioning. The research API contract it consumes is
versioned separately (`x-opencaselaw-contract-version` in `/api/research/openapi.json`).

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
- `cite` identifies long forms the same way, verifies an inline pinpoint, and
  returns the decision-level string when the Erwägung does not exist.
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
