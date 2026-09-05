# Research from the command line

`ocl` is a small command-line client for OpenCaseLaw. It searches the same
corpus as [opencaselaw.ch](https://opencaselaw.ch) and the
[MCP endpoint](https://opencaselaw.ch/mcp/): 1M+ published Swiss court
decisions from every federal and cantonal court, federal and cantonal
statutes, and the citation graph. It hands you the service's own records,
with identifiers, citation strings and passage text exactly as served. It is
made for two jobs that a chat window does badly: checking the citations in a
draft, and keeping the evidence behind a memo in a folder you can open years
later.

## Start here

Python 3.10 or newer. From a checkout of this repository (the client is not
on PyPI yet):

```bash
python3 -m pip install ./clients/python
ocl --help
```

Every command is a read-only call to the public service at
`mcp.opencaselaw.ch`. Nothing is downloaded, no account or key is needed.
Queries are subject to the [privacy notice](https://opencaselaw.ch/datenschutz/);
`--base-url` points at a separately operated server. At a terminal, results
are shown as readable, coloured text; when piped, or with `--format json` or
`--format jsonl`, they are JSON on standard output, so the same command feeds
both a person and a script. `--color never` or `NO_COLOR` turns colour off.
Messages go to standard error, and the exit code says how it went: 0 complete,
2 invalid input, 3 the service or network failed, 4 partial or unresolved.

Your first search:

```bash
ocl decisions search 'Rachekündigung Art. 336 OR' --max-results 3 --format jsonl --fields decision_id,citation_string_de,decision_date,court
```

```
{"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513", "decision_date": "2010-10-07", "court": "bge"}
{"decision_id": "ow_gerichte_AbR_1992_93_Nr._8", "citation_string_de": "Gericht OW AbR 1992/93 Nr. 8 vom 26. November 2015", "decision_date": "2015-11-26", "court": "ow_gerichte"}
{"decision_id": "sg_gerichte_BZ.2005.9", "citation_string_de": "Gericht SG BZ.2005.9 vom 20. Juli 2005", "decision_date": "2005-07-20", "court": "sg_gerichte"}
{"_type": "pagination", "total": 125, "total_is_lower_bound": true, "returned": 3, "has_more": true, "next_offset": 3, ...}
```

The last line is the service's own account of the selection: at least 125
matches exist, you asked for three, more are retrievable. Keep it with the
results.

## Two jobs it does well

### Check the citations in a draft

Before a brief goes out, you want to know that every cited decision exists and
that every pinpointed Erwägung is really there. Put the references from your
draft in a file, one per line, or as JSON lines when a reference carries a
pinpoint. `ATF`/`DTF` labels and a trailing `, E. 2.3` are understood:

```bash
cat > references.jsonl <<'JSONL'
{"reference":"BGE 136 III 513","pinpoint":"2.3"}
{"reference":"4A_747/2012"}
{"reference":"BGE 999 III 1"}
{"reference":"BGE 140 III 86","pinpoint":"2.3"}
JSONL
ocl citations resolve --input references.jsonl --format jsonl --fields reference,decision_id > resolution.jsonl
```

At a terminal the same command prints a table, one line per reference with a
coloured status; the JSON lines below are what a script receives:

```
{"reference": "BGE 136 III 513", "status": "resolved", "decision_id": "bge_BGE_136_III_513", "pinpoint_status": "retrieved"}
{"reference": "4A_747/2012", "status": "resolved", "decision_id": "bger_4A_747_2012"}
{"reference": "BGE 999 III 1", "status": "missing"}
{"reference": "BGE 140 III 86", "status": "pinpoint_unavailable", "decision_id": "bge_BGE_140_III_86", "pinpoint_status": "unavailable"}
{"_type": "pagination", "status": "partial", "counts": {"resolved": 2, "missing": 1, "pinpoint_unavailable": 1}, ...}
```

The first reference exists and so does its E. 2.3. The docket resolved to a
federal decision. `BGE 999 III 1` is not in the corpus: check the citation,
or accept that coverage may have a gap. BGE 140 III 86 exists, but the index
has no E. 2.3 for it, so quote from the decision itself, not from memory. The
exit code is 4 because not everything resolved. Without `--fields`, each row
also carries the service's citation strings in German, French and Italian,
the source link, and the passage text when a pinpoint was found.

What the check does not do: it never says that a decision supports your
proposition, is still good law, or fits your facts. That reading is yours.

### Keep the evidence behind a memo

When a memo relies on a search, you want a record of what was retrieved and
when. One command runs the search and saves the decisions it selects (ten by
default), the Erwägungen you name and the statute articles you name into a
folder:

```bash
ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 10 --passage 2 --law OR:336 --out rachekuendigung-2026-09
```

The folder is readable without any tool:

```
rachekuendigung-2026-09/
  INDEX.md                                      what was saved, in plain language
  manifest.json                                 the record: every request, timestamp, file hash, source link
  decisions/bge_BGE_136_III_513-5b3e22ef.txt    full text as served (a .json next to it holds the metadata)
  passages/bge_BGE_136_III_513_2-ff487655.txt   E. 2, verbatim
  laws/OR_336-812a5382.json                     Art. 336 OR with its consolidation date and Fedlex link
  search/page-0000-1850ce82.json                the search page the selection came from
```

`INDEX.md` from a real run with two decisions:

```
## Decisions
- BGE 136 III 513: saved, decided 2010-10-07 [decisions/bge_BGE_136_III_513-5b3e22ef.json, ...]
- Gericht OW AbR 1992/93 Nr. 8 vom 26. November 2015: saved, decided 2015-11-26 [...]
## Passages (Erwägungen)
- BGE 136 III 513, E. 2: saved [passages/bge_BGE_136_III_513_2-ff487655.json, ...]
- Gericht OW AbR 1992/93 Nr. 8 vom 26. November 2015, E. 2: failed: No structured Erwägungen found for 'ow_gerichte_AbR_1992_93_Nr._8'.
## Statute articles
- OR Art. 336 (consolidated 2026-01-01): saved [laws/OR_336-812a5382.json]
```

That run ended `partial` (exit 4) because the cantonal decision has no
numbered E. 2 in the index; the manifest says so and nothing is dropped
silently. Saved files are never overwritten. If a run is interrupted, repeat
the same command with `--resume`: the client re-verifies the hash of every
saved file, then fetches only what is missing. The folder preserves what the
service returned on that day; the same query next month can select other
decisions, because the corpus is rebuilt nightly.

## What the results mean

- `resolved`: the decision exists in the corpus. With a pinpoint, the named
  Erwägung exists and its text is in the row.
- `pinpoint_unavailable`: the decision exists; the numbered passage is not in
  the structure index.
- `missing`: no decision with that citation or docket. A wrong citation or a
  gap in coverage, never proof that a citation was invented.
- `ambiguous`: more than one decision carries that label (dockets are reused
  across courts). Pick a `decision_id`.
- `resolution_incomplete`, `unrecognized`, `error`: identity could not be
  established, or the request failed. Nothing is guessed.
- `identity_check.method` says why a match is trusted: an exact `decision_id`,
  the service's own citation string, the decision's own docket label, or an
  exact label among the service's lookup candidates.
- A bundle is `complete` when every requested item was saved, otherwise
  `partial` with each failed item listed. Complete describes the requested
  collection, not the law: `exhaustive_legal_research` is always `false`.
- `total_is_lower_bound: true` means "at least this many". A text query is
  ranked over a bounded candidate pool, so `has_more: false` never proves that
  every relevant decision was seen. Nothing here replaces reading the decision.

## Recipes

Search, then fetch the full decisions, in one pipeline:

```bash
ocl decisions search 'Rachekündigung Art. 336 OR' --max-results 5 --format jsonl |
  ocl decisions get --stdin --format jsonl > decisions.jsonl
```

List everything a court decided in a period. Without query text the filters
enumerate an exact set, page by page:

```bash
ocl decisions search --court bge --date-from 2026-01-01 --date-to 2026-06-30 --sort date_desc --max-results 500 --format jsonl > bge-2026-h1.jsonl
```

Quote one Erwägung verbatim, and get the citation to go with it:

```bash
ocl decisions passage bge_BGE_136_III_513 2.3 --fields citation_string_de,text
ocl cite 'BGE 136 III 513' --pinpoint 2.3 --language fr
```

Read a statute as it stood on a date:

```bash
ocl laws get OR --article 41 --as-of 2015-01-01
```

See who cites a leading case:

```bash
ocl citations list bge_BGE_140_III_86 --direction incoming --limit 50 --format jsonl
```

Finish an interrupted bundle:

```bash
ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 10 --passage 2 --law OR:336 --out rachekuendigung-2026-09 --resume
```

Coding agents such as Claude Code or Codex can run the same commands from a
shell and read the JSON, which keeps the evidence in files you can check
rather than in the model's memory.

## Where it fits

- Conversation: connect Claude, ChatGPT or another MCP client to
  `mcp.opencaselaw.ch` ([setup per client](https://opencaselaw.ch/mcp/)).
- Scripts, notebooks and agents: `ocl`, or the
  [REST API](https://opencaselaw.ch/api/) directly. Same records, same limits.
- Corpus-scale analysis: the
  [Parquet dataset](https://huggingface.co/datasets/voilaj/swiss-caselaw).

## Reference: search and retrieval

Text mode (the default at a terminal) renders search hits, decisions,
passages, statutes, citation lists, resolution reports and bundle summaries
for reading; it folds Markdown link markup inside served text for display and
ignores `--fields`. JSON is the default when output is piped. Search JSON keeps a result envelope with the server's
pagination fields and `_client` retrieval metadata; JSONL writes one decision
per line followed by a record with `_type: "pagination"`. Batch commands
(`decisions get`, `citations resolve`, `cite`) accept plain lines or JSONL
from `--input FILE` or `--stdin` and skip pagination records, so a search can
be piped straight into a fetch. `--fields a,b` projects result fields while
always keeping errors, statuses and completeness metadata. `--detail compact`
(the default) asks the service for lean records; `full` adds regeste, snippet
and a pinpoint suggestion for the top results.

A text query is ranked by relevance over one bounded candidate pool whose size
depends on the requested window, so its pages are not composable: `ocl` sends
it as a single request of `--max-results` (at most 800). Without query text
the filters enumerate an exact, stably ordered set in pages of `--page-size`
(default 50); rows repeated across pages are dropped and counted in
`_client.duplicates_dropped`.

A reference containing a slash (a docket such as `4A_747/2012`) is resolved
through the service first; `ocl` then requires the resolved decision to carry
that exact docket or citation label and otherwise stops, because the service
also matches docket fragments. `ocl cite --pinpoint` formats a citation without
checking that the passage exists; use `decisions passage` or
`citations resolve` for that. Statute responses can carry consolidation or
edition information; inspect it before relying on the text for a historical
question. `--as-of` covers federal law only.

## Reference: bundles

`--passage NUMBER` (repeatable) requests that Erwägung from every selected
decision; `--law ABBREVIATION:ARTICLE` (repeatable) requests federal statute
articles, in the search language when it is `de`, `fr` or `it`, otherwise in
German. `--court`, `--canton`, `--language`, `--date-from` and `--date-to`
filter the search.

The manifest links the request parameters and every response with retrieval
timestamps, source URLs, consolidation dates, errors, and a SHA-256 over the
saved bytes of each file. That hash verifies the file as saved, not the court's
original publication or the service's wire encoding; the service's own
`content_hash` is kept separately, as supplied. Decision `.txt` files are the
exact served full text; passage `.txt` files are the served passage text, in
which cross-references can carry Markdown links added by the service.
`INDEX.md` is a convenience view regenerated on every run; only files listed
in the manifest form the collection.

Resume requires the same query, filters and options in the same directory.
A crash between saving a response and checkpointing the manifest can leave an
unlisted file; resume keeps it and records a new request. Use one writer per
bundle directory. Resuming can span corpus updates; each response carries its
own timestamp. A case-number query behaves as a text search and can select
decisions citing that case; inspect the selected IDs.

## Reference: citation resolution

Input is one reference per line, or JSONL records with `reference` and an
optional `pinpoint`; `--stdin` and positional references also work. Exact
canonical IDs and the service's own citation strings confirm identity directly
(a trailing pinpoint such as `, E. 2.3`, the `ATF`/`DTF` labels and the
federal docket separator `4A_747/2012` = `4A 747/2012` are folded for the
comparison only; nothing is ever rewritten for output). Other references are
compared with the resolved decision's own docket label and with the docket
labels of the service's lookup candidates; a docket carried by more than one
decision stays `ambiguous`, and a lookup window filled with exact matches is
reported as `resolution_incomplete`. A `--language` other than German only
changes which citation string is primary.

## Bounds, errors and reusable scripts

Search stops at `--max-results` (default 50). A text query is ranked over one
bounded candidate pool whose size depends on the requested window, so its pages
are not composable: `ocl` sends it as a single request of `--max-results`
(at most 800). A filter-only search (no query text) enumerates an exact, stably
ordered set and is fetched in pages of `--page-size` (default 50); rows repeated
across pages are dropped and counted in `_client.duplicates_dropped`. Inspect
`total_is_lower_bound`, `has_more`, `next_offset` and `_client` metadata.
A relevance query can exhaust the server's bounded candidate pool before all
matches are returned. `has_more: false` does not prove exhaustive coverage of a
legal question. There is no exhaustive `--all` option. For corpus-scale work,
use the [Parquet dataset](https://huggingface.co/datasets/voilaj/swiss-caselaw).

Batch commands accept plain references/IDs or JSONL from `--input FILE` or
`--stdin`. Keep stdout for JSON/JSONL results and stderr for diagnostics. Check
the exit status as well as the output:

| Exit | Meaning |
|---|---|
| `0` | Successful command, including an intentionally bounded search |
| `2` | Invalid arguments or input |
| `3` | API or transport failure |
| `4` | Partial or unresolved result, including incomplete batches and workflows |
| `130` | Interrupted |

Leaf commands expose `--base-url`, `--timeout`, `--retries`, `--format` and
`--fields`. The client paces requests and uses bounded retries, respecting
`Retry-After` up to its retry-delay limit. A longer requested wait returns an
error so the job can be rescheduled without retrying early. Pace concurrent scripts so their
combined traffic stays within the service's published rate guidance. Save
selection parameters, client version, output and exit status with reusable
scripts. Do not discard a partial result or present it as complete.

## Research API contract

The selected public research operations have a typed OpenAPI document at
`/api/research/openapi.json` in this source version. It covers the core search,
retrieval, passage, statute and citation operations without the broader
application's billing/admin routes. The main specification remains at
`/api/openapi.json`.

The research contract becomes available on a server only after that source
version is deployed. This guide does not assert that it is live on the public
host. The client calls existing REST research routes and does not need the new
schema endpoint to be deployed. The schema describes returned evidence and
pagination; it does not certify legal relevance, corpus completeness or the
accuracy of every upstream record.
