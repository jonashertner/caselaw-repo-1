# ocl for agents

`ocl` is the command-line client of OpenCaseLaw (Swiss case law, statutes,
scholarship, practice). It is built for agents: JSON when piped, exit codes
that carry the verdict, a stable schema, and rules that keep citations and
quotations honest. Print this page with `ocl agent-guide`.

## Install and check

    pipx install opencaselaw-cli     # or: uv tool install opencaselaw-cli
    ocl doctor --format json         # connectivity, server generation, tool count, latency

Python 3.10+, no dependencies. Non-interactive; never prompts.

## Contract

- stdout: JSON (`--format json`, or automatically when piped); `--format jsonl`
  gives one row per line plus a trailing `{"_type": "pagination", ...}` record.
  stderr: diagnostics only.
- exit 0 all resolved · 2 invalid input · 3 transport or server failure (retry
  later) · 4 something did not resolve, is not in the corpus, or names no
  single decision (read the rows).
- `--fields a,b` trims rows; the verdict fields (status, errors, notes,
  discrepancies) always survive.
- `--cache DIR` (or `OCL_CACHE`) stores responses keyed by the server's
  database generation: repeated calls in a session cost nothing and stay
  consistent until the nightly rebuild.
- Requests are paced (200 ms) and retried; `--jobs` bounds concurrency.
  Keep a session to hundreds of requests, not thousands.

## Commands

| job | command |
|---|---|
| check citations in a draft | `ocl citations resolve --input refs.jsonl --format jsonl` |
| check quotations | `ocl quotes check --input quotes.jsonl --format jsonl` |
| a citation string to copy | `ocl cite '<reference>' --pinpoint 2.3` |
| find decisions | `ocl decisions search '<terms>' --max-results 20 --format jsonl` |
| read a decision / a passage | `ocl decisions get <id>` · `ocl decisions passage <id> 2.3` |
| statutes | `ocl laws get OR --article 336 [--as-of YYYY-MM-DD]` |
| citation graph | `ocl citations list <id> --direction incoming` |
| every other server tool | `ocl tool list` · `ocl tool schema <name>` · `ocl tool call <name> key=value ...` |
| keep evidence | `ocl bundle create ... --out folder` · `ocl bundle verify folder` |

`ocl tool call` reaches all research tools of the service (leading cases,
relevant considerations, scholarship, commentaries, practice, materials,
legislation changes, case briefs, claim support). Values that parse as JSON
are typed (`limit=5`, `flag=true`, `ids='["a","b"]'`); `--args '{...}'`
passes a whole object; `--stdin` runs one call per JSONL row.

## Statuses

`resolved` · `pinpoint_unavailable` (decision found, passage not indexed;
`parent_retrieved` when the parent number was served instead) · `discrepancy`
(date or docket written next to the label contradicts the record) · `missing` ·
`ambiguous` (several carriers; name the court) · `unrecognized` (the service's
proposal carries no label you wrote; see `service_candidate`, never cite it).
Quotes: `exact` · `near` (differences listed) · `not_found`.

## Rules that are not optional

1. Citation strings come from the service (`citation_string`); never compose one.
2. Quotations come from served passages; `quotes check` decides, not memory.
3. A close match or service candidate is information for the author, never a substitute.
4. Existence and wording are what the tool establishes; legal support is not.

## Skills

`ocl skills list` · `ocl skills show citation-check` · `ocl skills install --claude`
(copies the bundled skills into `~/.claude/skills/`; `--dir` for any other harness).

## As a library

    from opencaselaw_cli import api
    rows = api.resolve([{"reference": "BGE 136 III 513 E. 2.3"}])
    result = api.tool("find_leading_cases", query="Rachekündigung")
