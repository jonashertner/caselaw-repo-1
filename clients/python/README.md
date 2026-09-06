# OpenCaseLaw research CLI

`ocl` checks Swiss case citations against the open OpenCaseLaw corpus (over a
million published decisions of the Federal Supreme Court and the cantonal
courts, federal and cantonal statutes) and keeps the evidence behind a memo.
Dependency-free Python 3.10+; JSON when piped, readable text at a terminal.

```sh
pipx install opencaselaw-cli        # or: uv tool install opencaselaw-cli
```

## Two jobs

**Check the citations in a draft**, written the way lawyers write them:

```sh
ocl citations resolve 'BGE 136 III 513 E. 2.3' 'BGer 4A_747/2012 vom 5. April 2013' \
    'Obergericht ZH LA210005 vom 15. Juni 2021' 'BGE 999 III 1'
```

Each reference comes back `resolved`, `pinpoint_unavailable`, `discrepancy`
(the decision exists but the date or docket written next to it is wrong),
`missing`, `ambiguous` or `unrecognized`, with the service's own citation
strings and the verbatim text of the cited Erwägung. The rule behind it: the
decision the service proposes must carry the label the author wrote; nothing
is ever substituted, and no citation string is built by the client.

**Check the quotations** against the served text (`exact`, `near` with the
differing spans, or `not_found`):

```sh
ocl quotes check 'BGE 136 III 513 E. 2.3' --quote "le contrat de travail conclu pour une durée indéterminée"
```

**Keep the evidence behind a memo**:

```sh
ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 10 --passage 2 --law OR:336 --out evidence
ocl bundle verify evidence
```

A bundle folder holds the selected decisions (JSON and plain text), the named
Erwägungen and statute articles, a plain-language `INDEX.md`, and a
`manifest.json` with every request, timestamp, source link and SHA-256.

Also: `ocl decisions search`, `ocl decisions get`, `ocl decisions passage`,
`ocl laws get`, `ocl citations list`, `ocl cite`. `ocl <command> --help` has
examples for each.

## For agents

`ocl agent-guide` prints the contract on one page: JSON when piped, exit
codes that carry the verdict (0 resolved, 2 invalid input, 3 transport, 4
something did not resolve), `--cache DIR` for free repeats within a server
generation, and the rules (citation strings and quotations only from the
service). `ocl tool list|schema|call` reaches every research tool of the
service with its structured output. Three skills ship in the package
(`citation-check`, `research`, `evidence-bundle`): `ocl skills install
--claude` copies them into Claude Code's skills directory, `--dir` anywhere
else. `opencaselaw_cli.api` offers the same as a library.

## Scope

Existence, identity and wording only. The tool never says that a decision
supports a proposition, is still good law, or fits the facts. Results come
from a hosted corpus that is rebuilt nightly, not from an offline snapshot;
keep the returned source links and pagination metadata with your work.

Guide: <https://opencaselaw.ch/research-cli.md> · Site: <https://opencaselaw.ch/cli/> ·
Changelog: `CHANGELOG.md`. Commands contact `https://mcp.opencaselaw.ch` by
default; `--base-url` accepts another origin. Defaults can live in
`~/.config/ocl/config` or `OCL_*` variables; `ocl completion zsh|bash|fish`
prints a completion script. The client spaces requests by at least 200 ms,
retries at most twice by default and respects `Retry-After` up to 30 seconds.
