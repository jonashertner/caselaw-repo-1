# OpenCaseLaw research CLI

Dependency-free Python 3.10+ client for the public, read-only research API.
Install from the repository with `python3 -m pip install ./clients/python`.
This package does not install scraper/server dependencies or a local corpus.

```sh
ocl --help
ocl citations resolve 'BGE 136 III 513' '4A_747/2012'          # do these exist, and which decisions are they?
ocl bundle create 'Rachekündigung Art. 336 OR' --max-results 10 --passage 2 --law OR:336 --out evidence
ocl decisions search 'Rachekündigung' --max-results 5 --format jsonl | ocl decisions get --stdin --format jsonl > decisions.jsonl
ocl decisions passage bge_BGE_136_III_513 2.3
```

A bundle folder holds the selected decisions (JSON and plain text), the named
Erwägungen and statute articles, a plain-language `INDEX.md`, and a
`manifest.json` with every request, timestamp, source link and file hash.

See the repository's [research CLI guide](https://github.com/jonashertner/opencaselaw/blob/main/docs/research-cli.md)
for provenance, batch inputs, pagination, partial failures and repeatable workflows.
Commands contact `https://mcp.opencaselaw.ch` by default; `--base-url` accepts an
alternative HTTP(S) origin. Results come from a changing hosted corpus, not an
offline snapshot. Preserve returned source evidence and pagination metadata.

Exit codes: 0 success (including a bounded selection); 2 invalid input; 3 API
or transport failure; 4 partial or unresolved result; 130 interruption.
At a terminal the output is readable text (`--color never` or `NO_COLOR` for
plain text); piped, or with `--format json|jsonl`, it is JSON on stdout.
Diagnostics go to stderr. The client retries at most
twice by default, spaces requests by at least 200 ms, and respects Retry-After
up to 30 seconds; longer server delays fail without an early retry.
