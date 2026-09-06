# Verification pack (offline mode), 2026-09-06

**What.** `scripts/build_verification_pack.py` writes one SQLite file with what
the research CLI needs to check citations, pinpoints and quotations without the
service: decision metadata with the service's own citation strings
(`mcp_server._build_citation_strings`, so R1 holds offline), docket aliases,
canonical representations from the manifest, and every indexed Erwägung
(text zlib-compressed per row). No full texts, no search index.

**Why.** A memo's citations sent to a public API disclose the matter; the Word
add-in already needed client-side redaction for this. With `ocl --local`
nothing leaves the machine, there are no rate limits, and a check is pinned to
a snapshot generation.

**Size.** 9.28M paragraphs, ~16 GB of raw text, ~1,700 chars each; zlib per row
brings the pack to an estimated 5–6 GB; metadata under 1 GB. The gzip for
download is somewhat smaller. Free space on the data volume: 149 GB.

**Pipeline.** `publish.py` step 3b (`verification_pack`, non-fatal), Sundays
only (`OCL_PACK=1` forces it), after `export_parquet`; full scan of the
paragraph table inside the build, timeout 7,200 s. Output
`output/dataset/artifacts/verification_pack/<date>.sqlite.gz`, uploaded to
`voilaj/swiss-caselaw` as `artifacts/verification_pack/<date>.sqlite.gz` and
`latest.sqlite.gz`; the two newest gzips are kept locally.

**Client.** `ocl pack pull [--to FILE]` (stdlib download + gunzip),
`ocl pack info`, `--local [PACK]` / `OCL_LOCAL`. `LocalClient` answers
`/health`, `/api/cite`, `/api/lookup`, `/api/decisions/{id}`,
`/api/erwaegung/{id}/{n}`; every other path is "not available offline"
(status 200, exit 4). Results carry `offline: true`.

**First run.** The Sunday 2026-09-07 03:30 build (tonight) is the first; check
`publish.log` for "Build verification pack" and the HuggingFace artifact, then
`ocl pack pull` and `ocl --local citations resolve 'BGE 136 III 513 E. 2.3'`.

**Rollback.** Remove "3b" from the classic list / DAG; the step writes only
under `output/dataset/artifacts/` and the mirror path above.
