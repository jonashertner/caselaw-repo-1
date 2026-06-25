# Submission to mcp-get.com — DEPRECATED, do not submit

**Status (verified 2026-04-28):** the mcp-get repository at
<https://github.com/michaellatman/mcp-get> is officially **deprecated**.
The README is now a redirect notice pointing users to Smithery
(<https://smithery.ai>).

> "This repository is no longer actively maintained. We recommend
> Smithery for discovering, installing, and managing MCP servers."
> — michaellatman/mcp-get README

PRs against this repository will not be merged.

## Action

Skip this registry. Submission to **Smithery** (file
[`03-smithery.md`](03-smithery.md) in this directory) covers the
same audience and is now the canonical installer registry.

If a successor to mcp-get appears in the future, this file can be
repurposed by replacing the contents with the new registry's
submission template; the package.json shape below is portable.

## Reference: original package.json shape (kept for reuse)

If a future installer registry uses the same JSON-package convention,
this manifest can be dropped in directly:

```json
{
  "name": "swiss-caselaw",
  "displayName": "Swiss Caselaw",
  "description": "990,000+ Swiss court decisions, 5,519 federal + 15,589 cantonal statutes, 8.65M-edge citation graph, scholarly commentary (DE/FR/IT). 6-layer verification architecture catches AI hallucination of case references before answers ship. CC0 data, MIT code, no API key.",
  "vendor": "Jonas Hertner / OpenCaseLaw contributors",
  "sourceUrl": "https://github.com/jonashertner/caselaw-repo-1",
  "homepage": "https://opencaselaw.ch",
  "license": "MIT",
  "runtime": "remote",
  "transport": {
    "type": "streamable-http",
    "url": "https://mcp.opencaselaw.ch"
  },
  "fallbackTransport": {
    "type": "sse",
    "url": "https://mcp.opencaselaw.ch/sse"
  }
}
```
