# Submission to mcp-get.com

**URL:** https://mcp-get.com (registry behind the `mcp-get` installer)
**Repo:** https://github.com/michaellatman/mcp-get
**Method:** GitHub PR adding an entry to `packages/`
**Why it matters:** `mcp-get` is the convention-friendly installer
many Cursor / Continue / Claude Code users reach for first
(`npx mcp-get install <name>`).

## Entry format

`packages/swiss-caselaw.json` (paste verbatim):

```json
{
  "name": "swiss-caselaw",
  "displayName": "Swiss Caselaw",
  "description": "969,000+ Swiss court decisions, 5,500 federal + 15,700 cantonal statutes, 9M-edge citation graph, scholarly commentary (DE/FR/IT). 6-layer verification architecture catches AI hallucination of case references before answers ship. CC0 data, MIT code, no API key.",
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

## PR description (paste verbatim)

```
Add Swiss Caselaw MCP server (legal/government, hosted)

Adds a hosted (no-install) entry for OpenCaseLaw — a public Swiss
caselaw + legislation MCP server. Live endpoint at
https://mcp.opencaselaw.ch, no auth required.

Coverage:
- 969,000+ Swiss court decisions (federal + cantonal, 1875-today, DE/FR/IT)
- 5,500 federal statutes (Fedlex), 15,700 cantonal statutes
- 362 scholarly commentaries (OnlineKommentar.ch, CC-BY-4.0)
- 9M-edge citation graph
- 31 specialised tools (search, citation graph, statute lookup,
  doctrine timelines, leading-case discovery, exam-question
  generation, verification rail)

Distinctive feature: a 6-layer verification architecture that catches
AI hallucination of case references — peer-reviewed measurement
(Stanford RegLab 2024) shows 58-82% of legal LLM queries produce
fabricated authority on general models, 17-33% even on commercial
legal-RAG; OpenCaseLaw verifies every emitted citation against the
corpus before returning the answer to the user.

Maintained daily; commit history & scraper-health dashboard public.
```

## How to file

```bash
gh repo fork michaellatman/mcp-get --clone --remote=false
cd mcp-get
git checkout -b add-swiss-caselaw
mkdir -p packages
# paste the json above into packages/swiss-caselaw.json
git add packages/swiss-caselaw.json
git commit -m "Add Swiss Caselaw MCP server"
gh pr create --title "Add Swiss Caselaw MCP server" \
             --body-file ../caselaw-repo-1/submissions/mcp-registries/04-mcp-get.md
```
