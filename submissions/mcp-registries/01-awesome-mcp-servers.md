# Submission to awesome-mcp-servers

**Repo:** https://github.com/punkpeye/awesome-mcp-servers
**Method:** Pull request
**Branch from main:** add a single line under the appropriate category

## Where to add

The list is grouped by domain. The right home is **"Legal"** (or
**"Government"** if Legal does not exist). If neither exists, add the
entry under **"Knowledge & Memory"** with a clear domain tag.

## The line to add (Markdown)

```md
- [Swiss Caselaw](https://github.com/jonashertner/caselaw-repo-1) - 990,000+ Swiss court decisions, 5,519 federal + 15,589 cantonal statutes, 8.65M-edge citation graph (DE/FR/IT). 6-layer verification architecture catches AI hallucination of case references. Public endpoint at `https://mcp.opencaselaw.ch`. CC0 data, MIT code.
```

## PR description (paste verbatim)

```
Add Swiss Caselaw MCP server (legal/government category)

This adds the OpenCaseLaw MCP server, which serves the entire published
Swiss federal and cantonal court corpus (990k+ decisions), Swiss
federal and cantonal legislation (5,519 + 15,589 articles), scholarly
commentary (362 OnlineKommentar.ch entries) and a 8.65M-edge citation
graph — all CC0 / MIT, no API key, no registration.

Of particular interest to LLM users: the server implements a six-layer
verification architecture (cite, check_claim_support, attest_response)
designed to catch the "fabricated case reference" failure mode
documented by Stanford RegLab (Dahl et al. 2024 — 58–88% on general
LLMs; Magesh et al. 2024 — 17–33% on commercial legal-RAG tools).
Every case reference an LLM emits is verified against the corpus
before the answer ships, and rendered as a clickable Markdown link
to the canonical source.

Live endpoint: https://mcp.opencaselaw.ch/health
GitHub: https://github.com/jonashertner/caselaw-repo-1
Web: https://opencaselaw.ch
Manifest: https://github.com/jonashertner/caselaw-repo-1/blob/main/server.json

Maintained daily; commit history & scraper-health dashboard public.
```

## How to file

```bash
# From this repo:
gh repo fork punkpeye/awesome-mcp-servers --clone --remote=false
cd awesome-mcp-servers
# Locate the Legal section, insert the line alphabetically.
git checkout -b add-swiss-caselaw
git commit -am "Add Swiss Caselaw MCP server"
gh pr create --title "Add Swiss Caselaw MCP server" \
             --body-file ../caselaw-repo-1/submissions/mcp-registries/01-awesome-mcp-servers.md
```
