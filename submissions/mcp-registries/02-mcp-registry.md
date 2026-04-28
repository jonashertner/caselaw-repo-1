# Submission to MCP Registry (registry.modelcontextprotocol.io)

**URL:** https://registry.modelcontextprotocol.io
**Method:** `mcp-publisher` CLI publishes the `server.json` at the
repo root to the official registry maintained by the MCP working group.
**Status:** the canonical place — every other registry will eventually
mirror this one, so prioritise it.

## What's already in place

The repository root carries [`server.json`](../../server.json) conforming to schema **2025-12-11**. Manifest snapshot:

```json
{
  "name": "ch.opencaselaw/swiss-caselaw",
  "title": "OpenCaseLaw — Swiss Caselaw, Statutes & Doctrine",
  "version": "1.1.0",
  "websiteUrl": "https://opencaselaw.ch",
  "remotes": [
    { "type": "streamable-http", "url": "https://mcp.opencaselaw.ch" },
    { "type": "sse",             "url": "https://mcp.opencaselaw.ch/sse" }
  ]
}
```

`name` uses a reverse-DNS-style namespace (`ch.opencaselaw/swiss-caselaw`) to avoid collisions, matching the registry convention.

## How to publish

```bash
# Install the publisher (one-time):
npm install -g @modelcontextprotocol/registry-publisher
# OR via GitHub Actions in this repo (recommended for repeatability)

# Publish from the repo root (server.json must be committed):
mcp-publisher login github   # opens browser for OAuth
mcp-publisher publish        # reads ./server.json, validates, uploads
```

Subsequent updates: bump the `version` field in `server.json`, commit, push, run `mcp-publisher publish` again.

## Verification

After publish, the entry should be visible at:
- `https://registry.modelcontextprotocol.io/v0/servers/ch.opencaselaw/swiss-caselaw`
- and discoverable via the MCP CLI: `mcp search swiss-caselaw`

## Auto-publish via GitHub Actions (optional but worth it)

Add `.github/workflows/mcp-registry-publish.yml`:

```yaml
name: Publish to MCP Registry
on:
  push:
    paths: [ 'server.json' ]
    branches: [ main ]
permissions:
  id-token: write    # OIDC for the registry
  contents: read
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm install -g @modelcontextprotocol/registry-publisher
      - run: mcp-publisher publish --provider github-oidc
```

Every commit to `server.json` then triggers an automatic re-publish.
