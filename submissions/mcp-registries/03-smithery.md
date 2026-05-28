# Submission to Smithery (smithery.ai)

**URL:** https://smithery.ai/new
**Method:** Web form, GitHub OAuth + repo selection
**Why it matters:** Smithery is the registry that the ChatGPT app
catalog reads at app-install time, so this is the highest-leverage
single submission for ChatGPT-side discovery.

## Web-form fields (paste verbatim)

| Field | Value |
|-------|-------|
| **GitHub repo** | `jonashertner/caselaw-repo-1` |
| **Server name** | `Swiss Caselaw` |
| **Display title** | `OpenCaseLaw — Swiss Caselaw, Statutes & Doctrine` |
| **Category** | Legal / Government / Knowledge |
| **Tags** | `legal`, `swiss-law`, `caselaw`, `legislation`, `multilingual`, `verification`, `rag`, `citation-graph` |
| **Hosted endpoint** | `https://mcp.opencaselaw.ch` |
| **Transport** | Streamable HTTP (preferred) + SSE (legacy) |
| **Auth** | None |
| **Licence (data)** | CC0-1.0 |
| **Licence (code)** | MIT |
| **Maintainer email** | `team@jonashertner.com` |

## Description (300-char limit)

```
969,000+ Swiss court decisions, 5,500 federal + 15,700 cantonal statutes, 9M-edge citation graph, scholarly commentary — DE/FR/IT, daily refresh, CC0/MIT, no API key. 6-layer verification architecture catches AI hallucination of case references before answers ship.
```

## Long description (Markdown — for the listing page)

```md
OpenCaseLaw is the public infrastructure project for Swiss caselaw and legislation. The MCP server serves the entire published Swiss federal and cantonal court corpus, the federal statute book (Fedlex mirror), 19 cantonal portals, scholarly commentary from OnlineKommentar.ch, open-access Swiss legal scholarship (30,000+ records from 22 sources, 30 % with full-text), and the full citation graph (8.09M edges) — through 38 specialised tools.

**What's inside**
- 969,000+ decisions, 1875–today, all federal courts + 26 cantons
- 5,500 federal + 15,700 cantonal statutes (full article text, FTS5)
- 362 commentaries from OnlineKommentar.ch (CC-BY-4.0)
- Citation graph with appeal-chain + leading-case discovery

**Verification architecture (the differentiator)**
Peer-reviewed measurement (Stanford RegLab, 2024) found that 58–82 % of legal queries to general-purpose LLMs produce a fabricated authority, and 17–33 % even for commercial legal-RAG tools. OpenCaseLaw closes this gap with a six-layer verification rail: every case reference an LLM emits is parsed, verified against the corpus, validated for pinpoint correctness, statute-references checked against Fedlex, quotations matched verbatim against the cited Erwägung, decision dates cross-checked, and (opt-in) the proposition itself judged by an independent Sonnet judge against the source text. Verified citations come back as clickable Markdown links to the canonical source.

**Open**
Data CC0, code MIT, no API key, no registration. Daily refresh. Every component on GitHub: scrapers, indexer, MCP server, REST API, verification logic.
```

## Setup snippet (for the listing's "Connect" section)

The user copy-paste path varies by client; the most common are:

**Claude Desktop / Claude Code:**
```bash
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch
```

**ChatGPT:** Settings → Apps → Developer mode → Create app → URL `https://mcp.opencaselaw.ch/sse` → Auth: None.

**Cursor / Gemini CLI:**
```json
{ "mcpServers": { "swiss-caselaw": { "url": "https://mcp.opencaselaw.ch" } } }
```

## Verification screenshot (attach in the form)

Fresh screenshot of `mcp.opencaselaw.ch/health` returning `{"status":"ok","decisions":969738}` — captures the live state at submission time.
