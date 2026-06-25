# Submission to Anthropic's MCP server directory

**URL:** https://www.anthropic.com/news/mcp-server-directory (and the
"Add a server" form linked from there) — Anthropic curates a featured
list that ships in the Claude Desktop / Claude Code "Add connector" UI.

**Method:** contact form / partner-program email.
**Timeline:** weeks (manual review by Anthropic's developer relations
team).

## Email body (paste verbatim — to `partners-mcp@anthropic.com` or via the public form)

**Subject:** Submission for Anthropic MCP server directory — OpenCaseLaw (Swiss caselaw, verification-first)

```
Hi Anthropic team,

I'd like to submit OpenCaseLaw for inclusion in the Anthropic-curated
MCP server directory.

WHAT IT IS
OpenCaseLaw is a public, open MCP server for the entire published
Swiss federal and cantonal court corpus, federal and cantonal
legislation, scholarly commentary, and the citation graph. 990,000+
decisions, 5,519 federal + 15,589 cantonal statutes, 362
commentaries, 8.65M citation edges. Daily refresh. CC0 data, MIT code.
No API key, no registration.

Live: https://mcp.opencaselaw.ch  (auth: none, /health green)
Web:  https://opencaselaw.ch
Code: https://github.com/jonashertner/caselaw-repo-1
Spec: https://github.com/jonashertner/caselaw-repo-1/blob/main/server.json

WHY IT'S INTERESTING FOR THE DIRECTORY
The server is built around a six-layer verification architecture that
directly addresses the hallucination class measured by Stanford
RegLab (Dahl et al. 2024 — Large Legal Fictions; Magesh et al. 2024
— Hallucination-Free?): 58–88 % of legal queries to general LLMs and
17–33 % to commercial legal-RAG tools produce a fabricated authority.
Our attest_response tool parses every case reference an LLM emits,
verifies it against the corpus + statute mirror + the cited Erwägung
text, and returns clickable Markdown links to the canonical source.
The server's SYSTEM_PROMPT (R1–R8) embeds the contract so any
connecting model inherits it automatically — and Claude in particular
follows it well in our internal benchmarks.

We've also published Swiss Legal RAG Bench (HF dataset
voilaj/swiss-legal-rag-bench), modelled on Butler & Butler's Legal
RAG Bench (Isaacus, March 2026), measuring correctness, groundedness
and retrieval accuracy. Live baseline using Claude Sonnet 4.6 + this
MCP server: 100 % correctness, 90 % groundedness, 70 % retrieval
accuracy on the seed set.

QUALITY SIGNALS
- 9 months of daily uptime, atomic-swap rebuild
- Public coverage page: https://opencaselaw.ch/coverage/
- Per-call LLM cost telemetry shipped, public methodology
- 119/119 web tests, 32 export-format tests
- Multilingual (DE/FR/IT) and multi-jurisdictional (federal + 26
  cantons) by design — Switzerland is a real test of cross-language
  retrieval for any RAG system

WHAT WE NEED
Inclusion in the curated directory ships in Claude Desktop's
"Add connector" UI and Claude Code's `mcp add` discovery. For Swiss
lawyers, students and academic researchers, this is how the tool
will reach them — Switzerland's Anwaltschaft is small (≈11k members)
and conservative; the canonical surfacing inside Claude is more
important than search-engine ranking.

I'm happy to provide additional information or join a call. Manifest
attached (server.json) for direct ingestion.

Best,
Jonas Hertner
team@jonashertner.com
opencaselaw.ch
```

## Attachments

- `server.json` (this repo's root)
- A screenshot of `mcp.opencaselaw.ch/health` returning current state
- Optional: 30-second screen recording of adding the connector to
  Claude and asking it a Swiss-law question with the verified link
  appearing in the answer
