# LinkedIn Launch Post — final

> Verified 2026-04-12 15:30 UTC. All numbers from live SQL.

---

**OpenCaseLaw.ch**

OpenCaseLaw publishes 965,000 Swiss court decisions from every federal court and all 26 cantons, dating back to 1875 — alongside the full text of every federal and cantonal law in three languages, a citation graph of 8.8 million links between decisions, and the legislative history of 2,500 federal laws.

The project started in mid-February 2026. CC0 data. MIT code. Updated daily.

Close to a thousand users per day — lawyers, judges, researchers, students, developers — working through Claude, ChatGPT, Cursor, Gemini, Grok, a Word add-in, or the REST API. Individual users are not tracked; the number is an estimate from aggregate server data. Traffic patterns suggest the data is also being quietly integrated into larger platforms.

**Practitioners and judges** search 965,000 decisions by topic or docket number. Trace who cites a landmark decision and whether it still holds. Pull a structured case brief: regeste, key Erwägungen with sub-paragraph references, every statute the court relied on, authority ranking by citation count. Look up any statute article and get the current text alongside the Federal Council Botschaft that proposed it — the legislative intent behind the provision, not just its text. Verify a quoted passage in a draft brief against the actual decision. Insert a correctly formatted Swiss legal citation into Word without leaving the document.

**Researchers** query 8.8 million citation edges by direction and confidence. Track how a court's reading of a single provision evolved across three decades. Map which cantonal courts follow which Bundesgericht ruling. Download the complete dataset from HuggingFace for quantitative work.

**Students** generate exam questions from real Bundesgericht fact patterns — the analysis stays hidden until they submit their answer. Look up doctrine on any article: statute text, authority-ranked leading cases, doctrinal timeline, scholarly commentary.

**Anyone** can search in plain language. The platform translates everyday terms to legal vocabulary: "Vaterschaftsurlaub" finds Art. 329g OR even though the statute says "Urlaub des andern Elternteils." Dog-leash rules in Geneva come back in French, from a German query. No subscription. No login.

23 research tools at mcp.opencaselaw.ch — works with any MCP client. For Grok and other function-calling LLMs, OpenAI-compatible tool definitions are at the repo. No API key. No sign-up.

No cookies. No user accounts. No query logging. The analytics code is open source.

opencaselaw.ch
github.com/jonashertner/caselaw-repo-1

---

## Numbers (verified 2026-04-12)

| Decisions | 965,341 |
|---|---|
| Courts | 100+ across all 26 cantons |
| Federal laws | 5,097 (125,378 articles, DE/FR/IT) |
| Cantonal laws | 26,043 legislative texts |
| Citation edges | 8,843,206 |
| Statute references | 11,344,975 |
| Laws with Botschaft refs | 2,491 |
| Articles with Botschaft refs | 33,092 |
| BV Botschaft digests | 128 articles (full legislative intent) |
| BV debate transcripts | 748 pages (Nationalrat + Ständerat) |
| Commentaries | 362 (OnlineKommentar.ch) |
| MCP tools | 23 |
| Users (est.) | ~1,000/day avg |
