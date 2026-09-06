# MCP response shapes

Use `CallToolResult.structuredContent` for machine-readable data when a tool
advertises `outputSchema`. In the Python MCP SDK this attribute is
`result.structuredContent`; text blocks are in `result.content` and the error
flag is `result.isError`. JSON inside `TextContent.text` is **not** the same as
`structuredContent`.

This reference describes the current repository contract, not a promise that
an older deployment has the same capabilities. Read `tools/list` once when
initializing a client. Earlier versions exposed fewer schemas: current core
research tools also return structured data, independently of `OCL_UI_WIDGETS`.
The widget flag controls UI metadata, not whether these contracts exist.

## Per-tool formats

- **Text**: formatted prose/Markdown in text blocks; no stable dictionary
  fields. Render the blocks rather than scraping headings or table columns.
- **JSON text**: the success handler serializes JSON into a text block, but
  does not declare an output schema. Listed keys describe current success
  reports, not a validated transport contract. Error responses, ignored-argument
  notices and client-dependent open-access notes can contain other text; do
  not blindly `json.loads` every text block.
- **Text/JSON + structured**: use the separate structured object. Human text
  may include extra notes. `search` and `fetch` additionally serialize their
  structured object as JSON text for deep-research compatibility.

For schema-bearing tools, inspect the advertised schema for required versus
optional fields and allow additional fields. Citation fields can be absent or
null: never synthesize a missing citation from an ID. The core research schemas
accept an `error` object as an alternative to success. Check both `isError` and
`payload.get("error")`; a successful `cite` with `exists: false` means the
reference did not resolve, not a transport failure. Empty result lists are
valid outcomes.

| Tool | Success format | Fields / consumer contract |
| --- | --- | --- |
| `search` | JSON + structured | `results[]`: `id`, `title`, `url`, `snippet` |
| `fetch` | JSON + structured | `id`, `title`, `text`, `url`, `metadata` |
| `search_decisions` | Text + structured | `results[]`, `total`, `total_is_lower_bound`, `returned`, `limit`, `offset`, `has_more`, `next_offset` |
| `get_decision` | Text + structured | `decision_id`; optional citation fields, `full_text` and truncation metadata |
| `get_decisions` | Text | Read `content` text blocks; no declared dictionary fields |
| `list_courts` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_statistics` | Text | Summary prose followed by JSON; the whole block is not JSON |
| `find_citations` | Text + structured | `decision_id`, `direction`, `limit`, `offset`; direction-specific `incoming` / `outgoing` and pagination fields |
| `find_appeal_chain` | Text | Read `content` text blocks; no declared dictionary fields |
| `find_leading_cases` | Text | Read `content` text blocks; no declared dictionary fields |
| `analyze_legal_trend` | Text | Read `content` text blocks; no declared dictionary fields |
| `draft_mock_decision` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_case_brief` | JSON text | `decision_id`, `regeste`, `sachverhalt`, `key_erwaegungen`, citation fields |
| `get_decision_structure` | JSON text | `decision_id`, `erwaegungen_paragraphs`, `dispositiv_orders`, `extraction_methods` |
| `get_erwaegung` | JSON + structured | `decision_id`, `e_number`, `text`; optional citation fields and `parts` |
| `find_relevant_erwaegung` | JSON text | `decision_id`, `matches`, `no_match`; optional `confidence`, `suppression`, `best_low_confidence_match` |
| `get_article_purpose` | JSON text | `sr_number`, `article`, `language`, `sources` |
| `search_botschaft` | JSON text | `query`, `language_filter`, `total`, `results` |
| `get_article_history` | JSON text | `sr_number`, `article`, `statute`, `timeline`, `summary` |
| `get_regeste` | JSON text | `decision_id`, `regeste`; optional citation fields |
| `check_claim_support` | JSON text | Claim-support report; no declared output schema |
| `attest_response` | JSON text | `ok`, `citations_found`, `citations_ok`, `issues_count`, `issues_by_category`, `issues` |
| `cite` | JSON + structured | `exists`; optional `decision_id`, `citation_string_de`, `citation_string_fr`, `citation_string_it`, `canonical_url` |
| `get_doctrine` | JSON text | `query`, `statute`, `doctrine_summary`, `leading_cases`, `doctrine_timeline` |
| `generate_exam_question` | JSON text | `fact_pattern`, `difficulty`, `hint`, `source_decision_id`, `analysis` |
| `get_law` | Text + structured | `sr_number`; optional `articles`, `title`, `language`, `source_url` and version fields |
| `search_laws` | Text + structured | `query`, `query_lang`, `total`, `federal_hits`, `cantonal_hits`, `hits[]` |
| `get_commentary` | Text | Read `content` text blocks; no declared dictionary fields |
| `search_commentaries` | Text | Read `content` text blocks; no declared dictionary fields |
| `search_scholarship` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_scholarship` | Text | Read `content` text blocks; no declared dictionary fields |
| `find_scholarship_citing_statute` | Text | Read `content` text blocks; no declared dictionary fields |
| `find_scholarship_citing_decision` | Text | Read `content` text blocks; no declared dictionary fields |
| `list_scholarship_sources` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_scholarship_full_text` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_materialien` | JSON text | Preparatory-material report; no declared output schema |
| `search_materialien` | JSON text | Preparatory-material search report; no declared output schema |
| `search_practice` | Text | Read `content` text blocks; no declared dictionary fields |
| `get_practice` | Text | Read `content` text blocks; no declared dictionary fields |
| `search_legislation` | Text + structured | `query`, `total`, `hits[]` |
| `get_legislation` | Text | Read `content` text blocks; no declared dictionary fields |
| `browse_legislation_changes` | Text | Read `content` text blocks; no declared dictionary fields |
| `update_database` | Text | Local administrative tool; unavailable on the remote server |
| `check_update_status` | Text | Local administrative tool; unavailable on the remote server |

`results[]` from `search_decisions` contains decision records keyed by
`decision_id`. `decisions` is a legacy optional widget field; do not depend on
it instead of `results`. `has_more: false` does not prove a relevance-ranked
search enumerated the corpus, and `total_is_lower_bound` qualifies `total`.
For full decision text, inspect `full_text_truncated` and `full_text_url`.

The four compatibility/law-search tools (`search`, `fetch`, `search_laws`,
`search_legislation`) have permissive schemas. The core research tools use
versioned models from [research_contracts.py](https://github.com/jonashertner/opencaselaw/blob/main/research_contracts.py);
optional fields there are not guaranteed to appear on every success. The REST
`lookup` contract is not an additional MCP tool.

## Example: read stored citation fields

Given an initialized Python MCP `ClientSession`, this function searches and
returns up to three server-provided German citation strings. It uses `cite`
for the citation field, never parses search Markdown or constructs a citation.
A missing field or an older server without structured results is an explicit
error, rather than a successful run that prints nothing.

```python
async def top_citations(session, query):
    async def payload(tool, arguments):
        result = await session.call_tool(tool, arguments=arguments)
        data = result.structuredContent
        if result.isError or not isinstance(data, dict) or data.get("error"):
            raise RuntimeError(f"{tool} did not return a structured success")
        return data

    found = await payload("search_decisions", {"query": query, "limit": 3})
    if not isinstance(found.get("results"), list):
        raise RuntimeError("search_decisions results are missing")
    citations = []
    for decision in found["results"][:3]:
        cited = await payload("cite", {"reference": decision["decision_id"]})
        citation = cited.get("citation_string_de")
        if not cited.get("exists") or not isinstance(citation, str) or not citation:
            raise RuntimeError("No stored German citation is available")
        citations.append(citation)
    return citations
```

Print the returned strings directly. An empty list means the search returned
no decisions; it does not authorize inventing alternatives. To display a
text-only tool, join only blocks whose `type` is `"text"`, preserving their
contents. Do not treat generated summaries or low-confidence candidate passages
as verbatim quotations.

Source of truth: [_list_tools and the call wrapper](https://github.com/jonashertner/opencaselaw/blob/main/mcp_server.py), plus
[research contracts](https://github.com/jonashertner/opencaselaw/blob/main/research_contracts.py). The offline documentation
test checks table coverage/schema classifications and executes this example
against controlled MCP results.
