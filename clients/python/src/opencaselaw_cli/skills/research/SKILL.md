---
name: research
description: Research a Swiss legal question with the ocl CLI (search, read, follow citations, cite, verify) and hand back an answer whose every citation and quotation was checked against the OpenCaseLaw corpus.
---

# Legal research with `ocl`

Use when asked a question of Swiss law that needs authorities: what the
Federal Supreme Court or a cantonal court held, which statute article
applies, what the practice is. Install: `pipx install opencaselaw-cli` (or
`uv tool install opencaselaw-cli`). Every command prints JSON when piped;
`--fields` keeps the keys you need; exit code 4 means something did not
resolve, 3 means the service or network failed.

## Steps

1. Frame the question as search terms in the language of the likely
   decisions (German for most BGE, French for Romandie courts). Search:
   `ocl decisions search '<terms>' --max-results 20 --format jsonl --fields decision_id,citation_string_de,decision_date,court`
   (add `--court bge`, `--language fr`, `--date-from` as needed). A text
   search is one ranked window; `total` is a candidate pool, not a count.
2. Read before citing. For each candidate worth it:
   `ocl tool call get_regeste decision_id=<id>` (BGE headnote) and
   `ocl decisions passage <id> <number>` for the considerations. Never
   summarise a decision from its title.
3. Follow the law: `ocl tool call find_leading_cases query='<terms>'`,
   `ocl citations list <id> --direction incoming --limit 20` (who cites it
   later), `ocl tool call find_relevant_erwaegung decision_id=<id> query='<terms>'`.
   Statutes: `ocl laws get OR --article 336` (`--as-of YYYY-MM-DD` for the
   text in force at a date; a `text_status` other than ok means no text was
   recovered). Practice and scholarship: `ocl tool call search_practice`,
   `ocl tool call search_scholarship`, `ocl tool call search_commentaries`.
4. Cite only through the service: `ocl cite <reference> --pinpoint <n>`
   gives the string to copy verbatim (`citation_string`); an inline
   pinpoint in the reference is verified. Quote only what `passage` served,
   and keep the served wording.
5. Before answering, run the citation check on your own draft
   (`ocl citations resolve --input refs.jsonl --format jsonl`) and the
   quotation check (`ocl quotes check --input quotes.jsonl --format jsonl`).
   Fix or drop anything that is not `resolved` / `exact`; report what you
   dropped.
6. Say what the tool established (existence, identity, wording) and what it
   did not (legal support, whether a decision is still good law). Keep the
   evidence: `ocl bundle create '<terms>' --max-results 10 --passage <n> --out evidence`.

## Rules

- Citation strings and passage text come from the service unchanged; never
  compose a citation or edit a quotation.
- Never present a `close_match` or a `service_candidate` as the cited case.
- `ocl tool list` shows every server tool; `ocl tool schema <name>` its
  arguments. `ocl doctor` checks the connection first when things fail.
