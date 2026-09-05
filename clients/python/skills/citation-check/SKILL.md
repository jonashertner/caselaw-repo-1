---
name: citation-check
description: Verify every Swiss case citation in a draft against the OpenCaseLaw corpus with the ocl CLI before answering or filing; report resolved, missing, ambiguous and pinpoint status; never invent or "fix" a citation.
---

# Citation check with `ocl`

Use this whenever a draft, memo, brief or answer cites Swiss court decisions
(BGE/ATF/DTF, BGer dockets such as 4A_747/2012, cantonal dockets) or quotes an
Erwägung. Existence is checked against the corpus; legal support is not.

## Steps

1. Extract every citation from the text into `references.jsonl`, one JSON line
   each: `{"reference": "BGE 136 III 513", "pinpoint": "2.3"}` (omit `pinpoint`
   when none is cited). Keep the author's wording; do not normalise.
2. Run `ocl citations resolve --input references.jsonl --format jsonl > resolution.jsonl`
   (install: `pipx install ./clients/python` from the repository checkout, or
   `uv tool install ./clients/python`).
3. Read every row. `resolved` means the decision exists (and the pinpoint, if
   given, exists in the index). `pinpoint_unavailable` means the decision exists
   but the numbered passage is not indexed: open the decision text before quoting.
   `missing` means no such decision in the corpus: flag it for the author; never
   replace it with a "close" citation. `ambiguous` means several decisions carry
   the label: ask which one, or use the `decision_id`.
4. For quoted passages, fetch the verbatim text with
   `ocl decisions passage <decision_id> <number>` and compare with the quote.
   Report differences; do not paraphrase inside quotation marks.
5. Report per citation: reference, status, decision_id, canonical citation
   string from the service, pinpoint status. State that this establishes
   existence and wording only, not that the authority supports the proposition.

## Rules

- Citation strings and quotations come from the service unchanged.
- Exit code 4 means partial or unresolved: say so, list the items.
- Do not run broad text searches to "find a better citation" unless asked.
