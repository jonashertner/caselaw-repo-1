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
   each, exactly as the author wrote it, long form included:
   `{"reference": "BGer 4A_747/2012 vom 5. April 2013, E. 3.2"}`. A pinpoint
   written in the reference is read from it; use the separate `pinpoint` field
   only when the number is not part of the reference.
2. Run `ocl citations resolve --input references.jsonl --format jsonl > resolution.jsonl`
   (install: `pipx install opencaselaw-cli` or `uv tool install opencaselaw-cli`).
3. Read every row. `resolved` means the decision exists and carries a label the
   author wrote (and the pinpoint, if any, exists: `pinpoint_status: retrieved`).
   `pinpoint_unavailable` means the decision exists but the numbered passage is
   not indexed; with `pinpoint_status: parent_retrieved` the parent Erwägung's
   text is in the row and the lettered part must be located inside it. `discrepancy`
   means the decision exists but the date or the docket the author wrote next
   to it is wrong: report `discrepancies` verbatim. `missing` means no such
   decision in the corpus: flag it for the author; never replace it with a
   "close" citation. `ambiguous` means several decisions carry the label: name
   the court, ask which one, or use the `decision_id`. `unrecognized` means the
   service proposed a decision that does not carry the author's label: treat it
   as unverified, and never report `service_candidate` as the citation.
4. For quoted passages, fetch the verbatim text with
   `ocl decisions passage <decision_id> <number>` and compare the quote with
   `text_plain` (the served text with the service's Markdown cross-reference
   links reduced to their labels). Report differences; do not paraphrase inside
   quotation marks.
5. Report per citation: reference, status, decision_id, canonical citation
   string from the service, pinpoint status, discrepancies. State that this
   establishes existence and wording only, not that the authority supports the
   proposition.

## Rules

- Citation strings and quotations come from the service unchanged.
- Exit code 4 means partial or unresolved: say so, list the items.
- Do not run broad text searches to "find a better citation" unless asked.
