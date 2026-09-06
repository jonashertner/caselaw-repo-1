---
name: citation-check
description: Verify every Swiss case citation in a draft against the OpenCaseLaw corpus with the ocl CLI before answering or filing; report resolved, missing, ambiguous and pinpoint status; never invent or "fix" a citation.
---

# Citation check with `ocl`

Use this whenever a draft, memo, brief or answer cites Swiss court decisions
(BGE/ATF/DTF, BGer dockets such as 4A_747/2012, cantonal dockets) or quotes an
Erwägung. Existence is checked against the corpus; legal support is not.

## Steps

1. If the draft is a file (.docx, .md, .html, .txt), run
   `ocl check <draft> --format json --no-report > check.json`: the citations
   and the quotations next to them are found in the prose and checked in one
   go (install: `pipx install opencaselaw-cli` or `uv tool install opencaselaw-cli`).
   Otherwise extract every citation into `references.jsonl`, one JSON line
   each, exactly as the author wrote it, long form included:
   `{"reference": "BGer 4A_747/2012 vom 5. April 2013, E. 3.2"}`, with a
   `quote` field for quoted passages.
2. Run `ocl citations resolve --input references.jsonl --format jsonl > resolution.jsonl`
   for a list; the rows have the same shape as `check`'s `results`.
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
4. For quoted passages, run `ocl quotes check --input quotes.jsonl --format jsonl`
   with one row per quotation (`reference`, `pinpoint` when cited, `quote`
   exactly as the author wrote it). `exact` stands; for `near` report the
   listed differences and replace the quotation with the served wording; for
   `not_found` tell the author the quotation is not in the served text that
   was compared (the closest served text is shown); for `unverifiable` (no
   served text: offline without an indexed pinpoint) say the quotation was
   not checked and must be checked against the decision, never that it is
   not there. Do not paraphrase inside quotation marks.
5. Report per citation: reference, status, decision_id, canonical citation
   string from the service, pinpoint status, discrepancies. State that this
   establishes existence and wording only, not that the authority supports the
   proposition.

## Rules

- Citation strings and quotations come from the service unchanged.
- The label written first in a reference is the citation; a docket mentioned
  later (`vgl. auch ...`, a joined file) is reported under `other_dockets` and
  is never the decision cited.
- Exit code 4 means partial or unresolved: say so, list the items.
- Do not run broad text searches to "find a better citation" unless asked.
