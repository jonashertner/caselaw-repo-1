# Field test of the research CLI, 2026-09-05

Five agents used the published `ocl` 0.2.1 in distinct scenarios (French ATF/TF
memo, Italian/German memo with pre-2000 BGE, statutes and evidence bundles,
adversarial citation hygiene, a research workflow end to end). Each started
from real decisions found through search, rewrote the references the way a
lawyer writes them, and checked whether the resolver came back to the same
decision (179 round trips; 8-19 deliberately wrong references per scenario,
of which all but a few were rejected as intended). One skeptic per finding
re-ran the reproduction and classified it: 44 confirmed, 2 refuted, 2 left
unverified. Raw material: the scratchpad folder `agent-test-2/`.

## Client (fixed in ocl 0.3.0)

Long forms around a spaced docket and the service's own cantonal citation
strings came back `missing`/`unrecognized`; inline pinpoints were folded away
instead of verified; lettered pinpoints aborted the batch; a docket typo next
to a BGE label resolved to an unrelated ruling; wrong dates passed; a Geneva
summary made a canonical BGer string ambiguous; `unrecognized` rows carried an
unrelated decision's id; bundle counters and INDEX disagreed after `add`;
`verify`/`diff` had no text output; exit codes mapped per command. All of
these are covered by the 0.3.0 changelog and tests.

## Server (open; proposed fixes from the verification stage)

1. `laws get OR --article 336c --as-of 2010-01-01` returns the marginal note
   of Art. 336d (`text_source: fedlex_pdf`, exit 0). Cause verified on the
   Fedlex PDF: the body header is `Art. 336c147` (footnote superscript glued
   to the number), which `_pdf_article_excerpt` / `_PDF_ANY_ARTICLE_MARKER`
   does not match. Fix: allow an optional glued 1-3 digit superscript after
   the article number, anchored to end of line. Severity high (wrong statute
   text with a success exit).
2. `/api/cite` does not parse its own long-form BGer strings, spaced federal
   dockets (`4A 535/2018`) or its own cantonal strings (`Obergericht ZH
   LA210005 vom 15. Juni 2021`); the client now compensates. Fix in
   `_resolve_decision_id`: look up `docket_number IN (key, key with space, key
   with dot)` and accept the citation-string shape the server itself emits.
3. `/api/erwaegung` citation strings for cantonal decisions omit the court
   (`Gericht LA210005 vom ...`) while `/api/cite` says `Obergericht ZH ...`:
   the hand-built `decision_for_citation` dicts in `_handle_get_erwaegung` and
   `_handle_find_relevant_erwaegung` lack `canton`. One-line fix each (R1).
4. `_bge_ref_candidates` / `_parse_bge_ref_text`: wrapping parentheses,
   trailing punctuation and page pinpoints (`S. 357`) make an existing BGE
   `exists: false`.
5. Duplicate BGE identities: `bge_139 I 57` (date 2011-02-14, from the facts)
   and `bge_BGE_139_I_57` (date 1995-06-28, from a statute quoted in the
   regeste), `bge_138 III 416` / `bge_143 III 624` with wrong dates; search
   returns one id, resolve the other. Belongs to the representation merge
   (#40); short of it, expose the canonical id on search hits and attach a
   `decision_date_warning` where the BGE volume year contradicts the date.
6. 123 BGE rows carry a 31 December `decision_date` taken from "in der bis
   31. Dezember YYYY gültig gewesenen Fassung" (volume V social insurance);
   `derive_from_text.py` should prefer the BGE header date and skip dates
   preceded by "bis / jusqu'au / fino al".
7. Structure extraction: modern Italian BGE (`Dai considerandi:`) have no
   structured Erwägungen (143 III 38, 142 V 395, 142 V 349, 149 III 393); a
   GE decision is segmented on a date at a line start (`4 février 2013` became
   consid. 4). Fix in `search_stack/extract_decision_structure.py`: add the
   Italian headers; require the trailing dot for depth-1 candidates.
8. `research_contracts.py`: `text_source` and `verbatim_quotation` are served
   by `get_erwaegung` but absent from `ErwaegungResponse`; `degraded` is
   documented for search but not declared. Add them as optional fields so the
   curated OpenAPI matches the wire.

## Refuted

- A `/` in a pinpoint is percent-encoded by the client; the raw 404 is the
  server route. The client now falls back to the parent number.
- Search `total` growing with the window is documented and deliberate
  (candidate pool = max(60, (offset+limit)*4)).
