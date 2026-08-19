# Adversarial review — GPT-5.6-Sol (xhigh) via Codex, 2026-08-13

Reviewer ran read-only against paper.tex, tables/, data/ and the
underlying code, re-deriving numbers independently (jq over the
artifacts, direct regex tests against `search_stack/reference_extraction`,
its own two-proportion test). 21 findings: 8 blocking, 10 major,
3 minor. Codex could not write this file (read-only mount); findings
captured verbatim from the job result (`codex resume
019ffb7d-7600-75a3-ab09-c0211f25b5b4`), reconciled below by finding.

Reconciliation executed 2026-08-13/14 against the 2026-08-13 freeze
scan (both databases hashed at scan time; `data/db_hashes.json`).
Post-fix headline: 591 / 584,524 distinct edges = 1,011 ppm
(Wilson 933–1,096; decision bootstrap 931–1,093).

| # | Severity | Claim | Verdict | Action |
|---|---|---|---|---|
| 1 | blocking | Denominator unit is decision–locus edges, not tokens | **CONFIRMED** | Unit relabelled everywhere ("distinct citation edges, one per decision and cited locus"); §3.4 states the edge unit and its conservatism explicitly |
| 2 | blocking | Denominator inflated by citation_targets join multiplicity | **CONFIRMED** (366,400 pairs carry 2 target rows — the BGE/BGer dual-identity twins; verified two ways) | Scan rewritten to DISTINCT + EXISTS; denominator 899,560 → 584,524; rate 657 → 1,011 ppm; pool deduplicated (1,646 unique pairs) |
| 3 | blocking | Page-beyond window rule is inductive, not provable | **CONFIRMED as stated; closed externally** | §3.1 now tiers the conditions: division-absent/volume-out-of-range decidable from structure; page-beyond inductive, closed by the resolver's interior-page semantics (404 = no decision contains the page). 542/591 findings resolver-confirmed; 49 below coverage rest on corpus+window, stated |
| 4 | blocking | Ia/Ib citations never extracted | **CONFIRMED** (direct test) | Scoped as grammar-coverage guard + limitation with measured size: 10,723 occurrences / 1.4 % of surface (released `scripts/p2_count_iab.py`). Grammar fix queued for a pipeline-gated rebuild, not patched mid-study |
| 5 | blocking | Series index not deduplicated; completeness asserted | **CONFIRMED** (50,467 → 35,420 unique) | `_bge_series_index` deduplicates at source (windows unaffected — gap-based); figure counts corrected; completeness claim now cites the corpus paper and the resolver cross-check rather than asserting |
| 6 | blocking | Voice/source screen unfinished; FR/IT contexts missing (355/356 FR) | **CONFIRMED** — context attach searched normalized `BGE` in texts that write `ATF`/`DTF` | Prefix-alternation fix; contexts now 0 missing in all languages; quote screen re-run (25 → 49 flagged); all 49 re-read: 46 court, 3 party-attributed; disclosed as single-reader pass |
| 7 | blocking | Orders-of-magnitude ratio dimensionally invalid | **PARTIALLY ACCEPTED** | Ratio language removed; rates stated side by side in own units ("not divisible into a ratio"); comparability table remains the vehicle. Rejected the implication that no comparison may be drawn — the table's row-by-row alignment is the comparison |
| 8 | blocking | "First" claim rests on unfinished audit; title broader than object | **PARTIALLY ACCEPTED** | Claim scoped "to our knowledge, the first decidability-based answer"; related-work footnote documents the sweep and its open edges. Title retained: the tiering + resolver confirmation ground "provable" for the finding set |
| 9 | major | Dahl 2024 mischaracterized (9 of 14 reference-based; pooled rates span task types) | **CONFIRMED** | Abstract, related work and comparability table corrected (9 ref-based/5 ref-free; error definition spans existence/court/citation/author/disposition; n=5,000 with n=100 high-complexity) |
| 10 | major | CIs lack a defined inferential population | **CONFIRMED** | §4.1: census stated; intervals quantify decision-generating-process uncertainty, window read as one draw |
| 11 | major | Bootstrap clusters at the wrong level (within-decision already deduped; real dependence is cross-decision templates) | **CONFIRMED** | §4.1 states both: why Wilson ≈ bootstrap here, that cross-decision template dependence is uncaptured, and that the distinct-locus view is the least-affected reading |
| 12 | major | Sensitivity rows incoherent; language-equality overclaim; fed-cant difference (p≈0.045) denied; `other` class omitted | **CONFIRMED** | Mixed-unit dedup row removed (distinct view lives in Table 1); `other` row added; per-year rates now use per-year denominators from the cluster pairs; prose reports χ²=3.8 (no evidence of language difference ≠ equality) and states the federal 877 vs cantonal 1,064 ppm difference with nominal p=0.04 |
| 13 | major | Mechanism totals are signature collisions, not diagnoses | **CONFIRMED** | Reframed as priority-ordered signatures with multiplicity disclosed (464 multi / 115 single / 12 none); causal language removed |
| 14 | major | Repair candidates prove existence, not intent | **CONFIRMED** | §4.2 states it in those words; intent claimed only where the document itself answers (the appellate case); showcase ambiguity (148 I 356 → IV and V) already reflected in the figure fix |
| 15 | major | Pre-1955 "bounded and small" overstates; 278 is the risk pool (47 % of findings) | **CONFIRMED** | Limitation rewritten: 278 named as the unadjudicated risk pool of this one class; other wrong-but-existent channels explicitly unbounded; "small" deleted |
| 16 | major | "Zero scan false positives" overstates the probe | **CONFIRMED** | Scoped: zero nonresolutions among 254 probed in-coverage tokens; verifies normalized-token nonresolution, not source typography |
| 17 | major | Regeneration is formatting-level, not independent reproduction; no DB hashes; tar excludes data | **CONFIRMED** | Both scanned databases SHA-256-hashed at scan time (mtimes match graph_mtime to the microsecond); hashes merged into MANIFEST; §6 states the regeneration/reproduction distinction; submission package to include data/ |
| 18 | major | Local-damage-vs-invention narrative exceeds evidence | **CONFIRMED** | "Carry their own diagnosis" → "match a local-damage signature"; contrast sentence now concedes the machine studies' broader misgrounding definitions |
| 19 | minor | Open-volume rule described inconsistently; nonexistence is time-indexed | **CONFIRMED** | §3.1: nonexistence asserted as of the citing decision's date; exemption wording aligned with code |
| 20 | minor | 2026 stratum partial-year; date-population mismatch | **CONFIRMED** | Sensitivity caption marks 2026 as partial through scan date |
| 21 | minor | Remediation facts not reader-verifiable | **CONFIRMED** (reviewer independently checked the mailbox and found the summary accurate) | §4.4 closes with an explicit author-reported statement |

## Reviewer's closing claim

"The strongest defensible result currently is much narrower: 292
distinct normalized reporter loci, 254 of which returned 404 within the
public resolver's stated coverage."

**Response**: after reconciliation the defensible result is the full
statement now in the paper: 591 distinct citation edges provably
nonexistent out of 584,524, of which 542 carry resolver-confirmed
tokens and 49 rest on the corpus proof for pre-1954 volumes; unit,
tiers, signatures, strata and bounds all stated. The reviewer's
narrower formulation was the correct description of the paper *before*
the fixes; the fixes exist because of it.

## Full verbatim findings

See the Codex session (`codex resume 019ffb7d-7600-75a3-ab09-c0211f25b5b4`)
or the captured transcript in the session scratchpad; each finding's
quote and location is reproduced in the table context above.
