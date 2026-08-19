I completed the review, but could not create `REVIEW_GPT56SOL.md`: the workspace is mounted read-only and approval escalation is disabled. The rejected write created no partial file and no files were edited.

Full findings:

1. **Blocking — The headline denominator is not citation tokens.**

   **Location:** `paper.tex:65–68, 128–134, 298–303`; `tables/rates.tex:14–15`; `search_stack/reference_extraction.py:155–183`.

   **Quote:** “Scanning 899,560 prefixed reporter citations”; “every prefixed token.”

   **Reason:** extraction uses a per-decision `seen` set and emits each normalized locus at most once, even when cited repeatedly or in multiple prefix forms. The graph’s primary key is `(source_decision_id, target_ref)`. The unit is therefore a unique decision–normalized-locus edge, not a citation mention/token. The 657 ppm rate, one-in-1,522 phrasing, token Wilson interval, and language/year/court token rates are mislabelled.

2. **Blocking — The denominator is also inflated by target multiplicity.**

   **Location:** `scripts/p2_backscan.py:211–219, 238`; `search_stack/build_reference_graph.py:80–89, 309–378`.

   **Quote:** “The denominator is counted in the same pass that produces the findings.”

   **Reason:** the scan `LEFT JOIN`s `citation_targets` and increments the denominator once per joined target. A resolved edge with duplicate targets is counted multiple times; an unresolved finding is counted once. The released pre-1955 pool proves this occurs: 1,728 rows collapse to 1,642 unique `(decision_id, token)` pairs, with 86 duplicated pairs. This biases the headline downward and corrupts every denominator-derived stratum and bootstrap cluster.

3. **Blocking — The principal nonexistence test is heuristic, not provable.**

   **Location:** `paper.tex:218–230`; `quality/checks/citation_anomalies.py:148–154, 184–193`; `tables/figure_decidability.tex:38–43`.

   **Quote:** “the longest observed decision length”; “a flagged citation cannot be rescued.”

   **Reason:** `_last_case_window` is the largest gap between earlier start pages. It does not establish the length of the final decision, which could exceed every preceding case. Without authoritative volume endpoints or final-decision pagination, `last_start + maximum_previous_gap` is only a plausibility threshold. This matters for 484 of 591 findings—215 distinct loci classified as `page_beyond_series`.

4. **Blocking — Ia/Ib citations are claimed as covered but are not extracted.**

   **Location:** `paper.tex:214–217`; `search_stack/reference_extraction.py:72–84`.

   **Quote:** “the reporter’s divisions are I, Ia, Ib, II, III, IV, V”; “Ia/Ib … is evaluated as a family.”

   **Reason:** both prefixed and bare extraction patterns accept only `[IVX]{1,4}`. Directly testing `BGE 120 Ia 45` and `ATF 116 Ib 28` returns no citation records. The downstream family logic is therefore unreachable for actual Ia/Ib citations, creating a systematic false-negative channel in the supposedly exhaustive scan.

5. **Blocking — Closed-world completeness is asserted rather than demonstrated.**

   **Location:** `paper.tex:63–65, 121–126, 204–210`; `tables/figure_decidability.tex:9–13`.

   **Quote:** “held completely in our corpus from 1875”; “strictly increasing pages”; “complete sorted list.”

   **Reason:** the package contains no official volume-by-volume parity audit, missing-start analysis, source checksum, or authoritative table-of-contents comparison. The released index also contradicts its clean-set description: BGE 139 II has 54 entries but 38 unique starts; BGE 148 I has 35/20; BGE 148 IV has 71/44. Across the index, 50,467 entries reduce to 35,420 unique family/start values, with duplicates in 367 families. The duplicates do not prove omissions, but they show that the construction supporting the proof has not been deduplicated or independently validated.

6. **Blocking — The claimed voice and source-fidelity review is overwhelmingly unfinished.**

   **Location:** `paper.tex:151–153, 281–294, 451–459`; `data/p2_backscan.json`; `ARXIV_SUBMISSION.md:3–5, 67–70`.

   **Quote:** “We screen all findings”; “the findings were verified”; “single-reader pass.”

   **Reason:** the artifact says `machine findings; voice adjudication pending`. Of 591 findings, 566 voice fields are blank, only one has `source_verified=yes`, and only one has an intended target. Context is missing for 383 findings: 3/210 German, 355/356 French, and 25/25 Italian. Context attachment searches normalized `BGE ...` text, although French/Italian sources commonly contain `ATF`/`DTF`; missing contexts are then automatically assigned `quote_marker=false`. Thus neither voice nor official-source occurrence was screened for most findings, and this failure is highly language-dependent.

7. **Blocking — The human–machine orders-of-magnitude comparison is dimensionally invalid.**

   **Location:** `paper.tex:68–70, 132–136, 336–344, 432–440`; `tables/comparability.tex:14–30`.

   **Quote:** “two to three orders of magnitude below”; “Both asymmetries make the contrast conservative.”

   **Reason:** Dahl and Magesh measure whether an elicited response contains a factual or grounding error. This paper attempts to measure error per citation edge in naturally produced text. Responses contain variable numbers of claims and citations, and the benchmark prompts deliberately elicit legal facts. There is no conversion between these units, so an orders-of-magnitude ratio is meaningless. Moreover, excluding wrong-but-existent human errors lowers the human numerator and widens the apparent gap: it is conservative for total human-error estimation but anti-conservative for claiming human superiority.

8. **Blocking — The “first” claim rests on an explicitly unfinished novelty audit.**

   **Location:** `paper.tex:60–62, 104–110, 175–177`; `notes/novelty-audit.md:61–80`.

   **Quote:** “the first provable answer for a national jurisdiction”; “none measures the human side.”

   **Reason:** the internal audit still lists Bluebook-error studies, citator accuracy, German/Austrian/French literature, and judicial-error literature “to sweep,” and labels its conclusion “provisional — do not write into the paper yet.” No search protocol, databases, queries, screening counts, or inclusion criteria are reported. The title is also broader than the actual object: normalized BGE/ATF/DTF locus nonexistence in corpus-held published decisions since 2024.

9. **Major — Dahl et al. 2024 is materially mischaracterized.**

   **Location:** `paper.tex:57–59, 92–95, 167–170`; `tables/comparability.tex:19–23`.

   **Quote:** “fabricate legal authority in 58–88%”; “fourteen reference-based tasks”; “reference-based contradiction.”

   **Reason:** Dahl has fourteen tasks total: nine reference-based and five reference-free. The 58–88% Figure 6 headline pools the nine reference-based tasks, including court, author, disposition, quotation, overruling year, and doctrinal agreement—not merely fabricated authorities or citations. Known metadata is the reference-based method; emergent contradiction is the reference-free method. The main sampling frame is 5,000 cases per court level, while high-complexity tasks use 100 and the overruling task uses 279. See the [official JLA article](https://academic.oup.com/jla/article/16/1/64/7699227).

   The Magesh figures—202 queries, 17–33%, GPT-4 43%—and broad incorrect-or-misgrounded response definition are represented accurately. Their problem here is comparability, not arithmetic. See the [Magesh paper](https://arxiv.org/html/2405.20362).

10. **Major — The confidence intervals lack a defined inferential population.**

    **Location:** `paper.tex:65–69, 310–315`; `tables/rates.tex:4–15`; `tables/build_tables.py:53–61, 119–136`.

    **Reason:** the study describes an exhaustive census of a fixed 2024–2026 corpus. For that finite population, a correctly counted proportion has no sampling error. Wilson intervals assume Bernoulli sampling; the bootstrap assumes decisions are exchangeable draws from an unstated superpopulation. The paper must specify whether inference concerns future decisions, other time windows, or an underlying decision-generating process. The calculations themselves are correct conditional on the supplied counts.

11. **Major — The bootstrap clusters at the wrong level.**

    **Location:** `paper.tex:312–316, 393–400`; `tables/build_tables.py:119–136`.

    **Quote:** “Findings cluster within decisions (reused boilerplate repeats an error).”

    **Reason:** extraction already deduplicates a locus within each decision, and 591 findings occur in 587 decisions, leaving almost no within-decision clustering. The paper’s own propagation result is dependence across decisions, courts, years, and reused templates. Independently resampling decisions treats propagated boilerplate instances as independent. The multinomial pair-type implementation is mathematically sound for the stated decision bootstrap; the stated cluster is not the dependence-generating unit suggested by the data.

12. **Major — The sensitivity rows are not coherent rate sensitivities, and the strata claims overreach.**

    **Location:** `tables/sensitivity.tex:12–18`; `paper.tex:291–292, 323–330, 400–403`.

    **Reason:** “Each distinct token counted once” changes the numerator to 292 types while retaining the 899,560 edge-row denominator. The coherent distinct-locus view is already 292/16,697 = 1.75%. The quotation row removes 25 detected markers while treating 383 missing contexts as unquoted, so it cannot bound voice effects below 5%.

    Overlapping Wilson intervals do not prove that no language is an outlier. On the displayed counts, the language test gives χ²=3.633, df=2, p=0.163: failure to detect a difference, not evidence of equality. Federal versus cantonal counts give a nominal two-sided pooled p=0.045. The table also omits the `other` class—2 findings and 2,048 denominator rows—while the prose says no court class is an outlier.

13. **Major — Mechanism totals are signature collisions, not causal diagnoses.**

    **Location:** `paper.tex:71–75, 138–146, 348–380`; `tables/mechanisms.tex:4–19`; `scripts/p2_backscan.py:106–169`.

    **Quote:** “most errors carry their own diagnosis.”

    **Reason:** 464/591 findings satisfy multiple mechanism rules, 115 exactly one, and 12 none. Within the 272-item division class, 215 satisfy multiple rules, only 57 have division as their sole signature, and only 86 have a unique exact-start candidate. Volume substitution searches every volume within ±10 plus digit edits; page rules need only land inside a plausibility window; “year for page” checks no relevant decision year. No human validation or null/permutation analysis estimates chance collisions. The totals are reproducible priority labels, not evidence that those mechanisms caused the errors.

14. **Major — Repair-candidate probing proves existence, not intended correction.**

    **Location:** `paper.tex:72–78, 381–389`; `scripts/p2_probe.py:16–19, 95–105`.

    **Quote:** “propose the intended citation”; “external ground truth confirms them.”

    **Reason:** `candidate_unique` is selected because the candidate already appears as an exact start in the internal series index. The probe then confirms that this already-indexed locus exists externally. That cross-validates existence, not authorial intent. Only one finding has a source-verified intended target.

    The showcase ambiguity is visible in the artifacts: BGE 148 I 356 has exact sibling candidates in both IV and V; BGE 48 IV 137 has candidates 148 IV 137, 48 III 137, and 84 IV 137; BGE 147 IIII 249 has exact candidates in III and IV. Context may justify a manual correction, but the deterministic rule does not.

15. **Major — The pre-1955 analysis does not bound hidden errors or support “small.”**

    **Location:** `paper.tex:441–450`; `scripts/p2_backscan.py:280–286, 451–458`.

    **Quote:** “the hidden mass of this typo class is bounded and small.”

    **Reason:** all 1,728 adjudication fields are blank and 856 contexts are missing. After removing duplicate join rows there are 1,642 unique pairs. More importantly, 278 become plausible after restoring a leading digit; only 21 are exact starts, but the paper’s own validity rule accepts interior pin-cites. Thus 278—not 21—is the unadjudicated risk pool, amounting to 47% of the observed 591 findings. A single +100 screen cannot bound other wrong-but-existent division, volume, page, or semantic errors.

16. **Major — “Zero scan false positives” overstates the resolver experiment.**

    **Location:** `paper.tex:262–276, 456–459`; `scripts/p2_probe.py:46–67`.

    **Reason:** the probe shows that 254 normalized loci returned 404, zero resolved, and 38 were outside coverage. It does not verify the official citing text, voice, extraction boundary, source date, or corpus fidelity, nor establish exhaustive resolver coverage at every tested volume and interior page. It is useful independent evidence of nonresolution for 254 targets—not a zero-false-positive estimate for 591 source-level findings.

17. **Major — The release reformats stored totals but does not independently reproduce the scan.**

    **Location:** `paper.tex:85–86, 467–475`; `tables/build_tables.py:32–35, 82–116`; `data/MANIFEST.json`; `ARXIV_SUBMISSION.md:51–59`.

    **Quote:** “Every scan-derived number regenerates from released artifacts via a single script.”

    **Reason:** the generator reads totals and cluster pairs already stored in `p2_backscan.json`; it does not recompute extraction, denominator membership, resolution, or classification from frozen inputs. The exact `decisions.db` and `reference_graph.db` have no released content hashes, and resolved denominator rows are absent. The documented arXiv tar excludes both `data/` and the generator. Deterministic formatting is useful but is not independent reproducibility of the empirical result.

18. **Major — The human-error mechanism narrative exceeds the evidence.**

    **Location:** `paper.tex:141–144, 385–389`; abstract `paper.tex:79–80`.

    **Quote:** human errors are “overwhelmingly local damage to a true citation”; “Errors concentrate in reused boilerplate.”

    **Reason:** only one intended correction is source-verified and 78.5% of findings match multiple mechanisms. The propagation counts show that some erroneous tokens repeat; without a denominator of boilerplate versus novel citations, they do not show errors “concentrate” in boilerplate. Dahl and Magesh also include wrong metadata, holdings, quotations, and real-but-misgrounded sources, so the claimed human-local-damage versus machine-invention contrast misstates the comparison literature.

19. **Minor — The future/open-volume rule is described inconsistently.**

    **Location:** `paper.tex:223–230`; `quality/checks/citation_anomalies.py:180–183`.

    **Quote:** “the newest two volumes are still filling and are exempt”; “cannot be rescued by any future publication.”

    **Reason:** code exempts `max-1`, `max`, and `max+1` from division/page classification, not merely the two newest existing volumes. Volumes above `max+1` are publication-time nonexistent but can eventually be published. The paper should claim nonexistence at the citing decision’s date, not timeless nonexistence.

20. **Minor — The time population and partial 2026 stratum need precise labelling.**

    **Location:** `paper.tex:65–67, 128–131`; `tables/sensitivity.tex:19–21`; `scripts/p2_backscan.py:191–219`.

    **Reason:** decision metadata has only a lower date bound, whereas citation rows use an upper bound of scan date plus 366 days to admit future-dated portal noise. These are not identical populations. The 2026 rate is only through the August scan and may reflect publication/ingestion lag; it should be marked partial-year.

21. **Minor — The remediation numbers are credible but not reader-verifiable evidence.**

    **Location:** `paper.tex:409–423, 474–475`.

    **Quote:** “the correspondence facts … are dated and sourced in the text.”

    **Reason:** read-only inspection of the mailbox supports seven responding administrations, one implemented correction, one correction underway, one promised verification/correction, and four acknowledgments/routings. The factual summary appears sound. But the paper supplies no anonymized correspondence appendix, hashes, or public source; private mailbox evidence is not “sourced in the text.” It should be labelled author-reported unless a privacy-preserving record can be released.

The strongest defensible result currently is much narrower: the graph-based classifier emitted 292 distinct normalized reporter loci, 254 of which returned 404 within the public resolver’s stated coverage. Even that result still needs source-occurrence verification, complete voice adjudication, a corrected extraction grammar, and a genuinely provable endpoint rule before supporting the title or abstract.

Codex session ID: 019ffb7d-7600-75a3-ab09-c0211f25b5b4
Resume in Codex: codex resume 019ffb7d-7600-75a3-ab09-c0211f25b5b4
