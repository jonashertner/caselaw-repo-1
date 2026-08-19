# arXiv submission metadata — Paper 2 (When Courts Miscite)

**Status**: DRAFT FINALIZED 2026-08-13 pending (i) author sign-off on
the remediation facts and the two voice adjudications, (ii) adversarial
review reconcile, (iii) explicit user approval to submit. DO NOT SUBMIT
without that approval. Standing constraint: nothing external without
explicit approval.

## Title

```
When Courts Miscite: A Provable Baseline for Citation Errors in Swiss Judicial Decisions
```

## Authors

```
Jonas Hertner
```

Single author. Affiliation: OpenCaseLaw. Contact: jh@jonashertner.com.
(CRediT block for potential co-authors staged in paper.tex comments;
add only on confirmed acceptance.)

## Abstract (plain-text projection; regenerate wording from paper.tex on any macro change)

The paper.tex abstract is the source of truth; keep this projection in
sync after every `make paper2-tables && make paper2`. Character budget:
arXiv metadata limit 1,920 plain chars.

## Comments line

```
Companion to the OpenCaseLaw resource paper. All numbers regenerate from released artifacts (scan, findings with contexts and mechanism labels, per-decision cluster structure, complete BGE series index, pre-1955 pool, resolver probe results) via a single generator script. Live infrastructure at https://mcp.opencaselaw.ch and https://opencaselaw.ch; code MIT, data CC0.
```

## Primary category

```
cs.CY  (Computers and Society)
```

Cross-list: cs.CL, cs.DL.

## License

arXiv nonexclusive license (default). Data CC0; code MIT.

## Packaging

arXiv build tarball (what arXiv compiles):

```
make paper2-tables && make paper2
cd docs/paper/p2-citations && tar czf p2-arxiv.tar.gz \
    paper.tex paper.bbl bib/refs.bib tables/*.tex
```

Release package (published WITH the submission — the paper's §6 and
the related-work footnote promise these files, so submitting without
releasing them makes the paper false on day one):

- `data/` (findings, backscan summary, series index, pool, probe,
  db_hashes, MANIFEST)
- `tables/build_tables.py`, `scripts/p2_backscan.py`,
  `scripts/p2_probe.py`, `scripts/p2_count_iab.py`
- `notes/novelty-audit.md`, `REVIEW_GPT56SOL.md`

Release path: commit to the public repo (per approval) so the arXiv
comments line's GitHub link resolves.

## Pre-submission checklist

- [ ] Real determinism check: `cp -r tables /tmp/t1 && make
      paper2-tables && diff -r /tmp/t1 tables` clean
      (verified 2026-08-14)
- [ ] `make paper2` builds; 0 undefined references
- [ ] `grep -c pending paper.tex` → 0
- [ ] Remediation facts confirmed by author against the mail threads
      (refs 1449..1611, 1649)
- [ ] Voice labels (3 party-quote, 46 court) confirmed by author
- [x] Adversarial review reconciled (`REVIEW_GPT56SOL.md`, 21/21)
- [ ] data/MANIFEST.json hashes final at commit
- [ ] Repo commit + data release published together with submission
- [ ] Explicit user approval to submit
