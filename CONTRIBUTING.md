# Contributing to OpenCaseLaw

OpenCaseLaw is an open Swiss legal corpus and citation graph. Data is CC0,
code is MIT. Contributions of every size are welcome, and the most valuable
ones are usually not code.

Recognition for past contributors is in [CONTRIBUTORS.md](CONTRIBUTORS.md).

## The five things that help most

**1. Report a data-quality defect.** A decision with mangled text, a wrong
date, a citation that resolves to the wrong case, a missing Erwägung. These
are the reports that improve the corpus for everyone, and every one that has
come in so far has been reproduced, fixed, and converted into a permanent
regression test. Use the *Data quality* issue template.

**2. Tell us a court is missing or broken.** Switzerland has 26 cantonal
court systems and their portals change without notice. If a court you rely on
is absent, incomplete, or has stopped updating, say so — that is a concrete,
scoped piece of work. Use the *Missing court or source* template.

**3. Improve the documentation.** Setup guides for clients we do not use,
Windows instructions, a walkthrough in French or Italian, a worked research
example. The project is maintained by one person and the documentation
reflects one person's blind spots.

**4. Validate something.** Sample decisions and check whether the extracted
citations are right. Check whether a statute link means what the table name
implies. Adversarial checking of published claims is a real contribution and
is credited as such.

**5. Build on it.** A notebook, an integration, a benchmark, a paper. Tell us
what was hard — friction you hit is a bug report about the interface.

## Before you open a pull request

- **Tests must pass**: `make test` (offline, ~70 s). No live network in tests;
  use golden fixtures for scrapers and fixture databases for graph tests.
- **New behaviour needs a test.** A regression test that pins the defect is
  worth more than the fix.
- **Keep the pipeline gate in mind.** Changes to `publish.py`, database
  schemas, `base_scraper.py`, or anything under `state/` affect a nightly
  rebuild with hours of blast radius. Open an issue describing the change
  before writing it, so we can plan validation together.
- **Citation integrity is non-negotiable.** Any tool returning a citation
  string must source it from stored fields, never construct it. Quotations
  must come verbatim from the stored text. See the R1–R3 rules in the MCP
  server's system prompt.
- **Numbers come from one place.** Corpus and tool counts live in
  `docs/canonical_numbers.md` with a documented verification method. Please
  do not introduce a figure that contradicts it.

## Setting up

```bash
git clone https://github.com/jonashertner/caselaw-repo-1
cd caselaw-repo-1
python3 -m venv .venv && .venv/bin/pip install -e .
make test
```

You do not need the full 65 GB corpus to contribute. Most work — docs,
scraper fixtures, tests, tooling — needs only the repository. For work that
needs data, the hosted API at `https://mcp.opencaselaw.ch` and the CC0
Parquet export on Hugging Face are both open and need no key.

## Legal and licensing

Court decisions in Switzerland are official acts and are excluded from
copyright (URG/CopA Art. 5). Contributions to code are accepted under MIT;
contributions to data packaging under CC0. Please do not add material whose
licence you have not checked — in particular, commentary and scholarship
carry their own upstream terms, and ECtHR-origin texts are © ECHR-CEDH and
excluded from the CC0 export.

If you find a decision that should not be public, or that contains personal
data the court did not intend to publish, please use the takedown route in
[docs/governance-and-removal-policy.md](docs/governance-and-removal-policy.md)
rather than a public issue.

## Getting help

Open an issue with as much detail as you can, or write to
`jh@jonashertner.com`. Questions are welcome; there is no expectation that
you understand Swiss law, the scraper architecture, or MCP before asking.
