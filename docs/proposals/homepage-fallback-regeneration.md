# Proposal: regenerate the homepage's pre-hydration numbers from `stats.json`

**Status:** proposed, not implemented. Touches `publish.py` → **pipeline-gated**,
needs explicit approval and a run against a copy, not the live volume.
**Raised:** 2026-08-22, during a full test-suite and public-surface review.

## The defect

`opencaselaw.ch` renders its headline figures client-side from `stats.json`,
which is correct and fresh — the 2026-08-22 file was generated 09:34 UTC that
morning with `total: 1054206`, matching `/health` exactly.

But `docs/index.html` also carries **hardcoded fallback values** in the markup,
shown before hydration. Nothing regenerates them. They were written once and
have drifted ever since:

| Element | Hardcoded | Live (2026-08-22) | Drift |
|---|---:|---:|---|
| `#bignum`, `#f-decisions` | 991,298 | 1,054,206 | −62,908 (−6.0 %) |
| `#cov-courts` | 109 | 118 | −9 |
| `#f-laws` | 21,108 | 21,136 | −28 |
| citations (prose, `>8.9M`) | 8.9M | 9.84M | −0.94M |

## Why it matters more than it looks

Anyone who does not execute JavaScript sees only the stale numbers. That
includes a large share of crawlers and LLM fetchers — and this project's own
traffic analysis puts crawlers at ~97 % of requests. A corpus that advertises
itself as *"the complete record of Swiss case law — rebuilt every day"* is
serving two-month-old counts to precisely the machine audience it is built for.

It is also a silent failure: the page looks perfect in any browser, so the
defect is invisible unless someone fetches it without JS. That is how it went
unnoticed — the review that found it did exactly that by accident.

Note the `og:description` meta tag already avoids the problem by using an
ageless form (`1,050,000+`). The in-page fallbacks did not follow that pattern.

## Proposed fix

`stats.json` is already generated and already committed by the nightly. The
homepage fallbacks should be written from the same source in the same run, so
drift becomes structurally impossible rather than a thing to remember.

**Insertion point.** `publish.py` Step 5 runs `generate_stats.py` →
`docs/stats.json`; Step 5e recomputes the `interesting_stats` block after the
reference graph is rebuilt. The rewrite must run **after Step 5e**, when
`stats.json` is final. Step 6 then commits the docs set:

```python
paths = ["docs/stats.json", "docs/feed.xml", "docs/feeds",
         "docs/quality.json", "docs/quality.html", ...]
```

so the change is: a small step after 5e, plus `"docs/index.html"` appended to
that list.

**Shape.** A standalone script — `scripts/sync_homepage_fallbacks.py` — that
reads `docs/stats.json`, rewrites the text content of the known element ids in
`docs/index.html`, and is idempotent. Keeping it out of `publish.py` means it
can be run and tested by hand, and the pipeline change stays a one-line call.

Element ids to write, with their source fields:

| id | source in `stats.json` |
|---|---|
| `bignum`, `f-decisions` | `total` |
| `cov-courts` | `court_count` |
| `f-laws` | `corpus.federal_laws + corpus.cantonal_laws` |
| citation figure | `corpus.citation_edges` |

Match the existing display convention (typographic apostrophe as thousands
separator, e.g. `1'054'206`) so the pre- and post-hydration renders agree.

**Failure mode must be non-fatal.** A malformed `index.html` or a missing
`stats.json` key must log and continue, never abort the nightly. The homepage
being one day stale is a cosmetic problem; a failed publish is not.

## Alternative, if the pipeline change is unwanted

Replace the four literals with values that cannot go stale — `1,050,000+`,
`9.8M+`, and the exact slow-moving counts (118 courts, 26 cantons) — mirroring
what `og:description` already does. This is a one-time edit to `docs/index.html`
with no pipeline involvement. It is strictly worse than regeneration (the
numbers stop being precise, and "1,050,000+" will itself look conservative in a
year) but it removes the embarrassment permanently at near-zero risk.

## Verification

After implementing, the check is a single command that needs no browser:

```bash
curl -s https://opencaselaw.ch/ \
  | grep -oE 'id="(bignum|f-decisions|cov-courts|f-laws)"[^>]*>[^<]*'
```

Every figure it prints should match `docs/stats.json`. Worth adding as an
assertion to `quality/smoke.py` once the regeneration exists — until then a
probe would simply be permanently red, which
`tests/test_smoke_probe_coverage.py` explains is worse than no probe at all.
