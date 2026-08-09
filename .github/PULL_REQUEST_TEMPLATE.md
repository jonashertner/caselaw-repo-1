## What this changes

<!-- One or two sentences. If it fixes an issue, link it. -->

## Why

<!-- The defect or need. For data fixes: how you verified the correct value. -->

## Checklist

- [ ] `make test` passes locally (offline, no live network in tests)
- [ ] New behaviour has a test; a fixed defect has a regression test
- [ ] No figure introduced that contradicts `docs/canonical_numbers.md`
- [ ] Citation strings come from stored fields, never constructed (R1–R3)
- [ ] If this touches `publish.py`, DB schemas, `base_scraper.py` or `state/`:
      an issue was opened first to plan validation (nightly-rebuild blast radius)
