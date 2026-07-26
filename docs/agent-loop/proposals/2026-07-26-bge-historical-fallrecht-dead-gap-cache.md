# Investigation: `bge_historical` 2 h timeout — `fallrecht.ch` is dead, and the gap cache never runs in production

**Date:** 2026-07-26
**Origin:** `bge_historical` FAILed in the 2026-07-26 03:02 UTC scrape (timed out at the 7200 s cap) and tripped `STALE ... last_scraped 2026-04-21 (96d ago)`. First FAIL for this court since 2026-04-15.
**Status:** Root-caused. **A bounded fix is implemented** in `scrapers/bge_historical.py` (uncommitted; 5 tests in `tests/test_bge_historical_dead_host.py`): after 3 consecutive connection failures the scraper stops probing that host for the rest of the run. Turns the 2 h burn into ~13 min and clears the FAIL.

**I deliberately did NOT port the gap cache (P1 below).** On re-examination it is the wrong tool here and carries a real hazard — see "Why not the gap cache". P2 (the Wayback recovery of 141 decisions) is still open and needs approval.

Re-probed 2026-07-26 ~17:00 UTC, five hours after the first failure: `www.fallrecht.ch` and the apex are still dead from both networks. Not transient.

## Summary

Two independent problems, one visible symptom.

1. **The PDF source host `www.fallrecht.ch` went offline on 2026-07-26.** Not an IP block — it is unreachable from Hetzner *and* from the residential MacBook IP, on both :80 and :443, apex and `www`.
2. **The gap cache has never worked in production.** `CACHE_NONE_AS_GAP` is honoured only by `base_scraper.scrape()`, which production does not call. So 161 permanently-unfetchable stubs are re-probed every single night.

Problem 2 is the reason problem 1 costs two hours a night instead of being a no-op. Problem 2 also silently affects `emark` and `hudoc`.

**Nothing is lost from the corpus.** `bge_historical` holds a stable 14,578 decisions. The 161 affected are the residue, and **141 of them (88 %) are recoverable from the Wayback Machine**.

## Evidence

### The source is genuinely gone

| Probe | Result |
|---|---|
| `https://www.fallrecht.ch/` from Hetzner | TCP connect timeout (25 s) |
| `https://www.fallrecht.ch/` from MacBook (residential IP) | TCP connect timeout (30 s) |
| `https://fallrecht.ch/` (apex, different IPs) from MacBook | TCP connect timeout (25 s) |
| `http://www.fallrecht.ch/` (port 80) | TCP connect timeout (20 s) |
| via the MacBook SOCKS tunnel from Hetzner | `SOCKS5 connection ... (5)` — connection refused upstream |
| DNS `www.fallrecht.ch` | → `www.cornbags.ch` → `85.195.237.235` |
| DNS `fallrecht.ch` (apex) | `5.22.145.121`, `5.22.145.16` — also dead |
| `whois fallrecht.ch` | registration still `ACTIVE` |

The `www` host now aliases an unrelated domain (`cornbags.ch`) on a shared-hosting IP that answers nothing. The tunnel is **not** a workaround here; this is not the ne.ch / jura.ch situation.

### The primary source is healthy

`servat.unibe.ch` (Uni Bern, DFR) returns `200` in **64 ms**. The nightly index crawl over `dfr_bge00..07.html` completes normally. Only the PDF fallback is broken.

### The 161 are PDF-only

The DFR index links a subset of decisions as `fallrecht.ch/c*.pdf` rather than `servat.unibe.ch/dfr/c*.html`. Verified: `c1003457.html`, `c1036188.html`, `c1036191.html` all return **404** on servat. They exist only as PDFs on the dead host.

Volume spread of the 161: vol 3 (×1), 35 (×11), 36 (×38), 37 (×40), 38 (×37), 39 (×31), 42 (×1), 66 (×1), 79 (×1). So 157 of 161 sit in **BGE 35–39 (1909–1913)**.

### Why it costs 7200 s

`BGEHistoricalScraper.TIMEOUT = 60` (sized for large PDFs) and `base_scraper` installs `Retry(total=3, backoff_factor=2)`. A blackholed host therefore costs `4 × 60 s` connect + backoff ≈ **4 minutes per stub**. The systemd 7200 s cap is reached after ~28 stubs. `MAX_NONE_RETURNS = 2000` is far above 161, so the loop never short-circuits.

The change is dated precisely: the **2026-07-25 01:05 run finished in 4.2 min** with `NoneReturns: 161` (fast 404s). The **first `ConnectTimeoutError` in the log is 2026-07-26 01:02:08**.

### Why it repeats nightly — the actual defect

`scrapers/bge_historical.py:90` sets `CACHE_NONE_AS_GAP = True`, and `base_scraper.py:413-421` honours it:

```python
if getattr(self, "CACHE_NONE_AS_GAP", False):
    did = stub.get("decision_id") or make_decision_id(...)
    if did and "_" in did:
        self.state.mark_gap(did)
```

But production runs `python run_scraper.py <court>`, and **that loop has no equivalent branch**. `run_scraper.py:655-675` counts the `None`, logs it, checks `MAX_NONE_RETURNS`, and moves on. `state.mark_gap()` is never called.

Proof: **zero `.gaps.jsonl` files exist** among the 125 files in the production `state/` directory. The mechanism described in `CLAUDE.md` ("gap caching") has never fired in production.

Three scrapers opt in and all three are affected:

- `scrapers/bge_historical.py:90`
- `scrapers/emark.py:78`
- `scrapers/hudoc.py:64`, `:326`

### Recovery is available

Wayback holds **14,410 archived `fallrecht.ch/c*.pdf` keys** (status 200) — effectively a mirror of the whole historical PDF set, against a corpus of 14,578. Intersecting the 161 unfetchable dockets against that index:

- **141 recoverable (88 %)**
- 20 not archived: `36_I_74`, `36_I_89`, `37_I_1`, `37_I_4`, `37_I_14`, `37_I_18`, `37_I_22`, `37_I_37`, `38_I_1`, `38_I_15`, `38_I_26`, `38_I_31`, and 8 more

Same play as the BL Kantonsgericht Wayback recovery (903 decisions).

## Proposed changes (all gated — none applied)

**P1 — per-host circuit breaker. DONE.** `UNREACHABLE_HOST_STREAK = 3` on `BGEHistoricalScraper`: three consecutive connection failures to a host and the remaining stubs on it are skipped without a request. A 404 resets the counter (the host answered, so it is alive — the 161 stubs 404'd for months without this firing). A success resets it too, so an intermittent host is not abandoned.

Cost falls from 7200 s (capped, FAILED, nothing fetched) to ~13 min: 3 dead probes at ~4.2 min, then the remaining 158 return instantly. Run completes, `success=True`, no FAIL, no STALE.

### Why not the gap cache

The original P1 was to port the `CACHE_NONE_AS_GAP` branch from `base_scraper.py:413-421` into `run_scraper.py:655`, since production never runs `base_scraper.scrape()` and no `.gaps.jsonl` has ever been written (0 files among 125 in prod `state/`). That finding stands, but it is **not** the right fix for this failure:

- **A gap means "the source says this does not exist." A connect timeout means "I could not reach the source."** `fetch_decision` returns `None` for both, so the distinction is already lost at that boundary. Porting the branch as-is would let a network outage during a scrape write hundreds of decisions into the gap cache and suppress them for `GAP_TTL_DAYS = 7`. That is a silent data-loss mode, and it would apply to `emark` and `hudoc` too.
- The circuit breaker writes **nothing** to `state/`. A wrong call costs one run; the next run re-probes from scratch. That asymmetry is why it is the safe choice.
- It also would not have solved the burn on its own: with a 7-day TTL the 161 get re-probed weekly, and 161 × 4.2 min is 11 h — over the cap again, one night in seven.

Porting the gap cache is still worth doing for its actual purpose (404-driven pruning), but it needs the None/unreachable distinction plumbed through first. Separate change, separate approval.

**P2 — Wayback backfill for the 141.** A one-shot script in the existing `backfill_*.py` idiom, pulling `web.archive.org/web/<ts>id_/https://www.fallrecht.ch/c*.pdf` and feeding the existing `_extract_pdf_text` path. Closes the historical series to ~99.9 %.

**P3 — retire the live PDF fallback.** Once P2 lands, the `fallrecht.ch` branch in `discover_new` only produces dead stubs. Either drop `is_pdf` stubs at discovery, or repoint them at Wayback permanently.

## Recommendation

P1 first (small, general, stops the nightly burn and the false FAIL), with option (c) as the companion. P2 is the valuable one — it recovers 141 decisions from 1909–1913 that are otherwise gone from the live web. P3 is cleanup.

Do **not** add `bge_historical` to `KNOWN_DEAD_SOURCES`: unlike `be_steuerrekurs`, its primary source (servat.unibe.ch) is alive and the court is 99 % complete.
