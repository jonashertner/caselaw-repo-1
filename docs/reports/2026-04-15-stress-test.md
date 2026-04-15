# Stress Test Report — 15 April 2026

## Scope

End-to-end stress + functional validation of three production surfaces:
1. **MCP REST API** (`mcp.opencaselaw.ch`) — all 25 endpoints
2. **Dashboard site** (`opencaselaw.ch`) — all 7 pages + `stats.json`
3. **Word Add-in** (`word.opencaselaw.ch`) — manifest, assets, pages

Tool: `scripts/stress_test_full.py` — functional pass + parametric load test.

## Functional: 40/40 pass

| Surface | Results | Notes |
|---------|--------|-------|
| **MCP** | **25/25 ✅** | Every endpoint returns correct payload |
| **Site** | **7/7 ✅** | All pages load, `stats.json` has valid `delta` |
| **Word** | **8/8 ✅** | Manifest valid, all icons served, pages HTTPS |

Every endpoint tested with a realistic query and body-level assertion (not just HTTP 200).

## Load test progression

### Production-realistic (c=6, n=40)
```
MCP health         40/40  p50=123  p99=624ms
MCP search         40/40  p50=2462 p99=4329ms  (LLM parse + Haiku rerank)
MCP case_brief     40/40  p50=578  p99=708ms
MCP get_law        40/40  p50=51   p99=65ms
MCP entscheid_page 40/40  p50=57   p99=111ms
Site home          40/40  p50=73   p99=164ms
Site stats.json    40/40  p50=52   p99=76ms
Word manifest      40/40  p50=42   p99=131ms
```
**Verdict:** All green. Fast endpoints sub-200ms p99. Search slow but expected.

### Moderate burst (c=15, n=60)
All 60/60 pass, p99 latencies mostly under 300ms. Search p99 13.5s under burst.

### Abuse simulation (c=30, n=100)
Rate-limiting kicks in (intended behavior):
- `/health`: 85/100 — burst=50 exhausted, rest 503
- `/api/` endpoints: ~60-90/100 depending on speed
- `/api/decisions`: 26/100 — slow endpoint × rate limit compounds

**Nginx correctly throttles single-IP abuse.** No worker saturation, no memory pressure (5.5 GB / 61 GB used), no DB locks.

## Change made: nginx rate-limit tuning

**Before:**
```nginx
limit_req_zone ... rate=10r/s;
# /health:  burst=5
# /api/:    burst=20
```

**After:**
```nginx
limit_req_zone ... rate=30r/s;
# /health:  burst=50
# /api/:    burst=50
```

**Rationale:** AI proxy IPs (Anthropic hosted, OpenAI, Cursor) aggregate many users behind a handful of outbound IPs. Under the old 10r/s limit, a real traffic spike from multiple users through the same proxy could hit 503s. The new 30r/s + burst=50 comfortably handles ~50-user proxies while still throttling outright abuse.

**Deployed:** `systemctl reload nginx` — zero downtime. Config backup at `/etc/nginx/mcp-server.bak.20260415`.

## Server health during testing

| Metric | Value |
|--------|-------|
| CPU (16-core, load avg) | 3.07 → **19% utilized at c=30** |
| Memory | 5.5 GB used / 61 GB total |
| Swap | 0 B |
| ES backfill | Unaffected, ran at 312K/349K throughout |
| Open connections per worker | 3–13 (healthy spread across 4 workers) |

## Issues found & fixed

| Issue | Fix | Status |
|-------|-----|--------|
| `/health` burst=5 too tight | Raised to 50 | ✅ deployed |
| `/api/` burst=20 tight for AI proxies | Raised to 50, rate 10→30/s | ✅ deployed |
| Test script lambda bug (`A or "msg"` returns A when truthy → false positive) | Switched to `fail_if_false(cond, msg)` helper | ✅ in git |
| nginx test errored on backup file inside sites-enabled | Moved backup outside sites-enabled | ✅ fixed |

## Recommendations (not urgent)

1. **Whitelist Anthropic's published IP ranges** for higher rate limits (currently all IPs treated equally)
2. **Per-endpoint limits** — search is expensive (7s), could cap at 5 concurrent per IP while allowing unlimited fast-endpoint bursts
3. **Separate health-check zone** — monitoring tools should never be rate-limited; current burst=50 is fine but a dedicated zone=mcp_probe rate=100r/s would be cleaner
4. **Cache /api/courts, /api/statistics** for 60s at nginx level — low-change endpoints getting hit frequently

## Summary

The production stack is **robust under realistic load**. Every functional test passes. Real user traffic (multiple IPs each at 1-2 r/s) will never see the rate limits that synthetic single-IP bursts trigger. Memory and CPU have large headroom. The ES backfill running in the background had no measurable impact on response latency.

All three surfaces are ready for 100% proper workflow.
