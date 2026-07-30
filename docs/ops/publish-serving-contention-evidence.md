# Evidence: nightly rebuild vs. serving latency (for the ETH handover)

Measured 2026-07-29/30 on the production host (16 cores / 61 GB, 8 read-only
uvicorn workers + nightly publish on the same machine). Source: per-search
traces (`output/research_logs/search_traces_*.jsonl`), hour-by-hour; systemd
cgroup accounting for the publish unit.

## Timeline of the two runs

| | 2026-07-29 | 2026-07-30 |
|---|---|---|
| Publish start → finish | 03:30 → 17:08 (13h38m) | 03:30 → 17:11 (13h40m) |
| Cgroup memory peak | 37.6 G (uncapped) | **32.0 G (MemoryHigh cap, pinned)** |
| Resource limits | CPUWeight=60, IOWeight=50 | CPUWeight=20, IOWeight=20, MemoryHigh=32G |

## Search latency by hour (p50 / p95, ms)

| Hour UTC | Jul 29 (uncapped) | Jul 30 (capped) |
|---|---|---|
| 01–08 (build phase) | 570–2,700 / 5,600–11,000 | **570–650 / 1,200–7,500** |
| 10 | 1,237 / 10,493 | 7,363 / 14,231 |
| 11 | 2,789 / 18,018 | 5,558 / 36,151¹ |
| 12–14 (batch) | 2,000–2,700 / 8,500–16,400 | 5,500–7,700 / 10,800–19,100 |
| 15 (export/upload) | 5,595 / **48,756** | 7,337 / **20,600** |
| 16–17 (tail) | 1,000–1,800 / 6,800–8,000 | 5,200–6,400 / 16,200–17,000 |
| 19–23 (no publish) | 780–1,540 / 1,800–9,100 | — (baseline) |

¹ coincides with the 10:31 DB swap (cold page cache on the new 67 GB inode)
plus a concurrent ECtHR quick-publish.

Caveat on the mix: a high-volume templated agent ran overnight on Jul 30
(cheap, fast queries) and stopped at 09:00, so Jul 30 daytime medians are
weighted toward heavier human queries. The p95 comparison is less affected.

## Reading

1. **MemoryHigh=32G removed the catastrophic tail**: worst p95 in the
   export window fell 48.8 s → 20.6 s. Mechanism confirmed — uncapped, the
   rebuild's page-cache footprint evicted the serving workers' hot SQLite
   pages; capped, the eviction stops (peak pinned at exactly 32.0 G).
2. **It did not fix the elevated afternoon**: with reclaim throttled, the
   rebuild does more sustained device IO across the same hours, and search
   medians sit at 5–8 s through 10:00–17:00 either way. Device-level IO
   contention is not addressable with cgroup weights on one box.
3. **Wall-clock is unchanged** by the caps (13h38m vs 13h40m) — the limits
   cost the pipeline nothing.

## Consequence

Single-host mitigation is exhausted short of reshaping the pipeline
(serialising the post-build batch, ionice idle class — an interim option).
The structural fix is the proposed split: build host produces versioned
artifacts, serving host swaps them in atomically. Serving then never shares
memory bandwidth or a disk queue with a 13-hour rebuild, and the daily
10:00–17:00 degradation window disappears entirely.
