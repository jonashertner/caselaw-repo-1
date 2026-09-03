# OpenCaseLaw VPS — Technical Handover

## 1. Server

| | |
|---|---|
| Provider | Hetzner Cloud |
| Plan | CCX43 (dedicated CPU) |
| CPU | 16 dedicated cores (AMD) |
| RAM | 64 GB |
| Root disk | 150 GB (`/dev/sda1`), 45% used |
| Data volume | 250 GB (`/dev/sdb`), mounted at `/mnt/HC_Volume_104655575`, 40% used |
| OS | Ubuntu 24.04.3 LTS (Noble Numbat) |
| IP | 46.225.212.40 |
| Hostname | caselaw-mcp |
| Monthly cost | ~EUR 116 |

### Access

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40
```

The SSH key is a deploy key. The repo uses SSH git remote.

---

## 2. DNS

| Domain | Type | Target | Purpose |
|--------|------|--------|---------|
| `mcp.opencaselaw.ch` | A | 46.225.212.40 | MCP server + REST API + SEO pages |
| `opencaselaw.ch` | CNAME | GitHub Pages | Dashboard (static site) |

DNS managed at GoDaddy, TTL 600.

---

## 3. TLS

- **Provider**: Let's Encrypt (certbot)
- **Domain**: `mcp.opencaselaw.ch`
- **Current expiry**: 2026-05-23 (auto-renews)
- **Cert path**: `/etc/letsencrypt/live/mcp.opencaselaw.ch/`

Certbot timer handles renewal automatically. Verify with:
```bash
certbot certificates
```

---

## 4. Application Architecture

```
                    Internet
                       |
                    nginx (:443 TLS, :80 redirect)
                       |
              ┌────────┼────────┐
              │        │        │
         /health   /entscheid  / (root)
         /api/*    /sitemap*   /sse
         /robots   /messages
              │        │        │
              └────────┼────────┘
                       |
            ┌──────────┼──────────┐──────────┐
            │          │          │          │
        uvicorn    uvicorn    uvicorn    uvicorn
        :8770 … :8777 (8 workers)
            │          │          │          │
            └──────────┼──────────┘──────────┘
                       |
                  mcp_server.py
                       |
            ┌──────────┼──────────┐
            │          │          │
      decisions.db  reference_  statutes.db
      (FTS5, 61GB)  graph.db    (56MB)
                    (3.7GB)
```

### nginx

- Config: `/etc/nginx/sites-enabled/mcp-server`
- Proxies to the 8 upstream workers (8770-8777) via `ip_hash`
- SSE: `proxy_buffering off`, long timeouts (86400s)
- CORS headers on all responses
- Rate limiting: 10 req/s API, 1 req/s SSE
- Serves `/entscheid/*` (SEO pages), `/sitemap*`, `/robots.txt`
- Port 8765 also served for backward compatibility

### MCP Server (mcp_server.py)

- **Framework**: Starlette + FastAPI (REST sub-app)
- **MCP library**: `mcp` 1.26.0
- **Transports**: SSE (Claude) + Streamable HTTP (ChatGPT) on same endpoint
- **Tools**: 19 in remote mode, 21 in local mode
- **All tools have `readOnlyHint` annotations** (required for ChatGPT Developer Mode)
- **Search features**: FTS5 BM25, RRF fusion, LLM query parsing (Haiku), LLM reranking (Haiku), citation graph signals, compound decomposition

### Systemd Services

```
mcp-server@{8770..8777}.service
```

- **Unit file**: `/etc/systemd/system/mcp-server@.service`
- **User**: `mcp` (non-root)
- **Working dir**: `/opt/caselaw/repo`
- **Env file**: `/opt/caselaw/repo/.env.mcp`
- **Security**: `ProtectSystem=strict`, `PrivateTmp=true`, `NoNewPrivileges=true`
- **Auto-restart**: `Restart=always`, `RestartSec=5`

**Commands:**
```bash
# Status
systemctl status mcp-server@8770

# Rolling restart (one at a time, health-gated, no downtime): gate on the
# build window FIRST ("Restarting" below), then run the script. It discovers
# the pool from systemd; never restart workers with an ad-hoc loop.
bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh

# Logs
journalctl -u mcp-server@8770 --since "1 hour ago" --no-pager

# Health check
curl -s https://mcp.opencaselaw.ch/health
```

### Offline HuggingFace model cache (/opt/caselaw/hf_cache)

**Why it exists.** The workers run as `User=mcp` (uid 999, no home directory: `/home/mcp` does not exist) under `ProtectSystem=strict`. `huggingface_hub` resolves its cache to `~/.cache/huggingface`, i.e. `/home/mcp/.cache`, which it cannot create; the only model snapshots on the host lived in `/root/.cache/huggingface/hub` (`/root` is 0700). Every `SentenceTransformer(...)` call in a worker therefore raised `PermissionError`, and `_get_semantic_model()` (mcp_server.py, ~line 12938) sets `_SEMANTIC_MODEL_TRIED` on the first failure, pinning the feature off for the life of the process. `PINPOINT_SEMANTIC_ENABLED=true` had been in `.env.mcp` since 2026-08-23 and was never live: the journal showed 0 `loaded pinpoint semantic model` lines and 152 `pinpoint semantic model load failed: PermissionError at /home/mcp ...` lines (one per worker per boot). Fixed 2026-09-03 ~11:52 UTC by hand (copy, sandboxed test, env append); the same steps are now scripted in `scripts/hf_cache_stage.sh` for a rebuilt host (idempotent: an already-staged cache is reused, `DO_ENV=0` re-runs only the sandbox test). The env change is inert until the next worker restart of any kind: publish.py's post-swap rolling recycle (~10:07 UTC daily, `publish.py:_recycle_mcp_workers`) or a manual gated rolling restart.

**What is in it.**

| | |
|---|---|
| Path | `/opt/caselaw/hf_cache/hub/models--sentence-transformers--paraphrase-multilingual-MiniLM-L12-v2` |
| Size | 458 MB (`cp -a` copy; relative `snapshots/ -> blobs/` symlinks intact) |
| `refs/main` | `e8f8c211226b894fcb81acc59f3b34ba3efd5f42` |
| Owner | `mcp:mcp` (`chown -R`); `/opt/caselaw/hf_cache` and `hub/` are 755, the model tree keeps the modes `cp -a` copied from `/root/.cache` |
| Env | `HF_HOME=/opt/caselaw/hf_cache`, `HF_HUB_OFFLINE=1` in `.env.mcp` (§5) |
| Access | **Read only.** Not in `ReadWritePaths` of `mcp-server@.service` and must not be added; with `HF_HUB_OFFLINE=1` nothing is ever written |
| Cost | ~0.5 GB RSS extra per worker (torch is already resident). Latency: the model loads lazily, so the FIRST rescue after every worker boot stalls that one user request ~8 s and its sibling pinpoint threads get no semantic result; with publish.py's daily recycle of all 8 workers that is ~8 requests/day unless the workers are warmed after the restart (open item; p95 before/after not yet measured, see `docs/pinpoint_semantic_rollout.md` "Latency cost"). Sandboxed test: dim 384, load 7.8 s, 1.2 GB RSS |

**Scope.** The model serves `_compute_pinpoint`, i.e. the `pinpoint` attached by `search_decisions` / REST `GET /api/decisions` (top 5, `include_pinpoint`) and `find_leading_cases` (top 3). `find_relevant_erwaegung` (MCP tool and REST `/api/relevant-erwaegung`) has its own FTS5+BM25 flow and is not gated by the flag: do not verify via `/api/relevant-erwaegung`.

**Adding another model.** Download or copy it as root into `/root/.cache/huggingface/hub` (or anywhere), then:

```bash
cp -a /root/.cache/huggingface/hub/models--<org>--<name> /opt/caselaw/hf_cache/hub/
chown -R mcp:mcp /opt/caselaw/hf_cache
find /opt/caselaw/hf_cache/hub/models--<org>--<name>/snapshots -xtype l   # must print nothing
```

Then prove it loads as `mcp` under the SAME sandbox as the unit, BEFORE any restart (this is the test `scripts/hf_cache_stage.sh` runs):

```bash
systemd-run --quiet --wait --pipe --collect --uid=mcp --gid=mcp \
  -p ProtectSystem=strict -p PrivateTmp=true -p NoNewPrivileges=true \
  -p ReadWritePaths=/opt/caselaw/repo/output -p ReadWritePaths=/opt/caselaw/repo/logs \
  -p ReadWritePaths=/mnt/HC_Volume_104655575 -p WorkingDirectory=/opt/caselaw/repo \
  --setenv=HF_HOME=/opt/caselaw/hf_cache --setenv=HF_HUB_OFFLINE=1 \
  /usr/bin/python3 -c '
from sentence_transformers import SentenceTransformer
m = SentenceTransformer("<org>/<name>")
print("dim", m.get_sentence_embedding_dimension())'
```

A `PermissionError` or "not found in cache" here is exactly what the workers would hit; fix it before touching the pool. Models referenced by `mcp_server.py` that are deliberately NOT in the cache: `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` (`SWISS_CASELAW_CROSS_ENCODER`; copying it would silently change ranking at the next restart, MRR benchmark first, §13) and `BAAI/bge-m3` (`SWISS_CASELAW_VECTOR_SEARCH`, doubly gated off: flag `false` and no `vectors.db`; copy the snapshot before any flip). The only other `huggingface_hub` use in `mcp_server.py` is `update_database` (Parquet download), hidden in `REMOTE_MODE`, so offline mode affects nothing live.

**Restarting.** Build-window rule: no worker restarts (rolling-restart rule, this section and §9) and no full-table scans of `decisions.db` (CLAUDE.md invariant 9) from 03:30 UTC until the publish pipeline exits (weekdays ~19:35-19:45 UTC); the incremental publish runs 20:00 to ~22:00-22:47 UTC Mon-Sat (§7). Check at the moment of action. `opencaselaw-publish.service` is `Type=oneshot`, so `is-active` is useless:

```bash
systemctl show -p ActiveState --value opencaselaw-publish.service              # must not be "activating"
systemctl show -p ActiveState --value opencaselaw-publish-incremental.service  # must not be "activating"
tail -1 /opt/caselaw/repo/logs/publish.log               # 03:30 full run
tail -1 /opt/caselaw/repo/logs/incremental_nightly.log   # 20:00 incremental run (Mon-Sat); publish.log looks green during it
bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh                      # one worker at a time, health-gated
```

`bash /opt/caselaw/repo/scripts/hf_cache_restart_verify.sh restart` does the gate check (re-evaluated before every worker, plus a refusal when a publish timer fires within 10 min), the rolling restart (stopping on the FIRST unhealthy worker, unlike `rolling_restart_workers.sh`, which continues) and the verification below, with an ntfy summary. `... verify` runs only the checks, e.g. after publish.py's own recycle.

**Verifying after a restart.** The model loads lazily on the first rescue (~8 s) and the pinpoint threads race on `_SEMANTIC_MODEL_TRIED` during that first request, so warm each worker once, then check. Baseline before the fix: `pinpoint` null, because the French claim has no lexical match in the German Erwägungen.

```bash
Q='q=l%C3%A9gislateur%20maintenir%20syst%C3%A8me%20double%20d%C3%A9lai%20prescription%20amiante&court=bge&date_from=2019-11-06&date_to=2019-11-06&limit=5&include_pinpoint=true'
for u in $(systemctl list-units 'mcp-server@*.service' --state=active --no-legend --plain | awk '{print $1}'); do
  p="${u#mcp-server@}"; p="${p%.service}"
  curl -s --max-time 120 "http://127.0.0.1:$p/api/decisions?$Q" >/dev/null   # warm the lazy load
  curl -s --max-time 120 "http://127.0.0.1:$p/api/decisions?$Q" | python3 -c '
import sys, json
hit = [r for r in (json.load(sys.stdin).get("results") or []) if r.get("decision_id") == "bge_BGE_146_III_25"]
pp = hit[0].get("pinpoint") if hit else None
print(sys.argv[1], "OK semantic" if isinstance(pp, dict) and pp.get("source") == "semantic" else "NOT semantic", pp)' "$p"
done
# expect one "loaded pinpoint semantic model ... (dim=384)" per worker since ITS OWN boot, and zero "load failed"
# (per unit + epoch --since: a fleet-wide window would count lines from the replaced process of a later-restarted worker,
#  and a bare timestamp is parsed in the host TZ)
for u in $(systemctl list-units 'mcp-server@*.service' --state=active --no-legend --plain | awk '{print $1}'); do
  s=$(date -d "$(systemctl show -p ActiveEnterTimestamp --value "$u")" +%s)
  j=$(journalctl -u "$u" --since "@$s" --no-pager -q)
  echo "$u loaded=$(grep -c 'loaded pinpoint semantic model' <<<"$j") failed=$(grep -c 'semantic model load failed' <<<"$j")"
done
```

**Rollback.** The canonical procedure is `docs/pinpoint_semantic_rollout.md` "Deactivation"; do not improvise from this table. In short: (a) to turn the feature off, drop the `PINPOINT_SEMANTIC_ENABLED` line (rewrite the file with `cat >` from a `grep -v` copy so the `mcp:mcp 0600` inode survives; never `sed -i`), keep the two `HF_*` lines and the cache, gate, rolling restart; `rm -rf /opt/caselaw/hf_cache` only after every worker is back with the flag off. (b) Restoring `/root/.env.mcp.bak-2026-09-03-pre-hfcache` reverts only the 09-03 cache change: the flag stays on, the load fails again and `_compute_pinpoint` is lexical-only, the state users had from 2026-08-23 until the restart. Deleting the cache while the flag is still on re-enters that same broken state.

---

## 5. Environment Variables (.env.mcp)

| Variable | Purpose |
|----------|---------|
| `REMOTE_MODE=True` | Hides update_database/check_update_status tools |
| `SWISS_CASELAW_DIR=/opt/caselaw/repo/output` | Data directory |
| `ANTHROPIC_API_KEY=<redacted>` | Haiku API for query parsing + reranking (~$0.30/day) |
| `LLM_EXPANSION_ENABLED=true` | Enable LLM query expansion |
| `SPARSE_SEARCH_ENABLED=false` | Sparse vector search disabled |
| `SWISS_CASELAW_VECTOR_SEARCH=false` | Dense vector search disabled |
| `SWISS_CASELAW_CROSS_ENCODER=true` | Set but **dead**: the model has never loaded in the workers (§13). Do not "fix" it without an MRR benchmark |
| `PINPOINT_SEMANTIC_ENABLED=true` | Semantic rescue for `_compute_pinpoint` (search_decisions / `GET /api/decisions` / find_leading_cases). In .env.mcp since 2026-08-23; live only with the offline cache (§4 above) |
| `HF_HOME=/opt/caselaw/hf_cache` | Points huggingface_hub at the mcp-readable offline model cache (§4, "Offline HuggingFace model cache"); added 2026-09-03 |
| `HF_HUB_OFFLINE=1` | Workers never contact the Hub: load from the cache or fail fast; added 2026-09-03 |

**Editing .env.mcp**: the live file is `mcp:mcp 0600`. Append as root with `cat >>`, never `sed -i` (it recreates the file as root). Back it up to `/root/` (e.g. `/root/.env.mcp.bak-2026-09-03-pre-hfcache`), never into the repo tree: the nightly pipeline runs `git stash --include-untracked` there and would sweep the backup up. A change is inert until the worker restarts (§4).

**NE_PROXY and HF_TOKEN** are set in crontab, not in .env.mcp:
- `NE_PROXY=socks5h://127.0.0.1:1080` — SOCKS tunnel for Neuchâtel scraper (court blocks Hetzner IPs)
- `HF_TOKEN=<redacted>` — HuggingFace upload token

---

## 6. Databases

All large DBs live on the data volume and are symlinked from `output/`:

| Database | Path | Size | Purpose | Rebuild |
|----------|------|------|---------|---------|
| `decisions.db` | `/mnt/HC_Volume_104655575/output/` | 61 GB | FTS5 search index, 962K+ decisions | Nightly (incremental) |
| `reference_graph.db` | `/mnt/HC_Volume_104655575/output/` | 3.7 GB | Citation + statute links | Weekly (Sunday) |
| `statutes.db` | `/mnt/HC_Volume_104655575/output/` | 56 MB | 80 federal laws, 39K articles | On demand |
| `ok_commentaries.db` | `/mnt/HC_Volume_104655575/output/` | 43 MB | OnlineKommentar.ch commentary | On demand |
| `lexfind_cache.db` | `output/` (local, not symlinked) | 28 KB | LexFind API cache with TTL | Self-managing |

**Atomic updates**: Build scripts write to `.db.tmp`, then `os.replace()` for zero-downtime swap.

**SQLite mode**: `immutable=1` URI for read-only access in MCP workers. WAL mode disabled (DELETE journal).

---

## 7. Schedule (systemd timers + cron)

| Time (UTC) | Job | Log |
|------------|-----|-----|
| 01:00 daily (`opencaselaw-scrape.timer`) | `run_all_scrapers.py` — fetch new decisions from all 54 scrapers | `logs/daily_scrape.log` |
| 03:30 daily (`opencaselaw-publish.timer`) | `publish.py` full rebuild — FTS5 rebuild, Parquet export, HF upload, stats, git push; recycles the workers ~10:07; ends weekdays ~19:35-19:45 | `logs/publish.log` |
| 20:00 Mon-Sat (`opencaselaw-publish-incremental.timer`) | incremental publish; ends ~22:00-22:47 | `logs/incremental_nightly.log` |
| 22:00 Sunday (cron) | `entscheidsuche_download.py` + `entscheidsuche_ingest.py` — weekly bulk ingest | `logs/es_download.log`, `logs/es_ingest.log` |

The union of the two publish windows (03:30 until the full run exits, and 20:00 until the incremental run exits) is the no-restart window (§4 "Restarting"). Both publish units are `Type=oneshot`: check `systemctl show -p ActiveState --value <unit>` (must not be `activating`), not `is-active`.

The publish pipeline does `git pull --rebase` before push to handle conflicts from development commits.

Optional HuggingFace artifact publishing is controlled by environment variables:

- `OCL_PUBLISH_DELTA=1` publishes daily SQLite/Parquet delta artifacts and updates `artifacts/manifest.json`.
- `OCL_PUBLISH_SQLITE_SNAPSHOT=1` publishes a full compressed SQLite base snapshot and sets `manifest.snapshot`. This can run independently for one-off bootstrap snapshots.

---

## 8. SOCKS Tunnel (Neuchâtel Proxy)

The NE court (`jurisprudence.ne.ch`) blocks Hetzner IPs at TCP level. A SOCKS5 tunnel via SSH provides access:

```bash
# Check if tunnel is up
ss -tlnp | grep 1080

# If down, restart (requires a relay host with non-Hetzner IP)
ssh -D 1080 -fNq relay-host
```

The `NE_PROXY` env var in crontab tells the NE scraper to use this tunnel.

---

## 9. Git & Deployment

| | |
|---|---|
| Repo | `git@github.com:jonashertner/caselaw-repo-1.git` (SSH, deploy key) |
| Branch | `main` only |
| Deploy key | `/root/.ssh/` (read-write) |
| Local path | `/opt/caselaw/repo` |

**Deploy new code:** commit and push from the dev machine, then fast-forward the VPS checkout. Never `scp` into the git tree, never `git pull --rebase` on the VPS (the pipeline's own stash/rebase cycle owns that tree); if the ff-merge refuses, stop and investigate.

```bash
cd /opt/caselaw/repo && git fetch origin && git merge --ff-only origin/main   # aborts on divergence: investigate, do not force
# gate, AT THE MOMENT OF ACTION (§4 "Restarting")
systemctl show -p ActiveState --value opencaselaw-publish.service              # must not be "activating"
systemctl show -p ActiveState --value opencaselaw-publish-incremental.service  # must not be "activating"
tail -1 /opt/caselaw/repo/logs/publish.log; tail -1 /opt/caselaw/repo/logs/incremental_nightly.log
bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh                      # one worker at a time, health-gated
# or, when the change touches the model path: bash /opt/caselaw/repo/scripts/hf_cache_restart_verify.sh restart
curl -s https://mcp.opencaselaw.ch/health
```

**IMPORTANT**: Always rolling restart (one worker at a time). Never restart several workers at once — causes downtime.

---

## 10. Monitoring

| Check | Command | Expected |
|-------|---------|----------|
| Health | `curl -s https://mcp.opencaselaw.ch/health` | `{"status":"ok","decisions":962XXX}` |
| Dashboard freshness | Check `generated_at` in `opencaselaw.ch/stats.json` | Today's date |
| Worker status | `systemctl status mcp-server@{8770..8777}` | `active (running)` |
| Disk space | `df -h /dev/sda1 /dev/sdb` | Root <80%, volume <80% |
| TLS expiry | `certbot certificates` | >30 days to expiry |
| Publish success | `tail -10 logs/publish.log \| grep "Step 6"` | `OK` |
| NE tunnel | `ss -tlnp \| grep 1080` | Listening |
| Memory | `free -h` | `available` > 10GB |

**Alert conditions:**
- Health endpoint returns non-200
- Dashboard date is >1 day old
- Publish Step 6 (Git Push) fails — usually git conflict, fix with `git pull --rebase`
- TLS expiry <14 days — certbot should auto-renew, check if it didn't
- Disk >80% on either volume

---

## 11. Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| mcp | 1.26.0 | MCP protocol server |
| sentence-transformers | 5.2.3 | Pinpoint semantic rescue model (`paraphrase-multilingual-MiniLM-L12-v2`), served from the offline cache `/opt/caselaw/hf_cache` (§4). The cross-encoder flag is set but dead; never enable it without an MRR benchmark (§13) |
| torch | 2.10.0 | ML backend |
| httpx | 0.28.1 | HTTP client for LLM API calls |
| camoufox | 0.4.11 | Anti-detection browser for Cloudflare-protected courts |
| playwright | 1.58.0 | Browser automation for JS-rendered court portals |
| PyMuPDF (fitz) | 1.27.1 | PDF text extraction |
| pdfplumber | 0.11.9 | PDF text extraction (fallback) |
| huggingface_hub | 0.36.2 | Dataset upload to HuggingFace; in the workers, resolves model loads against `HF_HOME` with `HF_HUB_OFFLINE=1` (§4) |
| FlagEmbedding | 1.3.5 | BGE-M3 embeddings (currently disabled) |

---

## 12. Costs

| Item | Monthly |
|------|---------|
| Hetzner VPS (CCX43) | ~EUR 116 |
| Hetzner volume (250 GB) | ~EUR 12 |
| Anthropic API (Haiku) | ~EUR 10-15 (at ~1000 queries/day) |
| Domain (GoDaddy) | ~EUR 1.50 |
| **Total** | **~EUR 140-145/month** |

---

## 13. Known Issues

| Issue | Status |
|-------|--------|
| TG court hasn't created 2025/2026 year pages | Waiting on court |
| OW portal offline since Dec 2022 | Email sent, no response |
| ~6,000 BL decisions have no full text (court removed PDFs) | Permanent |
| ~344 old SH decisions have only one-line summaries | Permanent (CMS migration dropped PDFs) |
| ChatGPT GPT-5.4 doesn't invoke MCP tools | OpenAI regression, works with GPT-5.3 |
| `SWISS_CASELAW_CROSS_ENCODER=true` has never been live in the workers: `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` exists only in `/root`'s HF cache, the load fails with the same `PermissionError` as the semantic model did, and `_get_cross_encoder` logs the failure at DEBUG, so the journal never shows it. Reranking has been Haiku-only all along (BM25/RRF/citation-graph ranking unchanged); the cross-encoder boost has never been applied | Leave dead. Do NOT copy the model into `/opt/caselaw/hf_cache`: it would silently change ranking at the next restart. Enable only after an MRR benchmark (no-regression rule), or set the flag to `false` to make the config honest. A comment saying so sits in `.env.mcp` |
| Model-loading feature flags can be silently inert: workers run as `mcp` under `ProtectSystem=strict` with no home, so any model loaded via `huggingface_hub` must be in `/opt/caselaw/hf_cache` (§4). `PINPOINT_SEMANTIC_ENABLED=true` was inert for 11 days (2026-08-23 to 09-03) this way | Rule: a flag is live only when the WORKER journal (`journalctl -u 'mcp-server@*'`) shows the success line after a boot; `.env.mcp` contents prove nothing. Sandboxed `systemd-run` load test as `mcp` before any restart |
