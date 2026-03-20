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
        :8770      :8771      :8772      :8773
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
- Proxies to 4 upstream workers via `ip_hash`
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
mcp-server@{8770..8773}.service
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

# Rolling restart (one at a time, no downtime)
for p in 8770 8771 8772 8773; do
  systemctl restart mcp-server@$p && sleep 5
done

# Logs
journalctl -u mcp-server@8770 --since "1 hour ago" --no-pager

# Health check
curl -s https://mcp.opencaselaw.ch/health
```

---

## 5. Environment Variables (.env.mcp)

| Variable | Purpose |
|----------|---------|
| `REMOTE_MODE=True` | Hides update_database/check_update_status tools |
| `SWISS_CASELAW_DIR=/opt/caselaw/repo/output` | Data directory |
| `ANTHROPIC_API_KEY=sk-ant-...` | Haiku API for query parsing + reranking (~$0.30/day) |
| `LLM_EXPANSION_ENABLED=true` | Enable LLM query expansion |
| `SPARSE_SEARCH_ENABLED=false` | Sparse vector search disabled |
| `SWISS_CASELAW_VECTOR_SEARCH=false` | Dense vector search disabled |
| `SWISS_CASELAW_CROSS_ENCODER=false` | Cross-encoder reranking disabled (Haiku is better) |

**NE_PROXY and HF_TOKEN** are set in crontab, not in .env.mcp:
- `NE_PROXY=socks5h://127.0.0.1:1080` — SOCKS tunnel for Neuchâtel scraper (court blocks Hetzner IPs)
- `HF_TOKEN=hf_...` — HuggingFace upload token

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

## 7. Cron Schedule

| Time (UTC) | Job | Log |
|------------|-----|-----|
| 01:00 daily | `run_all_scrapers.py` — fetch new decisions from all 54 scrapers | `logs/daily_scrape.log` |
| 04:00 daily | `publish.py` — FTS5 rebuild, Parquet export, HF upload, stats, git push | `logs/publish.log` |
| 22:00 Sunday | `entscheidsuche_download.py` + `entscheidsuche_ingest.py` — weekly bulk ingest | `logs/es_download.log`, `logs/es_ingest.log` |

The publish pipeline does `git pull --rebase` before push to handle conflicts from development commits.

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

**Deploy new code:**
```bash
cd /opt/caselaw/repo
git pull --rebase origin main
for p in 8770 8771 8772 8773; do
  systemctl restart mcp-server@$p && sleep 5
done
curl -s https://mcp.opencaselaw.ch/health
```

**IMPORTANT**: Always rolling restart (one worker at a time). Never restart all 4 simultaneously — causes downtime.

---

## 10. Monitoring

| Check | Command | Expected |
|-------|---------|----------|
| Health | `curl -s https://mcp.opencaselaw.ch/health` | `{"status":"ok","decisions":962XXX}` |
| Dashboard freshness | Check `generated_at` in `opencaselaw.ch/stats.json` | Today's date |
| Worker status | `systemctl status mcp-server@{8770..8773}` | `active (running)` |
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
| sentence-transformers | 5.2.3 | Cross-encoder models (currently disabled) |
| torch | 2.10.0 | ML backend |
| httpx | 0.28.1 | HTTP client for LLM API calls |
| camoufox | 0.4.11 | Anti-detection browser for Cloudflare-protected courts |
| playwright | 1.58.0 | Browser automation for JS-rendered court portals |
| PyMuPDF (fitz) | 1.27.1 | PDF text extraction |
| pdfplumber | 0.11.9 | PDF text extraction (fallback) |
| huggingface_hub | 0.36.2 | Dataset upload to HuggingFace |
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
