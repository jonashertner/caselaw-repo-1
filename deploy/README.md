# deploy/

Versioned snapshots of infrastructure configuration that lives **only on the VPS**. Committed here purely for disaster-recovery readability — **nothing in this directory is consumed by any build or publish step**. The running configuration on `caselaw-mcp` is the source of truth.

## Files

| file | what it is | path on VPS |
|---|---|---|
| `nginx-mcp-server.conf` | nginx site config for `mcp.opencaselaw.ch`: TLS termination, rate-limit zones, proxy to the 4 uvicorn workers (`127.0.0.1:8770-8773`), SSE-friendly headers, SEO redirects | `/etc/nginx/sites-enabled/mcp-server` |

## Refreshing this snapshot

After an nginx change on the VPS, pull a fresh copy:

```bash
scp -i ~/.ssh/caselaw root@46.225.212.40:/etc/nginx/sites-enabled/mcp-server \
    deploy/nginx-mcp-server.conf
git add deploy/nginx-mcp-server.conf && git commit -m "deploy: refresh nginx config"
```

## Applying from this snapshot in a DR scenario

```bash
scp -i ~/.ssh/caselaw deploy/nginx-mcp-server.conf \
    root@NEW_HOST:/etc/nginx/sites-enabled/mcp-server
ssh -i ~/.ssh/caselaw root@NEW_HOST 'nginx -t && systemctl reload nginx'
```

Prerequisites on the target host:
- nginx installed, Let's Encrypt cert at `/etc/letsencrypt/live/mcp.opencaselaw.ch/`
- upstream `mcp_workers { server 127.0.0.1:8770; ... 8773; }` block (see `conf.d/` on the current VPS — separate file, not snapshotted here because it rarely changes)
- rate-limit zones `mcp_sse` and `mcp_api` declared in the `http {}` block (same reason)

## Notable endpoints (as of 2026-04-20)

- `/health`, `/metrics`, `/api/`, `/entscheid/`, `/sitemap*`, `/robots.txt` — all proxy to workers
- `/` — SSE stream for legacy MCP clients
- `/sse` — alias of `/`
- `/messages/` — POST side-channel for legacy MCP clients
- **`/mcp`** — Streamable HTTP transport (added 2026-04-20 after 6 clients hit 404s)

## Pitfalls

- `sites-enabled/` must not contain backup files. nginx loads **every** file in that directory as a server config, so a `mcp-server.bak` alongside the live file triggers duplicate `limit_req_zone` binding errors on reload. Keep backups in `/root/` or similar.
- The security blocklist regex near the top of the file drops requests to known scanner paths (`/wp-admin`, `/phpmyadmin`, etc.) with `return 444` (silent close). Previously included `/mcp/` which blocked legitimate Streamable-HTTP sub-paths — removed 2026-04-20.
