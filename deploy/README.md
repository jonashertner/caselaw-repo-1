# deploy/

Versioned snapshots of infrastructure configuration that lives **only on the VPS**. Committed here purely for disaster-recovery readability — **nothing in this directory is consumed by any build or publish step**. The running configuration on `caselaw-mcp` is the source of truth.

## Files

| file | what it is | path on VPS |
|---|---|---|
| `nginx-mcp-server.conf` | nginx site config for `mcp.opencaselaw.ch`: TLS termination, rate-limit zones, proxy to the 8 uvicorn workers (`127.0.0.1:8770-8777`), SSE-friendly headers, SEO redirects | `/etc/nginx/sites-enabled/mcp-server` |
| `certs/build-ca-bundle.sh` | Rebuilds the scraper CA bundle: certifi + the intermediates in `certs/extra/`. Run as `ExecStartPre=-` on every scraper unit, so a certifi upgrade cannot silently revert the fix. Writes atomically; on failure the last good bundle stays. | `/opt/caselaw/certs/build-ca-bundle.sh` |
| `certs/extra/*.pem` | CA **intermediates** that some Swiss court portals stopped sending. These are public certificates, not trust anchors: the builder admits one only if it already verifies against certifi, so nothing new is trusted. | `/opt/caselaw/certs/extra/` |

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

## Scraper CA bundle

Several portals serve a **leaf-only chain** — they omit the intermediate and
rely on clients chasing the leaf's AIA extension. Browsers do; OpenSSL does
not, so `requests` fails with `CERTIFICATE_VERIFY_FAILED`. Known cases:

| host | missing intermediate | since |
|---|---|---|
| `www.bger.ch` | DigiCert Global G2 TLS RSA SHA256 2020 CA1 | 2026-08-24 |
| `publicationtc.fr.ch` | RapidSSL TLS RSA CA G1 | 2025-11-03 (only surfaced 2026-08-26) |
| `www.appellationsgericht.bs.ch` and every other `*.bs.ch` court host | Thawte TLS RSA CA G1 | latent; found 2026-08-26 |

### Adding a portal whose chain breaks

```bash
# 1. take the CA Issuers URL from the leaf itself, not from a search engine
echo | openssl s_client -connect HOST:443 -servername HOST 2>/dev/null \
  | openssl x509 -noout -text | grep -A2 "Authority Information Access"
# 2. fetch it and convert DER -> PEM
curl -fsS -o /tmp/i.crt http://cacerts.EXAMPLE.com/Intermediate.crt
openssl x509 -inform DER -in /tmp/i.crt -out deploy/certs/extra/NAME.pem
# 3. it must already chain to certifi, or the builder will (correctly) skip it
openssl verify -CAfile "$(python3 -c 'import certifi;print(certifi.where())')" \
  deploy/certs/extra/NAME.pem
```

Do **not** reach for `VERIFY_SSL = False` instead. It no longer means what it
looks like (see `base_scraper._build_session`), and
`tests/test_scraper_tls_verification.py` fails if it is reintroduced.

### Applying from this snapshot

```bash
scp -i ~/.ssh/caselaw -r deploy/certs/* root@NEW_HOST:/opt/caselaw/certs/
scp -i ~/.ssh/caselaw -r systemd/*.service.d systemd/*.timer.d \
    root@NEW_HOST:/etc/systemd/system/
ssh -i ~/.ssh/caselaw root@NEW_HOST \
    'chmod +x /opt/caselaw/certs/build-ca-bundle.sh && systemctl daemon-reload'
```

The `*.service.d/ntfy.conf` drop-ins carry no secret — they only add
`EnvironmentFile=-/opt/caselaw/ops.env`. `ops.env` itself is a sibling of the
repo, mode 600, and is **never** committed; it defines `NTFY_TOPIC`.
