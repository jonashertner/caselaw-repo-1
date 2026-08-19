# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in OpenCaseLaw, please report it responsibly:

- **Email**: security@opencaselaw.ch (or contact the maintainer directly)
- **Do not** open a public GitHub issue for security vulnerabilities
- We will acknowledge receipt within 48 hours and aim to provide a fix within 7 days for critical issues

## Scope

This policy covers:

- The MCP server at `mcp.opencaselaw.ch`
- The REST API at `mcp.opencaselaw.ch/api`
- The GitHub repository and its code
- The VPS infrastructure

## What to Report

- Authentication or authorization bypasses
- SQL injection or command injection
- Server-side request forgery (SSRF)
- Path traversal or file disclosure
- Exposure of secrets or credentials
- Denial of service vulnerabilities
- Privacy issues (re-identification, data leakage)

## Out of Scope

- Court decisions are public records — their content is not a vulnerability
- Rate limiting thresholds (these are intentionally set)
- The absence of authentication on the MCP server (this is by design for public access)

## Security Measures

- MCP workers run as non-root user (`mcp`) with systemd hardening (ProtectSystem, PrivateTmp, NoNewPrivileges)
- SQLite databases opened in `immutable=1` mode (read-only)
- All user input in FTS5 queries is parameterized
- TLS 1.3 with automatic certificate renewal
- UFW firewall with explicit port allowlist
- fail2ban for SSH brute-force protection
- Rate limiting on all API and SSE endpoints

## Data Collection & Privacy

The authoritative, versioned privacy notice is
[opencaselaw.ch/datenschutz](https://opencaselaw.ch/datenschutz/). The summary
below mirrors it; where the two differ, the notice governs. (This section
previously described an older design — including query-text and session-ID
collection that no longer exists — and was rewritten 2026-08-19 to match the
code.)

### What is collected

| Data | Purpose | Retention |
|------|---------|-----------|
| Tool call counts, per-tool latency, error rates, outcome labels | Which features work, which return nothing | In-memory + daily aggregate flush |
| Client class (claude.ai, chatgpt, cursor, …) | Platform adoption | Same aggregate flush |
| Haiku rerank fire/change rate | Validate reranking value | Same aggregate flush |
| Rerank quality log: query text (≤200 chars), candidate decision IDs, model ordering | Search-quality research and development | 30 days, then deleted — no IP, no user ID, no session reference |
| Search traces (query *length*, strategies, timings, result IDs — no query text) | Latency and strategy analysis | 30 days, then deleted |

What is deliberately **not** collected: query text outside the rerank quality
log, zero-result query text (a former design, removed), any link between a
query and a person or between two queries, referer headers, fingerprints, and
any link between Stripe billing data and usage.

### Nginx access logs

Three tiers, as published in the notice: Tier 1 (IP + User-Agent) is kept
**72 hours** for abuse defence, then shredded; Tier 2 (class labels only) 14
days; Tier 3 is daily aggregates with differential privacy (ε = 1.0, k = 10)
and may be published. The tier configuration is in `ops/nginx/ocl-logging.conf`
and the rollup in `scripts/rollup_analytics.py` — both auditable in this repo.

### Access

Aggregated metrics are available at `/metrics` (public, JSON). A developer dashboard is available at `/dev` (token-protected, read-only).

### Principles

1. **Collect only what improves the product or ensures transparency** — every data point must be actionable
2. **No end-user tracking** — we identify platforms, not people
3. **Court decisions are public records** — but who profits from routing access through a nonprofit is our business
