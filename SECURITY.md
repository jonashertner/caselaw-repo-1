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

OpenCaseLaw collects minimal, anonymized operational data to improve search quality. **No user tracking, no IP logging, no session tracking.**

### What is collected

| Data | Purpose | Retention |
|------|---------|-----------|
| Tool call counts | Which features are used/unused | In-memory, resets on restart |
| Per-tool avg latency | Performance monitoring | In-memory, resets on restart |
| Zero-result query text | Fix search gaps | In-memory, last 500, resets on restart |
| Haiku rerank fire/change rate | Validate reranking value | In-memory, resets on restart |

### What is NOT collected

- No IP addresses or geolocation
- No user agents or device fingerprints  
- No session IDs or user identification
- No query text (except zero-result queries)
- No personal data of any kind
- No cookies or tracking pixels

### Access

Aggregated metrics are available at `/metrics` (public, JSON). A developer dashboard is available at `/dev` (token-protected, read-only).

### Principles

1. **Collect only what improves the product** — every data point must be actionable
2. **No persistent storage** — all metrics are in-memory and reset on worker restart
3. **No user tracking** — court decisions are public records; who reads them is not our business
