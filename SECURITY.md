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

OpenCaseLaw collects minimal operational data to improve search quality and to ensure transparency about how this nonprofit platform is used.

### What is collected

| Data | Purpose | Retention |
|------|---------|-----------|
| Tool call counts | Which features are used/unused | In-memory, resets on restart |
| Per-tool avg latency | Performance monitoring | In-memory, resets on restart |
| Zero-result query text | Fix search gaps | In-memory, last 500, resets on restart |
| Haiku rerank fire/change rate | Validate reranking value | In-memory, resets on restart |
| Search query text | Usage analysis, search quality research | In-memory, resets on restart |
| Client type (claude.ai, chatgpt, cursor, etc.) | Understand platform adoption | In-memory, resets on restart |

### Commercial platform monitoring

OpenCaseLaw is a nonprofit, open-access legal research platform. We log connection metadata (IP addresses, user agents, MCP session IDs, and tool call details) to detect when commercial platforms route their users' requests through our infrastructure.

**Why this matters:** Users of commercial AI products (e.g. Copilot, ChatGPT, Cursor) should know when their service provider is sourcing legal research from a nonprofit platform rather than from proprietary databases. Transparency about the data supply chain is important for informed use.

**What is logged for this purpose:**

| Data | Purpose | Retention |
|------|---------|-----------|
| IP address of connecting client | Identify commercial infrastructure (e.g. Azure, AWS) | In-memory + daily report (JSON) |
| User-Agent string | Classify client type (python-httpx, openai-mcp, etc.) | In-memory + daily report |
| MCP session ID | Correlate tool calls within a session | In-memory, resets on restart |
| Tool name and query arguments | Understand usage patterns | In-memory, resets on restart |

**What is NOT logged:**

- No end-user identity (we see the commercial platform's IP, not the individual user's)
- No personal data, cookies, or device fingerprints
- No geolocation beyond what is inherent in an IP address

### Nginx access logs

Standard nginx access logs (IP, timestamp, path, status, user-agent) are retained for 14 days and analyzed daily for commercial integrator detection. These logs are not shared with third parties.

### Access

Aggregated metrics are available at `/metrics` (public, JSON). A developer dashboard is available at `/dev` (token-protected, read-only).

### Principles

1. **Collect only what improves the product or ensures transparency** — every data point must be actionable
2. **No end-user tracking** — we identify platforms, not people
3. **Court decisions are public records** — but who profits from routing access through a nonprofit is our business
