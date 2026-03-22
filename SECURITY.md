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
