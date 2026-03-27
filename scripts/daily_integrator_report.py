#!/usr/bin/env python3
"""Daily integrator detection report.

Analyzes nginx access logs to identify:
- High-volume IPs (potential commercial integrators)
- Client types (Claude, ChatGPT, Cursor, custom bots)
- MCP tool usage patterns
- Suspicious or unusual access patterns

Output: JSON report + optional ntfy.sh push notification.
Designed to run daily via systemd timer.
"""

import gzip
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Nginx combined log format
LOG_RE = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" (?P<status>\d+) (?P<size>\d+) '
    r'"(?P<referer>[^"]*)" "(?P<ua>[^"]*)"'
)

# Known client signatures
CLIENT_PATTERNS = [
    ("claude.ai", re.compile(r"Claude-User|anthropic", re.I)),
    ("claude-code", re.compile(r"claude-code", re.I)),
    ("chatgpt", re.compile(r"undici|openai-mcp|ChatGPT", re.I)),
    ("cursor", re.compile(r"Cursor/", re.I)),
    ("gemini", re.compile(r"Gemini|Google-Extended", re.I)),
    ("googlebot", re.compile(r"Googlebot|GoogleOther", re.I)),
    ("bingbot", re.compile(r"bingbot|msnbot", re.I)),
    ("python-bot", re.compile(r"python-httpx|python-requests|aiohttp|urllib", re.I)),
    ("node-bot", re.compile(r"^node$|node-fetch|axios", re.I)),
    ("browser", re.compile(r"Mozilla.*(?:Chrome|Firefox|Safari)", re.I)),
]

# Paths that indicate MCP tool usage (not just crawling)
MCP_PATHS = re.compile(r"^/(sse|messages/|$)")
API_PATHS = re.compile(r"^/api/")
SEO_PATHS = re.compile(r"^/(entscheid/|sitemap|robots\.txt)")

# Exclude from analysis
SKIP_PATHS = {"/health", "/metrics", "/metrics/all", "/dev", "/favicon.ico"}

OUTPUT_DIR = Path("/opt/caselaw/repo/logs/integrator_reports")


def classify_client(ua: str) -> str:
    for name, pat in CLIENT_PATTERNS:
        if pat.search(ua):
            return name
    return "unknown"


def parse_logs(log_files: list[Path], since: datetime | None = None) -> list[dict]:
    """Parse nginx log entries from given files."""
    entries = []
    for lf in log_files:
        opener = gzip.open if lf.suffix == ".gz" else open
        try:
            with opener(lf, "rt", errors="replace") as f:
                for line in f:
                    m = LOG_RE.match(line)
                    if not m:
                        continue
                    d = m.groupdict()
                    # Parse time
                    try:
                        ts = datetime.strptime(d["time"], "%d/%b/%Y:%H:%M:%S %z")
                    except ValueError:
                        continue
                    if since and ts.replace(tzinfo=None) < since:
                        continue
                    d["timestamp"] = ts
                    d["status"] = int(d["status"])
                    d["size"] = int(d["size"])
                    entries.append(d)
        except Exception as e:
            print(f"Warning: could not read {lf}: {e}", file=sys.stderr)
    return entries


def analyze(entries: list[dict]) -> dict:
    """Produce integrator analysis report."""
    if not entries:
        return {"error": "no log entries found", "generated": datetime.now(timezone.utc).isoformat()}

    # Per-IP stats
    ip_stats = defaultdict(lambda: {
        "requests": 0, "mcp_requests": 0, "api_requests": 0,
        "seo_requests": 0, "user_agents": Counter(), "paths": Counter(),
        "methods": Counter(), "first_seen": None, "last_seen": None,
        "status_codes": Counter(), "bytes": 0,
    })

    # Global stats
    total = len(entries)
    client_counts = Counter()
    tool_calls = Counter()  # MCP tool call paths
    hourly = Counter()

    for e in entries:
        ip = e["ip"]
        path = e["path"]
        ua = e["ua"]
        ts = e["timestamp"]
        client = classify_client(ua)

        # Skip noise
        if any(path.startswith(sp) for sp in SKIP_PATHS):
            continue

        s = ip_stats[ip]
        s["requests"] += 1
        s["user_agents"][ua] += 1
        s["paths"][path] += 1
        s["methods"][e["method"]] += 1
        s["status_codes"][str(e["status"])] += 1
        s["bytes"] += e["size"]

        if s["first_seen"] is None or ts < s["first_seen"]:
            s["first_seen"] = ts
        if s["last_seen"] is None or ts > s["last_seen"]:
            s["last_seen"] = ts

        if MCP_PATHS.match(path):
            s["mcp_requests"] += 1
        if API_PATHS.match(path):
            s["api_requests"] += 1
        if SEO_PATHS.match(path):
            s["seo_requests"] += 1

        client_counts[client] += 1
        hourly[ts.strftime("%Y-%m-%d %H:00")] += 1

        # Track MCP message paths (tool invocations)
        if "/messages/" in path:
            tool_calls[path] += 1

    # Identify high-volume non-bot IPs (potential integrators)
    integrator_candidates = []
    for ip, s in sorted(ip_stats.items(), key=lambda x: -x[1]["requests"]):
        if s["requests"] < 10:
            continue
        top_ua = s["user_agents"].most_common(1)[0][0] if s["user_agents"] else ""
        client = classify_client(top_ua)

        # Skip known crawlers
        if client in ("googlebot", "bingbot"):
            continue

        duration_h = 0
        if s["first_seen"] and s["last_seen"]:
            duration_h = (s["last_seen"] - s["first_seen"]).total_seconds() / 3600

        integrator_candidates.append({
            "ip": ip,
            "requests": s["requests"],
            "mcp_requests": s["mcp_requests"],
            "api_requests": s["api_requests"],
            "client_type": client,
            "top_user_agent": top_ua[:200],
            "unique_paths": len(s["paths"]),
            "duration_hours": round(duration_h, 1),
            "first_seen": s["first_seen"].isoformat() if s["first_seen"] else None,
            "last_seen": s["last_seen"].isoformat() if s["last_seen"] else None,
            "bytes_total": s["bytes"],
            "is_high_volume": s["requests"] >= 50,
            "is_mcp_user": s["mcp_requests"] >= 5,
            "is_programmatic": client in ("python-bot", "node-bot", "unknown") and s["mcp_requests"] > 0,
        })

    # Flag likely commercial integrators
    commercial_flags = []
    for c in integrator_candidates:
        reasons = []
        if c["requests"] >= 100:
            reasons.append(f"high volume ({c['requests']} requests)")
        if c["is_programmatic"]:
            reasons.append(f"programmatic client ({c['client_type']})")
        if c["mcp_requests"] >= 20:
            reasons.append(f"heavy MCP usage ({c['mcp_requests']} tool calls)")
        if c["duration_hours"] >= 8:
            reasons.append(f"sustained access ({c['duration_hours']}h)")
        if reasons:
            commercial_flags.append({**c, "flags": reasons})

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "period": {
            "entries_analyzed": total,
            "first": min(e["timestamp"] for e in entries).isoformat() if entries else None,
            "last": max(e["timestamp"] for e in entries).isoformat() if entries else None,
        },
        "summary": {
            "total_requests": total,
            "unique_ips": len(ip_stats),
            "client_breakdown": dict(client_counts.most_common()),
            "mcp_sessions": sum(1 for s in ip_stats.values() if s["mcp_requests"] > 0),
        },
        "integrator_candidates": sorted(
            integrator_candidates, key=lambda x: -x["requests"]
        )[:30],
        "commercial_flags": commercial_flags,
        "hourly_volume": dict(sorted(hourly.items())[-48:]),  # last 48 hours
    }


def generate_alert(report: dict) -> str | None:
    """Generate alert text if commercial integrators detected."""
    flags = report.get("commercial_flags", [])
    if not flags:
        return None

    lines = [f"🔍 {len(flags)} potential commercial integrator(s) detected:\n"]
    for f in flags[:5]:
        lines.append(f"  • {f['ip']} — {f['requests']} req, {f['client_type']}")
        for reason in f["flags"]:
            lines.append(f"    → {reason}")
    return "\n".join(lines)


def notify(title: str, message: str, topic: str = "opencaselaw"):
    """Send ntfy.sh notification."""
    import urllib.request
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=message.encode(),
            headers={"Title": title, "Priority": "default"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Daily integrator detection report")
    parser.add_argument("--days", type=int, default=1, help="Days of logs to analyze")
    parser.add_argument("--notify", action="store_true", help="Send ntfy alert if commercial use detected")
    parser.add_argument("--output", type=Path, help="Output JSON file (default: auto-dated in logs/)")
    parser.add_argument("--print", action="store_true", dest="print_report", help="Print summary to stdout")
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    # Find log files
    log_dir = Path("/var/log/nginx")
    log_files = sorted(log_dir.glob("access.log*"))

    entries = parse_logs(log_files, since=since)
    report = analyze(entries)

    # Save report
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.output or OUTPUT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Report saved to {out_path}")

    # Print summary
    if args.print_report:
        s = report["summary"]
        print(f"\n{'='*60}")
        print(f"Period: {report['period']['first']} → {report['period']['last']}")
        print(f"Total requests: {s['total_requests']}, Unique IPs: {s['unique_ips']}")
        print(f"MCP sessions: {s['mcp_sessions']}")
        print(f"\nClient breakdown:")
        for client, count in s["client_breakdown"].items():
            print(f"  {client}: {count}")

        flags = report.get("commercial_flags", [])
        if flags:
            print(f"\n⚠️  Commercial integrator candidates ({len(flags)}):")
            for f in flags:
                print(f"  {f['ip']} — {f['requests']} req ({f['client_type']})")
                for r in f["flags"]:
                    print(f"    → {r}")
        else:
            print("\nNo commercial integrators flagged.")

    # Send alert
    if args.notify:
        alert = generate_alert(report)
        if alert:
            notify("OpenCaseLaw Integrator Alert", alert)
            print("Alert sent via ntfy.sh")


if __name__ == "__main__":
    main()
