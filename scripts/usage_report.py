"""Generate fine-grained usage report from tier1/tier2 nginx logs."""
import os, re
from datetime import datetime, timedelta, timezone
from collections import defaultdict

log_paths_t1 = ["/var/log/nginx/tier1.log", "/var/log/nginx/tier1.log.1"]
log_paths_t2 = ["/var/log/nginx/tier2.log", "/var/log/nginx/tier2.log.1"]

now = datetime.now(timezone.utc)
cutoff = now - timedelta(hours=24)

# Tier1 format: IP TIMESTAMP "METHOD PATH" STATUS TIME "UA"
t1_re = re.compile(
    r'^(\S+) (\S+) "(\S+) (\S+)[^"]*" (\d+) [\d.]+ "([^"]*)"'
)

by_hour = defaultdict(int)
by_status = defaultdict(int)
api_calls = defaultdict(int)
by_ip = defaultdict(int)
by_ua_class = defaultdict(int)
sse_connects = 0
entscheid_pages = 0
health_checks = 0
total_24h = 0
mcp_tool_calls = defaultdict(int)

for log_path in log_paths_t1:
    if not os.path.exists(log_path):
        continue
    with open(log_path, "r", errors="replace") as f:
        for line in f:
            m = t1_re.match(line)
            if not m:
                continue
            ip, ts, method, path, status, ua = m.groups()
            try:
                dt = datetime.fromisoformat(ts)
                if dt < cutoff:
                    continue
            except:
                continue

            total_24h += 1
            hour = dt.strftime("%Y-%m-%d %H:00")
            by_hour[hour] += 1
            by_status[status] += 1
            by_ip[ip] += 1

            # Classify UA
            ua_lower = ua.lower()
            if "googlebot" in ua_lower:
                by_ua_class["Googlebot"] += 1
            elif "bingbot" in ua_lower:
                by_ua_class["Bingbot"] += 1
            elif "claudebot" in ua_lower or "anthropic" in ua_lower:
                by_ua_class["ClaudeBot"] += 1
            elif "chatgpt" in ua_lower or "openai" in ua_lower:
                by_ua_class["ChatGPT/OpenAI"] += 1
            elif "python" in ua_lower:
                by_ua_class["Python client"] += 1
            elif "bot" in ua_lower or "crawler" in ua_lower or "spider" in ua_lower:
                by_ua_class["Other bot"] += 1
            else:
                by_ua_class["Browser/other"] += 1

            # Classify path
            if path in ("/", "/sse"):
                sse_connects += 1
            elif path == "/health":
                health_checks += 1
            elif path.startswith("/api/"):
                endpoint = path.split("?")[0].replace("/api/", "")
                api_calls[endpoint] += 1
            elif path.startswith("/entscheid/"):
                entscheid_pages += 1

# Tier2 format: TIMESTAMP CATEGORY TOOL METHOD STATUS TIME BYTES EXTRA
t2_re = re.compile(r'^(\S+) (\S+) (\S+) (\S+) (\d+)')
t2_categories = defaultdict(int)
t2_tools = defaultdict(int)

for log_path in log_paths_t2:
    if not os.path.exists(log_path):
        continue
    with open(log_path, "r", errors="replace") as f:
        for line in f:
            m = t2_re.match(line)
            if not m:
                continue
            ts, category, tool, method, status = m.groups()
            try:
                dt = datetime.fromisoformat(ts)
                if dt < cutoff:
                    continue
            except:
                continue
            t2_categories[category] += 1
            t2_tools[tool] += 1

print("=" * 75)
print(f"  OPENCASELAW USAGE REPORT — last 24h")
print(f"  {cutoff.strftime('%Y-%m-%d %H:%M')} to {now.strftime('%Y-%m-%d %H:%M')} UTC")
print("=" * 75)

print(f"\n  Total requests:     {total_24h:,}")
print(f"  SSE/MCP connects:   {sse_connects:,}")
print(f"  API tool calls:     {sum(api_calls.values()):,}")
print(f"  Decision pages:     {entscheid_pages:,}")
print(f"  Health checks:      {health_checks:,}")
print(f"  Unique IPs:         {len(by_ip):,}")

print("\n--- Traffic by hour ---")
for h in sorted(by_hour.keys())[-24:]:
    bar = "#" * max(1, by_hour[h] // 50)
    print(f"  {h}  {by_hour[h]:>5}  {bar}")

print("\n--- Status codes ---")
for s in sorted(by_status.keys()):
    pct = by_status[s] / total_24h * 100 if total_24h else 0
    print(f"  {s}: {by_status[s]:>6}  ({pct:.1f}%)")

print("\n--- Traffic by category (tier2) ---")
for cat in sorted(t2_categories.keys(), key=lambda k: -t2_categories[k]):
    print(f"  {t2_categories[cat]:>6}  {cat}")

print("\n--- Traffic by tool/page type (tier2) ---")
for tool in sorted(t2_tools.keys(), key=lambda k: -t2_tools[k])[:20]:
    print(f"  {t2_tools[tool]:>6}  {tool}")

print("\n--- API endpoint usage ---")
if api_calls:
    for ep in sorted(api_calls.keys(), key=lambda k: -api_calls[k]):
        print(f"  {api_calls[ep]:>5}  /api/{ep}")
else:
    print("  (no API calls in period)")

print("\n--- User agent classification ---")
for ua in sorted(by_ua_class.keys(), key=lambda k: -by_ua_class[k]):
    print(f"  {by_ua_class[ua]:>6}  {ua}")

print("\n--- Top 15 IPs ---")
for ip in sorted(by_ip.keys(), key=lambda k: -by_ip[k])[:15]:
    print(f"  {by_ip[ip]:>6}  {ip}")
