# Debug Suite Report — 2026-04-15 12:34 UTC

**Summary:** HIGH=0, MED=0, LOW=0
  runtime 200s

## Findings (by severity)

✅ **No findings** — all checks clean.


## Check outputs

### Python syntax
```
  ✅ mcp_server.py
  ✅ publish.py
  ✅ run_scraper.py
  ✅ run_all_scrapers.py
  ✅ generate_stats.py
  ✅ scrape_cantonal_laws.py
```

### Lint (ruff, critical checks)
```
All checks passed!
```

### Tests (pytest)
```
........................................................................ [ 69%]
...............................                                          [100%]
103 passed in 176.52s (0:02:56)

```

### Secrets
```
  ✅ No key patterns found in tracked files
```

### Git
```
  Modified:        0
  Untracked .py:   0
  Ahead of origin: 0 commits
```

### Dependencies
```
  194 outdated packages (20 in pyproject.toml)
  - beautifulsoup4 4.13.4 → 4.14.3
  - build 1.2.2.post1 → 1.4.3
  - coverage 7.13.1 → 7.13.5
  - fastapi 0.115.9 → 0.135.3
  - huggingface-hub 0.32.4 → 1.10.2
  - lxml 6.0.2 → 6.0.4
  - mcp 1.26.0 → 1.27.0
  - onnxruntime 1.22.0 → 1.24.4
  - pip 25.2 → 26.0.1
  - pyarrow 23.0.0 → 23.0.1
  - pydantic 2.12.5 → 2.13.0
  - PyMuPDF 1.26.6 → 1.27.2.2
  - pytest 9.0.2 → 9.0.3
  - requests 2.32.3 → 2.33.1
  - ruff 0.9.7 → 0.15.10
```

### Systemd
```
Timers:
NEXT                                  LEFT LAST                              PASSED UNIT                                ACTIVATES
Thu 2026-04-16 01:00:00 UTC            12h Wed 2026-04-15 01:00:04 UTC      11h ago opencaselaw-scrape.timer            opencaselaw-scrape.service
Thu 2026-04-16 03:00:08 UTC            14h -                                      - opencaselaw-alerts.timer            opencaselaw-alerts.service
Thu 2026-04-16 03:30:23 UTC            14h Wed 2026-04-15 03:30:09 UTC       9h ago opencaselaw-publish.timer           opencaselaw-publish.service
Thu 2026-04-16 04:31:50 UTC            15h Wed 2026-04-15 04:31:13 UTC       8h ago opencaselaw-analytics.timer         opencaselaw-analytics.service
Thu 2026-04-16 08:00:00 UTC            19h Wed 2026-04-15 08:00:00 UTC 4h 34min ago opencaselaw-integrator-report.timer opencaselaw-integrator-report.service
Thu 2026-04-16 09:00:00 UTC            20h Wed 2026-04-15 09:00:00 UTC 3h 34min ago opencaselaw-scrape-federal.timer    opencaselaw-scrape-federal.service
Sun 2026-04-19 09:30:00 UTC         3 days Sun 2026-04-12 09:30:00 UTC   3 days ago opencaselaw-research.timer          opencaselaw-research.service
Sun 2026-04-19 22:00:00 UTC         4 days Sun 2026-04-12 22:00:02 UTC   2 days ago opencaselaw-entscheidsuche.timer    opencaselaw-entscheidsuche.service
Sat 2026-05-02 04:09:05 UTC 2 weeks 2 days -                                      - opencaselaw-fedlex.timer            opencaselaw-fedlex.service
Sat 2026-05-02 07:07:21 UTC 2 weeks 2 days -                                      - opencaselaw-cantonal.timer          opencaselaw-cantonal.service
Sat 2026-05-02 13:02:47 UTC 2 weeks 3 days -                                      - opencaselaw-commentaries.timer      opencaselaw-commentaries.service

11 timers listed.
Pass --all to see loaded but inactive timers, too.

Failed units: (none)
MCP workers: active, active, active, active
```

### Nginx
```
nginx: the configuration file /etc/nginx/nginx.conf syntax is ok
nginx: configuration file /etc/nginx/nginx.conf test is successful
```

### Disk / Memory / Load
```
Filesystem      Size  Used Avail Use% Mounted on
/dev/sdb        250G  164G   87G  66% /mnt/HC_Volume_104655575
/dev/sda1       150G   68G   77G  47% /
               total        used        free      shared  buff/cache   available
Mem:            61Gi       3.9Gi       1.0Gi       5.0Mi        57Gi        57Gi
Swap:             0B          0B          0B
 12:34:12 up 52 days,  9:26,  3 users,  load average: 0.22, 0.20, 0.51

```

### DB integrity
```
decisions: 63094MB, 12 tables, readable=ok
reference_graph: 3783MB, 5 tables, readable=ok
statutes: 605MB, 9 tables, readable=ok
cantonal_laws: 762MB, 9 tables, readable=ok
ok_commentaries: 48MB, 8 tables, readable=ok
```

### Recent systemd errors (24h)
```
Apr 15 08:00:00 caselaw-mcp systemd[1]: Failed to start opencaselaw-integrator-report.service - OpenCaseLaw daily integrator detection report.
Apr 15 11:31:18 caselaw-mcp systemd[1]: Failed to start opencaselaw-research.service - OpenCaseLaw weekly ablation benchmark + research summary.
```

### Scraper registry
```
58 scrapers in registry
All import cleanly.
```

### MCP tool schemas
```
21 tools advertised
```
