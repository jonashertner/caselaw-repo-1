# OpenCaseLaw — Fix Report
**Date:** 15. April 2026
**Status:** Beide gemeldeten Probleme behoben + ein Dashboard-Bug zusätzlich

---

## Zusammenfassung

Drei Bugs wurden heute identifiziert und behoben:

| # | Problem | Status | Schweregrad |
|---|---------|--------|-------------|
| 1 | Entscheidsuche-Ingest läuft seit ~6 Wochen in Timeout | ✅ Fixed + Backfill läuft | Kritisch |
| 2 | `vs_gerichte` meldet API-Ausfälle nicht (silent failure) | ✅ Fixed | Mittel |
| 3 | Dashboard zeigt fälschlicherweise "+0 since yesterday" | ✅ Fixed | Niedrig |

Drei zusätzliche Verbesserungen aus früheren Sessions sind ebenfalls live:
- Publish-Pipeline um 4.5 h früher (08:00 → 03:30 UTC)
- Cantonal-Laws-Timer nutzt jetzt direkte Scraper statt LexFind-PDF
- `list_courts` NULL-Date-Bug behoben

---

## Problem 1: Entscheidsuche-Ingest scheitert seit ~6 Wochen

### Diagnose

Der Sonntags-Cron `opencaselaw-entscheidsuche.timer` lief tatsächlich **jeden** Sonntag — aber wurde wegen Timeout abgebrochen:

```
Apr 12 22:00:02 caselaw-mcp systemd[1]: Starting opencaselaw-entscheidsuche.service
Apr 13 02:00:02 caselaw-mcp systemd[1]: opencaselaw-entscheidsuche.service: start operation timed out. Terminating.
Apr 13 02:00:02 caselaw-mcp systemd[1]: Failed with result 'timeout'.
```

Drei aufeinanderfolgende Sonntage gleicher Fehler (30. März, 6. April, 13. April).

### Ursache

`TimeoutStartSec=14400` (4 h) im systemd-Service. Aber der `VD_FindInfo`-Spider allein braucht **>6 h** zum Download (war beim letzten Lauf bei 328 000 von 349 294 Records, als der SIGTERM kam):

```
2026-04-13 01:59:55 entscheidsuche_download INFO [VD_FindInfo] 328000/349294 (+50 new, 281350 exist, 0 err)
[KILLED via SIGTERM]
```

### Fix

`/etc/systemd/system/opencaselaw-entscheidsuche.service`:

```diff
- TimeoutStartSec=14400   # 4h
+ TimeoutStartSec=43200   # 12h
```

`systemctl daemon-reload` ausgeführt.

### Backfill

Manueller Lauf um 06:47 UTC gestartet:
```bash
systemctl start opencaselaw-entscheidsuche --no-block
```

Läuft jetzt im Hintergrund, voraussichtlich ~6–10 h Laufzeit. Lädt 52 Spider, darunter alle aus dem User-Report betroffenen (`VD_FindInfo`, `VD_Omni`, `SG_Gerichte`, `BE_BVD`, `BE_Steuerrekurs`, `TG_OG`, `SH_OG`, etc.).

### Erwartete Auswirkung

Nach Abschluss des Backfills + nächstem Publish-Cron (morgen 03:30 UTC):
- Tausende fehlende Entscheide werden ingested
- Stats-Delta auf opencaselaw.ch wird einen Sprung machen (+5 000 bis +20 000)
- Tägliche Wachstumsrate kehrt von ~3–7/Tag zurück auf ~70/Tag (ES-Anteil wieder dabei)

---

## Problem 2: `vs_gerichte` Silent Failure bei API-Ausfall

### Status der API zum Zeitpunkt der Prüfung

Aktuell **antwortet die API**:
```
$ curl https://api-justsearche.vs.ch/api/search/?offset=0&limit=1
HTTP 200 → {"count":4731, ...}
```

Der User-Report sah einen 500er. Die API ist also **intermittent** — manchmal up, manchmal down. Der Bug im Scraper bleibt unabhängig davon valide.

### Ursache

`scrapers/cantonal/vs_gerichte.py` Zeile 69–71:
```python
except Exception as e:
    logger.error(f"VS: search failed at offset {offset}: {e}")
    break   # <-- exit gracefully, return success
```

Bei totalem API-Ausfall (Fehler bereits beim ersten Request, `offset=0`) bricht der Scraper ab, gibt `success: true` mit `new_count: 0` zurück. → Daily Health zeigt grün, obwohl keine Daten geladen werden konnten.

### Fix

```python
except Exception as e:
    api_failures += 1
    logger.error(f"VS: search failed at offset {offset}: {e}")
    if offset == 0:
        # Erste Request fehlgeschlagen = Portal-Ausfall
        # → Surface als echter Error
        raise RuntimeError(f"VS justsearche API down (offset=0): {e}") from e
    break   # mid-pagination failure: keep partial data
```

Jetziges Verhalten:
- API down beim ersten Request → `success: false` in scraper_health.json → sichtbar im Dashboard
- API geht mid-pagination kaputt → graceful break, partial results behalten
- API funktioniert → unverändert

Commit: `a7e0b96`

---

## Problem 3: Dashboard "+0 since yesterday" (Bonus-Fund)

### Symptom

Dashboard zeigte heute "+0 since yesterday", obwohl 136 neue Entscheide vorhanden waren.

### Ursache

`generate_stats.py` berechnete das Delta gegen die `stats.json` **auf Disk** — nicht gegen einen Snapshot von gestern. Heute lief der Generator zweimal (Publish um 11:20 + manuelle Cantonal-DB-Rebuild um 14:07). Der zweite Lauf verglich gegen den ersten Lauf desselben Tages → `delta.total = 0`.

### Fix

`generate_stats.py` durchläuft jetzt die Git-History von `docs/stats.json`, nimmt den ersten Commit mit `generated_at` aus einem **früheren Kalendertag**:

```python
for commit_hash in git_log("docs/stats.json"):
    candidate = git_show(f"{commit_hash}:docs/stats.json")
    cand_date = candidate["generated_at"][:10]
    if cand_date < today_iso:
        prev = candidate
        break
```

### Verifikation

Nach dem Fix:
```json
{
  "generated_at": "2026-04-14T21:34:35Z",
  "delta": {
    "total": 136,
    "previous_generated_at": "2026-04-13T14:35:23Z",
    "by_canton": {"GE": 38, "JU": 22, "GR": 18, "FR": 16, "NE": 10, ...}
  }
}
```

Dashboard zeigt jetzt korrekt **+136 since yesterday** mit Canton-Breakdown. GitHub Pages deployed (Commit `15c7f73`).

Commits: `2f190f6`, `a11a527`, `98d119a`, `15c7f73`

---

## Was im User-Report nicht erwähnt war

### Bereits behoben in dieser Session

| Verbesserung | Effekt |
|--------------|--------|
| Publish 08:00 → 03:30 UTC | Daten 4.5 h früher live (12:00 statt 16:30 CH) |
| Scrape-Wait-Guard im Publish | Verhindert Race wenn Scrape überzieht |
| Cantonal-Laws-Timer auf direkte Scraper | 22 Kantone via API/HTML statt LexFind-PDF |
| `list_courts` NULL-Date-Format-String-Bug | Endpoint funktioniert wieder |
| Landing-Page Decision-Count dynamisch | Zeigt aktuelle Zahl statt fest „962 272" |

### Nicht im Report aber verifiziert OK

- **Direct Cantonal Laws live**: 15 722 Gesetze, 353 464 Artikel, alle 26 Kantone (22 direkt + 4 via LexFind-Fallback). DB 763 MB.
- **Federal Statutes**: 5 510 Gesetze, 398 397 Artikel (volle Fedlex-SR-Spiegelung).
- **MCP-Tools**: 21 Tools verfügbar, function test 25/25 grün.
- **Workers**: 4×uvicorn aktiv, Health OK.
- **Disk**: 47% (root), 64% (data volume) — kein Engpass.

---

## Empfehlungen aus dem User-Report (offen)

1. **Per-Spider-Checkpoint im ES-Download** — falls VD_FindInfo wieder über 12 h läuft, kann der Job mid-spider neugestartet werden. (Aktuell: alles oder nichts pro Lauf)
2. **Alerting für ES-Cron** — z.B. Slack/E-Mail wenn `last_scraped` für ES-only Gerichte > 14 Tage alt
3. **Eigene Scraper für die 11 ES-only Gerichte** — reduziert Abhängigkeit von Drittplattform
4. **VS API Status auf Dashboard surface** — wenn Fix #2 greift, sieht User es im Health-Status

Punkte 1+2 sind mittelfristig sinnvoll. Punkt 3 ist Aufwand-Nutzen-Frage. Punkt 4 funktioniert ab nächstem fehlgeschlagenen VS-Run automatisch.

---

## Action Items für User

| Wer | Was | Bis wann |
|-----|-----|----------|
| Auto | Backfill abwarten | Heute Abend |
| Auto | Morgen 03:30 UTC: Publish ingestiert die ~6 Wochen ES-Daten | Morgen 10:00 UTC |
| User | Dashboard prüfen — Delta sollte gross sein | Morgen Vormittag |
| Optional | Per-Spider-Checkpoint implementieren | Bei nächstem Timeout |
| Optional | Alerting einrichten | Wenn gewünscht |

---

*Bericht erstellt 15. April 2026 von Claude Code Agent. Alle Änderungen committed und gepusht zu `main`. VPS-Service neu geladen.*
