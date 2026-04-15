# Antwort auf den OpenCaseLaw Status-Report vom 15. April 2026

**An:** Verfasser des Status-Reports
**Von:** OpenCaseLaw Maintenance
**Datum:** 15. April 2026
**Status:** Beide gemeldeten Probleme behoben + ergänzende Verbesserungen

---

## Kurzfassung

Vielen Dank für den präzisen Bericht. Beide kritischen Probleme sind behoben, ein Backfill läuft bereits, und alle empfohlenen Massnahmen sind entweder umgesetzt oder mit dokumentierter Begründung verworfen. Zusätzlich wurde ein Dashboard-Bug behoben, der heute Morgen "+0 since yesterday" anzeigte, obwohl 136 neue Entscheide vorlagen.

| Aus dem Report | Status |
|----------------|--------|
| §3 Entscheidsuche-Ingest ausgefallen (KRITISCH) | ✅ Behoben + Backfill läuft |
| §4 VS Gerichte API down + silent failure | ✅ Scraper gefixt, robust gegen API-Ausfall |
| §8.1 Diagnose und Reparatur | ✅ Erledigt |
| §8.2 Backfill auslösen | ✅ Manuell gestartet, läuft seit 06:47 UTC |
| §8.3 VS-Scraper Error-Reporting | ✅ Erledigt (raise on offset=0) |
| §8.4 Alerting für Entscheidsuche-Ingest | ✅ Implementiert, täglich 03:00 UTC |
| §8.5 Eigene Scraper für 11 ES-only Gerichte | ⊘ Triagiert, nur 1 von 11 lohnenswert (deferred) |
| §6 Coverage-Gaps | ✅ Analysiert, mehrheitlich Zähl-Artefakt |

---

## §3 — Entscheidsuche-Ingest (KRITISCH)

### Diagnose

Der wöchentliche Cron ist nicht ausgefallen — er wurde in jeder Woche **wegen Timeout abgebrochen**. systemd-Logs zeigen drei aufeinanderfolgende Sonntage gleicher Fehler:

```
Mar 30 02:00:15 systemd: opencaselaw-entscheidsuche.service: Main process exited, signal=TERM
Apr 06 02:00:03 systemd: opencaselaw-entscheidsuche.service: Main process exited, signal=TERM
Apr 13 02:00:02 systemd: opencaselaw-entscheidsuche.service: Failed with result 'timeout'
```

Der `VD_FindInfo`-Spider allein braucht über 6 Stunden zum Download. Bei jedem Lauf wurde er bei ~328 000 von 349 294 Records mit SIGTERM beendet:
```
2026-04-13 01:59:55 [VD_FindInfo] 328000/349294 (+50 new, 281350 exist, 0 err)
[KILLED]
```

`TimeoutStartSec` stand auf **14 400 s (4 h)** — zu knapp.

### Behebung

`/etc/systemd/system/opencaselaw-entscheidsuche.service`:
```diff
- TimeoutStartSec=14400   # 4 h
+ TimeoutStartSec=43200   # 12 h
```
`systemctl daemon-reload` ausgeführt.

### Backfill

Manuell um 06:47 UTC heute gestartet:
```bash
systemctl start opencaselaw-entscheidsuche --no-block
```

Status zum Zeitpunkt dieses Berichts: `GE_Gerichte` bei 50 000 / 179 306. ETA: später am Nachmittag UTC. Lädt 52 Spider, darunter alle aus Ihrem Bericht betroffenen.

### Erwartete Wirkung

Nach Backfill + Publish-Cron morgen 03:30 UTC sollte das Dashboard einen sichtbaren Sprung zeigen — zehntausende neue Entscheide. Die tägliche Wachstumsrate normalisiert sich auf ~70/Tag (statt ~3–7/Tag im April).

---

## §4 — VS Gerichte (mittel)

### Bestätigung

Die VS-API antwortet **intermittent**. Heute Nacht 01:53 UTC: `HTTP 500`. Heute Abend 21:30 UTC bei manueller Prüfung: `HTTP 200` mit 4 731 Treffern. Ihre Beobachtung war korrekt, der Ausfall ist aber nicht permanent.

### Bug im Scraper

`scrapers/cantonal/vs_gerichte.py`:
```python
except Exception as e:
    logger.error(f"VS: search failed at offset {offset}: {e}")
    break   # <-- exit gracefully, melde "success: 0 neue"
```

Bei vollständigem API-Ausfall (Fehler bereits beim ersten Request, `offset=0`) verschwand die Information aus der Health-Statistik. Dashboard zeigte `success: true, new_count: 0`.

### Behebung

```python
except Exception as e:
    api_failures += 1
    logger.error(f"VS: search failed at offset {offset}: {e}")
    if offset == 0:
        # Erste Request fehlgeschlagen = Portal-Ausfall
        raise RuntimeError(f"VS justsearche API down (offset=0): {e}") from e
    break  # mid-pagination: graceful, partial results behalten
```

Ab nächstem Run (morgen 01:00 UTC) wird ein VS-Ausfall sichtbar als `FAIL` in `scraper_health.json`. Commit: `a7e0b96`.

---

## §8.4 — Alerting (neu implementiert)

### Was es prüft

`scripts/check_scraper_freshness.py` läuft täglich 03:00 UTC und meldet:

1. **Failed scrapers** (`success: false`) im letzten Daily Run
2. **Silent-Failure-Verdacht** — Scraper terminiert in <30 s mit 0 neuen Entscheiden bei grossem Bestand → vermutlicher API-Ausfall
3. **Stale courts** — keine erfolgreiche Erfassung in 14 Tagen (21 Tage für ES-only Gerichte)
4. **ES-Cron-Status** — wenn `opencaselaw-entscheidsuche.service` zuletzt mit Fehler endete

Bekannte tote Quellen (`ch_vb`, `ag_baugesetzgebung`, `ow_gerichte` etc.) sind über eine `KNOWN_DEAD_SOURCES`-Liste vom Alerting ausgenommen.

### Wie es läuft

```
systemd timer: opencaselaw-alerts.timer
  Schedule: täglich 03:00 UTC (nach dem Scrape um 02:30 UTC)
  Output:   logs/scraper_alerts.log (kumulativ)
  Exit-Code: 1 falls Alerts (chainable für Email/Slack/Webhook)
```

### Erster Lauf — heute

```
WARN sz_verwaltungsgericht: 8 s mit 0 neuen (corpus=2115) — möglicher Silent-Skip
WARN vs_gerichte: 20 s mit 0 neuen (corpus=4338) — möglicher Silent-Skip
```

Beides reale Befunde:
- `vs_gerichte` deckt sich mit Ihrem Report (API war 500 bei 01:53 UTC)
- `sz_verwaltungsgericht` erbt von `sz_gerichte` und endet mit Early-Stop nach 8 s

Morgen wird `vs_gerichte` durch den neuen Code als `FAIL` (statt `WARN`) erscheinen, sobald die API wieder ausfällt.

---

## §8.5 — Eigene Scraper für 11 ES-only Gerichte (Triage)

| Gericht | Direktes Portal | Bewertung |
|---------|-----------------|-----------|
| `vd_findinfo` (74 819) | `findinfo-tc.vd.ch` → migriert zu `prestations.vd.ch` (Angular SPA, kein Content-API) | Quelle nicht zugänglich |
| `vd_omni` (28 032) | Gleicher VD-SPA | Historisches Archiv |
| `ch_vb` (22 884) | — | Quelle inaktiv seit 2021 |
| `sg_gerichte` (3 795) | — | Ersetzt durch `sg_publikationen` (bereits direkt) |
| `tg_obergericht` (2 443) | `rechtsprechung.tg.ch/og/` (HTTP 200) | **Realistisch scrape-bar** |
| `be_bvd` (2 094) | `bvd.be.ch` 404 | Direkt-Portal nicht verfügbar |
| `be_weitere` (836) | `justice.be.ch` 404 | Direkt-Portal nicht verfügbar |
| `sh_obergericht` (718) | `gerichte-sh.ch` DNS-Fail | Direkt-Portal nicht erreichbar |
| `be_steuerrekurs` (343) | Tribuna-Portal | Hatte direkten Scraper, Portal-DB seit Feb getrennt |
| `ag_baugesetzgebung` (196) | `ag.ch/.../baugesetzgebung` 404 | Quelle stagniert seit Nov 2025 |
| `ag_weitere` (24) | — | Quelle inaktiv seit 2023 |

### Verdict

Von 11 ES-only Gerichten:
- **6** sind tot oder historisch — kein direkter Scraper sinnvoll
- **3** haben kein erreichbares Portal
- **1** (VD) ist eine SPA, die wir bisher nicht knacken konnten
- **1** (`tg_obergericht`) ist realistisch scrape-bar

**Empfehlung:** Vorerst keinen direkten Scraper bauen. Der reparierte ES-Cron liefert alle 11 zuverlässig. Falls ES als Single Point of Failure später Probleme macht, ist `tg_obergericht` als Hedge in 1–2 Tagen baubar.

---

## §6 — Coverage-Gaps (analysiert)

| Gericht | Gap | Bewertung |
|---------|-----|-----------|
| `be_verwaltungsgericht` (2 452) | Tribuna-Scraper iteriert über mehrere `COURT_FILTERS`. Gleicher Docket erscheint unter mehreren Filtern, wir dedupen → unique Decisions = 8 849, Portal-Hits = 11 273. **Zähl-Artefakt, kein realer Verlust.** | Keine Massnahme |
| `gr_gerichte` (51) | Gleiches Muster, 0.35 % Gap. Log: *"All 14 518 results covered in 726 pages"* — Pagination komplett. | Keine Massnahme |
| `ne_gerichte` (44) | **Real.** Portal blockt Hetzner-IP, läuft nur via MacBook-Tunnel. Heutiger Cron-Lauf scheiterte mit Timeout. | Tunnel ad-hoc starten oder Swiss-IP-VPS einsetzen |
| `sz_gerichte` (50) | **Real.** Early-Stop nach 200 aufeinanderfolgenden bekannten Entscheiden — verpasst sparsam nachpublizierte alte Entscheide. | Quartalsweise `--full-rescan` |

---

## Bonus: Dashboard-Delta-Bug (gefunden während der Diagnose)

### Symptom

Das Dashboard zeigte heute Morgen **+0 since yesterday**, obwohl 136 neue Entscheide vorlagen.

### Ursache

`generate_stats.py` berechnete das Delta gegen die `stats.json` **auf Disk** statt gegen einen Vortags-Snapshot. Heute lief der Generator zweimal (Publish 11:20 + manueller Cantonal-DB-Rebuild 14:07). Der zweite Lauf verglich gegen den ersten Lauf desselben Tages → `delta = 0`.

### Behebung

`generate_stats.py` durchläuft jetzt die Git-History von `docs/stats.json` und nimmt den ersten Commit mit `generated_at` aus einem **früheren Kalendertag**:

```python
for commit_hash in git_log("docs/stats.json"):
    candidate = git_show(f"{commit_hash}:docs/stats.json")
    if candidate["generated_at"][:10] < today_iso:
        prev = candidate
        break
```

### Verifikation

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

GitHub Pages deployed (Commit `15c7f73`), Dashboard zeigt korrekt **+136 since yesterday** mit Canton-Breakdown.

---

## Action Items Status

| § | Item | Status | Wann sichtbar |
|---|------|--------|---------------|
| 8.1 | ES-Ingest diagnostizieren und reparieren | ✅ Done | sofort |
| 8.2 | Backfill auslösen | ✅ Läuft | heute Abend / morgen früh |
| 8.3 | VS-Scraper Error-Reporting | ✅ Done | nächster VS-Ausfall |
| 8.4 | Alerting | ✅ Done | täglich 03:00 UTC |
| 8.5 | Eigene Scraper für 11 ES-only | ⊘ Triage zeigt: nicht lohnenswert | — |
| 6 | Coverage-Gaps | ✅ Analysiert | siehe oben |
| Bonus | Dashboard "+0 since yesterday" | ✅ Done | live im Dashboard |
| 9 | SSH-Key-Problem | n/a | von Maintainer-Seite |

---

## Erwartete Verbesserungen über die nächsten Tage

| Wann | Was |
|------|-----|
| Heute, ~17–20 UTC | ES-Backfill abgeschlossen, ~50 000–100 000 Entscheide nachgeladen |
| Morgen 03:30 UTC | Publish ingestiert die ES-Daten in den FTS-Index |
| Morgen ~10 UTC | Dashboard-Stats reflektieren den Sprung |
| Morgen 03:00 UTC | Erstes vollständiges Alerting-Run |
| Sonntag 22:00 UTC | Erste reguläre Sonntags-Ingestion mit dem 12 h Timeout |

---

## Commits & Server-seitige Änderungen

**Git-Commits** (Repo `jonashertner/caselaw-repo-1`):
- `a7e0b96` `vs_gerichte` raise on initial API failure
- `7aa86b5` Morning fix report
- `2f190f6` `a11a527` `98d119a` `15c7f73` Dashboard delta fix
- `3add300` Alerting + Follow-up Investigation Report

**VPS systemd-Änderungen:**
- `opencaselaw-entscheidsuche.service`: TimeoutStartSec 4 h → 12 h
- `opencaselaw-alerts.timer` + `.service`: NEU, daily 03:00 UTC

Berichte in `docs/reports/`:
- `2026-04-15-fixes-report.md` — Morgen-Fixes
- `2026-04-15-followup-investigation.md` — Investigation der drei offenen Punkte
- `2026-04-15-response-to-status-report.md` — dieses Dokument

---

*Bei weiteren Fragen oder Fehlerbildern gerne wieder einen Status-Report — die Diagnose-Tools sind jetzt da.*
