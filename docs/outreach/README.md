# Institutional outreach — completeness gap closing

Three completeness gaps are best closed by institutional contact rather
than scraper engineering. Templates below are ready to send.

| Gap | Contact | Template | Sent |
|---|---|---|---|
| BL Kantonsgericht 4,997 missing decisions | Berndt @ BL Justizverwaltung | [bl_kg_followup_2026_05.md](bl_kg_followup_2026_05.md) | follow-up — first mail 2026-04-17 |
| OW portal offline since 2022-12-19 | OW Gerichtsverwaltung | [ow_portal_offline.md](ow_portal_offline.md) | not yet |
| SG chamber-rich publication agreement | SG Kantonsgericht IT-Verantwortlicher | [sg_chamber_publication.md](sg_chamber_publication.md) | not yet |
| **JU/NE bypass** (replace MacBook tunnel) | Init7 / Solnet (CH residential proxy) | _(deferred — user wants existing VPS only)_ | — |

## Why outreach beats scraping for these

* **BL**: 4,997 Kantonsgerichts-Entscheide are simply not published online
  in machine-readable form anywhere we've found. No scraper can recover
  what was never put online. Berndt is the right contact (May 2026 mail
  was acknowledged).
* **OW**: portal has been down since 2022-12-19. Scraping the dead site
  is impossible. We need a human at OW Gerichtsverwaltung to confirm
  whether the portal is being rebuilt or whether they can grant us
  one-time access to the underlying data store.
* **SG**: chamber labels (Kantonsgericht / Verwaltungsgericht etc.)
  arrive correctly via direct scraping, but the underlying SG IT system
  could publish richer metadata (Spruchkörper, Aktennummer prefixes)
  with minimal effort on their side.
