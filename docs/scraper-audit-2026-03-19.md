# Scraper Audit — March 19, 2026

Total decisions: 962,272
Decisions with < 500 chars full text: 25,998 (2.7%)

## Summary

| Priority | Court | Short | Total | % | Status | Action |
|----------|-------|-------|-------|---|--------|--------|
| CRITICAL | gr_gerichte | 9,344 | 14,407 | 64.9% | OCR v2 running | Tribuna PDFs accessible, fitz re-extraction working |
| CRITICAL | bl_gerichte | 6,036 | 16,947 | 35.6% | **Lost** | baselland.ch behind Cloudflare + paths 404. Swisslex only has 9,141 (we have all). Entscheidsuche proxy dead. Court removed PDFs. |
| CRITICAL | zh_gerichte | 1,243 | 1,436 | 86.6% | OCR v2 running | gerichte-zh.ch alive, PDFs accessible |
| CRITICAL | ju_gerichte | 901 | 1,052 | 85.6% | OCR v2 running | jurisprudence.jura.ch Tribuna alive |
| CRITICAL | sh_gerichte | 486 | 696 | 69.8% | **Fixable** | See fix notes below |
| HIGH | vs_gerichte | 1,559 | 6,597 | 23.6% | OCR v2 running | rechtsprechung.vs.ch alive |
| HIGH | ar_gerichte | 1,246 | 4,447 | 28.0% | OCR v2 running | rechtsprechung.ar.ch alive |
| HIGH | sh_obergericht | 245 | 718 | 34.1% | **Fixable** | Same CMS as sh_gerichte |
| HIGH | be_weitere | 220 | 836 | 26.3% | OCR v2 running | jgk.be.ch — needs testing |
| HIGH | sg_publikationen | 143 | 658 | 21.7% | OCR v2 running | publikationen.sg.ch alive |
| MEDIUM | fr_gerichte | 1,420 | 14,124 | 10.1% | OCR v2 running | entscheidsuche.ch/fr_helper returns PDFs (200) |
| MEDIUM | ge_gerichte | 1,342 | 166,961 | 0.8% | **Fixing** | HTML fallback working (OCR v2), 1,063 have no PDF URL |
| MEDIUM | be_zivilstraf | 442 | 5,673 | 7.8% | OCR v2 running | zsg-entscheide.apps.be.ch alive |
| MEDIUM | be_verwaltungsgericht | 391 | 11,195 | 3.5% | OCR v2 running | vg-urteile.apps.be.ch alive |
| MEDIUM | ag_gerichte | 231 | 2,831 | 8.2% | OCR v2 running | decwork.ag.ch alive |
| MEDIUM | zg_obergericht | 134 | 1,219 | 11.0% | OCR v2 running | entscheidsuche.ch/zg_helper 404, but obergericht.zg.ch Tribuna works |
| LOW | All others | < 100 each | — | < 5% | OCR v2 running | Most have accessible PDFs |

## Fix Notes

### SH (sh_gerichte + sh_obergericht = 731 decisions)

**Status**: CMS was migrated. Old UUID-based PDF links (`/CMS/get/file/{old-uuid}`) return 404. But decisions are still published at new URLs on the same CMS.

**New site structure** (confirmed via camoufox):
1. Year index page: `https://obergerichtsentscheide.sh.ch/CMS/Webseite/Obergerichtsentscheide/{year}-{cms-id}-DE.html`
   - Years 2000–2026 available
   - Each year page lists ~12 decisions as JavaScript widget cards

2. Each widget card links to `javascript:;` — clicking navigates to a detail page:
   - Detail URL: `https://obergerichtsentscheide.sh.ch/CMS/Webseite/Obergerichtsentscheide/{year}-{cms-id}-DE.html`
   - Detail page contains the decision text and a NEW PDF UUID:
     `https://obergerichtsentscheide.sh.ch/CMS/get/file/{new-uuid}`

3. The new PDF UUIDs are different from the old ones stored in our DB.

**Fix approach**:
- Requires camoufox (JavaScript-rendered pages)
- Scrape each year page (2000–2026)
- Click each widget card to get the detail page
- Extract new PDF UUID from detail page HTML (`/CMS/get/file/{uuid}`)
- Download PDF and extract text with fitz
- Match to existing decisions by docket number (e.g., "Nr. 51/2024/2")
- Update JSONL with extracted text and new PDF URL

**Estimated effort**: 2–3 hours to write and test the scraper update.

### BL (6,036 decisions) — PERMANENTLY LOST

Three source paths all dead:
1. **baselland.ch**: Behind Cloudflare challenge. Even with camoufox bypass, individual PDF paths return 404 — court restructured site and removed old files.
2. **bl.swisslex.ch API**: Returns only 9,141 decisions (we already have all of them with full text). The 6,036 short-text decisions are NOT on Swisslex.
3. **entscheidsuche.ch proxy**: `v2202109132150164038.luckysrv.de:8181` is dead.

These decisions were published by BL on baselland.ch, scraped by entscheidsuche.ch, and later removed by the court. We have metadata (docket number, date, court, chamber) but no full text. The PDFs no longer exist online.

**Options** (none good):
- Contact BL Kanzleidirektion to ask if old PDFs can be restored
- Accept as metadata-only records
- Check Internet Archive Wayback Machine for cached PDFs

### GR (9,344 decisions) — IN PROGRESS

PDFs on entscheidsuche.gr.ch Tribuna platform. Some return 500 (server errors on specific old files). Others download fine but text extraction with fitz produces only the metadata header (scanned PDFs). OCR v2 is processing these — early results show ~83% success rate on courts with accessible PDFs.

**Residual**: GR decisions that are truly scanned images will need better OCR (cloud OCR API like Google Vision) or will remain as metadata-only.

### GE (1,342 decisions) — PARTIALLY FIXING

1,063 have no PDF URL at all — only `source_url` pointing to `justice.ge.ch`.
OCR v2's HTML fallback is extracting text from the HTML pages. Confirmed working: `ge_gerichte_ATA_75_2015: HTML fallback extracted 1205 chars`.
279 had PDF URLs that returned 404 (ATA chamber PDFs removed by court).

### ZG (134 decisions) — FIXABLE

entscheidsuche.ch/zg_helper proxy returns 404, but `obergericht.zg.ch` Tribuna portal works directly. OCR v2 with the direct URL should recover these. Already confirmed in dry run (8/10 ZG decisions recovered via fitz).

## OCR v2 Status

Running since 13:48 UTC. Early progress: 83/100 success rate (83%). Processing ~1,200/hr.
Improvements over v1:
- HTML fallback for decisions without PDF or with failed PDF download
- Referer header for portals that block direct access
- Tries fitz text extraction before OCR (faster, higher quality)

## Courts With 100% Coverage (no short-text issues)

These courts have no or negligible (<5) short-text decisions:
bger, bge, bvger, bstger, bpatger, ch_bundesrat, edoeb, finma, comcom, elcom, postcom, ubi, weko, bge_egmr, bge_historical, ch_vb, emark, ta_sst, hudoc_ch, gl_gerichte, lu_gerichte, ne_gerichte, ow_gerichte, tg_gerichte, vd_gerichte, vd_omni, zh_steuerrekursgericht, zh_verwaltungsgericht, zh_obergericht, zh_baurekursgericht, zh_bezirksgericht_zuerich
