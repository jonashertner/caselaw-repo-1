# Missing direct cantonal-law scrapers (Week 3 backlog)

Snapshot 2026-05-02 — three cantons still rely on the lossy `lexfind_pdf`
fallback for their cantonal statutes. Each needs a custom direct scraper
because their portals are not LexWork-compatible.

| Canton | # laws on lexfind_pdf | Portal | Platform | Language |
|---|---:|---|---|---|
| **JU** | 1,164 | https://rsju.jura.ch/ | Custom IceCube CMS (Hostsolutions.ch PHP) | fr |
| **VD** | 1,305 | https://www.vd.ch/etat-droit-finances/base-legislative-vaudoise (BLV) | AEM-rendered, was rs-vd.ch | fr |
| **SZ** | 587 | https://www.sz.ch/kanton/gesetze/systematische-gesetzsammlung.html | Custom AEM CMS (deep paths) | de |

Total: **3,056 cantonal laws** still on the lossy PDF-extraction path.

---

## JU — Recueil systématique des lois (RSJU)

* **Sommaire (TOC)**: https://rsju.jura.ch/fr/Sommaire/Sommaire.html
* **Document URL pattern**: `https://rsju.jura.ch/fr/viewdocument.html?idn=<idn>&id=<id>`
* **CMS**: IceCube V28 (Hostsolutions.ch). HTML rendered server-side.
* **Crawl plan**: parse the TOC for `viewdocument.html?idn=...&id=...` links;
  fetch each doc; extract the article body from a known DOM container
  (probably `<div id="content">` or similar — verify). Single-language (FR).
* **Effort**: ~2 days. Estimated 1,200 laws to fetch at 1 req/s = 20 min total.
* **Risk**: low — clean URL format, server-rendered HTML, no JS required.

## VD — Base législative vaudoise (BLV)

* **Landing**: https://www.vd.ch/etat-droit-finances/base-legislative-vaudoise
* **Old portal**: http://www.rsv-fic.vd.ch/celluleweb/search_loi_essentiel.html
  (still online for cross-checking abbreviations)
* **CMS**: AEM-based; replaced the older RSV in December 2018 ("BLV Atelier
  / Éditeur / Publication"). No documented public API; structure of law
  pages must be reverse-engineered from the BLV "Publication" front-end.
* **Effort**: 3–5 days. Largest of the three (1,305 laws).
* **Risk**: medium — modern AEM frontends sometimes need browser
  rendering; PDFs may be the most reliable source.
* **Alternative**: Vaud canton publishes the entire BLV as XML on the
  Confederation's IATI / OpenData portal — investigate before
  scraping HTML.

## SZ — Systematische Gesetzsammlung (SRSZ)

* **Landing**: https://www.sz.ch/kanton/gesetze/systematische-gesetzsammlung.html
  (deep AEM path with `/8756-8757-10021-11689` suffix)
* **CMS**: Custom AEM. Deep numeric paths.
* **Effort**: 2–3 days. Smaller than VD but messier paths.
* **Risk**: medium — paths are not stable across years.

---

## Acceptance criteria for each new direct scraper

1. Implements the same interface as `LexWorkScraper` / `SILScraper`:
   `iter_laws() → Iterator[dict]` and `fetch_law(stub) → dict | None`.
2. Registered in `scrapers/cantonal_laws/__init__.py::CANTONAL_LAW_SCRAPERS`.
3. Run via `python3 scrape_cantonal_laws.py --canton <CC>` produces
   `output/cantonal_laws_direct/<CC>.jsonl` with all known laws.
4. After build, `cantonal_laws.db` shows `text_source != 'lexfind_pdf'`
   for ≥ 95 % of that canton's laws.

## Why not LexWork for the remaining three

UR was discovered to be LexWork on 2026-05-02 (`rechtsbuch.ur.ch` returns
a `/api/manifest.json` indicating LexWork SPA, and the standard
`/api/de/texts_of_law/lightweight_index` endpoint responds with valid
JSON). Adding UR to the registry was a one-line fix.

JU, VD, and SZ each have their own custom CMS confirmed by inspection
(no `/api/manifest.json` LexWork marker; HTML renders an entirely
different page structure). They cannot be unlocked by a registry entry
alone.
