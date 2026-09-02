# Additional data sources for the Caritas access-to-justice demo

**Prepared 2026-09-02. Assessment only — no scrapers, pipeline edits or commits.**
Gap evidence comes from live queries against production MCP tools this morning;
every candidate source below was fetched and verified today unless marked
UNVERIFIED. Server version at time of probing: `swiss-caselaw` 1.27.x, 1,061,885
decision records, `practice.db` 3,062 documents from 6 federal sources.

## 1. What Caritas will actually ask

Caritas Schweiz's client-facing legal work sits in seven areas: Sozialhilfe,
Schulden/Betreibung, Sozialversicherungen (AHV/IV/EL/ALV/EO/FamZ),
Krankenkasse/Prämienverbilligung, Asyl/Migration (Caritas holds
Rechtsvertretung mandates in Bundesasylzentren), Miete, and
Familie/Unterhalt/KESB. A counsellor's question is rarely "what did the
Bundesgericht say" — it is "what does the Sozialamt / Ausgleichskasse / RAV /
Betreibungsamt apply, and can I challenge it". That is administrative practice
and first-instance material, which is exactly where the corpus is thin.

## 2. What the demo already shows well (pitch these)

Probed 2026-09-02 with counsellor-style queries; all returned strong,
multi-cantonal, trilingual results:

| Topic | What comes back |
|---|---|
| Sozialhilfe Kürzung / Sanktion / Mitwirkungspflicht | ZH VGer, TG OGer (TVR), SZ VGer, BL KGer, GR — DE; VD CDAP + GE CJ — FR; TI Tribunale delle assicurazioni — IT |
| EL Vermögensverzicht / hypothetisches Einkommen | ZH SVGer, SG VersGer, SZ, BL, SO, GR, BGer 8C_12/2024 |
| ALV Einstelltage / Arbeitsbemühungen | FR KGer, ZH SVGer, ZG VGer |
| Widerruf / Rückstufung wegen Sozialhilfeabhängigkeit | ZH VGer (rich, with Regesten) + SEM Weisungen AIG/FZA in `search_practice` (11 hits on Familiennachzug + Sozialhilfe) |
| Nothilfe Art. 12 BV, abgewiesene Asylsuchende | ZH VGer, BGE 138 V 310 |
| Existenzminimum / Lohnpfändung | BGE 121 III 20, BGer 7B, SO Aufsichtsbehörde (SCBES 2021–2026), BL, FR |
| Mietrecht 257d OR / Erstreckung | BGE 117 II 415, BGer 4A, SG, BS AppGer, FR, GR |
| Alimentenbevorschussung / Inkassohilfe | SG VersGer (ABV), ZH VGer, SO, ZG + 10 cantonal statute hits via LexFind |
| unentgeltliche Rechtspflege | BGE 139 III 475, 135 I 102, 124 I 1, ZH OGer |
| Prämienverbilligung (statute) | KVV 106/106b, AsylV 2 5b, cantonal GR/AR/GL/SO/BL |

Also strong: the 108k BVGer corpus (asylum), 34.6k ZH Sozialversicherungsgericht,
23k VPB/JAAC, ECtHR against Switzerland, and the Botschaft corpus for
legislative intent. The Word add-in and the `/entscheid/{id}` pages give
counsellors a citable, printable output.

## 3. Where the demo fails today (measured)

### 3a. Administrative practice — the biggest hole
`search_practice` covers FINMA, SECO-ArG, ESTV, SEM, BAFU (plus ARE/EPA/SSK
scrapers in the tree). Every social-law probe returned nothing:

| Query | Hits | Missing corpus |
|---|---|---|
| Ergänzungsleistungen Vermögensverzicht | 0 | BSV Wegleitungen (WEL, KSIH, RWL …) |
| Prämienverbilligung | 0 | BAG KVG Kreisschreiben + cantonal rules |
| Sozialhilfe Grundbedarf Kürzung | 0 | SKOS-Richtlinien, cantonal Sozialhilfe-Handbücher |
| Arbeitslosenentschädigung Einstellung | 1 (irrelevant SEM hit) | SECO AVIG-Praxis |

The tool description already says "NOT covered: BSV/AHV-IV, BAG and all
cantonal administrations", so the model will at least not hallucinate — but
for Caritas these four are the core.

### 3b. Coverage gaps inside sources we already scrape
- `zh_mietgericht` has **3** rows and `zh_arbeitsgericht` **31**. Both courts
  publish far more (see §4, ZMP and AGer-Z). First-instance tenancy and labour
  law is where Caritas's clients litigate.
- Freshness: latest decision per canton is OW 2022-12-19, TG 2025-12-17,
  TI 2026-02-15, LU 2026-03-10, AR 2026-03-17 (stats.json 2026-09-01), while
  `/api/scraper-health` shows all five as `success=true, new=0` — silent-stale,
  not caught by the ≥3-discovery-errors rule. `fr_gerichte` is red (known TLS).
- GE and BE publish only appellate tenancy/labour decisions; first-instance
  Tribunal des baux / prud'hommes are not published anywhere — not a scraper
  gap, a publication gap. Say so in the demo.

### 3c. Scholarship — social law almost absent
- "Sozialhilfe" → 10 hits, nine of them ZHAW (Pärli), one closed-access ZORA.
- "Schuldenberatung OR Überschuldung OR Betreibung" → **0**.
- "Zugang zum Recht" OR "unentgeltliche Rechtspflege" OR "accès à la justice" → **0**.
- No social-work repositories are in `scrapers/scholarship/sources.py`
  (BFH ARBOR, HSLU, HES-SO ArODES, OST, SUPSI absent — see §7.4).

## 4. Candidate sources, ranked

Ranking = relevance to Caritas × licence clarity × fit with an existing
scraper pattern. Licence buckets: **URG5** = official act of a Behörde, same
Art. 5 URG theory as everything in `practice.db` today; **GREY** =
intercantonal conference or public-law association, exemption arguable but not
settled; **©** = private copyright, needs permission or metadata-only.

### Tier 1 — do these; they turn the four zero-result queries into hits

**4.1 BSV Wegleitungen / Kreisschreiben / Rundschreiben / Mitteilungen** — URG5
- Host: https://sozialversicherungen.admin.ch/de/home (DE/FR/IT trees).
  Folders `/de/f/<id>`, documents `/de/d/<id>`, files
  `/de/d/<id>/download?version=N`.
- Census today: 335 folders, **2,629 document records** across AHV, IV, EL,
  EO, FamZ, BV, KV/UV, International — including the AHI-Praxis
  Rechtsprechung archive and BVG-Mitteilungen. PDF only. **Every superseded
  version is retained** with "Gültig ab" (KSRP has versions 1–4, 2006→2024),
  which maps directly onto the FINMA versioning model already in `practice.db`.
- No RSS/JSON/OAI, sitemap 404; crawl the folder tree. E-mail notification
  service exists for change detection.
- Effort: one `PracticeScraper` subclass, same shape as `finma_rundschreiben.py`
  (versioned) — the largest single addition to `practice.db` (~doubles it).

**4.2 SECO AVIG-Praxis (ALE, KAE, SWE, RVEI, IE, AMM, AVG)** — URG5
- https://www.arbeit.swiss/secoalv/de/home/service/publikationen/kreisschreiben---avig-praxis.html
  (FR mirror; IT UNVERIFIED). ~10 large PDFs, half-yearly (current valid
  1.7.2026). Archive page exists but older PDFs 404 — keep our own versions
  from the first ingest onward.
- Effort: trivial (`seco_arg.py` sibling).

**4.3 SKOS-Richtlinien + the cantonal Sozialhilfe-Handbücher** — GREY / URG5
- SKOS: https://rl.skos.ch/ (Lexwork JS app, public; `sitemap.xml` lists
  **6,052 URLs**: RL chapters A–E, keyword pages, and `dgn-doc_RL_Space3/4_*`
  collections that host cantonal handbooks). PDFs DE/FR/IT at
  skos.ch/skos-richtlinien (2026 link 404, archive 2015/16/17/20; 2021 = 64 pp).
  60 Praxishilfen + 31 Merkblätter, free. Binding only by reference in cantonal
  law; "© SKOS". **Ask SKOS for a licence** — they are a public-law-adjacent
  association whose stated mission is dissemination; a CC-BY-ND grant is
  plausible. Metadata + deep links until then.
- Cantonal handbooks, all URG5 (issued by the cantonal Sozialamt as
  administrative directives):
  - ZH https://www.zh.ch/de/soziales/sozialhilfe/sozialhilfehandbuch.html — 238 HTML pages
  - AG https://www.ag.ch/de/verwaltung/dgs/soziales/soziale-sicherheit/handbuch-soziales — 23 chapters HTML, updated 2026-05-11
  - BS https://www.bs.ch/wsu/sozialhilfe/rechtliche-grundlagen-und-handbuch — Handbuch PDF (v2026-02) + Unterstützungsrichtlinien 2026
  - LU https://disg.lu.ch/themen/Existenzsicherung_Sozialhilfe/sozialhilfe_handbuch — 2026 PDF + change logs
  - BE (BKSE) and SG (KOS) — hosted on rl.skos.ch, public, DE/FR
  - BL (403 to bots), VD (Normes RI 2017 PDF; newer UNVERIFIED), GE (Hospice
    général directives 404), TI (direttive 2026 PDF 404) — UNVERIFIED
- Effort: ZH/AG are plain HTML (easy); BS/LU PDFs (easy); rl.skos.ch needs a
  Playwright fetch (the base_scraper already supports it).

**4.4 Betreibungsrechtliches Existenzminimum — the tables themselves** — URG5 for the cantonal tables and BJ material (strongest case in the list); GREY for the KBK Richtlinien
- National Richtlinien (KBK, 1.7.2009): https://www.betreibung-konkurs.ch/fileadmin/user_upload/02_Informationen/Richtlinien_Existenzminimum.pdf (4 pp). The Konferenz der Betreibungs- und Konkursbeamten is an association of officials, not a Behörde — same GREY bucket as SKOS/KOKES; most cantons adopt it by Kreisschreiben, which *is* URG5.
- BJ Oberaufsicht SchKG: **11 Weisungen + 4 Anhänge** https://www.bj.admin.ch/de/weisungen-schkg and **1 federal + 47 cantonal Kreisschreiben** https://www.bj.admin.ch/de/kreisschreiben-schkg (ZH 14, GL 6, LU 5, BL 4, OW 3 …), plus Musterformulare.
- Cantonal amount tables: ZH (HTML in Steuerbuch ZStB 183.3, KS 16.9.2009 PDF
  at gerichte-zh.ch), BE KS B1 2020, AG PDF, GE NI-2026 in the official RS
  (silgeneve.ch, base 1,200/1,350/1,700), LU steuerbuch.lu.ch, FR fr.ch; VD
  refers to the national guidelines.
- Why it matters: the corpus already has excellent Existenzminimum *case law*
  (§2); what a Schuldenberater needs first is the table the Betreibungsamt used.
  The cantonal Kreisschreiben and BJ Weisungen are the strongest Art. 5 URG
  case in this memo.
- Effort: ~60 PDFs + a handful of HTML pages, one scraper.

**4.5 SEM Handbuch Asyl und Rückkehr** — URG5, and a gap in a source we already have
- https://www.sem.admin.ch/sem/de/home/asyl/asylverfahren/nationale-verfahren/handbuch-asyl-rueckkehr.html
  — **52 article PDFs** in 9 chapters A–I (DE + FR verified, IT UNVERIFIED),
  per-article dates 2019–2024. `sem_weisungen.py` ingests the Weisungen index
  only; the Handbuch lives under a different path and is not in `practice.db`.
- Effort: extend `sem_weisungen.py` with one more section.

**4.6 BAG KVG Kreisschreiben** — URG5
- https://www.bag.admin.ch/de/krankenversicherung-kreisschreiben-schweiz —
  **19 Kreisschreiben** (Nr. 1.1–7.10, 2008–2026; 5.1 Prämien, 5.3 besondere
  Versicherungsformen 2026, 7.4 Akteneinsicht, 7.10 Observation). Small; it
  closes BAG-level KVG practice, **not** Prämienverbilligung: no IPV guidance
  is on the BAG index (KS 5.1 is premium approval). Individuelle
  Prämienverbilligung is cantonal execution — the SVA / Ausgleichskassen
  Wegleitungen and Merkblätter per canton — which nobody researched today.
  Add cantonal IPV guidance to the unresearched list alongside the BL/VD/GE/TI
  handbooks.

### Tier 2 — fill first-instance gaps in courts we already list

**4.7 Zürcher Mietrechtspraxis (ZMP)** — URG5
- https://www.gerichte-zh.ch/entscheide/ee0/jahrgang-2026.html — leading
  decisions of the Mietgericht **and the Schlichtungsbehörde** Bezirk Zürich,
  HTML, Jahrgänge 2013–2026, per-decision print view. Explains why
  `zh_mietgericht` holds 3 rows. Only published Schlichtungsbehörde output in
  the country that I could find.

**4.8 Arbeitsgericht Zürich (AGer-Z)** — URG5
- Annual PDFs 2003–2023 https://www.gerichte-zh.ch/themen/arbeit/hilfen/entscheidsammlung;
  2024+ per-decision PDFs at https://www.gerichte-zh.ch/entscheide/entscheide-arbeitsgericht-zuerich.html.
  Explains `zh_arbeitsgericht` = 31.

**4.9 ZH Bezirksrat KES decisions** — URG5
- ZHEntscheide, Rechtsgebiet "Kindes- und Erwachsenenschutz", from 2001:
  https://www.zh.ch/de/politik-staat/gesetze-beschluesse/rekursentscheide.html.
  First-instance KESB decisions are not published anywhere; the Bezirksrat is
  the first appeal layer and the closest thing to KESB practice in the open.

**4.10 BWO tenancy material** — URG5
- Datenbank Gerichtsentscheide (cantonal Mietrecht selection from 2002, search
  form only, count UNVERIFIED), Mitteilungen zum Mietrecht (64 Bände
  1975–2025, PDF), Schlichtungsstatistik (half-yearly PDFs 2005–2025).

**4.11 Silent-stale scrapers** (OW, TG, TI, LU, AR) — not a new source, but the
demo will show a Ticino counsellor nothing newer than February. Worth a
freshness rule keyed on "days since newest decision" per court, independent of
subprocess success.

### Tier 3 — licence-gated; ask, or metadata-only

| Source | What | Verified facts | Status |
|---|---|---|---|
| **AHV/IV Merkblätter** (Informationsstelle AHV/IV) | 78 DE Merkblätter, PDFs at `https://www.ahv-iv.ch/p/<nr>.<lang>`; DE/FR/IT full, EN subset, ES/PT 5 each; no Albanian/Turkish/Tigrinya | Copyright Informationsstelle, written consent required; not a federal office → Art. 5 URG unlikely | **Ask.** The single most client-facing document set in Swiss social insurance; the Informationsstelle is funded by the Ausgleichskassen and exists to disseminate |
| **SODK Empfehlungen** | 26 Empfehlungen 1992–2026 (Alimentenbevorschussung 2013, Nothilfe Asyl 2012, Opferhilfe 2010/2024, MNA 2016/2024), PDF DE/FR | SODK ownership, no licence notice, "no legal force" | GREY — ask |
| **KOKES Empfehlungen** | 23 free PDFs DE/FR 1990–2026 | "© KOKES"; Praxisanleitungen are sold (Dike, CHF 98–134) | GREY — ask for Empfehlungen only |
| **SFH/OSAR Herkunftsländerberichte** | 215 PDFs DE/FR, **RSS feed verified** | No licence on site (NGO ©) | Ask SFH; RSS makes ingest trivial once granted |
| **EUAA Country Guidance** | 7 countries (AFG 06/2026, SYR, SOM, SDN, IRN, IRQ, NGA), HTML+PDF, EN | "Reproduction authorised provided the source is acknowledged" | Ingestible now; attribution required |
| **Schuldenberatung Schweiz Richtlinien**, **Budgetberatung Schweiz** tables | PDF/Excel, DE/FR/IT | No licence | Ask; Caritas itself is a member of the Dachverband — a natural demo hook |
| **Ombudsstellen** (KV, Privatversicherung, Banken) | Jahresberichte + Fallbeispiele PDFs | Stiftung © | Low value; skip |
| **BJ Kindesunterhalt reports**, **BJ Leitfaden Genugtuung OHG 2024** | 7 + 1 PDFs | URG5 | Easy, small; bundle with 4.4 |

### Blocked — do not plan on these
- **OHCHR treaty-body views vs Switzerland** (juris.ohchr.org, tbinternet):
  no API/bulk export; UN Terms of Use forbid redistribution and commercial
  use. Same problem as the ECtHR gate (`proposals/2026-07-27-ecthr-cc0-redistribution-gate.md`).
- **UNHCR Schweiz** publications: Cloudflare 403 + UN TOU.
- **mietrecht.ch / mp**, **SVIT MRA**, **BlSchK** (Weblaw), **KOKES
  Praxisanleitungen**: subscription. Link out, don't ingest.
- **Zürcher Kinderkosten-Tabelle**: discontinued per 2026, no replacement
  (last 01.03.2025 table is still online and citable as historical).

## 5. Licensing framework

Apply the ECtHR-gate logic in reverse: Tier 1 and 2 are acts of federal or
cantonal authorities (BSV, SECO, BAG, SEM, BJ, cantonal Sozialämter, courts)
— the same Art. 5 URG basis as the SEM/ESTV/FINMA material already redistributed
under CC0. SKOS, SODK and KOKES are conferences/associations: not Behörden in
the Art. 5 sense even though their texts are applied as if they were law. For
those, ingest metadata + deep links immediately and full text only after a
written grant. The AHV/IV Merkblätter carry an explicit consent clause and
must not be ingested without one.

## 6. What to ask Caritas at the demo
1. Their **internal Merkblätter and case guidance** for Sozialberatung — a
   private-corpus ingest (not CC0) would make the tool answer in their own
   terms.
2. Which **cantons** their counsellors work in most: BL/VD/GE/TI handbooks are
   the UNVERIFIED ones and Caritas regional offices will know where they live.
3. A joint request to **SKOS** and the **Informationsstelle AHV/IV** for a
   redistribution licence: Caritas's name on the letter changes the answer.
4. Who built **CaritasGo** (Caritas Zentralschweiz's GPT-4o chatbot) and
   whether it can call an MCP tool — grounding it on OpenCaseLaw is the
   concrete integration to propose (see §7.1).
5. Whether **plain-language / multilingual** output matters more than more
   sources (see §7.2).

## 7. Plain-language, multilingual and Caritas's own material

### 7.1 Caritas's own publications — all rights reserved
- caritas.ch Impressum: "Sämtliche Urheberrechte … sind Eigentum von Caritas
  Schweiz", no commercial reproduction, no CC licence. Sozialalmanach 2026 is
  purchase-only (the "PDF" on the 2025 page is a teaser flyer). Positionspapiere
  are free PDFs (DE; FR/IT/EN per site language). Handbuch Armut: shop-only.
- The old caritas-schuldenberatung.ch redirects to caritas-regio.ch, which has
  three HTML Ratgeber pages (Schuldensanierung, Konsumkredite,
  Betreibung/Pfändung) and links out to schulden.ch for the Richtlinien. No
  downloadable Merkblätter found.
- **Caritas Zentralschweiz already runs a legal chatbot**: CaritasGo
  (caritas-go.ch → caritas-regio.ch), GPT-4o via poemAI GmbH, launched
  16.06.2025 for Kanton Luzern, 10 languages, >8,000 queries by mid-2025.
  Knowledge sources undisclosed. **This is the demo's real hook**: an anonymous
  MCP endpoint with R1–R3 citation discipline is exactly the grounding layer
  that chatbot lacks. Ask who built it and whether it can call a tool.
- A national "Zugang zum Recht" project could not be verified (DE and FR
  searches). Adjacent: BSV study 2021 "Rechtsberatung und Rechtsschutz von
  Armutsbetroffenen in der Sozialhilfe" (HSLU/Uni Basel; Caritas named as
  contact point) and the SFH "Charta Rechtsschutz" (end-2025, Caritas partner).
  Conclusion: ingest nothing from Caritas without a written grant; the value
  runs the other way (their private Merkblätter into a non-CC0 side corpus).

### 7.2 Multilingual official text — one cheap, high-value add
- **Fedlex English and Romansh expressions are already in the SPARQL endpoint
  we harvest**: 464 consolidations with an EN expression (283 in force), 149
  with RM (85 in force). `scrapers/fedlex.py:41` hard-codes
  `LANGUAGES = ["de", "fr", "it"]`. Adding `en` (non-binding, but what a
  counsellor with an English-speaking client needs) is the cheapest single
  change in this memo. Art. 5 URG.
- SEM brochures: "Willkommen in der Schweiz" in **12 languages**, Infobroschüre
  Flüchtlinge/vorläufig Aufgenommene in **17**, Sozialversicherungen brochure in
  11 (PDF, federal publisher, 2019/2020, reuse terms UNVERIFIED — admin.ch
  terms page 403'd).
- ch.ch is **not** CC (Bundeskanzlei copyright, written consent) and has no
  Leichte-Sprache section. EBGB has 8 Leichte-Sprache explainers (BehiG,
  UNO-BRK, Beistandschaft …), "© EBGB". migesplus.ch is Swiss Red Cross ©.
  migraweb.ch is dead (domain lapsed to a Schwingfest). AHV/IV Merkblätter:
  see Tier 3.

### 7.3 Access infrastructure and statistics
- SAV Rechtsauskunft directory (26 cantons × Rechtsauskunft / Pikett / URP):
  plain HTML, SAV ©. No national URP statistics exist; BFS collects criminal
  justice only.
- **BFS Sozialhilfe/Armut**: 92 + 4 datasets on opendata.swiss, terms
  "Freie Nutzung, Quellenangabe Pflicht", CKAN API + PxWeb JSON API
  (https://www.pxweb.bfs.admin.ch/api/v1/de/). Useful for a
  `get_statistics`-style tool answering "Sozialhilfequote in Kanton X".
- KOKES statistics 2013–2024 PDF only (community CSV on GitHub, no licence).

### 7.4 Social-work repositories — fixes the scholarship zeros
All speak OAI-PMH except ARBOR; the existing `scrapers/scholarship/oai_pmh.py`
harvester applies unchanged. Licences are per record (`dc:rights`, mostly
CC BY-NC-ND) → metadata + link, full text only where the record allows, same
policy as the 24 sources already in.

| Repository | OAI base | Records | Notes |
|---|---|---|---|
| HES-SO ArODES | `https://arodes.hes-so.ch/oai2d` | 17,364 | FR/DE/EN; Identify asks harvesters to contact admins first |
| SUPSI ARIS | `https://aris.supsi.ch/server/oai/request` | 16,814 | IT/EN; the only Italian social-work source |
| OST ORIX | `https://orix.ost.ch/server/oai/request` | 9,509 | new instance (earliest datestamp 2026-06-04), backfill depth unknown |
| HSLU (Zenodo) | `https://zenodo.org/oai2d`, sets `user-lory_hslu`, `user-lara_sa_hslu_sa` | 3,321 + 3,504 | second set = Soziale Arbeit Bachelor theses; CC BY-NC-ND dominant |
| BFH ARBOR | no OAI found; REST `https://arbor.bfh.ch/server/api` | 17,244 | DSpace-CRIS REST, needs a small adapter |

These are the institutions that write about Sozialhilfe, Schulden and
Rechtsberatung; the "Schuldenberatung → 0" result is a source-list problem,
not a corpus problem.

### 7.5 Human-rights monitoring — mostly blocked, two exceptions
- UHRI bulk export (`https://uhri.ohchr.org/api/uhri/export-results/export-full-en.json`,
  daily) covers concluding observations and UPR for Switzerland, **not**
  individual communications; its terms of use contradict each other ("freely
  and without restriction" vs "exclusive property of UHRI"). juris.ohchr.org:
  no export. Treat as link-out.
- ECSR: Switzerland never ratified the Social Charter — nothing to ingest.
- **EKR Sammlung Rechtsfälle** (Art. 261bis StGB decisions, anonymised HTML):
  the decisions are URG5; EKR summaries UNVERIFIED. Small, ingestible.
- SKMR closed 2022 (archive PDFs), SMRI infoportal took over humanrights.ch
  (DE/FR/IT/RM/EN, has a Leichte-Sprache mode, "© SMRI"), EKM 98 PDFs
  2001–2026. Reuse UNVERIFIED for all three.

## 8. Suggested order of work
1. BSV (4.1) + SECO ALV (4.2) + BAG (4.6) + SEM Handbuch (4.5): four
   `PracticeScraper` subclasses, all URG5, all PDF, one `build_practice_db.py`
   run. Update the `search_practice` docstring and `issuing_authority` enum.
2. Existenzminimum bundle (4.4) + BJ SchKG.
3. ZH first-instance: ZMP (4.7), AGer-Z (4.8), Bezirksrat KES (4.9).
4. Cantonal Sozialhilfe handbooks ZH/AG/BS/LU (4.3, URG5 half).
5. Send licence letters: SKOS, Informationsstelle AHV/IV, SODK, KOKES, SFH.
6. Fedlex `en` (+`rm`) expressions: one-line change in `scrapers/fedlex.py`, then
   a statutes rebuild — check `search_laws` language handling first.
7. Five social-work repositories via the existing OAI harvester (7.4).
8. Freshness rule for silent-stale courts (4.11).

None of this touches `publish.py` or `decisions.db`. `practice.db` is built by
its own unit, `systemd/opencaselaw-practice.service` (`ExecStartPost` runs
`search_stack.build_practice_db`), not inside the nightly rebuild — but Tier 1
roughly doubles that corpus, and `opencaselaw-practice.timer` fires **Sat 06:00
UTC** — inside a Saturday full build (03:30–~17:00). Move it after the build
exits, or run the first full BSV ingest by hand on a Sunday (invariant 9). Items 3 are new court scrapers and fall under the
pipeline gate (CLAUDE.md invariant 5) — they need explicit approval before any
write.
