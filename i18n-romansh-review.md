# Romansh (Rumantsch Grischun) review — opencaselaw.ch i18n

The 2026-06-19 i18n pass (commit `a8325f3`) made every dashboard page render in
de/fr/it/rm/en. The **de/fr/it** strings are anchored to the site's existing
glossary and are high-confidence. The **rm** strings are **machine-translated,
best-effort** and shipped live so RM users see Romansh rather than English —
but each item below was flagged by the implementer for a native-speaker pass.

How to use: each row is `key` · current RM · EN/DE meaning · concern. Edit the RM
value in the file noted per section, then redeploy (push to `main`).

---

## docs/index.html — scholarship section + footer (`I18N.rm`)
| key | current RM | meaning (EN) | concern |
|---|---|---|---|
| `sch_heading` | Scienza giuridica en access liber | Open-access legal scholarship | "Scienza" reads Italian; confirm RG form (Scientscha?) |
| `sch_intro` | (long paragraph) | intro paragraph | review whole sentence for RG idiom |
| `sch_search_label` | … | "Check if a publication is indexed — …" | review |
| `sch_bridge_a` | citaziuns da decisiuns schliadas | "case citations resolved" | "schliadas" for "resolved" — verify |
| `sch_bridge_c` | punt | "bridge" | confirm "punt" vs alternative |
| `sch_bridge_e` | cuvertura dal punt | "bridge coverage" | review |
| `footer_tagline` | Rom da qualitad cun quatter stresch | "4-layer quality framework" | "Rom"/"stresch" (framework/layer) — verify |
| `sch_lic_federal` | … art. 5 LDA | "no copyright (Art. 5 URG)" | DE uses "Art. 5 URG"; confirm RM statute abbreviation |

## docs/methodology.html — `window.I18N_PAGE` (rm)
Recommend reviewing the **entire rm column** (dense technical register). Specific flags:
| key | current RM | meaning | concern |
|---|---|---|---|
| (rerank terms) | reponderaziun | "re-ranking" | technical neologism |
| (retrieval) | retschav | "retrieval" | technical neologism |
| (pinpoint) | endrins exacts | "pinpoint references" | verify |
| `s8.p1`,`s8.li1` | rahmenwerk | "framework" | German loanword kept (no RG glossary term); native may paraphrase |
| `s6.li5` | pleds vids | "stopwords" | idiom check |
| `s9.li1` | sblundregiads | "fired/launched" | idiom check |
| `s5.r10` | dretgira suprema | "supreme court" | confirm vs "tribunal federal" |
| `s3.li2` | …scripturas d'umlaut | umlaut-spelling example | confirm reads naturally |

## docs/coverage/index.html + docs/laws/index.html — `window.I18N_PAGE` (rm)
| key | current RM | meaning | concern |
|---|---|---|---|
| coverage `archives.heading` | Archivs zueglals supplementars | "frozen archives" | "zueglal" (frozen) is an unusual coinage |
| coverage `th.gap` / `lead` | largezza | "gap" | verify vs "lacuna" |
| coverage `archives.lead` | reincorporain / maletgs istorics | "historical snapshots" | review phrasing |
| laws `results.noMatch` | in singul pled magic | "a single keyword" | **likely wrong** — should be "pled-clav" |
| (general) | Vulais Vus / Dovrai | formal "you" forms | check pronoun/verb agreement across longer strings |

## docs/standards/ + docs/integrity/ — `window.I18N` (rm)
| key | current RM | meaning | concern |
|---|---|---|---|
| integrity `h1` | Integritad / Provegnientscha | "Integrity / Provenance" | "provegnientscha" — confirm |
| `prom_h2` | Tge che la ragisch empermetta | "what the root promises" | review |
| (inclusion proof) | Percurs da cumprova d'inclusiun | "inclusion proof path" | review |
| (lazy memoisation) | Memoisaziun paronza dals sutarbres | "lazy sub-tree memoisation" | "paronza" (lazy) uncertain |
| (sibling hashes) | hashs fragliuns | "sibling hashes" | verify |
| (cross-anchor) | ancoraziun crusada vers … marcas temporalas | "cross-anchoring to timestamping services" | review |
| (defence in depth) | defensiun en profunditad | "defence in depth" | verify |
| (Merkle tree) | arbre da Merkle | "Merkle tree" | "arbre" vs "planta" — match across pages |

## docs/word/rm/index.html (full standalone RM page)
| current RM | meaning | concern |
|---|---|---|
| radiografà structuralmain / radiografia da l'alinea | "structurally x-rayed" / "paragraph x-ray" (Strengthen) | x-ray metaphor — may prefer idiomatic RM |
| schwarzaziun / stgischadas structuralmain da datas persunalas | "structural PII redaction" | RM term for redaction (Schwärzung) uncertain |
| considerand | "Erwägung" (legal consideration) | confirm standard RG legal term |
| decisiuns directivas | "Leitentscheide" (leading cases) | verify preferred RG legal-tech term |
| sboz | "Entwurf" (draft) | confirm register |
| sectur d'incumbensas | "task pane" (Aufgabenbereich) | UI term — verify |
| audit da serrada cun 5 binaris | "5-Rail-Closing-Audit" | "binaris" (rails) literal — verify |
| revocabel da tut temp | "cancel anytime" (jederzeit kündbar) | confirm idiom |
| Pront? | "Ready?" (CTA heading) | confirm |
| `og:locale` = `rm_CH` | meta locale tag | new value on the site; confirm `rm_CH` vs `rm-CH` vs omit (DE word page omits it) |

---

### Notes
- de/fr/it are not in scope here — they reuse the established site glossary.
- A separate, pre-existing inconsistency (not introduced by this pass): `seo_pages.py`
  `NAV_I18N`/`I18N_FOOTER` use ASCII-folded forms (`Qualitaet`, `Cuvrida` for Coverage)
  vs the glossary's accented `Qualität`/`Cuvertura`. Worth a separate consistency pass.
