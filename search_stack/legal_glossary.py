"""Swiss legal trilingual glossary — deterministic DE/FR/IT concept bridge.

Problem (user report 2026-07-04): Italian BGer decisions needed repeated
prompting; DE/FR surfaced easily. Root cause: the LLM query-expansion prompt
asks for "French/Italian equivalents IF YOU KNOW THEM", and Haiku's Italian
legal vocabulary is weaker than its French, so IT synonyms are dropped
inconsistently. With IT only ~6% of the corpus, a language-agnostic query
then returns almost no Italian results.

Fix: a curated map of high-frequency Swiss legal concepts across the three
official languages. Given the query tokens + LLM synonyms, we ADD the
equivalents in the other two languages, deterministically — so the FTS query
always reaches the Italian (and French, and German) forms regardless of the
LLM's confidence. Additive only: it can widen recall, never narrow it.

Each concept is a frozenset of surface forms (lowercased). Multi-word forms
are matched as phrases; single words as tokens. Keep entries to genuinely
canonical equivalents — a wrong synonym here pollutes every matching query.
"""
from __future__ import annotations

import re

# One row per legal concept. Order within a row is irrelevant; every term
# maps to every other term in the same row. Lowercased, diacritics kept
# (FTS5 unicode61 folds them on both sides).
_CONCEPTS: list[tuple[str, ...]] = [
    # ── Procedure / remedies ──────────────────────────────────────────
    ("beschwerde", "recours", "ricorso"),
    ("berufung", "appel", "appello", "appellazione"),
    ("revision", "révision", "revisione"),
    ("einsprache", "opposition", "opposizione"),
    ("rechtsöffnung", "mainlevée", "rigetto dell'opposizione"),
    ("verjährung", "prescription", "prescrizione"),
    ("verwirkung", "péremption", "perenzione"),
    ("frist", "délai", "termine"),
    ("rechtliches gehör", "droit d'être entendu", "diritto di essere sentito"),
    ("willkür", "arbitraire", "arbitrio"),
    ("kognition", "pouvoir d'examen", "potere di esame"),
    ("beweislast", "fardeau de la preuve", "onere della prova"),
    ("beweiswürdigung", "appréciation des preuves", "apprezzamento delle prove"),
    ("aufschiebende wirkung", "effet suspensif", "effetto sospensivo"),
    ("kostenvorschuss", "avance de frais", "anticipo delle spese"),
    ("unentgeltliche rechtspflege", "assistance judiciaire", "assistenza giudiziaria"),
    ("streitwert", "valeur litigieuse", "valore litigioso"),
    ("rechtskraft", "force de chose jugée", "forza di cosa giudicata"),
    ("zuständigkeit", "compétence", "competenza"),
    ("nichteintreten", "irrecevabilité", "inammissibilità"),
    ("noven", "faits nouveaux", "fatti nuovi"),
    # ── Obligations / contract ────────────────────────────────────────
    ("vertrag", "contrat", "contratto"),
    ("kündigung", "résiliation", "disdetta"),
    ("schadenersatz", "dommages-intérêts", "risarcimento del danno"),
    ("schaden", "dommage", "danno"),
    ("verschulden", "faute", "colpa"),
    ("haftung", "responsabilité", "responsabilità"),
    ("werkvertrag", "contrat d'entreprise", "contratto di appalto"),
    ("kaufvertrag", "contrat de vente", "contratto di compravendita"),
    ("gewährleistung", "garantie", "garanzia"),
    ("verzug", "demeure", "mora"),
    ("mängel", "défauts", "difetti"),
    ("solidarität", "solidarité", "solidarietà"),
    ("verrechnung", "compensation", "compensazione"),
    ("ungerechtfertigte bereicherung", "enrichissement illégitime", "indebito arricchimento"),
    # ── Tenancy / lease ───────────────────────────────────────────────
    ("miete", "bail", "locazione"),
    ("mietzins", "loyer", "pigione"),
    ("mieterstreckung", "prolongation du bail", "protrazione della locazione"),
    ("ausweisung", "expulsion", "sfratto"),
    # ── Employment ────────────────────────────────────────────────────
    ("arbeitsvertrag", "contrat de travail", "contratto di lavoro"),
    ("fristlose kündigung", "licenciement immédiat", "licenziamento immediato"),
    ("missbräuchliche kündigung", "congé abusif", "disdetta abusiva"),
    ("überstunden", "heures supplémentaires", "ore straordinarie"),
    ("konkurrenzverbot", "clause de non-concurrence", "divieto di concorrenza"),
    # ── Family ────────────────────────────────────────────────────────
    ("scheidung", "divorce", "divorzio"),
    ("unterhalt", "entretien", "mantenimento"),
    ("kindesunterhalt", "entretien de l'enfant", "mantenimento del figlio"),
    ("obhut", "garde", "custodia"),
    ("elterliche sorge", "autorité parentale", "autorità parentale"),
    ("güterrecht", "régime matrimonial", "regime dei beni"),
    ("eheschutz", "mesures protectrices de l'union conjugale", "unione coniugale"),
    # ── Property / rights in rem ──────────────────────────────────────
    ("eigentum", "propriété", "proprietà"),
    ("besitz", "possession", "possesso"),
    ("dienstbarkeit", "servitude", "servitù"),
    ("grundpfand", "gage immobilier", "pegno immobiliare"),
    ("nachbarrecht", "rapports de voisinage", "rapporti di vicinato"),
    # ── Enforcement / bankruptcy ──────────────────────────────────────
    ("betreibung", "poursuite", "esecuzione"),
    ("konkurs", "faillite", "fallimento"),
    ("pfändung", "saisie", "pignoramento"),
    ("arrest", "séquestre", "sequestro"),
    ("kollokation", "collocation", "graduatoria"),
    # ── Criminal ──────────────────────────────────────────────────────
    ("notwehr", "légitime défense", "legittima difesa"),
    ("vorsatz", "intention", "dolo"),
    ("fahrlässigkeit", "négligence", "negligenza"),
    ("strafzumessung", "fixation de la peine", "commisurazione della pena"),
    ("betrug", "escroquerie", "truffa"),
    ("diebstahl", "vol", "furto"),
    ("körperverletzung", "lésions corporelles", "lesioni personali"),
    ("landesverweisung", "expulsion pénale", "espulsione"),
    ("untersuchungshaft", "détention provisoire", "carcerazione preventiva"),
    # ── Public / administrative / social insurance ────────────────────
    ("verfügung", "décision", "decisione"),
    ("aufsicht", "surveillance", "vigilanza"),
    ("enteignung", "expropriation", "espropriazione"),
    ("baubewilligung", "permis de construire", "licenza edilizia"),
    ("niederlassungsbewilligung", "autorisation d'établissement", "permesso di domicilio"),
    ("wegweisung", "renvoi", "allontanamento"),
    ("asyl", "asile", "asilo"),
    ("invalidenrente", "rente d'invalidité", "rendita d'invalidità"),
    ("arbeitsunfähigkeit", "incapacité de travail", "incapacità lavorativa"),
    ("unfall", "accident", "infortunio"),
    ("steuer", "impôt", "imposta"),
    ("mehrwertsteuer", "taxe sur la valeur ajoutée", "imposta sul valore aggiunto"),
]

# Build the surface-form -> concept-index map once at import.
_FORM_TO_CONCEPT: dict[str, int] = {}
for _i, _row in enumerate(_CONCEPTS):
    for _form in _row:
        _FORM_TO_CONCEPT[_form.lower()] = _i


def _forms_in(text: str) -> set[int]:
    """Return concept indices whose surface form appears in `text`."""
    low = text.lower()
    hits: set[int] = set()
    for form, idx in _FORM_TO_CONCEPT.items():
        if " " in form or "'" in form:
            if form in low:
                hits.add(idx)
        else:
            # single token: word-boundary match so 'vol' doesn't hit 'volume'
            if re.search(r"\b" + re.escape(form) + r"\b", low):
                hits.add(idx)
    return hits


def trilingual_equivalents(terms) -> list[str]:
    """Given query text and/or a list of terms, return the cross-language
    equivalents (in the OTHER languages) of every matched concept.

    Deterministic and additive: the caller ORs these into the FTS query so
    an Italian (or French, or German) form is always searched even when the
    LLM expansion omitted it. Never returns a term already present in the
    input. Order is stable (concept order, then surface order).
    """
    if isinstance(terms, str):
        blob = terms
    else:
        blob = " ".join(t for t in terms if t)
    present_forms = {m.lower() for m in re.findall(r"[\w'\-]+", blob)}
    matched = _forms_in(blob)
    out: list[str] = []
    seen: set[str] = set()
    for idx in sorted(matched):
        for form in _CONCEPTS[idx]:
            fl = form.lower()
            # skip forms already literally in the query
            if fl in present_forms:
                continue
            # skip a single-word form that is a substring already covered
            if fl in seen:
                continue
            seen.add(fl)
            out.append(form)
    return out
