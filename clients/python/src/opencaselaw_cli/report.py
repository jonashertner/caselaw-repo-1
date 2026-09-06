"""The report a person reads after `ocl check`: what exists, what needs attention, and
what to do about each item. HTML (opens anywhere) or Markdown; the terminal renderer
draws on the same strings. Wording comes from the service's answers; the report never
composes a citation, and it never claims more than what was compared: a decision
"exists" (and its "passage retrieved" when one was cited); a quotation is "verbatim",
"differs", "not found" only after a served text was compared, else "not checked".

Labels and advice follow `--language` (de, fr, it); anything else reads English."""
from __future__ import annotations

import html
from datetime import datetime, timezone

from ._version import __version__
from .statutes import statutes_html, statutes_markdown, summarize_statutes  # statute rows: their own table, see statutes.py

# Status keys of a row as the report sees them: the resolve status, refined by the
# quotation check and by whether a cited passage was retrieved.
_OK = ("resolved", "resolved_pin")
_BAD = ("missing", "discrepancy", "quote_not_found", "unrecognized", "resolution_incomplete")

_EN = {
    "title": "Citation check: {source}",
    "scope": ("Existence, identity and wording only: whether each cited decision exists and carries the label as written, "
              "whether the cited Erwägung exists, whether dates and dockets match the record, and whether quotations stand in the served text. "
              "Not whether a decision supports the argument or is still good law."),
    "scope_short": "Existence, identity and wording only; not whether a decision supports the argument or is still good law.",
    "headline": "{checked} citations found · {exists} exist ({passages} with the cited passage retrieved) · {attention} need attention · {unparsed} possibly citations, not checked",
    "checked_line": "Checked {when} against {base}, client {version}. The citation strings shown are the service's; nothing was rewritten.",
    "h_attention": "Needs attention", "h_exists": "Exists", "h_unparsed": "Possibly citations, not checked",
    "unparsed_note": "Strings that look like dockets or collection references but were not read as a citation; nothing about them was checked.",
    "c_reference": "Citation as written", "c_finding": "Finding", "c_advice": "What to do", "c_detail": "Detail",
    "c_decision": "Decision (service's citation)", "c_where": "Where", "c_context": "Context",
    "resolved_pin": ("exists, passage retrieved", ""),
    "resolved": ("exists", ""),
    "pinpoint_unavailable": ("passage not indexed", "The decision exists; the cited Erwägung is not in the index. Quote from the decision text, not from memory."),
    "discrepancy": ("detail wrong", "The decision exists, but the date or docket written next to it does not match the record."),
    "missing": ("not in the corpus", "Check the citation. If the decision is unpublished or is the decision under appeal, it cannot be in any corpus."),
    "ambiguous": ("ambiguous", "Several decisions carry this label. Name the court, or cite by decision id."),
    "unrecognized": ("not verifiable", "The service proposed a decision that does not carry the label as written. Not cited."),
    "resolution_incomplete": ("not verifiable", "Too many exact matches to decide. Cite by decision id."),
    "error": ("check failed", "The service or network did not answer for this item; run the check again."),
    "skipped": ("skipped", "Stopped after repeated failures; run the check again."),
    "quote_near": ("quotation differs", "The quotation differs from the served wording; the differences are listed."),
    "quote_not_found": ("quotation not found", "The quotation does not appear in the cited passage or in the decision text that was compared."),
    "quote_unverifiable": ("quotation not checked", "The decision text is not available in this mode; check against the decision."),
    "d_date": "date written {written}, decision dated {decision}",
    "d_docket": "docket {written} names {resolves_to}",
    "d_parent": "E. {pinpoint} is not indexed as such; E. {parent} was retrieved instead",
    "d_indexed": "indexed: {list}",
    "d_candidate": "candidate {id} ({court}, {date})",
    "d_proposed": "service proposed {id}, label not carried",
    "d_diff": "written «{quote}», served «{served}»",
    "d_closest": "closest served wording: «{text}»",
    "d_quote_unchecked": "quotation not checked: no served text",
    "d_quote_not_found": "quotation not found in the compared text",
    "d_inferred": "read as {name}, label {label}",
    "d_coverage": "{name}: {n} decisions {first}–{last} in the corpus; unpublished decisions and the decision under appeal are expected to be absent",
    "d_coverage_none": "{name}: no coverage figure available in this mode",
    "verbatim": "quotation verbatim",
    "canton": "canton {canton}",
    "footer": "{exists} exist ({passages} with the cited passage retrieved), {attention} need attention, {unparsed} possibly citations, not checked",
    "none_found": "no citations found in the document",
    "read_line": "{paragraphs} paragraphs read, {checked} citations found",
    "report_line": "report: {path}",
    "unparsed_line": "possibly citations, not checked ({n}):",
}

_DE = {
    "title": "Zitatprüfung: {source}",
    "scope": ("Nur Existenz, Identität und Wortlaut: ob jeder zitierte Entscheid existiert und die Bezeichnung trägt, wie sie geschrieben ist, "
              "ob die zitierte Erwägung existiert, ob Datum und Geschäftsnummer mit dem Eintrag übereinstimmen und ob Zitate im gelieferten Text stehen. "
              "Nicht, ob ein Entscheid das Argument stützt oder noch geltende Praxis ist."),
    "scope_short": "Nur Existenz, Identität und Wortlaut; nicht, ob ein Entscheid das Argument stützt oder noch geltende Praxis ist.",
    "headline": "{checked} Zitate gefunden · {exists} vorhanden ({passages} mit abgerufener Erwägung) · {attention} zu prüfen · {unparsed} möglicherweise Zitate, nicht geprüft",
    "checked_line": "Geprüft am {when} gegen {base}, Client {version}. Die angezeigten Zitierstrings stammen vom Dienst; nichts wurde umformuliert.",
    "h_attention": "Zu prüfen", "h_exists": "Vorhanden", "h_unparsed": "Möglicherweise Zitate, nicht geprüft",
    "unparsed_note": "Zeichenfolgen, die wie Geschäftsnummern oder Fundstellen aussehen, aber nicht als Zitat gelesen wurden; dazu wurde nichts geprüft.",
    "c_reference": "Zitat wie geschrieben", "c_finding": "Befund", "c_advice": "Was zu tun ist", "c_detail": "Detail",
    "c_decision": "Entscheid (Zitierstring des Dienstes)", "c_where": "Wo", "c_context": "Kontext",
    "resolved_pin": ("vorhanden, Erwägung abgerufen", ""),
    "resolved": ("vorhanden", ""),
    "pinpoint_unavailable": ("Erwägung nicht indexiert", "Der Entscheid existiert; die zitierte Erwägung ist nicht im Index. Aus dem Entscheidtext zitieren, nicht aus dem Gedächtnis."),
    "discrepancy": ("Angabe falsch", "Der Entscheid existiert, aber das daneben geschriebene Datum oder die Geschäftsnummer stimmt nicht mit dem Eintrag überein."),
    "missing": ("nicht im Korpus", "Zitat prüfen. Ist der Entscheid unpubliziert oder der angefochtene Entscheid, kann er in keinem Korpus sein."),
    "ambiguous": ("mehrdeutig", "Mehrere Entscheide tragen diese Bezeichnung. Gericht nennen oder mit der Entscheid-ID zitieren."),
    "unrecognized": ("nicht verifizierbar", "Der Dienst hat einen Entscheid vorgeschlagen, der die Bezeichnung nicht so trägt, wie sie geschrieben ist. Nicht zitiert."),
    "resolution_incomplete": ("nicht verifizierbar", "Zu viele exakte Treffer. Mit der Entscheid-ID zitieren."),
    "error": ("Prüfung fehlgeschlagen", "Dienst oder Netz haben für diesen Eintrag nicht geantwortet; Prüfung wiederholen."),
    "skipped": ("übersprungen", "Nach wiederholten Fehlern abgebrochen; Prüfung wiederholen."),
    "quote_near": ("Zitat weicht ab", "Das Zitat weicht vom gelieferten Wortlaut ab; die Abweichungen sind aufgeführt."),
    "quote_not_found": ("Zitat nicht gefunden", "Das Zitat steht weder in der zitierten Erwägung noch im verglichenen Entscheidtext."),
    "quote_unverifiable": ("Zitat nicht geprüft", "Der Entscheidtext ist in diesem Modus nicht verfügbar; am Entscheid prüfen."),
    "d_date": "Datum geschrieben {written}, Entscheid vom {decision}",
    "d_docket": "Geschäftsnummer {written} bezeichnet {resolves_to}",
    "d_parent": "E. {pinpoint} ist nicht als solche indexiert; stattdessen wurde E. {parent} abgerufen",
    "d_indexed": "indexiert: {list}",
    "d_candidate": "Kandidat {id} ({court}, {date})",
    "d_proposed": "Dienst schlug {id} vor, Bezeichnung nicht getragen",
    "d_diff": "geschrieben «{quote}», geliefert «{served}»",
    "d_closest": "nächster gelieferter Wortlaut: «{text}»",
    "d_quote_unchecked": "Zitat nicht geprüft: kein gelieferter Text",
    "d_quote_not_found": "Zitat im verglichenen Text nicht gefunden",
    "d_inferred": "gelesen als {name}, Bezeichnung {label}",
    "d_coverage": "{name}: {n} Entscheide {first}–{last} im Korpus; unpublizierte Entscheide und der angefochtene Entscheid fehlen erwartungsgemäss",
    "d_coverage_none": "{name}: in diesem Modus keine Abdeckungsangabe verfügbar",
    "verbatim": "Zitat wörtlich",
    "canton": "Kanton {canton}",
    "footer": "{exists} vorhanden ({passages} mit abgerufener Erwägung), {attention} zu prüfen, {unparsed} möglicherweise Zitate, nicht geprüft",
    "none_found": "keine Zitate im Dokument gefunden",
    "read_line": "{paragraphs} Absätze gelesen, {checked} Zitate gefunden",
    "report_line": "Bericht: {path}",
    "unparsed_line": "möglicherweise Zitate, nicht geprüft ({n}):",
}

_FR = {
    "title": "Vérification des citations: {source}",
    "scope": ("Existence, identité et libellé seulement: si chaque décision citée existe et porte la référence telle qu'elle est écrite, "
              "si le considérant cité existe, si la date et le numéro de dossier correspondent à la fiche, et si les citations figurent dans le texte servi. "
              "Non pas si une décision soutient l'argument ou fait encore jurisprudence."),
    "scope_short": "Existence, identité et libellé seulement; non pas si une décision soutient l'argument ou fait encore jurisprudence.",
    "headline": "{checked} références trouvées · {exists} existent ({passages} avec le considérant cité récupéré) · {attention} à examiner · {unparsed} citations possibles, non vérifiées",
    "checked_line": "Vérifié le {when} contre {base}, client {version}. Les références affichées sont celles du service; rien n'a été reformulé.",
    "h_attention": "À examiner", "h_exists": "Existent", "h_unparsed": "Citations possibles, non vérifiées",
    "unparsed_note": "Chaînes qui ressemblent à des numéros de dossier ou à des références de recueil mais n'ont pas été lues comme une citation; rien n'a été vérifié à leur sujet.",
    "c_reference": "Référence telle qu'écrite", "c_finding": "Constat", "c_advice": "Que faire", "c_detail": "Détail",
    "c_decision": "Décision (référence du service)", "c_where": "Où", "c_context": "Contexte",
    "resolved_pin": ("existe, considérant récupéré", ""),
    "resolved": ("existe", ""),
    "pinpoint_unavailable": ("considérant non indexé", "La décision existe; le considérant cité n'est pas dans l'index. Citer d'après le texte de la décision, pas de mémoire."),
    "discrepancy": ("indication erronée", "La décision existe, mais la date ou le numéro de dossier écrit à côté ne correspond pas à la fiche."),
    "missing": ("pas dans le corpus", "Vérifier la citation. Si la décision est non publiée ou est la décision attaquée, elle ne peut figurer dans aucun corpus."),
    "ambiguous": ("ambigu", "Plusieurs décisions portent cette référence. Nommer le tribunal ou citer par identifiant de décision."),
    "unrecognized": ("non vérifiable", "Le service a proposé une décision qui ne porte pas la référence telle qu'écrite. Non citée."),
    "resolution_incomplete": ("non vérifiable", "Trop de correspondances exactes pour trancher. Citer par identifiant de décision."),
    "error": ("vérification échouée", "Le service ou le réseau n'a pas répondu pour cet élément; relancer la vérification."),
    "skipped": ("ignoré", "Arrêt après des échecs répétés; relancer la vérification."),
    "quote_near": ("citation divergente", "La citation diffère du libellé servi; les différences sont listées."),
    "quote_not_found": ("citation introuvable", "La citation ne figure ni dans le considérant cité ni dans le texte de décision comparé."),
    "quote_unverifiable": ("citation non vérifiée", "Le texte de la décision n'est pas disponible dans ce mode; vérifier sur la décision."),
    "d_date": "date écrite {written}, décision du {decision}",
    "d_docket": "le numéro {written} désigne {resolves_to}",
    "d_parent": "le consid. {pinpoint} n'est pas indexé comme tel; le consid. {parent} a été récupéré à la place",
    "d_indexed": "indexés: {list}",
    "d_candidate": "candidat {id} ({court}, {date})",
    "d_proposed": "le service a proposé {id}, référence non portée",
    "d_diff": "écrit «{quote}», servi «{served}»",
    "d_closest": "libellé servi le plus proche: «{text}»",
    "d_quote_unchecked": "citation non vérifiée: aucun texte servi",
    "d_quote_not_found": "citation introuvable dans le texte comparé",
    "d_inferred": "lu comme {name}, référence {label}",
    "d_coverage": "{name}: {n} décisions {first}–{last} dans le corpus; les décisions non publiées et la décision attaquée en sont normalement absentes",
    "d_coverage_none": "{name}: aucune donnée de couverture disponible dans ce mode",
    "verbatim": "citation textuelle",
    "canton": "canton {canton}",
    "footer": "{exists} existent ({passages} avec le considérant cité récupéré), {attention} à examiner, {unparsed} citations possibles, non vérifiées",
    "none_found": "aucune référence trouvée dans le document",
    "read_line": "{paragraphs} paragraphes lus, {checked} références trouvées",
    "report_line": "rapport: {path}",
    "unparsed_line": "citations possibles, non vérifiées ({n}):",
}

_IT = {
    "title": "Verifica delle citazioni: {source}",
    "scope": ("Solo esistenza, identità e tenore: se ogni decisione citata esiste e porta il riferimento come è scritto, "
              "se il considerando citato esiste, se data e numero d'incarto corrispondono alla scheda e se le citazioni figurano nel testo servito. "
              "Non se una decisione sostiene l'argomento o è ancora giurisprudenza valida."),
    "scope_short": "Solo esistenza, identità e tenore; non se una decisione sostiene l'argomento o è ancora giurisprudenza valida.",
    "headline": "{checked} riferimenti trovati · {exists} esistono ({passages} con il considerando citato recuperato) · {attention} da esaminare · {unparsed} possibili citazioni, non verificate",
    "checked_line": "Verificato il {when} contro {base}, client {version}. I riferimenti mostrati sono quelli del servizio; nulla è stato riformulato.",
    "h_attention": "Da esaminare", "h_exists": "Esistono", "h_unparsed": "Possibili citazioni, non verificate",
    "unparsed_note": "Stringhe che sembrano numeri d'incarto o riferimenti a raccolte ma non sono state lette come citazione; su di esse non è stato verificato nulla.",
    "c_reference": "Riferimento come scritto", "c_finding": "Esito", "c_advice": "Cosa fare", "c_detail": "Dettaglio",
    "c_decision": "Decisione (riferimento del servizio)", "c_where": "Dove", "c_context": "Contesto",
    "resolved_pin": ("esiste, considerando recuperato", ""),
    "resolved": ("esiste", ""),
    "pinpoint_unavailable": ("considerando non indicizzato", "La decisione esiste; il considerando citato non è nell'indice. Citare dal testo della decisione, non a memoria."),
    "discrepancy": ("indicazione errata", "La decisione esiste, ma la data o il numero d'incarto scritto accanto non corrisponde alla scheda."),
    "missing": ("non nel corpus", "Verificare la citazione. Se la decisione non è pubblicata o è la decisione impugnata, non può trovarsi in alcun corpus."),
    "ambiguous": ("ambiguo", "Più decisioni portano questo riferimento. Indicare il tribunale o citare con l'identificativo della decisione."),
    "unrecognized": ("non verificabile", "Il servizio ha proposto una decisione che non porta il riferimento come scritto. Non citata."),
    "resolution_incomplete": ("non verificabile", "Troppe corrispondenze esatte per decidere. Citare con l'identificativo della decisione."),
    "error": ("verifica fallita", "Il servizio o la rete non hanno risposto per questa voce; ripetere la verifica."),
    "skipped": ("saltato", "Interrotto dopo errori ripetuti; ripetere la verifica."),
    "quote_near": ("citazione divergente", "La citazione differisce dal tenore servito; le differenze sono elencate."),
    "quote_not_found": ("citazione non trovata", "La citazione non figura nel considerando citato né nel testo della decisione confrontato."),
    "quote_unverifiable": ("citazione non verificata", "Il testo della decisione non è disponibile in questa modalità; verificare sulla decisione."),
    "d_date": "data scritta {written}, decisione del {decision}",
    "d_docket": "il numero {written} designa {resolves_to}",
    "d_parent": "il consid. {pinpoint} non è indicizzato come tale; è stato recuperato il consid. {parent}",
    "d_indexed": "indicizzati: {list}",
    "d_candidate": "candidato {id} ({court}, {date})",
    "d_proposed": "il servizio ha proposto {id}, riferimento non portato",
    "d_diff": "scritto «{quote}», servito «{served}»",
    "d_closest": "tenore servito più vicino: «{text}»",
    "d_quote_unchecked": "citazione non verificata: nessun testo servito",
    "d_quote_not_found": "citazione non trovata nel testo confrontato",
    "d_inferred": "letto come {name}, riferimento {label}",
    "d_coverage": "{name}: {n} decisioni {first}–{last} nel corpus; le decisioni non pubblicate e la decisione impugnata ne sono di regola assenti",
    "d_coverage_none": "{name}: nessun dato di copertura disponibile in questa modalità",
    "verbatim": "citazione testuale",
    "canton": "canton {canton}",
    "footer": "{exists} esistono ({passages} con il considerando citato recuperato), {attention} da esaminare, {unparsed} possibili citazioni, non verificate",
    "none_found": "nessun riferimento trovato nel documento",
    "read_line": "{paragraphs} paragrafi letti, {checked} riferimenti trovati",
    "report_line": "rapporto: {path}",
    "unparsed_line": "possibili citazioni, non verificate ({n}):",
}

STRINGS = {"en": _EN, "de": _DE, "fr": _FR, "it": _IT}


def language_of(value) -> str:
    """The report language for a --language value: de, fr, it; anything else is English."""
    code = str(value or "").strip().lower()[:2]
    return code if code in ("de", "fr", "it") else "en"


def t(language: str, key: str, **values) -> str:
    """A report string in the given language, English when the language has none."""
    table = STRINGS.get(language_of(language), _EN)
    text = table.get(key, _EN.get(key, key))
    if isinstance(text, tuple):
        text = text[0]
    return text.format(**values) if values else text


def fmt_int(language: str, n) -> str:
    """Thousands as the language writes them (12,345 / 12'345 / 12 345)."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    lang = language_of(language)
    if lang == "fr":
        return f"{n:,}".replace(",", " ")
    if lang in ("de", "it"):
        return f"{n:,}".replace(",", "'")
    return f"{n:,}"


def status_key(row: dict) -> str:
    """The row's status as the report reads it: a resolve status, or the quotation
    verdict on a resolved row, or resolved_pin when the cited passage was retrieved."""
    status = row.get("status", "error")
    quote = (row.get("quote_check") or {}).get("quote_status")
    if status == "resolved":
        if quote in ("near", "not_found", "unverifiable"):
            return "quote_" + quote
        if row.get("pinpoint_status") in ("retrieved", "parent_retrieved"):
            return "resolved_pin"
    return status


def is_ok(row: dict) -> bool:
    return status_key(row) in _OK


def is_bad(row: dict) -> bool:
    return status_key(row) in _BAD


def label(row: dict, language: str = "en") -> tuple[str, str]:
    """(finding, advice) for a row in the report language."""
    key = status_key(row)
    table = STRINGS.get(language_of(language), _EN)
    value = table.get(key) or _EN.get(key)
    if isinstance(value, tuple):
        return value
    return (key, "")


# Kept under their 0.7 names for callers that imported them.
def _label(row: dict) -> tuple[str, str]:
    return label(row, "en")


def _detail(row: dict) -> str:
    return detail(row, "en")


def court_name(coverage: dict | None, language: str = "en") -> str:
    """The court or collection a reference was read as, for a coverage line."""
    inferred = (coverage or {}).get("inferred") or {}
    word, canton = inferred.get("court_word"), inferred.get("canton")
    if (coverage or {}).get("canton_wide") and canton:
        # The figure covers the canton's collections, not the one court named.
        return t(language, "canton", canton=canton)
    if word and canton:
        return f"{word} {canton}"
    if word:
        return str(word)
    if canton:
        return t(language, "canton", canton=canton)
    return ", ".join(inferred.get("courts") or []) or "?"


def coverage_line(coverage: dict | None, language: str = "en") -> str:
    """The corpus's coverage for the inferred court, or that none is available."""
    if not coverage:
        return ""
    name = coverage.get("name") or court_name(coverage, language)
    if coverage.get("decisions") is not None:
        return t(language, "d_coverage", name=name, n=fmt_int(language, coverage["decisions"]),
                 first=coverage.get("first_year") or "?", last=coverage.get("last_year") or "?")
    return t(language, "d_coverage_none", name=name)


def detail(row: dict, language: str = "en") -> str:
    bits = []
    key = status_key(row)
    for d in row.get("discrepancies") or []:
        if d.get("kind") == "date":
            bits.append(t(language, "d_date", written=d.get("written"), decision=d.get("decision")))
        elif d.get("kind") == "docket":
            bits.append(t(language, "d_docket", written=d.get("written"), resolves_to=d.get("resolves_to")))
    if row.get("pinpoint_status") == "parent_retrieved":
        bits.append(t(language, "d_parent", pinpoint=row.get("pinpoint"), parent=(row.get("passage") or {}).get("e_number")))
    if row.get("available_e_numbers"):
        bits.append(t(language, "d_indexed", list=", ".join(map(str, row["available_e_numbers"][:12]))))
    for c in row.get("candidates") or []:
        bits.append(t(language, "d_candidate", id=c.get("decision_id"), court=c.get("court"), date=c.get("decision_date")))
    if row.get("service_candidate"):
        bits.append(t(language, "d_proposed", id=row["service_candidate"].get("decision_id")))
    if row.get("status") == "missing" and row.get("coverage"):
        coverage = row["coverage"]
        inferred = coverage.get("inferred") or {}
        word, canton = inferred.get("court_word"), inferred.get("canton")
        if coverage.get("canton_wide") and word and canton:
            # The figure is canton-wide; say which court the reference actually named.
            bits.append(t(language, "d_inferred", name=f"{word} {canton}", label=inferred.get("label") or row.get("reference")))
        bits.append(coverage_line(coverage, language))
    quote = row.get("quote_check") or {}
    qs = quote.get("quote_status")
    if qs == "near":
        for d in (quote.get("differences") or [])[:3]:
            bits.append(t(language, "d_diff", quote=d.get("quote"), served=d.get("served")))
    elif qs == "not_found":
        if quote.get("served"):
            bits.append(t(language, "d_closest", text=" ".join(str(quote["served"]).split())[:240]))
        elif key != "quote_not_found":
            bits.append(t(language, "d_quote_not_found"))
    elif qs == "unverifiable" and key != "quote_unverifiable":
        bits.append(t(language, "d_quote_unchecked"))
    if row.get("error") and row.get("status") == "error":
        bits.append(str(row["error"].get("message")))
    return "; ".join(b for b in bits if b)


def _citation(row: dict, language: str = "en") -> str:
    provenance = row.get("provenance") or {}
    lang = language_of(language)
    return (provenance.get(f"citation_string_{lang}") if lang != "en" else None) or provenance.get("citation_string_de") \
        or provenance.get("citation_string") or ""


def summarize(result: dict, source: str, language: str = "en") -> dict:
    rows = result.get("results") or []
    ok = [r for r in rows if is_ok(r)]
    return {"source": source, "checked": len(rows), "exists": len(ok),
            "passages_retrieved": sum(1 for r in ok if status_key(r) == "resolved_pin"),
            "attention": len(rows) - len(ok), "unparsed": len(result.get("unparsed") or []),
            "language": language_of(language),
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "client_version": __version__,
            "base_url": result.get("base_url"), "requests": result.get("requests"),
            **summarize_statutes(result.get("statutes") or [])}


def headline(summary: dict, language: str = "en") -> str:
    return t(language, "headline", checked=summary.get("checked", 0), exists=summary.get("exists", 0),
             passages=summary.get("passages_retrieved", 0), attention=summary.get("attention", 0), unparsed=summary.get("unparsed", 0))


def _finding_ok(row: dict, language: str) -> str:
    """The finding for a row that exists. The passage number is stated here, next to the
    finding, never appended to the service's citation string (the client composes none)."""
    text = label(row, language)[0]
    if status_key(row) == "resolved_pin":
        number = (row.get("passage") or {}).get("e_number") or row.get("pinpoint")
        text += f" (E. {number})"
    if (row.get("quote_check") or {}).get("quote_status") == "exact":
        text += " · " + t(language, "verbatim")
    return text


def render_markdown(result: dict, source: str, found: list[dict], language: str = "en") -> str:
    rows = result.get("results") or []
    unparsed = result.get("unparsed") or []
    summary = summarize(result, source, language)
    lines = [f"# {t(language, 'title', source=source)}", "",
             t(language, "scope"), "",
             headline(summary, language) + ". " + t(language, "checked_line", when=summary["generated_at"], base=summary["base_url"], version=__version__), ""]
    attention = [r for r in rows if not is_ok(r)]
    if attention:
        lines += [f"## {t(language, 'h_attention')}", "",
                  f"| {t(language, 'c_reference')} | {t(language, 'c_finding')} | {t(language, 'c_advice')} | {t(language, 'c_detail')} |", "|---|---|---|---|"]
        for r in attention:
            finding, advice = label(r, language)
            lines.append(f"| {r.get('reference')} | {finding} | {advice} | {detail(r, language)} |")
        lines.append("")
    ok = [r for r in rows if is_ok(r)]
    if ok:
        lines += [f"## {t(language, 'h_exists')}", "",
                  f"| {t(language, 'c_reference')} | {t(language, 'c_decision')} | {t(language, 'c_finding')} |", "|---|---|---|"]
        for r in ok:
            lines.append(f"| {r.get('reference')} | {_citation(r, language)} | {_finding_ok(r, language)} |")
        lines.append("")
    if unparsed:
        lines += [f"## {t(language, 'h_unparsed')}", "", t(language, "unparsed_note"), ""]
        for u in unparsed:
            lines.append(f"- {u.get('text')} (§{u.get('paragraph')}): {u.get('context')}")
        lines.append("")
    lines += statutes_markdown(result.get("statutes") or [])
    return "\n".join(lines)


def render_html(result: dict, source: str, found: list[dict], language: str = "en") -> str:
    rows = result.get("results") or []
    unparsed = result.get("unparsed") or []
    summary = summarize(result, source, language)
    e = html.escape
    attention = [r for r in rows if not is_ok(r)]
    ok = [r for r in rows if is_ok(r)]
    title = t(language, "title", source=source)
    parts = [f"<!doctype html><html lang=\"{language_of(language)}\"><head><meta charset=\"utf-8\"><title>{e(title)}</title>",
             "<style>body{font:15px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;max-width:1000px;margin:2rem auto;padding:0 1rem;color:#222}"
             "h1{font-size:1.4rem}h2{font-size:1.1rem;margin-top:2rem}table{border-collapse:collapse;width:100%}th,td{border-top:1px solid #ddd;padding:.5rem;vertical-align:top;text-align:left}"
             ".ok{color:#1a7f37}.warn{color:#9a6700}.bad{color:#b42318}.muted{color:#666;font-size:.9rem}.scope{border-left:3px solid #999;padding:.4rem .8rem;background:#f6f6f6}"
             "code{font-family:ui-monospace,Menlo,monospace;font-size:.9em}</style></head><body>",
             f"<h1>{e(title)}</h1>",
             f"<p class=\"scope\">{e(t(language, 'scope'))}</p>",
             f"<p class=\"{'bad' if summary['attention'] else 'ok'}\"><strong>{e(headline(summary, language))}</strong></p>",
             f"<p class=\"muted\">{e(t(language, 'checked_line', when=summary['generated_at'], base=str(summary['base_url']), version=__version__))}</p>"]
    if attention:
        parts.append(f"<h2>{e(t(language, 'h_attention'))}</h2><table><tr><th>{e(t(language, 'c_reference'))}</th><th>{e(t(language, 'c_finding'))}</th>"
                     f"<th>{e(t(language, 'c_advice'))}</th><th>{e(t(language, 'c_detail'))}</th></tr>")
        for r in attention:
            finding, advice = label(r, language)
            cls = "bad" if is_bad(r) else "warn"
            parts.append(f"<tr><td><code>{e(str(r.get('reference')))}</code>" + (f"<br><span class=\"muted\">§{r.get('input', {}).get('paragraph', '')}</span>" if (r.get('input') or {}).get('paragraph') else "") +
                         f"</td><td class=\"{cls}\">{e(finding)}</td><td>{e(advice)}</td><td class=\"muted\">{e(detail(r, language))}</td></tr>")
        parts.append("</table>")
    if ok:
        parts.append(f"<h2>{e(t(language, 'h_exists'))}</h2><table><tr><th>{e(t(language, 'c_reference'))}</th><th>{e(t(language, 'c_decision'))}</th><th>{e(t(language, 'c_finding'))}</th></tr>")
        for r in ok:
            parts.append(f"<tr><td><code>{e(str(r.get('reference')))}</code></td><td>{e(_citation(r, language))}</td><td class=\"ok\">{e(_finding_ok(r, language))}</td></tr>")
        parts.append("</table>")
    if unparsed:
        parts.append(f"<h2>{e(t(language, 'h_unparsed'))}</h2><p class=\"muted\">{e(t(language, 'unparsed_note'))}</p>"
                     f"<table><tr><th>{e(t(language, 'c_reference'))}</th><th>{e(t(language, 'c_where'))}</th><th>{e(t(language, 'c_context'))}</th></tr>")
        for u in unparsed:
            parts.append(f"<tr><td><code>{e(str(u.get('text')))}</code></td><td class=\"muted\">§{e(str(u.get('paragraph')))}</td><td class=\"muted\">{e(str(u.get('context') or ''))}</td></tr>")
        parts.append("</table>")
    parts += statutes_html(result.get("statutes") or [], e)
    parts.append("</body></html>")
    return "\n".join(parts)
