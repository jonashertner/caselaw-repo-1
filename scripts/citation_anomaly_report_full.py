"""Render the full analytical report over a back-scan result set.

Input:  findings.json produced by scripts/citation_anomaly_backscan.py
        (optionally enriched with a verified-targets map)
Output: one Markdown report — figures computed, never transcribed.

Confidential by construction: it names decisions of named courts as
carrying defective citations. Keep it in the private channel.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

TOKEN = re.compile(r"^BGE\s+(\d{1,3})\s+([IVXa-z]+)\s+(\d{1,4})$")
FEDERAL = {"bger", "bge", "bvger", "bstger", "bpatger", "weko", "ch_vb"}

REASON_DE = {
    "volume_out_of_range": "Bandnummer existiert nicht",
    "division_absent_for_volume": "Abteilung existiert in diesem Band nicht",
    "page_beyond_series": "Seitenzahl liegt ausserhalb des Bandes",
    "page_looks_like_year": "Seitenzahl sieht aus wie eine Jahreszahl",
}

COURT_DE = {
    "bger": "Bundesgericht", "bge": "Bundesgericht (amtliche Sammlung)",
    "bvger": "Bundesverwaltungsgericht", "bstger": "Bundesstrafgericht",
    "weko": "Wettbewerbskommission",
    "zh_obergericht": "Obergericht Zürich",
    "zh_handelsgericht": "Handelsgericht Zürich",
    "zh_verwaltungsgericht": "Verwaltungsgericht Zürich",
    "zh_sozialversicherungsgericht": "Sozialversicherungsgericht Zürich",
    "zh_baurekursgericht": "Baurekursgericht Zürich",
    "so_gerichte": "Gerichte Solothurn",
    "gr_gerichte": "Obergericht Graubünden",
    "bl_gerichte": "Gerichte Basel-Landschaft",
    "bs_appellationsgericht": "Appellationsgericht Basel-Stadt",
    "bs_sozialversicherungsgericht": "Sozialversicherungsgericht Basel-Stadt",
    "be_zivilstraf": "Obergericht Bern (Zivil/Straf)",
    "be_verwaltungsgericht": "Verwaltungsgericht Bern",
    "ag_zivilgericht": "Obergericht Aargau (Zivil)",
    "ag_strafgericht": "Obergericht Aargau (Straf)",
    "ag_versicherungsgericht": "Versicherungsgericht Aargau",
    "ag_spezialverwaltungsgericht": "Spezialverwaltungsgericht Aargau",
    "ag_gerichte": "Gerichte Aargau",
    "sg_kantonsgericht": "Kantonsgericht St. Gallen",
    "sg_verwaltungsgericht": "Verwaltungsgericht St. Gallen",
    "sg_versicherungsgericht": "Versicherungsgericht St. Gallen",
    "zg_obergericht": "Obergericht Zug",
    "zg_verwaltungsgericht": "Verwaltungsgericht Zug",
    "fr_gerichte": "Kantonsgericht Freiburg",
    "lu_gerichte": "Kantonsgericht Luzern",
    "tg_gerichte": "Obergericht Thurgau",
    "nw_gerichte": "Gerichte Nidwalden",
    "ar_gerichte": "Obergericht Appenzell Ausserrhoden",
    "sz_gerichte": "Kantonsgericht Schwyz",
}


def court_name(code: str) -> str:
    if code in COURT_DE:
        return COURT_DE[code]
    if code.startswith("zh_bezirksgericht_"):
        return "Bezirksgericht " + code.split("_")[-1].capitalize()
    return code


def error_class(h: dict) -> str:
    """Human taxonomy of the mistake, from token shape + reason."""
    m = TOKEN.match(h["token"])
    if not m:
        return "unparsbar"
    vol, div, page = int(m.group(1)), m.group(2).upper(), int(m.group(3))
    if div not in ("I", "IA", "IB", "II", "III", "IV", "V"):
        return "Abteilungsziffer verschrieben (z. B. «IIII», «IIV»)"
    if 1875 <= page <= 2100 and page > 700:
        return "Jahreszahl in der Seitenangabe"
    if page >= 1000:
        return "zusätzliche Ziffer in der Seitenzahl"
    if "volume_out_of_range" in h["reason"]:
        return "Bandnummer existiert nicht"
    if "division_absent_for_volume" in h["reason"]:
        return "Abteilung im betreffenden Band nicht vorhanden"
    return "Seitenzahl ausserhalb des Bandes"


def render(hits: list[dict], since: str, verified: dict) -> str:
    n = len(hits)
    decisions = {h["decision_id"] for h in hits}
    courts = defaultdict(list)
    for h in hits:
        courts[h.get("court", "unbekannt")].append(h)
    tiers = Counter(h.get("tier", "?")[0] for h in hits)
    classes = Counter(error_class(h) for h in hits)
    years = Counter((h.get("date") or "?")[:4] for h in hits)
    tokens = Counter(h["token"] for h in hits)
    fed = sum(len(v) for k, v in courts.items() if k in FEDERAL)

    L: list[str] = []
    A = L.append
    A("# Fehlerhafte BGE-Zitate in der schweizerischen Rechtsprechung")
    A("")
    A(f"Auswertung aller Entscheide im Korpus von opencaselaw.ch, "
      f"ergangen seit dem {since}.")
    A("")
    A("**Vertraulich.** Der Bericht benennt einzelne Entscheide einzelner "
      "Gerichte. Er ist als Arbeitsgrundlage für die Kontaktaufnahme mit den "
      "Gerichten gedacht, nicht zur Veröffentlichung.")
    A("")
    A("## 1. Das Wichtigste in Kürze")
    A("")
    A(f"- **{n} fehlerhafte BGE-Zitate** in **{len(decisions)} Entscheiden** "
      f"von **{len(courts)} Gerichten**.")
    A(f"- Davon **{fed}** in Entscheiden eidgenössischer Gerichte, "
      f"**{n - fed}** in kantonalen Entscheiden.")
    A(f"- **{tiers.get('A', 0)}** Befunde sind vollständig verifiziert und "
      "mit einem begründeten Vorschlag versehen, welche Fundstelle gemeint "
      f"war; bei **{tiers.get('B', 0)}** ist die Nichtexistenz belegt, ohne "
      f"dass wir eine Zuordnung wagen; **{tiers.get('C', 0)}** sind vor einer "
      "Kontaktaufnahme zurückzustellen (siehe Ziffer 6).")
    A("- Der wichtigste Befund ist nicht die Zahl, sondern ein Muster: "
      "**dieselben falschen Fundstellen erscheinen in mehreren Entscheiden "
      "verschiedener Gerichte**. Fehlzitate werden offenkundig aus Entscheid "
      "zu Entscheid übernommen (Ziffer 4).")
    A("")

    A("## 2. Wie die Befunde zustande kommen")
    A("")
    A("Die amtliche Sammlung ist in unserem Korpus seit 1875 vollständig. "
      "Daraus lässt sich ein geschlossenes Verzeichnis bilden: für jeden Band "
      "und jede Abteilung sind sämtliche Anfangsseiten bekannt. Gegen dieses "
      "Verzeichnis ist die Nichtexistenz einer zitierten Fundstelle "
      "*beweisbar* und nicht bloss «bei uns nicht gefunden». Gemeldet wird "
      "nur, was nachweislich unmöglich ist:")
    A("")
    A("- die Bandnummer gibt es nicht (mit Nachsicht für den Band des "
      "Folgejahres, den ein Gericht zitieren kann, bevor er bei uns erfasst ist);")
    A("- die Abteilung gab es in diesem Band nicht (Band 13 von 1887 war "
      "noch nicht in Abteilungen gegliedert; die Abteilung III entstand erst "
      "mit Band 40);")
    A("- die Seite liegt hinter dem letzten Fall des Bandes, wobei dessen "
      "unbekannte Länge grosszügig veranschlagt wird (mindestens 30 Seiten, "
      "sonst so lang wie der längste beobachtete Fall dieses Bandes).")
    A("")
    A("Bewusst **nicht** gemeldet wird alles Übrige. Insbesondere gilt eine "
      "Seite mitten im Band immer als zulässiges Tiefenzitat: die Bände sind "
      "durchgehend paginiert, jede Seite zwischen zwei Fallanfängen gehört "
      "zum vorangehenden Fall. Ebenso bleiben die beiden jüngsten Bände "
      "aussen vor, weil sie sich noch füllen.")
    A("")

    A("## 3. Verifikation")
    A("")
    A("Jeder Befund durchlief drei voneinander unabhängige Prüfungen:")
    A("")
    A("1. **Unser Verzeichnis** der vollständigen BGE-Serie (rund 50'000 "
      "Einträge, drei Docket-Schreibweisen, Abteilungen gross- und "
      "kleingeschrieben).")
    A("2. **Die Entscheiddatenbank des Bundesgerichts.** Für jede mehrfach "
      "auftretende Fundstelle wurde direkt geprüft, ob sie dort auflösbar "
      "ist. Ergebnis: sämtliche beanstandeten Fundstellen sind dort nicht "
      "vorhanden, die Kontrollzitate hingegen schon. Geprüft wurde zusätzlich "
      "in französischer und italienischer Fassung, damit "
      "nicht ein bloss anderssprachig publizierter Entscheid als fehlend "
      "erscheint.")
    A("3. **Der Wortlaut des zitierenden Entscheids.** Nur wo die "
      "Fundstelle wörtlich im Entscheidtext steht, ist ausgeschlossen, dass "
      "der Fehler aus unserer eigenen maschinellen Erfassung stammt. Wo das "
      "nicht zutrifft, ist der Befund zurückgestellt (Ziffer 6).")
    A("")
    A("Eine Stichprobe an der Bandgrenze bestätigte die Deckungsgleichheit "
      "beider Verzeichnisse: unser Verzeichnis lässt Band 150 Abteilung I "
      "auf Seite 213 enden, und die Datenbank des Bundesgerichts löst "
      "Seite 213 auf, Seite 250 dagegen nicht.")
    A("")

    A("## 4. Wiederkehrende Fehlzitate")
    A("")
    A("Diese Fundstellen sind mehrfach zitiert worden. Wo mehrere Gerichte "
      "betroffen sind, spricht das für eine Übernahme aus einem früheren "
      "Entscheid oder aus einer gemeinsamen Quelle.")
    A("")
    A("| Falsche Fundstelle | Gemeint ist wohl | Anzahl | Gerichte |")
    A("|---|---|---:|---|")
    for t, cnt in tokens.most_common():
        if cnt < 2:
            continue
        v = verified.get(t) or {}
        tgt = v.get("target") or "–"
        cs = sorted({court_name(h.get("court", "?")) for h in hits
                     if h["token"] == t})
        A(f"| {t} | {tgt} | {cnt} | {', '.join(cs)} |")
    A("")
    A("Für die verifizierten Fälle stützt sich der Zuordnungsvorschlag "
      "jeweils auf den Zitatzusammenhang und wurde gegen die "
      "Bundesgerichtsdatenbank geprüft. Beispiele:")
    A("")
    for t in ("BGE 137 V 2010", "BGE 140 IV 373", "BGE 123 III 626"):
        v = verified.get(t)
        if not v:
            continue
        A(f"- **{t} → {v['target']}**: {v['evidence']}.")
    A("")

    A("## 5. Fehlerarten")
    A("")
    A("| Art des Fehlers | Anzahl |")
    A("|---|---:|")
    for k, c in classes.most_common():
        A(f"| {k} | {c} |")
    A("")
    top2 = classes.most_common(2)
    A(f"Am häufigsten ist «{top2[0][0]}» ({top2[0][1]} Befunde), gefolgt von "
      f"«{top2[1][0]}» ({top2[1][1]}).")
    A("")
    A("Die Einteilung beschreibt die *Form* des Fehlers, nicht seine "
      "Ursache. Ein grosser Teil der Fälle «Seitenzahl ausserhalb des "
      "Bandes» erweist sich bei näherer Betrachtung als vertauschte "
      "Abteilung: die zitierte Seite existiert im selben Band sehr wohl, "
      "nur in einer anderen Abteilung, und der Zitatzusammenhang weist "
      "eindeutig dorthin (etwa «BGE 142 II 612» zu Art. 298 ZGB, wo die "
      "zivilrechtliche Abteilung III gemeint ist). Die zweitgrösste Gruppe "
      "entsteht, wenn eine Jahreszahl an die Stelle der Seitenzahl tritt "
      "(«BGE 137 V 2010» statt «BGE 137 V 210»).")
    A("")

    A("## 6. Einteilung für die Kontaktaufnahme")
    A("")
    A("| Stufe | Bedeutung | Anzahl | Empfehlung |")
    A("|---|---|---:|---|")
    A(f"| A | Nichtexistenz verifiziert, Zielfundstelle begründet "
      f"vorgeschlagen | {tiers.get('A', 0)} | versandbereit |")
    A(f"| B | Nichtexistenz verifiziert, keine Zuordnung | "
      f"{tiers.get('B', 0)} | versandbereit als reiner Hinweis |")
    A(f"| C | Fundstelle nicht wörtlich im Entscheidtext | "
      f"{tiers.get('C', 0)} | zurückstellen: eigener Erfassungsfehler "
      "möglich |")
    A("")

    A("## 7. Verteilung")
    A("")
    A("### Nach Gericht")
    A("")
    A("| Gericht | Befunde | A | B | C |")
    A("|---|---:|---:|---:|---:|")
    for code, rows in sorted(courts.items(), key=lambda kv: -len(kv[1])):
        t = Counter(r.get("tier", "?")[0] for r in rows)
        A(f"| {court_name(code)} | {len(rows)} | {t.get('A', 0)} | "
          f"{t.get('B', 0)} | {t.get('C', 0)} |")
    A("")
    A("### Nach Entscheidjahr")
    A("")
    A("| Jahr | Befunde |")
    A("|---|---:|")
    for y, c in sorted(years.items()):
        A(f"| {y} | {c} |")
    A("")

    A("## 8. Grenzen")
    A("")
    A("- Der Bericht erfasst **nur Zitate der amtlichen Sammlung**. Zitate "
      "unpublizierter Bundesgerichtsurteile, kantonaler Entscheide oder der "
      "Literatur werden nicht geprüft, weil dort kein geschlossenes "
      "Verzeichnis existiert.")
    A("- Ob ein Fehler dem Gericht, einer Rechtsschrift, einer "
      "Kommentarstelle oder der Texterfassung des Publikationsportals "
      "entstammt, lässt sich von aussen nicht entscheiden. Der "
      "Kontextauszug erlaubt dem Gericht diese Beurteilung.")
    A("- Die Zuordnungsvorschläge der Stufe A sind begründete Vermutungen, "
      "keine Feststellungen.")
    A("- Die Auswertung sagt nichts über die Häufigkeit im Verhältnis zur "
      "Gesamtzahl der Zitate; sie ist eine Fehlerliste, keine Fehlerquote.")
    A("")

    A("## 9. Anhang: sämtliche Befunde")
    A("")
    for code, rows in sorted(courts.items(), key=lambda kv: -len(kv[1])):
        A(f"### {court_name(code)} (`{code}`) — {len(rows)}")
        A("")
        A("| Entscheid | Datum | Zitiert | Stufe | Bemerkung |")
        A("|---|---|---|---|---|")
        for h in sorted(rows, key=lambda x: (x.get("tier", ""),
                                             x.get("date") or "")):
            v = verified.get(h["token"]) or {}
            note = (f"gemeint wohl {v['target']}" if v.get("target")
                    else REASON_DE.get(h["reason"].split()[0], h["reason"]))
            A(f"| {h.get('docket', h['decision_id'])} | "
              f"{h.get('date', '?')} | {h['token']} | "
              f"{h.get('tier', '?')[0]} | {note} |")
        A("")
    return "\n".join(L) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--findings", required=True)
    ap.add_argument("--verified", default=None)
    ap.add_argument("--since", default="1. Januar 2024")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    hits = json.loads(Path(a.findings).read_text())
    verified = json.loads(Path(a.verified).read_text()) if a.verified else {}
    Path(a.out).write_text(render(hits, a.since, verified))
    print(f"report written: {a.out} ({len(hits)} findings)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
