# entscheidsuche.ch — bug report (docket-number newlines)

**To**: [entscheidsuche.ch maintainers — info@entscheidsuche.ch /
GitHub issue if applicable]
**Subject**: Datenqualität: Newlines + Tab-Zeichen in `docket_number`-Feld
in vd_findinfo + ch_vb Exporten

Sehr geehrte Damen und Herren

OpenCaseLaw (https://opencaselaw.ch) bezieht einen wichtigen Teil der
Schweizer Gerichtsentscheide aus Ihrem entscheidsuche.ch-Datenstrom.
Vielen Dank für die kontinuierliche Pflege dieser Quelle — sie ist
für unser Projekt zentral.

Wir möchten zwei Datenqualitätsprobleme melden, die in zwei Subsets
Ihres Exports stabil reproduzierbar sind und die wir aktuell nur
nachträglich beheben:

## Problem

In den Exporten der folgenden zwei Quellen enthalten 5'441 Zeilen ein
Newline- (LF) oder Tab-Zeichen mitten im `docket_number`-Feld:

| Quelle | Anzahl betroffene Zeilen | Beispiel |
|---|---:|---|
| `es_vd_findinfo.jsonl` | 2'705 | `'1\n'`, `'10\n'`, `'11\n'` |
| `es_ch_vb.jsonl` | 2'735 | (ähnliches Muster) |

Das Muster lässt vermuten, dass beim Scraping zwei aneinandergrenzende
Tabellenzellen aus dem Quellportal mit einem Zeilenumbruch verkettet
werden — das Aktenzeichen wird dann fälschlicherweise nur als
Zeilennummer („1“, „10“, …) übernommen und der Rest des
Tabelleninhalts an das `docket_number`-Feld angehängt.

## Auswirkungen

Auf der Konsumentenseite verhindert das Newline:

* Exact-Match-Queries (`docket_number = '1A.122/2005'` matcht nicht,
  wenn das gespeicherte Feld `'1\n'` ist).
* URL-Routen wie `/entscheid/{decision_id}` brechen, wenn das Newline
  in den abgeleiteten Decision-ID eingeht.
* Citation-Resolver verlieren legitime Cross-References, weil das
  normalisierte Aktenzeichen nicht zurück auf eine echte Form führt.

OpenCaseLaw normalisiert seit dem 2026-05-01 die Felder im
Build-Prozess (`build_fts5._normalize_dockets`) — damit ist das
Problem für unsere Endnutzer gelöst, aber jeder andere
Nachnutzungs-Konsument Ihres Datenstroms erlebt es erneut.

## Bitte

Wäre es möglich, im Export-Pipeline der zwei betroffenen Quellen
(`vd_findinfo` und `ch_vb`) ein einfaches `docket_number =
docket_number.strip().replace("\n", " ").replace("\t", " ")` zu
ergänzen? Das würde die Datenqualität für alle Nachnutzer
upstream beheben.

Sehr gerne unterstützen wir mit einem reproduzierbaren Auszug aus
unserem Quality-Audit (61 codifizierte Checks, davon
`dockets.internal_newlines` für genau dieses Problem) — siehe
opencaselaw.ch/quality.html.

Mit freundlichen Grüssen
Jonas Hertner — OpenCaseLaw
jonashertner@protonmail.ch
