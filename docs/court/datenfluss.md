# Datenfluss

Was das Gerät verlässt, in jedem Betriebsmodus. Stand 6. September 2026; prüfbar am offenen Quellcode (`clients/python/src/opencaselaw_cli/`).

| Modus | Was das Gerät verlässt | Für Gerichte |
|---|---|---|
| Lokal (`--local`, Standard des Installers) | Nichts. Die Eingabe oder der Entwurf, die Zitate und der Bericht bleiben auf dem Gerät. Einzige Netzverbindung: der Download des Prüfpakets (einmal pro Woche, vom Spiegel des Gerichts oder von opencaselaw.ch), mit Prüfsumme. | ja |
| Gehostet (`ocl check` ohne `--local`) | Die erkannten Aktenzeichen und BGE-Nummern als Anfragen an mcp.opencaselaw.ch; nie der Text der Eingabe oder des Entwurfs, nie die Zitate. Der Server protokolliert Anfragen mit IP-Adresse. | nein |
| Word-Add-in (word.opencaselaw.ch) | Der markierte Text, nach clientseitiger Schwärzung von Namen, AHV- und IBAN-Nummern. | nein |

Im Lokalmodus wurde mit einem Netz-Sperrtest (jede Socket-Verbindung löst einen Fehler aus) nachgewiesen, dass eine vollständige Prüfung ohne einen einzigen Verbindungsversuch durchläuft.

## Was das Prüfpaket enthält

Metadaten und Zitierstrings aller Entscheide, die Erwägungen der erschlossenen Entscheide (komprimiert), die Gesetzesartikel. Keine Volltexte, keine Suche. Das Paket ist öffentlich und enthält keine Daten des Gerichts.

## Eingaben der Parteien

Eingaben enthalten Personendaten der Parteien, auch besonders schützenswerte. Im Lokalmodus liest das Programm die PDF-Eingabe (PDF/A aus justitia.swiss; gescannte Eingaben nach einer Texterkennung) auf dem Gerät des Gerichts, schreibt den Bericht daneben und legt keine weiteren Kopien an. Die Parteidaten werden damit nur dort bearbeitet, wo die Akte ohnehin liegt; kein Dritter erhält sie, und für die Prüfung braucht es keine Rechtsgrundlage für eine Bekanntgabe. Der gehostete Modus und das Word-Add-in sind für Eingaben nicht vorgesehen.

## Was der Bericht enthält

Die Zitierungen wie in der Eingabe oder im Entwurf geschrieben, die zugehörigen Zitate und Auszüge des Entscheidtexts. Der Bericht ist damit so vertraulich wie das geprüfte Dokument und liegt neben ihm (`Eingabe.check.html`, `Entwurf.check.html`; bei einem Ordner zusätzlich eine Übersicht). Er wird nirgends hochgeladen.

## Protokolle

Lokal keine. Das Programm meldet keine Nutzung, prüft keine Versionen und sendet keine Fehlerberichte.
