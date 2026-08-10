# Entwurf: Antwort an BGPartner (Copilot-Evaluation) — Fassung 2

Betreff: Re: Anbindung von OpenCaseLaw an einen Copilot-Agenten

---

Sehr geehrte Kolleginnen und Kollegen

Besten Dank für Ihre Rückmeldung. Sie hat fünf Änderungen ausgelöst, die seit gestern produktiv sind - bei zweien Ihrer Beobachtungen lag der Fehler bei uns in einer Form, die Sie von aussen gar nicht als solche erkennen konnten.

Zwei echte Mängel haben Sie getroffen, und beide liessen sich exakt reproduzieren. Eine Suche mit Gesetzeszitat ("missbräuchliche Kündigung Art. 336 OR") brauchte über die REST-Schnittstelle 43.9 Sekunden, bei identischer Anfrage heute 5.6. Ursache war die automatische Erwägungs-Verortung: Abkürzungen wie "Art." und "OR" liessen sie den gesamten Index abschreiten, also ausgerechnet bei der Anfrageform, die in der anwaltlichen Praxis am häufigsten vorkommt. Und Ihr Kündigungsschreiben-Szenario lief bis vorgestern nach 120 Sekunden in einen Timeout, ohne Fehlermeldung; das war der Abbruch, den Sie gesehen haben. Neu wird langer Text automatisch auf Zitate und Kernbegriffe verdichtet und normal durchsucht - gemessen 8.5 statt 120 Sekunden -, und ab 4'000 Zeichen kommt eine verständliche Fehlermeldung statt eines Hängers. Über die ganze Flotte liegen die Suchen seither bei p95 von 4.0 Sekunden. Der Median lag schon vorher bei rund 1.6 Sekunden und hat sich nicht verändert; Ihr Befund betraf die Ausreisser, und dort liegt die Verbesserung.

Zwei weitere Punkte betrafen Bestände, die es längst gibt, die Sie aber nicht finden konnten. Zur Verlinkung: Entscheidtreffer liefern ihre Links seit jeher als Markdown mit, Copilot entfernt aber nackte URLs aus Antworten, was den Eindruck erklärt. Wirklich gefehlt hat die Verlinkung bei den Gesetzen, `get_law` gab überhaupt keine Quelle zurück. Jede Gesetzesantwort trägt jetzt einen Fedlex-Link mit Artikelanker (kantonal: LexFind), und die übrigen Werkzeuge liefern gerenderte Links in der Form, die Copilots Filter übersteht. Zu den Wegleitungen und Kreisschreiben: Der Bestand ist da - 790 Dokumente eidgenössischer Verwaltungspraxis (ESTV 438, DE/FR/IT; BAFU 297; SEM 55), dazu 23'000 VPB/JAAC-Entscheide bis 2016. Ihr Agent konnte ihn nicht finden, weil unsere eigene Werkzeug-Beschreibung ihn nirgends erwähnte. Das war unser Fehler und ist behoben; das Werkzeug heisst `search_practice`.

Der Punkt zur Struktur liegt bei Ihnen, und zwar handfest: Copilot übergibt dem Modell nach der publizierten Dokumentation nur Name, Beschreibung und Eingabeschema der Werkzeuge, nicht aber die serverseitigen Instruktionen. Tabellen, Vergleiche und ein Vorgehensvorschlag müssen deshalb in Ihrer Agent-Anweisung stehen. Bewusst restriktiv bleibt bei uns einzig das Zitierregime: Fundstellen und wörtliche Zitate stammen aus den Werkzeugen, das Modell darf sie nicht selbst bilden. Bei einem Instrument, dessen Ergebnisse in Rechtsschriften landen, scheint mir das der richtige Tausch.

Für die Anbindung konkret, in absteigender Wirkung:

(1) Beschränken Sie den Agenten auf 10 bis 25 Werkzeuge und nehmen Sie `search_practice` ausdrücklich hinein. Microsoft empfiehlt höchstens 25 bis 30; darüber leidet die Werkzeugwahl messbar.
(2) Übernehmen Sie den untenstehenden Block in Ihre Agent-Anweisung.
(3) Setzen Sie bei der Ersttreffersuche `include_pinpoint=false` und rufen Sie die Erwägungs-Verortung erst für die Entscheide ab, die Sie tatsächlich zitieren.
(4) Rechnen Sie damit, dass Copilot Werkzeugantworten über 500 KB abbricht und bei rund 100 bis 120 Sekunden ein Timeout setzt, ohne Zwischenstand anzuzeigen.

Baustein für das Feld "Anweisungen":

> Zitate und Fundstellen übernimmst Du wörtlich aus den Feldern citation_string_* und markdown_link der Werkzeugantworten; Du bildest nie eigene Zitate und setzt Anführungszeichen nur um Text, der wörtlich aus einem Werkzeug stammt. Jede zitierte Quelle verlinkst Du mit ihrem markdown_link. Dokument- und Briefinhalte gibst Du nie als Suchtext weiter: Du benennst zuerst die Rechtsfragen, dann suchst Du pro Frage einmal mit drei bis acht präzisen Begriffen. Für Kreisschreiben, Weisungen und Wegleitungen verwendest Du search_practice. Vergleiche stellst Du tabellarisch dar, und jede Antwort schliesst mit einem kurzen Vorgehensvorschlag. Du verweist nie auf kommerzielle Datenbanken; weiterführende Recherche formulierst Du als konkrete Suche in OpenCaseLaw oder in freien amtlichen Quellen (Fedlex, LexFind, entscheidsuche.ch).

Zu den Grenzen, damit Sie richtig disponieren: Bei der Verwaltungspraxis fehlen BSV/AHV-IV, SECO, FINMA-Rundschreiben, BAG und sämtliche kantonalen Verwaltungen; SEM und BAFU liegen nur auf Deutsch vor. Der Ausbau ist geplant, ein Datum verspreche ich nicht - das Projekt wird von einer Person betrieben, ohne SLA.

Drei Fragen an Sie, damit wir gezielt weiterkommen: Nutzen Sie die MCP-Anbindung oder den REST-Konnektor von Copilot Studio? Welches Timeout ist in Ihrem Konnektor eingestellt? Und könnten Sie mir drei konkrete fehlgeschlagene Anfragen mit Zeitstempel nennen - serverseitig sehe ich die Verarbeitungsschritte und kann dann präzise statt vermutend antworten. Falls Sie über den REST-Konnektor arbeiten: Die Verwaltungspraxis ist dort noch nicht als Endpunkt exponiert, das ergänze ich bei Bedarf kurzfristig.

Bei Fragen jederzeit gerne, auch telefonisch.

Mit bestem Dank und freundlichen Grüssen

Jonas Hertner, Advokat
Münsterplatz 17, 4051 Basel
jh@jonashertner.com
+41 43 215 08 50
