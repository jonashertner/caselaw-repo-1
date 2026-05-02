# SG Kantonsgericht — Anfrage Spruchkörper-Metadaten (initial mail)

**To**: [SG Kantonsgericht — IT-Verantwortlicher / Veröffentlichung]
**Subject**: OpenCaseLaw — Anfrage zu Spruchkörper- und
Aktennummer-Metadaten in publizierten St. Galler Entscheiden

Sehr geehrte Damen und Herren

OpenCaseLaw (https://opencaselaw.ch) ist ein gemeinnütziges Projekt,
das Schweizer Gerichtsentscheide unter CC-BY-4.0 zugänglich macht.
Wir publizieren aktuell 970'649 Entscheide aus 108 Gerichten — darunter
für St. Gallen vollständige Daten ab www.gerichte.sg.ch /
sg_publikationen.

Im Verlauf eines internen Quality-Checks am 30. April 2026 haben wir
bei den St. Galler Publikationen festgestellt, dass die zugewiesenen
Spruchkörper-Bezeichnungen (Kantonsgericht / Verwaltungsgericht /
Versicherungsgericht / Kassationsgericht / Verwaltungsrekurskommission /
Handelsgericht) bei einem Teil der Entscheide korrekt aus dem
direkten Datenfeed kommen — bei einem anderen Teil aber nur die generische
Quelle "sg_publikationen" tragen, weil die ursprüngliche
entscheidsuche.ch-Veröffentlichung die Chamber nicht mitgibt. Wir haben
das auf unserer Seite mit einer architektonischen Korrektur
(commit `713afe3`) entschärft, sodass der direkte Datenfeed Vorrang
hat, sobald er verfügbar ist.

Für die durchgängige Korrektheit wären zwei Punkte aus Ihrer
IT-Infrastruktur sehr hilfreich:

1. **Spruchkörper-Feld** in der publizierten JSON/XML-Ausgabe (falls
   nicht bereits vorhanden — z. B. ein eigenes Feld `chamber` oder
   `kammer`). Aktuell extrahieren wir die Information aus dem Aktenzeichen-
   Präfix, was an Randfällen scheitert.

2. **Kanonisches Aktenzeichen** in seiner publikationsrechtlichen
   Endform (mit allfälligen Verweisen auf parallel geführte
   Verfahren). Damit könnten wir die "joined-docket"-Klasse
   (z. B. `XBE.2025.32 _ XBE.2025.5`) als saubere
   Mehrfach-Referenzen darstellen statt als einen einzelnen
   zusammengesetzten Schlüssel.

Wenn diese Felder bereits intern vorliegen und nur ein Publikations-
Mapping ergänzt werden müsste, würden wir uns sehr über einen kurzen
Austausch mit Ihrer IT freuen — auch ein 30-minütiger Call genügt
in der Regel, um die Schnittstelle zu klären.

Mit freundlichen Grüssen
Jonas Hertner — OpenCaseLaw
jonashertner@protonmail.ch
