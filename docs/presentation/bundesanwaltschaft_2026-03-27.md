# OpenCaseLaw — Talking Points für die Bundesanwaltschaft

## Was ist OpenCaseLaw?

Eine offene Datenbank mit **963'459 Entscheiden** von 102 Schweizer Gerichten und Behörden, inklusive eines **Zitationsgraphen mit 6.5 Mio. aufgelösten Entscheid-Referenzen** und 11.3 Mio. Gesetzesverweisen. Die Daten werden täglich automatisch aktualisiert. Der Zugang ist kostenlos, ohne Login, über das MCP-Protokoll (Model Context Protocol) direkt in Claude oder ChatGPT nutzbar.

## BStGer-Abdeckung

- **11'399 Entscheide**, davon 52.5% deutsch, 34.4% französisch, 13.1% italienisch
- Strafkammer, Beschwerdekammer, Berufungskammer — vollständig seit Gründung 2004
- Tägliche Aktualisierung, Verzögerung typischerweise <24h nach Publikation

---

## Funktionen, die kein anderes Tool bietet

### 1. Zitationsgraph: Wer zitiert wen?

Für jeden Entscheid ist sichtbar, welche anderen Entscheide er zitiert und welche ihn zitieren. Das ist keine redaktionelle Auswahl, sondern automatisch aus 963'000 Volltexten extrahiert.

Konkret für die BA: Zu jedem BStGer-Entscheid sehen Sie sofort, ob und wie das BGer ihn behandelt hat. Wir haben **244 BGer-Überprüfungen von BStGer-Strafkammerentscheiden** identifiziert, davon 187 aus der Strafrechtlichen Abteilung (6B).

### 2. Leitentscheid-Ranking nach Zitationshäufigkeit

Für jede Gesetzesbestimmung zeigt das System die meistzitierten BGEs — sortiert nach der tatsächlichen Anzahl Zitierungen in der gesamten Datenbank.

Beispiele für BA-relevante Bestimmungen:

| Bestimmung | Top-BGE | Zitierungen |
|---|---|---|
| Art. 305bis StGB (Geldwäscherei) | BGE 126 I 97 | 5'943x |
| Art. 260ter StGB (Krim. Organisation) | BGE 133 IV 235 | 816x |
| Art. 260quinquies StGB (Terrorismusfinanzierung) | BGE 130 II 337 | 1'579x |
| Art. 146 StGB (Betrug) | BGE 147 IV 73 | 4'730x |
| Art. 251 StGB (Urkundenfälschung) | BGE 141 IV 369 | 3'685x |

### 3. Anklage-Koinzidenzanalyse

Aus den 11'399 BStGer-Entscheiden haben wir extrahiert, welche Straftatbestände wie oft gemeinsam in einem Verfahren vorkommen:

- Art. 305bis + Art. 146 (Geldwäscherei + Betrug): **150 Verfahren**
- Art. 146 + Art. 251 (Betrug + Urkundenfälschung): **125 Verfahren**
- Art. 305bis + Art. 260ter (Geldwäscherei + Krim. Organisation): **107 Verfahren**
- Art. 138 + Art. 158 (Veruntreuung + Ungetreue Geschäftsbesorgung): **81 Verfahren**
- Art. 305bis + Art. 322ter (Geldwäscherei + Bestechung): **35 Verfahren**

Das zeigt empirisch, welche Tatbestände in der Praxis zusammen angeklagt werden.

### 4. Zeitliche Entwicklung der BStGer-Geschäftslast

- Geldwäscherei: stabil bei 60–100 Verfahren/Jahr, total 1'595
- Terrorismus: Anstieg von 2–6/Jahr (2010–2015) auf **25 im Jahr 2021**, seither 10–19/Jahr
- Korruption: Verdopplung von ~16/Jahr (2010–2015) auf 30–50/Jahr (2020+), Peak 2020 mit 52
- Rechtshilfe: Peak 2014 und 2020 mit je 218 Verfahren, aktuell Ukraine, Spanien, Deutschland, Italien, Vatikan

### 5. Sprachübergreifende Suche

47.5% der BStGer-Entscheide sind auf Französisch oder Italienisch. Die italienischen Entscheide betreffen fast ausschliesslich Rechtshilfe mit Italien und dem Vatikan. Eine Suche auf Deutsch findet auch FR/IT-Entscheide zum gleichen Thema.

---

## Konkrete Prompts und Suchbeispiele

Die folgenden Prompts funktionieren direkt in Claude (mit MCP-Anbindung) oder ChatGPT:

**Fallrecherche zu einer bestimmten Strafnorm:**
```
Suche alle BStGer-Entscheide zu Geldwäscherei (Art. 305bis StGB)
seit 2020 auf Deutsch.
```

**Leitentscheide finden:**
```
Was sind die meistzitierten BGEs zu Art. 260ter StGB
(kriminelle Organisation)?
```

**Case Brief zu einem bestimmten Entscheid:**
```
Erstelle ein Case Brief zum Entscheid SK.2011.5
(Strafkammer, Verfahren "Montecristo").
```

**Zitationsnetzwerk erkunden:**
```
Welche Entscheide zitieren den BGE 133 IV 235
(Bundesgerichtsbarkeit bei krimineller Organisation)?
Welche davon sind vom BStGer?
```

**Rechtshilfe nach Land:**
```
Suche BStGer-Entscheide zur internationalen Rechtshilfe
an die Ukraine seit 2022.
```

**Instanzenzug verfolgen:**
```
Zeige den Instanzenzug zum BStGer-Entscheid SK.2019.77 —
gibt es einen BGer-Entscheid dazu?
```

**Gesetzestext mit Leitentscheiden:**
```
Zeige mir Art. 305bis StGB im Volltext mit den wichtigsten
BGE-Leitentscheiden und allfälliger Kommentarliteratur.
```

Dies nutzt intern `get_doctrine`, welches Gesetzestext (Fedlex), Leitentscheide (Zitationsgraph) und Kommentare (OnlineKommentar.ch) kombiniert.

**Vergleich zweier Rechtsgebiete:**
```
Wie hat sich das Verhältnis von Geldwäscherei- zu
Terrorismus-Verfahren am BStGer in den letzten 10 Jahren
entwickelt?
```

**Für Analysearbeit / Berichte:**
```
Welche Tatbestände werden am BStGer am häufigsten zusammen
mit Art. 305bis StGB angeklagt? Zeige die Top-10-Kombinationen
mit Fallzahlen.
```

---

## Zugang

- **MCP-Server (öffentlich):** `mcp.opencaselaw.ch` — funktioniert mit Claude Desktop, Claude Code, ChatGPT, jedem MCP-kompatiblen Client
- **Websuche:** `mcp.opencaselaw.ch/entscheid/{id}` — jeder Entscheid hat eine eigene URL
- **Bulk-Daten:** HuggingFace-Dataset `voilaj/swiss-caselaw` (~7 GB Parquet)
- **Kein Login, keine Lizenz, keine Kosten**

## Grenzen

- Nur öffentlich publizierte Entscheide — keine unveröffentlichten oder nicht anonymisierten Verfahren
- Der Zitationsgraph ist automatisch extrahiert, nicht manuell kuratiert — einzelne Fehlzuordnungen sind möglich (Confidence-Score mitgeliefert)
- Keine Volltextsuche in PDF-only-Entscheiden ohne Text-Layer (betrifft ca. 5'000 ältere Scans)
- Keine eigene Übersetzungsfunktion — sprachübergreifende Suche funktioniert über Synonymexpansion, nicht maschinelle Übersetzung
