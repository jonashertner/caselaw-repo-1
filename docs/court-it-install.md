# OpenCaseLaw CLI: Installation auf verwalteten Windows-Arbeitsplätzen

Für die IT von Gerichten und Verwaltungen. Beschreibt, was der Installer
`OpenCaseLaw-CLI-<version>-setup.exe` auf einem Rechner ablegt, welche
Programme dabei laufen, wann das Netzwerk benutzt wird und wie sich das Ganze
allow-listen, verteilen, aktualisieren, prüfen und wieder entfernen lässt.
Stand: Client 0.9.0 (unveröffentlicht), Laufzeit Python 3.13.7, PDF-Bibliothek
pypdf 6.17.0.

Was das Werkzeug leistet und was nicht, steht in der
[Anleitung zum Client](research-cli.md): `ocl check EINGABE.pdf` liest eine
Eingabe der Parteien oder einen eigenen Entwurf (PDF, Word inklusive
Fussnoten, Markdown, HTML, Text), findet die zitierten Entscheide und die
daneben stehenden Zitate, prüft jedes gegen das lokal gespeicherte
Verifikationspaket und schreibt einen Bericht neben die Datei. Geprüft werden
Existenz und Identität des zitierten Entscheids, die zitierte Erwägung (wo
indexiert) und der Wortlaut eines Zitats. Nicht geprüft wird, ob ein Entscheid
die Aussage trägt, noch gilt oder zum Sachverhalt passt. Eigene,
unveröffentlichte Entscheide und der angefochtene Entscheid sind nie im
Korpus; "nicht gefunden" bedeutet "nicht im veröffentlichten Korpus", nicht
"falsch".

## 1. Was installiert wird

Standard ist eine Installation pro Rechner (Administratorrechte) nach
`C:\Program Files\OpenCaseLaw`. Ohne Administratorrechte (`/CURRENTUSER`
oder Auswahl im Dialog) landet alles in `%LOCALAPPDATA%\OpenCaseLaw`.

| Pfad (unter dem Installationsordner) | Inhalt | Herkunft |
|---|---|---|
| `python.exe`, `pythonw.exe`, `python3.dll`, `python313.dll`, `*.pyd`, `sqlite3.dll`, `libssl-3.dll`, `libcrypto-3.dll`, `libffi-8.dll`, `vcruntime140.dll`, `vcruntime140_1.dll`, `python313.zip`, `python.cat`, `LICENSE-python.txt` | Die "embeddable" Laufzeit von python.org, Byte für Byte, wie die Python Software Foundation (PSF) sie veröffentlicht. Die SHA-256 des Zips ist im Build-Workflow festgeschrieben und wird bei jedem Build gegen das von python.org veröffentlichte SPDX-Dokument geprüft. | python.org |
| `python313._pth` | Legt fest, was die Laufzeit sieht: nur `python313.zip`, den Installationsordner und `Lib\site-packages`. `import site` bleibt aus, also kein Benutzer-`site-packages`, kein `PYTHONPATH`, keine `.pth`-Dateien anderer Programme. | Installer |
| `Lib\site-packages\opencaselaw_cli\` | Der Client als Python-Quelltext (reine Standardbibliothek, keine Abhängigkeiten), aus dem auf PyPI veröffentlichten Wheel entpackt. Enthält `AGENTS.md` und drei Skill-Dateien (Text). | Repository, MIT-Lizenz |
| `Lib\site-packages\pypdf\`, `Lib\site-packages\pypdf-6.17.0.dist-info\` | Die PDF-Bibliothek pypdf als Python-Quelltext (reines Python, keine Abhängigkeiten auf dieser Laufzeit), aus dem auf PyPI veröffentlichten Wheel `pypdf-6.17.0-py3-none-any.whl` entpackt; die SHA-256 des Wheels ist im Build-Workflow festgeschrieben und wird bei jedem Build gegen die geladene Datei geprüft. Nur zum Lesen der Textebene von PDF-Eingaben (Abschnitt "PDF-Eingaben" unten). | PyPI, BSD-3-Clause-Lizenz |
| `ocl.cmd` | Startskript: ruft `python.exe -m opencaselaw_cli` mit den übergebenen Argumenten auf. Setzt `PYTHONDONTWRITEBYTECODE=1` (nichts wird unter Program Files geschrieben) und `XDG_DATA_HOME=%LOCALAPPDATA%`, falls nicht gesetzt, damit das Verifikationspaket in `%LOCALAPPDATA%\ocl` liegt. | Installer |
| `check-draft.cmd` | Ziel der "Senden an"-Verknüpfung: `ocl check "<Datei>" --local`, danach wird `<Datei>.check.html` geöffnet. Mehrere markierte Dateien: ein Lauf mit allen Dateien, danach wird der Übersichtsbericht `check-index.html` geöffnet (Abschnitt "Mehrere Eingaben auf einmal" unten). | Installer |
| `pull-pack.cmd` | Ziel des Startmenü-Eintrags "OpenCaseLaw - Verifikationspaket laden": `ocl pack pull`; das Fenster bleibt bis zum Tastendruck offen. | Installer |
| `shortcuts\Eingabe oder Entwurf prüfen (offline).lnk` | Vorlage der "Senden an"-Verknüpfung für die Verteilung an alle Benutzer (Abschnitt 5). | Installer |
| `unins000.exe`, `unins000.dat` | Deinstallation (Inno Setup). | Installer |
| `LICENSE-opencaselaw-cli.txt`, `LICENSE-pypdf.txt`, `TREE.json` | Lizenzen des Clients (MIT) und von pypdf (BSD-3-Clause); Inventar des Build-Baums mit Dateiname und SHA-256 jedes entpackten Wheels. | Installer |

Ausserhalb des Installationsordners:

- Startmenü: Gruppe `OpenCaseLaw` mit "OpenCaseLaw - Verifikationspaket
  laden" und "OpenCaseLaw CLI deinstallieren".
- "Senden an"-Menü des installierenden Benutzers
  (`%APPDATA%\Microsoft\Windows\SendTo\Eingabe oder Entwurf prüfen (offline).lnk`),
  abwählbar (Task `sendto`). Bis 0.8.0 hiess der Eintrag "Entwurf prüfen
  (offline)"; die Aktualisierung entfernt den alten Eintrag beim
  installierenden Benutzer und in `shortcuts\`.
- Registry: nur die Deinstallationsinformation unter
  `HKLM\Software\Microsoft\Windows\CurrentVersion\Uninstall\{7B2E9C7A-3F4D-4C33-9B2B-2D0F8E6A5C11}_is1`
  (bei Benutzerinstallation unter HKCU). Der Task `addtopath` (standardmässig
  aus) hängt den Installationsordner an `Path` (HKLM bei Rechner-, HKCU bei
  Benutzerinstallation); die Deinstallation entfernt den Eintrag wieder.
- Benutzerdaten: das Verifikationspaket
  (`%LOCALAPPDATA%\ocl\verification_pack.sqlite`, mehrere GB, siehe Abschnitt
  6) und die Berichte `<Entwurf>.check.html` neben den geprüften Entwürfen.
  Beides bleibt bei der Deinstallation erhalten.

Es gibt keine Dienste, keine geplanten Aufgaben, keine Autostart-Einträge,
keine Shell-Erweiterungen (die "Senden an"-Verknüpfung ist eine gewöhnliche
`.lnk`-Datei) und keine COM-Registrierung.

### PDF-Eingaben

Elektronische Eingaben der Parteien kommen als PDF (PDF/A über
justitia.swiss). Damit `ocl check` sie ohne Umweg liest, liegt die
PDF-Bibliothek **pypdf** (Version 6.17.0, reines Python, Lizenz BSD-3-Clause,
Lizenztext in `LICENSE-pypdf.txt`) unter `Lib\site-packages\pypdf\` neben dem
Client. Sie stammt aus dem auf PyPI veröffentlichten Wheel
`pypdf-6.17.0-py3-none-any.whl`: der Build-Workflow lädt genau diese Datei,
prüft ihre SHA-256 gegen den im Workflow festgeschriebenen Wert
(`5bd827266a21553b74d910e350131a6227b72f2ab4209bf372814b8195fa11c5`) und
entpackt sie wie den Client, ohne pip; `TREE.json` nennt Datei und Prüfsumme.
pypdf ist die einzige Komponente ausserhalb der Python-Standardbibliothek. Sie
wird nur zum Lesen der Textebene benutzt: keine Schrift wird gerendert, kein
Bild dekodiert, nichts geschrieben, keine Netzverbindung geöffnet. Nichts
muss auf den Arbeitsplätzen nachinstalliert werden.

Gelesen wird, was das PDF als Text enthält. Ein gescanntes Dokument ohne
Textebene (Bild-PDF, etwa eine eingescannte Papiereingabe) liefert keinen
Text; der Bericht meldet dann, dass keine Zitate gefunden wurden, und nennt
das ausdrücklich. Solche Eingaben brauchen vorher eine Texterkennung (OCR),
zum Beispiel durch die Scan-Software oder das Dokumentenmanagement des
Gerichts; der Client bringt keine mit. PDFs, die zum Öffnen ein Passwort
verlangen, werden nicht gelesen; AES-verschlüsselte Dateien mit blossem
Besitzer-Passwort bräuchten die optionale Bibliothek `cryptography`, die nicht
mitgeliefert wird (RC4 liest pypdf selbst).

### Mehrere Eingaben auf einmal

Wer im Explorer mehrere Dateien markiert und an "Eingabe oder Entwurf prüfen
(offline)" sendet, startet einen Lauf: `check-draft.cmd` übergibt alle
Dateien einem `ocl check`-Aufruf (`ocl check EINGABE1.pdf EINGABE2.pdf …
--local`), der je Datei einen Bericht `<Name>.check.html` und im Ordner der
ersten Datei den Übersichtsbericht `check-index.html` schreibt; der
Übersichtsbericht wird geöffnet. Der Rückgabewert ist 4, sobald eine der
Dateien Aufmerksamkeit braucht. Für den Tagesbetrieb (viele Eingaben pro Tag)
reicht das: die Eingaben des Tages markieren, senden, Übersichtsbericht lesen.
Eine Ordnerüberwachung ist nicht Teil des Installers. Ein älterer Client, der
eine Datei pro Aufruf nimmt, wird von `check-draft.cmd` erkannt (Rückgabewert
2 bei mehreren Dateien) und je Datei einmal aufgerufen; dann gibt es keinen
Übersichtsbericht, und die Berichte liegen neben den Dateien.

## 2. Welche Programme laufen

Die einzigen ausführbaren Dateien sind `python.exe`, `pythonw.exe` und die
DLLs beziehungsweise `.pyd`-Module aus dem Zip von python.org. `python.exe`,
`pythonw.exe` und `python3*.dll` tragen die eingebettete Authenticode-Signatur
der Python Software Foundation; die übrigen Module sind über den mitgelieferten
Katalog `python.cat` signiert. Prüfung:

```powershell
Get-AuthenticodeSignature "C:\Program Files\OpenCaseLaw\python.exe" | Format-List Status, SignerCertificate
```

Der Client selbst und pypdf sind Python-Quelltext, den `python.exe` liest.
Die drei `.cmd`-Dateien sind Batch-Skripte in reinem ASCII, jeweils wenige
Dutzend Zeilen, und können vor der Freigabe gelesen werden. Der Installer selbst
(`OpenCaseLaw-CLI-<version>-setup.exe`, Inno Setup) und der Deinstaller sind
derzeit **nicht** signiert (Abschnitt 4 und 8).

Bei Benutzung schreibt der Client nur an drei Orte: das Verifikationspaket in
`%LOCALAPPDATA%\ocl` (nur `pack pull`), den Bericht neben den Entwurf
(`ocl check`) und, falls verlangt, Beweismittelordner (`ocl bundle`, für den
Gerichtsbetrieb nicht nötig). Unter dem Installationsordner wird nichts
geschrieben, auch kein Bytecode.

## 3. Netzwerk

Nur ein Vorgang benutzt das Netzwerk: **`ocl pack pull`** (Startmenü
"Verifikationspaket laden" oder `pull-pack.cmd`). Er lädt eine Datei per HTTPS
(Port 443) von

```
https://huggingface.co/datasets/voilaj/swiss-caselaw/resolve/main/artifacts/verification_pack/latest.sqlite.gz
```

(Weiterleitungen auf die CDN-Hosts von Hugging Face, `cdn-lfs*.huggingface.co`
und `*.hf.co`, sind möglich) und entpackt sie nach
`%LOCALAPPDATA%\ocl\verification_pack.sqlite`. Das Paket ist eine SQLite-Datei
mit Metadaten aller Entscheide, den Zitierstrings des Dienstes, den
Aktenzeichen-Aliassen und allen indexierten Erwägungen (CC0). Geschätzte
Grösse: Schätzung 8 bis 9 GB komprimiert (die gemessene Zahl steht nach dem ersten wöchentlichen Paket in `ocl pack info`), entsprechend mehr entpackt; genaue Zahlen
zeigt `ocl pack info` nach dem Laden.

Die Prüfung eines Entwurfs (`check-draft.cmd`, `ocl check ... --local`) öffnet
**keine** Netzwerkverbindung. Der Entwurf, seine Zitate und der Bericht bleiben
auf dem Rechner. Das lässt sich mit einem Netzwerkmonitor oder einer
Firewall-Regel für `python.exe` bestätigen; ohne `--local` (und ohne die
Umgebungsvariable `OCL_LOCAL`, Abschnitt 6) würde derselbe Befehl den
öffentlichen Dienst `mcp.opencaselaw.ch` befragen. `check-draft.cmd` setzt
`--local` immer. Wer jede Verbindung ausschliessen will, sperrt `python.exe` aus
dem Installationsordner in der Firewall für ausgehende Verbindungen und stellt
das Paket über einen internen Spiegel oder eine Freigabe bereit (Abschnitt 6);
dann schlägt nur `pack pull` gegen huggingface.co fehl, alles andere läuft.

Proxy: `pack pull` verwendet die Umgebungsvariablen `HTTPS_PROXY`/`HTTP_PROXY`
und, falls diese fehlen, die Proxy-Einstellung aus den Internetoptionen des
Benutzers. Proxys mit NTLM- oder Kerberos-Authentisierung werden von der
Python-Standardbibliothek nicht bedient; in diesem Fall Spiegel oder
Freigabe verwenden. Die Zertifikatsprüfung nutzt den Windows-Zertifikatspeicher,
TLS-Inspection-Proxys mit eigener CA funktionieren also, wenn die CA im
Speicher des Rechners liegt.

## 4. AppLocker und WDAC

Empfohlene Regeln, in dieser Reihenfolge der Präferenz:

1. **Herausgeberregel** (AppLocker "Publisher", WDAC "Publisher"/"PCA
   Certificate") auf das Signaturzertifikat von `python.exe`: Herausgeber
   `O=Python Software Foundation, L=Beaverton, S=Oregon, C=US`, Produktname
   `Python`, beliebige Datei und Version. Das deckt `python.exe` und
   `pythonw.exe` ab, auch nach einem Laufzeit-Update. Zertifikatsdetails am
   einfachsten aus der Datei selbst lesen (`Get-AuthenticodeSignature`),
   nicht aus dieser Anleitung abschreiben.
2. **Pfadregel** für `%PROGRAMFILES%\OpenCaseLaw\*`. Die AppLocker-Standardregeln
   erlauben ausführbare Dateien und Skripte unter Program Files für alle
   Benutzer bereits; wer sie enger gezogen hat, ergänzt diesen Pfad. Program
   Files ist für Benutzer nicht beschreibbar, daher ist die Pfadregel hier
   vertretbar. Bei Benutzerinstallationen unter `%LOCALAPPDATA%` sind Pfadregeln
   nicht vertretbar; dort nur Herausgeber- oder Hashregeln verwenden, oder besser
   pro Rechner installieren.
3. **Skriptregeln**: `ocl.cmd`, `check-draft.cmd` und `pull-pack.cmd` sind
   Batch-Dateien; unter AppLocker fallen sie in die Kategorie "Skript" und sind
   durch die Standard-Pfadregel für Program Files erlaubt. WDAC kontrolliert
   `.cmd`-Dateien nicht, sondern `cmd.exe`; wenn `cmd.exe` für Benutzer gesperrt
   ist, kann `ocl.cmd` direkt durch `python.exe -m opencaselaw_cli` ersetzt
   werden (die Verknüpfungen entsprechend anpassen).
4. **DLL-Regeln** (nur falls aktiviert): die Module aus dem Zip sind über
   `python.cat` katalogsigniert, was AppLocker ohne Installation des Katalogs
   nicht auswertet; dann die Pfadregel aus Punkt 2 auch für DLLs anlegen.
5. **Installer und Deinstaller**: bis zur Signatur (Abschnitt 8) eine
   **Hashregel** auf `OpenCaseLaw-CLI-<version>-setup.exe` (Hash aus der
   `.sha256`-Datei, Abschnitt 7) und bei Bedarf auf `unins000.exe` nach der
   Installation. Alternativ die Verteilung über SCCM/Intune im Systemkontext,
   wo AppLocker den Installer nicht bewertet.

Zu beachten: AppLocker und WDAC beurteilen, ob `python.exe` starten darf, nicht,
was es ausführt. Die Laufzeit lädt nur, was `python313._pth` nennt (den
Installationsordner und `Lib\site-packages`); solange der Installationsordner
für Benutzer nicht beschreibbar ist, ist das der Client und nichts anderes.

## 5. Verteilung

Unbeaufsichtigt (SCCM, Intune, PDQ, Skript):

```
OpenCaseLaw-CLI-0.9.0-setup.exe /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /LOG="C:\Windows\Temp\opencaselaw-setup.log"
```

Optionen: `/DIR="D:\Apps\OpenCaseLaw"` für einen anderen Ordner,
`/TASKS="sendto,addtopath"` beziehungsweise `/MERGETASKS="!sendto"` für die
Tasks, `/CURRENTUSER` für eine Benutzerinstallation, `/LANG=de` oder `en`.
Rückgabewert 0 bedeutet Erfolg (Inno-Setup-Exit-Codes).

"Senden an" ist ein Ordner pro Benutzer; ein Installer im Systemkontext kann
ihn nicht für alle anlegen. Deshalb liegt die fertige Verknüpfung als Vorlage
unter `<Installationsordner>\shortcuts\Eingabe oder Entwurf prüfen (offline).lnk`. Sie kann
per Anmeldeskript oder Gruppenrichtlinie (Benutzerkonfiguration, Einstellungen,
Windows-Einstellungen, Verknüpfungen oder Dateien) nach
`%APPDATA%\Microsoft\Windows\SendTo\` kopiert werden. Wer das Menü nicht will,
lässt den Task `sendto` weg; `check-draft.cmd EINGABE.pdf [WEITERE …]`
funktioniert auch von Hand oder aus einer eigenen Verknüpfung. Per Richtlinie
verteilte Kopien der bis 0.8.0 verwendeten Verknüpfung "Entwurf prüfen
(offline)" ersetzt die Richtlinie durch die neue Vorlage.

Ein Office-Add-in gibt es für den Offline-Betrieb bewusst nicht (das
Word-Add-in unter word.opencaselaw.ch arbeitet gegen den Online-Dienst).

## 6. Verifikationspaket: Erstbezug, Spiegel, wöchentliche Aktualisierung

Das Paket wird wöchentlich neu gebaut (Sonntagsbuild) und veröffentlicht. Der
Client prüft beim Laden derzeit keine Prüfsumme und keine Signatur des Pakets;
die Datei ist öffentlich (CC0) und dient nur dem Abgleich, nicht der
Ausführung. Hugging Face zeigt für die Datei die SHA-256 (LFS) auf der
Dateiseite an; wer den Erstbezug absichern will, vergleicht sie mit
`certutil -hashfile latest.sqlite.gz SHA256` vor dem Entpacken.

Drei Betriebsarten:

- **Pro Benutzer, direkt**: Startmenü "Verifikationspaket laden" (oder
  `pull-pack.cmd`). Ziel `%LOCALAPPDATA%\ocl\verification_pack.sqlite`.
  Wöchentlich wiederholen; die neue Datei ersetzt die alte erst nach
  vollständigem Download (atomarer Austausch).
- **Interner Spiegel**: IT lädt `latest.sqlite.gz` einmal auf einen internen
  Webserver und verteilt `pull-pack.cmd --url https://intranet/…/latest.sqlite.gz`
  (oder trägt die URL in eine Verknüpfung ein). Auf den Arbeitsplätzen ist dann
  keine Verbindung nach aussen nötig.
- **Gemeinsame Freigabe**: IT entpackt das Paket einmal (`gzip -d` oder 7-Zip)
  auf eine Freigabe oder einen lokalen Pfad, zum Beispiel
  `C:\ProgramData\OpenCaseLaw\verification_pack.sqlite`, und setzt die
  Systemumgebungsvariable `OCL_LOCAL` auf diesen Pfad (Gruppenrichtlinie).
  `check-draft.cmd` und `ocl check` verwenden dann dieses Paket, ohne dass
  Benutzer etwas laden; `pull-pack.cmd` schreibt in diesem Fall nach
  `OCL_LOCAL` (`--to`), was nur für den Benutzer mit Schreibrecht sinnvoll ist.
  SQLite liest von SMB-Freigaben, aber mit spürbar längeren Laufzeiten bei
  vielen Zitaten; ein lokaler Pfad ist vorzuziehen.

Stand prüfen: `ocl pack info` zeigt Baudatum, Korpusgeneration, Anzahl
Entscheide und Erwägungen sowie die Dateigrösse; dieselben Werte stehen im
Kopf jedes Berichts. Ein Paket, das älter als eine Woche ist, kennt die
neuesten Entscheide nicht; die Meldung "nicht im Paket" im Bericht sagt dies
ausdrücklich.

## 7. Prüfsumme und Herkunftsnachweis des Installers

Jede Veröffentlichung auf
`https://github.com/jonashertner/opencaselaw/releases` (Tag `cli-v<version>`)
enthält `OpenCaseLaw-CLI-<version>-setup.exe`, die zugehörige `.sha256`-Datei
und einen Herkunftsnachweis (GitHub Artifact Attestation, SLSA-Provenienz):
er belegt, dass genau diese Datei vom Workflow
`.github/workflows/installer-cli.yml` aus dem getaggten Commit gebaut wurde.

```
certutil -hashfile OpenCaseLaw-CLI-0.9.0-setup.exe SHA256
type OpenCaseLaw-CLI-0.9.0-setup.exe.sha256
```

Die beiden Werte müssen übereinstimmen. Herkunft mit der GitHub CLI
(`winget install GitHub.cli`, einmalig `gh auth login` mit einem beliebigen
GitHub-Konto):

```
gh attestation verify OpenCaseLaw-CLI-0.9.0-setup.exe --repo jonashertner/opencaselaw
```

Ausgabe bei Erfolg: "Loaded digest sha256:… / Verification succeeded"
mit dem Workflow-Pfad und dem Commit. Wer den Installer selbst nachbauen will:
`clients/python/installer/README.md` beschreibt die Schritte; die
Laufzeit-Prüfsumme (`f6cca216a359be84797cabb54149ce5e062afb16cc7567eb7fc51cacb2d86b65`
für `python-3.13.7-embed-amd64.zip`) steht im Workflow.

## 8. Signatur

Der Installer ist bisher nicht Authenticode-signiert; Windows SmartScreen
warnt bei einem interaktiven Start, und AppLocker ohne Hash- oder Pfadregel
verweigert ihn. Geplant ist die Signatur über SignPath Foundation
(kostenlos für Open-Source-Projekte; der vorbereitete, deaktivierte
Workflow-Schritt ist dokumentiert) oder ein OV-Zertifikat. Bis dahin gelten
Hashregel und Herkunftsnachweis (Abschnitte 4 und 7). Die Laufzeit selbst
(`python.exe` und DLLs) ist unabhängig davon von der PSF signiert und kann
per Herausgeberregel freigegeben werden.

## 9. Aktualisierung und Deinstallation

Neue Version: den neuen Installer über die bestehende Installation laufen
lassen (gleiche `AppId`; Dateien werden ersetzt, Verknüpfungen bleiben, das
Verifikationspaket bleibt). Silent wie in Abschnitt 5.

Deinstallation: Startmenü "OpenCaseLaw CLI deinstallieren", Systemsteuerung
"Apps und Features", oder

```
"C:\Program Files\OpenCaseLaw\unins000.exe" /VERYSILENT /SUPPRESSMSGBOXES /NORESTART
```

Entfernt werden der Installationsordner, die Startmenü-Gruppe, die
"Senden an"-Verknüpfung des installierenden Benutzers und der PATH-Eintrag,
falls gesetzt. Nicht entfernt werden das Verifikationspaket
(`%LOCALAPPDATA%\ocl`, per Benutzer, mehrere GB) sowie Berichte neben den
Entwürfen; beides von Hand oder per Skript löschen. Per Gruppenrichtlinie
verteilte "Senden an"-Kopien (Abschnitt 5) entfernt die Richtlinie.

## 10. Grenzen und Betrieb

- Nur x64-Windows ab Windows 10 1809 / Server 2019. ARM64-Windows führt die
  x64-Laufzeit emuliert aus; ein ARM64-Paket kann bei Bedarf gebaut werden.
- Das Paket enthält keine Volltexte: ein Zitat ohne Erwägungsangabe kann
  offline nicht gegen den Text verglichen werden, und Gesetzesartikel werden
  offline nicht geprüft. Der Bericht kennzeichnet beides.
- PDF-Eingaben ohne Textebene (Scans) liefern keine Zitate; sie brauchen
  vorher OCR (Abschnitt 1, "PDF-Eingaben").
- Die Abdeckung je Gericht ist unterschiedlich (`https://opencaselaw.ch/coverage/`);
  "nicht im veröffentlichten Korpus" ist bei Gerichten mit lückenhafter
  Veröffentlichung häufig und kein Fehlerbefund.
- Kontakt für Betrieb und Rückfragen: `https://github.com/jonashertner/opencaselaw/issues`
  (öffentlich; keine Entwürfe oder Aktenauszüge anhängen) oder die auf
  `https://opencaselaw.ch/ueber/` genannte Adresse.
