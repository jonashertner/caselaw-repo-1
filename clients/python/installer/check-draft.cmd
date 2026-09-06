@echo off
rem "Send to" target: check one draft against the local verification pack and
rem open the report. Nothing leaves this machine (--local).
rem Usage: check-draft.cmd ENTWURF.docx   (also .md, .txt, .html)
setlocal
if "%~1"=="" (
  echo Aufruf: check-draft.cmd ENTWURF.docx
  echo Oder im Explorer: Rechtsklick auf den Entwurf, "Senden an", "Entwurf pruefen (offline)".
  pause
  exit /b 2
)
set "DRAFT=%~1"
rem The report is written next to the draft as <name>.check.html (ocl check default).
set "REPORT=%~dpn1.check.html"
echo OpenCaseLaw: pruefe "%DRAFT%" gegen das lokale Verifikationspaket ...
rem A report from an earlier run must never pass for this run's result.
if exist "%REPORT%" del /q "%REPORT%" 2>nul
if defined OCL_LOCAL (
  rem OCL_LOCAL names the pack (for example a share provisioned by IT); the flag
  rem alone would ignore it, so the environment carries the choice.
  call "%~dp0ocl.cmd" check "%DRAFT%" --color never
) else (
  call "%~dp0ocl.cmd" check "%DRAFT%" --local --color never
)
set "RC=%ERRORLEVEL%"
rem 0 = all found, 4 = something needs attention: both wrote a report. 1/2/3 did not.
if exist "%REPORT%" if %RC% LEQ 4 if not %RC%==1 if not %RC%==2 if not %RC%==3 (
  start "" "%REPORT%"
  goto :done
)
(

  echo.
  echo Kein Bericht geschrieben ^(Code %RC%^).
  echo Fehlt das Verifikationspaket: Startmenue, OpenCaseLaw, "Verifikationspaket laden".
  pause
)
:done
rem 0 = alles gefunden, 4 = etwas braucht Aufmerksamkeit (steht im Bericht),
rem 2 = Eingabe, 3 = Paket oder Dienst nicht erreichbar.
exit /b %RC%
