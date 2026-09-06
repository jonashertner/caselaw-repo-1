@echo off
rem "Send to" target: check one or more filings (Eingaben) or drafts against the
rem local verification pack and open the report. Nothing leaves this machine (--local).
rem Usage: check-draft.cmd EINGABE.pdf [WEITERE ...]   (.pdf, .docx, .md, .txt, .html)
rem Explorer passes every selected file as its own argument.
setlocal
rem The launcher next to this script; taken before `shift` moves %0 and before
rem any `call :label`, where %~dp0 no longer names this file.
set "OCL_DIR=%~dp0"
if "%~1"=="" (
  echo Aufruf: check-draft.cmd EINGABE.pdf [WEITERE DATEIEN ...]
  echo Oder im Explorer: Datei^(en^) markieren, Rechtsklick, "Senden an", "Eingabe oder Entwurf pruefen (offline)".
  pause
  exit /b 2
)
rem --local unless OCL_LOCAL already names the pack (for example a share
rem provisioned by IT); the flag alone would ignore it, so the environment carries the choice.
set "LOCALFLAG=--local"
if defined OCL_LOCAL set "LOCALFLAG="
rem Several files are one run with one index report, written next to the first file.
set "INDEX=%~dp1check-index.html"
if "%~2"=="" goto :single

echo OpenCaseLaw: pruefe mehrere Dateien gegen das lokale Verifikationspaket ...
rem An index from an earlier run must never pass for this run's result.
if exist "%INDEX%" del /q "%INDEX%" 2>nul
call "%OCL_DIR%ocl.cmd" check %* %LOCALFLAG% --color never
set "RC=%ERRORLEVEL%"
if not "%RC%"=="2" goto :multi_done
rem Exit 2 with several files: a client that takes one file per call rejects the
rem extra arguments ("unrecognized arguments"). Check the files one after another.
echo.
echo Dieser Client prueft eine Datei pro Aufruf; die Dateien werden nacheinander geprueft.
set "RC=0"
:next
if "%~1"=="" goto :multi_done
call :one "%~1"
if %ERRORLEVEL% GTR %RC% set "RC=%ERRORLEVEL%"
shift
goto :next

:multi_done
if exist "%INDEX%" (
  start "" "%INDEX%"
) else (
  echo.
  echo Die Berichte liegen neben den geprueften Dateien ^(NAME.check.html^).
  if %RC% GEQ 1 if %RC% LEQ 3 echo Fehlt das Verifikationspaket: Startmenue, OpenCaseLaw, "Verifikationspaket laden".
  pause
)
rem 0 = alles gefunden, 4 = etwas braucht Aufmerksamkeit (steht im Bericht),
rem 2 = Eingabe, 3 = Paket oder Dienst nicht erreichbar; bei mehreren Dateien der hoechste Code.
exit /b %RC%

:single
set "DRAFT=%~1"
rem The report is written next to the file as <name>.check.html (ocl check default).
set "REPORT=%~dpn1.check.html"
echo OpenCaseLaw: pruefe "%DRAFT%" gegen das lokale Verifikationspaket ...
rem A report from an earlier run must never pass for this run's result.
if exist "%REPORT%" del /q "%REPORT%" 2>nul
call "%OCL_DIR%ocl.cmd" check "%DRAFT%" %LOCALFLAG% --color never
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
exit /b %RC%

:one
rem Fallback, one file per call: check it, leave the report next to it, return the exit code.
set "REPORT=%~dpn1.check.html"
echo.
echo --- %~nx1
if exist "%REPORT%" del /q "%REPORT%" 2>nul
call "%OCL_DIR%ocl.cmd" check "%~1" %LOCALFLAG% --color never
exit /b %ERRORLEVEL%
