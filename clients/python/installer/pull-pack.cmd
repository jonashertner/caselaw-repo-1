@echo off
rem Start-menu target: download the weekly verification pack (several GB).
rem This is the only step that uses the network; it talks to the mirror over
rem HTTPS (HTTPS_PROXY is honoured; --url points at an internal mirror).
setlocal
echo OpenCaseLaw: Verifikationspaket laden ^(mehrere GB, einmal pro Woche^).
if defined OCL_LOCAL (
  echo Ziel: %OCL_LOCAL%
  call "%~dp0ocl.cmd" pack pull --to "%OCL_LOCAL%" %*
) else (
  call "%~dp0ocl.cmd" pack pull %*
)
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (
  echo Fertig. "ocl pack info" zeigt Stand und Groesse des Pakets.
) else (
  echo Fehlgeschlagen ^(Code %RC%^). Proxy und Freigabe fuer huggingface.co pruefen,
  echo oder das Paket von einem internen Spiegel laden: pull-pack.cmd --url https://...
)
pause
exit /b %RC%
