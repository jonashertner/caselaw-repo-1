@echo off
rem OpenCaseLaw CLI launcher (Windows). Runs the pure-stdlib package with the
rem python.org embeddable runtime that lives next to this file. No PATH, no
rem registry, no user site-packages: python3XX._pth pins what the runtime sees.
setlocal
set "OCL_HOME=%~dp0"
rem Program Files is read-only for users; never try to write bytecode there.
set "PYTHONDONTWRITEBYTECODE=1"
set "PYTHONUTF8=1"
rem The pack from `ocl pack pull` lands in %%LOCALAPPDATA%%\ocl unless the
rem caller already chose a data directory (or a pack via OCL_LOCAL).
if not defined XDG_DATA_HOME set "XDG_DATA_HOME=%LOCALAPPDATA%"
"%OCL_HOME%python.exe" -m opencaselaw_cli %*
exit /b %ERRORLEVEL%
