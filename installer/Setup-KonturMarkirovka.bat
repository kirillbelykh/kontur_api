@echo off
setlocal
cd /d "%~dp0"
set "ZIP=%~dp0KonturMarkirovka-1.0.0-payload.zip"
set "PS1=%~dp0Install-KonturMarkirovka.ps1"
if not exist "%PS1%" (
  echo [ERROR] Install-KonturMarkirovka.ps1 not found next to this Setup.
  pause
  exit /b 1
)
echo Installing Kontur Markirovka...
if exist "%ZIP%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -SourceZip "%ZIP%"
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
)
set "ERR=%ERRORLEVEL%"
if not "%ERR%"=="0" (
  echo Installation failed: %ERR%
  pause
  exit /b %ERR%
)
echo.
echo Done. Use desktop shortcut: Kontur Markirovka
pause
