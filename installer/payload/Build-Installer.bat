@echo off
setlocal
cd /d "%~dp0"
echo Building Kontur Markirovka installer...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build_installer.ps1"
set "ERR=%ERRORLEVEL%"
if not "%ERR%"=="0" (
  echo Build failed: %ERR%
  pause
  exit /b %ERR%
)
echo.
echo Output: dist\installer\
dir /b "%~dp0dist\installer\"
pause
