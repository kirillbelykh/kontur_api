@echo off
setlocal
cd /d "%~dp0.."
echo Building optional Inno payload into dist\installer\ ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build_installer.ps1"
set "ERR=%ERRORLEVEL%"
if not "%ERR%"=="0" (
  echo Build failed: %ERR%
  pause
  exit /b %ERR%
)
echo.
echo Optional payload is in dist\installer\ — not copied to the repo root.
echo Operator installer is Install.exe in the project root (scripts\build_install_exe.ps1).
pause
