@echo off
setlocal
cd /d "%~dp0"

REM Explorer always opens a console for .bat. Bounce into a minimized helper
REM and close this window so closing a terminal cannot kill the app.
if /I not "%~1"=="_silent" (
  start "" /min "%ComSpec%" /c ""%~f0" _silent"
  exit 0
)

set "PYTHONW=%~dp0.venv\Scripts\pythonw.exe"
if exist "%PYTHONW%" (
  start "" /D "%~dp0" "%PYTHONW%" "%~dp0main.py"
  exit 0
)

echo [ERROR] pythonw.exe not found. Run setup.bat or reinstall.
pause
exit /b 1
