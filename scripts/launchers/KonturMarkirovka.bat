@echo off
setlocal
cd /d "%~dp0..\.."

REM Fallback launcher. Desktop shortcut should point at pythonw.exe + main.py.
if /I not "%~1"=="_silent" (
  start "" /min "%ComSpec%" /c ""%~f0" _silent"
  exit 0
)

set "PYTHONW=%CD%\.venv\Scripts\pythonw.exe"
if exist "%PYTHONW%" (
  start "" /D "%CD%" "%PYTHONW%" "%CD%\main.py"
  exit 0
)

echo [ERROR] pythonw.exe not found. Run Install.bat or Install.exe.
pause
exit /b 1
