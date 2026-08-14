@echo off
setlocal
cd /d "%~dp0"
set "PYTHONW=%~dp0.venv\Scripts\pythonw.exe"
if exist "%PYTHONW%" (
  start "" "%PYTHONW%" "%~dp0main.py"
  exit /b 0
)
if exist "%~dp0run_kontur.vbs" (
  start "" "%SystemRoot%\System32\wscript.exe" //nologo "%~dp0run_kontur.vbs"
  exit /b 0
)
echo [ERROR] pythonw.exe not found. Run setup.bat or reinstall.
pause
exit /b 1
