@echo off
setlocal
cd /d "%~dp0"
set "PYTHONW=%~dp0.venv\Scripts\pythonw.exe"
set "PYTHON=%~dp0.venv\Scripts\python.exe"
if exist "%PYTHONW%" (
  start "" "%PYTHONW%" "%~dp0main.py"
  exit /b 0
)
if exist "%PYTHON%" (
  start "" "%PYTHON%" "%~dp0main.py"
  exit /b 0
)
echo [ERROR] Python venv not found. Run setup.bat or reinstall.
pause
exit /b 1
