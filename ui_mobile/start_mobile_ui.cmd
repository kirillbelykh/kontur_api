@echo off
setlocal
cd /d "%~dp0.."
set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" ui_mobile\server_mobile.py --host 0.0.0.0 --port 8787 --https-port 8788
