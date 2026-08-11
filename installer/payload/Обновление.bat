@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "UPDATE_PS1=%SCRIPT_DIR%scripts\update_windows.ps1"

if not exist "%UPDATE_PS1%" (
    echo [ERROR] Update script not found: %UPDATE_PS1%
    pause
    exit /b 1
)

echo Updating KonturAPI and rebuilding local installation...
powershell -NoProfile -ExecutionPolicy Bypass -File "%UPDATE_PS1%"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] Update failed with code %EXIT_CODE%.
    echo See log: "%SCRIPT_DIR%kontur_update.log"
    pause
    exit /b %EXIT_CODE%
)

echo.
echo [OK] Update completed successfully.
pause
