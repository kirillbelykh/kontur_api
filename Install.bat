@echo off
setlocal
cd /d "%~dp0"

set "INSTALL_PS1=%~dp0scripts\install_windows.ps1"
if not exist "%INSTALL_PS1%" (
    echo [ERROR] Installer script not found: %INSTALL_PS1%
    pause
    exit /b 1
)

echo Reinstalling Kontur Markirovka in this folder...
powershell -NoProfile -ExecutionPolicy Bypass -File "%INSTALL_PS1%" -Reinstall
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] Installation failed with code %EXIT_CODE%.
    pause
    exit /b %EXIT_CODE%
)

echo.
echo [OK] Installation completed. Use the desktop shortcut "Kontur Markirovka".
pause
