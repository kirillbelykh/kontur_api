@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "INSTALL_PS1=%SCRIPT_DIR%scripts\install_windows.ps1"

if not exist "%INSTALL_PS1%" (
    echo [ERROR] Installer script not found: %INSTALL_PS1%
    pause
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%INSTALL_PS1%"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] Installation failed with code %EXIT_CODE%.
    pause
    exit /b %EXIT_CODE%
)

echo.
echo [OK] Installation completed successfully.
pause
