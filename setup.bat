@echo off
setlocal enabledelayedexpansion

set "INSTALL_DIR=%CD%"
set "PROJECT_DIR=%INSTALL_DIR%\kontur_api"
set "SHORTCUT_NAME=KonturGL.lnk"

REM Проверка зависимостей
set DEPENDENCIES=0
where winget >nul 2>nul || (
    echo ❌ Winget not found. Please install Windows Package Manager.
    set DEPENDENCIES=1
)

where git >nul 2>nul || (
    echo ❌ Git not found.
    set DEPENDENCIES=1
)

where py >nul 2>nul || (
    echo ❌ Python not found.
    set DEPENDENCIES=1
)

if !DEPENDENCIES! equ 1 (
    echo.
    echo ��� Installing missing dependencies...
    where git >nul 2>nul || (
        echo ⬇️ Installing Git...
        winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements
    )
    
    where py >nul 2>nul || (
        echo ⬇️ Installing Python...
        winget install --id Python.Python.3.12 -e --source winget --accept-package-agreements --accept-source-agreements
        echo ⚠️ Please relaunch script after Python installation
        pause
        exit /b
    )
)

REM Клонирование репозитория
if not exist "%PROJECT_DIR%" (
    echo ⬇️ Cloning repository...
    git clone https://github.com/kirillbelykh/kontur_api "%PROJECT_DIR%" || (
        echo ❌ Clone failed
        exit /b 1
    )
) else (
    echo ✅ Repository already exists
)

REM Создание виртуального окружения
cd /d "%PROJECT_DIR%" || exit /b 1

if not exist "venv" (
    echo ⬇️ Creating virtual environment...
    py -3 -m venv venv || (
        echo ❌ Virtual environment creation failed
        exit /b 1
    )
)

REM Активация и настройка окружения
call venv\Scripts\activate || (
    echo ❌ Virtual environment activation failed
    exit /b 1
)

echo ⬇️ Updating pip...
py -3 -m pip install --upgrade pip || echo ⚠️ Pip update failed

if exist "requirements.txt" (
    echo ⬇️ Installing dependencies...
    py -3 -m pip install -r requirements.txt || (
        echo ❌ Dependency installation failed
        exit /b 1
    )
) else (
    echo ⚠️ requirements.txt not found
)

REM Создание ярлыка
set "DESKTOP=%USERPROFILE%\Desktop"
set "SHORTCUT=%DESKTOP%\%SHORTCUT_NAME%"

echo ⬇️ Creating desktop shortcut...
powershell -NoProfile -Command "
    $ws = New-Object -ComObject WScript.Shell;
    $sc = $ws.CreateShortcut('%SHORTCUT%');
    $sc.TargetPath = '%PROJECT_DIR%\main.pyw';
    $sc.WorkingDirectory = '%PROJECT_DIR%';
    $sc.IconLocation = '%PROJECT_DIR%\icon.ico';
    $sc.Save()
" && echo ✅ Shortcut created || echo ❌ Shortcut creation failed

echo.
echo ✅ Installation completed successfully!
echo 📂 Project location: %PROJECT_DIR%
echo 🔗 Shortcut: %SHORTCUT%
pause