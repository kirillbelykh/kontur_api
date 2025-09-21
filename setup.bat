@echo off
setlocal enabledelayedexpansion

REM === Текущая папка, где запущен скрипт ===
set "INSTALL_DIR=%CD%"
set "PROJECT_DIR=%INSTALL_DIR%\kontur_api"

REM === Проверка наличия winget ===
where winget >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ Winget не найден. Установите Windows Package Manager вручную.
    exit /b 1
)

REM === Установка Git ===
where git >nul 2>nul
if %errorlevel% neq 0 (
    echo ⬇️ Устанавливаю Git...
    winget install --id Git.Git -e --source winget
) else (
    echo ✅ Git уже установлен
)

REM === Установка Python ===
where py >nul 2>nul
if %errorlevel% neq 0 (
    echo ⬇️ Устанавливаю Python...
    winget install --id Python.Python.3.12 -e --source winget
    echo ⚠️ Python установлен. Перезапустите этот скрипт ещё раз!
    pause
    exit /b
) else (
    echo ✅ Python уже установлен
)

REM === Клонирование проекта ===
if not exist "%PROJECT_DIR%" (
    echo ⬇️ Клонирую проект в %PROJECT_DIR%...
    git clone https://github.com/kirillbelykh/kontur_api "%PROJECT_DIR%"
) else (
    echo ✅ Папка проекта уже существует: %PROJECT_DIR%
)

cd "%PROJECT_DIR%"

REM === Создание виртуального окружения ===
if not exist venv (
    echo ⬇️ Создаю виртуальное окружение...
    py -3 -m venv venv
)

REM === Активация окружения ===
call venv\Scripts\activate

REM === Обновление pip и установка зависимостей ===
py -3 -m pip install --upgrade pip
py -3 -m pip install -r requirements.txt

REM === Создание ярлыка на рабочем столе ===
set "DESKTOP=%USERPROFILE%\Desktop"
set "TARGET=%CD%\main.pyw"
set "SHORTCUT=%DESKTOP%\Заказ кодов Контур.lnk"
set "ICON=%CD%\icon.ico"

echo ⬇️ Создаю ярлык на рабочем столе с иконкой...

powershell -Command ^
  $s=(New-Object -COM WScript.Shell).CreateShortcut('%SHORTCUT%'); ^
  $s.TargetPath='%TARGET%'; ^
  $s.WorkingDirectory='%CD%'; ^
  $s.IconLocation='%ICON%'; ^
  $s.Save()

echo.
echo ✅ Установка завершена!
echo 📂 Проект установлен в: %PROJECT_DIR%
echo 🖥️ Ярлык создан: %SHORTCUT%
pause
