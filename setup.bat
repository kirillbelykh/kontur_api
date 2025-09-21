@echo off
setlocal enabledelayedexpansion

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
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo ⬇️ Устанавливаю Python...
    winget install --id Python.Python.3.12 -e --source winget
) else (
    echo ✅ Python уже установлен
)

REM === Клонирование проекта ===
if not exist kontur_api (
    echo ⬇️ Клонирую проект...
    git clone https://github.com/kirillbelykh/kontur_api
) else (
    echo ✅ Папка kontur_api уже существует
)

cd kontur_api

REM === Создание виртуального окружения ===
if not exist venv (
    echo ⬇️ Создаю виртуальное окружение...
    python -m venv venv
)

REM === Активация окружения ===
call venv\Scripts\activate

REM === Обновление pip и установка зависимостей ===
python -m pip install --upgrade pip
pip install -r requirements.txt

REM === Создание ярлыка на рабочем столе ===
set DESKTOP=%USERPROFILE%\Desktop
set TARGET=%CD%\main.pyw
set SHORTCUT=%DESKTOP%\Kontur_API.lnk
set ICON=%CD%\icon.ico

echo ⬇️ Создаю ярлык на рабочем столе с иконкой...

powershell -Command ^
  $s=(New-Object -COM WScript.Shell).CreateShortcut('%SHORTCUT%'); ^
  $s.TargetPath='%TARGET%'; ^
  $s.WorkingDirectory='%CD%'; ^
  $s.IconLocation='%ICON%'; ^
  $s.Save()

echo ✅ Установка завершена!
pause
