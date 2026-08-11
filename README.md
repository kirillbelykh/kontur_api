# Kontur API

Windows desktop app for Kontur.Markirovka: order codes, download, introduction
into circulation, TSD tasks, aggregation, BarTender labels, WMS CHZ requests.

## Установка на новый ПК (Windows)

1. Склонировать репозиторий (или скопировать `KonturMarkirovka-Setup.exe`).
2. Запустить `KonturMarkirovka-Setup.exe` — установщик распакует приложение в
   `%LOCALAPPDATA%\Programs\KonturMarkirovka`, поставит uv/Python/зависимости,
   скачает YandexDriver и создаст ярлык «Контур Маркировка» на рабочем столе.
3. Запустить ярлык — можно пользоваться.

Пересборка установщика после изменений: `Build-Installer.bat`
(готовый `KonturMarkirovka-Setup.exe` появляется в корне проекта).

## Quick start

```powershell
# already installed
.\.venv\Scripts\python.exe main.py
# or
KonturMarkirovka.bat
# or
wscript run_kontur.vbs
```

Frontend (optional hot reload):

```powershell
cd frontend
npm install
npm run dev
# set VITE_DEV_URL=http://127.0.0.1:5173 in .env, then restart main.py
```

Production UI build:

```powershell
cd frontend
npm run build
```

## Layout

See [AGENTS.md](AGENTS.md). Short map:

| Path | Role |
|------|------|
| `backend/` | Python auth, Kontur API, services, ApiBridge, desktop shell |
| `frontend/` | React + Vite + TypeScript + Tailwind (WMS-aligned design) |
| `archive/legacy_ui/` | Old CustomTkinter UI (reserve only) |
| `archive/ui_v2_static/` | Previous HTML/JS UI (reserve) |
| `assets/labels/` | BarTender `.btw` templates |
| `runtime/` | Local cookies, logs, backups, temp (not committed) |
| `scripts/launchers/` | Extra VBS launchers (CRPT bridge) |
| `tests/` | Unit tests |

## Rules

- Design: `.cursor/rules/design.mdc`
- Backend: `.cursor/rules/backend.mdc`
- Project: `.cursor/rules/project.mdc`

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_auth_cookies tests.test_cookies_prolongation tests.test_ui_v2_api_bridge tests.test_history_db_unit
```


## Windows installer

Run `Build-Installer.bat` from the project root. Intermediates stay under `dist\installer\` / `installer\payload\`; the deliverable `KonturMarkirovka-Setup.exe` is published to the **project root** (large binaries are gitignored).

## Update

```powershell
git pull origin main
uv sync --python 3.12 --frozen
cd frontend && npm install && npm run build
```

BarTender and CryptoPro/CAdES are external licensed software and are not bundled.
