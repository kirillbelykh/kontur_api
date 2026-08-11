# Kontur API

Windows desktop app for Kontur.Markirovka: order codes, download, introduction
into circulation, TSD tasks, aggregation, BarTender labels, WMS CHZ requests.

## Quick start

```powershell
# already installed
.\.venv\Scripts\python.exe main.py
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
| `runtime/` | Local cookies, logs, temp (not committed) |
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
