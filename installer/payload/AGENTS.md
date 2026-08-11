# Kontur API — project map for agents

Desktop app for Kontur.Markirovka: orders, code download, introduction into circulation, TSD tasks, aggregation, BarTender labels, WMS Honest Sign (CHZ) requests.

## Layout

```
kontur_api/
  AGENTS.md                 # this file
  README.md
  pyproject.toml            # Python deps (uv)
  .env.example
  main.py                   # thin desktop launcher → backend.app.desktop
  backend/                  # all Python business logic
    auth/                   # cookies, Selenium, prolongation
    kontur/                 # HTTP API, CryptoPro, WinHTTP
    services/               # history, aggregation, labels, queue, utils
    app/                    # ApiBridge, PyWebView entry, CHZ bridge
  frontend/                 # React + Vite + TypeScript + Tailwind
  archive/
    legacy_ui/              # CustomTkinter main (read-only reserve)
  assets/
    labels/                 # BarTender .btw templates
    icons/
  data/                     # xlsx reference data
  driver/                   # yandexdriver.exe
  runtime/                  # local cookies, logs, tmp (not committed)
  scripts/                  # Windows install / update
  tests/
  docs/
```

## How to run (local)

```powershell
# Backend + desktop shell (loads frontend/dist, or VITE_DEV_URL if set)
.\.venv\Scripts\python.exe main.py

# Frontend dev (optional second terminal)
cd frontend
npm run dev
# then set VITE_DEV_URL=http://127.0.0.1:5173 and restart main.py
```

## Architecture

```
React (frontend)  --pywebview.api-->  ApiBridge (backend/app)
                                           |
                     +---------------------+---------------------+
                     |                     |                     |
                   auth/                kontur/              services/
              cookies / Selenium      REST + signing      history / labels
```

- **No WMS embed / no ui_mobile.** Desktop only. CHZ HTTP bridge for WMS callbacks may remain under `backend/app`.
- **Legacy CustomTkinter** lives only in `archive/legacy_ui/` — do not extend it.
- **Git:** commit locally as you go; `git push` only after explicit user approval.

## Rules

- Design / visual laws: `.cursor/rules/design.mdc`
- Backend laws: `.cursor/rules/backend.mdc`
- Project overview: `.cursor/rules/project.mdc`

## Cookie auth (critical)

Working background collection (commit lineage `d647455` / `60bd74f`):

- Real Yandex profile (prefer `Vinsent O\`neal` or `KONTUR_YANDEX_PROFILE`)
- `HEADLESS = False` by default — do **not** rely on `--headless=new`
- Always `--window-position=-32000,-32000` + Win32 `SW_HIDE`
- Flow: fresh file cookies + live `/api/v1/user` → else profile cookies → else Selenium

## Screens (frontend routes)

| Route        | Purpose                          |
|--------------|----------------------------------|
| orders       | Order codes / queue / history    |
| chz          | WMS Honest Sign requests         |
| download     | Download marking codes           |
| intro        | Introduction into circulation    |
| tsd          | TSD tasks                        |
| aggregation  | Aggregation codes                |
| labels       | BarTender 100×180 / 100×136      |
