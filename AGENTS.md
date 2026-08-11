# Kontur API — project map for agents

Desktop app for Kontur.Markirovka: orders, code download, introduction into circulation, TSD tasks, aggregation, BarTender labels, WMS Honest Sign (CHZ) requests.

## Layout

```
kontur_api/
  AGENTS.md / README.md / pyproject.toml / uv.lock / requirements.txt
  .env.example / .gitignore / mypy.ini / .flake8
  main.py                   # thin desktop launcher → backend.app.desktop
  cookies.py                # thin shim → backend.auth (legacy imports)
  KonturMarkirovka.bat      # primary Windows launcher
  run_kontur.vbs            # silent launcher (desktop shortcuts)
  run_crpt_server.vbs       # thin stub → scripts/launchers/
  setup.bat / Build-Installer.bat / update.bat  # thin entrypoints → scripts/
  full_orders_history.json  # order history DB (path expected by history_db)
  backend/                  # Python business logic (auth, kontur, services, app)
  frontend/                 # React + Vite + TypeScript + Tailwind
  archive/                  # legacy_ui, ui_v2_static, local_junk
  assets/                   # labels/, icons/
  data/                     # xlsx reference data
  driver/                   # yandexdriver.exe
  runtime/                  # cookies, logs/, backups/, tmp (not committed)
  scripts/                  # install / update / launchers/
    launchers/              # run_crpt_server.vbs, run_kontur_v2.vbs
  installer/                # Inno / payload staging
  tests/ / docs/
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


## Windows installer

`Build-Installer.bat` -> `KonturMarkirovka-Setup.exe` appears in the **project root** (build cache: `dist\installer\`, payload staging: `installer\payload\`).

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
