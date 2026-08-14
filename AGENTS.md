# Kontur API — project map for agents

Desktop app for Kontur.Markirovka: orders, code download, introduction into circulation, TSD tasks, aggregation, BarTender labels. WMS Honest Sign (CHZ) requests arrive via a local HTTP bridge (port 8791) — there is no CHZ screen in the UI.

## Layout

```
kontur_api/
  AGENTS.md / README.md / pyproject.toml / uv.lock / requirements.txt
  .env.example / .gitignore / mypy.ini / .flake8
  .github/workflows/ci.yml  # CI: backend unittest (Windows) + frontend typecheck/build (Ubuntu)
  main.py                   # thin desktop launcher → backend.app.desktop
  cookies.py                # thin shim → backend.auth (legacy imports)
  Update.bat / Install.bat / Install.exe  # operator entrypoints (in-place on the git clone)
  full_orders_history.json  # local order metadata (CSV paths, TSD flags); not synced between PCs
  backend/                  # Python business logic (see below)
  frontend/                 # React 19 + Vite + TS + Tailwind 4 + HeroUI 3; dist/ is COMMITTED deliberately
  archive/                  # legacy_ui (CustomTkinter), ui_v2_static, local_junk
  assets/                   # labels/ (100x180, 100x136 .btw), icons/kontur.ico
  data/                     # xlsx reference data
  driver/                   # yandexdriver.exe (not committed; auto-downloaded)
  runtime/                  # cookies, logs/, backups/, state/ (not committed)
  scripts/                  # install_windows / update_windows / build_installer / launchers/
  installer/                # Inno KonturMarkirovka.iss + payload staging
  tests/ / docs/
```

```
backend/
  app/       api_bridge.py (~6.9k lines — ApiBridge, the UI contract), desktop.py (pywebview shell),
             chz_bridge_server.py (HTTP :8791), server_only.py (bridge without a window),
             true_status_worker.py (status subprocess), bartender_label_formats.py
  auth/      service.py (cookie flow), browser.py (Selenium + window hiding/sweep),
             yandex_cookies.py (profile cookies), kontur_check.py (live /api/v1/user),
             store.py, prolongation.py, constants.py, paths.py
  kontur/    api.py (Kontur REST), cryptopro.py (CAdES signing), winhttp.py
  services/  history_db.py (git-branch sync), update.py (git updates), bartender_print.py,
             aggregation_bulk.py, queue_utils.py, logger.py, ...
```

## How to run (local)

```powershell
# Backend + desktop shell (loads frontend/dist, or VITE_DEV_URL if set)
.\.venv\Scripts\python.exe main.py

# Frontend dev (optional second terminal)
cd frontend
npm install
npm run dev
# then set VITE_DEV_URL=http://127.0.0.1:5173 in .env and restart main.py
```

## Updates & committed dist

- In-app updates: `check_for_updates` (git fetch, compare HEAD vs `origin/main`, polled every 5 min) → `apply_update` (`git merge --ff-only origin/main`), restart is manual.
- `frontend/dist` is committed **deliberately** — operator PCs get UI updates via the same git pull, no Node required. After frontend changes run `npm run build` and commit `dist` together with the sources.
- Other PCs: `git pull`, then `Install.bat` / `Install.exe` — stops the app, rebuilds `.venv`, driver, desktop shortcut (`pythonw` + `main.py`).
- `Update.bat` → `scripts/update_windows.ps1`: stash local changes, pull/reset `main`, then the same full reinstall.

## Windows installer

Operator surface in the repo root is only `Update.bat`, `Install.bat`, `Install.exe`. All three work **in the git clone folder** (they do not copy the app to `%LOCALAPPDATA%`). `Install.exe` is a tiny wrapper around `scripts/install_windows.ps1 -Reinstall`. Rebuild it with `scripts/build_install_exe.ps1`. Optional Inno payload stays under `dist/installer/` and is not published to the root.

## Architecture

```
React (frontend)  --pywebview.api-->  ApiBridge (backend/app)
WMS --HTTP :8791 (X-CHZ-Token)-->  chz_bridge_server --^
                                           |
                     +---------------------+---------------------+
                     |                     |                     |
                   auth/                kontur/              services/
              cookies / Selenium      REST + signing      history / labels / update
```

- **No WMS embed / no ui_mobile.** Desktop only. The CHZ HTTP bridge for WMS callbacks lives in `backend/app/chz_bridge_server.py` (also runs windowless via `server_only.py`, startup shortcut "CRPT server").
- **Order list:** live from Kontur. `full_orders_history.json` is a local cache of extras (CSV/PDF paths, TSD flags, deleted-archive) — not synced between PCs.
- **Legacy CustomTkinter** lives only in `archive/legacy_ui/` — do not extend it.
- **Git:** commit locally as you go; `git push` only after explicit user approval.
- Details and data flows: `docs/ARCHITECTURE.md`.

## CI

`.github/workflows/ci.yml` (push to `main`/`engineering-pass`, PRs):
- backend: `windows-latest`, Python 3.12, `pip install -r requirements.txt`, `python -m unittest discover -s tests -v`
- frontend: `ubuntu-latest`, Node 22, `npm ci`, `npx tsc --noEmit`, `npm run build`

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
- Stray-window sweep (`backend/auth/browser.py`): a parallel watcher hides windows of browser processes spawned during launch, and failed attempts terminate their orphaned `browser.exe` processes

## Screens (frontend routes)

| Route        | Purpose                          |
|--------------|----------------------------------|
| welcome      | Start screen (default route)     |
| orders       | Order codes / queue / history    |
| download     | Download marking codes           |
| intro        | Introduction into circulation    |
| tsd          | TSD tasks                        |
| aggregation  | Aggregation codes                |
| labels       | BarTender 100×180 / 100×136      |
