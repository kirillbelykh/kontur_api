# Kontur Go Workbench

Альтернативный desktop-клиент рядом с текущим Python-приложением.

Что уже есть:

- отдельный `Wails + React` shell в `go-app/`;
- shared history через тот же `full_orders_history.json`;
- перенесённые Go-пакеты для `history`, `aggregation`, `orders`, `downloads`, `introduction`, `tsd`, `queue`, `system`, `session`;
- UI привязан ко всем текущим `uiapi` backend-операциям: `AppState`, `Auth`, `Orders`, `Downloads`, `Introduction`, `TSD`, `Aggregation`, `History`, `System`;
- unit-тесты на чистую бизнес-логику;
- Windows adapters для `chromedp` и `go-ole` вынесены отдельно через build tags;
- добавлен Windows installer pipeline на `NSIS`.

Быстрый запуск:

```bash
cd go-app
go test ./...
cd frontend
npm install
npm run build
cd ..
wails dev
```

Сборка Windows installer:

Требования:

- `Go`
- `Node.js` и `npm`
- `Wails CLI`
- `NSIS` (`makensis`) на Windows

Команда:

```powershell
cd go-app
.\scripts\build_windows_installer.ps1 -Version 0.1.0
```

Короткий wrapper:

```cmd
cd go-app
scripts\build_windows_installer.cmd
```

Что делает скрипт:

- при необходимости ставит frontend-зависимости;
- собирает frontend;
- собирает `Wails` binary для `windows/amd64`;
- запускает `NSIS` и выпускает installer.

Результат:

- executable: `go-app/build/bin/KonturGoWorkbench.exe`
- installer: `go-app/build/installer/KonturGoWorkbench-Setup-<version>.exe`

Ограничения текущего этапа:

- Windows-specific session/certificate adapters не проверялись на macOS-хосте;
- нужен реальный Windows smoke-test для `Yandex Browser + CryptoPro + installer`;
- production rollout без такого smoke-test считать завершённым нельзя.
