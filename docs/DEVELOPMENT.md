# Разработка

## Среда

```powershell
uv python install 3.12
uv sync --python 3.12 --group dev
cd frontend
npm install
```

## Запуск

```powershell
.\.venv\Scripts\python.exe main.py     # окно pywebview, грузит frontend/dist
```

Дев-режим фронта: `npm run dev` во втором терминале, в `.env` раскомментировать
`VITE_DEV_URL=http://127.0.0.1:5173`, перезапустить `main.py`.

## Проверки (те же, что в CI)

```powershell
python -m unittest discover -s tests -v      # backend (нужен Windows)
cd frontend
npx tsc --noEmit
npm run build
```

CI: `.github/workflows/ci.yml` — backend на `windows-latest`
(Python 3.12, `requirements.txt`), frontend на `ubuntu-latest` (Node 22).
Гоняется на push в `main` / `engineering-pass` и на PR.
Линтеры настроены в `.flake8` и `mypy.ini`.

## Git-гигиена

- Не коммитьте `.env`, `runtime/`, `driver/`, cookies и логи.
- `full_orders_history.json` синхронизируется автоматически через ветку
  `orders-history` — не коммитьте его в `main` без осознанной причины.
- `frontend/dist` коммитится сознательно (обновления уезжают на рабочие ПК
  через git pull): после изменений фронта выполните `npm run build` и
  закоммитьте `dist` вместе с исходниками.
- Методы `ApiBridge` — контракт для UI: не меняйте сигнатуры без
  одновременного обновления фронта.
- `git push` — только по явной просьбе владельца.

## Код

- Новые функции — с type hints.
- Сетевые запросы к Контуру не должны блокировать интерфейс без видимого
  прогресса.
- Тексты интерфейса — UTF-8, следите за mojibake.
