# Kontur API

Windows-приложение для рабочих сценариев Контур.Маркировки: заказ кодов,
загрузка кодов, ввод в оборот, задания на ТСД, агрегация и печать этикеток.

В проекте поддерживаются два интерфейса:

- `KonturTestAPI` - актуальный интерфейс UI v2 на PyWebView.
- `KonturAPI` - старый интерфейс на CustomTkinter для резервных сценариев.

## Быстрый Старт

1. Склонируйте проект с GitHub.
2. Откройте папку проекта.
3. Запустите `setup.bat`.
4. После установки запускайте приложение ярлыком `KonturTestAPI`.

Установщик подготавливает Python 3.12, зависимости, `.env`, рабочие папки на
рабочем столе, Yandex Driver и ярлыки. BarTender и CryptoPro/CAdES должны быть
установлены отдельно, так как это внешнее лицензируемое ПО.

## Обновление

Для обновления установленного проекта используйте ярлык `Обновление` или:

```powershell
git pull origin main
uv sync --python 3.12 --frozen
```

## Ручной Запуск

```powershell
uv python install 3.12
uv sync --python 3.12 --frozen
.\.venv\Scripts\python.exe ui_v2\main_v2.py
```

Старый интерфейс:

```powershell
.\.venv\Scripts\python.exe main.pyw
```

## Структура Проекта

- `ui_v2/` - новый desktop-интерфейс, API bridge и frontend.
- `ui_mobile/` - мобильный интерфейс, который работает через сервер на ПК.
- `scripts/` - установщик, обновление, сборка и сервисные скрипты.
- `tests/` - unit-тесты и регрессионные проверки.
- `data/` - справочники и исходные таблицы.
- `BarTender/`, `Шаблоны BarTender/` - шаблоны печати этикеток.
- `docs/` - документация по архитектуре, разработке и эксплуатации.
- `api.py`, `cookies.py`, `cryptopro.py`, `history_db.py` - интеграционный слой.
- `bartender_*.py` - подготовка данных и отправка печати в BarTender.

## Рабочие Данные

Локальные логи, cookies, временные файлы, драйверы, runtime-кэш и установщики
не должны попадать в Git. Основные правила перечислены в `.gitignore`.

`full_orders_history.json` является рабочей синхронизируемой историей заказов.
Коммитьте его только осознанно, когда нужно обновить общую историю в репозитории.

## Проверки Для Разработки

```powershell
uv sync --python 3.12 --group dev
python -m py_compile ui_v2\api_bridge.py ui_v2\main_v2.py
python -m unittest tests.test_ui_v2_api_bridge tests.test_history_db_unit
node --check ui_v2\ui\app.js
```

Подробности: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).
