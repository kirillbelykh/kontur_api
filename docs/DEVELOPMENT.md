# Разработка

## Среда

```powershell
uv python install 3.12
uv sync --python 3.12 --group dev
```

Основной запуск:

```powershell
.\.venv\Scripts\python.exe ui_v2\main_v2.py
```

## Проверки

Перед пушем запускайте минимум:

```powershell
python -m py_compile ui_v2\api_bridge.py ui_v2\main_v2.py
node --check ui_v2\ui\app.js
python -m unittest tests.test_ui_v2_api_bridge tests.test_history_db_unit
```

Если менялись утилиты верхнего уровня, добавьте их в `py_compile` или точечные
unit-тесты.

## Git-Гигиена

- Не коммитьте `.env`, cookies, логи, драйверы и временные файлы.
- Не коммитьте `full_orders_history.json`, если это не осознанное обновление
  общей истории заказов.
- Изменения шаблонов BarTender коммитьте только после ручной проверки печати.
- Не удаляйте пользовательские рабочие файлы командой очистки без проверки.

## Код

- Новые функции пишите с type hints.
- Для неочевидной бизнес-логики добавляйте короткие docstring или комментарий.
- Сетевые запросы к Контуру не должны блокировать интерфейс без видимого
  прогресса.
- Тексты интерфейса храните в UTF-8 и проверяйте, что в коде нет mojibake.
