from pathlib import Path
import os
import json
import requests
import winreg
from typing import Dict, Optional
from dataclasses import asdict
from datetime import datetime
from logger import logger

COOKIES_FILE = Path("cookies.json")

# ---------------- helpers ----------------

def load_cookies() -> Optional[Dict[str, str]]:
    """Загружает cookies из файла, если они существуют и корректны."""
    if not COOKIES_FILE.exists():
        return None
    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data:
            return data
    except Exception as e:
        logger.error(f"Ошибка при загрузке cookies: {e}")
    return None


def make_session_with_cookies(cookies: Optional[Dict[str, str]]) -> requests.Session:
    """Создаёт сессию requests с установленными cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    if cookies:
        for k, v in cookies.items():
            session.cookies.set(k, v, domain="mk.kontur.ru", path="/")
    return session


def get_tnved_code(simpl: str) -> str:
    """Возвращает TNVED код на основе ключевых слов в упрощённом названии."""
    simpl_lower = simpl.lower()
    if any(word in simpl_lower for word in ["хир", "микро", "ультра", "гинек", "дв пара"]):
        return "4015120001"
    return "4015120009"


def save_order_history(order_items) -> Optional[str]:
    """
    Сохраняет историю заказов в папке 'История заказов КМ' на рабочем столе.
    Возвращает путь к файлу или None при ошибке.
    """
    try:
        desktop_path = Path.home() / "Desktop"
        history_folder = desktop_path / "История заказов КМ"
        history_folder.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"Заказ_{timestamp}.txt"
        file_path = history_folder / filename
        timestamp_display = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"{'='*80}\n")
            f.write(f"Заказ от: {timestamp_display}\n")
            f.write(f"{'='*80}\n\n")

            for i, item in enumerate(order_items, 1):
                f.write(f"Позиция #{i}:\n")
                f.write(f"  Номер заказа: {getattr(item, 'order_name', 'Не указан')}\n")
                f.write(f"  Упрощенное название: {getattr(item, 'simpl_name', 'Не указано')}\n")
                f.write(f"  Размер: {getattr(item, 'size', 'Не указан')}\n")
                f.write(f"  Кол-во в упаковке: {getattr(item, 'units_per_pack', 'Не указано')}\n")
                f.write(f"  Кол-во кодов: {getattr(item, 'codes_count', 'Не указано')}\n")
                f.write(f"  GTIN: {getattr(item, 'gtin', 'Не указан')}\n")
                f.write(f"  Полное наименование: {getattr(item, 'full_name', 'Не указано')}\n")
                f.write(f"  Код ТН ВЭД: {getattr(item, 'tnved_code', 'Не указан')}\n")
                f.write(f"  Тип КМ: {getattr(item, 'cisType', 'Не указан')}\n")
                f.write(f"  UID: {getattr(item, '_uid', 'Не указан')}\n")
                f.write("-"*50 + "\n")

            f.write(f"\nИтого позиций: {len(order_items)}\n")
            total_codes = sum(int(getattr(item, 'codes_count', 0)) for item in order_items)
            f.write(f"Общее количество кодов: {total_codes}\n")

        return str(file_path)

    except Exception as e:
        logger.error(f"Не удалось сохранить историю заказа: {e}")
        return None


def save_snapshot(to_process) -> bool:
    """Сохраняет последний снимок списка заказов в JSON."""
    try:
        snapshot = []
        for x in to_process:
            d = asdict(x)
            d["_uid"] = getattr(x, "_uid", None)
            snapshot.append(d)
        with open("last_snapshot.json", "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Не удалось сохранить snapshot: {e}")
        return False


def find_yandex_paths() -> Dict[str, Optional[Path]]:
    """
    Автоматически находит пути Яндекс Браузера и пользовательских данных.
    Возвращает словарь с ключами 'browser', 'user_data', 'profile'.
    """
    paths = {
        'browser': None,
        'user_data': None,
        'profile': "Vinsent O`neal"
    }

    # Поиск браузера через реестр
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                            r"Software\Classes\YandexBrowserHTML\shell\open\command") as key:
            value = winreg.QueryValue(key, "")
            if value:
                browser_path = value.split('"')[1] if '"' in value else value.split()[0]
                paths['browser'] = Path(browser_path)
    except Exception:
        pass

    # Альтернативные пути поиска браузера
    if not paths['browser'] or not paths['browser'].exists():
        possible_browser_paths = [
            Path(os.environ.get('LOCALAPPDATA', '')) / "Yandex/YandexBrowser/Application/browser.exe",
            Path(os.environ.get('PROGRAMFILES', '')) / "Yandex/YandexBrowser/Application/browser.exe",
            Path(os.environ.get('PROGRAMFILES(X86)', '')) / "Yandex/YandexBrowser/Application/browser.exe",
        ]
        for browser_path in possible_browser_paths:
            if browser_path.exists():
                paths['browser'] = browser_path
                break

    # Поиск папки с пользовательскими данными
    if paths['browser'] and paths['browser'].exists():
        user_data_paths = [
            Path(os.environ.get('LOCALAPPDATA', '')) / "Yandex/YandexBrowser/User Data/Default",
            paths['browser'].parent.parent / "User Data/Default",
        ]
        for user_data_path in user_data_paths:
            if user_data_path.exists():
                paths['user_data'] = user_data_path
                break

    # Если папка с данными не найдена, создаём путь по умолчанию
    if paths['browser'] and not paths['user_data']:
        default_user_data = Path(os.environ.get('LOCALAPPDATA', '')) / "Yandex/YandexBrowser/User Data/Default"
        paths['user_data'] = default_user_data

    return paths

def process_csv_file(csv_path):
    """
    Обрабатывает CSV-файл: очищает первый столбец от кавычек и добавляет префикс ^1
    """
    try:
        temp_file = csv_path + ".tmp"
        
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
            
            for line in infile:
                # Разделяем строку по табуляции
                parts = line.strip().split('\t')
                
                if len(parts) >= 3:
                    # Обрабатываем первый столбец
                    first_col = parts[0]
                    
                    # Удаляем кавычки в начале и конце, если есть
                    first_col = first_col.strip('"')
                    
                    # Заменяем двойные кавычки на одинарные внутри строки
                    first_col = first_col.replace('""', '"')
                    
                    # Добавляем префикс ^1
                    formatted_first_col = f"^1{first_col}"
                    
                    # Формируем новую строку
                    new_line = f"{formatted_first_col}\t{parts[1]}\t{parts[2]}"
                    outfile.write(new_line + '\n')
                else:
                    # Если строка не соответствует ожидаемому формату, записываем как есть
                    outfile.write(line)
        
        # Заменяем оригинальный файл обработанным
        import shutil
        shutil.move(temp_file, csv_path)
        logger.info(f"CSV файл обработан: {csv_path}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обработке CSV файла {csv_path}: {e}")
        # Удаляем временный файл в случае ошибки
        import os
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False