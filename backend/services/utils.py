import requests
from typing import Dict, Optional
from backend.services.logger import logger

# ---------------- helpers ----------------

def make_session_with_cookies(cookies: Optional[Dict[str, str]]) -> requests.Session:
    """Создаёт сессию requests с установленными cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    if cookies:
        for name, value in cookies.items():
            # Set for API host and parent domain — Kontur auth cookies are often
            # issued for .kontur.ru and must still attach to mk.kontur.ru calls.
            session.cookies.set(str(name), str(value), domain="mk.kontur.ru", path="/")
            session.cookies.set(str(name), str(value), domain=".kontur.ru", path="/")
    return session


def clone_session(session: requests.Session) -> requests.Session:
    """Копия сессии для потока: CookieJar с доменами, без get_dict()."""
    clone = requests.Session()
    clone.headers.update(session.headers)
    clone.cookies.update(session.cookies)
    return clone


def pluralize_ru(value: int, singular: str, few: str, many: str) -> str:
    """Подбирает русскую форму слова по числу: 1 пара, 2 пары, 5 пар."""
    remainder10 = value % 10
    remainder100 = value % 100
    if remainder10 == 1 and remainder100 != 11:
        return singular
    if remainder10 in (2, 3, 4) and remainder100 not in (12, 13, 14):
        return few
    return many


def get_tnved_code(simpl: str) -> str:
    """Возвращает TNVED код на основе ключевых слов в упрощённом названии."""
    simpl_lower = simpl.lower()
    if any(word in simpl_lower for word in ["хир", "микро", "ультра", "гинек", "дв пара"]):
        return "4015120001"
    return "4015120009"


def find_yandex_paths():
    """Compatibility wrapper — implementation lives in ``auth.paths``."""
    from backend.auth.paths import find_yandex_paths as _find_yandex_paths

    return _find_yandex_paths()

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
