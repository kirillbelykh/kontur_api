from pathlib import Path
import requests
import json
from typing import Dict, Optional
from dataclasses import asdict
from datetime import datetime
from logger import logger

COOKIES_FILE = Path("cookies.json")
# ---------------- helpers ----------------
def load_cookies() -> Optional[Dict[str, str]]:
    if COOKIES_FILE.exists():
        try:
            data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass
    return None

def make_session_with_cookies(cookies: Optional[Dict[str, str]]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    if cookies:
        for k, v in cookies.items():
            s.cookies.set(k, v, domain="mk.kontur.ru", path="/")
    return s

def get_tnved_code(simpl: str):
    simpl_lower = simpl.lower()
    if any(word in simpl_lower for word in ["хир", "микро", "ультра", "гинек", "дв пара"]):
        return "4015120001"
    else:
        return "4015120009"
    
def save_order_history(order_items):
    """Сохраняет информацию о заказах в текстовый файл 'История заказов.txt'"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open("История заказов.txt", "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"Заказ от: {timestamp}\n")
            f.write(f"{'='*80}\n")
            
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
                f.write("-" * 50 + "\n")
            
            f.write(f"Итого позиций: {len(order_items)}\n")
            total_codes = sum(int(getattr(item, 'codes_count', 0)) for item in order_items)
            f.write(f"Общее количество кодов: {total_codes}\n")
        
        logger.info(f"История заказа сохранена в файл 'История заказов.txt'")
        
    except Exception as e:
        logger.error(f"Не удалось сохранить историю заказа: {e}")
    
def save_snapshot(to_process) -> bool:
    try:
        snapshot = []
        for x in to_process:
            d = asdict(x)
            d["_uid"] = getattr(x, "_uid", None)
            snapshot.append(d)
        with open("last_snapshot.json", "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)    
    except Exception as e:
        logger.error("Не удалось сохранить snapshot в json")
        