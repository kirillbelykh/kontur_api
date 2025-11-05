import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from logger import logger
class OrderHistoryDB:
    def __init__(self, db_file: str = "orders_history.json"):
        self.db_file = db_file
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Создает файл БД если его нет"""
        if not os.path.exists(self.db_file):
            with open(self.db_file, 'w', encoding='utf-8') as f:
                json.dump({"orders": [], "last_update": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
    
    def _load_data(self) -> Dict[str, Any]:
        """Загружает данные из JSON файла"""
        try:
            with open(self.db_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"orders": [], "last_update": datetime.now().isoformat()}
    
    def _save_data(self, data: Dict[str, Any]):
        """Сохраняет данные в JSON файл"""
        data["last_update"] = datetime.now().isoformat()
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_order_by_document_id(self, document_id):
        """Получает заказ по document_id из истории"""
        try:
            self.cursor.execute("SELECT * FROM orders WHERE document_id = ?", (document_id,))
            row = self.cursor.fetchone()
            if row:
                return self._row_to_dict(row)
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении заказа по document_id {document_id}: {e}")
            return None
        
    def add_order(self, order_data: Dict[str, Any]):
        """Добавляет новый заказ в историю"""
        data = self._load_data()
        
        # Проверяем нет ли уже заказа с таким document_id
        for order in data["orders"]:
            if order.get("document_id") == order_data.get("document_id"):
                return  # Уже существует
        
        # Добавляем метаданные
        order_data["created_at"] = datetime.now().isoformat()
        order_data["tsd_created"] = False
        order_data["tsd_created_at"] = None
        order_data["tsd_intro_number"] = None
        
        data["orders"].insert(0, order_data)  # Новые сверху
        self._save_data(data)
    
    # В классе OrderHistoryDB
    def mark_tsd_created(self, document_id: str, intro_number: str):
        """Помечает заказ как отправленный на ТСД"""
        data = self._load_data()
        
        updated = False
        for order in data["orders"]:
            if order.get("document_id") == document_id:
                order["tsd_created"] = True
                order["tsd_created_at"] = datetime.now().isoformat()
                order["tsd_intro_number"] = intro_number
                updated = True
                break
        
        if updated:
            self._save_data(data)
            print(f"✅ Заказ {document_id} помечен как отправленный на ТСД")
        else:
            print(f"⚠️ Заказ {document_id} не найден в истории")
    
    def get_orders_without_tsd(self) -> List[Dict[str, Any]]:
        """Возвращает заказы без ТСД (новые сверху)"""
        data = self._load_data()
        return [order for order in data["orders"] if not order.get("tsd_created", False)]
    
    def get_all_orders(self) -> List[Dict[str, Any]]:
        """Возвращает все заказы (новые сверху)"""
        data = self._load_data()
        return data["orders"]
    
    def get_order_by_document_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Находит заказ по document_id"""
        data = self._load_data()
        for order in data["orders"]:
            if order.get("document_id") == document_id:
                return order
        return None