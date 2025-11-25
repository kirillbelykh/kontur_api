import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from logger import logger

class OrderHistoryDB:
    def __init__(self, db_file: str = None):
        # Указываем сетевой путь
        self.db_file = db_file or r"\\192.168.100.2\!files\orders_history.json"
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Создает файл БД если его нет в сетевой папке"""
        try:
            # Создаем директорию если её нет (для сетевого пути)
            db_dir = os.path.dirname(self.db_file)
            if db_dir and not os.path.exists(db_dir):
                try:
                    os.makedirs(db_dir, exist_ok=True)
                except PermissionError:
                    logger.warning(f"Нет прав на создание директории {db_dir}")
            
            if not os.path.exists(self.db_file):
                initial_data = {
                    "orders": [], 
                    "last_update": datetime.now().isoformat(),
                    "created_by": os.getenv('USERNAME', 'unknown'),
                    "network_path": self.db_file
                }
                with open(self.db_file, 'w', encoding='utf-8') as f:
                    json.dump(initial_data, f, ensure_ascii=False, indent=2)
                logger.info(f"Создана новая БД заказов: {self.db_file}")
                
        except Exception as e:
            logger.error(f"Ошибка создания БД в сетевой папке {self.db_file}: {e}")
            # Фолбэк на локальную папку если сетевая недоступна
            local_fallback = "orders_history.json"
            if self.db_file != local_fallback:
                logger.info(f"Использую локальную БД: {local_fallback}")
                self.db_file = local_fallback
                if not os.path.exists(self.db_file):
                    with open(self.db_file, 'w', encoding='utf-8') as f:
                        json.dump({"orders": [], "last_update": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
    
    def _load_data(self) -> Dict[str, Any]:
        """Загружает данные из JSON файла с обработкой сетевых ошибок"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data
                
            except (json.JSONDecodeError, FileNotFoundError) as e:
                if attempt == max_retries - 1:  # Последняя попытка
                    logger.warning(f"Ошибка загрузки {self.db_file}: {e}, создаем новую БД")
                    self._ensure_db_exists()
                    return {"orders": [], "last_update": datetime.now().isoformat()}
                continue
                
            except PermissionError as e:
                logger.error(f"Нет прав доступа к {self.db_file}: {e}")
                raise
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Сетевая ошибка доступа к {self.db_file}: {e}")
                    # Пробуем локальную БД как запасной вариант
                    local_fallback = "orders_history.json"
                    if self.db_file != local_fallback and os.path.exists(local_fallback):
                        logger.info(f"Использую локальную БД: {local_fallback}")
                        self.db_file = local_fallback
                        return self._load_data()
                    raise
                continue
    
    def _save_data(self, data: Dict[str, Any]):
        """Сохраняет данные в JSON файл с обработкой сетевых ошибок"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                data["last_update"] = datetime.now().isoformat()
                data["updated_by"] = os.getenv('USERNAME', 'unknown')
                data["network_path"] = self.db_file
                
                # Временный файл для атомарной записи
                temp_file = self.db_file + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                # Заменяем оригинальный файл
                os.replace(temp_file, self.db_file)
                return
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Ошибка сохранения в {self.db_file}: {e}")
                    # Пробуем сохранить локально как запасной вариант
                    local_fallback = "orders_history.json"
                    if self.db_file != local_fallback:
                        logger.info(f"Сохраняю в локальную БД: {local_fallback}")
                        original_path = self.db_file
                        self.db_file = local_fallback
                        try:
                            self._save_data(data)
                            # Восстанавливаем оригинальный путь для следующих операций
                            self.db_file = original_path
                        except Exception as fallback_error:
                            logger.error(f"Ошибка сохранения в локальную БД: {fallback_error}")
                            self.db_file = original_path
                            raise e
                    else:
                        raise
                continue
    
    def add_order(self, order_data: Dict[str, Any]):
        """Добавляет новый заказ в историю"""
        try:
            data = self._load_data()
            
            # Проверяем нет ли уже заказа с таким document_id
            for order in data["orders"]:
                if order.get("document_id") == order_data.get("document_id"):
                    logger.info(f"Заказ {order_data.get('document_id')} уже существует в истории")
                    return  # Уже существует
            
            # Добавляем метаданные
            order_data["created_at"] = datetime.now().isoformat()
            order_data["created_by"] = os.getenv('USERNAME', 'unknown')
            order_data["tsd_created"] = False
            order_data["tsd_created_at"] = None
            order_data["tsd_intro_number"] = None
            
            data["orders"].insert(0, order_data)  # Новые сверху
            self._save_data(data)
            logger.info(f"✅ Добавлен новый заказ: {order_data.get('document_id')}")
            
        except Exception as e:
            logger.error(f"Ошибка добавления заказа {order_data.get('document_id')}: {e}")
    
    def mark_tsd_created(self, document_id: str, intro_number: str):
        """Помечает заказ как отправленный на ТСД"""
        try:
            data = self._load_data()
            
            updated = False
            for order in data["orders"]:
                if order.get("document_id") == document_id:
                    order["tsd_created"] = True
                    order["tsd_created_at"] = datetime.now().isoformat()
                    order["tsd_intro_number"] = intro_number
                    order["tsd_created_by"] = os.getenv('USERNAME', 'unknown')
                    updated = True
                    break
            
            if updated:
                self._save_data(data)
                logger.info(f"✅ Заказ {document_id} помечен как отправленный на ТСД")
            else:
                logger.warning(f"⚠️ Заказ {document_id} не найден в истории")
                
        except Exception as e:
            logger.error(f"Ошибка обновления статуса ТСД для заказа {document_id}: {e}")
    
    def get_orders_without_tsd(self) -> List[Dict[str, Any]]:
        """Возвращает заказы без ТСД (новые сверху)"""
        try:
            data = self._load_data()
            orders = [order for order in data["orders"] if not order.get("tsd_created", False)]
            logger.info(f"Найдено {len(orders)} заказов без ТСД")
            return orders
        except Exception as e:
            logger.error(f"Ошибка получения заказов без ТСД: {e}")
            return []
    
    def get_all_orders(self) -> List[Dict[str, Any]]:
        """Возвращает все заказы (новые сверху)"""
        try:
            data = self._load_data()
            logger.info(f"Загружено {len(data['orders'])} заказов из {self.db_file}")
            return data["orders"]
        except Exception as e:
            logger.error(f"Ошибка получения всех заказов: {e}")
            return []
    
    def get_order_by_document_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Находит заказ по document_id"""
        try:
            data = self._load_data()
            for order in data["orders"]:
                if order.get("document_id") == document_id:
                    return order
            logger.info(f"Заказ {document_id} не найден")
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска заказа {document_id}: {e}")
            return None
    
    def get_db_info(self) -> Dict[str, Any]:
        """Возвращает информацию о БД"""
        try:
            data = self._load_data()
            return {
                "file_path": self.db_file,
                "total_orders": len(data["orders"]),
                "orders_without_tsd": len(self.get_orders_without_tsd()),
                "last_update": data.get("last_update"),
                "file_exists": os.path.exists(self.db_file),
                "file_size": os.path.getsize(self.db_file) if os.path.exists(self.db_file) else 0
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о БД: {e}")
            return {"file_path": self.db_file, "error": str(e)}