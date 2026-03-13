import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from logger import logger

DEFAULT_HISTORY_FILE = "full_orders_history.json"
LEGACY_HISTORY_FILE = "orders_history.json"
LEGACY_NETWORK_HISTORY = r"\\192.168.100.2\!files\orders_history.json"


class OrderHistoryDB:
    def __init__(self, db_file: Optional[str] = None, legacy_db_files: Optional[Iterable[str]] = None):
        self.repo_root = Path(__file__).resolve().parent
        self.db_file = self._resolve_path(db_file or DEFAULT_HISTORY_FILE)
        self.legacy_db_files = self._build_legacy_paths(legacy_db_files)
        self._ensure_db_exists()
        self._migrate_legacy_history()

    def _resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _build_legacy_paths(self, legacy_db_files: Optional[Iterable[str]]) -> List[Path]:
        if legacy_db_files is None:
            candidates = [
                LEGACY_NETWORK_HISTORY,
                LEGACY_HISTORY_FILE,
            ]
        else:
            candidates = list(legacy_db_files)

        resolved_paths: List[Path] = []
        for candidate in candidates:
            path = self._resolve_path(candidate)
            if path != self.db_file and path not in resolved_paths:
                resolved_paths.append(path)
        return resolved_paths

    def _empty_data(self) -> Dict[str, Any]:
        now = datetime.now().isoformat()
        username = os.getenv("USERNAME", "unknown")
        return {
            "orders": [],
            "last_update": now,
            "created_by": username,
            "updated_by": username,
            "storage_path": str(self.db_file),
        }

    def _ensure_db_exists(self):
        try:
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            if not self.db_file.exists():
                self._write_data(self.db_file, self._empty_data())
                logger.info(f"Создана новая БД заказов: {self.db_file}")
        except Exception as e:
            logger.error(f"Ошибка создания БД заказов {self.db_file}: {e}")
            raise

    def _read_data(self, path: Path) -> Dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)
            if not isinstance(data, dict):
                raise ValueError("Некорректный формат БД заказов")
            data.setdefault("orders", [])
            return data
        except FileNotFoundError:
            return self._empty_data()
        except json.JSONDecodeError as e:
            logger.warning(f"Ошибка чтения JSON из {path}: {e}. Используется пустая БД.")
            return self._empty_data()
        except Exception as e:
            logger.error(f"Ошибка чтения БД заказов {path}: {e}")
            raise

    def _write_data(self, path: Path, data: Dict[str, Any]):
        payload = dict(data)
        payload["last_update"] = datetime.now().isoformat()
        payload["updated_by"] = os.getenv("USERNAME", "unknown")
        payload["storage_path"] = str(path)

        temp_file = path.with_suffix(path.suffix + ".tmp")
        with temp_file.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        temp_file.replace(path)

    def _load_data(self) -> Dict[str, Any]:
        data = self._read_data(self.db_file)
        data.setdefault("orders", [])
        return data

    def _save_data(self, data: Dict[str, Any]):
        self._write_data(self.db_file, data)

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        if not value or not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _pick_latest_timestamp(self, left: Any, right: Any) -> Any:
        left_dt = self._parse_timestamp(left)
        right_dt = self._parse_timestamp(right)

        if left_dt and right_dt:
            return left if left_dt >= right_dt else right
        return left or right

    def _pick_earliest_timestamp(self, left: Any, right: Any) -> Any:
        left_dt = self._parse_timestamp(left)
        right_dt = self._parse_timestamp(right)

        if left_dt and right_dt:
            return left if left_dt <= right_dt else right
        return left or right

    def _prepare_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        prepared = dict(order_data)
        prepared.setdefault("created_at", datetime.now().isoformat())
        prepared.setdefault("created_by", os.getenv("USERNAME", "unknown"))
        prepared.setdefault("tsd_created", False)
        prepared.setdefault("tsd_created_at", None)
        prepared.setdefault("tsd_intro_number", None)
        return prepared

    def _merge_order_records(self, current: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(current)

        for key, value in incoming.items():
            if value not in (None, "", [], {}):
                merged[key] = value
            else:
                merged.setdefault(key, value)

        merged["created_at"] = self._pick_earliest_timestamp(current.get("created_at"), incoming.get("created_at"))
        merged["created_by"] = current.get("created_by") or incoming.get("created_by")
        merged["tsd_created"] = bool(current.get("tsd_created") or incoming.get("tsd_created"))
        merged["tsd_created_at"] = self._pick_latest_timestamp(
            current.get("tsd_created_at"),
            incoming.get("tsd_created_at"),
        )
        merged["tsd_intro_number"] = incoming.get("tsd_intro_number") or current.get("tsd_intro_number")
        merged["tsd_created_by"] = incoming.get("tsd_created_by") or current.get("tsd_created_by")

        return merged

    def _sort_orders(self, orders: List[Dict[str, Any]]):
        orders.sort(
            key=lambda order: self._parse_timestamp(order.get("created_at")) or datetime.min,
            reverse=True,
        )

    def _upsert_order_in_data(self, data: Dict[str, Any], order_data: Dict[str, Any]) -> bool:
        document_id = order_data.get("document_id")
        if not document_id:
            logger.warning("Пропущена запись истории без document_id")
            return False

        prepared = self._prepare_order(order_data)
        orders = data.setdefault("orders", [])

        for index, order in enumerate(orders):
            if order.get("document_id") == document_id:
                merged = self._merge_order_records(order, prepared)
                if merged != order:
                    orders[index] = merged
                    self._sort_orders(orders)
                    return True
                return False

        orders.append(prepared)
        self._sort_orders(orders)
        return True

    def _migrate_legacy_history(self):
        data = self._load_data()
        changed = False

        for legacy_path in self.legacy_db_files:
            if not legacy_path.exists():
                continue

            try:
                legacy_data = self._read_data(legacy_path)
            except Exception as e:
                logger.warning(f"Не удалось прочитать старую историю {legacy_path}: {e}")
                continue

            migrated = 0
            for legacy_order in legacy_data.get("orders", []):
                if self._upsert_order_in_data(data, legacy_order):
                    migrated += 1

            if migrated:
                changed = True
                logger.info(f"Перенесено {migrated} записей из {legacy_path} в {self.db_file}")

        if changed:
            self._save_data(data)

    def add_order(self, order_data: Dict[str, Any]):
        """Добавляет новый заказ в историю или обновляет существующий."""
        try:
            data = self._load_data()
            if self._upsert_order_in_data(data, order_data):
                self._save_data(data)
                logger.info(f"✅ История обновлена для заказа: {order_data.get('document_id')}")
            else:
                logger.info(f"Заказ {order_data.get('document_id')} уже актуален в истории")
        except Exception as e:
            logger.error(f"Ошибка добавления заказа {order_data.get('document_id')}: {e}")

    def mark_tsd_created(self, document_id: str, intro_number: str):
        """Помечает заказ как отправленный на ТСД."""
        try:
            data = self._load_data()

            updated = False
            for order in data["orders"]:
                if order.get("document_id") == document_id:
                    order["tsd_created"] = True
                    order["tsd_created_at"] = datetime.now().isoformat()
                    order["tsd_intro_number"] = intro_number
                    order["tsd_created_by"] = os.getenv("USERNAME", "unknown")
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
        """Возвращает заказы без ТСД (новые сверху)."""
        try:
            data = self._load_data()
            orders = [order for order in data["orders"] if not order.get("tsd_created", False)]
            self._sort_orders(orders)
            logger.info(f"Найдено {len(orders)} заказов без ТСД")
            return orders
        except Exception as e:
            logger.error(f"Ошибка получения заказов без ТСД: {e}")
            return []

    def get_all_orders(self) -> List[Dict[str, Any]]:
        """Возвращает все заказы (новые сверху)."""
        try:
            data = self._load_data()
            orders = list(data["orders"])
            self._sort_orders(orders)
            logger.info(f"Загружено {len(orders)} заказов из {self.db_file}")
            return orders
        except Exception as e:
            logger.error(f"Ошибка получения всех заказов: {e}")
            return []

    def get_order_by_document_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Находит заказ по document_id."""
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
        """Возвращает информацию о БД."""
        try:
            data = self._load_data()
            return {
                "file_path": str(self.db_file),
                "total_orders": len(data["orders"]),
                "orders_without_tsd": len([order for order in data["orders"] if not order.get("tsd_created", False)]),
                "last_update": data.get("last_update"),
                "file_exists": self.db_file.exists(),
                "file_size": self.db_file.stat().st_size if self.db_file.exists() else 0,
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о БД: {e}")
            return {"file_path": str(self.db_file), "error": str(e)}
