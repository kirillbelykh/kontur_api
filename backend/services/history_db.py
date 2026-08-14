import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from backend.services.logger import logger

DEFAULT_HISTORY_FILE = "full_orders_history.json"
LEGACY_HISTORY_FILE = "orders_history.json"
LEGACY_NETWORK_HISTORY = r"\\192.168.100.2\!files\orders_history.json"
LEGACY_NETWORK_ENABLED_ENV = "HISTORY_ENABLE_LEGACY_NETWORK"


class OrderHistoryDB:
    """Локальный JSON с метаданными заказов (пути к CSV, флаги ТСД).

    Список заказов берётся из Контура. Git-ветка ``orders-history`` больше
    не используется — ``sync_with_github`` / ``flush_github_sync`` оставлены
    как no-op для старых вызовов.
    """

    _io_lock = threading.RLock()

    def __init__(
        self,
        db_file: Optional[str] = None,
        legacy_db_files: Optional[Iterable[str]] = None,
        sync_enabled: Optional[bool] = None,
        sync_branch: Optional[str] = None,
        startup_sync: str = "none",
    ):
        del sync_enabled, sync_branch, startup_sync
        self.repo_root = Path(__file__).resolve().parents[2]
        self.db_file = self._resolve_path(db_file or DEFAULT_HISTORY_FILE)
        self.legacy_db_files = self._build_legacy_paths(legacy_db_files)
        self.sync_enabled = False
        self.sync_branch = ""
        # (cache_key, data) — parsed history JSON keyed by file mtime+size
        self._data_cache: Optional[Tuple[Tuple[int, int], Dict[str, Any]]] = None
        self._legacy_warning_keys: set[Tuple[str, str, str]] = set()
        self._last_logged_total_orders: Optional[int] = None
        self._last_logged_without_tsd: Optional[int] = None

        self._ensure_db_exists()
        self._migrate_legacy_history()

    def _resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _build_legacy_paths(self, legacy_db_files: Optional[Iterable[str]]) -> List[Path]:
        if legacy_db_files is None:
            candidates = [LEGACY_HISTORY_FILE]
            enable_network_legacy = os.getenv(LEGACY_NETWORK_ENABLED_ENV, "0").strip().lower()
            if enable_network_legacy in {"1", "true", "yes", "on"}:
                candidates.insert(0, LEGACY_NETWORK_HISTORY)
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

        path.parent.mkdir(parents=True, exist_ok=True)
        temp_file = path.with_suffix(path.suffix + ".tmp")
        with temp_file.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        temp_file.replace(path)

    def _data_cache_key(self) -> Optional[Tuple[int, int]]:
        try:
            stat = self.db_file.stat()
            return (stat.st_mtime_ns, stat.st_size)
        except OSError:
            return None

    def _load_data(self) -> Dict[str, Any]:
        """Читает историю с кэшем по mtime — файл на 2+ МБ парсится не на каждый вызов."""
        cache_key = self._data_cache_key()
        cached = self._data_cache
        if cache_key is not None and cached is not None and cached[0] == cache_key:
            data = cached[1]
            # ponytail: shallow per-order copies — записи истории плоские; если появятся
            # вложенные изменяемые поля, перейти на copy.deepcopy
            return {**data, "orders": [dict(order) for order in data.get("orders", [])]}

        data = self._read_data(self.db_file)
        data.setdefault("orders", [])
        if cache_key is not None:
            self._data_cache = (cache_key, {**data, "orders": [dict(order) for order in data["orders"]]})
        return data

    def _save_data(self, data: Dict[str, Any]):
        self._write_data(self.db_file, data)
        self._data_cache = None

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

    def _is_empty_value(self, value: Any) -> bool:
        return value in (None, "", [], {})

    def _prefer_incoming_record(self, current: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        current_updated = self._parse_timestamp(current.get("updated_at"))
        incoming_updated = self._parse_timestamp(incoming.get("updated_at"))

        if current_updated and incoming_updated:
            return incoming_updated >= current_updated
        if incoming_updated and not current_updated:
            return True
        return False

    def _prepare_order(
        self,
        order_data: Dict[str, Any],
        *,
        assign_create_metadata: bool = True,
        assign_update_metadata: bool = True,
    ) -> Dict[str, Any]:
        now = datetime.now().isoformat()
        prepared = dict(order_data)
        if assign_create_metadata:
            prepared.setdefault("created_at", now)
            prepared.setdefault("created_by", os.getenv("USERNAME", "unknown"))
        if assign_update_metadata:
            prepared.setdefault("updated_at", now)
            prepared.setdefault("updated_by", os.getenv("USERNAME", "unknown"))
        prepared.setdefault("tsd_created", False)
        prepared.setdefault("tsd_created_at", None)
        prepared.setdefault("tsd_intro_number", None)
        prepared.setdefault("tsd_created_by", None)
        return prepared

    def _warn_legacy_once(self, legacy_path: Path, stage: str, error: Exception):
        warning_key = (str(legacy_path), stage, str(error))
        if warning_key in self._legacy_warning_keys:
            return
        self._legacy_warning_keys.add(warning_key)
        logger.warning("Не удалось %s старую историю %s: %s", stage, legacy_path, error)

    def _merge_order_records(self, current: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(current)
        prefer_incoming = self._prefer_incoming_record(current, incoming)
        if not self._parse_timestamp(incoming.get("updated_at")):
            prefer_incoming = True

        for key in set(current.keys()) | set(incoming.keys()):
            current_value = current.get(key)
            incoming_value = incoming.get(key)

            if self._is_empty_value(incoming_value):
                merged[key] = current_value
                continue
            if self._is_empty_value(current_value):
                merged[key] = incoming_value
                continue
            if current_value == incoming_value:
                merged[key] = current_value
                continue

            merged[key] = incoming_value if prefer_incoming else current_value

        merged["created_at"] = self._pick_earliest_timestamp(current.get("created_at"), incoming.get("created_at"))
        merged["created_by"] = current.get("created_by") or incoming.get("created_by")
        merged["updated_at"] = self._pick_latest_timestamp(current.get("updated_at"), incoming.get("updated_at"))
        merged["updated_by"] = incoming.get("updated_by") if prefer_incoming else current.get("updated_by")
        merged["updated_by"] = merged["updated_by"] or current.get("updated_by") or incoming.get("updated_by")
        merged["tsd_created"] = bool(current.get("tsd_created") or incoming.get("tsd_created"))
        merged["tsd_created_at"] = self._pick_latest_timestamp(
            current.get("tsd_created_at"),
            incoming.get("tsd_created_at"),
        )

        current_tsd_dt = self._parse_timestamp(current.get("tsd_created_at"))
        incoming_tsd_dt = self._parse_timestamp(incoming.get("tsd_created_at"))
        if incoming_tsd_dt and (not current_tsd_dt or incoming_tsd_dt >= current_tsd_dt):
            merged["tsd_intro_number"] = incoming.get("tsd_intro_number") or current.get("tsd_intro_number")
            merged["tsd_created_by"] = incoming.get("tsd_created_by") or current.get("tsd_created_by")
        else:
            merged["tsd_intro_number"] = current.get("tsd_intro_number") or incoming.get("tsd_intro_number")
            merged["tsd_created_by"] = current.get("tsd_created_by") or incoming.get("tsd_created_by")

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

        orders = data.setdefault("orders", [])

        for index, order in enumerate(orders):
            if order.get("document_id") == document_id:
                prepared = self._prepare_order(
                    order_data,
                    assign_create_metadata=False,
                    assign_update_metadata=False,
                )
                merged = self._merge_order_records(order, prepared)
                if merged != order:
                    if not prepared.get("updated_at"):
                        merged["updated_at"] = datetime.now().isoformat()
                        merged["updated_by"] = os.getenv("USERNAME", "unknown")
                    orders[index] = merged
                    self._sort_orders(orders)
                    return True
                return False

        prepared = self._prepare_order(
            order_data,
            assign_create_metadata=True,
            assign_update_metadata=True,
        )
        orders.append(prepared)
        self._sort_orders(orders)
        return True

    def _merge_history_payloads(self, base_data: Dict[str, Any], incoming_data: Dict[str, Any]) -> Dict[str, Any]:
        merged = self._empty_data()
        merged["orders"] = []

        for source in (base_data.get("orders", []), incoming_data.get("orders", [])):
            for order in source:
                self._upsert_order_in_data(merged, order)

        merged["created_by"] = base_data.get("created_by") or incoming_data.get("created_by") or os.getenv(
            "USERNAME", "unknown"
        )
        merged["updated_by"] = os.getenv("USERNAME", "unknown")
        merged["last_update"] = self._pick_latest_timestamp(base_data.get("last_update"), incoming_data.get("last_update"))
        merged["last_update"] = merged["last_update"] or datetime.now().isoformat()
        return merged

    def _migrate_legacy_history(self):
        data = self._load_data()
        changed = False

        for legacy_path in self.legacy_db_files:
            try:
                legacy_exists = legacy_path.exists()
            except OSError as e:
                self._warn_legacy_once(legacy_path, "проверить", e)
                continue

            if not legacy_exists:
                continue

            try:
                legacy_data = self._read_data(legacy_path)
            except Exception as e:
                self._warn_legacy_once(legacy_path, "прочитать", e)
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

    def sync_with_github(self, force: bool = False, push: bool = False, reason: str = "") -> bool:
        del force, push, reason
        return False

    def flush_github_sync(self, reason: str = "flush") -> bool:
        del reason
        return False

    def add_order(self, order_data: Dict[str, Any], *, sync: bool = True) -> bool:
        """Добавляет новый заказ в историю или обновляет существующий."""
        del sync
        try:
            with self._io_lock:
                data = self._load_data()
                changed = self._upsert_order_in_data(data, order_data)
                if changed:
                    self._save_data(data)
                    logger.info("История обновлена для заказа: %s", order_data.get("document_id"))
                else:
                    logger.debug("Заказ %s уже актуален в истории", order_data.get("document_id"))
            return True
        except Exception:
            logger.exception("Ошибка добавления заказа %s", order_data.get("document_id"))
            return False

    def mark_tsd_created(self, document_id: str, intro_number: str) -> bool:
        """Помечает заказ как отправленный на ТСД.

        Возвращает False, если пометка не сохранилась (заказ не найден или ошибка записи);
        ошибку не пробрасываем, чтобы не ломать конвейер — вызывающий код показывает предупреждение.
        """
        try:
            with self._io_lock:
                data = self._load_data()

                updated = False
                for order in data["orders"]:
                    if order.get("document_id") == document_id:
                        now = datetime.now().isoformat()
                        order["tsd_created"] = True
                        order["tsd_created_at"] = now
                        order["tsd_intro_number"] = intro_number
                        order["tsd_created_by"] = os.getenv("USERNAME", "unknown")
                        order["updated_at"] = now
                        order["updated_by"] = os.getenv("USERNAME", "unknown")
                        updated = True
                        break

                if updated:
                    self._save_data(data)
                    logger.info("Заказ %s помечен как отправленный на ТСД", document_id)
                else:
                    logger.warning("Заказ %s не найден в истории", document_id)
            return updated
        except Exception:
            logger.exception("Ошибка обновления статуса ТСД для заказа %s", document_id)
            return False

    def get_orders_without_tsd(self) -> List[Dict[str, Any]]:
        """Возвращает заказы без ТСД (новые сверху)."""
        try:
            data = self._load_data()
            orders = [order for order in data["orders"] if not order.get("tsd_created", False)]
            self._sort_orders(orders)
            if self._last_logged_without_tsd != len(orders):
                logger.info("Найдено %s заказов без ТСД", len(orders))
                self._last_logged_without_tsd = len(orders)
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
            if self._last_logged_total_orders != len(orders):
                logger.info("Загружено %s заказов из %s", len(orders), self.db_file)
                self._last_logged_total_orders = len(orders)
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
                "sync_enabled": False,
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о БД: {e}")
            return {"file_path": str(self.db_file), "error": str(e)}
