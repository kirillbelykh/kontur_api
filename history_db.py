import json
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from logger import logger

DEFAULT_HISTORY_FILE = "full_orders_history.json"
LEGACY_HISTORY_FILE = "orders_history.json"
LEGACY_NETWORK_HISTORY = r"\\192.168.100.2\!files\orders_history.json"

DEFAULT_SYNC_BRANCH = "orders-history"
SYNC_BRANCH_ENV = "HISTORY_SYNC_BRANCH"
SYNC_ENABLED_ENV = "HISTORY_SYNC_ENABLED"
SYNC_CACHE_DIR = ".history_sync_cache"
SYNC_PULL_INTERVAL_SECONDS = 20
SYNC_PUSH_RETRIES = 3


class OrderHistoryDB:
    _io_lock = threading.RLock()

    def __init__(
        self,
        db_file: Optional[str] = None,
        legacy_db_files: Optional[Iterable[str]] = None,
        sync_enabled: Optional[bool] = None,
        sync_branch: Optional[str] = None,
    ):
        self.repo_root = Path(__file__).resolve().parent
        self.db_file = self._resolve_path(db_file or DEFAULT_HISTORY_FILE)
        self.legacy_db_files = self._build_legacy_paths(legacy_db_files)
        self.sync_branch = sync_branch or os.getenv(SYNC_BRANCH_ENV, DEFAULT_SYNC_BRANCH)
        self.sync_enabled = self._resolve_sync_enabled(sync_enabled)
        self.sync_cache_dir = self.repo_root / SYNC_CACHE_DIR
        self._last_sync_pull_at = 0.0

        self._sync_rel_path = self._resolve_sync_relative_path()
        self._origin_url = self._detect_origin_url() if self.sync_enabled else None
        if self.sync_enabled and (not self._sync_rel_path or not self._origin_url):
            self.sync_enabled = False

        self._ensure_db_exists()
        self._migrate_legacy_history()
        self.sync_with_github(force=True, push=False, reason="startup")

    def _resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _resolve_sync_enabled(self, explicit: Optional[bool]) -> bool:
        if explicit is not None:
            return explicit
        value = os.getenv(SYNC_ENABLED_ENV, "1").strip().lower()
        return value not in {"0", "false", "no", "off"}

    def _resolve_sync_relative_path(self) -> Optional[Path]:
        try:
            return self.db_file.relative_to(self.repo_root)
        except ValueError:
            logger.info(
                "Синхронизация истории отключена: файл истории находится вне репозитория (%s)",
                self.db_file,
            )
            return None

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

        path.parent.mkdir(parents=True, exist_ok=True)
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

    def _prepare_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now().isoformat()
        prepared = dict(order_data)
        prepared.setdefault("created_at", now)
        prepared.setdefault("created_by", os.getenv("USERNAME", "unknown"))
        prepared.setdefault("updated_at", now)
        prepared.setdefault("updated_by", os.getenv("USERNAME", "unknown"))
        prepared.setdefault("tsd_created", False)
        prepared.setdefault("tsd_created_at", None)
        prepared.setdefault("tsd_intro_number", None)
        return prepared

    def _merge_order_records(self, current: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(current)
        prefer_incoming = self._prefer_incoming_record(current, incoming)

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

    def _subprocess_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"text": True}
        if os.name == "nt":
            startupinfo_cls = getattr(subprocess, "STARTUPINFO", None)
            startf_use_showwindow = getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
            create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", None)

            if startupinfo_cls is not None:
                startupinfo = startupinfo_cls()
                startupinfo.dwFlags |= startf_use_showwindow
                startupinfo.wShowWindow = 0
                kwargs["startupinfo"] = startupinfo

            if create_no_window is not None:
                kwargs["creationflags"] = int(create_no_window)
        return kwargs

    def _run_git(
        self,
        args: List[str],
        cwd: Path,
        check: bool = True,
        capture_output: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        kwargs = self._subprocess_kwargs()
        if capture_output:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.PIPE
        else:
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL

        result = subprocess.run(["git"] + args, cwd=str(cwd), check=False, **kwargs)
        if check and result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"git {' '.join(args)} failed: {stderr}")
        return result

    def _detect_origin_url(self) -> Optional[str]:
        try:
            result = self._run_git(
                ["remote", "get-url", "origin"],
                cwd=self.repo_root,
                check=True,
                capture_output=True,
            )
            origin = (result.stdout or "").strip()
            return origin or None
        except Exception as e:
            logger.warning(f"Синхронизация истории отключена: не удалось определить origin ({e})")
            return None

    def _ensure_git_identity(self, repo_dir: Path):
        username = os.getenv("USERNAME") or os.getenv("USER") or "kontur-user"
        email = os.getenv("HISTORY_SYNC_EMAIL", f"{username}@local")

        name_result = self._run_git(
            ["config", "--get", "user.name"],
            cwd=repo_dir,
            check=False,
            capture_output=True,
        )
        if not (name_result.stdout or "").strip():
            self._run_git(["config", "user.name", username], cwd=repo_dir, check=False, capture_output=True)

        email_result = self._run_git(
            ["config", "--get", "user.email"],
            cwd=repo_dir,
            check=False,
            capture_output=True,
        )
        if not (email_result.stdout or "").strip():
            self._run_git(["config", "user.email", email], cwd=repo_dir, check=False, capture_output=True)

    def _ensure_sync_repo(self) -> Optional[Path]:
        if not self.sync_enabled or not self._origin_url:
            return None

        try:
            git_dir = self.sync_cache_dir / ".git"
            if not git_dir.exists():
                if self.sync_cache_dir.exists():
                    shutil.rmtree(self.sync_cache_dir, ignore_errors=True)
                self._run_git(
                    ["clone", self._origin_url, str(self.sync_cache_dir)],
                    cwd=self.repo_root,
                    check=True,
                    capture_output=False,
                )

            remote_result = self._run_git(
                ["remote", "get-url", "origin"],
                cwd=self.sync_cache_dir,
                check=False,
                capture_output=True,
            )
            current_origin = (remote_result.stdout or "").strip()
            if not current_origin:
                self._run_git(
                    ["remote", "add", "origin", self._origin_url],
                    cwd=self.sync_cache_dir,
                    check=False,
                    capture_output=True,
                )
            elif current_origin != self._origin_url:
                self._run_git(
                    ["remote", "set-url", "origin", self._origin_url],
                    cwd=self.sync_cache_dir,
                    check=False,
                    capture_output=True,
                )

            self._ensure_git_identity(self.sync_cache_dir)
            return self.sync_cache_dir
        except Exception as e:
            logger.warning(f"Синхронизация истории недоступна: {e}")
            return None

    def _remote_sync_branch_exists(self, repo_dir: Path) -> bool:
        result = self._run_git(
            ["ls-remote", "--heads", "origin", self.sync_branch],
            cwd=repo_dir,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"ls-remote failed: {stderr}")
        return bool((result.stdout or "").strip())

    def _checkout_sync_branch(self, repo_dir: Path):
        self._run_git(["fetch", "origin", "--prune"], cwd=repo_dir, check=True, capture_output=True)

        remote_exists = self._remote_sync_branch_exists(repo_dir)
        if remote_exists:
            self._run_git(
                ["checkout", "-B", self.sync_branch, f"origin/{self.sync_branch}"],
                cwd=repo_dir,
                check=True,
                capture_output=True,
            )
            return

        self._run_git(["checkout", "-B", self.sync_branch], cwd=repo_dir, check=True, capture_output=True)

    def _sync_history_file_path(self, repo_dir: Path) -> Path:
        assert self._sync_rel_path is not None
        return repo_dir / self._sync_rel_path

    def _stage_and_commit_history(self, repo_dir: Path, commit_message: str) -> bool:
        assert self._sync_rel_path is not None
        rel_path = str(self._sync_rel_path).replace("\\", "/")
        self._run_git(["add", rel_path], cwd=repo_dir, check=True, capture_output=True)

        status_result = self._run_git(
            ["status", "--porcelain", "--", rel_path],
            cwd=repo_dir,
            check=True,
            capture_output=True,
        )
        if not (status_result.stdout or "").strip():
            return False

        self._run_git(["commit", "-m", commit_message], cwd=repo_dir, check=True, capture_output=True)
        return True

    def _push_sync_branch(self, repo_dir: Path) -> Tuple[bool, bool]:
        push_result = self._run_git(
            ["push", "origin", self.sync_branch],
            cwd=repo_dir,
            check=False,
            capture_output=True,
        )
        if push_result.returncode == 0:
            return True, False

        stderr = (push_result.stderr or "").lower()
        retryable = "non-fast-forward" in stderr or "rejected" in stderr
        return False, retryable

    def _sync_with_github_locked(self, push: bool, reason: str) -> bool:
        if not self.sync_enabled:
            return False

        repo_dir = self._ensure_sync_repo()
        if repo_dir is None:
            return False

        local_data = self._load_data()
        merged_for_local = local_data

        for attempt in range(SYNC_PUSH_RETRIES):
            try:
                self._checkout_sync_branch(repo_dir)
            except Exception as e:
                logger.warning(f"Синхронизация истории: не удалось обновить ветку {self.sync_branch}: {e}")
                break

            sync_file = self._sync_history_file_path(repo_dir)
            remote_data = self._read_data(sync_file) if sync_file.exists() else self._empty_data()
            merged_data = self._merge_history_payloads(remote_data, local_data)
            merged_for_local = merged_data

            if push and merged_data != remote_data:
                self._write_data(sync_file, merged_data)
                committed = self._stage_and_commit_history(
                    repo_dir,
                    commit_message=f"Sync order history ({reason or 'runtime'})",
                )
            else:
                committed = False

            if not push:
                break

            if not committed:
                break

            pushed, retryable = self._push_sync_branch(repo_dir)
            if pushed:
                break
            if retryable and attempt < SYNC_PUSH_RETRIES - 1:
                logger.info("Синхронизация истории: обнаружена гонка push, повторяем merge")
                continue
            logger.warning("Синхронизация истории: push не удался, история сохранена локально")
            break

        if merged_for_local != local_data:
            self._save_data(merged_for_local)
            return True
        return False

    def sync_with_github(self, force: bool = False, push: bool = False, reason: str = "") -> bool:
        if not self.sync_enabled:
            return False

        now = time.time()
        if not force and not push and (now - self._last_sync_pull_at) < SYNC_PULL_INTERVAL_SECONDS:
            return False

        with self._io_lock:
            changed = self._sync_with_github_locked(push=push, reason=reason)
            self._last_sync_pull_at = time.time()
            return changed

    def _migrate_legacy_history(self):
        data = self._load_data()
        changed = False

        for legacy_path in self.legacy_db_files:
            try:
                legacy_exists = legacy_path.exists()
            except OSError as e:
                logger.warning(f"Не удалось проверить старую историю {legacy_path}: {e}")
                continue

            if not legacy_exists:
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
            with self._io_lock:
                data = self._load_data()
                if self._upsert_order_in_data(data, order_data):
                    self._save_data(data)
                    self._sync_with_github_locked(push=True, reason="add_order")
                    logger.info(f"✅ История обновлена для заказа: {order_data.get('document_id')}")
                else:
                    logger.info(f"Заказ {order_data.get('document_id')} уже актуален в истории")
        except Exception as e:
            logger.error(f"Ошибка добавления заказа {order_data.get('document_id')}: {e}")

    def mark_tsd_created(self, document_id: str, intro_number: str):
        """Помечает заказ как отправленный на ТСД."""
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
                    self._sync_with_github_locked(push=True, reason="mark_tsd_created")
                    logger.info(f"✅ Заказ {document_id} помечен как отправленный на ТСД")
                else:
                    logger.warning(f"⚠️ Заказ {document_id} не найден в истории")

        except Exception as e:
            logger.error(f"Ошибка обновления статуса ТСД для заказа {document_id}: {e}")

    def get_orders_without_tsd(self) -> List[Dict[str, Any]]:
        """Возвращает заказы без ТСД (новые сверху)."""
        try:
            self.sync_with_github(force=False, push=False, reason="get_orders_without_tsd")
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
            self.sync_with_github(force=False, push=False, reason="get_all_orders")
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
            self.sync_with_github(force=False, push=False, reason="get_order_by_document_id")
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
                "sync_enabled": self.sync_enabled,
                "sync_branch": self.sync_branch,
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о БД: {e}")
            return {"file_path": str(self.db_file), "error": str(e)}
