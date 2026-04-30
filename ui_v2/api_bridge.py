from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("HISTORY_SYNC_ENABLED", "0")

import api as api_module
import cookies as cookies_module
from aggregation_bulk import AggregateInfo, BulkAggregationService, BulkAggregationSummary, extract_sntin
from api import (
    check_order_status,
    codes_order,
    download_codes,
    make_task_on_tsd,
    put_into_circulation,
)
try:
    from bartender_label_formats import (
        AGGREGATION_SOURCE_KIND,
        DEFAULT_LABEL_SHEET_FORMAT,
        MARKING_SOURCE_KIND,
        build_label_print_context,
        format_label_sheet_title,
        list_aggregation_csv_files,
        list_label_sheet_formats,
        list_label_templates,
        list_marking_csv_files,
        print_label_sheet,
        resolve_order_metadata,
    )
except ModuleNotFoundError:
    from ui_v2.bartender_label_formats import (
        AGGREGATION_SOURCE_KIND,
        DEFAULT_LABEL_SHEET_FORMAT,
        MARKING_SOURCE_KIND,
        build_label_print_context,
        format_label_sheet_title,
        list_aggregation_csv_files,
        list_label_sheet_formats,
        list_label_templates,
        list_marking_csv_files,
        print_label_sheet,
        resolve_order_metadata,
    )
from bartender_print import build_print_context, list_installed_printers, print_labels
from cookies import get_valid_cookies
from cryptopro import find_certificate_by_thumbprint, sign_data, sign_text_data
from date_defaults import get_default_production_window
from get_gtin import lookup_by_gtin, lookup_gtin
from get_thumb import find_certificate_thumbprint
from history_db import OrderHistoryDB
from options import (
    color_options,
    simplified_options,
    size_options,
    units_options,
    color_required,
    venchik_options,
    venchik_required,
)
from queue_utils import is_order_ready_for_intro, is_order_ready_for_tsd, remove_order_by_document_id
from utils import get_tnved_code, make_session_with_cookies

LABEL_PRINT_SELECTION_CLEANUP_DELAY_SECONDS = 300


LOG_CHANNELS = ("orders", "download", "intro", "tsd", "aggregation", "labels")
MAX_LOG_LINES = 500
DOCUMENT_STATUS_CACHE_TTL_SECONDS = 60
CODE_STATUS_CACHE_TTL_SECONDS = 180
CODE_STATUS_SAMPLE_SIZE = 25
TRUE_STATUS_WORKER_TIMEOUT_SECONDS = 25
DELETED_ORDERS_DIRNAME = "Удаленные"
DELETED_ORDERS_FILE = "deleted_orders.json"
MARKING_CODES_DIRNAME = "\u041a\u043e\u0434\u044b \u043a\u043c"
AGGREGATION_CODES_DIRNAME = "\u0410\u0433\u0440\u0435\u0433 \u043a\u043e\u0434\u044b \u043a\u043c"

STATUS_LABELS = {
    "available": "Доступен для скачивания",
    "created": "Создан",
    "doesNotHaveErrors": "Проверен без ошибок",
    "downloaded": "Скачан",
    "error": "Ошибка",
    "inProgress": "В обработке",
    "introduced": "Введен в оборот",
    "processing": "Генерируется",
    "received": "Доступен для скачивания",
    "released": "Доступен для скачивания",
    "sentForApprove": "Отправлен на подпись",
    "approveFailed": "Не зарегистрирован",
    "approved": "Зарегистрирован",
    "readyForSend": "Готов к проведению",
    "readyForSendAfterApproved": "Состав изменен после регистрации",
    "returnedToTsd": "Возвращен на ТСД",
    "tsdProcessStart": "На ТСД",
    "tsd_created": "Отправлено на ТСД",
    "tsd_not_created": "Не отправлено на ТСД",
    "INTRODUCED": "Введен в оборот",
    "APPLIED": "В обороте",
    "EMITTED": "Эмитирован",
    "WRITTEN_OFF": "Выведен из оборота",
    "RETIRED": "Выведен из оборота",
    "DISAGGREGATED": "Расформирован",
    "disaggregated": "Расформирован",
    "UNKNOWN": "Неизвестно",
}

AGGREGATION_TABLE_STATUSES = (
    "tsdProcessStart",
    "readyForSend",
    "approveFailed",
    "returnedToTsd",
    "sentForApprove",
    "approved",
    "disaggregated",
)

AGGREGATION_FETCH_STATUSES = tuple(
    status for status in AGGREGATION_TABLE_STATUSES if status != "disaggregated"
)


_CYRILLIC_HEADS = {chr(code) for code in (0x0420, 0x0421, 0x00D0, 0x00D1, 0x00C3, 0x00C2, 0x00C7, 0x00CA)}
_SAFE_MOJIBAKE_FOLLOWERS = set(" \r\n\t.,:;!?)]}\"'`-_/\\")


def _text_quality_score(text: str) -> int:
    cyrillic_letters = sum(1 for char in text if "\u0400" <= char <= "\u04FF")
    control_chars = sum(1 for char in text if ord(char) < 32 and char not in "\r\n\t")
    replacement_chars = text.count("\ufffd")
    high_latin_chars = sum(1 for char in text if 0x00C0 <= ord(char) <= 0x00FF)
    suspicious_pairs = sum(
        1
        for index in range(len(text) - 1)
        if text[index] in _CYRILLIC_HEADS
        and text[index + 1] not in _SAFE_MOJIBAKE_FOLLOWERS
        and not text[index + 1].isascii()
    )
    suspicious_penalty = max(0, suspicious_pairs - 1) * 8
    return (cyrillic_letters * 2) - suspicious_penalty - (high_latin_chars * 6) - (control_chars * 10) - (replacement_chars * 12)


def _decode_text_candidate(text: str, source_encoding: str, target_encoding: str) -> Optional[str]:
    try:
        return text.encode(source_encoding).decode(target_encoding)
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        return None


def _normalize_ui_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    best = text
    best_score = _text_quality_score(text)
    seen = {text}
    queue = [text]
    transforms = (
        ("cp1251", "utf-8"),
        ("latin1", "cp1251"),
        ("cp1252", "cp1251"),
        ("latin1", "cp866"),
    )
    for _ in range(3):
        next_queue: List[str] = []
        for current in queue:
            for source_encoding, target_encoding in transforms:
                candidate = _decode_text_candidate(current, source_encoding, target_encoding)
                if not candidate or candidate in seen or candidate == current:
                    continue
                seen.add(candidate)
                next_queue.append(candidate)
                candidate_score = _text_quality_score(candidate)
                if candidate_score > best_score:
                    best = candidate
                    best_score = candidate_score
        if not next_queue:
            break
        queue = next_queue
    return best


def _desktop_data_dir(*dirnames: str) -> Path:
    desktop_root = Path.home() / "Desktop"
    candidates: List[str] = []
    for dirname in dirnames:
        raw_name = str(dirname or "").strip()
        normalized_name = _normalize_ui_text(raw_name)
        for candidate in (normalized_name, raw_name):
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    if not candidates:
        return desktop_root
    for candidate in candidates:
        path = desktop_root / candidate
        if path.exists():
            return path
    return desktop_root / candidates[0]


def _translate_status(value: Any) -> str:
    raw = _normalize_ui_text(str(value or "").strip())
    if not raw:
        return "Неизвестно"
    translated = STATUS_LABELS.get(raw, STATUS_LABELS.get(raw.lower(), raw))
    return _normalize_ui_text(translated)


def _format_status_counts(counts: Counter[str]) -> str:
    parts = []
    for raw_status, count in counts.items():
        parts.append(f"{_translate_status(raw_status)}: {count}")
    return ", ".join(parts)


def _format_datetime_value(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return text[:19].replace("T", " ")


class _BridgeRuntime:
    def __init__(self):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.history_db = OrderHistoryDB(startup_sync="none", sync_enabled=False)
        self.lock = Lock()
        self.session: Optional[requests.Session] = None
        self.session_created_at = 0.0
        self.session_ttl_seconds = 13 * 60
        self.nomenclature_df: Optional[pd.DataFrame] = None
        self.bulk_aggregation_service = BulkAggregationService()
        self.order_queue: List[Dict[str, Any]] = []
        self.session_orders: List[Dict[str, Any]] = []
        self.download_items: List[Dict[str, Any]] = []
        self.logs: Dict[str, List[str]] = {channel: [] for channel in LOG_CHANNELS}
        self.cached_thumbprint: Optional[str] = os.getenv("THUMBPRINT") or None
        self.document_status_cache: Dict[str, Dict[str, Any]] = {}
        self.code_status_cache: Dict[str, Dict[str, Any]] = {}
        self.aggregation_cache_items: List[Dict[str, Any]] = []
        self.aggregation_cache_at = 0.0
        self.aggregation_cache_ttl_seconds = 90.0
        self.load_download_items_from_history()

    def load_download_items_from_history(self) -> None:
        existing_ids = {item.get("document_id") for item in self.download_items if item.get("document_id")}
        for order in self.history_db.get_orders_without_tsd():
            document_id = str(order.get("document_id") or "").strip()
            if not document_id or document_id in existing_ids:
                continue
            self.download_items.append(_history_order_to_download_item(order))
            existing_ids.add(document_id)


_RUNTIME: Optional[_BridgeRuntime] = None


def _get_runtime() -> _BridgeRuntime:
    global _RUNTIME
    if _RUNTIME is None:
        _RUNTIME = _BridgeRuntime()
    return _RUNTIME


def _history_order_to_download_item(order_data: Dict[str, Any]) -> Dict[str, Any]:
    filename = order_data.get("filename")
    csv_path = order_data.get("csv_path")
    status = "Скачан" if filename or csv_path else "Из истории"
    return {
        "order_name": str(order_data.get("order_name") or "").strip(),
        "document_id": str(order_data.get("document_id") or "").strip(),
        "status": status,
        "filename": filename,
        "csv_path": order_data.get("csv_path"),
        "pdf_path": order_data.get("pdf_path"),
        "xls_path": order_data.get("xls_path"),
        "simpl": str(order_data.get("simpl") or order_data.get("simpl_name") or "").strip(),
        "full_name": _extract_position_name(order_data),
        "gtin": _extract_gtin(order_data),
        "from_history": True,
        "downloading": False,
        "history_data": dict(order_data),
    }


def _extract_gtin(order_data: Dict[str, Any]) -> str:
    gtin = str(order_data.get("gtin") or "").strip()
    if gtin:
        return gtin
    positions = order_data.get("positions")
    if isinstance(positions, list) and positions:
        return str(positions[0].get("gtin") or "").strip()
    return ""


def _extract_position_name(order_data: Dict[str, Any]) -> str:
    full_name = str(order_data.get("full_name") or "").strip()
    if full_name:
        return full_name
    positions = order_data.get("positions")
    if isinstance(positions, list) and positions:
        return str(positions[0].get("name") or "").strip()
    return ""


class ApiBridge:
    def __init__(self):
        _get_runtime()

    def _normalize_order_name_key(self, value: str) -> str:
        return " ".join(str(value or "").strip().lower().split())

    def _ensure_unique_order_name(self, order_name: str, *, ignore_uid: str | None = None) -> None:
        runtime = _get_runtime()
        normalized = self._normalize_order_name_key(order_name)
        if not normalized:
            raise RuntimeError("Укажите название заявки.")

        for item in runtime.order_queue:
            if ignore_uid and str(item.get("uid") or "") == ignore_uid:
                continue
            if self._normalize_order_name_key(item.get("order_name")) == normalized:
                raise RuntimeError(f"Заявка с названием '{order_name}' уже есть в очереди.")

        for item in runtime.session_orders:
            if self._normalize_order_name_key(item.get("order_name")) == normalized:
                raise RuntimeError(f"Заявка с названием '{order_name}' уже была создана в этой сессии.")

        for item in runtime.download_items:
            if self._normalize_order_name_key(item.get("order_name")) == normalized:
                raise RuntimeError(f"Заявка с названием '{order_name}' уже существует в активных заказах.")

        for item in runtime.history_db.get_all_orders():
            if self._normalize_order_name_key(item.get("order_name")) == normalized:
                raise RuntimeError(f"Заявка с названием '{order_name}' уже существует в истории заказов.")

    def _log(self, channel: str, message: str) -> None:
        runtime = _get_runtime()
        if channel not in runtime.logs:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        normalized_message = _normalize_ui_text(message)
        runtime.logs[channel].append(f"[{timestamp}] {normalized_message}")
        if len(runtime.logs[channel]) > MAX_LOG_LINES:
            runtime.logs[channel] = runtime.logs[channel][-MAX_LOG_LINES:]

    def _run_background_job(
        self,
        *,
        name: str,
        action,
        error_log_channel: str,
        error_log_prefix: str,
        cleanup=None,
    ) -> None:
        def _worker() -> None:
            try:
                action()
            except Exception as exc:
                self._log(error_log_channel, f"{error_log_prefix}: {exc}")
            finally:
                if cleanup is not None:
                    try:
                        cleanup()
                    except Exception:
                        pass

        worker = Thread(target=_worker, name=name, daemon=True)
        worker.start()

    def _deleted_orders_path(self) -> Path:
        path = _get_runtime().root_dir / DELETED_ORDERS_DIRNAME / DELETED_ORDERS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _load_deleted_orders(self) -> List[Dict[str, Any]]:
        path = self._deleted_orders_path()
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            if isinstance(payload, dict):
                orders = payload.get("orders", [])
                if isinstance(orders, list):
                    return [item for item in orders if isinstance(item, dict)]
            return []
        except Exception:
            return []

    def _save_deleted_orders(self, orders: Sequence[Dict[str, Any]]) -> None:
        path = self._deleted_orders_path()
        payload = {
            "orders": list(orders),
            "updated_at": datetime.now().isoformat(),
        }
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

    def _get_deleted_document_ids(self) -> set[str]:
        return {
            str(item.get("document_id") or "").strip()
            for item in self._load_deleted_orders()
            if str(item.get("document_id") or "").strip()
        }

    def _load_history_payload(self) -> Dict[str, Any]:
        history_db = _get_runtime().history_db
        with history_db._io_lock:  # type: ignore[attr-defined]
            return history_db._load_data()  # type: ignore[attr-defined]

    def _save_history_payload(self, payload: Dict[str, Any], reason: str) -> None:
        history_db = _get_runtime().history_db
        with history_db._io_lock:  # type: ignore[attr-defined]
            history_db._save_data(payload)  # type: ignore[attr-defined]
            sync_locked = getattr(history_db, "_sync_with_github_locked", None)
            if callable(sync_locked):
                try:
                    sync_locked(push=True, reason=reason)
                except Exception:
                    pass

    def _build_file_label(self, item: Dict[str, Any]) -> str:
        parts: List[str] = []
        candidates = [
            item.get("csv_path"),
            item.get("pdf_path"),
            item.get("xls_path"),
            item.get("filename"),
        ]
        for value in candidates:
            if not value:
                continue
            raw_chunks = [value] if not isinstance(value, str) else value.split(",")
            for chunk in raw_chunks:
                cleaned = str(chunk or "").strip()
                if not cleaned:
                    continue
                label = Path(cleaned).name or cleaned
                if label not in parts:
                    parts.append(label)
        return ", ".join(parts)

    def _merge_order_data(self, *items: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            merged.update(item)
        history_data = merged.get("history_data")
        if isinstance(history_data, dict):
            combined = dict(history_data)
            combined.update(merged)
            merged = combined
        return merged

    def _read_codes_sample(self, csv_path: str, *, limit: int = CODE_STATUS_SAMPLE_SIZE) -> List[str]:
        path = Path(str(csv_path or "").strip())
        if not path.exists():
            return []

        codes: List[str] = []
        seen: set[str] = set()
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                for cell in row:
                    value = str(cell or "").strip()
                    if not value:
                        continue
                    if re.search(r"[А-Яа-яA-Za-z]", value) and len(value) < 24:
                        continue
                    if value in seen:
                        continue
                    seen.add(value)
                    codes.append(value)
                    if len(codes) >= limit:
                        return codes
        return codes

    def _resolve_document_status(self, session: Optional[requests.Session], document_id: str, fallback_status: str) -> Dict[str, Any]:
        runtime = _get_runtime()
        normalized_id = str(document_id or "").strip()
        translated_fallback = _translate_status(fallback_status)
        if translated_fallback in {
            "Скачан",
            "Введен в оборот",
            "Ошибка ввода",
            "Ошибка ТСД",
            "Отправлено на ТСД",
        }:
            return {
                "raw": str(fallback_status or "").strip(),
                "label": translated_fallback,
                "source": "history",
            }
        if not normalized_id or session is None:
            return {
                "raw": str(fallback_status or "").strip(),
                "label": translated_fallback,
                "source": "history",
            }

        cached = runtime.document_status_cache.get(normalized_id)
        now = time.time()
        if cached and now - float(cached.get("timestamp") or 0.0) < DOCUMENT_STATUS_CACHE_TTL_SECONDS:
            return dict(cached.get("payload") or {})

        try:
            raw_status = check_order_status(session, normalized_id)
            payload = {
                "raw": raw_status,
                "label": _translate_status(raw_status),
                "source": "kontur",
            }
            runtime.document_status_cache[normalized_id] = {"timestamp": now, "payload": payload}
            return payload
        except Exception:
            return {
                "raw": str(fallback_status or "").strip(),
                "label": translated_fallback,
                "source": "history",
            }

    def _resolve_marking_status_via_worker(self, csv_path: str) -> Optional[Dict[str, Any]]:
        worker_path = Path(__file__).resolve().parent / "true_status_worker.py"
        if not worker_path.exists():
            return None

        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("HISTORY_SYNC_ENABLED", "0")
        env["LOG_FILE"] = str((Path(__file__).resolve().parent / "ui_v2_true_status.log").resolve())

        command = [
            sys.executable,
            str(worker_path),
            str(Path(csv_path).resolve()),
            str(CODE_STATUS_SAMPLE_SIZE),
        ]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TRUE_STATUS_WORKER_TIMEOUT_SECONDS,
            env=env,
            cwd=str(_get_runtime().root_dir),
        )

        output = (completed.stdout or "").strip()
        if not output:
            return None

        try:
            payload = json.loads(output)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("ok") is False:
            return None
        return payload

    def _resolve_marking_status(self, order_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        runtime = _get_runtime()
        document_id = str(order_data.get("document_id") or "").strip()
        csv_path = self._resolve_order_csv_path(order_data)
        if not csv_path:
            return None

        cache_key = document_id or csv_path
        cached = runtime.code_status_cache.get(cache_key)
        now = time.time()
        if cached and now - float(cached.get("timestamp") or 0.0) < CODE_STATUS_CACHE_TTL_SECONDS:
            return dict(cached.get("payload") or {})

        payload = self._resolve_marking_status_via_worker(csv_path)
        if not payload:
            return None

        runtime.code_status_cache[cache_key] = {"timestamp": now, "payload": payload}
        return payload

    def _compose_status_payload(
        self,
        order_data: Dict[str, Any],
        *,
        session: Optional[requests.Session] = None,
        include_marking_status: bool = False,
    ) -> Dict[str, Any]:
        fallback_status = str(order_data.get("status") or "").strip()
        translated_fallback = _translate_status(fallback_status)
        if order_data.get("tsd_created") and translated_fallback in {"Ожидает", "Выполнен", "Отправлено на ТСД"}:
            document_status = {
                "raw": fallback_status,
                "label": translated_fallback,
                "source": "history",
            }
        else:
            document_status = self._resolve_document_status(
                session,
                str(order_data.get("document_id") or "").strip(),
                fallback_status,
            )
        marking_status = self._resolve_marking_status(order_data) if include_marking_status else None
        final_status = marking_status or document_status
        return {
            "status": final_status.get("label") or _translate_status(fallback_status),
            "status_raw": final_status.get("raw") or fallback_status,
            "status_source": final_status.get("source") or "history",
            "status_summary": final_status.get("summary") or "",
        }

    def _should_include_marking_status(self, item: Dict[str, Any]) -> bool:
        translated_status = _translate_status(item.get("status"))
        return bool(
            item.get("tsd_created")
            or translated_status in {"Введен в оборот", "Ошибка ввода"}
        )

    def _serialize_order_record(
        self,
        item: Dict[str, Any],
        *,
        session: Optional[requests.Session] = None,
        include_marking_status: bool = False,
    ) -> Dict[str, Any]:
        merged = self._merge_order_data(item)
        status_payload = self._compose_status_payload(
            merged,
            session=session,
            include_marking_status=include_marking_status,
        )
        tsd_created = bool(merged.get("tsd_created", False))
        return {
            "order_name": str(merged.get("order_name") or merged.get("document_number") or "").strip(),
            "document_id": str(merged.get("document_id") or "").strip(),
            "status": status_payload["status"],
            "status_raw": status_payload["status_raw"],
            "status_source": status_payload["status_source"],
            "status_summary": status_payload["status_summary"],
            "simpl": str(merged.get("simpl") or merged.get("simpl_name") or "").strip(),
            "full_name": _extract_position_name(merged),
            "gtin": _extract_gtin(merged),
            "file_label": self._build_file_label(merged),
            "csv_path": merged.get("csv_path") or "",
            "pdf_path": merged.get("pdf_path") or "",
            "xls_path": merged.get("xls_path") or "",
            "created_at": merged.get("created_at") or "",
            "updated_at": merged.get("updated_at") or "",
            "tsd_created": tsd_created,
            "tsd_intro_number": merged.get("tsd_intro_number") or "",
            "tsd_status": _translate_status("tsd_created" if tsd_created else "tsd_not_created"),
            "can_intro": is_order_ready_for_intro(merged),
            "can_tsd": is_order_ready_for_tsd(merged) or tsd_created,
        }

    def _run_with_session_retry(
        self,
        action,
        *,
        retry_statuses: Sequence[int] = (400, 401, 403),
        log_channel: str | None = None,
        retry_message: str = "Обновляем сессию и повторяем запрос",
    ):
        try:
            return action(self._ensure_session())
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in retry_statuses:
                raise
            if log_channel:
                self._log(log_channel, f"{retry_message} (HTTP {status_code})")
            return action(self._ensure_session(force_refresh=True))

    def _run_with_transient_network_retry(
        self,
        action,
        *,
        attempts: int = 3,
        log_channel: str | None = None,
        retry_message: str = "Повторяем запрос после сетевого обрыва",
    ):
        last_error: Exception | None = None
        total_attempts = max(int(attempts or 1), 1)
        for attempt in range(1, total_attempts + 1):
            try:
                return action()
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt >= total_attempts:
                    raise
                if log_channel:
                    self._log(
                        log_channel,
                        f"{retry_message} ({attempt}/{total_attempts - 1}): {exc}",
                    )
                try:
                    self._ensure_session(force_refresh=True, force_browser_refresh=True)
                except Exception as refresh_exc:
                    if log_channel:
                        self._log(log_channel, f"Не удалось обновить сессию перед повтором: {refresh_exc}")
                time.sleep(min(1.5 * attempt, 4.0))
        if last_error is not None:
            raise last_error

    def _load_nomenclature_df(self) -> pd.DataFrame:
        runtime = _get_runtime()
        if runtime.nomenclature_df is None:
            path = runtime.root_dir / "data" / "nomenclature.xlsx"
            runtime.nomenclature_df = pd.read_excel(path)
        return runtime.nomenclature_df

    def _ensure_session(
        self,
        force_refresh: bool = False,
        *,
        force_browser_refresh: bool = False,
    ) -> requests.Session:
        runtime = _get_runtime()
        with runtime.lock:
            age = time.time() - runtime.session_created_at if runtime.session_created_at else 0.0
            if force_refresh or runtime.session is None or age >= runtime.session_ttl_seconds:
                cookies: Optional[Dict[str, str]]
                if force_browser_refresh:
                    cookies = cookies_module.get_cookies()
                elif force_refresh:
                    cookies = get_valid_cookies() or cookies_module.get_cookies()
                else:
                    cookies = get_valid_cookies()
                if not cookies:
                    raise RuntimeError("Не удалось получить валидные cookies для Контур.Маркировки.")
                runtime.session = make_session_with_cookies(cookies)
                runtime.session_created_at = time.time()
            return runtime.session

    def _ensure_session_safely(self, log_channel: str | None = None) -> Optional[requests.Session]:
        try:
            return self._ensure_session()
        except Exception as exc:
            if log_channel:
                self._log(log_channel, f"Не удалось обновить live-статусы: {exc}")
            return None

    def _get_thumbprint(self) -> Optional[str]:
        runtime = _get_runtime()
        if runtime.cached_thumbprint:
            return runtime.cached_thumbprint
        thumbprint = find_certificate_thumbprint()
        runtime.cached_thumbprint = thumbprint
        return thumbprint

    def _get_certificate(self):
        return find_certificate_by_thumbprint(self._get_thumbprint())

    def _normalize_history_item(
        self,
        item: Dict[str, Any],
        *,
        session: Optional[requests.Session] = None,
        include_marking_status: bool = False,
    ) -> Dict[str, Any]:
        return self._serialize_order_record(
            item,
            session=session,
            include_marking_status=include_marking_status,
        )

    def _serialize_download_item(
        self,
        item: Dict[str, Any],
        *,
        session: Optional[requests.Session] = None,
        include_marking_status: bool = False,
    ) -> Dict[str, Any]:
        payload = self._serialize_order_record(
            item,
            session=session,
            include_marking_status=include_marking_status,
        )
        payload["from_history"] = bool(item.get("from_history"))
        return payload

    def _serialize_queue_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "uid": item.get("uid"),
            "order_name": item.get("order_name") or "",
            "mode": item.get("mode") or "params",
            "simpl_name": item.get("simpl_name") or "",
            "size": item.get("size") or "",
            "color": item.get("color") or "",
            "venchik": item.get("venchik") or "",
            "units_per_pack": item.get("units_per_pack") or "",
            "codes_count": item.get("codes_count") or 0,
            "gtin": item.get("gtin") or "",
            "full_name": item.get("full_name") or "",
            "tnved_code": item.get("tnved_code") or "",
            "cisType": item.get("cisType") or "",
        }

    def _build_lookup_payload(
        self,
        *,
        name: str,
        size: str,
        units_per_pack: str,
        color: str = "",
        venchik: str = "",
    ) -> Dict[str, str]:
        gtin, full_name = lookup_gtin(
            self._load_nomenclature_df(),
            name,
            size,
            str(units_per_pack),
            color or None,
            venchik or None,
        )
        if not gtin:
            raise RuntimeError("GTIN не найден для выбранных параметров.")
        return {
            "gtin": gtin,
            "full_name": full_name or name,
            "tnved_code": get_tnved_code(name),
        }

    def _lookup_by_gtin(self, gtin_value: str) -> Dict[str, str]:
        full_name, simpl_name = lookup_by_gtin(self._load_nomenclature_df(), gtin_value)
        if not full_name:
            raise RuntimeError(f"GTIN {gtin_value} не найден в nomenclature.xlsx.")
        return {
            "gtin": gtin_value,
            "full_name": full_name,
            "simpl_name": simpl_name or "",
            "tnved_code": get_tnved_code(simpl_name or full_name),
        }

    def _prepare_order_item(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        order_name = str(payload.get("order_name") or "").strip()
        self._ensure_unique_order_name(order_name, ignore_uid=str(payload.get("uid") or "") or None)
        mode = str(payload.get("mode") or "params").strip().lower()
        codes_count = int(payload.get("codes_count") or 0)
        units_per_pack = str(payload.get("units_per_pack") or "").strip()
        size = str(payload.get("size") or "").strip()
        color = str(payload.get("color") or "").strip()
        venchik = str(payload.get("venchik") or "").strip()

        if not order_name:
            raise RuntimeError("Укажите название заявки.")
        if codes_count <= 0:
            raise RuntimeError("Количество кодов должно быть больше нуля.")

        if mode == "gtin":
            gtin_value = str(payload.get("gtin") or "").strip()
            if not gtin_value:
                raise RuntimeError("Укажите GTIN.")
            gtin_payload = self._lookup_by_gtin(gtin_value)
            simpl_name = str(payload.get("name") or gtin_payload["simpl_name"] or "").strip()
            full_name = gtin_payload["full_name"]
            tnved_code = gtin_payload["tnved_code"]
            gtin = gtin_payload["gtin"]
        else:
            simpl_name = str(payload.get("name") or payload.get("simplified_name") or "").strip()
            if not simpl_name:
                raise RuntimeError("Укажите наименование товара.")
            if not size:
                raise RuntimeError("Укажите размер.")
            if not units_per_pack:
                raise RuntimeError("Укажите количество единиц в упаковке.")
            lookup_payload = self._build_lookup_payload(
                name=simpl_name,
                size=size,
                units_per_pack=units_per_pack,
                color=color,
                venchik=venchik,
            )
            gtin = lookup_payload["gtin"]
            full_name = lookup_payload["full_name"]
            tnved_code = lookup_payload["tnved_code"]

        return {
            "uid": str(payload.get("uid") or uuid.uuid4().hex),
            "order_name": order_name,
            "mode": mode,
            "simpl_name": simpl_name,
            "size": size,
            "color": color,
            "venchik": venchik,
            "units_per_pack": units_per_pack,
            "codes_count": codes_count,
            "gtin": gtin,
            "full_name": full_name,
            "tnved_code": tnved_code,
            "cisType": str(getattr(api_module, "CIS_TYPE", "unit")),
        }

    def _add_download_item(self, item: Dict[str, Any], document_id: str) -> Dict[str, Any]:
        runtime = _get_runtime()
        existing = self._find_download_item(document_id)
        if existing:
            return existing
        download_item = {
            "order_name": item["order_name"],
            "document_id": document_id,
            "status": "Ожидает",
            "filename": None,
            "csv_path": None,
            "pdf_path": None,
            "xls_path": None,
            "simpl": item["simpl_name"],
            "full_name": item["full_name"],
            "gtin": item["gtin"],
            "from_history": False,
            "downloading": False,
            "history_data": None,
        }
        runtime.download_items.insert(0, download_item)
        return download_item

    def _find_download_item(self, document_id: str) -> Optional[Dict[str, Any]]:
        for item in _get_runtime().download_items:
            if str(item.get("document_id") or "").strip() == str(document_id).strip():
                return item
        return None

    def _find_order_data(self, document_id: str) -> Optional[Dict[str, Any]]:
        active = self._find_download_item(document_id)
        if active:
            history_data = active.get("history_data")
            if isinstance(history_data, dict):
                return history_data
        return _get_runtime().history_db.get_order_by_document_id(document_id)

    def _sync_history_from_download_item(self, item: Dict[str, Any]) -> None:
        order_data = self._find_order_data(str(item.get("document_id") or ""))
        base_record = dict(order_data or {})
        base_record.update(
            {
                "order_name": item.get("order_name"),
                "document_id": item.get("document_id"),
                "status": item.get("status"),
                "filename": item.get("filename"),
                "csv_path": item.get("csv_path"),
                "pdf_path": item.get("pdf_path"),
                "xls_path": item.get("xls_path"),
                "simpl": item.get("simpl"),
                "full_name": item.get("full_name"),
                "gtin": item.get("gtin"),
            }
        )
        _get_runtime().history_db.add_order(base_record)
        item["history_data"] = base_record

    def _mark_tsd_created_local(self, document_id: str, intro_number: str = "") -> None:
        runtime = _get_runtime()
        normalized_id = str(document_id or "").strip()
        if not normalized_id:
            return
        runtime.history_db.mark_tsd_created(normalized_id, intro_number)
        for collection in (runtime.download_items, runtime.session_orders):
            for item in collection:
                if str(item.get("document_id") or "").strip() != normalized_id:
                    continue
                item["tsd_created"] = True
                item["tsd_intro_number"] = intro_number
                history_data = item.get("history_data")
                if isinstance(history_data, dict):
                    history_data["tsd_created"] = True
                    history_data["tsd_intro_number"] = intro_number
                break

    @staticmethod
    def _looks_like_session_error_message(error_text: str) -> bool:
        normalized = str(error_text or "").strip().lower()
        if not normalized:
            return False
        return any(
            token in normalized
            for token in (
                "401",
                "403",
                "forbidden",
                "unauthorized",
                "cookie",
                "cookies",
                "сесс",
                "авториз",
                "csrf",
                "access denied",
            )
        )

    def _should_retry_tsd_creation(
        self,
        result: Dict[str, Any],
        *,
        attempt: int,
    ) -> bool:
        if attempt > 0:
            return False
        if not isinstance(result, dict):
            return True
        if str(result.get("introduction_id") or "").strip():
            return False
        errors = result.get("errors") or []
        if not isinstance(errors, list):
            return True
        if not errors:
            return True
        return any(self._looks_like_session_error_message(error_text) for error_text in errors) or True

    def _create_tsd_task_with_retry(
        self,
        *,
        item: Dict[str, Any],
        intro_number: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
    ) -> tuple[bool, Dict[str, Any]]:
        positions_data, production_patch = self._build_tsd_payload(
            item,
            intro_number,
            production_date,
            expiration_date,
            batch_number,
        )
        last_result: Dict[str, Any] = {"errors": []}
        for attempt in range(2):
            force_refresh = attempt > 0
            force_browser_refresh = attempt > 0
            if attempt > 0:
                self._log("tsd", f"Повторяем создание задания после обновления сессии: {item.get('order_name')}")
            session = self._ensure_session(
                force_refresh=force_refresh,
                force_browser_refresh=force_browser_refresh,
            )
            ok, result = make_task_on_tsd(
                session=session,
                codes_order_id=str(item.get("document_id") or ""),
                positions_data=positions_data,
                production_patch=production_patch,
            )
            last_result = result
            if ok:
                return True, result
            if not self._should_retry_tsd_creation(result, attempt=attempt):
                return False, result
        return False, last_result

    def _resolve_order_csv_path(self, item: Dict[str, Any]) -> Optional[str]:
        candidate_paths = [
            item.get("csv_path"),
            (item.get("history_data") or {}).get("csv_path"),
        ]

        filename_value = item.get("filename") or (item.get("history_data") or {}).get("filename")
        if filename_value:
            for chunk in str(filename_value).split(","):
                normalized = chunk.strip()
                if normalized.lower().endswith(".csv"):
                    candidate_paths.append(normalized)

        for path in candidate_paths:
            if path and os.path.exists(path):
                return str(path)

        desktop = _desktop_data_dir(MARKING_CODES_DIRNAME)
        safe_order_name = "".join(
            char
            for char in str(item.get("order_name") or item.get("document_id") or "")
            if char.isalnum() or char in " -_"
        ).strip()

        if safe_order_name:
            order_dir = desktop / safe_order_name[:120]
            if order_dir.exists():
                csv_files = sorted(
                    order_dir.glob("*.csv"),
                    key=lambda path: path.stat().st_mtime,
                    reverse=True,
                )
                if csv_files:
                    return str(csv_files[0])

        return None

    def _collect_known_orders(self) -> List[Dict[str, Any]]:
        runtime = _get_runtime()
        runtime.load_download_items_from_history()
        merged_by_id: Dict[str, Dict[str, Any]] = {}

        for source in (runtime.session_orders, runtime.download_items, runtime.history_db.get_all_orders()):
            for item in source:
                document_id = str(item.get("document_id") or "").strip()
                if not document_id:
                    continue
                existing = merged_by_id.get(document_id)
                if existing is None:
                    merged_by_id[document_id] = dict(item)
                else:
                    merged_by_id[document_id] = self._merge_order_data(existing, item)

        items = list(merged_by_id.values())
        items.sort(
            key=lambda row: (
                str(row.get("updated_at") or row.get("created_at") or ""),
                str(row.get("document_id") or ""),
            ),
            reverse=True,
        )
        return items

    def _get_order_for_document_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        normalized_id = str(document_id or "").strip()
        if not normalized_id:
            return None

        item = self._find_download_item(normalized_id)
        if item:
            return item

        for session_item in _get_runtime().session_orders:
            if str(session_item.get("document_id") or "").strip() == normalized_id:
                return self._merge_order_data(session_item)

        history_order = _get_runtime().history_db.get_order_by_document_id(normalized_id)
        if isinstance(history_order, dict):
            return _history_order_to_download_item(history_order)
        return None

    def _ensure_order_downloaded_for_intro(
        self,
        session: requests.Session,
        item: Dict[str, Any],
    ) -> Dict[str, Any]:
        csv_path = self._resolve_order_csv_path(item)
        if csv_path:
            if str(item.get("status") or "").strip() != "Скачан":
                item["status"] = "Скачан"
                self._sync_history_from_download_item(item)
            return item

        document_id = str(item.get("document_id") or "").strip()
        if not document_id:
            raise RuntimeError("У заказа не найден document_id.")

        raw_status = str(item.get("status_raw") or item.get("status") or "").strip()
        if raw_status not in {"released", "received", "downloaded"}:
            raw_status = check_order_status(session, document_id)
            item["status"] = raw_status
            self._sync_history_from_download_item(item)

        if raw_status not in {"released", "received", "downloaded"}:
            raise RuntimeError("Заказ ещё не готов для ввода в оборот.")

        self._log("intro", f"Заказ {item.get('order_name') or document_id} ещё не скачан. Скачиваем перед вводом в оборот.")
        self._download_order_internal(
            session,
            item,
            log_prefix="Автоскачивание перед вводом в оборот: ",
        )
        return item

    def _download_order_internal(self, session: requests.Session, item: Dict[str, Any], log_prefix: str = "") -> Dict[str, Any]:
        if item.get("downloading"):
            raise RuntimeError(f"{log_prefix}Заказ уже скачивается.")

        item["downloading"] = True
        item["status"] = "Скачивается"
        self._log("download", f"{log_prefix}Начинаем скачивание: {item.get('order_name')}")
        try:
            paths = download_codes(session, item["document_id"], item["order_name"])
            if not paths:
                raise RuntimeError("download_codes не вернул файлов для скачивания.")
            item["pdf_path"], item["csv_path"], item["xls_path"] = paths
            filename = ", ".join([path for path in paths if path])
            if not filename:
                raise RuntimeError("Сервис не вернул ни одного сохранённого файла.")
            item["filename"] = filename
            item["status"] = "Скачан"
            self._sync_history_from_download_item(item)
            self._log("download", f"{log_prefix}Успешно скачан: {filename}")
            return self._serialize_download_item(item)
        finally:
            item["downloading"] = False

    def _parse_iso_date(self, value: str, *, field_name: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise RuntimeError(f"Укажите поле '{field_name}'.")
        for pattern in ("%Y-%m", "%Y/%m", "%m.%Y"):
            try:
                return datetime.strptime(text, pattern).strftime("%Y-%m-01")
            except ValueError:
                continue
        for pattern in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(text, pattern).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise RuntimeError(f"Поле '{field_name}' должно быть в формате YYYY-MM-DD или DD-MM-YYYY.")

    def _build_intro_patch(self, item: Dict[str, Any], production_date: str, expiration_date: str, batch_number: str) -> Dict[str, Any]:
        simpl_name = str(item.get("simpl") or "").strip()
        return {
            "comment": "",
            "documentNumber": str(item.get("order_name") or "").strip(),
            "productionType": "ownProduction",
            "warehouseId": str(os.getenv("WAREHOUSE_ID") or getattr(api_module, "WAREHOUSE_ID", "")),
            "expirationType": "milkMoreThan72",
            "containsUtilisationReport": "true",
            "usageType": "verified",
            "cisType": "unit",
            "fillingMethod": "file",
            "isAutocompletePositionsDataNeeded": "true",
            "productsHasSameDates": "true",
            "isForKegs": "true",
            "productionDate": production_date,
            "expirationDate": expiration_date,
            "batchNumber": batch_number,
            "TnvedCode": get_tnved_code(simpl_name) if simpl_name else "",
        }

    def _build_tsd_payload(self, item: Dict[str, Any], intro_number: str, production_date: str, expiration_date: str, batch_number: str) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
        gtin = str(item.get("gtin") or _extract_gtin(item.get("history_data") or {}) or "").strip()
        if not gtin:
            raise RuntimeError(f"У заказа {item.get('order_name')} не найден GTIN.")
        if not gtin.startswith("0"):
            gtin = f"0{gtin}"
        positions_data = [{
            "name": str(item.get("full_name") or item.get("order_name") or "").strip(),
            "gtin": gtin,
        }]
        production_patch = {
            "documentNumber": intro_number,
            "productionDate": production_date,
            "expirationDate": expiration_date,
            "batchNumber": batch_number,
            "TnvedCode": get_tnved_code(str(item.get("simpl") or "").strip()),
        }
        return positions_data, production_patch

    @staticmethod
    def _set_cookie_value(session: requests.Session, name: str, value: str) -> None:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return
        session.cookies.set(name, normalized_value, domain="mk.kontur.ru", path="/")

    @staticmethod
    def _normalize_full_marking_code(value: str) -> str:
        normalized = str(value or "").strip().replace("\\x1d", "\x1d")
        if normalized.startswith("^1"):
            normalized = normalized[2:]
        if normalized.startswith("]C1"):
            normalized = normalized[3:]
        return normalized.strip()

    def _iter_saved_marking_rows(self, csv_path: Path):
        encodings = ("utf-8-sig", "utf-8", "cp1251")
        last_error: Optional[Exception] = None
        for encoding in encodings:
            try:
                with csv_path.open("r", encoding=encoding, newline="") as csv_file:
                    reader = csv.reader(csv_file, delimiter="\t")
                    for row in reader:
                        prepared = list(row)
                        if len(prepared) == 1:
                            raw_line = str(prepared[0] or "")
                            if "\t" in raw_line:
                                prepared = raw_line.split("\t")
                            elif ";" in raw_line:
                                prepared = raw_line.split(";")
                            elif "," in raw_line:
                                prepared = raw_line.split(",")
                        if not prepared:
                            continue
                        raw_code = str(prepared[0] or "").strip()
                        normalized_code = self._normalize_full_marking_code(raw_code)
                        if not normalized_code or not normalized_code.startswith("01"):
                            continue
                        gtin = str(prepared[1] or "").strip() if len(prepared) > 1 else ""
                        full_name = str(prepared[2] or "").strip() if len(prepared) > 2 else ""
                        yield {
                            "full_code": normalized_code,
                            "gtin": gtin or normalized_code[2:16],
                            "full_name": full_name,
                        }
                return
            except UnicodeDecodeError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error

    def _match_saved_marking_codes(self, raw_codes: Sequence[str]) -> Dict[str, Any]:
        desktop_dir = _desktop_data_dir(MARKING_CODES_DIRNAME)
        if not desktop_dir.exists():
            raise RuntimeError(f"Папка с кодами маркировки не найдена: {desktop_dir}")

        targets: Dict[str, str] = {}
        for raw_code in raw_codes:
            sntin = extract_sntin(str(raw_code or "").strip())
            if sntin:
                targets[sntin] = str(raw_code or "").strip()
        if not targets:
            raise RuntimeError("Не найдены коды маркировки для поиска в папке 'Коды км'.")

        matched: Dict[str, Dict[str, Any]] = {}
        scanned_files = 0
        for csv_path in sorted(desktop_dir.rglob("*.csv")):
            scanned_files += 1
            for row in self._iter_saved_marking_rows(csv_path):
                sntin = extract_sntin(row["full_code"])
                if sntin not in targets or sntin in matched:
                    continue
                matched[sntin] = {
                    "sntin": sntin,
                    "partial_code": targets[sntin],
                    "full_code": row["full_code"],
                    "gtin": str(row.get("gtin") or "").strip(),
                    "full_name": str(row.get("full_name") or "").strip(),
                    "source_path": str(csv_path),
                    "order_name": csv_path.parent.name,
                }
                if len(matched) >= len(targets):
                    break
            if len(matched) >= len(targets):
                break

        unmatched = [targets[sntin] for sntin in targets.keys() if sntin not in matched]
        groups: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for item in matched.values():
            key = (
                str(item.get("source_path") or ""),
                str(item.get("order_name") or ""),
                str(item.get("gtin") or ""),
                str(item.get("full_name") or ""),
            )
            groups[key].append(item)

        grouped_payload = [
            {
                "source_path": source_path,
                "order_name": order_name,
                "gtin": gtin,
                "full_name": full_name,
                "codes": sorted(items, key=lambda row: row["sntin"]),
            }
            for (source_path, order_name, gtin, full_name), items in groups.items()
        ]
        grouped_payload.sort(key=lambda item: (item["order_name"], item["gtin"], item["source_path"]))

        return {
            "matched": matched,
            "groups": grouped_payload,
            "unmatched": unmatched,
            "scanned_files": scanned_files,
        }

    def _prepare_marking_match_result(
        self,
        match_result: Dict[str, Any],
        *,
        action_label: str,
    ) -> Dict[str, Any]:
        matched = match_result.get("matched") or {}
        groups = match_result.get("groups") or []
        unmatched = [
            str(code or "").strip()
            for code in (match_result.get("unmatched") or [])
            if str(code or "").strip()
        ]
        scanned_files = int(match_result.get("scanned_files") or 0)
        unmatched_preview = [extract_sntin(code) for code in unmatched[:5]]

        if unmatched:
            preview_text = ", ".join(unmatched_preview)
            if not matched:
                raise RuntimeError(
                    f"Не удалось найти полные коды в папке 'Коды км': {len(unmatched)} шт. Примеры: {preview_text}"
                )
            self._log(
                "aggregation",
                f"{action_label}: не удалось найти полные коды в папке 'Коды км' для {len(unmatched)} шт. "
                f"Примеры: {preview_text}. Продолжаем обработку найденных кодов.",
            )

        return {
            "matched": matched,
            "groups": groups,
            "matched_count": len(matched),
            "unmatched_count": len(unmatched),
            "unmatched_preview": unmatched_preview,
            "scanned_files": scanned_files,
        }

    def _lookup_intro_product_metadata(self, gtin: str, fallback_name: str) -> Dict[str, str]:
        normalized_gtin = str(gtin or "").strip()
        full_name = str(fallback_name or "").strip()
        simpl_name = ""
        if normalized_gtin:
            try:
                looked_up_name, looked_up_simpl = lookup_by_gtin(self._load_nomenclature_df(), normalized_gtin)
                if looked_up_name:
                    full_name = looked_up_name
                if looked_up_simpl:
                    simpl_name = looked_up_simpl
            except Exception:
                pass
        tnved_code = get_tnved_code(simpl_name or full_name)
        return {
            "gtin": normalized_gtin,
            "full_name": full_name,
            "simpl_name": simpl_name,
            "tnved_code": tnved_code,
        }

    def _build_aggregate_intro_document_number(
        self,
        order_name: str,
        group_index: int,
        *,
        custom_title: str = "",
        groups_total: int = 1,
    ) -> str:
        safe_order_name = " ".join(str(order_name or "").strip().split()) or "АК"
        custom_label = " ".join(str(custom_title or "").strip().split())
        if custom_label:
            if int(groups_total or 1) > 1:
                return f"{custom_label} [{group_index}]"[:180]
            return custom_label[:180]
        timestamp = datetime.now().strftime("%d.%m %H:%M")
        return f"Ввод АК {safe_order_name} [{group_index}] {timestamp}"[:180]

    def _get_intro_production_state(self, session: requests.Session, introduction_id: str) -> Dict[str, Any]:
        response = session.get(
            f"{str(os.getenv('BASE_URL') or 'https://mk.kontur.ru').rstrip('/')}/api/v1/codes-introduction/{introduction_id}/production",
            headers={"Connection": "close"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _get_intro_document_state(self, session: requests.Session, introduction_id: str) -> Dict[str, Any]:
        response = session.get(
            f"{str(os.getenv('BASE_URL') or 'https://mk.kontur.ru').rstrip('/')}/api/v1/codes-introduction/{introduction_id}",
            headers={"Connection": "close"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _wait_for_intro_document_status(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        expected_statuses: Sequence[str],
        timeout_seconds: float = 180.0,
    ) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        expected = {str(status) for status in expected_statuses if str(status)}
        last_payload: Dict[str, Any] = {}
        last_status = ""
        while time.time() < deadline:
            payload = self._get_intro_production_state(session, introduction_id)
            status = str(payload.get("documentStatus") or "").strip()
            if status != last_status:
                self._log("aggregation", f"Статус ввода в оборот {introduction_id}: {status or 'неизвестно'}")
                last_status = status
            last_payload = payload
            if status in expected:
                return payload
            time.sleep(2)
        raise RuntimeError(
            f"Документ ввода в оборот {introduction_id} не перешёл в статусы {', '.join(sorted(expected))}. "
            f"Последний статус: {last_status or 'неизвестно'}."
        )

    def _wait_for_intro_codes_check(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        timeout_seconds: float = 180.0,
    ) -> Dict[str, Any]:
        base_url = str(os.getenv("BASE_URL") or "https://mk.kontur.ru").rstrip("/")
        deadline = time.time() + timeout_seconds
        last_payload: Dict[str, Any] = {}
        last_status = ""
        terminal_statuses = {"doesNotHaveErrors", "hasErrors", "checked", "noErrors"}
        while time.time() < deadline:
            response = session.get(
                f"{base_url}/api/v1/codes-checking/{introduction_id}",
                headers={"Connection": "close"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                last_payload = payload
            status = str(payload.get("status") or "").strip()
            if status != last_status:
                self._log("aggregation", f"Проверка кодов {introduction_id}: {status or 'неизвестно'}")
                last_status = status
            if status in terminal_statuses:
                return last_payload
            time.sleep(2)
        raise RuntimeError(
            f"Проверка кодов для документа {introduction_id} не завершилась. "
            f"Последний статус: {last_status or 'неизвестно'}."
        )

    def _wait_for_intro_final_status(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        timeout_seconds: float = 300.0,
    ) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_payload: Dict[str, Any] = {}
        last_status = ""
        failed_statuses = {"introductionFailed", "crptSendingError", "relatedDocumentFailed"}
        while time.time() < deadline:
            payload = self._get_intro_document_state(session, introduction_id)
            status = str(payload.get("documentStatus") or payload.get("status") or "").strip()
            if status != last_status:
                self._log("aggregation", f"Финальный статус ввода в оборот {introduction_id}: {status or 'неизвестно'}")
                last_status = status
            last_payload = payload
            if status == "introduced":
                return payload
            if status in failed_statuses:
                raise RuntimeError(
                    f"Документ ввода в оборот {introduction_id} завершился ошибкой: {status}."
                )
            time.sleep(5)
        raise RuntimeError(
            f"Документ ввода в оборот {introduction_id} не перешёл в финальный статус introduced. "
            f"Последний статус: {last_status or 'неизвестно'}."
        )

    def _fill_intro_document_from_tsd(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        tsd_token: str = "",
        full_codes: Sequence[str],
    ) -> None:
        normalized_token = str(tsd_token or "").strip()
        if not normalized_token:
            raise RuntimeError("Введите TSD токен для ввода в оборот АК.")
        codes_payload = [{"code": self._normalize_full_marking_code(code)} for code in full_codes if self._normalize_full_marking_code(code)]
        if not codes_payload:
            raise RuntimeError("Не удалось подготовить полный список кодов для отправки на ТСД.")

        self._set_cookie_value(session, "tsdToken", normalized_token)
        payload = {
            "positionsSave": [
                {
                    "position": 1,
                    "markingCodeModels": codes_payload,
                }
            ]
        }
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json; charset=utf-8",
            "Origin": "https://mk.kontur.ru",
            "Referer": f"https://mk.kontur.ru/tsd/enrichment/{introduction_id}",
        }
        response = session.post(
            f"https://mk.kontur.ru/tsd/api/v1/documents/enrichments/{introduction_id}/result",
            json=payload,
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()

    def _create_exact_intro_file_document(
        self,
        session: requests.Session,
        *,
        product_group: str,
        order_name: str,
        document_number: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
    ) -> str:
        base_url = str(os.getenv("BASE_URL") or "https://mk.kontur.ru").rstrip("/")
        warehouse_id = str(api_module.WAREHOUSE_ID or "").strip()
        if not warehouse_id:
            raise RuntimeError("Не задан WAREHOUSE_ID для создания документа ввода в оборот.")
        close_headers = {"Connection": "close"}

        create_payload = {
            "introductionType": "introduction",
            "productGroup": str(product_group or api_module.PRODUCT_GROUP or "wheelChairs"),
        }
        create_response = session.post(
            f"{base_url}/api/v1/codes-introduction?warehouseId={warehouse_id}",
            json=create_payload,
            headers=close_headers,
            timeout=30,
        )
        create_response.raise_for_status()
        introduction_id = create_response.text.strip().strip('"')
        if not introduction_id:
            raise RuntimeError(f"{order_name}: API не вернул id документа ввода в оборот.")

        production_payload = {
            "comment": f"Ввод по кодам из АК: {order_name}"[:500],
            "documentNumber": document_number,
            "producerInn": "",
            "ownerInn": "",
            "productionDate": f"{production_date}T00:00:00.000+03:00",
            "productionType": "ownProduction",
            "warehouseId": warehouse_id,
            "expirationType": "milkMoreThan72",
            "expirationDate": f"{expiration_date}T00:00:00.000+03:00",
            "containsUtilisationReport": True,
            "usageType": "verified",
            "cisType": "unit",
            "fillingMethod": "file",
            "batchNumber": batch_number,
            "isAutocompletePositionsDataNeeded": True,
            "productsHasSameDates": True,
            "productGroup": str(product_group or api_module.PRODUCT_GROUP or "wheelChairs"),
        }
        production_response = session.patch(
            f"{base_url}/api/v1/codes-introduction/{introduction_id}/production",
            json=production_payload,
            headers=close_headers,
            timeout=30,
        )
        production_response.raise_for_status()
        return introduction_id

    def _build_intro_upload_rows(
        self,
        *,
        metadata: Dict[str, str],
        full_codes: Sequence[str],
        fallback_name: str,
    ) -> Dict[str, Any]:
        normalized_gtin = str(metadata.get("gtin") or "").strip()
        if normalized_gtin and not normalized_gtin.startswith("0"):
            normalized_gtin = f"0{normalized_gtin}"
        full_name = str(metadata.get("full_name") or "").strip() or str(fallback_name or "").strip()
        tnved_code = str(metadata.get("tnved_code") or "").strip()

        rows: List[Dict[str, Any]] = []
        for code in full_codes:
            normalized_code = self._normalize_full_marking_code(code)
            if not normalized_code:
                continue
            rows.append(
                {
                    "name": full_name,
                    "code": normalized_code,
                    "gtin": normalized_gtin,
                    "tnvedCode": tnved_code,
                }
            )
        if not rows:
            raise RuntimeError("Не удалось подготовить строки для ввода в оборот по кодам из АК.")
        return {"rows": rows}

    def _upload_intro_positions_from_file(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        rows_payload: Dict[str, Any],
    ) -> None:
        base_url = str(os.getenv("BASE_URL") or "https://mk.kontur.ru").rstrip("/")
        response = session.post(
            f"{base_url}/api/v1/codes-introduction/{introduction_id}/positions",
            json=rows_payload,
            headers={"Connection": "close"},
            timeout=60,
        )
        response.raise_for_status()
        self._autocomplete_intro_positions(session, introduction_id)

    def _autocomplete_intro_positions(
        self,
        session: requests.Session,
        introduction_id: str,
    ) -> Dict[str, Any]:
        base_url = str(os.getenv("BASE_URL") or "https://mk.kontur.ru").rstrip("/")
        response = session.post(
            f"{base_url}/api/v1/codes-introduction/{introduction_id}/positions/autocomplete",
            headers={"Connection": "close"},
            timeout=30,
        )
        status_code = int(response.status_code or 0)
        if status_code in {200, 201, 204}:
            self._log("aggregation", f"Автозаполнение позиций для {introduction_id}: HTTP {status_code}")
        else:
            self._log(
                "aggregation",
                f"Автозаполнение позиций для {introduction_id} вернуло HTTP {status_code}, продолжаем проверку документа.",
            )
        if response.content:
            try:
                payload = response.json()
            except Exception:
                payload = {"raw": response.text}
        else:
            payload = {}
        return {
            "status_code": status_code,
            "payload": payload,
        }

    def _sign_and_send_intro_document(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        cert: Any,
    ) -> Dict[str, Any]:
        base_url = str(os.getenv("BASE_URL") or "https://mk.kontur.ru").rstrip("/")
        generated = session.get(
            f"{base_url}/api/v1/codes-introduction/{introduction_id}/generate-multiple",
            headers={"Connection": "close"},
            timeout=30,
        )
        generated.raise_for_status()
        items = generated.json()
        if not isinstance(items, list) or not items:
            raise RuntimeError(f"generate-multiple вернул пустой список для документа {introduction_id}")

        signed_payload: List[Dict[str, str]] = []
        for item in items:
            base64_content = str(item.get("base64Content") or "").strip()
            document_id = str(item.get("documentId") or item.get("id") or "").strip()
            if not document_id or not base64_content:
                raise RuntimeError(f"Некорректный элемент generate-multiple для документа {introduction_id}")
            signature = sign_data(cert, base64_content, b_detached=True)
            if isinstance(signature, tuple):
                signature = signature[0]
            signed_payload.append(
                {
                    "documentId": document_id,
                    "signedContent": str(signature or "").strip(),
                }
            )

        send_response = session.post(
            f"{base_url}/api/v1/codes-introduction/{introduction_id}/send-multiple",
            json=signed_payload,
            headers={"Connection": "close"},
            timeout=30,
        )
        send_response.raise_for_status()
        final_intro = self._get_intro_document_state(session, introduction_id)
        final_check = session.get(
            f"{base_url}/api/v1/codes-checking/{introduction_id}",
            headers={"Connection": "close"},
            timeout=30,
        )
        final_check.raise_for_status()
        try:
            send_payload = send_response.json() if send_response.content else {}
        except Exception:
            send_payload = {"raw": send_response.text}
        current_status = str(final_intro.get("documentStatus") or final_intro.get("status") or "").strip()
        self._log(
            "aggregation",
            f"Документ ввода в оборот {introduction_id} отправлен. Текущий статус: {current_status or 'неизвестно'}.",
        )
        return {
            "generated_count": len(items),
            "send_response": send_payload,
            "final_introduction": final_intro,
            "final_check": final_check.json(),
        }

    def _collect_intro_partial_details(self, production_state: Dict[str, Any]) -> Dict[str, Any]:
        positions = production_state.get("positions") or []
        total_codes = 0
        broken_codes: List[str] = []
        broken_count = 0

        for position in positions:
            try:
                total_codes += max(int(position.get("codesCount") or 0), 0)
            except Exception:
                pass
            try:
                broken_count += max(int(position.get("brokenCodesCount") or 0), 0)
            except Exception:
                pass
            for broken_group in position.get("brokenCodes") or []:
                broken_codes.extend(
                    str(code or "").strip()
                    for code in (broken_group.get("codes") or [])
                    if str(code or "").strip()
                )

        successful_codes_count = max(total_codes - broken_count, 0)
        return {
            "total_codes_count": total_codes,
            "broken_codes_count": broken_count,
            "successful_codes_count": successful_codes_count,
            "broken_codes": broken_codes,
        }

    def _finalize_intro_document_after_check(
        self,
        session: requests.Session,
        introduction_id: str,
        *,
        cert: Any,
        codes_check: Dict[str, Any],
        uploaded_codes_count: int,
        log_channel: str,
        source_label: str,
    ) -> Dict[str, Any]:
        production_state = self._get_intro_production_state(session, introduction_id)
        check_status = str(codes_check.get("status") or "").strip()

        if check_status == "hasErrors":
            partial_details = self._collect_intro_partial_details(production_state)
            successful_codes_count = int(partial_details.get("successful_codes_count") or 0)
            broken_codes_count = int(partial_details.get("broken_codes_count") or 0)
            broken_codes = [str(code or "").strip() for code in partial_details.get("broken_codes") or [] if str(code or "").strip()]

            if successful_codes_count <= 0:
                preview = ", ".join(broken_codes[:5])
                raise RuntimeError(
                    f"{source_label}: проверка кодов вернула ошибки для {broken_codes_count} КМ, "
                    f"успешно обработанных кодов нет. Примеры: {preview}"
                )

            self._log(
                log_channel,
                f"{source_label}: документ {introduction_id} обработан частично. "
                f"Успешно: {successful_codes_count}, с ошибками: {broken_codes_count}. "
                f"Подписываем и отправляем успешно обработанные коды.",
            )
            send_result = self._sign_and_send_intro_document(
                session,
                introduction_id,
                cert=cert,
            )
            send_result["partial_success"] = partial_details
            return {
                "send_result": send_result,
                "document_state": self._get_intro_document_state(session, introduction_id),
                "production_state": production_state,
                "actual_sent_codes_count": successful_codes_count,
            }

        document_state = self._get_intro_document_state(session, introduction_id)
        document_status = str(document_state.get("documentStatus") or document_state.get("status") or "").strip()
        self._log(
            log_channel,
            f"Статус документа ввода в оборот {introduction_id} после проверки кодов: {document_status or 'неизвестно'}",
        )
        if document_status != "introduced":
            self._log(log_channel, f"Подписываем и отправляем документ {introduction_id}.")
            send_result = self._sign_and_send_intro_document(
                session,
                introduction_id,
                cert=cert,
            )
        else:
            send_result = {
                "generated_count": 0,
                "send_response": {},
                "final_introduction": document_state or {"documentId": introduction_id, "documentStatus": "introduced"},
                "final_check": codes_check,
            }
        return {
            "send_result": send_result,
            "document_state": document_state,
            "production_state": production_state,
            "actual_sent_codes_count": max(int(uploaded_codes_count or 0), 0),
        }

    def _fetch_code_states_resilient(
        self,
        *,
        service: BulkAggregationService,
        cert: Any,
        product_group: str,
        raw_codes: Sequence[str],
        context_label: str,
    ) -> List[Any]:
        resolved_group = (
            str(product_group or "").strip()
            or str(getattr(service, "true_api_product_group", "") or "").strip()
            or "wheelchairs"
        )

        last_error: Optional[Exception] = None
        for attempt in range(1, 3):
            try:
                if attempt > 1:
                    service._true_api_token = None
                    service._true_api_token_expires_at = 0.0
                    self._log(
                        "aggregation",
                        f"Повторяем запрос статусов КМ для {context_label}: pg={resolved_group}, попытка {attempt}.",
                    )
                states = service.fetch_code_states(
                    cert=cert,
                    sign_text_func=sign_text_data,
                    product_group=resolved_group,
                    raw_codes=raw_codes,
                )
                return self._retry_code_state_api_errors(
                    service=service,
                    cert=cert,
                    product_group=resolved_group,
                    states=states,
                    context_label=context_label,
                )
            except requests.HTTPError as exc:
                last_error = exc
                status_code = getattr(exc.response, "status_code", None)
                if status_code in {401, 403, 404} and attempt == 1:
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("Не удалось получить статусы кодов маркировки в Честном Знаке.")

    def _retry_code_state_api_errors(
        self,
        *,
        service: BulkAggregationService,
        cert: Any,
        product_group: str,
        states: Sequence[Any],
        context_label: str,
    ) -> List[Any]:
        error_states = [state for state in states if getattr(state, "api_error", None)]
        if not error_states:
            return list(states)

        self._log(
            "aggregation",
            f"Повторно запрашиваем статусы КМ с ошибкой для {context_label}: {len(error_states)} шт.",
        )
        recovered_by_sntin: Dict[str, Any] = {}
        for state in error_states:
            raw_code = str(getattr(state, "raw_code", "") or "").strip()
            sntin = str(getattr(state, "sntin", "") or raw_code).strip()
            if not raw_code:
                continue
            try:
                refreshed_states = service.fetch_code_states(
                    cert=cert,
                    sign_text_func=sign_text_data,
                    product_group=product_group,
                    raw_codes=[raw_code],
                )
            except Exception as exc:
                self._log(
                    "aggregation",
                    f"Повторный запрос статуса КМ не удался для {sntin}: {exc}",
                )
                continue
            refreshed_state = refreshed_states[0] if refreshed_states else None
            if not refreshed_state or getattr(refreshed_state, "api_error", None):
                continue
            recovered_key = str(getattr(refreshed_state, "sntin", "") or sntin).strip()
            if recovered_key:
                recovered_by_sntin[recovered_key] = refreshed_state

        if recovered_by_sntin:
            self._log(
                "aggregation",
                f"Повторный запрос статусов КМ успешно восстановил {len(recovered_by_sntin)} шт. для {context_label}.",
            )

        merged_states: List[Any] = []
        for state in states:
            key = str(getattr(state, "sntin", "") or "").strip()
            merged_states.append(recovered_by_sntin.get(key, state))
        return merged_states

    def _partition_intro_code_states(
        self,
        states: Sequence[Any],
        *,
        action_label: str,
    ) -> Dict[str, Any]:
        status_counts = Counter(getattr(state, "status", None) or "UNKNOWN" for state in states)
        api_errors = [state for state in states if getattr(state, "api_error", None)]
        if api_errors and len(api_errors) == len(states):
            preview = ", ".join(
                str(getattr(state, "sntin", "") or getattr(state, "raw_code", "") or "").strip()
                for state in api_errors[:5]
            )
            self._log(
                "aggregation",
                f"{action_label}: не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. Примеры: {preview}. Продолжаем обработку остальных кодов.",
            )

        target_codes: List[str] = []
        already_introduced = 0
        unsupported_states: List[Any] = []
        for state in states:
            if getattr(state, "api_error", None):
                continue
            normalized_status = str(getattr(state, "status", "") or "").upper()
            if normalized_status in {"INTRODUCED", "APPLIED"}:
                already_introduced += 1
                continue
            if normalized_status == "EMITTED":
                raw_code = str(getattr(state, "raw_code", "") or "").strip()
                if raw_code:
                    target_codes.append(raw_code)
                continue
            unsupported_states.append(state)

        return {
            "status_counts": status_counts,
            "api_errors": api_errors,
            "target_codes": list(dict.fromkeys(target_codes)),
            "already_introduced": already_introduced,
            "unsupported_states": unsupported_states,
        }


    def _introduce_aggregations_via_exact_codes(
        self,
        session: requests.Session,
        *,
        comment_filter: str,
        tsd_token: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
        cert: Any,
    ) -> Dict[str, Any]:
        service = _get_runtime().bulk_aggregation_service
        normalized_filter = str(comment_filter or "").strip() or None
        ready_aggregates = service.list_ready_aggregates(
            session,
            comment_filter=normalized_filter,
            status_filters=("readyForSend",),
        )
        if not ready_aggregates:
            if normalized_filter:
                raise RuntimeError(f"Не найдены АК readyForSend по фильтру '{normalized_filter}'.")
            raise RuntimeError("Не найдены АК readyForSend для ввода в оборот.")

        aggregate_codes: List[str] = []
        skipped_nested: List[str] = []
        product_group = ready_aggregates[0].product_group or "wheelChairs"
        for aggregate in ready_aggregates:
            raw_codes, reaggregation_codes = service.fetch_aggregate_codes(session, aggregate.document_id)
            if reaggregation_codes:
                skipped_nested.append(aggregate.aggregate_code or aggregate.document_id)
                self._log(
                    "aggregation",
                    f"Пропускаем АК {aggregate.aggregate_code or aggregate.document_id}: обнаружены вложенные АК ({len(reaggregation_codes)}).",
                )
                continue
            aggregate_codes.extend(raw_codes)

        unique_codes = list(dict.fromkeys(code for code in aggregate_codes if str(code or "").strip()))
        if not unique_codes:
            raise RuntimeError("В найденных АК нет кодов маркировки для ввода в оборот.")

        self._log(
            "aggregation",
            f"Найдены КМ в readyForSend АК: {len(unique_codes)} шт., начинаем проверку статусов в Честном Знаке.",
        )
        true_product_group = service._resolve_true_product_group(product_group)
        states = self._fetch_code_states_resilient(
            service=service,
            cert=cert,
            product_group=true_product_group,
            raw_codes=unique_codes,
            context_label="ввода в оборот АК",
        )
        status_counts = Counter(state.status or "UNKNOWN" for state in states)
        api_errors = [state for state in states if state.api_error]
        if api_errors and len(api_errors) == len(states):
            preview = ", ".join(state.sntin for state in api_errors[:5])
            raise RuntimeError(
                f"Не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. "
                f"Примеры: {preview}"
            )

        target_codes: List[str] = []
        if api_errors:
            preview = ", ".join(state.sntin for state in api_errors[:5])
            self._log(
                "aggregation",
                f"Ввод в оборот АК: не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. Примеры: {preview}. Продолжаем обработку остальных кодов.",
            )
        already_introduced = 0
        unsupported_states: List[CodeState] = []
        for state in states:
            if state.api_error:
                continue
            normalized_status = str(state.status or "").upper()
            if normalized_status in {"INTRODUCED", "APPLIED"}:
                already_introduced += 1
                continue
            if normalized_status == "EMITTED":
                target_codes.append(state.raw_code)
                continue
            unsupported_states.append(state)
        self._log(
            "aggregation",
            f"Статусы КМ из АК: {_format_status_counts(status_counts)}. Уже введено в оборот: {already_introduced}.",
        )
        if unsupported_states:
            preview = ", ".join(
                f"{state.sntin} ({state.status})"
                for state in unsupported_states[:5]
            )
            raise RuntimeError(
                f"Часть кодов из АК нельзя ввести в оборот автоматически: {len(unsupported_states)} шт. "
                f"Примеры: {preview}"
            )
        if not target_codes:
            raise RuntimeError("Все коды из найденных АК уже введены в оборот.")

        match_result = self._match_saved_marking_codes(target_codes)
        prepared_match = self._prepare_marking_match_result(
            match_result,
            action_label="Ввод в оборот АК",
        )
        groups = prepared_match["groups"]
        introduced_results: List[Dict[str, Any]] = []
        total_sent_codes = 0
        self._log(
            "aggregation",
            f"Полные коды найдены в папке 'Коды км': {prepared_match['matched_count']} шт., "
            f"не найдено: {prepared_match['unmatched_count']} шт., файлов просмотрено: {prepared_match['scanned_files']}.",
        )
        for index, group in enumerate(groups, start=1):
            source_order_name = str(group.get("order_name") or "").strip() or f"Группа {index}"
            codes = [row["full_code"] for row in group.get("codes", [])]
            if not codes:
                continue
            metadata = self._lookup_intro_product_metadata(
                str(group.get("gtin") or "").strip(),
                str(group.get("full_name") or "").strip(),
            )
            document_number = self._build_aggregate_intro_document_number(source_order_name, index)
            positions_data = [
                {
                    "name": metadata["full_name"] or source_order_name,
                    "gtin": metadata["gtin"] if str(metadata["gtin"]).startswith("0") else f"0{metadata['gtin']}",
                }
            ]
            production_patch = {
                "documentNumber": document_number,
                "productionDate": production_date,
                "expirationDate": expiration_date,
                "batchNumber": batch_number,
                "TnvedCode": metadata["tnved_code"],
            }
            self._log(
                "aggregation",
                f"Создаём ввод в оборот по АК: заказ '{source_order_name}', кодов {len(codes)}, GTIN {metadata['gtin']}.",
            )
            ok, create_result = make_task_on_tsd(
                session=session,
                codes_order_id=source_order_name,
                positions_data=positions_data,
                production_patch=production_patch,
            )
            if not ok:
                error_text = "; ".join(create_result.get("errors", [])) or "Не удалось создать документ ввода в оборот"
                raise RuntimeError(f"{source_order_name}: {error_text}")

            introduction_id = str(create_result.get("introduction_id") or "").strip()
            if not introduction_id:
                raise RuntimeError(f"{source_order_name}: API не вернул id документа ввода в оборот.")
            self._log("aggregation", f"Документ ввода в оборот создан: {introduction_id}. Отправляем точные коды через ТСД.")
            self._fill_intro_document_from_tsd(
                session,
                introduction_id,
                tsd_token=tsd_token,
                full_codes=codes,
            )
            codes_check = self._wait_for_intro_codes_check(session, introduction_id)
            finalize_result = self._finalize_intro_document_after_check(
                session,
                introduction_id,
                cert=cert,
                codes_check=codes_check,
                uploaded_codes_count=len(codes),
                log_channel="aggregation",
                source_label=source_order_name,
            )
            send_result = finalize_result["send_result"]
            actual_sent_codes_count = int(finalize_result.get("actual_sent_codes_count") or 0)

            introduced_results.append(
                {
                    "introduction_id": introduction_id,
                    "order_name": source_order_name,
                    "source_path": str(group.get("source_path") or ""),
                    "gtin": metadata["gtin"],
                    "codes_count": len(codes),
                    "actual_sent_codes_count": actual_sent_codes_count,
                    "result": send_result,
                }
            )
            total_sent_codes += actual_sent_codes_count
            self._log(
                "aggregation",
                f"Ввод в оборот отправлен: {source_order_name} "
                f"({actual_sent_codes_count} из {len(codes)} кодов, документ {introduction_id}).",
            )

        return {
            "ready_aggregates": len(ready_aggregates),
            "skipped_nested": skipped_nested,
            "checked_codes": len(unique_codes),
            "matched_codes": prepared_match["matched_count"],
            "missing_full_codes": prepared_match["unmatched_count"],
            "missing_full_codes_preview": prepared_match["unmatched_preview"],
            "already_introduced_codes": already_introduced,
            "skipped_api_error_codes": len(api_errors),
            "skipped_api_error_preview": [state.sntin for state in api_errors[:5]],
            "introduced_codes": total_sent_codes,
            "groups": introduced_results,
            "status_counts": dict(status_counts),
            "scanned_saved_files": prepared_match["scanned_files"],
        }

    def _introduce_aggregations_via_exact_codes_file(
        self,
        session: requests.Session,
        *,
        comment_filter: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
        cert: Any,
    ) -> Dict[str, Any]:
        service = _get_runtime().bulk_aggregation_service
        normalized_filter = str(comment_filter or "").strip() or None
        ready_aggregates = service.list_ready_aggregates(
            session,
            comment_filter=normalized_filter,
            status_filters=("readyForSend",),
        )
        if not ready_aggregates:
            if normalized_filter:
                raise RuntimeError(f"Не найдены АК readyForSend по фильтру '{normalized_filter}'.")
            raise RuntimeError("Не найдены АК readyForSend для ввода в оборот.")

        aggregate_codes: List[str] = []
        skipped_nested: List[str] = []
        product_group = ready_aggregates[0].product_group or "wheelChairs"
        for aggregate in ready_aggregates:
            raw_codes, reaggregation_codes = service.fetch_aggregate_codes(session, aggregate.document_id)
            if reaggregation_codes:
                skipped_nested.append(aggregate.aggregate_code or aggregate.document_id)
                self._log(
                    "aggregation",
                    f"Пропускаем АК {aggregate.aggregate_code or aggregate.document_id}: обнаружены вложенные АК ({len(reaggregation_codes)}).",
                )
                continue
            aggregate_codes.extend(raw_codes)

        unique_codes = list(dict.fromkeys(code for code in aggregate_codes if str(code or "").strip()))
        if not unique_codes:
            raise RuntimeError("В найденных АК нет кодов маркировки для ввода в оборот.")

        self._log(
            "aggregation",
            f"Найдены КМ в readyForSend АК: {len(unique_codes)} шт., начинаем проверку статусов в Честном Знаке.",
        )
        true_product_group = service._resolve_true_product_group(product_group)
        states = self._fetch_code_states_resilient(
            service=service,
            cert=cert,
            product_group=true_product_group,
            raw_codes=unique_codes,
            context_label="ввода в оборот АК из файла",
        )
        status_counts = Counter(state.status or "UNKNOWN" for state in states)
        api_errors = [state for state in states if state.api_error]
        if api_errors and len(api_errors) == len(states):
            preview = ", ".join(state.sntin for state in api_errors[:5])
            raise RuntimeError(
                f"Не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. "
                f"Примеры: {preview}"
            )

        target_codes: List[str] = []
        if api_errors:
            preview = ", ".join(state.sntin for state in api_errors[:5])
            self._log(
                "aggregation",
                f"Ввод в оборот АК из файла: не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. Примеры: {preview}. Продолжаем обработку остальных кодов.",
            )
        already_introduced = 0
        unsupported_states: List[CodeState] = []
        for state in states:
            if state.api_error:
                continue
            normalized_status = str(state.status or "").upper()
            if normalized_status in {"INTRODUCED", "APPLIED"}:
                already_introduced += 1
                continue
            if normalized_status == "EMITTED":
                target_codes.append(state.raw_code)
                continue
            unsupported_states.append(state)

        self._log(
            "aggregation",
            f"Статусы КМ из АК: {_format_status_counts(status_counts)}. Уже введено в оборот: {already_introduced}.",
        )
        if unsupported_states:
            preview = ", ".join(
                f"{state.sntin} ({state.status})"
                for state in unsupported_states[:5]
            )
            raise RuntimeError(
                f"Часть кодов из АК нельзя ввести в оборот автоматически: {len(unsupported_states)} шт. "
                f"Примеры: {preview}"
            )
        if not target_codes:
            raise RuntimeError("Все коды из найденных АК уже введены в оборот.")

        match_result = self._match_saved_marking_codes(target_codes)
        prepared_match = self._prepare_marking_match_result(
            match_result,
            action_label="Ввод в оборот АК",
        )
        groups = prepared_match["groups"]
        introduced_results: List[Dict[str, Any]] = []
        total_sent_codes = 0
        self._log(
            "aggregation",
            f"Полные коды найдены в папке 'Коды км': {prepared_match['matched_count']} шт., "
            f"не найдено: {prepared_match['unmatched_count']} шт., файлов просмотрено: {prepared_match['scanned_files']}.",
        )
        for index, group in enumerate(groups, start=1):
            source_order_name = str(group.get("order_name") or "").strip() or f"Группа {index}"
            codes = [row["full_code"] for row in group.get("codes", []) if str(row.get("full_code") or "").strip()]
            if not codes:
                continue

            metadata = self._lookup_intro_product_metadata(
                str(group.get("gtin") or "").strip(),
                str(group.get("full_name") or "").strip(),
            )
            document_number = self._build_aggregate_intro_document_number(source_order_name, index)
            self._log(
                "aggregation",
                f"Создаём ввод в оборот по АК: заказ '{source_order_name}', кодов {len(codes)}, GTIN {metadata['gtin']}.",
            )

            introduction_id = self._create_exact_intro_file_document(
                session,
                product_group=product_group,
                order_name=source_order_name,
                document_number=document_number,
                production_date=production_date,
                expiration_date=expiration_date,
                batch_number=batch_number,
            )
            self._log(
                "aggregation",
                f"Документ ввода в оборот создан: {introduction_id}. Загружаем точные коды как файл.",
            )

            rows_payload = self._build_intro_upload_rows(
                metadata=metadata,
                full_codes=codes,
                fallback_name=source_order_name,
            )
            self._upload_intro_positions_from_file(
                session,
                introduction_id,
                rows_payload=rows_payload,
            )

            codes_check = self._wait_for_intro_codes_check(session, introduction_id)
            finalize_result = self._finalize_intro_document_after_check(
                session,
                introduction_id,
                cert=cert,
                codes_check=codes_check,
                uploaded_codes_count=len(codes),
                log_channel="aggregation",
                source_label=source_order_name,
            )
            send_result = finalize_result["send_result"]
            actual_sent_codes_count = int(finalize_result.get("actual_sent_codes_count") or 0)

            introduced_results.append(
                {
                    "introduction_id": introduction_id,
                    "order_name": source_order_name,
                    "source_path": str(group.get("source_path") or ""),
                    "gtin": metadata["gtin"],
                    "codes_count": len(codes),
                    "actual_sent_codes_count": actual_sent_codes_count,
                    "result": send_result,
                }
            )
            total_sent_codes += actual_sent_codes_count
            self._log(
                "aggregation",
                f"Ввод в оборот отправлен: {source_order_name} "
                f"({actual_sent_codes_count} из {len(codes)} кодов, документ {introduction_id}).",
            )

        return {
            "ready_aggregates": len(ready_aggregates),
            "skipped_nested": skipped_nested,
            "checked_codes": len(unique_codes),
            "matched_codes": prepared_match["matched_count"],
            "missing_full_codes": prepared_match["unmatched_count"],
            "missing_full_codes_preview": prepared_match["unmatched_preview"],
            "already_introduced_codes": already_introduced,
            "skipped_api_error_codes": len(api_errors),
            "skipped_api_error_preview": [state.sntin for state in api_errors[:5]],
            "introduced_codes": total_sent_codes,
            "groups": introduced_results,
            "status_counts": dict(status_counts),
            "scanned_saved_files": prepared_match["scanned_files"],
        }

    def _create_aggregate_codes(self, session: requests.Session, comment: str, count: int) -> List[Dict[str, Any]]:
        base_url = f"{str(os.getenv('BASE_URL') or 'https://mk.kontur.ru').rstrip('/')}/api/v1/aggregates"
        payload = {
            "extensionSymbol": "0",
            "comment": comment,
            "count": int(count),
            "productGroup": str(getattr(api_module, "PRODUCT_GROUP", "wheelChairs")),
            "aggregationType": "gs1GlnAggregate",
        }
        response = session.post(
            base_url,
            params={"warehouseId": str(os.getenv("WAREHOUSE_ID") or getattr(api_module, "WAREHOUSE_ID", ""))},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError("Сервис создания агрегационных кодов вернул неожиданный ответ.")
        return data

    def _download_aggregate_codes(
        self,
        session: requests.Session,
        mode: str,
        target_value: str,
        *,
        status_filter: str = "tsdProcessStart",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        base_url = f"{str(os.getenv('BASE_URL') or 'https://mk.kontur.ru').rstrip('/')}/api/v1/aggregates"
        warehouse_id = str(os.getenv("WAREHOUSE_ID") or getattr(api_module, "WAREHOUSE_ID", ""))
        all_codes: List[Dict[str, Any]] = []
        seen_codes: set[str] = set()
        page_limit = 100
        offset = 0
        normalized_target = str(target_value or "").strip().lower()

        if mode == "comment" and not normalized_target:
            raise RuntimeError("Введите название для поиска агрегационных кодов.")

        while True:
            params = {
                "warehouseId": warehouse_id,
                "limit": page_limit,
                "offset": offset,
                "statuses": status_filter,
                "sortField": "createDate",
                "sortOrder": "descending",
            }
            response = session.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            items = payload.get("items", []) if isinstance(payload, dict) else []
            if not items:
                break

            filtered = items if mode == "count" else [
                item for item in items
                if normalized_target in str(item.get("comment") or "").strip().lower()
            ]

            for item in filtered:
                aggregate_code = str(item.get("aggregateCode") or "").strip()
                if not aggregate_code or aggregate_code in seen_codes:
                    continue
                seen_codes.add(aggregate_code)
                all_codes.append(
                    {
                        "aggregateCode": aggregate_code,
                        "documentId": item.get("documentId"),
                        "createdDate": item.get("createdDate"),
                        "status": item.get("status"),
                        "updatedDate": item.get("updatedDate"),
                        "includesUnitsCount": item.get("includesUnitsCount"),
                        "comment": item.get("comment", ""),
                        "productGroup": item.get("productGroup"),
                        "aggregationType": item.get("aggregationType"),
                        "codesChecked": item.get("codesChecked"),
                        "codesCheckErrorsCount": item.get("codesCheckErrorsCount"),
                    }
                )

            if mode == "count" and len(all_codes) >= int(target_value):
                break
            if mode == "comment" and limit is not None and len(all_codes) >= limit:
                break
            if len(items) < page_limit:
                break

            offset += page_limit
            time.sleep(0.25)

        if mode == "count":
            all_codes = all_codes[: int(target_value)]
        elif mode == "comment" and limit is not None:
            all_codes = all_codes[:limit]

        all_codes.sort(key=lambda item: int(str(item["aggregateCode"])[-10:]) if str(item["aggregateCode"])[-10:].isdigit() else str(item["aggregateCode"]))
        return all_codes

    def _save_simple_aggregation_csv(self, codes: Sequence[Dict[str, Any]], filename: str) -> str:
        desktop = _desktop_data_dir(AGGREGATION_CODES_DIRNAME)
        safe_name = "".join(char for char in str(filename) if char.isalnum() or char in " -_().").strip()
        if not safe_name:
            safe_name = f"aggregation_{int(time.time())}.csv"
        if not safe_name.lower().endswith(".csv"):
            safe_name += ".csv"
        target_dir = desktop / safe_name
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_name

        with target_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            for item in codes:
                writer.writerow([item.get("aggregateCode")])
        return str(target_path)

    def _serialize_summary(self, summary: Any) -> Dict[str, Any]:
        return {
            "ready_found": int(getattr(summary, "ready_found", 0)),
            "processed": int(getattr(summary, "processed", 0)),
            "sent_for_approve": int(getattr(summary, "sent_for_approve", 0)),
            "skipped_due_to_status": int(getattr(summary, "skipped_due_to_status", 0)),
            "skipped_empty": int(getattr(summary, "skipped_empty", 0)),
            "skipped_not_ready": int(getattr(summary, "skipped_not_ready", 0)),
            "skipped_unsupported": int(getattr(summary, "skipped_unsupported", 0)),
            "disaggregated_parents": int(getattr(summary, "disaggregated_parents", 0)),
            "errors": int(getattr(summary, "errors", 0)),
            "lines": list(summary.to_lines()) if hasattr(summary, "to_lines") else [],
        }

    def _raise_if_aggregation_action_noop(self, summary: Dict[str, Any], *, action_label: str) -> None:
        processed = int(summary.get("processed") or 0)
        errors = int(summary.get("errors") or 0)
        sent_for_approve = int(summary.get("sent_for_approve") or 0)
        skipped_not_ready = int(summary.get("skipped_not_ready") or 0)
        skipped_empty = int(summary.get("skipped_empty") or 0)
        skipped_unsupported = int(summary.get("skipped_unsupported") or 0)
        skipped_due_to_status = int(summary.get("skipped_due_to_status") or 0)

        if processed <= 0 or errors > 0 or sent_for_approve > 0:
            return
        if skipped_not_ready == processed and not any((skipped_empty, skipped_unsupported, skipped_due_to_status)):
            raise RuntimeError(
                f"{action_label}: Контур не разрешает обработать выбранные АК в их текущем состоянии. "
                "Если АК уже был зарегистрирован в ГИС МТ, повторная отправка того же состава не создаёт пакет "
                "на регистрацию: для такого случая нужна отдельная переагрегация только по изменяемым кодам."
            )

    def get_bootstrap(self) -> Dict[str, Any]:
        try:
            return {
                "options": self.get_options(),
                "session": self.get_session_info(),
                "orders": self.get_orders_view_state(),
                "download": self.get_download_state(),
                "intro": self.get_intro_state(),
                "tsd": self.get_tsd_state(),
                "aggregation": self.get_aggregation_state(),
                "labels": self.get_labels_state(),
            }
        except Exception as exc:
            return {"error": str(exc)}

    def get_options(self) -> Dict[str, Any]:
        try:
            return {
                "simplified_options": simplified_options,
                "color_options": color_options,
                "size_options": size_options,
                "units_options": units_options,
                "color_required": color_required,
                "venchik_options": venchik_options,
                "venchik_required": venchik_required,
            }
        except Exception as exc:
            return {"error": str(exc)}

    def lookup_gtin(
        self,
        name: str,
        size: str = "",
        units_per_pack: str = "",
        color: str = "",
        venchik: str = "",
    ) -> Dict[str, Any]:
        try:
            if not str(name).strip():
                raise RuntimeError("Укажите название товара.")
            if not str(size).strip():
                raise RuntimeError("Укажите размер.")
            if not str(units_per_pack).strip():
                raise RuntimeError("Укажите количество единиц в упаковке.")
            result = self._build_lookup_payload(
                name=str(name).strip(),
                size=str(size).strip(),
                units_per_pack=str(units_per_pack).strip(),
                color=str(color).strip(),
                venchik=str(venchik).strip(),
            )
            self._log("orders", f"GTIN найден для '{name}': {result.get('gtin')}")
            return result
        except Exception as exc:
            self._log("orders", f"Ошибка поиска GTIN: {exc}")
            return {"error": str(exc)}

    def lookup_gtin_by_code(self, gtin_value: str) -> Dict[str, Any]:
        try:
            result = self._lookup_by_gtin(str(gtin_value or "").strip())
            self._log("orders", f"Поиск по GTIN выполнен: {result.get('gtin')}")
            return result
        except Exception as exc:
            self._log("orders", f"Ошибка поиска по GTIN: {exc}")
            return {"error": str(exc)}

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]] | Dict[str, Any]:
        try:
            safe_limit = max(1, min(int(limit), 500))
            deleted_ids = self._get_deleted_document_ids()
            orders = [
                item
                for item in _get_runtime().history_db.get_all_orders()
                if str(item.get("document_id") or "").strip() not in deleted_ids
            ]
            session = self._ensure_session_safely()
            return [self._normalize_history_item(item, session=session) for item in orders[:safe_limit]]
        except Exception as exc:
            return {"error": str(exc)}

    def get_session_info(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            with runtime.lock:
                age = time.time() - runtime.session_created_at if runtime.session_created_at else 0.0
                return {
                    "has_session": runtime.session is not None,
                    "age_seconds": round(age, 2),
                    "minutes_until_update": round(max(0.0, runtime.session_ttl_seconds - age) / 60.0, 2),
                }
        except Exception as exc:
            return {"error": str(exc)}

    def get_default_date_window(self) -> Dict[str, str]:
        production_date, expiration_date = get_default_production_window()
        return {
            "production_date": production_date,
            "expiration_date": expiration_date,
        }

    def refresh_session(self) -> Dict[str, Any]:
        try:
            self._ensure_session(force_refresh=True, force_browser_refresh=True)
            return {"success": True, "session": self.get_session_info()}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def get_logs(self, channel: str) -> List[str] | Dict[str, Any]:
        try:
            normalized = str(channel or "").strip()
            if normalized not in LOG_CHANNELS:
                raise RuntimeError(f"Неизвестный канал логов: {normalized}")
            return [_normalize_ui_text(line) for line in _get_runtime().logs[normalized]]
        except Exception as exc:
            return {"error": str(exc)}

    def clear_logs(self, channel: str) -> Dict[str, Any]:
        try:
            normalized = str(channel or "").strip()
            if normalized not in LOG_CHANNELS:
                raise RuntimeError(f"Неизвестный канал логов: {normalized}")
            _get_runtime().logs[normalized] = []
            return {"success": True}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def get_orders_view_state(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            deleted_ids = self._get_deleted_document_ids()
            session = self._ensure_session_safely("orders")
            return {
                "queue": [self._serialize_queue_item(item) for item in runtime.order_queue],
                "session_orders": [
                    self._serialize_order_record(item, session=session)
                    for item in runtime.session_orders
                ],
                "history": [
                    self._normalize_history_item(item, session=session)
                    for item in runtime.history_db.get_all_orders()
                    if str(item.get("document_id") or "").strip() not in deleted_ids
                ][:250],
                "deleted_orders": [
                    self._serialize_order_record(item, session=None)
                    | {
                        "deleted_at": item.get("deleted_at") or "",
                        "deleted_by": item.get("deleted_by") or "",
                    }
                    for item in self._load_deleted_orders()[:250]
                ],
            }
        except Exception as exc:
            return {"error": str(exc)}

    def add_order_item(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            item = self._prepare_order_item(payload)
            runtime = _get_runtime()
            runtime.order_queue.append(item)
            self._log("orders", f"Добавлено в очередь: {item['order_name']} ({item['gtin']})")
            return {
                "success": True,
                "item": self._serialize_queue_item(item),
                "queue": [self._serialize_queue_item(queue_item) for queue_item in runtime.order_queue],
            }
        except Exception as exc:
            self._log("orders", f"Ошибка добавления в очередь: {exc}")
            return {"success": False, "error": str(exc)}

    def remove_order_item(self, uid: str) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            runtime.order_queue = [item for item in runtime.order_queue if item.get("uid") != uid]
            return {
                "success": True,
                "queue": [self._serialize_queue_item(queue_item) for queue_item in runtime.order_queue],
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def clear_order_queue(self) -> Dict[str, Any]:
        try:
            _get_runtime().order_queue = []
            self._log("orders", "Очередь заказов очищена")
            return {"success": True, "queue": []}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def delete_order(self, document_id: str) -> Dict[str, Any]:
        try:
            normalized_id = str(document_id or "").strip()
            if not normalized_id:
                raise RuntimeError("Выберите заказ для удаления.")

            runtime = _get_runtime()
            order_data = self._find_order_data(normalized_id)
            if not order_data:
                raise RuntimeError("Заказ не найден в истории.")

            deleted_orders = [item for item in self._load_deleted_orders() if str(item.get("document_id") or "").strip() != normalized_id]
            archived_record = dict(order_data)
            archived_record["deleted_at"] = datetime.now().isoformat()
            archived_record["deleted_by"] = os.getenv("USERNAME", "unknown")
            deleted_orders.insert(0, archived_record)
            self._save_deleted_orders(deleted_orders)

            history_db = runtime.history_db
            with history_db._io_lock:  # type: ignore[attr-defined]
                payload = history_db._load_data()  # type: ignore[attr-defined]
                payload["orders"] = [
                    item
                    for item in payload.get("orders", [])
                    if str(item.get("document_id") or "").strip() != normalized_id
                ]
                history_db._save_data(payload)  # type: ignore[attr-defined]
                sync_locked = getattr(history_db, "_sync_with_github_locked", None)
                if callable(sync_locked):
                    try:
                        sync_locked(push=True, reason="delete_order_ui_v2")
                    except Exception:
                        pass

            runtime.download_items = [
                item for item in runtime.download_items
                if str(item.get("document_id") or "").strip() != normalized_id
            ]
            runtime.session_orders = [
                item for item in runtime.session_orders
                if str(item.get("document_id") or "").strip() != normalized_id
            ]
            runtime.document_status_cache.pop(normalized_id, None)
            runtime.code_status_cache.pop(normalized_id, None)
            self._log("orders", f"Заказ удален в архив: {order_data.get('order_name') or normalized_id}")
            self._log("download", f"Заказ удален из активных списков: {order_data.get('order_name') or normalized_id}")
            self._log("intro", f"Заказ удален из списка ввода в оборот: {order_data.get('order_name') or normalized_id}")
            self._log("tsd", f"Заказ удален из списка ТСД: {order_data.get('order_name') or normalized_id}")
            self._log("labels", f"Заказ удален из печати этикеток: {order_data.get('order_name') or normalized_id}")
            return {"success": True}
        except Exception as exc:
            self._log("orders", f"Ошибка удаления заказа: {exc}")
            return {"success": False, "error": str(exc)}

    def restore_deleted_order(self, document_id: str) -> Dict[str, Any]:
        try:
            normalized_id = str(document_id or "").strip()
            if not normalized_id:
                raise RuntimeError("Выберите удаленный заказ для восстановления.")

            deleted_orders = self._load_deleted_orders()
            restored_order = None
            remaining_orders: List[Dict[str, Any]] = []
            for item in deleted_orders:
                if restored_order is None and str(item.get("document_id") or "").strip() == normalized_id:
                    restored_order = dict(item)
                    continue
                remaining_orders.append(item)

            if not restored_order:
                raise RuntimeError("Удаленный заказ не найден.")

            restored_order.pop("deleted_at", None)
            restored_order.pop("deleted_by", None)
            _get_runtime().history_db.add_order(restored_order)
            self._save_deleted_orders(remaining_orders)
            self._log("orders", f"Заказ восстановлен из архива: {restored_order.get('order_name') or normalized_id}")
            return {"success": True}
        except Exception as exc:
            self._log("orders", f"Ошибка восстановления заказа: {exc}")
            return {"success": False, "error": str(exc)}

    def create_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            item = self._prepare_order_item(payload)
            result = self._submit_order_item(item)
            return {"success": True, **result}
        except Exception as exc:
            self._log("orders", f"Ошибка создания заказа: {exc}")
            return {"success": False, "document_id": "", "status": "", "error": str(exc)}

    def _submit_order_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        session = self._ensure_session()
        positions = [
            {
                "gtin": item["gtin"],
                "name": item["full_name"],
                "tnvedCode": item["tnved_code"],
                "quantity": item["codes_count"],
                "cisType": item["cisType"],
            }
        ]

        self._log("orders", f"Создаём заказ: {item['order_name']}")
        response = codes_order(
            session,
            item["order_name"],
            str(getattr(api_module, "PRODUCT_GROUP", "")),
            str(getattr(api_module, "RELEASE_METHOD_TYPE", "")),
            positions,
            filling_method=str(getattr(api_module, "FILLING_METHOD", "")),
            thumbprint=None,
        )
        if not response:
            raise RuntimeError("API не вернуло результат по созданию заказа.")

        document_id = str(response.get("documentId") or response.get("id") or "")
        status = str(response.get("status") or "unknown")
        history_entry = {
            "order_name": item["order_name"],
            "document_id": document_id,
            "status": status,
            "filename": None,
            "csv_path": None,
            "pdf_path": None,
            "xls_path": None,
            "simpl": item["simpl_name"],
            "full_name": item["full_name"],
            "gtin": item["gtin"],
            "positions": positions,
        }
        existing_history_entry = _get_runtime().history_db.get_order_by_document_id(document_id)
        if existing_history_entry is None:
            _get_runtime().history_db.add_order(history_entry)
        download_item = self._add_download_item(item, document_id)

        result = {
            "document_id": document_id,
            "status": status,
            "order_name": item["order_name"],
            "name": item["simpl_name"],
            "gtin": item["gtin"],
            "full_name": item["full_name"],
            "tnved_code": item["tnved_code"],
            "size": item["size"],
            "color": item["color"],
            "units_per_pack": item["units_per_pack"],
            "codes_count": item["codes_count"],
            "download_item": self._serialize_download_item(download_item),
        }
        _get_runtime().session_orders.insert(0, result)
        self._log("orders", f"Заказ создан: {item['order_name']} ({document_id})")
        self._log("download", f"Добавлен в очередь загрузки: {item['order_name']}")
        return result

    def submit_order_queue(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            if not runtime.order_queue:
                raise RuntimeError("Очередь заказов пуста.")

            results = []
            errors = []
            for item in list(runtime.order_queue):
                try:
                    results.append(self._submit_order_item(item))
                except Exception as exc:
                    errors.append({"order_name": item["order_name"], "error": str(exc)})
                    self._log("orders", f"Ошибка заказа {item['order_name']}: {exc}")

            runtime.order_queue = []
            return {
                "success": not errors,
                "results": results,
                "errors": errors,
                "state": self.get_orders_view_state(),
            }
        except Exception as exc:
            self._log("orders", f"Ошибка выполнения очереди: {exc}")
            return {"success": False, "error": str(exc)}

    def get_download_state(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            runtime.load_download_items_from_history()
            printers, default_printer = list_installed_printers()
            deleted_ids = self._get_deleted_document_ids()
            session = self._ensure_session_safely("download")
            return {
                "items": [
                    self._serialize_download_item(item, session=session)
                    for item in runtime.download_items
                    if str(item.get("document_id") or "").strip() not in deleted_ids
                ],
                "printers": printers,
                "default_printer": default_printer,
            }
        except Exception as exc:
            return {"error": str(exc)}

    def restore_orders_from_history(self, document_ids: Sequence[str]) -> Dict[str, Any]:
        try:
            restored = 0
            for document_id in document_ids:
                doc = str(document_id or "").strip()
                if not doc or self._find_download_item(doc):
                    continue
                order = _get_runtime().history_db.get_order_by_document_id(doc)
                if not order:
                    continue
                _get_runtime().download_items.append(_history_order_to_download_item(order))
                restored += 1
            self._log("download", f"Добавлено из истории в активный список: {restored}")
            return {"success": True, "restored": restored, "state": self.get_download_state()}
        except Exception as exc:
            self._log("download", f"Ошибка восстановления заявок из истории: {exc}")
            return {"success": False, "error": str(exc)}

    def sync_download_statuses(self, auto_download: bool = True) -> Dict[str, Any]:
        try:
            session = self._ensure_session()
            updated = []
            runtime = _get_runtime()
            runtime.load_download_items_from_history()
            deleted_ids = self._get_deleted_document_ids()

            for item in runtime.download_items:
                document_id = str(item.get("document_id") or "").strip()
                if not document_id or document_id in deleted_ids:
                    continue

                if item.get("filename") or item.get("csv_path"):
                    if item.get("status") != "Скачан":
                        item["status"] = "Скачан"
                        self._sync_history_from_download_item(item)
                    updated.append(self._serialize_download_item(item, session=session))
                    continue

                raw_status = check_order_status(session, document_id)
                item["status"] = raw_status
                if raw_status in {"released", "received"} and auto_download:
                    self._download_order_internal(session, item, log_prefix="Автоскачивание: ")
                updated.append(self._serialize_download_item(item, session=session))

            self._log("download", "Синхронизация статусов завершена")
            return {"success": True, "items": updated, "state": self.get_download_state()}
        except Exception as exc:
            self._log("download", f"Ошибка синхронизации статусов: {exc}")
            return {"success": False, "error": str(exc)}

    def manual_download_order(self, document_id: str) -> Dict[str, Any]:
        try:
            item = self._find_download_item(str(document_id or "").strip())
            if not item:
                raise RuntimeError("Заказ не найден в активном списке загрузок.")
            session = self._ensure_session()
            result = self._download_order_internal(session, item)
            return {"success": True, "item": result, "state": self.get_download_state()}
        except Exception as exc:
            self._log("download", f"Ошибка ручного скачивания: {exc}")
            return {"success": False, "error": str(exc)}

    def _resolve_download_print_selection(
        self,
        csv_path: str,
        record_number: Any = None,
    ) -> Dict[str, Any]:
        normalized_csv_path = str(csv_path or "").strip()
        if not normalized_csv_path:
            raise RuntimeError("Не найден CSV-файл заказа для печати.")

        rows, delimiter = self._read_label_csv_rows(Path(normalized_csv_path))
        total_record_count = len(rows)
        if total_record_count <= 0:
            raise RuntimeError("В CSV нет строк для печати.")

        raw_record_number = str(record_number or "").strip()
        if not raw_record_number:
            return {
                "csv_path": normalized_csv_path,
                "cleanup_path": None,
                "total_record_count": total_record_count,
                "selected_record_number": None,
                "record_preview": None,
            }

        try:
            selected_record_number = int(raw_record_number)
        except ValueError as exc:
            raise RuntimeError("Номер этикетки должен быть целым числом.") from exc

        if selected_record_number < 1 or selected_record_number > total_record_count:
            raise RuntimeError(
                f"Номер этикетки должен быть в диапазоне от 1 до {total_record_count}."
            )

        selected_row = rows[selected_record_number - 1]
        temp_csv_path = Path(tempfile.gettempdir()) / f"kontur_ui_v2_download_label_{uuid.uuid4().hex}.csv"
        with temp_csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.writer(csv_file, delimiter=delimiter, lineterminator="\n")
            writer.writerow(selected_row)

        return {
            "csv_path": str(temp_csv_path),
            "cleanup_path": str(temp_csv_path),
            "total_record_count": total_record_count,
            "selected_record_number": selected_record_number,
            "record_preview": self._build_label_record_preview(selected_row, MARKING_SOURCE_KIND),
        }

    def print_download_order(self, document_id: str, printer_name: str, record_number: Any = None) -> Dict[str, Any]:
        try:
            item = self._find_download_item(str(document_id or "").strip())
            if not item:
                raise RuntimeError("Заказ не найден в списке загрузок.")
            csv_path = self._resolve_order_csv_path(item)
            if not csv_path:
                raise RuntimeError("У заказа не найден CSV-файл с кодами маркировки.")
            selection = self._resolve_download_print_selection(csv_path, record_number)
            context = build_print_context(
                order_name=str(item.get("order_name") or ""),
                document_id=str(item.get("document_id") or ""),
                csv_path=str(selection.get("csv_path") or csv_path),
                printer_name=printer_name,
            )
            self._run_background_job(
                name=f"download-print-{context.document_id or uuid.uuid4().hex}",
                action=lambda: print_labels(context),
                error_log_channel="download",
                error_log_prefix="Ошибка печати термоэтикеток",
                cleanup=lambda: self._cleanup_label_selection(selection),
            )
            if selection.get("selected_record_number"):
                preview = selection.get("record_preview") or {}
                self._log(
                    "download",
                    "Печать одной термоэтикетки отправлена в BarTender: "
                    f"{item.get('order_name')}, запись №{selection.get('selected_record_number')} "
                    f"({preview.get('value_short') or 'код не распознан'})",
                )
            else:
                self._log("download", f"Печать термоэтикеток отправлена в BarTender: {item.get('order_name')}")
            return {
                "success": True,
                "context": {
                    "order_name": context.order_name,
                    "document_id": context.document_id,
                    "csv_path": context.csv_path,
                    "template_path": context.template_path,
                    "printer_name": context.printer_name,
                    "size": context.size,
                    "label_count": context.label_count,
                },
                "selection": {
                    "total_record_count": int(selection.get("total_record_count") or 0),
                    "selected_record_number": selection.get("selected_record_number"),
                    "record_preview": selection.get("record_preview"),
                },
            }
        except Exception as exc:
            self._log("download", f"Ошибка печати термоэтикеток: {exc}")
            return {"success": False, "error": str(exc)}

    def get_intro_state(self) -> Dict[str, Any]:
        try:
            deleted_ids = self._get_deleted_document_ids()
            intro_items = [
                item
                for item in self._collect_known_orders()
                if str(item.get("document_id") or "").strip()
                and str(item.get("document_id") or "").strip() not in deleted_ids
            ]
            return {
                "items": [
                    self._normalize_history_item(
                        item,
                        session=None,
                        include_marking_status=False,
                    )
                    for item in intro_items
                ]
            }
        except Exception as exc:
            return {"error": str(exc)}

    def introduce_orders(
        self,
        document_ids: Sequence[str],
        production_date: str,
        expiration_date: str,
        batch_number: str,
    ) -> Dict[str, Any]:
        try:
            if not document_ids:
                raise RuntimeError("Выберите хотя бы один заказ для ввода в оборот.")
            normalized_batch = str(batch_number or "").strip()
            if not normalized_batch:
                raise RuntimeError("Укажите номер партии.")
            session = self._ensure_session()
            thumbprint = self._get_thumbprint()
            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            results = []
            errors = []

            for document_id in document_ids:
                normalized_id = str(document_id or "").strip()
                item = self._get_order_for_document_id(normalized_id)
                if not item:
                    errors.append({"document_id": document_id, "error": "Заказ не найден"})
                    continue

                try:
                    item = self._ensure_order_downloaded_for_intro(session, item)
                except Exception as exc:
                    errors.append({"document_id": document_id, "error": str(exc)})
                    continue

                self._log("intro", f"Запускаем ввод в оборот: {item.get('order_name')}")
                patch = self._build_intro_patch(item, prod, exp, normalized_batch)
                ok, result = put_into_circulation(
                    session=session,
                    codes_order_id=str(item.get("document_id") or ""),
                    production_patch=patch,
                    organization_id=os.getenv("ORGANIZATION_ID"),
                    thumbprint=thumbprint,
                    check_poll_interval=10,
                    check_poll_attempts=30,
                )

                if ok:
                    item["status"] = "Введен в оборот"
                    self._sync_history_from_download_item(item)
                    self._log("intro", f"Успешно: {item.get('order_name')} ({result.get('introduction_id')})")
                    results.append({"document_id": document_id, "result": result})
                else:
                    item["status"] = "Ошибка ввода"
                    self._sync_history_from_download_item(item)
                    error_text = "; ".join(result.get("errors", [])) or "Неизвестная ошибка"
                    self._log("intro", f"Ошибка: {item.get('order_name')} - {error_text}")
                    errors.append({"document_id": document_id, "error": error_text, "result": result})

            return {
                "success": not errors,
                "results": results,
                "errors": errors,
                "state": self.get_intro_state(),
            }
        except Exception as exc:
            self._log("intro", f"Ошибка ввода в оборот: {exc}")
            return {"success": False, "error": str(exc)}

    def introduce_saved_order_exact(
        self,
        document_id: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
        excluded_codes: Sequence[str] | None = None,
        document_title: str = "",
    ) -> Dict[str, Any]:
        try:
            normalized_id = str(document_id or "").strip()
            if not normalized_id:
                raise RuntimeError("Не указан document_id заказа.")

            normalized_batch = str(batch_number or "").strip()
            if not normalized_batch:
                raise RuntimeError("Укажите номер партии.")

            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            normalized_title = " ".join(str(document_title or "").strip().split())

            session = self._ensure_session()
            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            item = self._find_download_item(normalized_id)
            if not item:
                history_order = _get_runtime().history_db.get_order_by_document_id(normalized_id)
                if isinstance(history_order, dict):
                    item = _history_order_to_download_item(history_order)
            if not item:
                raise RuntimeError("Заказ не найден в истории.")

            csv_path = self._resolve_order_csv_path(item)
            if not csv_path:
                raise RuntimeError("Не найден CSV-файл заказа в папке 'Коды км'.")

            rows = list(self._iter_saved_marking_rows(Path(csv_path)))
            if not rows:
                raise RuntimeError("CSV-файл заказа пуст или не содержит кодов маркировки.")

            excluded_sntins = {
                extract_sntin(str(code or "").strip())
                for code in (excluded_codes or [])
                if extract_sntin(str(code or "").strip())
            }

            filtered_rows: List[Dict[str, Any]] = []
            seen_sntins: set[str] = set()
            for row in rows:
                full_code = str(row.get("full_code") or "").strip()
                if not full_code:
                    continue
                sntin = extract_sntin(full_code)
                if not sntin or sntin in seen_sntins or sntin in excluded_sntins:
                    continue
                seen_sntins.add(sntin)
                filtered_rows.append(row)

            if not filtered_rows:
                raise RuntimeError("После исключения уже введённых КМ не осталось кодов для отправки.")

            order_name = str(item.get("order_name") or Path(csv_path).parent.name or normalized_id).strip()
            metadata = self._lookup_intro_product_metadata(
                str(item.get("gtin") or filtered_rows[0].get("gtin") or "").strip(),
                str(item.get("full_name") or filtered_rows[0].get("full_name") or order_name).strip(),
            )
            document_number = normalized_title or order_name

            self._log(
                "intro",
                f"Точный ввод в оборот: {order_name}. Исключаем {len(excluded_sntins)} КМ, отправляем {len(filtered_rows)} КМ.",
            )
            introduction_id = self._create_exact_intro_file_document(
                session,
                product_group=str(getattr(api_module, "PRODUCT_GROUP", "wheelChairs")),
                order_name=order_name,
                document_number=document_number,
                production_date=prod,
                expiration_date=exp,
                batch_number=normalized_batch,
            )
            self._log("intro", f"Документ ввода в оборот создан: {introduction_id}. Загружаем точные коды.")

            rows_payload = self._build_intro_upload_rows(
                metadata=metadata,
                full_codes=[row["full_code"] for row in filtered_rows],
                fallback_name=order_name,
            )
            self._upload_intro_positions_from_file(
                session,
                introduction_id,
                rows_payload=rows_payload,
            )
            self._log("intro", f"Позиции загружены в документ {introduction_id}. Ждём проверку кодов.")

            codes_check = self._wait_for_intro_codes_check(session, introduction_id)
            finalize_result = self._finalize_intro_document_after_check(
                session,
                introduction_id,
                cert=cert,
                codes_check=codes_check,
                uploaded_codes_count=len(filtered_rows),
                log_channel="intro",
                source_label=order_name,
            )
            send_result = finalize_result["send_result"]
            actual_sent_codes_count = int(finalize_result.get("actual_sent_codes_count") or 0)

            self._log(
                "intro",
                f"Точный ввод в оборот отправлен: {order_name} "
                f"({actual_sent_codes_count} из {len(filtered_rows)} КМ, документ {introduction_id}).",
            )
            return {
                "success": True,
                "document_id": normalized_id,
                "introduction_id": introduction_id,
                "excluded_codes_count": len(excluded_sntins),
                "sent_codes_count": actual_sent_codes_count,
                "csv_path": csv_path,
                "result": send_result,
            }
        except Exception as exc:
            self._log("intro", f"Ошибка точного ввода в оборот: {exc}")
            return {"success": False, "error": str(exc)}

    def get_tsd_state(self, live: bool = False) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            deleted_ids = self._get_deleted_document_ids()
            session = self._ensure_session_safely("tsd") if live else None
            orders: List[Dict[str, Any]] = []
            for item in runtime.history_db.get_all_orders():
                document_id = str(item.get("document_id") or "").strip()
                if not document_id or document_id in deleted_ids:
                    continue
                download_like = _history_order_to_download_item(item)
                if item.get("tsd_created") or is_order_ready_for_tsd(download_like):
                    orders.append(item)

            orders.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
            return {
                "items": [
                    self._normalize_history_item(
                        item,
                        session=session,
                        include_marking_status=bool(live) and self._should_include_marking_status(item),
                    )
                    for item in orders[:120]
                ],
                "live": bool(live),
            }
        except Exception as exc:
            return {"error": str(exc)}

    def add_history_orders_to_active(self, document_ids: Sequence[str]) -> Dict[str, Any]:
        return self.restore_orders_from_history(document_ids)

    def create_tsd_tasks(
        self,
        document_ids: Sequence[str],
        intro_number: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
    ) -> Dict[str, Any]:
        try:
            if not document_ids:
                raise RuntimeError("Выберите хотя бы один заказ для задания на ТСД.")
            if not str(intro_number or "").strip():
                raise RuntimeError("Укажите номер ввода в оборот.")

            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            results = []
            errors = []
            normalized_ids = [str(document_id or "").strip() for document_id in document_ids if str(document_id or "").strip()]
            total = len(normalized_ids)

            for index, document_id in enumerate(normalized_ids, start=1):
                self._log("tsd", f"Прогресс создания заданий: {index - 1}/{total}")
                item = self._find_download_item(document_id)
                if not item:
                    history_order = _get_runtime().history_db.get_order_by_document_id(document_id)
                    if history_order:
                        item = _history_order_to_download_item(history_order)
                    else:
                        errors.append({"document_id": document_id, "error": "Заказ не найден"})
                        continue

                retry_tsd = bool(item.get("tsd_created") or (item.get("history_data") or {}).get("tsd_created"))
                if retry_tsd:
                    item = dict(item)
                    item["status"] = "Готов для ТСД"

                if not retry_tsd and not is_order_ready_for_tsd(item):
                    errors.append({"document_id": document_id, "error": "Заказ ещё не готов для задания на ТСД"})
                    continue

                action_label = "Повторно создаём задание на ТСД" if retry_tsd else "Создаём задание на ТСД"
                self._log("tsd", f"{action_label}: {item.get('order_name')}")
                try:
                    ok, result = self._create_tsd_task_with_retry(
                        item=item,
                        intro_number=str(intro_number or "").strip(),
                        production_date=prod,
                        expiration_date=exp,
                        batch_number=str(batch_number or "").strip(),
                    )
                except Exception as exc:
                    error_text = str(exc)
                    self._log("tsd", f"Ошибка ТСД: {item.get('order_name')} - {error_text}")
                    errors.append({"document_id": document_id, "error": error_text})
                    continue
                if ok:
                    introduction_id = result.get("introduction_id", "")
                    self._mark_tsd_created_local(str(item.get("document_id") or ""), introduction_id)
                    remove_order_by_document_id(_get_runtime().download_items, str(item.get("document_id") or ""))
                    _get_runtime().document_status_cache.pop(document_id, None)
                    self._log("tsd", f"Задание на ТСД создано: {item.get('order_name')} ({introduction_id})")
                    results.append({"document_id": document_id, "result": result})
                else:
                    error_text = "; ".join(result.get("errors", [])) or "Неизвестная ошибка"
                    self._log("tsd", f"Ошибка ТСД: {item.get('order_name')} - {error_text}")
                    errors.append({"document_id": document_id, "error": error_text, "result": result})

            return {
                "success": not errors,
                "results": results,
                "errors": errors,
                "processed": len(results) + len(errors),
                "total": total,
            }
        except Exception as exc:
            self._log("tsd", f"Ошибка создания заданий на ТСД: {exc}")
            return {"success": False, "error": str(exc)}

    def _list_aggregation_documents(
        self,
        session: requests.Session,
        *,
        status_filters: Sequence[str] | None = None,
    ) -> List[Dict[str, Any]]:
        base_url = f"{str(os.getenv('BASE_URL') or 'https://mk.kontur.ru').rstrip('/')}/api/v1/aggregates"
        warehouse_id = str(os.getenv("WAREHOUSE_ID") or getattr(api_module, "WAREHOUSE_ID", ""))
        statuses = [
            str(status).strip()
            for status in (status_filters or AGGREGATION_FETCH_STATUSES)
            if str(status).strip()
        ]
        if not warehouse_id:
            raise RuntimeError("Не задан WAREHOUSE_ID для загрузки списка АК.")

        rows: List[Dict[str, Any]] = []
        seen_document_ids: set[str] = set()
        page_size = 1000
        status_batches: List[Optional[str]] = statuses or [None]
        for status_filter in status_batches:
            offset = 0
            status_label = _translate_status(status_filter) if status_filter else "всех статусов"
            while True:
                try:
                    params = {
                        "warehouseId": warehouse_id,
                        "limit": page_size,
                        "offset": offset,
                        "sortField": "createDate",
                        "sortOrder": "descending",
                    }
                    if status_filter:
                        params["statuses"] = status_filter
                    response = session.get(
                        base_url,
                        params=params,
                        timeout=30,
                    )
                except requests.RequestException as exc:
                    self._log(
                        "aggregation",
                        (
                            f"Контур не отдал страницу списка АК для "
                            f"{status_label} (offset {offset}): {exc}. "
                            f"Показываем уже загруженные АК."
                        ),
                    )
                    break
                if response.status_code in {400, 404}:
                    break
                if response.status_code >= 500:
                    self._log(
                        "aggregation",
                        (
                            f"Контур вернул {response.status_code} при загрузке АК "
                            f"для {status_label} "
                            f"(offset {offset}). Показываем уже загруженные АК."
                        ),
                    )
                    break
                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError as exc:
                    self._log(
                        "aggregation",
                        (
                            f"Контур вернул некорректный JSON для статуса "
                            f"{status_label} (offset {offset}): {exc}. "
                            f"Показываем уже загруженные АК."
                        ),
                    )
                    break
                items = payload.get("items") or []
                for item in items:
                    document_id = str(item.get("documentId") or "").strip()
                    aggregate_code = str(item.get("aggregateCode") or "").strip()
                    if not document_id or not aggregate_code or document_id in seen_document_ids:
                        continue
                    seen_document_ids.add(document_id)
                    raw_status = str(item.get("status") or "").strip()
                    comment = str(item.get("comment") or "").strip()
                    created_at = str(item.get("createdDate") or item.get("createDate") or "").strip()
                    rows.append(
                        {
                            "document_id": document_id,
                            "aggregate_code": aggregate_code,
                            "comment": comment,
                            "status": raw_status,
                            "status_label": _translate_status(raw_status),
                            "created_at": created_at,
                            "created_at_label": _format_datetime_value(created_at),
                            "product_group": str(item.get("productGroup") or "").strip(),
                            "includes_units_count": int(item.get("includesUnitsCount") or 0),
                            "codes_check_errors_count": int(item.get("codesCheckErrorsCount") or 0),
                        }
                    )
                total = int(payload.get("total") or len(items))
                offset += len(items)
                if not items or offset >= total:
                    break

        rows.sort(
            key=lambda row: (
                str(row.get("created_at") or ""),
                str(row.get("comment") or ""),
                str(row.get("aggregate_code") or ""),
            ),
            reverse=True,
        )
        return rows

    def _get_aggregation_items_cached(
        self,
        session: Optional[requests.Session],
        *,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        runtime = _get_runtime()
        now = time.time()
        cache_is_fresh = (
            bool(runtime.aggregation_cache_items)
            and runtime.aggregation_cache_at > 0
            and (now - runtime.aggregation_cache_at) <= runtime.aggregation_cache_ttl_seconds
        )
        if not force_refresh and cache_is_fresh:
            return list(runtime.aggregation_cache_items)
        if not session:
            return list(runtime.aggregation_cache_items)
        items = self._list_aggregation_documents(session)
        runtime.aggregation_cache_items = list(items)
        runtime.aggregation_cache_at = now
        return list(items)

    def _invalidate_aggregation_cache(self) -> None:
        runtime = _get_runtime()
        runtime.aggregation_cache_items = []
        runtime.aggregation_cache_at = 0.0

    def _resolve_aggregate_infos_by_ids(
        self,
        session: requests.Session,
        document_ids: Sequence[str],
    ) -> List[AggregateInfo]:
        service = _get_runtime().bulk_aggregation_service
        aggregates: List[AggregateInfo] = []
        seen_document_ids: set[str] = set()
        for raw_document_id in document_ids:
            document_id = str(raw_document_id or "").strip()
            if not document_id or document_id in seen_document_ids:
                continue
            seen_document_ids.add(document_id)
            detail = service.fetch_aggregate_detail(session, document_id)
            aggregates.append(
                AggregateInfo(
                    document_id=detail.document_id,
                    aggregate_code=detail.aggregate_code,
                    comment=detail.comment,
                    status=detail.status,
                    product_group=detail.product_group,
                    includes_units_count=detail.includes_units_count,
                    codes_check_errors_count=detail.codes_check_errors_count,
                )
            )
        return aggregates

    def get_aggregation_state(self, force_refresh: bool = False) -> Dict[str, Any]:
        try:
            session = self._ensure_session_safely("aggregation")
            items = self._get_aggregation_items_cached(session, force_refresh=bool(force_refresh))
            cache_age = 0
            runtime = _get_runtime()
            if runtime.aggregation_cache_at > 0:
                cache_age = max(0, int(time.time() - runtime.aggregation_cache_at))
            return {
                "items": items,
                "status_options": [
                    {"value": "", "label": "Все статусы"},
                    *[
                        {"value": status, "label": _translate_status(status)}
                        for status in AGGREGATION_TABLE_STATUSES
                    ],
                ],
                "cache_age_seconds": cache_age,
                "total_items": len(items),
                "logs": list(_get_runtime().logs["aggregation"]),
            }
        except Exception as exc:
            return {"error": str(exc)}

    def create_aggregation_codes(self, comment: str, count: int) -> Dict[str, Any]:
        try:
            normalized_comment = str(comment or "").strip()
            normalized_count = int(count or 0)
            if not normalized_comment:
                raise RuntimeError("Введите название агрегации.")
            if normalized_count <= 0:
                raise RuntimeError("Количество агрегатов должно быть больше нуля.")
            batch_limit = 99
            remaining = normalized_count
            batch_counts: List[int] = []
            while remaining > 0:
                batch_size = min(batch_limit, remaining)
                batch_counts.append(batch_size)
                remaining -= batch_size

            total_batches = len(batch_counts)
            created_items: List[Any] = []
            for batch_index, batch_count in enumerate(batch_counts, start=1):
                self._log(
                    "aggregation",
                    f"Запрос {batch_index}/{total_batches}: создаем {batch_count} кодов агрегации",
                )
                batch_items = self._run_with_session_retry(
                    lambda session, current_count=batch_count: self._create_aggregate_codes(
                        session,
                        normalized_comment,
                        current_count,
                    ),
                    log_channel="aggregation",
                    retry_message="Получили ошибку создания АК, обновляем cookies и повторяем",
                )
                created_items.extend(batch_items)
                self._log(
                    "aggregation",
                    f"Запрос {batch_index}/{total_batches} выполнен: получено {len(batch_items)} кодов",
                )

            self._invalidate_aggregation_cache()
            self._log("aggregation", f"Создано АК: {normalized_comment}, количество {len(created_items)}")
            return {
                "success": True,
                "created_count": len(created_items),
                "batch_count": total_batches,
                "items": created_items,
            }
        except Exception as exc:
            self._log("aggregation", f"Ошибка создания АК: {exc}")
            return {"success": False, "error": str(exc)}

    def download_aggregation_codes(
        self,
        mode: str,
        target_value: str,
        status_filter: str = "tsdProcessStart",
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            normalized_mode = str(mode or "").strip()
            normalized_target = str(target_value or "").strip()
            if normalized_mode not in {"comment", "count"}:
                raise RuntimeError("Неверный режим скачивания агрегационных кодов.")
            if normalized_mode == "count" and int(normalized_target or "0") <= 0:
                raise RuntimeError("Введите корректное количество агрегационных кодов.")
            items = self._run_with_session_retry(
                lambda session: self._download_aggregate_codes(
                    session,
                    normalized_mode,
                    normalized_target,
                    status_filter=status_filter,
                    limit=limit,
                ),
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторной загрузкой АК",
            )
            if normalized_mode == "count":
                filename = f"Коды_агрегации_{normalized_target}_шт.csv"
            else:
                safe_comment = "".join(char for char in normalized_target if char.isalnum() or char in " -_").strip()[:80]
                filename = f"{safe_comment}_{len(items)}.csv"
            saved_path = self._save_simple_aggregation_csv(items, filename)
            self._log("aggregation", f"Скачано АК: {len(items)}, сохранено в {saved_path}")
            return {
                "success": True,
                "count": len(items),
                "items": items,
                "saved_path": saved_path,
                "state": self.get_aggregation_state(),
            }
        except Exception as exc:
            self._log("aggregation", f"Ошибка скачивания АК: {exc}")
            return {"success": False, "error": str(exc)}

    def download_selected_aggregations(self, document_ids: Sequence[str]) -> Dict[str, Any]:
        try:
            normalized_ids = [str(document_id or "").strip() for document_id in document_ids if str(document_id or "").strip()]
            if not normalized_ids:
                raise RuntimeError("Выберите хотя бы один АК.")

            def _run(session: requests.Session) -> Dict[str, Any]:
                aggregates = self._resolve_aggregate_infos_by_ids(session, normalized_ids)
                if not aggregates:
                    raise RuntimeError("Не удалось загрузить выбранные АК из Контур.Маркировки.")
                items = [
                    {
                        "aggregateCode": aggregate.aggregate_code,
                        "documentId": aggregate.document_id,
                        "createdDate": None,
                        "status": aggregate.status,
                        "updatedDate": None,
                        "includesUnitsCount": aggregate.includes_units_count,
                        "comment": aggregate.comment,
                        "productGroup": aggregate.product_group,
                        "aggregationType": "gs1GlnAggregate",
                        "codesChecked": None,
                        "codesCheckErrorsCount": aggregate.codes_check_errors_count,
                    }
                    for aggregate in aggregates
                    if str(aggregate.aggregate_code or "").strip()
                ]
                if not items:
                    raise RuntimeError("У выбранных АК нет кодов агрегации для скачивания.")

                grouped_items: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for item in items:
                    comment = " ".join(str(item.get("comment") or "").strip().split()) or "Без названия"
                    grouped_items[comment].append(item)

                saved_groups: List[Dict[str, Any]] = []
                saved_paths: List[str] = []
                for comment, group_rows in sorted(grouped_items.items(), key=lambda pair: pair[0]):
                    safe_comment = "".join(char for char in comment if char.isalnum() or char in " -_").strip()[:80]
                    filename = f"{safe_comment or 'selected_aggregations'}_{len(group_rows)}.csv"
                    saved_path = self._save_simple_aggregation_csv(group_rows, filename)
                    saved_paths.append(saved_path)
                    saved_groups.append(
                        {
                            "comment": comment,
                            "count": len(group_rows),
                            "saved_path": saved_path,
                        }
                    )
                    self._log(
                        "aggregation",
                        f"Скачано АК '{comment}': {len(group_rows)}, сохранено в {saved_path}",
                    )
                return {
                    "count": len(items),
                    "items": items,
                    "saved_path": saved_paths[0] if len(saved_paths) == 1 else "",
                    "saved_paths": saved_paths,
                    "groups": saved_groups,
                }

            result = self._run_with_session_retry(
                _run,
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторным скачиванием выбранных АК",
            )
            return {"success": True, **result}
        except Exception as exc:
            self._log("aggregation", f"Ошибка скачивания выбранных АК: {exc}")
            return {"success": False, "error": str(exc)}

    def approve_selected_aggregations(
        self,
        document_ids: Sequence[str],
        allow_disaggregate: bool = False,
    ) -> Dict[str, Any]:
        try:
            normalized_ids = [str(document_id or "").strip() for document_id in document_ids if str(document_id or "").strip()]
            if not normalized_ids:
                raise RuntimeError("Выберите хотя бы один АК.")

            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            self._log("aggregation", f"Запускаем проведение выбранных АК: {len(normalized_ids)} шт.")

            def _run(session: requests.Session) -> Dict[str, Any]:
                service = _get_runtime().bulk_aggregation_service
                aggregates = self._resolve_aggregate_infos_by_ids(session, normalized_ids)
                summary = BulkAggregationSummary()
                summary.ready_found = len(aggregates)
                service._run_sequential(
                    ready_aggregates=aggregates,
                    kontur_session=session,
                    cert_provider=lambda: cert,
                    sign_base64_func=sign_data,
                    sign_text_func=sign_text_data,
                    log=lambda message: self._log("aggregation", message),
                    progress=lambda processed, total: self._log("aggregation", f"Прогресс проведения: {processed}/{total}"),
                    confirm=lambda _title, _message: bool(allow_disaggregate),
                    summary=summary,
                )
                return self._serialize_summary(summary)

            summary = self._run_with_session_retry(
                _run,
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторным проведением выбранных АК",
            )
            self._raise_if_aggregation_action_noop(summary, action_label="Проведение АК")
            return {"success": True, "summary": summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка проведения выбранных АК: {exc}")
            return {"success": False, "error": str(exc)}

    def introduce_selected_aggregations(
        self,
        document_ids: Sequence[str],
        production_date: str,
        expiration_date: str,
        batch_number: str,
        document_title: str = "",
    ) -> Dict[str, Any]:
        try:
            normalized_ids = [str(document_id or "").strip() for document_id in document_ids if str(document_id or "").strip()]
            if not normalized_ids:
                raise RuntimeError("Выберите хотя бы один АК.")

            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            normalized_batch = str(batch_number or "").strip()
            normalized_title = " ".join(str(document_title or "").strip().split())
            if not normalized_batch:
                raise RuntimeError("Укажите номер партии.")

            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            self._log("aggregation", f"Запускаем ввод в оборот кодов из выбранных АК: {len(normalized_ids)} шт.")

            def _run(session: requests.Session) -> Dict[str, Any]:
                self._log("aggregation", f"Загружаем выбранные АК из Контура: {len(normalized_ids)} шт.")
                aggregates = self._resolve_aggregate_infos_by_ids(session, normalized_ids)
                if not aggregates:
                    raise RuntimeError("Не удалось загрузить выбранные АК из Контур.Маркировки.")

                aggregate_codes: List[str] = []
                skipped_nested: List[str] = []
                service = _get_runtime().bulk_aggregation_service
                self._log("aggregation", f"Считываем коды маркировки из выбранных АК: {len(aggregates)} шт.")
                for aggregate in aggregates:
                    raw_codes, reaggregation_codes = service.fetch_aggregate_codes(session, aggregate.document_id)
                    if reaggregation_codes:
                        skipped_nested.append(aggregate.aggregate_code or aggregate.document_id)
                        self._log(
                            "aggregation",
                            f"Пропускаем АК {aggregate.aggregate_code or aggregate.document_id}: обнаружены вложенные АК ({len(reaggregation_codes)}).",
                        )
                        continue
                    aggregate_codes.extend(raw_codes)

                unique_codes = list(dict.fromkeys(code for code in aggregate_codes if str(code or "").strip()))
                if not unique_codes:
                    raise RuntimeError("В выбранных АК нет кодов маркировки для ввода в оборот.")

                product_group = next((aggregate.product_group for aggregate in aggregates if aggregate.product_group), "wheelChairs")
                true_product_group = service._resolve_true_product_group(product_group)
                states = self._fetch_code_states_resilient(
                    service=service,
                    cert=cert,
                    product_group=true_product_group,
                    raw_codes=unique_codes,
                    context_label="ввода в оборот выбранных АК",
                )
                status_counts = Counter(state.status or "UNKNOWN" for state in states)
                api_errors = [state for state in states if state.api_error]
                if api_errors and len(api_errors) == len(states):
                    preview = ", ".join(state.sntin for state in api_errors[:5])
                    raise RuntimeError(
                        f"Не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. Примеры: {preview}"
                    )

                target_codes: List[str] = []
                if api_errors:
                    preview = ", ".join(state.sntin for state in api_errors[:5])
                    self._log(
                        "aggregation",
                        f"Ввод в оборот выбранных АК: не удалось получить статусы части кодов в Честном Знаке: {len(api_errors)} шт. Примеры: {preview}. Продолжаем обработку остальных кодов.",
                    )
                already_introduced = 0
                unsupported_states: List[Any] = []
                for state in states:
                    if state.api_error:
                        continue
                    normalized_status = str(state.status or "").upper()
                    if normalized_status in {"INTRODUCED", "APPLIED"}:
                        already_introduced += 1
                        continue
                    if normalized_status == "EMITTED":
                        target_codes.append(state.raw_code)
                        continue
                    unsupported_states.append(state)

                self._log(
                    "aggregation",
                    f"Статусы КМ из выбранных АК: {_format_status_counts(status_counts)}. Уже введено в оборот: {already_introduced}.",
                )
                if unsupported_states:
                    preview = ", ".join(f"{state.sntin} ({state.status})" for state in unsupported_states[:5])
                    raise RuntimeError(
                        f"Часть кодов из выбранных АК нельзя ввести в оборот автоматически: {len(unsupported_states)} шт. Примеры: {preview}"
                    )
                if not target_codes:
                    raise RuntimeError("Все коды из выбранных АК уже введены в оборот.")

                self._log("aggregation", f"Ищем полные коды в папке 'Коды км': {len(target_codes)} шт.")
                match_result = self._match_saved_marking_codes(target_codes)
                prepared_match = self._prepare_marking_match_result(
                    match_result,
                    action_label="Ввод в оборот выбранных АК",
                )

                introduced_results: List[Dict[str, Any]] = []
                total_sent_codes = 0
                self._log(
                    "aggregation",
                    f"Полные коды найдены в папке 'Коды км': {prepared_match['matched_count']} шт., "
                    f"не найдено: {prepared_match['unmatched_count']} шт., файлов просмотрено: {prepared_match['scanned_files']}.",
                )
                groups = prepared_match["groups"]
                groups_total = len(groups)
                for index, group in enumerate(groups, start=1):
                    source_order_name = str(group.get("order_name") or "").strip() or f"Группа {index}"
                    codes = [row["full_code"] for row in group.get("codes", []) if str(row.get("full_code") or "").strip()]
                    if not codes:
                        continue

                    metadata = self._lookup_intro_product_metadata(
                        str(group.get("gtin") or "").strip(),
                        str(group.get("full_name") or "").strip(),
                    )
                    document_number = self._build_aggregate_intro_document_number(
                        source_order_name,
                        index,
                        custom_title=normalized_title,
                        groups_total=groups_total,
                    )
                    self._log(
                        "aggregation",
                        f"Создаём документ ввода в оборот для группы {index}/{groups_total}: {source_order_name} ({len(codes)} кодов).",
                    )
                    introduction_id = self._create_exact_intro_file_document(
                        session,
                        product_group=product_group,
                        order_name=source_order_name,
                        document_number=document_number,
                        production_date=prod,
                        expiration_date=exp,
                        batch_number=normalized_batch,
                    )
                    self._log("aggregation", f"Документ ввода в оборот создан: {introduction_id}. Загружаем позиции.")
                    rows_payload = self._build_intro_upload_rows(
                        metadata=metadata,
                        full_codes=codes,
                        fallback_name=source_order_name,
                    )
                    self._upload_intro_positions_from_file(
                        session,
                        introduction_id,
                        rows_payload=rows_payload,
                    )
                    self._log("aggregation", f"Позиции загружены в документ {introduction_id}. Ждём проверку кодов.")
                    codes_check = self._wait_for_intro_codes_check(session, introduction_id)
                    finalize_result = self._finalize_intro_document_after_check(
                        session,
                        introduction_id,
                        cert=cert,
                        codes_check=codes_check,
                        uploaded_codes_count=len(codes),
                        log_channel="aggregation",
                        source_label=source_order_name,
                    )
                    send_result = finalize_result["send_result"]
                    actual_sent_codes_count = int(finalize_result.get("actual_sent_codes_count") or 0)

                    introduced_results.append(
                        {
                            "introduction_id": introduction_id,
                            "order_name": source_order_name,
                            "source_path": str(group.get("source_path") or ""),
                            "gtin": metadata["gtin"],
                            "codes_count": len(codes),
                            "actual_sent_codes_count": actual_sent_codes_count,
                            "result": send_result,
                        }
                    )
                    total_sent_codes += actual_sent_codes_count
                    self._log(
                        "aggregation",
                        f"Ввод в оборот отправлен: {source_order_name} "
                        f"({actual_sent_codes_count} из {len(codes)} кодов, документ {introduction_id}).",
                    )

                return {
                    "selected_aggregates": len(aggregates),
                    "skipped_nested": skipped_nested,
                    "checked_codes": len(unique_codes),
                    "matched_codes": prepared_match["matched_count"],
                    "missing_full_codes": prepared_match["unmatched_count"],
                    "missing_full_codes_preview": prepared_match["unmatched_preview"],
                    "already_introduced_codes": already_introduced,
                    "skipped_api_error_codes": len(api_errors),
                    "skipped_api_error_preview": [state.sntin for state in api_errors[:5]],
                    "introduced_codes": total_sent_codes,
                    "groups": introduced_results,
                    "status_counts": dict(status_counts),
                    "scanned_saved_files": prepared_match["scanned_files"],
                }

            summary = self._run_with_transient_network_retry(
                lambda: self._run_with_session_retry(
                    _run,
                    log_channel="aggregation",
                    retry_message="Обновляем сессию перед повторной отправкой ввода в оборот выбранных АК",
                ),
                attempts=3,
                log_channel="aggregation",
                retry_message="Сетевое соединение оборвалось при вводе в оборот выбранных АК. Обновляем сессию и повторяем",
            )
            return {"success": True, "summary": summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка ввода в оборот выбранных АК: {exc}")
            return {"success": False, "error": str(exc)}

    def approve_aggregations(self, comment_filter: str = "", allow_disaggregate: bool = False) -> Dict[str, Any]:
        try:
            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            self._log("aggregation", "Запускаем проведение АК в статусах readyForSend и approveFailed")
            summary = self._run_with_session_retry(
                lambda session: _get_runtime().bulk_aggregation_service.run(
                    kontur_session=session,
                    cert_provider=lambda: cert,
                    sign_base64_func=sign_data,
                    sign_text_func=sign_text_data,
                    log_callback=lambda message: self._log("aggregation", message),
                    progress_callback=lambda processed, total: self._log("aggregation", f"Прогресс проведения: {processed}/{total}"),
                    confirm_callback=lambda _title, _message: bool(allow_disaggregate),
                    comment_filter=str(comment_filter or "").strip() or None,
                ),
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторным проведением АК",
            )
            serialized_summary = self._serialize_summary(summary)
            self._raise_if_aggregation_action_noop(serialized_summary, action_label="Проведение АК")
            return {"success": True, "summary": serialized_summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка проведения АК: {exc}")
            return {"success": False, "error": str(exc)}

    def refill_aggregations(self, comment_filter: str, tsd_token: str) -> Dict[str, Any]:
        try:
            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")
            normalized_filter = str(comment_filter or "").strip()
            normalized_token = str(tsd_token or "").strip()
            if not normalized_filter:
                raise RuntimeError("Введите название для повторного наполнения АК.")
            if not normalized_token:
                raise RuntimeError("Введите TSD токен.")

            self._log("aggregation", f"Запускаем повторное наполнение АК по названию '{normalized_filter}'")
            summary = self._run_with_session_retry(
                lambda session: _get_runtime().bulk_aggregation_service.run_tsd_refill(
                    kontur_session=session,
                    cert_provider=lambda: cert,
                    sign_base64_func=sign_data,
                    tsd_token=normalized_token,
                    log_callback=lambda message: self._log("aggregation", message),
                    progress_callback=lambda processed, total: self._log("aggregation", f"Прогресс повторного наполнения: {processed}/{total}"),
                    comment_filter=normalized_filter,
                ),
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторным наполнением АК",
            )
            serialized_summary = self._serialize_summary(summary)
            self._raise_if_aggregation_action_noop(serialized_summary, action_label="Повторное наполнение АК")
            return {"success": True, "summary": serialized_summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка повторного наполнения АК: {exc}")
            return {"success": False, "error": str(exc)}

    def introduce_aggregations(
        self,
        comment_filter: str,
        tsd_token: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
    ) -> Dict[str, Any]:
        try:
            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            normalized_token = str(tsd_token or "").strip()
            normalized_batch = str(batch_number or "").strip()
            if not normalized_token:
                raise RuntimeError("Введите TSD токен.")
            if not normalized_batch:
                raise RuntimeError("Укажите номер партии.")

            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            normalized_filter = str(comment_filter or "").strip()
            if normalized_filter:
                self._log("aggregation", f"Запускаем ввод в оборот АК по фильтру '{normalized_filter}'.")
            else:
                self._log("aggregation", "Запускаем ввод в оборот для всех АК в статусе readyForSend.")

            summary = self._run_with_session_retry(
                lambda session: self._introduce_aggregations_via_exact_codes(
                    session,
                    comment_filter=normalized_filter,
                    tsd_token=normalized_token,
                    production_date=prod,
                    expiration_date=exp,
                    batch_number=normalized_batch,
                    cert=cert,
                ),
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторной отправкой ввода в оборот АК",
            )
            return {"success": True, "summary": summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка ввода в оборот АК: {exc}")
            return {"success": False, "error": str(exc)}

    def introduce_aggregations(
        self,
        comment_filter: str,
        production_date: str,
        expiration_date: str,
        batch_number: str,
        tsd_token: str = "",
    ) -> Dict[str, Any]:
        try:
            cert = self._get_certificate()
            if not cert:
                raise RuntimeError("Не найден сертификат для подписи.")

            normalized_batch = str(batch_number or "").strip()
            if not normalized_batch:
                raise RuntimeError("Укажите номер партии.")

            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            normalized_filter = str(comment_filter or "").strip()
            if normalized_filter:
                self._log("aggregation", f"Запускаем ввод в оборот АК по фильтру '{normalized_filter}'.")
            else:
                self._log("aggregation", "Запускаем ввод в оборот для всех АК в статусе readyForSend.")

            summary = self._run_with_session_retry(
                lambda session: self._introduce_aggregations_via_exact_codes_file(
                    session,
                    comment_filter=normalized_filter,
                    production_date=prod,
                    expiration_date=exp,
                    batch_number=normalized_batch,
                    cert=cert,
                ),
                log_channel="aggregation",
                retry_message="Обновляем сессию перед повторной отправкой ввода в оборот АК",
            )
            return {"success": True, "summary": summary}
        except Exception as exc:
            self._log("aggregation", f"Ошибка ввода в оборот АК: {exc}")
            return {"success": False, "error": str(exc)}

    def get_labels_state(self) -> Dict[str, Any]:
        try:
            df = self._load_nomenclature_df()
            printers, default_printer = list_installed_printers()
            deleted_ids = self._get_deleted_document_ids()
            orders = sorted(
                [
                    item
                    for item in _get_runtime().history_db.get_all_orders()
                    if str(item.get("document_id") or "").strip() not in deleted_ids
                ],
                key=lambda order: str(order.get("updated_at") or order.get("created_at") or ""),
                reverse=True,
            )
            serialized_orders = []
            for order in orders[:300]:
                try:
                    metadata = resolve_order_metadata(order, df)
                    serialized_orders.append(
                        {
                            "document_id": metadata.document_id,
                            "order_name": metadata.order_name,
                            "gtin": metadata.gtin,
                            "full_name": metadata.full_name,
                            "simpl_name": metadata.simpl_name,
                            "size": metadata.size,
                            "batch": metadata.batch,
                            "color": metadata.color,
                            "units_per_pack": metadata.units_per_pack,
                            "order_quantity": metadata.order_quantity,
                        }
                    )
                except Exception:
                    serialized_orders.append(
                        {
                            "document_id": str(order.get("document_id") or "").strip(),
                            "order_name": str(order.get("order_name") or "").strip(),
                            "gtin": _extract_gtin(order),
                            "full_name": _extract_position_name(order),
                            "simpl_name": str(order.get("simpl") or "").strip(),
                            "size": "",
                            "batch": "",
                            "color": "",
                            "units_per_pack": 0,
                            "order_quantity": 0,
                        }
                    )

            sheet_formats = list_label_sheet_formats()
            templates: list[dict[str, Any]] = []
            for sheet_format in sheet_formats:
                sheet_format_key = str(sheet_format.get("key") or DEFAULT_LABEL_SHEET_FORMAT).strip()
                sheet_format_label = str(sheet_format.get("label") or sheet_format_key).strip()
                for item in list_label_templates(sheet_format_key):
                    templates.append(
                        {
                            "name": item.name,
                            "category": item.category,
                            "relative_path": item.relative_path,
                            "path": item.path,
                            "data_source_kind": item.data_source_kind,
                            "source_label": "Агрег коды км" if item.data_source_kind == AGGREGATION_SOURCE_KIND else "Коды км",
                            "sheet_format": sheet_format_key,
                            "sheet_format_label": sheet_format_label,
                        }
                    )

            return {
                "sheet_formats": sheet_formats,
                "default_sheet_format": DEFAULT_LABEL_SHEET_FORMAT,
                "templates": templates,
                "aggregation_files": [
                    {
                        "name": item.name,
                        "folder_name": item.folder_name,
                        "path": item.path,
                        "record_count": item.record_count,
                        "modified_timestamp": item.modified_timestamp,
                    }
                    for item in list_aggregation_csv_files()[:200]
                ],
                "marking_files": [
                    {
                        "name": item.name,
                        "folder_name": item.folder_name,
                        "path": item.path,
                        "record_count": item.record_count,
                        "modified_timestamp": item.modified_timestamp,
                    }
                    for item in list_marking_csv_files()[:200]
                ],
                "orders": serialized_orders,
                "printers": printers,
                "default_printer": default_printer,
                "data_source_kinds": {
                    "aggregation": AGGREGATION_SOURCE_KIND,
                    "marking": MARKING_SOURCE_KIND,
                },
            }
        except Exception as exc:
            return {"error": str(exc)}

    def _read_label_csv_rows(self, csv_path: Path) -> tuple[list[list[str]], str]:
        encodings = ("utf-8-sig", "utf-8", "cp1251")
        last_error: Optional[Exception] = None
        for encoding in encodings:
            try:
                sample_line = ""
                with csv_path.open("r", encoding=encoding, newline="") as csv_file:
                    for raw_line in csv_file:
                        if str(raw_line or "").strip():
                            sample_line = raw_line
                            break

                delimiter = "\t"
                if ";" in sample_line and "\t" not in sample_line:
                    delimiter = ";"
                elif "," in sample_line and "\t" not in sample_line and ";" not in sample_line:
                    delimiter = ","

                rows: list[list[str]] = []
                with csv_path.open("r", encoding=encoding, newline="") as csv_file:
                    reader = csv.reader(csv_file, delimiter=delimiter)
                    for row in reader:
                        prepared = [str(cell or "") for cell in row]
                        if len(prepared) == 1:
                            raw_line = str(prepared[0] or "")
                            if delimiter == "\t" and "\t" in raw_line:
                                prepared = raw_line.split("\t")
                            elif delimiter == ";" and ";" in raw_line:
                                prepared = raw_line.split(";")
                            elif delimiter == "," and "," in raw_line:
                                prepared = raw_line.split(",")
                        if any(cell.strip() for cell in prepared):
                            rows.append(prepared)
                return rows, delimiter
            except UnicodeDecodeError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise RuntimeError(f"Не удалось прочитать CSV для печати: {csv_path}")

    def _resolve_label_template_info(self, *, sheet_format: str, template_path: str) -> Dict[str, Any]:
        normalized_template_path = str(template_path or "").strip()
        if not normalized_template_path:
            raise RuntimeError("Выберите шаблон BarTender.")

        resolved_template_path = str(Path(normalized_template_path).resolve())
        for item in list_label_templates(sheet_format):
            if str(Path(item.path).resolve()) == resolved_template_path:
                return {
                    "path": item.path,
                    "category": item.category,
                    "data_source_kind": item.data_source_kind,
                    "name": item.name,
                }

        template_file = Path(normalized_template_path)
        if not template_file.exists():
            raise RuntimeError(f"Шаблон BarTender не найден: {template_file}")
        return {
            "path": str(template_file),
            "category": template_file.parent.name,
            "data_source_kind": MARKING_SOURCE_KIND,
            "name": template_file.name,
        }

    @staticmethod
    def _shorten_label_preview_value(value: str, *, limit: int = 96) -> str:
        prepared = str(value or "").replace("\x1d", "\\x1d").strip()
        if len(prepared) <= limit:
            return prepared
        head = max(24, limit // 2 - 8)
        tail = max(16, limit - head - 1)
        return f"{prepared[:head]}вЂ¦{prepared[-tail:]}"

    def _build_label_record_preview(self, row: Sequence[str], data_source_kind: str) -> Dict[str, Any]:
        first_value = str(row[0] or "").strip() if row else ""
        if data_source_kind == MARKING_SOURCE_KIND:
            normalized_code = self._normalize_full_marking_code(first_value)
            gtin = str(row[1] or "").strip() if len(row) > 1 else ""
            if not gtin and normalized_code.startswith("01") and len(normalized_code) >= 16:
                gtin = normalized_code[2:16]
            full_name = str(row[2] or "").strip() if len(row) > 2 else ""
            return {
                "kind": "marking",
                "label": "Код маркировки",
                "value": normalized_code or first_value,
                "value_short": self._shorten_label_preview_value(normalized_code or first_value),
                "gtin": gtin,
                "full_name": full_name,
            }

        aggregate_code = first_value
        return {
            "kind": "aggregation",
            "label": "Агрегационный код",
            "value": aggregate_code,
            "value_short": self._shorten_label_preview_value(aggregate_code, limit=64),
            "gtin": "",
            "full_name": "",
        }

    def _resolve_label_print_selection(
        self,
        *,
        template_info: Dict[str, Any],
        csv_path: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        print_scope = str(payload.get("print_scope") or "all").strip().lower()
        if print_scope not in {"all", "single"}:
            print_scope = "all"

        normalized_csv_path = str(csv_path or "").strip()
        if not normalized_csv_path:
            raise RuntimeError("Выберите файл с кодами для печати.")

        rows, delimiter = self._read_label_csv_rows(Path(normalized_csv_path))
        total_record_count = len(rows)
        if print_scope != "single":
            return {
                "print_scope": "all",
                "csv_path": normalized_csv_path,
                "cleanup_path": None,
                "total_record_count": total_record_count,
                "selected_record_number": None,
                "record_preview": None,
            }

        raw_record_number = payload.get("record_number")
        try:
            selected_record_number = int(str(raw_record_number or "").strip())
        except (TypeError, ValueError):
            raise RuntimeError("Укажите корректный номер этикетки для печати.")

        if selected_record_number < 1 or selected_record_number > total_record_count:
            raise RuntimeError(
                f"Номер этикетки должен быть в диапазоне от 1 до {total_record_count}."
            )

        selected_row = rows[selected_record_number - 1]
        record_preview = self._build_label_record_preview(
            selected_row,
            str(template_info.get("data_source_kind") or MARKING_SOURCE_KIND),
        )

        temp_csv_path = Path(tempfile.gettempdir()) / f"kontur_ui_v2_label_{uuid.uuid4().hex}.csv"
        with temp_csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.writer(csv_file, delimiter=delimiter, lineterminator="\n")
            writer.writerow(selected_row)

        return {
            "print_scope": "single",
            "csv_path": str(temp_csv_path),
            "cleanup_path": str(temp_csv_path),
            "total_record_count": total_record_count,
            "selected_record_number": selected_record_number,
            "record_preview": record_preview,
        }

    @staticmethod
    def _cleanup_label_selection(
        selection: Dict[str, Any],
        *,
        delay_seconds: int = 0,
    ) -> None:
        cleanup_path = str(selection.get("cleanup_path") or "").strip()
        if not cleanup_path:
            return
        cleanup_target = Path(cleanup_path)

        def _cleanup() -> None:
            try:
                if delay_seconds > 0:
                    time.sleep(max(1, int(delay_seconds)))
                cleanup_target.unlink()
            except OSError:
                pass
            except Exception:
                pass

        if delay_seconds > 0:
            Thread(
                target=_cleanup,
                name=f"kontur-ui-label-cleanup-{cleanup_target.stem[:24]}",
                daemon=True,
            ).start()
            return

        _cleanup()

    @staticmethod
    def _normalize_label_manual_text(value: Any) -> str:
        prepared = str(value or "").strip()
        if prepared.lower() in {"nan", "none", "null", "nat"}:
            return ""
        return prepared

    @staticmethod
    def _parse_label_positive_int(value: Any, *, field_name: str) -> int:
        prepared = str(value or "").strip().replace(" ", "").replace(",", ".")
        if not prepared:
            raise RuntimeError(f"Заполните поле '{field_name}'.")
        try:
            parsed_value = int(float(prepared))
        except ValueError as exc:
            raise RuntimeError(f"Поле '{field_name}' должно быть целым числом.") from exc
        if parsed_value <= 0:
            raise RuntimeError(f"Поле '{field_name}' должно быть больше нуля.")
        return parsed_value

    def _normalize_label_year_month(self, value: Any, *, field_name: str) -> str:
        normalized = self._parse_iso_date(str(value or "").strip(), field_name=field_name)
        return normalized[:7]

    @staticmethod
    def _pluralize_ru(value: int, singular: str, few: str, many: str) -> str:
        remainder10 = value % 10
        remainder100 = value % 100
        if remainder10 == 1 and remainder100 != 11:
            return singular
        if remainder10 in (2, 3, 4) and remainder100 not in (12, 13, 14):
            return few
        return many

    def _should_offer_manual_label_input(self, error_text: str) -> bool:
        normalized_error = str(error_text or "").lower()
        triggers = (
            "gtin",
            "nomenclature.xlsx",
            "размер",
            "партии",
            "товарные данные",
            "справочник",
            "не найден gtin",
        )
        return any(trigger in normalized_error for trigger in triggers)

    def _build_label_manual_form(self, *, order_data: Dict[str, Any], error_text: str) -> Dict[str, Any]:
        order_name = str(order_data.get("order_name") or "").strip()
        guessed_batch = ""
        batch_match = re.search(r"\b(\d{6})\b", order_name)
        if batch_match:
            guessed_batch = batch_match.group(1)

        guessed_size = ""
        size_match = re.search(r"\b(XXL|XL|XS|S|M|L|\d+[.,]\d)\b", order_name, flags=re.IGNORECASE)
        if size_match:
            guessed_size = size_match.group(1).upper().replace(".", ",")

        return {
            "prompt": "Не удалось автоматически прочитать данные заказа для этикетки. Заполните форму вручную и повторите действие.",
            "error": str(error_text or "").strip(),
            "fields": {
                "gtin": str(order_data.get("gtin") or "").strip(),
                "size": guessed_size,
                "batch": guessed_batch,
                "color": self._normalize_label_manual_text(order_data.get("color")),
                "units_per_pack": str(order_data.get("units_per_pack") or "").strip(),
            },
        }

    def _build_manual_label_context(
        self,
        *,
        order_data: Dict[str, Any],
        template_info: Dict[str, Any],
        csv_path: str,
        printer_name: str,
        manufacture_date: Any,
        expiration_date: Any,
        quantity_value: Any,
        manual_override: Dict[str, Any],
    ):
        template_path = str(template_info.get("path") or "").strip()
        template_file = Path(template_path)
        if not template_file.exists():
            raise RuntimeError(f"Шаблон BarTender не найден: {template_file}")

        csv_file = Path(str(csv_path or "").strip())
        if not csv_file.exists():
            raise RuntimeError(f"CSV для печати не найден: {csv_file}")

        rows, _delimiter = self._read_label_csv_rows(csv_file)
        label_count = len(rows)
        if label_count <= 0:
            raise RuntimeError(f"В CSV нет строк для печати: {csv_file}")

        printer_name_text = str(printer_name or "").strip()
        if not printer_name_text:
            raise RuntimeError("Не выбран принтер для печати этикеток.")

        size = self._normalize_label_manual_text(manual_override.get("size"))
        batch = self._normalize_label_manual_text(manual_override.get("batch"))
        color = self._normalize_label_manual_text(manual_override.get("color"))
        gtin = self._normalize_label_manual_text(manual_override.get("gtin")) or str(order_data.get("gtin") or "").strip()
        units_per_pack = self._parse_label_positive_int(
            manual_override.get("units_per_pack"),
            field_name="Единиц в упаковке",
        )

        if not size:
            raise RuntimeError("Заполните поле 'Размер'.")
        if not batch:
            raise RuntimeError("Заполните поле 'Партия'.")

        manufacture_date_text = self._normalize_label_year_month(
            manufacture_date,
            field_name="Дата изготовления",
        )
        expiration_date_text = self._normalize_label_year_month(
            expiration_date,
            field_name="Срок годности",
        )

        data_source_kind = str(template_info.get("data_source_kind") or MARKING_SOURCE_KIND).strip()
        if data_source_kind == AGGREGATION_SOURCE_KIND:
            quantity_pairs = self._parse_label_positive_int(quantity_value, field_name="Количество")
            if quantity_pairs % units_per_pack != 0:
                raise RuntimeError(
                    "Количество должно быть кратно значению 'Единиц в упаковке'. "
                    f"Сейчас: {quantity_pairs}, в упаковке: {units_per_pack}."
                )
            dispenser_count = quantity_pairs // units_per_pack
            package_text = (
                f"({dispenser_count} {self._pluralize_ru(dispenser_count, 'диспенсер', 'диспенсера', 'диспенсеров')} "
                f"по {units_per_pack} {self._pluralize_ru(units_per_pack, 'пара', 'пары', 'пар')})"
            )
        else:
            quantity_pairs = units_per_pack
            dispenser_count = 0
            package_text = None

        return SimpleNamespace(
            document_id=str(order_data.get("document_id") or "").strip(),
            order_name=str(order_data.get("order_name") or "").strip(),
            template_path=str(template_file),
            aggregation_csv_path=str(csv_file),
            printer_name=printer_name_text,
            data_source_kind=data_source_kind,
            template_category=str(template_info.get("category") or ""),
            label_count=label_count,
            gtin=gtin,
            size=size,
            batch=batch,
            color=color,
            manufacture_date=manufacture_date_text,
            expiration_date=expiration_date_text,
            quantity_pairs=quantity_pairs,
            quantity_pairs_word=self._pluralize_ru(quantity_pairs, "пара", "пары", "пар"),
            units_per_pack=units_per_pack,
            dispenser_count=dispenser_count,
            package_text=package_text,
        )

    def _resolve_label_context(
        self,
        *,
        sheet_format: str,
        order_data: Dict[str, Any],
        template_info: Dict[str, Any],
        selection: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        context_kwargs = {
            "sheet_format": sheet_format,
            "df": self._load_nomenclature_df(),
            "order_data": order_data,
            "template_path": str(template_info.get("path") or ""),
            "aggregation_csv_path": str(selection.get("csv_path") or ""),
            "printer_name": str(payload.get("printer_name") or ""),
            "manufacture_date": str(payload.get("manufacture_date") or ""),
            "expiration_date": str(payload.get("expiration_date") or ""),
            "quantity_value": payload.get("quantity_value"),
        }
        try:
            return {
                "context": build_label_print_context(**context_kwargs),
                "used_manual_override": False,
            }
        except Exception as exc:
            manual_override = payload.get("manual_override")
            if isinstance(manual_override, dict) and manual_override.get("enabled"):
                return {
                    "context": self._build_manual_label_context(
                        order_data=order_data,
                        template_info=template_info,
                        csv_path=str(selection.get("csv_path") or ""),
                        printer_name=str(payload.get("printer_name") or ""),
                        manufacture_date=payload.get("manufacture_date"),
                        expiration_date=payload.get("expiration_date"),
                        quantity_value=payload.get("quantity_value"),
                        manual_override=manual_override,
                    ),
                    "used_manual_override": True,
                }

            error_text = str(exc)
            if self._should_offer_manual_label_input(error_text):
                return {
                    "needs_manual_input": True,
                    "prompt": "Не удалось автоматически прочитать данные заказа для этикетки. Заполните форму вручную и повторите действие.",
                    "manual_form": self._build_label_manual_form(
                        order_data=order_data,
                        error_text=error_text,
                    ),
                }
            raise

    @staticmethod
    def _serialize_label_preview(context, selection: Dict[str, Any], *, sheet_format: str) -> Dict[str, Any]:
        record_preview = selection.get("record_preview") or {}
        return {
            "document_id": context.document_id,
            "order_name": context.order_name,
            "template_path": context.template_path,
            "sheet_format": sheet_format,
            "sheet_format_label": format_label_sheet_title(sheet_format),
            "aggregation_csv_path": context.aggregation_csv_path,
            "printer_name": context.printer_name,
            "data_source_kind": context.data_source_kind,
            "template_category": context.template_category,
            "label_count": context.label_count,
            "gtin": context.gtin,
            "size": context.size,
            "batch": context.batch,
            "color": context.color,
            "manufacture_date": context.manufacture_date,
            "expiration_date": context.expiration_date,
            "quantity_pairs": context.quantity_pairs,
            "quantity_pairs_word": context.quantity_pairs_word,
            "units_per_pack": context.units_per_pack,
            "dispenser_count": context.dispenser_count,
            "package_text": context.package_text,
            "print_scope": selection.get("print_scope") or "all",
            "print_scope_label": "Одна этикетка" if selection.get("print_scope") == "single" else "Весь файл",
            "total_record_count": int(selection.get("total_record_count") or context.label_count or 0),
            "selected_record_number": selection.get("selected_record_number"),
            "selected_code_label": record_preview.get("label") or "",
            "selected_code_value": record_preview.get("value") or "",
            "selected_code_value_short": record_preview.get("value_short") or "",
            "selected_code_gtin": record_preview.get("gtin") or "",
            "selected_code_name": record_preview.get("full_name") or "",
        }

    def preview_100x180_label(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            sheet_format = str(payload.get("sheet_format") or DEFAULT_LABEL_SHEET_FORMAT).strip()
            order_id = str(payload.get("document_id") or "").strip()
            template_path = str(payload.get("template_path") or "").strip()
            csv_path = str(payload.get("csv_path") or "").strip()
            order_data = _get_runtime().history_db.get_order_by_document_id(order_id)
            if not order_data:
                raise RuntimeError("Заказ не найден в истории.")
            template_info = self._resolve_label_template_info(
                sheet_format=sheet_format,
                template_path=template_path,
            )
            selection = self._resolve_label_print_selection(
                template_info=template_info,
                csv_path=csv_path,
                payload=payload,
            )
            try:
                context_result = self._resolve_label_context(
                    sheet_format=sheet_format,
                    order_data=order_data,
                    template_info=template_info,
                    selection=selection,
                    payload=payload,
                )
                if context_result.get("needs_manual_input"):
                    return {
                        "success": True,
                        "needs_manual_input": True,
                        "prompt": context_result.get("prompt") or "",
                        "manual_form": context_result.get("manual_form") or {},
                    }
                context = context_result["context"]
                preview_payload = self._serialize_label_preview(context, selection, sheet_format=sheet_format)
                preview_payload["manual_override_used"] = bool(context_result.get("used_manual_override"))
            finally:
                self._cleanup_label_selection(selection)
            return {
                "success": True,
                "preview": preview_payload,
            }
        except Exception as exc:
            self._log("labels", f"Ошибка подготовки контекста печати: {exc}")
            return {"success": False, "error": str(exc)}

    def print_100x180_label(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            sheet_format = str(payload.get("sheet_format") or DEFAULT_LABEL_SHEET_FORMAT).strip()
            sheet_format_label = format_label_sheet_title(sheet_format)
            order_id = str(payload.get("document_id") or "").strip()
            order_data = _get_runtime().history_db.get_order_by_document_id(order_id)
            if not order_data:
                raise RuntimeError("Заказ не найден в истории.")
            template_path = str(payload.get("template_path") or "").strip()
            template_info = self._resolve_label_template_info(
                sheet_format=sheet_format,
                template_path=template_path,
            )
            selection = self._resolve_label_print_selection(
                template_info=template_info,
                csv_path=str(payload.get("csv_path") or "").strip(),
                payload=payload,
            )
            cleanup_delegated = False
            try:
                context_result = self._resolve_label_context(
                    sheet_format=sheet_format,
                    order_data=order_data,
                    template_info=template_info,
                    selection=selection,
                    payload=payload,
                )
                if context_result.get("needs_manual_input"):
                    return {
                        "success": True,
                        "needs_manual_input": True,
                        "prompt": context_result.get("prompt") or "",
                        "manual_form": context_result.get("manual_form") or {},
                    }
                context = context_result["context"]
                preview_payload = self._serialize_label_preview(context, selection, sheet_format=sheet_format)
                preview_payload["manual_override_used"] = bool(context_result.get("used_manual_override"))
                self._run_background_job(
                    name=f"labels-print-{context.document_id or uuid.uuid4().hex}",
                    action=lambda: print_label_sheet(context),
                    error_log_channel="labels",
                    error_log_prefix=f"Ошибка печати {sheet_format_label}",
                    cleanup=lambda: self._cleanup_label_selection(
                        selection,
                        delay_seconds=LABEL_PRINT_SELECTION_CLEANUP_DELAY_SECONDS,
                    ),
                )
                cleanup_delegated = True
                if selection.get("print_scope") == "single":
                    self._log(
                        "labels",
                        f"Печать одной этикетки {sheet_format_label} отправлена в BarTender: "
                        f"{context.order_name}, запись №{selection.get('selected_record_number')} "
                        f"({(selection.get('record_preview') or {}).get('value_short') or 'код не распознан'})",
                    )
                else:
                    self._log("labels", f"Печать {sheet_format_label} отправлена в BarTender: {context.order_name}")
            finally:
                if not cleanup_delegated:
                    self._cleanup_label_selection(selection)
            return {"success": True, "preview": preview_payload}
        except Exception as exc:
            self._log("labels", f"Ошибка печати: {exc}")
            return {"success": False, "error": str(exc)}
