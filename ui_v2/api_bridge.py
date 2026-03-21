from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("HISTORY_SYNC_ENABLED", "0")

import api as api_module
from aggregation_bulk import BulkAggregationService
from api import (
    check_order_status,
    codes_order,
    download_codes,
    make_task_on_tsd,
    mark_order_as_tsd_created,
    put_into_circulation,
)
from bartender_label_100x180 import (
    AGGREGATION_SOURCE_KIND,
    MARKING_SOURCE_KIND,
    build_label_print_context,
    list_100x180_templates,
    list_aggregation_csv_files,
    list_marking_csv_files,
    print_100x180_labels,
    resolve_order_metadata,
)
from bartender_print import build_print_context, list_installed_printers, print_labels
from cookies import get_valid_cookies
from cryptopro import find_certificate_by_thumbprint, sign_data, sign_text_data
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


LOG_CHANNELS = ("orders", "download", "intro", "tsd", "aggregation", "labels")
MAX_LOG_LINES = 500
DOCUMENT_STATUS_CACHE_TTL_SECONDS = 60
CODE_STATUS_CACHE_TTL_SECONDS = 180
CODE_STATUS_SAMPLE_SIZE = 25
TRUE_STATUS_WORKER_TIMEOUT_SECONDS = 25
DELETED_ORDERS_DIRNAME = "Удаленные"
DELETED_ORDERS_FILE = "deleted_orders.json"

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
    "UNKNOWN": "Неизвестно",
}


def _translate_status(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Неизвестно"
    return STATUS_LABELS.get(raw, STATUS_LABELS.get(raw.lower(), raw))


def _format_status_counts(counts: Counter[str]) -> str:
    parts = []
    for raw_status, count in counts.items():
        parts.append(f"{_translate_status(raw_status)}: {count}")
    return ", ".join(parts)


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
        runtime.logs[channel].append(f"[{timestamp}] {message}")
        if len(runtime.logs[channel]) > MAX_LOG_LINES:
            runtime.logs[channel] = runtime.logs[channel][-MAX_LOG_LINES:]

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
            "can_tsd": is_order_ready_for_tsd(merged) and not tsd_created,
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

    def _load_nomenclature_df(self) -> pd.DataFrame:
        runtime = _get_runtime()
        if runtime.nomenclature_df is None:
            path = runtime.root_dir / "data" / "nomenclature.xlsx"
            runtime.nomenclature_df = pd.read_excel(path)
        return runtime.nomenclature_df

    def _ensure_session(self, force_refresh: bool = False) -> requests.Session:
        runtime = _get_runtime()
        with runtime.lock:
            age = time.time() - runtime.session_created_at if runtime.session_created_at else 0.0
            if force_refresh or runtime.session is None or age >= runtime.session_ttl_seconds:
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

        desktop = Path.home() / "Desktop" / "Коды км"
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
        desktop = Path.home() / "Desktop" / "Агрег коды км"
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

    def refresh_session(self) -> Dict[str, Any]:
        try:
            self._ensure_session(force_refresh=True)
            return {"success": True, "session": self.get_session_info()}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def get_logs(self, channel: str) -> List[str] | Dict[str, Any]:
        try:
            normalized = str(channel or "").strip()
            if normalized not in LOG_CHANNELS:
                raise RuntimeError(f"Неизвестный канал логов: {normalized}")
            return list(_get_runtime().logs[normalized])
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

    def print_download_order(self, document_id: str, printer_name: str) -> Dict[str, Any]:
        try:
            item = self._find_download_item(str(document_id or "").strip())
            if not item:
                raise RuntimeError("Заказ не найден в списке загрузок.")
            csv_path = self._resolve_order_csv_path(item)
            if not csv_path:
                raise RuntimeError("У заказа не найден CSV-файл с кодами маркировки.")
            context = build_print_context(
                order_name=str(item.get("order_name") or ""),
                document_id=str(item.get("document_id") or ""),
                csv_path=csv_path,
                printer_name=printer_name,
            )
            print_labels(context)
            self._log("download", f"Печать термоэтикеток запущена: {item.get('order_name')}")
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
            }
        except Exception as exc:
            self._log("download", f"Ошибка печати термоэтикеток: {exc}")
            return {"success": False, "error": str(exc)}

    def get_intro_state(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            runtime.load_download_items_from_history()
            session = self._ensure_session_safely("intro")
            deleted_ids = self._get_deleted_document_ids()
            ready_items = [item for item in runtime.download_items if is_order_ready_for_intro(item)]
            return {
                "items": [
                    self._serialize_download_item(
                        item,
                        session=session,
                        include_marking_status=self._should_include_marking_status(item),
                    )
                    for item in ready_items
                    if str(item.get("document_id") or "").strip() not in deleted_ids
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
            session = self._ensure_session()
            thumbprint = self._get_thumbprint()
            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            results = []
            errors = []

            for document_id in document_ids:
                item = self._find_download_item(str(document_id or "").strip())
                if not item:
                    errors.append({"document_id": document_id, "error": "Заказ не найден"})
                    continue
                if not is_order_ready_for_intro(item):
                    errors.append({"document_id": document_id, "error": "Заказ ещё не готов для ввода в оборот"})
                    continue

                self._log("intro", f"Запускаем ввод в оборот: {item.get('order_name')}")
                patch = self._build_intro_patch(item, prod, exp, str(batch_number or "").strip())
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

    def get_tsd_state(self) -> Dict[str, Any]:
        try:
            runtime = _get_runtime()
            deleted_ids = self._get_deleted_document_ids()
            session = self._ensure_session_safely("tsd")
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
                        include_marking_status=self._should_include_marking_status(item),
                    )
                    for item in orders[:120]
                ],
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

            session = self._ensure_session()
            prod = self._parse_iso_date(production_date, field_name="Дата производства")
            exp = self._parse_iso_date(expiration_date, field_name="Срок годности")
            results = []
            errors = []

            for document_id in document_ids:
                item = self._find_download_item(str(document_id or "").strip())
                if not item:
                    history_order = _get_runtime().history_db.get_order_by_document_id(str(document_id or "").strip())
                    if history_order:
                        item = _history_order_to_download_item(history_order)
                    else:
                        errors.append({"document_id": document_id, "error": "Заказ не найден"})
                        continue
                if item.get("tsd_created") or (item.get("history_data") or {}).get("tsd_created"):
                    errors.append({"document_id": document_id, "error": "Задание на ТСД уже создано"})
                    continue
                if not is_order_ready_for_tsd(item):
                    errors.append({"document_id": document_id, "error": "Заказ ещё не готов для задания на ТСД"})
                    continue

                self._log("tsd", f"Создаём задание на ТСД: {item.get('order_name')}")
                positions_data, production_patch = self._build_tsd_payload(
                    item,
                    str(intro_number or "").strip(),
                    prod,
                    exp,
                    str(batch_number or "").strip(),
                )
                ok, result = make_task_on_tsd(
                    session=session,
                    codes_order_id=str(item.get("document_id") or ""),
                    positions_data=positions_data,
                    production_patch=production_patch,
                )
                if ok:
                    introduction_id = result.get("introduction_id", "")
                    mark_order_as_tsd_created(str(item.get("document_id") or ""), introduction_id)
                    remove_order_by_document_id(_get_runtime().download_items, str(item.get("document_id") or ""))
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
                "state": self.get_tsd_state(),
                "download_state": self.get_download_state(),
            }
        except Exception as exc:
            self._log("tsd", f"Ошибка создания заданий на ТСД: {exc}")
            return {"success": False, "error": str(exc)}

    def get_aggregation_state(self) -> Dict[str, Any]:
        try:
            return {
                "csv_files": [
                    {
                        "name": item.name,
                        "folder_name": item.folder_name,
                        "path": item.path,
                        "record_count": item.record_count,
                        "modified_timestamp": item.modified_timestamp,
                    }
                    for item in list_aggregation_csv_files()[:100]
                ],
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
            data = self._run_with_session_retry(
                lambda session: self._create_aggregate_codes(session, normalized_comment, normalized_count),
                log_channel="aggregation",
                retry_message="Получили ошибку создания АК, обновляем cookies и повторяем",
            )
            self._log("aggregation", f"Создано АК: {normalized_comment}, количество {len(data)}")
            return {"success": True, "created_count": len(data), "items": data}
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
            return {"success": True, "summary": self._serialize_summary(summary)}
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
            return {"success": True, "summary": self._serialize_summary(summary)}
        except Exception as exc:
            self._log("aggregation", f"Ошибка повторного наполнения АК: {exc}")
            return {"success": False, "error": str(exc)}

    def get_labels_state(self) -> Dict[str, Any]:
        try:
            df = self._load_nomenclature_df()
            printers, default_printer = list_installed_printers()
            deleted_ids = self._get_deleted_document_ids()
            orders = [
                item
                for item in _get_runtime().history_db.get_all_orders()
                if str(item.get("document_id") or "").strip() not in deleted_ids
            ]
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

            return {
                "templates": [
                    {
                        "name": item.name,
                        "category": item.category,
                        "relative_path": item.relative_path,
                        "path": item.path,
                        "data_source_kind": item.data_source_kind,
                        "source_label": "Агрег коды км" if item.data_source_kind == AGGREGATION_SOURCE_KIND else "Коды км",
                    }
                    for item in list_100x180_templates()
                ],
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

    def preview_100x180_label(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            order_id = str(payload.get("document_id") or "").strip()
            template_path = str(payload.get("template_path") or "").strip()
            csv_path = str(payload.get("csv_path") or "").strip()
            printer_name = str(payload.get("printer_name") or "").strip()
            order_data = _get_runtime().history_db.get_order_by_document_id(order_id)
            if not order_data:
                raise RuntimeError("Заказ не найден в истории.")
            context = build_label_print_context(
                df=self._load_nomenclature_df(),
                order_data=order_data,
                template_path=template_path,
                aggregation_csv_path=csv_path,
                printer_name=printer_name,
                manufacture_date=str(payload.get("manufacture_date") or ""),
                expiration_date=str(payload.get("expiration_date") or ""),
                quantity_value=payload.get("quantity_value"),
            )
            return {
                "success": True,
                "preview": {
                    "document_id": context.document_id,
                    "order_name": context.order_name,
                    "template_path": context.template_path,
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
                },
            }
        except Exception as exc:
            self._log("labels", f"Ошибка подготовки контекста 100x180: {exc}")
            return {"success": False, "error": str(exc)}

    def print_100x180_label(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            preview_result = self.preview_100x180_label(payload)
            if not preview_result.get("success"):
                return preview_result
            order_id = str(payload.get("document_id") or "").strip()
            order_data = _get_runtime().history_db.get_order_by_document_id(order_id)
            context = build_label_print_context(
                df=self._load_nomenclature_df(),
                order_data=order_data,
                template_path=str(payload.get("template_path") or "").strip(),
                aggregation_csv_path=str(payload.get("csv_path") or "").strip(),
                printer_name=str(payload.get("printer_name") or "").strip(),
                manufacture_date=str(payload.get("manufacture_date") or ""),
                expiration_date=str(payload.get("expiration_date") or ""),
                quantity_value=payload.get("quantity_value"),
            )
            print_100x180_labels(context)
            self._log("labels", f"Печать 100x180 запущена: {context.order_name}")
            return {"success": True, "preview": preview_result.get("preview")}
        except Exception as exc:
            self._log("labels", f"Ошибка печати 100x180: {exc}")
            return {"success": False, "error": str(exc)}
