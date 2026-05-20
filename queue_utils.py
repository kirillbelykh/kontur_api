from pathlib import Path
from typing import Any


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
    "unknown": "Неизвестно",
}

INTRO_READY_RAW_STATUSES = {"downloaded"}
TSD_READY_RAW_STATUSES = {"released", "received", "downloaded"}
LOCAL_TRANSIENT_STATUSES = {"Скачивается", "Готов для ТСД", "Отправлено на ТСД"}


def normalize_status_key(value: Any) -> str:
    return str(value or "").strip()


def translate_order_status(value: Any) -> str:
    raw = normalize_status_key(value)
    if not raw:
        return "Неизвестно"
    return STATUS_LABELS.get(raw, STATUS_LABELS.get(raw.lower(), raw))


def _iter_order_file_candidates(item: dict[str, Any]):
    for key in ("csv_path", "pdf_path", "xls_path", "filename"):
        value = item.get(key)
        if not value:
            continue
        chunks = [value] if not isinstance(value, str) else value.split(",")
        for chunk in chunks:
            candidate = str(chunk or "").strip()
            if candidate:
                yield candidate


def has_local_downloaded_files(item: dict[str, Any]) -> bool:
    for candidate in _iter_order_file_candidates(item):
        try:
            if Path(candidate).exists():
                return True
        except OSError:
            continue
    return False


def get_download_tab_status(item: dict[str, Any]) -> str:
    return "Скачан" if has_local_downloaded_files(item) else "Не скачан"


def get_intro_tab_status(item: dict[str, Any]) -> str:
    raw = normalize_status_key(item.get("status"))
    raw_lower = raw.lower()
    if raw_lower in {"introduced", "applied"} or raw in {"Введен в оборот", "Введены в оборот"}:
        return "Введены в оборот"
    return "Не введены в оборот"


def get_tsd_tab_status(item: dict[str, Any]) -> str:
    raw = normalize_status_key(item.get("status"))
    raw_lower = raw.lower()
    if get_intro_tab_status(item) == "Введены в оборот":
        return "Введены в оборот"
    if bool(item.get("tsd_created")):
        return "Отправлено"
    if raw_lower in {"released", "received", "downloaded"} or raw in {"Скачан", "Готов для ТСД"} or is_order_ready_for_tsd(item):
        return "Наполнен на ТСД"
    return "Не отправлено"


def is_order_ready_for_intro(item: dict[str, Any]) -> bool:
    """Обычный ввод в оборот доступен после скачивания кодов."""
    if not item.get("document_id"):
        return False
    status = normalize_status_key(item.get("status"))
    return bool(item.get("filename") or item.get("csv_path") or status in INTRO_READY_RAW_STATUSES)


def is_order_ready_for_tsd(item: dict[str, Any]) -> bool:
    """Задание на ТСД доступно для готовых заказов и уже скачанных файлов."""
    if not item.get("document_id"):
        return False
    status = normalize_status_key(item.get("status"))
    return bool(
        item.get("filename")
        or item.get("csv_path")
        or status in TSD_READY_RAW_STATUSES
        or status in LOCAL_TRANSIENT_STATUSES
    )


def remove_order_by_document_id(download_list: list[dict[str, Any]], document_id: str | None) -> bool:
    """Удаляет заказ из активной очереди по document_id."""
    if not document_id:
        return False

    remaining_items = [
        item for item in download_list
        if item.get("document_id") != document_id
    ]
    removed = len(remaining_items) != len(download_list)
    if removed:
        download_list[:] = remaining_items
    return removed
