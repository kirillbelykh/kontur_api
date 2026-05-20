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
