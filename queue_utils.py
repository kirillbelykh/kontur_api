from typing import Any

DOWNLOADED_STATUSES = {"Скачан", "Downloaded"}
TSD_READY_STATUSES = DOWNLOADED_STATUSES | {"Ожидает", "Скачивается", "Готов для ТСД"}


def is_order_ready_for_intro(item: dict[str, Any]) -> bool:
    """Обычный ввод в оборот доступен только после успешного скачивания кодов."""
    if not item.get("document_id"):
        return False
    return item.get("status") in DOWNLOADED_STATUSES or bool(item.get("filename"))


def is_order_ready_for_tsd(item: dict[str, Any]) -> bool:
    """Задание на ТСД доступно сразу после заказа кодов и после скачивания тоже."""
    if not item.get("document_id"):
        return False
    return item.get("status") in TSD_READY_STATUSES or bool(item.get("filename"))


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
