from __future__ import annotations

from typing import Any, MutableSequence


DOWNLOADED_STATUSES = {
    "Скачан",
    "Скачаны",
    "Downloaded",
    "downloaded",
}

TSD_READY_STATUSES = DOWNLOADED_STATUSES | {
    "Ожидает",
    "Скачивается",
    "Готов для ТСД",
    "Готов для задания на ТСД",
    "released",
    "received",
}


def is_order_ready_for_intro(item: dict[str, Any]) -> bool:
    """Return whether a code order has enough local data for introduction."""
    if not item.get("document_id"):
        return False
    return item.get("status") in DOWNLOADED_STATUSES or bool(item.get("filename"))


def is_order_ready_for_tsd(item: dict[str, Any]) -> bool:
    """Return whether a code order can be used to create a TSD task."""
    if not item.get("document_id"):
        return False
    return item.get("status") in TSD_READY_STATUSES or bool(item.get("filename"))


def remove_order_by_document_id(
    download_list: MutableSequence[dict[str, Any]],
    document_id: str | None,
) -> bool:
    """Remove a queued order by ``document_id`` and report whether it changed."""
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
